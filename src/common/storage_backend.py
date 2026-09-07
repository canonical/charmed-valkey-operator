#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Remote object-store backends for Valkey RDB backup/restore."""

from __future__ import annotations

import json
import logging
import pathlib
import shutil
from enum import Enum
from typing import IO, TYPE_CHECKING, BinaryIO, NamedTuple, Protocol, cast
from urllib.parse import urlparse

import boto3
import requests
from azure.core.exceptions import AzureError, ResourceExistsError
from azure.storage.blob import ContainerClient
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from google.api_core.exceptions import (
    Conflict,
    Forbidden,
    GoogleAPIError,
    NotFound,
    RetryError,
    from_http_status,
)
from google.auth.exceptions import GoogleAuthError
from google.cloud import storage
from google.cloud.storage.exceptions import DataCorruption, InvalidResponse

from common.exceptions import StorageBackendError
from core.models import AzureStorageParameters, BackupCredentials, GCSParameters, S3Parameters
from literals import AZURE_HTTPS_PROTOCOLS, GCS_INVALID_KEY_CODE

if TYPE_CHECKING:
    from azure.storage.blob import BlobClient
    from google.cloud.storage.blob import Blob as GCSBlob
    from google.cloud.storage.bucket import Bucket as GCSBucket
    from mypy_boto3_s3.literals import BucketLocationConstraintType
    from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource

logger = logging.getLogger(__name__)

# Every root the GCS SDK can raise past its own retries. InvalidResponse and
# DataCorruption derive from plain Exception and reach us raw from the BlobWriter
# path, which the SDK never wraps in _raise_from_invalid_response.
_GCS_ERRORS = (
    GoogleAPIError,
    GoogleAuthError,
    InvalidResponse,
    DataCorruption,
    requests.RequestException,
)


class RemoteObject(NamedTuple):
    """A stored object opened for reading."""

    body: BinaryIO
    """Binary read()-able stream of the object's content."""

    size: int | None
    """Length in bytes, when the backend reports one alongside the body."""


class StorageBackend(Protocol):
    """Remote object-store operations, keyed by backup-id under a fixed path prefix.

    Every method raises ``StorageBackendError`` (carrying the backend's structured
    error code) instead of leaking an SDK-specific exception to its caller, so
    ``BackupManager`` needs no knowledge of which object store is behind it.
    """

    @property
    def location(self) -> str:
        """Credential-free description of where objects land, for the audit log."""
        ...

    def ensure_container(self) -> None:
        """Idempotently create the bucket/container."""
        ...

    def list_object_ids(self) -> list[str]:
        """Return the object ids stored under the path prefix (prefix stripped)."""
        ...

    def head(self, backup_id: str, n: int = 16) -> bytes:
        """Return the first ``n`` bytes of the object for ``backup_id``."""
        ...

    def upload(self, backup_id: str, reader: IO[bytes]) -> None:
        """Stream ``reader`` into the object for ``backup_id``.

        Must not replace a stored object: where the store can express the
        precondition (Azure's ``overwrite=False``, GCS's ``if_generation_match=0``),
        use it. ``BackupManager`` checks the id up front for the backends whose SDK
        cannot.
        """
        ...

    def download(self, backup_id: str) -> RemoteObject:
        """Open the object for ``backup_id`` for reading."""
        ...

    def delete(self, backup_id: str) -> None:
        """Best-effort delete of the object for ``backup_id``."""
        ...


def build_backend(params: BackupCredentials, ca_path: pathlib.Path) -> StorageBackend:
    """Return the backend that handles ``params``.

    The single place a credentials type maps to a backend: a new backend is added
    here and nowhere above, so BackupManager never learns which stores exist.
    ``ca_path`` is the charm-local CA bundle, used only by backends that need one.
    """
    if isinstance(params, S3Parameters):
        return S3Backend(params, ca_path)
    if isinstance(params, AzureStorageParameters):
        return AzureBackend(params)
    if isinstance(params, GCSParameters):
        return GCSBackend(params)
    raise StorageBackendError(f"No storage backend for {type(params).__name__} credentials")


class S3Backend:
    """S3 object store via boto3."""

    _EXISTS_CODES = frozenset(
        {"BucketAlreadyOwnedByYou", "BucketAlreadyExists", "BucketNameUnavailable"}
    )
    """CreateBucket codes meaning the bucket is already there -- not a failure."""

    def __init__(self, params: "S3Parameters", ca_path: pathlib.Path):
        self.params = params
        # Charm-process-local CA bundle for the endpoint; written by BackupManager,
        # which owns the charm dir. See BackupManager._backup_ca_path.
        self._ca_path = ca_path

    @property
    def location(self) -> str:
        """Endpoint host, bucket and prefix -- the audit trail's destination.

        ``hostname``, not ``netloc``: a URL carrying userinfo must never put
        credentials into the log.
        """
        parsed = urlparse(self.params.endpoint)
        host = parsed.hostname or self.params.endpoint
        if parsed.port:
            host = f"{host}:{parsed.port}"
        return f"s3://{host}/{self.params.bucket}/{self.params.path}"

    def _key(self, backup_id: str) -> str:
        return f"{self.params.path}/{backup_id}"

    @staticmethod
    def _error_code(exc: ClientError | BotoCoreError) -> str:
        """Structured error code of a botocore failure ("AccessDenied", ...).

        Read the code, never the message: alt-S3 backends reword and recase the
        message, and the code is the only part safe to surface in an action result.
        Empty for a ``BotoCoreError``, which the service never answered at all.
        """
        if not isinstance(exc, ClientError):
            return ""
        return exc.response.get("Error", {}).get("Code", "")

    def _bucket(self) -> "Bucket":
        """Build a boto3 Bucket resource configured per the s3-integrator envelope."""
        verify: bool | str = True
        if self.params.tls_ca_chain:
            verify = self._ca_path.as_posix()

        # Scope creds to a Session so they don't surface in boto3 traceback repr(args).
        session = boto3.Session(
            aws_access_key_id=self.params.access_key,
            aws_secret_access_key=self.params.secret_key,
            region_name=self.params.region,
        )
        s3 = cast(
            "S3ServiceResource",
            session.resource(
                "s3",
                endpoint_url=self.params.endpoint,
                config=Config(
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                ),
                verify=verify,
            ),
        )
        return s3.Bucket(self.params.bucket)

    def ensure_container(self) -> None:
        """Create the configured bucket; idempotent across S3 implementations."""
        bucket = self._bucket()
        region = self.params.region
        try:
            # us-east-1 must NOT be sent as a LocationConstraint (CreateBucket
            # rejects it); any other region is passed explicitly. See aws-sdk-js#3647.
            if region and region != "us-east-1":
                bucket.create(
                    CreateBucketConfiguration={
                        # stub wants a region Literal; any non-default region is valid.
                        "LocationConstraint": cast("BucketLocationConstraintType", region)
                    }
                )
            else:
                bucket.create()
            # Bound the wait (boto3 default is up to 100s) so a slow endpoint can't
            # block leader_elected. The stub doesn't model WaiterConfig here.
            bucket.wait_until_exists(
                WaiterConfig={"Delay": 1, "MaxAttempts": 5}  # pyright: ignore[reportCallIssue]
            )
        except (ClientError, BotoCoreError) as e:
            if self._error_code(e) in self._EXISTS_CODES:
                logger.info("Using existing bucket %s", self.params.bucket)
                return
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e

    def list_object_ids(self) -> list[str]:
        """Return every object id stored under the path prefix (auto-paginated)."""
        prefix = f"{self.params.path}/"
        try:
            keys = [obj.key for obj in self._bucket().objects.filter(Prefix=prefix)]
        except (ClientError, BotoCoreError) as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e
        return [k.removeprefix(prefix) for k in keys]

    def head(self, backup_id: str, n: int = 16) -> bytes:
        """Ranged GET of the first ``n`` bytes of the object for ``backup_id``."""
        try:
            return (
                self._bucket()
                .Object(self._key(backup_id))
                .get(Range=f"bytes=0-{n - 1}")["Body"]
                .read()
            )
        except (ClientError, BotoCoreError) as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e

    def upload(self, backup_id: str, reader: IO[bytes]) -> None:
        """Stream ``reader`` into the object for ``backup_id`` (managed multipart)."""
        try:
            self._bucket().upload_fileobj(
                reader,
                self._key(backup_id),
                Config=TransferConfig(multipart_chunksize=8 * 1024 * 1024),
            )
        except (ClientError, BotoCoreError) as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e

    def download(self, backup_id: str) -> RemoteObject:
        """Open the object for ``backup_id``; its StreamingBody reads as binary."""
        try:
            obj = self._bucket().Object(self._key(backup_id)).get()
        except (ClientError, BotoCoreError) as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e
        # The StreamingBody is a binary read()-able at runtime (cast for the stub).
        return RemoteObject(cast(BinaryIO, obj["Body"]), obj.get("ContentLength"))

    def delete(self, backup_id: str) -> None:
        """Delete the object for ``backup_id``, swallowing any error (best-effort)."""
        try:
            self._bucket().Object(self._key(backup_id)).delete()
        except Exception as e:  # best-effort cleanup
            logger.warning("Failed to delete object %s: %s", self._key(backup_id), e)


class AzureBackend:
    """Azure Blob object store via azure-storage-blob."""

    def __init__(self, params: "AzureStorageParameters"):
        self.params = params

    @property
    def location(self) -> str:
        """Account host, container and prefix -- the audit trail's destination.

        ``hostname``, not the raw URL: an endpoint carrying userinfo must
        never put credentials into the log.
        """
        parsed = urlparse(self._account_url())
        host = parsed.hostname or self.params.storage_account
        if parsed.port:
            host = f"{host}:{parsed.port}"
        return f"azure://{host}/{self.params.container}/{self.params.path}"

    def _account_url(self) -> str:
        """Explicit endpoint (emulator/private) if given, else the derived Blob host."""
        if self.params.endpoint:
            return self.params.endpoint
        scheme = "https" if self.params.connection_protocol in AZURE_HTTPS_PROTOCOLS else "http"
        return f"{scheme}://{self.params.storage_account}.blob.core.windows.net"

    def _container(self) -> ContainerClient:
        """Build a client scoped to the configured container -- every call starts here.

        ``ContainerClient`` takes the account URL and container name itself, so
        there is no BlobServiceClient to hop through.

        The account name is passed explicitly rather than left to the SDK: given a
        bare key it derives the account from the host's first label, which raises
        "Unable to determine account name for shared key credential" whenever the
        endpoint is not ``<account>.blob.*`` -- an emulator or a custom domain,
        where the account sits in the URL path instead.
        """
        return ContainerClient(
            account_url=self._account_url(),
            container_name=self.params.container,
            credential={
                "account_name": self.params.storage_account,
                "account_key": self.params.secret_key,
            },
        )

    def _blob(self, backup_id: str) -> "BlobClient":
        return self._container().get_blob_client(self._key(backup_id))

    def _key(self, backup_id: str) -> str:
        return f"{self.params.path}/{backup_id}"

    @staticmethod
    def _error_code(exc: AzureError) -> str:
        """Structured error code of an azure-storage failure ("AuthenticationFailed", ...).

        ``process_storage_error`` attaches the provider's code as a ``StorageErrorCode``
        -- a ``(str, Enum)`` member whose ``str()`` renders "StorageErrorCode.AUTH..." --
        so read ``.value`` to get the wire code an action result should show. Absent on
        transport-level failures the service never answered, hence the default.
        """
        code = getattr(exc, "error_code", None) or ""
        return code.value if isinstance(code, Enum) else str(code)

    def ensure_container(self) -> None:
        """Create the configured container; tolerates an already-existing one."""
        try:
            self._container().create_container()
        except ResourceExistsError:
            # Subclass of AzureError, so this clause must stay first.
            logger.info("Using existing container %s", self.params.container)
        except AzureError as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e

    def list_object_ids(self) -> list[str]:
        """Return every blob id stored under the path prefix (auto-paginated)."""
        prefix = f"{self.params.path}/"
        try:
            names = [b.name for b in self._container().list_blobs(name_starts_with=prefix)]
        except AzureError as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e
        return [n.removeprefix(prefix) for n in names]

    def head(self, backup_id: str, n: int = 16) -> bytes:
        """Ranged read of the first ``n`` bytes of the blob for ``backup_id``."""
        try:
            return self._blob(backup_id).download_blob(offset=0, length=n).readall()
        except AzureError as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e

    def upload(self, backup_id: str, reader: IO[bytes]) -> None:
        """Stream ``reader`` into the blob for ``backup_id`` (sequential block blob).

        ``length=None`` + ``max_concurrency=1``: the source is a non-rewindable
        pipe from ``valkey-cli --rdb -``, so the SDK must stage blocks
        sequentially off ``read()`` rather than seek around a known-size stream.

        ``overwrite=False`` (the SDK's If-None-Match) so an existing snapshot is
        never replaced; the commit fails with ``ResourceExistsError`` instead.
        """
        try:
            self._blob(backup_id).upload_blob(
                reader, blob_type="BlockBlob", overwrite=False, length=None, max_concurrency=1
            )
        except AzureError as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e

    def download(self, backup_id: str) -> RemoteObject:
        """Open the blob for ``backup_id``; its downloader reads as binary."""
        try:
            downloader = self._blob(backup_id).download_blob()
        except AzureError as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e
        # The downloader already knows the blob length, so the restore trail gets
        # its byte count without a second round trip.
        return RemoteObject(cast(BinaryIO, downloader), downloader.size)

    def delete(self, backup_id: str) -> None:
        """Delete the blob for ``backup_id``, swallowing any error (best-effort)."""
        try:
            self._blob(backup_id).delete_blob()
        except Exception as e:  # best-effort cleanup
            logger.warning("Failed to delete blob %s: %s", self._key(backup_id), e)


class GCSBackend:
    """Google Cloud Storage via google-cloud-storage."""

    _CHUNK = 8 * 1024 * 1024
    """Resumable-upload and ranged-read chunk: a multiple of 256 KiB, as the SDK
    requires for resumable sessions, and the same size as S3's multipart part."""

    def __init__(self, params: "GCSParameters"):
        self.params = params

    @property
    def location(self) -> str:
        """Bucket and prefix -- the audit trail's destination.

        No host in a ``gs://`` locator, so no userinfo can ever reach the log.
        """
        return f"gs://{self.params.bucket}/{self.params.path}"

    def _info(self) -> dict:
        """Parse the service-account key. GCSParameters canonicalised it to JSON."""
        return json.loads(self.params.secret_key)

    def _client(self) -> storage.Client:
        """Build a client from the service-account key -- every call starts here.

        ``project`` is passed explicitly for clarity (the SDK would also take it
        from the key). No env vars, no Application Default Credentials.

        cryptography rejects a PEM body it cannot parse with a bare ValueError;
        its message names the format, never the key material.
        """
        info = self._info()
        try:
            return storage.Client.from_service_account_info(info, project=info.get("project_id"))
        except ValueError as e:
            raise StorageBackendError(str(e), safe_code=GCS_INVALID_KEY_CODE) from e

    def _bucket(self) -> "GCSBucket":
        """Return a bucket handle; no round trip until a method is called on it."""
        return self._client().bucket(self.params.bucket)

    def _blob(self, backup_id: str) -> "GCSBlob":
        return self._bucket().blob(self._key(backup_id))

    def _key(self, backup_id: str) -> str:
        return f"{self.params.path}/{backup_id}"

    @staticmethod
    def _error_code(exc: BaseException) -> str:
        """Structured code of an SDK failure: the exception class name.

        ``Forbidden``, ``NotFound``, ``PreconditionFailed``, ``RefreshError`` --
        stable and structured, unlike ``str(exc)``, which the SDK formats as
        ``<status> <verb> <url>: <message>`` and which must never reach a
        world-readable action result. Two normalisations first: a ``RetryError``
        is named after its cause (a 503 storm reads ``ServiceUnavailable``), and
        an ``InvalidResponse`` from the resumable-media layer carries only an
        HTTP status, mapped to the api_core class of that status.
        """
        if isinstance(exc, RetryError) and exc.cause is not None:
            exc = exc.cause
        if isinstance(exc, InvalidResponse):
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if isinstance(status, int):
                return type(from_http_status(status, "")).__name__
        return type(exc).__name__

    def ensure_container(self) -> None:
        """Create the configured bucket; tolerates an already-existing one.

        Get first. NotFound means absent (GCS answers 404 only for a name nobody
        owns), so create; a Conflict there is a race on a global name -- usually
        ours, and if not, the first backup reports Forbidden. Forbidden on the
        get means the bucket exists but its metadata is not readable: our bucket
        under a key without ``storage.buckets.get``, or somebody else's. A
        create could only fail, so a one-object list under the prefix decides
        instead -- rather than storing credentials that fail at the first backup.
        """
        try:
            client = self._client()
            try:
                client.get_bucket(self.params.bucket)
                return
            except NotFound:
                bucket = client.bucket(self.params.bucket)
                if self.params.storage_class:
                    bucket.storage_class = self.params.storage_class
                try:
                    client.create_bucket(bucket, project=self._info().get("project_id"))
                except Conflict:
                    logger.info("Using existing bucket %s", self.params.bucket)
            except Forbidden:
                prefix = f"{self.params.path}/"
                next(
                    iter(client.list_blobs(self.params.bucket, prefix=prefix, max_results=1)),
                    None,
                )
                logger.info("Using existing bucket %s (objects listable)", self.params.bucket)
        except _GCS_ERRORS as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e

    def list_object_ids(self) -> list[str]:
        """Return every object id stored under the path prefix (auto-paginated)."""
        prefix = f"{self.params.path}/"
        try:
            names = [b.name for b in self._client().list_blobs(self.params.bucket, prefix=prefix)]
        except _GCS_ERRORS as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e
        return [n.removeprefix(prefix) for n in names]

    def head(self, backup_id: str, n: int = 16) -> bytes:
        """Ranged read of the first ``n`` bytes of the object for ``backup_id``."""
        try:
            return self._blob(backup_id).download_as_bytes(start=0, end=n - 1)
        except _GCS_ERRORS as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e

    def upload(self, backup_id: str, reader: IO[bytes]) -> None:
        """Stream ``reader`` into the object for ``backup_id`` (resumable upload).

        ``blob.open("wb")`` rather than ``upload_from_file``: the SDK's uploader
        calls ``tell()`` on its source, and the source here is a non-rewindable
        pipe from ``valkey-cli --rdb -``. The writer buffers ``_CHUNK`` bytes and
        drives the resumable session itself, so the pipe is only ever ``read()``.

        ``if_generation_match=0`` goes on the session-initiate request, so a
        colliding id fails before any RDB bytes leave the unit. On success the
        final chunk carries the SDK's own checksum (md5 on this build), which GCS
        verifies.

        Two cancellation paths, both needed. ``__exit__`` terminates the session
        for an exception raised inside the block. But for an RDB under one chunk
        the session is initiated and its only chunk sent from ``close()``, i.e.
        inside ``__exit__`` on the success path; a failure there leaves the
        buffer open, and ``io.IOBase.__del__`` would re-send it at GC -- a commit
        after the action reported failure -- so the writer is terminated here.
        The cancel itself is best-effort: its DELETE can fail for the same
        network reason the upload did, and the original error is the one the
        action must report.
        """
        try:
            writer = self._blob(backup_id).open(
                "wb", chunk_size=self._CHUNK, ignore_flush=True, if_generation_match=0
            )
            try:
                with writer:
                    # blob.open("wb") is typed as a union of read/write/text
                    # handles (no py.typed in google-cloud-storage), but "wb"
                    # always returns a BlobWriter, which is binary-writable.
                    shutil.copyfileobj(reader, writer, self._CHUNK)  # pyright: ignore[reportArgumentType]
            except BaseException:
                if not writer.closed:
                    # Best-effort: the DELETE that cancels the session can fail for
                    # the same reason the upload did, and the original error is the
                    # one the action must report.
                    try:
                        writer.terminate()  # pyright: ignore[reportAttributeAccessIssue]
                    except Exception as e:
                        logger.warning(
                            "Could not cancel the resumable session for %s: %s",
                            self._key(backup_id),
                            e,
                        )
                raise
        except _GCS_ERRORS as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e

    def download(self, backup_id: str) -> RemoteObject:
        """Open the object for ``backup_id``; the reader fetches ranged chunks."""
        try:
            blob = self._bucket().get_blob(self._key(backup_id))
        except _GCS_ERRORS as e:
            raise StorageBackendError(str(e), safe_code=self._error_code(e)) from e
        if blob is None:
            raise StorageBackendError(
                f"No object for backup-id {backup_id}", safe_code=NotFound.__name__
            )
        # get_blob populated the metadata, so the restore trail gets its byte
        # count without a second round trip. The reader disables checksums for
        # its own ranged reads.
        return RemoteObject(cast(BinaryIO, blob.open("rb", chunk_size=self._CHUNK)), blob.size)

    def delete(self, backup_id: str) -> None:
        """Delete the object for ``backup_id``, swallowing any error (best-effort)."""
        try:
            self._blob(backup_id).delete()
        except Exception as e:  # best-effort cleanup
            logger.warning("Failed to delete object %s: %s", self._key(backup_id), e)
