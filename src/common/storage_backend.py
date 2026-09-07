#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Remote object-store backends for Valkey RDB backup/restore."""

from __future__ import annotations

import logging
import pathlib
from enum import Enum
from typing import IO, TYPE_CHECKING, BinaryIO, NamedTuple, Protocol, cast
from urllib.parse import urlparse

import boto3
from azure.core.exceptions import AzureError, ResourceExistsError
from azure.storage.blob import ContainerClient
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from common.exceptions import StorageBackendError
from core.models import AzureStorageParameters, BackupCredentials, S3Parameters
from literals import AZURE_HTTPS_PROTOCOLS

if TYPE_CHECKING:
    from azure.storage.blob import BlobClient
    from mypy_boto3_s3.literals import BucketLocationConstraintType
    from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource

logger = logging.getLogger(__name__)


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
        precondition, use it. ``BackupManager`` checks the id up front for the
        backends whose SDK cannot.
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
