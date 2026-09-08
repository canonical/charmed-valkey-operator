#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the storage backends behind BackupManager.

The only place boto3 is faked: everything above the StorageBackend Protocol is
tested against a fake backend in test_backup.py / test_restore.py. Exceptions are
imported flat (`common.exceptions`) because src/ imports are flat, so the class
the backend raises is the flat one, not the `src.`-prefixed copy.
"""

import io

import pytest
from botocore.exceptions import ClientError


def _s3_params(**overrides):
    """Build a valid S3Parameters, overriding individual fields by name.

    Flat import: src/ imports are flat, so `core.models.S3Parameters` is the class
    production builds and isinstance-checks against -- the `src.`-prefixed copy is
    a different class object and would miss every isinstance dispatch.
    """
    from core.models import S3Parameters

    base = {
        "bucket": "b",
        "endpoint": "https://e",
        "path": "valkey",
        "access-key": "AK",
        "secret-key": "SK",
    }
    base.update(overrides)
    return S3Parameters.model_validate(base)


def _backend(mocker, **overrides):
    """Build an S3Backend with its boto3 Bucket faked out; return (backend, bucket)."""
    from src.common.storage_backend import S3Backend

    bucket = mocker.MagicMock()
    mocker.patch.object(S3Backend, "_bucket", return_value=bucket)
    return S3Backend(_s3_params(**overrides), mocker.MagicMock()), bucket


# ── client construction ─────────────────────────────────────────────────


def test_s3backend_bucket_built_with_checksum_workaround(mocker, tmp_path):
    import boto3

    from src.common.storage_backend import S3Backend

    fake_session = mocker.MagicMock()
    fake_resource = mocker.MagicMock()
    fake_bucket = mocker.MagicMock()
    fake_resource.Bucket.return_value = fake_bucket
    fake_session.resource.return_value = fake_resource
    mocker.patch("boto3.Session", return_value=fake_session)

    params = _s3_params(endpoint="https://s3.example.com", region="us-west-2")
    bucket = S3Backend(params, tmp_path / "ca.pem")._bucket()

    _, session_kwargs = boto3.Session.call_args
    assert session_kwargs["aws_access_key_id"] == "AK"
    assert session_kwargs["aws_secret_access_key"] == "SK"
    assert session_kwargs["region_name"] == "us-west-2"
    args, kwargs = fake_session.resource.call_args
    assert args[0] == "s3"
    assert kwargs["endpoint_url"] == "https://s3.example.com"
    cfg = kwargs["config"]
    assert cfg.request_checksum_calculation == "when_required"
    assert cfg.response_checksum_validation == "when_required"
    assert kwargs["verify"] is True  # no tls-ca-chain provided
    fake_resource.Bucket.assert_called_once_with("b")
    assert bucket is fake_bucket


def test_s3backend_bucket_uses_ca_chain_when_provided(mocker, tmp_path):
    import boto3

    from src.common.storage_backend import S3Backend

    mocker.patch("boto3.Session")
    ca_path = tmp_path / "s3_ca_chain.pem"

    params = _s3_params(tls_ca_chain=["-----BEGIN CERTIFICATE-----\n..."])
    S3Backend(params, ca_path)._bucket()

    _, kwargs = boto3.Session.return_value.resource.call_args
    assert kwargs["verify"] == ca_path.as_posix()


def test_s3backend_location_names_the_destination_without_credentials(mocker):
    """The audit trail gets host/bucket/prefix -- never anything from the URL's userinfo."""
    backend, _ = _backend(mocker, endpoint="https://AKIA:SECRET@s3.example.com:9000")
    assert backend.location == "s3://s3.example.com:9000/b/valkey"
    assert "SECRET" not in backend.location

    plain, _ = _backend(mocker, endpoint="https://s3.example.com")
    assert plain.location == "s3://s3.example.com/b/valkey"


# ── bucket lifecycle ────────────────────────────────────────────────────


def test_s3backend_ensure_container_us_east_1_omits_location_constraint(mocker):
    backend, bucket = _backend(mocker, region="us-east-1")

    backend.ensure_container()

    bucket.create.assert_called_once_with()
    bucket.wait_until_exists.assert_called_once()


def test_s3backend_ensure_container_non_default_region_sets_location_constraint(mocker):
    backend, bucket = _backend(mocker, region="eu-west-1")

    backend.ensure_container()

    bucket.create.assert_called_once_with(
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )


def test_s3backend_ensure_container_tolerates_existing_buckets(mocker):
    backend, bucket = _backend(mocker, region="us-east-1")

    for token in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists", "BucketNameUnavailable"):
        bucket.reset_mock()
        bucket.create.side_effect = ClientError(
            {"Error": {"Code": token, "Message": token}}, "CreateBucket"
        )
        backend.ensure_container()  # must not raise


def test_s3backend_ensure_container_wraps_other_client_errors(mocker):
    from common.exceptions import StorageBackendError

    backend, bucket = _backend(mocker, region="us-east-1")
    bucket.create.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "CreateBucket"
    )

    with pytest.raises(StorageBackendError) as excinfo:
        backend.ensure_container()
    assert excinfo.value.safe_code == "AccessDenied"


# ── list ────────────────────────────────────────────────────────────────


def test_s3backend_list_object_ids_filters_by_prefix_and_strips_it(mocker):
    backend, bucket = _backend(mocker)
    bucket.objects.filter.return_value = [
        mocker.MagicMock(key=k)
        for k in ("valkey/2026-05-13T10:00:00Z", "valkey/.s3-lifecycle-marker")
    ]

    assert backend.list_object_ids() == ["2026-05-13T10:00:00Z", ".s3-lifecycle-marker"]
    bucket.objects.filter.assert_called_once_with(Prefix="valkey/")


def test_s3backend_list_object_ids_wraps_client_error(mocker):
    from common.exceptions import StorageBackendError

    backend, bucket = _backend(mocker)
    bucket.objects.filter.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "x"}}, "ListObjectsV2"
    )

    with pytest.raises(StorageBackendError) as excinfo:
        backend.list_object_ids()
    assert excinfo.value.safe_code == "NoSuchBucket"


# ── head / upload / download / delete ───────────────────────────────────


def test_s3backend_head_ranges_over_the_first_bytes_only(mocker):
    backend, bucket = _backend(mocker)
    bucket.Object.return_value.get.return_value = {"Body": mocker.Mock(read=lambda: b"REDIS0011")}

    assert backend.head("2026-05-13T10:00:00Z") == b"REDIS0011"
    bucket.Object.assert_called_once_with("valkey/2026-05-13T10:00:00Z")
    bucket.Object.return_value.get.assert_called_once_with(Range="bytes=0-15")


def test_s3backend_head_wraps_client_error(mocker):
    from common.exceptions import StorageBackendError

    backend, bucket = _backend(mocker)
    bucket.Object.return_value.get.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "x"}}, "GetObject"
    )

    with pytest.raises(StorageBackendError) as excinfo:
        backend.head("2026-05-13T10:00:00Z")
    assert excinfo.value.safe_code == "NoSuchKey"


def test_s3backend_upload_streams_to_the_backup_id_key_in_parts(mocker):
    backend, bucket = _backend(mocker)
    reader = mocker.MagicMock()

    backend.upload("2026-05-13T10:00:00Z", reader)

    args, kwargs = bucket.upload_fileobj.call_args
    assert args[0] is reader
    assert args[1] == "valkey/2026-05-13T10:00:00Z"
    # Multipart, so a long stream never has to be buffered whole.
    assert kwargs["Config"].multipart_chunksize == 8 * 1024 * 1024


def test_s3backend_upload_wraps_client_error(mocker):
    from common.exceptions import StorageBackendError

    backend, bucket = _backend(mocker)
    bucket.upload_fileobj.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "x"}}, "PutObject"
    )

    with pytest.raises(StorageBackendError) as excinfo:
        backend.upload("2026-05-13T10:00:00Z", mocker.MagicMock())
    assert excinfo.value.safe_code == "NoSuchBucket"


def test_s3backend_download_returns_the_body_and_its_length(mocker):
    """One GET yields both: the size rides along instead of costing a second round trip."""
    backend, bucket = _backend(mocker)
    body = mocker.Mock()
    bucket.Object.return_value.get.return_value = {"Body": body, "ContentLength": 4096}

    obj = backend.download("2026-05-13T10:00:00Z")

    assert obj.body is body
    assert obj.size == 4096
    bucket.Object.assert_called_once_with("valkey/2026-05-13T10:00:00Z")


def test_s3backend_download_tolerates_a_response_without_content_length(mocker):
    backend, bucket = _backend(mocker)
    bucket.Object.return_value.get.return_value = {"Body": mocker.Mock()}

    assert backend.download("2026-05-13T10:00:00Z").size is None


def test_s3backend_download_wraps_client_error(mocker):
    from common.exceptions import StorageBackendError

    backend, bucket = _backend(mocker)
    bucket.Object.return_value.get.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "x"}}, "GetObject"
    )

    with pytest.raises(StorageBackendError) as excinfo:
        backend.download("2026-05-13T10:00:00Z")
    assert excinfo.value.safe_code == "AccessDenied"


def test_s3backend_delete_removes_the_backup_id_key(mocker):
    backend, bucket = _backend(mocker)

    backend.delete("2026-05-13T10:00:00Z")

    bucket.Object.assert_called_once_with("valkey/2026-05-13T10:00:00Z")
    bucket.Object.return_value.delete.assert_called_once()


def test_s3backend_delete_swallows_errors(mocker):
    """Cleanup is best-effort: it must never mask the failure that triggered it."""
    backend, bucket = _backend(mocker)
    bucket.Object.return_value.delete.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "x"}}, "DeleteObject"
    )

    backend.delete("2026-05-13T10:00:00Z")  # must not raise


# ── dispatch ────────────────────────────────────────────────────────────


def test_build_backend_selects_by_credentials_type(mocker):
    """The one place a credentials type maps to a backend; BackupManager never sees it."""
    from common.storage_backend import S3Backend, build_backend

    assert isinstance(build_backend(_s3_params(), mocker.MagicMock()), S3Backend)


def test_build_backend_rejects_unknown_credentials(mocker):
    """An unregistered credentials type fails loudly rather than silently doing nothing."""
    from common.exceptions import StorageBackendError
    from common.storage_backend import build_backend

    with pytest.raises(StorageBackendError):
        build_backend(object(), mocker.MagicMock())  # pyright: ignore[reportArgumentType]


# ── Azure Blob backend ──────────────────────────────────────────────────


def _az_params(**overrides):
    """Build a valid AzureStorageParameters, overriding individual fields by name.

    Flat import, for the same reason as `_s3_params`: `build_backend` dispatches
    on isinstance, and the `src.`-prefixed copy is a different class object.
    """
    from core.models import AzureStorageParameters

    base = {
        "container": "c",
        "storage-account": "acct",
        "secret-key": "SK",
        "connection-protocol": "https",
        "path": "valkey",
    }
    base.update(overrides)
    return AzureStorageParameters.model_validate(base)


def _az_backend(mocker, **overrides):
    """Build an AzureBackend with its ContainerClient faked; return (backend, container)."""
    from src.common.storage_backend import AzureBackend

    container = mocker.MagicMock()
    mocker.patch.object(AzureBackend, "_container", return_value=container)
    return AzureBackend(_az_params(**overrides)), container


def _http_error(code, message="boom"):
    """Build an azure-storage failure carrying a structured code, as the SDK raises it.

    ``process_storage_error`` attaches ``error_code`` as a ``StorageErrorCode``
    -- a (str, Enum) member -- which is what the backend has to translate.
    """
    from azure.core.exceptions import HttpResponseError

    exc = HttpResponseError(message=message)
    exc.error_code = code
    return exc


# ── client construction ─────────────────────────────────────────────────


def test_azurebackend_account_url_defaults_to_the_blob_host(mocker):
    backend, _ = _az_backend(mocker)
    assert backend._account_url() == "https://acct.blob.core.windows.net"


def test_azurebackend_account_url_follows_the_connection_protocol(mocker):
    """wasb/http are plaintext Blob; wasbs/https are TLS."""
    for proto, scheme in (
        ("https", "https"),
        ("wasbs", "https"),
        ("http", "http"),
        ("wasb", "http"),
    ):
        backend, _ = _az_backend(mocker, **{"connection-protocol": proto})
        assert backend._account_url() == f"{scheme}://acct.blob.core.windows.net"


def test_azurebackend_account_url_honours_an_explicit_endpoint(mocker):
    """An emulator or private endpoint replaces the derived account URL wholesale."""
    backend, _ = _az_backend(mocker, endpoint="http://10.0.0.5:10000/devstoreaccount1")
    assert backend._account_url() == "http://10.0.0.5:10000/devstoreaccount1"


def test_azurebackend_location_names_the_destination_without_credentials(mocker):
    """The audit trail gets host/container/prefix -- never userinfo or a query string."""
    backend, _ = _az_backend(
        mocker, endpoint="https://acct.blob.core.windows.net:8443/?sig=SECRETTOKEN"
    )
    assert backend.location == "azure://acct.blob.core.windows.net:8443/c/valkey"
    assert "SECRETTOKEN" not in backend.location

    plain, _ = _az_backend(mocker)
    assert plain.location == "azure://acct.blob.core.windows.net/c/valkey"


# ── container lifecycle ─────────────────────────────────────────────────


def test_azurebackend_ensure_container_creates_it(mocker):
    backend, container = _az_backend(mocker)

    backend.ensure_container()

    container.create_container.assert_called_once_with()


def test_azurebackend_ensure_container_tolerates_an_existing_container(mocker):
    from azure.core.exceptions import ResourceExistsError

    backend, container = _az_backend(mocker)
    container.create_container.side_effect = ResourceExistsError("exists")

    backend.ensure_container()  # must not raise


def test_azurebackend_ensure_container_wraps_other_http_errors(mocker):
    from common.exceptions import StorageBackendError

    backend, container = _az_backend(mocker)
    container.create_container.side_effect = _http_error("AuthenticationFailed")

    with pytest.raises(StorageBackendError) as excinfo:
        backend.ensure_container()
    assert excinfo.value.safe_code == "AuthenticationFailed"


def test_azurebackend_safe_code_is_the_wire_code_not_the_enum_repr(mocker):
    """azure-storage sets error_code to a StorageErrorCode member.

    Its str() renders "StorageErrorCode.AUTHENTICATION_FAILED"; the action result
    must carry the wire code the provider returned, matching the S3 side.
    """
    from azure.storage.blob import StorageErrorCode

    from common.exceptions import StorageBackendError

    backend, container = _az_backend(mocker)
    container.create_container.side_effect = _http_error(StorageErrorCode.AUTHENTICATION_FAILED)

    with pytest.raises(StorageBackendError) as excinfo:
        backend.ensure_container()
    assert excinfo.value.safe_code == "AuthenticationFailed"


def test_azurebackend_safe_code_empty_when_the_sdk_attached_none(mocker):
    """A transport-level failure carries no provider code; the action falls back."""
    from azure.core.exceptions import HttpResponseError

    from common.exceptions import StorageBackendError

    backend, container = _az_backend(mocker)
    container.create_container.side_effect = HttpResponseError(message="connection reset")

    with pytest.raises(StorageBackendError) as excinfo:
        backend.ensure_container()
    assert excinfo.value.safe_code == ""


# ── list ────────────────────────────────────────────────────────────────


def test_azurebackend_list_object_ids_filters_by_prefix_and_strips_it(mocker):
    backend, container = _az_backend(mocker)
    container.list_blobs.return_value = [mocker.MagicMock(name=n) for n in ("a", "b")]
    # MagicMock(name=...) sets the mock's own name, not the attribute; set it explicitly.
    for blob, name in zip(
        container.list_blobs.return_value, ("valkey/2026-05-13T10:00:00Z", "valkey/other")
    ):
        blob.name = name

    assert backend.list_object_ids() == ["2026-05-13T10:00:00Z", "other"]
    container.list_blobs.assert_called_once_with(name_starts_with="valkey/")


def test_azurebackend_list_object_ids_wraps_http_errors(mocker):
    from common.exceptions import StorageBackendError

    backend, container = _az_backend(mocker)
    container.list_blobs.side_effect = _http_error("ContainerNotFound")

    with pytest.raises(StorageBackendError) as excinfo:
        backend.list_object_ids()
    assert excinfo.value.safe_code == "ContainerNotFound"


# ── head / upload / download / delete ───────────────────────────────────


def test_azurebackend_head_ranges_over_the_first_bytes_only(mocker):
    backend, container = _az_backend(mocker)
    blob = container.get_blob_client.return_value
    blob.download_blob.return_value.readall.return_value = b"REDIS0011"

    assert backend.head("2026-05-13T10:00:00Z") == b"REDIS0011"
    container.get_blob_client.assert_called_once_with("valkey/2026-05-13T10:00:00Z")
    blob.download_blob.assert_called_once_with(offset=0, length=16)


def test_azurebackend_head_wraps_http_errors(mocker):
    from common.exceptions import StorageBackendError

    backend, container = _az_backend(mocker)
    container.get_blob_client.return_value.download_blob.side_effect = _http_error("BlobNotFound")

    with pytest.raises(StorageBackendError) as excinfo:
        backend.head("2026-05-13T10:00:00Z")
    assert excinfo.value.safe_code == "BlobNotFound"


def test_azurebackend_upload_streams_to_the_backup_id_blob(mocker):
    """Single-shot, max_concurrency=1: the source is a non-rewindable pipe."""
    backend, container = _az_backend(mocker)
    blob = container.get_blob_client.return_value
    reader = mocker.MagicMock()

    backend.upload("2026-05-13T10:00:00Z", reader)

    container.get_blob_client.assert_called_once_with("valkey/2026-05-13T10:00:00Z")
    args, kwargs = blob.upload_blob.call_args
    assert args[0] is reader
    assert kwargs["blob_type"] == "BlockBlob"
    # A stored backup is never replaced; the SDK turns this into If-None-Match.
    assert kwargs["overwrite"] is False
    assert kwargs["length"] is None
    assert kwargs["max_concurrency"] == 1


def test_azurebackend_upload_refuses_to_replace_an_existing_blob(mocker):
    """The store itself rejects the write, and its code reaches the action result."""
    from azure.core.exceptions import ResourceExistsError

    from common.exceptions import StorageBackendError

    backend, container = _az_backend(mocker)
    exists = ResourceExistsError(message="already there")
    exists.error_code = "BlobAlreadyExists"
    container.get_blob_client.return_value.upload_blob.side_effect = exists

    with pytest.raises(StorageBackendError) as excinfo:
        backend.upload("2026-05-13T10:00:00Z", mocker.MagicMock())
    assert excinfo.value.safe_code == "BlobAlreadyExists"


def test_azurebackend_upload_wraps_http_errors(mocker):
    from common.exceptions import StorageBackendError

    backend, container = _az_backend(mocker)
    container.get_blob_client.return_value.upload_blob.side_effect = _http_error(
        "AccountIsDisabled"
    )

    with pytest.raises(StorageBackendError) as excinfo:
        backend.upload("2026-05-13T10:00:00Z", mocker.MagicMock())
    assert excinfo.value.safe_code == "AccountIsDisabled"


def test_azurebackend_download_returns_the_downloader_and_its_length(mocker):
    """The downloader already knows the blob size, so the restore trail is free."""
    backend, container = _az_backend(mocker)
    downloader = container.get_blob_client.return_value.download_blob.return_value
    downloader.size = 4096

    obj = backend.download("2026-05-13T10:00:00Z")

    assert obj.body is downloader
    assert obj.size == 4096
    container.get_blob_client.assert_called_once_with("valkey/2026-05-13T10:00:00Z")


def test_azurebackend_download_wraps_http_errors(mocker):
    from common.exceptions import StorageBackendError

    backend, container = _az_backend(mocker)
    container.get_blob_client.return_value.download_blob.side_effect = _http_error("BlobNotFound")

    with pytest.raises(StorageBackendError) as excinfo:
        backend.download("2026-05-13T10:00:00Z")
    assert excinfo.value.safe_code == "BlobNotFound"


def test_azurebackend_delete_removes_the_backup_id_blob(mocker):
    backend, container = _az_backend(mocker)

    backend.delete("2026-05-13T10:00:00Z")

    container.get_blob_client.assert_called_once_with("valkey/2026-05-13T10:00:00Z")
    container.get_blob_client.return_value.delete_blob.assert_called_once()


def test_azurebackend_delete_swallows_errors(mocker):
    """Cleanup is best-effort: it must never mask the failure that triggered it."""
    backend, container = _az_backend(mocker)
    container.get_blob_client.return_value.delete_blob.side_effect = _http_error("BlobNotFound")

    backend.delete("2026-05-13T10:00:00Z")  # must not raise


def test_build_backend_selects_the_azure_backend(mocker):
    from common.storage_backend import AzureBackend, build_backend

    assert isinstance(build_backend(_az_params(), mocker.MagicMock()), AzureBackend)


# ── transport-level failures ────────────────────────────────────────────


def _transport_calls(backend):
    """Every StorageBackend method that must translate an SDK failure, by name."""
    return {
        "ensure_container": backend.ensure_container,
        "list_object_ids": backend.list_object_ids,
        "head": lambda: backend.head("2026-05-13T10:00:00Z"),
        "upload": lambda: backend.upload("2026-05-13T10:00:00Z", io.BytesIO(b"x")),
        "download": lambda: backend.download("2026-05-13T10:00:00Z"),
    }


def test_azurebackend_wraps_transport_errors(mocker):
    """A failure the service never answered is still a StorageBackendError.

    ``ServiceRequestError`` (DNS, refused connection, TLS handshake) is an
    ``AzureError`` but *not* an ``HttpResponseError``, so catching only the
    latter lets a raw SDK exception past the Protocol's contract -- and past
    ``create_backup``'s handler, orphaning the ``valkey-cli --rdb`` producer.
    """
    from azure.core.exceptions import ServiceRequestError

    from common.exceptions import StorageBackendError

    for name in _transport_calls(_az_backend(mocker)[0]):
        backend, container = _az_backend(mocker)
        container.create_container.side_effect = ServiceRequestError("unreachable")
        container.list_blobs.side_effect = ServiceRequestError("unreachable")
        blob = container.get_blob_client.return_value
        blob.download_blob.side_effect = ServiceRequestError("unreachable")
        blob.upload_blob.side_effect = ServiceRequestError("unreachable")

        with pytest.raises(StorageBackendError) as excinfo:
            _transport_calls(backend)[name]()
        # No wire code exists when the service never replied.
        assert excinfo.value.safe_code == "", name


def test_s3backend_wraps_transport_errors(mocker):
    """The same gap on the S3 side: BotoCoreError is disjoint from ClientError."""
    from botocore.exceptions import EndpointConnectionError

    from common.exceptions import StorageBackendError

    for name in _transport_calls(_backend(mocker)[0]):
        backend, bucket = _backend(mocker)
        err = EndpointConnectionError(endpoint_url="https://e")
        bucket.create.side_effect = err
        bucket.objects.filter.side_effect = err
        bucket.Object.return_value.get.side_effect = err
        bucket.upload_fileobj.side_effect = err

        with pytest.raises(StorageBackendError) as excinfo:
            _transport_calls(backend)[name]()
        assert excinfo.value.safe_code == "", name


# ── client construction (container-scoped) ──────────────────────────────


def test_azurebackend_container_client_is_built_directly(mocker):
    """One client, not a service client that hands out a container client.

    ``ContainerClient`` takes the account URL and container name itself, so the
    ``BlobServiceClient`` hop buys nothing.
    """
    from src.common import storage_backend
    from src.common.storage_backend import AzureBackend

    container_cls = mocker.patch.object(storage_backend, "ContainerClient")
    assert not hasattr(storage_backend, "BlobServiceClient")

    container = AzureBackend(_az_params(container="valkey-backups"))._container()

    _, kwargs = container_cls.call_args
    assert kwargs["account_url"] == "https://acct.blob.core.windows.net"
    assert kwargs["container_name"] == "valkey-backups"
    assert kwargs["credential"] == {"account_name": "acct", "account_key": "SK"}
    assert container is container_cls.return_value


def test_azurebackend_container_client_works_for_a_path_style_endpoint(mocker):
    """The emulator case: host is an IP, the account is the first path segment.

    A bare string credential makes azure-storage derive the account from the
    host's first label, which raises "Unable to determine account name for
    shared key credential" for any endpoint whose host is not
    ``<account>.blob.*``, so the account name is always passed explicitly.
    """
    from src.common import storage_backend
    from src.common.storage_backend import AzureBackend

    container_cls = mocker.patch.object(storage_backend, "ContainerClient")

    AzureBackend(
        _az_params(
            endpoint="http://10.0.0.5:10000/devstoreaccount1",
            **{"storage-account": "devstoreaccount1", "connection-protocol": "http"},
        )
    )._container()

    _, kwargs = container_cls.call_args
    assert kwargs["account_url"] == "http://10.0.0.5:10000/devstoreaccount1"
    assert kwargs["credential"]["account_name"] == "devstoreaccount1"


# ── GCS backend ─────────────────────────────────────────────────────────


def _gcs_key(**overrides) -> str:
    """Build a syntactically complete service-account key.

    The PEM body is a stub that the mocked SDK never parses.
    """
    import json

    info = {
        "type": "service_account",
        "project_id": "proj",
        "client_email": "backup@proj.iam.gserviceaccount.com",
        "private_key": "-----BEGIN PRIVATE KEY-----\nstub\n-----END PRIVATE KEY-----\n",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    info.update(overrides)
    return json.dumps(info)


def _gcs_params(**overrides):
    """Build a valid GCSParameters, overriding individual fields by name.

    Flat import, for the same reason as `_s3_params`: `build_backend` dispatches
    on isinstance, and the `src.`-prefixed copy is a different class object.
    """
    from core.models import GCSParameters

    base = {"bucket": "b", "path": "valkey", "secret-key": _gcs_key()}
    base.update(overrides)
    return GCSParameters.model_validate(base)


def _gcs_backend(mocker, **overrides):
    """Build a GCSBackend with its storage.Client faked; return (backend, client).

    The blob's writer is a context manager that re-raises (``__exit__`` -> False),
    as the real one does.
    """
    from src.common.storage_backend import GCSBackend

    client = mocker.MagicMock()
    bucket = mocker.MagicMock()
    client.bucket.return_value = bucket
    writer = mocker.MagicMock()
    writer.__enter__.return_value = writer
    writer.__exit__.return_value = False
    bucket.blob.return_value.open.return_value = writer
    mocker.patch.object(GCSBackend, "_client", return_value=client)
    return GCSBackend(_gcs_params(**overrides)), client


def _invalid_response(status: int):
    """Build an InvalidResponse as the resumable-media layer raises it on a bad status.

    That layer is what BlobWriter drives directly, unwrapped by the SDK's
    `_raise_from_invalid_response`, so the backend has to translate it.
    """
    import requests
    from google.cloud.storage.exceptions import InvalidResponse

    response = requests.Response()
    response.status_code = status
    return InvalidResponse(
        response, "Request failed with status code", status, "Expected one of", 200, 201
    )


GCS_CHUNK = 8 * 1024 * 1024


# ── client construction ─────────────────────────────────────────────────


def test_gcsbackend_client_is_built_from_the_service_account_key(mocker):
    """No env vars, no ADC: the key from the relation, and its project, explicitly."""
    from src.common import storage_backend
    from src.common.storage_backend import GCSBackend

    from_info = mocker.patch.object(storage_backend.storage.Client, "from_service_account_info")

    client = GCSBackend(_gcs_params())._client()

    args, kwargs = from_info.call_args
    assert args[0]["client_email"] == "backup@proj.iam.gserviceaccount.com"
    assert kwargs == {"project": "proj"}
    assert client is from_info.return_value


def test_gcsbackend_client_passes_project_none_when_the_key_has_none(mocker):
    from src.common import storage_backend
    from src.common.storage_backend import GCSBackend

    from_info = mocker.patch.object(storage_backend.storage.Client, "from_service_account_info")

    GCSBackend(_gcs_params(**{"secret-key": _gcs_key(project_id=None)}))._client()

    _, kwargs = from_info.call_args
    assert kwargs == {"project": None}


def test_gcsbackend_client_translates_an_unparsable_private_key(mocker):
    """Cryptography rejects a PEM body it cannot parse with a bare ValueError.

    That happens at client construction; it is the only ValueError the backend
    translates.
    """
    from common.exceptions import StorageBackendError
    from literals import GCS_INVALID_KEY_CODE
    from src.common import storage_backend
    from src.common.storage_backend import GCSBackend

    mocker.patch.object(
        storage_backend.storage.Client,
        "from_service_account_info",
        side_effect=ValueError("Could not deserialize key data."),
    )

    with pytest.raises(StorageBackendError) as excinfo:
        GCSBackend(_gcs_params())._client()
    assert excinfo.value.safe_code == GCS_INVALID_KEY_CODE
    assert "stub" not in str(excinfo.value)


def test_gcsbackend_location_names_the_destination_without_credentials(mocker):
    backend, _ = _gcs_backend(mocker, bucket="data-charms-testing", path="valkey/k8s")
    assert backend.location == "gs://data-charms-testing/valkey/k8s"
    assert "stub" not in backend.location


# ── ensure_container ────────────────────────────────────────────────────


def test_gcsbackend_ensure_container_creates_a_missing_bucket(mocker):
    backend, client = _gcs_backend(mocker, **{"storage-class": "nearline"})

    backend.ensure_container()

    bucket = client.bucket.return_value
    assert bucket.storage_class == "NEARLINE"
    client.create_bucket.assert_called_once_with(bucket, project="proj")
    client.list_blobs.assert_not_called()


def test_gcsbackend_ensure_container_tolerates_an_existing_bucket(mocker):
    from google.api_core.exceptions import Conflict

    backend, client = _gcs_backend(mocker)
    client.create_bucket.side_effect = Conflict("exists")
    client.list_blobs.return_value = iter([])

    backend.ensure_container()  # no raise

    client.list_blobs.assert_called_once_with("b", prefix="valkey/", max_results=1)


def test_gcsbackend_ensure_container_probes_the_prefix_when_create_is_forbidden(mocker):
    """A key without bucket permissions gets 403 from create even when the bucket exists."""
    from google.api_core.exceptions import Forbidden

    backend, client = _gcs_backend(mocker)
    client.create_bucket.side_effect = Forbidden("no buckets.create")
    client.list_blobs.return_value = iter([])

    backend.ensure_container()  # no raise

    client.list_blobs.assert_called_once_with("b", prefix="valkey/", max_results=1)


def test_gcsbackend_ensure_container_surfaces_a_failed_probe(mocker):
    from google.api_core.exceptions import Forbidden, NotFound

    from common.exceptions import StorageBackendError

    for probe_error, code in [
        (Forbidden("403 GET https://storage.googleapis.com/b"), "Forbidden"),
        (NotFound("404 GET https://storage.googleapis.com/b"), "NotFound"),
    ]:
        backend, client = _gcs_backend(mocker)
        client.create_bucket.side_effect = Forbidden("no buckets.create")
        client.list_blobs.side_effect = probe_error

        with pytest.raises(StorageBackendError) as excinfo:
            backend.ensure_container()
        assert excinfo.value.safe_code == code
        assert "storage.googleapis.com" not in excinfo.value.safe_code


def test_gcsbackend_ensure_container_wraps_other_errors(mocker):
    from google.api_core.exceptions import ServiceUnavailable

    from common.exceptions import StorageBackendError

    backend, client = _gcs_backend(mocker)
    client.create_bucket.side_effect = ServiceUnavailable(
        "503 POST https://storage.googleapis.com/b"
    )

    with pytest.raises(StorageBackendError) as excinfo:
        backend.ensure_container()
    assert excinfo.value.safe_code == "ServiceUnavailable"
    client.list_blobs.assert_not_called()


# ── list / head ─────────────────────────────────────────────────────────


def test_gcsbackend_list_object_ids_filters_by_prefix_and_strips_it(mocker):
    from types import SimpleNamespace

    backend, client = _gcs_backend(mocker)
    client.list_blobs.return_value = [
        SimpleNamespace(name="valkey/2026-05-13T10:00:00Z"),
        SimpleNamespace(name="valkey/2026-05-13T10:00:05Z"),
    ]

    ids = backend.list_object_ids()

    client.list_blobs.assert_called_once_with("b", prefix="valkey/")
    assert ids == ["2026-05-13T10:00:00Z", "2026-05-13T10:00:05Z"]


def test_gcsbackend_list_object_ids_wraps_errors(mocker):
    from google.api_core.exceptions import Forbidden

    from common.exceptions import StorageBackendError

    backend, client = _gcs_backend(mocker)
    client.list_blobs.side_effect = Forbidden("denied")

    with pytest.raises(StorageBackendError) as excinfo:
        backend.list_object_ids()
    assert excinfo.value.safe_code == "Forbidden"


def test_gcsbackend_head_ranges_over_the_first_bytes_only(mocker):
    backend, client = _gcs_backend(mocker)
    blob = client.bucket.return_value.blob.return_value
    blob.download_as_bytes.return_value = b"REDIS0012"

    assert backend.head("2026-05-13T10:00:00Z") == b"REDIS0012"

    client.bucket.return_value.blob.assert_called_once_with("valkey/2026-05-13T10:00:00Z")
    blob.download_as_bytes.assert_called_once_with(start=0, end=15)


def test_gcsbackend_head_wraps_errors(mocker):
    from google.api_core.exceptions import NotFound

    from common.exceptions import StorageBackendError

    backend, client = _gcs_backend(mocker)
    client.bucket.return_value.blob.return_value.download_as_bytes.side_effect = NotFound("x")

    with pytest.raises(StorageBackendError) as excinfo:
        backend.head("2026-05-13T10:00:00Z")
    assert excinfo.value.safe_code == "NotFound"


# ── upload ──────────────────────────────────────────────────────────────


def test_gcsbackend_upload_streams_through_a_blob_writer(mocker):
    """blob.open("wb") as a context manager, not upload_from_file.

    The resumable uploader calls tell() on its source, and the source is a
    non-rewindable pipe. No checksum kwarg: the SDK default (md5 on this
    build) is what GCS verifies.
    """
    backend, client = _gcs_backend(mocker)
    blob = client.bucket.return_value.blob.return_value
    writer = blob.open.return_value
    data = b"REDIS0012" + b"x" * (GCS_CHUNK + 1000)

    backend.upload("2026-05-13T10:00:00Z", io.BytesIO(data))

    client.bucket.return_value.blob.assert_called_once_with("valkey/2026-05-13T10:00:00Z")
    blob.open.assert_called_once_with(
        "wb", chunk_size=GCS_CHUNK, ignore_flush=True, if_generation_match=0
    )
    assert b"".join(call.args[0] for call in writer.write.call_args_list) == data
    writer.__exit__.assert_called_once_with(None, None, None)


def test_gcsbackend_upload_refuses_to_replace_an_existing_object(mocker):
    """if_generation_match=0 makes the session-initiate fail on a colliding id.

    The writer path raises the resumable-media InvalidResponse, not an
    api_core error, so it is mapped to the status's class: 412 ->
    PreconditionFailed. The block exits on the exception, which is what
    cancels the session.
    """
    from google.cloud.storage.exceptions import InvalidResponse

    from common.exceptions import StorageBackendError

    backend, client = _gcs_backend(mocker)
    writer = client.bucket.return_value.blob.return_value.open.return_value
    writer.write.side_effect = _invalid_response(412)

    with pytest.raises(StorageBackendError) as excinfo:
        backend.upload("2026-05-13T10:00:00Z", io.BytesIO(b"REDIS0012"))
    assert excinfo.value.safe_code == "PreconditionFailed"
    assert writer.__exit__.call_args.args[0] is InvalidResponse


def test_gcsbackend_upload_lets_a_reader_failure_propagate_through_the_block(mocker):
    """A producer that dies mid-stream is not an SDK error.

    It propagates untouched to the manager's catch-all, and the block exits
    on it (terminate), so no truncated object is committed.
    """
    backend, client = _gcs_backend(mocker)
    writer = client.bucket.return_value.blob.return_value.open.return_value
    reader = mocker.MagicMock()
    reader.read.side_effect = BrokenPipeError("valkey-cli died")

    with pytest.raises(BrokenPipeError):
        backend.upload("2026-05-13T10:00:00Z", reader)
    assert writer.__exit__.call_args.args[0] is BrokenPipeError


def _real_blob_writer(mocker, chunk_size):
    """Build the SDK's own BlobWriter over a fake blob; return (writer, upload, transport)."""
    from google.cloud.storage.fileio import BlobWriter

    blob = mocker.MagicMock()
    upload, transport = mocker.MagicMock(), mocker.MagicMock()
    blob._initiate_resumable_upload.return_value = (upload, transport)
    writer = BlobWriter(blob, chunk_size=chunk_size, ignore_flush=True, if_generation_match=0)
    return writer, upload, transport


def test_blobwriter_context_manager_cancels_the_session_on_error(mocker):
    """Pin the SDK semantics that upload() relies on.

    An exception inside the block cancels the session after the chunks
    already sent, and never transmits the buffered remainder. (An abandoned
    writer would: io.IOBase.__del__ calls close(), which commits whatever is
    buffered as the final chunk.)
    """
    chunk = 256 * 1024
    writer, upload, transport = _real_blob_writer(mocker, chunk)

    with pytest.raises(RuntimeError):
        with writer:
            writer.write(b"x" * (chunk + 1))  # one full chunk goes out, 1 byte stays
            raise RuntimeError("producer died")

    assert upload.transmit_next_chunk.call_count == 1
    transport.delete.assert_called_once_with(upload.upload_url)
    assert writer.closed


def test_blobwriter_context_manager_commits_on_success(mocker):
    """A clean exit sends the final (short) chunk.

    The precondition rode on the initiate call.
    """
    chunk = 256 * 1024
    writer, upload, transport = _real_blob_writer(mocker, chunk)

    with writer:
        writer.write(b"REDIS0012")

    assert upload.transmit_next_chunk.call_count == 1
    transport.delete.assert_not_called()
    _, kwargs = writer._blob._initiate_resumable_upload.call_args
    assert kwargs["if_generation_match"] == 0
    assert kwargs["chunk_size"] == chunk


def test_gcsbackend_upload_terminates_a_writer_that_failed_in_close(mocker):
    """For an RDB under one chunk, the session's only chunk is sent from close().

    That is, inside __exit__ on the success path. A failure there is raised
    by __exit__ itself, so the with statement does not terminate; the buffer
    would stay open and io.IOBase.__del__ would re-send the chunk at GC -- a
    commit after the action reported failure. upload() terminates the writer
    itself in that case (real BlobWriter, fake blob).
    """
    from common.exceptions import StorageBackendError

    chunk = 256 * 1024
    backend, client = _gcs_backend(mocker)
    backend._CHUNK = chunk
    writer, upload, transport = _real_blob_writer(mocker, chunk)
    client.bucket.return_value.blob.return_value.open.return_value = writer
    upload.transmit_next_chunk.side_effect = _invalid_response(503)

    with pytest.raises(StorageBackendError) as excinfo:
        backend.upload("2026-05-13T10:00:00Z", io.BytesIO(b"REDIS0012"))

    assert excinfo.value.safe_code == "ServiceUnavailable"
    assert writer.closed
    transport.delete.assert_called_once_with(upload.upload_url)
    # A later close() -- what io.IOBase.__del__ does at GC -- is now a no-op.
    writer.close()
    assert upload.transmit_next_chunk.call_count == 1


def test_gcsbackend_upload_keeps_the_original_error_when_terminate_fails(mocker):
    """Cancelling a failed upload is best-effort.

    The DELETE can fail for the same reason the upload did; the action must
    still report the upload's code, not the cancel's.
    """
    import requests

    from common.exceptions import StorageBackendError

    chunk = 256 * 1024
    backend, client = _gcs_backend(mocker)
    backend._CHUNK = chunk
    writer, upload, transport = _real_blob_writer(mocker, chunk)
    client.bucket.return_value.blob.return_value.open.return_value = writer
    upload.transmit_next_chunk.side_effect = _invalid_response(503)
    transport.delete.side_effect = requests.ConnectionError("unreachable")

    with pytest.raises(StorageBackendError) as excinfo:
        backend.upload("2026-05-13T10:00:00Z", io.BytesIO(b"REDIS0012"))

    assert excinfo.value.safe_code == "ServiceUnavailable"
    transport.delete.assert_called_once()


# ── download / delete ───────────────────────────────────────────────────


def test_gcsbackend_download_returns_the_reader_and_its_length(mocker):
    backend, client = _gcs_backend(mocker)
    bucket = client.bucket.return_value
    blob = bucket.get_blob.return_value
    blob.size = 4096
    reader = mocker.MagicMock()
    blob.open.return_value = reader

    obj = backend.download("2026-05-13T10:00:00Z")

    bucket.get_blob.assert_called_once_with("valkey/2026-05-13T10:00:00Z")
    blob.open.assert_called_once_with("rb", chunk_size=GCS_CHUNK)
    assert obj.body is reader
    assert obj.size == 4096


def test_gcsbackend_download_reports_a_missing_object_as_not_found(mocker):
    from common.exceptions import StorageBackendError

    backend, client = _gcs_backend(mocker)
    client.bucket.return_value.get_blob.return_value = None

    with pytest.raises(StorageBackendError) as excinfo:
        backend.download("2026-05-13T10:00:00Z")
    assert excinfo.value.safe_code == "NotFound"


def test_gcsbackend_download_wraps_errors(mocker):
    from google.api_core.exceptions import Forbidden

    from common.exceptions import StorageBackendError

    backend, client = _gcs_backend(mocker)
    client.bucket.return_value.get_blob.side_effect = Forbidden("denied")

    with pytest.raises(StorageBackendError) as excinfo:
        backend.download("2026-05-13T10:00:00Z")
    assert excinfo.value.safe_code == "Forbidden"


def test_gcsbackend_delete_removes_the_backup_id_object(mocker):
    backend, client = _gcs_backend(mocker)

    backend.delete("2026-05-13T10:00:00Z")

    client.bucket.return_value.blob.assert_called_once_with("valkey/2026-05-13T10:00:00Z")
    client.bucket.return_value.blob.return_value.delete.assert_called_once_with()


def test_gcsbackend_delete_swallows_errors(mocker):
    from google.api_core.exceptions import NotFound

    backend, client = _gcs_backend(mocker)
    client.bucket.return_value.blob.return_value.delete.side_effect = NotFound("gone")

    backend.delete("2026-05-13T10:00:00Z")  # no raise


# ── dispatch and error codes ────────────────────────────────────────────


def test_build_backend_selects_the_gcs_backend(mocker):
    from common.storage_backend import GCSBackend, build_backend

    assert isinstance(build_backend(_gcs_params(), mocker.MagicMock()), GCSBackend)


def test_gcsbackend_safe_code_is_the_class_name_never_the_message(mocker):
    """str(exc) is "<status> <verb> <url>: <message>" -- never for an action result."""
    from google.api_core.exceptions import Forbidden

    from src.common.storage_backend import GCSBackend

    exc = Forbidden("403 GET https://storage.googleapis.com/storage/v1/b/secret-bucket")
    assert GCSBackend._error_code(exc) == "Forbidden"


def test_gcsbackend_safe_code_normalises_retry_and_invalid_response(mocker):
    """RetryError hides its cause behind a generic name.

    InvalidResponse carries only an HTTP status. Both are mapped to the
    status's api_core class.
    """
    from google.api_core.exceptions import RetryError, ServiceUnavailable
    from google.cloud.storage.exceptions import DataCorruption

    from src.common.storage_backend import GCSBackend

    assert GCSBackend._error_code(RetryError("deadline", ServiceUnavailable("503"))) == (
        "ServiceUnavailable"
    )
    assert GCSBackend._error_code(_invalid_response(412)) == "PreconditionFailed"
    assert GCSBackend._error_code(_invalid_response(400)) == "BadRequest"
    assert GCSBackend._error_code(RetryError("deadline", _invalid_response(503))) == (
        "ServiceUnavailable"
    )
    corrupt = DataCorruption(_invalid_response(200).response, "checksum mismatch")
    assert GCSBackend._error_code(corrupt) == "DataCorruption"

    from google.cloud.storage.exceptions import InvalidResponse

    # A response object without an int status_code: no mapping, class name wins.
    assert GCSBackend._error_code(InvalidResponse(object(), "no status")) == "InvalidResponse"


def test_gcsbackend_wraps_every_sdk_root(mocker):
    """api_core, google-auth, resumable-media and requests are the roots the SDK can raise.

    That is, past its own retries; each must become a StorageBackendError
    with a structured code, or a raw exception escapes create_backup's
    handlers.
    """
    import requests
    from google.api_core.exceptions import RetryError, ServiceUnavailable
    from google.auth.exceptions import RefreshError
    from google.cloud.storage.exceptions import DataCorruption

    from common.exceptions import StorageBackendError

    cases = [
        (requests.ConnectionError("unreachable"), "ConnectionError"),
        (RefreshError("invalid_grant: Invalid JWT Signature."), "RefreshError"),
        (RetryError("deadline", ServiceUnavailable("503")), "ServiceUnavailable"),
        (_invalid_response(412), "PreconditionFailed"),
        (DataCorruption(_invalid_response(200).response, "checksum mismatch"), "DataCorruption"),
    ]
    for err, code in cases:
        for name in _transport_calls(_gcs_backend(mocker)[0]):
            backend, client = _gcs_backend(mocker)
            bucket = client.bucket.return_value
            client.create_bucket.side_effect = err
            client.list_blobs.side_effect = err
            bucket.get_blob.side_effect = err
            bucket.blob.return_value.download_as_bytes.side_effect = err
            bucket.blob.return_value.open.side_effect = err

            with pytest.raises(StorageBackendError) as excinfo:
                _transport_calls(backend)[name]()
            assert excinfo.value.safe_code == code, (name, err)
