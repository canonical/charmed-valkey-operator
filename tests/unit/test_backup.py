#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the S3 backup feature."""

import base64
import io
import json
import logging
import pathlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import PropertyMock

import pytest
import yaml
from botocore.exceptions import ClientError
from ops import testing
from pydantic import ValidationError

# Flat imports for everything the charm dispatches on by class: the charm runs the
# flat modules, and a `src.`-prefixed copy is a different class object.
from common.exceptions import StorageBackendError, ValkeyBackupError
from common.storage_backend import S3Backend
from core.base_workload import TLSPaths, WorkloadBase
from core.models import (
    AzureStorageParameters,
    GCSParameters,
    PeerAppModel,
    PeerUnitModel,
    S3Parameters,
    ValkeyCluster,
    ValkeyServer,
)
from src.charm import ValkeyCharm
from src.events.backup import _safe_error
from src.literals import (
    AZURE_HTTP_PROTOCOLS,
    AZURE_HTTPS_PROTOCOLS,
    AZURE_RELATION_NAME,
    BACKUP_CA_FILENAME,
    BACKUP_CREDENTIAL_FIELDS,
    BACKUP_EXISTS_CODE,
    DATA_STORAGE,
    GCS_RELATION_NAME,
    PEER_RELATION,
    S3_RELATION_NAME,
    STATUS_PEERS_RELATION,
)
from src.managers.backup import BackupManager
from src.statuses import BackupStatuses, CharmStatuses


def test_backup_statuses_present():
    assert BackupStatuses.BACKUP_IN_PROGRESS.value.status == "maintenance"
    assert BackupStatuses.BACKUP_CREDENTIALS_MISSING.value.status == "blocked"
    assert BackupStatuses.BACKUP_BACKENDS_CONFLICT.value.status == "blocked"
    assert BackupStatuses.BACKUP_FAILED.value.status == "blocked"


def test_peer_app_model_has_s3_credentials_field():
    fields = PeerAppModel.model_fields
    assert "s3_credentials" in fields
    assert fields["s3_credentials"].default is None


def test_cluster_s3_credentials_parses_envelope_and_defaults_none(mocker):
    """The stored envelope parses back to S3Parameters; unset reads as None."""
    cluster = ValkeyCluster.__new__(ValkeyCluster)

    cluster.model = mocker.MagicMock()
    cluster.model.s3_credentials = json.dumps(
        {
            "bucket": "b",
            "endpoint": "https://e",
            "path": "p",
            "access-key": "AK",
            "secret-key": "SK",
            "tls-ca-chain": ["c1", "c2"],
        }
    )
    params = cluster.s3_credentials
    assert isinstance(params, S3Parameters)
    assert params.bucket == "b"
    assert params.tls_ca_chain == ["c1", "c2"]

    cluster.model.s3_credentials = None
    assert cluster.s3_credentials is None

    cluster.model = None
    assert cluster.s3_credentials is None


def test_peer_unit_model_has_backup_id_field():
    assert "backup_id" in PeerUnitModel.model_fields
    assert PeerUnitModel.model_fields["backup_id"].default == ""


def test_valkey_server_is_backup_in_progress_reflects_model_field():
    server = ValkeyServer.__new__(ValkeyServer)
    server.model = PeerUnitModel(backup_id="2026-05-13T10:00:00Z")
    assert server.is_backup_in_progress is True

    server.model = PeerUnitModel(backup_id="")
    assert server.is_backup_in_progress is False

    server.model = None
    assert server.is_backup_in_progress is False


def test_cluster_state_exposes_s3_relation():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    s3_rel = testing.Relation(
        id=3,
        endpoint=S3_RELATION_NAME,
        interface="s3",
        remote_app_name="s3-integrator",
    )
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd"),
        leader=True,
        relations={peer, status_peer, s3_rel},
        containers={testing.Container(name="valkey", can_connect=True)},
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        assert manager.charm.state.s3_relation is not None
        assert manager.charm.state.s3_relation.name == S3_RELATION_NAME


def test_active_backup_credentials_follows_the_relation(mocker):
    """Credentials are only "active" while the backend they belong to is related.

    The leader clears the stored envelope on relation-broken; until it does, the
    stored value must not keep a peer talking to a backend nobody is related to.
    ``s3_credentials`` is an ``ExtraSecretStr`` routed through a Juju secret, so
    it's patched at the property rather than forged into the databag.
    """
    stored = _s3_params()
    mocker.patch(
        "core.models.ValkeyCluster.s3_credentials",
        new_callable=PropertyMock,
        return_value=stored,
    )

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    s3_rel = testing.Relation(
        id=3,
        endpoint=S3_RELATION_NAME,
        interface="s3",
        remote_app_name="s3-integrator",
    )
    common = {
        "model": testing.Model(name="m", type="lxd"),
        "leader": True,
        "containers": {testing.Container(name="valkey", can_connect=True)},
    }

    related = testing.State(relations={peer, status_peer, s3_rel}, **common)
    with ctx(ctx.on.update_status(), related) as manager:
        assert manager.charm.state.active_backup_credentials is stored

    # Same stored envelope, relation gone -> nothing active.
    unrelated = testing.State(relations={peer, status_peer}, **common)
    with ctx(ctx.on.update_status(), unrelated) as manager:
        assert manager.charm.state.active_backup_credentials is None


def test_backup_credential_registry_maps_relations_to_databag_fields():
    """Adding a backend is one registry entry: its relation and where creds land."""
    assert BACKUP_CREDENTIAL_FIELDS[S3_RELATION_NAME] == "s3_credentials"
    assert BACKUP_CREDENTIAL_FIELDS[AZURE_RELATION_NAME] == "azure_credentials"
    assert BACKUP_CREDENTIAL_FIELDS[GCS_RELATION_NAME] == "gcs_credentials"
    # Every registered field must exist on the app databag model, or the leader
    # would silently write credentials nothing reads back.
    for field in BACKUP_CREDENTIAL_FIELDS.values():
        assert field in PeerAppModel.model_fields


def test_backup_relations_and_conflict_follow_the_registry():
    """Relation discovery and the conflict check are driven by the registry alone.

    Exercised with the three registered backends, so it also pins the mutual
    exclusion the charm enforces: relate exactly one storage integrator.
    """
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    s3_rel = testing.Relation(id=3, endpoint=S3_RELATION_NAME, interface="s3")
    azure_rel = testing.Relation(id=4, endpoint=AZURE_RELATION_NAME, interface="azure_storage")
    gcs_rel = testing.Relation(id=5, endpoint=GCS_RELATION_NAME, interface="gcs")
    common = {
        "model": testing.Model(name="m", type="lxd"),
        "leader": True,
        "containers": {testing.Container(name="valkey", can_connect=True)},
    }

    one = testing.State(relations={peer, status_peer, s3_rel}, **common)
    with ctx(ctx.on.update_status(), one) as manager:
        assert len(manager.charm.state.backup_relations) == 1
        assert manager.charm.state.backup_backends_conflict is False

    other = testing.State(relations={peer, status_peer, azure_rel}, **common)
    with ctx(ctx.on.update_status(), other) as manager:
        assert len(manager.charm.state.backup_relations) == 1
        assert manager.charm.state.backup_backends_conflict is False

    both = testing.State(relations={peer, status_peer, s3_rel, azure_rel}, **common)
    with ctx(ctx.on.update_status(), both) as manager:
        assert len(manager.charm.state.backup_relations) == 2
        assert manager.charm.state.backup_backends_conflict is True
        # Nothing can pick a backend, so no credentials are active.
        assert manager.charm.state.active_backup_credentials is None

    third = testing.State(relations={peer, status_peer, gcs_rel}, **common)
    with ctx(ctx.on.update_status(), third) as manager:
        assert len(manager.charm.state.backup_relations) == 1
        assert manager.charm.state.backup_backends_conflict is False

    all_three = testing.State(relations={peer, status_peer, s3_rel, azure_rel, gcs_rel}, **common)
    with ctx(ctx.on.update_status(), all_three) as manager:
        assert len(manager.charm.state.backup_relations) == 3
        assert manager.charm.state.backup_backends_conflict is True
        assert manager.charm.state.active_backup_credentials is None


def test_backup_ca_path_is_charm_local_not_workload_tls_dir(mocker, tmp_path):
    """The S3 CA path is charm-process-local, never a workload TLS path.

    boto3 runs in the charm process, so the bundle must sit under the charm
    dir (``charm.charm_dir``), never in the workload-container ``tls_paths``
    and never on the workload object at all.
    """
    # backup CA must be neither a workload (container) TLS path nor any
    # attribute of the workload -- it belongs to the charm-process side.
    assert not hasattr(TLSPaths, "backup_ca")
    assert not hasattr(WorkloadBase, "backup_ca_path")

    state = mocker.MagicMock()
    state.charm.charm_dir = tmp_path
    mgr = BackupManager(state=state, workload=mocker.MagicMock())
    assert mgr._backup_ca_path == tmp_path / BACKUP_CA_FILENAME


def test_backup_manager_store_tls_ca_chain_writes_charm_local_file(mocker, tmp_path):
    state = mocker.MagicMock()
    state.charm.charm_dir = tmp_path
    workload = mocker.MagicMock()
    mgr = BackupManager(state=state, workload=workload)

    certs = [
        "-----BEGIN CERTIFICATE-----\nMIICert1\n-----END CERTIFICATE-----",
        "-----BEGIN CERTIFICATE-----\nMIICert2\n-----END CERTIFICATE-----",
    ]
    mgr.store_tls_ca_chain({"tls-ca-chain": certs})
    assert (tmp_path / BACKUP_CA_FILENAME).read_text() == "\n".join(certs)

    mgr.remove_tls_ca_chain()
    assert not (tmp_path / BACKUP_CA_FILENAME).exists()


def test_backup_manager_store_tls_ca_chain_noop_without_chain(mocker, tmp_path):
    state = mocker.MagicMock()
    state.charm.charm_dir = tmp_path
    mgr = BackupManager(state=state, workload=mocker.MagicMock())
    mgr.store_tls_ca_chain({"bucket": "b"})
    assert not (tmp_path / BACKUP_CA_FILENAME).exists()


def test_backup_manager_store_tls_ca_chain_rejects_non_list(mocker, tmp_path):
    state = mocker.MagicMock()
    state.charm.charm_dir = tmp_path
    mgr = BackupManager(state=state, workload=mocker.MagicMock())
    # A bare string must not be char-joined into a corrupt bundle.
    mgr.store_tls_ca_chain({"tls-ca-chain": "-----BEGIN CERTIFICATE-----"})
    assert not (tmp_path / BACKUP_CA_FILENAME).exists()


def test_backup_manager_store_tls_ca_chain_rejects_non_pem_items(mocker, tmp_path):
    state = mocker.MagicMock()
    state.charm.charm_dir = tmp_path
    mgr = BackupManager(state=state, workload=mocker.MagicMock())
    # A list whose items lack a PEM armour header is malformed; the whole
    # chain is rejected rather than written as an unloadable CA bundle.
    mgr.store_tls_ca_chain({"tls-ca-chain": ["not-a-cert", "also-not-a-cert"]})
    assert not (tmp_path / BACKUP_CA_FILENAME).exists()


def test_ensure_container_delegates_to_the_backend(mocker):
    """The manager just asks the backend; the create semantics are the backend's."""
    backend = _fake_built_backend(mocker)

    BackupManager(state=mocker.MagicMock(), workload=mocker.MagicMock()).ensure_container(
        _s3_params()
    )
    backend.ensure_container.assert_called_once_with()


def test_ensure_container_wraps_backend_error_and_keeps_the_code(mocker):
    """Bucket setup failures reach the credentials handler as a backup error."""
    backend = _fake_built_backend(mocker)
    backend.ensure_container.side_effect = StorageBackendError("x", safe_code="AccessDenied")

    with pytest.raises(ValkeyBackupError) as excinfo:
        BackupManager(state=mocker.MagicMock(), workload=mocker.MagicMock()).ensure_container(
            _s3_params()
        )
    assert excinfo.value.safe_code == "AccessDenied"


def test_list_backups_keeps_only_backup_ids_newest_first(mocker):
    """The manager filters the backend's object ids and orders them, newest first."""
    state = mocker.MagicMock()
    state.active_backup_credentials = _s3_params(path="valkey")
    backend = _fake_backend(mocker)
    backend.list_object_ids.return_value = [
        "2026-05-13T10:00:00Z",
        "2026-05-12T10:00:00Z",
        "2026-05-14T10:00:00Z",
        # Non-backup objects under the prefix must be excluded.
        ".s3-lifecycle-marker",
        "subdir/something",
    ]

    result = BackupManager(state=state, workload=mocker.MagicMock()).list_backups()
    assert result == [
        "2026-05-14T10:00:00Z",
        "2026-05-13T10:00:00Z",
        "2026-05-12T10:00:00Z",
    ]


def test_list_backups_wraps_backend_error_and_keeps_the_code(mocker):
    """A backend failure surfaces as ValkeyBackupError, code intact for the action."""
    state = mocker.MagicMock()
    state.active_backup_credentials = _s3_params(path="p")
    backend = _fake_backend(mocker)
    backend.list_object_ids.side_effect = StorageBackendError("x", safe_code="NoSuchBucket")

    with pytest.raises(ValkeyBackupError) as excinfo:
        BackupManager(state=state, workload=mocker.MagicMock()).list_backups()
    assert excinfo.value.safe_code == "NoSuchBucket"


def test_list_backups_without_credentials_raises(mocker):
    """No related backend (or nothing stored yet) is an error, not an empty list."""
    state = mocker.MagicMock()
    state.active_backup_credentials = None

    with pytest.raises(ValkeyBackupError):
        BackupManager(state=state, workload=mocker.MagicMock()).list_backups()


def test_format_backup_list_renders_table():
    formatted = BackupManager.format_backup_list(["2026-05-13T10:00:00Z"])
    assert "backup-id" in formatted
    assert "backup-status" in formatted
    assert "2026-05-13T10:00:00Z" in formatted
    assert "finished" in formatted


def test_format_backup_list_empty():
    assert BackupManager.format_backup_list([]) == "No backups found."


def _s3_params(**overrides):
    """Build a valid S3Parameters, overriding individual fields by name."""
    base = {
        "bucket": "b",
        "endpoint": "https://e",
        "path": "valkey",
        "access-key": "AK",
        "secret-key": "SK",
    }
    base.update(overrides)
    return S3Parameters.model_validate(base)


def _fake_backend(mocker):
    """Patch BackupManager's cached backend onto a fake StorageBackend.

    Manager tests assert what the manager asks of the backend; the SDK wiring
    behind the Protocol is covered in test_storage_backend.py.
    """
    backend = mocker.MagicMock()
    mocker.patch.object(BackupManager, "storage_backend", backend)
    return backend


def _fake_built_backend(mocker):
    """Patch the registry itself, for the paths that build a backend from params.

    ensure_container runs before the credentials reach the databag, so it calls
    build_backend directly instead of going through the cached property.

    Patched on `src.managers.backup`: that is the module these tests import
    BackupManager from, so it is the namespace the call resolves against.
    """
    backend = mocker.MagicMock()
    mocker.patch("src.managers.backup.build_backend", return_value=backend)
    return backend


def _make_state(mocker, *, backup_id="", admin_pw="pw", tls=False):
    state = mocker.MagicMock()
    state.active_backup_credentials = _s3_params()
    state.unit_server.model.backup_id = backup_id
    state.unit_server.valkey_admin_password = admin_pw
    state.unit_server.is_tls_enabled = tls
    state.endpoint = "127.0.0.1"
    return state


def _drain(reader) -> None:
    """Mimic a backend upload draining the stream to completion."""
    while reader.read(8192):
        pass


def test_create_backup_success_sets_lock_streams_and_clears(mocker):

    state = _make_state(mocker)
    workload = mocker.MagicMock()
    workload.cli = "valkey-cli"
    proc = mocker.MagicMock()
    proc.stdout = io.BytesIO(b"VALKEY0011" + b"\x00" * 200)
    proc.wait.return_value = (0, "")
    workload.exec_stream.return_value = proc

    backend = _fake_backend(mocker)
    backend.upload.side_effect = lambda backup_id, reader: _drain(reader)
    fixed_now = mocker.patch("src.managers.backup.datetime")
    fixed_now.now.return_value.strftime.return_value = "2026-05-13T10:00:00Z"

    mgr = BackupManager(state=state, workload=workload)
    backup_id = mgr.create_backup()

    assert backup_id == "2026-05-13T10:00:00Z"
    update_calls = state.unit_server.update.call_args_list
    assert update_calls[0].args[0] == {"backup_id": "2026-05-13T10:00:00Z"}
    assert update_calls[-1].args[0] == {"backup_id": ""}
    backend.upload.assert_called_once()
    assert backend.upload.call_args.args[0] == "2026-05-13T10:00:00Z"
    proc.wait.assert_called_once()
    # A valid RDB was streamed, so no cleanup delete happened.
    backend.delete.assert_not_called()


def test_create_backup_rejects_empty_or_non_rdb_stream(mocker):

    fixed_now = mocker.patch("src.managers.backup.datetime")
    fixed_now.now.return_value.strftime.return_value = "2026-05-13T10:00:00Z"

    for payload in (b"", b"-ERR auth failed\r\n"):
        state = _make_state(mocker)
        workload = mocker.MagicMock()
        workload.cli = "valkey-cli"
        proc = mocker.MagicMock()
        proc.stdout = io.BytesIO(payload)
        proc.wait.return_value = (0, "")
        workload.exec_stream.return_value = proc

        backend = _fake_backend(mocker)
        backend.upload.side_effect = lambda backup_id, reader: _drain(reader)

        with pytest.raises(ValkeyBackupError):
            BackupManager(state=state, workload=workload).create_backup()

        # The bogus object is deleted and the lock is released.
        backend.delete.assert_called_once_with("2026-05-13T10:00:00Z")
        assert state.unit_server.update.call_args_list[-1].args[0] == {"backup_id": ""}


def test_create_backup_deletes_object_and_raises_when_cli_fails(mocker):

    state = _make_state(mocker)
    workload = mocker.MagicMock()
    workload.cli = "valkey-cli"
    proc = mocker.MagicMock()
    proc.wait.return_value = (1, "WRONGPASS")
    workload.exec_stream.return_value = proc

    backend = _fake_backend(mocker)
    fixed_now = mocker.patch("src.managers.backup.datetime")
    fixed_now.now.return_value.strftime.return_value = "2026-05-13T10:00:00Z"

    with pytest.raises(ValkeyBackupError):
        BackupManager(state=state, workload=workload).create_backup()

    backend.delete.assert_called_once_with("2026-05-13T10:00:00Z")
    last_update = state.unit_server.update.call_args_list[-1]
    assert last_update.args[0] == {"backup_id": ""}


def test_create_backup_refuses_to_overwrite_an_existing_backup(mocker):
    """An object already under this id is never replaced -- on any backend."""
    state = _make_state(mocker)
    workload = mocker.MagicMock()
    backend = _fake_backend(mocker)
    backend.list_object_ids.return_value = ["2026-05-13T10:00:00Z"]
    fixed_now = mocker.patch("src.managers.backup.datetime")
    fixed_now.now.return_value.strftime.return_value = "2026-05-13T10:00:00Z"

    with pytest.raises(ValkeyBackupError) as excinfo:
        BackupManager(state=state, workload=workload).create_backup()

    assert excinfo.value.safe_code == BACKUP_EXISTS_CODE
    # Nothing ran: no producer, no upload, and the stored backup is untouched.
    workload.exec_stream.assert_not_called()
    backend.upload.assert_not_called()
    backend.delete.assert_not_called()
    state.unit_server.update.assert_not_called()


def test_create_backup_wraps_a_failing_existence_probe(mocker):
    """A backend error on the pre-flight listing fails the action, safe code intact."""
    state = _make_state(mocker)
    workload = mocker.MagicMock()
    backend = _fake_backend(mocker)
    backend.list_object_ids.side_effect = StorageBackendError("nope", safe_code="AccessDenied")

    with pytest.raises(ValkeyBackupError) as excinfo:
        BackupManager(state=state, workload=workload).create_backup()

    assert excinfo.value.safe_code == "AccessDenied"
    workload.exec_stream.assert_not_called()
    state.unit_server.update.assert_not_called()


def test_create_backup_refuses_to_run_without_credentials(mocker):
    """No related backup backend: fail before touching valkey or the databag lock."""
    state = mocker.MagicMock()
    state.active_backup_credentials = None
    workload = mocker.MagicMock()

    with pytest.raises(ValkeyBackupError):
        BackupManager(state=state, workload=workload).create_backup()
    workload.exec_stream.assert_not_called()
    state.unit_server.update.assert_not_called()


def test_get_statuses_idle(mocker):
    state = mocker.MagicMock()
    state.statuses.get.return_value.root = []
    state.unit_server.is_backup_in_progress = False
    state.s3_relation = None
    state.cluster.s3_credentials = None

    statuses = BackupManager(state=state, workload=mocker.MagicMock()).get_statuses(scope="unit")
    assert statuses == [CharmStatuses.ACTIVE_IDLE.value]


def test_get_statuses_backup_in_progress_unit_scope(mocker):
    state = mocker.MagicMock()
    state.statuses.get.return_value.root = []
    state.unit_server.is_backup_in_progress = True
    state.s3_relation = None
    state.cluster.s3_credentials = None

    statuses = BackupManager(state=state, workload=mocker.MagicMock()).get_statuses(scope="unit")
    assert BackupStatuses.BACKUP_IN_PROGRESS.value in statuses


def test_safe_error_surfaces_s3_code_only(mocker):
    """Only the structured, backend-neutral error code reaches the action result.

    Raised through the whole stack -- SDK error, backend, manager, action -- not
    hand-wired: every layer in between rewraps the error, and a test that builds
    the chain itself would keep passing after one of them stopped forwarding the
    code.
    """
    state = mocker.MagicMock()
    state.active_backup_credentials = _s3_params(path="p")
    fake_bucket = mocker.MagicMock()
    fake_bucket.objects.filter.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDenied",
                "Message": "leak https://s3.internal RequestId=ABC123",
            }
        },
        "ListObjectsV2",
    )
    mocker.patch.object(S3Backend, "_bucket", return_value=fake_bucket)

    with pytest.raises(ValkeyBackupError) as excinfo:
        BackupManager(state=state, workload=mocker.MagicMock()).list_backups()

    msg = _safe_error(excinfo.value)
    assert msg == "Object storage request failed: AccessDenied"
    assert "s3.internal" not in msg
    assert "RequestId" not in msg


def test_safe_error_generic_for_non_client_errors():
    """Errors that are not S3 ClientErrors collapse to a generic message."""
    wrapped = ValkeyBackupError("valkey-cli --rdb exited 1: connection refused 10.1.2.3:6379")
    msg = _safe_error(wrapped)
    assert "10.1.2.3" not in msg
    assert "debug-log" in msg


def test_storage_detaching_refuses_during_backup():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"backup_id": "2026-05-13T10:00:00Z"},
    )
    status_peer = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    storage = testing.Storage(name=DATA_STORAGE)
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd"),
        leader=True,
        relations={peer, status_peer},
        storages={storage},
        containers={testing.Container(name="valkey", can_connect=True)},
    )

    # Must raise so the hook errors and Juju retries teardown until the
    # backup finishes -- a plain return would let scale-down proceed.
    with pytest.raises(testing.errors.UncaughtCharmError) as exc_info:
        ctx.run(ctx.on.storage_detaching(storage), state_in)
    assert "ValkeyBackupInProgressError" in str(exc_info.value)


def test_charm_constructs_backup_manager_and_events():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd"),
        leader=True,
        relations={peer, status_peer},
        containers={testing.Container(name="valkey", can_connect=True)},
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        assert manager.charm.backup_manager.__class__.__name__ == "BackupManager"
        assert manager.charm.backup_events is not None


def test_get_statuses_credentials_missing(mocker):
    state = mocker.MagicMock()
    state.statuses.get.return_value.root = []
    state.unit_server.is_backup_in_progress = False
    state.unit_server.is_started = True
    state.backup_backends_conflict = False
    state.backup_relations = [mocker.MagicMock()]
    state.active_backup_credentials = None

    statuses = BackupManager(state=state, workload=mocker.MagicMock()).get_statuses(scope="app")
    assert BackupStatuses.BACKUP_CREDENTIALS_MISSING.value in statuses


def test_get_statuses_credentials_missing_hidden_before_started(mocker):
    """The missing-parameters status stays hidden until the unit is started.

    A relation present from deploy time (e.g. Terraform) but with credentials not
    yet applied must not surface the status while the unit is still starting up.
    """
    state = mocker.MagicMock()
    state.statuses.get.return_value.root = []
    state.unit_server.is_backup_in_progress = False
    state.unit_server.is_started = False
    state.backup_backends_conflict = False
    state.backup_relations = [mocker.MagicMock()]
    state.active_backup_credentials = None

    statuses = BackupManager(state=state, workload=mocker.MagicMock()).get_statuses(scope="app")
    assert BackupStatuses.BACKUP_CREDENTIALS_MISSING.value not in statuses


def test_get_statuses_flags_a_backend_conflict(mocker):
    """Two related backup integrators is a config error the app must surface.

    Nothing can pick a backend, so this beats the credentials-missing status.
    """
    state = mocker.MagicMock()
    state.statuses.get.return_value.root = []
    state.unit_server.is_backup_in_progress = False
    state.unit_server.is_started = True
    state.backup_backends_conflict = True
    state.active_backup_credentials = None

    statuses = BackupManager(state=state, workload=mocker.MagicMock()).get_statuses(scope="app")
    assert BackupStatuses.BACKUP_BACKENDS_CONFLICT.value in statuses
    assert BackupStatuses.BACKUP_CREDENTIALS_MISSING.value not in statuses


def test_create_backup_kills_producer_on_upload_failure(mocker):
    """A mid-stream upload failure stops valkey-cli; the SDK aborts its transfer.

    No explicit object delete is issued -- a failed multipart/PutObject leaves
    no complete object to clean up, and the SDK aborts the upload itself.
    """
    state = _make_state(mocker)
    workload = mocker.MagicMock()
    workload.cli = "valkey-cli"
    proc = mocker.MagicMock()
    workload.exec_stream.return_value = proc

    backend = _fake_backend(mocker)
    backend.upload.side_effect = StorageBackendError("x", safe_code="NoSuchBucket")
    fixed_now = mocker.patch("src.managers.backup.datetime")
    fixed_now.now.return_value.strftime.return_value = "2026-05-13T10:00:00Z"

    with pytest.raises(ValkeyBackupError) as excinfo:
        BackupManager(state=state, workload=workload).create_backup()

    assert excinfo.value.safe_code == "NoSuchBucket"
    proc.kill.assert_called_once()
    backend.delete.assert_not_called()
    # The lock is still released on the way out.
    assert state.unit_server.update.call_args_list[-1].args[0] == {"backup_id": ""}


# ── Azure Blob backend ──────────────────────────────────────────────────


def test_metadata_declares_azure_relation():
    """The Azure integrator relation is declared and mutually exclusive-friendly."""
    meta = yaml.safe_load(pathlib.Path("metadata.yaml").read_text())
    az = meta["requires"]["azure-credentials"]
    assert az["interface"] == "azure_storage"
    assert az["limit"] == 1
    assert az["optional"] is True


def test_azure_parameters_valid_and_normalised():
    """Trims whitespace, strips the separators that would corrupt blob keys."""
    p = AzureStorageParameters.model_validate(
        {
            "container": " c ",
            "storage-account": "acct",
            "secret-key": " KEY ",
            "connection-protocol": "https",
            "endpoint": "https://acct.blob.core.windows.net/",
            "path": "/valkey/",
        }
    )
    assert p.container == "c"
    assert p.storage_account == "acct"
    assert p.secret_key == "KEY"
    assert p.path == "valkey"
    assert p.endpoint == "https://acct.blob.core.windows.net"
    assert p.resource_group is None


def test_azure_parameters_lowercases_the_connection_protocol():
    """The protocol is matched against the scheme sets, which are lowercase.

    An integrator sending "HTTPS" must not fall through to the http branch of
    the account URL.
    """
    p = AzureStorageParameters.model_validate(
        {
            "container": "c",
            "storage-account": "a",
            "secret-key": "k",
            "connection-protocol": "HTTPS",
            "path": "valkey",
        }
    )
    assert p.connection_protocol == "https"


def test_azure_parameters_rejects_empty_required():
    """An empty path would make list_backups enumerate the whole container."""
    base = {
        "container": "c",
        "storage-account": "a",
        "secret-key": "k",
        "connection-protocol": "https",
        "path": "valkey",
    }
    for field in ("container", "storage-account", "secret-key", "connection-protocol", "path"):
        with pytest.raises(ValidationError):
            AzureStorageParameters.model_validate({**base, field: ""})


def test_azure_parameters_rejects_adls_protocols():
    """abfs/abfss are ADLS-Gen2, served by the datalake SDK -- not BlobServiceClient."""
    for proto in ("abfs", "abfss", "ABFSS"):
        with pytest.raises(ValidationError):
            AzureStorageParameters.model_validate(
                {
                    "container": "c",
                    "storage-account": "a",
                    "secret-key": "k",
                    "connection-protocol": proto,
                    "path": "valkey",
                }
            )


def test_peer_app_model_has_azure_credentials_field():
    fields = PeerAppModel.model_fields
    assert "azure_credentials" in fields
    assert fields["azure_credentials"].default is None


def test_cluster_azure_credentials_parses_envelope_and_defaults_none(mocker):
    """The stored envelope parses back to AzureStorageParameters; unset reads as None."""
    cluster = ValkeyCluster.__new__(ValkeyCluster)
    cluster.model = mocker.MagicMock()

    params = AzureStorageParameters.model_validate(
        {
            "container": "c",
            "storage-account": "a",
            "secret-key": "k",
            "connection-protocol": "https",
            "path": "valkey",
        }
    )
    cluster.model.azure_credentials = params.model_dump_json(by_alias=True)
    got = cluster.azure_credentials
    assert isinstance(got, AzureStorageParameters)
    assert got.container == "c"
    assert got.secret_key == "k"

    cluster.model.azure_credentials = None
    assert cluster.azure_credentials is None

    cluster.model = None
    assert cluster.azure_credentials is None


def test_credentials_changed_handlers_cover_every_registered_backend(mocker):
    """Every registry entry has a changed handler, or its integrator is inert.

    ``_reconcile_other_backends`` and the leader_elected recovery observers are
    both generated from this table, so a missing entry silently loses a backend.
    """
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd"),
        leader=True,
        relations={
            testing.PeerRelation(id=1, endpoint=PEER_RELATION),
            testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION),
        },
        containers={testing.Container(name="valkey", can_connect=True)},
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        handlers = manager.charm.backup_events._credentials_changed_handlers
    assert set(handlers) == set(BACKUP_CREDENTIAL_FIELDS)


def test_azure_parameters_rejects_an_unknown_connection_protocol():
    """Only the six documented integrator values are accepted.

    Anything else would fall through to the plaintext branch of the account URL
    and fail obscurely at request time instead of at the relation boundary.
    """
    with pytest.raises(ValidationError):
        AzureStorageParameters.model_validate(
            {
                "container": "c",
                "storage-account": "a",
                "secret-key": "k",
                "connection-protocol": "ftp",
                "path": "valkey",
            }
        )


def test_azure_parameters_accepts_every_blob_connection_protocol():
    for proto in AZURE_HTTPS_PROTOCOLS | AZURE_HTTP_PROTOCOLS:
        params = AzureStorageParameters.model_validate(
            {
                "container": "c",
                "storage-account": "a",
                "secret-key": "k",
                "connection-protocol": proto,
                "path": "valkey",
            }
        )
        assert params.connection_protocol == proto


def test_create_backup_kills_producer_on_an_unexpected_upload_error(mocker):
    """No SDK escape may orphan the producer.

    The backends translate their own failures, but an exception neither handler
    names would otherwise skip `proc.kill()` and leave `valkey-cli --rdb -`
    blocked on a pipe nobody drains.
    """
    state = _make_state(mocker)
    workload = mocker.MagicMock()
    workload.cli = "valkey-cli"
    proc = mocker.MagicMock()
    workload.exec_stream.return_value = proc

    backend = _fake_backend(mocker)
    backend.upload.side_effect = RuntimeError("SDK leaked something new")
    fixed_now = mocker.patch("src.managers.backup.datetime")
    fixed_now.now.return_value.strftime.return_value = "2026-05-13T10:00:00Z"

    with pytest.raises(RuntimeError):
        BackupManager(state=state, workload=workload).create_backup()

    proc.kill.assert_called_once()
    # The lock is still released on the way out.
    assert state.unit_server.update.call_args_list[-1].args[0] == {"backup_id": ""}


# ── GCS backend ─────────────────────────────────────────────────────────


def _gcs_service_account(**overrides) -> str:
    """Build a syntactically complete service-account key; the private key is a stub.

    Only the model and the mocked SDK ever see it, so the PEM body is never
    parsed. ``overrides`` replace top-level keys (``None`` keeps the key with a
    null value, which is how "absent" is exercised).
    """
    info = {
        "type": "service_account",
        "project_id": "proj",
        "client_email": "backup@proj.iam.gserviceaccount.com",
        "private_key": "-----BEGIN PRIVATE KEY-----\nstub\n-----END PRIVATE KEY-----\n",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    info.update(overrides)
    return json.dumps(info)


def test_gcs_parameters_valid_and_normalised():
    """Trims whitespace, strips the separators that would corrupt object names.

    Also stores the key as canonical JSON text.
    """
    key = _gcs_service_account()
    p = GCSParameters.model_validate(
        {
            "bucket": " /data-charms-testing/ ",
            "path": "/valkey/",
            "secret-key": f"  {key}\n",
            "storage-class": " nearline ",
        }
    )
    assert p.bucket == "data-charms-testing"
    assert p.path == "valkey"
    assert p.secret_key == json.dumps(json.loads(key), sort_keys=True)
    assert p.storage_class == "NEARLINE"


def test_gcs_parameters_accepts_the_dict_the_requirer_lib_hands_over():
    """object-storage-charmlib json.loads every published field, so the JSON key reaches the charm as a dict.

    It is canonicalised back to JSON text.
    """
    info = json.loads(_gcs_service_account())
    p = GCSParameters.model_validate({"bucket": "b", "path": "valkey", "secret-key": info})
    assert isinstance(p.secret_key, str)
    assert json.loads(p.secret_key) == info


def test_gcs_parameters_accepts_a_base64_encoded_key():
    """CI secret plumbing cannot carry raw JSON, so a base64 form is accepted and decoded at the boundary.

    As mongo does: either alphabet, and wrapped at 76 columns the way the
    `base64` CLI emits it without -w0 (spread's env export turns those
    newlines into spaces, which is the same case).
    """
    key = _gcs_service_account()
    wrapped = base64.encodebytes(key.encode()).decode()
    assert "\n" in wrapped.strip()  # the case under test really is multi-line
    for encoded in (
        base64.b64encode(key.encode()).decode(),
        base64.urlsafe_b64encode(key.encode()).decode(),
        wrapped,
        wrapped.replace("\n", " "),
    ):
        p = GCSParameters.model_validate({"bucket": "b", "path": "valkey", "secret-key": encoded})
        assert json.loads(p.secret_key) == json.loads(key)


def test_gcs_parameters_canonical_form_is_key_order_independent():
    """The stored envelope must compare equal to a re-supplied key with the same content.

    Otherwise _store_credentials re-runs ensure_container on every
    leader-elected and rewrites the secret for nothing.
    """
    info = json.loads(_gcs_service_account())
    reordered = dict(reversed(list(info.items())))
    base = {"bucket": "b", "path": "valkey"}
    a = GCSParameters.model_validate({**base, "secret-key": info})
    b = GCSParameters.model_validate({**base, "secret-key": json.dumps(reordered)})
    assert a.secret_key == b.secret_key
    assert a.model_dump() == b.model_dump()


def test_gcs_parameters_storage_class_is_optional():
    base = {"bucket": "b", "path": "valkey", "secret-key": _gcs_service_account()}
    assert GCSParameters.model_validate(base).storage_class is None
    assert GCSParameters.model_validate({**base, "storage-class": ""}).storage_class is None


def test_gcs_parameters_rejects_an_unknown_storage_class():
    """Only the integrator's documented classes are accepted.

    The SDK would refuse the rest only at bucket creation, long after the
    relation settled.
    """
    with pytest.raises(ValidationError):
        GCSParameters.model_validate(
            {
                "bucket": "b",
                "path": "valkey",
                "secret-key": _gcs_service_account(),
                "storage-class": "GLACIER",
            }
        )


def test_gcs_parameters_rejects_empty_required():
    """An empty path would make list_backups enumerate the whole bucket."""
    base = {"bucket": "b", "path": "valkey", "secret-key": _gcs_service_account()}
    for field in ("bucket", "path", "secret-key"):
        with pytest.raises(ValidationError):
            GCSParameters.model_validate({**base, field: ""})
    with pytest.raises(ValidationError):
        GCSParameters.model_validate({"bucket": "b", "path": "valkey"})


def test_gcs_parameters_rejects_a_malformed_service_account_key():
    """A key that is not JSON, not base64 JSON, not an object, or lacks a required field is refused.

    The relation boundary names the field, never the value, whether the
    input was a string or the lib's dict.
    """
    base = {"bucket": "b", "path": "valkey"}
    bad = {
        "not json": "-----BEGIN PRIVATE KEY-----\nstub\n-----END PRIVATE KEY-----",
        "base64 of not json": base64.b64encode(b"stub, not json").decode(),
        "not an object": "[1, 2]",
        "no client_email": _gcs_service_account(client_email=None),
        "no private_key": _gcs_service_account(private_key=""),
        "dict without private_key": json.loads(_gcs_service_account(private_key="")),
    }
    for reason, key in bad.items():
        with pytest.raises(ValidationError) as excinfo:
            GCSParameters.model_validate({**base, "secret-key": key})
        text = str(excinfo.value)
        assert "stub" not in text and "BEGIN PRIVATE KEY" not in text, reason
    assert "private_key" in str(excinfo.value)


def test_gcs_parameters_ignores_unknown_fields():
    p = GCSParameters.model_validate(
        {
            "bucket": "b",
            "path": "valkey",
            "secret-key": _gcs_service_account(),
            "endpoint": "https://storage.googleapis.com",
        }
    )
    assert not hasattr(p, "endpoint")


def test_peer_app_model_has_gcs_credentials_field():
    fields = PeerAppModel.model_fields
    assert "gcs_credentials" in fields
    assert fields["gcs_credentials"].default is None


def test_cluster_gcs_credentials_parses_envelope_and_defaults_none(mocker):
    """The stored envelope parses back to GCSParameters; unset reads as None."""
    cluster = ValkeyCluster.__new__(ValkeyCluster)
    cluster.model = mocker.MagicMock()

    key = _gcs_service_account()
    params = GCSParameters.model_validate(
        {"bucket": "b", "path": "valkey", "secret-key": key, "storage-class": "STANDARD"}
    )
    cluster.model.gcs_credentials = params.model_dump_json(by_alias=True)
    got = cluster.gcs_credentials
    assert isinstance(got, GCSParameters)
    assert got.bucket == "b"
    assert json.loads(got.secret_key) == json.loads(key)
    assert got.storage_class == "STANDARD"
    # Round trip is stable: what was stored re-validates to the same envelope.
    assert got.model_dump() == params.model_dump()

    cluster.model.gcs_credentials = None
    assert cluster.gcs_credentials is None

    cluster.model.gcs_credentials = "{not json"
    assert cluster.gcs_credentials is None

    cluster.model = None
    assert cluster.gcs_credentials is None


def test_metadata_declares_gcs_relation():
    """The GCS integrator relation is declared and mutually exclusive-friendly."""
    meta = yaml.safe_load(pathlib.Path("metadata.yaml").read_text())
    gcs = meta["requires"]["gcs-credentials"]
    assert gcs["interface"] == "gcs"
    assert gcs["limit"] == 1
    assert gcs["optional"] is True


def test_gcs_credentials_flow_through_the_real_requirer(mocker):
    """Contract test through object-storage-charmlib, not a hand-shaped payload.

    The lib json.loads every published field, so the key reaches the charm as a
    dict; the provider's databag points at a Juju secret holding it. The leader
    must end up with the envelope stored.
    """
    ensure = mocker.patch("managers.backup.BackupManager.ensure_container")
    key = _gcs_service_account()
    secret = testing.Secret(tracked_content={"secret-key": key})
    gcs_rel = testing.Relation(
        id=5,
        endpoint=GCS_RELATION_NAME,
        interface="gcs",
        remote_app_name="gcs-integrator",
        remote_app_data={
            "bucket": "b",
            "path": "valkey",
            "secret-extra": secret.id,
            "version": "1",
        },
        local_app_data={"requested-secrets": json.dumps(["secret-key"]), "version": "1"},
    )
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd"),
        leader=True,
        relations={
            testing.PeerRelation(id=1, endpoint=PEER_RELATION),
            testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION),
            gcs_rel,
        },
        secrets={secret},
        containers={testing.Container(name="valkey", can_connect=True)},
    )

    with ctx(ctx.on.relation_changed(gcs_rel, remote_unit=0), state_in) as manager:
        state_out = manager.run()
        stored = manager.charm.state.cluster.gcs_credentials

    ensure.assert_called_once()
    assert stored is not None
    assert stored.bucket == "b"
    assert stored.path == "valkey"
    assert json.loads(stored.secret_key)["client_email"] == "backup@proj.iam.gserviceaccount.com"

    # Only a secret URI may hit the databag: the envelope lives in an app-owned
    # Juju secret (dpcharmlibs hyphenates the field name).
    peer_out = state_out.get_relation(1)
    assert "gcs-credentials" not in peer_out.local_app_data
    assert not any("private_key" in v for v in peer_out.local_app_data.values())
    envelopes = [s for s in state_out.secrets if "gcs-credentials" in (s.latest_content or {})]
    assert len(envelopes) == 1
    assert (
        json.loads(json.loads(envelopes[0].latest_content["gcs-credentials"])["secret-key"])[
            "client_email"
        ]
        == "backup@proj.iam.gserviceaccount.com"
    )


# ── event-driven (ops.testing / Scenario) ────────────────────────────────────
#
# The credentials handlers, the relation-gone paths and the backup actions are
# driven through real Juju events, so the object-storage requirer lib and the
# peer-relation secret routing are part of every test.

BACKUP_ID = "2026-05-13T10:00:00Z"


def _backup_context_and_state(*, leader=True, relations=(), secrets=(), unit_data=None, peer=True):
    """Build a Context + State with the peer relations and the given storage relations."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peers = {testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)}
    if peer:
        peers.add(
            testing.PeerRelation(
                id=1,
                endpoint=PEER_RELATION,
                local_unit_data={"start-state": "started", **(unit_data or {})},
            )
        )
    state = testing.State(
        model=testing.Model(name="m", type="lxd"),
        leader=leader,
        relations={*peers, *relations},
        secrets=set(secrets),
        containers={testing.Container(name="valkey", can_connect=True)},
    )
    return ctx, state


def _integrator_relation(rel_id, endpoint, interface, secret_content, data):
    """Build a storage-integrator relation as the provider publishes it.

    Secret fields travel in a Juju secret referenced from ``secret-extra``, the
    lib's protocol. Returns the relation and the secret for the State.
    """
    secret = testing.Secret(tracked_content=secret_content)
    relation = testing.Relation(
        id=rel_id,
        endpoint=endpoint,
        interface=interface,
        remote_app_name=f"{interface}-integrator",
        remote_app_data={
            **{k: v for k, v in data.items() if v is not None},
            "secret-extra": secret.id,
            "version": "1",
        },
        local_app_data={"requested-secrets": json.dumps(list(secret_content)), "version": "1"},
    )
    return relation, secret


def _s3_relation(**overrides):
    data = {"bucket": "b", "endpoint": "https://e", "path": "p", **overrides}
    return _integrator_relation(
        3, S3_RELATION_NAME, "s3", {"access-key": "AK", "secret-key": "SK"}, data
    )


def _azure_relation(**overrides):
    data = {
        "container": "c",
        "storage-account": "acct",
        "connection-protocol": "https",
        "path": "valkey",
        **overrides,
    }
    return _integrator_relation(
        4, AZURE_RELATION_NAME, "azure_storage", {"secret-key": "SK"}, data
    )


def _gcs_relation(**overrides):
    data = {"bucket": "b", "path": "valkey", **overrides}
    return _integrator_relation(
        5, GCS_RELATION_NAME, "gcs", {"secret-key": _gcs_service_account()}, data
    )


def _stored_credentials(state, field):
    """Return the credentials envelope from the app-owned peer secret, or None."""
    for secret in state.secrets:
        if field in (secret.latest_content or {}):
            return json.loads(secret.latest_content[field])
    return None


def _run_relation_changed(ctx, state, relation):
    """Deliver the provider's data through the requirer lib; return the new State."""
    return ctx.run(ctx.on.relation_changed(state.get_relation(relation.id), remote_unit=0), state)


def _run_relation_broken(ctx, state, relation):
    return ctx.run(ctx.on.relation_broken(state.get_relation(relation.id)), state)


@pytest.fixture
def backup_managers(mocker):
    """Patch the manager/workload ops behind the backup events; return the mocks."""
    return SimpleNamespace(
        ensure_container=mocker.patch("managers.backup.BackupManager.ensure_container"),
        store_tls_ca_chain=mocker.patch("managers.backup.BackupManager.store_tls_ca_chain"),
        remove_tls_ca_chain=mocker.patch("managers.backup.BackupManager.remove_tls_ca_chain"),
        create_backup=mocker.patch(
            "managers.backup.BackupManager.create_backup", return_value=BACKUP_ID
        ),
        list_backups=mocker.patch("managers.backup.BackupManager.list_backups", return_value=[]),
        alive=mocker.patch("workload_k8s.ValkeyK8sWorkload.alive", return_value=True),
    )


# ── credentials changed ──────────────────────────────────────────────────────


def test_s3_credentials_changed_leader_stores_the_normalised_envelope(backup_managers):
    s3_rel, secret = _s3_relation(bucket=" b ", endpoint="https://e/", path="/p/")
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])

    state_out = _run_relation_changed(ctx, state, s3_rel)

    backup_managers.ensure_container.assert_called_once()
    stored = _stored_credentials(state_out, "s3-credentials")
    assert stored["bucket"] == "b"
    assert stored["endpoint"] == "https://e"
    assert stored["path"] == "p"
    assert stored["access-key"] == "AK"


def test_s3_credentials_changed_stores_the_ca_on_every_unit(backup_managers):
    """The endpoint CA is needed on disk by every unit; only the leader stores credentials."""
    s3_rel, secret = _s3_relation(**{"tls-ca-chain": json.dumps(["-----CERT-----"])})
    ctx, state = _backup_context_and_state(leader=False, relations=[s3_rel], secrets=[secret])

    state_out = _run_relation_changed(ctx, state, s3_rel)

    backup_managers.store_tls_ca_chain.assert_called_once()
    assert backup_managers.store_tls_ca_chain.call_args.args[0]["tls-ca-chain"] == [
        "-----CERT-----"
    ]
    assert _stored_credentials(state_out, "s3-credentials") is None


def test_s3_credentials_changed_rejects_a_path_that_strips_to_empty(backup_managers):
    s3_rel, secret = _s3_relation(path="/")
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])

    state_out = _run_relation_changed(ctx, state, s3_rel)

    backup_managers.ensure_container.assert_not_called()
    assert _stored_credentials(state_out, "s3-credentials") is None


def test_s3_credentials_changed_rejects_an_incomplete_payload(backup_managers):
    s3_rel, secret = _s3_relation(endpoint=None)
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])

    state_out = _run_relation_changed(ctx, state, s3_rel)

    backup_managers.ensure_container.assert_not_called()
    assert _stored_credentials(state_out, "s3-credentials") is None


def test_s3_credentials_changed_skips_an_unchanged_envelope(backup_managers):
    """leader_elected re-fires the handler; an unchanged envelope must not hit the store again."""
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])

    state = _run_relation_changed(ctx, state, s3_rel)
    _run_relation_changed(ctx, state, s3_rel)

    backup_managers.ensure_container.assert_called_once()


def test_s3_credentials_changed_defers_without_the_peer_relation(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret], peer=False)

    state_out = _run_relation_changed(ctx, state, s3_rel)

    assert len(state_out.deferred) == 1
    backup_managers.ensure_container.assert_not_called()


def test_credentials_are_not_stored_while_backends_conflict(backup_managers):
    """With two integrators related there is no answer to "which backend"; store nothing."""
    s3_rel, s3_secret = _s3_relation()
    azure_rel, azure_secret = _azure_relation()
    ctx, state = _backup_context_and_state(
        relations=[s3_rel, azure_rel], secrets=[s3_secret, azure_secret]
    )

    state_out = _run_relation_changed(ctx, state, s3_rel)

    backup_managers.ensure_container.assert_not_called()
    assert _stored_credentials(state_out, "s3-credentials") is None


def test_azure_credentials_changed_leader_stores_the_normalised_envelope(backup_managers):
    azure_rel, secret = _azure_relation(container=" c ", path="/valkey/")
    ctx, state = _backup_context_and_state(relations=[azure_rel], secrets=[secret])

    state_out = _run_relation_changed(ctx, state, azure_rel)

    backup_managers.ensure_container.assert_called_once()
    stored = _stored_credentials(state_out, "azure-credentials")
    assert stored["container"] == "c"
    assert stored["path"] == "valkey"
    assert stored["storage-account"] == "acct"


def test_azure_credentials_changed_never_touches_the_s3_ca(backup_managers):
    azure_rel, secret = _azure_relation()
    ctx, state = _backup_context_and_state(leader=False, relations=[azure_rel], secrets=[secret])

    _run_relation_changed(ctx, state, azure_rel)

    backup_managers.store_tls_ca_chain.assert_not_called()
    backup_managers.remove_tls_ca_chain.assert_not_called()


def test_gcs_credentials_changed_leader_stores_the_normalised_envelope(backup_managers):
    """The key arrives as the dict the lib hands over and is stored as canonical JSON text."""
    gcs_rel, secret = _gcs_relation(
        bucket=" data-charms-testing ", path="/valkey/", **{"storage-class": "standard"}
    )
    ctx, state = _backup_context_and_state(relations=[gcs_rel], secrets=[secret])

    state_out = _run_relation_changed(ctx, state, gcs_rel)

    backup_managers.ensure_container.assert_called_once()
    stored = _stored_credentials(state_out, "gcs-credentials")
    assert stored["bucket"] == "data-charms-testing"
    assert stored["path"] == "valkey"
    assert stored["storage-class"] == "STANDARD"
    assert json.loads(stored["secret-key"]) == json.loads(_gcs_service_account())


def test_gcs_credentials_changed_never_touches_the_s3_ca(backup_managers):
    gcs_rel, secret = _gcs_relation()
    ctx, state = _backup_context_and_state(leader=False, relations=[gcs_rel], secrets=[secret])

    _run_relation_changed(ctx, state, gcs_rel)

    backup_managers.store_tls_ca_chain.assert_not_called()
    backup_managers.remove_tls_ca_chain.assert_not_called()


def test_leader_elected_without_a_storage_relation_stores_nothing(backup_managers):
    """Every backend's handler re-fires on leader_elected and must no-op without its relation."""
    ctx, state = _backup_context_and_state()

    state_out = ctx.run(ctx.on.leader_elected(), state)

    backup_managers.ensure_container.assert_not_called()
    for field in ("s3-credentials", "azure-credentials", "gcs-credentials"):
        assert _stored_credentials(state_out, field) is None


# ── credentials gone ─────────────────────────────────────────────────────────


def test_s3_credentials_gone_removes_the_ca_and_clears_the_envelope(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])
    state = _run_relation_changed(ctx, state, s3_rel)
    assert _stored_credentials(state, "s3-credentials") is not None

    state_out = _run_relation_broken(ctx, state, s3_rel)

    backup_managers.remove_tls_ca_chain.assert_called_once_with()
    assert _stored_credentials(state_out, "s3-credentials") is None


def test_s3_credentials_gone_on_a_non_leader_keeps_the_envelope(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])
    state = replace(_run_relation_changed(ctx, state, s3_rel), leader=False)

    state_out = _run_relation_broken(ctx, state, s3_rel)

    backup_managers.remove_tls_ca_chain.assert_called_once_with()
    assert _stored_credentials(state_out, "s3-credentials") is not None


def test_s3_credentials_gone_defers_during_a_backup(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(
        relations=[s3_rel], secrets=[secret], unit_data={"backup-id": BACKUP_ID}
    )

    state_out = _run_relation_broken(ctx, state, s3_rel)

    assert len(state_out.deferred) == 1
    backup_managers.remove_tls_ca_chain.assert_not_called()


def test_azure_credentials_gone_clears_the_envelope_without_touching_the_ca(backup_managers):
    azure_rel, secret = _azure_relation()
    ctx, state = _backup_context_and_state(relations=[azure_rel], secrets=[secret])
    state = _run_relation_changed(ctx, state, azure_rel)

    state_out = _run_relation_broken(ctx, state, azure_rel)

    backup_managers.remove_tls_ca_chain.assert_not_called()
    assert _stored_credentials(state_out, "azure-credentials") is None


def test_azure_credentials_gone_on_a_non_leader_keeps_the_envelope(backup_managers):
    azure_rel, secret = _azure_relation()
    ctx, state = _backup_context_and_state(relations=[azure_rel], secrets=[secret])
    state = replace(_run_relation_changed(ctx, state, azure_rel), leader=False)

    state_out = _run_relation_broken(ctx, state, azure_rel)

    assert _stored_credentials(state_out, "azure-credentials") is not None


def test_azure_credentials_gone_defers_during_a_backup(backup_managers):
    azure_rel, secret = _azure_relation()
    ctx, state = _backup_context_and_state(
        relations=[azure_rel], secrets=[secret], unit_data={"backup-id": BACKUP_ID}
    )

    state_out = _run_relation_broken(ctx, state, azure_rel)

    assert len(state_out.deferred) == 1


def test_gcs_credentials_gone_clears_the_envelope_without_touching_the_ca(backup_managers):
    gcs_rel, secret = _gcs_relation()
    ctx, state = _backup_context_and_state(relations=[gcs_rel], secrets=[secret])
    state = _run_relation_changed(ctx, state, gcs_rel)

    state_out = _run_relation_broken(ctx, state, gcs_rel)

    backup_managers.remove_tls_ca_chain.assert_not_called()
    assert _stored_credentials(state_out, "gcs-credentials") is None


def test_gcs_credentials_gone_on_a_non_leader_keeps_the_envelope(backup_managers):
    gcs_rel, secret = _gcs_relation()
    ctx, state = _backup_context_and_state(relations=[gcs_rel], secrets=[secret])
    state = replace(_run_relation_changed(ctx, state, gcs_rel), leader=False)

    state_out = _run_relation_broken(ctx, state, gcs_rel)

    assert _stored_credentials(state_out, "gcs-credentials") is not None


def test_gcs_credentials_gone_defers_during_a_backup(backup_managers):
    gcs_rel, secret = _gcs_relation()
    ctx, state = _backup_context_and_state(
        relations=[gcs_rel], secrets=[secret], unit_data={"backup-id": BACKUP_ID}
    )

    state_out = _run_relation_broken(ctx, state, gcs_rel)

    assert len(state_out.deferred) == 1


@pytest.mark.parametrize(
    ("removed", "survivor", "survivor_field"),
    [
        (_s3_relation, _azure_relation, "azure-credentials"),
        (_s3_relation, _gcs_relation, "gcs-credentials"),
        (_azure_relation, _s3_relation, "s3-credentials"),
        (_gcs_relation, _s3_relation, "s3-credentials"),
        (_gcs_relation, _azure_relation, "azure-credentials"),
    ],
)
def test_credentials_gone_converges_the_surviving_backend(
    backup_managers, removed, survivor, survivor_field
):
    """Two integrators conflict; removing one lets the other store its credentials."""
    removed_rel, removed_secret = removed()
    survivor_rel, survivor_secret = survivor()
    ctx, state = _backup_context_and_state(
        relations=[removed_rel, survivor_rel],
        secrets=[removed_secret, survivor_secret],
    )
    state = _run_relation_changed(ctx, state, survivor_rel)
    assert _stored_credentials(state, survivor_field) is None

    state_out = _run_relation_broken(ctx, state, removed_rel)

    backup_managers.ensure_container.assert_called_once()
    assert _stored_credentials(state_out, survivor_field) is not None


# ── backup actions ───────────────────────────────────────────────────────────


def _with_s3_credentials(ctx, state, backup_managers):
    """Store S3 credentials through the real relation-changed path."""
    state = _run_relation_changed(ctx, state, state.get_relation(3))
    backup_managers.ensure_container.reset_mock()
    return state


def _with_backup_running(state):
    """Mark a backup as running on this unit (its per-unit databag lock)."""
    peer = state.get_relation(1)
    running = replace(peer, local_unit_data={**peer.local_unit_data, "backup-id": BACKUP_ID})
    return replace(state, relations={running, *(r for r in state.relations if r.id != 1)})


def test_create_backup_action_reports_the_backup_id(backup_managers, caplog):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])
    state = _with_s3_credentials(ctx, state, backup_managers)

    with caplog.at_level(logging.INFO):
        ctx.run(ctx.on.action("create-backup", id="42"), state)

    assert ctx.action_results == {"backup-id": BACKUP_ID}
    # Audit trail: the action id, no unit name (Juju prefixes every line with it).
    audit = [r.message for r in caplog.records if "audit: create-backup" in r.message]
    assert audit and "action_id=42" in audit[0] and "unit=" not in audit[0]


def test_create_backup_action_fails_without_a_storage_relation(backup_managers):
    ctx, state = _backup_context_and_state()

    with pytest.raises(testing.ActionFailed) as exc:
        ctx.run(ctx.on.action("create-backup"), state)

    assert "No backup storage relation" in exc.value.message
    # The hint is generated from the registry, so every backend appears in it.
    assert S3_RELATION_NAME in exc.value.message
    backup_managers.create_backup.assert_not_called()


def test_create_backup_action_fails_without_credentials(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])

    with pytest.raises(testing.ActionFailed) as exc:
        ctx.run(ctx.on.action("create-backup"), state)

    assert "credentials" in exc.value.message.lower()


def test_create_backup_action_fails_while_backends_conflict(backup_managers):
    s3_rel, s3_secret = _s3_relation()
    azure_rel, azure_secret = _azure_relation()
    ctx, state = _backup_context_and_state(
        relations=[s3_rel, azure_rel], secrets=[s3_secret, azure_secret]
    )

    with pytest.raises(testing.ActionFailed) as exc:
        ctx.run(ctx.on.action("create-backup"), state)

    assert "exactly one" in exc.value.message


def test_create_backup_action_fails_when_valkey_is_down(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])
    state = _with_s3_credentials(ctx, state, backup_managers)
    backup_managers.alive.return_value = False

    with pytest.raises(testing.ActionFailed) as exc:
        ctx.run(ctx.on.action("create-backup"), state)

    assert "not running" in exc.value.message


def test_create_backup_action_fails_while_a_backup_is_running_here(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])
    state = _with_backup_running(_with_s3_credentials(ctx, state, backup_managers))

    with pytest.raises(testing.ActionFailed) as exc:
        ctx.run(ctx.on.action("create-backup"), state)

    assert "already in progress" in exc.value.message
    backup_managers.create_backup.assert_not_called()


def test_create_backup_action_reports_a_backup_error(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])
    state = _with_s3_credentials(ctx, state, backup_managers)
    backup_managers.create_backup.side_effect = ValkeyBackupError("boom")

    with pytest.raises(testing.ActionFailed):
        ctx.run(ctx.on.action("create-backup"), state)


def test_restore_and_backup_guards_share_the_storage_checks(backup_managers):
    """Both actions gate on backup storage the same way, from one implementation."""
    s3_rel, s3_secret = _s3_relation()
    azure_rel, azure_secret = _azure_relation()
    ctx, state = _backup_context_and_state(
        relations=[s3_rel, azure_rel], secrets=[s3_secret, azure_secret]
    )

    with ctx(ctx.on.update_status(), state) as manager:
        events = manager.charm.backup_events
        assert events._blocking_reason() == events._restore_blocking_reason(BACKUP_ID)


def test_list_backups_action_returns_a_table(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])
    state = _with_s3_credentials(ctx, state, backup_managers)
    backup_managers.list_backups.return_value = [BACKUP_ID]

    ctx.run(ctx.on.action("list-backups", params={"output": "table"}), state)

    assert BACKUP_ID in ctx.action_results["backups"]


def test_list_backups_action_returns_json(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])
    state = _with_s3_credentials(ctx, state, backup_managers)
    backup_managers.list_backups.return_value = ["2026-05-14T10:00:00Z", BACKUP_ID]

    ctx.run(ctx.on.action("list-backups", params={"output": "json"}), state)

    assert json.loads(ctx.action_results["backups"]) == [
        {"backup-id": "2026-05-14T10:00:00Z", "backup-status": "finished"},
        {"backup-id": BACKUP_ID, "backup-status": "finished"},
    ]


def test_list_backups_action_rejects_an_invalid_format(backup_managers):
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])

    with pytest.raises(testing.ActionFailed) as exc:
        ctx.run(ctx.on.action("list-backups", params={"output": "yaml"}), state)

    assert "invalid output format" in exc.value.message
    backup_managers.list_backups.assert_not_called()


def test_list_backups_action_runs_while_a_backup_is_running_here(backup_managers):
    """list-backups is read-only, so a backup on this unit must not block it."""
    s3_rel, secret = _s3_relation()
    ctx, state = _backup_context_and_state(relations=[s3_rel], secrets=[secret])
    state = _with_backup_running(_with_s3_credentials(ctx, state, backup_managers))

    ctx.run(ctx.on.action("list-backups"), state)

    backup_managers.list_backups.assert_called_once()
    assert "No backups" in ctx.action_results["backups"]
