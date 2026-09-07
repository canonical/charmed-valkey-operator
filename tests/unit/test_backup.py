#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the S3 backup feature."""

from src.statuses import BackupStatuses


def test_backup_statuses_present():
    assert BackupStatuses.BACKUP_IN_PROGRESS.value.status == "maintenance"
    assert BackupStatuses.BACKUP_CREDENTIALS_MISSING.value.status == "blocked"
    assert BackupStatuses.BACKUP_BACKENDS_CONFLICT.value.status == "blocked"
    assert BackupStatuses.BACKUP_FAILED.value.status == "blocked"


def test_peer_app_model_has_s3_credentials_field():
    from src.core.models import PeerAppModel

    fields = PeerAppModel.model_fields
    assert "s3_credentials" in fields
    assert fields["s3_credentials"].default is None


def test_cluster_s3_credentials_parses_envelope_and_defaults_none(mocker):
    """The stored envelope parses back to S3Parameters; unset reads as None."""
    import json

    from src.core.models import S3Parameters, ValkeyCluster

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
    from src.core.models import PeerUnitModel

    assert "backup_id" in PeerUnitModel.model_fields
    assert PeerUnitModel.model_fields["backup_id"].default == ""


def test_valkey_server_is_backup_in_progress_reflects_model_field():
    from src.core.models import PeerUnitModel, ValkeyServer

    server = ValkeyServer.__new__(ValkeyServer)
    server.model = PeerUnitModel(backup_id="2026-05-13T10:00:00Z")
    assert server.is_backup_in_progress is True

    server.model = PeerUnitModel(backup_id="")
    assert server.is_backup_in_progress is False

    server.model = None
    assert server.is_backup_in_progress is False


def test_cluster_state_exposes_s3_relation(cloud_spec):
    from ops import testing

    from src.charm import ValkeyCharm
    from src.literals import PEER_RELATION, S3_RELATION_NAME, STATUS_PEERS_RELATION

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
        model=testing.Model(name="m", type="lxd", cloud_spec=cloud_spec),
        leader=True,
        relations={peer, status_peer, s3_rel},
        containers={testing.Container(name="valkey", can_connect=True)},
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        assert manager.charm.state.s3_relation is not None
        assert manager.charm.state.s3_relation.name == S3_RELATION_NAME


def test_active_backup_credentials_follows_the_relation(cloud_spec, mocker):
    """Credentials are only "active" while the backend they belong to is related.

    The leader clears the stored envelope on relation-broken; until it does, the
    stored value must not keep a peer talking to a backend nobody is related to.
    ``s3_credentials`` is an ``ExtraSecretStr`` routed through a Juju secret, so
    it's patched at the property rather than forged into the databag.
    """
    from unittest.mock import PropertyMock

    from ops import testing

    from src.charm import ValkeyCharm
    from src.literals import PEER_RELATION, S3_RELATION_NAME, STATUS_PEERS_RELATION

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
        "model": testing.Model(name="m", type="lxd", cloud_spec=cloud_spec),
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
    from src.core.models import PeerAppModel
    from src.literals import AZURE_RELATION_NAME, BACKUP_CREDENTIAL_FIELDS, S3_RELATION_NAME

    assert BACKUP_CREDENTIAL_FIELDS[S3_RELATION_NAME] == "s3_credentials"
    assert BACKUP_CREDENTIAL_FIELDS[AZURE_RELATION_NAME] == "azure_credentials"
    # Every registered field must exist on the app databag model, or the leader
    # would silently write credentials nothing reads back.
    for field in BACKUP_CREDENTIAL_FIELDS.values():
        assert field in PeerAppModel.model_fields


def test_backup_relations_and_conflict_follow_the_registry(cloud_spec):
    """Relation discovery and the conflict check are driven by the registry alone.

    Exercised with the two registered backends, so it also pins the mutual
    exclusion the charm enforces: relate exactly one storage integrator.
    """
    from ops import testing

    from src.charm import ValkeyCharm
    from src.literals import (
        AZURE_RELATION_NAME,
        PEER_RELATION,
        S3_RELATION_NAME,
        STATUS_PEERS_RELATION,
    )

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    s3_rel = testing.Relation(id=3, endpoint=S3_RELATION_NAME, interface="s3")
    azure_rel = testing.Relation(id=4, endpoint=AZURE_RELATION_NAME, interface="azure_storage")
    common = {
        "model": testing.Model(name="m", type="lxd", cloud_spec=cloud_spec),
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


def test_backup_ca_path_is_charm_local_not_workload_tls_dir(mocker, tmp_path):
    """The S3 CA path is charm-process-local, never a workload TLS path.

    boto3 runs in the charm process, so the bundle must sit under the charm
    dir (``charm.charm_dir``), never in the workload-container ``tls_paths``
    and never on the workload object at all.
    """
    from src.core.base_workload import TLSPaths, WorkloadBase
    from src.literals import BACKUP_CA_FILENAME
    from src.managers.backup import BackupManager

    # backup CA must be neither a workload (container) TLS path nor any
    # attribute of the workload -- it belongs to the charm-process side.
    assert not hasattr(TLSPaths, "backup_ca")
    assert not hasattr(WorkloadBase, "backup_ca_path")

    state = mocker.MagicMock()
    state.charm.charm_dir = tmp_path
    mgr = BackupManager(state=state, workload=mocker.MagicMock())
    assert mgr._backup_ca_path == tmp_path / BACKUP_CA_FILENAME


def test_backup_manager_store_tls_ca_chain_writes_charm_local_file(mocker, tmp_path):
    from src.literals import BACKUP_CA_FILENAME
    from src.managers.backup import BackupManager

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
    from src.literals import BACKUP_CA_FILENAME
    from src.managers.backup import BackupManager

    state = mocker.MagicMock()
    state.charm.charm_dir = tmp_path
    mgr = BackupManager(state=state, workload=mocker.MagicMock())
    mgr.store_tls_ca_chain({"bucket": "b"})
    assert not (tmp_path / BACKUP_CA_FILENAME).exists()


def test_backup_manager_store_tls_ca_chain_rejects_non_list(mocker, tmp_path):
    from src.literals import BACKUP_CA_FILENAME
    from src.managers.backup import BackupManager

    state = mocker.MagicMock()
    state.charm.charm_dir = tmp_path
    mgr = BackupManager(state=state, workload=mocker.MagicMock())
    # A bare string must not be char-joined into a corrupt bundle.
    mgr.store_tls_ca_chain({"tls-ca-chain": "-----BEGIN CERTIFICATE-----"})
    assert not (tmp_path / BACKUP_CA_FILENAME).exists()


def test_backup_manager_store_tls_ca_chain_rejects_non_pem_items(mocker, tmp_path):
    from src.literals import BACKUP_CA_FILENAME
    from src.managers.backup import BackupManager

    state = mocker.MagicMock()
    state.charm.charm_dir = tmp_path
    mgr = BackupManager(state=state, workload=mocker.MagicMock())
    # A list whose items lack a PEM armour header is malformed; the whole
    # chain is rejected rather than written as an unloadable CA bundle.
    mgr.store_tls_ca_chain({"tls-ca-chain": ["not-a-cert", "also-not-a-cert"]})
    assert not (tmp_path / BACKUP_CA_FILENAME).exists()


def test_ensure_container_delegates_to_the_backend(mocker):
    """The manager just asks the backend; the create semantics are the backend's."""
    from src.managers.backup import BackupManager

    backend = _fake_built_backend(mocker)

    BackupManager(state=mocker.MagicMock(), workload=mocker.MagicMock()).ensure_container(
        _s3_params()
    )
    backend.ensure_container.assert_called_once_with()


def test_ensure_container_wraps_backend_error_and_keeps_the_code(mocker):
    """Bucket setup failures reach the credentials handler as a backup error."""
    import pytest

    from common.exceptions import StorageBackendError, ValkeyBackupError
    from src.managers.backup import BackupManager

    backend = _fake_built_backend(mocker)
    backend.ensure_container.side_effect = StorageBackendError("x", safe_code="AccessDenied")

    with pytest.raises(ValkeyBackupError) as excinfo:
        BackupManager(state=mocker.MagicMock(), workload=mocker.MagicMock()).ensure_container(
            _s3_params()
        )
    assert excinfo.value.safe_code == "AccessDenied"


def test_list_backups_keeps_only_backup_ids_newest_first(mocker):
    """The manager filters the backend's object ids and orders them, newest first."""
    from src.managers.backup import BackupManager

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
    import pytest

    from common.exceptions import StorageBackendError, ValkeyBackupError
    from src.managers.backup import BackupManager

    state = mocker.MagicMock()
    state.active_backup_credentials = _s3_params(path="p")
    backend = _fake_backend(mocker)
    backend.list_object_ids.side_effect = StorageBackendError("x", safe_code="NoSuchBucket")

    with pytest.raises(ValkeyBackupError) as excinfo:
        BackupManager(state=state, workload=mocker.MagicMock()).list_backups()
    assert excinfo.value.safe_code == "NoSuchBucket"


def test_list_backups_without_credentials_raises(mocker):
    """No related backend (or nothing stored yet) is an error, not an empty list."""
    import pytest

    from common.exceptions import ValkeyBackupError
    from src.managers.backup import BackupManager

    state = mocker.MagicMock()
    state.active_backup_credentials = None

    with pytest.raises(ValkeyBackupError):
        BackupManager(state=state, workload=mocker.MagicMock()).list_backups()


def test_format_backup_list_renders_table():
    from src.managers.backup import BackupManager

    formatted = BackupManager.format_backup_list(["2026-05-13T10:00:00Z"])
    assert "backup-id" in formatted
    assert "backup-status" in formatted
    assert "2026-05-13T10:00:00Z" in formatted
    assert "finished" in formatted


def test_format_backup_list_empty():
    from src.managers.backup import BackupManager

    assert BackupManager.format_backup_list([]) == "No backups found."


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


def _fake_backend(mocker):
    """Patch BackupManager's cached backend onto a fake StorageBackend.

    Manager tests assert what the manager asks of the backend; the SDK wiring
    behind the Protocol is covered in test_storage_backend.py.
    """
    from src.managers.backup import BackupManager

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
    import io

    from src.managers.backup import BackupManager

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
    import io

    import pytest

    from common.exceptions import ValkeyBackupError
    from src.managers.backup import BackupManager

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
    import pytest

    from common.exceptions import ValkeyBackupError
    from src.managers.backup import BackupManager

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
    import pytest

    from common.exceptions import ValkeyBackupError
    from literals import BACKUP_EXISTS_CODE
    from src.managers.backup import BackupManager

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
    import pytest

    from common.exceptions import StorageBackendError, ValkeyBackupError
    from src.managers.backup import BackupManager

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
    import pytest

    from common.exceptions import ValkeyBackupError
    from src.managers.backup import BackupManager

    state = mocker.MagicMock()
    state.active_backup_credentials = None
    workload = mocker.MagicMock()

    with pytest.raises(ValkeyBackupError):
        BackupManager(state=state, workload=workload).create_backup()
    workload.exec_stream.assert_not_called()
    state.unit_server.update.assert_not_called()


def test_get_statuses_idle(mocker):
    from src.managers.backup import BackupManager
    from src.statuses import CharmStatuses

    state = mocker.MagicMock()
    state.statuses.get.return_value.root = []
    state.unit_server.is_backup_in_progress = False
    state.s3_relation = None
    state.cluster.s3_credentials = None

    statuses = BackupManager(state=state, workload=mocker.MagicMock()).get_statuses(scope="unit")
    assert statuses == [CharmStatuses.ACTIVE_IDLE.value]


def test_get_statuses_backup_in_progress_unit_scope(mocker):
    from src.managers.backup import BackupManager
    from src.statuses import BackupStatuses

    state = mocker.MagicMock()
    state.statuses.get.return_value.root = []
    state.unit_server.is_backup_in_progress = True
    state.s3_relation = None
    state.cluster.s3_credentials = None

    statuses = BackupManager(state=state, workload=mocker.MagicMock()).get_statuses(scope="unit")
    assert BackupStatuses.BACKUP_IN_PROGRESS.value in statuses


def _blocking_evt(mocker, *, relation=True, credentials=True, alive=True, conflict=False):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.state.s3_relation = mocker.MagicMock() if relation else None
    charm.state.backup_relations = [mocker.MagicMock()] if relation else []
    charm.state.backup_backends_conflict = conflict
    charm.state.active_backup_credentials = (
        None if conflict else ({"bucket": "b"} if credentials else None)
    )
    charm.workload.alive.return_value = alive
    charm.state.unit_server.is_backup_in_progress = False
    charm.state.cluster.is_restore_in_progress = False
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    return evt


def test_blocking_reason_no_relation(mocker):
    assert "No backup storage relation" in _blocking_evt(mocker, relation=False)._blocking_reason()


def test_blocking_reason_no_credentials(mocker):
    assert "credentials" in _blocking_evt(mocker, credentials=False)._blocking_reason().lower()


def test_blocking_reason_rejects_a_backend_conflict(mocker):
    """Two related integrators: refuse rather than pick one."""
    reason = _blocking_evt(mocker, conflict=True)._blocking_reason()
    assert "exactly one" in reason


def test_blocking_reason_names_the_registered_relations(mocker):
    """The no-relation hint is generated, so a new backend appears in it for free."""
    from src.literals import S3_RELATION_NAME

    reason = _blocking_evt(mocker, relation=False)._blocking_reason()
    assert S3_RELATION_NAME in reason


def test_restore_and_backup_guards_share_the_storage_checks(mocker):
    """Both actions gate on backup storage the same way, from one implementation."""
    evt = _blocking_evt(mocker, conflict=True)
    evt.charm.unit.is_leader.return_value = True
    assert evt._blocking_reason() == evt._restore_blocking_reason("2026-05-13T10:00:00Z")


def test_blocking_reason_workload_down(mocker):
    assert "not running" in _blocking_evt(mocker, alive=False)._blocking_reason()


def test_blocking_reason_none_when_all_ok(mocker):
    assert _blocking_evt(mocker)._blocking_reason() is None


def test_blocking_reason_in_progress_check_is_toggleable(mocker):
    # The default checks for a running backup (create-backup); list-backups
    # passes check_running_operations=False because it is read-only.
    evt = _blocking_evt(mocker)
    evt.charm.state.unit_server.is_backup_in_progress = True
    assert "already in progress" in evt._blocking_reason()
    assert evt._blocking_reason(check_running_operations=False) is None


def test_on_s3_credentials_changed_stores_ca_on_all_units(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.unit.is_leader.return_value = False
    charm.state.peer_relation = mocker.MagicMock()
    charm.backup_manager = mocker.MagicMock()

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt.s3_requirer = mocker.MagicMock()
    evt.s3_requirer.get_storage_connection_info.return_value = {
        "bucket": "b",
        "endpoint": "https://e/",
        "path": "/p/",
        "access-key": "AK",
        "secret-key": "SK",
        "tls-ca-chain": ["-----CERT-----"],
    }

    evt._on_s3_credentials_changed(mocker.MagicMock())
    charm.backup_manager.store_tls_ca_chain.assert_called_once()
    charm.state.cluster.update.assert_not_called()


def test_on_s3_credentials_changed_leader_writes_databag(mocker):
    import json

    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.unit.is_leader.return_value = True
    charm.state.peer_relation = mocker.MagicMock()
    charm.state.backup_backends_conflict = False

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt.s3_requirer = mocker.MagicMock()
    evt.s3_requirer.get_storage_connection_info.return_value = {
        "bucket": " b ",
        "endpoint": "https://e/",
        "path": "/p/",
        "access-key": "AK",
        "secret-key": "SK",
    }
    # No backup/restore in flight, so the credentials change is applied, not deferred.
    charm.state.is_backup_in_progress_any = False
    charm.state.cluster.is_restore_in_progress = False

    evt._on_s3_credentials_changed(mocker.MagicMock())
    charm.backup_manager.ensure_container.assert_called_once()
    args, _ = charm.state.cluster.update.call_args
    payload = args[0]
    creds = json.loads(payload["s3_credentials"])
    assert creds["bucket"] == "b"
    assert creds["endpoint"] == "https://e"
    assert creds["path"] == "p"


def test_safe_error_surfaces_s3_code_only(mocker):
    """Only the structured, backend-neutral error code reaches the action result.

    Raised through the whole stack -- SDK error, backend, manager, action -- not
    hand-wired: every layer in between rewraps the error, and a test that builds
    the chain itself would keep passing after one of them stopped forwarding the
    code.
    """
    import pytest
    from botocore.exceptions import ClientError

    # Flat import: the manager raises `common.exceptions.ValkeyBackupError`, a
    # different class object from the `src.`-prefixed one, so pytest.raises must
    # be given the flat one to catch it.
    # ...and flat for S3Backend too: src/ imports are flat, so the class the
    # manager actually instantiates is `common.storage_backend.S3Backend`.
    from common.exceptions import ValkeyBackupError
    from common.storage_backend import S3Backend
    from src.events.backup import _safe_error
    from src.managers.backup import BackupManager

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
    from src.common.exceptions import ValkeyBackupError
    from src.events.backup import _safe_error

    wrapped = ValkeyBackupError("valkey-cli --rdb exited 1: connection refused 10.1.2.3:6379")
    msg = _safe_error(wrapped)
    assert "10.1.2.3" not in msg
    assert "debug-log" in msg


def test_on_s3_credentials_changed_rejects_path_that_strips_to_empty(mocker):
    """path='/' normalises to '' and must be rejected, not stored."""
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.unit.is_leader.return_value = True
    charm.state.peer_relation = mocker.MagicMock()
    charm.state.backup_backends_conflict = False

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt.s3_requirer = mocker.MagicMock()
    evt.s3_requirer.get_storage_connection_info.return_value = {
        "bucket": "b",
        "endpoint": "https://e",
        "path": "/",
        "access-key": "AK",
        "secret-key": "SK",
    }

    evt._on_s3_credentials_changed(mocker.MagicMock())
    charm.backup_manager.ensure_container.assert_not_called()
    charm.state.cluster.update.assert_not_called()


def test_on_s3_credentials_changed_skips_when_envelope_unchanged(mocker):
    """An unchanged envelope must not trigger another ensure_container call."""
    from src.core.models import S3Parameters
    from src.events.backup import BackupEvents

    envelope = {
        "bucket": "b",
        "endpoint": "https://e",
        "path": "p",
        "access-key": "AK",
        "secret-key": "SK",
    }
    charm = mocker.MagicMock()
    charm.unit.is_leader.return_value = True
    charm.state.peer_relation = mocker.MagicMock()
    charm.state.backup_backends_conflict = False
    # already stored: the parsed envelope the handler will compare against
    charm.state.cluster.s3_credentials = S3Parameters.model_validate(dict(envelope))

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt.s3_requirer = mocker.MagicMock()
    evt.s3_requirer.get_storage_connection_info.return_value = dict(envelope)

    evt._on_s3_credentials_changed(mocker.MagicMock())
    charm.backup_manager.ensure_container.assert_not_called()
    charm.state.cluster.update.assert_not_called()


def test_on_s3_credentials_changed_missing_params_skips_databag(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.unit.is_leader.return_value = True
    charm.state.peer_relation = mocker.MagicMock()
    charm.state.backup_backends_conflict = False

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt.s3_requirer = mocker.MagicMock()
    evt.s3_requirer.get_storage_connection_info.return_value = {"bucket": "b"}

    evt._on_s3_credentials_changed(mocker.MagicMock())
    charm.state.cluster.update.assert_not_called()
    charm.backup_manager.ensure_container.assert_not_called()


def test_credentials_are_not_stored_while_backends_conflict(mocker):
    """With two integrators related there is no answer to "which backend"; store nothing."""
    from src.events.backup import BackupEvents

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = mocker.MagicMock()
    evt.charm.unit.is_leader.return_value = True
    evt.charm.state.backup_backends_conflict = True

    evt._store_credentials({"bucket": "b"}, mocker.MagicMock(), "s3_credentials", mocker.Mock())

    evt.charm.state.cluster.update.assert_not_called()
    evt.charm.backup_manager.ensure_container.assert_not_called()


def test_credentials_gone_re_drives_the_other_backends(mocker):
    """Removing one integrator clears the conflict, so the others get another go.

    Otherwise a still-related backend would sit unconfigured until some unrelated
    event happened to re-fire its handler.
    """
    from src.events.backup import BackupEvents

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = mocker.MagicMock()
    other = mocker.Mock()
    evt._credentials_changed_handlers = {"s3-credentials": mocker.Mock(), "other": other}

    evt._reconcile_other_backends(mocker.Mock(), exclude="s3-credentials")

    other.assert_called_once()
    evt._credentials_changed_handlers["s3-credentials"].assert_not_called()


def test_on_s3_credentials_gone_defers_during_backup(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.state.unit_server.is_backup_in_progress = True

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    event = mocker.MagicMock()
    evt._on_s3_credentials_gone(event)
    event.defer.assert_called_once()
    charm.backup_manager.remove_tls_ca_chain.assert_not_called()


def test_on_s3_credentials_gone_removes_ca_and_clears_databag(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.state.is_backup_in_progress_any = False
    charm.state.cluster.is_restore_in_progress = False
    charm.unit.is_leader.return_value = True

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt._credentials_changed_handlers = {}
    evt._on_s3_credentials_gone(mocker.MagicMock())
    charm.backup_manager.remove_tls_ca_chain.assert_called_once_with()
    charm.state.cluster.update.assert_called_once_with({"s3_credentials": ""})


def test_on_create_backup_action_happy(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.backup_manager.create_backup.return_value = "2026-05-13T10:00:00Z"
    charm.state.unit_server.is_backup_in_progress = False
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    mocker.patch.object(evt, "_blocking_reason", return_value=None)

    event = mocker.MagicMock()
    evt._on_create_backup_action(event)
    event.set_results.assert_called_with({"backup-id": "2026-05-13T10:00:00Z"})
    event.fail.assert_not_called()


def test_on_create_backup_action_audit_logs_invocation(mocker, caplog):
    """Each create-backup invocation is audit-logged with its action id.

    No unit name in the message -- Juju already prefixes every log line with
    the unit that emitted it.
    """
    import logging

    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.unit.name = "valkey/2"
    charm.backup_manager.create_backup.return_value = "2026-05-13T10:00:00Z"
    charm.state.unit_server.is_backup_in_progress = False
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    mocker.patch.object(evt, "_blocking_reason", return_value=None)

    event = mocker.MagicMock()
    event.id = "42"
    with caplog.at_level(logging.INFO):
        evt._on_create_backup_action(event)

    audit = [r.message for r in caplog.records if "audit: create-backup" in r.message]
    assert audit, "expected an audit log line for the action invocation"
    assert "action_id=42" in audit[0]
    assert "unit=" not in audit[0]


def test_on_create_backup_action_fails_when_guard_blocks(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    mocker.patch.object(evt, "_blocking_reason", return_value="No S3 relation.")

    event = mocker.MagicMock()
    evt._on_create_backup_action(event)
    event.fail.assert_called_once_with("No S3 relation.")
    charm.backup_manager.create_backup.assert_not_called()


def test_on_create_backup_action_handles_backup_error(mocker):
    from common.exceptions import ValkeyBackupError
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.backup_manager.create_backup.side_effect = ValkeyBackupError("boom")
    charm.state.unit_server.is_backup_in_progress = False
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    mocker.patch.object(evt, "_blocking_reason", return_value=None)

    event = mocker.MagicMock()
    evt._on_create_backup_action(event)
    event.fail.assert_called_once()


def test_on_list_backups_action_returns_table(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.backup_manager.list_backups.return_value = ["2026-05-13T10:00:00Z"]
    charm.backup_manager.format_backup_list.return_value = "table"
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    mocker.patch.object(evt, "_blocking_reason", return_value=None)

    event = mocker.MagicMock()
    event.params = {"output": "table"}
    evt._on_list_backups_action(event)
    event.set_results.assert_called_with({"backups": "table"})


def test_on_list_backups_action_returns_json(mocker):
    import json

    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.backup_manager.list_backups.return_value = [
        "2026-05-14T10:00:00Z",
        "2026-05-13T10:00:00Z",
    ]
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    mocker.patch.object(evt, "_blocking_reason", return_value=None)

    event = mocker.MagicMock()
    event.params = {"output": "json"}
    evt._on_list_backups_action(event)
    _, kwargs_or_args = event.set_results.call_args
    payload = event.set_results.call_args.args[0]["backups"]
    assert json.loads(payload) == [
        {"backup-id": "2026-05-14T10:00:00Z", "backup-status": "finished"},
        {"backup-id": "2026-05-13T10:00:00Z", "backup-status": "finished"},
    ]
    # The text formatter is not used for JSON output.
    charm.backup_manager.format_backup_list.assert_not_called()


def test_on_list_backups_action_rejects_invalid_format(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm

    event = mocker.MagicMock()
    event.params = {"output": "yaml"}
    evt._on_list_backups_action(event)
    event.fail.assert_called_once()
    assert "invalid output format" in event.fail.call_args[0][0]
    charm.backup_manager.list_backups.assert_not_called()


def test_storage_detaching_refuses_during_backup(cloud_spec):
    import pytest
    from ops import testing

    from src.charm import ValkeyCharm
    from src.literals import (
        DATA_STORAGE,
        PEER_RELATION,
        STATUS_PEERS_RELATION,
    )

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"backup_id": "2026-05-13T10:00:00Z"},
    )
    status_peer = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    storage = testing.Storage(name=DATA_STORAGE)
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd", cloud_spec=cloud_spec),
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


def test_charm_constructs_backup_manager_and_events(cloud_spec):
    from ops import testing

    from src.charm import ValkeyCharm
    from src.literals import PEER_RELATION, STATUS_PEERS_RELATION

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd", cloud_spec=cloud_spec),
        leader=True,
        relations={peer, status_peer},
        containers={testing.Container(name="valkey", can_connect=True)},
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        assert manager.charm.backup_manager.__class__.__name__ == "BackupManager"
        assert manager.charm.backup_events is not None


def test_on_list_backups_action_runs_while_a_backup_is_in_progress(mocker):
    """list-backups is read-only.

    A backup running on the unit must not block it (the in-progress check
    is create-backup only).
    """
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.state.s3_relation = mocker.MagicMock()
    charm.state.backup_relations = [mocker.MagicMock()]
    charm.state.backup_backends_conflict = False
    charm.state.active_backup_credentials = {"bucket": "b"}
    charm.workload.alive.return_value = True
    charm.state.unit_server.is_backup_in_progress = True  # backup running here
    charm.backup_manager.list_backups.return_value = []
    charm.backup_manager.format_backup_list.return_value = "No backups found."
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm

    event = mocker.MagicMock()
    event.params = {}  # default output format (table)
    evt._on_list_backups_action(event)
    event.fail.assert_not_called()
    charm.backup_manager.list_backups.assert_called_once()


def test_on_s3_credentials_gone_non_leader_does_not_clear_databag(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.state.is_backup_in_progress_any = False
    charm.state.cluster.is_restore_in_progress = False
    charm.unit.is_leader.return_value = False

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt._credentials_changed_handlers = {}
    evt._on_s3_credentials_gone(mocker.MagicMock())
    charm.backup_manager.remove_tls_ca_chain.assert_called_once()
    charm.state.cluster.update.assert_not_called()


def test_on_s3_credentials_changed_defers_without_peer_relation(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.unit.is_leader.return_value = True
    charm.state.peer_relation = None

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt.s3_requirer = mocker.MagicMock()
    evt.s3_requirer.get_storage_connection_info.return_value = {
        "bucket": "b",
        "endpoint": "e",
        "path": "p",
        "access-key": "AK",
        "secret-key": "SK",
    }

    event = mocker.MagicMock()
    evt._on_s3_credentials_changed(event)
    event.defer.assert_called_once()


def test_on_create_backup_action_rejected_when_backup_already_running(mocker):
    """The in-progress check lives in _blocking_reason (default-on for create)."""
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.state.s3_relation = mocker.MagicMock()
    charm.state.backup_relations = [mocker.MagicMock()]
    charm.state.backup_backends_conflict = False
    charm.state.active_backup_credentials = {"bucket": "b"}
    charm.workload.alive.return_value = True
    charm.state.unit_server.is_backup_in_progress = True
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm

    event = mocker.MagicMock()
    evt._on_create_backup_action(event)
    event.fail.assert_called_once()
    assert "already in progress" in event.fail.call_args[0][0]
    charm.backup_manager.create_backup.assert_not_called()


def test_get_statuses_credentials_missing(mocker):
    from src.managers.backup import BackupManager
    from src.statuses import BackupStatuses

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
    from src.managers.backup import BackupManager
    from src.statuses import BackupStatuses

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
    from src.managers.backup import BackupManager
    from src.statuses import BackupStatuses

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
    import pytest

    from common.exceptions import StorageBackendError, ValkeyBackupError
    from src.managers.backup import BackupManager

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
    import pathlib

    import yaml

    meta = yaml.safe_load(pathlib.Path("metadata.yaml").read_text())
    az = meta["requires"]["azure-credentials"]
    assert az["interface"] == "azure_storage"
    assert az["limit"] == 1
    assert az["optional"] is True


def test_azure_parameters_valid_and_normalised():
    """Trims whitespace, strips the separators that would corrupt blob keys."""
    from src.core.models import AzureStorageParameters

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
    from src.core.models import AzureStorageParameters

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
    import pytest
    from pydantic import ValidationError

    from src.core.models import AzureStorageParameters

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
    import pytest
    from pydantic import ValidationError

    from src.core.models import AzureStorageParameters

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
    from src.core.models import PeerAppModel

    fields = PeerAppModel.model_fields
    assert "azure_credentials" in fields
    assert fields["azure_credentials"].default is None


def test_cluster_azure_credentials_parses_envelope_and_defaults_none(mocker):
    """The stored envelope parses back to AzureStorageParameters; unset reads as None."""
    from src.core.models import AzureStorageParameters, ValkeyCluster

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


def test_credentials_changed_handlers_cover_every_registered_backend(mocker, cloud_spec):
    """Every registry entry has a changed handler, or its integrator is inert.

    ``_reconcile_other_backends`` and the leader_elected recovery observers are
    both generated from this table, so a missing entry silently loses a backend.
    """
    from ops import testing

    from src.charm import ValkeyCharm
    from src.literals import BACKUP_CREDENTIAL_FIELDS, PEER_RELATION, STATUS_PEERS_RELATION

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd", cloud_spec=cloud_spec),
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


def test_on_azure_credentials_changed_leader_writes_databag(mocker):
    """The Azure handler delegates the leader/conflict/validate/store half."""
    import json

    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.unit.is_leader.return_value = True
    charm.state.peer_relation = mocker.MagicMock()
    charm.state.backup_backends_conflict = False
    charm.state.is_backup_in_progress_any = False
    charm.state.cluster.is_restore_in_progress = False

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt.azure_requirer = mocker.MagicMock()
    evt.azure_requirer.get_storage_connection_info.return_value = {
        "container": " c ",
        "storage-account": "acct",
        "secret-key": "SK",
        "connection-protocol": "https",
        "path": "/valkey/",
    }

    evt._on_azure_credentials_changed(mocker.MagicMock())

    charm.backup_manager.ensure_container.assert_called_once()
    args, _ = charm.state.cluster.update.call_args
    creds = json.loads(args[0]["azure_credentials"])
    assert creds["container"] == "c"
    assert creds["path"] == "valkey"
    assert creds["storage-account"] == "acct"


def test_on_azure_credentials_changed_without_info_is_a_noop(mocker):
    """Fired on leader_elected with no Azure relation: nothing to store."""
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt.azure_requirer = mocker.MagicMock()
    evt.azure_requirer.get_storage_connection_info.return_value = {}

    evt._on_azure_credentials_changed(mocker.MagicMock())

    charm.state.cluster.update.assert_not_called()
    charm.backup_manager.ensure_container.assert_not_called()


def test_on_azure_credentials_changed_never_touches_the_s3_ca(mocker):
    """The endpoint CA is an S3-only concept; Azure must not clobber the file."""
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.unit.is_leader.return_value = False

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt.azure_requirer = mocker.MagicMock()
    evt.azure_requirer.get_storage_connection_info.return_value = {
        "container": "c",
        "storage-account": "acct",
        "secret-key": "SK",
        "connection-protocol": "https",
        "path": "valkey",
    }

    evt._on_azure_credentials_changed(mocker.MagicMock())

    charm.backup_manager.store_tls_ca_chain.assert_not_called()
    charm.backup_manager.remove_tls_ca_chain.assert_not_called()


def test_on_azure_credentials_gone_defers_during_backup(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.state.is_backup_in_progress_any = True

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    event = mocker.MagicMock()

    evt._on_azure_credentials_gone(event)

    event.defer.assert_called_once()
    charm.state.cluster.update.assert_not_called()


def test_on_azure_credentials_gone_clears_databag_and_converges_s3(mocker):
    """Removing the Azure integrator clears the conflict, so S3 gets another go."""
    from src.events.backup import BackupEvents
    from src.literals import AZURE_RELATION_NAME, S3_RELATION_NAME

    charm = mocker.MagicMock()
    charm.state.is_backup_in_progress_any = False
    charm.state.cluster.is_restore_in_progress = False
    charm.unit.is_leader.return_value = True

    s3_handler = mocker.Mock()
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt._credentials_changed_handlers = {
        S3_RELATION_NAME: s3_handler,
        AZURE_RELATION_NAME: mocker.Mock(),
    }

    evt._on_azure_credentials_gone(mocker.MagicMock())

    charm.state.cluster.update.assert_called_once_with({"azure_credentials": ""})
    # Azure carries no charm-local CA, so nothing on disk is touched.
    charm.backup_manager.remove_tls_ca_chain.assert_not_called()
    s3_handler.assert_called_once()
    evt._credentials_changed_handlers[AZURE_RELATION_NAME].assert_not_called()


def test_on_azure_credentials_gone_non_leader_does_not_clear_databag(mocker):
    from src.events.backup import BackupEvents

    charm = mocker.MagicMock()
    charm.state.is_backup_in_progress_any = False
    charm.state.cluster.is_restore_in_progress = False
    charm.unit.is_leader.return_value = False

    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt._credentials_changed_handlers = {}

    evt._on_azure_credentials_gone(mocker.MagicMock())

    charm.state.cluster.update.assert_not_called()


def test_on_s3_credentials_gone_converges_the_surviving_azure_backend(mocker):
    """The mirror case: dropping S3 must re-drive the still-related Azure backend."""
    from src.events.backup import BackupEvents
    from src.literals import AZURE_RELATION_NAME, S3_RELATION_NAME

    charm = mocker.MagicMock()
    charm.state.is_backup_in_progress_any = False
    charm.state.cluster.is_restore_in_progress = False
    charm.unit.is_leader.return_value = True

    azure_handler = mocker.Mock()
    evt = BackupEvents.__new__(BackupEvents)
    evt.charm = charm
    evt._credentials_changed_handlers = {
        S3_RELATION_NAME: mocker.Mock(),
        AZURE_RELATION_NAME: azure_handler,
    }

    evt._on_s3_credentials_gone(mocker.MagicMock())

    charm.state.cluster.update.assert_called_once_with({"s3_credentials": ""})
    azure_handler.assert_called_once()


def test_azure_parameters_rejects_an_unknown_connection_protocol():
    """Only the six documented integrator values are accepted.

    Anything else would fall through to the plaintext branch of the account URL
    and fail obscurely at request time instead of at the relation boundary.
    """
    import pytest
    from pydantic import ValidationError

    from src.core.models import AzureStorageParameters

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
    from src.core.models import AzureStorageParameters
    from src.literals import AZURE_HTTP_PROTOCOLS, AZURE_HTTPS_PROTOCOLS

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
    import pytest

    from src.managers.backup import BackupManager

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
