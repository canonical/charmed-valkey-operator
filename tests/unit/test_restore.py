#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the S3 restore feature.

Two layers, on purpose:

* The restore **state machine** and the **restore action** are exercised through
  real Juju events (``ctx.on.action`` / ``ctx.on.update_status``) via
  ``ops.testing``, so the peer-relation data-interface integration is covered
  end-to-end and a later refactor of the private handlers won't silently gut the
  tests. See the "event-driven" section.
* Managers, workloads, models, and pure helpers are unit-tested directly below
  the event layer, where that granularity belongs.
"""

import pytest

from src.core.models import PeerAppModel, PeerUnitModel
from src.literals import RestoreStep

# ── models / enums (pure) ────────────────────────────────────────────────────


def test_restore_step_order_and_values():
    assert RestoreStep.NOT_STARTED.value == ""
    assert [s.value for s in RestoreStep] == [
        "",
        "restore",
        "resync",
        "completed",
    ]


def test_new_model_fields_default_falsy():
    app = PeerAppModel()
    assert app.restore_id == ""
    assert app.restore_instruction == ""
    assert app.restore_participants == ""
    unit = PeerUnitModel()
    assert unit.restore_step == ""
    assert unit.restore_role == ""


def test_model_tolerates_missing_keys():
    # Old-revision databag (no restore_* keys) must still parse.
    app = PeerAppModel.model_validate({"start_member": "valkey/0"})
    assert app.restore_id == ""
    unit = PeerUnitModel.model_validate({"hostname": "h"})
    assert unit.restore_step == ""


def test_valkey_server_restore_step_maps_enum(mocker):
    from src.core.models import ValkeyServer

    srv = mocker.Mock()
    srv.model = mocker.Mock(restore_step="restore")
    assert ValkeyServer.restore_step.fget(srv) == RestoreStep.RESTORE
    srv.model = None
    assert ValkeyServer.restore_step.fget(srv) == RestoreStep.NOT_STARTED


def test_valkey_cluster_is_restore_in_progress(mocker):
    from src.core.models import ValkeyCluster

    cl = mocker.Mock()
    cl.model = mocker.Mock(restore_id="2026-05-13T10:00:00Z")
    assert ValkeyCluster.is_restore_in_progress.fget(cl) is True
    cl.model = mocker.Mock(restore_id="")
    assert ValkeyCluster.is_restore_in_progress.fget(cl) is False


# ── barrier / cluster-state (pure) ───────────────────────────────────────────


def test_barrier_fails_closed_on_departed_participant(mocker):
    from src.core.cluster_state import ClusterState
    from src.literals import RestoreStep

    def srv(name, step):
        return mocker.Mock(unit_name=name, restore_step=step)

    cs = mocker.Mock(spec=ClusterState)
    # Only valkey/0 and valkey/1 are live; valkey/2 departed.
    cs.servers = {
        srv("valkey/0", RestoreStep.RESTORE),
        srv("valkey/1", RestoreStep.RESTORE),
    }
    cs.cluster = mocker.Mock(
        restore_instruction=RestoreStep.RESTORE,
        restore_participants=["valkey/0", "valkey/1", "valkey/2"],
    )
    # Call the real implementation against the mock.
    assert ClusterState.can_restore_workflow_proceed.fget(cs) is False


def test_barrier_passes_when_all_participants_reached(mocker):
    from src.core.cluster_state import ClusterState
    from src.literals import RestoreStep

    def srv(name, step):
        return mocker.Mock(unit_name=name, restore_step=step)

    cs = mocker.Mock(spec=ClusterState)
    cs.servers = {srv("valkey/0", RestoreStep.RESTORE), srv("valkey/1", RestoreStep.RESTORE)}
    cs.cluster = mocker.Mock(
        restore_instruction=RestoreStep.RESTORE,
        restore_participants=["valkey/0", "valkey/1"],
    )
    assert ClusterState.can_restore_workflow_proceed.fget(cs) is True


def test_is_backup_in_progress_any_checks_all_servers(mocker):
    from src.core.cluster_state import ClusterState

    cs = mocker.Mock(spec=ClusterState)
    cs.servers = {
        mocker.Mock(is_backup_in_progress=False),
        mocker.Mock(is_backup_in_progress=True),  # a backup on a *different* unit
    }
    assert ClusterState.is_backup_in_progress_any.fget(cs) is True
    cs.servers = {mocker.Mock(is_backup_in_progress=False)}
    assert ClusterState.is_backup_in_progress_any.fget(cs) is False


def test_restore_statuses_present():
    from src.statuses import RestoreStatuses

    assert RestoreStatuses.RESTORE_IN_PROGRESS.value.status == "maintenance"
    assert RestoreStatuses.RESTORE_FAILED.value.status == "blocked"
    assert RestoreStatuses.RESTORE_UNHEALTHY.value.status == "blocked"
    assert RestoreStatuses.RESTORE_FAILED.value.running == "async"


# ── workload primitives (per-substrate) ──────────────────────────────────────


def test_workload_start_stop_alive_take_optional_service():
    import inspect

    from src.core.base_workload import WorkloadBase

    # Single-service ops are folded into start/stop/alive via an optional `service`.
    for name in ("start", "stop", "alive"):
        param = inspect.signature(getattr(WorkloadBase, name)).parameters.get("service")
        assert param is not None and param.default is None

    # Liveness verification is an explicit opt-in/out, decoupled from `service`:
    # start verifies by default, stop does not.
    assert inspect.signature(WorkloadBase.start).parameters["check_alive"].default is True
    assert inspect.signature(WorkloadBase.stop).parameters["check_alive"].default is False

    for name in ("push_data_file", "move_file"):
        assert getattr(WorkloadBase, name).__isabstractmethod__ is True


def test_vm_stop_stops_only_the_named_service(mocker):
    from src.workload_vm import ValkeyVmWorkload

    # The alive() verify is opt-in via check_alive; here only assert the targeted stop.
    mocker.patch.object(ValkeyVmWorkload, "alive", return_value=False)
    wl = ValkeyVmWorkload.__new__(ValkeyVmWorkload)
    wl.valkey = mocker.Mock()
    wl.valkey_service = "server"
    wl.stop("server")
    wl.valkey.stop.assert_called_once_with(services=["server"])


def test_k8s_start_skips_alive_check_when_disabled(mocker):
    """start(service, check_alive=False) starts the service and runs no liveness check."""
    from src.workload_k8s import ValkeyK8sWorkload

    wl = ValkeyK8sWorkload.__new__(ValkeyK8sWorkload)
    wl.container = mocker.Mock()
    wl.valkey_service = "valkey"
    alive = mocker.patch.object(ValkeyK8sWorkload, "alive")

    wl.start("valkey", check_alive=False)

    wl.container.start.assert_called_once_with("valkey")
    alive.assert_not_called()


def test_k8s_stop_verifies_down_only_when_check_alive(mocker):
    """Verify a service's down-check runs only when check_alive is set."""
    from common.exceptions import ValkeyServicesCouldNotBeStoppedError
    from src.workload_k8s import ValkeyK8sWorkload

    wl = ValkeyK8sWorkload.__new__(ValkeyK8sWorkload)
    wl.container = mocker.Mock()
    wl.valkey_service = "valkey"
    mocker.patch.object(ValkeyK8sWorkload, "alive", return_value=True)  # still up after stop

    wl.stop("valkey")  # default check_alive=False: no verify, tolerated
    with pytest.raises(ValkeyServicesCouldNotBeStoppedError):
        wl.stop("valkey", check_alive=True)  # verify catches the still-running service


def test_vm_stop_verifies_down_only_when_check_alive(mocker):
    """VM stop verifies the service went down only when check_alive is set."""
    from common.exceptions import ValkeyServicesCouldNotBeStoppedError
    from src.workload_vm import ValkeyVmWorkload

    wl = ValkeyVmWorkload.__new__(ValkeyVmWorkload)
    wl.valkey = mocker.Mock()
    wl.valkey_service = "server"
    mocker.patch.object(ValkeyVmWorkload, "alive", return_value=True)  # still up after stop

    wl.stop("server")  # default check_alive=False: no verify, tolerated
    with pytest.raises(ValkeyServicesCouldNotBeStoppedError):
        wl.stop("server", check_alive=True)


def test_k8s_move_file_uses_container_exec(mocker):
    from src.workload_k8s import ValkeyK8sWorkload

    wl = ValkeyK8sWorkload.__new__(ValkeyK8sWorkload)
    wl.container = mocker.Mock()
    src = mocker.Mock(as_posix=lambda: "/var/lib/valkey/dump.rdb")
    dest = mocker.Mock(as_posix=lambda: "/var/lib/valkey/dump.rdb.pre-restore")
    wl.move_file(src, dest)
    wl.container.exec.assert_called_once()
    args = wl.container.exec.call_args.kwargs["command"]
    assert args == ["mv", "/var/lib/valkey/dump.rdb", "/var/lib/valkey/dump.rdb.pre-restore"]


def test_vm_move_file_uses_shutil_move_for_cross_device(mocker):
    """VM move_file uses shutil.move so cross-partition (data<->archive) moves work."""
    from src.workload_vm import ValkeyVmWorkload

    wl = ValkeyVmWorkload.__new__(ValkeyVmWorkload)
    move = mocker.patch("workload_vm.shutil.move")
    src = mocker.Mock(as_posix=lambda: "/data/dump.rdb")
    dest = mocker.Mock(as_posix=lambda: "/archive/dump.rdb.pre-restore")
    wl.move_file(src, dest)
    move.assert_called_once_with("/data/dump.rdb", "/archive/dump.rdb.pre-restore")


# ── sentinel manager ─────────────────────────────────────────────────────────


def test_suppress_and_resume_failover_iterate_all_sentinels(mocker):
    from src.literals import (
        PRIMARY_NAME,
        SENTINEL_DOWN_AFTER_MS,
        SENTINEL_DOWN_AFTER_SUPPRESSED_MS,
    )
    from src.managers.sentinel import SentinelManager

    mgr = SentinelManager.__new__(SentinelManager)
    client = mocker.Mock()
    mocker.patch.object(mgr, "_get_sentinel_client", return_value=client)
    mocker.patch.object(mgr, "all_sentinel_endpoints", return_value=["10.0.0.1", "10.0.0.2"])

    mgr.suppress_failover()
    client.set.assert_any_call(
        "10.0.0.1", PRIMARY_NAME, "down-after-milliseconds", str(SENTINEL_DOWN_AFTER_SUPPRESSED_MS)
    )
    client.set.assert_any_call(
        "10.0.0.2", PRIMARY_NAME, "down-after-milliseconds", str(SENTINEL_DOWN_AFTER_SUPPRESSED_MS)
    )

    client.reset_mock()
    mgr.resume_failover()
    client.set.assert_any_call(
        "10.0.0.1", PRIMARY_NAME, "down-after-milliseconds", str(SENTINEL_DOWN_AFTER_MS)
    )
    client.reset.assert_any_call(hostname="10.0.0.2")


def test_suppress_failover_raises_when_set_reports_non_ok(mocker):
    """A non-OK SENTINEL SET must fail-closed BEFORE the primary is stopped.

    Otherwise that sentinel stays at the normal down-after and could promote a
    replica while the primary is stopped for the restore.
    """
    from common.exceptions import SentinelFailoverError
    from src.managers.sentinel import SentinelManager

    mgr = SentinelManager.__new__(SentinelManager)
    client = mocker.Mock()
    client.set.return_value = False  # non-OK reply, no exception raised
    mocker.patch.object(mgr, "_get_sentinel_client", return_value=client)
    mocker.patch.object(mgr, "all_sentinel_endpoints", return_value=["10.0.0.1"])

    with pytest.raises(SentinelFailoverError):
        mgr.suppress_failover()


def test_resume_failover_best_effort_on_non_ok_set(mocker, caplog):
    """resume_failover must NOT raise on a non-OK SET (raising would re-wedge teardown).

    It logs the degraded endpoint and still issues SENTINEL RESET.
    """
    import logging

    from src.managers.sentinel import SentinelManager

    mgr = SentinelManager.__new__(SentinelManager)
    client = mocker.Mock()
    client.set.return_value = False
    mocker.patch.object(mgr, "_get_sentinel_client", return_value=client)
    mocker.patch.object(mgr, "all_sentinel_endpoints", return_value=["10.0.0.1"])

    with caplog.at_level(logging.WARNING):
        mgr.resume_failover()  # must not raise

    client.reset.assert_called_once_with(hostname="10.0.0.1")  # RESET still runs
    assert "10.0.0.1" in caplog.text  # the non-OK endpoint was logged


def test_is_failover_suppressed_reads_local_sentinel_down_after(mocker):
    """is_failover_suppressed is a local read: this unit's sentinel, suppressed value only."""
    from src.literals import SENTINEL_DOWN_AFTER_MS, SENTINEL_DOWN_AFTER_SUPPRESSED_MS
    from src.managers.sentinel import SentinelManager

    mgr = SentinelManager.__new__(SentinelManager)
    mgr.state = mocker.Mock(endpoint="10.0.0.1")
    client = mocker.Mock()
    mocker.patch.object(mgr, "_get_sentinel_client", return_value=client)

    client.primary.return_value = {
        "down-after-milliseconds": str(SENTINEL_DOWN_AFTER_SUPPRESSED_MS)
    }
    assert mgr.is_failover_suppressed() is True
    client.primary.assert_called_once_with(hostname="10.0.0.1")

    client.primary.return_value = {"down-after-milliseconds": str(SENTINEL_DOWN_AFTER_MS)}
    assert mgr.is_failover_suppressed() is False
    client.primary.return_value = {}  # field missing: not suppressed
    assert mgr.is_failover_suppressed() is False


def test_resume_local_failover_targets_only_this_sentinel(mocker):
    """resume_local_failover resets + RESETs this unit's sentinel and no other."""
    from src.literals import PRIMARY_NAME, SENTINEL_DOWN_AFTER_MS
    from src.managers.sentinel import SentinelManager

    mgr = SentinelManager.__new__(SentinelManager)
    mgr.state = mocker.Mock(endpoint="10.0.0.1")
    client = mocker.Mock()
    mocker.patch.object(mgr, "_get_sentinel_client", return_value=client)
    all_endpoints = mocker.patch.object(
        mgr, "all_sentinel_endpoints", return_value=["10.0.0.1", "10.0.0.2"]
    )

    mgr.resume_local_failover()

    client.set.assert_called_once_with(
        "10.0.0.1", PRIMARY_NAME, "down-after-milliseconds", str(SENTINEL_DOWN_AFTER_MS)
    )
    client.reset.assert_called_once_with(hostname="10.0.0.1")
    all_endpoints.assert_not_called()


def test_reconcile_failover_suppression_is_a_manager_op(mocker, caplog):
    """The self-heal lives in the sentinel manager: resume only a still-suppressed sentinel."""
    import logging

    from common.exceptions import ValkeyWorkloadCommandError
    from src.managers.sentinel import SentinelManager

    mgr = SentinelManager.__new__(SentinelManager)
    suppressed = mocker.patch.object(mgr, "is_failover_suppressed", return_value=False)
    resume = mocker.patch.object(mgr, "resume_local_failover")

    mgr.reconcile_failover_suppression()
    resume.assert_not_called()

    suppressed.return_value = True
    with caplog.at_level(logging.WARNING):
        mgr.reconcile_failover_suppression()
    resume.assert_called_once()
    assert "suppression_leak" in caplog.text

    # An unreachable sentinel is skipped (retried next update-status), never a crash.
    resume.reset_mock()
    suppressed.side_effect = ValkeyWorkloadCommandError("down")
    mgr.reconcile_failover_suppression()  # must not raise
    resume.assert_not_called()


def test_sentinel_is_failover_in_progress_reads_flags(mocker):
    """Manager helper reports failover from the primary flags with no retry/blocking."""
    from src.managers.sentinel import SentinelManager

    mgr = SentinelManager.__new__(SentinelManager)
    mgr.state = mocker.Mock(endpoint="10.0.0.1")
    client = mocker.Mock()
    mocker.patch.object(mgr, "_get_sentinel_client", return_value=client)

    client.primary.return_value = {"flags": "master,failover_in_progress"}
    assert mgr.is_failover_in_progress() is True

    client.primary.return_value = {"flags": "master"}
    assert mgr.is_failover_in_progress() is False


def test_failover_suppression_logs_in_the_manager(mocker, caplog):
    """suppress/resume log their own effect, so the event handler doesn't have to.

    The log line belongs next to the work it describes.
    """
    import logging

    from src.managers.sentinel import SentinelManager

    mgr = SentinelManager.__new__(SentinelManager)
    client = mocker.Mock()
    client.set.return_value = True
    mocker.patch.object(mgr, "_get_sentinel_client", return_value=client)
    mocker.patch.object(mgr, "all_sentinel_endpoints", return_value=["10.0.0.1"])

    with caplog.at_level(logging.INFO):
        mgr.suppress_failover()
        mgr.resume_failover()

    assert "restore.failover_suppressed" in caplog.text
    assert "restore.failover_resumed" in caplog.text


# ── backup manager: download / verify / restore primitives ───────────────────


def test_download_backup_streams_body_to_data_partition_and_moves_atomically(mocker, caplog):
    """The full RDB streams straight to the data partition; no whole-object charm buffer.

    The magic header is validated up front by verify_backup_is_rdb (a tiny ranged
    read), so download_backup itself just streams the backend's body onto disk.
    """
    import logging

    from common.storage_backend import RemoteObject
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.state = mocker.MagicMock()
    mgr.workload = mocker.Mock()
    mgr.workload.working_dir = mocker.MagicMock()  # download lands on the data partition

    body = mocker.Mock()  # the backend's streaming body
    backend = mocker.MagicMock()
    backend.download.return_value = RemoteObject(body, 4096)
    mocker.patch.object(BackupManager, "storage_backend", backend)

    with caplog.at_level(logging.INFO):
        mgr.download_backup("2026-05-13T10:00:00Z")

    # Fetched the object by backup-id and pushed the streaming body straight
    # through -- the pushed source is the backend's body, not a charm-local file.
    backend.download.assert_called_once_with("2026-05-13T10:00:00Z")
    assert mgr.workload.push_data_file.call_args.args[0] is body
    # Then atomically promoted the .part file onto the final name.
    assert mgr.workload.move_file.called
    # The size the backend reported rides along into the operator-facing trail.
    assert "bytes=4096" in caplog.text


def test_verify_backup_is_rdb_accepts_valid_head(mocker):
    """Pre-stop check reads only the head, not the whole object, and passes RDB magic."""
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.state = mocker.MagicMock()
    backend = mocker.MagicMock()
    backend.head.return_value = b"REDIS0011"
    mocker.patch.object(BackupManager, "storage_backend", backend)

    mgr.verify_backup_is_rdb("2026-05-13T10:00:00Z")  # no raise

    # the head, never a download
    backend.head.assert_called_once_with("2026-05-13T10:00:00Z")
    backend.download.assert_not_called()


def test_restore_reads_wrap_backend_errors_and_keep_the_code(mocker):
    """verify/download turn a backend failure into a restore error, code intact.

    The code is what the restore action puts in its (world-readable) result, so
    it has to survive both hops -- backend to manager, manager to action.
    """
    from common.exceptions import StorageBackendError, ValkeyRestoreError
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.state = mocker.MagicMock()
    mgr.workload = mocker.MagicMock()
    backend = mocker.MagicMock()
    backend.head.side_effect = StorageBackendError("x", safe_code="NoSuchKey")
    backend.download.side_effect = StorageBackendError("x", safe_code="AccessDenied")
    mocker.patch.object(BackupManager, "storage_backend", backend)

    with pytest.raises(ValkeyRestoreError) as excinfo:
        mgr.verify_backup_is_rdb("2026-05-13T10:00:00Z")
    assert excinfo.value.safe_code == "NoSuchKey"

    with pytest.raises(ValkeyRestoreError) as excinfo:
        mgr.download_backup("2026-05-13T10:00:00Z")
    assert excinfo.value.safe_code == "AccessDenied"


def test_restore_reads_refuse_to_run_without_credentials(mocker):
    """No related backup backend: fail the restore rather than reach for a client."""
    from common.exceptions import ValkeyRestoreError
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.state = mocker.MagicMock()
    mgr.state.active_backup_credentials = None
    mgr.workload = mocker.MagicMock()

    with pytest.raises(ValkeyRestoreError):
        mgr.verify_backup_is_rdb("2026-05-13T10:00:00Z")
    with pytest.raises(ValkeyRestoreError):
        mgr.download_backup("2026-05-13T10:00:00Z")


def test_verify_backup_is_rdb_rejects_bad_head(mocker):
    """A non-RDB object is rejected by the pre-stop check (before valkey is touched)."""
    from common.exceptions import ValkeyRestoreError
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.state = mocker.MagicMock()
    backend = mocker.MagicMock()
    backend.head.return_value = b"NOT-AN-RDB.."
    mocker.patch.object(BackupManager, "storage_backend", backend)

    with pytest.raises(ValkeyRestoreError):
        mgr.verify_backup_is_rdb("2026-05-13T10:00:00Z")


def test_restore_files_on_correct_partitions(mocker):
    """Only the rollback copy is on archive; the dump and its download temp stay on data."""
    import pathlib

    from src.literals import PRE_RESTORE_SUFFIX
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.workload = mocker.Mock()
    mgr.workload.working_dir = pathlib.PurePosixPath("/data")
    mgr.workload.archive_dir = pathlib.PurePosixPath("/archive")

    # New dump is downloaded straight onto the data partition (single write, no
    # cross-device install); only the pre-restore rollback copy goes to archive.
    assert str(mgr._dump_path) == "/data/dump.rdb"
    assert str(mgr._dump_tmp_path) == "/data/dump.rdb.part"
    assert str(mgr._pre_restore_path) == f"/archive/dump.rdb{PRE_RESTORE_SUFFIX}"


def test_next_restore_step():
    from src.literals import RestoreStep
    from src.managers.backup import BackupManager

    assert BackupManager.next_restore_step(RestoreStep.NOT_STARTED) == RestoreStep.RESTORE
    assert BackupManager.next_restore_step(RestoreStep.RESTORE) == RestoreStep.RESYNC
    assert BackupManager.next_restore_step(RestoreStep.RESYNC) == RestoreStep.COMPLETED


def test_set_restore_step_logs_the_completed_step(mocker, caplog):
    """The step-completed line lives in the manager that writes the databag.

    The log belongs next to the work, and the unit name is dropped -- Juju
    already prefixes every log line with it.
    """
    import logging

    from src.literals import RestoreStep
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.state = mocker.Mock()

    with caplog.at_level(logging.INFO):
        mgr.set_restore_step(RestoreStep.RESYNC)

    mgr.state.unit_server.update.assert_called_once_with({"restore_step": "resync"})
    assert "restore.step_done step=resync" in caplog.text
    assert "unit=" not in caplog.text


def test_restore_on_primary_orders_stop_backup_download_start(mocker):
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.state = mocker.Mock()
    mgr.workload = mocker.Mock(valkey_service="valkey", working_dir=mocker.Mock())
    mgr.workload.path_exists.return_value = False  # no existing pre-restore -> move-aside runs
    calls = []
    mgr.workload.stop.side_effect = lambda s, check_alive=False: calls.append("stop")
    mgr.workload.move_file.side_effect = lambda a, b: calls.append("move")  # dump -> pre-restore
    mgr.workload.start.side_effect = lambda s, check_alive=True: calls.append("start")
    mocker.patch.object(mgr, "download_backup", side_effect=lambda bid: calls.append("download"))
    mocker.patch.object(BackupManager, "_dump_path", new_callable=mocker.PropertyMock)
    mocker.patch.object(BackupManager, "_pre_restore_path", new_callable=mocker.PropertyMock)

    mgr.restore_on_primary()

    # back up the current dump first, download the new one onto the freed data
    # partition, then start -- the download must land after the move-aside. The
    # health wait is now the caller's job (cluster_manager.wait_until_loaded).
    assert calls == ["stop", "move", "download", "start"]


def test_restore_on_primary_drops_stale_copy(mocker):
    """A leftover pre-restore copy from a PRIOR restore is dropped before capturing this one.

    restore_on_primary only ever runs for a fresh swap (a redelivered mid-swap is
    rolled back upstream), so any existing copy is stale: it must be removed so the
    move-aside captures THIS restore's dump as the rollback, not a prior restore's.
    """
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.state = mocker.Mock()
    mgr.workload = mocker.Mock(valkey_service="valkey")
    mgr.workload.alive.return_value = True  # fresh swap: valkey is up
    mgr.workload.path_exists.return_value = True  # a stale copy from a prior restore exists

    dump = mocker.Mock()
    pre = mocker.Mock()
    mocker.patch.object(
        BackupManager, "_dump_path", new_callable=mocker.PropertyMock, return_value=dump
    )
    mocker.patch.object(
        BackupManager, "_pre_restore_path", new_callable=mocker.PropertyMock, return_value=pre
    )
    mocker.patch.object(mgr, "download_backup")

    mgr.restore_on_primary()

    mgr.workload.remove_file.assert_any_call(pre)  # stale copy dropped
    mgr.workload.move_file.assert_called_once_with(dump, pre)  # this dump captured as rollback
    mgr.download_backup.assert_called_once()


def test_roll_back_stops_service_before_swap(mocker):
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.workload = mocker.Mock(valkey_service="valkey")
    mgr.workload.alive.return_value = True  # server up -> _ensure_stopped stops it
    calls = []
    mgr.workload.stop.side_effect = lambda s, check_alive=False: calls.append("stop")
    mgr.workload.move_file.side_effect = lambda a, b: calls.append("move")
    mgr.workload.start.side_effect = lambda s, check_alive=True: calls.append("start")
    mocker.patch.object(BackupManager, "_dump_path", new_callable=mocker.PropertyMock)
    mocker.patch.object(BackupManager, "_pre_restore_path", new_callable=mocker.PropertyMock)
    tmp = mocker.patch.object(
        BackupManager, "_dump_tmp_path", new_callable=mocker.PropertyMock
    ).return_value
    mgr.workload.path_exists.return_value = True

    mgr.roll_back()
    assert calls == ["stop", "move", "start"]
    # A partial download left on the data partition is dropped during rollback.
    mgr.workload.remove_file.assert_called_once_with(tmp)


def test_roll_back_tolerates_already_stopped_service(mocker):
    """On a rollback after a crash mid-download the server is already down; don't re-stop.

    On K8s stopping an already-stopped service errors, so roll_back must
    skip the stop when the server isn't alive (the leading-stop guard).
    """
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.workload = mocker.Mock(valkey_service="valkey")
    mgr.workload.alive.return_value = False  # already stopped (mid-restore crash)
    mocker.patch.object(BackupManager, "_dump_path", new_callable=mocker.PropertyMock)
    mocker.patch.object(BackupManager, "_pre_restore_path", new_callable=mocker.PropertyMock)
    mocker.patch.object(BackupManager, "_dump_tmp_path", new_callable=mocker.PropertyMock)
    mgr.workload.path_exists.return_value = True

    mgr.roll_back()

    mgr.workload.stop.assert_not_called()  # guarded: server already down
    mgr.workload.start.assert_called_once()  # still restarted on the rollback copy


def test_restore_on_primary_stops_running_valkey_via_single_service_check(mocker):
    """The stop gate must check the SPECIFIC valkey service, not all-services alive().

    On K8s the all-services alive() is False when a sibling (e.g. the metrics
    exporter) is down; a gate on alive() would SKIP stopping a still-running
    valkey and swap the dump under a live server -> a silent no-op restore. So
    the gate uses the single-service alive(valkey_service): valkey running but
    the all-services check False must still stop valkey before the swap.
    """
    from src.managers.backup import BackupManager

    mgr = BackupManager.__new__(BackupManager)
    mgr.state = mocker.Mock()
    mgr.workload = mocker.Mock(valkey_service="valkey")
    # Single-service alive(valkey) is True; the all-services alive() would be False.
    mgr.workload.alive.side_effect = lambda service=None: service == "valkey"
    mgr.workload.path_exists.return_value = False  # no stale/existing pre-restore copy
    mocker.patch.object(BackupManager, "_dump_path", new_callable=mocker.PropertyMock)
    mocker.patch.object(BackupManager, "_pre_restore_path", new_callable=mocker.PropertyMock)
    mocker.patch.object(mgr, "download_backup")

    mgr.restore_on_primary()

    # NOT skipped; opts into the confirm-stopped verify before the RDB swap.
    mgr.workload.stop.assert_called_once_with("valkey", check_alive=True)
    mgr.workload.alive.assert_called_once_with("valkey")  # single-service gate, never alive()


def test_get_statuses_reports_restore_in_progress(mocker):
    from src.managers.backup import BackupManager
    from src.statuses import RestoreStatuses

    mgr = BackupManager.__new__(BackupManager)
    mgr.name = "backup"
    mgr.state = mocker.Mock()
    mgr.state.statuses.get.return_value.root = []
    mgr.state.cluster.is_restore_in_progress = True
    mgr.state.s3_relation = None
    assert RestoreStatuses.RESTORE_IN_PROGRESS.value in mgr.get_statuses(scope="app")


def test_valkey_server_is_tls_transitioning(mocker):
    from src.core.models import ValkeyServer
    from src.literals import TLSCARotationState, TLSState

    def srv(client_state, rotation):
        # Real ValkeyServer so the nested tls_client_state/tls_ca_rotation_state
        # properties actually compute from the model.
        s = ValkeyServer.__new__(ValkeyServer)
        s.model = mocker.Mock(tls_client_state=client_state.value, tls_ca_rotation=rotation.value)
        return s

    assert srv(TLSState.TLS, TLSCARotationState.NO_ROTATION).is_tls_transitioning is False
    assert srv(TLSState.TO_TLS, TLSCARotationState.NO_ROTATION).is_tls_transitioning is True
    assert srv(TLSState.TO_NO_TLS, TLSCARotationState.NO_ROTATION).is_tls_transitioning is True
    assert srv(TLSState.TLS, TLSCARotationState.NEW_CA_DETECTED).is_tls_transitioning is True


def test_cluster_is_tls_transitioning_aggregates_servers(mocker):
    from src.core.cluster_state import ClusterState

    cs = mocker.Mock(spec=ClusterState)
    cs.servers = {mocker.Mock(is_tls_transitioning=False), mocker.Mock(is_tls_transitioning=True)}
    assert ClusterState.is_tls_transitioning.fget(cs) is True
    cs.servers = {mocker.Mock(is_tls_transitioning=False)}
    assert ClusterState.is_tls_transitioning.fget(cs) is False


def _passing_restore_guard(mocker):
    """Return a BackupEvents whose _restore_blocking_reason returns None (all gates pass)."""
    from src.events.backup import BackupEvents

    ev = BackupEvents.__new__(BackupEvents)
    ev.charm = mocker.Mock()
    ev.charm.backup_manager.list_backups.return_value = ["b1"]
    ev.charm.unit.is_leader.return_value = True
    ev.charm.state.s3_relation = True
    ev.charm.state.backup_relations = [True]
    ev.charm.state.backup_backends_conflict = False
    ev.charm.state.active_backup_credentials = True
    ev.charm.state.is_backup_in_progress_any = False
    ev.charm.state.cluster.is_restore_in_progress = False
    ev.charm.state.is_tls_transitioning = False
    ev.charm.state.servers = [mocker.Mock(is_active=True)]
    ev.charm.sentinel_manager.get_primary_ip.return_value = "10.0.0.1"
    ev.charm.sentinel_manager.is_failover_in_progress.return_value = False
    return ev


def test_restore_guard_passes_when_all_gates_ok(mocker):
    assert _passing_restore_guard(mocker)._restore_blocking_reason("b1") is None


def test_restore_blocked_during_tls_transition(mocker):
    """A restore restarts the primary; refuse to start one mid-TLS-transition."""
    ev = _passing_restore_guard(mocker)
    ev.charm.state.is_tls_transitioning = True
    reason = ev._restore_blocking_reason("b1")
    assert reason is not None and "tls" in reason.lower()


def test_restore_blocked_gracefully_when_sentinel_query_errors(mocker):
    """A Sentinel command error during preflight fails the restore cleanly, not with a crash."""
    from common.exceptions import ValkeyWorkloadCommandError

    ev = _passing_restore_guard(mocker)
    ev.charm.sentinel_manager.is_failover_in_progress.side_effect = ValkeyWorkloadCommandError(
        "sentinel unreachable"
    )
    reason = ev._restore_blocking_reason("b1")  # must NOT raise
    assert reason is not None


def test_restore_guard_covers_the_backup_id_checks(mocker):
    """The backup-id checks are gates like any other, ordered after the cheap ones."""
    ev = _passing_restore_guard(mocker)

    assert "backup-id" in (ev._restore_blocking_reason("") or "")
    ev.charm.backup_manager.list_backups.assert_not_called()  # cheap gates first

    reason = ev._restore_blocking_reason("nope")
    assert reason is not None and "not found" in reason


def test_restore_guard_blocks_gracefully_when_listing_fails(mocker):
    """An S3 listing error blocks the restore with a safe reason, not a traceback at the user."""
    from common.exceptions import ValkeyBackupError

    ev = _passing_restore_guard(mocker)
    ev.charm.backup_manager.list_backups.side_effect = ValkeyBackupError("bucket on fire")

    reason = ev._restore_blocking_reason("b1")  # must NOT raise

    assert reason is not None and "list backups" in reason
    assert "bucket on fire" not in reason  # detail goes to the unit log only


def test_credentials_rotation_defers_during_restore(mocker):
    """A real S3 credentials rotation mid-restore is deferred, not applied.

    Swapping the bucket/creds while a restore is downloading from them would break
    it; the CA is still stored (the in-flight restore needs it), only the
    bucket/credential update waits until the restore finishes.
    """
    from src.events.backup import BackupEvents

    ev = BackupEvents.__new__(BackupEvents)
    ev.charm = mocker.Mock()
    ev.s3_requirer = mocker.Mock()
    ev.s3_requirer.get_storage_connection_info.return_value = {"bucket": "b", "access-key": "k"}
    mocker.patch("events.backup.S3Parameters.model_validate", return_value=mocker.Mock())
    ev.charm.unit.is_leader.return_value = True
    ev.charm.state.peer_relation = True
    ev.charm.state.backup_backends_conflict = False
    ev.charm.state.cluster.s3_credentials = None  # nothing stored yet → a real change
    ev.charm.state.is_backup_in_progress_any = False
    ev.charm.state.cluster.is_restore_in_progress = True
    event = mocker.Mock()

    ev._on_s3_credentials_changed(event)

    event.defer.assert_called_once()
    ev.charm.backup_manager.ensure_container.assert_not_called()
    ev.charm.backup_manager.store_tls_ca_chain.assert_called_once()  # CA still stored


def test_blocking_reason_blocks_backup_during_restore(mocker):
    """A restore in progress blocks create-backup but never the read-only list-backups."""
    from src.events.backup import BackupEvents

    ev = BackupEvents.__new__(BackupEvents)
    ev.charm = mocker.Mock()
    ev.charm.state.s3_relation = True
    ev.charm.state.backup_relations = [True]
    ev.charm.state.backup_backends_conflict = False
    ev.charm.state.active_backup_credentials = True
    ev.charm.workload.alive.return_value = True
    ev.charm.state.unit_server.is_backup_in_progress = False
    ev.charm.state.cluster.is_restore_in_progress = True
    # create-backup (check_running_operations=True) is blocked...
    assert ev._blocking_reason(check_running_operations=True) is not None
    # ...but list-backups (False) is NOT blocked by a restore.
    assert ev._blocking_reason(check_running_operations=False) is None


# ── event-driven (ops.testing / Scenario) ────────────────────────────────────
#
# The restore action + state machine are driven through real Juju events so the
# peer-relation data-interface wiring is part of the test.


def _restore_context_and_state(
    cloud_spec, *, leader=True, app_data=None, unit_data=None, peers_data=None
):
    """Build a Context + State wired for the restore workflow.

    ``peers_data`` (``{unit_id: {hyphen-keyed databag}}``) adds peer units, so a
    multi-unit restore (e.g. the leader observing a *peer's* failure) can be
    driven through real events.
    """
    from ops import testing

    from src.charm import ValkeyCharm
    from src.literals import PEER_RELATION, S3_RELATION_NAME, STATUS_PEERS_RELATION

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    # PeerModel serializes with hyphenated aliases, so app_data/unit_data keys
    # (and the reads below) use hyphens.
    peer = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data=app_data or {},
        local_unit_data={"start-state": "started", **(unit_data or {})},
        peers_data=peers_data or {},
    )
    status_peer = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    s3_rel = testing.Relation(
        id=3, endpoint=S3_RELATION_NAME, interface="s3", remote_app_name="s3-integrator"
    )
    state = testing.State(
        model=testing.Model(name="m", type="lxd", cloud_spec=cloud_spec),
        leader=leader,
        relations={peer, status_peer, s3_rel},
        containers={testing.Container(name="valkey", can_connect=True)},
    )
    return ctx, state


def _peer_app_data(state):
    from src.literals import PEER_RELATION

    return next(r for r in state.relations if r.endpoint == PEER_RELATION).local_app_data


def _peer_unit_data(state):
    from src.literals import PEER_RELATION

    return next(r for r in state.relations if r.endpoint == PEER_RELATION).local_unit_data


def _drive_restore(ctx, state, *, capture_statuses=False):
    """Drive the restore workflow to a fixed point across hooks.

    The workflow advances one step per hook (no in-hook loop): in
    real Juju each leader app-databag write re-delivers relation_changed, with
    update_status as a backstop. ops.testing emits one event and can't model a
    peer relation_changed for the leader's own app-data write (no remote unit),
    so we drive the equivalent update_status backstop until the peer databags
    settle. ``capture_statuses`` also returns the backup component's running
    statuses from the final hook.
    """
    statuses = None
    for _ in range(10):  # generous; single-unit convergence is ~4 hooks
        before = (dict(_peer_app_data(state)), dict(_peer_unit_data(state)))
        with ctx(ctx.on.update_status(), state) as mgr:
            state = mgr.run()
            if capture_statuses:
                statuses = mgr.charm.state.statuses.get(
                    scope="app", component="backup", running_status_only=True
                ).root
        after = (dict(_peer_app_data(state)), dict(_peer_unit_data(state)))
        if before == after:
            break
    return (state, statuses) if capture_statuses else state


@pytest.fixture
def restore_managers(mocker):
    """Patch every manager op the restore workflow drives, returning the mocks.

    These are the external/infra effects (Valkey/Sentinel/S3/workload). Patching
    them lets the Scenario tests assert orchestration and the resulting peer
    databag without a live cluster. ``is_primary`` defaults to True (this unit is
    the primary); flip it per-test for the replica branch.
    """
    from types import SimpleNamespace

    return SimpleNamespace(
        is_primary=mocker.patch("managers.cluster.ClusterManager.is_primary", return_value=True),
        has_pre_restore_copy=mocker.patch(
            "managers.backup.BackupManager.has_pre_restore_copy", return_value=False
        ),
        wait_until_loaded=mocker.patch("managers.cluster.ClusterManager.wait_until_loaded"),
        wait_until_resynced=mocker.patch("managers.cluster.ClusterManager.wait_until_resynced"),
        verify_backup_is_rdb=mocker.patch("managers.backup.BackupManager.verify_backup_is_rdb"),
        restore_on_primary=mocker.patch("managers.backup.BackupManager.restore_on_primary"),
        download_backup=mocker.patch("managers.backup.BackupManager.download_backup"),
        cleanup_restore_files=mocker.patch("managers.backup.BackupManager.cleanup_restore_files"),
        roll_back=mocker.patch("managers.backup.BackupManager.roll_back"),
        suppress_failover=mocker.patch("managers.sentinel.SentinelManager.suppress_failover"),
        resume_failover=mocker.patch("managers.sentinel.SentinelManager.resume_failover"),
        is_failover_suppressed=mocker.patch(
            "managers.sentinel.SentinelManager.is_failover_suppressed", return_value=False
        ),
        resume_local_failover=mocker.patch(
            "managers.sentinel.SentinelManager.resume_local_failover"
        ),
        save_dataset_before_shutdown=mocker.patch(
            "managers.cluster.ClusterManager.save_dataset_before_shutdown"
        ),
        reconcile_min_replicas_to_write=mocker.patch(
            "managers.cluster.ClusterManager.reconcile_min_replicas_to_write"
        ),
    )


def _pass_restore_preconditions(mocker, backups):
    """Satisfy the gates in ``_restore_blocking_reason``.

    ``s3_credentials`` is an ``ExtraSecretStr`` routed through a Juju secret
    (only presence is gated on here), so it's patched at the property rather than
    forged into the databag.
    """
    from unittest.mock import PropertyMock

    mocker.patch(
        "core.models.ValkeyCluster.s3_credentials",
        new_callable=PropertyMock,
        return_value=object(),  # truthy; the credentials themselves are unused here
    )
    mocker.patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="10.0.0.1")
    mocker.patch("managers.sentinel.SentinelManager.is_failover_in_progress", return_value=False)
    mocker.patch("managers.backup.BackupManager.list_backups", return_value=backups)


# ── restore action ───────────────────────────────────────────────────────────


def test_restore_action_initiates_workflow(mocker, cloud_spec, restore_managers):
    """`restore` on the leader validates, then writes the workflow's app-databag target."""
    from src.literals import RestoreStep

    _pass_restore_preconditions(mocker, ["2026-05-13T10:00:00Z"])
    ctx, state = _restore_context_and_state(cloud_spec)

    state_out = ctx.run(
        ctx.on.action("restore", params={"backup-id": "2026-05-13T10:00:00Z"}), state
    )

    app = _peer_app_data(state_out)
    assert app["restore-id"] == "2026-05-13T10:00:00Z"
    assert app["restore-instruction"] == RestoreStep.RESTORE.value
    assert app["restore-participants"] == "valkey/0"
    assert ctx.action_results == {"restore": "initiated for 2026-05-13T10:00:00Z"}
    # The action only *initiates*; the destructive work is the async workflow.
    restore_managers.restore_on_primary.assert_not_called()


def test_restore_action_logs_every_rejection(mocker, cloud_spec, restore_managers, caplog):
    """Every rejected restore action leaves a traceable log line, not just a failed task.

    The action result is transient; the unit log must show why a restore
    didn't start.
    """
    import logging

    from ops.testing import ActionFailed

    _pass_restore_preconditions(mocker, ["2026-05-13T10:00:00Z"])
    ctx, state = _restore_context_and_state(cloud_spec)

    with caplog.at_level(logging.WARNING), pytest.raises(ActionFailed):
        ctx.run(ctx.on.action("restore", params={"backup-id": "2026-01-01T00:00:00Z"}), state)
    assert "restore.rejected" in caplog.text
    assert "2026-01-01T00:00:00Z" in caplog.text  # the unknown backup-id is in the log

    caplog.clear()
    ctx, state = _restore_context_and_state(cloud_spec, leader=False)
    with caplog.at_level(logging.WARNING), pytest.raises(ActionFailed):
        ctx.run(ctx.on.action("restore", params={"backup-id": "2026-05-13T10:00:00Z"}), state)
    assert "restore.rejected" in caplog.text
    assert "leader" in caplog.text


def test_restore_workflow_logs_each_transition(cloud_spec, restore_managers, caplog):
    """A full restore leaves a step-by-step trail in the unit log (review: hard to follow)."""
    import logging

    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    with caplog.at_level(logging.INFO):
        _drive_restore(ctx, state)

    for marker in (
        "restore.step",  # a step ran on this unit
        "role=primary",
        "restore.advance",  # the leader moved the barrier
        "restore.completed",
    ):
        assert marker in caplog.text, f"missing log marker {marker!r}"
    # The event handler keeps only the state-machine trail -- per-step work
    # logs itself in its manager (mocked out here) -- and never repeats the unit
    # name Juju already prefixes onto every line.
    assert "unit=" not in caplog.text


def test_restore_action_rejected_on_non_leader(cloud_spec):
    """A follower must refuse restore and write nothing to the app databag."""
    from ops import testing

    ctx, state = _restore_context_and_state(cloud_spec, leader=False)
    with pytest.raises(testing.ActionFailed) as exc:
        ctx.run(ctx.on.action("restore", params={"backup-id": "2026-05-13T10:00:00Z"}), state)

    assert "leader" in exc.value.message.lower()
    assert _peer_app_data(exc.value.state).get("restore-id", "") == ""


def test_restore_action_rejects_unknown_backup_id(mocker, cloud_spec, restore_managers):
    """An unknown backup-id fails the action and never initiates the workflow."""
    from ops import testing

    _pass_restore_preconditions(mocker, ["2026-05-13T10:00:00Z"])
    ctx, state = _restore_context_and_state(cloud_spec)

    with pytest.raises(testing.ActionFailed) as exc:
        ctx.run(ctx.on.action("restore", params={"backup-id": "nope"}), state)

    assert "not found" in exc.value.message.lower()
    assert _peer_app_data(exc.value.state).get("restore-id", "") == ""


# ── restore workflow state machine ───────────────────────────────────────────


def test_single_unit_restore_completes_via_relation_changed_cascade(cloud_spec, restore_managers):
    """A single-unit restore converges to done (id + per-unit step cleared).

    A single unit is always the leader, and Juju delivers relation_changed to the
    leader for its own writes to the peer *app* databag, so the machine cascades
    forward one step per hook off the restore_id/instruction writes -- no peers
    and no in-hook loop needed.
    """
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    state_out = _drive_restore(ctx, state)

    assert _peer_app_data(state_out).get("restore-id", "") == ""
    assert _peer_unit_data(state_out).get("restore-step", "") == ""


def test_primary_runs_full_restore_workflow(cloud_spec, restore_managers):
    """A single-unit primary drives RESTORE→RESYNC→COMPLETED and clears state, no rollback."""
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    state_out = _drive_restore(ctx, state)

    restore_managers.suppress_failover.assert_called_once()  # RESTORE
    restore_managers.restore_on_primary.assert_called_once()  # RESTORE
    restore_managers.wait_until_loaded.assert_called_once()  # RESTORE post-check
    restore_managers.resume_failover.assert_called_once()  # RESYNC (primary)
    restore_managers.cleanup_restore_files.assert_called_once()  # COMPLETED
    restore_managers.roll_back.assert_not_called()
    assert _peer_app_data(state_out).get("restore-id", "") == ""
    assert _peer_unit_data(state_out).get("restore-step", "") == ""


def test_primary_saves_dataset_before_restore(cloud_spec, restore_managers):
    """The primary persists in-memory data to disk before restoring.

    restore_on_primary moves the on-disk dump aside as the rollback copy, which is
    only faithful if it reflects current memory — so save runs first, and outside
    the rollback try (a save failure leaves the primary untouched, below).
    """
    order = []
    restore_managers.save_dataset_before_shutdown.side_effect = lambda: order.append("save")
    restore_managers.restore_on_primary.side_effect = lambda: order.append("restore")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    ctx.run(ctx.on.update_status(), state)

    assert order == ["save", "restore"]  # persisted before the swap


def test_save_failure_aborts_restore_without_rollback(cloud_spec, restore_managers):
    """If the pre-restore save fails, the primary is never stopped and nothing rolls back."""
    from common.exceptions import ValkeyWorkloadCommandError

    restore_managers.save_dataset_before_shutdown.side_effect = ValkeyWorkloadCommandError(
        "save failed"
    )
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    state_out = ctx.run(ctx.on.update_status(), state)  # must NOT raise

    restore_managers.restore_on_primary.assert_not_called()
    restore_managers.roll_back.assert_not_called()  # nothing changed → nothing to undo
    assert _peer_app_data(state_out).get("restore-id", "") == ""  # torn down


def test_redelivered_restore_rolls_back_and_fails(cloud_spec, restore_managers, mocker):
    """Crash mid-swap + redelivery must roll back to the pre-restore data and FAIL.

    After a crash inside restore_on_primary, valkey is stopped and Juju never
    committed restore_role. On redelivery the on-disk rollback copy proves this unit
    is the mid-swap primary. Rather than probe the dead server or push the
    interrupted download forward, the workflow restores the original data
    (roll_back) and fails so the operator re-runs from a known-good baseline.
    """
    from src.statuses import RestoreStatuses

    # ROLE against a stopped server would raise; the on-disk signal must be used first.
    restore_managers.has_pre_restore_copy.return_value = True
    mocker.patch("workload_k8s.ValkeyK8sWorkload.alive", return_value=False)

    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
        unit_data={"restore-step": "", "restore-role": ""},  # nothing committed pre-crash
    )

    with ctx(ctx.on.update_status(), state) as mgr:
        state_out = mgr.run()
        statuses = mgr.charm.state.statuses.get(
            scope="app", component="backup", running_status_only=True
        ).root

    # Rolled back to a known-good baseline and failed; the dead server is never
    # probed and the interrupted swap is NOT continued.
    restore_managers.roll_back.assert_called_once()  # restored the original data
    restore_managers.is_primary.assert_not_called()  # dead server never probed
    restore_managers.restore_on_primary.assert_not_called()  # did NOT continue the swap
    assert RestoreStatuses.RESTORE_FAILED.value in statuses
    assert _peer_app_data(state_out).get("restore-id", "") == ""  # torn down


def test_replica_redelivery_does_not_resume_as_primary(cloud_spec, restore_managers):
    """A participant whose valkey is down but has NO pre-restore copy must not resume-as-primary.

    Only a mid-swap primary leaves a pre-restore copy; a replica whose valkey merely
    crashed must fall through to a real is_primary() probe, not be assumed primary
    (which would make it download+swap a full DB and split-brain the cluster).
    """
    from common.exceptions import ValkeyWorkloadCommandError

    restore_managers.has_pre_restore_copy.return_value = False  # no copy -> not a mid-swap primary
    restore_managers.is_primary.side_effect = ValkeyWorkloadCommandError("down")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=False,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-token": "tok-r",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    state_out = ctx.run(ctx.on.update_status(), state)

    # It PROBES is_primary (which raises -> teardown); it never assumes primary.
    restore_managers.is_primary.assert_called()
    restore_managers.restore_on_primary.assert_not_called()  # no primary swap
    restore_managers.suppress_failover.assert_not_called()
    assert _peer_unit_data(state_out)["restore-failed"] == "failed:tok-r"


def test_primary_reconciles_min_replicas_after_restore(cloud_spec, restore_managers):
    """After the primary restarts on the restored RDB, min-replicas-to-write is reasserted.

    A raw stop/start bypasses the rolling-restart path, so the topology-correct
    runtime value must be reconciled explicitly or a small cluster stays write-frozen.
    """
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    ctx.run(ctx.on.update_status(), state)

    restore_managers.reconcile_min_replicas_to_write.assert_called_once()


def test_reconciles_min_replicas_after_rollback(cloud_spec, restore_managers):
    """A failed restore rolls back (another restart), so min-replicas must be reasserted too.

    roll_back does a raw stop/start that reverts the runtime value; without this a
    small cluster is left write-frozen after a failed restore.
    """
    from common.exceptions import ValkeyServicesFailedToStartError

    restore_managers.restore_on_primary.side_effect = ValkeyServicesFailedToStartError("boom")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    ctx.run(ctx.on.update_status(), state)

    restore_managers.roll_back.assert_called_once()
    restore_managers.reconcile_min_replicas_to_write.assert_called_once()


def test_reconcile_failure_does_not_fail_a_successful_restore(cloud_spec, restore_managers):
    """A raise from the post-restart min-replicas reconcile must not fail a good restore.

    reconcile runs in a finally; the manager swallows its expected errors, but an
    exotic one (e.g. a raw OSError from the VM CLI exec) must not turn a
    successful restore into a false RESTORE_FAILED via the teardown path.
    """
    from src.statuses import RestoreStatuses

    # The restore's own post-restart reconcile (first call) raises an exotic error;
    # later, unrelated reconciles (e.g. base_events on a post-restore update_status)
    # behave normally -- the error is in the restore path, not a permanent fault.
    restore_managers.reconcile_min_replicas_to_write.side_effect = [
        RuntimeError("config set blew up"),
        *([None] * 20),
    ]
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    state_out, statuses = _drive_restore(ctx, state, capture_statuses=True)  # must NOT raise

    # Full success path still reached, no rollback, not marked failed.
    restore_managers.cleanup_restore_files.assert_called_once()
    restore_managers.roll_back.assert_not_called()
    assert RestoreStatuses.RESTORE_FAILED.value not in statuses
    assert _peer_app_data(state_out).get("restore-id", "") == ""


def test_replica_records_role_and_barrier_holds(cloud_spec, restore_managers):
    """A replica records role/step but never restores; the barrier waits on a lagging peer.

    valkey/1 is still a peer-relation member (present) but hasn't recorded RESTORE
    yet, so the barrier legitimately holds without advancing — distinct from a
    *departed* participant, which the leader now fails (see the departure test).
    """
    restore_managers.is_primary.return_value = False
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0,valkey/1",
        },
        # valkey/1 is present (still a member) but has not reached RESTORE -> the
        # barrier waits; it is NOT gone, so no teardown fires.
        peers_data={1: {"start-state": "started"}},
    )

    state_out = ctx.run(ctx.on.update_status(), state)

    restore_managers.restore_on_primary.assert_not_called()
    restore_managers.suppress_failover.assert_not_called()
    assert _peer_unit_data(state_out)["restore-role"] == "replica"
    assert _peer_unit_data(state_out)["restore-step"] == RestoreStep.RESTORE.value
    # Barrier not met (valkey/1 never reached RESTORE): still in progress, no advance.
    assert _peer_app_data(state_out)["restore-id"] == "2026-05-13T10:00:00Z"
    assert _peer_app_data(state_out)["restore-instruction"] == RestoreStep.RESTORE.value


def test_leader_fails_restore_when_participant_departs(cloud_spec, restore_managers):
    """A participant that vanished mid-restore must not wedge the cluster forever.

    valkey/1 was snapshotted as a participant at initiation but is no longer a
    peer-relation member (force-removed / lost machine), so it can never reach the
    barrier or record a failure. The fail-closed barrier would otherwise stall in
    "restore in progress" forever, freezing restarts/scaling/TLS/S3 cluster-wide.
    The leader must FAIL the restore instead: resume failover, flag RESTORE_FAILED,
    clear the app-level restore state.
    """
    from src.statuses import RestoreStatuses

    # This unit (the leader) is a replica that already recorded RESTORE; its only
    # blocker is the absent valkey/1 -- i.e. the wedge, not a normal lagging wait.
    restore_managers.is_primary.return_value = False
    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=True,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-token": "tok-1",
            "restore-instruction": RestoreStep.RESTORE.value,
            # valkey/1 was a participant at initiation but has since departed:
            # only valkey/0 remains in the peer relation (empty peers_data).
            "restore-participants": "valkey/0,valkey/1",
        },
        unit_data={"restore-step": RestoreStep.RESTORE.value, "restore-role": "replica"},
    )

    with ctx(ctx.on.update_status(), state) as mgr:
        state_out = mgr.run()
        statuses = mgr.charm.state.statuses.get(
            scope="app", component="backup", running_status_only=True
        ).root

    # Torn down, not wedged.
    assert _peer_app_data(state_out).get("restore-id", "") == ""
    assert RestoreStatuses.RESTORE_FAILED.value in statuses
    restore_managers.resume_failover.assert_called()


def test_non_leader_does_not_fail_restore_on_departed_participant(cloud_spec, restore_managers):
    """Only the leader may tear a restore down when a participant departs.

    A non-leader that observes a departed participant must leave app state alone
    (it cannot write it anyway) and must not resume failover.
    """
    restore_managers.is_primary.return_value = False
    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=False,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-token": "tok-d",
            "restore-instruction": RestoreStep.RESTORE.value,
            # valkey/1 is a participant but absent (departed); valkey/0 is a non-leader.
            "restore-participants": "valkey/0,valkey/1",
        },
        unit_data={"restore-step": RestoreStep.RESTORE.value, "restore-role": "replica"},
    )

    state_out = ctx.run(ctx.on.update_status(), state)

    assert _peer_app_data(state_out)["restore-id"] == "2026-05-13T10:00:00Z"  # untouched
    restore_managers.resume_failover.assert_not_called()


def test_step_skipped_when_prior_not_reached(cloud_spec, restore_managers):
    """instruction=RESYNC but this unit never recorded RESTORE → it acts on nothing."""
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESYNC.value,
            "restore-participants": "valkey/0",
        },
        # No restore-step recorded: this unit missed the fused RESTORE.
    )

    state_out = ctx.run(ctx.on.update_status(), state)

    restore_managers.wait_until_resynced.assert_not_called()
    restore_managers.resume_failover.assert_not_called()
    assert _peer_unit_data(state_out).get("restore-step", "") == ""
    # An out-of-step unit can never satisfy the barrier, so the instruction holds.
    assert _peer_app_data(state_out)["restore-instruction"] == RestoreStep.RESYNC.value


def test_non_participant_unit_skips_restore_workflow(cloud_spec, restore_managers):
    """A unit that joined AFTER initiation (absent from restore_participants) must no-op.

    Otherwise it matches (RESTORE, NOT_STARTED), queries its own not-yet-started
    Valkey, and the teardown fans resume_failover (down-after 30s + SENTINEL RESET)
    across every peer while the real primary is stopped mid-download -- a genuine
    bad-failover window. The leader ignores its non-participant failure marker
    anyway, so the restore would proceed with suppression removed.
    """
    from common.exceptions import ValkeyWorkloadCommandError

    # StartLock is withheld during a restore, so this newcomer's Valkey is down;
    # is_primary() would raise -> broad except -> _fail_restore -> resume_failover.
    restore_managers.is_primary.side_effect = ValkeyWorkloadCommandError("not up")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=False,  # a freshly-joined unit is not the leader
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-token": "tok-x",
            "restore-instruction": RestoreStep.RESTORE.value,
            # valkey/0 (the unit under test) is NOT a participant: it joined late.
            "restore-participants": "valkey/1",
        },
        peers_data={1: {"start-state": "started"}},
    )

    state_out = ctx.run(ctx.on.update_status(), state)

    # A non-participant does nothing: no step, no teardown, no failover churn.
    restore_managers.is_primary.assert_not_called()
    restore_managers.suppress_failover.assert_not_called()
    restore_managers.resume_failover.assert_not_called()
    assert _peer_unit_data(state_out).get("restore-step", "") == ""
    assert _peer_unit_data(state_out).get("restore-failed", "") == ""


def test_non_participant_leader_advances_restore(cloud_spec, restore_managers):
    """A non-participant LEADER must still advance the barrier, not wedge the restore.

    If leadership drifts to a unit that joined after initiation (a non-participant)
    without any original participant departing, that leader must still advance the
    shared instruction once the real participants reach it -- otherwise nobody
    advances the barrier and the restore wedges in-progress forever.
    """
    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=True,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            # valkey/0 (this leader) joined late: NOT a participant. valkey/1 is the
            # sole participant and has already reached RESTORE.
            "restore-participants": "valkey/1",
        },
        peers_data={1: {"start-state": "started", "restore-step": RestoreStep.RESTORE.value}},
    )

    state_out = ctx.run(ctx.on.update_status(), state)

    # Ran no step of its own (non-participant), but advanced the barrier to RESYNC.
    restore_managers.is_primary.assert_not_called()
    restore_managers.restore_on_primary.assert_not_called()
    assert _peer_app_data(state_out)["restore-instruction"] == RestoreStep.RESYNC.value


def test_bad_backup_fails_restore_before_stopping_primary(cloud_spec, restore_managers):
    """A non-RDB backup fails the pre-stop check: the primary is never stopped or rolled back."""
    from common.exceptions import ValkeyRestoreError

    restore_managers.verify_backup_is_rdb.side_effect = ValkeyRestoreError("not an RDB")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    state_out = ctx.run(ctx.on.update_status(), state)

    restore_managers.restore_on_primary.assert_not_called()
    restore_managers.roll_back.assert_not_called()
    # Teardown resumes the suppression it turned on before validating — exactly
    # once: the leader-self _finish_failed_restore skips the redundant backstop.
    restore_managers.resume_failover.assert_called_once()
    assert _peer_app_data(state_out).get("restore-id", "") == ""


def test_restore_failure_rolls_back_and_resumes_failover(cloud_spec, restore_managers):
    """A service failure mid-restore rolls back, resumes failover, flags FAILED, clears state.

    Regression for FIX 1: service-control errors are standalone Exception
    subclasses outside the restore-error hierarchy, so only the broad catch keeps
    them from escaping resume_failover().
    """
    from common.exceptions import ValkeyServicesFailedToStartError
    from src.statuses import RestoreStatuses

    restore_managers.restore_on_primary.side_effect = ValkeyServicesFailedToStartError("boom")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    with ctx(ctx.on.update_status(), state) as mgr:
        state_out = mgr.run()
        statuses = mgr.charm.state.statuses.get(
            scope="app", component="backup", running_status_only=True
        ).root

    restore_managers.roll_back.assert_called_once()
    # The critical invariant: failover is resumed — exactly once on the
    # leader-self path (teardown resumes; _finish_failed_restore skips the backstop).
    restore_managers.resume_failover.assert_called_once()
    assert RestoreStatuses.RESTORE_FAILED.value in statuses
    assert _peer_app_data(state_out).get("restore-id", "") == ""


def test_restore_failure_unhealthy_status(cloud_spec, restore_managers):
    """A cluster-not-ready failure surfaces RESTORE_UNHEALTHY, not RESTORE_FAILED."""
    from common.exceptions import ValkeyClusterNotReadyError
    from src.statuses import RestoreStatuses

    restore_managers.wait_until_loaded.side_effect = ValkeyClusterNotReadyError("unhealthy")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    with ctx(ctx.on.update_status(), state) as mgr:
        mgr.run()
        statuses = mgr.charm.state.statuses.get(
            scope="app", component="backup", running_status_only=True
        ).root

    # The RDB swap succeeded but the server never came up healthy → roll back.
    restore_managers.roll_back.assert_called_once()
    assert RestoreStatuses.RESTORE_UNHEALTHY.value in statuses
    assert RestoreStatuses.RESTORE_FAILED.value not in statuses


# ── restore failure when the valkey primary is not the juju leader ────────────
#
# The valkey primary (which runs the destructive RDB swap) is frequently NOT the
# juju leader, and only the leader can clear the app-level restore_id. A failing
# non-leader unit must therefore signal failure on its OWN unit databag so the
# leader can tear the restore down, instead of silently wedging the whole cluster
# in "restore in progress" forever.


def test_non_leader_primary_failure_records_failure_marker(cloud_spec, restore_managers):
    """A non-leader primary whose restore fails records a per-unit failure marker.

    It cannot clear the app-level restore_id (leader-only), so it must leave a
    signal the leader can act on — and it must resume failover and NOT crash.
    """
    from common.exceptions import ValkeyServicesFailedToStartError

    restore_managers.restore_on_primary.side_effect = ValkeyServicesFailedToStartError("boom")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=False,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-token": "tok-1",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    state_out = ctx.run(ctx.on.update_status(), state)  # must NOT raise

    restore_managers.roll_back.assert_called_once()
    restore_managers.resume_failover.assert_called_once()
    # Failure recorded on this unit's own databag, stamped with the attempt token
    # so it can't be misread against a later restore.
    assert _peer_unit_data(state_out)["restore-failed"] == "failed:tok-1"


def test_leader_ends_restore_when_peer_restore_failed(cloud_spec, restore_managers):
    """The leader clears the app-level restore state when a *peer* reports failure.

    valkey/1 (a non-leader) recorded a failure for this attempt's token; the
    leader (valkey/0) must abort the whole restore — clear restore_id, resume
    failover (the failing peer may not have), and flag RESTORE_FAILED — rather
    than proceeding with its own step.
    """
    from src.statuses import RestoreStatuses

    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=True,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-token": "tok-2",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0,valkey/1",
        },
        peers_data={1: {"start-state": "started", "restore-failed": "failed:tok-2"}},
    )

    with ctx(ctx.on.update_status(), state) as mgr:
        state_out = mgr.run()
        statuses = mgr.charm.state.statuses.get(
            scope="app", component="backup", running_status_only=True
        ).root

    # The leader aborts before doing its own restore work.
    restore_managers.restore_on_primary.assert_not_called()
    # The leader resumes failover as a backstop: the failing peer's own
    # best-effort resume may have raised, so the leader must not rely on it.
    restore_managers.resume_failover.assert_called()
    assert RestoreStatuses.RESTORE_FAILED.value in statuses
    assert _peer_app_data(state_out).get("restore-id", "") == ""


def test_fail_restore_records_failure_even_if_resume_failover_raises(cloud_spec, restore_managers):
    """A raising resume_failover must not abort teardown (else restore re-wedges).

    resume_failover hits every sentinel via the CLI and can raise; teardown must
    still run to completion — failover-resume attempted, failure flagged, and the
    app-level restore state cleared rather than left wedged.
    """
    from common.exceptions import ValkeyServicesFailedToStartError, ValkeyWorkloadCommandError
    from src.statuses import RestoreStatuses

    restore_managers.restore_on_primary.side_effect = ValkeyServicesFailedToStartError("boom")
    restore_managers.resume_failover.side_effect = ValkeyWorkloadCommandError("sentinel down")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=True,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    with ctx(ctx.on.update_status(), state) as mgr:
        state_out = mgr.run()  # must NOT raise despite the resume_failover error
        statuses = mgr.charm.state.statuses.get(
            scope="app", component="backup", running_status_only=True
        ).root

    restore_managers.resume_failover.assert_called()  # attempted, even though it raised
    assert RestoreStatuses.RESTORE_FAILED.value in statuses
    # Torn down, not wedged: the app-level restore state is cleared.
    assert _peer_app_data(state_out).get("restore-id", "") == ""


def test_finish_failed_restore_unwedges_before_status_add(mocker):
    """The un-wedge must happen before, and independently of, the status write.

    statuses.add is not wrapped (matching the project convention); a failing add
    (e.g. a status databag another revision left invalid — add doesn't swallow it
    the way delete does) propagates, but only AFTER _clear_restore_state has
    cleared restore_id, so the restore isn't wedged and the hook self-heals on retry.
    """
    from src.events.backup import BackupEvents

    ev = BackupEvents.__new__(BackupEvents)
    ev.charm = mocker.Mock()
    ev.charm.backup_manager.name = "backup"
    ev.charm.state.failed_restore_kind = "failed"
    ev.charm.state.statuses.add.side_effect = RuntimeError("bad status databag")

    with pytest.raises(RuntimeError):
        ev._finish_failed_restore(resume=False)

    # Restore state was cleared BEFORE the status write raised.
    ev.charm.state.cluster.update.assert_called_once()
    assert ev.charm.state.cluster.update.call_args.args[0]["restore_id"] == ""


def test_finish_failed_restore_clears_state_before_resume_failover(mocker):
    """State is cleared before resume_failover, so an unexpected resume error can't wedge.

    resume_failover is a best-effort backstop reached outside the workflow's
    step try/except; if it raises outside the narrow catch, restore_id must
    already be cleared or the leader re-enters teardown forever.
    """
    from src.events.backup import BackupEvents

    ev = BackupEvents.__new__(BackupEvents)
    ev.charm = mocker.Mock()
    ev.charm.backup_manager.name = "backup"
    ev.charm.state.failed_restore_kind = "failed"
    ev.charm.sentinel_manager.resume_failover.side_effect = RuntimeError("sentinel unreachable")

    with pytest.raises(RuntimeError):
        ev._finish_failed_restore(resume=True)

    # Cleared BEFORE the resume raised -> not wedged.
    ev.charm.state.cluster.update.assert_called_once()
    assert ev.charm.state.cluster.update.call_args.args[0]["restore_id"] == ""


def test_stale_token_marker_ignored_for_new_restore(cloud_spec, restore_managers):
    """A failure marker from a PRIOR attempt must not abort the current restore.

    restore_id is the backup-id, so a same-backup re-run reuses it; the marker is
    scoped to a per-attempt token instead. A marker carrying an old token
    (`failed:tok-old`) must be ignored for the current attempt (`tok-new`) — the
    leader proceeds with the restore rather than false-aborting it.
    """
    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=True,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-token": "tok-new",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
        # Stale marker left from a previous, torn-down attempt.
        unit_data={"restore-failed": "failed:tok-old"},
    )

    state_out = _drive_restore(ctx, state)

    # The stale-token marker is ignored: the leader runs the restore, not a teardown.
    restore_managers.restore_on_primary.assert_called_once()
    assert _peer_app_data(state_out).get("restore-id", "") == ""  # completed normally


def test_restore_action_clears_stale_terminal_statuses(mocker, cloud_spec, restore_managers):
    """Initiating a restore clears BOTH stale terminal statuses (FAILED and UNHEALTHY)."""
    from data_platform_helpers.advanced_statuses.components import StatusesState

    from src.statuses import RestoreStatuses

    _pass_restore_preconditions(mocker, ["2026-05-13T10:00:00Z"])
    delete = mocker.patch.object(StatusesState, "delete")
    ctx, state = _restore_context_and_state(cloud_spec)

    ctx.run(ctx.on.action("restore", params={"backup-id": "2026-05-13T10:00:00Z"}), state)

    deleted = {call.args[0] for call in delete.call_args_list}
    assert RestoreStatuses.RESTORE_FAILED.value in deleted
    assert RestoreStatuses.RESTORE_UNHEALTHY.value in deleted


def test_completed_restore_clears_terminal_statuses(mocker, cloud_spec, restore_managers):
    """A restore that reaches COMPLETED clears BOTH terminal statuses (FAILED and UNHEALTHY)."""
    from data_platform_helpers.advanced_statuses.components import StatusesState

    from src.statuses import RestoreStatuses

    delete = mocker.patch.object(StatusesState, "delete")
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-token": "tok-3",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    _drive_restore(ctx, state)

    deleted = {call.args[0] for call in delete.call_args_list}
    assert RestoreStatuses.RESTORE_FAILED.value in deleted
    assert RestoreStatuses.RESTORE_UNHEALTHY.value in deleted


# ── failover-suppression self-heal ───────────────────────────────────────────
#
# The workflow's "no restore in progress" branch also resumes a sentinel a failed
# restore left suppressed, so it runs on every hook the workflow observes.


def test_update_status_resumes_failover_left_suppressed(mocker, cloud_spec, restore_managers):
    """A sentinel still at the suppressed down-after outside a restore is resumed.

    resume_failover is best-effort on every teardown path and Sentinel persists
    SENTINEL SET to its own conf, so a sentinel unreachable at teardown would
    otherwise stay failover-suppressed until the next config re-render. Each
    unit self-heals its own sentinel, with update-status as the periodic backstop.
    """
    restore_managers.is_failover_suppressed.return_value = True
    # A resume clears the suppression, so a hook that re-enters the workflow (a
    # single-unit deployment re-emits peer relation-changed from the TLS handler)
    # heals once and then reads the configured value.
    restore_managers.resume_local_failover.side_effect = lambda: setattr(
        restore_managers.is_failover_suppressed, "return_value", False
    )
    ctx, state = _restore_context_and_state(cloud_spec)  # no restore in progress

    ctx.run(ctx.on.update_status(), state)

    restore_managers.resume_local_failover.assert_called_once()


def test_peer_relation_changed_resumes_failover_left_suppressed(
    mocker, cloud_spec, restore_managers
):
    """The self-heal also runs on the peer hook, not only on the 5-minute backstop.

    A restore ends by clearing the app-level restore-id, which re-delivers peer
    relation-changed to every unit -- exactly when a sentinel left suppressed by a
    best-effort teardown should be caught, rather than up to an update-status later.
    """
    restore_managers.is_failover_suppressed.return_value = True
    ctx, state = _restore_context_and_state(
        cloud_spec,
        leader=False,  # no restore in progress
        peers_data={1: {"start-state": "started"}},
    )
    peer = next(r for r in state.relations if r.id == 1)

    ctx.run(ctx.on.relation_changed(peer, remote_unit=1), state)

    restore_managers.resume_local_failover.assert_called_once()


def test_update_status_keeps_suppression_during_restore(mocker, cloud_spec, restore_managers):
    """Suppression is by design mid-restore: the self-heal must not undo it."""
    restore_managers.is_failover_suppressed.return_value = True
    ctx, state = _restore_context_and_state(
        cloud_spec,
        app_data={
            "restore-id": "2026-05-13T10:00:00Z",
            "restore-instruction": RestoreStep.RESTORE.value,
            "restore-participants": "valkey/0",
        },
    )

    ctx.run(ctx.on.update_status(), state)

    restore_managers.resume_local_failover.assert_not_called()


def test_update_status_suppression_check_tolerates_sentinel_error(
    mocker, cloud_spec, restore_managers
):
    """A sentinel that can't be queried is skipped (retried next update-status), not a crash."""
    from common.exceptions import ValkeyWorkloadCommandError

    restore_managers.is_failover_suppressed.side_effect = ValkeyWorkloadCommandError("down")
    ctx, state = _restore_context_and_state(cloud_spec)

    ctx.run(ctx.on.update_status(), state)  # must not raise

    restore_managers.resume_local_failover.assert_not_called()


# ── restore-awareness guards (single early-return clauses) ────────────────────
#
# These assert one guard clause on a handler that otherwise has nothing to do
# with restore; driving them through full events would entangle unrelated
# handlers (and re-run the restore workflow), so they stay at the unit layer.


def test_storage_detaching_refuses_during_restore(mocker):
    from src.events.base_events import BaseEvents

    ev = BaseEvents.__new__(BaseEvents)
    ev.charm = mocker.Mock()
    # Not already scaled down (the is_being_removed early-return runs first).
    ev.charm.state.unit_server.is_being_removed = False
    ev.charm.state.unit_server.is_backup_in_progress = False
    ev.charm.state.cluster.is_restore_in_progress = True

    with pytest.raises(Exception):  # ValkeyBackupInProgressError or a restore-specific error
        ev._on_storage_detaching(mocker.Mock())


def test_restart_workload_defers_during_restore(mocker):
    from src.charm import ValkeyCharm

    charm = ValkeyCharm.__new__(ValkeyCharm)
    charm.state = mocker.Mock()
    charm.state.unit_server.is_backup_in_progress = False
    charm.state.cluster.is_restore_in_progress = True
    event = mocker.Mock()
    ValkeyCharm._on_restart_workload(charm, event)
    event.defer.assert_called_once()


def test_external_clients_prc_skips_during_restore(mocker):
    from src.events.external_clients import ExternalClientsEvents

    ev = ExternalClientsEvents.__new__(ExternalClientsEvents)
    ev.charm = mocker.Mock()
    ev.charm.state.unit_server.is_started = True
    ev.charm.state.cluster.is_restore_in_progress = True
    ev._on_peer_relation_changed(mocker.Mock())
    ev.charm.sentinel_manager.reconcile_k8s_services.assert_not_called()
