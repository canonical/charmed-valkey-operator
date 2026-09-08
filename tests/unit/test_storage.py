#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the logs/archive storage volumes."""

import logging
from unittest.mock import MagicMock, patch

import pytest
from ops import testing

from literals import PEER_RELATION, STATUS_PEERS_RELATION
from src.charm import ValkeyCharm
from workload_k8s import ValkeyK8sWorkload
from workload_vm import ValkeyVmWorkload

CONTAINER = "valkey"


def _base_relations():
    return {
        testing.PeerRelation(id=1, endpoint=PEER_RELATION),
        testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION),
    }


def test_k8s_workload_exposes_log_and_archive_dirs():
    wl = ValkeyK8sWorkload(MagicMock())
    assert wl.log_dir.as_posix() == "/var/log/valkey"
    assert wl.archive_dir.as_posix() == "/var/backups/valkey"


def test_vm_workload_exposes_log_and_archive_dirs():
    with patch("workload_vm.snap.SnapCache"):
        wl = ValkeyVmWorkload()
    assert wl.log_dir.as_posix() == "/var/snap/valkey-charmed/common/var/log/valkey"
    assert wl.archive_dir.as_posix() == "/var/snap/valkey-charmed/common/var/backups/valkey"


@pytest.mark.parametrize("storage_name", ["logs", "archive"])
def test_storage_is_a_readable_writable_mount(cloud_spec, storage_name):
    """A file placed on the volume is visible to the charm and survives the hook."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    storage = testing.Storage(name=storage_name)
    (storage.get_filesystem(ctx) / "seed.txt").write_text("seeded")

    state_in = testing.State(
        model=testing.Model(name="m", type="lxd", cloud_spec=cloud_spec),
        leader=True,
        containers={testing.Container(name=CONTAINER, can_connect=True)},
        storages={storage},
        relations=_base_relations(),
    )

    with (
        patch("workload_k8s.ValkeyK8sWorkload.exec"),
        ctx(ctx.on.storage_attached(storage), state_in) as manager,
    ):
        location = manager.charm.model.storages[storage_name][0].location
        assert (location / "seed.txt").read_text() == "seeded"
        (location / "written.txt").write_text("by-charm")

    assert (storage.get_filesystem(ctx) / "written.txt").read_text() == "by-charm"


def test_storage_attached_unknown_storage_logs_warning(cloud_spec, caplog):
    """An unrecognised storage name is logged and skipped, not silently dropped."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd", cloud_spec=cloud_spec),
        leader=True,
        containers={testing.Container(name=CONTAINER, can_connect=True)},
        relations=_base_relations(),
    )
    with (
        patch("workload_k8s.ValkeyK8sWorkload.exec") as mock_exec,
        ctx(ctx.on.update_status(), state_in) as manager,
    ):
        mock_exec.reset_mock()  # ignore any setup-time calls
        event = MagicMock()
        event.storage.name = "bogus"
        with caplog.at_level(logging.WARNING):
            manager.charm.base_events._on_storage_attached(event)
        # assert inside the block, before the manager runs update-status on exit
        mock_exec.assert_not_called()
        event.defer.assert_not_called()

    assert any("bogus" in record.message for record in caplog.records)


def test_storage_attached_logs_chmods_only_on_vm(cloud_spec_vm):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    logs = testing.Storage(name="logs")
    state_in = testing.State(
        model=testing.Model(name="m", type="lxd", cloud_spec=cloud_spec_vm),
        leader=True,
        storages={logs},
        relations=_base_relations(),
    )
    with (
        patch("workload_vm.snap.SnapCache"),  # avoid flaky snap-store lookup in __init__
        patch("workload_vm.ValkeyVmWorkload.exec") as mock_exec,
    ):
        ctx.run(ctx.on.storage_attached(logs), state_in)
    log_path = "/var/snap/valkey-charmed/common/var/log/valkey"
    mock_exec.assert_any_call(["chmod", "-R", "750", log_path])
    chown_calls = [c for c in mock_exec.call_args_list if c.args and c.args[0][0] == "chown"]
    assert chown_calls == [], "VM must not chown — valkey runs as root"
