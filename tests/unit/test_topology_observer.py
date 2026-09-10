#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the TopologyManager observer lifecycle."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from literals import (
    TOPOLOGY_OBSERVER_PID_FILENAME,
    TOPOLOGY_OBSERVER_SIGNATURE_FILENAME,
    CharmUsers,
)
from managers.topology import TopologyManager


def _make_manager(
    charm_dir: Path,
    pid: int | None = 1234,
    signature: str | None = None,
    endpoints: tuple[str, ...] = ("10.0.0.1", "10.0.0.2"),
    tls: bool = False,
    password: str = "sentinel-password",
    client_ca: str = "ca-pem",
):
    """Build a TopologyManager whose observer inputs are fully determined.

    `pid` and `signature` are what the unit recorded for the observer it last
    launched; `signature` defaults to the digest of the very inputs given here,
    i.e. the running observer was launched with exactly the current topology.
    `pid=None` records no observer at all.
    """
    state = MagicMock()
    state.charm.charm_dir = charm_dir
    state.unit_server.is_tls_enabled = tls
    servers = []
    for endpoint in endpoints:
        server = MagicMock(is_active=True)
        server.get_endpoint.return_value = endpoint
        servers.append(server)
    state.servers = servers
    state.cluster.internal_users_credentials = {CharmUsers.SENTINEL_CHARM_ADMIN.value: password}

    workload = MagicMock()
    workload.read_file.return_value = client_ca
    manager = TopologyManager(state=state, workload=workload)

    if pid is not None:
        (charm_dir / TOPOLOGY_OBSERVER_PID_FILENAME).write_text(str(pid))
        (charm_dir / TOPOLOGY_OBSERVER_SIGNATURE_FILENAME).write_text(
            manager.observer_signature() if signature is None else signature
        )
    return manager, state


@pytest.fixture
def charm_dir(tmp_path: Path) -> Path:
    """Return a throwaway charm directory to hold the observer's bookkeeping files."""
    return tmp_path


def test_restart_is_noop_when_running_and_inputs_unchanged(charm_dir: Path):
    """A re-delivered peer event must not churn the observer.

    The observer only reports a primary change against the one it saw last, and
    it keeps that in memory -- a needless relaunch throws it away and leaves the
    cluster unwatched while the replacement starts up.
    """
    manager, _ = _make_manager(charm_dir)

    with (
        patch("managers.topology.os.kill") as mock_kill,
        patch.object(TopologyManager, "stop_observer") as mock_stop,
        patch.object(TopologyManager, "start_observer") as mock_start,
    ):
        manager.restart_observer()

    mock_stop.assert_not_called()
    mock_start.assert_not_called()
    # only the liveness probe, never a signal
    mock_kill.assert_called_once_with(1234, 0)


def test_restart_when_topology_changed(charm_dir: Path):
    """A changed host set must relaunch the observer with the new arguments."""
    manager, _ = _make_manager(charm_dir, signature="stale-digest-from-a-different-topology")

    with (
        patch("managers.topology.os.kill"),
        patch.object(TopologyManager, "stop_observer") as mock_stop,
        patch.object(TopologyManager, "start_observer") as mock_start,
    ):
        manager.restart_observer()

    mock_stop.assert_called_once()
    mock_start.assert_called_once()


def test_restart_when_observer_process_is_gone(charm_dir: Path):
    """A dead observer is relaunched even though its inputs are unchanged."""
    manager, _ = _make_manager(charm_dir)

    with (
        patch("managers.topology.os.kill", side_effect=OSError),
        patch.object(TopologyManager, "stop_observer") as mock_stop,
        patch.object(TopologyManager, "start_observer") as mock_start,
    ):
        manager.restart_observer()

    mock_stop.assert_called_once()
    mock_start.assert_called_once()


def test_restart_when_no_observer_recorded(charm_dir: Path):
    """A unit that never started an observer starts one."""
    manager, _ = _make_manager(charm_dir, pid=None)

    with (
        patch("managers.topology.os.kill") as mock_kill,
        patch.object(TopologyManager, "stop_observer") as mock_stop,
        patch.object(TopologyManager, "start_observer") as mock_start,
    ):
        manager.restart_observer()

    mock_kill.assert_not_called()
    mock_stop.assert_called_once()
    mock_start.assert_called_once()


def test_restart_when_pid_file_predates_the_signature_file(charm_dir: Path):
    """An observer launched by an older revision has no signature; relaunch it once."""
    manager, _ = _make_manager(charm_dir)
    (charm_dir / TOPOLOGY_OBSERVER_SIGNATURE_FILENAME).unlink()

    with (
        patch("managers.topology.os.kill"),
        patch.object(TopologyManager, "stop_observer") as mock_stop,
        patch.object(TopologyManager, "start_observer") as mock_start,
    ):
        manager.restart_observer()

    mock_stop.assert_called_once()
    mock_start.assert_called_once()


def test_stop_observer_clears_both_records(charm_dir: Path):
    """Stopping must leave nothing behind that a later check could mistake for a live observer."""
    manager, _ = _make_manager(charm_dir)

    with patch("managers.topology.os.kill") as mock_kill:
        manager.stop_observer()

    mock_kill.assert_called_once()
    assert not (charm_dir / TOPOLOGY_OBSERVER_PID_FILENAME).exists()
    assert not (charm_dir / TOPOLOGY_OBSERVER_SIGNATURE_FILENAME).exists()


def test_signature_tracks_every_launch_argument(charm_dir: Path):
    """Host set, TLS mode and the Sentinel password each change the digest."""
    baseline, _ = _make_manager(charm_dir)
    digest = baseline.observer_signature()

    changed_hosts, _ = _make_manager(charm_dir, endpoints=("10.0.0.1", "10.0.0.9"))
    changed_tls, _ = _make_manager(charm_dir, tls=True)
    changed_password, _ = _make_manager(charm_dir, password="rotated-password")

    assert changed_hosts.observer_signature() != digest
    assert changed_tls.observer_signature() != digest
    assert changed_password.observer_signature() != digest


def test_signature_tracks_the_ca_when_tls_is_on(charm_dir: Path):
    """A CA rotation must relaunch the observer, which holds its own copy of the CA."""
    before, _ = _make_manager(charm_dir, tls=True, client_ca="old-ca-pem")
    after, _ = _make_manager(charm_dir, tls=True, client_ca="rotated-ca-pem")

    assert before.observer_signature() != after.observer_signature()


def test_signature_ignores_the_ca_when_tls_is_off(charm_dir: Path):
    """Without TLS the observer is never handed a CA, so it must not be read."""
    manager, _ = _make_manager(charm_dir, tls=False)

    manager.observer_signature()

    manager.workload.read_file.assert_not_called()


def test_signature_survives_an_unreadable_ca(charm_dir: Path):
    """An unreadable CA must not crash the leader's peer hook."""
    manager, _ = _make_manager(charm_dir, tls=True)
    manager.workload.read_file.side_effect = OSError("gone")

    assert manager.observer_signature()


def test_signature_is_order_independent(charm_dir: Path):
    """Peer ordering is not a topology change; the digest must not move."""
    one, _ = _make_manager(charm_dir, endpoints=("10.0.0.1", "10.0.0.2"))
    other, _ = _make_manager(charm_dir, endpoints=("10.0.0.2", "10.0.0.1"))

    assert one.observer_signature() == other.observer_signature()


def test_signature_does_not_leak_the_password(charm_dir: Path):
    """The digest is written to a file on the unit, so it must not carry the secret."""
    manager, _ = _make_manager(charm_dir, password="super-secret-password")

    assert "super-secret-password" not in manager.observer_signature()
