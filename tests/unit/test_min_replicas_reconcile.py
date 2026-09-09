#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Wiring tests: scale events reassert min-replicas-to-write at runtime."""

from unittest.mock import MagicMock, PropertyMock, patch

from ops import testing

from src.charm import ValkeyCharm
from src.common.custom_events import RestartWorkloadEvent
from src.literals import PEER_RELATION, StartState

CONTAINER = "valkey"


def _started_3_unit_state(cloud_spec):
    """Return a started leader unit with two started peers (3 units total)."""
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data={"start-member": "valkey/1"},
        local_unit_data={"start-state": StartState.STARTED.value},
        peers_data={
            1: {"start-state": StartState.STARTED.value},
            2: {"start-state": StartState.STARTED.value},
        },
    )
    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={testing.Container(name=CONTAINER, can_connect=True)},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    return relation, state_in


def test_peer_relation_changed_reconciles_min_replicas(cloud_spec):
    """A peer relation-changed on a started unit reasserts min-replicas-to-write."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation, state_in = _started_3_unit_state(cloud_spec)

    with (
        patch("common.client.SentinelClient.primary", return_value={"quorum": "2"}),
        patch("common.client.SentinelClient.set"),
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="127.1.0.1"),
        patch("managers.cluster.ClusterManager.reconcile_min_replicas_to_write") as mock_reconcile,
    ):
        ctx.run(ctx.on.relation_changed(relation), state_in)
        mock_reconcile.assert_called_once()


def test_peer_relation_departed_reconciles_min_replicas(cloud_spec):
    """A peer relation-departed (scale down) on a started unit reasserts the value."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation, state_in = _started_3_unit_state(cloud_spec)

    with (
        patch("common.client.SentinelClient.primary", return_value={"quorum": "2"}),
        patch("common.client.SentinelClient.set"),
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="127.1.0.1"),
        patch("managers.sentinel.SentinelManager.get_active_sentinel_ips"),
        patch("managers.cluster.ClusterManager.reconcile_min_replicas_to_write") as mock_reconcile,
    ):
        ctx.run(ctx.on.relation_departed(relation, remote_unit=2), state_in)
        mock_reconcile.assert_called_once()


def test_restart_workload_reconciles_min_replicas(cloud_spec):
    """A Valkey workload restart reasserts the runtime value.

    The file ships min-replicas-to-write=1, but CONFIG SET does not survive a
    restart, so _on_restart_workload must reconcile once Valkey is healthy
    again or a small cluster would be write-frozen.
    """
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    _, state_in = _started_3_unit_state(cloud_spec)

    event = MagicMock(spec=RestartWorkloadEvent)
    event.restart_valkey = True
    event.restart_sentinel = False
    event.primary_endpoint = ""

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm = manager.charm
        charm.workload.restart = MagicMock()
        with (
            patch("common.locks.RestartLock.request_lock"),
            patch("common.locks.RestartLock.release_lock"),
            patch(
                "common.locks.RestartLock.is_held_by_this_unit",
                new_callable=PropertyMock,
                return_value=True,
            ),
            patch("managers.cluster.ClusterManager._save_database_blocking"),
            patch("managers.cluster.ClusterManager._disable_save_on_shutdown"),
            patch("managers.cluster.ClusterManager.is_healthy", return_value=True),
            patch("managers.topology.TopologyManager.start_observer"),
            patch(
                "managers.cluster.ClusterManager.reconcile_min_replicas_to_write"
            ) as mock_reconcile,
        ):
            charm._on_restart_workload(event)
            mock_reconcile.assert_called_once()
