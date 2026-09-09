#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import PropertyMock, patch

import pytest
from ops import testing

from charm import ValkeyCharm
from common.exceptions import ValkeyCannotGetPrimaryIPError, ValkeyWorkloadCommandError
from literals import CONTAINER, PEER_RELATION, STATUS_PEERS_RELATION, ScaleDownState
from statuses import ScaleDownStatuses
from tests.unit.helpers import status_is


def get_3_unit_peer_relation():
    return testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "hostname": "valkey-0",
            "private-ip": "10.0.1.0",
            "start-state": "started",
        },
        peers_data={
            unit_id: {
                "hostname": f"valkey-{unit_id}",
                "private-ip": f"10.0.1.{unit_id}",
                "start-state": "started",
            }
            for unit_id in range(1, 3)
        },
    )


def test_other_unit_has_lock(cloud_spec):
    """Test that if another unit has the lock, then the lock is not acquired."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = get_3_unit_peer_relation()
    container = testing.Container(name=CONTAINER, can_connect=True)
    data_storage = testing.Storage(name="data")
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        containers={container},
        storages={data_storage},
    )

    with (
        patch("common.locks.ScaleDownLock.request_lock", return_value=False),
        patch(
            "common.client.SentinelClient.get_primary_addr_by_name",
            side_effect=[
                ValkeyWorkloadCommandError("errored out"),
                ("10.0.1.1", 6379),
            ],
        ),
    ):
        # expect raised exception due to lock not being acquired
        with pytest.raises(testing.errors.UncaughtCharmError) as exc_info:
            ctx.run(ctx.on.storage_detaching(data_storage), state_in)
        assert "RequestingLockTimedOutError" in str(exc_info.value)


def test_non_primary(cloud_spec):
    """Test scale-down behavior when this unit is not the primary but successfully acquires the lock."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = get_3_unit_peer_relation()
    container = testing.Container(name=CONTAINER, can_connect=True)
    data_strorage = testing.Storage(name="data")
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        containers={container},
        storages={data_strorage},
    )

    with (
        patch(
            "core.cluster_state.ClusterState.bind_address",
            new_callable=PropertyMock(return_value="10.0.1.0"),
        ),
        patch("common.locks.ScaleDownLock.request_lock", return_value=True),
        patch("common.locks.ScaleDownLock.release_lock", return_value=True),
        patch(
            "common.client.SentinelClient.get_primary_addr_by_name",
            return_value=("valkey-1", 6379),
        ),
        patch("workload_k8s.ValkeyK8sWorkload.stop") as mock_stop,
        patch("common.client.SentinelClient.reset") as mock_reset,
        patch("common.client.ValkeyClient.role") as get_replica_offset,
        patch("common.client.ValkeyClient.save") as save_dataset,
        patch(
            "common.client.SentinelClient.sentinels_primary",
            side_effect=[
                [{"ip": "valkey-0"}, {"ip": "valkey-2"}],  # for get_active_sentinel_ips
            ],
        ),
    ):
        state_out = ctx.run(ctx.on.storage_detaching(data_strorage), state_in)
        mock_stop.assert_called_once()
        mock_reset.assert_not_called()
        assert get_replica_offset.call_count == 2
        save_dataset.assert_called_once()
        status_is(state_out, ScaleDownStatuses.GOING_AWAY.value)


def test_non_primary_block_until_synced(cloud_spec):
    """Test scale-down behavior when this unit is not the primary but needs sync before shutdown."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = get_3_unit_peer_relation()
    container = testing.Container(name=CONTAINER, can_connect=True)
    data_strorage = testing.Storage(name="data")
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        containers={container},
        storages={data_strorage},
    )

    with (
        patch(
            "core.cluster_state.ClusterState.bind_address",
            new_callable=PropertyMock(return_value="10.0.1.0"),
        ),
        patch("common.locks.ScaleDownLock.request_lock", return_value=True),
        patch("common.locks.ScaleDownLock.release_lock", return_value=True),
        patch(
            "common.client.SentinelClient.get_primary_addr_by_name",
            return_value=("valkey-1", 6379),
        ),
        patch("workload_k8s.ValkeyK8sWorkload.stop") as mock_stop,
        patch("common.client.SentinelClient.reset") as mock_reset,
        patch(
            "common.client.ValkeyClient.role",
            side_effect=[
                ["master", 1108321968, ["valkey-0.valkey-endpoints", "6380", "1108321473"]],
                ["slave", "valkey-1.valkey-endpoints", 6380, "connected", 1108321473],
                ["slave", "valkey-1.valkey-endpoints", 6380, "connected", 1108321968],
            ],
        ) as get_replica_offset,
        patch("common.client.ValkeyClient.save") as save_dataset,
        patch(
            "common.client.SentinelClient.sentinels_primary",
            side_effect=[
                [{"ip": "valkey-0"}, {"ip": "valkey-2"}],  # for get_active_sentinel_ips
            ],
        ),
    ):
        state_out = ctx.run(ctx.on.storage_detaching(data_strorage), state_in)
        mock_stop.assert_called_once()
        mock_reset.assert_not_called()
        assert get_replica_offset.call_count == 3
        save_dataset.assert_called_once()
        status_is(state_out, ScaleDownStatuses.GOING_AWAY.value)


def test_primary(cloud_spec):
    """Test scale-down behavior when this unit is the primary and successfully acquires the lock."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = get_3_unit_peer_relation()
    container = testing.Container(name=CONTAINER, can_connect=True)
    data_strorage = testing.Storage(name="data")
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        containers={container},
        storages={data_strorage},
    )

    with (
        patch(
            "core.cluster_state.ClusterState.bind_address",
            new_callable=PropertyMock(return_value="10.0.1.0"),
        ),
        patch("common.locks.ScaleDownLock.request_lock", return_value=True),
        patch("common.locks.ScaleDownLock.release_lock", return_value=True),
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="valkey-0"),
        patch("workload_k8s.ValkeyK8sWorkload.stop") as mock_stop,
        patch("common.client.SentinelClient.failover_primary_coordinated") as mock_failover,
        patch(
            "common.client.SentinelClient.is_failover_in_progress", return_value=False
        ) as mock_failover_in_progress,
        patch("common.client.SentinelClient.reset") as mock_reset,
        patch("common.client.ValkeyClient.role") as get_replica_offset,
        patch("common.client.ValkeyClient.save") as save_dataset,
        patch(
            "common.client.SentinelClient.sentinels_primary",
            side_effect=[
                [{"ip": "10.0.1.1"}, {"ip": "10.0.1.2"}],  # for get_active_sentinel_ips
            ],
        ),
        patch(
            "common.client.SentinelClient.ping",
            side_effect=[True, False, True],  # valkey-1 is no longer active
        ),
    ):
        state_out = ctx.run(ctx.on.storage_detaching(data_strorage), state_in)
        mock_failover.assert_called_once()
        mock_failover_in_progress.assert_called_once()
        mock_stop.assert_called_once()
        mock_reset.assert_not_called()
        get_replica_offset.assert_not_called()
        save_dataset.assert_called_once()
        status_is(state_out, ScaleDownStatuses.GOING_AWAY.value)


def test_last_leader_unit_going_down(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "hostname": "valkey-0",
            "private-ip": "10.0.1.0",
            "start-state": "started",
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    data_strorage = testing.Storage(name="data")
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        containers={container},
        storages={data_strorage},
    )

    with (
        patch(
            "core.cluster_state.ClusterState.bind_address",
            new_callable=PropertyMock(return_value="10.0.1.0"),
        ),
        patch("common.locks.ScaleDownLock.request_lock", return_value=True),
        patch("common.locks.ScaleDownLock.release_lock", return_value=True),
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="valkey-0"),
        patch("workload_k8s.ValkeyK8sWorkload.stop") as mock_stop,
        patch("common.client.SentinelClient.sentinels_primary", return_value=[]),
        patch("common.client.ValkeyClient.save") as save_dataset,
        patch("core.models.ValkeyCluster.update") as cluster_update,
        patch("ops.model.Application.planned_units", return_value=0),
    ):
        state_out = ctx.run(ctx.on.storage_detaching(data_strorage), state_in)
        mock_stop.assert_called_once()
        save_dataset.assert_called_once()
        status_is(state_out, ScaleDownStatuses.GOING_AWAY.value)
        cluster_update.assert_called_once_with(
            {"internal_ca_certificate": None, "internal_ca_private_key": None}
        )


def test_logs_storage_detaching_triggers_scaledown(cloud_spec):
    """Detaching a non-data storage (logs) must also run the scale-down path.

    A unit teardown detaches every storage; whichever detaches first must run
    the safe scale-down so the workload is stopped before it can lose a volume.
    """
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = get_3_unit_peer_relation()
    container = testing.Container(name=CONTAINER, can_connect=True)
    logs_storage = testing.Storage(name="logs")
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        containers={container},
        storages={logs_storage},
    )

    with (
        patch("common.locks.ScaleDownLock.request_lock", return_value=False),
        patch(
            "common.client.SentinelClient.get_primary_addr_by_name",
            side_effect=[
                ValkeyWorkloadCommandError("errored out"),
                ("10.0.1.1", 6379),
            ],
        ),
    ):
        # reaching the lock request proves the handler ran for the logs storage
        with pytest.raises(testing.errors.UncaughtCharmError) as exc_info:
            ctx.run(ctx.on.storage_detaching(logs_storage), state_in)
        assert "RequestingLockTimedOutError" in str(exc_info.value)


def test_repeat_detach_is_noop_once_going_away(cloud_spec):
    """Once scale-down has run, later storage detaches must not re-run it."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "hostname": "valkey-0",
            "private-ip": "10.0.1.0",
            "start-state": "started",
            "scale-down-state": ScaleDownState.GOING_AWAY.value,
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    data_storage = testing.Storage(name="data")
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        containers={container},
        storages={data_storage},
    )

    with (
        patch(
            "managers.sentinel.SentinelManager.get_primary_ip_for_scale_down",
            return_value="10.0.1.0",
        ) as mock_get_primary,
        patch("common.locks.ScaleDownLock.request_lock") as mock_request_lock,
        patch("workload_k8s.ValkeyK8sWorkload.stop") as mock_stop,
    ):
        ctx.run(ctx.on.storage_detaching(data_storage), state_in)

    # the guard returns before any scale-down work happens
    mock_get_primary.assert_not_called()
    mock_request_lock.assert_not_called()
    mock_stop.assert_not_called()


def test_cannot_get_primary_ip_leader(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "hostname": "valkey-0",
            "private-ip": "10.0.1.0",
            "start-state": "started",
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    data_strorage = testing.Storage(name="data")
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        containers={container},
        storages={data_strorage},
    )

    with (
        patch(
            "core.cluster_state.ClusterState.bind_address",
            new_callable=PropertyMock(return_value="10.0.1.0"),
        ),
        patch(
            "managers.sentinel.SentinelManager.get_primary_ip",
            side_effect=ValkeyCannotGetPrimaryIPError("errored out"),
        ),
        patch("workload_k8s.ValkeyK8sWorkload.stop") as mock_stop,
        patch("core.models.ValkeyCluster.update") as cluster_update,
        patch("ops.model.Application.planned_units", return_value=0),
    ):
        state_out = ctx.run(ctx.on.storage_detaching(data_strorage), state_in)
        mock_stop.assert_not_called()
        status_is(state_out, ScaleDownStatuses.GOING_AWAY.value)
        cluster_update.assert_called_once_with(
            {"internal_ca_certificate": None, "internal_ca_private_key": None}
        )


def test_unit_departure_sentinel_reset_flag(cloud_spec):
    """Test that the flag for removing Sentinels is set on peer-relation-departed."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "hostname": "valkey-0",
            "private-ip": "10.0.1.0",
            "start-state": "started",
        },
        peers_data={
            1: {
                "hostname": "valkey-2",
                "private-ip": "10.0.1.2",
                "start-state": "started",
            }
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        planned_units=2,
        containers={container},
    )
    state_out = ctx.run(ctx.on.relation_departed(relation, remote_unit=2), state_in)
    assert state_out.get_relation(1).local_app_data.get("sentinel-reset-required") == "true"


def test_unit_departure_leader(cloud_spec):
    """Test Sentinel removal after scale down."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data={"sentinel-reset-required": "True"},
        local_unit_data={
            "hostname": "valkey-0",
            "private-ip": "10.0.1.0",
            "start-state": "started",
        },
        peers_data={
            1: {
                "hostname": "valkey-2",
                "private-ip": "10.0.1.2",
                "start-state": "started",
            }
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation, status_peer_relation},
        leader=True,
        planned_units=2,
        containers={container},
    )

    with (
        patch(
            "core.cluster_state.ClusterState.bind_address",
            new_callable=PropertyMock(return_value="10.0.1.0"),
        ),
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="valkey-0"),
        patch("events.base_events.BaseEvents._reconfigure_quorum_if_necessary"),
        patch("managers.cluster.ClusterManager.reconcile_min_replicas_to_write"),
        patch(
            "common.client.SentinelClient.sentinels_primary",
            side_effect=[
                [{"ip": "valkey-1"}, {"ip": "valkey-2"}],  # for get_active_sentinel_ips
                [{"ip": "valkey-2"}],  # for target_sees_all_others unit valkey-0
                [{"ip": "valkey-0"}],  # for target_sees_all_others unit valkey-2
            ],
        ),
        patch(
            "common.client.SentinelClient.ping",
            side_effect=[True, False, True],  # valkey-1 is no longer active
        ),
        patch("common.client.SentinelClient.reset"),
        patch(
            "common.client.SentinelClient.replicas_primary",
            side_effect=[{"ip": "ip"}, {"ip": "ip"}],
        ),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation, remote_unit=2), state_in)
        assert state_out.get_relation(1).local_app_data.get("sentinel-reset-required") == "false"


def test_unit_departure_not_yet_removed(cloud_spec):
    """Test scale-down behavior when this unit is the primary and successfully acquires the lock."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data={"sentinel-reset-required": "True"},
        local_unit_data={
            "hostname": "valkey-0",
            "private-ip": "10.0.1.0",
            "start-state": "started",
        },
        peers_data={
            1: {
                "hostname": "valkey-2",
                "private-ip": "10.0.1.2",
                "start-state": "started",
            }
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation, status_peer_relation},
        leader=True,
        planned_units=2,
        containers={container},
    )

    with (
        patch(
            "core.cluster_state.ClusterState.bind_address",
            new_callable=PropertyMock(return_value="10.0.1.0"),
        ),
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="valkey-0"),
        patch(
            "common.client.SentinelClient.sentinels_primary",
            side_effect=[
                [{"ip": "valkey-1"}, {"ip": "valkey-2"}],  # for get_active_sentinel_ips
            ],
        ),
        patch(
            "common.client.SentinelClient.ping",
            side_effect=[True, True, True],  # valkey-1 is still active
        ),
        patch("common.client.SentinelClient.replicas_primary") as verify,
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation, remote_unit=2), state_in)
        verify.assert_not_called()
        assert "valkey_peers_relation_changed" in [e.name for e in state_out.deferred]
        assert state_out.get_relation(1).local_app_data.get("sentinel-reset-required") == "true"
        assert status_is(
            state_out, ScaleDownStatuses.SENTINEL_NOT_REMOVED_AFTER_SCALEDOWN.value, is_app=True
        )


def test_unit_departure_leader_failed(cloud_spec):
    """Test scale-down behavior when this unit is the primary and successfully acquires the lock."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data={"sentinel-reset-required": "True"},
        local_unit_data={
            "hostname": "valkey-0",
            "private-ip": "10.0.1.0",
            "start-state": "started",
        },
        peers_data={
            1: {
                "hostname": "valkey-2",
                "private-ip": "10.0.1.2",
                "start-state": "started",
            }
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation, status_peer_relation},
        leader=True,
        planned_units=2,
        containers={container},
    )

    with (
        patch(
            "core.cluster_state.ClusterState.bind_address",
            new_callable=PropertyMock(return_value="10.0.1.0"),
        ),
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="valkey-0"),
        patch(
            "common.client.SentinelClient.sentinels_primary",
            side_effect=[
                [{"ip": "valkey-2"}],  # for get_active_sentinel_ips
                [{"ip": "valkey-2"}],  # for target_sees_all_others unit valkey-0
                [{"ip": "valkey-0"}, {"ip": "valkey-1"}],  # valkey-2 still sees valkey-1
            ],
        ),
        patch(
            "common.client.SentinelClient.ping",
            side_effect=[True, False, True],  # valkey-1 is no longer active
        ),
        patch("common.client.SentinelClient.reset"),
        patch(
            "common.client.SentinelClient.replicas_primary",
            side_effect=[{"ip": "ip"}, {"another_ip": "another_ip"}],
        ),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation, remote_unit=2), state_in)
        assert state_out.get_relation(1).local_app_data.get("sentinel-reset-required") == "true"
        assert "valkey_peers_relation_changed" in [e.name for e in state_out.deferred]
        assert status_is(
            state_out, ScaleDownStatuses.SENTINEL_NOT_REMOVED_AFTER_SCALEDOWN.value, is_app=True
        )


def test_unit_departure_non_leader(cloud_spec):
    """Test scale-down behavior when this unit is the primary and successfully acquires the lock."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = get_3_unit_peer_relation()
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation, status_peer_relation},
        leader=False,
        containers={container},
    )

    with patch("managers.sentinel.SentinelManager.verify_expected_replica_count") as verify:
        state_out = ctx.run(ctx.on.relation_changed(relation, remote_unit=2), state_in)
        verify.assert_not_called()
        assert not status_is(
            state_out, ScaleDownStatuses.SENTINEL_NOT_REMOVED_AFTER_SCALEDOWN.value, is_app=True
        )
