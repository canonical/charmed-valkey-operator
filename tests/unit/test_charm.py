#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import PropertyMock, patch

import pytest
from ops import ActiveStatus, pebble, testing

from common.exceptions import ValkeyServiceNotAliveError, ValkeyWorkloadCommandError
from src.charm import ValkeyCharm
from src.literals import (
    INTERNAL_USERS_PASSWORD_CONFIG,
    INTERNAL_USERS_SECRET_LABEL_SUFFIX,
    PEER_RELATION,
    PRIMARY_NAME,
    STATUS_PEERS_RELATION,
    CharmUsers,
    StartState,
)
from src.statuses import CharmStatuses, ClusterStatuses, StartStatuses

from .helpers import APP_NAME, status_is

CHARM_USER = "_daemon_"
CONTAINER = "valkey"
SERVICE_VALKEY = "valkey"
SERVICE_METRIC_EXPORTER = "metrics-exporter"
SERVICE_SENTINEL = "sentinel"


internal_passwords_secret = testing.Secret(
    tracked_content={f"{user.value}-password": "secure-password" for user in CharmUsers},
    owner="app",
    label=f"{PEER_RELATION}.{APP_NAME}.app.{INTERNAL_USERS_SECRET_LABEL_SUFFIX}",
)


def test_start_primary(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)

    # happy path
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        leader=True,
        relations={relation, status_peer_relation},
        containers={container},
    )

    expected_plan = {
        "services": {
            SERVICE_VALKEY: {
                "override": "replace",
                "summary": "Valkey service",
                "command": "valkey-server /var/lib/valkey/valkey.conf",
                "user": CHARM_USER,
                "group": CHARM_USER,
                "startup": "enabled",
            },
            SERVICE_SENTINEL: {
                "override": "replace",
                "summary": "Valkey sentinel service",
                "command": "valkey-sentinel /var/lib/valkey/sentinel.conf",
                "user": CHARM_USER,
                "group": CHARM_USER,
                "startup": "enabled",
            },
            SERVICE_METRIC_EXPORTER: {
                "override": "replace",
                "summary": "Valkey metric exporter",
                "command": "prometheus-redis-exporter",
                "user": CHARM_USER,
                "group": CHARM_USER,
                "startup": "enabled",
            },
        }
    }

    # generate passwords
    state_in = ctx.run(ctx.on.leader_elected(), state_in)

    # start event
    state_out = ctx.run(ctx.on.start(), state_in)
    assert state_out.get_container(container.name).plan == expected_plan
    assert (
        state_out.get_container(container.name).service_statuses[SERVICE_VALKEY]
        == pebble.ServiceStatus.ACTIVE
    )
    assert (
        state_out.get_container(container.name).service_statuses[SERVICE_METRIC_EXPORTER]
        == pebble.ServiceStatus.ACTIVE
    )
    assert status_is(state_out, StartStatuses.SERVICE_STARTING.value)

    with (
        patch("common.client.ValkeyClient.ping", return_value=True),
        patch("common.client.ValkeyClient.info_persistence", return_value={"loading": "0"}),
        patch("common.client.ValkeyClient.set", return_value=True),
    ):
        state_out = ctx.run(ctx.on.start(), state_out)
        assert status_is(state_out, StartStatuses.WAITING_FOR_SENTINEL_DISCOVERY.value)

    with (
        patch("common.client.ValkeyClient.ping", return_value=True),
        patch("common.client.SentinelClient.ping", return_value=True),
        patch("common.client.ValkeyClient.info_persistence", return_value={"loading": "0"}),
        patch("common.client.ValkeyClient.set", return_value=True),
        patch("common.client.SentinelClient.primary", return_value={"ip": "test"}),
    ):
        state_out = ctx.run(ctx.on.start(), state_out)
        assert state_out.unit_status == ActiveStatus()
        assert state_out.app_status == ActiveStatus()

    with (
        patch(
            "managers.config.ConfigManager.set_config_properties",
            side_effect=ValkeyWorkloadCommandError,
        ),
    ):
        state_out = ctx.run(ctx.on.start(), state_in)
        assert status_is(state_out, StartStatuses.CONFIGURATION_ERROR.value)

    with (
        patch(
            "workload_k8s.ValkeyK8sWorkload.start",
            side_effect=ValkeyServiceNotAliveError,
        ),
    ):
        state_out = ctx.run(ctx.on.start(), state_in)
        assert status_is(state_out, StartStatuses.ERROR_ON_START.value)

    # container not ready
    container = testing.Container(name=CONTAINER, can_connect=False)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        leader=True,
        relations={relation, status_peer_relation},
        containers={container},
    )

    state_out = ctx.run(ctx.on.start(), state_in)
    assert status_is(state_out, StartStatuses.SERVICE_NOT_STARTED.value)


def test_start_non_primary(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        leader=False,
        relations={relation, status_peer_relation},
        containers={container},
    )

    with patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="127.1.0.1"):
        state_out = ctx.run(ctx.on.start(), state_in)
        assert not state_out.get_container(container.name).service_statuses.get(SERVICE_VALKEY)
        assert not state_out.get_container(container.name).service_statuses.get(
            SERVICE_METRIC_EXPORTER
        )
        assert "start" in [e.name for e in state_out.deferred]

        relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
        state_in = testing.State(
            model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
            leader=False,
            relations={relation, status_peer_relation},
            secrets={internal_passwords_secret},
            containers={container},
        )
        state_out = ctx.run(ctx.on.start(), state_in)

        assert status_is(state_out, StartStatuses.WAITING_TO_START.value)

        relation = testing.PeerRelation(
            id=1,
            endpoint=PEER_RELATION,
            local_app_data={"primary-ip": "127.1.0.1"},
            peers_data={1: {"start-state": "started"}},
        )
        state_in = testing.State(
            model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
            leader=False,
            relations={relation, status_peer_relation},
            secrets={internal_passwords_secret},
            containers={container},
        )
        state_out = ctx.run(ctx.on.start(), state_in)

        assert status_is(state_out, StartStatuses.WAITING_TO_START.value)

        # health check
        with patch(
            "common.client.ValkeyClient.role",
            return_value=["slave", "ip", 6379, "sync", 467184],
        ):
            relation = testing.PeerRelation(
                id=1,
                endpoint=PEER_RELATION,
                local_app_data={"start-member": "valkey/0"},
                peers_data={1: {"start-state": "started"}},
            )
            state_in = testing.State(
                model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
                leader=False,
                relations={relation, status_peer_relation},
                secrets={internal_passwords_secret},
                containers={container},
            )
            state_out = ctx.run(ctx.on.start(), state_in)
            assert status_is(state_out, StartStatuses.SERVICE_STARTING.value)

        # sentinel not yet discovered error raised
        with (
            patch(
                "core.cluster_state.ClusterState.bind_address",
                new_callable=PropertyMock(return_value="10.0.1.0"),
            ),
            patch(
                "common.client.SentinelClient.sentinels_primary",
                side_effect=ValkeyWorkloadCommandError("errored out"),
            ),
            patch("managers.cluster.ClusterManager.is_healthy", return_value=True),
            patch("managers.sentinel.SentinelManager.is_healthy", return_value=True),
        ):
            relation = testing.PeerRelation(
                id=1,
                endpoint=PEER_RELATION,
                local_app_data={"start-member": "valkey/0"},
                peers_data={1: {"start-state": "started"}},
            )
            state_in = testing.State(
                model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
                leader=False,
                relations={relation, status_peer_relation},
                secrets={internal_passwords_secret},
                containers={container},
            )
            state_out = ctx.run(ctx.on.start(), state_in)
            assert status_is(state_out, StartStatuses.WAITING_FOR_SENTINEL_DISCOVERY.value)

        # sentinel not yet discovered sentinel not seeing other sentinel
        with (
            patch(
                "core.cluster_state.ClusterState.bind_address",
                new_callable=PropertyMock(return_value="10.0.1.0"),
            ),
            patch(
                "common.client.SentinelClient.sentinels_primary",
                return_value=[{"ip": "10.0.1.1"}, {"ip": "10.0.1.2"}],
            ),
            patch("managers.cluster.ClusterManager.is_healthy", return_value=True),
            patch("managers.sentinel.SentinelManager.is_healthy", return_value=True),
        ):
            relation = testing.PeerRelation(
                id=1,
                endpoint=PEER_RELATION,
                local_app_data={"start-member": "valkey/0"},
                peers_data={1: {"start-state": "started"}},
            )
            state_in = testing.State(
                model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
                leader=False,
                relations={relation, status_peer_relation},
                secrets={internal_passwords_secret},
                containers={container},
            )
            state_out = ctx.run(ctx.on.start(), state_in)
            assert status_is(state_out, StartStatuses.WAITING_FOR_SENTINEL_DISCOVERY.value)

        # replica syncing
        with (
            patch("managers.cluster.ClusterManager.is_replica_synced", return_value=False),
            patch("managers.sentinel.SentinelManager.is_sentinel_discovered", return_value=True),
            patch("managers.cluster.ClusterManager.is_healthy", return_value=True),
            patch("managers.sentinel.SentinelManager.is_healthy", return_value=True),
        ):
            relation = testing.PeerRelation(
                id=1,
                endpoint=PEER_RELATION,
                local_app_data={"start-member": "valkey/0"},
                peers_data={1: {"start-state": "started"}},
            )
            state_in = testing.State(
                model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
                leader=False,
                relations={relation, status_peer_relation},
                secrets={internal_passwords_secret},
                containers={container},
            )
            state_out = ctx.run(ctx.on.start(), state_in)
            assert status_is(state_out, StartStatuses.WAITING_FOR_REPLICA_SYNC.value)
        # Happy path with sentinel discovered and replica synced
        with (
            patch("managers.sentinel.SentinelManager.is_sentinel_discovered", return_value=True),
            patch("managers.cluster.ClusterManager.is_replica_synced", return_value=True),
            patch("managers.cluster.ClusterManager.is_healthy", return_value=True),
            patch("managers.sentinel.SentinelManager.is_healthy", return_value=True),
        ):
            relation = testing.PeerRelation(
                id=1,
                endpoint=PEER_RELATION,
                local_app_data={"start-member": "valkey/0"},
                peers_data={1: {"start-state": "started"}},
            )
            state_in = testing.State(
                model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
                leader=False,
                relations={relation, status_peer_relation},
                secrets={internal_passwords_secret},
                containers={container},
            )
            state_out = ctx.run(ctx.on.start(), state_in)
            assert status_is(state_out, CharmStatuses.ACTIVE_IDLE.value)

            assert state_out.get_container(container.name).service_statuses.get(SERVICE_VALKEY)
            assert state_out.get_container(container.name).service_statuses.get(
                SERVICE_METRIC_EXPORTER
            )
            assert state_out.get_container(container.name).service_statuses[SERVICE_SENTINEL]
            assert state_out.get_relation(1).local_unit_data["start-state"] == "started"


def test_update_status_leader_unit(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)

    # happy path
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        leader=True,
        relations={relation, status_peer_relation},
        containers={container},
    )

    with (
        patch("managers.tls.TLSManager.will_certificate_expire"),
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)
        assert state_out.unit_status == ActiveStatus()


def test_update_status_non_leader_unit(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1, endpoint=PEER_RELATION, local_unit_data={"start-state": "started"}
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        leader=False,
        relations={relation, status_peer_relation},
        containers={container},
    )
    with (
        patch("managers.tls.TLSManager.will_certificate_expire"),
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)
        assert state_out.unit_status == ActiveStatus()


def test_internal_user_creation(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
        relations={relation},
        leader=True,
        containers={container},
    )
    state_out = ctx.run(ctx.on.leader_elected(), state_in)
    secret_out = state_out.get_secret(
        label=f"{PEER_RELATION}.{APP_NAME}.app.{INTERNAL_USERS_SECRET_LABEL_SUFFIX}"
    )
    assert secret_out.latest_content.get(f"{CharmUsers.VALKEY_ADMIN.value}-password")


def test_leader_elected_no_peer_relation(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    state_out = ctx.run(ctx.on.leader_elected(), state_in)
    assert "leader_elected" in [e.name for e in state_out.deferred]


def test_leader_elected_leader_password_specified(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)

    password_secret = testing.Secret(
        tracked_content={CharmUsers.VALKEY_ADMIN.value: "secure-password"},
        remote_grants={1: [APP_NAME]},
    )
    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={container},
        secrets={password_secret},
        config={INTERNAL_USERS_PASSWORD_CONFIG: password_secret.id},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch("managers.auth.AuthManager.generate_password", return_value="generated-password"),
    ):
        state_out = ctx.run(ctx.on.leader_elected(), state_in)
        secret_out = state_out.get_secret(
            label=f"{PEER_RELATION}.{APP_NAME}.app.{INTERNAL_USERS_SECRET_LABEL_SUFFIX}"
        )
        for user in CharmUsers:
            if user == CharmUsers.VALKEY_ADMIN:
                assert secret_out.latest_content.get(f"{user.value}-password") == "secure-password"
                continue
            assert secret_out.latest_content.get(f"{user.value}-password") == "generated-password"


def test_leader_elected_leader_password_specified_wrong_secret(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={relation, status_relation},
        containers={container},
        config={INTERNAL_USERS_PASSWORD_CONFIG: "secret:1tf1wk0tmfrodp8ofwxn"},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with pytest.raises(testing.errors.UncaughtCharmError) as exc_info:
        ctx.run(ctx.on.leader_elected(), state_in)
        assert "SecretNotFoundError" in str(exc_info.value)


def test_config_changed_non_leader_unit(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    password_secret = testing.Secret(
        tracked_content={CharmUsers.VALKEY_ADMIN.value: "secure-password"},
        remote_grants={1: [APP_NAME]},
    )

    state_in = testing.State(
        leader=False,
        relations={relation},
        containers={container},
        secrets={password_secret},
        config={INTERNAL_USERS_PASSWORD_CONFIG: password_secret.id},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with patch("events.base_events.BaseEvents._update_internal_users_password") as mock_update:
        ctx.run(ctx.on.config_changed(), state_in)
        mock_update.assert_not_called()


def test_config_changed_leader_unit(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1, endpoint=PEER_RELATION, local_unit_data={"start-state": "started"}
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    password_secret = testing.Secret(
        tracked_content={user.value: "secure-password" for user in CharmUsers},
        remote_grants={1: [APP_NAME]},
    )
    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={container},
        secrets={password_secret},
        config={INTERNAL_USERS_PASSWORD_CONFIG: password_secret.id},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch("managers.auth.AuthManager.set_acl_file") as mock_set_acl_file,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch("common.client.ValkeyClient.acl_load") as mock_acl_load,
        patch("common.client.ValkeyClient.config_set") as mock_config_set,
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)
        mock_set_acl_file.assert_called_once()
        mock_acl_load.assert_called_once()
        mock_config_set.assert_called_once()
        set_sentinel_acl_file.assert_called_once()
        restart_sentinel.assert_called_once()
        secret_out = state_out.get_secret(
            label=f"{PEER_RELATION}.{APP_NAME}.app.{INTERNAL_USERS_SECRET_LABEL_SUFFIX}"
        )
        assert (
            secret_out.latest_content.get(f"{CharmUsers.VALKEY_ADMIN.value}-password")
            == "secure-password"
        )


def test_config_changed_leader_unit_wrong_username(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)

    password_secret = testing.Secret(
        tracked_content={"wrong-username": "secure-password"},
        remote_grants={1: [APP_NAME]},
    )
    state_in = testing.State(
        leader=True,
        relations={relation, status_peer_relation},
        containers={container},
        secrets={password_secret},
        config={INTERNAL_USERS_PASSWORD_CONFIG: password_secret.id},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch("managers.auth.AuthManager.set_acl_file") as mock_set_acl_file,
        ctx(ctx.on.config_changed(), state_in) as manager,
    ):
        charm: ValkeyCharm = manager.charm
        manager.run()
        cluster_statuses = charm.state.statuses.get(
            scope="app",
            component=charm.cluster_manager.name,
        )
        assert ClusterStatuses.PASSWORD_UPDATE_FAILED.value in cluster_statuses
        mock_set_acl_file.assert_not_called()


def test_config_changed_ip_change_no_tls_relation(cloud_spec_vm):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "private-ip": "127.0.1.1"},
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    password_secret = testing.Secret(
        tracked_content={user.value: "secure-password" for user in CharmUsers},
        remote_grants={1: [APP_NAME]},
    )
    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={container},
        secrets={password_secret},
        config={INTERNAL_USERS_PASSWORD_CONFIG: password_secret.id},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec_vm),
    )
    with (
        patch("managers.config.ConfigManager.configure_services"),
        patch("managers.auth.AuthManager.configure_auth"),
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="127.1.1.2"),
        patch("managers.sentinel.SentinelManager.restart_service") as mock_restart_sentinel,
        patch(
            "workload_vm.ValkeyVmWorkload.exec",
            return_value=("DNS:www.example.com, IP Address:127.1.1.1",),
        ),
        patch("workload_vm.ValkeyVmWorkload.restart") as mock_workload_restart,
        patch("managers.tls.TLSManager.build_sans_ip", return_value=frozenset({"127.0.1.1"})),
        patch(
            "managers.tls.TLSManager.build_sans_dns", return_value=frozenset({"www.example.com"})
        ),
        patch("events.base_events.BaseEvents._update_internal_users_password"),
        patch(
            "managers.tls.TLSManager.create_and_store_self_signed_certificate"
        ) as mock_create_certificate,
        patch("managers.cluster.ClusterManager.is_healthy", return_value=True),
        patch("managers.sentinel.SentinelManager.is_healthy", return_value=True),
        patch("managers.cluster.ClusterManager.reconcile_min_replicas_to_write"),
    ):
        ctx.run(ctx.on.config_changed(), state_in)
        mock_create_certificate.assert_called_once()
        mock_restart_sentinel.assert_called_once()
        mock_workload_restart.assert_called_once()


def test_change_password_secret_changed_non_leader_unit(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "private-ip": "127.0.1.0"},
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    password_secret = testing.Secret(
        label=f"{PEER_RELATION}.{APP_NAME}.app.{INTERNAL_USERS_SECRET_LABEL_SUFFIX}",
        tracked_content={CharmUsers.VALKEY_ADMIN.value: "secure-password"},
        remote_grants={1: [APP_NAME]},
    )

    state_in = testing.State(
        leader=False,
        relations={relation},
        containers={container},
        secrets={password_secret},
        config={INTERNAL_USERS_PASSWORD_CONFIG: password_secret.id},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch(
            "events.base_events.BaseEvents._update_internal_users_password"
        ) as mock_update_password,
        patch("managers.auth.AuthManager.set_acl_file") as mock_set_acl_file,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch("common.client.ValkeyClient.acl_load") as mock_acl_load,
        patch("common.client.ValkeyClient.config_set") as mock_config_set,
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="127.0.1.1"),
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        ctx.run(ctx.on.secret_changed(password_secret), state_in)
        mock_update_password.assert_not_called()
        mock_set_acl_file.assert_called_once()
        mock_acl_load.assert_called_once()
        mock_config_set.assert_called_once()
        set_sentinel_acl_file.assert_called_once()
        restart_sentinel.assert_called_once()


def test_change_password_secret_changed_non_leader_unit_not_successful(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1, endpoint=PEER_RELATION, local_unit_data={"start-state": "started"}
    )
    statuses_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)

    password_secret = testing.Secret(
        label=f"{PEER_RELATION}.{APP_NAME}.app.{INTERNAL_USERS_SECRET_LABEL_SUFFIX}",
        tracked_content={CharmUsers.VALKEY_ADMIN.value: "secure-password"},
        remote_grants={1: [APP_NAME]},
    )

    state_in = testing.State(
        leader=False,
        relations={relation, statuses_peer_relation},
        containers={container},
        secrets={password_secret},
        config={INTERNAL_USERS_PASSWORD_CONFIG: password_secret.id},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch(
            "events.base_events.BaseEvents._update_internal_users_password"
        ) as mock_update_password,
        patch("managers.auth.AuthManager.set_acl_file") as mock_set_acl_file,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch(
            "common.client.ValkeyClient.exec_cli_command",
            side_effect=ValkeyWorkloadCommandError("Failed to execute command"),
        ) as mock_exec_command,
        ctx(ctx.on.secret_changed(password_secret), state_in) as manager,
    ):
        charm: ValkeyCharm = manager.charm
        state_out = manager.run()
        mock_update_password.assert_not_called()
        mock_set_acl_file.assert_called_once()
        set_sentinel_acl_file.assert_called_once()
        mock_exec_command.assert_called_once_with(
            ["acl", "load"], hostname="valkey-0.valkey-endpoints"
        )
        cluster_statuses = charm.state.statuses.get(
            scope="unit",
            component=charm.cluster_manager.name,
        )
        assert "secret_changed" in [e.name for e in state_out.deferred]
        assert ClusterStatuses.PASSWORD_UPDATE_FAILED.value in cluster_statuses


def test_change_password_secret_changed_leader_unit(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)

    password_secret = testing.Secret(
        tracked_content={CharmUsers.VALKEY_ADMIN.value: "secure-password"},
        remote_grants={1: [APP_NAME]},
    )

    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={container},
        secrets={password_secret},
        config={INTERNAL_USERS_PASSWORD_CONFIG: password_secret.id},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch(
            "events.base_events.BaseEvents._update_internal_users_password"
        ) as mock_update_password,
    ):
        ctx.run(ctx.on.secret_changed(password_secret), state_in)
        mock_update_password.assert_called_once_with(password_secret.id)


def test_relation_changed_event_leader_setting_starting_member(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
        peers_data={1: {"request-start-lock": "true"}},
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch("managers.tls.TLSManager.will_certificate_expire"),
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation), state_in)
        assert state_out.get_relation(1).local_app_data.get("start-member") == "valkey/1"


def test_relation_changed_event_leader_clears_starting_member(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data={"start-member": "valkey/1"},
        local_unit_data={"start-state": "started"},
        peers_data={1: {"start-state": "started"}},
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch("managers.tls.TLSManager.will_certificate_expire"),
        patch("common.client.SentinelClient.primary", return_value={"quorum": "2"}),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation), state_in)
        assert state_out.get_relation(1).local_app_data.get("start-member") is None


def test_relation_changed_event_leader_leaves_starting_member_as_is(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data={"start-member": "valkey/1"},
        local_unit_data={"start-state": StartState.STARTED.value},
        peers_data={
            1: {
                "start-state": StartState.STARTING_WAITING_REPLICA_SYNC.value,
                "request-start-lock": "true",
            }
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch("managers.tls.TLSManager.will_certificate_expire"),
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation), state_in)
        assert state_out.get_relation(1).local_app_data.get("start-member") == "valkey/1"


def test_relation_changed_event_update_quorum(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data={"start-member": "valkey/1"},
        local_unit_data={"start-state": StartState.STARTED.value},
        peers_data={1: {"start-state": StartState.STARTED.value}},
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
        patch("common.client.SentinelClient.set") as mock_set,
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="127.1.0.1"),
    ):
        ctx.run(ctx.on.relation_changed(relation), state_in)
        mock_set.assert_called_once_with("valkey-0.valkey-endpoints", PRIMARY_NAME, "quorum", "2")


def test_relation_changed_event_do_not_update_quorum(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
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
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch("common.client.SentinelClient.primary", return_value={"quorum": "2"}),
        patch("common.client.SentinelClient.set") as mock_set,
    ):
        ctx.run(ctx.on.relation_changed(relation), state_in)
        mock_set.assert_not_called()
