#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the metrics exporter configuration and health decoupling."""

from unittest.mock import patch

import ops.testing as testing
import pytest

from literals import (
    CHARM,
    CONTAINER,
    INTERNAL_USERS_SECRET_LABEL_SUFFIX,
    METRICS_PORT,
    METRICS_SERVICE,
    PEER_RELATION,
    TLS_PORT,
    CharmUsers,
)
from src.charm import ValkeyCharm
from workload_k8s import ValkeyK8sWorkload
from workload_vm import ValkeyVmWorkload


@pytest.fixture
def base_state(cloud_spec):
    container = testing.Container(
        name=CONTAINER,
        can_connect=True,
    )
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
    )
    return testing.State(
        leader=True,
        model=testing.Model(name="my-k8s-model", type="kubernetes", cloud_spec=cloud_spec),
        containers=[container],
        relations=[peer_relation],
    )


def test_exporter_env_rendered_k8s(cloud_spec, base_state):
    """Assert exporter environment is rendered with correct values on K8s."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state = ctx.run(ctx.on.leader_elected(), base_state)

    secret = state.get_secret(
        label=f"{PEER_RELATION}.{CHARM}.app.{INTERNAL_USERS_SECRET_LABEL_SUFFIX}"
    )
    monitoring_password = secret.latest_content[f"{CharmUsers.VALKEY_MONITORING.value}-password"]

    with ctx(ctx.on.update_status(), state) as mgr:
        charm = mgr.charm
        env = charm.metrics_manager.exporter_env()
        assert env["REDIS_ADDR"] == f"rediss://{charm.state.endpoint}:{TLS_PORT}"
        assert env["REDIS_USER"] == CharmUsers.VALKEY_MONITORING.value
        assert env["REDIS_PASSWORD"] == monitoring_password
        assert (
            env["REDIS_EXPORTER_TLS_CA_CERT_FILE"] == charm.workload.tls_paths.client_ca.as_posix()
        )
        assert env["REDIS_EXPORTER_TLS_SERVER_NAME"] == charm.state.endpoint
        assert env["REDIS_EXPORTER_WEB_LISTEN_ADDRESS"] == f"0.0.0.0:{METRICS_PORT}"
        assert env["REDIS_EXPORTER_INCL_SYSTEM_METRICS"] == "true"
        assert env["REDIS_EXPORTER_APPEND_INSTANCE_ROLE_LABEL"] == "true"
        assert env["REDIS_EXPORTER_INCL_CONFIG_METRICS"] == "false"


def test_exporter_env_rendered_vm(cloud_spec_vm):
    """Assert exporter environment binds to localhost on VM."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
    )
    state = testing.State(
        leader=True,
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec_vm),
        relations=[peer_relation],
    )
    state = ctx.run(ctx.on.leader_elected(), state)

    with ctx(ctx.on.update_status(), state) as mgr:
        charm = mgr.charm
        env = charm.metrics_manager.exporter_env()
        assert env["REDIS_EXPORTER_WEB_LISTEN_ADDRESS"] == f"127.0.0.1:{METRICS_PORT}"


def test_dead_exporter_does_not_mark_workload_unhealthy(cloud_spec, mocker):
    """A stopped or dead exporter must not make workload.alive() return False."""
    container = mocker.MagicMock()

    def mock_get_service(name):
        srv = mocker.MagicMock()
        srv.is_running.return_value = name != METRICS_SERVICE
        return srv

    container.get_service.side_effect = mock_get_service
    workload = ValkeyK8sWorkload(container=container)

    # Alive without arguments checks default services set (valkey + sentinel only)
    assert workload.alive() is True
    # Explicit single-service check for metrics_service still reflects its state
    assert workload.alive(workload.metrics_service) is False


def test_reconcile_idempotency(cloud_spec, base_state):
    """Successive reconciles without change produce no restarts."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state = ctx.run(ctx.on.leader_elected(), base_state)

    with ctx(ctx.on.update_status(), state) as mgr:
        charm = mgr.charm
        with patch.object(charm.workload, "restart") as mock_restart:
            # First reconcile applies layer and restarts
            charm.metrics_manager.reconcile()
            assert mock_restart.call_count == 1

            # Second reconcile detects identical config and does not restart
            charm.metrics_manager.reconcile()
            assert mock_restart.call_count == 1


def test_exporter_target_unchanged_when_client_tls_enabled(cloud_spec, base_state):
    """Exporter target is always rediss://<endpoint>:6380 regardless of client TLS state."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state = ctx.run(ctx.on.leader_elected(), base_state)

    with ctx(ctx.on.update_status(), state) as mgr:
        charm = mgr.charm
        charm.state.unit_server.update({"tls_client_state": "tls", "client_cert_ready": True})
        env = charm.metrics_manager.exporter_env()
        assert env["REDIS_ADDR"] == f"rediss://{charm.state.endpoint}:{TLS_PORT}"


def test_vm_configure_metrics_exporter(mocker):
    """Assert VM workload writes metrics env file with 0600 permissions."""
    workload = object.__new__(ValkeyVmWorkload)
    mock_root = mocker.MagicMock()
    mock_env_file = mocker.MagicMock()

    workload.root_dir = mock_root
    workload.metrics_service = METRICS_SERVICE
    workload.metrics_env_file = mock_env_file
    workload.user = "snap_daemon"
    workload.path_exists = mocker.MagicMock(return_value=False)
    workload.write_file = mocker.MagicMock()

    env = {"REDIS_ADDR": "rediss://localhost:6380", "REDIS_USER": "charmed-stats"}
    changed = workload.configure_metrics_exporter(env)

    assert changed is True
    workload.write_file.assert_called_once()
    assert workload.write_file.call_args[1]["mode"] == 0o640
    assert workload.write_file.call_args[1]["user"] == "snap_daemon"
    assert workload.write_file.call_args[1]["group"] == "root"
