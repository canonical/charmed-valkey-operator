#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Self-healing reconciliation of a VM unit's address change."""

from contextlib import ExitStack
from unittest.mock import PropertyMock, patch

from ops import testing

from common.custom_events import RefreshTLSCertificatesEvent
from common.exceptions import ValkeyWorkloadCommandError
from src.charm import ValkeyCharm
from src.literals import CLIENT_TLS_RELATION_NAME, PEER_RELATION

CONTAINER = "valkey"

OLD_IP = "127.0.1.1"
# must match the autouse `mock_bind_address` fixture in conftest.py
NEW_IP = "127.1.1.1"


def _peer_relation(private_ip: str = OLD_IP) -> testing.PeerRelation:
    return testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "private-ip": private_ip},
    )


def _state(*, relations: set, leader: bool = True) -> testing.State:
    return testing.State(
        leader=leader,
        relations=relations,
        containers={testing.Container(name=CONTAINER, can_connect=True)},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )


def _client_tls_relation() -> testing.Relation:
    return testing.Relation(
        id=2,
        endpoint=CLIENT_TLS_RELATION_NAME,
        interface="tls-certificates",
        remote_app_name="self-signed-certificates",
    )


def _reconcile_env() -> ExitStack:
    """Patch the workload/manager side effects the address reconcile performs."""
    patches = (
        patch("managers.config.ConfigManager.configure_services"),
        patch("managers.auth.AuthManager.configure_auth"),
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value="127.1.1.2"),
        patch("managers.sentinel.SentinelManager.restart_service"),
        # stands in for `openssl x509 -ext subjectAltName`: the on-disk SANs carry OLD_IP.
        # exec returns (stdout, stderr) -- the second element matters because other
        # update-status work unpacks both.
        patch(
            "workload_vm.ValkeyVmWorkload.exec",
            return_value=(f"DNS:www.example.com, IP Address:{OLD_IP}", None),
        ),
        patch("workload_vm.ValkeyVmWorkload.restart"),
        patch("managers.tls.TLSManager.build_sans_ip", return_value=frozenset({NEW_IP})),
        patch(
            "managers.tls.TLSManager.build_sans_dns", return_value=frozenset({"www.example.com"})
        ),
        patch("managers.tls.TLSManager.will_certificate_expire", return_value=False),
        patch("managers.cluster.ClusterManager.is_healthy", return_value=True),
        patch("managers.sentinel.SentinelManager.is_healthy", return_value=True),
        patch("managers.cluster.ClusterManager.reconcile_min_replicas_to_write"),
        # unrelated update-status self-heal: it would otherwise drive the sentinel
        # CLI through the openssl stub above.
        patch("managers.sentinel.SentinelManager.reconcile_failover_suppression"),
    )
    stack = ExitStack()
    for p in patches:
        stack.enter_context(p)
    return stack


def test_update_status_refreshes_client_certificate_when_ip_changed(vm_environment):
    """update-status must converge a unit whose IP changed while client TLS is enabled."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = _state(relations={_peer_relation(), _client_tls_relation()})

    with (
        _reconcile_env(),
        patch(
            "managers.tls.TLSManager.create_and_store_self_signed_certificate"
        ) as mock_create_certificate,
    ):
        ctx.run(ctx.on.update_status(), state_in)

    # with a provider related, the charm must request a new CSR, not self-sign
    assert any(isinstance(e, RefreshTLSCertificatesEvent) for e in ctx.emitted_events), (
        "update-status did not request a certificate refresh after the unit's IP changed"
    )
    mock_create_certificate.assert_not_called()


def test_update_status_regenerates_self_signed_certificate_when_ip_changed(vm_environment):
    """Without a client TLS provider the charm regenerates the self-signed cert itself."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = _state(relations={_peer_relation()})

    with (
        _reconcile_env(),
        patch(
            "managers.tls.TLSManager.create_and_store_self_signed_certificate"
        ) as mock_create_certificate,
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)

    mock_create_certificate.assert_called_once()
    assert state_out.get_relation(1).local_unit_data["private-ip"] == NEW_IP


def test_update_status_does_not_reconcile_when_ip_unchanged(vm_environment):
    """A unit whose address is unchanged must not reconfigure or reissue certificates."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = _state(relations={_peer_relation(private_ip=NEW_IP)})

    with (
        _reconcile_env(),
        patch(
            "managers.tls.TLSManager.create_and_store_self_signed_certificate"
        ) as mock_create_certificate,
        patch("managers.auth.AuthManager.configure_auth") as mock_configure_auth,
    ):
        ctx.run(ctx.on.update_status(), state_in)

    mock_create_certificate.assert_not_called()
    mock_configure_auth.assert_not_called()
    assert not any(isinstance(e, RefreshTLSCertificatesEvent) for e in ctx.emitted_events)


def test_update_status_does_not_reconcile_before_an_address_is_recorded(vm_environment):
    """A unit that has not recorded an address yet has nothing to reconcile.

    `private-ip` is first written on start, so a config-changed/update-status arriving
    before that must not be read as "the address changed" and drive a reconfigure and
    restart of a unit that has not started.
    """
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = _state(relations={_peer_relation(private_ip="")})

    with (
        _reconcile_env(),
        patch(
            "managers.tls.TLSManager.create_and_store_self_signed_certificate"
        ) as mock_create_certificate,
        patch("managers.auth.AuthManager.configure_auth") as mock_configure_auth,
    ):
        ctx.run(ctx.on.update_status(), state_in)

    mock_create_certificate.assert_not_called()
    mock_configure_auth.assert_not_called()
    assert not any(isinstance(e, RefreshTLSCertificatesEvent) for e in ctx.emitted_events)


def test_update_status_reconciles_on_non_leader_unit(vm_environment):
    """Any unit can be the one whose address changed, so the reconcile precedes the leader check."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = _state(relations={_peer_relation()}, leader=False)

    with (
        _reconcile_env(),
        patch(
            "managers.tls.TLSManager.create_and_store_self_signed_certificate"
        ) as mock_create_certificate,
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)

    mock_create_certificate.assert_called_once()
    assert state_out.get_relation(1).local_unit_data["private-ip"] == NEW_IP


def test_update_status_survives_certificate_read_failure(vm_environment):
    """A failed cert read must not error the unit — update-status runs on every interval."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = _state(relations={_peer_relation()})

    with (
        _reconcile_env(),
        patch(
            "managers.tls.TLSManager.get_current_sans",
            side_effect=ValkeyWorkloadCommandError("cannot read certificate"),
        ),
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)

    assert not state_out.deferred


def test_config_changed_defers_on_certificate_read_failure(vm_environment):
    """config-changed has no periodic retry, so it must defer instead of dropping the reconcile."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = _state(relations={_peer_relation()})

    with (
        _reconcile_env(),
        patch("events.base_events.BaseEvents._update_internal_users_password"),
        patch(
            "managers.tls.TLSManager.get_current_sans",
            side_effect=ValkeyWorkloadCommandError("cannot read certificate"),
        ),
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert [d.name for d in state_out.deferred] == ["config_changed"]


def test_update_status_does_not_defer_while_awaiting_provider_certificate(vm_environment):
    """Waiting on the TLS provider must not build a deferral backlog on a periodic hook."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = _state(relations={_peer_relation(), _client_tls_relation()})

    with _reconcile_env():
        state_out = ctx.run(ctx.on.update_status(), state_in)

    assert any(isinstance(e, RefreshTLSCertificatesEvent) for e in ctx.emitted_events)
    assert not state_out.deferred


def test_update_status_is_noop_on_k8s():
    """K8s uses hostnames, so the address reconcile must not even read the binding."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    state_in = _state(relations={_peer_relation()})

    with (
        _reconcile_env(),
        patch(
            "core.cluster_state.ClusterState.bind_address",
            new_callable=PropertyMock,
            side_effect=ValueError("no binding on k8s"),
        ),
        patch(
            "managers.tls.TLSManager.create_and_store_self_signed_certificate"
        ) as mock_create_certificate,
    ):
        ctx.run(ctx.on.update_status(), state_in)

    mock_create_certificate.assert_not_called()
