#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from hashlib import sha256
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from charmlibs.interfaces.tls_certificates import (
    CertificateAvailableEvent,
    CertificateDeniedEvent,
    CertificateError,
    PrivateKey,
    ProviderCertificate,
    RequirerCertificateRequest,
)
from ops import testing

from common.exceptions import ValkeyTLSLoadError
from src.charm import ValkeyCharm
from src.common.exceptions import ValkeyWorkloadCommandError
from src.literals import (
    CLIENT_TLS_RELATION_NAME,
    INTERNAL_CERTS_SECRET_LABEL_SUFFIX,
    PEER_RELATION,
    STATUS_PEERS_RELATION,
    TLS_CLIENT_PRIVATE_KEY_CONFIG,
    TLSCARotationState,
)
from src.statuses import TLSStatuses

from .helpers import status_is

CONTAINER = "valkey"

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_client_tls_relation_created():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    state_out = ctx.run(ctx.on.relation_created(relation=client_tls_relation), state_in)
    assert status_is(state_out, TLSStatuses.ENABLING_CLIENT_TLS.value)


def test_enable_internal_tls_by_default():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with patch("charmlibs.pathops.ContainerPath.mkdir"):
        state_out = ctx.run(ctx.on.relation_created(relation=peer_relation), state_in)
        secret_out = state_out.get_secret(
            label=f"{PEER_RELATION}.{APP_NAME}.app.{INTERNAL_CERTS_SECRET_LABEL_SUFFIX}"
        )
        assert secret_out.latest_content.get("internal-ca-certificate")
        assert secret_out.latest_content.get("internal-ca-private-key")


def test_enable_internal_tls_no_ca_cert_available():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with patch("managers.tls.TLSManager.create_and_store_self_signed_certificate") as create_certs:
        state_out = ctx.run(ctx.on.relation_created(relation=peer_relation), state_in)
        create_certs.assert_not_called()
        assert "valkey_peers_relation_created" in [e.name for e in state_out.deferred]


def test_client_tls_relation_broken():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "client-cert-ready": "true",
        },
        local_app_data={},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch("managers.tls.TLSManager.rehash_ca_certificates"),
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("managers.sentinel.SentinelManager.restart_service"),
        patch(
            "common.client.SentinelClient.get_primary_addr_by_name",
            return_value=("10.0.1.1", 6379),
        ),
    ):
        state_out = ctx.run(ctx.on.relation_broken(relation=client_tls_relation), state_in)
        assert reload_tls.call_count == 2
        assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "false"
        assert state_out.get_relation(1).local_unit_data.get("tls-client-state") == "no-tls"

        secret_out = state_out.get_secret(
            label=f"{PEER_RELATION}.{APP_NAME}.app.{INTERNAL_CERTS_SECRET_LABEL_SUFFIX}"
        )
        assert secret_out.latest_content.get("internal-ca-certificate")
        assert secret_out.latest_content.get("internal-ca-private-key")


def test_client_tls_relation_broken_disabling_tls_fails():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "client-cert-ready": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch("managers.tls.TLSManager.rehash_ca_certificates"),
        patch(
            "managers.config.ConfigManager.set_config_properties", side_effect=ValueError("failed")
        ),
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch(
            "common.client.SentinelClient.get_primary_addr_by_name",
            return_value=("10.0.1.1", 6379),
        ),
    ):
        state_out = ctx.run(ctx.on.relation_broken(relation=client_tls_relation), state_in)
        reload_tls.assert_not_called()
        assert "client_certificates_relation_broken" in [e.name for e in state_out.deferred]
        assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "true"
        assert state_out.get_relation(1).local_unit_data.get("tls-client-state") == "to-no-tls"


def test_client_tls_relation_broken_no_internal_ca_cert_available():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "client-cert-ready": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
    ):
        state_out = ctx.run(ctx.on.relation_broken(relation=client_tls_relation), state_in)
        reload_tls.assert_not_called()
        assert "client_certificates_relation_broken" in [e.name for e in state_out.deferred]
        assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "true"
        assert state_out.get_relation(1).local_unit_data.get("tls-client-state") == "tls"


def test_client_tls_relation_broken_writing_internal_cert_fails():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "client-cert-ready": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch("core.base_workload.WorkloadBase.write_file", side_effect=PermissionError("failed")),
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("managers.sentinel.SentinelManager.restart_service"),
        patch(
            "common.client.SentinelClient.get_primary_addr_by_name",
            return_value=("10.0.1.1", 6379),
        ),
    ):
        state_out = ctx.run(ctx.on.relation_broken(relation=client_tls_relation), state_in)
        reload_tls.assert_called_once()
        assert "client_certificates_relation_broken" in [e.name for e in state_out.deferred]
        assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "false"
        assert state_out.get_relation(1).local_unit_data.get("tls-client-state") == "no-tls"


def test_client_certificate_denied():
    csr = MagicMock("my_csr")

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "to-tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    requirer_certificate_request = RequirerCertificateRequest(
        relation_id=3, certificate_signing_request=csr, is_ca=False
    )
    certificate_error = CertificateError(
        code=101, name="IP not allowed", message="IP address not allowed"
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=CertificateDeniedEvent)

        with (
            patch(
                "charmlibs.interfaces.tls_certificates.TLSCertificatesRequiresV4.get_csrs_from_requirer_relation_data",
                return_value=[requirer_certificate_request],
            ),
            patch("managers.tls.TLSManager.will_certificate_expire"),
        ):
            event.certificate_signing_request = csr
            event.error = certificate_error
            charm.tls_events._on_certificate_denied(event)
            state_out = manager.run()

            status_is(state_out, TLSStatuses.CERTIFICATE_DENIED.value)


def test_client_certificate_available():
    ca = MagicMock("my_ca")
    ca.raw = "my_ca"
    csr = MagicMock("my_csr")
    csr.raw = "my_csr"
    cert = MagicMock("my_cert")
    cert.raw = "my_cert"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "to-tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    certificate = ProviderCertificate(
        relation_id=3, certificate=cert, certificate_signing_request=csr, ca=ca, chain=[cert, ca]
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=CertificateAvailableEvent)

        with (
            patch(
                "charmlibs.interfaces.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
                return_value=([certificate], None),
            ),
            patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
            patch("managers.sentinel.SentinelManager.restart_service"),
            patch("managers.tls.TLSManager.write_certificate"),
            patch("managers.tls.TLSManager.will_certificate_expire"),
            patch(
                "common.client.SentinelClient.get_primary_addr_by_name",
                return_value=("10.0.1.1", 6379),
            ),
        ):
            event.certificate = certificate.certificate
            charm.tls_events._on_certificate_available(event)
            state_out = manager.run()

            reload_tls.assert_called_once()
            assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "true"
            assert state_out.get_relation(1).local_unit_data.get("tls-client-state") == "tls"


def test_client_certificate_available_enabling_fails():
    ca = MagicMock("my_ca")
    csr = MagicMock("my_csr")
    cert = MagicMock("my_cert")

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "to-tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    certificate = ProviderCertificate(
        relation_id=3, certificate=cert, certificate_signing_request=csr, ca=ca, chain=[cert, ca]
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=CertificateAvailableEvent)

        with (
            patch(
                "charmlibs.interfaces.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
                return_value=([certificate], None),
            ),
            patch(
                "managers.config.ConfigManager.set_config_properties",
                side_effect=ValueError("failed"),
            ),
            patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
            patch("managers.tls.TLSManager.write_certificate"),
            patch("managers.tls.TLSManager.will_certificate_expire"),
            patch(
                "common.client.SentinelClient.get_primary_addr_by_name",
                return_value=("10.0.1.1", 6379),
            ),
        ):
            event.certificate = certificate.certificate
            charm.tls_events._on_certificate_available(event)
            state_out = manager.run()

            reload_tls.assert_not_called()
            assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "true"
            assert state_out.get_relation(1).local_unit_data.get("tls-client-state") == "to-tls"


def test_client_certificate_available_not_all_units_ready():
    ca = MagicMock("my_ca")
    client_csr = MagicMock("my_csr")
    client_cert = MagicMock("my_cert")

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "to-tls",
        },
        peers_data={1: {"start-state": "started", "client-cert-ready": "False"}},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=4,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    client_certificate = ProviderCertificate(
        relation_id=4,
        certificate=client_cert,
        certificate_signing_request=client_csr,
        ca=ca,
        chain=[client_cert, ca],
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        planned_units=2,
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=CertificateAvailableEvent)

        with (
            patch(
                "charmlibs.interfaces.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
                side_effect=[([client_certificate], None)],
            ),
            patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
            patch("managers.tls.TLSManager.write_certificate"),
            patch("managers.tls.TLSManager.will_certificate_expire"),
        ):
            event.certificate = client_certificate.certificate
            charm.tls_events._on_certificate_available(event)
            state_out = manager.run()

            reload_tls.assert_not_called()
            assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "true"
            assert state_out.get_relation(1).local_unit_data.get("tls-client-state") == "to-tls"


def test_check_certificate_expiration():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    # exec returns (stdout, stderr); the openssl -checkend path only cares that it
    # doesn't raise, and the sentinel down-after self-heal on update-status parses
    # stdout as JSON.
    with patch("workload_k8s.ValkeyK8sWorkload.exec", return_value=("{}", None)):
        state_out = ctx.run(ctx.on.update_status(), state_in)
        assert not state_out.get_relation(1).local_unit_data.get("tls-certificate-expiring")
        assert not status_is(state_out, TLSStatuses.CERTIFICATE_EXPIRING.value)

    with (
        patch(
            "core.base_workload.WorkloadBase.exec",
            side_effect=ValkeyWorkloadCommandError("failed"),
        ),
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)
        reload_tls.assert_not_called()
        assert state_out.get_relation(1).local_unit_data.get("tls-certificate-expiring") == "true"
        assert status_is(state_out, TLSStatuses.CERTIFICATE_EXPIRING.value)


def test_client_certificate_renewed():
    # Mock the certificate values that are in the relation databag otherwise
    ca = MagicMock("my_ca")
    ca.raw = "my_ca"
    csr = MagicMock("my_csr")
    csr.raw = "my_csr"
    cert = MagicMock("my_cert")
    cert.raw = "my_cert"
    private_key = MagicMock("my_key")
    private_key.raw = "my_key"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    certificate = ProviderCertificate(
        relation_id=3, certificate=cert, certificate_signing_request=csr, ca=ca, chain=[cert, ca]
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=CertificateAvailableEvent)

        with (
            patch(
                "charmlibs.interfaces.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
                return_value=([certificate], private_key),
            ),
            patch("charmlibs.pathops.ContainerPath.mkdir"),
            patch("charmlibs.pathops.ContainerPath.exists", return_value=True),
            patch("charmlibs.pathops.ContainerPath.read_text", return_value="my_ca"),
            patch("charmlibs.pathops.ContainerPath.write_text"),
            patch("workload_k8s.ValkeyK8sWorkload.write_file") as write_certs,
            patch("managers.tls.TLSManager.rehash_ca_certificates"),
            patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("workload_k8s.ValkeyK8sWorkload.restart"),
        ):
            event.certificate = certificate.certificate
            charm.tls_events._on_certificate_available(event)
            state_out = manager.run()

            # we store the cert, the key and the ca cert
            assert write_certs.call_count == 3
            reload_tls.assert_called_once()
            assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "true"
            assert (
                state_out.get_relation(1).local_unit_data.get("applied-client-cert-fingerprint")
                == sha256(b"my_certmy_ca").hexdigest()
            )


def test_certificate_re_emitted_unchanged_skips_restart():
    # The tls lib re-emits certificate-available for an unchanged certificate on
    # every relation-changed of the client-certificates relation (shared across
    # units, so every peer's renewal fires it); an already-applied certificate
    # must be a no-op, or Sentinel gets restarted on every re-emission.
    ca = MagicMock("my_ca")
    ca.raw = "my_ca"
    csr = MagicMock("my_csr")
    csr.raw = "my_csr"
    cert = MagicMock("my_cert")
    cert.raw = "my_cert"
    private_key = MagicMock("my_key")
    private_key.raw = "my_key"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "client-cert-ready": "true",
            "applied-client-cert-fingerprint": sha256(b"my_certmy_ca").hexdigest(),
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    certificate = ProviderCertificate(
        relation_id=3, certificate=cert, certificate_signing_request=csr, ca=ca, chain=[cert, ca]
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=CertificateAvailableEvent)

        with (
            patch(
                "charmlibs.interfaces.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
                return_value=([certificate], private_key),
            ),
            patch("charmlibs.pathops.ContainerPath.mkdir"),
            patch("charmlibs.pathops.ContainerPath.exists", return_value=True),
            patch("charmlibs.pathops.ContainerPath.read_text", return_value="my_ca"),
            patch("workload_k8s.ValkeyK8sWorkload.write_file") as write_certs,
            patch("managers.tls.TLSManager.rehash_ca_certificates") as rehash,
            patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
            patch("workload_k8s.ValkeyK8sWorkload.restart") as restart,
        ):
            event.certificate = certificate.certificate
            charm.tls_events._on_certificate_available(event)
            manager.run()

            write_certs.assert_not_called()
            rehash.assert_not_called()
            reload_tls.assert_not_called()
            restart.assert_not_called()
            event.defer.assert_not_called()


def test_failed_tls_reload_does_not_record_fingerprint():
    # A deferred apply must stay retryable: the fingerprint is only recorded
    # once reload+restart has been handed off, so a redelivered event after a
    # failed reload still does the work instead of being skipped.
    ca = MagicMock("my_ca")
    ca.raw = "my_ca"
    csr = MagicMock("my_csr")
    csr.raw = "my_csr"
    cert = MagicMock("my_cert")
    cert.raw = "my_cert"
    private_key = MagicMock("my_key")
    private_key.raw = "my_key"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    certificate = ProviderCertificate(
        relation_id=3, certificate=cert, certificate_signing_request=csr, ca=ca, chain=[cert, ca]
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=CertificateAvailableEvent)

        with (
            patch(
                "charmlibs.interfaces.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
                return_value=([certificate], private_key),
            ),
            patch("charmlibs.pathops.ContainerPath.mkdir"),
            patch("charmlibs.pathops.ContainerPath.exists", return_value=True),
            patch("charmlibs.pathops.ContainerPath.read_text", return_value="my_ca"),
            patch("charmlibs.pathops.ContainerPath.write_text"),
            patch("workload_k8s.ValkeyK8sWorkload.write_file"),
            patch("managers.tls.TLSManager.rehash_ca_certificates"),
            patch(
                "managers.cluster.ClusterManager.reload_tls_settings",
                side_effect=ValkeyTLSLoadError("reload failed"),
            ),
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("workload_k8s.ValkeyK8sWorkload.restart"),
        ):
            event.certificate = certificate.certificate
            charm.tls_events._on_certificate_available(event)
            state_out = manager.run()

            event.defer.assert_called_once()
            assert (
                state_out.get_relation(1).local_unit_data.get("applied-client-cert-fingerprint")
                is None
            )


def test_new_client_ca_single_unit():
    # Mock the certificate values that are in the relation databag otherwise
    ca = MagicMock("my_new_ca")
    ca.raw = "my_new_ca"
    csr = MagicMock("my_csr")
    csr.raw = "my_csr"
    cert = MagicMock("my_cert")
    cert.raw = "my_cert"
    private_key = MagicMock("my_key")
    private_key.raw = "my_key"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    certificate = ProviderCertificate(
        relation_id=3, certificate=cert, certificate_signing_request=csr, ca=ca, chain=[cert, ca]
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=CertificateAvailableEvent)

        with (
            patch(
                "charmlibs.interfaces.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
                return_value=([certificate], private_key),
            ),
            patch("charmlibs.pathops.ContainerPath.exists", return_value=True),
            patch("charmlibs.pathops.ContainerPath.read_text", return_value="my_old_ca"),
            patch("charmlibs.pathops.ContainerPath.write_text"),
            patch("workload_k8s.ValkeyK8sWorkload.write_file") as write_certs,
            patch("managers.tls.TLSManager.rehash_ca_certificates"),
            patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("workload_k8s.ValkeyK8sWorkload.restart"),
        ):
            event.certificate = certificate.certificate
            charm.tls_events._on_certificate_available(event)
            state_out = manager.run()

            # we copy the old ca and then store the cert, the key and the ca cert
            assert write_certs.call_count == 4
            assert reload_tls.call_count == 2
            assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "true"
            assert (
                state_out.get_relation(1).local_unit_data.get("tls-ca-rotation")
                == TLSCARotationState.NO_ROTATION.value
            )


def test_new_client_ca_rotation_started():
    # Mock the certificate values that are in the relation databag otherwise
    ca = MagicMock("my_new_ca")
    ca.raw = "my_new_ca"
    csr = MagicMock("my_csr")
    csr.raw = "my_csr"
    cert = MagicMock("my_cert")
    cert.raw = "my_cert"
    private_key = MagicMock("my_key")
    private_key.raw = "my_key"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
        peers_data={1: {"start-state": "started", "tls-client-state": "tls"}},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    certificate = ProviderCertificate(
        relation_id=3, certificate=cert, certificate_signing_request=csr, ca=ca, chain=[cert, ca]
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=CertificateAvailableEvent)

        with (
            patch(
                "charmlibs.interfaces.tls_certificates.TLSCertificatesRequiresV4.get_assigned_certificates",
                return_value=([certificate], private_key),
            ),
            patch("charmlibs.pathops.ContainerPath.exists", return_value=True),
            patch("charmlibs.pathops.ContainerPath.read_text", return_value="my_old_ca"),
            patch("charmlibs.pathops.ContainerPath.write_text"),
            patch("workload_k8s.ValkeyK8sWorkload.write_file") as write_certs,
            patch("managers.tls.TLSManager.rehash_ca_certificates"),
            patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        ):
            event.certificate = certificate.certificate
            charm.tls_events._on_certificate_available(event)
            state_out = manager.run()

            # we copy the old ca and then store the cert, the key and the ca cert
            assert write_certs.call_count == 4
            reload_tls.assert_not_called()
            assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "true"
            assert (
                state_out.get_relation(1).local_unit_data.get("tls-ca-rotation")
                == TLSCARotationState.NEW_CA_ADDED.value
            )


def test_internal_peer_ca_rotation_single_unit():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.tls.TLSManager.will_certificate_expire", return_value=True),
        patch("managers.tls.TLSManager.generate_ca_certificate") as generate_ca,
        patch("core.models.ValkeyCluster.internal_ca_certificate", return_value="my_new_ca"),
        patch("managers.tls.TLSManager.create_and_store_self_signed_certificate") as create_certs,
        patch("charmlibs.pathops.ContainerPath.exists", return_value=True),
        patch("charmlibs.pathops.ContainerPath.read_text", return_value="my_old_ca"),
        patch("charmlibs.pathops.ContainerPath.write_text"),
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch("managers.tls.TLSManager.rehash_ca_certificates"),
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("managers.sentinel.SentinelManager.restart_service"),
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.relation_changed(peer_relation, remote_unit=1), state_in)

        create_certs.assert_called_once()
        generate_ca.assert_called_once()
        assert reload_tls.call_count == 2
        assert (
            state_out.get_relation(1).local_unit_data.get("tls-ca-rotation")
            == TLSCARotationState.NO_ROTATION.value
        )


def test_internal_peer_ca_rotation_started():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
        peers_data={1: {"start-state": "started", "tls-client-state": "tls"}},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.tls.TLSManager.will_certificate_expire", return_value=True),
        patch("managers.tls.TLSManager.generate_ca_certificate") as generate_ca,
        patch("core.models.ValkeyCluster.internal_ca_certificate", return_value="my_new_ca"),
        patch("managers.tls.TLSManager.create_and_store_self_signed_certificate") as create_certs,
        patch("charmlibs.pathops.ContainerPath.exists", return_value=True),
        patch("charmlibs.pathops.ContainerPath.read_text", return_value="my_old_ca"),
        patch("charmlibs.pathops.ContainerPath.write_text"),
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch("managers.tls.TLSManager.rehash_ca_certificates"),
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("managers.sentinel.SentinelManager.restart_service"),
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.relation_changed(peer_relation, remote_unit=1), state_in)

        create_certs.assert_called_once()
        generate_ca.assert_called_once()
        reload_tls.assert_not_called()
        assert (
            state_out.get_relation(1).local_unit_data.get("tls-ca-rotation")
            == TLSCARotationState.NEW_CA_ADDED.value
        )


def test_ca_rotation_not_all_units_added():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "tls-ca-rotation": "new-ca-added",
        },
        peers_data={
            1: {
                "start-state": "started",
                "tls-client-state": "tls",
            }
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        planned_units=2,
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with (
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.relation_changed(peer_relation), state_in)

        # we copy the old ca and then store the cert, the key and the ca cert
        reload_tls.assert_not_called()
        assert (
            state_out.get_relation(1).local_unit_data.get("tls-ca-rotation")
            == TLSCARotationState.NEW_CA_ADDED.value
        )


def test_ca_rotation_all_units_added():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "tls-ca-rotation": "new-ca-added",
        },
        peers_data={
            1: {
                "start-state": "started",
                "tls-client-state": "tls",
                "tls-ca-rotation": "new-ca-added",
            }
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        planned_units=2,
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with (
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("managers.sentinel.SentinelManager.restart_service"),
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.relation_changed(peer_relation), state_in)

        # we copy the old ca and then store the cert, the key and the ca cert
        reload_tls.assert_called_once()
        assert (
            state_out.get_relation(1).local_unit_data.get("tls-ca-rotation")
            == TLSCARotationState.CA_UPDATED.value
        )


def test_ca_rotation_not_all_units_ca_updated():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "tls-ca-rotation": "ca-updated",
        },
        peers_data={
            1: {
                "start-state": "started",
                "tls-client-state": "tls",
                "tls-ca-rotation": "ca-added",
            }
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        planned_units=2,
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with (
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.relation_changed(peer_relation), state_in)

        # we copy the old ca and then store the cert, the key and the ca cert
        reload_tls.assert_not_called()
        assert (
            state_out.get_relation(1).local_unit_data.get("tls-ca-rotation")
            == TLSCARotationState.CA_UPDATED.value
        )


def test_ca_rotation_all_units_ca_updated():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "tls-ca-rotation": "ca-updated",
        },
        peers_data={
            1: {
                "start-state": "started",
                "tls-client-state": "tls",
                "tls-ca-rotation": "ca-updated",
            }
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        planned_units=2,
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with (
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("managers.sentinel.SentinelManager.restart_service"),
        patch("managers.tls.TLSManager.rehash_ca_certificates"),
        patch("common.client.SentinelClient.primary", return_value={"quorum": "1"}),
    ):
        state_out = ctx.run(ctx.on.relation_changed(peer_relation), state_in)

        # we copy the old ca and then store the cert, the key and the ca cert
        reload_tls.assert_called_once()
        assert (
            state_out.get_relation(1).local_unit_data.get("tls-ca-rotation")
            == TLSCARotationState.NO_ROTATION.value
        )


def test_private_key_without_client_tls():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)

    private_key = PrivateKey.generate()
    secret = testing.Secret(
        {"private-key": private_key.raw},
        label=TLS_CLIENT_PRIVATE_KEY_CONFIG,
    )

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation},
        secrets={secret},
        config={TLS_CLIENT_PRIVATE_KEY_CONFIG: secret.id},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)
    secret_out = state_out.get_secret(label=f"{PEER_RELATION}.{APP_NAME}.app")
    assert secret_out.latest_content.get("tls-client-private-key") == private_key.raw
    assert status_is(state_out, TLSStatuses.PRIVATE_KEY_BUT_NO_TLS.value)


def test_invalid_private_key():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    private_key = "invalid-private-key"
    secret = testing.Secret(
        {"private-key": private_key},
        label=TLS_CLIENT_PRIVATE_KEY_CONFIG,
    )

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        secrets={secret},
        config={TLS_CLIENT_PRIVATE_KEY_CONFIG: secret.id},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with patch("workload_k8s.ValkeyK8sWorkload.exec"):
        state_out = ctx.run(ctx.on.config_changed(), state_in)
        # ensure the secret was not populated with the invalid private key
        with pytest.raises(KeyError):
            state_out.get_secret(label=f"{PEER_RELATION}.{APP_NAME}.app")
        assert status_is(state_out, TLSStatuses.PRIVATE_KEY_INVALID.value)


def test_private_key_refreshes_certificate():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    private_key = PrivateKey.generate()
    user_secret = testing.Secret(
        {"private-key": private_key.raw},
        label=TLS_CLIENT_PRIVATE_KEY_CONFIG,
    )

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        secrets={user_secret},
        config={TLS_CLIENT_PRIVATE_KEY_CONFIG: user_secret.id},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with patch("workload_k8s.ValkeyK8sWorkload.exec"):
        state_out = ctx.run(ctx.on.config_changed(), state_in)
        secret_out = state_out.get_secret(label=f"{PEER_RELATION}.{APP_NAME}.app")
        assert secret_out.latest_content.get("tls-client-private-key") == private_key.raw
        assert ctx.emitted_events[1].handle.kind == "refresh_tls_certificates_event"


def test_private_key_secret_changed():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    private_key = PrivateKey.generate()
    user_secret = testing.Secret(
        {"private-key": private_key.raw},
        label=TLS_CLIENT_PRIVATE_KEY_CONFIG,
    )

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        secrets={user_secret},
        config={TLS_CLIENT_PRIVATE_KEY_CONFIG: user_secret.id},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    state_out = ctx.run(ctx.on.secret_changed(secret=user_secret), state_in)
    secret_out = state_out.get_secret(label=f"{PEER_RELATION}.{APP_NAME}.app")
    assert secret_out.latest_content.get("tls-client-private-key") == private_key.raw
    assert ctx.emitted_events[1].handle.kind == "refresh_tls_certificates_event"


def test_set_extra_sans_config_option():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        relations={peer_relation, status_peer_relation, client_tls_relation},
        config={"certificate-extra-sans": "192.168.1.100, myhostname"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    current_sans_value = (
        "X509v3 Subject Alternative Name: \n    "
        "DNS:valkey0, DNS:valkey-0.valkey-endpoints, "
        "DNS:valkey-primary, DNS:valkey-replicas, "
        "IP Address:127.1.1.1, IP Address:192.0.2.0"
    )
    with (
        patch("workload_k8s.ValkeyK8sWorkload.exec", return_value=[current_sans_value]),
    ):
        ctx.run(ctx.on.config_changed(), state_in)
        assert ctx.emitted_events[1].handle.kind == "refresh_tls_certificates_event"


def test_set_extra_sans_config_option_unit_placeholder():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        relations={peer_relation, status_peer_relation, client_tls_relation},
        config={
            "certificate-extra-sans": "192.168.1.100, valkey-{unit}.hostname",
        },
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    current_sans_value = (
        "X509v3 Subject Alternative Name: \n    "
        "DNS:myhostname, DNS:valkey0, DNS:valkey-0.valkey-endpoints, "
        "DNS:valkey-primary, DNS:valkey-replicas, "
        "IP Address:127.1.1.1, IP Address:192.168.1.100, IP Address:192.0.2.0"
    )
    with (
        patch("workload_k8s.ValkeyK8sWorkload.exec", return_value=[current_sans_value]),
    ):
        ctx.run(ctx.on.config_changed(), state_in)
        assert ctx.emitted_events[1].handle.kind == "refresh_tls_certificates_event"


def test_set_extra_sans_config_option_invalid_ip():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        relations={peer_relation, status_peer_relation, client_tls_relation},
        config={"certificate-extra-sans": "192.168.257.100, myhostname"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)
    assert status_is(state_out, TLSStatuses.SANS_CONFIG_INVALID.value)
    # no RefreshTLSCertificatesEvent must be emitted
    assert len(ctx.emitted_events) == 1


def test_set_extra_sans_config_option_invalid_dns():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        relations={peer_relation, status_peer_relation, client_tls_relation},
        config={"certificate-extra-sans": "-myhostname"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)
    assert status_is(state_out, TLSStatuses.SANS_CONFIG_INVALID.value)
    # no RefreshTLSCertificatesEvent must be emitted
    assert len(ctx.emitted_events) == 1


def test_set_extra_sans_config_option_special_chars():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        relations={peer_relation, status_peer_relation, client_tls_relation},
        config={"certificate-extra-sans": "192.168.1.100, my$*hostname"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)
    assert status_is(state_out, TLSStatuses.SANS_CONFIG_INVALID.value)
    # no RefreshTLSCertificatesEvent must be emitted
    assert len(ctx.emitted_events) == 1


def test_set_extra_sans_config_option_no_update():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "tls-client-state": "tls"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(
        id=3,
        endpoint=CLIENT_TLS_RELATION_NAME,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        relations={peer_relation, status_peer_relation, client_tls_relation},
        config={"certificate-extra-sans": "192.168.1.100, myhostname"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    current_sans_value = (
        "X509v3 Subject Alternative Name: \n    "
        "DNS:myhostname, DNS:valkey0, DNS:valkey-0.valkey-endpoints, "
        "DNS:valkey-primary, DNS:valkey-replicas, "
        "IP Address:192.168.1.100"
    )
    with (
        patch("workload_k8s.ValkeyK8sWorkload.exec", return_value=[current_sans_value]),
        patch("workload_k8s.ValkeyK8sWorkload.exec", return_value=[current_sans_value]),
    ):
        ctx.run(ctx.on.config_changed(), state_in)
        # no RefreshTLSCertificatesEvent must be emitted
        assert len(ctx.emitted_events) == 1


def test_client_tls_relation_broken_run_deferred_event():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "to-no-tls",
            "client-cert-ready": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.cluster.ClusterManager.reload_tls_settings"),
        patch("managers.sentinel.SentinelManager.restart_service"),
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch(
            "common.client.SentinelClient.get_primary_addr_by_name",
            return_value=("10.0.1.1", 6379),
        ),
    ):
        state_out = ctx.run(ctx.on.relation_broken(relation=client_tls_relation), state_in)
        assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "false"
        assert state_out.get_relation(1).local_unit_data.get("tls-client-state") == "no-tls"


def test_client_tls_relation_broken_not_active_yet():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "to-tls",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    state_out = ctx.run(ctx.on.relation_broken(relation=client_tls_relation), state_in)
    assert state_out.get_relation(1).local_unit_data.get("client-cert-ready") == "false"
    assert state_out.get_relation(1).local_unit_data.get("tls-client-state") == "no-tls"


def test_peer_relation_changed_ca_rotation_workload_error_defers():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "tls-client-state": "tls",
            "tls-ca-rotation": TLSCARotationState.NEW_CA_DETECTED.value,
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with patch(
        "src.events.tls.TLSEvents._orchestrate_ca_rotation",
        side_effect=ValkeyWorkloadCommandError("Pebble down"),
    ):
        state_out = ctx.run(
            ctx.on.relation_changed(relation=peer_relation, remote_unit=1), state_in
        )
    assert "valkey_peers_relation_changed" in [e.name for e in state_out.deferred]


def test_certificate_already_applied_without_peer_relation():
    # A certificate-available can arrive before the peer relation exists, so the
    # idempotency guard must degrade to "not applied" instead of dereferencing the
    # unbuilt unit databag model.
    ca = MagicMock("my_ca")
    ca.raw = "my_ca"
    csr = MagicMock("my_csr")
    csr.raw = "my_csr"
    cert = MagicMock("my_cert")
    cert.raw = "my_cert"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION_NAME)
    certificate = ProviderCertificate(
        relation_id=3, certificate=cert, certificate_signing_request=csr, ca=ca, chain=[cert, ca]
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={status_peer_relation, client_tls_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm

        assert charm.state.unit_server.model is None
        assert charm.tls_manager.certificate_already_applied(certificate) is False
