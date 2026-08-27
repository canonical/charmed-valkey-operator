#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from ops import testing
from pytest import raises

from charm import ValkeyCharm
from common.exceptions import ValkeyWorkloadCommandError
from lib.charms.glauth_k8s.v0.ldap import LdapReadyEvent, LdapUnavailableEvent
from literals import (
    EXTERNAL_CLIENTS_RELATION,
    LDAP_CA_CERT_RELATION,
    LDAP_RELATION,
    PEER_RELATION,
    STATUS_PEERS_RELATION,
)
from statuses import AuthStatuses

from .helpers import status_is

CONTAINER = "valkey"

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_ldap_new_ca_cert(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_ca_cert_relation = testing.Relation(
        id=3,
        endpoint=LDAP_CA_CERT_RELATION,
        remote_app_data={
            "certificates": '["ldap_ca_certificate", "ldap_intermediate_certificate"]',
            "version": "1",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,  # must be stored on all units
        relations={peer_relation, status_peer_relation, ldap_ca_cert_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with (
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch("workload_k8s.ValkeyK8sWorkload.write_file") as write_ldap_ca,
        patch("managers.tls.TLSManager.rehash_ca_certificates") as rehash_ca_certs,
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
    ):
        ctx.run(ctx.on.relation_changed(relation=ldap_ca_cert_relation), state_in)
        write_ldap_ca.assert_called_once()
        assert (
            write_ldap_ca.call_args.kwargs["content"]
            == "ldap_ca_certificate\nldap_intermediate_certificate"
        )
        rehash_ca_certs.assert_not_called()
        reload_tls.assert_not_called()


def test_ldap_ca_removed(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_ca_cert_relation = testing.Relation(id=3, endpoint=LDAP_CA_CERT_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, ldap_ca_cert_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with (
        patch("workload_k8s.ValkeyK8sWorkload.remove_file") as remove_ldap_ca,
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
    ):
        ctx.run(ctx.on.relation_broken(relation=ldap_ca_cert_relation), state_in)
        remove_ldap_ca.assert_called_once()
        reload_tls.assert_not_called()


def test_ca_available_error_defers(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_ca_cert_relation = testing.Relation(
        id=3,
        endpoint=LDAP_CA_CERT_RELATION,
        remote_app_data={
            "certificates": '["ldap_ca_certificate"]',
            "version": "1",
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, ldap_ca_cert_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with (
        patch(
            "core.base_workload.WorkloadBase.make_dir",
            side_effect=ValkeyWorkloadCommandError("Pebble down"),
        ),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation=ldap_ca_cert_relation), state_in)
    assert "certificate_set_updated" in [e.name for e in state_out.deferred]


def test_ca_removed_error_defers(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_ca_cert_relation = testing.Relation(id=3, endpoint=LDAP_CA_CERT_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, ldap_ca_cert_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    with patch(
        "workload_k8s.ValkeyK8sWorkload.remove_file",
        side_effect=ValkeyWorkloadCommandError("Pebble down"),
    ):
        state_out = ctx.run(ctx.on.relation_broken(relation=ldap_ca_cert_relation), state_in)
    assert "certificates_removed" in [e.name for e in state_out.deferred]


def test_no_ldap_ca_cert_relation(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_relation = testing.Relation(id=3, endpoint=LDAP_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, ldap_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    state_out = ctx.run(ctx.on.relation_changed(relation=ldap_relation), state_in)
    assert status_is(state_out, AuthStatuses.LDAP_CA_CERT_MISSING.value, is_app=True)
    assert not status_is(state_out, AuthStatuses.LDAP_CA_CERT_MISSING.value, is_app=False)


def test_enable_ldap(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                    "entity-permissions": [{"resource_name": "valkey_group", "resource_type": "acl", \
                     "privileges": ["read", "write", "pubsub"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group:valkey_group"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=LdapReadyEvent)

        with (
            patch("charmlibs.pathops.ContainerPath.exists"),
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("managers.config.ConfigManager.set_config_properties") as set_config,
            patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
            patch("managers.auth.AuthManager.set_acl_file") as set_acl,
            patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
        ):
            charm.ldap_events._on_ldap_ready(event)
            state_out = manager.run()

            ldap_config = charm.config_manager.generate_ldap_config()
            assert ldap_config["ldap.search_bind_passwd"] == "dummy"
            assert ldap_config["ldap.search_base"] == ldap_relation_data["base_dn"]
            assert ldap_config["ldap.servers"] == "ldaps://glauth-k8s.ldap.svc.cluster.local:3894"
            assert ldap_config["ldap.search_bind_dn"] == ldap_relation_data["bind_dn"]
            assert ldap_config["ldap.search_attribute"] == "cn"
            assert ldap_config["ldap.search_dn_attribute"] == "DN"
            assert ldap_config["ldap.search_filter"] == "objectClass=posixAccount"

            set_config.assert_called_once()
            reload_ldap.assert_called_once()
            set_acl.assert_called_once()
            reload_acl.assert_called_once()
            assert state_out.get_relation(1).local_unit_data.get("ldap-enabled") == "true"


def test_disable_ldap(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data={"bind_password_secret": ldap_secret.id},
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, ldap_relation, ldap_ca_cert_relation},
        secrets={ldap_secret},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=LdapUnavailableEvent)

        with (
            patch("charmlibs.pathops.ContainerPath.exists"),
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("managers.config.ConfigManager.set_config_properties") as set_config,
            patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
            patch("managers.auth.AuthManager.set_acl_file") as set_acl,
            patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
        ):
            charm.ldap_events._on_ldap_unavailable(event)
            state_out = manager.run()
            set_config.assert_called_once()
            reload_ldap.assert_called_once()
            set_acl.assert_called_once()
            reload_acl.assert_called_once()
            assert state_out.get_relation(1).local_unit_data.get("ldap-enabled") == "false"


def test_invalid_config(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, ldap_relation, ldap_ca_cert_relation},
        secrets={ldap_secret},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=LdapReadyEvent)

        with (
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("managers.config.ConfigManager.set_config_properties") as set_config,
            patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
            patch("managers.auth.AuthManager.set_acl_file") as set_acl,
            patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
        ):
            charm.ldap_events._on_ldap_ready(event)
            state_out = manager.run()

            ldap_config = charm.config_manager.generate_ldap_config()
            assert ldap_config == {}

            set_config.assert_not_called()
            reload_ldap.assert_not_called()
            set_acl.assert_not_called()
            reload_acl.assert_not_called()
            assert state_out.get_relation(1).local_unit_data.get("ldap-enabled") == "false"


def test_invalid_bind_secret(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"invalid": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, ldap_relation, ldap_ca_cert_relation},
        secrets={ldap_secret},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=LdapReadyEvent)

        with (
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("managers.config.ConfigManager.set_config_properties") as set_config,
            patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
            patch("managers.auth.AuthManager.set_acl_file") as set_acl,
            patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
        ):
            charm.ldap_events._on_ldap_ready(event)
            state_out = manager.run()

            ldap_config = charm.config_manager.generate_ldap_config()
            assert ldap_config == {}

            set_config.assert_not_called()
            reload_ldap.assert_not_called()
            set_acl.assert_not_called()
            reload_acl.assert_not_called()
            assert state_out.get_relation(1).local_unit_data.get("ldap-enabled") == "false"


def test_config_change(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894", "ldaps://glauth-k8s-1.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group", "resource_type": "acl", \
                         "privileges": ["read", "write", "pubsub"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={
            "ldap-map": "ldap_group:valkey_group",
            "ldap-search-attribute": "uid",
            "ldap-search-filter": "objectClass=user",
            "ldap-search-dn-attribute": "entryDN",
        },
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.config_changed(), state_in) as manager:
        charm: ValkeyCharm = manager.charm

        with (
            patch("charmlibs.pathops.ContainerPath.exists"),
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("managers.config.ConfigManager.set_config_properties") as set_config,
            patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
            patch("managers.auth.AuthManager.set_acl_file") as set_acl,
            patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
        ):
            state_out = manager.run()

            ldap_config = charm.config_manager.generate_ldap_config()
            assert ldap_config["ldap.search_bind_passwd"] == "dummy"
            assert ldap_config["ldap.search_base"] == ldap_relation_data["base_dn"]
            assert (
                ldap_config["ldap.servers"]
                == "ldaps://glauth-k8s.ldap.svc.cluster.local:3894 ldaps://glauth-k8s-1.ldap.svc.cluster.local:3894"
            )
            assert ldap_config["ldap.search_bind_dn"] == ldap_relation_data["bind_dn"]
            assert ldap_config["ldap.search_attribute"] == "uid"
            assert ldap_config["ldap.search_dn_attribute"] == "entryDN"
            assert ldap_config["ldap.search_filter"] == "objectClass=user"

            set_config.assert_called_once()
            reload_ldap.assert_called_once()
            set_acl.assert_called_once()
            reload_acl.assert_called_once()
            assert state_out.get_relation(1).local_unit_data.get("ldap-enabled") == "true"


def test_config_change_but_invalid(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, ldap_relation},
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group:valkey_group"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with (
        patch("managers.sentinel.SentinelManager.get_primary_ip"),
        patch("managers.config.ConfigManager.set_config_properties") as set_config,
        patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
        patch("managers.auth.AuthManager.set_acl_file") as set_acl,
        patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)

        set_config.assert_not_called()
        reload_ldap.assert_not_called()
        set_acl.assert_not_called()
        reload_acl.assert_not_called()
        assert not state_out.get_relation(1).local_unit_data.get("ldap-enabled") == "true"


def test_ldap_bind_secret_update(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group", "resource_type": "acl", \
                         "privileges": ["read", "write", "pubsub"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group:valkey_group"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.secret_changed(secret=ldap_secret), state_in) as manager:
        charm: ValkeyCharm = manager.charm

        with (
            patch("charmlibs.pathops.ContainerPath.exists"),
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("managers.config.ConfigManager.set_config_properties") as set_config,
            patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
            patch("managers.auth.AuthManager.set_acl_file") as set_acl,
            patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
        ):
            manager.run()

            ldap_config = charm.config_manager.generate_ldap_config()
            assert ldap_config["ldap.search_bind_passwd"] == "dummy"
            assert ldap_config["ldap.search_base"] == ldap_relation_data["base_dn"]
            assert ldap_config["ldap.servers"] == "ldaps://glauth-k8s.ldap.svc.cluster.local:3894"
            assert ldap_config["ldap.search_bind_dn"] == ldap_relation_data["bind_dn"]

            set_config.assert_called_once()
            reload_ldap.assert_called_once()
            set_acl.assert_called_once()
            reload_acl.assert_called_once()


def test_reload_ldap_fails(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group", "resource_type": "acl", \
                         "privileges": ["read", "write", "pubsub"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group:valkey_group"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with (
        patch("charmlibs.pathops.ContainerPath.exists"),
        patch("managers.sentinel.SentinelManager.get_primary_ip"),
        patch("managers.config.ConfigManager.set_config_properties") as set_config,
        patch(
            "common.client.ValkeyClient.exec_cli_command",
            side_effect=ValkeyWorkloadCommandError("Failed to load LDAP settings"),
        ),
        patch("managers.auth.AuthManager.set_acl_file") as set_acl,
        patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)

        set_config.assert_called_once()
        set_acl.assert_not_called()
        reload_acl.assert_not_called()
        assert "config_changed" in [e.name for e in state_out.deferred]


def test_not_started_no_config_load(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group", "resource_type": "acl", \
                         "privileges": ["read", "write", "pubsub"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group:valkey_group"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=LdapReadyEvent)

        with (
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("managers.config.ConfigManager.set_config_properties") as set_config,
            patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
            patch("managers.auth.AuthManager.set_acl_file") as set_acl,
            patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
        ):
            charm.ldap_events._on_ldap_ready(event)
            state_out = manager.run()

            set_config.assert_not_called()
            reload_ldap.assert_not_called()
            set_acl.assert_not_called()
            reload_acl.assert_not_called()
            assert state_out.get_relation(1).local_unit_data.get("ldap-enabled") == "true"


def test_sync_ldap_users_leader_only(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with raises(testing.ActionFailed) as e:
        ctx.run(ctx.on.action("sync-ldap-users"), state_in)
    assert "Action can only be run on the leader unit" in e.value.message


def test_sync_ldap_users_unit_not_started(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with raises(testing.ActionFailed) as e:
        state_out = ctx.run(ctx.on.action("sync-ldap-users"), state_in)

        assert state_out.get_relation(1).local_app_data.get("ldap-user-epoch") == "0"
        assert state_out.get_relation(1).local_unit_data.get("ldap-user-epoch") == "0"
    assert "wait for startup to complete" in e.value.message


def test_sync_ldap_users_not_yet_enabled(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with raises(testing.ActionFailed) as e:
        ctx.run(ctx.on.action("sync-ldap-users"), state_in)
    assert "LDAP not yet enabled on this unit" in e.value.message


def test_sync_ldap_users_not_yet_enabled_non_leader(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl:
        state_out = ctx.run(
            ctx.on.relation_changed(relation=peer_relation, remote_unit=1), state_in
        )

        reload_acl.assert_not_called()
        assert state_out.get_relation(1).local_unit_data.get("ldap-user-epoch") == "0"


def test_sync_ldap_users_no_ldap_relation(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with raises(testing.ActionFailed) as e:
        ctx.run(ctx.on.action("sync-ldap-users"), state_in)
    assert "LDAP configuration is invalid" in e.value.message


def test_sync_ldap_users_invalid_config(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, ldap_relation, ldap_ca_cert_relation},
        secrets={ldap_secret},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with raises(testing.ActionFailed) as e:
        ctx.run(ctx.on.action("sync-ldap-users"), state_in)
    assert "LDAP configuration is invalid" in e.value.message


def test_sync_ldap_users(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group_1", "resource_type": "acl", \
                        "privileges": ["read", "write", "pubsub"]}, {"resource_name": "valkey_group_2", \
                        "resource_type": "acl", "privileges": ["read"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group_1:valkey_group_1, ldap_group_2:valkey_group_2"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.action("sync-ldap-users"), state_in) as manager:
        charm: ValkeyCharm = manager.charm

        with (
            patch("managers.auth.AuthManager._get_internal_user_acl_line"),
            patch("managers.auth.AuthManager._get_client_user_acl_lines"),
            patch("workload_k8s.ValkeyK8sWorkload.write_file"),
            patch(
                "managers.auth.AuthManager._get_ldap_users_for_group",
                return_value=[["user_1"], ["user_2", "user_3"]],
            ),
            patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
        ):
            state_out = manager.run()

            ldap_group_permissions = charm.auth_manager._resolve_ldap_group_permissions()
            assert ldap_group_permissions.get("ldap_group_1") == ["read", "write", "pubsub"]
            assert ldap_group_permissions.get("ldap_group_2") == ["read"]

            assert state_out.get_relation(1).local_app_data.get("ldap-user-epoch") != "0"
            assert state_out.get_relation(1).local_unit_data.get("ldap-user-epoch") != "0"

    assert "Updated ACL configuration" in ctx.action_results.get("result")
    reload_acl.assert_called_once()


def test_sync_ldap_users_non_leader(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
        },
        local_app_data={"ldap-user-epoch": "1774854243.6019819"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group_1", "resource_type": "acl", \
                        "privileges": ["read", "write", "pubsub"]}, {"resource_name": "valkey_group_2", \
                        "resource_type": "acl", "privileges": ["read"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group_1:valkey_group_1, ldap_group_2:valkey_group_2"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.relation_changed(relation=peer_relation, remote_unit=1), state_in) as manager:
        charm: ValkeyCharm = manager.charm

        with (
            patch("managers.auth.AuthManager._get_internal_user_acl_line"),
            patch("managers.auth.AuthManager._get_client_user_acl_lines"),
            patch("workload_k8s.ValkeyK8sWorkload.write_file"),
            patch(
                "managers.auth.AuthManager._get_ldap_users_for_group",
                return_value=[["user_1"], ["user_2", "user_3"]],
            ),
            patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
        ):
            state_out = manager.run()

            ldap_group_permissions = charm.auth_manager._resolve_ldap_group_permissions()
            assert ldap_group_permissions.get("ldap_group_1") == ["read", "write", "pubsub"]
            assert ldap_group_permissions.get("ldap_group_2") == ["read"]

            assert state_out.get_relation(1).local_unit_data.get("ldap-user-epoch") != "0"
            reload_acl.assert_called_once()


def test_ldap_peer_relation_changed_skipped_during_restore(cloud_spec):
    """During a restore the LDAP peer-relation handler must not reload ACLs.

    The restore workflow itself drives peer relation-changed events; reloading
    ACLs on the primary mid-restart would collide with the RDB swap. Restore
    completion re-fires relation-changed, reconciling from current state then.
    """
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
        },
        local_app_data={
            "ldap-user-epoch": "1774854243.6019819",
            "restore-id": "2026-05-13T10:00:00Z",
        },
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})
    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }
    ldap_relation = testing.Relation(
        id=3, endpoint=LDAP_RELATION, remote_app_data=ldap_relation_data
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)
    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group", "resource_type": "acl", \
                         "privileges": ["read", "write", "pubsub"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group:valkey_group"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with (
        patch("managers.auth.AuthManager._get_internal_user_acl_line"),
        patch("managers.auth.AuthManager._get_client_user_acl_lines"),
        patch("workload_k8s.ValkeyK8sWorkload.write_file"),
        patch("managers.auth.AuthManager._get_ldap_users_for_group", return_value=[["u"]]),
        patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl,
    ):
        ctx.run(ctx.on.relation_changed(relation=peer_relation, remote_unit=1), state_in)
        reload_acl.assert_not_called()


def test_ldap_config_changed_deferred_during_restore(cloud_spec):
    """A config change arriving during a restore is deferred, not applied to the primary."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
        local_app_data={"restore-id": "2026-05-13T10:00:00Z"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})
    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }
    ldap_relation = testing.Relation(
        id=3, endpoint=LDAP_RELATION, remote_app_data=ldap_relation_data
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)
    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group", "resource_type": "acl", \
                         "privileges": ["read", "write", "pubsub"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group:valkey_group"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with (
        patch("managers.sentinel.SentinelManager.get_primary_ip"),
        patch("managers.config.ConfigManager.set_config_properties") as set_config,
        patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)
        set_config.assert_not_called()
        reload_ldap.assert_not_called()
        # Valid LDAP + restore in progress -> the reconfigure is deferred.
        assert "config_changed" in [e.name for e in state_out.deferred]


def test_ldap_config_changed_not_deferred_when_ldap_invalid_during_restore(cloud_spec):
    """A config change on a non-LDAP cluster must not defer during a restore.

    The restore guard sits below the is_ldap_valid filter, so a cluster without
    LDAP configured returns early instead of needlessly deferring and replaying.
    """
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
        local_app_data={"restore-id": "2026-05-13T10:00:00Z"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation},  # no LDAP relation -> is_ldap_valid False
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        assert not charm.state.is_ldap_valid
        event = MagicMock()
        charm.ldap_events._on_config_changed(event)
        event.defer.assert_not_called()


def test_ldap_ready_deferred_during_restore(cloud_spec):
    """An ldap-ready event during a restore is deferred, not applied to the primary."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
        local_app_data={"restore-id": "2026-05-13T10:00:00Z"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})
    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }
    ldap_relation = testing.Relation(
        id=3, endpoint=LDAP_RELATION, remote_app_data=ldap_relation_data
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, ldap_relation, ldap_ca_cert_relation},
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group:valkey_group"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        event = MagicMock(spec=LdapReadyEvent)

        with (
            patch("managers.sentinel.SentinelManager.get_primary_ip"),
            patch("managers.config.ConfigManager.set_config_properties") as set_config,
            patch("managers.cluster.ClusterManager.reload_ldap_settings") as reload_ldap,
        ):
            charm.ldap_events._on_ldap_ready(event)
            manager.run()
            set_config.assert_not_called()
            reload_ldap.assert_not_called()
            event.defer.assert_called_once()


def test_ldap_secret_changed_guard_only_defers_the_ldap_secret_during_restore(cloud_spec):
    """Only the LDAP bind-password secret defers during a restore.

    An unrelated secret must return early, since the restore guard now sits
    below the secret-id filter rather than above it.
    """
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "ldap-enabled": "true"},
        local_app_data={"restore-id": "2026-05-13T10:00:00Z"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})
    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }
    ldap_relation = testing.Relation(
        id=3, endpoint=LDAP_RELATION, remote_app_data=ldap_relation_data
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)
    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group", "resource_type": "acl", \
                         "privileges": ["read", "write", "pubsub"]}]}]""",
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group:valkey_group"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        assert charm.state.is_ldap_valid  # the guard-below-filter path is what we're testing

        # An unrelated secret must NOT be deferred by the LDAP handler.
        other = MagicMock()
        other.secret.id = "unrelated-secret-id"
        charm.ldap_events._on_secret_changed(other)
        other.defer.assert_not_called()

        # The LDAP bind-password secret is still deferred during a restore.
        ldap_evt = MagicMock()
        ldap_evt.secret.id = charm.state.ldap.bind_password_secret
        charm.ldap_events._on_secret_changed(ldap_evt)
        ldap_evt.defer.assert_called_once()


def test_sync_ldap_users_rejected_during_restore(cloud_spec):
    """The sync-ldap-users action is rejected while a restore is in progress."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
        },
        local_app_data={"restore-id": "2026-05-13T10:00:00Z"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with raises(testing.ActionFailed) as e:
        ctx.run(ctx.on.action("sync-ldap-users"), state_in)
    assert "restore" in e.value.message.lower()


def test_sync_ldap_users_up_to_date(cloud_spec):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "ldap-enabled": "true",
            "ldap-user-epoch": "1774854243.6034567",
        },
        local_app_data={"ldap-user-epoch": "1774854243.6019819"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation_data = {
        "auth_method": "simple",
        "base_dn": "dc=glauth,dc=com",
        "bind_dn": "cn=valkey,ou=ldap,dc=glauth,dc=com",
        "bind_password_secret": ldap_secret.id,
        "ldaps_urls": '["ldaps://glauth-k8s.ldap.svc.cluster.local:3894"]',
        "starttls": "True",
        "urls": '["ldap://glauth-k8s.ldap.svc.cluster.local:3893"]',
    }

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data=ldap_relation_data,
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)

    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                        "entity-permissions": [{"resource_name": "valkey_group_1", "resource_type": "acl", \
                        "privileges": ["read", "write", "pubsub"]}, {"resource_name": "valkey_group_2", \
                        "resource_type": "acl", "privileges": ["read"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "ldap_group_1:valkey_group_1, ldap_group_2:valkey_group_2"},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )

    with patch("managers.cluster.ClusterManager.reload_acl_file") as reload_acl:
        ctx.run(ctx.on.relation_changed(relation=peer_relation, remote_unit=1), state_in)
        reload_acl.assert_not_called()


def _ldap_query_state(
    cloud_spec, config: dict[str, str], leader: bool = False
) -> tuple[testing.Context, testing.State]:
    """Build a context and state with a fully valid LDAP setup for filter tests."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    ldap_secret = testing.Secret({"password": "dummy"})

    ldap_relation = testing.Relation(
        id=3,
        endpoint=LDAP_RELATION,
        remote_app_data={
            "auth_method": "simple",
            "base_dn": "dc=ldap,dc=goauthentik,dc=io",
            "bind_dn": "cn=valkey,ou=users,dc=ldap,dc=goauthentik,dc=io",
            "bind_password_secret": ldap_secret.id,
            "ldaps_urls": '["ldaps://10.0.0.1:636"]',
            "starttls": "False",
            "urls": '["ldap://10.0.0.1:3389"]',
        },
    )
    ldap_ca_cert_relation = testing.Relation(id=4, endpoint=LDAP_CA_CERT_RELATION)
    client_relation = testing.Relation(
        id=5,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": """[{"resource": "my-keys", "request-id": "8865631800293def", "salt": "6TNjC2Aid8hlfBpf", \
                    "entity-permissions": [{"resource_name": "valkey_group", "resource_type": "acl", \
                     "privileges": ["read", "write", "pubsub"]}]}]""",
        },
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=leader,
        relations={
            peer_relation,
            status_peer_relation,
            ldap_relation,
            ldap_ca_cert_relation,
            client_relation,
        },
        secrets={ldap_secret},
        config={"ldap-map": "superheroes:valkey_group", **config},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd", cloud_spec=cloud_spec),
    )
    return ctx, state_in


def test_ldap_query_default_template(cloud_spec):
    """The default `ldap-query-template` addresses groups by their `cn` RDN."""
    ctx, state_in = _ldap_query_state(cloud_spec, {})

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        connection = MagicMock()
        connection.entries = []

        with patch("managers.auth.AuthManager._get_ldap_connection", return_value=connection):
            charm.auth_manager._get_ldap_users_for_group("superheroes")

    assert (
        connection.search.call_args.kwargs["search_filter"]
        == "(&(objectClass=posixAccount)(memberOf=cn=superheroes,*))"
    )


def test_ldap_query_configured_template(cloud_spec):
    """A configured `ldap-query-template` replaces the default, e.g. for GLAuth."""
    ctx, state_in = _ldap_query_state(
        cloud_spec,
        {"ldap-query-template": "(&(objectClass=posixAccount)(memberOf=ou={group},*))"},
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        connection = MagicMock()
        connection.entries = []

        with patch("managers.auth.AuthManager._get_ldap_connection", return_value=connection):
            charm.auth_manager._get_ldap_users_for_group("superheroes")

    assert (
        connection.search.call_args.kwargs["search_filter"]
        == "(&(objectClass=posixAccount)(memberOf=ou=superheroes,*))"
    )


def test_ldap_query_template_without_placeholder_is_invalid(cloud_spec):
    """A template that never substitutes the group name blocks the charm."""
    ctx, state_in = _ldap_query_state(
        cloud_spec, {"ldap-query-template": "(objectClass=posixAccount)"}, leader=True
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        assert not manager.charm.state.is_ldap_valid
        state_out = manager.run()

    assert status_is(state_out, AuthStatuses.LDAP_QUERY_TEMPLATE_INVALID.value, is_app=True)


def test_ldap_query_template_with_unknown_placeholder_is_invalid(cloud_spec):
    """A template referencing a placeholder the charm does not provide blocks the charm."""
    ctx, state_in = _ldap_query_state(
        cloud_spec, {"ldap-query-template": "(memberOf=cn={grp},*)"}, leader=True
    )

    with ctx(ctx.on.update_status(), state_in) as manager:
        assert not manager.charm.state.is_ldap_valid
        state_out = manager.run()

    assert status_is(state_out, AuthStatuses.LDAP_QUERY_TEMPLATE_INVALID.value, is_app=True)


def test_ldap_acl_skips_users_while_ca_cert_missing(cloud_spec):
    """A CA file that has not landed yet omits LDAP users instead of failing the ACL write.

    `is_ldap_valid` is satisfied by the `ldap-ca-cert` relation existing, but the LDAP connection
    needs the CA on disk. Raising here would fail `configure_auth` and latch the unit in
    CONFIGURATION_ERROR; the CA-available event regenerates the ACL once the file arrives.
    """
    ctx, state_in = _ldap_query_state(cloud_spec, {})

    with ctx(ctx.on.update_status(), state_in) as manager:
        charm: ValkeyCharm = manager.charm
        assert charm.state.is_ldap_valid

        with (
            patch("charmlibs.pathops.ContainerPath.exists", return_value=False),
            patch("managers.auth.AuthManager._get_ldap_connection") as get_connection,
        ):
            assert charm.auth_manager._get_ldap_user_acl_lines() == ""
            get_connection.assert_not_called()
