#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import json
from pathlib import Path
from unittest.mock import patch

import yaml
from ops import testing

from charm import ValkeyCharm
from common.exceptions import ValkeyCannotGetPrimaryIPError, ValkeyWorkloadCommandError
from literals import (
    CERTIFICATE_TRANSFER_RELATION,
    CLIENT_PORT,
    CLIENTS_USERS_SECRET_LABEL_SUFFIX,
    EXTERNAL_CLIENTS_RELATION,
    PEER_RELATION,
    SENTINEL_PORT,
    STATUS_PEERS_RELATION,
)
from statuses import ExternalClientsStatuses

from .helpers import status_is

CONTAINER = "valkey"

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_add_new_client_user():
    primary_endpoint = "valkey-0.valkey-endpoints"
    replica_endpoint = "valkey-1.valkey-endpoints"
    valkey_version = "9.0.1"
    key_prefix = "test:*"
    request_id = "0cbbc9781f189ea5"
    salt = "mWpK32IQW4bsu65t"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "hostname": primary_endpoint},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_relation = testing.Relation(
        id=3,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": f'[{{"resource": "{key_prefix}", "request-id": "{request_id}", "salt": "{salt}"}}]',
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value=primary_endpoint),
        patch(
            "common.client.SentinelClient.replicas_primary",
            return_value=[{"ip": replica_endpoint}],
        ),
        patch(
            "common.client.ValkeyClient.info_server",
            return_value={"valkey_version": valkey_version},
        ),
        patch("managers.auth.AuthManager.set_acl_file") as set_acl_file,
        patch("common.client.ValkeyClient.acl_load") as load_acl,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation=client_relation), state_in)
        set_acl_file.assert_called_once()
        load_acl.assert_called_once()
        set_sentinel_acl_file.assert_called_once()
        restart_sentinel.assert_called_once()
        relation = state_out.get_relation(client_relation.id)
        response = json.loads(relation.local_app_data["requests"])[0]
        secret_user_id = response["secret-user"]
        secret_user = state_out.get_secret(id=secret_user_id)
        secret_tls_id = response["secret-tls"]
        secret_tls = state_out.get_secret(id=secret_tls_id)

        assert response["resource"] == key_prefix
        assert response["request-id"] == request_id
        assert response["salt"] == salt
        assert response["endpoints"] == f"valkey-primary:{CLIENT_PORT}"
        assert response["read-only-endpoints"] == f"valkey-replicas:{CLIENT_PORT}"
        assert response["sentinel-endpoints"] == f"{primary_endpoint}:{SENTINEL_PORT}"
        assert response["version"] == valkey_version
        assert response["mode"] == "sentinel"
        assert (
            secret_user.latest_content.get("username")
            == f"relation-{client_relation.id}-{request_id}"
        )
        assert secret_user.latest_content.get("password")
        assert secret_tls.latest_content.get("tls") == "false"


def test_add_new_client_user_v0():
    primary_endpoint = "valkey-0.valkey-endpoints"
    replica_endpoint = "valkey-1.valkey-endpoints"
    valkey_version = "9.0.1"
    key_prefix = "test:*"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "hostname": primary_endpoint},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_relation = testing.Relation(
        id=3,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={"database": f"{key_prefix}"},
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.sentinel.SentinelManager.get_primary_ip", return_value=primary_endpoint),
        patch(
            "common.client.SentinelClient.replicas_primary",
            return_value=[{"ip": replica_endpoint}],
        ),
        patch(
            "common.client.ValkeyClient.info_server",
            return_value={"valkey_version": valkey_version},
        ),
        patch("managers.auth.AuthManager.set_acl_file") as set_acl_file,
        patch("common.client.ValkeyClient.acl_load") as load_acl,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation=client_relation), state_in)
        set_acl_file.assert_called_once()
        load_acl.assert_called_once()
        set_sentinel_acl_file.assert_called_once()
        restart_sentinel.assert_called_once()
        relation = state_out.get_relation(client_relation.id)
        response = relation.local_app_data
        secret_user_id = response["secret-user"]
        secret_user = state_out.get_secret(id=secret_user_id)
        secret_tls_id = response["secret-tls"]
        secret_tls = state_out.get_secret(id=secret_tls_id)

        assert response["database"] == key_prefix
        assert response["endpoints"] == f"valkey-primary:{CLIENT_PORT}"
        assert response["read-only-endpoints"] == f"valkey-replicas:{CLIENT_PORT}"
        assert response["sentinel_endpoints"] == f"{primary_endpoint}:{SENTINEL_PORT}"
        assert response["version"] == valkey_version
        assert response["mode"] == "sentinel"
        assert secret_user.latest_content.get("username") == f"relation-{client_relation.id}"
        assert secret_user.latest_content.get("password")
        assert secret_tls.latest_content.get("tls") == "false"


def test_client_user_already_exists():
    primary_endpoint = "valkey-0.valkey-endpoints"
    key_prefix = "test:*"
    request_id = "0cbbc9781f189ea5"
    salt = "mWpK32IQW4bsu65t"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "hostname": primary_endpoint},
        local_app_data={"client-user-epoch": "1774854243.6019819"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_relation = testing.Relation(
        id=3,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": f'[{{"resource": "{key_prefix}", "request-id": "{request_id}", "salt": "{salt}"}}]',
        },
    )
    managed_users_secret = testing.Secret(
        tracked_content={
            "external-client-users": """ \
                {"relation-3-0cbbc9781f189ea5": \
                {"resource": "test:*", "password": "mypassword"}, \
                "relation-4-08154711": \
                {"resource": "another_keyspace:*", "password": "anotherpassword"}} \
                """
        },
        owner="app",
        label=f"{PEER_RELATION}.{APP_NAME}.app.{CLIENTS_USERS_SECRET_LABEL_SUFFIX}",
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_relation},
        secrets={managed_users_secret},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.sentinel.SentinelManager.get_primary_ip"),
        patch("common.client.SentinelClient.replicas_primary"),
        patch("common.client.ValkeyClient.info_server"),
        patch("managers.auth.AuthManager.set_acl_file") as set_acl_file,
        patch("common.client.ValkeyClient.acl_load") as load_acl,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation=client_relation), state_in)
        set_acl_file.assert_not_called()
        load_acl.assert_not_called()
        set_sentinel_acl_file.assert_not_called()
        restart_sentinel.assert_not_called()
        assert (
            state_out.get_relation(peer_relation.id).local_app_data.get("client-user-epoch")
            == "1774854243.6019819"
        )


def test_client_request_failed():
    primary_endpoint = "valkey-0.valkey-endpoints"
    key_prefix = "test:*"
    request_id = "0cbbc9781f189ea5"
    salt = "mWpK32IQW4bsu65t"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "hostname": primary_endpoint},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_relation = testing.Relation(
        id=3,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": f'[{{"resource": "{key_prefix}", "request-id": "{request_id}", "salt": "{salt}"}}]',
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch(
            "managers.sentinel.SentinelManager.get_primary_ip",
            side_effect=ValkeyCannotGetPrimaryIPError("error"),
        ),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation=client_relation), state_in)
        assert status_is(
            state_out, ExternalClientsStatuses.RESOURCE_REQUEST_UNPROCESSED.value, is_app=True
        )


def test_client_request_acl_load_failed():
    primary_endpoint = "valkey-0.valkey-endpoints"
    key_prefix = "test:*"
    request_id = "0cbbc9781f189ea5"
    salt = "mWpK32IQW4bsu65t"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "hostname": primary_endpoint},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_relation = testing.Relation(
        id=3,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": f'[{{"resource": "{key_prefix}", "request-id": "{request_id}", "salt": "{salt}"}}]',
        },
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.sentinel.SentinelManager.get_primary_ip"),
        patch("common.client.SentinelClient.replicas_primary"),
        patch("common.client.ValkeyClient.info_server"),
        patch("managers.auth.AuthManager.set_acl_file"),
        patch(
            "common.client.ValkeyClient.exec_cli_command",
            side_effect=ValkeyWorkloadCommandError("Failed to reload ACLs"),
        ),
    ):
        state_out = ctx.run(ctx.on.relation_changed(relation=client_relation), state_in)
        assert "bulk_resources_requested" in [e.name for e in state_out.deferred]


def test_add_new_client_user_non_leader():
    primary_endpoint = "valkey-0.valkey-endpoints"
    key_prefix = "test:*"
    request_id = "0cbbc9781f189ea5"
    salt = "mWpK32IQW4bsu65t"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "hostname": primary_endpoint},
        local_app_data={"client-user-epoch": "1774854243.6019819"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_relation = testing.Relation(
        id=3,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": f'[{{"resource": "{key_prefix}", "request-id": "{request_id}", "salt": "{salt}"}}]',
        },
    )
    managed_users_secret = testing.Secret(
        tracked_content={
            "external-client-users": """ \
            {"relation-3-0cbbc9781f189ea5": \
            {"resource": "test:*", "password": "mypassword"}} \
            """
        },
        owner="app",
        label=f"{PEER_RELATION}.{APP_NAME}.app.{CLIENTS_USERS_SECRET_LABEL_SUFFIX}",
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, client_relation},
        secrets={managed_users_secret},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.auth.AuthManager.set_acl_file") as set_acl_file,
        patch("common.client.ValkeyClient.acl_load") as load_acl,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        state_out = ctx.run(
            ctx.on.relation_changed(relation=peer_relation, remote_unit=1), state_in
        )
        set_acl_file.assert_called_once()
        load_acl.assert_called_once()
        set_sentinel_acl_file.assert_called_once()
        restart_sentinel.assert_called_once()
        assert state_out.get_relation(1).local_unit_data.get("client-user-epoch") != 0


def test_client_user_not_created_yet():
    primary_endpoint = "valkey-0.valkey-endpoints"
    key_prefix = "test:*"
    request_id = "0cbbc9781f189ea5"
    salt = "mWpK32IQW4bsu65t"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": "started",
            "hostname": primary_endpoint,
            "client-user-epoch": "1774854243.6019819",
        },
        local_app_data={"client-user-epoch": "1774854240.6000019"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_relation = testing.Relation(
        id=3,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": f'[{{"resource": "{key_prefix}", "request-id": "{request_id}", "salt": "{salt}"}}]',
        },
    )
    managed_users_secret = testing.Secret(
        tracked_content={
            "external-client-users": """ \
            {"relation-2-08154711": \
            {"resource": "*", "password": "secret"}} \
            """
        },
        owner="app",
        label=f"{PEER_RELATION}.{APP_NAME}.app.{CLIENTS_USERS_SECRET_LABEL_SUFFIX}",
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, client_relation},
        secrets={managed_users_secret},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.auth.AuthManager.set_acl_file") as set_acl_file,
        patch("common.client.ValkeyClient.acl_load") as load_acl,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        ctx.run(ctx.on.relation_changed(relation=peer_relation, remote_unit=1), state_in)
        set_acl_file.assert_not_called()
        load_acl.assert_not_called()
        set_sentinel_acl_file.assert_not_called()
        restart_sentinel.assert_not_called()


def test_remove_client_user():
    primary_endpoint = "valkey-0.valkey-endpoints"
    key_prefix = "test:*"
    request_id = "0cbbc9781f189ea5"
    salt = "mWpK32IQW4bsu65t"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "hostname": primary_endpoint},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_relation = testing.Relation(
        id=3,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": f'[{{"resource": "{key_prefix}", "request-id": "{request_id}", "salt": "{salt}"}}]',
        },
    )
    managed_users_secret = testing.Secret(
        tracked_content={
            "external-client-users": """ \
                {"relation-3-0cbbc9781f189ea5": \
                {"resource": "test:*", "password": "mypassword"}, \
                "relation-4-08154711": \
                {"resource": "another_keyspace:*", "password": "anotherpassword"}} \
                """
        },
        owner="app",
        label=f"{PEER_RELATION}.{APP_NAME}.app.{CLIENTS_USERS_SECRET_LABEL_SUFFIX}",
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=True,
        relations={peer_relation, status_peer_relation, client_relation},
        secrets={managed_users_secret},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.auth.AuthManager.set_acl_file") as set_acl_file,
        patch("common.client.ValkeyClient.acl_load") as load_acl,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        state_out = ctx.run(ctx.on.relation_broken(relation=client_relation), state_in)
        set_acl_file.assert_called_once()
        load_acl.assert_called_once()
        set_sentinel_acl_file.assert_called_once()
        restart_sentinel.assert_called_once()

        managed_users_secret = state_out.get_secret(
            label=f"{PEER_RELATION}.{APP_NAME}.app.{CLIENTS_USERS_SECRET_LABEL_SUFFIX}"
        )
        managed_users = json.loads(
            managed_users_secret.latest_content.get("external-client-users")
        )
        assert not managed_users.get(f"relation-{client_relation.id}-{request_id}")
        assert managed_users.get("relation-4-08154711")


def test_relation_broken_non_leader():
    primary_endpoint = "valkey-0.valkey-endpoints"
    key_prefix = "test:*"
    request_id = "0cbbc9781f189ea5"
    salt = "mWpK32IQW4bsu65t"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started", "hostname": primary_endpoint},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    client_relation = testing.Relation(
        id=3,
        endpoint=EXTERNAL_CLIENTS_RELATION,
        remote_app_data={
            "version": "v1",
            "requests": f'[{{"resource": "{key_prefix}", "request-id": "{request_id}", "salt": "{salt}"}}]',
        },
    )
    managed_users_secret = testing.Secret(
        tracked_content={
            # the managed user was already removed by the leader
            "external-client-users": """ \
                {"relation-4-08154711": \
                {"resource": "another_keyspace:*", "password": "anotherpassword"}} \
                """
        },
        owner="app",
        label=f"{PEER_RELATION}.{APP_NAME}.app.{CLIENTS_USERS_SECRET_LABEL_SUFFIX}",
    )
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, client_relation},
        secrets={managed_users_secret},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("managers.auth.AuthManager.set_acl_file") as set_acl_file,
        patch(
            "managers.external_clients.ExternalClientsManager.remove_managed_users"
        ) as remove_user,
        patch("common.client.ValkeyClient.acl_load") as load_acl,
        patch("managers.auth.AuthManager.set_sentinel_acl_file") as set_sentinel_acl_file,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        ctx.run(ctx.on.relation_broken(relation=client_relation), state_in)
        remove_user.assert_not_called()
        set_acl_file.assert_called_once()
        load_acl.assert_called_once()
        set_sentinel_acl_file.assert_called_once()
        restart_sentinel.assert_called_once()


def test_certificate_transfer_version_set():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    certificate_transfer_relation = testing.Relation(
        id=5,
        endpoint=CERTIFICATE_TRANSFER_RELATION,
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=True,
        relations={certificate_transfer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    state_out = ctx.run(ctx.on.relation_created(relation=certificate_transfer_relation), state_in)
    assert (
        state_out.get_relation(certificate_transfer_relation.id).local_app_data.get("version")
        == "1"
    )


def test_certificate_transfer_new_ca():
    ca_cert = "client_ca_certificate"

    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    certificate_transfer_relation = testing.Relation(
        id=5,
        endpoint=CERTIFICATE_TRANSFER_RELATION,
        remote_app_data={"certificates": ca_cert},
    )

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, certificate_transfer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch(
            "charmlibs.interfaces.certificate_transfer.CertificateTransferRequires.get_all_certificates",
            return_value=[ca_cert],
        ),
        patch("workload_k8s.ValkeyK8sWorkload.write_file") as write_ca_certs,
        patch("managers.tls.TLSManager.rehash_ca_certificates") as rehash_ca_certs,
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        ctx.run(ctx.on.relation_changed(relation=certificate_transfer_relation), state_in)
        write_ca_certs.assert_called_once()
        rehash_ca_certs.assert_called_once()
        reload_tls.assert_called_once()
        restart_sentinel.assert_called_once()


def test_certificate_transfer_no_ca_available():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    certificate_transfer_relation = testing.Relation(id=5, endpoint=CERTIFICATE_TRANSFER_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, certificate_transfer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch(
            "charmlibs.interfaces.certificate_transfer.CertificateTransferRequires.get_all_certificates",
            return_value=[],
        ),
        patch("workload_k8s.ValkeyK8sWorkload.write_file") as write_ca_certs,
        patch("managers.tls.TLSManager.rehash_ca_certificates") as rehash_ca_certs,
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        ctx.run(ctx.on.relation_changed(relation=certificate_transfer_relation), state_in)
        write_ca_certs.assert_not_called()
        rehash_ca_certs.assert_not_called()
        reload_tls.assert_not_called()
        restart_sentinel.assert_not_called()


def test_certificate_transfer_ca_removed():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    certificate_transfer_relation = testing.Relation(id=5, endpoint=CERTIFICATE_TRANSFER_RELATION)

    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, certificate_transfer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )

    with (
        patch("charmlibs.pathops.ContainerPath.exists", return_value=True),
        patch("charmlibs.pathops.ContainerPath.mkdir"),
        patch("workload_k8s.ValkeyK8sWorkload.remove_file") as remove_ca_certs,
        patch("workload_k8s.ValkeyK8sWorkload.write_file") as write_ca_certs,
        patch("managers.tls.TLSManager.rehash_ca_certificates") as rehash_ca_certs,
        patch("managers.cluster.ClusterManager.reload_tls_settings") as reload_tls,
        patch("common.locks.DataBagLock.is_held_by_this_unit", return_value=True),
        patch("managers.sentinel.SentinelManager.restart_service") as restart_sentinel,
        patch("managers.sentinel.SentinelManager.is_healthy"),
    ):
        ctx.run(ctx.on.relation_broken(relation=certificate_transfer_relation), state_in)
        write_ca_certs.assert_not_called()
        remove_ca_certs.assert_called_once()
        rehash_ca_certs.assert_called_once()
        reload_tls.assert_called_once()
        restart_sentinel.assert_called_once()


def test_certificate_transfer_ca_available_pebble_down_defers():
    ca_cert = "client_ca_certificate"
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    certificate_transfer_relation = testing.Relation(
        id=5,
        endpoint=CERTIFICATE_TRANSFER_RELATION,
        remote_app_data={"certificates": ca_cert},
    )
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, certificate_transfer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with (
        patch(
            "charmlibs.interfaces.certificate_transfer.CertificateTransferRequires.get_all_certificates",
            return_value=[ca_cert],
        ),
        patch(
            "core.base_workload.WorkloadBase.make_dir",
            side_effect=ValkeyWorkloadCommandError("Pebble down"),
        ),
    ):
        state_out = ctx.run(
            ctx.on.relation_changed(relation=certificate_transfer_relation), state_in
        )
    assert "certificate_set_updated" in [e.name for e in state_out.deferred]


def test_certificate_transfer_ca_removed_pebble_down_defers():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={"start-state": "started"},
    )
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    certificate_transfer_relation = testing.Relation(id=5, endpoint=CERTIFICATE_TRANSFER_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(
        leader=False,
        relations={peer_relation, status_peer_relation, certificate_transfer_relation},
        containers={container},
        model=testing.Model(name="my-vm-model", type="lxd"),
    )
    with patch(
        "core.base_workload.WorkloadBase.path_exists",
        side_effect=ValkeyWorkloadCommandError("Pebble down"),
    ):
        state_out = ctx.run(
            ctx.on.relation_broken(relation=certificate_transfer_relation), state_in
        )
    assert "certificates_removed" in [e.name for e in state_out.deferred]
