#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path

import jubilant
import pytest

from literals import CharmUsers, Substrate
from statuses import AuthStatuses
from tests.integration.clients.authentik import ENTRY_DN_ATTRIBUTE, provision_directory
from tests.integration.helpers import (
    APP_NAME,
    GLIDE_RUNNER_NAME,
    IMAGE_RESOURCE,
    TLS_CHANNEL,
    TLS_NAME,
    WrongPassError,
    are_agents_idle,
    are_apps_active_and_agents_idle,
    auth_test,
    does_status_match,
    get_cluster_endpoints,
    get_key,
    get_password,
    set_key,
)

logger = logging.getLogger(__name__)

NUM_UNITS = 3
TEST_KEY = "test_key"
TEST_VALUE = "test_value"
LDAP_NAME = "authentik-ldap-outpost"
LDAP_SERVER_NAME = "authentik-server"
LDAP_WORKER_NAME = "authentik-worker"
LDAP_PG_NAME = "postgresql-k8s"
LDAP_INGRESS_NAME = "traefik-k8s"
DATA_INTEGRATOR_NAME = "data-integrator"

AUTHENTIK_CHANNEL = "latest/edge"
# Every authentication costs Valkey an admin bind, a search and a user bind against the outpost,
# and Authentik answers each in the high hundreds of milliseconds -- an order of magnitude slower
# than GLAuth. Credentials are supplied when the client connects, so all three have to fit inside
# the connection timeout; Glide's 2s default expires mid-bind and surfaces as "timed out".
LDAP_CONNECTION_TIMEOUT_MS = 20000
DIRECTORY_ENTRIES = json.loads(
    Path("./tests/integration/clients/data/authentik_entries.json").read_text()
)


def test_build_and_deploy(
    charm: str,
    glide_runner_charm: str,
    juju: jubilant.Juju,
    substrate: Substrate,
    juju_k8s_model: jubilant.Juju,
) -> None:
    """Deploy the charm under test, the Authentik LDAP stack and Data Integrator."""
    juju.deploy(
        charm,
        resources=IMAGE_RESOURCE if substrate == Substrate.K8S else None,
        num_units=NUM_UNITS,
        trust=True,
    )
    juju.deploy(glide_runner_charm, app=GLIDE_RUNNER_NAME)
    juju.deploy(DATA_INTEGRATOR_NAME, channel="latest/edge")

    juju_k8s_model.deploy(
        LDAP_PG_NAME,
        channel="14/stable",
        trust=True,
        config={"profile": "testing"},
    )
    juju_k8s_model.deploy(LDAP_SERVER_NAME, channel=AUTHENTIK_CHANNEL, trust=True)
    juju_k8s_model.deploy(LDAP_WORKER_NAME, channel=AUTHENTIK_CHANNEL, trust=True)
    # `direct` reads the live Authentik state; the default `cached` modes would serve a directory
    # snapshot taken before this test creates its users
    juju_k8s_model.deploy(
        LDAP_NAME,
        channel=AUTHENTIK_CHANNEL,
        trust=True,
        config={"search_mode": "direct", "bind_mode": "direct"},
    )
    # Traefik terminates LDAPS for the outpost, which never provisions a certificate of its own.
    # `ingress_domain` stays unset so Traefik keeps a single certificate and serves it as the
    # default one for the LDAPS TCP router.
    juju_k8s_model.deploy(LDAP_INGRESS_NAME, trust=True)
    juju_k8s_model.deploy(TLS_NAME, channel=TLS_CHANNEL)

    logger.info("Add integrations for LDAP")
    juju_k8s_model.integrate(f"{LDAP_SERVER_NAME}:pg-database", f"{LDAP_PG_NAME}:database")
    juju_k8s_model.integrate(f"{LDAP_SERVER_NAME}:authentik-cluster", LDAP_WORKER_NAME)
    juju_k8s_model.integrate(f"{LDAP_SERVER_NAME}:authentik-server-info", LDAP_NAME)
    juju_k8s_model.integrate(f"{LDAP_SERVER_NAME}:traefik-route", LDAP_INGRESS_NAME)
    juju_k8s_model.integrate(f"{LDAP_NAME}:traefik-route", LDAP_INGRESS_NAME)
    juju_k8s_model.integrate(f"{LDAP_INGRESS_NAME}:certificates", f"{TLS_NAME}:certificates")

    juju.wait(
        lambda status: are_agents_idle(status, APP_NAME, idle_period=30, unit_count=NUM_UNITS),
        timeout=600,
    )

    # Named apps only: on K8s this is the Valkey model too, where Data Integrator sits blocked
    # until a later test configures it. PostgreSQL is deliberately absent -- it re-stamps its
    # agent status every few seconds, so no `idle_period` it takes part in ever elapses, and an
    # active authentik-server already implies a working database.
    juju_k8s_model.wait(
        lambda status: are_apps_active_and_agents_idle(
            status,
            LDAP_NAME,
            LDAP_SERVER_NAME,
            LDAP_WORKER_NAME,
            LDAP_INGRESS_NAME,
            TLS_NAME,
            idle_period=30,
        ),
        timeout=1800,
    )

    logger.info("Set up LDAP users")
    provision_directory(juju_k8s_model, LDAP_SERVER_NAME, DIRECTORY_ENTRIES)

    if substrate == Substrate.VM:
        logger.info("Set up cross-model offers")
        juju_k8s_model.offer(app=LDAP_NAME, endpoint="ldap", name="ldap")
        # The outpost has no `send-ca-cert`; the CA that signed the LDAPS certificate Traefik
        # serves is the one held by the certificate provider
        juju_k8s_model.offer(app=TLS_NAME, endpoint="send-ca-cert", name="ca")


def test_ldap_integration(
    juju: jubilant.Juju, juju_k8s_model: jubilant.Juju, substrate: Substrate
) -> None:
    """Connect Valkey to the LDAP provider."""
    logger.info("Integrating Valkey with LDAP")
    if substrate == Substrate.VM:
        juju_k8s_model_name = juju_k8s_model.model.split(":")[1]
        ldap_name = "ldap"
        ca_name = "ca"
        juju.consume(f"{juju_k8s_model_name}.{ldap_name}")
        juju.consume(f"{juju_k8s_model_name}.{ca_name}")
    else:
        ldap_name = LDAP_NAME
        ca_name = TLS_NAME

    juju.integrate(f"{APP_NAME}:ldap", ldap_name)

    juju.wait(
        lambda status: does_status_match(
            status,
            expected_app_statuses={APP_NAME: [AuthStatuses.LDAP_CA_CERT_MISSING.value]},
        ),
        timeout=100,
    )

    logger.info("Add LDAP CA certificate")
    juju.integrate(f"{APP_NAME}:ldap-ca-cert", ca_name)
    juju.wait(
        lambda status: does_status_match(
            status,
            expected_app_statuses={APP_NAME: [AuthStatuses.LDAP_MAP_CONFIG_MISSING.value]},
        ),
        timeout=100,
    )


def test_relation_with_data_integrator(juju: jubilant.Juju) -> None:
    """Connect Valkey to Data Integrator for setup of permission model."""
    data_integrator_config = {
        "prefix-name": "my-keys:",
        "entity-permissions": '[{"resource_name": "ldap_users_write", "resource_type": "acl", "privileges": ["+@read", "+@write", "+@pubsub", "~*", "&*"]}, {"resource_name": "ldap_users_read", "resource_type": "acl", "privileges": ["+@read",  "~*"]}]',
    }
    juju.config(DATA_INTEGRATOR_NAME, data_integrator_config)

    logger.info("Integrating Valkey with Data Integrator")
    juju.integrate(f"{APP_NAME}:valkey-client", f"{DATA_INTEGRATOR_NAME}:valkey")
    juju.wait(
        lambda status: does_status_match(
            status,
            expected_app_statuses={APP_NAME: [AuthStatuses.LDAP_MAP_CONFIG_MISSING.value]},
        ),
        timeout=100,
    )


def test_enable_ldap(juju: jubilant.Juju) -> None:
    """Enable LDAP in Valkey and ensure access works correctly."""
    logger.info("Enabling LDAP in Valkey by matching permission roles to LDAP groups")
    valkey_ldap_config = {
        # Authentik publishes no DN attribute; each test user carries its own bind DN here
        "ldap-search-dn-attribute": ENTRY_DN_ATTRIBUTE,
        # the keys have to match the LDAP groups in authentik_entries.json
        "ldap-map": "superheroes:ldap_users_write, normies:ldap_users_read",
    }
    juju.config(APP_NAME, valkey_ldap_config)
    juju.wait(
        lambda status: are_agents_idle(status, APP_NAME, idle_period=30, unit_count=NUM_UNITS),
        timeout=600,
    )

    logger.info("Ensure access for charm user still works")
    endpoints = get_cluster_endpoints(juju, APP_NAME)
    result = set_key(
        juju=juju,
        endpoints=endpoints,
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
        key=TEST_KEY,
        value=TEST_VALUE,
        connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
    )
    assert result == "OK", "Failed to write data for charm user with LDAP enabled"

    assert (
        get_key(
            juju=juju,
            endpoints=endpoints,
            username=CharmUsers.VALKEY_ADMIN.value,
            password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
            key=TEST_KEY,
            connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
        )
        == TEST_VALUE
    ), "Failed to read data for charm user with LDAP enabled"


def test_ensure_ldap_auth(juju: jubilant.Juju) -> None:
    """Ensure authentication with LDAP works and authorization is set up correctly."""
    endpoints = get_cluster_endpoints(juju, APP_NAME)

    logger.info("Ensure access for LDAP user with read and write permissions")
    # connect with user in LDAP group "superheroes"
    username = "johndoe"
    password = "dogood"

    result = set_key(
        juju=juju,
        endpoints=endpoints,
        username=username,
        password=password,
        key=TEST_KEY,
        value=TEST_VALUE,
        connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
    )
    assert result == "OK", f"Failed to write data for user {username} with LDAP enabled"

    assert (
        get_key(
            juju=juju,
            endpoints=endpoints,
            username=username,
            password=password,
            key=TEST_KEY,
            connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
        )
        == TEST_VALUE
    ), f"Failed to read data for user {username} with LDAP enabled"

    logger.info("Ensure access for LDAP user with read-only permissions")
    # connect with user in LDAP group "normies"
    username = "janedoe"
    password = "dogood"

    assert (
        get_key(
            juju=juju,
            endpoints=endpoints,
            username=username,
            password=password,
            key=TEST_KEY,
            connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
        )
        == TEST_VALUE
    ), f"Failed to read data for user {username} with LDAP enabled"

    result = set_key(
        juju=juju,
        endpoints=endpoints,
        username=username,
        password=password,
        key=TEST_KEY,
        value=TEST_VALUE,
        connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
    )
    assert not result == "OK", "Failed to write data with LDAP enabled"

    logger.info("Ensure access fails for LDAP user without required group membership")
    # connect with user in LDAP group "others"
    username = "jennydoe"
    password = "dogood"

    assert not auth_test(
        juju=juju,
        endpoints=endpoints,
        username=username,
        password=password,
        connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
    )


def test_disable_ldap(juju: jubilant.Juju, substrate: Substrate) -> None:
    """Ensure LDAP users can no longer access Valkey after LDAP was disabled."""
    logger.info("Disabling LDAP")
    ldap_name = "ldap" if substrate == Substrate.VM else LDAP_NAME
    juju.remove_relation(f"{APP_NAME}:ldap", ldap_name)

    juju.wait(
        lambda status: are_agents_idle(status, APP_NAME, idle_period=30, unit_count=NUM_UNITS),
        timeout=600,
    )

    logger.info("Ensure access for charm user still works")
    endpoints = get_cluster_endpoints(juju, APP_NAME)
    result = set_key(
        juju=juju,
        endpoints=endpoints,
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
        key=TEST_KEY,
        value=TEST_VALUE,
        connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
    )
    assert result == "OK", "Failed to write data for charm user with LDAP disabled"

    assert (
        get_key(
            juju=juju,
            endpoints=endpoints,
            username=CharmUsers.VALKEY_ADMIN.value,
            password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
            key=TEST_KEY,
            connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
        )
        == TEST_VALUE
    ), "Failed to read data for charm user with LDAP disabled"

    logger.info("Ensure access fails for LDAP user")
    # connect with user in LDAP group "superheroes"
    username = "johndoe"
    password = "dogood"

    with pytest.raises(WrongPassError):
        auth_test(
            juju=juju,
            endpoints=endpoints,
            username=username,
            password=password,
            connection_timeout=LDAP_CONNECTION_TIMEOUT_MS,
        )
