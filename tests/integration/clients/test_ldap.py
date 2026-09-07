#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import jubilant
import pytest

from literals import CharmUsers, Substrate
from statuses import AuthStatuses
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
LDAP_NAME = "glauth-k8s"
LDAP_UTILS_NAME = "glauth-utils"
LDAP_PG_NAME = "postgresql-k8s"
LDAP_INGRESS_NAME = "traefik-k8s"
DATA_INTEGRATOR_NAME = "data-integrator"


def test_build_and_deploy(
    charm: str,
    glide_runner_charm: str,
    juju: jubilant.Juju,
    substrate: Substrate,
    juju_k8s_model: jubilant.Juju,
) -> None:
    """Deploy the charm under test, the LDAP stack and Data Integrator."""
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
    juju_k8s_model.deploy(LDAP_NAME, channel="latest/edge", trust=True)
    juju_k8s_model.deploy(LDAP_UTILS_NAME, channel="latest/edge", trust=True)
    juju_k8s_model.deploy(LDAP_INGRESS_NAME, trust=True)
    juju_k8s_model.deploy(TLS_NAME, channel=TLS_CHANNEL)

    logger.info("Add integrations for LDAP")
    juju_k8s_model.integrate(f"{LDAP_NAME}:pg-database", f"{LDAP_PG_NAME}:database")
    juju_k8s_model.integrate(LDAP_NAME, LDAP_UTILS_NAME)
    juju_k8s_model.integrate(LDAP_NAME, TLS_NAME)
    juju_k8s_model.integrate(f"{LDAP_INGRESS_NAME}:certificates", TLS_NAME)

    juju.wait(
        lambda status: are_agents_idle(status, APP_NAME, idle_period=30, unit_count=NUM_UNITS),
        timeout=600,
    )

    # PostgreSQL is deliberately absent: it re-stamps its agent status every few seconds under
    # load, so an `idle_period` it takes part in may never elapse. An active GLAuth already
    # implies a working database.
    juju_k8s_model.wait(
        lambda status: are_agents_idle(
            status,
            LDAP_NAME,
            LDAP_UTILS_NAME,
            TLS_NAME,
            idle_period=30,
        ),
        timeout=600,
    )

    if substrate == Substrate.VM:
        logger.info("Set up ingress")
        juju_k8s_model.integrate(f"{LDAP_NAME}:ingress", f"{LDAP_INGRESS_NAME}:ingress-per-unit")
        # GLAuth replaces its certificate the moment ingress lands, so that the ingress address
        # becomes a SAN. Until the reissued one is loaded it keeps serving a certificate valid
        # only for its in-cluster DNS name, and a Valkey connecting to the ingress address inside
        # that window fails verification and caches the server as unhealthy for good. GLAuth
        # reports `active` throughout the swap, so wait for its agent to fall idle instead.
        # PostgreSQL stays out of the gate for the reason given above.
        juju_k8s_model.wait(
            lambda status: are_apps_active_and_agents_idle(
                status,
                LDAP_NAME,
                LDAP_UTILS_NAME,
                LDAP_INGRESS_NAME,
                TLS_NAME,
                idle_period=30,
            ),
            timeout=600,
        )

    logger.info("Set up LDAP users")
    utils_unit = next(iter(juju_k8s_model.status().get_units(LDAP_UTILS_NAME)))
    ldif_file = "ldap_entries.ldif"
    source_path = f"./tests/integration/clients/data/{ldif_file}"
    target_path = f"/var/tmp/{ldif_file}"
    juju_k8s_model.scp(source_path, f"{utils_unit}:{target_path}")
    ldif_action = juju_k8s_model.run(utils_unit, "apply-ldif", params={"path": target_path})
    assert ldif_action.status == "completed", "ldif-apply should succeed"

    if substrate == Substrate.VM:
        logger.info("Set up cross-model offers")
        juju_k8s_model.offer(app=LDAP_NAME, endpoint="ldap", name="ldap")
        # GLAuth only speaks certificate-transfer v0; take the CA from the provider that issued
        # GLAuth's own certificate instead
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
        # workaround for `DN` missing in GLAuth search entry attributes
        "ldap-search-dn-attribute": "mail",
        # GLAuth models groups as organizational units, unlike the `cn` default
        "ldap-query-template": "(&(objectClass=posixAccount)(memberOf=ou={group},*))",
        # the keys have to match the LDAP groups in ldap_entries.ldif
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
    )
    assert result == "OK", "Failed to write data for charm user with LDAP enabled"

    assert (
        get_key(
            juju=juju,
            endpoints=endpoints,
            username=CharmUsers.VALKEY_ADMIN.value,
            password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
            key=TEST_KEY,
        )
        == TEST_VALUE
    ), "Failed to read data for charm user with LDAP enabled"


def test_ensure_ldap_auth(juju: jubilant.Juju, substrate: Substrate) -> None:
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
    )
    assert result == "OK", f"Failed to write data for user {username} with LDAP enabled"

    assert (
        get_key(
            juju=juju,
            endpoints=endpoints,
            username=username,
            password=password,
            key=TEST_KEY,
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
    )
    assert not result == "OK", "Failed to write data with LDAP enabled"

    logger.info("Ensure access fails for LDAP user without required group membership")
    # connect with user in LDAP group "others"
    username = "jennydoe"
    password = "dogood"

    try:
        assert not auth_test(
            juju=juju,
            endpoints=endpoints,
            username=username,
            password=password,
        )
    except WrongPassError:
        logger.info("Auth test failed as expected")


def test_disable_ldap(juju: jubilant.Juju, substrate: Substrate) -> None:
    """Ensure LDAP users can no longer access Valkey after LDAP was disabled."""
    logger.info("Disabling LDAP")
    if substrate == Substrate.VM:
        ldap_name = "ldap"
    else:
        ldap_name = LDAP_NAME
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
    )
    assert result == "OK", "Failed to write data for charm user with LDAP enabled"

    assert (
        get_key(
            juju=juju,
            endpoints=endpoints,
            username=CharmUsers.VALKEY_ADMIN.value,
            password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
            key=TEST_KEY,
        )
        == TEST_VALUE
    ), "Failed to read data for charm user with LDAP enabled"

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
        )
