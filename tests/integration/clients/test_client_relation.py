#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Test Matrix:
#
# Test name                             | Server TLS | mTLS  | Auth             | TLS provider
# ----------------------------------------------------------------------------------------
# test_integrate_client_interface_v0/1  | no         | no    | user/password    | same as Valkey
# test_enable_tls                       | yes        | no    | user/password    | same as Valkey
# test_certificate_transfer             | yes        | no    | user/password    | different CA
# test_mtls                             | yes        | yes   | user/password    | different CA
# test_certificate_authentication       | yes        | yes   | via certificate  | different CA
import logging

import jubilant
import pytest

from literals import CharmUsers, Substrate
from tests.integration.helpers import (
    APP_NAME,
    DEPLOY_TIMEOUT_TLS_S,
    IMAGE_RESOURCE,
    TLS_CHANNEL,
    TLS_NAME,
    are_agents_idle,
    exec_valkey_cli,
    get_cluster_addresses,
    get_password,
    get_primary_ip,
    has_leader_settled_since,
    utc_now,
    wait_for_failover,
)

logger = logging.getLogger(__name__)

NUM_UNITS = 3
TEST_KEY = "test_key"
TEST_VALUE = "test_value"
REQUIRER_V1_NAME = "req-v1"
REQUIRER_V0_NAME = "req-v0"
REQUIRER_TLS_PROVIDER = "ssc-req"
# Valkey replies `NOPERM No permissions to access a key`, but valkey-glide rewrites the error code
# when it wraps the reply in a RequestError: >=2.4 reports `PermissionDenied`, older builds passed
# `NOPERM` through. Accept either so the assertion tracks the denial, not the client's wording.
PERMISSION_DENIED_CODES = ("NOPERM", "PermissionDenied")


@pytest.fixture
def requirer_charm(arch: str) -> str:
    """Path to the requirer charm file to use for testing."""
    return f"./tests/integration/clients/requirer-charm/requirer-charm_ubuntu@24.04-{arch}.charm"


def test_build_and_deploy(
    charm: str, juju: jubilant.Juju, substrate: Substrate, requirer_charm: str
) -> None:
    """Deploy the charm under test, the client application charms and a TLS provider."""
    juju.deploy(
        charm,
        resources=IMAGE_RESOURCE if substrate == Substrate.K8S else None,
        num_units=NUM_UNITS,
        trust=True,
    )
    juju.deploy(
        requirer_charm,
        app=REQUIRER_V0_NAME,
        config={"data-interfaces-version": "0"},
    )
    juju.deploy(
        requirer_charm,
        app=REQUIRER_V1_NAME,
        config={"data-interfaces-version": "1"},
    )
    juju.deploy(TLS_NAME, channel=TLS_CHANNEL)
    juju.deploy(TLS_NAME, app=REQUIRER_TLS_PROVIDER, channel=TLS_CHANNEL)

    juju.wait(
        lambda status: are_agents_idle(status, APP_NAME, idle_period=30, unit_count=NUM_UNITS),
        timeout=600,
    )


def test_integrate_client_interface_v0(juju: jubilant.Juju) -> None:
    """Create the client integration."""
    logger.info("Integrating client applications")
    juju.integrate(f"{APP_NAME}:valkey-client", f"{REQUIRER_V0_NAME}:valkey-client")
    juju.wait(
        lambda status: are_agents_idle(status, APP_NAME, idle_period=30, unit_count=NUM_UNITS),
        timeout=600,
    )

    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V0_NAME)))
    logger.info("Trying to access global keyspace - should be denied")
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    username = get_credentials_action.results["usernames"]

    with pytest.raises(jubilant.TaskError) as task_error:
        juju.run(
            requirer_unit, "set", params={"key": TEST_KEY, "value": TEST_VALUE, "user": username}
        )
    assert any(code in str(task_error) for code in PERMISSION_DENIED_CODES), (
        "Action expected to fail because of permission denied"
    )

    logger.info("Trying to access granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={"key": f"requirer-charm:{TEST_KEY}", "value": TEST_VALUE, "user": username},
    )
    assert set_action.status == "completed", "Action should succeed"

    get_action = juju.run(
        requirer_unit, "get", params={"key": f"requirer-charm:{TEST_KEY}", "user": username}
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE


def test_integrate_client_interface_v1(juju: jubilant.Juju) -> None:
    """Create the client integration."""
    logger.info("Integrating client applications")
    juju.integrate(f"{APP_NAME}:valkey-client", f"{REQUIRER_V1_NAME}:valkey-client")
    juju.wait(
        lambda status: are_agents_idle(status, APP_NAME, idle_period=30, unit_count=NUM_UNITS),
        timeout=600,
    )

    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V1_NAME)))
    logger.info("Trying to access global keyspace - should be denied")
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    usernames = get_credentials_action.results["usernames"]
    user_restricted_keyspace, user_global_keyspace = usernames.split(",")

    with pytest.raises(jubilant.TaskError) as task_error:
        juju.run(
            requirer_unit,
            "set",
            params={"key": TEST_KEY, "value": TEST_VALUE, "user": user_restricted_keyspace},
        )
    assert any(code in str(task_error) for code in PERMISSION_DENIED_CODES), (
        "Action expected to fail because of permission denied"
    )

    logger.info("Trying to access granted keyspace with restricted permissions")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={
            "key": f"requirer-charm:{TEST_KEY}",
            "value": TEST_VALUE,
            "user": user_restricted_keyspace,
        },
    )
    assert set_action.status == "completed", "Action should succeed"

    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": user_restricted_keyspace},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE

    logger.info("Trying to access global keyspace with unrestricted permissions")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={"key": TEST_KEY, "value": TEST_VALUE, "user": user_global_keyspace},
    )
    assert set_action.status == "completed", "Action should succeed"

    get_action = juju.run(
        requirer_unit, "get", params={"key": TEST_KEY, "user": user_global_keyspace}
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE


def test_failover_topology_update(juju: jubilant.Juju) -> None:
    """Trigger a failover and ensure clients can still access Valkey."""
    ip_address = get_cluster_addresses(juju, APP_NAME)[0]
    old_primary_ip = get_primary_ip(juju, APP_NAME)
    logger.info("Initiate failover through Sentinel %s", ip_address)

    # Anchor before the failover: the gate below has to wait for the charm to react to it,
    # and until the leader is woken it still reports the idle it entered beforehand.
    failover_requested_at = utc_now()

    failover_result = exec_valkey_cli(
        hostname=ip_address,
        username=CharmUsers.SENTINEL_CHARM_ADMIN,
        password=get_password(juju, CharmUsers.SENTINEL_CHARM_ADMIN),
        command="sentinel failover primary",
        tls_enabled=False,
        sentinel=True,
    ).stdout
    assert failover_result == "OK", "Failover not successful"

    wait_for_failover(juju, APP_NAME, old_primary_ip=old_primary_ip, unit_count=NUM_UNITS)
    # Sentinel has switched, but the client still reaches Valkey through whatever the charm
    # last published; `topology_changed` is what re-points it, and only the leader handles it.
    juju.wait(
        lambda status: has_leader_settled_since(
            status, APP_NAME, since=failover_requested_at, idle_period=60
        ),
        timeout=600,
    )

    logger.info("Ensure access after failover for v0 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V0_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    username = get_credentials_action.results["usernames"]

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={"key": f"requirer-charm:{TEST_KEY}", "value": TEST_VALUE, "user": username},
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": username},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE

    logger.info("Ensure access after failover for v1 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V1_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    usernames = get_credentials_action.results["usernames"]
    user_restricted_keyspace = usernames.split(",")[0]

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={
            "key": f"requirer-charm:{TEST_KEY}",
            "value": TEST_VALUE,
            "user": user_restricted_keyspace,
        },
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": user_restricted_keyspace},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE


def test_enable_tls(juju: jubilant.Juju) -> None:
    """Enable TLS on Valkey and the clients and ensure they can still read and write."""
    logger.info("Enabling client TLS")
    juju.integrate(f"{APP_NAME}:client-certificates", TLS_NAME)
    juju.integrate(f"{REQUIRER_V0_NAME}:certificates", TLS_NAME)
    juju.integrate(f"{REQUIRER_V1_NAME}:certificates", TLS_NAME)
    juju.wait(
        lambda status: are_agents_idle(status, APP_NAME, idle_period=30, unit_count=NUM_UNITS),
        timeout=DEPLOY_TIMEOUT_TLS_S,
    )

    logger.info("Ensure TLS access for v0 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V0_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    username = get_credentials_action.results["usernames"]

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={"key": f"requirer-charm:{TEST_KEY}", "value": TEST_VALUE, "user": username},
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": username},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE

    logger.info("Ensure TLS access for v1 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V1_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    usernames = get_credentials_action.results["usernames"]
    user_restricted_keyspace, user_global_keyspace = usernames.split(",")

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={
            "key": f"requirer-charm:{TEST_KEY}",
            "value": TEST_VALUE,
            "user": user_restricted_keyspace,
        },
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": user_restricted_keyspace},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={"key": TEST_KEY, "value": TEST_VALUE, "user": user_global_keyspace},
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": TEST_KEY, "user": user_global_keyspace},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE


def test_certificate_transfer(juju: jubilant.Juju) -> None:
    """Relate Requirer charms to separate TLS providers and ensure functionality."""
    logger.info("Enable certificate transfer to Valkey")
    juju.integrate(f"{APP_NAME}:certificate-transfer", REQUIRER_TLS_PROVIDER)
    juju.wait(
        lambda status: are_agents_idle(status, APP_NAME, idle_period=30, unit_count=NUM_UNITS),
        timeout=600,
    )

    logger.info("Switch Requirer charms to other TLS provider")
    juju.remove_relation(f"{REQUIRER_V0_NAME}:certificates", TLS_NAME)
    juju.remove_relation(f"{REQUIRER_V1_NAME}:certificates", TLS_NAME)
    juju.wait(
        lambda status: are_agents_idle(status, REQUIRER_V1_NAME, idle_period=30),
        timeout=100,
    )

    juju.integrate(f"{REQUIRER_V1_NAME}:certificates", REQUIRER_TLS_PROVIDER)
    juju.integrate(f"{REQUIRER_V0_NAME}:certificates", REQUIRER_TLS_PROVIDER)
    juju.wait(
        lambda status: are_agents_idle(status, REQUIRER_V1_NAME, idle_period=30),
        timeout=100,
    )

    logger.info("Ensure TLS access for v0 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V0_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    username = get_credentials_action.results["usernames"]

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={"key": f"requirer-charm:{TEST_KEY}", "value": TEST_VALUE, "user": username},
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": username},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE

    logger.info("Ensure TLS access for v1 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V1_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    usernames = get_credentials_action.results["usernames"]
    user_restricted_keyspace, user_global_keyspace = usernames.split(",")

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={
            "key": f"requirer-charm:{TEST_KEY}",
            "value": TEST_VALUE,
            "user": user_restricted_keyspace,
        },
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": user_restricted_keyspace},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={"key": TEST_KEY, "value": TEST_VALUE, "user": user_global_keyspace},
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": TEST_KEY, "user": user_global_keyspace},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE


def test_mtls(juju: jubilant.Juju) -> None:
    """Ensure clients can use mTLS and password-authentication."""
    logger.info("Enable config `use-mtls` for requirer charms")
    juju.config(app=REQUIRER_V0_NAME, values={"use-mtls": "true"})
    juju.config(app=REQUIRER_V1_NAME, values={"use-mtls": "true"})
    juju.wait(
        lambda status: are_agents_idle(status, REQUIRER_V1_NAME, idle_period=30),
        timeout=100,
    )

    logger.info("Ensure mTLS access for v0 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V0_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    username = get_credentials_action.results["usernames"]

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={"key": f"requirer-charm:{TEST_KEY}", "value": TEST_VALUE, "user": username},
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": username},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE

    logger.info("Ensure mTLS access for v1 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V1_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    usernames = get_credentials_action.results["usernames"]
    user_restricted_keyspace = usernames.split(",")[0]

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={
            "key": f"requirer-charm:{TEST_KEY}",
            "value": TEST_VALUE,
            "user": user_restricted_keyspace,
        },
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": user_restricted_keyspace},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE


def test_certificate_authentication(juju: jubilant.Juju) -> None:
    """Ensure clients can use mTLS and password-less authentication."""
    logger.info("Enable config `use-certificate-auth` for requirer charms")
    juju.config(app=REQUIRER_V0_NAME, values={"use-certificate-auth": "true"})
    juju.config(app=REQUIRER_V1_NAME, values={"use-certificate-auth": "true"})
    juju.wait(
        lambda status: are_agents_idle(status, REQUIRER_V1_NAME, idle_period=30),
        timeout=100,
    )

    logger.info("Ensure password-less access for v0 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V0_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    username = get_credentials_action.results["usernames"]

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={"key": f"requirer-charm:{TEST_KEY}", "value": TEST_VALUE, "user": username},
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": username},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE

    logger.info("Ensure password-less access for v1 client")
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_V1_NAME)))
    get_credentials_action = juju.run(requirer_unit, "get-credentials")
    usernames = get_credentials_action.results["usernames"]
    user_restricted_keyspace = usernames.split(",")[0]

    logger.info("Write data to granted keyspace")
    set_action = juju.run(
        requirer_unit,
        "set",
        params={
            "key": f"requirer-charm:{TEST_KEY}",
            "value": TEST_VALUE,
            "user": user_restricted_keyspace,
        },
    )
    assert set_action.status == "completed", "Action should succeed"

    logger.info("Read data that was just written")
    get_action = juju.run(
        requirer_unit,
        "get",
        params={"key": f"requirer-charm:{TEST_KEY}", "user": user_restricted_keyspace},
    )
    assert get_action.status == "completed", "Action should succeed"
    result = get_action.results["result"]
    assert result == TEST_VALUE
