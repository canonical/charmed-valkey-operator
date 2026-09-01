#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import re
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Literal, NamedTuple

import jubilant
import yaml
from data_platform_helpers.advanced_statuses.models import StatusObject
from dateutil.parser import parse
from glide import (
    AdvancedGlideClientConfiguration,
    GlideClientConfiguration,
    NodeAddress,
    ServerCredentials,
    TlsAdvancedConfiguration,
)
from ops import SecretNotFoundError, StatusBase
from tenacity import Retrying, stop_after_delay, wait_fixed

from literals import (
    CLIENT_PORT,
    INTERNAL_USERS_PASSWORD_CONFIG,
    INTERNAL_USERS_SECRET_LABEL_SUFFIX,
    PEER_RELATION,
    SENTINEL_PORT,
    SENTINEL_TLS_PORT,
    TLS_PORT,
    CharmUsers,
    Substrate,
)
from tests.integration.glide_helpers import serialize_glide_config

logger = logging.getLogger(__name__)


METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME: str = METADATA["name"]
GLIDE_RUNNER_NAME = "glide-runner"
IMAGE_RESOURCE = {"valkey-image": METADATA["resources"]["valkey-image"]["upstream-source"]}
# DEPLOY_TIMEOUT_TLS_S covers any wait that spans a client-TLS enable: applying the client
# certificate takes a rolling sentinel restart serialized by RestartLock, whose handoffs
# alone take ~6-9 min for three units on a loaded runner.
DEPLOY_TIMEOUT_S = 600
DEPLOY_TIMEOUT_TLS_S = 900
INTERNAL_USERS_SECRET_LABEL = (
    f"{PEER_RELATION}.{APP_NAME}.app.{INTERNAL_USERS_SECRET_LABEL_SUFFIX}"
)
TLS_NAME = "self-signed-certificates"
TLS_CHANNEL = "1/edge"
TLS_CERT_FILE = "client.pem"
TLS_KEY_FILE = "client.key"
TLS_CA_FILE = "client_ca.pem"


def does_status_match(
    model_status: jubilant.Status,
    expected_unit_statuses: dict[str, List[StatusObject]] | None = None,
    expected_app_statuses: dict[str, List[StatusObject]] | None = None,
    num_units: dict[str, int] | None = None,
) -> bool:
    """Check that current app and/or unit status matches expectation for given apps.

    Args:
        model_status: represents the jubilant model's current status
        expected_unit_statuses: dict mapping app name to list of expected StatusObject for units
        expected_app_statuses: dict mapping app name to its list of expected StatusObject
        num_units: dict mapping app name to expected number of units
    """
    return (
        (
            expected_unit_statuses is None
            or _does_unit_workload_status_match(model_status, expected_unit_statuses)
        )
        and (
            expected_app_statuses is None
            or _does_app_status_match(model_status, expected_app_statuses)
        )
        and (num_units is None or verify_unit_count(model_status, unit_count=num_units))
    )


def _does_unit_workload_status_match(
    model_status: jubilant.Status, expected_statuses: dict[str, List[StatusObject]]
) -> bool:
    """Check that current workload status matches expectation for given apps' units.

    Args:
        model_status: represents the jubilant model's current status
        expected_statuses: dict mapping app names to list of expected StatusObject
    """
    return all(
        all(
            any(
                does_message_match(unit_status.workload_status.message, status)
                for status in expected_status
            )
            for unit_status in model_status.get_units(app).values()
        )
        for app, expected_status in expected_statuses.items()
    )


def _does_app_status_match(
    model_status: jubilant.Status, expected_statuses: dict[str, List[StatusObject]]
) -> bool:
    """Check that current app status matches expectation for given apps.

    Args:
        model_status: represents the jubilant model's current status
        expected_statuses: dict mapping app names to list of expected StatusObject
    """
    return all(
        any(
            does_message_match(model_status.apps.get(app).app_status.message, status)
            for status in expected_status
        )
        for app, expected_status in expected_statuses.items()
    )


def does_message_match(expected_status_message: str, status: StatusObject) -> bool:
    """Check if the status message matches the expected message."""
    try:
        juju_status = StatusBase.from_name(status.status, status.message)
        return (
            expected_status_message == juju_status.message
            or expected_status_message.startswith(juju_status.message)
            or juju_status.message.startswith(f"{expected_status_message:.40}")
            or (
                status.short_message is not None
                and status.short_message in expected_status_message
            )
        )
    except KeyError as e:
        logger.error("Error attempting to convert StatusObject to ops.StatusBase: %s", e)
        return False


def are_apps_active_and_agents_idle(
    status: jubilant.Status,
    *apps: str,
    idle_period: int = 0,
    unit_count: int | dict[str, int] | None = None,
) -> bool:
    """Check that all given apps are active, their agents idle (optional idle interval too) and optionally verify unit count as well.

    Args:
        status: represents the jubilant model's current status
        apps: A list of applications whose statuses to test against
        idle_period: Seconds to wait for the agents of each application unit to be idle.
        unit_count: The desired number of units to wait for, can be > to 0.
            If set as int, this value is expected for all apps but if more granularity is needed,
            pass a dictionary such as: {"app1": 2, "app2": 1, ...}, if set to -1, the check
            only happens at the application level.
    """
    return (
        jubilant.all_active(status, *apps)
        and jubilant.all_agents_idle(status, *apps)
        and _check_apps_idle_period(status, *apps, idle_period=idle_period)
        and verify_unit_count(status, *apps, unit_count=unit_count)
    )


def are_agents_idle(
    status: jubilant.Status,
    *apps: str,
    idle_period: int = 0,
    unit_count: int | dict[str, int] | None = None,
) -> bool:
    """Check that agents of all given apps are idle (optional idle interval too). Optionally verify unit count as well.

    Args:
        status: represents the jubilant model's current status
        apps: A list of applications whose statuses to test against
        idle_period: Seconds to wait for the agents of each application unit to be idle.
        unit_count: The desired number of units to wait for, should be > 0.
            If set as int, this value is expected for all apps but if more granularity is needed,
            pass a dictionary such as: {"app1": 2, "app2": 1, ...}, if set to -1, the check
            only happens at the application level.
    """
    return (
        jubilant.all_agents_idle(status, *apps)
        and _check_apps_idle_period(status, *apps, idle_period=idle_period)
        and verify_unit_count(status, *apps, unit_count=unit_count)
    )


def _check_apps_idle_period(status: jubilant.Status, *apps: str, idle_period: int) -> bool:
    return all(
        parse(unit.juju_status.since, ignoretz=True) + timedelta(seconds=idle_period)
        < datetime.now()
        for app in apps
        for unit in status.get_units(app).values()
    )


def utc_now() -> datetime:
    """Return the current UTC time, naive, to compare against Juju status timestamps."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def has_leader_settled_since(
    status: jubilant.Status, app: str, since: datetime, idle_period: int = 0
) -> bool:
    """Whether the leader has run a hook and gone idle again after `since` (naive UTC).

    An `idle_period` on its own cannot express "the charm has reacted to X". An agent that
    has not been woken yet still carries the idle timestamp it had before X, and that
    timestamp already satisfies any period, so the wait returns on its first poll and the
    test races ahead of the charm. Anchoring on `since` waits for the leader to actually
    pick the event up; the period then lets any deferral it left behind drain.

    Only the leader is considered: handlers such as `topology_changed` are leader-only and
    write no peer data, so the other units are never woken and would never settle.
    """
    for unit in status.get_units(app).values():
        if not unit.leader:
            continue
        if unit.juju_status.current != "idle":
            return False
        idle_since = parse(unit.juju_status.since, ignoretz=True)
        return idle_since >= since and idle_since + timedelta(seconds=idle_period) < utc_now()
    return False


def verify_unit_count(
    status: jubilant.Status, *apps: str, unit_count: int | dict[str, int] | None = None
):
    """Verify the unit count for an application.

    Args:
        status: represents the jubilant model's current status
        apps: A list of applications whose statuses to test against
        unit_count: The desired number of units to wait for, can be >= to -1
            if set as int, this value is expected for all apps but if more granularity is needed,
            pass a dictionary such as: {"app1": 2, "app2": 1, ...}, if set to -1, the check
            only happens at the application level.
    """
    if not unit_count:
        return True

    if isinstance(unit_count, int):
        if unit_count == 0:
            return True
        unit_count = dict.fromkeys(apps, unit_count)
    elif not unit_count:
        unit_count = dict.fromkeys(apps, -1)
    else:
        for app in apps:
            if app not in unit_count:
                unit_count[app] = 1

    return all(count == len(status.get_units(app)) for app, count in unit_count.items())


def get_cluster_addresses(juju: jubilant.Juju, app_name: str) -> list[str]:
    """Get the addresses of all units in the Valkey application.

    Args:
        juju: The Juju client instance.
        app_name: The name of the Valkey application.

    Returns:
        A list of addresses for all units in the Valkey application.
    """
    status = juju.status()
    model_info = juju.show_model()

    if model_info.type == "kubernetes":
        return [unit.address for unit in status.get_units(app_name).values()]

    return [unit.public_address for unit in status.get_units(app_name).values()]


def get_cluster_endpoints(juju: jubilant.Juju, app_name: str) -> list[str]:
    """Get the addresses of all units in the Valkey application.

    Args:
        juju: The Juju client instance.
        app_name: The name of the Valkey application.

    Returns:
        A list of addresses for all units in the Valkey application.
    """
    model_info = juju.show_model()

    if model_info.type == "kubernetes":
        return [
            unit_name.replace("/", "-") + "." + app_name + "-endpoints"
            for unit_name in juju.status().get_units(app_name)
        ]

    return get_cluster_addresses(juju, app_name)


def get_secret_by_label(juju: jubilant.Juju, label: str) -> dict[str, str]:
    for secret in juju.secrets():
        if label == secret.label:
            revealed_secret = juju.show_secret(secret.uri, reveal=True)
            return revealed_secret.content

    raise SecretNotFoundError(f"Secret with label {label} not found")


def get_glide_config(
    juju: jubilant.Juju,
    app_name: str,
    endpoints: list[str] | None = None,
    username: str | None = CharmUsers.VALKEY_ADMIN.value,
    password: str | None = None,
    tls_enabled: bool = False,
) -> GlideClientConfiguration:
    """Construct a GlideClientConfiguration from Juju model information and secrets."""
    endpoints = endpoints or get_cluster_endpoints(juju, app_name)
    addresses = [
        NodeAddress(host=host, port=TLS_PORT if tls_enabled else CLIENT_PORT) for host in endpoints
    ]

    credentials = None
    if username or password:
        credentials = ServerCredentials(username=username, password=password)

    tls_cert = tls_key = tls_ca_cert = None
    if tls_enabled:
        # Read locally stored certificate files
        with open("client.pem", "rb") as f:
            tls_cert = f.read()
        with open("client.key", "rb") as f:
            tls_key = f.read()
        with open("client_ca.pem", "rb") as f:
            tls_ca_cert = f.read()

    tls_config = TlsAdvancedConfiguration(
        client_cert_pem=tls_cert if tls_enabled else None,
        client_key_pem=tls_key if tls_enabled else None,
        root_pem_cacerts=tls_ca_cert if tls_enabled else None,
    )

    client_config = GlideClientConfiguration(
        addresses,
        credentials=credentials,
        use_tls=True if tls_enabled else False,
        advanced_config=AdvancedGlideClientConfiguration(tls_config=tls_config),
    )
    return client_config


def set_password(
    juju: jubilant.Juju,
    password: str,
    username: str = CharmUsers.VALKEY_ADMIN.value,
    application: str = APP_NAME,
) -> None:
    """Set a user password (or update it if existing) via secret.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        password: password to use
        username: the user to set the password
        application: the application the created secret will be granted to
    """
    secret_name = "system_users_secret"

    # if secret exists, update it, else add secret
    existing = next((s for s in juju.secrets() if s.name == secret_name), None)
    if existing:
        juju.update_secret(identifier=existing.uri, content={username: password})
        secret_id = existing.uri
    else:
        secret_id = juju.add_secret(name=secret_name, content={username: password})

    # grant the application access to this secret
    juju.grant_secret(identifier=secret_id, app=application)

    # update the application config to include the secret
    juju.config(app=application, values={INTERNAL_USERS_PASSWORD_CONFIG: secret_id})


@contextmanager
def fast_forward(juju: jubilant.Juju, update_interval: str = "10s"):
    """Context manager that temporarily speeds up update-status hooks to fire every 10s."""
    old = juju.model_config()["update-status-hook-interval"]
    juju.model_config({"update-status-hook-interval": update_interval})
    try:
        yield
    finally:
        juju.model_config({"update-status-hook-interval": old})


def download_client_certificate_from_unit(
    juju: jubilant.Juju, app_name: str = APP_NAME, unit_name: str | None = None
) -> None:
    """Copy the client certificate files from a unit to the host's filesystem."""
    unit = unit_name or next(iter(juju.status().get_units(app_name)))
    model_info = juju.show_model()

    if model_info.type == "kubernetes":
        tls_path = "/var/lib/valkey/tls"
        juju.scp(f"{unit}:{tls_path}/{TLS_CERT_FILE}", TLS_CERT_FILE, container="valkey")
        juju.scp(f"{unit}:{tls_path}/{TLS_KEY_FILE}", TLS_KEY_FILE, container="valkey")
        juju.scp(f"{unit}:{tls_path}/ca_certs/{TLS_CA_FILE}", TLS_CA_FILE, container="valkey")
    else:
        tls_path = "/var/snap/charmed-valkey/current/tls"
        juju.scp(f"{unit}:{tls_path}/{TLS_CERT_FILE}", TLS_CERT_FILE)
        juju.scp(f"{unit}:{tls_path}/{TLS_KEY_FILE}", TLS_KEY_FILE)
        juju.scp(f"{unit}:{tls_path}/ca_certs/{TLS_CA_FILE}", TLS_CA_FILE)


def get_primary_ip(
    juju: jubilant.Juju, app: str, tls_enabled: bool = False, addresses: list[str] | None = None
) -> str:
    """Get the primary node of the Valkey cluster.

    Returns:
        The IP address of the primary node.
    """
    addresses = addresses or get_cluster_addresses(juju, app)
    for address in addresses:
        try:
            replication_info = exec_valkey_cli(
                address,
                username=CharmUsers.VALKEY_ADMIN.value,
                password=get_password(juju),
                command="info replication",
                tls_enabled=tls_enabled,
            ).stdout
            # if master then we return the hostname
            if "role:master" in replication_info:
                return address
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(f"Error executing Valkey CLI on {address}: {e}")

    raise ValueError("No primary node found in the cluster")


def get_connected_replicas_from_unit(
    juju: jubilant.Juju, address: str, tls_enabled: bool = False
) -> int:
    """Get the number of replicas connected to the Valkey node at `address`.

    Unlike `get_number_connected_replicas`, this queries the node directly with `valkey-cli`,
    so it is usable in suites that do not deploy the glide-runner.

    Args:
        juju: The Juju client instance.
        address: The address of the node to query.
        tls_enabled: Whether TLS certificates are needed.

    Returns:
        The number of replicas connected to that node.
    """
    replication_info = exec_valkey_cli(
        address,
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju),
        command="info replication",
        tls_enabled=tls_enabled,
    ).stdout
    if not (search_result := re.search(r"connected_slaves:(\d+)", replication_info)):
        raise ValueError("Could not parse number of connected replicas from info output")
    return int(search_result.group(1))


def wait_for_failover(
    juju: jubilant.Juju,
    app: str,
    old_primary_ip: str,
    unit_count: int,
    tls_enabled: bool = False,
    timeout: int = 300,
) -> str:
    """Block until Sentinel has finished a failover and every replica is re-attached.

    `sentinel failover` returns `OK` as soon as the failover is *initiated*: until the old
    primary is demoted and re-attached, writes routed to it fail with `NOREPLICAS` and the
    published client endpoints may still point at it. Idle agents indicate none of this,
    so poll the cluster itself.

    Args:
        juju: The Juju client instance.
        app: The name of the Valkey application.
        old_primary_ip: Address of the primary as it was before the failover.
        unit_count: The number of Valkey units in the cluster.
        tls_enabled: Whether TLS certificates are needed.
        timeout: Seconds to wait for the cluster to converge.

    Returns:
        The address of the new primary.
    """
    new_primary_ip = ""
    for attempt in Retrying(stop=stop_after_delay(timeout), wait=wait_fixed(10), reraise=True):
        with attempt:
            new_primary_ip = get_primary_ip(juju, app, tls_enabled=tls_enabled)
            assert new_primary_ip != old_primary_ip, (
                f"Primary is still {old_primary_ip}, failover has not completed yet."
            )
            connected_replicas = get_connected_replicas_from_unit(
                juju, new_primary_ip, tls_enabled=tls_enabled
            )
            assert connected_replicas == unit_count - 1, (
                f"Expected {unit_count - 1} replicas connected to the new primary"
                f" {new_primary_ip}, got {connected_replicas}."
            )

    logger.info("Failover complete, new primary is %s (was %s)", new_primary_ip, old_primary_ip)
    return new_primary_ip


def get_password(juju: jubilant.Juju, user: CharmUsers = CharmUsers.VALKEY_ADMIN) -> str:
    """Retrieve the password for a given internal user from Juju secrets.

    Args:
        juju: The Juju client instance.
        user: The internal user whose password to retrieve.

    Returns:
        The password for the specified internal user.
    """
    secret = get_secret_by_label(juju, label=INTERNAL_USERS_SECRET_LABEL)
    return secret.get(f"{user.value}-password", "")


valkey_cli_result = NamedTuple(
    "ValkeyCliResult", [("stdout", str), ("stderr", str), ("returncode", int)]
)


def exec_valkey_cli(
    hostname: str,
    username: str,
    password: str,
    command: str,
    tls_enabled: bool = False,
    json: bool = False,
    sentinel: bool = False,
) -> valkey_cli_result:
    """Execute a Valkey CLI command and returns the output as a string."""
    port = TLS_PORT if tls_enabled else CLIENT_PORT
    if sentinel:
        port = SENTINEL_TLS_PORT if tls_enabled else SENTINEL_PORT
    pre_command = f"valkey-cli --no-auth-warning -h {hostname} -p {port} --user {username} --pass {password} {'--json' if json else ''}"
    if tls_enabled:
        pre_command += " --tls --cert client.pem --key client.key --cacert client_ca.pem"
    exec_command = f"{pre_command} {command}"
    result = subprocess.run(
        exec_command.split(),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=10,
    )
    return valkey_cli_result(
        stdout=result.stdout.strip(), stderr=result.stderr.strip(), returncode=result.returncode
    )


def get_quorum(juju: jubilant.Juju, unit_name: str) -> int:
    """Get the currently configured sentinel quorum."""
    status = juju.status()
    model_info = juju.show_model()
    units = status.get_units(APP_NAME)
    unit_endpoint = (
        units[unit_name].public_address
        if model_info.type != "kubernetes"
        else units[unit_name].address
    )
    result = exec_valkey_cli(
        hostname=unit_endpoint,
        username=CharmUsers.SENTINEL_CHARM_ADMIN.value,
        password=get_password(juju, user=CharmUsers.SENTINEL_CHARM_ADMIN),
        command="SENTINEL primary primary",
        sentinel=True,
        json=True,
    )
    return int(json.loads(result.stdout)["quorum"])


def set_key(
    juju: jubilant.Juju,
    endpoints: list[str],
    username: str,
    password: str,
    key: str,
    value: str,
    tls_enabled: bool = False,
) -> str:
    """Write a key-value pair to the Valkey cluster.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        endpoints: List of endpoints of the Valkey cluster nodes.
        username: The username for authentication.
        password: The password for authentication.
        key: The key to set.
        value: The value to set.
        tls_enabled: Whether TLS certificates are needed.
    """
    glide_config = get_glide_config(
        juju=juju,
        app_name=APP_NAME,
        endpoints=endpoints,
        username=username,
        password=password,
        tls_enabled=tls_enabled,
    )
    task = juju.run(
        f"{GLIDE_RUNNER_NAME}/leader",
        "execute",
        params={"command": f"SET {key} {value}", "config": serialize_glide_config(glide_config)},
    )
    if task.status != "completed":
        raise RuntimeError(f"Command execution failed: {task.results}")
    return json.loads(task.results.get("result", "null"))


def get_key(
    juju: jubilant.Juju,
    endpoints: list[str],
    username: str,
    password: str,
    key: str,
    tls_enabled: bool = False,
) -> bytes | None:
    """Read a value from the Valkey cluster by key.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        endpoints: List of endpoints of the Valkey cluster nodes.
        key: The key to read.
        username: The username for authentication.
        password: The password for authentication.
        tls_enabled: Whether TLS certificates are needed.
    """
    glide_config = get_glide_config(
        juju=juju,
        app_name=APP_NAME,
        endpoints=endpoints,
        username=username,
        password=password,
        tls_enabled=tls_enabled,
    )
    task = juju.run(
        f"{GLIDE_RUNNER_NAME}/leader",
        "execute",
        params={"command": f"GET {key}", "config": serialize_glide_config(glide_config)},
    )
    if task.status != "completed":
        raise RuntimeError(f"Command execution failed: {task.results}")
    return json.loads(task.results.get("result", "null"))


def ping(
    hostname: str,
    username: str,
    password: str,
    tls_enabled: bool = False,
) -> bool:
    """Ping a Valkey cluster node.

    Args:
        hostname: The hostname of the Valkey cluster node.
        username: The username for authentication.
        password: The password for authentication.
        tls_enabled: Whether TLS certificates are needed.

    Returns:
        True if the node responds to a ping, False otherwise.
    """
    try:
        return (
            exec_valkey_cli(hostname, username, password, "ping", tls_enabled=tls_enabled).stdout
            == "PONG"
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.warning(f"Error executing Valkey CLI ping on {hostname}: {e}")
        return False


def ping_cluster(
    juju: jubilant.Juju,
    app_name: str,
    endpoints: list[str],
    username: str,
    password: str,
    tls_enabled: bool = False,
) -> bool:
    """Ping all nodes in the Valkey cluster.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        app_name: The name of the Valkey application
        endpoints: List of endpoints of the Valkey cluster nodes.
        username: The username for authentication.
        password: The password for authentication.
        tls_enabled: Whether TLS certificates are needed.

    Returns:
        True if all nodes respond to a ping, False otherwise.
    """
    glide_config = get_glide_config(
        juju=juju,
        app_name=app_name,
        endpoints=endpoints,
        username=username,
        password=password,
        tls_enabled=tls_enabled,
    )
    task = juju.run(
        f"{GLIDE_RUNNER_NAME}/leader",
        "execute",
        params={"command": "ping", "config": serialize_glide_config(glide_config)},
    )
    return task.status == "completed" and json.loads(task.results.get("result", "")) == "PONG"


def get_number_connected_replicas(
    juju: jubilant.Juju,
    glide_runner_unit: str = f"{GLIDE_RUNNER_NAME}/leader",
    tls_enabled: bool = False,
) -> int:
    """Get the number of connected replicas in the Valkey cluster.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        glide_runner_unit: The unit name of the glide-runner to execute the command on
        tls_enabled: Whether TLS certificates are needed.

    Returns:
        The number of connected replicas.
    """
    glide_config = get_glide_config(
        juju=juju,
        app_name=APP_NAME,
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju),
        tls_enabled=tls_enabled,
    )
    task_result = juju.run(
        glide_runner_unit,
        "execute",
        {"command": "info replication", "config": serialize_glide_config(glide_config)},
    )
    assert task_result.status == "completed", f"Command execution failed: {task_result.results}"
    search_result = re.search(r"connected_slaves:(\d+)", task_result.results.get("result", ""))
    if not search_result:
        raise ValueError("Could not parse number of connected replicas from info output")
    return int(search_result.group(1))


class NoAuthError(Exception):
    """Raised when authentication fails due to missing credentials."""


class WrongPassError(Exception):
    """Raised when authentication fails due to incorrect credentials."""


def auth_test(
    juju: jubilant.Juju,
    endpoints: list[str] | None = None,
    username: str | None = None,
    password: str | None = None,
    tls_enabled: bool = False,
    glide_runner_unit: str = f"{GLIDE_RUNNER_NAME}/leader",
) -> bool:
    """Test authentication to the Valkey cluster by attempting to ping it.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        endpoints: List of endpoints of the Valkey cluster nodes. If None, will be retrieved from Juju.
        username: The username for authentication.
        password: The password for authentication.
        tls_enabled: Whether TLS certificates are needed.
        glide_runner_unit: The unit name of the glide-runner to execute the command on

    Returns:
        True if authentication is successful and the cluster responds to a ping, False otherwise.
    """
    glide_config = get_glide_config(
        juju=juju,
        endpoints=endpoints,
        app_name=APP_NAME,
        username=username,
        password=password,
        tls_enabled=tls_enabled,
    )
    task = juju.run(
        glide_runner_unit,
        "execute",
        params={"command": "ping", "config": serialize_glide_config(glide_config)},
    )
    result = json.loads(task.results.get("result", ""))
    if "NOAUTH" in result:
        raise NoAuthError("Authentication failed: NOAUTH error")
    elif "WRONGPASS" in result:
        raise WrongPassError("Authentication failed: WRONGPASS error")
    return task.status == "completed" and result == "PONG"


def remove_number_units(
    juju: jubilant.Juju, app: str, num_units: int, substrate: Substrate
) -> None:
    """Remove a specified number of units from an application.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        app: The name of the application from which to remove units
        num_units: The number of units to remove
        substrate: The substrate type ("k8s" or "vm")
    """
    match substrate:
        case "k8s":
            juju.remove_unit(app, num_units=num_units)
        case "vm":
            # get units names
            unit_names = list(juju.status().get_units(app))
            # remove units by name until num_units have been removed
            juju.remove_unit(*unit_names[:num_units])


def get_data_bag(
    juju: jubilant.Juju,
    app_name: str,
    relation_name: str,
    scope: Literal["app", "unit"] = "unit",
) -> dict:
    """Get the data bag for a given unit.

    Args:
        juju: An instance of Jubilant's Juju class on which to run Juju commands
        app_name: The name of the application whose data bag to retrieve
        relation_name: The name of the relation for which to retrieve the data bag
        scope: Specify whether to get the data bag for the app or unit
    =
    Returns:
        The data bag for the specified unit.
    """
    unit_name = next(iter(juju.status().get_units(app_name)))
    unit_info = juju.cli("show-unit", unit_name, "--format", "json")
    json_info = json.loads(unit_info)
    relation = next(
        rel for rel in json_info[unit_name]["relation-info"] if rel["endpoint"] == relation_name
    )
    if not relation:
        raise ValueError(f"Relation {relation_name} not found for unit {unit_name}")
    if scope == "app":
        return relation["application-data"]
    local_data = relation["local-unit"]["data"]
    remote_data = (
        {u_name: data["data"] for u_name, data in relation["related-units"].items()}
        if relation.get("related-units")
        else {}
    )
    return {unit_name: local_data} | remote_data


def existing_app(juju: jubilant.Juju) -> str | None:
    """Return the name of an existing valkey cluster.

    Returns:
        str | None: name of an application deployment for `valkey` if it exists, None otherwise.
    """
    for app_name, app_status in juju.status().apps.items():
        if "valkey" == app_status.charm_name:
            return app_name

    return None


def get_ip_from_unit(juju: jubilant.Juju, unit_name: str, substrate: Substrate) -> str:
    """Get the IP address of a unit based on the substrate type."""
    for unit, unit_info in juju.status().get_units(unit_name.split("/")[0]).items():
        if unit == unit_name:
            return unit_info.public_address if substrate == Substrate.VM else unit_info.address
    raise ValueError(f"Unit {unit_name} not found in Juju status")


def get_sentinels(juju: jubilant.Juju, primary_ip: str, tls_enabled: bool = False) -> list[dict]:
    """Get the list of sentinels from the data bag."""
    return json.loads(
        exec_valkey_cli(
            primary_ip,
            username=CharmUsers.SENTINEL_CHARM_ADMIN.value,
            password=get_password(juju, user=CharmUsers.SENTINEL_CHARM_ADMIN),
            tls_enabled=tls_enabled,
            command="sentinel sentinels primary",
            json=True,
            sentinel=True,
        ).stdout
    )


def get_full_sync_stat(
    juju: jubilant.Juju, endpoint: str, tls_enabled: bool = False
) -> int | None:
    """Query an endpoint for the `sync_full` stat."""
    result = exec_valkey_cli(
        endpoint,
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
        tls_enabled=tls_enabled,
        command="info stats",
    )

    stats = result.stdout.split("\n")
    for stat in stats:
        if "sync_full" in stat:
            return int(stat.split(":")[1])

    raise ValueError("Stats for full syncs not found")


def get_storage_id(juju: jubilant.Juju, unit_name: str, storage_name: str) -> str | None:
    """Retrieve the storage id associated with a unit."""
    storage_data = juju.cli("storage")
    for line in storage_data.splitlines():
        # skip the header and irrelevant lines
        if not line or "Storage" in line or "detached" in line:
            continue

        if line.split()[0] == unit_name and line.split()[1].startswith(storage_name):
            return line.split()[1]

    raise RuntimeError(f"Storage {storage_name} not found for unit {unit_name}")
