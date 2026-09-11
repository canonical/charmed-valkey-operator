#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
from time import sleep

import jubilant
import pytest
from tenacity import Retrying, stop_after_attempt, wait_fixed

from literals import CharmUsers, Substrate
from tests.integration.cw_helpers import (
    assert_continuous_writes_consistent,
    assert_continuous_writes_increasing,
    configure_cw_runner,
    start_continuous_writes,
    stop_continuous_writes,
)
from tests.integration.ha.helpers.helpers import (
    K8S_RESTART_DELAY_DEFAULT,
    RESTART_DELAY_PATCHED,
    VM_RESTART_DELAY_DEFAULT,
    get_unit_name_from_primary_ip,
    patch_restart_delay,
    reboot_unit,
    send_process_control_signal,
)

from ..helpers import (
    APP_NAME,
    DEPLOY_TIMEOUT_S,
    DEPLOY_TIMEOUT_TLS_S,
    GLIDE_RUNNER_NAME,
    IMAGE_RESOURCE,
    TLS_CHANNEL,
    TLS_NAME,
    are_apps_active_and_agents_idle,
    download_client_certificate_from_unit,
    exec_valkey_cli,
    existing_app,
    get_cluster_addresses,
    get_ip_from_unit,
    get_number_connected_replicas,
    get_password,
    get_primary_ip,
    ping,
)

logger = logging.getLogger(__name__)

NUM_UNITS = 3
FAILOVER_DELAY = 45
TEST_KEY = "test_key"
TEST_VALUE = "42"
PROCESS_PATTERN = "valkey-server"


@pytest.mark.parametrize("tls_enabled", [False, True], ids=["tls_off", "tls_on"])
def test_build_and_deploy(
    tls_enabled: bool,
    charm: str,
    juju: jubilant.Juju,
    substrate: Substrate,
    glide_runner_charm: str,
) -> None:
    """Build the charm-under-test and deploy it with three units."""
    if app := existing_app(juju):
        logger.info(f"App {app} already exists, skipping deploy.")
        return

    juju.deploy(
        charm,
        resources=IMAGE_RESOURCE if substrate == Substrate.K8S else None,
        num_units=NUM_UNITS,
        trust=True,
    )

    juju.deploy(glide_runner_charm, GLIDE_RUNNER_NAME)

    if tls_enabled:
        juju.deploy(TLS_NAME, channel=TLS_CHANNEL)
        juju.integrate(f"{APP_NAME}:client-certificates", TLS_NAME)

    juju.wait(
        lambda status: are_apps_active_and_agents_idle(
            status, APP_NAME, GLIDE_RUNNER_NAME, idle_period=30
        ),
        timeout=DEPLOY_TIMEOUT_TLS_S if tls_enabled else DEPLOY_TIMEOUT_S,
    )

    assert len(juju.status().apps[APP_NAME].units) == NUM_UNITS, (
        f"Unexpected number of units after initial deploy: expected {NUM_UNITS}, got {len(juju.status().apps[APP_NAME].units)}"
    )


@pytest.mark.parametrize("tls_enabled", [False, True], ids=["tls_off", "tls_on"])
@pytest.mark.parametrize("signal", ["SIGKILL", "SIGTERM"], ids=["sigkill", "sigterm"])
@pytest.mark.parametrize("patched_delay", [False, True], ids=["default_delay", "patched_delay"])
def test_signal_db_process_on_primary(
    tls_enabled: bool,
    signal: str,
    patched_delay: bool,
    juju: jubilant.Juju,
    substrate: Substrate,
) -> None:
    """Make sure the cluster can self-heal when the leader goes down."""
    app_name = existing_app(juju) or APP_NAME
    if tls_enabled:
        download_client_certificate_from_unit(juju, APP_NAME)

    configure_cw_runner(
        juju,
        valkey_app=app_name,
        tls_enabled=tls_enabled,
        substrate=substrate,
    )

    # make sure we have at least two units so we can stop one of them
    init_units_count = len(juju.status().get_units(app_name))
    if init_units_count < 2:
        juju.add_unit(app_name, num_units=2 - init_units_count)
        juju.wait(
            lambda status: are_apps_active_and_agents_idle(
                status, app_name, idle_period=10, unit_count=2
            ),
            timeout=1200,
        )

    init_units_count = len(juju.status().get_units(app_name))
    start_continuous_writes(juju, clear=True)
    sleep(10)

    primary_ip = get_primary_ip(juju, app_name, tls_enabled=tls_enabled)
    assert primary_ip, "Failed to get primary endpoint from valkey."

    logger.info("Axing away primary unit at %s", primary_ip)
    primary_unit_name = get_unit_name_from_primary_ip(juju, primary_ip, substrate)

    if patched_delay:
        logger.info("Patching restart delay to %s seconds.", RESTART_DELAY_PATCHED)
        patch_restart_delay(
            juju=juju,
            unit_name=primary_unit_name,
            delay=RESTART_DELAY_PATCHED,
            substrate=substrate,
        )

    # axe away the database process of the primary
    send_process_control_signal(
        unit_name=primary_unit_name,
        model_full_name=juju.model,
        signal=signal,
        db_process=PROCESS_PATTERN,
        substrate=substrate,
    )
    # make sure the process is stopped
    admin_password = get_password(juju, CharmUsers.VALKEY_ADMIN)
    if substrate == Substrate.VM:
        # K8s restarts much faster so pinging to check will be very flakey
        logger.info("Pinging primary unit to ensure it's down.")
        assert not ping(
            primary_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled
        ), f"Primary unit is still responding after {signal}."

    # ensure the stopped unit was restarted
    restart_delay = (
        VM_RESTART_DELAY_DEFAULT if substrate == Substrate.VM else K8S_RESTART_DELAY_DEFAULT
    )
    if patched_delay:
        restart_delay = RESTART_DELAY_PATCHED

    restart_delay += 10  # add some buffer to the restart delay
    logger.info("Waiting for primary unit to restart. Restart delay is %s seconds.", restart_delay)
    sleep(restart_delay)

    logger.info("Pinging primary unit to ensure it's up.")
    for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(5), reraise=True):
        with attempt:
            assert ping(
                primary_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled
            ), "Primary unit is not responding after restart delay."
            logger.info("Primary unit is available again.")

    # SIGKILL without patching we have 20s before systemd restarts the process not enough for failover
    # SIGTERM just restarts the process so failover should not happen
    addresses = get_cluster_addresses(juju, app_name)
    if patched_delay:
        # failover should have happened during the downtime of the primary since the restart delay is longer than the failover delay
        new_primary_ip = get_primary_ip(
            juju,
            app_name,
            tls_enabled=tls_enabled,
            addresses=[ip for ip in addresses if ip != primary_ip],
        )
        assert new_primary_ip != primary_ip, "Primary IP did not change after failover delay."
        logger.info(
            "Failover successful, new primary is at %s vs old at %s", new_primary_ip, primary_ip
        )

        # reset the restart delay to the original value
        patch_restart_delay(
            juju,
            unit_name=primary_unit_name,
            delay=None,
            substrate=substrate,
        )

    logger.info("Checking number of connected replicas after primary restart.")
    # if failover happened the old primary will need some time to restart and sync with the new primary before it shows up as a connected replica
    for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(10), reraise=True):
        with attempt:
            number_of_replicas = get_number_connected_replicas(juju, tls_enabled=tls_enabled)
            assert number_of_replicas == init_units_count - 1, (
                f"Expected {init_units_count - 1} replicas to be connected after primary restart, got {number_of_replicas}"
            )

    # ensure data is written in the cluster
    logger.info("Checking continuous writes are increasing after primary restart.")
    assert_continuous_writes_increasing(juju)

    stats = stop_continuous_writes(juju)

    assert_continuous_writes_consistent(
        endpoints=get_cluster_addresses(juju, app_name),
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
        last_written_value=stats.last_written_value,
        tls_enabled=tls_enabled,
    )


@pytest.mark.parametrize("tls_enabled", [False, True], ids=["tls_off", "tls_on"])
def test_freeze_db_process_on_primary(
    tls_enabled: bool,
    juju: jubilant.Juju,
    substrate: Substrate,
) -> None:
    """Make sure the cluster can self-heal when the leader goes down."""
    app_name = existing_app(juju) or APP_NAME
    addresses = get_cluster_addresses(juju, app_name)
    if tls_enabled:
        download_client_certificate_from_unit(juju, APP_NAME)

    configure_cw_runner(
        juju,
        valkey_app=app_name,
        tls_enabled=tls_enabled,
        substrate=substrate,
    )

    # make sure we have at least two units so we can stop one of them
    init_units_count = len(juju.status().get_units(app_name))
    if init_units_count < 2:
        juju.add_unit(app_name, num_units=2 - init_units_count)
        juju.wait(
            lambda status: are_apps_active_and_agents_idle(
                status, app_name, idle_period=10, unit_count=2
            ),
            timeout=1200,
        )

    init_units_count = len(juju.status().get_units(app_name))
    start_continuous_writes(juju, clear=True)
    sleep(10)

    primary_ip = get_primary_ip(juju, app_name, tls_enabled=tls_enabled)
    assert primary_ip, "Failed to get primary endpoint from valkey."

    logger.info("Axing away primary unit at %s", primary_ip)
    primary_unit_name = get_unit_name_from_primary_ip(juju, primary_ip, substrate)

    # axe away the database process of the primary
    send_process_control_signal(
        unit_name=primary_unit_name,
        model_full_name=juju.model,
        signal="SIGSTOP",
        db_process=PROCESS_PATTERN,
        substrate=substrate,
    )
    # make sure the process is stopped
    logger.info("Pinging primary unit to ensure it's down.")
    admin_password = get_password(juju, CharmUsers.VALKEY_ADMIN)
    assert not ping(
        primary_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled
    ), "Primary unit is still responding after SIGSTOP."

    # ensure the stopped unit was restarted
    logger.info("Waiting for failover to happen.")
    sleep(FAILOVER_DELAY)

    new_primary_ip = get_primary_ip(juju, app_name, tls_enabled=tls_enabled)
    assert new_primary_ip != primary_ip, "Primary IP did not change after failover delay."
    logger.info("Failover successful, new primary is at %s", new_primary_ip)

    new_primary_unit_name = get_unit_name_from_primary_ip(juju, new_primary_ip, substrate)
    new_primary_hostname = (
        f"{new_primary_unit_name.replace('/', '-')}.{app_name}-endpoints.testing.svc.cluster.local"
    )
    new_primary_endpoint = new_primary_ip if substrate == Substrate.VM else new_primary_hostname

    number_of_replicas = get_number_connected_replicas(juju, tls_enabled=tls_enabled)
    assert number_of_replicas == init_units_count - 2, (
        f"Expected {init_units_count - 2} replicas to be connected, got {number_of_replicas}"
    )

    assert_continuous_writes_increasing(juju)

    send_process_control_signal(
        unit_name=primary_unit_name,
        model_full_name=juju.model,
        signal="SIGCONT",
        db_process=PROCESS_PATTERN,
        substrate=substrate,
    )

    # give time to the unit to start and sync with the other units
    # it will detect a failover happened and switch to be a replica
    for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(5)):
        with attempt:
            if (
                "role:master"
                in exec_valkey_cli(
                    primary_ip,
                    CharmUsers.VALKEY_ADMIN,
                    admin_password,
                    "info replication",
                    tls_enabled=tls_enabled,
                ).stdout
            ):
                logger.warning(
                    "Unit is still primary after SIGCONT, waiting for unit to pick up on failover..."
                )
                raise Exception("Unit is still primary after SIGCONT.")
    assert ping(primary_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled), (
        "Old primary unit is not responding after SIGCONT."
    )
    logger.info("Old primary unit is available again.")

    logger.info("Checking number of connected replicas after primary restart.")
    number_of_replicas = get_number_connected_replicas(juju, tls_enabled=tls_enabled)
    assert number_of_replicas == init_units_count - 1, (
        f"Expected {init_units_count - 1} replicas to be connected after primary restart, got {number_of_replicas}"
    )

    for ip_address in addresses:
        # Make sure all sentinels are connected to new primary
        master_addr = exec_valkey_cli(
            hostname=ip_address,
            username=CharmUsers.SENTINEL_CHARM_ADMIN,
            password=get_password(juju, CharmUsers.SENTINEL_CHARM_ADMIN),
            command="sentinel get-master-addr-by-name primary",
            tls_enabled=tls_enabled,
            sentinel=True,
            json=True,
        ).stdout
        assert json.loads(master_addr)[0] == new_primary_endpoint, (
            f"Sentinel at {ip_address} is not connected to the new primary."
        )

    # ensure data is written in the cluster
    logger.info("Checking continuous writes are increasing after primary restart.")
    assert_continuous_writes_increasing(juju)

    stats = stop_continuous_writes(juju)

    assert_continuous_writes_consistent(
        endpoints=get_cluster_addresses(juju, app_name),
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
        last_written_value=stats.last_written_value,
        tls_enabled=tls_enabled,
    )


@pytest.mark.parametrize("tls_enabled", [False, True], ids=["tls_off", "tls_on"])
def test_full_cluster_restart(
    tls_enabled: bool, juju: jubilant.Juju, substrate: Substrate
) -> None:
    """Make sure the cluster can self-heal after all members went down."""
    app_name = existing_app(juju) or APP_NAME
    if tls_enabled:
        download_client_certificate_from_unit(juju, APP_NAME)

    configure_cw_runner(
        juju,
        valkey_app=app_name,
        tls_enabled=tls_enabled,
        substrate=substrate,
    )

    # make sure we have at least two units so we can stop one of them
    init_units_count = len(juju.status().get_units(app_name))
    if init_units_count < 2:
        juju.add_unit(app_name, num_units=2 - init_units_count)
        juju.wait(
            lambda status: are_apps_active_and_agents_idle(
                status, app_name, idle_period=10, unit_count=2
            ),
            timeout=1200,
        )

    init_units_count = len(juju.status().get_units(app_name))
    start_continuous_writes(juju, clear=True)
    sleep(10)

    # update the restart delay for all units
    for unit in juju.status().get_units(app_name):
        patch_restart_delay(
            juju,
            unit_name=unit,
            delay=RESTART_DELAY_PATCHED,
            substrate=substrate,
        )

    for unit in juju.status().get_units(app_name):
        send_process_control_signal(
            unit_name=unit,
            model_full_name=juju.model,
            signal="SIGTERM",
            db_process=PROCESS_PATTERN,
            substrate=substrate,
        )

    # make sure the process is stopped
    admin_password = get_password(juju, CharmUsers.VALKEY_ADMIN)
    for unit, unit_info in juju.status().get_units(app_name).items():
        unit_ip = unit_info.public_address if substrate == Substrate.VM else unit_info.address
        logger.info("Pinging %s to ensure it's down.", unit)
        assert not ping(
            unit_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled
        ), f"{unit} still responding after SIGTERM."

    # ensure the stopped unit was restarted
    logger.info("Waiting for units to restart.")
    sleep(RESTART_DELAY_PATCHED + 10)

    for unit, unit_info in juju.status().get_units(app_name).items():
        unit_ip = unit_info.public_address if substrate == Substrate.VM else unit_info.address
        logger.info("Pinging %s to ensure it's up.", unit)
        assert ping(unit_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled), (
            f"{unit} is not responding after restart delay."
        )

    logger.info("All units are available again.")

    logger.info("Checking number of connected replicas after primary restart.")

    number_of_replicas = get_number_connected_replicas(juju, tls_enabled=tls_enabled)
    assert number_of_replicas == init_units_count - 1, (
        f"Expected {init_units_count - 1} replicas to be connected after primary restart, got {number_of_replicas}"
    )

    # ensure data is written in the cluster
    logger.info("Checking continuous writes are increasing after primary restart.")
    assert_continuous_writes_increasing(juju)

    stats = stop_continuous_writes(juju)

    assert_continuous_writes_consistent(
        endpoints=get_cluster_addresses(juju, app_name),
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
        last_written_value=stats.last_written_value,
        tls_enabled=tls_enabled,
    )

    # reset the restart delay to the original value
    for unit in juju.status().get_units(app_name):
        patch_restart_delay(
            juju,
            unit_name=unit,
            delay=None,
            substrate=substrate,
        )


@pytest.mark.parametrize("tls_enabled", [False, True], ids=["tls_off", "tls_on"])
def test_full_cluster_crash(tls_enabled: bool, juju: jubilant.Juju, substrate: Substrate) -> None:
    """Make sure the cluster can self-heal after all members went down."""
    app_name = existing_app(juju) or APP_NAME
    if tls_enabled:
        download_client_certificate_from_unit(juju, APP_NAME)

    configure_cw_runner(
        juju,
        valkey_app=app_name,
        tls_enabled=tls_enabled,
        substrate=substrate,
    )

    # make sure we have at least two units so we can stop one of them
    init_units_count = len(juju.status().get_units(app_name))
    if init_units_count < 2:
        juju.add_unit(app_name, num_units=2 - init_units_count)
        juju.wait(
            lambda status: are_apps_active_and_agents_idle(
                status, app_name, idle_period=10, unit_count=2
            ),
            timeout=1200,
        )

    init_units_count = len(juju.status().get_units(app_name))
    start_continuous_writes(juju, clear=True)
    sleep(10)

    # update the restart delay for all units
    for unit in juju.status().get_units(app_name):
        patch_restart_delay(
            juju,
            unit_name=unit,
            delay=RESTART_DELAY_PATCHED,
            substrate=substrate,
        )

    for unit in juju.status().get_units(app_name):
        send_process_control_signal(
            unit_name=unit,
            model_full_name=juju.model,
            signal="SIGKILL",
            db_process=PROCESS_PATTERN,
            substrate=substrate,
        )

    # make sure the process is stopped
    admin_password = get_password(juju, CharmUsers.VALKEY_ADMIN)
    for unit, unit_info in juju.status().get_units(app_name).items():
        unit_ip = unit_info.public_address if substrate == Substrate.VM else unit_info.address
        logger.info("Pinging %s to ensure it's down.", unit)
        assert not ping(
            unit_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled
        ), f"{unit} still responding after SIGKILL."

    # ensure the stopped unit was restarted
    logger.info("Waiting for units to restart.")
    sleep(RESTART_DELAY_PATCHED + 10)

    for unit, unit_info in juju.status().get_units(app_name).items():
        unit_ip = unit_info.public_address if substrate == Substrate.VM else unit_info.address
        logger.info("Pinging %s to ensure it's up.", unit)
        assert ping(unit_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled), (
            f"{unit} is not responding after restart delay."
        )

    logger.info("All units are available again.")

    logger.info("Checking number of connected replicas after primary restart.")

    number_of_replicas = get_number_connected_replicas(juju, tls_enabled=tls_enabled)
    assert number_of_replicas == init_units_count - 1, (
        f"Expected {init_units_count - 1} replicas to be connected after primary restart, got {number_of_replicas}"
    )

    # ensure data is written in the cluster
    logger.info("Checking continuous writes are increasing after primary restart.")
    assert_continuous_writes_increasing(juju)

    stats = stop_continuous_writes(juju)

    assert_continuous_writes_consistent(
        endpoints=get_cluster_addresses(juju, app_name),
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
        last_written_value=stats.last_written_value,
        tls_enabled=tls_enabled,
    )

    # reset the restart delay to the original value
    for unit in juju.status().get_units(app_name):
        patch_restart_delay(
            juju,
            unit_name=unit,
            delay=None,
            substrate=substrate,
        )


@pytest.mark.parametrize("tls_enabled", [False, True], ids=["tls_off", "tls_on"])
def test_reboot_primary(tls_enabled: bool, juju: jubilant.Juju, substrate: Substrate) -> None:
    """Make sure the cluster can self-heal when the leader goes down."""
    app_name = existing_app(juju) or APP_NAME
    if tls_enabled:
        download_client_certificate_from_unit(juju, APP_NAME)

    configure_cw_runner(
        juju,
        valkey_app=app_name,
        tls_enabled=tls_enabled,
        substrate=substrate,
    )

    # make sure we have at least two units so we can stop one of them
    init_units_count = len(juju.status().get_units(app_name))
    if init_units_count < 2:
        juju.add_unit(app_name, num_units=2 - init_units_count)
        juju.wait(
            lambda status: are_apps_active_and_agents_idle(
                status, app_name, idle_period=10, unit_count=2
            ),
            timeout=1200,
        )

    init_units_count = len(juju.status().get_units(app_name))
    start_continuous_writes(juju, clear=True)
    sleep(10)

    primary_ip = get_primary_ip(juju, app_name, tls_enabled=tls_enabled)
    assert primary_ip, "Failed to get primary endpoint from valkey."

    # Reboot the primary unit
    logger.info("Rebooting primary unit at %s", primary_ip)
    primary_unit_name = get_unit_name_from_primary_ip(juju, primary_ip, substrate)

    reboot_unit(juju, primary_unit_name, substrate)

    # wait for unit to reboot
    sleep(3)

    # make sure the process is stopped
    admin_password = get_password(juju, CharmUsers.VALKEY_ADMIN)
    logger.info("Pinging primary unit to ensure it's down.")
    assert not ping(
        primary_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled
    ), "Primary unit is still responding after reboot."

    logger.info("Waiting for primary unit to reboot and become available.")
    juju.wait(
        lambda status: are_apps_active_and_agents_idle(
            status, app_name, idle_period=30, unit_count=init_units_count
        ),
        timeout=1200,
    )

    configure_cw_runner(
        juju,
        valkey_app=app_name,
        tls_enabled=tls_enabled,
        substrate=substrate,
    )

    # on k8s we get a new ip
    new_ip = get_ip_from_unit(juju, primary_unit_name, substrate)
    assert ping(new_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled), (
        "Primary unit is not responding after reboot."
    )

    number_of_replicas = get_number_connected_replicas(juju, tls_enabled=tls_enabled)
    assert number_of_replicas == init_units_count - 1, (
        f"Expected {init_units_count - 1} replicas to be connected, got {number_of_replicas}"
    )

    assert_continuous_writes_increasing(juju)

    stats = stop_continuous_writes(juju)

    assert_continuous_writes_consistent(
        endpoints=get_cluster_addresses(juju, app_name),
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
        last_written_value=stats.last_written_value,
        tls_enabled=tls_enabled,
    )


@pytest.mark.parametrize("tls_enabled", [False, True], ids=["tls_off", "tls_on"])
def test_full_cluster_reboot(tls_enabled: bool, juju: jubilant.Juju, substrate: Substrate) -> None:
    """Make sure the cluster can self-heal after all members went down."""
    app_name = existing_app(juju) or APP_NAME
    if tls_enabled:
        download_client_certificate_from_unit(juju, APP_NAME)

    configure_cw_runner(
        juju,
        valkey_app=app_name,
        tls_enabled=tls_enabled,
        substrate=substrate,
    )

    # make sure we have at least two units so we can stop one of them
    init_units_count = len(juju.status().get_units(app_name))
    if init_units_count < 2:
        juju.add_unit(app_name, num_units=2 - init_units_count)
        juju.wait(
            lambda status: are_apps_active_and_agents_idle(
                status, app_name, idle_period=10, unit_count=2
            ),
            timeout=1200,
        )

    init_units_count = len(juju.status().get_units(app_name))
    start_continuous_writes(juju, clear=True)
    sleep(10)

    for unit in juju.status().get_units(app_name):
        reboot_unit(juju, unit, substrate)

    sleep(3)

    # make sure the process is stopped
    admin_password = get_password(juju, CharmUsers.VALKEY_ADMIN)
    for unit, unit_info in juju.status().get_units(app_name).items():
        unit_ip = unit_info.public_address if substrate == Substrate.VM else unit_info.address
        logger.info("Pinging %s to ensure it's down.", unit)
        assert not ping(
            unit_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled
        ), f"{unit} still responding after reboot."

    # ensure the stopped unit was restarted
    logger.info("Waiting for cluster to become available.")
    juju.wait(
        lambda status: are_apps_active_and_agents_idle(
            status, app_name, idle_period=30, unit_count=init_units_count
        ),
        timeout=1200,
    )

    configure_cw_runner(
        juju,
        valkey_app=app_name,
        tls_enabled=tls_enabled,
        substrate=substrate,
    )

    for unit, unit_info in juju.status().get_units(app_name).items():
        unit_ip = unit_info.public_address if substrate == Substrate.VM else unit_info.address
        logger.info("Pinging %s to ensure it's up.", unit)
        assert ping(unit_ip, CharmUsers.VALKEY_ADMIN, admin_password, tls_enabled=tls_enabled), (
            f"{unit} is not responding after restart delay."
        )

    logger.info("All units are available again.")

    logger.info("Checking number of connected replicas after primary restart.")

    number_of_replicas = get_number_connected_replicas(juju, tls_enabled=tls_enabled)
    assert number_of_replicas == init_units_count - 1, (
        f"Expected {init_units_count - 1} replicas to be connected after primary restart, got {number_of_replicas}"
    )

    # ensure data is written in the cluster
    logger.info("Checking continuous writes are increasing after primary restart.")
    assert_continuous_writes_increasing(juju)

    stats = stop_continuous_writes(juju)

    assert_continuous_writes_consistent(
        endpoints=get_cluster_addresses(juju, app_name),
        username=CharmUsers.VALKEY_ADMIN.value,
        password=get_password(juju, user=CharmUsers.VALKEY_ADMIN),
        last_written_value=stats.last_written_value,
        tls_enabled=tls_enabled,
    )
