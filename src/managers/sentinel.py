#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for all sentinel related tasks."""

import logging

import tenacity
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from tenacity import retry, retry_if_result, stop_after_attempt, wait_fixed

from common.client import SentinelClient
from common.exceptions import (
    CannotSeeAllActiveSentinelsError,
    NotAllDepartingSentinelsStoppedError,
    SentinelFailoverError,
    SentinelIncorrectReplicaCountError,
    ValkeyCannotGetPrimaryIPError,
    ValkeyWorkloadCommandError,
)
from common.k8s_client import K8sClient
from core.base_workload import WorkloadBase
from core.cluster_state import ClusterState
from literals import (
    CLIENT_PORT,
    PRIMARY_NAME,
    SENTINEL_DOWN_AFTER_MS,
    SENTINEL_DOWN_AFTER_SUPPRESSED_MS,
    SENTINEL_PORT,
    SENTINEL_TLS_PORT,
    TLS_PORT,
    CharmUsers,
    K8sService,
    Substrate,
)
from statuses import CharmStatuses, ScaleDownStatuses

logger = logging.getLogger(__name__)


class SentinelManager(ManagerStatusProtocol):
    """Manage sentinel members."""

    name: str = "sentinel"
    state: ClusterState

    def __init__(self, state: ClusterState, workload: WorkloadBase):
        # `ClusterState` satisfies `StatusesStateProtocol`; pyright flags this only
        # because the protocol declares `state` as a mutable (invariant) attribute.
        self.state = state  # pyright: ignore[reportIncompatibleVariableOverride]
        self.workload = workload
        self.admin_user = CharmUsers.SENTINEL_CHARM_ADMIN.value

        self.k8s_client: K8sClient | None = None
        if self.state.substrate == Substrate.K8S:
            self.k8s_client = K8sClient(
                namespace=self.state.model.name,
                app_name=self.state.model.app.name,
            )

    @property
    def admin_password(self) -> str:
        """Get the password of the admin user for the sentinel service."""
        return self.state.cluster.internal_users_credentials.get(
            CharmUsers.SENTINEL_CHARM_ADMIN.value, ""
        )

    def _get_sentinel_client(self) -> SentinelClient:
        """Get a client connection to Sentinel."""
        return SentinelClient(
            username=self.admin_user,
            password=self.admin_password,
            tls=self.state.unit_server.is_tls_enabled,
            workload=self.workload,
        )

    @retry(
        wait=wait_fixed(5),
        stop=stop_after_attempt(6),
        retry=retry_if_result(lambda result: result is False),
        retry_error_callback=lambda _: False,
    )
    def is_sentinel_discovered(self) -> bool:
        """Check if the sentinel of the local unit was discovered by the other sentinels in the cluster."""
        # list of active sentinels: units with started flag true and not being removed
        active_sentinels = [
            unit.get_endpoint(self.state.substrate)
            for unit in self.state.servers
            if unit.is_active and unit.get_endpoint(self.state.substrate) != self.state.endpoint
        ]

        client = self._get_sentinel_client()

        for sentinel_host in active_sentinels:
            try:
                discovered_sentinels = {
                    sentinel["ip"] for sentinel in client.sentinels_primary(hostname=sentinel_host)
                }
                if self.state.endpoint not in discovered_sentinels:
                    logger.warning(
                        "Sentinel at %s does not see local sentinel at %s.",
                        sentinel_host,
                        self.state.endpoint,
                    )
                    return False

            except ValkeyWorkloadCommandError:
                logger.warning(
                    "Could not query sentinel at %s for primary discovery.", sentinel_host
                )
                return False
        return True

    def get_primary_ip(self) -> str:
        """Get the IP address of the primary node in the cluster.

        This method queries the sentinels in the cluster for the primary information and returns the primary's IP address.

        Raises:
            ValkeyCannotGetPrimaryIPError: If the CLI command to get primary information fails on all sentinels.
        """
        started_servers = [
            unit.get_endpoint(self.state.substrate)
            for unit in self.state.servers
            if unit.is_active
        ]

        client = self._get_sentinel_client()

        for unit_ip in started_servers:
            try:
                return client.get_primary_addr_by_name(hostname=unit_ip)[0]
            except ValkeyWorkloadCommandError:
                logger.warning(
                    "Could not query sentinel for primary information from server at %s.",
                    unit_ip,
                )
                continue
        logger.error(
            "Could not determine primary IP from sentinels: %s.",
            started_servers,
        )
        raise ValkeyCannotGetPrimaryIPError("Could not determine primary IP from sentinels.")

    def get_primary_endpoint(self) -> str:
        """Get the endpoint of the primary node, consisting of address and port."""
        port = TLS_PORT if self.state.unit_server.is_tls_enabled else CLIENT_PORT

        if self.state.substrate == Substrate.K8S:
            # get the DNS name of the K8s service
            primary_address = f"{self.state.model.app.name}-{K8sService.PRIMARY.value}"
            return f"{primary_address}:{port}"

        primary_address = self.get_primary_ip()
        return f"{primary_address}:{port}"

    def get_replica_endpoints(self) -> str:
        """Get the endpoints of all replica nodes, consisting of address and port."""
        port = TLS_PORT if self.state.unit_server.is_tls_enabled else CLIENT_PORT

        if self.state.substrate == Substrate.K8S:
            # get the DNS name of the K8s service
            replicas_address = f"{self.state.model.app.name}-{K8sService.REPLICAS.value}"
            return f"{replicas_address}:{port}"

        client = self._get_sentinel_client()
        replica_list = client.replicas_primary(hostname=self.state.endpoint)
        return ",".join(sorted([f"{replica['ip']}:{port}" for replica in replica_list]))

    def get_sentinel_endpoints(self) -> str:
        """Get the endpoints of all sentinel nodes, consisting of address and port."""
        started_servers = [
            unit.get_endpoint(self.state.substrate)
            for unit in self.state.servers
            if unit.is_active
        ]
        port = SENTINEL_TLS_PORT if self.state.unit_server.is_tls_enabled else SENTINEL_PORT

        return ",".join(sorted([f"{server}:{port}" for server in started_servers]))

    @retry(
        wait=wait_fixed(5),
        stop=stop_after_attempt(6),
        retry=retry_if_result(lambda result: result is False),
        retry_error_callback=lambda _: False,
    )
    def is_healthy(self) -> bool:
        """Check if the sentinel service is healthy."""
        client = self._get_sentinel_client()

        if not client.ping(hostname=self.state.endpoint):
            logger.warning("Health check failed: Sentinel did not respond to ping.")
            return False

        try:
            client.primary(hostname=self.state.endpoint)
        except ValkeyWorkloadCommandError:
            logger.warning("Health check failed: Could not query sentinel for master information.")
            return False

        return True

    def is_sentinel_active(self, sentinel_ip: str) -> bool:
        """Check if the sentinel is active and responds to a ping."""
        client = self._get_sentinel_client()
        return client.ping(hostname=sentinel_ip)

    def failover(self) -> None:
        """Trigger a failover in the cluster.

        This method triggers a failover through the sentinel client and then checks if the failover is in progress.

        Raises:
            SentinelFailoverError: If triggering failover fails or if failover does not start after triggering.
        """
        client = self._get_sentinel_client()
        try:
            client.failover_primary_coordinated(self.state.endpoint)
            if client.is_failover_in_progress(self.state.endpoint):
                raise SentinelFailoverError("Failover is in progress after triggering failover.")
        except ValkeyWorkloadCommandError as e:
            logger.error("Failed to trigger failover: %s", e)
            raise SentinelFailoverError from e

    def remove_departed_sentinels_from_cluster(self) -> None:
        """Ensure departed units get removed from the Sentinel cluster.

        Raises:
            ValkeyCannotGetPrimaryIPError: If fails to query primary
            ValkeyWorkloadCommandError: If any client command fails
            NotAllDepartingSentinelsStoppedError: If a departing unit has not yet stopped its Sentinel
            CannotSeeAllActiveSentinelsError: If any Sentinel does not see all other Sentinels
            SentinelIncorrectReplicaCountError: If any sentinel sees an incorrect number of replicas
        """
        if not self.state.cluster.model.sentinel_reset_required:
            return

        logger.info("Resetting Sentinels after unit removal")
        primary_ip = self.get_primary_ip()
        active_sentinels = self.get_active_sentinel_ips(primary_ip)
        expected_sentinels = self.all_sentinel_endpoints()

        # ensure that no departing sentinels is still alive before the reset
        if (
            len(expected_sentinels) != self.state.model.app.planned_units()
            or len(active_sentinels) != self.state.model.app.planned_units()
            or set(active_sentinels) != set(expected_sentinels)
        ):
            raise NotAllDepartingSentinelsStoppedError(
                "Cluster should have %s units, but %s Sentinels are active",
                self.state.model.app.planned_units(),
                len(active_sentinels),
            )

        self.reset_sentinel_states(active_sentinels)
        self.verify_expected_replica_count(active_sentinels)
        self.state.cluster.update({"sentinel_reset_required": False})

    def reset_sentinel_states(self, sentinel_ips: list[str]) -> None:
        """Reset the sentinel states on all sentinels in the cluster.

        Raises:
            ValkeyWorkloadCommandError: If the CLI command to reset sentinel state fails on any sentinel.
            CannotSeeAllActiveSentinelsError: If any sentinel does not see all other active sentinels after reset.
        """
        client = self._get_sentinel_client()

        for sentinel_ip in sentinel_ips:
            logger.debug("Resetting sentinel state on %s.", sentinel_ip)
            client.reset(hostname=sentinel_ip)

            if not self.target_sees_all_others(
                target_sentinel_ip=sentinel_ip, sentinel_ips=sentinel_ips
            ):
                logger.warning(
                    "Sentinel at %s does not see all other sentinels after reset.", sentinel_ip
                )
                raise CannotSeeAllActiveSentinelsError(
                    "Sentinel at %s does not see all other sentinels after reset." % sentinel_ip
                )

            # ensure the cluster can still reach quorum before continuing to the next sentinel
            primary_ip = self.get_primary_ip()
            logger.debug("Reset completed for %s, primary ip is %s", sentinel_ip, primary_ip)

    @retry(
        wait=wait_fixed(5),
        stop=stop_after_attempt(6),
        retry=retry_if_result(lambda result: result is False),
        retry_error_callback=lambda _: False,
    )
    def target_sees_all_others(self, target_sentinel_ip: str, sentinel_ips: list[str]) -> bool:
        """Check if the sentinel of the local unit sees all the other sentinels in the cluster.

        Args:
            target_sentinel_ip: The IP address of the sentinel to check.
            sentinel_ips: The list of IP addresses of all active sentinels in the cluster.

        Returns:
            bool: True if the target sentinel sees all other sentinels, False otherwise.
        """
        client = self._get_sentinel_client()

        sentinel_ips_set = set(sentinel_ips) - {target_sentinel_ip}

        logger.debug(
            "Checking if sentinel at %s sees all other sentinels: %s",
            target_sentinel_ip,
            sentinel_ips_set,
        )

        try:
            discovered_sentinels = {
                sentinel["ip"]
                for sentinel in client.sentinels_primary(hostname=target_sentinel_ip)
            }
            if discovered_sentinels != sentinel_ips_set:
                logger.warning(
                    "Sentinel at %s sees sentinels %s, expected %s.",
                    target_sentinel_ip,
                    discovered_sentinels,
                    sentinel_ips_set,
                )
                return False
        except ValkeyWorkloadCommandError:
            logger.warning(
                "Could not query sentinel at %s for sentinel discovery.", target_sentinel_ip
            )
            return False
        return True

    @retry(
        wait=wait_fixed(5),
        stop=stop_after_attempt(6),
        reraise=True,
    )
    def verify_expected_replica_count(self, sentinel_ips: list[str]) -> None:
        """Verify that the sentinels in the cluster see the expected number of replicas.

        The expected number of replicas is the number of active sentinels minus one (the primary).

        Args:
            sentinel_ips: The list of IP addresses of all active sentinels in the cluster.

        Raises:
            SentinelIncorrectReplicaCountError: If any sentinel sees an incorrect number of replicas.
            ValkeyWorkloadCommandError: If the CLI command to get replica information fails on any sentinel.
        """
        client = self._get_sentinel_client()

        # all started servers except primary are expected to be replicas
        expected_replicas = len(sentinel_ips) - 1
        logger.debug(
            "Verifying expected replica count. Expected replicas: %d, active servers: %s",
            expected_replicas,
            sentinel_ips,
        )

        for sentinel_ip in sentinel_ips:
            if expected_replicas != (
                number_replicas := len(client.replicas_primary(hostname=sentinel_ip))
            ):
                logger.warning(
                    "Sentinel at %s sees %d replicas, expected %d.",
                    sentinel_ip,
                    number_replicas,
                    expected_replicas,
                )
                raise SentinelIncorrectReplicaCountError(
                    "Sentinel at %s sees %d replicas, expected %d.",
                    sentinel_ip,
                    number_replicas,
                    expected_replicas,
                )

    def get_active_sentinel_ips(self, hostname: str) -> list[str]:
        """Get a list of IP addresses of the active sentinels in the cluster.

        Args:
            hostname: The hostname to query the sentinels from.

        Returns:
            list[str]: A list of IP addresses of the active sentinels.

        Raises:
            ValkeyWorkloadCommandError: If the CLI command to get sentinel information fails.
        """
        client = self._get_sentinel_client()

        all_sentinels = [hostname] + [
            sentinel["ip"] for sentinel in client.sentinels_primary(hostname=hostname)
        ]

        active_sentinels = []
        for sentinel in all_sentinels:
            if self.is_sentinel_active(sentinel):
                active_sentinels.append(sentinel)

        return active_sentinels

    def restart_service(self) -> None:
        """Restart the sentinel service to load configuration."""
        logger.info("Restarting sentinel service")
        self.workload.restart(self.workload.sentinel_service)

    def all_sentinel_endpoints(self) -> list[str]:
        """Endpoints of every active unit's sentinel (the suppression fan-out set)."""
        return [
            unit.get_endpoint(self.state.substrate)
            for unit in self.state.servers
            if unit.is_active
        ]

    def is_failover_in_progress(self) -> bool:
        """Snapshot whether Sentinel reports a failover in flight for the primary.

        Reads the primary flags directly rather than the ``@retry``-decorated
        ``SentinelClient.is_failover_in_progress`` (which blocks ~3 min): the
        restore guard needs an instantaneous, fail-fast read.
        """
        client = self._get_sentinel_client()
        return "failover_in_progress" in client.primary(hostname=self.state.endpoint).get(
            "flags", ""
        )

    def suppress_failover(self) -> None:
        """Raise down-after on every sentinel so the primary's restart isn't seen as a failure.

        Fail-closed: raise on a non-OK SET before the primary is stopped, else that
        sentinel could promote a replica mid-restore.
        """
        client = self._get_sentinel_client()
        for endpoint in self.all_sentinel_endpoints():
            if not client.set(
                endpoint,
                PRIMARY_NAME,
                "down-after-milliseconds",
                str(SENTINEL_DOWN_AFTER_SUPPRESSED_MS),
            ):
                raise SentinelFailoverError(f"Failed to suppress failover on sentinel {endpoint}")
        logger.info("restore.failover_suppressed: down-after raised on every sentinel")

    def resume_failover(self) -> None:
        """Reset down-after and SENTINEL RESET on every sentinel; idempotent.

        Best-effort on a non-OK SET (logged, not raised) — raising on a restore-
        failure path would re-wedge the restore. A sentinel left stuck (unreachable
        here; it persists SET to its own conf, so a restart won't clear it) is
        self-healed by its own unit via is_failover_suppressed/resume_local_failover.
        """
        for endpoint in self.all_sentinel_endpoints():
            self._resume_failover_on(endpoint)
        logger.info("restore.failover_resumed: down-after reset on every sentinel")

    def resume_local_failover(self) -> None:
        """Reset down-after and SENTINEL RESET on this unit's sentinel only."""
        self._resume_failover_on(self.state.endpoint)

    def is_failover_suppressed(self) -> bool:
        """Whether this unit's sentinel still has the restore-suppressed down-after."""
        client = self._get_sentinel_client()
        down_after = client.primary(hostname=self.state.endpoint).get("down-after-milliseconds")
        return str(down_after) == str(SENTINEL_DOWN_AFTER_SUPPRESSED_MS)

    def reconcile_failover_suppression(self) -> None:
        """Reset this unit's sentinel if a restore left it failover-suppressed.

        resume_failover is best-effort on every restore-failure path, and Sentinel
        persists SENTINEL SET to its own conf (a restart won't clear it), so a
        sentinel unreachable at that moment would otherwise stay at the suppressed
        down-after -- no automatic failover -- until the next config re-render.
        Callers invoke this outside a restore, where the value must be the
        configured one. An unreachable sentinel is skipped, not an error: the next
        call retries it.
        """
        try:
            if not self.is_failover_suppressed():
                return
            logger.warning(
                "restore.suppression_leak: sentinel still failover-suppressed outside a "
                "restore; resuming failover on this unit"
            )
            self.resume_local_failover()
        except ValkeyWorkloadCommandError as e:
            logger.warning("Could not check sentinel failover suppression: %s", e)

    def _resume_failover_on(self, endpoint: str) -> None:
        client = self._get_sentinel_client()
        if not client.set(
            endpoint, PRIMARY_NAME, "down-after-milliseconds", str(SENTINEL_DOWN_AFTER_MS)
        ):
            logger.warning("Failed to reset down-after-milliseconds on sentinel %s", endpoint)
        client.reset(hostname=endpoint)

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Compute the sentinel manager's statuses."""
        status_list: list[StatusObject] = self.state.statuses.get(
            scope=scope, component=self.name, running_status_only=True, running_status_type="async"
        ).root

        # Peer relation not established yet or model not built yet
        if not self.state.cluster.model:
            return status_list or [CharmStatuses.ACTIVE_IDLE.value]

        if (
            scope == "app"
            and self.state.cluster.model.sentinel_reset_required
            and self.state.model.app.planned_units() != 0
        ):
            status_list.append(ScaleDownStatuses.SENTINEL_NOT_REMOVED_AFTER_SCALEDOWN.value)

        return status_list or [CharmStatuses.ACTIVE_IDLE.value]

    @tenacity.retry(wait=tenacity.wait_fixed(5), stop=tenacity.stop_after_attempt(8), reraise=True)
    def get_primary_ip_for_scale_down(self) -> str:
        """Get the primary IP to use for scale down operations.

        Retry to get the primary ip until 2x restart delay is reached.
        Pebble uses backoff and is maxed at 30s
        Snap delay is set at 20s
        40s covers both substrates
        """
        return self.get_primary_ip()

    def get_configured_quorum(self) -> int:
        """Get the currently configured quorum for the sentinel cluster."""
        client = self._get_sentinel_client()
        return int(client.primary(self.state.endpoint)["quorum"])

    def set_quorum(self, quorum: int) -> None:
        """Set the quorum for the sentinel cluster."""
        client = self._get_sentinel_client()
        client.set(self.state.endpoint, PRIMARY_NAME, "quorum", str(quorum))

    def reconcile_k8s_services(self) -> None:
        """Create or update the services in Kubernetes."""
        valkey_port = TLS_PORT if self.state.unit_server.is_tls_enabled else CLIENT_PORT

        self.k8s_client.ensure_endpoint_service(role=K8sService.PRIMARY.value, port=valkey_port)  # pyright: ignore[reportOptionalMemberAccess]
        self.k8s_client.ensure_endpoint_service(role=K8sService.REPLICAS.value, port=valkey_port)  # pyright: ignore[reportOptionalMemberAccess]

    def set_pod_labels(self) -> None:
        """Set labels for primary and replica pods in Kubernetes."""
        primary_endpoint = self.get_primary_ip()
        for unit in self.state.servers:
            if not unit.is_active:
                continue

            pod_name = unit.unit_name.replace("/", "-")
            self.k8s_client.update_pod_label(  # pyright: ignore[reportOptionalMemberAccess]
                pod_name=pod_name,
                role=K8sService.PRIMARY.value
                if unit.get_endpoint(Substrate.K8S) == primary_endpoint
                else K8sService.REPLICAS.value,
            )
