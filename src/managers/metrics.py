# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Metrics manager for configuring and reconciling the Prometheus metrics exporter."""

import logging

from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope

from core.base_workload import WorkloadBase
from core.cluster_state import ClusterState
from literals import METRICS_PORT, TLS_PORT, CharmUsers, Substrate

logger = logging.getLogger(__name__)


class MetricsManager(ManagerStatusProtocol):
    """Manager for the metrics exporter."""

    name = "metrics"
    state: ClusterState

    def __init__(self, state: ClusterState, workload: WorkloadBase) -> None:
        # `ClusterState` satisfies `StatusesStateProtocol`; pyright flags this only
        # because the protocol declares `state` as a mutable (invariant) attribute.
        self.state = state  # pyright: ignore[reportIncompatibleVariableOverride]
        self.workload = workload

    def exporter_env(self) -> dict[str, str]:
        """Compute the environment variables for the metrics exporter."""
        password = self.state.cluster.internal_users_credentials.get(
            CharmUsers.VALKEY_MONITORING.value, ""
        )
        listen_addr = (
            f"0.0.0.0:{METRICS_PORT}"
            if self.state.substrate == Substrate.K8S
            else f"127.0.0.1:{METRICS_PORT}"
        )
        return {
            "REDIS_ADDR": f"rediss://{self.state.endpoint}:{TLS_PORT}",
            "REDIS_USER": CharmUsers.VALKEY_MONITORING.value,
            "REDIS_PASSWORD": password,
            "REDIS_EXPORTER_TLS_CA_CERT_FILE": self.workload.tls_paths.client_ca.as_posix(),
            "REDIS_EXPORTER_TLS_SERVER_NAME": self.state.endpoint,
            "REDIS_EXPORTER_WEB_LISTEN_ADDRESS": listen_addr,
            "REDIS_EXPORTER_INCL_SYSTEM_METRICS": "true",
            "REDIS_EXPORTER_APPEND_INSTANCE_ROLE_LABEL": "true",
            "REDIS_EXPORTER_INCL_CONFIG_METRICS": "false",
        }

    def reconcile(self) -> None:
        """Reconcile the metrics exporter configuration and service state."""
        if not self.workload.can_connect:
            return
        password = self.state.cluster.internal_users_credentials.get(
            CharmUsers.VALKEY_MONITORING.value
        )
        if not password:
            return

        env = self.exporter_env()
        changed = self.workload.configure_metrics_exporter(env)
        if changed:
            logger.info("Metrics exporter configuration updated -> Restarting exporter")
            self.workload.restart(self.workload.metrics_service)

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Compute the metrics manager statuses."""
        return self.state.statuses.get(
            scope=scope,
            component=self.name,
            running_status_only=True,
        ).root
