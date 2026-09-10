# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Observability event handlers for COS integrations."""

import logging
from typing import TYPE_CHECKING

import ops
from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v1.loki_push_api import LogForwarder
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider

from literals import (
    DASHBOARDS_DIR,
    GRAFANA_DASHBOARD_RELATION,
    LOGGING_RELATION,
    LOGS_RULES_DIR,
    METRICS_ENDPOINT_RELATION,
    METRICS_PORT,
    METRICS_RULES_DIR,
    Substrate,
)

if TYPE_CHECKING:
    from charm import ValkeyCharm

logger = logging.getLogger(__name__)


class ObservabilityEvents(ops.Object):
    """Event handler for observability integrations."""

    def __init__(self, charm: "ValkeyCharm") -> None:
        super().__init__(charm, "observability")
        self.charm = charm

        if self.charm.state.substrate == Substrate.K8S:
            self.metrics_endpoint = MetricsEndpointProvider(
                self.charm,
                relation_name=METRICS_ENDPOINT_RELATION,
                jobs=[{"static_configs": [{"targets": [f"*:{METRICS_PORT}"]}]}],
                alert_rules_path=METRICS_RULES_DIR,
            )
            self.grafana_dashboards = GrafanaDashboardProvider(
                self.charm,
                relation_name=GRAFANA_DASHBOARD_RELATION,
                dashboards_path=DASHBOARDS_DIR,
            )
            self.log_forwarder = LogForwarder(
                self.charm,
                relation_name=LOGGING_RELATION,
                alert_rules_path=LOGS_RULES_DIR,
            )
