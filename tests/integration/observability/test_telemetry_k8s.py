#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""K8s observability integration tests for Valkey charm."""

import json
import logging

import jubilant
import pytest
from tenacity import Retrying, stop_after_delay, wait_fixed

from literals import (
    CLIENT_TLS_RELATION_NAME,
    GRAFANA_DASHBOARD_RELATION,
    LOGGING_RELATION,
    METRICS_ENDPOINT_RELATION,
    METRICS_PORT,
    Substrate,
)
from tests.integration.helpers import (
    APP_NAME,
    DEPLOY_TIMEOUT_S,
    DEPLOY_TIMEOUT_TLS_S,
    TLS_CHANNEL,
    TLS_NAME,
    are_agents_idle,
    are_apps_active_and_agents_idle,
)
from tests.integration.observability.helpers import (
    COS_CHANNEL,
    GRAFANA_APP,
    LOKI_APP,
    NUM_UNITS,
    OTELCOL_K8S_APP,
    PROMETHEUS_APP,
    assert_redis_up_and_single_primary,
    get_relation_data,
    is_integrated,
    read_metrics,
)

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def skip_if_not_k8s(substrate: Substrate):
    """Guard all tests in this module to K8s substrate."""
    if substrate != Substrate.K8S:
        pytest.skip("K8s telemetry integration tests are K8s-only")


def test_deploy(ensure_valkey, juju: jubilant.Juju) -> None:
    """Verify Valkey cluster is active with the expected number of units."""
    status = juju.status()
    assert APP_NAME in status.apps
    assert len(status.apps[APP_NAME].units) == NUM_UNITS


def test_metrics_exporter_without_tls(ensure_valkey, juju: jubilant.Juju) -> None:
    """Verify metrics exporter functions and reports redis_up 1 before TLS is enabled."""
    if is_integrated(juju, APP_NAME, CLIENT_TLS_RELATION_NAME, TLS_NAME):
        logger.info("Removing TLS relation to test without TLS")
        juju.remove_relation(f"{APP_NAME}:{CLIENT_TLS_RELATION_NAME}", TLS_NAME)
        juju.wait(
            lambda s: are_apps_active_and_agents_idle(s, APP_NAME, idle_period=30),
            timeout=DEPLOY_TIMEOUT_TLS_S,
            delay=10,
            successes=3,
        )

    logger.info("Testing metrics exporter without TLS")
    status = juju.status()
    units = list(status.apps[APP_NAME].units.keys())
    assert len(units) == NUM_UNITS

    metrics_by_unit = {u: read_metrics(juju, u) for u in units}
    assert_redis_up_and_single_primary(metrics_by_unit)
    logger.info("Confirmed redis_up 1 and exactly 1 primary across cluster without TLS")


def test_metrics_exporter_with_tls(ensure_valkey, juju: jubilant.Juju) -> None:
    """Verify metrics exporter continues to report redis_up 1 after TLS is enabled."""
    status = juju.status()
    if TLS_NAME not in status.apps:
        logger.info("Deploying TLS provider: %s", TLS_NAME)
        juju.deploy(TLS_NAME, channel=TLS_CHANNEL)

    if not is_integrated(juju, APP_NAME, CLIENT_TLS_RELATION_NAME, TLS_NAME):
        logger.info("Integrating %s:%s with %s", APP_NAME, CLIENT_TLS_RELATION_NAME, TLS_NAME)
        juju.integrate(f"{APP_NAME}:{CLIENT_TLS_RELATION_NAME}", TLS_NAME)
        juju.wait(
            lambda s: are_apps_active_and_agents_idle(s, APP_NAME, TLS_NAME, idle_period=30),
            timeout=DEPLOY_TIMEOUT_TLS_S,
            delay=10,
            successes=3,
        )

    logger.info("Testing metrics exporter with TLS enabled")
    status = juju.status()
    units = list(status.apps[APP_NAME].units.keys())
    assert len(units) == NUM_UNITS

    metrics_by_unit = {u: read_metrics(juju, u) for u in units}
    assert_redis_up_and_single_primary(metrics_by_unit)
    logger.info("Confirmed exporter TLS integration operational across all %s units", NUM_UNITS)


def test_k8s_otelcol_integration(ensure_valkey, juju: jubilant.Juju) -> None:
    """Deploy OpenTelemetry Collector and verify K8s telemetry relations and databags."""
    logger.info("Deploying OpenTelemetry Collector for K8s COS testing")
    status = juju.status()

    if OTELCOL_K8S_APP not in status.apps:
        juju.deploy(
            "opentelemetry-collector-k8s",
            app=OTELCOL_K8S_APP,
            channel=COS_CHANNEL,
            trust=True,
        )

    needs_wait = False
    if not is_integrated(juju, APP_NAME, METRICS_ENDPOINT_RELATION, OTELCOL_K8S_APP):
        logger.info(
            "Integrating %s:%s with %s", APP_NAME, METRICS_ENDPOINT_RELATION, OTELCOL_K8S_APP
        )
        juju.integrate(
            f"{APP_NAME}:{METRICS_ENDPOINT_RELATION}", f"{OTELCOL_K8S_APP}:metrics-endpoint"
        )
        needs_wait = True

    if not is_integrated(juju, APP_NAME, GRAFANA_DASHBOARD_RELATION, OTELCOL_K8S_APP):
        logger.info(
            "Integrating %s:%s with %s", APP_NAME, GRAFANA_DASHBOARD_RELATION, OTELCOL_K8S_APP
        )
        juju.integrate(
            f"{APP_NAME}:{GRAFANA_DASHBOARD_RELATION}",
            f"{OTELCOL_K8S_APP}:grafana-dashboards-consumer",
        )
        needs_wait = True

    if not is_integrated(juju, APP_NAME, LOGGING_RELATION, OTELCOL_K8S_APP):
        logger.info("Integrating %s:%s with %s", APP_NAME, LOGGING_RELATION, OTELCOL_K8S_APP)
        juju.integrate(f"{APP_NAME}:{LOGGING_RELATION}", f"{OTELCOL_K8S_APP}:receive-loki-logs")
        needs_wait = True

    if needs_wait:
        logger.info("Waiting for applications to settle after K8s COS integration")
        juju.wait(
            lambda s: (
                s.apps[APP_NAME].app_status.current == "active"
                and are_agents_idle(s, APP_NAME, OTELCOL_K8S_APP, idle_period=30)
            ),
            timeout=DEPLOY_TIMEOUT_S,
            delay=10,
            successes=3,
        )

    cos_unit = f"{OTELCOL_K8S_APP}/0"

    # 1. Verify metrics-endpoint relation data
    logger.info("Verifying metrics-endpoint relation data on %s", cos_unit)
    for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(5)):
        with attempt:
            metrics_rel = get_relation_data(juju, cos_unit, "metrics-endpoint")
            assert metrics_rel, f"No relation info found for metrics-endpoint on {cos_unit}"
            app_data = metrics_rel.get("application-data") or {}
            assert "scrape_jobs" in app_data, f"Missing 'scrape_jobs' in {app_data}"
            assert f":{METRICS_PORT}" in app_data["scrape_jobs"], (
                f"Scrape target port {METRICS_PORT} not found in scrape_jobs"
            )
            assert "alert_rules" in app_data, f"Missing 'alert_rules' in {app_data}"
            assert "ValkeyDown" in app_data["alert_rules"], "ValkeyDown not found in alert_rules"
            assert "ValkeyMissingPrimary" in app_data["alert_rules"], (
                "ValkeyMissingPrimary not found in alert_rules"
            )
    logger.info("Confirmed scrape_jobs and alert rules delivered via metrics-endpoint")

    # 2. Verify grafana-dashboard relation data
    logger.info("Verifying grafana-dashboard relation data on %s", cos_unit)
    for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(5)):
        with attempt:
            dashboard_rel = get_relation_data(juju, cos_unit, "grafana-dashboards-consumer")
            assert dashboard_rel, (
                f"No relation info found for grafana-dashboards-consumer on {cos_unit}"
            )
            dash_app_data = dashboard_rel.get("application-data") or {}
            assert "dashboards" in dash_app_data, f"Missing 'dashboards' in {dash_app_data}"
            assert len(dash_app_data["dashboards"]) > 0, "No dashboards sent in relation data"
    logger.info("Confirmed Grafana dashboards delivered via grafana-dashboard")

    # 3. Verify logging relation
    logger.info("Verifying logging relation on %s", cos_unit)
    for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(5)):
        with attempt:
            logging_rel = get_relation_data(juju, cos_unit, "receive-loki-logs")
            assert logging_rel, f"No relation info found for receive-loki-logs on {cos_unit}"
            log_app_data = logging_rel.get("application-data") or {}
            assert "alert_rules" in log_app_data, f"Missing 'alert_rules' in {log_app_data}"
            assert "ValkeyBackgroundSaveFailed" in log_app_data["alert_rules"], (
                "ValkeyBackgroundSaveFailed not found in alert_rules"
            )
    logger.info("Confirmed log alert rules delivered via receive-loki-logs")

    for unit_name in juju.status().apps[APP_NAME].units:
        metrics_out = read_metrics(juju, unit_name)
        assert "redis_up" in metrics_out


def test_k8s_cos_lite_full_stack(ensure_valkey, juju: jubilant.Juju) -> None:
    """Deploy COS Lite core charms (Prometheus, Grafana, Loki) and test live telemetry ingestion."""
    logger.info("Deploying core COS Lite applications (Prometheus, Grafana, Loki)")
    status = juju.status()

    # 1. Deploy Prometheus, Grafana, and Loki if not already present
    for app, charm in [
        (PROMETHEUS_APP, "prometheus-k8s"),
        (GRAFANA_APP, "grafana-k8s"),
        (LOKI_APP, "loki-k8s"),
    ]:
        if app not in status.apps:
            logger.info("Deploying %s (%s)", app, charm)
            juju.deploy(charm, app=app, channel=COS_CHANNEL, trust=True)

    # 2. Integrate with Valkey
    needs_wait = False
    if not is_integrated(juju, APP_NAME, METRICS_ENDPOINT_RELATION, PROMETHEUS_APP):
        logger.info(
            "Integrating %s:%s with %s", APP_NAME, METRICS_ENDPOINT_RELATION, PROMETHEUS_APP
        )
        juju.integrate(
            f"{APP_NAME}:{METRICS_ENDPOINT_RELATION}", f"{PROMETHEUS_APP}:metrics-endpoint"
        )
        needs_wait = True

    if not is_integrated(juju, APP_NAME, GRAFANA_DASHBOARD_RELATION, GRAFANA_APP):
        logger.info("Integrating %s:%s with %s", APP_NAME, GRAFANA_DASHBOARD_RELATION, GRAFANA_APP)
        juju.integrate(
            f"{APP_NAME}:{GRAFANA_DASHBOARD_RELATION}", f"{GRAFANA_APP}:grafana-dashboard"
        )
        needs_wait = True

    if not is_integrated(juju, APP_NAME, LOGGING_RELATION, LOKI_APP):
        logger.info("Integrating %s:%s with %s", APP_NAME, LOGGING_RELATION, LOKI_APP)
        juju.integrate(f"{APP_NAME}:{LOGGING_RELATION}", f"{LOKI_APP}:logging")
        needs_wait = True

    # 3. Connect Prometheus and Loki as datasources for Grafana
    if not is_integrated(juju, PROMETHEUS_APP, "grafana-source", GRAFANA_APP):
        logger.info("Integrating %s:grafana-source with %s", PROMETHEUS_APP, GRAFANA_APP)
        juju.integrate(f"{PROMETHEUS_APP}:grafana-source", f"{GRAFANA_APP}:grafana-source")
        needs_wait = True

    if not is_integrated(juju, LOKI_APP, "grafana-source", GRAFANA_APP):
        logger.info("Integrating %s:grafana-source with %s", LOKI_APP, GRAFANA_APP)
        juju.integrate(f"{LOKI_APP}:grafana-source", f"{GRAFANA_APP}:grafana-source")
        needs_wait = True

    if needs_wait:
        logger.info("Waiting for COS Lite and Valkey to become active and idle")
        juju.wait(
            lambda s: are_apps_active_and_agents_idle(
                s, APP_NAME, PROMETHEUS_APP, GRAFANA_APP, LOKI_APP, idle_period=30
            ),
            timeout=DEPLOY_TIMEOUT_S,
            delay=10,
            successes=3,
        )

    status = juju.status()
    prom_ip = status.apps[PROMETHEUS_APP].units[f"{PROMETHEUS_APP}/0"].address
    grafana_ip = status.apps[GRAFANA_APP].units[f"{GRAFANA_APP}/0"].address
    loki_ip = status.apps[LOKI_APP].units[f"{LOKI_APP}/0"].address
    probe_unit = list(status.apps[APP_NAME].units.keys())[0]

    # 4. Verify Prometheus live scrape via PromQL API
    logger.info("Querying Prometheus PromQL API for redis_up metric")
    prom_query_cmd = f"curl -sf 'http://{prom_ip}:9090/api/v1/query?query=redis_up'"
    prom_res = json.loads(juju.ssh(target=probe_unit, command=prom_query_cmd))
    assert prom_res.get("status") == "success", f"PromQL query failed: {prom_res}"
    results = prom_res["data"]["result"]
    assert len(results) == NUM_UNITS, (
        f"Expected {NUM_UNITS} scrape targets in Prometheus, got {len(results)}"
    )
    assert all(r["value"][1] == "1" for r in results), (
        f"Expected all targets redis_up=1, got {results}"
    )

    # 5. Verify Prometheus alert rules loaded
    logger.info("Querying Prometheus Rules API for Valkey alert rules")
    prom_rules_cmd = f"curl -sf 'http://{prom_ip}:9090/api/v1/rules'"
    rules_res = json.loads(juju.ssh(target=probe_unit, command=prom_rules_cmd))
    assert rules_res.get("status") == "success", f"Rules query failed: {rules_res}"
    groups = rules_res["data"]["groups"]
    rule_names = [
        r.get("name")
        for g in groups
        if "valkey" in g.get("name", "").lower()
        for r in g.get("rules", [])
    ]
    assert "ValkeyDown" in rule_names, f"ValkeyDown alert not in Prometheus rules: {rule_names}"
    assert "ValkeyMissingPrimary" in rule_names, (
        f"ValkeyMissingPrimary alert not in Prometheus rules: {rule_names}"
    )
    logger.info("Confirmed Prometheus is actively evaluating Valkey alert rules")

    # 6. Verify Grafana registered the Valkey dashboard
    logger.info("Querying Grafana Search API for Valkey dashboard")
    pw_res = json.loads(
        juju.cli("run", f"{GRAFANA_APP}/0", "get-admin-password", "--format", "json")
    )
    admin_password = pw_res[f"{GRAFANA_APP}/0"]["results"]["admin-password"]
    grafana_cmd = (
        f"curl -sf -u admin:{admin_password} 'http://{grafana_ip}:3000/api/search?query=valkey'"
    )
    grafana_res = json.loads(juju.ssh(target=probe_unit, command=grafana_cmd))
    assert any("valkey" in d.get("title", "").lower() for d in grafana_res), (
        f"Valkey dashboard not found in Grafana search results: {grafana_res}"
    )
    logger.info("Confirmed Grafana dashboard registered and accessible")

    # 7. Verify Loki received logs
    logger.info("Querying Loki LogQL API for Valkey logs")
    loki_cmd = f"curl -sf 'http://{loki_ip}:3100/loki/api/v1/query?query=%7Bjob%3D~%22.%2B%22%7D'"
    loki_res = json.loads(juju.ssh(target=probe_unit, command=loki_cmd))
    assert loki_res.get("status") == "success", f"Loki query failed: {loki_res}"
    assert len(loki_res["data"]["result"]) > 0, "No log streams found in Loki"
    logger.info("COS Lite full integration verified successfully")
