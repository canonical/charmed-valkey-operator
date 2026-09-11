#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""VM observability integration tests for Valkey charm."""

import json
import logging

import jubilant
import pytest
from tenacity import Retrying, stop_after_delay, wait_fixed

from literals import (
    CLIENT_TLS_RELATION_NAME,
    COS_AGENT_RELATION,
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
    OTELCOL_VM_APP,
    PROMETHEUS_APP,
    assert_redis_up_and_single_primary,
    ensure_k8s_dns_resolution,
    get_or_create_k8s_model,
    get_subordinate_relation_data,
    is_integrated,
    probe_port,
    read_metrics,
)

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def skip_if_not_vm(substrate: Substrate):
    """Guard all tests in this module to VM substrate."""
    if substrate != Substrate.VM:
        pytest.skip("VM telemetry integration tests are VM-only")


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

    logger.info("Testing metrics exporter without TLS on loopback")
    status = juju.status()
    units = list(status.apps[APP_NAME].units.keys())
    assert len(units) == NUM_UNITS

    metrics_by_unit = {u: read_metrics(juju, u) for u in units}
    assert_redis_up_and_single_primary(metrics_by_unit)
    logger.info("Confirmed redis_up 1 and exactly 1 primary on loopback without TLS")


def test_metrics_exporter_loopback_security(ensure_valkey, juju: jubilant.Juju) -> None:
    """Verify that port 9121 is bound strictly to loopback and refused on private network IP."""
    status = juju.status()
    first_unit = list(status.apps[APP_NAME].units.keys())[0]
    unit_info = status.apps[APP_NAME].units[first_unit]
    unit_ip = unit_info.public_address or unit_info.address

    logger.info(
        "Probing metrics port %s on private IP %s of %s", METRICS_PORT, unit_ip, first_unit
    )
    assert not probe_port(juju, first_unit, unit_ip), (
        f"Port {METRICS_PORT} must not be reachable on private IP {unit_ip} on VM (loopback only)"
    )
    logger.info("Confirmed port %s is refused on network address %s", METRICS_PORT, unit_ip)


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


def test_vm_cos_agent_integration(ensure_valkey, juju: jubilant.Juju) -> None:
    """Deploy subordinate OpenTelemetry Collector and verify cos-agent telemetry databag."""
    logger.info("Deploying subordinate OpenTelemetry Collector for VM COS testing")
    status = juju.status()

    if OTELCOL_VM_APP not in status.apps:
        juju.deploy(
            "opentelemetry-collector",
            app=OTELCOL_VM_APP,
            channel=COS_CHANNEL,
        )

    needs_wait = False
    if not is_integrated(juju, APP_NAME, COS_AGENT_RELATION, OTELCOL_VM_APP):
        logger.info("Integrating %s:%s with %s", APP_NAME, COS_AGENT_RELATION, OTELCOL_VM_APP)
        juju.integrate(f"{APP_NAME}:{COS_AGENT_RELATION}", f"{OTELCOL_VM_APP}:cos-agent")
        needs_wait = True

    if needs_wait:
        logger.info("Waiting for applications to settle after cos-agent integration")
        juju.wait(
            lambda s: (
                s.apps[APP_NAME].app_status.current == "active"
                and are_agents_idle(s, APP_NAME, OTELCOL_VM_APP, idle_period=30)
            ),
            timeout=DEPLOY_TIMEOUT_S,
            delay=10,
            successes=3,
        )

    valkey_unit = f"{APP_NAME}/0"
    logger.info("Verifying cos-agent relation data for %s", valkey_unit)
    raw_config = None
    for attempt in Retrying(stop=stop_after_delay(180), wait=wait_fixed(5)):
        with attempt:
            cos_rel = get_subordinate_relation_data(juju, valkey_unit, COS_AGENT_RELATION)
            assert cos_rel, (
                f"No relation info found for {COS_AGENT_RELATION} on subordinate of {valkey_unit}"
            )
            related_units = cos_rel.get("related-units") or {}
            valkey_data = next(
                (u.get("data", {}) for name, u in related_units.items() if APP_NAME in name),
                {},
            )
            raw_config = valkey_data.get("config")
            assert raw_config, (
                f"config not yet available in relation data for {APP_NAME} on subordinate of {valkey_unit}"
            )

    config = json.loads(raw_config)

    # 1. Verify scrape jobs (localhost:9121)
    scrape_jobs = config.get("metrics_scrape_jobs", [])
    assert any(
        f"localhost:{METRICS_PORT}" in target
        for job in scrape_jobs
        for sc in job.get("static_configs", [])
        for target in sc.get("targets", [])
    ), f"Scrape target localhost:{METRICS_PORT} not found in scrape_jobs: {scrape_jobs}"
    logger.info("Confirmed localhost:%s scrape job delivered via cos-agent", METRICS_PORT)

    # 2. Verify dashboards
    dashboards = config.get("dashboards", [])
    assert len(dashboards) > 0, "No dashboards found in cos-agent data"
    logger.info("Confirmed %d Grafana dashboard(s) delivered via cos-agent", len(dashboards))

    # 3. Verify metrics alert rules
    metrics_rules = json.dumps(config.get("metrics_alert_rules", {}))
    assert "ValkeyDown" in metrics_rules, "ValkeyDown alert not found in cos-agent rules"
    assert "ValkeyMissingPrimary" in metrics_rules, (
        "ValkeyMissingPrimary alert not found in cos-agent rules"
    )
    logger.info("Confirmed metrics alert rules delivered via cos-agent")

    # 4. Verify log alert rules
    log_rules = json.dumps(config.get("log_alert_rules", {}))
    assert "ValkeyBackgroundSaveFailed" in log_rules, (
        "ValkeyBackgroundSaveFailed alert not found in cos-agent rules"
    )
    logger.info("Confirmed log alert rules delivered via cos-agent")

    for unit_name in juju.status().apps[APP_NAME].units:
        output = read_metrics(juju, unit_name)
        assert "redis_up" in output

    logger.info("VM cos-agent integration test completed successfully")


def _setup_cos_lite_k8s(juju_k8s: jubilant.Juju, k8s_model_name: str) -> None:
    """Deploy COS Lite applications and create cross-model offers in K8s model."""
    status_k8s = juju_k8s.status()
    for app, charm in [
        (PROMETHEUS_APP, "prometheus-k8s"),
        (GRAFANA_APP, "grafana-k8s"),
        (LOKI_APP, "loki-k8s"),
    ]:
        if app not in status_k8s.apps:
            logger.info("Deploying %s in K8s model %s", app, k8s_model_name)
            juju_k8s.deploy(charm, app=app, channel=COS_CHANNEL, trust=True)

    if not is_integrated(juju_k8s, PROMETHEUS_APP, "grafana-source", GRAFANA_APP):
        juju_k8s.integrate(f"{PROMETHEUS_APP}:grafana-source", f"{GRAFANA_APP}:grafana-source")
    if not is_integrated(juju_k8s, LOKI_APP, "grafana-source", GRAFANA_APP):
        juju_k8s.integrate(f"{LOKI_APP}:grafana-source", f"{GRAFANA_APP}:grafana-source")

    existing_offers = juju_k8s.status().offers
    if "prometheus-remote-write" not in existing_offers:
        juju_k8s.cli(
            "offer",
            f"{k8s_model_name}.{PROMETHEUS_APP}:receive-remote-write",
            "prometheus-remote-write",
            include_model=False,
        )
    if "loki-logging" not in existing_offers:
        juju_k8s.cli(
            "offer",
            f"{k8s_model_name}.{LOKI_APP}:logging",
            "loki-logging",
            include_model=False,
        )
    if "grafana-dashboard" not in existing_offers:
        juju_k8s.cli(
            "offer",
            f"{k8s_model_name}.{GRAFANA_APP}:grafana-dashboard",
            "grafana-dashboard",
            include_model=False,
        )


def _connect_vm_to_cos_lite(juju: jubilant.Juju, k8s_model_name: str) -> bool:
    """Consume offers and connect otelcol on VM to COS Lite backends."""
    status_vm = juju.status()
    for offer_name in ["prometheus-remote-write", "loki-logging", "grafana-dashboard"]:
        if offer_name not in status_vm.apps:
            logger.info("Consuming offer %s.%s", k8s_model_name, offer_name)
            juju.cli("consume", f"{k8s_model_name}.{offer_name}")

    needs_wait = False
    if not is_integrated(juju, OTELCOL_VM_APP, "send-remote-write", "prometheus-remote-write"):
        juju.integrate(f"{OTELCOL_VM_APP}:send-remote-write", "prometheus-remote-write")
        needs_wait = True
    if not is_integrated(juju, OTELCOL_VM_APP, "send-loki-logs", "loki-logging"):
        juju.integrate(f"{OTELCOL_VM_APP}:send-loki-logs", "loki-logging")
        needs_wait = True
    if not is_integrated(juju, OTELCOL_VM_APP, "grafana-dashboards-provider", "grafana-dashboard"):
        juju.integrate(f"{OTELCOL_VM_APP}:grafana-dashboards-provider", "grafana-dashboard")
        needs_wait = True

    ensure_k8s_dns_resolution(juju, APP_NAME)
    return needs_wait


def _verify_telemetry_in_cos_lite(juju: jubilant.Juju, juju_k8s: jubilant.Juju) -> None:
    """Verify live metrics, alert rules, and dashboard in COS Lite."""
    status_k8s = juju_k8s.status()
    prom_ip = status_k8s.apps[PROMETHEUS_APP].units[f"{PROMETHEUS_APP}/0"].address
    grafana_ip = status_k8s.apps[GRAFANA_APP].units[f"{GRAFANA_APP}/0"].address
    probe_unit = list(juju.status().apps[APP_NAME].units.keys())[0]

    # Verify Prometheus PromQL query
    logger.info("Querying Prometheus PromQL API for redis_up metric across substrates")
    prom_query_cmd = f"curl -sf 'http://{prom_ip}:9090/api/v1/query?query=redis_up'"
    for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(10)):
        with attempt:
            prom_res = json.loads(juju.ssh(target=probe_unit, command=prom_query_cmd))
            assert prom_res.get("status") == "success", f"PromQL query failed: {prom_res}"
            results = prom_res["data"]["result"]
            assert len(results) == NUM_UNITS, (
                f"Expected {NUM_UNITS} scrape targets in Prometheus, got {len(results)}"
            )
            assert all(r["value"][1] == "1" for r in results), (
                f"Expected all targets redis_up=1, got {results}"
            )

    logger.info("Confirmed all %s VM units reporting redis_up=1 in Prometheus", NUM_UNITS)

    # Verify Prometheus Alert Rules loaded
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
    logger.info("Confirmed Prometheus is actively evaluating Valkey alert rules from VM")

    # Verify Grafana Dashboard registered
    logger.info("Querying Grafana Search API for Valkey dashboard")
    pw_res = json.loads(
        juju_k8s.cli("run", f"{GRAFANA_APP}/0", "get-admin-password", "--format", "json")
    )
    admin_password = pw_res[f"{GRAFANA_APP}/0"]["results"]["admin-password"]
    grafana_cmd = (
        f"curl -sf -u admin:{admin_password} 'http://{grafana_ip}:3000/api/search?query=valkey'"
    )
    grafana_res = json.loads(juju.ssh(target=probe_unit, command=grafana_cmd))
    assert any("valkey" in d.get("title", "").lower() for d in grafana_res), (
        f"Valkey dashboard not found in Grafana search results: {grafana_res}"
    )
    logger.info("Confirmed Grafana dashboard registered and accessible via VM otelcol")


def test_vm_cos_lite_full_stack(ensure_valkey, juju: jubilant.Juju) -> None:
    """Deploy COS Lite applications in a Kubernetes model and test live telemetry from VM."""
    logger.info("Setting up Kubernetes model for COS Lite")
    juju_k8s = get_or_create_k8s_model(juju, model_name="cos-lite")
    k8s_model_name = juju_k8s.model.split(":")[-1]

    _setup_cos_lite_k8s(juju_k8s, k8s_model_name)
    needs_wait = _connect_vm_to_cos_lite(juju, k8s_model_name)

    if needs_wait:
        logger.info("Waiting for COS Lite and otelcol to become active")
        juju_k8s.wait(
            lambda s: are_apps_active_and_agents_idle(
                s, PROMETHEUS_APP, GRAFANA_APP, LOKI_APP, idle_period=30
            ),
            timeout=DEPLOY_TIMEOUT_S,
            delay=10,
            successes=3,
        )
        juju.wait(
            lambda s: (
                s.apps[APP_NAME].app_status.current == "active"
                and s.apps[OTELCOL_VM_APP].app_status.current == "active"
                and are_agents_idle(s, APP_NAME, OTELCOL_VM_APP, idle_period=30)
            ),
            timeout=DEPLOY_TIMEOUT_S,
            delay=10,
            successes=3,
        )

    _verify_telemetry_in_cos_lite(juju, juju_k8s)
