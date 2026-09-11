#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for ObservabilityEvents across K8s and VM substrates."""

import json

import ops.testing as testing

from literals import (
    CONTAINER,
    COS_AGENT_RELATION,
    DASHBOARDS_DIR,
    GRAFANA_DASHBOARD_RELATION,
    LOGGING_RELATION,
    LOGS_RULES_DIR,
    METRICS_ENDPOINT_RELATION,
    METRICS_PORT,
    METRICS_RULES_DIR,
    PEER_RELATION,
)
from src.charm import ValkeyCharm


def test_observability_providers_k8s():
    """On K8s, K8s telemetry providers must be instantiated and cos_agent must not."""
    ctx = testing.Context(ValkeyCharm)
    container = testing.Container(
        name=CONTAINER,
        can_connect=True,
    )
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
    )
    state = testing.State(
        leader=True,
        containers=[container],
        relations=[peer_relation],
    )
    with ctx(ctx.on.update_status(), state) as mgr:
        charm = mgr.charm
        obs = charm.observability_events
        assert obs.metrics_endpoint._relation_name == METRICS_ENDPOINT_RELATION
        assert obs.grafana_dashboards._relation_name == GRAFANA_DASHBOARD_RELATION
        assert obs.log_forwarder._relation_name == LOGGING_RELATION
        assert not hasattr(obs, "cos_agent")


def test_observability_providers_vm(vm_environment):
    """On VM, COSAgentProvider must be instantiated and K8s providers must not."""
    ctx = testing.Context(ValkeyCharm)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
    )
    state = testing.State(
        leader=True,
        relations=[peer_relation],
    )
    with ctx(ctx.on.update_status(), state) as mgr:
        charm = mgr.charm
        obs = charm.observability_events
        assert hasattr(obs, "cos_agent")
        assert obs.cos_agent is not None
        assert not hasattr(obs, "metrics_endpoint")
        assert not hasattr(obs, "grafana_dashboards")
        assert not hasattr(obs, "log_forwarder")


def test_cos_agent_configuration_vm(vm_environment):
    """Assert COSAgentProvider parameters match expected paths, ports, and relations."""
    ctx = testing.Context(ValkeyCharm)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
    )
    state = testing.State(
        leader=True,
        relations=[peer_relation],
    )
    with ctx(ctx.on.update_status(), state) as mgr:
        charm = mgr.charm
        cos_agent = charm.observability_events.cos_agent
        assert cos_agent._relation_name == COS_AGENT_RELATION
        assert cos_agent._metrics_endpoints == [{"path": "/metrics", "port": METRICS_PORT}]
        assert cos_agent._metrics_rules == METRICS_RULES_DIR
        assert cos_agent._logs_rules == LOGS_RULES_DIR
        assert cos_agent._dashboard_dirs == [DASHBOARDS_DIR]


def test_cos_agent_relation_data_vm(vm_environment):
    """Assert COSAgentProvider populates unit databag on relation events under VM."""
    ctx = testing.Context(ValkeyCharm)
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
    )
    cos_relation = testing.Relation(
        id=2,
        endpoint=COS_AGENT_RELATION,
        interface="cos_agent",
    )
    state = testing.State(
        leader=True,
        relations=[peer_relation, cos_relation],
    )
    out = ctx.run(ctx.on.config_changed(), state)
    cos_rel_out = out.get_relation(cos_relation.id)
    raw_config = cos_rel_out.local_unit_data.get("config")
    assert raw_config is not None
    config = json.loads(raw_config)
    assert "metrics_scrape_jobs" in config
    assert "dashboards" in config
    assert "metrics_alert_rules" in config
    assert "log_alert_rules" in config
    # Ensure localhost:9121 is in the scrape target
    scrape_jobs = config["metrics_scrape_jobs"]
    assert any(
        f"localhost:{METRICS_PORT}" in target
        for job in scrape_jobs
        for sc in job.get("static_configs", [])
        for target in sc.get("targets", [])
    )


def test_same_rules_and_dashboards_both_substrates(monkeypatch):
    """Ensure both K8s and VM reference the exact same rule and dashboard constants."""
    # 1. K8s
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
    ctx_k8s = testing.Context(ValkeyCharm)
    container = testing.Container(
        name=CONTAINER,
        can_connect=True,
    )
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
    )
    state_k8s = testing.State(
        leader=True,
        containers=[container],
        relations=[peer_relation],
    )
    with ctx_k8s(ctx_k8s.on.update_status(), state_k8s) as mgr_k8s:
        k8s_obs = mgr_k8s.charm.observability_events
        k8s_metrics_rules = k8s_obs.metrics_endpoint._alert_rules_path
        k8s_logs_rules = k8s_obs.log_forwarder._alert_rules_path
        k8s_dashboards = k8s_obs.grafana_dashboards._dashboards_path

    # 2. VM
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    ctx_vm = testing.Context(ValkeyCharm)
    state_vm = testing.State(
        leader=True,
        relations=[peer_relation],
    )
    with ctx_vm(ctx_vm.on.update_status(), state_vm) as mgr_vm:
        vm_cos = mgr_vm.charm.observability_events.cos_agent
        assert vm_cos._metrics_rules in k8s_metrics_rules
        assert vm_cos._logs_rules in k8s_logs_rules
        assert vm_cos._dashboard_dirs[0] in k8s_dashboards
