#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Shared helpers and fixtures for observability integration tests."""

import json
import logging
import re
import subprocess

import jubilant

from literals import METRICS_PORT
from tests.integration.helpers import APP_NAME

logger = logging.getLogger(__name__)

NUM_UNITS = 3
COS_CHANNEL = "2/stable"
OTELCOL_K8S_APP = "otelcol-k8s"
OTELCOL_VM_APP = "otelcol"
PROMETHEUS_APP = "prometheus"
GRAFANA_APP = "grafana"
LOKI_APP = "loki"


def read_metrics(juju: jubilant.Juju, unit_name: str, host: str = "127.0.0.1") -> str:
    """Fetch metrics from exporter on the given unit, falling back to python3 urllib if curl is absent."""
    cmd = (
        f"curl -sf http://{host}:{METRICS_PORT}/metrics 2>/dev/null "
        f"|| python3 -c \"import urllib.request; print(urllib.request.urlopen('http://{host}:{METRICS_PORT}/metrics').read().decode())\""
    )
    return juju.ssh(target=unit_name, command=cmd)


def probe_port(juju: jubilant.Juju, unit_name: str, host: str, port: int = METRICS_PORT) -> bool:
    """Check if a specific port is reachable on the given host address."""
    cmd = (
        f'python3 -c "import urllib.request; '
        f"urllib.request.urlopen('http://{host}:{port}/metrics', timeout=2)\""
    )
    try:
        juju.ssh(target=unit_name, command=cmd)
        return True
    except Exception:
        return False


def get_relation_data(juju: jubilant.Juju, unit_name: str, endpoint: str) -> dict:
    """Retrieve relation data dictionary for a specific endpoint from juju show-unit."""
    raw = juju.cli("show-unit", unit_name, "--format", "json")
    unit_data = json.loads(raw)[unit_name]
    for rel in unit_data.get("relation-info", []):
        if rel.get("endpoint") == endpoint:
            return rel
    return {}


def get_subordinate_relation_data(juju: jubilant.Juju, principal_unit: str, endpoint: str) -> dict:
    """Retrieve relation data from the subordinate unit attached to principal_unit."""
    status = juju.status()
    unit_status = status.apps[APP_NAME].units.get(principal_unit)
    if not unit_status or not unit_status.subordinates:
        return {}

    subordinate_unit = list(unit_status.subordinates.keys())[0]
    return get_relation_data(juju, subordinate_unit, endpoint)


def is_integrated(juju: jubilant.Juju, app_name: str, endpoint: str, target_app: str) -> bool:
    """Check if an application endpoint is currently integrated with target_app."""
    status = juju.status()
    if app_name not in status.apps:
        return False
    relations = status.apps[app_name].relations.get(endpoint, [])
    return any(rel.related_app == target_app for rel in relations)


def assert_redis_up_and_single_primary(metrics_by_unit: dict[str, str]) -> None:
    """Assert all units report redis_up 1 and exactly one unit reports instance_role="master"."""
    master_count = 0
    for unit_name, metrics_output in metrics_by_unit.items():
        assert re.search(r"^redis_up(?:\{[^}]*\})?\s+1", metrics_output, re.MULTILINE), (
            f"Expected 'redis_up 1' on {unit_name}, got:\n{metrics_output[:500]}"
        )
        if re.search(r'redis_up\{[^}]*instance_role="master"[^}]*\}\s+1', metrics_output):
            master_count += 1

    assert master_count == 1, (
        f"Expected exactly 1 master reported across exporter instances, found {master_count}"
    )


def get_or_create_k8s_model(juju: jubilant.Juju, model_name: str = "cos-lite") -> jubilant.Juju:
    """Get or create a Kubernetes model on the active controller for COS Lite deployments."""
    raw_models = juju.cli("models", "--format", "json", include_model=False)
    models_list = json.loads(raw_models).get("models", [])

    for m in models_list:
        if m.get("name") in [model_name, f"admin/{model_name}"]:
            j_k8s = jubilant.Juju(model=model_name)
            j_k8s.wait_timeout = 1000
            return j_k8s

    controller_name = None
    try:
        whoami = json.loads(juju.cli("whoami", "--format", "json", include_model=False))
        controller_name = whoami.get("controller")
    except Exception:
        show_ctrl = json.loads(
            juju.cli("show-controller", "--format", "json", include_model=False)
        )
        controller_name = next(iter(show_ctrl.keys()), None)

    clouds_cmd = ["clouds", "--format", "json"]
    if controller_name:
        clouds_cmd.extend(["-c", controller_name])

    raw_clouds = juju.cli(*clouds_cmd, include_model=False)
    clouds_dict = json.loads(raw_clouds)
    k8s_cloud_name = None
    for c_name, c_info in clouds_dict.items():
        if c_info.get("type") == "k8s":
            k8s_cloud_name = c_name
            break

    if not k8s_cloud_name:
        add_k8s_args = ["add-k8s", "k8s-cloud"]
        if controller_name:
            add_k8s_args.extend(["-c", controller_name])
        try:
            juju.cli(*add_k8s_args, include_model=False)
            k8s_cloud_name = "k8s-cloud"
        except Exception:
            k8s_cloud_name = "k8s"

    juju.cli("add-model", model_name, k8s_cloud_name, include_model=False)
    j_k8s = jubilant.Juju(model=model_name)
    j_k8s.wait_timeout = 1000
    return j_k8s


def ensure_k8s_dns_resolution(juju: jubilant.Juju, app_name: str) -> None:
    """Configure VM units to resolve Kubernetes cluster.local domain names."""
    try:
        coredns_ip = (
            subprocess.check_output(
                "kubectl get svc -n kube-system coredns -o jsonpath='{.spec.clusterIP}' 2>/dev/null",
                shell=True,
            )
            .decode()
            .strip()
        )
    except Exception:
        coredns_ip = "10.152.183.63"

    if not coredns_ip:
        coredns_ip = "10.152.183.63"

    status = juju.status()
    for unit_name in status.apps[app_name].units:
        try:
            juju.ssh(
                unit_name,
                f"sudo resolvectl dns eth0 {coredns_ip} && "
                f"sudo resolvectl domain eth0 ~cluster.local && "
                f"sudo systemctl restart snap.opentelemetry-collector.opentelemetry-collector 2>/dev/null || true",
            )
        except Exception as e:
            logger.warning("Could not set DNS on %s: %s", unit_name, e)
