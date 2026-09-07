#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import PropertyMock

import pytest


@pytest.fixture(autouse=True)
def mock_write_config_file(mocker):
    mocker.patch("workload_k8s.ValkeyK8sWorkload.write_config_file")


@pytest.fixture(autouse=True)
def mock_write_file(mocker):
    mocker.patch("workload_k8s.ValkeyK8sWorkload.write_file")


@pytest.fixture(autouse=True)
def mock_bind_address(mocker):
    mocker.patch(
        "core.cluster_state.ClusterState.bind_address",
        new_callable=PropertyMock,
        return_value="127.1.1.1",
    )


@pytest.fixture(autouse=True)
def mock_k8s_client(mocker):
    mocker.patch("lightkube.core.client.GenericSyncClient")


@pytest.fixture(autouse=True)
def mock_start_topology_observer(mocker):
    mocker.patch("managers.topology.TopologyManager.start_observer")


@pytest.fixture(autouse=True)
def tenacity_wait(mocker):
    mocker.patch("tenacity.nap.time")


@pytest.fixture(autouse=True)
def k8s_environment(monkeypatch):
    """Simulate a Kubernetes container environment by default."""
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")


@pytest.fixture
def vm_environment(monkeypatch):
    """Simulate a VM environment without Kubernetes environment variables."""
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
