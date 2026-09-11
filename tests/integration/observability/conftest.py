#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fixtures for observability integration tests."""

import logging

import jubilant
import pytest

from literals import Substrate
from tests.integration.helpers import (
    APP_NAME,
    DEPLOY_TIMEOUT_S,
    IMAGE_RESOURCE,
    are_apps_active_and_agents_idle,
)
from tests.integration.observability.helpers import NUM_UNITS

logger = logging.getLogger(__name__)


@pytest.fixture
def ensure_valkey(juju: jubilant.Juju, charm: str, substrate: Substrate) -> None:
    """Ensure the Valkey application is deployed with NUM_UNITS units."""
    status = juju.status()
    if APP_NAME not in status.apps:
        logger.info("Deploying %s with %s units", APP_NAME, NUM_UNITS)
        juju.deploy(
            charm,
            resources=IMAGE_RESOURCE if substrate == Substrate.K8S else None,
            num_units=NUM_UNITS,
            trust=True,
        )
        juju.wait(
            lambda s: are_apps_active_and_agents_idle(s, APP_NAME, idle_period=30),
            timeout=DEPLOY_TIMEOUT_S,
            delay=5,
            successes=3,
        )
