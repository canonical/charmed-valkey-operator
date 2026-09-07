#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Shared helpers for the object-storage backup/restore integration tests."""

from __future__ import annotations

import base64
import re

import jubilant

from literals import Substrate
from tests.integration.helpers import APP_NAME, IMAGE_RESOURCE, are_apps_active_and_agents_idle

S3_INTEGRATOR_APP = "s3-integrator"
S3_CREDS_SECRET = "s3-creds"
AZURE_INTEGRATOR_APP = "azure-storage-integrator"
AZURE_CREDS_SECRET = "azure-creds"
BACKUP_ID_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def deploy_and_relate_s3(
    juju: jubilant.Juju,
    charm: str,
    substrate: Substrate,
    microceph: dict,
    num_units: int = 3,
) -> None:
    """Deploy the local valkey charm + s3-integrator, wire S3 creds/TLS, and relate them.

    Idempotent: each step is skipped when already done, so it is safe to call at
    the top of every test and again after a redeploy. The disaster-recovery
    scenario removes only the valkey app -- s3-integrator, its credentials secret,
    and its config persist -- so the whole s3-integrator setup is guarded on its
    presence and the valkey app is (re)deployed and (re)related on its own.
    ``num_units`` only applies to a fresh valkey deploy (an existing app is left
    at its current size).
    """
    status = juju.status()

    if S3_INTEGRATOR_APP not in status.apps:
        juju.deploy(S3_INTEGRATOR_APP, channel="2/edge")
        # s3-integrator 2/edge takes credentials as a Juju secret (the
        # sync-s3-credentials action is gone in v2): create + grant the secret,
        # then point the `credentials` config at its URI.
        creds = juju.add_secret(
            name=S3_CREDS_SECRET,
            content={
                "access-key": microceph["access-key"],
                "secret-key": microceph["secret-key"],
            },
        )
        juju.grant_secret(identifier=creds, app=S3_INTEGRATOR_APP)
        # s3-integrator base64-decodes tls-ca-chain so the charm can verify
        # MicroCeph's self-signed RGW endpoint over TLS; without it every S3
        # call falls back to the system trust store and fails.
        ca_chain = base64.b64encode(microceph["tls-ca-chain"][0].encode()).decode()
        juju.config(
            S3_INTEGRATOR_APP,
            {
                "credentials": creds,
                "bucket": microceph["bucket"],
                "endpoint": microceph["endpoint"],
                "region": microceph["region"],
                "path": microceph["path"],
                "s3-uri-style": "path",
                "tls-ca-chain": ca_chain,
            },
        )

    # Always deploy the local charm under test -- never a Charmhub revision.
    if APP_NAME not in status.apps:
        juju.deploy(
            charm,
            resources=IMAGE_RESOURCE if substrate == Substrate.K8S else None,
            num_units=num_units,
            trust=True,
        )

    # Require agents idle as well as workloads active: after `integrate` the
    # workloads stay active while the leader's relation hooks (ensure_container +
    # credential storage) are still running, so a workload-only wait can return
    # before S3 is actually wired up.
    juju.wait(
        lambda status: are_apps_active_and_agents_idle(
            status, APP_NAME, S3_INTEGRATOR_APP, idle_period=30
        ),
        timeout=1000,
        delay=5,
        successes=3,
    )

    # Integrate only when the relation does not yet exist (a redeploy drops it).
    try:
        juju.integrate(APP_NAME, S3_INTEGRATOR_APP)
    except jubilant.CLIError as exc:
        if "already exists" not in str(exc).lower():
            raise
    juju.wait(
        lambda status: are_apps_active_and_agents_idle(
            status, APP_NAME, S3_INTEGRATOR_APP, idle_period=30
        ),
        timeout=1000,
        delay=5,
        successes=3,
    )


def deploy_and_relate_azure(
    juju: jubilant.Juju,
    charm: str,
    substrate: Substrate,
    azurite: dict,
    num_units: int = 3,
) -> None:
    """Deploy the local valkey charm + azure-storage-integrator, wire creds, and relate them.

    Mirrors ``deploy_and_relate_s3``: idempotent, each step skipped when already
    done, so it is safe to call at the top of every test and again after a
    redeploy. The integrator setup is guarded on the integrator app's presence,
    and the valkey app is (re)deployed and (re)related on its own.

    The `azurite` fixture mints a fresh container name per run, so a model left
    over from an earlier run keeps the *old* container in the integrator's config
    (`juju remove-application azure-storage-integrator` + `juju remove-secret
    azure-creds` between runs, or backups land where the test does not look).
    """
    status = juju.status()

    if AZURE_INTEGRATOR_APP not in status.apps:
        juju.deploy(AZURE_INTEGRATOR_APP, channel="1/edge")
        # azure-storage-integrator takes the account key as a Juju secret (content
        # key `secret-key`): create + grant it, then point `credentials` at its URI.
        # The rest is plain config -- for Azurite, `connection-protocol=http` plus an
        # explicit `endpoint` keeps the integrator on the emulator, not real Azure.
        creds = juju.add_secret(
            name=AZURE_CREDS_SECRET,
            content={"secret-key": azurite["secret-key"]},
        )
        juju.grant_secret(identifier=creds, app=AZURE_INTEGRATOR_APP)
        juju.config(
            AZURE_INTEGRATOR_APP,
            {
                "credentials": creds,
                "container": azurite["container"],
                "storage-account": azurite["storage-account"],
                "connection-protocol": azurite["connection-protocol"],
                "endpoint": azurite["endpoint"],
                # Required by the charm even though the integrator defaults it to
                # "": an empty prefix would let list-backups enumerate the whole
                # container.
                "path": azurite["path"],
            },
        )

    # Always deploy the local charm under test -- never a Charmhub revision.
    if APP_NAME not in status.apps:
        juju.deploy(
            charm,
            resources=IMAGE_RESOURCE if substrate == Substrate.K8S else None,
            num_units=num_units,
            trust=True,
        )

    # Require agents idle as well as workloads active: after `integrate` the
    # workloads stay active while the leader's relation hooks (ensure_container +
    # credential storage) are still running, so a workload-only wait can return
    # before Azure is actually wired up.
    juju.wait(
        lambda status: are_apps_active_and_agents_idle(
            status, APP_NAME, AZURE_INTEGRATOR_APP, idle_period=30
        ),
        timeout=1000,
        delay=5,
        successes=3,
    )

    # Integrate only when the relation does not yet exist (a redeploy drops it).
    try:
        juju.integrate(APP_NAME, AZURE_INTEGRATOR_APP)
    except jubilant.CLIError as exc:
        if "already exists" not in str(exc).lower():
            raise
    juju.wait(
        lambda status: are_apps_active_and_agents_idle(
            status, APP_NAME, AZURE_INTEGRATOR_APP, idle_period=30
        ),
        timeout=1000,
        delay=5,
        successes=3,
    )
