#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""End-to-end Azure Blob backup/restore integration test against Azurite.

Mirrors ``test_s3_backup.py`` (any-unit backup, list ordering, object presence,
RDB magic) against azure-storage-integrator + the Azurite emulator, and adds a
restore smoke on the leader (the ``restore`` action is leader-only).

Runs on both substrates. Azurite is a host service reached at the host's
routable IP, exactly as MicroCeph serves the S3 modules, so nothing here is
K8s-specific.

Needs a bootstrapped Juju controller and a built charm:

    tox run -e integration -- tests/integration/backup/test_azure_backup.py --substrate k8s
"""

from __future__ import annotations

import time

import jubilant

from literals import Substrate
from tests.integration.backup.helpers import BACKUP_ID_RE, deploy_and_relate_azure
from tests.integration.helpers import APP_NAME, are_apps_active_and_agents_idle


def test_backup_list_and_restore(
    charm: str,
    juju: jubilant.Juju,
    substrate: Substrate,
    azurite: dict,
    azure_container,
) -> None:
    deploy_and_relate_azure(juju, charm, substrate, azurite)

    # Three distinct units exercise the any-unit backup guarantee.
    units = list(juju.status().get_units(APP_NAME))
    assert len(units) >= 3, units

    # Backup from the first unit.
    task0 = juju.run(units[0], "create-backup")
    assert task0.success, task0.stderr
    backup_id_0 = task0.results["backup-id"]
    assert BACKUP_ID_RE.match(backup_id_0), backup_id_0

    # Backup from a second unit (distinct id, exercises any-unit guarantee).
    time.sleep(2)
    task1 = juju.run(units[1], "create-backup")
    assert task1.success, task1.stderr
    backup_id_1 = task1.results["backup-id"]
    assert backup_id_1 != backup_id_0

    # List from a third unit. Newest first.
    listing = juju.run(units[2], "list-backups")
    assert listing.success
    table = listing.results["backups"]
    assert backup_id_0 in table
    assert backup_id_1 in table
    assert table.index(backup_id_1) < table.index(backup_id_0)

    # Verify the blobs exist under the configured path prefix.
    names = [b.name for b in azure_container.list_blobs(name_starts_with=azurite["path"])]
    assert any(backup_id_0 in n for n in names), names
    assert any(backup_id_1 in n for n in names), names

    # Validate RDB magic bytes for the first object.
    name = next(n for n in names if backup_id_0 in n)
    head = azure_container.download_blob(name, offset=0, length=9).readall()
    assert head.startswith(b"REDIS") or head.startswith(b"VALKEY"), head

    # Restore smoke: the restore action is leader-only. Initiating it must succeed
    # and the cluster must converge back to active/idle.
    restore = juju.run(f"{APP_NAME}/leader", "restore", {"backup-id": backup_id_0})
    assert restore.success, restore.stderr
    assert "restore" in restore.results, f"Unexpected action results: {restore.results}"
    juju.wait(
        lambda status: are_apps_active_and_agents_idle(status, APP_NAME, idle_period=30),
        timeout=1200,
        delay=5,
        successes=3,
    )
