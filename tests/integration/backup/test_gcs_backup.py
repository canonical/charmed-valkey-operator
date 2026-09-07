#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""End-to-end GCS backup/restore integration test against real Google Cloud Storage.

Mirrors ``test_azure_backup.py`` (any-unit backup, list ordering, object
presence, RDB magic, leader restore) against gcs-integrator, and checks the
restored data: a key written before the backup and overwritten after it must
come back with its pre-backup value on every unit. There is no
emulator: gcs-integrator publishes no endpoint, so the charm can only reach
storage.googleapis.com. The key comes from ``GCS_SERVICE_ACCOUNT`` and the
bucket from ``GCS_BUCKET`` (see the ``gcs`` fixture in ``conftest.py``); a
missing or empty key fails the module rather than skipping it.

Runs on both substrates. Needs a bootstrapped Juju controller and a built charm:

    export GCS_SERVICE_ACCOUNT="$(cat service_account.json)"
    tox run -e integration -- tests/integration/backup/test_gcs_backup.py --substrate k8s
"""

from __future__ import annotations

import time

import jubilant

from literals import Substrate
from tests.integration.backup.helpers import (
    BACKUP_ID_RE,
    deploy_and_relate_gcs,
    read_key,
    write_key,
)
from tests.integration.helpers import APP_NAME, are_apps_active_and_agents_idle


def test_backup_list_and_restore(
    charm: str,
    juju: jubilant.Juju,
    substrate: Substrate,
    gcs: dict,
    gcs_bucket,
) -> None:
    deploy_and_relate_gcs(juju, charm, substrate, gcs)

    # Three distinct units exercise the any-unit backup guarantee.
    units = list(juju.status().get_units(APP_NAME))
    assert len(units) >= 3, units

    # Data that only the restore can bring back: written before backup 0, then
    # overwritten before the restore.
    write_key(juju, "gcs_restore_key", "original")

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

    # Verify the objects exist under the configured path prefix.
    prefix = f"{gcs['path']}/"
    names = [b.name for b in gcs_bucket.client.list_blobs(gcs_bucket, prefix=prefix)]
    assert any(backup_id_0 in n for n in names), names
    assert any(backup_id_1 in n for n in names), names

    # Validate RDB magic bytes for the first object (ranged read, no checksum).
    name = next(n for n in names if backup_id_0 in n)
    head = gcs_bucket.blob(name).download_as_bytes(start=0, end=8)
    assert head.startswith(b"REDIS") or head.startswith(b"VALKEY"), head

    # Overwrite the key so the restore has something visible to undo.
    write_key(juju, "gcs_restore_key", "mutated")

    # Restore: the action is leader-only. Initiating it must succeed and the
    # cluster must converge back to active/idle.
    restore = juju.run(f"{APP_NAME}/leader", "restore", {"backup-id": backup_id_0})
    assert restore.success, restore.stderr
    assert "restore" in restore.results, f"Unexpected action results: {restore.results}"
    juju.wait(
        lambda status: are_apps_active_and_agents_idle(status, APP_NAME, idle_period=30),
        timeout=1200,
        delay=5,
        successes=3,
    )

    # The bytes that came back from GCS are the pre-backup snapshot, on every unit.
    for unit_name in juju.status().apps[APP_NAME].units:
        got = read_key(juju, unit_name, "gcs_restore_key")
        assert got == "original", f"Expected 'original' on {unit_name}, got {got!r}"
