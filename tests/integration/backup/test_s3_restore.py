#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for S3 restore against MicroCeph.

Scenarios:
  1. rollback          – write → backup → mutate → restore → original value back on all units.
  2. disaster-recovery – write → backup → remove app → redeploy → restore → data back.
  3. corrupt-restore   – attempt restore of a corrupt S3 object → old data preserved →
                         Sentinel failover still works (suppression-leak regression guard).
  4. leader ≠ primary  – a failed restore on a non-leader primary must not wedge the cluster.
  5. single unit       – happy path on a 1-unit app: no replicas, no resync, leader == primary.

Run only with a bootstrapped Juju controller and built charm:
    tox run -e integration -- tests/integration/backup/test_s3_restore.py --substrate k8s
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import jubilant
import pytest
from tenacity import Retrying, stop_after_delay, wait_fixed

from literals import PRIMARY_NAME, SENTINEL_DOWN_AFTER_MS, CharmUsers, Substrate
from statuses import RestoreStatuses
from tests.integration.backup.helpers import (
    BACKUP_ID_RE,
    deploy_and_relate_s3,
    read_key,
    write_key,
)
from tests.integration.ha.helpers.helpers import get_unit_name_from_primary_ip
from tests.integration.helpers import (
    APP_NAME,
    are_apps_active_and_agents_idle,
    does_status_match,
    exec_valkey_cli,
    get_password,
    get_primary_ip,
)

logger = logging.getLogger(__name__)

# Sentinel's own failover-timeout (sentinel.conf) is 180 s; poll to that ceiling.
_FAILOVER_WAIT_S = 180


# ── helpers ──────────────────────────────────────────────────────────────────


def _wait_restore_active(juju: jubilant.Juju) -> None:
    """Wait for the valkey app to converge back to active/idle after a restore.

    The restore steps (RESTORE -> RESYNC -> COMPLETED) are driven by
    relation_changed / update_status hooks; active workloads + idle agents is the
    convergence signal. Generous timeout -- a restore restarts the primary and
    resyncs replicas.
    """
    juju.wait(
        lambda status: are_apps_active_and_agents_idle(status, APP_NAME, idle_period=30),
        timeout=1200,
        delay=5,
        successes=3,
    )


def _leader_unit_name(juju: jubilant.Juju) -> str:
    """Return the unit name of the current Juju leader for the valkey app."""
    for unit_name, unit in juju.status().apps[APP_NAME].units.items():
        if unit.leader:
            return unit_name
    raise ValueError(f"No leader found in app {APP_NAME}")


def _sentinel_down_after_ms(juju: jubilant.Juju) -> dict[str, int]:
    """Return each unit's sentinel `down-after-milliseconds` for the primary.

    Read straight from every sentinel (SENTINEL primary primary) so a caller can
    assert whether failover suppression is on or off -- deterministic, unlike
    killing the primary and waiting for a real failover (which on K8s races
    Pebble's auto-restart of the process).
    """
    status = juju.status()
    model_info = juju.show_model()
    password = get_password(juju, user=CharmUsers.SENTINEL_CHARM_ADMIN)
    result: dict[str, int] = {}
    for unit_name, unit in status.apps[APP_NAME].units.items():
        address = unit.address if model_info.type == "kubernetes" else unit.public_address
        out = exec_valkey_cli(
            hostname=address,
            username=CharmUsers.SENTINEL_CHARM_ADMIN.value,
            password=password,
            command="SENTINEL primary primary",
            sentinel=True,
            json=True,
        )
        result[unit_name] = int(json.loads(out.stdout)["down-after-milliseconds"])
    return result


def upload_corrupt_backup(juju: jubilant.Juju, s3_bucket, microceph: dict) -> str:  # noqa: ARG001
    """Upload a corrupt (non-RDB) object to the S3 bucket; return its backup-id.

    Its bytes aren't the RDB magic, so download_backup fails the magic-byte check
    and tears down without touching the live RDB. The id is timestamped an hour
    back to avoid colliding with real backups made during the run.
    """
    backup_id = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    key = f"{microceph['path']}/{backup_id}"
    s3_bucket.put_object(Key=key, Body=b"CORRUPT_NOT_A_REAL_RDB\x00" * 16)
    logger.info("Uploaded corrupt object at S3 key=%s backup_id=%s", key, backup_id)
    return backup_id


def get_primary_unit(juju: jubilant.Juju, substrate: Substrate) -> str:
    """Return the unit name of the current Valkey primary node."""
    primary_ip = get_primary_ip(juju, APP_NAME)
    return get_unit_name_from_primary_ip(juju, primary_ip, substrate)


# ── tests ────────────────────────────────────────────────────────────────────


@pytest.mark.abort_on_fail
def test_restore_rollback(
    charm: str,
    juju: jubilant.Juju,
    microceph: dict,
    s3_bucket,
    substrate: Substrate,
) -> None:
    """Write data -> backup -> mutate -> restore -> original value is back on all units."""
    deploy_and_relate_s3(juju, charm, substrate, microceph)

    write_key(juju, "restore_test_key", "original")

    task = juju.run(f"{APP_NAME}/leader", "create-backup")
    assert task.success, task.stderr
    backup_id = task.results["backup-id"]
    assert BACKUP_ID_RE.match(backup_id), f"Unexpected backup-id format: {backup_id!r}"

    # Overwrite the key so the restore has something visible to undo.
    write_key(juju, "restore_test_key", "mutated")

    # restore action initiates the async restore workflow (leader only).
    task = juju.run(f"{APP_NAME}/leader", "restore", {"backup-id": backup_id})
    assert task.success, task.stderr
    assert "restore" in task.results, f"Unexpected action results: {task.results}"

    # Wait for the restore workflow to complete and the cluster to return to active.
    _wait_restore_active(juju)

    # Verify every unit has the pre-backup value.
    for unit_name in juju.status().apps[APP_NAME].units:
        got = read_key(juju, unit_name, "restore_test_key")
        assert got == "original", f"Expected 'original' on {unit_name}, got {got!r}"


@pytest.mark.abort_on_fail
def test_restore_disaster_recovery(
    charm: str,
    juju: jubilant.Juju,
    microceph: dict,
    s3_bucket,
    substrate: Substrate,
) -> None:
    """Remove the app entirely, redeploy a fresh cluster, restore from S3 -- data comes back."""
    # Independently runnable: deploy the cluster + S3 wiring if a prior test did
    # not already leave them in place (deploy_and_relate_s3 is idempotent).
    deploy_and_relate_s3(juju, charm, substrate, microceph)

    write_key(juju, "dr_key", "dr-value")

    task = juju.run(f"{APP_NAME}/leader", "create-backup")
    assert task.success, task.stderr
    backup_id = task.results["backup-id"]
    assert BACKUP_ID_RE.match(backup_id), f"Unexpected backup-id format: {backup_id!r}"

    # Simulate a catastrophic loss: remove the entire application.
    juju.remove_application(APP_NAME)
    juju.wait(lambda s: APP_NAME not in s.apps, timeout=600, delay=5)

    # Redeploy a blank 3-unit cluster and reconnect it to the existing S3 bucket
    # (the backup objects are still there).
    deploy_and_relate_s3(juju, charm, substrate, microceph)

    # Restore the pre-wipe snapshot.
    task = juju.run(f"{APP_NAME}/leader", "restore", {"backup-id": backup_id})
    assert task.success, task.stderr
    assert "restore" in task.results, f"Unexpected action results: {task.results}"

    _wait_restore_active(juju)

    got = read_key(juju, _leader_unit_name(juju), "dr_key")
    assert got == "dr-value", f"Expected 'dr-value' after DR restore, got {got!r}"


@pytest.mark.abort_on_fail
def test_corrupt_restore_keeps_cluster_and_failover(
    charm: str,
    juju: jubilant.Juju,
    microceph: dict,
    s3_bucket,
    substrate: Substrate,
) -> None:
    """A failed restore leaves old data intact and failover suppression is resumed.

    Regression guard for the suppression-leak bug: if suppress_failover() is not
    matched by resume_failover() on the _fail_restore path, sentinel's
    down-after-milliseconds stays at the suppressed value and the cluster
    silently loses automatic failover.
    """
    # Independently runnable (deploy_and_relate_s3 is idempotent).
    deploy_and_relate_s3(juju, charm, substrate, microceph)

    write_key(juju, "safe_key", "safe-value")

    corrupt_id = upload_corrupt_backup(juju, s3_bucket, microceph)

    # Initiate a restore of the corrupt object.  The action itself succeeds
    # (the backup-id is present in S3 and matches _BACKUP_ID_RE), but the
    # async download step raises ValkeyRestoreError on the magic-byte check,
    # causing _fail_restore() to call resume_failover() and set RESTORE_FAILED.
    task = juju.run(f"{APP_NAME}/leader", "restore", {"backup-id": corrupt_id})
    assert task.success, task.stderr
    assert "restore" in task.results, f"Unexpected action results: {task.results}"

    # The corrupt object fails the magic-byte check in the restore step, so the
    # workflow fails and the leader records RESTORE_FAILED on the app. Wait
    # on that status -- a real convergence signal -- rather than a bare sleep,
    # and settle the agents before probing data.
    juju.wait(
        lambda s: (
            does_status_match(
                s, expected_app_statuses={APP_NAME: [RestoreStatuses.RESTORE_FAILED.value]}
            )
            and jubilant.all_agents_idle(s, APP_NAME)
        ),
        timeout=600,
        delay=5,
    )

    # Old data must still be present (restore rolled back or never committed).
    got = read_key(juju, _leader_unit_name(juju), "safe_key")
    assert got == "safe-value", f"Old data lost after corrupt restore; got {got!r}"

    # _fail_restore must have resumed failover: every sentinel's
    # down-after-milliseconds is back to normal, not the suppressed value. Checked
    # directly -- a leak leaves it at the suppressed value -- rather than killing
    # the primary and waiting for a real failover, which on K8s races Pebble's
    # auto-restart of the process and is not a reliable signal of suppression.
    down_after = _sentinel_down_after_ms(juju)
    assert all(ms == SENTINEL_DOWN_AFTER_MS for ms in down_after.values()), (
        "Failover suppression not resumed after the corrupt restore failed "
        f"(suppression-leak regression): down-after-milliseconds={down_after}, "
        f"expected {SENTINEL_DOWN_AFTER_MS} on every sentinel."
    )


def _force_primary_off_leader(juju: jubilant.Juju, substrate: Substrate) -> tuple[str, str]:
    """Arrange the valkey primary onto a NON-leader unit; return (leader, primary).

    The valkey primary (Sentinel-elected) is independent of the juju leader, but
    fresh deployments often land both on unit 0. Ask Sentinel for a coordinated
    failover -- the same command the charm itself issues in
    ``SentinelManager.failover`` -- so the destructive restore runs on a non-leader,
    the case that used to wedge restore_id.

    SIGKILLing the primary does NOT work here: the supervisor restarts it well
    inside down-after-milliseconds (5 s on K8s, 20 s on VM, vs 30 s), so Sentinel
    never sees the primary as down and never promotes. The HA failover tests get
    around that with patch_restart_delay(); this test only needs the primary moved,
    not a crash simulated, so it asks Sentinel directly.
    """
    leader = _leader_unit_name(juju)
    primary = get_primary_unit(juju, substrate)
    if primary == leader:
        exec_valkey_cli(
            hostname=get_primary_ip(juju, APP_NAME),
            username=CharmUsers.SENTINEL_CHARM_ADMIN.value,
            password=get_password(juju, user=CharmUsers.SENTINEL_CHARM_ADMIN),
            command=f"SENTINEL FAILOVER {PRIMARY_NAME} COORDINATED",
            sentinel=True,
        )
        # Poll INFO replication for the promotion rather than sleeping blind; mid
        # failover there is briefly no primary at all, which raises and retries.
        for attempt in Retrying(
            stop=stop_after_delay(_FAILOVER_WAIT_S), wait=wait_fixed(5), reraise=True
        ):
            with attempt:
                assert get_primary_unit(juju, substrate) != leader, (
                    "Sentinel has not promoted a new primary yet"
                )
        _wait_restore_active(juju)  # cluster settles with the new primary
        primary = get_primary_unit(juju, substrate)
    assert primary != leader, f"Could not move primary off leader {leader}"
    return leader, primary


@pytest.mark.abort_on_fail
def test_failed_restore_not_wedged_when_leader_not_primary(
    charm: str,
    juju: jubilant.Juju,
    microceph: dict,
    s3_bucket,
    substrate: Substrate,
) -> None:
    """A restore that fails on a non-leader primary must not wedge the cluster.

    The unit that runs the destructive restore is the valkey primary, which is
    frequently NOT the juju leader; only the leader can clear the app-level
    restore_id. Before the per-unit failure-marker fix, a failure on a non-leader
    primary left restore_id set forever -- the app stuck in RESTORE_IN_PROGRESS,
    backups blocked. Here we force primary != leader, fail a restore, and assert
    the leader tears it down (RESTORE_FAILED) and the cluster is usable again.
    """
    # Independently runnable (deploy_and_relate_s3 is idempotent).
    deploy_and_relate_s3(juju, charm, substrate, microceph)

    write_key(juju, "wedge_key", "wedge-value")

    leader, primary = _force_primary_off_leader(juju, substrate)
    logger.info("Arranged leader=%s primary=%s for the failed-restore test", leader, primary)

    corrupt_id = upload_corrupt_backup(juju, s3_bucket, microceph)

    # Initiate a restore of the corrupt object. It fails on the (non-leader)
    # primary's magic-byte check; that unit records a failure marker, and the
    # leader observes it and clears the app-level restore state.
    task = juju.run(f"{APP_NAME}/leader", "restore", {"backup-id": corrupt_id})
    assert task.success, task.stderr

    # The leader must reach RESTORE_FAILED (only the leader can write it) -- proof
    # the non-leader failure was propagated and the workflow torn down, not wedged
    # in RESTORE_IN_PROGRESS.
    juju.wait(
        lambda s: (
            does_status_match(
                s, expected_app_statuses={APP_NAME: [RestoreStatuses.RESTORE_FAILED.value]}
            )
            and jubilant.all_agents_idle(s, APP_NAME)
        ),
        timeout=600,
        delay=5,
    )

    # Old data survived the failed restore.
    got = read_key(juju, leader, "wedge_key")
    assert got == "wedge-value", f"Old data lost after failed restore; got {got!r}"

    # Not wedged: restore_id was cleared, so create-backup is no longer blocked by
    # "A restore is in progress" -- the decisive end-to-end proof the fix works.
    task = juju.run(f"{APP_NAME}/leader", "create-backup")
    assert task.success, (
        f"create-backup blocked after a failed non-leader-primary restore "
        f"(restore_id wedged?): {task.stderr}"
    )


@pytest.mark.abort_on_fail
def test_restore_single_unit(
    charm: str,
    juju: jubilant.Juju,
    microceph: dict,
    s3_bucket,
    substrate: Substrate,
) -> None:
    """Happy path on a single-unit app: write → backup → mutate → restore → original back.

    A one-unit cluster takes a different path from the 3-unit scenarios above: the
    only participant is both juju leader and valkey primary, there is no replica
    RESYNC, the barrier has a single member, and the RESTORE → RESYNC → COMPLETED
    cascade is driven purely by the leader's own app-databag relation-changed
    self-delivery (with update-status as the backstop). Also checks the unit is
    writable again afterwards -- the restart resets the rendered
    min-replicas-to-write=1, which a lone primary can never satisfy, so the
    post-restore reconcile must relax it.
    """
    # Start from a fresh 1-unit app; the earlier scenarios leave a 3-unit one.
    if APP_NAME in juju.status().apps:
        juju.remove_application(APP_NAME)
        juju.wait(lambda s: APP_NAME not in s.apps, timeout=600, delay=5)
    deploy_and_relate_s3(juju, charm, substrate, microceph, num_units=1)
    assert len(juju.status().apps[APP_NAME].units) == 1

    write_key(juju, "single_unit_key", "original")

    task = juju.run(f"{APP_NAME}/leader", "create-backup")
    assert task.success, task.stderr
    backup_id = task.results["backup-id"]
    assert BACKUP_ID_RE.match(backup_id), f"Unexpected backup-id format: {backup_id!r}"

    write_key(juju, "single_unit_key", "mutated")

    task = juju.run(f"{APP_NAME}/leader", "restore", {"backup-id": backup_id})
    assert task.success, task.stderr
    assert "restore" in task.results, f"Unexpected action results: {task.results}"

    _wait_restore_active(juju)

    unit_name = _leader_unit_name(juju)
    got = read_key(juju, unit_name, "single_unit_key")
    assert got == "original", f"Expected 'original' on {unit_name}, got {got!r}"

    # Writable after the restore restart (min-replicas-to-write relaxed for a lone primary).
    write_key(juju, "single_unit_post_restore", "ok")
    got = read_key(juju, unit_name, "single_unit_post_restore")
    assert got == "ok", f"Single unit not writable after restore; got {got!r}"
