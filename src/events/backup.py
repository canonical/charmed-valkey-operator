#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Juju event wiring for object-storage backups."""

from __future__ import annotations

import json
import logging
import uuid
from typing import TYPE_CHECKING

import ops
from object_storage import (
    AzureStorageRequirer,
    S3Requirer,
    StorageConnectionInfoChangedEvent,
    StorageConnectionInfoGoneEvent,
)
from pydantic import ValidationError

from common.exceptions import (
    ValkeyBackupError,
    ValkeyCannotGetPrimaryIPError,
    ValkeyClusterNotReadyError,
    ValkeyRestoreError,
    ValkeyWorkloadCommandError,
)
from core.models import AzureStorageParameters, BackupCredentials, S3Parameters
from literals import (
    AZURE_RELATION_NAME,
    BACKUP_CREDENTIAL_FIELDS,
    PEER_RELATION,
    RESTORE_LOAD_TIMEOUT_S,
    RESTORE_RESYNC_TIMEOUT_S,
    S3_RELATION_NAME,
    RestoreFailure,
    RestoreStep,
)
from statuses import BackupStatuses, RestoreStatuses

if TYPE_CHECKING:
    from charm import ValkeyCharm

logger = logging.getLogger(__name__)


def _safe_error(exc: ValkeyBackupError | ValkeyRestoreError) -> str:
    """Return an action-safe error string.

    Action results are world-readable, so surface only the structured error code
    the failing operation attached (e.g. "AccessDenied"); the rest of the message
    -- endpoint, container, request id -- goes to the unit log only.
    """
    if exc.safe_code:
        return f"Object storage request failed: {exc.safe_code}"
    return "Backup operation failed. See juju debug-log on this unit for details."


class BackupEvents(ops.Object):
    """Backup-related Juju event observers."""

    def __init__(self, charm: "ValkeyCharm"):
        super().__init__(charm, key="backup")
        self.charm = charm
        self.s3_requirer = S3Requirer(self.charm, S3_RELATION_NAME)

        self.framework.observe(
            self.s3_requirer.on.storage_connection_info_changed, self._on_s3_credentials_changed
        )
        self.framework.observe(
            self.s3_requirer.on.storage_connection_info_gone, self._on_s3_credentials_gone
        )
        self.azure_requirer = AzureStorageRequirer(self.charm, AZURE_RELATION_NAME)
        self.framework.observe(
            self.azure_requirer.on.storage_connection_info_changed,
            self._on_azure_credentials_changed,
        )
        self.framework.observe(
            self.azure_requirer.on.storage_connection_info_gone,
            self._on_azure_credentials_gone,
        )
        # Every backend's changed path, keyed by its relation: used to re-drive the
        # others when one integrator goes away and un-blocks them. Keys must cover
        # BACKUP_CREDENTIAL_FIELDS or a registered backend never converges.
        self._credentials_changed_handlers = {
            S3_RELATION_NAME: self._on_s3_credentials_changed,
            AZURE_RELATION_NAME: self._on_azure_credentials_changed,
        }
        # Recover credentials when leadership moves; each handler early-returns if
        # its requirer has no info, so firing them all on leader-elected is safe.
        for handler in self._credentials_changed_handlers.values():
            self.framework.observe(self.charm.on.leader_elected, handler)
        self.framework.observe(self.charm.on.create_backup_action, self._on_create_backup_action)
        self.framework.observe(self.charm.on.list_backups_action, self._on_list_backups_action)
        self.framework.observe(self.charm.on.restore_action, self._on_restore_action)
        # Drive the async restore workflow on peer changes + update-status; also
        # relation_departed, so a departed-participant restore fails promptly.
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_changed, self._on_restore_workflow
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_departed, self._on_restore_workflow
        )
        self.framework.observe(self.charm.on.update_status, self._on_restore_workflow)

    # ── event handlers ──────────────────────────────────────────────────

    def _store_credentials(
        self,
        info: dict,
        model_cls: type[BackupCredentials],
        field: str,
        event: StorageConnectionInfoChangedEvent
        | StorageConnectionInfoGoneEvent
        | ops.LeaderElectedEvent,
    ) -> None:
        """Validate ``info`` and store it as the app's backup credentials.

        The backend-neutral half of every credentials-changed handler: leader
        gating, conflict refusal, validation, container setup and the databag
        write. A backend's own handler fetches its payload, does whatever is
        specific to it, and delegates here.

        The peer-relation defer precedes the conflict guard so a leader still
        wiring up its peer relation retries rather than short-circuiting.
        """
        if not self.charm.unit.is_leader():
            return
        if not self.charm.state.peer_relation:
            event.defer()
            return
        if self.charm.state.backup_backends_conflict:
            logger.warning(
                "More than one backup storage integrator related; refusing to store credentials"
            )
            return

        # The model trims/validates the payload and rejects missing/empty fields.
        try:
            params = model_cls.model_validate(info)
        except ValidationError as e:
            logger.warning("Backup storage parameters invalid or incomplete: %s", e)
            return

        # leader_elected re-fires this; skip the ensure_container round trip if unchanged.
        stored = getattr(self.charm.state.cluster, field)
        if stored is not None and stored.model_dump() == params.model_dump():
            return

        # Don't swap the container/creds an in-flight backup or restore is using; any
        # CA is already stored and the current creds stay in the databag meanwhile.
        if self._backup_or_restore_in_progress():
            logger.info("Backup or restore in progress; deferring credentials rotation")
            event.defer()
            return

        try:
            self.charm.backup_manager.ensure_container(params)
        except ValkeyBackupError as e:
            logger.error("Container setup failed: %s", e)
            return

        self.charm.state.cluster.update({field: params.model_dump_json(by_alias=True)})

    def _reconcile_other_backends(
        self,
        event: StorageConnectionInfoGoneEvent,
        exclude: str,
    ) -> None:
        """Re-drive every backend's changed path except ``exclude``'s.

        Removing one integrator clears any conflict, so a still-related backend
        can now store its credentials; without this it would stay unconfigured
        until some unrelated event re-fired its handler. Each handler no-ops when
        its own relation is absent.
        """
        for relation_name, handler in self._credentials_changed_handlers.items():
            if relation_name != exclude:
                handler(event)

    def _on_s3_credentials_changed(
        self,
        event: StorageConnectionInfoChangedEvent
        | StorageConnectionInfoGoneEvent
        | ops.LeaderElectedEvent,
    ) -> None:
        """Handle initial and updated S3 integrator credentials."""
        if not (s3_info := self.s3_requirer.get_storage_connection_info()):
            return
        logger.info("S3 credentials changed; refreshing backup configuration")
        # The endpoint CA is S3-only and every unit needs it on disk, so it is
        # stored before the leader gating in _store_credentials.
        self.charm.backup_manager.store_tls_ca_chain(dict(s3_info))
        self._store_credentials(dict(s3_info), S3Parameters, "s3_credentials", event)

    def _backup_or_restore_in_progress(self) -> bool:
        """Return whether a backup (on any unit) or a restore is running.

        Cluster-wide: a backup runs on any single unit, so the leader must not
        clobber the shared backup credentials from under it.
        """
        return (
            self.charm.state.is_backup_in_progress_any
            or self.charm.state.cluster.is_restore_in_progress
        )

    def _on_s3_credentials_gone(self, event: StorageConnectionInfoGoneEvent) -> None:
        """Handle removal of the S3 credentials relation."""
        if self._backup_or_restore_in_progress():
            logger.warning("Backup or restore in progress; deferring credentials_gone")
            event.defer()
            return

        self.charm.backup_manager.remove_tls_ca_chain()

        if self.charm.unit.is_leader():
            self.charm.state.cluster.update({"s3_credentials": ""})

        self._reconcile_other_backends(event, exclude=S3_RELATION_NAME)

    def _on_azure_credentials_changed(
        self,
        event: StorageConnectionInfoChangedEvent
        | StorageConnectionInfoGoneEvent
        | ops.LeaderElectedEvent,
    ) -> None:
        """Handle initial and updated Azure Storage integrator credentials."""
        if not (az_info := self.azure_requirer.get_storage_connection_info()):
            return
        logger.info("Azure credentials changed; refreshing backup configuration")
        self._store_credentials(dict(az_info), AzureStorageParameters, "azure_credentials", event)

    def _on_azure_credentials_gone(self, event: StorageConnectionInfoGoneEvent) -> None:
        """Handle removal of the Azure Storage credentials relation."""
        if self._backup_or_restore_in_progress():
            logger.warning("Backup or restore in progress; deferring credentials_gone")
            event.defer()
            return

        # The endpoint CA is an S3-only concept (azure_storage carries no CA
        # field), so there is nothing on disk to remove here.
        if self.charm.unit.is_leader():
            self.charm.state.cluster.update({"azure_credentials": ""})

        self._reconcile_other_backends(event, exclude=AZURE_RELATION_NAME)

    def _on_create_backup_action(self, event: ops.ActionEvent) -> None:
        """Run a streaming RDB backup of this unit's Valkey instance to object storage."""
        if reason := self._blocking_reason():
            event.set_results({"error": reason})
            event.fail(reason)
            return
        # Audit log: tie this action invocation to the backup it produces.
        logger.info(
            "audit: create-backup action invoked action_id=%s",
            event.id,
        )
        event.log("Streaming backup to object storage ...")
        # Surface the running backup in juju status; is_action beats lower-priority statuses.
        self.charm.status.set_running_status(
            BackupStatuses.BACKUP_IN_PROGRESS.value,
            scope="unit",
            is_action=True,
            component_name=self.charm.backup_manager.name,
            statuses_state=self.charm.state.statuses,
        )
        try:
            backup_id = self.charm.backup_manager.create_backup()
        except ValkeyBackupError as e:
            logger.exception("Backup failed")
            event.set_results({"error": _safe_error(e)})
            event.fail("Backup failed. Check juju debug-log for details.")
            return
        finally:
            self.charm.state.statuses.delete(
                BackupStatuses.BACKUP_IN_PROGRESS.value,
                scope="unit",
                component=self.charm.backup_manager.name,
            )
        event.set_results({"backup-id": backup_id})

    def _on_list_backups_action(self, event: ops.ActionEvent) -> None:
        """List backups currently in object storage, newest first."""
        if (output_format := event.params.get("output", "table").lower()) not in {"json", "table"}:
            event.fail("Failed: invalid output format, must be either 'json' or 'table'.")
            return
        # Read-only: a backup running on this unit must not block listing.
        if reason := self._blocking_reason(check_running_operations=False):
            event.set_results({"error": reason})
            event.fail(reason)
            return
        logger.info(
            "audit: list-backups action invoked action_id=%s",
            event.id,
        )
        try:
            ids = self.charm.backup_manager.list_backups()
        except ValkeyBackupError as e:
            logger.exception("List backups failed")
            event.set_results({"error": _safe_error(e)})
            event.fail("List backups failed. Check juju debug-log for details.")
            return
        if output_format == "json":
            backups = json.dumps([{"backup-id": bid, "backup-status": "finished"} for bid in ids])
        else:
            backups = self.charm.backup_manager.format_backup_list(ids)
        event.set_results({"backups": backups})

    # ── guard ────────────────────────────────────────────────────────────

    def _backup_storage_reason(self) -> str | None:
        """Return why no usable backup storage is configured, or None if it is.

        Shared by the backup and restore guards, and generated from the backend
        registry so a new backend needs no edit here.
        """
        if self.charm.state.backup_backends_conflict:
            return "More than one backup storage integrator is related; relate exactly one."
        if not self.charm.state.backup_relations:
            return (
                "No backup storage relation. Integrate a storage integrator on one of: "
                f"{', '.join(BACKUP_CREDENTIAL_FIELDS)}."
            )
        if not self.charm.state.active_backup_credentials:
            return "Backup storage credentials unavailable. Check the integrator config."
        return None

    def _blocking_reason(self, check_running_operations: bool = True) -> str | None:
        """Return why a backup action cannot run, or None if it can.

        Shared by create-backup and list-backups; the latter passes
        ``check_running_operations=False`` since it is read-only.
        """
        if reason := self._backup_storage_reason():
            return reason
        if not self.charm.workload.alive():
            return "Valkey is not running on this unit."
        if check_running_operations and self.charm.state.unit_server.is_backup_in_progress:
            return "A backup is already in progress on this unit."
        if check_running_operations and self.charm.state.cluster.is_restore_in_progress:
            return "A restore is in progress; backups are paused."
        return None

    def _restore_blocking_reason(self, backup_id: str) -> str | None:
        """Return why a restore of ``backup_id`` cannot start, or None if it can."""
        if not self.charm.unit.is_leader():
            return "Restore must be run on the leader unit."
        if reason := self._backup_storage_reason():
            return reason
        if self.charm.state.is_backup_in_progress_any:
            return "A backup is in progress; cannot restore."
        if self.charm.state.cluster.is_restore_in_progress:
            return "A restore is already in progress."
        # A restore restarts the primary; don't start one while TLS is changing.
        if self.charm.state.is_tls_transitioning:
            return "A TLS transition is in progress; wait for it to settle."
        # Require a stable cluster: all active, a resolvable primary, no failover in flight.
        if not all(s.is_active for s in self.charm.state.servers):
            return "Not all units are active; wait for the cluster to settle."
        if reason := self._unstable_primary_reason():
            return reason
        # Last, since it is the only gate that costs an object-storage round-trip.
        return self._unusable_backup_reason(backup_id)

    def _unusable_backup_reason(self, backup_id: str) -> str | None:
        """Return why ``backup_id`` can't be restored from, or None if it can."""
        if not backup_id:
            return "Must provide backup-id to restore."
        try:
            if backup_id not in self.charm.backup_manager.list_backups():
                return f"backup-id {backup_id} not found."
        except ValkeyBackupError as e:
            logger.exception("restore.list_backups_failed backup_id=%s", backup_id)
            return f"Could not list backups: {_safe_error(e)}"
        return None

    def _unstable_primary_reason(self) -> str | None:
        """Return why the Sentinel-managed primary isn't restorable, or None if it is."""
        try:
            self.charm.sentinel_manager.get_primary_ip()
            if self.charm.sentinel_manager.is_failover_in_progress():
                return "A Sentinel failover is in progress; cannot restore."
        except ValkeyCannotGetPrimaryIPError:
            return "No primary available; cannot restore."
        except ValkeyWorkloadCommandError:
            # A sentinel query failed outright; treat as an unsettled cluster.
            return "Could not query Sentinel; wait for the cluster to settle."
        return None

    def _on_restore_action(self, event: ops.ActionEvent) -> None:
        """Validate, then initiate the async restore workflow (leader only)."""
        backup_id = event.params.get("backup-id", "")
        if reason := self._restore_blocking_reason(backup_id):
            logger.warning(
                "restore.rejected backup_id=%s reason=%s", backup_id or "<none>", reason
            )
            event.set_results({"error": reason})
            event.fail(reason)
            return

        logger.info(
            "audit: restore action invoked action_id=%s backup_id=%s participants=%s",
            event.id,
            backup_id,
            [s.unit_name for s in self.charm.state.servers],
        )
        # Clear any stale terminal status from a prior attempt.
        self._clear_terminal_restore_statuses()
        participants = ",".join(sorted(s.unit_name for s in self.charm.state.servers))
        # Per-attempt token: restore_id is the backup-id (repeats on a re-run), so
        # failure markers are keyed to this fresh token instead.
        self.charm.state.cluster.update(
            {
                "restore_id": backup_id,
                "restore_token": uuid.uuid4().hex,
                "restore_instruction": RestoreStep.RESTORE.value,
                "restore_participants": participants,
            }
        )
        # the action is async, the actual restore procedure is triggered by relation-changed events
        event.set_results({"restore": f"initiated for {backup_id}"})

    # ── restore workflow ─────────────────────────────────────────────────

    def _on_restore_workflow(
        self, _: ops.RelationChangedEvent | ops.RelationDepartedEvent | ops.UpdateStatusEvent
    ) -> None:
        """Advance the restore state machine one step for this unit.

        Runs this unit's matching step and (leader) advances the shared
        instruction once every participant catches up. Each app-databag write
        re-delivers peer relation_changed (update_status backstops), so the
        machine cascades one step per hook. Single-unit works the same way: the
        leader receives relation_changed for its own peer *app*-databag writes --
        Juju's one self-delivery guarantee. No in-hook loop; every guard below is
        idempotent, so a redelivered hook re-runs and converges.
        """
        # Restore done: each unit clears its own per-unit state once restore_id is gone,
        # and self-heals a sentinel a failed restore left failover-suppressed --
        # suppression is only legitimate while a restore runs.
        if not self.charm.state.cluster.is_restore_in_progress:
            self._clear_local_restore_state()
            if self.charm.state.unit_server.is_active:
                self.charm.sentinel_manager.reconcile_failover_suppression()
            return

        # Leader ends a failed restore on a participant failure or departure
        # (only it can clear the app-level restore_id).
        if self._leader_must_fail_restore():
            self._finish_failed_restore()
            return
        # A unit that joined after initiation isn't a participant and must run no
        # step (it would query its own down Valkey and spuriously fail the restore).
        if self.charm.unit.name not in self.charm.state.cluster.restore_participants:
            # ...but a non-participant leader must still advance the barrier
            # (leadership can drift to a late-joiner), or nobody does -> wedge.
            if self.charm.unit.is_leader():
                self._advance_if_leader()
            return
        # This unit already failed this attempt (token-scoped, so a stale marker
        # won't block a new one): wait for the leader to end the restore.
        if self.charm.state.unit_server.restore_failure_kind(
            self.charm.state.cluster.restore_token
        ):
            return

        instruction = self.charm.state.cluster.restore_instruction
        step = self.charm.state.unit_server.restore_step
        role = self.charm.state.unit_server.restore_role  # "" until RESTORE records it

        try:
            self._run_restore_step(instruction, step, role)
        except Exception as e:
            # Catch everything: the failure marker must be recorded and failover
            # resumed on ANY error (service-control errors sit outside the
            # restore-error hierarchy), or the restore wedges.
            logger.exception(
                "restore.step_failed instruction=%s step=%s role=%s", instruction, step, role
            )
            self._fail_restore(e)
            return

        if self.charm.unit.is_leader():
            self._advance_if_leader()

    def _clear_local_restore_state(self) -> None:
        """Clear this unit's per-unit restore fields once a restore has ended."""
        unit = self.charm.state.unit_server
        if unit.restore_step != RestoreStep.NOT_STARTED or unit.restore_failed:
            unit.update({"restore_step": "", "restore_role": "", "restore_failed": ""})

    def _leader_must_fail_restore(self) -> bool:
        """Whether the leader must end the restore as failed now.

        True on a participant failure this attempt, or a departed participant (which
        can never satisfy the barrier, so the restore would otherwise wedge).
        """
        if not self.charm.unit.is_leader():
            return False
        if kind := self.charm.state.failed_restore_kind:
            logger.warning("restore.participant_failed kind=%s; ending the restore", kind)
            return True
        if self.charm.state.restore_participant_departed:
            logger.warning("restore.participant_departed; ending the restore")
            return True
        return False

    def _run_restore_step(self, instruction: RestoreStep, step: RestoreStep, role: str) -> None:
        """Run exactly the step whose (instruction, prior-step) tuple matches. Else no-op."""
        match (instruction, step):
            case (RestoreStep.RESTORE, RestoreStep.NOT_STARTED):
                # Fused download+restore: primary suppresses failover, downloads
                # and swaps the RDB in one sweep; replicas just record the step.
                #
                # Redelivered mid-swap (valkey down + a rollback copy present): don't
                # continue the interrupted download -- roll back to the original data
                # and fail, so the operator re-runs from a known-good baseline.
                if self.charm.backup_manager.has_pre_restore_copy() and not (
                    self.charm.workload.alive(self.charm.workload.valkey_service)
                ):
                    self.charm.backup_manager.roll_back()
                    raise ValkeyRestoreError("primary restore interrupted mid-swap; rolled back")

                is_primary = self.charm.cluster_manager.is_primary()
                role = "primary" if is_primary else "replica"
                self.charm.state.unit_server.update({"restore_role": role})
                logger.info(
                    "restore.step restore role=%s backup_id=%s",
                    role,
                    self.charm.state.cluster.restore_id,
                )
                if is_primary:
                    self.charm.sentinel_manager.suppress_failover()
                    self._do_primary_restore()
                self.charm.backup_manager.set_restore_step(RestoreStep.RESTORE)

            case (RestoreStep.RESYNC, RestoreStep.RESTORE):
                if role == "primary":
                    self.charm.sentinel_manager.resume_failover()
                else:
                    self.charm.cluster_manager.wait_until_resynced(RESTORE_RESYNC_TIMEOUT_S)
                self.charm.backup_manager.set_restore_step(RestoreStep.RESYNC)

            case (RestoreStep.COMPLETED, RestoreStep.RESYNC):
                if role == "primary":
                    self.charm.backup_manager.cleanup_restore_files()
                self.charm.backup_manager.set_restore_step(RestoreStep.COMPLETED)

            case _:
                # Not our turn: tuple doesn't match a valid transition.
                return

    def _do_primary_restore(self) -> None:
        """Validate, restore in-place, and roll back on any failure.

        restore_on_primary does stop -> move dump aside -> download -> restart;
        the caller then confirms it loaded. Any failure rolls back to the
        pre-restore copy before propagating to _fail_restore.
        """
        # Pre-stop (outside the try, nothing to roll back yet): reject a non-RDB
        # object before bouncing the primary, and persist memory to disk so the
        # rollback copy restore_on_primary moves aside is current, not stale.
        self.charm.backup_manager.verify_backup_is_rdb(self.charm.state.cluster.restore_id)
        self.charm.cluster_manager.save_dataset_before_shutdown()
        try:
            self.charm.backup_manager.restore_on_primary()
            self.charm.cluster_manager.wait_until_loaded(RESTORE_LOAD_TIMEOUT_S)
        except Exception:
            self.charm.backup_manager.roll_back()
            raise
        finally:
            # valkey.conf ships min-replicas-to-write 1 and the topology-aware
            # value (0 below 3 active units) is a non-persistent CONFIG SET, so any
            # restart here (restore or rollback) comes back at 1 -- reassert it or a
            # 1-2 unit cluster is write-frozen. Best-effort: a raise in a finally
            # must not mask the failure or false-fail a success.
            try:
                self.charm.cluster_manager.reconcile_min_replicas_to_write()
            except Exception:
                logger.exception("min-replicas reconcile after restore failed")

    def _advance_if_leader(self) -> None:
        """Advance the instruction once every participant has reached it; clear on COMPLETED."""
        if not self.charm.state.can_restore_workflow_proceed:
            return
        instruction = self.charm.state.cluster.restore_instruction
        if instruction == RestoreStep.COMPLETED:
            logger.info("restore.completed backup_id=%s", self.charm.state.cluster.restore_id)
            self._clear_terminal_restore_statuses()
            self._clear_restore_state()
            return
        next_step = self.charm.backup_manager.next_restore_step(instruction)
        logger.info(
            "restore.advance %s -> %s (all participants caught up)", instruction, next_step
        )
        self.charm.state.cluster.update({"restore_instruction": next_step.value})

    def _fail_restore(self, exc: Exception | None = None) -> None:
        """Mark this unit's restore attempt failed; the leader then ends the restore.

        Resumes sentinel failover (best-effort: a raise must not stop the marker
        being recorded, or the restore re-wedges) and records a failure marker on
        this unit's own databag. The failing unit is often not the juju leader,
        and only the leader can clear the app-level restore_id, so the leader
        acts on the marker via _finish_failed_restore (inline if this unit is it).
        """
        try:
            self.charm.sentinel_manager.resume_failover()
        except Exception:
            logger.exception("resume_failover after a failed restore step raised")
        kind = (
            RestoreFailure.UNHEALTHY
            if isinstance(exc, ValkeyClusterNotReadyError)
            else RestoreFailure.FAILED
        )
        # Stamp with the attempt token so it can't be misread against a later restore.
        marker = f"{kind.value}:{self.charm.state.cluster.restore_token}"
        self.charm.state.unit_server.update({"restore_failed": marker})
        logger.warning("restore.failed kind=%s", kind.value)
        if self.charm.unit.is_leader():
            # Already resumed failover just above; don't do it twice.
            self._finish_failed_restore(resume=False)

    def _finish_failed_restore(self, resume: bool = True) -> None:
        """Leader-only: end a failed restore and surface its terminal status.

        Clears the app-level restore state (un-wedging the cluster) and adds
        RESTORE_FAILED / RESTORE_UNHEALTHY. ``resume`` resumes failover as a
        backstop, since a failing peer's own best-effort resume may have raised.
        The leader-self path already resumed, so it passes resume=False to avoid
        a redundant SENTINEL RESET.
        """
        # Read the failure kind while the token still matches the marker.
        status = (
            RestoreStatuses.RESTORE_UNHEALTHY
            if self.charm.state.failed_restore_kind == RestoreFailure.UNHEALTHY.value
            else RestoreStatuses.RESTORE_FAILED
        )
        logger.warning(
            "restore.ended backup_id=%s status=%s",
            self.charm.state.cluster.restore_id,
            status.name,
        )
        # Clear restore state before the best-effort resume and status write.
        # This ordering isn't itself the wedge guard: if resume_failover raises
        # outside the narrow catch the hook errors and Juju rolls back this write
        # too, so the leader just retries until resume succeeds. Clearing first
        # only keeps the intent (unwedge is the priority) explicit.
        self._clear_restore_state()
        if resume:
            try:
                self.charm.sentinel_manager.resume_failover()
            except ValkeyWorkloadCommandError:
                logger.exception("resume_failover while ending the failed restore raised")
        self.charm.state.statuses.add(
            status.value,
            scope="app",
            component=self.charm.backup_manager.name,
        )

    def _clear_terminal_restore_statuses(self) -> None:
        """Delete both terminal restore statuses (delete tolerates an absent one)."""
        for status in (RestoreStatuses.RESTORE_FAILED, RestoreStatuses.RESTORE_UNHEALTHY):
            self.charm.state.statuses.delete(
                status.value,
                scope="app",
                component=self.charm.backup_manager.name,
            )

    def _clear_restore_state(self) -> None:
        """Clear the app-level restore coordination fields, ending the workflow."""
        self.charm.state.cluster.update(
            {
                "restore_id": "",
                "restore_token": "",
                "restore_instruction": "",
                "restore_participants": "",
            }
        )
