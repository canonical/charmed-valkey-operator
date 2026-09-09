#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for backups of Valkey RDB snapshots to remote object storage."""

from __future__ import annotations

import logging
import pathlib
import re
from datetime import datetime, timezone
from functools import cached_property
from typing import IO, TYPE_CHECKING, Any, cast

from charmlibs import pathops
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope

from common.client import ValkeyClient
from common.exceptions import StorageBackendError, ValkeyBackupError, ValkeyRestoreError
from common.storage_backend import StorageBackend, build_backend
from literals import (
    BACKUP_CA_FILENAME,
    BACKUP_EXISTS_CODE,
    BACKUP_ID_FORMAT,
    PRE_RESTORE_SUFFIX,
    CharmUsers,
    RestoreStep,
)
from statuses import BackupStatuses, CharmStatuses, RestoreStatuses

if TYPE_CHECKING:
    from core.base_workload import WorkloadBase
    from core.cluster_state import ClusterState
    from core.models import BackupCredentials

logger = logging.getLogger(__name__)

# RDB streams start with "REDIS" (Redis) or "VALKEY"; anything else is not a
# valid snapshot and must not be recorded as a backup.
_RDB_MAGIC = (b"REDIS", b"VALKEY")

# Only ISO-8601 backup ids (BACKUP_ID_FORMAT) belong in list-backups; this
# skips stray uploads, lifecycle markers, and future PITR/AOF objects.
_BACKUP_ID_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

# A CA-chain entry must look like PEM (armour header) or boto3 can't load the
# bundle. Same shape as managers/tls.py.
_PEM_HEADER_RE = re.compile(r"-+BEGIN [A-Z ]+-+")


class _CountingReader:
    """Wrap a binary stream, counting bytes read and capturing the head.

    The backend's ``upload`` reads through this, letting ``create_backup``
    assert post-upload that the stream was non-empty and started with a
    valid RDB magic header.
    """

    def __init__(self, stream: IO[bytes], head_size: int = 16):
        self._stream = stream
        self._head_size = head_size
        self.head = b""
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        chunk = self._stream.read(size)
        self.bytes_read += len(chunk)
        if len(self.head) < self._head_size:
            self.head += chunk[: self._head_size - len(self.head)]
        return chunk


class BackupManager(ManagerStatusProtocol):
    """Manage S3 backup uploads for the local Valkey instance."""

    name: str = "backup"
    # Narrow the protocol's state to ClusterState so attribute access type-checks;
    # the pyright override warning is invariance strictness (other managers do the same).
    state: "ClusterState"

    def __init__(self, state: "ClusterState", workload: "WorkloadBase"):
        self.state = state  # pyright: ignore[reportIncompatibleVariableOverride]
        self.workload = workload

    @property
    def _backup_ca_path(self) -> pathlib.Path:
        """Charm-local path to the S3 endpoint CA bundle used by boto3.

        Charm-process-local, not a workload ``tls_paths`` entry: boto3 runs in
        the charm process (a separate filesystem from the workload on K8s), and
        this keeps the S3 CA from being trusted as a Valkey client CA.
        """
        return self.state.charm.charm_dir / BACKUP_CA_FILENAME

    @property
    def _dump_path(self) -> pathops.PathProtocol:
        """Live Valkey RDB path (``dump.rdb``) inside the workload's data directory."""
        return self.workload.working_dir / "dump.rdb"

    @property
    def _dump_tmp_path(self) -> pathops.PathProtocol:
        """Temp path for a fresh download; renamed atomically onto ``_dump_path``.

        On the data partition next to ``dump.rdb``, so the promote is a
        same-partition rename (not a cross-device copy).
        """
        return self.workload.working_dir / "dump.rdb.part"

    @property
    def _pre_restore_path(self) -> pathops.PathProtocol:
        """Pre-restore RDB snapshot kept for rollback, on the archive partition.

        On archive_dir so the rollback copy doesn't double the data partition.
        """
        return self.workload.archive_dir / ("dump.rdb" + PRE_RESTORE_SUFFIX)

    # ── backend selection ────────────────────────────────────────────────

    @cached_property
    def storage_backend(self) -> StorageBackend:
        """The backend serving the app's active backup credentials.

        Cached because a manager instance lives for exactly one hook: every
        operation in that hook shares one backend object. Raises
        ``StorageBackendError`` when no integrator has supplied credentials yet,
        which each caller translates into its own backup/restore error.
        """
        params = self.state.active_backup_credentials
        if params is None:
            raise StorageBackendError("Backup storage credentials unavailable")
        return build_backend(params, self._backup_ca_path)

    # ── container lifecycle ──────────────────────────────────────────────

    def ensure_container(self, params: "BackupCredentials") -> None:
        """Idempotently create the bucket/container for just-validated params.

        Builds its own backend instead of using ``storage_backend``: this runs
        before the credentials are written to the databag, so ``state`` cannot
        supply them yet.
        """
        try:
            build_backend(params, self._backup_ca_path).ensure_container()
        except StorageBackendError as e:
            raise ValkeyBackupError(str(e), safe_code=e.safe_code) from e

    # ── TLS CA chain ─────────────────────────────────────────────────────

    def store_tls_ca_chain(self, s3_parameters: dict[str, Any]) -> None:
        """Write the S3 endpoint CA chain to the charm-local path for boto3."""
        chain = s3_parameters.get("tls-ca-chain")
        if not chain:
            return
        # Require a list of PEM certs: a bare string would make "\n".join iterate
        # characters and write a corrupt bundle (mirrors the TLS manager's check).
        if not isinstance(chain, list) or not all(
            isinstance(c, str) and _PEM_HEADER_RE.search(c) for c in chain
        ):
            logger.warning("tls-ca-chain is malformed (not a list of PEM certificates); ignoring")
            return
        raw = "\n".join(chain)
        self._backup_ca_path.write_text(raw)

    def remove_tls_ca_chain(self) -> None:
        """Delete the charm-local S3 endpoint CA bundle, if present."""
        self._backup_ca_path.unlink(missing_ok=True)

    # ── list ────────────────────────────────────────────────────────────

    def list_backups(self) -> list[str]:
        """Return valid backup ids in the configured container, newest first."""
        try:
            ids = self.storage_backend.list_object_ids()
        except StorageBackendError as e:
            raise ValkeyBackupError(str(e), safe_code=e.safe_code) from e
        ids = [bid for bid in ids if _BACKUP_ID_RE.match(bid)]
        ids.sort(reverse=True)
        return ids

    @staticmethod
    def format_backup_list(ids: list[str]) -> str:
        """Render a backup list as a text table sized to the data."""
        if not ids:
            return "No backups found."
        width = max(len("backup-id"), max(len(bid) for bid in ids))
        header = f"{'backup-id':<{width}} | backup-status"
        separator = "-" * len(header)
        rows = "\n".join(f"{bid:<{width}} | finished" for bid in ids)
        return f"{header}\n{separator}\n{rows}"

    # ── create ──────────────────────────────────────────────────────────

    def create_backup(self) -> str:
        """Stream a fresh RDB from the local Valkey instance to backup storage.

        Sets a per-unit lock on the running unit's databag, streams
        ``valkey-cli --rdb -`` stdout into the backend's ``upload``,
        and cleans up the stored object on failure.
        """
        try:
            backend = self.storage_backend
        except StorageBackendError as e:
            raise ValkeyBackupError(str(e), safe_code=e.safe_code) from e
        started = datetime.now(timezone.utc)
        backup_id = started.strftime(BACKUP_ID_FORMAT)
        # A stored backup is never replaced. Azure refuses the write itself
        # (upload_blob overwrite=False), but boto3's managed uploader takes no
        # If-None-Match, so the guard both backends share has to live here.
        try:
            stored = backend.list_object_ids()
        except StorageBackendError as e:
            raise ValkeyBackupError(str(e), safe_code=e.safe_code) from e
        if backup_id in stored:
            logger.warning("backup.rejected backup_id=%s reason=already-exists", backup_id)
            raise ValkeyBackupError(
                f"An object already exists for backup-id {backup_id}",
                safe_code=BACKUP_EXISTS_CODE,
            )
        # Structured audit trail for forensics; destination logged, creds never.
        # No unit= -- Juju already prefixes every log line with the emitting unit.
        logger.info("backup.started backup_id=%s destination=%s", backup_id, backend.location)

        self.state.unit_server.update({"backup_id": backup_id})
        # Pass the admin password via VALKEYCLI_AUTH, never on argv (world-visible).
        proc = self.workload.exec_stream(
            self._build_rdb_command(),
            env={"VALKEYCLI_AUTH": self.state.unit_server.valkey_admin_password},
        )
        reader = _CountingReader(proc.stdout)

        try:
            # Don't retry the whole upload: reader can't rewind (the SDK retries parts).
            backend.upload(backup_id, cast("IO[bytes]", reader))
            rc, stderr = proc.wait()
            if rc != 0:
                raise ValkeyBackupError(f"valkey-cli --rdb exited {rc}: {stderr}")
            # valkey-cli can exit 0 with no/invalid stdout; refuse a non-RDB object.
            if reader.bytes_read == 0 or not reader.head.startswith(_RDB_MAGIC):
                raise ValkeyBackupError(
                    f"Uploaded object is not a valid RDB stream "
                    f"({reader.bytes_read} bytes); refusing to record this backup"
                )
        except ValkeyBackupError:
            # A complete-but-invalid object is stored; delete it. (A mid-stream
            # backend error is handled below, after the SDK aborts its transfer.)
            backend.delete(backup_id)
            logger.warning("backup.failed backup_id=%s", backup_id)
            raise
        except StorageBackendError as e:
            # The SDK aborts its own managed transfer; just stop the producer.
            proc.kill()
            logger.warning("backup.failed backup_id=%s", backup_id)
            raise ValkeyBackupError(str(e), safe_code=e.safe_code) from e
        except Exception:
            # Nothing should get past the backends' translation, but an SDK that
            # grows a new exception type must not leave valkey-cli blocked on a
            # pipe nobody drains. The producer dies with the backup either way.
            proc.kill()
            logger.warning("backup.failed backup_id=%s", backup_id)
            raise
        else:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            logger.info(
                "backup.completed backup_id=%s bytes=%d elapsed_seconds=%.1f",
                backup_id,
                reader.bytes_read,
                elapsed,
            )
        finally:
            self.state.unit_server.update({"backup_id": ""})

        return backup_id

    # ── restore ─────────────────────────────────────────────────────────

    def verify_backup_is_rdb(self, backup_id: str) -> None:
        """Cheaply confirm the stored object is an RDB before touching valkey.

        A ranged read of the first 16 bytes (not a full download), so a missing or
        non-RDB backup-id fails while valkey is still serving.
        """
        try:
            head = self.storage_backend.head(backup_id)
        except StorageBackendError as e:
            raise ValkeyRestoreError(str(e), safe_code=e.safe_code) from e
        if not head.startswith(_RDB_MAGIC):
            raise ValkeyRestoreError(f"Object for {backup_id} is not a valid RDB stream")
        logger.info("restore.verified backup_id=%s (RDB header ok)", backup_id)

    def download_backup(self, backup_id: str) -> None:
        """Stream the full RDB from backup storage onto the data partition.

        push_data_file copies in bounded chunks (never buffering the whole object
        in the charm container) to ``dump.rdb.part``, then an atomic same-partition
        rename onto ``dump.rdb`` so it never appears partial.
        """
        try:
            obj = self.storage_backend.download(backup_id)
        except StorageBackendError as e:
            raise ValkeyRestoreError(str(e), safe_code=e.safe_code) from e

        logger.info(
            "restore.download.started backup_id=%s bytes=%s -> %s",
            backup_id,
            obj.size,
            self._dump_tmp_path,
        )
        # Stream the backend's body -> data partition, in bounded chunks.
        self.workload.push_data_file(
            obj.body,
            self._dump_tmp_path,
            user=self.workload.user,
            group=self.workload.user,
        )
        # Atomic promote: same-partition rename, so dump.rdb only ever appears complete.
        self.workload.move_file(self._dump_tmp_path, self._dump_path)
        logger.info("restore.download.finished backup_id=%s -> %s", backup_id, self._dump_path)

    # ── restore steps ────────────────────────────────────────────────────

    @staticmethod
    def next_restore_step(step: RestoreStep) -> RestoreStep:
        """Return the step following ``step`` in the restore workflow."""
        order = [
            RestoreStep.NOT_STARTED,
            RestoreStep.RESTORE,
            RestoreStep.RESYNC,
            RestoreStep.COMPLETED,
        ]
        return order[order.index(step) + 1]

    def set_restore_step(self, step: RestoreStep) -> None:
        """Record this unit's completed restore step on its databag."""
        self.state.unit_server.update({"restore_step": step.value})
        logger.info("restore.step_done step=%s", step.value)

    def has_pre_restore_copy(self) -> bool:
        """Whether a rollback copy exists -- an interrupted primary restore.

        Only the primary creates it, and it's removed on success or rollback, so
        (copy present + valkey down) uniquely marks a mid-swap primary on redelivery.
        """
        return self.workload.path_exists(self._pre_restore_path)

    def _ensure_stopped(self) -> None:
        """Stop valkey-server only if it is running.

        K8s errors on stopping a stopped service. Gate on the single-service
        ``alive(valkey_service)``, not the all-services ``alive()`` (False when a
        sibling is down, which would skip stopping a live valkey and swap the dump
        under it).
        """
        if self.workload.alive(self.workload.valkey_service):
            # Confirm it stopped before the RDB swap.
            self.workload.stop(self.workload.valkey_service, check_alive=True)

    def restore_on_primary(self) -> None:
        """Stop valkey, move the dump aside for rollback, download the RDB, restart.

        Move-aside to the archive partition keeps peak usage ~1x. Bypasses
        restart_workload (can't bracket a file swap); the is_restore_in_progress
        defer holds off concurrent restarts. Only runs for a fresh swap (redelivery
        is handled upstream), so any copy here is stale and dropped first.
        """
        logger.info("restore.primary: stopping valkey-server for the RDB swap")
        self._ensure_stopped()
        if self.workload.path_exists(self._pre_restore_path):
            self.cleanup_restore_files()
        logger.info("restore.primary: keeping the current dump at %s", self._pre_restore_path)
        self.workload.move_file(self._dump_path, self._pre_restore_path)
        self.download_backup(self.state.cluster.restore_id)
        # Readiness is gated later by wait_until_loaded.
        logger.info("restore.primary: starting valkey-server on the restored dump")
        self.workload.start(self.workload.valkey_service, check_alive=False)

    def roll_back(self) -> None:
        """Restore the pre-restore dump and restart (stop FIRST to defeat auto-restart)."""
        logger.warning("restore.rollback: reinstating the pre-restore dump and restarting")
        self._ensure_stopped()
        if self.workload.path_exists(self._pre_restore_path):
            self.workload.move_file(self._pre_restore_path, self._dump_path)
        # Drop any partial download left on the data partition by a failed stream.
        self.workload.remove_file(self._dump_tmp_path)
        self.workload.start(self.workload.valkey_service, check_alive=False)

    def cleanup_restore_files(self) -> None:
        """Remove the pre-restore rollback copy after a successful restore."""
        logger.info("restore.cleanup: removing the pre-restore copy %s", self._pre_restore_path)
        self.workload.remove_file(self._pre_restore_path)

    # ── helpers ─────────────────────────────────────────────────────────

    def _build_rdb_command(self) -> list[str]:
        """Build the argv for ``valkey-cli --rdb -`` against the local server."""
        client = ValkeyClient(
            username=CharmUsers.VALKEY_ADMIN.value,
            password=self.state.unit_server.valkey_admin_password,
            tls=self.state.unit_server.is_tls_enabled,
            workload=self.workload,
        )
        prefix = client.build_command_prefix(json_output=False, hostname=self.state.endpoint)
        return prefix + ["--rdb", "-"]

    # ── advanced statuses ───────────────────────────────────────────────

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Contribute backup- and restore-related statuses to the StatusHandler."""
        # Copy: .root is the live list; the appends below must not mutate persisted state.
        status_list: list[StatusObject] = list(
            self.state.statuses.get(
                scope=scope,
                component=self.name,
                running_status_only=True,
            ).root
        )

        if scope == "unit":
            if self.state.unit_server.is_backup_in_progress:
                status_list.append(BackupStatuses.BACKUP_IN_PROGRESS.value)
            return status_list or [CharmStatuses.ACTIVE_IDLE.value]

        if self.state.backup_backends_conflict:
            # Nothing can pick a backend, so this beats the credentials status.
            status_list.append(BackupStatuses.BACKUP_BACKENDS_CONFLICT.value)
        elif (
            self.state.unit_server.is_started
            and self.state.backup_relations
            and not self.state.active_backup_credentials
        ):
            status_list.append(BackupStatuses.BACKUP_CREDENTIALS_MISSING.value)

        if self.state.cluster.is_restore_in_progress:
            status_list.append(RestoreStatuses.RESTORE_IN_PROGRESS.value)

        return status_list or [CharmStatuses.ACTIVE_IDLE.value]
