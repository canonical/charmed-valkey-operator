#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

"""Valkey base event handlers."""

import logging
from typing import TYPE_CHECKING

import ops

from common.custom_events import UnitFullyStartedEvent
from common.exceptions import (
    CannotSeeAllActiveSentinelsError,
    NotAllDepartingSentinelsStoppedError,
    RequestingLockTimedOutError,
    SentinelIncorrectReplicaCountError,
    ValkeyACLLoadError,
    ValkeyBackupInProgressError,
    ValkeyCannotGetPrimaryIPError,
    ValkeyConfigurationError,
    ValkeyServiceNotAliveError,
    ValkeyServicesCouldNotBeStoppedError,
    ValkeyServicesFailedToStartError,
    ValkeyWorkloadCommandError,
)
from common.locks import RestartLock, ScaleDownLock, StartLock
from literals import (
    ARCHIVE_STORAGE,
    CLIENT_PORT,
    DATA_STORAGE,
    INTERNAL_USERS_PASSWORD_CONFIG,
    INTERNAL_USERS_SECRET_LABEL_SUFFIX,
    LOG_STORAGE,
    PEER_RELATION,
    SENTINEL_PORT,
    SENTINEL_TLS_PORT,
    TLS_PORT,
    CharmUsers,
    ScaleDownState,
    StartState,
    Substrate,
    TLSState,
)
from statuses import CharmStatuses, ClusterStatuses, ScaleDownStatuses

if TYPE_CHECKING:
    from charm import ValkeyCharm

logger = logging.getLogger(__name__)


class BaseEvents(ops.Object):
    """Handle all base events."""

    unit_fully_started = ops.EventSource(UnitFullyStartedEvent)

    def __init__(self, charm: "ValkeyCharm"):
        super().__init__(charm, key="base_events")
        self.charm = charm

        for storage_name in (DATA_STORAGE, LOG_STORAGE, ARCHIVE_STORAGE):
            self.framework.observe(
                self.charm.on[storage_name].storage_attached, self._on_storage_attached
            )
        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.start, self._on_start)
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_changed, self._on_peer_relation_changed
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_departed, self._on_peer_relation_departed
        )
        self.framework.observe(self.charm.on.update_status, self._on_update_status)
        self.framework.observe(self.charm.on.leader_elected, self._on_leader_elected)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)
        self.framework.observe(self.unit_fully_started, self._on_unit_fully_started)
        for storage_name in (DATA_STORAGE, LOG_STORAGE, ARCHIVE_STORAGE):
            self.framework.observe(
                self.charm.on[storage_name].storage_detaching, self._on_storage_detaching
            )

    def _on_storage_attached(self, event: ops.StorageAttachedEvent) -> None:
        """Set ownership/permissions on the attached storage dir."""
        storage_dirs = {
            DATA_STORAGE: self.charm.workload.working_dir,
            LOG_STORAGE: self.charm.workload.log_dir,
            ARCHIVE_STORAGE: self.charm.workload.archive_dir,
        }
        if (target := storage_dirs.get(event.storage.name)) is None:
            logger.warning("Unexpected storage %s attached; skipping", event.storage.name)
            return

        if event.storage.name == DATA_STORAGE:
            self.charm.cluster_manager.clean_up_inconsistent_dump_files()

        if self.charm.state.substrate == Substrate.K8S:
            return

        try:
            self.charm.workload.exec(["chmod", "-R", "750", target.as_posix()])
        except ValkeyWorkloadCommandError as e:
            logger.error("Error when setting storage permissions: %s", e)
            event.defer()
            return

    def _on_install(self, event: ops.InstallEvent) -> None:
        """Handle install event."""
        if self.charm.substrate == Substrate.K8S:
            logger.debug("No installation required.")
            return

        try:
            self.charm.workload.install()  # pyright: ignore[reportAttributeAccessIssue]
        except RuntimeError:
            raise RuntimeError("Failed to install the Valkey snap")

    def _on_start(self, event: ops.StartEvent) -> None:
        """Handle the on start event."""
        self.charm.state.unit_server.update(
            {
                "start_state": StartState.NOT_STARTED.value,
                "hostname": self.charm.state.hostname,
                "private_ip": self.charm.state.bind_address,
            }
        )
        start_lock = StartLock(self.charm.state)

        if not self.charm.workload.can_connect:
            logger.warning("Workload not ready yet")
            event.defer()
            return

        if not self.charm.state.cluster.internal_users_credentials:
            logger.info(
                "Internal users' credentials not set yet. Deferring start event until credentials are set."
            )
            event.defer()
            return

        if (
            self.charm.state.client_tls_relation
            and not self.charm.state.unit_server.model.client_cert_ready
        ):
            logger.warning("Waiting for client TLS certificates before starting")
            event.defer()
            return

        try:
            primary_endpoint = self.charm.sentinel_manager.get_primary_ip()
        except ValkeyCannotGetPrimaryIPError:
            if self.charm.state.number_units_started == 0 and self.charm.unit.is_leader():
                primary_endpoint = self.charm.state.unit_server.get_endpoint(
                    self.charm.state.substrate
                )
            else:
                logger.debug(
                    "Primary IP not available yet or other units have already started, deferring start event until leader starts the primary"
                )
                self.charm.state.unit_server.update(
                    {"start_state": StartState.WAITING_FOR_PRIMARY_START.value}
                )
                event.defer()
                return

        self.charm.state.unit_server.update({"start_state": StartState.WAITING_TO_START.value})
        start_lock.request_lock()

        if not start_lock.is_held_by_this_unit:
            logger.info("Waiting for lock to start")
            event.defer()
            return

        try:
            self.charm.auth_manager.configure_auth()
            self.charm.config_manager.configure_services(primary_endpoint)
            self.charm.workload.start()
        except ValkeyConfigurationError:
            self.charm.state.unit_server.update(
                {"start_state": StartState.CONFIGURATION_ERROR.value}
            )
            start_lock.release_lock()
            event.defer()
            return
        except (ValkeyServicesFailedToStartError, ValkeyServiceNotAliveError) as e:
            logger.error(e)
            self.charm.state.unit_server.update({"start_state": StartState.ERROR_ON_START.value})
            start_lock.release_lock()
            event.defer()
            return

        self.charm.state.unit_server.update(
            {"start_state": StartState.STARTING_WAITING_VALKEY.value}
        )
        self.unit_fully_started.emit(
            is_primary=primary_endpoint
            == self.charm.state.unit_server.get_endpoint(self.charm.state.substrate)
        )

    # TODO check how to trigger if deferred without update status event
    def _on_unit_fully_started(self, event: UnitFullyStartedEvent) -> None:
        """Handle the unit-fully-started event."""
        if not self.charm.cluster_manager.is_healthy(
            is_primary=event.is_primary, check_replica_sync=False
        ):
            logger.warning("Unit is not healthy after start, deferring event.")
            self.charm.state.unit_server.update(
                {"start_state": StartState.STARTING_WAITING_VALKEY.value}
            )
            event.defer()
            return

        if not self.charm.sentinel_manager.is_healthy():
            logger.warning("Sentinel is not healthy after start, deferring event.")
            self.charm.state.unit_server.update(
                {"start_state": StartState.STARTING_WAITING_SENTINEL.value}
            )
            event.defer()
            return

        if not event.is_primary and not self.charm.sentinel_manager.is_sentinel_discovered():
            logger.info("Sentinel service not yet discovered by other units. Deferring event.")
            self.charm.state.unit_server.update(
                {"start_state": StartState.STARTING_WAITING_SENTINEL.value}
            )
            event.defer()
            return

        if not event.is_primary and not self.charm.cluster_manager.is_replica_synced():
            logger.info("Replica not yet synced. Deferring event.")
            self.charm.state.unit_server.update(
                {"start_state": StartState.STARTING_WAITING_REPLICA_SYNC.value}
            )
            event.defer()
            return

        logger.info("Services started")
        self.charm.state.unit_server.update({"start_state": StartState.STARTED.value})
        StartLock(self.charm.state).release_lock()

        # the rendered config ships min-replicas-to-write=1; reassert the
        # topology-correct runtime value now the server is up, as CONFIG SET
        # does not persist across a restart
        self.charm.cluster_manager.reconcile_min_replicas_to_write()

        if self.charm.state.unit_server.tls_client_state != TLSState.TLS:
            self.charm.unit.open_port("tcp", CLIENT_PORT)
            self.charm.unit.open_port("tcp", SENTINEL_PORT)
        self.charm.unit.open_port("tcp", TLS_PORT)
        self.charm.unit.open_port("tcp", SENTINEL_TLS_PORT)

        if not self.charm.unit.is_leader():
            return

        try:
            self.charm.topology_manager.start_observer()
        except (ValkeyWorkloadCommandError, ValueError) as e:
            logger.error("Failed to start topology observer: %s", e)

    def _on_peer_relation_changed(self, event: ops.RelationChangedEvent) -> None:
        """Handle event received by all units when a unit's relation data changes."""
        if self.charm.state.cluster.is_restore_in_progress:
            return

        try:
            self._reconfigure_quorum_if_necessary()
        except ValkeyWorkloadCommandError as e:
            logger.error(f"Failed to update sentinel quorum: {e}")
            # not critical to defer here, we can wait for the next relation change

        # reassert min-replicas-to-write to match the (possibly changed) topology
        if self.charm.state.unit_server.is_started:
            self.charm.cluster_manager.reconcile_min_replicas_to_write()

        if not self.charm.unit.is_leader():
            return

        for lock in [StartLock(self.charm.state), RestartLock(self.charm.state)]:
            lock.process()

        if not self.charm.state.unit_server.is_active:
            return

        try:
            self.charm.sentinel_manager.remove_departed_sentinels_from_cluster()
        except (
            CannotSeeAllActiveSentinelsError,
            NotAllDepartingSentinelsStoppedError,
            SentinelIncorrectReplicaCountError,
            ValkeyCannotGetPrimaryIPError,
            ValkeyWorkloadCommandError,
        ) as e:
            logger.error(e)
            event.defer()
            return

        # return early during TLS switchover to avoid unnecessary operation during rolling restart for sentinel
        if self.charm.state.unit_server.model.tls_client_state in (
            TLSState.TO_TLS,
            TLSState.TO_NO_TLS,
        ):
            return

        # need to pick up scaling operations, TLS switchover, CA rotation and so on
        try:
            self.charm.topology_manager.restart_observer()
        except (ValkeyWorkloadCommandError, ValueError) as e:
            logger.error("Failed to restart topology observer: %s", e)

    def _on_peer_relation_departed(self, _: ops.RelationDepartedEvent) -> None:
        """Handle event received by all units when a unit departs."""
        # Shouldn't happen mid-restore (storage-detaching refuses scale-down); if
        # it does, return rather than defer (deferring a departed event is unsafe).
        if self.charm.state.cluster.is_restore_in_progress:
            return

        try:
            self._reconfigure_quorum_if_necessary()
        except ValkeyWorkloadCommandError as e:
            logger.error(f"Failed to update sentinel quorum: {e}")
            # not critical to defer here, we can wait for the next relation change

        # a unit departed (scale down): relax min-replicas-to-write if we dropped below 3
        if self.charm.state.unit_server.is_started:
            self.charm.cluster_manager.reconcile_min_replicas_to_write()

        if not self.charm.unit.is_leader():
            return

        # trigger Sentinel reset and health check, executed on relation-changed
        self.charm.state.cluster.update({"sentinel_reset_required": True})

        if not self.charm.state.unit_server.is_active:
            return

        try:
            self.charm.topology_manager.restart_observer()
        except (ValkeyWorkloadCommandError, ValueError) as e:
            logger.error("Failed to restart topology observer: %s", e)

    def _on_update_status(self, event: ops.UpdateStatusEvent) -> None:
        """Handle the update-status event."""
        if not self.charm.state.unit_server.is_started:
            logger.warning("Service not started")
            return

        # runs before the leader check (any unit's address can change) and is not
        # deferred: update-status repeats on its own
        if not self._reconcile_unit_address():
            return

        if not self.charm.unit.is_leader():
            return

        try:
            self.charm.topology_manager.start_observer()
        except (ValkeyWorkloadCommandError, ValueError) as e:
            logger.error("Failed to start topology observer: %s", e)

    def _on_leader_elected(self, event: ops.LeaderElectedEvent) -> None:
        """Handle the leader-elected event."""
        if not (self.charm.state.peer_relation and self.charm.workload.can_connect):
            logger.info("Workload not ready")
            event.defer()
            return

        self.charm.state.unit_server.update(
            {
                "hostname": self.charm.state.hostname,
                "private_ip": self.charm.state.bind_address,
            }
        )

        if not self.charm.unit.is_leader():
            return

        if self.charm.state.unit_server.is_active:
            try:
                self.charm.sentinel_manager.remove_departed_sentinels_from_cluster()
            except (
                CannotSeeAllActiveSentinelsError,
                NotAllDepartingSentinelsStoppedError,
                SentinelIncorrectReplicaCountError,
                ValkeyCannotGetPrimaryIPError,
                ValkeyWorkloadCommandError,
            ) as e:
                logger.error(e)
                event.defer()
                return

            try:
                self.charm.topology_manager.start_observer()
            except (ValkeyWorkloadCommandError, ValueError) as e:
                logger.error("Failed to start topology observer: %s", e)

        if self.charm.state.cluster.internal_users_credentials:
            logger.debug("Internal user credentials already set")
            return

        passwords = {}
        user_specified_passwords = {}
        if admin_secret_id := self.charm.config.get(INTERNAL_USERS_PASSWORD_CONFIG):
            try:
                user_specified_passwords = self.charm.state.get_secret_from_id(
                    str(admin_secret_id)
                )
            except (ops.ModelError, ops.SecretNotFoundError) as e:
                logger.error("Could not access secret %s: %s", admin_secret_id, e)
                raise

        # generate passwords for all internal users if not specified in the user secret
        for user in CharmUsers:
            passwords[user.value] = user_specified_passwords.get(
                user.value, self.charm.auth_manager.generate_password()
            )

        self.charm.state.cluster.update(
            {
                f"{user.value.replace('-', '_')}_password": passwords[user.value]
                for user in CharmUsers
            }
        )
        # update local unit admin password
        self.charm.auth_manager.update_local_valkey_admin_password()

    def _reconcile_unit_address(self) -> bool:
        """Reconfigure the unit and reissue its certificate after its address changed.

        When a machine changes address, Juju's binding may still report the old one while
        `config-changed` runs, with no later `config-changed` to correct it — so
        `update-status` must also converge the unit. The caller owns the retry strategy.

        Returns:
            bool: True if the address is reconciled (or there was nothing to do), False if
                the reconcile could not complete and must be retried.
        """
        # on k8s we use hostnames, so the address is irrelevant and the binding unread
        if self.charm.state.substrate != Substrate.VM:
            return True

        recorded_ip = self.charm.state.unit_server.model.private_ip
        if not recorded_ip:
            # the address is first recorded on start; there is nothing to reconcile before that
            return True

        if self.charm.state.bind_address == recorded_ip:
            return True

        try:
            self.charm.auth_manager.configure_auth()
            self.charm.config_manager.configure_services(
                self.charm.sentinel_manager.get_primary_ip()
            )

            if self.charm.tls_manager.certificate_sans_require_update():
                if self.charm.state.client_tls_relation:
                    # the provider owns the certificate; converge once it signs the new one
                    self.charm.tls_events.refresh_tls_certificates_event.emit()
                    return False

                self.charm.tls_manager.create_and_store_self_signed_certificate()
        except (
            ValkeyCannotGetPrimaryIPError,
            ValkeyConfigurationError,
            ValkeyWorkloadCommandError,
        ) as e:
            logger.error("Failed to reconcile the unit address: %s", e)
            return False

        self.charm.state.unit_server.update(
            {
                "hostname": self.charm.state.hostname,
                "private_ip": self.charm.state.bind_address,
            }
        )
        self.charm.restart_workload.emit()
        return True

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Handle the config_changed event."""
        if not self._reconcile_unit_address():
            # config-changed does not repeat on its own, so the reconcile must be retried
            event.defer()
            return

        if not self.charm.unit.is_leader():
            return

        if admin_secret_id := self.charm.config.get(INTERNAL_USERS_PASSWORD_CONFIG):
            try:
                self._update_internal_users_password(str(admin_secret_id))
            except (
                ops.ModelError,
                ops.SecretNotFoundError,
                ValkeyACLLoadError,
                ValkeyWorkloadCommandError,
            ):
                event.defer()
                return

            # propagate updated credentials to topology observer
            try:
                self.charm.topology_manager.restart_observer()
            except (ValkeyWorkloadCommandError, ValueError) as e:
                logger.error("Failed to restart topology observer: %s", e)

    def _on_secret_changed(self, event: ops.SecretChangedEvent) -> None:
        """Handle the secret_changed event."""
        if not (admin_secret_id := self.charm.config.get(INTERNAL_USERS_PASSWORD_CONFIG)):
            return

        if self.charm.unit.is_leader():
            if admin_secret_id == event.secret.id:
                try:
                    self._update_internal_users_password(str(admin_secret_id))
                except (
                    ops.ModelError,
                    ops.SecretNotFoundError,
                    ValkeyACLLoadError,
                    ValkeyWorkloadCommandError,
                ):
                    event.defer()
                    return

                # propagate updated credentials to topology observer
                try:
                    self.charm.topology_manager.restart_observer()
                except (ValkeyWorkloadCommandError, ValueError) as e:
                    logger.error("Failed to restart topology observer: %s", e)

            return

        # from here, code is only relevant for non-leader units
        if event.secret.label and event.secret.label.endswith(INTERNAL_USERS_SECRET_LABEL_SUFFIX):
            # leader unit processed the secret change from user, non-leader units can replicate
            try:
                self.charm.auth_manager.set_acl_file()
                self.charm.auth_manager.set_sentinel_acl_file()
                if self.charm.state.unit_server.is_started:
                    self.charm.cluster_manager.reload_acl_file()
                    self.charm.restart_workload.emit(restart_valkey=False, restart_sentinel=True)
                # update the local unit admin password to match the leader
                self.charm.auth_manager.update_local_valkey_admin_password()
                if self.charm.state.unit_server.is_started:
                    self.charm.cluster_manager.update_primary_auth()
            except (ValkeyACLLoadError, ValkeyWorkloadCommandError) as e:
                logger.error(e)
                self.charm.status.set_running_status(
                    ClusterStatuses.PASSWORD_UPDATE_FAILED.value,
                    scope="unit",
                    component_name=self.charm.cluster_manager.name,
                    statuses_state=self.charm.state.statuses,
                )
                event.defer()
                return
            self.charm.state.statuses.delete(
                ClusterStatuses.PASSWORD_UPDATE_FAILED.value,
                scope="unit",
                component=self.charm.cluster_manager.name,
            )

    def _update_internal_users_password(self, secret_id: str) -> None:
        """Update internal users' passwords in charm/valkey if they have changed.

        Args:
            secret_id (str): The id of the secret containing the internal users' passwords.
        """
        try:
            secret_content = self.charm.state.get_secret_from_id(secret_id)
        except (ops.ModelError, ops.SecretNotFoundError) as e:
            logger.error(e)
            self.charm.status.set_running_status(
                CharmStatuses.SECRET_ACCESS_ERROR.value,
                scope="app",
                component_name=self.charm.cluster_manager.name,
                statuses_state=self.charm.state.statuses,
            )
            raise

        self.charm.state.statuses.delete(
            CharmStatuses.SECRET_ACCESS_ERROR.value,
            scope="app",
            component=self.charm.cluster_manager.name,
        )

        if any(key not in CharmUsers for key in secret_content.keys()):
            logger.error("Invalid username in secret %s.", secret_id)
            self.charm.status.set_running_status(
                ClusterStatuses.PASSWORD_UPDATE_FAILED.value,
                scope="app",
                component_name=self.charm.cluster_manager.name,
                statuses_state=self.charm.state.statuses,
            )
            # do not raise here, we don't want to run again if data is wrong
            return

        # merge the credentials, replacing those which have been updated
        new_passwords = self.charm.state.cluster.internal_users_credentials | secret_content
        if new_passwords != self.charm.state.cluster.internal_users_credentials:
            logger.info("Password(s) for internal users have changed")
            try:
                self.charm.auth_manager.set_acl_file(passwords=new_passwords)
                self.charm.auth_manager.set_sentinel_acl_file(passwords=new_passwords)
                if self.charm.state.unit_server.is_started:
                    self.charm.cluster_manager.reload_acl_file()
                    self.charm.restart_workload.emit(restart_valkey=False, restart_sentinel=True)
                self.charm.state.cluster.update(
                    {
                        f"{user.value.replace('-', '_')}_password": new_passwords[user.value]
                        for user in CharmUsers
                    }
                )
                # update the local unit admin password
                self.charm.auth_manager.update_local_valkey_admin_password()
                if self.charm.state.unit_server.is_started:
                    self.charm.cluster_manager.update_primary_auth()
            except (
                ValkeyACLLoadError,
                ValueError,
                ValkeyWorkloadCommandError,
            ) as e:
                logger.error(e)
                self.charm.status.set_running_status(
                    ClusterStatuses.PASSWORD_UPDATE_FAILED.value,
                    scope="unit",
                    component_name=self.charm.cluster_manager.name,
                    statuses_state=self.charm.state.statuses,
                )
                raise e

        self.charm.state.statuses.delete(
            ClusterStatuses.PASSWORD_UPDATE_FAILED.value,
            scope="unit",
            component=self.charm.cluster_manager.name,
        )
        self.charm.state.statuses.delete(
            ClusterStatuses.PASSWORD_UPDATE_FAILED.value,
            scope="app",
            component=self.charm.cluster_manager.name,
        )

    def _on_storage_detaching(self, event: ops.StorageDetachingEvent) -> None:
        """Handle removal of a storage mount, e.g. when removing a unit.

        Unit teardown detaches every storage; whichever detaches first runs the
        safe scale-down (which stops the workload), so the later ones are no-ops.
        """
        if self.charm.state.unit_server.is_being_removed:
            return

        if (
            self.charm.state.unit_server.is_backup_in_progress
            or self.charm.state.cluster.is_restore_in_progress
        ):
            # Raise (not return) so the hook errors and Juju retries until the
            # backup/restore finishes; a plain return would lose the in-flight RDB.
            raise ValkeyBackupInProgressError(
                "Backup or restore in progress on this unit; refusing to scale down until it finishes."
            )

        self._scale_down_unit()

    def _scale_down_unit(self) -> None:
        """Failover if needed, flush the dataset, and stop the workload."""
        # get scale down lock
        scale_down_lock = ScaleDownLock(self.charm)

        self.charm.status.set_running_status(
            ScaleDownStatuses.WAIT_FOR_LOCK.value,
            scope="unit",
            component_name=self.charm.cluster_manager.name,
            statuses_state=self.charm.state.statuses,
        )

        try:
            primary_ip = self.charm.sentinel_manager.get_primary_ip_for_scale_down()
        except ValkeyCannotGetPrimaryIPError as e:
            logger.error(e)
            self._set_state_for_going_away()
            return

        # blocks until the lock is acquired
        if not scale_down_lock.request_lock(primary_ip=primary_ip):
            raise RequestingLockTimedOutError("Failed to acquire scale down lock within timeout")

        self.charm.state.statuses.delete(
            ScaleDownStatuses.WAIT_FOR_LOCK.value,
            scope="unit",
            component=self.charm.cluster_manager.name,
        )

        self.charm.status.set_running_status(
            ScaleDownStatuses.SCALING_DOWN.value,
            scope="unit",
            component_name=self.charm.cluster_manager.name,
            statuses_state=self.charm.state.statuses,
        )

        # if unit has primary then failover
        try:
            primary_ip = self.charm.sentinel_manager.get_primary_ip_for_scale_down()
        except ValkeyCannotGetPrimaryIPError as e:
            logger.error(e)
            self._set_state_for_going_away()
            return

        active_sentinels = self.charm.sentinel_manager.get_active_sentinel_ips(primary_ip)
        unit_is_primary = primary_ip == self.charm.state.unit_server.get_endpoint(
            self.charm.state.substrate
        )

        if unit_is_primary and len(active_sentinels) > 1:
            logger.debug("Triggering sentinel failover on primary IP %s", primary_ip)
            self.charm.sentinel_manager.failover()
            primary_ip = self.charm.sentinel_manager.get_primary_ip()
            logger.debug(
                "Failover completed, new primary ip %s",
                primary_ip,
            )

        if self.charm.unit.is_leader():
            self.charm.topology_manager.stop_observer()

        if not unit_is_primary:
            logger.info("Waiting for replica to be fully-synced before saving the dataset")
            self.charm.cluster_manager.wait_for_replica_fully_synced(primary_ip)

        # stop valkey
        self.charm.cluster_manager.save_dataset_before_shutdown()
        try:
            self.charm.workload.stop()
        except ValkeyServicesCouldNotBeStoppedError as e:
            logger.error("Could not stop Valkey services cleanly: %s", e)

        self.charm.state.unit_server.update({"start_state": StartState.NOT_STARTED.value})

        try:
            scale_down_lock.release_lock(primary_ip=primary_ip)
        except ValkeyWorkloadCommandError:
            logger.warning("Failed to release scale-down lock, will be released when TTL expires")

        self._set_state_for_going_away()

    def _set_state_for_going_away(self) -> None:
        """Set the state to going away when the unit is going down."""
        if self.charm.app.planned_units() == 0 and self.charm.unit.is_leader():
            # clear app data bag
            self.charm.state.cluster.update(
                {
                    "internal_ca_certificate": None,
                    "internal_ca_private_key": None,
                }
            )

        self.charm.state.unit_server.update({"scale_down_state": ScaleDownState.GOING_AWAY.value})

    def _reconfigure_quorum_if_necessary(self) -> None:
        """Reconfigure the sentinel quorum if it does not match the current cluster size."""
        # if the unit / all units are being removed, we do not need to reconfigure the quorum
        if (
            not self.charm.state.unit_server.is_active
            or self.charm.state.unit_server.is_being_removed
            or self.model.app.planned_units() == 0
            # to avoid failures if a Sentinel has not been restarted yet
            # does not rely on TLS state because databag might be outdated in deferred events
            or (
                self.charm.state.client_tls_relation
                and not self.charm.state.unit_server.is_tls_enabled
            )
            or (
                self.charm.state.unit_server.is_tls_enabled
                and not self.charm.state.client_tls_relation
            )
        ):
            return

        if self.charm.sentinel_manager.get_configured_quorum() != self.charm.config_manager.quorum:
            logger.debug("Updating sentinel quorum to match current cluster size")
            self.charm.sentinel_manager.set_quorum(self.charm.config_manager.quorum)
            self.charm.config_manager.set_sentinel_config_properties(
                self.charm.sentinel_manager.get_primary_ip()
            )
