#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed k8s operator for Valkey."""

import logging
import os

import ops
from data_platform_helpers.advanced_statuses.handler import StatusHandler

from common.custom_events import RestartWorkloadEvent, TopologyChangedCharmEvents
from common.exceptions import ValkeyServicesFailedToStartError, ValkeyWorkloadCommandError
from common.locks import RestartLock
from core.cluster_state import ClusterState
from events.backup import BackupEvents
from events.base_events import BaseEvents
from events.external_clients import ExternalClientsEvents
from events.ldap import LDAPEvents
from events.tls import TLSEvents
from literals import CONTAINER, Substrate
from managers.auth import AuthManager
from managers.backup import BackupManager
from managers.cluster import ClusterManager
from managers.config import ConfigManager
from managers.external_clients import ExternalClientsManager
from managers.sentinel import SentinelManager
from managers.tls import TLSManager
from managers.topology import TopologyManager
from workload_k8s import ValkeyK8sWorkload
from workload_vm import ValkeyVmWorkload

logger = logging.getLogger(__name__)


class ValkeyCharm(ops.CharmBase):
    """Charmed Operator for Valkey."""

    restart_workload = ops.EventSource(RestartWorkloadEvent)
    # Overriding `on` with a custom CharmEvents subclass is the intended ops API;
    # pyright flags it only because `CharmBase.on` is declared as a property.
    on = TopologyChangedCharmEvents()  # pyright: ignore[reportIncompatibleMethodOverride, reportAssignmentType]

    def __init__(self, *args) -> None:
        super().__init__(*args)
        if "KUBERNETES_SERVICE_HOST" in os.environ:
            self.substrate = Substrate.K8S
            self.workload = ValkeyK8sWorkload(container=self.unit.get_container(CONTAINER))
        else:
            self.substrate = Substrate.VM
            self.workload = ValkeyVmWorkload()
        self.state = ClusterState(self, self.substrate)

        # --- MANAGERS ---
        self.cluster_manager = ClusterManager(state=self.state, workload=self.workload)
        self.config_manager = ConfigManager(state=self.state, workload=self.workload)
        self.sentinel_manager = SentinelManager(state=self.state, workload=self.workload)
        self.tls_manager = TLSManager(state=self.state, workload=self.workload)
        self.client_manager = ExternalClientsManager(state=self.state, workload=self.workload)
        self.topology_manager = TopologyManager(state=self.state, workload=self.workload)
        self.auth_manager = AuthManager(state=self.state, workload=self.workload)
        self.backup_manager = BackupManager(state=self.state, workload=self.workload)

        # --- STATUS HANDLER ---
        self.status = StatusHandler(
            self,
            self.cluster_manager,
            self.config_manager,
            self.auth_manager,
            self.sentinel_manager,
            self.tls_manager,
            self.client_manager,
            self.backup_manager,
        )

        # --- EVENT HANDLERS ---
        self.base_events = BaseEvents(self)
        self.tls_events = TLSEvents(self)
        self.client_events = ExternalClientsEvents(self)
        self.backup_events = BackupEvents(self)
        self.ldap_events = LDAPEvents(self)

        self.framework.observe(self.restart_workload, self._on_restart_workload)

    def _on_restart_workload(self, event: RestartWorkloadEvent) -> None:
        """Handle the restart_workload event."""
        logger.info(
            "Restarting workload Event. Restart Valkey: %s, Restart Sentinel: %s",
            event.restart_valkey,
            event.restart_sentinel,
        )
        if (
            self.state.unit_server.is_backup_in_progress
            or self.state.cluster.is_restore_in_progress
        ):
            logger.info("Backup/restore in progress on this unit; deferring restart_workload")
            event.defer()
            return
        restart_lock = RestartLock(self.state)
        restart_lock.request_lock()
        if not restart_lock.is_held_by_this_unit:
            logger.info("Waiting for lock to restart workload")
            event.defer()
            return

        try:
            if event.restart_valkey:
                self.workload.restart(self.workload.valkey_service)
            if event.restart_sentinel:
                # if primary endpoint is given, write sentinel config
                # this is necessary as Sentinel may rewrite its config file since the last write
                if event.primary_endpoint != "":
                    self.config_manager.set_sentinel_config_properties(
                        primary_endpoint=event.primary_endpoint
                    )
                self.sentinel_manager.restart_service()
        except (
            ValkeyServicesFailedToStartError,
            ValkeyWorkloadCommandError,
        ) as e:
            logger.error(e)
            restart_lock.release_lock()
            event.defer()
            return

        if event.restart_valkey and not self.cluster_manager.is_healthy(check_replica_sync=False):
            self.state.unit_server.update({"is_valkey_healthy": False})
            restart_lock.release_lock()
            event.defer()
            return
        self.state.unit_server.update({"is_valkey_healthy": True})

        if event.restart_valkey:
            # CONFIG SET min-replicas-to-write does not survive the restart we
            # just performed; the rendered file ships 1, so reassert the
            # topology-correct runtime value (0 on < 3 active units) now that
            # Valkey is back up and healthy, or a small cluster would be
            # write-frozen until the next peer-relation event.
            self.cluster_manager.reconcile_min_replicas_to_write()

        if event.restart_sentinel and not self.sentinel_manager.is_healthy():
            self.state.unit_server.update({"is_sentinel_healthy": False})
            restart_lock.release_lock()
            event.defer()
            return

        self.state.unit_server.update({"is_sentinel_healthy": True})
        restart_lock.release_lock()


if __name__ == "__main__":
    ops.main(ValkeyCharm)
