#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""LDAP related event handlers."""

import logging
import time
from typing import TYPE_CHECKING

import ops
from charmlibs.interfaces.certificate_transfer import (
    CertificatesAvailableEvent,
    CertificatesRemovedEvent,
    CertificateTransferRequires,
)
from charms.glauth_k8s.v0.ldap import (
    LdapReadyEvent,
    LdapRequirer,
    LdapUnavailableEvent,
)

from common.exceptions import (
    ValkeyACLLoadError,
    ValkeyCannotGetPrimaryIPError,
    ValkeyWorkloadCommandError,
)
from literals import LDAP_CA_CERT_RELATION, LDAP_RELATION, PEER_RELATION

if TYPE_CHECKING:
    from charm import ValkeyCharm

logger = logging.getLogger(__name__)


class LDAPEvents(ops.Object):
    """Handle all events related to LDAP."""

    def __init__(self, charm: "ValkeyCharm"):
        super().__init__(charm, key="ldap_events")
        self.charm = charm

        self.ldap_requirer = LdapRequirer(self.charm, LDAP_RELATION)
        self.ldap_ca_transfer = CertificateTransferRequires(self.charm, LDAP_CA_CERT_RELATION)

        self.framework.observe(self.ldap_requirer.on.ldap_ready, self._on_ldap_ready)
        self.framework.observe(self.ldap_requirer.on.ldap_unavailable, self._on_ldap_unavailable)
        self.framework.observe(
            self.ldap_ca_transfer.on.certificate_set_updated, self._on_ldap_ca_available
        )
        self.framework.observe(
            self.ldap_ca_transfer.on.certificates_removed, self._on_ldap_ca_removed
        )
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)
        self.framework.observe(self.charm.on.sync_ldap_users_action, self._on_sync_ldap_users)
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_changed, self._on_peer_relation_changed
        )

    def _on_ldap_ready(self, event: LdapReadyEvent) -> None:
        """Handle the setup of the LDAP relation."""
        # A restore restarts the primary; defer LDAP reconfiguration until it
        # completes rather than colliding with the restart. These events don't
        # re-fire once the restore clears, so defer (not return) to keep them.
        if self.charm.state.cluster.is_restore_in_progress:
            event.defer()
            return

        if not self.charm.state.is_ldap_valid:
            return

        try:
            self._update_ldap_config()
            self.charm.state.unit_server.update({"ldap_enabled": True})
        except (
            ValkeyACLLoadError,
            ValkeyCannotGetPrimaryIPError,
            ValkeyWorkloadCommandError,
        ) as e:
            logger.error("Failed to enable LDAP: %s", e)
            event.defer()
            return

    def _on_ldap_unavailable(self, event: LdapUnavailableEvent) -> None:
        """Handle the removal of the LDAP relation."""
        if self.charm.state.cluster.is_restore_in_progress:
            event.defer()
            return

        if not self.charm.state.unit_server.is_ldap_enabled:
            return

        try:
            self._update_ldap_config()
            self.charm.state.unit_server.update({"ldap_enabled": False})
        except (
            ValkeyACLLoadError,
            ValkeyCannotGetPrimaryIPError,
            ValkeyWorkloadCommandError,
        ) as e:
            logger.error("Failed to disable LDAP: %s", e)
            event.defer()
            return

    def _on_ldap_ca_available(self, event: CertificatesAvailableEvent) -> None:
        """Handle the CA certificate available event for LDAP."""
        if self.charm.state.cluster.is_restore_in_progress:
            event.defer()
            return

        if not (
            ca_certs := self.ldap_ca_transfer.get_all_certificates(
                relation_id=event.relation_id  # pyright: ignore[reportArgumentType]
            )
        ):
            logger.warning("No CA certificates published yet by the LDAP CA provider")
            return

        try:
            self.charm.workload.make_dir(self.charm.workload.tls_dir, exist_ok=True)
            self.charm.workload.write_file(
                # sorted so a re-delivered event rewrites byte-identical content
                content="\n".join(sorted(ca_certs)),
                path=self.charm.workload.tls_paths.ldap_ca,
            )
        except ValkeyWorkloadCommandError as e:
            logger.error("Error storing CA certificate for LDAP provider: %s", e)
            event.defer()
            return

        if not self.charm.state.is_ldap_valid:
            return

        try:
            self._update_ldap_config()
            self.charm.state.unit_server.update({"ldap_enabled": True})
        except (
            ValkeyACLLoadError,
            ValkeyCannotGetPrimaryIPError,
            ValkeyWorkloadCommandError,
        ) as e:
            logger.error("Failed to enable LDAP: %s", e)
            event.defer()
            return

    def _on_ldap_ca_removed(self, event: CertificatesRemovedEvent) -> None:
        """Handle the CA certificate removed event for LDAP."""
        if self.charm.state.cluster.is_restore_in_progress:
            event.defer()
            return

        try:
            self.charm.workload.remove_file(self.charm.workload.tls_paths.ldap_ca)
        except ValkeyWorkloadCommandError as e:
            logger.error("Error removing CA certificate for LDAP: %s", e)
            event.defer()
            return

        if not self.charm.state.unit_server.is_ldap_enabled:
            return

        try:
            self._update_ldap_config()
            self.charm.state.unit_server.update({"ldap_enabled": False})
        except (
            ValkeyACLLoadError,
            ValkeyCannotGetPrimaryIPError,
            ValkeyWorkloadCommandError,
        ) as e:
            logger.error("Failed to disable LDAP: %s", e)
            event.defer()
            return

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Handle LDAP related config changes."""
        if not self.charm.state.is_ldap_valid:
            return

        # Guard AFTER the is_ldap_valid filter so a non-LDAP cluster isn't
        # needlessly deferred; defer only a real LDAP reconfigure during a restore.
        if self.charm.state.cluster.is_restore_in_progress:
            event.defer()
            return

        try:
            self._update_ldap_config()
            self.charm.state.unit_server.update({"ldap_enabled": True})
        except (
            ValkeyACLLoadError,
            ValkeyCannotGetPrimaryIPError,
            ValkeyWorkloadCommandError,
        ) as e:
            logger.error("Failed to update LDAP settings: %s", e)
            event.defer()
            return

    def _on_secret_changed(self, event: ops.SecretChangedEvent) -> None:
        """Handle LDAP related secret updates."""
        if not self.charm.state.is_ldap_valid:
            return

        if event.secret.id != self.charm.state.ldap.bind_password_secret:
            return

        # Guard AFTER the secret-id filter so an unrelated secret change isn't
        # needlessly deferred; defer only a real LDAP reconfigure during a restore.
        if self.charm.state.cluster.is_restore_in_progress:
            event.defer()
            return

        try:
            self._update_ldap_config()
        except (
            ValkeyACLLoadError,
            ValkeyCannotGetPrimaryIPError,
            ValkeyWorkloadCommandError,
        ) as e:
            logger.error("Failed to update LDAP settings: %s", e)
            event.defer()
            return

    def _update_ldap_config(self) -> None:
        """Update the current LDAP configuration."""
        if not self.charm.state.unit_server.is_started:
            return

        if (
            self.charm.state.ldap_ca_cert_relation
            and not self.charm.workload.tls_paths.ldap_ca.exists()
        ):
            raise ValkeyWorkloadCommandError("LDAP TLS CA certificate not stored yet")

        logger.info("Update LDAP configuration")
        primary_ip = self.charm.sentinel_manager.get_primary_ip()
        self.charm.config_manager.set_config_properties(primary_endpoint=primary_ip)
        ldap_config = self.charm.config_manager.generate_ldap_config()
        self.charm.cluster_manager.reload_ldap_settings(ldap_config)

        logger.info("Update ACL configuration")
        # only update Valkey ACLs, we do not support LDAP for Sentinel
        self.charm.auth_manager.set_acl_file()
        self.charm.cluster_manager.reload_acl_file()

    def _on_sync_ldap_users(self, event: ops.ActionEvent) -> None:
        """Handle the action event for sync-ldap-users."""
        if not self.charm.unit.is_leader():
            error = "Action can only be run on the leader unit"
            event.set_results({"error": error})
            event.fail(error)
            return

        if self.charm.state.cluster.is_restore_in_progress:
            error = "A restore is in progress; try again after it completes"
            event.set_results({"error": error})
            event.fail(error)
            return

        if not self.charm.state.unit_server.is_started:
            error = "Unit not fully started yet, wait for startup to complete"
            event.set_results({"error": error})
            event.fail(error)
            return

        if not self.charm.state.unit_server.is_ldap_enabled:
            error = "LDAP not yet enabled on this unit"
            event.set_results({"error": error})
            event.fail(error)
            return

        if not self.charm.state.is_ldap_valid:
            error = "LDAP configuration is invalid, see `debug-log` for details"
            event.set_results({"error": error})
            event.fail(error)
            return

        logger.info("Update ACL configuration")
        event.log(f"Querying LDAP and updating Valkey ACL configuration on {self.charm.unit.name}")
        self.charm.state.cluster.update({"ldap_user_epoch": time.time()})

        try:
            self.charm.auth_manager.set_acl_file()
            self.charm.cluster_manager.reload_acl_file()
        except (ValkeyACLLoadError, ValkeyWorkloadCommandError) as e:
            logger.error("Failed to update ACL settings: %s", e)
            event.set_results({"error": e})
            event.fail("Failed to update ACL settings, check `debug-log` for details")
            return

        self.charm.state.unit_server.update({"ldap_user_epoch": time.time()})
        event.set_results(
            {
                "result": f"Updated ACL configuration on {self.charm.unit.name}, check `debug-log` for errors"
            }
        )

    def _on_peer_relation_changed(self, event: ops.RelationChangedEvent) -> None:
        """Handle peer relation changes in regard to LDAP.

        This catches updates to the 'ldap-users-epoch' triggered by the 'sync-ldap-users'  action.
        """
        # Skip during a restore: the restore workflow drives peer relation-changed
        # itself, and reloading ACLs mid-restart would collide with the RDB swap.
        # Restore completion re-fires relation-changed, reconciling then.
        if self.charm.state.cluster.is_restore_in_progress:
            return

        if (
            self.charm.unit.is_leader()
            or not self.charm.state.unit_server.is_started
            or not self.charm.state.unit_server.is_ldap_enabled
            or not self.charm.state.is_ldap_valid
            or self.charm.state.unit_server.model.ldap_user_epoch
            >= self.charm.state.cluster.model.ldap_user_epoch
        ):
            return

        logger.info("Update ACL configuration")
        try:
            self.charm.auth_manager.set_acl_file()
            self.charm.cluster_manager.reload_acl_file()
        except (ValkeyACLLoadError, ValkeyWorkloadCommandError) as e:
            logger.error("Failed to update ACL settings: %s", e)
            return

        self.charm.state.unit_server.update({"ldap_user_epoch": time.time()})

    def _on_update_status(self, _: ops.UpdateStatusEvent) -> None:
        """Handle update status for LDAP."""
        if (
            not self.charm.state.unit_server.is_ldap_enabled
            or not self.charm.state.is_ldap_valid
            or self.charm.state.cluster.is_restore_in_progress
        ):
            return

        logger.info("Update ACL configuration")
        try:
            self.charm.auth_manager.set_acl_file()
            self.charm.cluster_manager.reload_acl_file()
        except (ValkeyACLLoadError, ValkeyWorkloadCommandError) as e:
            logger.error("Failed to update ACL settings: %s", e)
            return
