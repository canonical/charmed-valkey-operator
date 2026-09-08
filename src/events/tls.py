#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""TLS related event handlers."""

import logging
from typing import TYPE_CHECKING

import ops
from charmlibs.interfaces.tls_certificates import (
    CertificateAvailableEvent,
    CertificateDeniedEvent,
    CertificateRequestAttributes,
    TLSCertificatesRequiresV4,
)

from common.custom_events import RefreshTLSCertificatesEvent
from common.exceptions import (
    ValkeyCannotGetPrimaryIPError,
    ValkeyCertificatesNotReadyError,
    ValkeyTLSLoadError,
    ValkeyWorkloadCommandError,
)
from literals import (
    CLIENT_PORT,
    CLIENT_TLS_RELATION_NAME,
    PEER_RELATION,
    SENTINEL_PORT,
    TLS_CLIENT_PRIVATE_KEY_CONFIG,
    TLSCARotationState,
    TLSState,
)

if TYPE_CHECKING:
    from charm import ValkeyCharm

logger = logging.getLogger(__name__)


class TLSEvents(ops.Object):
    """Handle all TLS related events."""

    refresh_tls_certificates_event = ops.EventSource(RefreshTLSCertificatesEvent)

    def __init__(self, charm: "ValkeyCharm"):
        super().__init__(charm, key="tls_events")
        self.charm = charm

        self.client_certificate = TLSCertificatesRequiresV4(
            self.charm,
            CLIENT_TLS_RELATION_NAME,
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=self.charm.tls_manager.build_common_name(),
                    sans_ip=self.charm.tls_manager.build_sans_ip(),
                    sans_dns=self.charm.tls_manager.build_sans_dns(),
                ),
            ],
            private_key=self.charm.tls_manager.get_client_tls_private_key(),
            refresh_events=[self.refresh_tls_certificates_event],
        )

        # --- EVENTS TO OBSERVE ---
        self.framework.observe(
            self.charm.on[CLIENT_TLS_RELATION_NAME].relation_created,
            self._on_tls_relation_created,
        )
        self.framework.observe(
            self.charm.on[CLIENT_TLS_RELATION_NAME].relation_broken,
            self._on_tls_relation_broken,
        )
        self.framework.observe(
            self.client_certificate.on.certificate_available,
            self._on_certificate_available,
        )
        self.framework.observe(
            self.client_certificate.on.certificate_denied, self._on_certificate_denied
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_created,
            self._on_peer_relation_created,
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_changed,
            self._on_peer_relation_changed,
        )
        self.framework.observe(self.charm.on.update_status, self._on_update_status)
        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)

    def _on_peer_relation_created(self, event: ops.RelationCreatedEvent) -> None:
        """Set up self-signed certificates for peer TLS by default."""
        if self.charm.unit.is_leader():
            self.charm.tls_manager.generate_ca_certificate()

        # in case a non-leader unit gets the event before the leader unit has processed it
        if not self.charm.state.cluster.internal_ca_certificate:
            logger.warning("Self-signed CA certificate not yet available")
            event.defer()
            return

        if self.charm.state.unit_server.model.client_cert_ready:
            logger.debug("Client TLS certificate provided, no need to generate self-signed cert")
            return

        try:
            self.charm.tls_manager.create_and_store_self_signed_certificate()
        except ValkeyWorkloadCommandError as e:
            logger.error("Failed to create certificate for peer-TLS, startup will fail: %s", e)
            event.defer()

    def _on_peer_relation_changed(self, event: ops.RelationChangedEvent) -> None:
        """Handle TLS related changes to the peer relation."""
        # Skip during a restore (CA rotation would collide with the primary
        # restart). Restore completion re-fires relation-changed, reconciling then.
        if self.charm.state.cluster.is_restore_in_progress:
            return

        if self.charm.state.unit_server.tls_ca_rotation_state != TLSCARotationState.NO_ROTATION:
            try:
                self._orchestrate_ca_rotation()
            except ValkeyCertificatesNotReadyError:
                logger.debug("Not all units ready")
            except ValkeyTLSLoadError:
                logger.error("Failed to reload TLS certificates")
                event.defer()
            except ValkeyWorkloadCommandError as e:
                logger.error("Workload error during CA rotation: %s", e)
                event.defer()
            return

        if (
            not self.charm.tls_manager.will_certificate_expire()
            or self.charm.state.client_tls_relation
        ):
            return

        logger.info("Renewing certificate for internal peer-TLS because it expires soon")
        if self.charm.unit.is_leader():
            # this triggers relation-changed event on all other units
            self.charm.tls_manager.generate_ca_certificate()

        try:
            # internal certs: new cert always means a CA rotation because of same expiration
            self.charm.tls_manager.start_ca_rotation_if_required()
            self.charm.tls_manager.set_ca_rotation_state(TLSCARotationState.NEW_CA_DETECTED)
            self.charm.tls_manager.create_and_store_self_signed_certificate()
            self.charm.tls_manager.set_ca_rotation_state(TLSCARotationState.NEW_CA_ADDED)
            self._orchestrate_ca_rotation()
        except ValkeyCertificatesNotReadyError:
            logger.debug("Not all units ready")
            return
        except (ValkeyTLSLoadError, ValkeyWorkloadCommandError):
            logger.error("Failed to reload TLS certificates")
            event.defer()
            return

    def _on_tls_relation_created(self, event: ops.RelationCreatedEvent) -> None:
        """Handle the `relation-created` event."""
        self.charm.tls_manager.set_tls_state(TLSState.TO_TLS)

    def _on_certificate_available(  # noqa: C901
        self, event: CertificateAvailableEvent
    ) -> None:
        """Handle the `certificate-available` event from TLS provider."""
        cert = event.certificate
        client_certificates, client_private_key = (
            self.client_certificate.get_assigned_certificates()
        )

        try:
            if client_certificates and client_certificates[0].certificate == cert:
                cert = client_certificates[0]
                private_key = client_private_key
                tls_state = self.charm.state.unit_server.tls_client_state
            else:
                logger.error("Received unknown certificate: %s", cert)
                return
        except IndexError:
            logger.error("Received certificate does not match provided certificates: %s", cert)
            return

        if self.charm.tls_manager.certificate_already_applied(cert):
            logger.debug("Client certificate unchanged and already active, nothing to do")
            return

        logger.info("Storing client certificate")
        try:
            if rotate_ca := self.charm.tls_manager.start_ca_rotation_if_required(cert):
                self.charm.tls_manager.set_ca_rotation_state(TLSCARotationState.NEW_CA_DETECTED)
            self.charm.tls_manager.write_certificate(cert, private_key)  # pyright: ignore[reportArgumentType]
            self.charm.tls_manager.set_cert_state(is_ready=True)
        except ValkeyWorkloadCommandError as e:
            logger.error("Failed to store certificate: %s", e)
            event.defer()
            return

        if tls_state == TLSState.TLS:
            logger.info("Refreshing TLS certificates in Valkey")
            try:
                if rotate_ca:
                    self.charm.tls_manager.set_ca_rotation_state(TLSCARotationState.NEW_CA_ADDED)
                    # safe before orchestration: the skip guard is inert while
                    # tls_ca_rotation is not NO_ROTATION
                    self.charm.tls_manager.record_applied_certificate(cert)
                    self._orchestrate_ca_rotation()
                    return

                tls_config = self.charm.config_manager.generate_tls_config()
                self.charm.cluster_manager.reload_tls_settings(tls_config)
                self.charm.restart_workload.emit(restart_valkey=False, restart_sentinel=True)
                self.charm.tls_manager.record_applied_certificate(cert)
            except ValkeyCertificatesNotReadyError:
                logger.debug("Not all units ready")
            except (ValkeyTLSLoadError, ValkeyWorkloadCommandError):
                logger.error("Failed to reload TLS certificates")
                event.defer()
            return

        primary_ip = ""
        if self.charm.state.unit_server.is_started:
            try:
                primary_ip = self.charm.sentinel_manager.get_primary_ip()
            except ValkeyCannotGetPrimaryIPError as e:
                logger.error("No Primary available: %s", e)
                event.defer()
                return

        try:
            self._enable_client_tls()
        except (
            ValkeyCannotGetPrimaryIPError,
            ValkeyWorkloadCommandError,
            ValkeyTLSLoadError,
            ValueError,
        ):
            logger.error("Failed to enable client TLS")
            event.defer()
            return
        except ValkeyCertificatesNotReadyError:
            logger.warning("Not all units have stored the client certificate")
            event.defer()
            return

        self.charm.tls_manager.set_tls_state(TLSState.TLS)
        self.charm.unit.close_port("tcp", CLIENT_PORT)

        if self.charm.state.unit_server.is_started:
            logger.info("Restarting Sentinel")
            self.charm.restart_workload.emit(
                restart_valkey=False, restart_sentinel=True, primary_endpoint=primary_ip
            )
        self.charm.tls_manager.record_applied_certificate(cert)
        self._trigger_relation_change_if_required()

    def _on_certificate_denied(self, event: CertificateDeniedEvent) -> None:
        """Handle the `certificate-denied` event from TLS provider."""
        if event.certificate_signing_request in [
            csr.certificate_signing_request
            for csr in self.client_certificate.get_csrs_from_requirer_relation_data()
        ]:
            logger.error("Certificate request was denied: %s", event.error.message)
            return

        logger.warning(
            "Certificate denied event received for unknown signing request: %s",
            event.certificate_signing_request,
        )

    def _on_tls_relation_broken(self, event: ops.RelationBrokenEvent) -> None:
        """Handle the `relation-broken` event."""
        if self.charm.app.planned_units() == 0 or self.charm.state.unit_server.is_being_removed:
            return

        if not self.charm.state.unit_server.model.client_cert_ready:
            logger.info("Client TLS relation removed, no certificate was stored yet")
            self.charm.tls_manager.set_tls_state(TLSState.NO_TLS)
            return

        if not self.charm.state.cluster.internal_ca_certificate:
            if self.charm.unit.is_leader():
                self.charm.tls_manager.generate_ca_certificate()
            else:
                logger.warning("Self-signed CA certificate not yet available")
                event.defer()
                return

        try:
            primary_ip = self.charm.sentinel_manager.get_primary_ip()
        except ValkeyCannotGetPrimaryIPError as e:
            logger.error("No Primary available: %s", e)
            event.defer()
            return

        if self.charm.state.unit_server.is_tls_enabled:
            logger.info("Disabling client TLS")
            self.charm.tls_manager.set_tls_state(TLSState.TO_NO_TLS)
            try:
                self.charm.config_manager.set_config_properties(primary_endpoint=primary_ip)
                tls_config = self.charm.config_manager.generate_tls_config()
                self.charm.cluster_manager.reload_tls_settings(tls_config)
            except (ValkeyWorkloadCommandError, ValkeyTLSLoadError, ValueError):
                logger.error("Failed to disable client TLS")
                event.defer()
                return

            self.charm.tls_manager.set_cert_state(is_ready=False)
            self.charm.tls_manager.set_tls_state(TLSState.NO_TLS)
            self.charm.unit.open_port("tcp", CLIENT_PORT)
            self.charm.unit.open_port("tcp", SENTINEL_PORT)
            self._trigger_relation_change_if_required()

        try:
            self.charm.tls_manager.create_and_store_self_signed_certificate()
            tls_config = self.charm.config_manager.generate_tls_config()
            self.charm.cluster_manager.reload_tls_settings(tls_config)
        except (ValkeyWorkloadCommandError, ValkeyTLSLoadError, ValueError) as e:
            logger.error("Failed to setup peer-TLS: %s", e)
            event.defer()
            return

        logger.info("Restarting Sentinel")
        self.charm.restart_workload.emit(
            restart_valkey=False, restart_sentinel=True, primary_endpoint=primary_ip
        )

    def _on_update_status(self, event: ops.UpdateStatusEvent) -> None:
        """Handle TLS related parts of update_status event."""
        if not self.charm.tls_manager.will_certificate_expire():
            return

        if self.charm.state.client_tls_relation:
            return

        if self.charm.unit.is_leader():
            # need to renew CA first (same validity), this triggers relation-changed event
            self.charm.tls_manager.generate_ca_certificate()

        self._trigger_relation_change_if_required()

    def _on_secret_changed(self, event: ops.SecretChangedEvent) -> None:
        """Handle TLS related secret changes."""
        if not (secret_id := self.charm.config.get(TLS_CLIENT_PRIVATE_KEY_CONFIG)):
            return

        if secret_id != event.secret.id:
            return

        if not (
            private_key := self.charm.tls_manager.read_and_validate_private_key(str(secret_id))
        ):
            logger.error("Invalid private key provided, cannot update TLS certificates.")
            return

        if self.charm.unit.is_leader():
            self.charm.state.cluster.update({"tls_client_private_key": private_key.raw})

        if self.charm.state.client_tls_relation:
            self.refresh_tls_certificates_event.emit()

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Handle TLS related config changes."""
        if not self.charm.tls_manager.extra_sans_config_is_valid():
            logger.warning("Invalid configuration for 'certificate-extra-sans'")
            return

        if secret_id := self.charm.config.get(TLS_CLIENT_PRIVATE_KEY_CONFIG):
            if private_key := self.charm.tls_manager.read_and_validate_private_key(str(secret_id)):
                if self.charm.unit.is_leader():
                    self.charm.state.cluster.update({"tls_client_private_key": private_key.raw})
                # refresh event will be ignored by the tls lib if the csr is unchanged
                self.refresh_tls_certificates_event.emit()
            else:
                logger.error("Invalid private key provided, cannot update TLS certificates.")

        if (
            self.charm.state.client_tls_relation
            and self.charm.tls_manager.certificate_sans_require_update()
        ):
            logger.info("Configuration change for TLS, refresh TLS certificates")
            self.refresh_tls_certificates_event.emit()

    def _enable_client_tls(self) -> None:
        """Check preconditions and enable TLS if possible."""
        if not all(server.model.client_cert_ready for server in self.charm.state.servers):
            raise ValkeyCertificatesNotReadyError

        if not self.charm.state.unit_server.is_started:
            logger.debug("Not started yet, enabling client TLS will happen on start")
            return

        logger.info("Enabling client TLS in Valkey")
        primary_ip = self.charm.sentinel_manager.get_primary_ip()
        self.charm.config_manager.set_config_properties(primary_endpoint=primary_ip)
        tls_config = self.charm.config_manager.generate_tls_config()
        self.charm.cluster_manager.reload_tls_settings(tls_config)

    def _orchestrate_ca_rotation(self) -> None:
        """Orchestrate the workflow when a TLS CA rotation has been initiated."""
        match self.charm.state.unit_server.tls_ca_rotation_state:
            case TLSCARotationState.NEW_CA_DETECTED:
                if self.charm.state.client_tls_relation:
                    # new client TLS CA is stored in certificate_available event
                    return
                self.charm.tls_manager.create_and_store_self_signed_certificate()
                self.charm.tls_manager.set_ca_rotation_state(TLSCARotationState.NEW_CA_ADDED)
            case TLSCARotationState.NEW_CA_ADDED:
                if not all(
                    server.tls_ca_rotation_state
                    in [TLSCARotationState.NEW_CA_ADDED, TLSCARotationState.CA_UPDATED]
                    for server in self.charm.state.servers
                ):
                    raise ValkeyCertificatesNotReadyError

                logger.info("Reload TLS certificates after all units have added the new CA")
                tls_config = self.charm.config_manager.generate_tls_config()
                self.charm.cluster_manager.reload_tls_settings(tls_config)
                self.charm.tls_manager.set_ca_rotation_state(TLSCARotationState.CA_UPDATED)
                self._trigger_relation_change_if_required()

            case TLSCARotationState.CA_UPDATED:
                if not all(
                    server.model.tls_ca_rotation
                    in [TLSCARotationState.CA_UPDATED, TLSCARotationState.NO_ROTATION]
                    for server in self.charm.state.servers
                ):
                    raise ValkeyCertificatesNotReadyError

                logger.info("Remove old CA after all units have updated the TLS certificate")
                self.charm.workload.remove_file(
                    self.charm.workload.tls_paths.client_ca.with_name("old_client_ca.pem")
                )
                self.charm.tls_manager.rehash_ca_certificates()
                tls_config = self.charm.config_manager.generate_tls_config()
                self.charm.cluster_manager.reload_tls_settings(tls_config)
                self.charm.restart_workload.emit(restart_valkey=False, restart_sentinel=True)
                self.charm.tls_manager.set_ca_rotation_state(TLSCARotationState.NO_ROTATION)

    def _trigger_relation_change_if_required(self) -> None:
        """Trigger a relation changed event if it is a single-unit deployment."""
        if len(self.charm.state.servers) != 1:
            return

        logger.debug("Trigger a relation-changed event in a single-unit deployment")
        self.charm.on[PEER_RELATION].relation_changed.emit(self.charm.state.peer_relation)
