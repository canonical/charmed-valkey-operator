#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""Manager for handling TLS-related operations."""

import base64
import binascii
import logging
import re
from datetime import timedelta
from hashlib import sha256
from ipaddress import ip_address

from charmlibs.interfaces.tls_certificates import (
    Certificate,
    CertificateRequestAttributes,
    CertificateSigningRequest,
    PrivateKey,
    ProviderCertificate,
)
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from ops import ModelError, SecretNotFoundError
from validators import ValidationError, hostname

from common.exceptions import ValkeyWorkloadCommandError
from common.network_utils import resolve_k8s_fqdn
from core.base_workload import WorkloadBase
from core.cluster_state import ClusterState
from literals import (
    TLS_CLIENT_PRIVATE_KEY_CONFIG,
    K8sService,
    Substrate,
    TLSCARotationState,
    TLSState,
)
from statuses import CharmStatuses, TLSStatuses

logger = logging.getLogger(__name__)


class TLSManager(ManagerStatusProtocol):
    """Manage all TLS related events."""

    name: str = "tls"
    state: ClusterState

    def __init__(self, state: ClusterState, workload: WorkloadBase):
        # `ClusterState` satisfies `StatusesStateProtocol`; pyright flags this only
        # because the protocol declares `state` as a mutable (invariant) attribute.
        self.state = state  # pyright: ignore[reportIncompatibleVariableOverride]
        self.workload = workload

    def set_tls_state(self, state: TLSState) -> None:
        """Set the TLS state.

        Args:
            state (TLSState): The TLS state.
        """
        logger.debug("Setting TLS state to %s", state)
        self.state.unit_server.update({"tls_client_state": state.value})

    def set_cert_state(self, is_ready: bool) -> None:
        """Set the certificate state."""
        self.state.unit_server.update({"client_cert_ready": is_ready})

    def set_ca_rotation_state(self, state: TLSCARotationState) -> None:
        """Set the CA rotation state.

        Args:
            state (TLSCARotationState): The CA rotation state.
        """
        logger.debug(f"Setting TLS CA rotation state to {state}")
        self.state.unit_server.update({"tls_ca_rotation": state.value})

    def client_cert_fingerprint(self, certificate: ProviderCertificate) -> str:
        """Return a stable fingerprint for a client certificate + CA pair."""
        return sha256((certificate.certificate.raw + certificate.ca.raw).encode()).hexdigest()

    def certificate_already_applied(self, certificate: ProviderCertificate) -> bool:
        """Return whether this exact certificate is stored AND fully activated on this unit.

        The tls-certificates lib re-emits ``certificate_available`` for an
        unchanged certificate on every relation-changed of the (app-shared)
        client-certificates relation, so requirers must be idempotent.

        The fingerprint is recorded only once an apply has been handed off
        (restart emitted, or CA rotation started — the rotation state machine
        owns activation from there), so a deferred/failed apply is still
        retried instead of being skipped.
        """
        if self.state.unit_server.tls_client_state != TLSState.TLS:
            return False
        if self.state.unit_server.tls_ca_rotation_state != TLSCARotationState.NO_ROTATION:
            return False
        if not self.state.unit_server.model.client_cert_ready:
            return False
        return self.state.unit_server.model.applied_client_cert_fingerprint == (
            self.client_cert_fingerprint(certificate)
        )

    def record_applied_certificate(self, certificate: ProviderCertificate) -> None:
        """Record the certificate as applied, enabling the skip on re-emission."""
        self.state.unit_server.update(
            {"applied_client_cert_fingerprint": self.client_cert_fingerprint(certificate)}
        )

    def write_certificate(self, certificate: ProviderCertificate, private_key: PrivateKey) -> None:
        """Store the certificate on the unit.

        Args:
            certificate (ProviderCertificate): The certificate.
            private_key (PrivateKey): The private key.
        """
        self.workload.make_dir(self.workload.tls_dir, exist_ok=True)
        self.workload.make_dir(self.workload.tls_paths.ca_certs_dir, exist_ok=True)

        self.workload.write_file(private_key.raw, self.workload.tls_paths.client_key)
        self.workload.write_file(certificate.certificate.raw, self.workload.tls_paths.client_cert)
        self.workload.write_file(certificate.ca.raw, self.workload.tls_paths.client_ca)
        self.rehash_ca_certificates()

    def rehash_ca_certificates(self) -> None:
        """Generate hashed certificate names according to x509 format."""
        # using a CA directory for TLS requires hashed file links, see:
        # https://docs.openssl.org/1.1.1/man1/verify/#options
        cmd = ["c_rehash", self.workload.tls_paths.ca_certs_dir.as_posix()]
        self.workload.exec(cmd)

    def build_common_name(self) -> str:
        """Build the Common Name for the TLS certificate."""
        # default common name, as per DA-166
        # https://docs.google.com/document/d/1OyxzBq5H4sFYZgmhWWVy5F4tQFmASQ7XKHVghxjW6go/edit?usp=sharing
        return f"{self.state.unit_server.unit_name.replace('/', '')}-{self.state.model.uuid}"

    def build_sans_ip(self) -> frozenset[str]:
        """Build the SANs IP for the TLS certificate."""
        sans_ip = set()

        # Rely fully on FQDN on k8s
        if not self.state.unit_server.model:
            return frozenset(sans_ip)

        if self.extra_sans_config_is_valid() and (
            extra_sans_config := self.state.config.get("certificate-extra-sans")
        ):
            extra_sans = [san.strip() for san in extra_sans_config.split(",")]  # pyright: ignore[reportAttributeAccessIssue]
            sans_ip = {san for san in extra_sans if self._is_ip_address(san)}

        if self.state.substrate == Substrate.K8S:
            return frozenset(sans_ip)

        sans_ip.add(self.state.bind_address)

        if ingress_ip := self.state.ingress_address:
            sans_ip.add(ingress_ip)

        return frozenset(sans_ip)

    def build_sans_dns(self) -> frozenset[str]:
        """Build the SANs DNS for the TLS certificate.

        Returns:
            frozenset[str]: The SANs DNS.
        """
        sans_dns = set()

        if not self.state.unit_server.model:
            return frozenset(sans_dns)

        if self.extra_sans_config_is_valid() and (
            extra_sans_config := self.state.config.get("certificate-extra-sans")
        ):
            extra_sans = [san.strip() for san in extra_sans_config.split(",")]  # pyright: ignore[reportAttributeAccessIssue]
            sans_dns = {
                san.replace("{unit}", str(self.state.unit_server.unit_id))
                for san in extra_sans
                if not self._is_ip_address(san)
            }

        sans_dns.add(self.state.unit_server.unit_name.replace("/", ""))
        sans_dns.add(self.state.hostname)

        if self.state.substrate == Substrate.K8S:
            primary_service = f"{self.state.model.app.name}-{K8sService.PRIMARY.value}"
            replicas_service = f"{self.state.model.app.name}-{K8sService.REPLICAS.value}"
            sans_dns.add(resolve_k8s_fqdn(primary_service))
            sans_dns.add(resolve_k8s_fqdn(replicas_service))

        return frozenset(sans_dns)

    def read_and_validate_private_key(self, private_key_secret_id: str) -> PrivateKey | None:
        """Read and validate a private key provided via Juju secret.

        Args:
            private_key_secret_id (str): The Juju secret ID for the secret
                that stores the private key.

        Returns:
            PrivateKey: The private key.
        """
        try:
            secret_content = self.state.get_secret_from_id(private_key_secret_id).get(
                "private-key"
            )
        except (ModelError, SecretNotFoundError) as e:
            logger.error(e)
            return None

        if secret_content is None:
            logger.error(f"Secret {private_key_secret_id} does not contain a private key.")
            return None

        try:
            private_key = (
                secret_content
                if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", secret_content)
                else base64.b64decode(secret_content).decode("utf-8").strip()
            )
        except (UnicodeDecodeError, binascii.Error) as e:
            logger.error(e)
            return None
        try:
            private_key = PrivateKey(raw=private_key)
        except ValueError as e:
            logger.error(e)
            return None
        if not private_key.is_valid():
            logger.error("Invalid private key format.")
            return None

        return private_key

    def get_client_tls_private_key(self) -> PrivateKey | None:
        """Get the private key provided by users, if available."""
        if secret_id := self.state.config.get(TLS_CLIENT_PRIVATE_KEY_CONFIG):
            if private_key := self.read_and_validate_private_key(str(secret_id)):
                return private_key

            # in case the configured secret is invalid
            return self.state.cluster.tls_client_private_key

        # in case no user supplied private key configured
        return None

    def _generate_private_key(self) -> PrivateKey:
        """Generate a private key for use in peer TLS."""
        return PrivateKey.generate()

    def generate_ca_certificate(self) -> None:
        """Generate a CA certificate for use in peer TLS and store it to peer relation data."""
        private_key = self._generate_private_key()
        ca_attributes = CertificateRequestAttributes(
            common_name="Valkey-Operator",
            is_ca=True,
            add_unique_id_to_subject_name=False,
        )

        ca_cert = Certificate.generate_self_signed_ca(
            attributes=ca_attributes,
            private_key=private_key,
            validity=timedelta(days=10950),
        )

        self.state.cluster.update({"internal_ca_certificate": ca_cert.raw})
        self.state.cluster.update({"internal_ca_private_key": private_key.raw})
        logger.info("Generated new self-signed CA certificate")

    def _generate_self_signed_certificate(self, private_key: PrivateKey) -> Certificate:
        """Generate a self-signed certificate for use in peer TLS."""
        cert_attributes = CertificateRequestAttributes(
            common_name=self.build_common_name(),
            sans_ip=self.build_sans_ip(),
            sans_dns=self.build_sans_dns(),
            is_ca=False,
            add_unique_id_to_subject_name=False,
        )
        certificate_signing_request = CertificateSigningRequest.generate(
            attributes=cert_attributes,
            private_key=private_key,
        )

        ca_cert = self.state.cluster.internal_ca_certificate
        ca_private_key = self.state.cluster.internal_ca_private_key
        if ca_cert is None or ca_private_key is None:
            raise ValkeyWorkloadCommandError("Internal CA certificate or private key is missing")

        cert = Certificate.generate(
            csr=certificate_signing_request,
            ca=ca_cert,
            ca_private_key=ca_private_key,
            validity=timedelta(days=10950),
            is_ca=False,
        )

        logger.info("Generated new self-signed certificate")
        return cert

    def create_and_store_self_signed_certificate(self) -> None:
        """Generate certificate files and store them for peer-TLS by default."""
        private_key = self._generate_private_key()
        certificate = self._generate_self_signed_certificate(private_key)
        ca_cert = self.state.cluster.internal_ca_certificate
        if ca_cert is None:
            raise ValkeyWorkloadCommandError("Internal CA certificate is missing")

        self.workload.make_dir(self.workload.tls_dir, exist_ok=True)
        self.workload.make_dir(self.workload.tls_paths.ca_certs_dir, exist_ok=True)

        self.workload.write_file(private_key.raw, self.workload.tls_paths.client_key)
        self.workload.write_file(certificate.raw, self.workload.tls_paths.client_cert)
        self.workload.write_file(ca_cert.raw, self.workload.tls_paths.client_ca)
        self.rehash_ca_certificates()

    def will_certificate_expire(self) -> bool:
        """Check if the certificates installed on the unit will soon expire.

        Returns:
            True if certificate expires in less than 24h, False if not.
        """
        for cert_file in [
            self.workload.tls_paths.client_cert.as_posix(),
            self.workload.tls_paths.client_ca.as_posix(),
        ]:
            try:
                self.workload.exec(
                    [
                        "openssl",
                        "x509",
                        "-checkend",
                        "86400",
                        "-noout",
                        "-in",
                        cert_file,
                    ]
                )
            except ValkeyWorkloadCommandError:
                logger.warning("Certificate will expire in less than 24h")
                self.state.unit_server.update({"tls_certificate_expiring": True})
                return True

        if self.state.unit_server.model.tls_certificate_expiring:
            self.state.unit_server.update({"tls_certificate_expiring": False})
        return False

    def start_ca_rotation_if_required(
        self, certificate: ProviderCertificate | None = None
    ) -> bool:
        """Check a certificate if the CA is new and if so, start the CA rotation on this unit.

        Args:
            certificate (ProviderCertificate): The certificate to check. If not given,
                the internal CA cert from the peer relation will be used.

        Returns:
            True if CA rotation was started, False if not
        """
        if self.state.unit_server.tls_ca_rotation_state != TLSCARotationState.NO_ROTATION:
            # safeguard in case another new certificate arrives during a CA rotation
            logger.debug("CA rotation already in progress")
            return True

        if self.state.unit_server.tls_client_state == TLSState.TO_TLS:
            return False

        if certificate:
            ca_cert = certificate.ca
        else:
            ca_cert = self.state.cluster.internal_ca_certificate
        if ca_cert is None:
            raise ValkeyWorkloadCommandError("Internal CA certificate is missing")
        current_ca_cert = self.workload.read_file(self.workload.tls_paths.client_ca)

        if ca_cert.raw == current_ca_cert:
            logger.debug("No CA rotation, CA cert is up-to-date")
            return False

        logger.info("New CA certificate detected")
        self.workload.write_file(
            current_ca_cert, self.workload.tls_paths.client_ca.with_name("old_client_ca.pem")
        )
        return True

    def extra_sans_config_is_valid(self) -> bool:
        """Validate configuration value for certificate-extra-sans option.

        Returns:
            bool: True if config value is valid, False if invalid.
        """
        if not (extra_sans_config := self.state.config.get("certificate-extra-sans")):
            return True

        extra_sans = [san.strip() for san in extra_sans_config.split(",")]  # pyright: ignore[reportAttributeAccessIssue]

        for san in extra_sans:
            if not self._is_ip_address(san) and not self._is_hostname(
                san.replace("{unit}", str(self.state.unit_server.unit_id))
            ):
                logger.error(f"certificate-extra-sans configuration is invalid for {san}")
                return False

        return True

    def _is_ip_address(self, input_value: str) -> bool:
        """Validate a given str and return True if it is an IP address, False if not."""
        try:
            ip_address(input_value)
            return True
        except ValueError:
            return False

    def _is_hostname(self, input_value: str) -> bool:
        """Validate a given str and return True if it is a hostname, False if not."""
        try:
            # Hostname string may only be hyphens and alpha-numerals.
            # `validators.hostname` returns `True` or a falsy `ValidationError`.
            return bool(
                hostname(
                    input_value,
                    skip_ipv4_addr=True,
                    skip_ipv6_addr=True,
                    may_have_port=False,
                    maybe_simple=True,
                )
            )
        except ValidationError:
            return False

    def get_current_sans(self) -> dict[str, set[str]]:
        """Get the current SANs for a unit's cert."""
        cert_file = self.workload.tls_paths.client_cert

        sans_ip = set()
        sans_dns = set()
        if not (
            san_lines := self.workload.exec(
                [
                    "openssl",
                    "x509",
                    "-ext",
                    "subjectAltName",
                    "-noout",
                    "-in",
                    cert_file.as_posix(),
                ]
            )[0].splitlines()
        ):
            return {"sans_ip": sans_ip, "sans_dns": sans_dns}

        for line in san_lines:
            for sans in line.split(", "):
                san_type, san_value = sans.split(":")

                if san_type.strip() == "DNS":
                    sans_dns.add(san_value)
                if san_type.strip() == "IP Address":
                    sans_ip.add(san_value)

        return {"sans_ip": sans_ip, "sans_dns": sans_dns}

    def certificate_sans_require_update(self) -> bool:
        """Check current certificate sans and determine if certificate requires update.

        Returns:
            bool: True if certificate sans have changed, False if they are still the same.
        """
        current_sans = self.get_current_sans()
        new_sans_ip = self.build_sans_ip()
        new_sans_dns = self.build_sans_dns()

        if new_sans_ip ^ current_sans["sans_ip"] or new_sans_dns ^ current_sans["sans_dns"]:
            return True

        return False

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:  # noqa: C901
        """Compute the TLS statuses."""
        status_list: list[StatusObject] = []

        # Peer relation not established yet, or model not built yet for unit or app
        if not self.state.cluster.model or not self.state.unit_server.model:
            return status_list or [CharmStatuses.ACTIVE_IDLE.value]

        if (relation := self.state.client_tls_relation) and relation.data[relation.app].get(
            "request_errors"
        ):
            status_list.append(TLSStatuses.CERTIFICATE_DENIED.value)

        if self.state.unit_server.tls_client_state == TLSState.TO_TLS:
            status_list.append(TLSStatuses.ENABLING_CLIENT_TLS.value)

        if (
            self.state.unit_server.is_started
            and self.state.unit_server.tls_client_state == TLSState.TLS
            and not self.state.client_tls_relation
        ):
            status_list.append(TLSStatuses.DISABLING_CLIENT_TLS_FAILED.value)

        if (
            private_key_id := self.state.config.get(TLS_CLIENT_PRIVATE_KEY_CONFIG)
        ) and self.read_and_validate_private_key(str(private_key_id)) is None:
            status_list.append(TLSStatuses.PRIVATE_KEY_INVALID.value)

        if self.state.cluster.tls_client_private_key and not self.state.client_tls_relation:
            status_list.append(TLSStatuses.PRIVATE_KEY_BUT_NO_TLS.value)

        if self.state.unit_server.tls_client_state == TLSState.TO_NO_TLS:
            status_list.append(TLSStatuses.DISABLING_CLIENT_TLS.value)

        match self.state.unit_server.tls_ca_rotation_state:
            case TLSCARotationState.NEW_CA_DETECTED:
                status_list.append(TLSStatuses.CA_ROTATION_DETECTED.value)
            case TLSCARotationState.NEW_CA_ADDED:
                status_list.append(TLSStatuses.CA_ROTATION_CA_ADDED.value)
            case TLSCARotationState.CA_UPDATED:
                status_list.append(TLSStatuses.CA_ROTATION_UPDATED.value)

        if self.state.unit_server.model.tls_certificate_expiring:
            status_list.append(TLSStatuses.CERTIFICATE_EXPIRING.value)

        if not self.extra_sans_config_is_valid():
            status_list.append(TLSStatuses.SANS_CONFIG_INVALID.value)

        return status_list if status_list else [CharmStatuses.ACTIVE_IDLE.value]
