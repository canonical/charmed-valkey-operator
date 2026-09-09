#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Objects representing the cluster state of Valkey."""

import logging

import ops
from data_platform_helpers.advanced_statuses.components import StatusesState
from data_platform_helpers.advanced_statuses.protocol import StatusesStateProtocol
from dpcharmlibs.interfaces import (
    EntityPermissionModel,
    OpsOtherPeerUnitRepositoryInterface,
    OpsPeerRepositoryInterface,
    OpsPeerUnitRepositoryInterface,
    OpsRelationRepository,
)

from core.models import (
    BackupCredentials,
    LDAPState,
    PeerAppModel,
    PeerUnitModel,
    ValkeyCluster,
    ValkeyServer,
)
from literals import (
    BACKUP_CREDENTIAL_FIELDS,
    CLIENT_TLS_RELATION_NAME,
    EXTERNAL_CLIENTS_RELATION,
    LDAP_CA_CERT_RELATION,
    LDAP_RELATION,
    PEER_RELATION,
    S3_RELATION_NAME,
    STATUS_PEERS_RELATION,
    RestoreFailure,
    Substrate,
)

logger = logging.getLogger(__name__)


class ClusterState(ops.Object, StatusesStateProtocol):
    """Global state object for the Valkey cluster."""

    def __init__(self, charm: ops.CharmBase, substrate: Substrate):
        super().__init__(parent=charm, key="charm_state")
        self.charm = charm
        self.peer_app_interface = OpsPeerRepositoryInterface(
            model=charm.model, relation_name=PEER_RELATION, data_model=PeerAppModel
        )
        self.peer_unit_interface = OpsPeerUnitRepositoryInterface(
            model=charm.model, relation_name=PEER_RELATION, data_model=PeerUnitModel
        )
        self.statuses_relation_name = STATUS_PEERS_RELATION
        self.statuses = StatusesState(self, self.statuses_relation_name)
        self.config = charm.config
        self.substrate = substrate

    @property
    def peer_relation(self) -> ops.model.Relation | None:
        """Get the Valkey peer relation."""
        return self.model.get_relation(PEER_RELATION)

    @property
    def s3_relation(self) -> ops.model.Relation | None:
        """Get the S3 credentials relation, if any."""
        return self.model.get_relation(S3_RELATION_NAME)

    @property
    def backup_relations(self) -> list[ops.model.Relation]:
        """Every related backup-storage integrator relation, per the registry."""
        return [
            relation
            for name in BACKUP_CREDENTIAL_FIELDS
            if (relation := self.model.get_relation(name)) is not None
        ]

    @property
    def backup_backends_conflict(self) -> bool:
        """True when more than one backup backend is related.

        They are mutually exclusive: with two related there is no single answer
        to "where do backups go", so the charm refuses rather than guessing.
        """
        return len(self.backup_relations) > 1

    @property
    def active_backup_credentials(self) -> BackupCredentials | None:
        """Stored credentials for the single related backup backend, else None.

        Gated on the relation, not just the databag: the leader clears the stored
        envelope on relation-broken, and until it does, a peer must not keep using
        credentials for a backend nobody is related to any more. None, too, when
        the related backend has nothing stored yet or when several are related.
        """
        if self.backup_backends_conflict:
            return None
        for name, field in BACKUP_CREDENTIAL_FIELDS.items():
            if self.model.get_relation(name) is not None:
                return getattr(self.cluster, field)
        return None

    @property
    def peer_units_data_interfaces(
        self,
    ) -> dict[ops.model.Unit, OpsOtherPeerUnitRepositoryInterface[PeerUnitModel]]:
        """Get unit data interface of all peer units from the Valkey peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        return {
            unit: OpsOtherPeerUnitRepositoryInterface(
                model=self.charm.model,
                relation_name=PEER_RELATION,
                unit=unit,
                data_model=PeerUnitModel,
            )
            for unit in self.peer_relation.units
        }

    @property
    def unit_server(self) -> ValkeyServer:
        """Get the server state of this unit."""
        return ValkeyServer(
            relation=self.peer_relation,
            data_interface=self.peer_unit_interface,
            component=self.model.unit,
        )

    @property
    def cluster(self) -> ValkeyCluster:
        """Get the cluster state of the entire Valkey application."""
        return ValkeyCluster(
            relation=self.peer_relation,
            data_interface=self.peer_app_interface,
            component=self.model.app,
        )

    @property
    def servers(self) -> set[ValkeyServer]:
        """Get all servers/units in the current peer relation, including this unit itself.

        Returns:
            Set of ValkeyServers with their unit data.
        """
        if not self.peer_relation:
            return set()

        servers = set()
        for unit, data_interface in self.peer_units_data_interfaces.items():
            servers.add(
                ValkeyServer(
                    relation=self.peer_relation,
                    data_interface=data_interface,
                    component=unit,
                )
            )
        servers.add(self.unit_server)

        return servers

    @property
    def client_tls_relation(self) -> ops.Relation | None:
        """Get the client certificates relation."""
        return self.model.get_relation(CLIENT_TLS_RELATION_NAME)

    @property
    def external_client_relations(self) -> set[ops.Relation]:
        """Get the client relations."""
        return set(self.model.relations[EXTERNAL_CLIENTS_RELATION])

    @property
    def ldap_relation(self) -> ops.Relation | None:
        """Get the LDAP relation."""
        return self.model.get_relation(LDAP_RELATION)

    @property
    def ldap_ca_cert_relation(self) -> ops.Relation | None:
        """Get the certificate-transfer relation for LDAP."""
        return self.model.get_relation(LDAP_CA_CERT_RELATION)

    @property
    def bind_address(self) -> str:
        """The network binding address from the peer relation."""
        if not (
            binding := self.model.get_binding(self.peer_relation)  # pyright: ignore[reportArgumentType]
        ):
            raise ValueError

        if not (address := binding.network.bind_address):
            raise ValueError

        return str(address)

    @property
    def ingress_address(self) -> str | None:
        """The network ingress address from the peer relation."""
        if not (
            binding := self.model.get_binding(self.peer_relation)  # pyright: ignore[reportArgumentType]
        ):
            raise ValueError

        if not (address := binding.network.ingress_address):
            return None

        return str(address)

    @property
    def hostname(self) -> str:
        """The hostname of the unit."""
        return self.get_unit_hostname(self.model.unit.name)

    @property
    def endpoint(self) -> str:
        """The endpoint to be used by other units to connect to this unit.

        On VM-based substrates, this should be the bind address.
        On Kubernetes, this should be the fully qualified domain name of the unit.
        """
        return self.bind_address if self.substrate == Substrate.VM else self.hostname

    def get_secret_from_id(self, secret_id: str) -> dict[str, str]:
        """Resolve the given id of a Juju secret and return the content as a dict.

        Args:
            secret_id (str): The id of the secret.

        Returns:
            dict: The content of the secret.
        """
        try:
            secret_content = self.charm.model.get_secret(id=secret_id).get_content(refresh=True)
        except ops.SecretNotFoundError:
            raise ops.SecretNotFoundError(f"The secret '{secret_id}' does not exist.")
        except ops.ModelError:
            raise

        return secret_content

    def get_unit_hostname(self, unit_name: str | None = None) -> str:
        """Get the hostname.localdomain for a unit.

        Translate juju unit name to hostname.localdomain, necessary
        for correct name resolution under k8s.

        Args:
            unit_name: unit name
        Returns:
            A string representing the hostname.localdomain of the unit.
        """
        unit_name = unit_name or self.charm.unit.name
        return f"{unit_name.replace('/', '-')}.{self.charm.app.name}-endpoints"

    @property
    def number_units_started(self) -> int:
        """Return the number of units in the cluster that have their Valkey server started."""
        return len([unit for unit in self.servers if unit.model and unit.is_started])

    @property
    def can_restore_workflow_proceed(self) -> bool:
        """True only when every restore participant has reached the current step.

        Gates the leader's next advance. Fail-closed: a departed participant is
        absent from ``servers`` and counts as not reached, so the restore stalls
        rather than silently skipping it.
        """
        instruction = self.cluster.restore_instruction
        by_name = {server.unit_name: server for server in self.servers}
        for name in self.cluster.restore_participants:
            server = by_name.get(name)
            if server is None or server.restore_step != instruction:
                return False
        return True

    @property
    def restore_participant_departed(self) -> bool:
        """True if a restore participant left the peer relation.

        A departed participant can never satisfy the barrier, so the restore would
        stall forever. Membership only changes on removal, never a restart, so a
        present-but-behind unit doesn't trip this.
        """
        live = {server.unit_name for server in self.servers}
        return any(name not in live for name in self.cluster.restore_participants)

    @property
    def is_backup_in_progress_any(self) -> bool:
        """True if ANY peer unit is uploading a backup (backup_id is per-unit)."""
        return any(server.is_backup_in_progress for server in self.servers)

    @property
    def is_tls_transitioning(self) -> bool:
        """True while any unit is mid client-TLS enable/disable or a CA rotation.

        A restore restarts the primary, which would collide with the cert swap.
        """
        return any(server.is_tls_transitioning for server in self.servers)

    @property
    def failed_restore_kind(self) -> str:
        """Failure kind of any participant that failed the CURRENT attempt, or ``""``.

        Scoped to ``restore_token`` so a stale marker from a prior attempt is
        ignored. UNHEALTHY wins over FAILED so a not-ready cluster surfaces the
        more specific status.
        """
        token = self.cluster.restore_token
        participants = set(self.cluster.restore_participants)
        # restore_failure_kind is "" for a participant that didn't fail this attempt.
        kinds = {
            kind
            for server in self.servers
            if server.unit_name in participants and (kind := server.restore_failure_kind(token))
        }
        if RestoreFailure.UNHEALTHY.value in kinds:
            return RestoreFailure.UNHEALTHY.value
        return RestoreFailure.FAILED.value if kinds else ""

    @property
    def ldap(self) -> LDAPState:
        """The ldap relation state."""
        return LDAPState(
            relation=self.ldap_relation,
        )

    @property
    def is_ldap_valid(self) -> bool:
        """Validate the LDAP relations and configuration."""
        if not self.ldap_relation:
            return False

        if not self.ldap_ca_cert_relation:
            logger.warning("LDAP: Missing relation for TLS CA certificate")
            return False

        if not self.config.get("ldap-map"):
            logger.warning("LDAP: Missing config value for `ldap-map`")
            return False

        try:
            ldap_secret = self.get_secret_from_id(self.ldap.bind_password_secret)
        except (ops.ModelError, ops.SecretNotFoundError, TypeError) as e:
            logger.error("Cannot read LDAP bind secret: %s", e)
            return False

        if not ldap_secret.get("password"):
            logger.warning("Password missing in LDAP bind secret")
            return False

        if not self.is_ldap_permission_config_valid:
            logger.warning("Permission request missing for LDAP group in `ldap-map`")
            return False

        return True

    @property
    def is_ldap_permission_config_valid(self) -> bool:
        """Validate the `ldap-map` config against the `requested entity-permissions`."""
        ldap_maps = str(self.config.get("ldap-map", "")).split(",")
        requested_permissions = [p.resource_name for p in self.requested_entity_permissions]

        for mapping in ldap_maps:
            try:
                _, permission = mapping.split(":")
            except ValueError:
                return False

            if permission not in requested_permissions:
                logger.debug("No requested entity-permission for ldap-map %s", permission)
                return False

        return True

    @property
    def requested_entity_permissions(self) -> list[EntityPermissionModel]:
        """All requested entity-permissions from client relations / Data integrator."""
        entity_permissions = []

        if not self.external_client_relations:
            return entity_permissions

        for relation in self.external_client_relations:
            if not relation.app:
                continue

            repository = OpsRelationRepository(self.model, relation, relation.app)
            if not (relation_data := repository.get_data()):
                continue

            if not (requests := relation_data.get("requests")):
                # Data integrator supports valkey-client with data-interfaces v1, no need for v0
                continue

            for request in requests:
                if not (requested_permissions := request.get("entity-permissions")):
                    continue

                for permission in requested_permissions:
                    entity_permissions.append(
                        EntityPermissionModel(
                            resource_name=permission.get("resource_name"),
                            resource_type=permission.get("resource_type"),
                            privileges=permission.get("privileges"),
                        )
                    )

        return entity_permissions
