#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for authentication and authorization."""

import hashlib
import logging
import secrets
import ssl
import string
from pathlib import Path

import ldap3
import ldap3.core.exceptions
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope

from common.exceptions import ValkeyConfigurationError, ValkeyWorkloadCommandError
from core.base_workload import WorkloadBase
from core.cluster_state import ClusterState
from literals import CHARM_USERS_ROLE_MAP, CharmUsers, StartState
from statuses import AuthStatuses, CharmStatuses

logger = logging.getLogger(__name__)

WORKING_DIR = Path(__file__).absolute().parent


class AuthManager(ManagerStatusProtocol):
    """Manage authentication and authorization."""

    name: str = "auth"
    state: ClusterState

    def __init__(self, state: ClusterState, workload: WorkloadBase):
        self.state = state  # pyright: ignore[reportIncompatibleVariableOverride]
        self.workload = workload

    def set_acl_file(self, passwords: dict[str, str] | None = None) -> None:
        """Write the ACL file with appropriate user permissions.

        Args:
            passwords (dict[str, str] | None): Optional dictionary of passwords to use. If not provided,
                the passwords from the cluster state will be used.
        """
        logger.debug("Writing ACL configuration")
        acl_content = "user default off\n"

        # charm internal users
        for user in CharmUsers:
            # only process VALKEY users
            # Sentinel users should be in the sentinel acl file
            if "VALKEY_" not in user.name:
                continue
            acl_content += self._get_internal_user_acl_line(user, passwords=passwords)

        # users for external client relations
        acl_content += self._get_client_user_acl_lines()

        # users for LDAP
        acl_content += self._get_ldap_user_acl_lines()

        self.workload.write_file(
            acl_content,
            self.workload.acl_file,
            user=self.workload.user,
            group=self.workload.user,
        )

    def _get_ldap_connection(self) -> ldap3.Connection:
        """Get a connection to the related LDAP provider."""
        ldap_urls = (
            self.state.ldap.ldaps_urls if self.state.ldap.ldaps_urls else self.state.ldap.urls
        )
        ldap_ca_cert = self.workload.read_file(self.workload.tls_paths.ldap_ca)

        tls_context = ldap3.Tls(
            ciphers="ALL",
            version=ssl.PROTOCOL_TLS_CLIENT,
            validate=ssl.CERT_REQUIRED,
            ca_certs_data=ldap_ca_cert,
        )
        ldap_server = ldap3.Server(host=ldap_urls[0], use_ssl=True, tls=tls_context)

        # no need for error handling, already present in state validator
        ldap_bind_password = self.state.get_secret_from_id(
            self.state.ldap.bind_password_secret
        ).get("password")

        return ldap3.Connection(
            server=ldap_server,
            user=self.state.ldap.bind_dn,
            password=ldap_bind_password,
            read_only=True,
            auto_bind=True,
        )

    def _get_internal_user_acl_line(
        self, user: CharmUsers, passwords: dict[str, str] | None = None
    ) -> str:
        """Generate an ACL line for a given internal user.

        Args:
            user (CharmUsers): Internal User for which to generate the ACL line.
            passwords (dict[str, str] | None): Optional dictionary of passwords to use. If not provided,
                the passwords from the cluster state will be used.

        Returns:
            str: ACL line for the internal user.
        """
        passwords = passwords or self.state.cluster.internal_users_credentials
        if not (password := passwords.get(user.value, "")):
            raise ValueError(f"No password found for user {user}")
        password_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
        return f"user {user.value} on #{password_hash} {CHARM_USERS_ROLE_MAP[user]}\n"

    def _get_client_user_acl_lines(self, for_sentinel: bool = False) -> str:
        """Generate the ACL lines for all external client users.

        Returns:
            str: ACL lines for the external client users.
        """
        valkey_base_permissions = (
            "-@all +@read +@write +@keyspace -migrate +@pubsub +@transaction +info +ping +role "
        )
        valkey_scripting_permissions = "+eval +evalsha +@scripting "
        valkey_client_caching_permissions = "+client|id +client|tracking "
        sentinel_base_permissions = "-@all +auth +client +command +hello +ping +role "
        sentinel_sentinel_permissions = "+sentinel|get-master-addr-by-name +sentinel|master +sentinel|masters +sentinel|replicas +sentinel|sentinels"
        acl_content = ""

        if not (external_client_users := self.state.cluster.external_users_credentials):
            return acl_content

        for username, values in external_client_users.items():
            user_specific_permissions = f"~{values['resource']} &{values['resource']}"
            permissions = (
                valkey_base_permissions
                + valkey_client_caching_permissions
                + valkey_scripting_permissions
                + user_specific_permissions
            )
            if for_sentinel:
                permissions = sentinel_base_permissions + sentinel_sentinel_permissions
            password_hash = hashlib.sha256(values["password"].encode("utf-8")).hexdigest()
            acl_content += f"user {username} on #{password_hash} {permissions}\n"

        return acl_content

    def _get_ldap_user_acl_lines(self) -> str:
        """Generate the ACL lines for LDAP users.

        Gets all configured LDAP groups, queries the LDAP provider for the users per group and
        creates an ACL line with the configured group permissions for each user.

        Returns:
            str: ACL lines for the LDAP users.
        """
        acl_content = ""
        if not self.state.is_ldap_valid:
            return acl_content

        self.state.unit_server.update({"ldap-sync-failed": False})

        # get non-LDAP users to avoid adding duplicate usernames to ACL files
        internal_users = [user.value for user in CharmUsers]
        client_users = []
        if external_client_users := self.state.cluster.external_users_credentials:
            client_users = list(external_client_users.keys())

        # get configured LDAP groups and generate ACL lines for the LDAP members of these groups
        base_auth_rule = "-@all +info +ping"
        for group, permissions in self._resolve_ldap_group_permissions().items():
            group_auth_rule = " ".join(permission for permission in permissions)
            ldap_users = self._get_ldap_users_for_group(group)
            for username in ldap_users:
                # safeguards to avoid inconsistent ACL files
                if (
                    username in internal_users
                    or username in client_users
                    or f" {username} " in acl_content
                ):
                    continue

                acl_content += f"user {username} on {base_auth_rule} {group_auth_rule}\n"

        return acl_content

    def _resolve_ldap_group_permissions(self) -> dict[str, list[str]]:
        """Match the configured LDAP groups from `ldap-map` to requested entity-permissions.

        Return:
            A dict with the list of privileges per configured LDAP group.
        """
        ldap_maps = str(self.state.config.get("ldap-map", "")).split(",")
        ldap_group_permissions = {}

        for mapping in ldap_maps:
            ldap_group, role = mapping.strip().split(":")
            for entity_permission in self.state.requested_entity_permissions:
                if entity_permission.resource_name == role:
                    ldap_group_permissions[ldap_group] = entity_permission.privileges
                    break

        return ldap_group_permissions

    def _get_ldap_users_for_group(self, ldap_group: str) -> set[str]:
        """Query the related LDAP provider and get all users for an LDAP group."""
        ldap_users = set()

        try:
            ldap_connection = self._get_ldap_connection()
        except ldap3.core.exceptions.LDAPException as e:
            logger.error("Could not get LDAP connection: %s", e)
            self.state.unit_server.update({"ldap-sync-failed": True})
            return ldap_users

        base_dn = self.state.ldap.base_dn
        search_attribute = str(self.state.config.get("ldap-search-attribute", ""))
        search_filter = str(self.state.config.get("ldap-search-filter", ""))

        try:
            if not ldap_connection.search(
                search_base=base_dn,
                search_filter=f"(&({search_filter})(memberOf=ou={ldap_group},*))",
                attributes=search_attribute,
                time_limit=10,
            ):
                logger.error(
                    "Failed to perform LDAP search with base dn %s, filter %s, group %s and attribute %s",
                    base_dn,
                    search_filter,
                    ldap_group,
                    search_attribute,
                )
                self.state.unit_server.update({"ldap-sync-failed": True})
                return ldap_users
        except ldap3.core.exceptions.LDAPException as e:
            logger.error("Could not get LDAP connection: %s", e)
            self.state.unit_server.update({"ldap-sync-failed": True})
            return ldap_users

        for entry in ldap_connection.entries:
            ldap_users.add(entry[search_attribute].value)

        return ldap_users

    def set_sentinel_acl_file(self, passwords: dict[str, str] | None = None) -> None:
        """Write the Sentinel ACL file with appropriate user permissions.

        Args:
            passwords (dict[str, str] | None): Optional dictionary of passwords to use. If not provided,
                the passwords from the cluster state will be used.
        """
        logger.debug("Writing Sentinel ACL configuration")
        acl_content = "user default off\n"
        for user in CharmUsers:
            # only process VALKEY users
            # Sentinel users should be in the sentinel acl file
            if "VALKEY_" in user.name:
                continue
            acl_content += self._get_internal_user_acl_line(user, passwords=passwords)
        acl_content += self._get_client_user_acl_lines(for_sentinel=True)
        self.workload.write_file(
            acl_content,
            self.workload.sentinel_acl_file,
            user=self.workload.user,
            group=self.workload.user,
        )

    def generate_password(self) -> str:
        """Create randomized string for use as app passwords.

        Returns:
            str: String of 32 randomized letter+digit characters
        """
        return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])

    def update_local_valkey_admin_password(self) -> None:
        """Update the local unit's valkey admin password in the state."""
        self.state.unit_server.update(
            {
                "charmed_operator_password_local_unit_copy": self.state.cluster.internal_users_credentials.get(
                    CharmUsers.VALKEY_ADMIN.value
                )
            }
        )

    def configure_auth(self) -> None:
        """Configure ACL files.

        Raises:
            ValkeyConfigurationError: If there was an error during configuration.
        """
        try:
            self.update_local_valkey_admin_password()
            self.set_acl_file()
            self.set_sentinel_acl_file()
        except (ValkeyWorkloadCommandError, ValueError) as e:
            logger.error("Failed to set configuration properties: %s", e)
            self.state.unit_server.update(
                {"start_state": StartState.CONFIGURATION_ERROR.value, "request_start_lock": False}
            )
            raise ValkeyConfigurationError("Failed to set configuration") from e

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Compute the auth manager's statuses."""
        status_list: list[StatusObject] = []

        if not self.state.ldap_relation:
            return [CharmStatuses.ACTIVE_IDLE.value]

        # Peer relation not established yet, or model not built yet for unit or app
        if not self.state.cluster.model or not self.state.unit_server.model:
            return status_list or [CharmStatuses.ACTIVE_IDLE.value]

        if scope == "app":
            if not self.state.ldap_ca_cert_relation:
                status_list.append(AuthStatuses.LDAP_CA_CERT_MISSING.value)

            if not self.state.config.get("ldap-map"):
                status_list.append(AuthStatuses.LDAP_MAP_CONFIG_MISSING.value)

            if not self.state.requested_entity_permissions:
                status_list.append(AuthStatuses.LDAP_MAP_INTEGRATION_MISSING.value)

            if not self.state.is_ldap_permission_config_valid:
                status_list.append(AuthStatuses.LDAP_MAP_PERMISSION_REQUEST_MISSING.value)

            return status_list if status_list else [CharmStatuses.ACTIVE_IDLE.value]

        if not self.state.unit_server.is_ldap_enabled:
            status_list.append(AuthStatuses.LDAP_NOT_ENABLED.value)

        if (
            self.state.unit_server.model.ldap_user_epoch < self.state.cluster.model.ldap_user_epoch
            or self.state.unit_server.model.ldap_sync_failed
        ):
            status_list.append(AuthStatuses.LDAP_USER_SYNC_FAILED.value)

        return status_list if status_list else [CharmStatuses.ACTIVE_IDLE.value]
