#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for all config related tasks."""

import logging
from pathlib import Path

from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope

from common.exceptions import ValkeyConfigurationError, ValkeyWorkloadCommandError
from core.base_workload import WorkloadBase
from core.cluster_state import ClusterState
from literals import (
    CLIENT_PORT,
    PRIMARY_NAME,
    SENTINEL_DOWN_AFTER_MS,
    SENTINEL_LOG_FILE,
    SENTINEL_PORT,
    SENTINEL_TLS_PORT,
    TLS_PORT,
    VALKEY_LOG_FILE,
    CharmUsers,
    StartState,
    Substrate,
    TLSState,
)
from statuses import CharmStatuses

logger = logging.getLogger(__name__)

WORKING_DIR = Path(__file__).absolute().parent

_REPL_BACKLOG_MIN_RAM_BYTES = 1 * 1024**3  # 1 GiB; strict greater-than
_REPL_BACKLOG_SIZE = "256mb"


class ConfigManager(ManagerStatusProtocol):
    """Manage configuration for Valkey and Sentinel."""

    name: str = "config"
    state: ClusterState

    def __init__(self, state: ClusterState, workload: WorkloadBase):
        # `ClusterState` satisfies `StatusesStateProtocol`; pyright flags this only
        # because the protocol declares `state` as a mutable (invariant) attribute.
        self.state = state  # pyright: ignore[reportIncompatibleVariableOverride]
        self.workload = workload

    def get_config_properties(self, primary_endpoint: str) -> dict[str, str]:
        """Assemble the config properties.

        Args:
            primary_endpoint: Endpoint of the current primary unit.

        Returns:
            Dictionary of properties to be written to the config file.
        """
        config_properties = {}
        if not self.state.unit_server.model or not self.state.cluster.model:
            return config_properties

        # load the config properties provided from the template in this repo
        # it does NOT load the file from disk in the charm unit in order to avoid config drift
        with open(f"{WORKING_DIR}/config-template/valkey.conf") as config:
            # The valkey.conf file contains a number of directives that have a very simple format:
            # keyword argument1 argument2 ... argumentN
            for line in config:
                if not line or line.startswith("#"):
                    # ignore comments and empty lines
                    continue
                try:
                    key, value = line.split(" ", 1)
                except ValueError:
                    key = line.strip()
                    value = ""
                config_properties[key.strip()] = value.strip()

        # Adjust default values
        config_properties["loglevel"] = "verbose"
        config_properties["aclfile"] = self.workload.acl_file.as_posix()
        config_properties["dir"] = self.workload.working_dir.as_posix()
        config_properties["logfile"] = (self.workload.log_dir / VALKEY_LOG_FILE).as_posix()

        config_properties["bind"] = self.state.endpoint

        # DA261 backup-safety overrides
        config_properties["repl-diskless-load"] = "on-empty-db"
        config_properties["save"] = "900 1 300 100 60 10000"
        config_properties["maxmemory-policy"] = "noeviction"
        config_properties["min-replicas-max-lag"] = "10"
        # min-replicas-to-write gates writes on having an in-sync replica. The
        # file ships 1 (the backup-safe default); ClusterManager.reconcile_min_replicas_to_write
        # relaxes it to 0 at runtime whenever fewer than 3 units are active, so
        # the primary is not write-frozen when its sole replica is unavailable
        # (e.g. during a rolling restart or an in-progress scale-up). The runtime
        # value is reasserted after every (re)start because CONFIG SET does not
        # persist.
        config_properties["min-replicas-to-write"] = "1"
        if self.workload.total_memory_bytes() > _REPL_BACKLOG_MIN_RAM_BYTES:
            config_properties["repl-backlog-size"] = _REPL_BACKLOG_SIZE

        # replica related config
        replica_config = self._generate_replica_config(primary_endpoint=primary_endpoint)
        config_properties.update(replica_config)

        # TLS related configuration
        tls_config = self.generate_tls_config()
        config_properties.update(tls_config)

        # LDAP related configuration
        config_properties["loadmodule"] = self.workload.lib_dir.as_posix() + "/libvalkey_ldap.so"
        # disable the failure detection by default because glauth does not support `WhoAmI` command
        # https://glauth.github.io/docs/limitations.html
        config_properties["ldap.failure_detector_interval"] = "9223372036854775807"
        ldap_config = self.generate_ldap_config()
        config_properties.update(ldap_config)

        # Client-side caching: limit memory consumption for key-invalidation table
        config_properties["tracking-table-max-keys"] = "1000000"

        return config_properties

    def _generate_replica_config(self, primary_endpoint: str) -> dict[str, str]:
        """Generate the config properties related to replica configuration based on the current cluster state."""
        local_unit_endpoint = self.state.endpoint
        replica_config = {
            "primaryuser": CharmUsers.VALKEY_REPLICA.value,
            "primaryauth": self.state.cluster.internal_users_credentials.get(
                CharmUsers.VALKEY_REPLICA.value, ""
            ),
            "replica-announce-ip": local_unit_endpoint,
        }
        if primary_endpoint != local_unit_endpoint:
            # set replicaof
            logger.debug("Setting replicaof to primary %s", primary_endpoint)
            # internal communication always uses peer TLS (`tls-replication=yes`)
            replica_config["replicaof"] = f"{primary_endpoint} {TLS_PORT}"
        return replica_config

    def set_config_properties(self, primary_endpoint: str) -> None:
        """Write the config properties to the config file."""
        logger.debug("Writing configuration")
        self.workload.write_config_file(
            config=self.get_config_properties(primary_endpoint=primary_endpoint)
        )

    def generate_tls_config(self) -> dict[str, str]:
        """Return the TLS configuration based on the current state."""
        tls_config = {
            "port": str(CLIENT_PORT),
            "tls-port": str(TLS_PORT),
            "tls-cert-file": self.workload.tls_paths.client_cert.as_posix(),
            "tls-key-file": self.workload.tls_paths.client_key.as_posix(),
            "tls-ca-cert-dir": self.workload.tls_paths.ca_certs_dir.as_posix(),
            "tls-replication": "yes",
            "tls-auth-clients": "optional",
            "tls-auth-clients-user": "CN",
        }

        if (
            self.state.unit_server.tls_client_state in [TLSState.TLS, TLSState.TO_TLS]
            and self.state.unit_server.model.client_cert_ready
        ):
            # if client TLS is enabled, we shut down the default port to discard non-TLS traffic
            tls_config["port"] = "0"

        return tls_config

    def generate_sentinel_tls_config(self) -> dict[str, str]:
        """Return the TLS configuration for sentinel based on the current state."""
        tls_config = {
            "port": str(SENTINEL_PORT),
            "tls-port": str(SENTINEL_TLS_PORT),
            "tls-cert-file": self.workload.tls_paths.client_cert.as_posix(),
            "tls-key-file": self.workload.tls_paths.client_key.as_posix(),
            "tls-ca-cert-dir": self.workload.tls_paths.ca_certs_dir.as_posix(),
            "tls-replication": "yes",
            "tls-auth-clients": "optional",
            "tls-auth-clients-user": "CN",
        }

        if (
            self.state.unit_server.tls_client_state in [TLSState.TLS, TLSState.TO_TLS]
            and self.state.unit_server.model.client_cert_ready
        ):
            # if client TLS is enabled, we shut down the default port to discard non-TLS traffic
            tls_config["port"] = "0"

        return tls_config

    def generate_ldap_config(self) -> dict:
        """Return the LDAP configuration for Valkey based on the current state."""
        if not self.state.is_ldap_valid:
            return {}

        # no need for error handling, already present in state validator
        ldap_bind_password = self.state.get_secret_from_id(
            self.state.ldap.bind_password_secret
        ).get("password")

        ldap_urls = (
            self.state.ldap.ldaps_urls if self.state.ldap.ldaps_urls else self.state.ldap.urls
        )
        ldap_servers = " ".join(url for url in ldap_urls)

        ldap_config = {
            "ldap.auth_mode": "search+bind",
            "ldap.servers": ldap_servers,
            "ldap.use_starttls": "no" if self.state.ldap.ldaps_urls else "yes",
            "ldap.tls_ca_cert_path": self.workload.tls_paths.ldap_ca.as_posix(),
            "ldap.search_bind_dn": self.state.ldap.bind_dn,
            "ldap.search_bind_passwd": ldap_bind_password,
            "ldap.search_base": self.state.ldap.base_dn,
            "ldap.search_attribute": self.state.config.get("ldap-search-attribute", ""),
            "ldap.search_dn_attribute": self.state.config.get("ldap-search-dn-attribute", ""),
            "ldap.search_filter": self.state.config.get("ldap-search-filter", ""),
        }

        return ldap_config

    def get_sentinel_config_properties(
        self, primary_endpoint: str
    ) -> dict[str, str | dict[str, str]]:
        """Assemble the sentinel config properties.

        Returns:
            Dictionary of sentinel properties to be written to the config file.
        """
        config_properties = {}
        if not self.state.unit_server.model or not self.state.cluster.model:
            return config_properties
        sentinel_properties = {}

        # load the config properties provided from the template in this repo
        # it does NOT load the file from disk in the charm unit in order to avoid config drift
        with open(f"{WORKING_DIR}/config-template/sentinel.conf") as config:
            # The sentinel.conf file contains a number of directives that have a simple format:
            # keyword argument1 argument2 ... argumentN
            # sentinel keyword argument1 argument2 ... argumentN
            for line in config:
                line = line.strip().lower()
                if not line or line.startswith("#"):
                    # ignore comments and empty lines
                    continue
                elif line.startswith("sentinel "):
                    try:
                        # some config options for sentinel start with "sentinel" followed by the
                        # directive and its arguments, for example "sentinel monitor mymaster
                        key, value = line.split(" ", 2)[1:]
                    except ValueError:
                        key = line.strip().split(" ", 1)[1]
                        value = ""
                    sentinel_properties[key.strip()] = value.strip().replace(
                        "mymaster", PRIMARY_NAME
                    )
                else:
                    try:
                        # other config options that are not specific to sentinel
                        # just have the format "keyword argument1 argument2 ... argumentN",
                        # for example "port 6379"
                        key, value = line.split(" ", 1)
                    except ValueError:
                        key = line.strip()
                        value = ""
                    config_properties[key.strip()] = value.strip()

        config_properties["aclfile"] = self.workload.sentinel_acl_file.as_posix()
        config_properties["logfile"] = (self.workload.log_dir / SENTINEL_LOG_FILE).as_posix()

        # sentinel configs
        config_properties["sentinel"] = sentinel_properties | self._generate_sentinel_configs(
            primary_endpoint=primary_endpoint
        )

        # tls config
        tls_config = self.generate_sentinel_tls_config()
        config_properties.update(tls_config)

        return config_properties

    def _generate_sentinel_configs(self, primary_endpoint: str) -> dict[str, str]:
        """Generate the sentinel config properties based on the current cluster state."""
        sentinel_configs = {}
        sentinel_configs["monitor"] = f"{PRIMARY_NAME} {primary_endpoint} {TLS_PORT} {self.quorum}"
        # auth settings
        # auth-user is used by sentinel to authenticate to the valkey primary
        sentinel_configs["auth-user"] = f"{PRIMARY_NAME} {CharmUsers.VALKEY_SENTINEL.value}"
        sentinel_configs["auth-pass"] = (
            f"{PRIMARY_NAME} {self.state.cluster.internal_users_credentials.get(CharmUsers.VALKEY_SENTINEL.value, '')}"
        )
        # sentinel admin user settings used by sentinel for its own authentication
        sentinel_configs["sentinel-user"] = f"{CharmUsers.SENTINEL_ADMIN.value}"
        sentinel_configs["sentinel-pass"] = (
            f"{self.state.cluster.internal_users_credentials.get(CharmUsers.SENTINEL_ADMIN.value, '')}"
        )
        sentinel_configs["down-after-milliseconds"] = f"{PRIMARY_NAME} {SENTINEL_DOWN_AFTER_MS}"
        sentinel_configs["failover-timeout"] = f"{PRIMARY_NAME} 60000"
        sentinel_configs["parallel-syncs"] = f"{PRIMARY_NAME} 1"
        if self.state.substrate == Substrate.K8S:
            sentinel_configs["resolve-hostnames"] = "yes"
            sentinel_configs["announce-hostnames"] = "yes"
            sentinel_configs["announce-ip"] = self.state.unit_server.model.hostname
        return sentinel_configs

    def set_sentinel_config_properties(self, primary_endpoint: str) -> None:
        """Write sentinel configuration file."""
        logger.debug("Writing Sentinel configuration")

        sentinel_config = self.get_sentinel_config_properties(primary_endpoint=primary_endpoint)

        sentinel_config_string = "\n".join(
            f"sentinel {key} {value}"
            for key, value in sentinel_config["sentinel"].items()  # pyright: ignore[reportAttributeAccessIssue]
        )
        other_config_string = "\n".join(
            f"{key} {value}" for key, value in sentinel_config.items() if key != "sentinel"
        )
        full_config_string = f"{other_config_string}\n{sentinel_config_string}"

        # on k8s we need to set the ownership of the sentinel config file to the non-root user that the valkey process runs as in order for sentinel to be able to read/write it
        self.workload.write_file(
            full_config_string,
            self.workload.sentinel_config_file,
            mode=0o600,
            user=self.workload.user,
            group=self.workload.user,
        )

    def configure_services(self, primary_endpoint: str) -> None:
        """Start Valkey and Sentinel services.

        Raises:
            ValkeyConfigurationError: If there was an error during configuration.
        """
        try:
            self.set_config_properties(primary_endpoint=primary_endpoint)
            self.set_sentinel_config_properties(primary_endpoint=primary_endpoint)
        except (ValkeyWorkloadCommandError, ValueError) as e:
            logger.error("Failed to set configuration properties: %s", e)
            self.state.unit_server.update(
                {"start_state": StartState.CONFIGURATION_ERROR.value, "request_start_lock": False}
            )
            raise ValkeyConfigurationError("Failed to set configuration") from e

    @property
    def quorum(self) -> int:
        """Calculate the quorum based on the number of units in the cluster."""
        num_units = len([server for server in self.state.servers if server.is_active])
        return (num_units // 2) + 1

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Compute the config manager's statuses."""
        status_list: list[StatusObject] = []

        return status_list if status_list else [CharmStatuses.ACTIVE_IDLE.value]
