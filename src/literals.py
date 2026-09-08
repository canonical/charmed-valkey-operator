#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of global literals for the Valkey charm."""

from enum import StrEnum

CHARM = "valkey"
CONTAINER = "valkey"

SNAP_NAME = "valkey-charmed"
SNAP_REVISIONS = {"x86_64": 141, "aarch64": 143}
SNAP_SERVICE = "server"
SNAP_SENTINEL_SERVICE = "sentinel"
SNAP_COMMON_PATH = f"var/snap/{SNAP_NAME}/common"
SNAP_CURRENT_PATH = f"var/snap/{SNAP_NAME}/current"
SNAP_CONFIG_FILE = "etc/valkey/valkey.conf"
SNAP_SENTINEL_CONFIG_FILE = "etc/valkey/sentinel.conf"
SNAP_ACL_FILE = "etc/valkey/users.acl"
SNAP_SENTINEL_ACL_FILE = "etc/valkey/sentinel-users.acl"

CONFIG_FILE = "var/lib/valkey/valkey.conf"
SENTINEL_CONFIG_FILE = "var/lib/valkey/sentinel.conf"
ACL_FILE = "var/lib/valkey/users.acl"
SENTINEL_ACL_FILE = "var/lib/valkey/sentinel-users.acl"

TOPOLOGY_OBSERVER_LOG_FILENAME = "topology_observer.log"
TOPOLOGY_OBSERVER_TLS_CA_FILENAME = "valkey_ca.pem"
TOPOLOGY_OBSERVER_PID_FILENAME = "topology_observer.pid"

PEER_RELATION = "valkey-peers"
STATUS_PEERS_RELATION = "status-peers"
CLIENT_TLS_RELATION_NAME = "client-certificates"
CERTIFICATE_TRANSFER_RELATION = "certificate-transfer"
LDAP_CA_CERT_RELATION = "ldap-ca-cert"
LDAP_RELATION = "ldap"
EXTERNAL_CLIENTS_RELATION = "valkey-client"
S3_RELATION_NAME = "s3-credentials"
AZURE_RELATION_NAME = "azure-credentials"
# azure-storage-integrator connection-protocol values that designate an https/http
# Blob endpoint. abfs/abfss designate ADLS-Gen2 (*.dfs.*), served by the datalake
# SDK rather than the Blob SDK, so they are rejected up front.
AZURE_HTTPS_PROTOCOLS = frozenset({"https", "wasbs"})
AZURE_HTTP_PROTOCOLS = frozenset({"http", "wasb"})
AZURE_REJECTED_PROTOCOLS = frozenset({"abfs", "abfss"})
# Every backup backend: its integrator relation -> the app databag field its
# validated credentials envelope is stored under. Relation discovery, the
# mutual-exclusion check, credential selection and the action guards all read
# this, so a new backend is one entry here plus its model, backend and wiring.
BACKUP_CREDENTIAL_FIELDS: dict[str, str] = {
    S3_RELATION_NAME: "s3_credentials",
    AZURE_RELATION_NAME: "azure_credentials",
}
BACKUP_ID_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# Structured code create-backup reports when an object already occupies the
# generated id. Ids have one-second resolution, so two units starting a backup
# in the same second would otherwise replace each other's snapshot.
BACKUP_EXISTS_CODE = "BackupAlreadyExists"
BACKUP_CA_FILENAME = "s3_ca_chain.pem"
PRE_RESTORE_SUFFIX = ".pre-restore"
# Normal down-after (rendered by ConfigManager, reasserted by resume_failover);
# suppression raises it to a day so the primary's restart isn't seen as a failure.
SENTINEL_DOWN_AFTER_MS = 30000
SENTINEL_DOWN_AFTER_SUPPRESSED_MS = 86_400_000
# Bounded waits: big enough for a large RDB load/resync, small enough that a stuck
# restore self-terminates (freeing the cluster-wide hold) fast. Raise for bigger data.
RESTORE_LOAD_TIMEOUT_S = 600
RESTORE_RESYNC_TIMEOUT_S = 900

CLIENT_PORT = 6379
TLS_PORT = 6380
SENTINEL_PORT = 26379
SENTINEL_TLS_PORT = 26380

PRIMARY_NAME = "primary"
QUORUM_NUMBER = 2
INTERNAL_USERS_PASSWORD_CONFIG = "system-users"
INTERNAL_USERS_SECRET_LABEL_SUFFIX = "internal_users_secret"
CLIENTS_USERS_SECRET_LABEL_SUFFIX = "client_users_secret"
INTERNAL_CERTS_SECRET_LABEL_SUFFIX = "internal_certificates_secret"
TLS_CLIENT_PRIVATE_KEY_CONFIG = "tls-client-private-key"

DATA_STORAGE = "data"
DATA_STORAGE_PATH = "var/lib/valkey"
LOG_STORAGE = "logs"
LOG_STORAGE_PATH = "var/log/valkey"
ARCHIVE_STORAGE = "archive"
ARCHIVE_STORAGE_PATH = "var/backups/valkey"

VALKEY_LOG_FILE = "valkey.log"
SENTINEL_LOG_FILE = "sentinel.log"


# As per the valkey users spec
# https://docs.google.com/document/d/1EImKKHK3wLY73-D1M2ItpHe88NHeB-Iq2M3lz7AQB7E
class CharmUsers(StrEnum):
    """Enumeration of Valkey charm users."""

    VALKEY_ADMIN = "charmed-operator"
    VALKEY_SENTINEL = "charmed-sentinel-valkey"
    VALKEY_REPLICA = "charmed-replication"
    VALKEY_MONITORING = "charmed-stats"

    # Sentinel users
    SENTINEL_ADMIN = "charmed-sentinel-peers"
    SENTINEL_CHARM_ADMIN = "charmed-sentinel-operator"


CHARM_USERS_ROLE_MAP = {
    CharmUsers.VALKEY_ADMIN: "~* +@all",
    CharmUsers.VALKEY_SENTINEL: "+subscribe +publish +failover +script|kill +ping +info +multi +slaveof +config +client +exec &__sentinel__:hello",
    CharmUsers.VALKEY_REPLICA: "+psync +replconf +ping",
    CharmUsers.VALKEY_MONITORING: "-@all +@connection +memory -readonly +strlen +config|get +xinfo +pfcount -quit +zcard +type +xlen -readwrite -command +client -wait +scard +llen +hlen +get +eval +slowlog +cluster|info +cluster|slots +cluster|nodes -hello -echo +info +latency +scan -reset -auth -asking",
    CharmUsers.SENTINEL_ADMIN: "~* +@all",
    CharmUsers.SENTINEL_CHARM_ADMIN: "~* +@all",
}


class Substrate(StrEnum):
    """Substrate types."""

    VM = "vm"
    K8S = "k8s"


class StartState(StrEnum):
    """Start states for the service."""

    NOT_STARTED = "not_started"
    WAITING_TO_START = "waiting_to_start"
    WAITING_FOR_PRIMARY_START = "waiting_for_primary_start"
    CONFIGURATION_ERROR = "configuration_error"
    STARTING_WAITING_VALKEY = "starting_waiting_valkey"
    STARTING_WAITING_SENTINEL = "starting_waiting_sentinel"
    STARTING_WAITING_REPLICA_SYNC = "starting_waiting_replica_sync"
    ERROR_ON_START = "error_on_start"
    STARTED = "started"


class RestoreStep(StrEnum):
    """Steps of the restore coordination workflow (ordered).

    RESTORE is one fused barrier (primary suppresses failover, downloads, swaps
    in the RDB); download and restore aren't split since no unit has work between.
    """

    NOT_STARTED = ""
    RESTORE = "restore"
    RESYNC = "resync"
    COMPLETED = "completed"


class RestoreFailure(StrEnum):
    """Per-unit restore failure kind, recorded on the failing unit's databag.

    UNHEALTHY marks a cluster that never became ready (surfaces RESTORE_UNHEALTHY);
    FAILED is any other failure.
    """

    FAILED = "failed"
    UNHEALTHY = "unhealthy"


class ScaleDownState(StrEnum):
    """Scale down states for the service."""

    NO_SCALE_DOWN = ""
    WAIT_FOR_LOCK = "wait_for_lock"
    WAIT_TO_FAILOVER = "wait_to_failover"
    STOP_SERVICES = "stopping_services"
    RESET_SENTINEL = "reset_sentinel"
    HEALTH_CHECK = "health_check"
    GOING_AWAY = "going_away"


class TLSState(StrEnum):
    """TLS states."""

    NO_TLS = "no-tls"
    TO_TLS = "to-tls"
    TLS = "tls"
    TO_NO_TLS = "to-no-tls"


class TLSCARotationState(StrEnum):
    """TLS CA Rotation state."""

    NO_ROTATION = "no-rotation"
    NEW_CA_DETECTED = "new-ca-detected"
    NEW_CA_ADDED = "new-ca-added"
    CA_UPDATED = "ca-updated"


class K8sService(StrEnum):
    """Services managed by the charm in Kubernetes."""

    PRIMARY = "primary"
    REPLICAS = "replicas"
