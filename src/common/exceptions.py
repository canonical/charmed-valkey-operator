# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm-specific exceptions."""


class ValkeyClientError(Exception):
    """Custom Exception if user could not be added or updated in Valkey cluster."""


class ValkeyCustomCommandError(ValkeyClientError):
    """Custom Exception if a custom command fails on Valkey cluster."""


class ValkeyACLLoadError(ValkeyClientError):
    """Custom Exception if ACL file could not be loaded in Valkey cluster."""


class ValkeyTLSLoadError(ValkeyClientError):
    """Custom Exception if TLS settings could not be loaded in Valkey."""


class ValkeyConfigSetError(ValkeyClientError):
    """Custom Exception if setting configuration on valkey cluster fails."""


class ValkeyCannotGetPrimaryIPError(ValkeyClientError):
    """Custom Exception if the primary IP cannot be determined from the sentinels."""


class ValkeyWorkloadCommandError(Exception):
    """Custom Exception if any workload-related command fails."""


class ValkeyServicesFailedToStartError(Exception):
    """Custom Exception if Valkey service fails to start."""


class ValkeyServiceNotAliveError(Exception):
    """Custom Exception if Valkey service is not alive after start."""


class ValkeyConfigurationError(Exception):
    """Custom Exception if Valkey configuration fails to be set."""


class ValkeyClusterNotReadyError(Exception):
    """Raised when the cluster does not reach a ready state within a bounded wait."""


class SentinelFailoverError(Exception):
    """Custom Exception if triggering sentinel failover fails."""


class ValkeyServicesCouldNotBeStoppedError(Exception):
    """Custom Exception if Valkey services could not be stopped."""


class CannotSeeAllActiveSentinelsError(Exception):
    """Custom Exception if the local sentinel cannot see all active sentinels in the cluster."""


class SentinelIncorrectReplicaCountError(Exception):
    """Custom Exception if the sentinel sees an incorrect number of replicas."""


class NotAllDepartingSentinelsStoppedError(Exception):
    """Custom Exception if not all departing Sentinels have already been stopped."""


class RequestingLockTimedOutError(Exception):
    """Custom Exception if requesting a lock times out."""


class ValkeyCertificatesNotReadyError(Exception):
    """Custom Exception if not all units have stored the TLS certificates."""


class KubernetesClientError(Exception):
    """Custom Exception if a connection to the Kubernetes Cluster API fails."""


class StorageBackendError(Exception):
    """Backend-neutral remote-object-store failure.

    Raised by every ``StorageBackend`` implementation so callers never have to
    catch an SDK-specific exception; ``safe_code`` is the backend's structured
    error code, forwarded onto the backup/restore error the manager raises.
    """

    def __init__(self, message: str, *, safe_code: str | None = None):
        super().__init__(message)
        self.safe_code = safe_code


class ValkeyBackupError(Exception):
    """Raised when a backup operation fails.

    ``safe_code`` is the structured error code from the failing object-storage
    call (e.g. "AccessDenied"), safe to put in a world-readable action result;
    the message itself is not (it carries endpoints and request ids).
    """

    def __init__(self, *args: object, safe_code: str | None = None):
        super().__init__(*args)
        self.safe_code = safe_code


class ValkeyBackupInProgressError(ValkeyBackupError):
    """Raised to block unit teardown while a backup is still streaming.

    Raising from ``storage-detaching`` errors the hook so Juju retries it
    until the backup finishes; a plain ``return`` would not hold teardown.
    """


class ValkeyRestoreError(Exception):
    """Raised when a restore operation fails.

    Carries ``safe_code`` for the same reason as ``ValkeyBackupError``.
    """

    def __init__(self, *args: object, safe_code: str | None = None):
        super().__init__(*args)
        self.safe_code = safe_code
