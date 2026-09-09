# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Network utilities."""

import logging
import socket
from functools import cache

logger = logging.getLogger(__name__)


@cache
def resolve_k8s_fqdn(service_name: str) -> str:
    """Resolve the canonical FQDN for a Kubernetes service or pod name."""
    if not service_name:
        return ""

    try:
        info = socket.getaddrinfo(
            host=service_name,
            port=None,
            family=socket.AF_UNSPEC,
            flags=socket.AI_CANONNAME,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as e:
        logger.warning(
            "Failed to resolve canonical name for %s: %s",
            service_name,
            e,
        )
        return socket.getfqdn(service_name)

    for entry in info:
        if canonname := entry[3]:
            return canonname

    logger.warning("Failed to resolve canonical name for %s", service_name)
    return socket.getfqdn(service_name)
