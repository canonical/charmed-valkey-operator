#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Match a live Juju status message against a charm `StatusObject`.

Kept free of Juju-driving dependencies so the unit suite can cover it.
"""

import logging

from data_platform_helpers.advanced_statuses.models import StatusObject
from ops import StatusBase

logger = logging.getLogger(__name__)


def does_message_match(live_message: str, status: StatusObject) -> bool:
    """Check whether the message Juju currently shows corresponds to `status`."""
    try:
        juju_status = StatusBase.from_name(status.status, status.message)
        if live_message == juju_status.message:
            return True
        # An empty live message is the charm not having reacted yet (or `active`); every prefix
        # and substring test below would accept it, so it only matches by equality.
        if not live_message:
            return False
        return (
            live_message.startswith(juju_status.message)
            or juju_status.message.startswith(f"{live_message:.40}")
            or bool(status.short_message and status.short_message in live_message)
        )
    except KeyError as e:
        logger.error("Error attempting to convert StatusObject to ops.StatusBase: %s", e)
        return False
