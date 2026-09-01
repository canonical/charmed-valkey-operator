#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Directory provisioning for the Authentik LDAP integration test.

Authentik has no equivalent of the GLAuth `apply-ldif` action: its directory lives in the
Authentik server's database and is written through the REST API. These helpers create the groups
and users the LDAP test authenticates with, reusing the API token the Authentik server mints for
the LDAP outpost charm.
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request

import jubilant
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

from tests.integration.helpers import get_secret_by_label

logger = logging.getLogger(__name__)

API_PORT = 9000
API_TOKEN_SECRET_LABEL = "authentik-api-token"

# The Authentik LDAP outpost publishes no attribute holding an entry's DN, so `ldap-search-dn-
# attribute` has nothing to read by default. It does render a user's Authentik attributes as LDAP
# attributes, substituting `%s` with the username, which lets each user carry its own bind DN.
ENTRY_DN_ATTRIBUTE = "entryDN"
_ENTRY_DN_VALUE = "cn=%s,ou=users,{base_dn}"


class AuthentikAPIError(Exception):
    """Raised when a call to the Authentik REST API fails."""


class AuthentikDirectory:
    """An Authentik REST API client scoped to what the LDAP integration test needs."""

    def __init__(self, base_url: str, token: str):
        self._base_url = base_url.rstrip("/")
        self._token = token

    @classmethod
    def from_model(cls, juju: jubilant.Juju, app_name: str) -> "AuthentikDirectory":
        """Build a client for the Authentik server deployed in the given model."""
        status = juju.status()
        unit = next(iter(status.apps[app_name].units.values()))
        return cls(base_url=f"http://{unit.address}:{API_PORT}", token=_api_token(juju))

    def wait_until_ready(self, timeout: int = 300) -> None:
        """Block until the Authentik API answers queries."""
        for attempt in Retrying(
            stop=stop_after_delay(timeout),
            wait=wait_fixed(5),
            retry=retry_if_exception_type(AuthentikAPIError),
            reraise=True,
        ):
            with attempt:
                self._request("GET", "/core/users/?page_size=1")

    def ensure_group(self, name: str) -> str:
        """Create the group if it does not exist yet and return its primary key."""
        query = urllib.parse.urlencode({"name": name})
        for group in self._request("GET", f"/core/groups/?{query}").get("results", []):
            if group["name"] == name:
                return group["pk"]

        logger.info("Creating Authentik group %s", name)
        return self._request("POST", "/core/groups/", {"name": name})["pk"]

    def ensure_user(
        self, username: str, password: str, email: str, groups: list[str], base_dn: str
    ) -> int:
        """Create or update a user with the given group membership and set its password."""
        attributes = {ENTRY_DN_ATTRIBUTE: _ENTRY_DN_VALUE.format(base_dn=base_dn)}
        query = urllib.parse.urlencode({"username": username})
        existing = [
            user
            for user in self._request("GET", f"/core/users/?{query}").get("results", [])
            if user["username"] == username
        ]

        payload = {
            "username": username,
            "name": username,
            "email": email,
            "is_active": True,
            "groups": groups,
            "attributes": attributes,
        }
        if existing:
            logger.info("Updating Authentik user %s", username)
            pk = existing[0]["pk"]
            self._request("PATCH", f"/core/users/{pk}/", payload)
        else:
            logger.info("Creating Authentik user %s", username)
            pk = self._request("POST", "/core/users/", {**payload, "path": "users"})["pk"]

        # Passwords are never part of the user object; they are set through their own endpoint
        self._request("POST", f"/core/users/{pk}/set_password/", {"password": password})
        return pk

    def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        url = f"{self._base_url}/api/v3{path}"
        data = json.dumps(payload).encode() if payload is not None else None

        request = urllib.request.Request(url, data=data, method=method)
        request.add_header("Authorization", f"Bearer {self._token}")
        request.add_header("Accept", "application/json")
        if data is not None:
            request.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode()
        except urllib.error.HTTPError as e:
            raise AuthentikAPIError(f"{method} {path} returned {e.code}: {e.read().decode()}")
        except urllib.error.URLError as e:
            raise AuthentikAPIError(f"{method} {path} failed: {e.reason}")

        return json.loads(body) if body else {}


def provision_directory(juju: jubilant.Juju, app_name: str, entries: dict) -> None:
    """Create the groups and users described by `entries` in the Authentik directory."""
    directory = AuthentikDirectory.from_model(juju, app_name)
    directory.wait_until_ready()

    base_dn = entries["base_dn"]
    group_pks = {name: directory.ensure_group(name) for name in entries["groups"]}

    for user in entries["users"]:
        directory.ensure_user(
            username=user["username"],
            password=user["password"],
            email=user["email"],
            groups=[group_pks[group] for group in user["groups"]],
            base_dn=base_dn,
        )


def _api_token(juju: jubilant.Juju) -> str:
    """Read the Authentik API token from the Juju secret the server charm owns."""
    return get_secret_by_label(juju, API_TOKEN_SECRET_LABEL)["api-token"]
