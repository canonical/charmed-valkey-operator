#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import patch

from ops import testing
from pytest import raises

from src.charm import ValkeyCharm
from src.literals import PEER_RELATION, STATUS_PEERS_RELATION

CONTAINER = "valkey"


def test_install_on_vm(vm_environment):
    ctx = testing.Context(ValkeyCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)

    state_in = testing.State(
        model=testing.Model(name="my-vm-model"),
        leader=True,
        relations={relation, status_peer_relation},
    )

    with (
        patch("workload_vm.ValkeyVmWorkload.install") as workload_install,
        patch("workload_vm.ValkeyVmWorkload.exec"),
    ):
        ctx.run(ctx.on.install(), state_in)
        workload_install.assert_called_once()


def test_install_on_k8s():
    ctx = testing.Context(ValkeyCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)

    state_in = testing.State(
        model=testing.Model(name="my-k8s-model"),
        leader=True,
        relations={relation, status_peer_relation},
        containers={container},
    )

    with (
        patch("workload_vm.ValkeyVmWorkload.install") as workload_install,
    ):
        ctx.run(ctx.on.install(), state_in)
        workload_install.assert_not_called()


def test_install_failure(vm_environment):
    ctx = testing.Context(ValkeyCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    status_peer_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)

    state_in = testing.State(
        model=testing.Model(name="my-vm-model"),
        leader=True,
        relations={relation, status_peer_relation},
    )

    with (
        patch("workload_vm.ValkeyVmWorkload.install", side_effect=RuntimeError()),
        patch("workload_vm.ValkeyVmWorkload.exec"),
    ):
        with raises(testing.errors.UncaughtCharmError) as e:
            ctx.run(ctx.on.install(), state_in)
        assert isinstance(e.value.__cause__, RuntimeError)
