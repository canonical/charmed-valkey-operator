# AGENTS.md

Guidance for AI coding agents (Codex, Cursor, Copilot, Claude Code, Gemini CLI, …) working in this
repository. `CLAUDE.md` and `GEMINI.md` are symlinks to this file; `tests/AGENTS.md` adds
integration/spread-test specifics; `docs/AGENTS.md` adds documentation specifics.

## What this is

A [Juju](https://juju.is) charm (Charmhub: `valkey`, track `9/edge`) that deploys and operates
[Valkey](https://valkey.io) — a Redis-compatible key-value store — in **both Kubernetes and VM/bare-metal**
environments from a single codebase. HA is provided by Valkey **Sentinel** (primary/replica with automatic
failover). Built with the `ops` framework following Canonical's Data Platform charm architecture.
The substrate (VM vs K8s) is detected at runtime in `charm.py` via `self.model.get_cloud_spec()`.

## Hard rules (NEVER)

- NEVER read/write `relation.data` directly — go through `ClusterState` and the pydantic models.
- NEVER use ops `StoredState` — all charm state lives in peer-relation databags (typed via the
  pydantic models) and Juju secrets, the single source of truth; `StoredState` is per-unit and
  bypasses that model. Don't reach for it as an escape hatch.
- NEVER call `workload.exec()` with raw CLI strings from managers — add a method to
  `ValkeyClient`/`SentinelClient` in `common/client.py` (the only place CLI commands are built).
- NEVER restart services inline — emit `self.charm.restart_workload.emit(...)`; the handler in
  `charm.py` acquires `RestartLock`, restarts, health-checks, and defers if unhealthy.
- NEVER hand-edit `lib/charms/*` — CI's `lib-check` fails the PR unless content matches the
  published Charmhub lib; update only via `charmcraft fetch-lib`.
- A new `WorkloadBase` operation MUST be an `@abstractmethod` implemented in BOTH
  `workload_vm.py` and `workload_k8s.py`. Decorate every method that overrides a base-class or
  `Protocol` method with `@override` (from `typing`), as the workloads and `common/locks.py`
  already do.
- NEVER log, print, or put secret values (passwords, CA/TLS private keys) into exception messages
  or statuses — log identifiers/labels only.
- NEVER `git push` or merge/land a PR — the user does all pushing and merging; commit locally only
  when asked, NEVER add Claude-Session url.
- NEVER comment on, review, or approve GitHub PRs/issues — surface findings in the conversation.

## Correctness invariants (every change MUST hold these)

- **Backward-compatible upgrades / migrations.** New-revision code MUST keep working against
  old-revision state (peer databags, Juju secrets, on-disk config/ACL, Pebble layer) and against
  old-revision peers mid-upgrade. On any schema/layout change, ship migration logic and tolerate
  missing keys (default in the pydantic model). `9/edge` rolls unit-by-unit, so old and new always
  coexist — no flag day.
- **Idempotency.** Handlers and manager ops MUST be safe to re-run and converge to the same state
  (Juju redelivers events; `StartState`/`restart_workload` re-enter). Check-then-act before
  mutating; never assume a step ran exactly once.
- **Eventual consistency / self-healing.** Don't depend on a specific event firing. If a unit
  misses one (peer churn, leader change, restart, observer death, deferred hook), the next
  reconcile — `update-status`, a `*-relation-changed`, or the deferring `StartState`/restart
  machine — MUST converge it. Reconcile from current state, not from the delta; defer (don't crash)
  when prerequisites aren't ready.
- **Reasoned guardrails, not reflexive defense.** Validate and defend at boundaries — hook entry,
  relation/secret reads, `workload.exec` — then trust those invariants inward; don't re-check the
  same precondition at every leaf "just in case". Reason about the charm as one predictable system
  from hook to leaf, not a pile of local defensive patches. (This shapes the idempotency and
  eventual-consistency rules above — reconcile and defer at the boundary, don't scatter guards.)
- **Integration coverage.** Every feature / user-facing change ships an integration test under
  `tests/integration/` (requirer-charm/glide-runner for HA) — passing unit tests alone aren't
  sign-off.

## Before you call a change done (YOU MUST)

1. **Write the test first (TDD).** For any feature or bugfix, add the failing unit test
   (`ops.testing` Scenario) and watch it fail *before* writing implementation, then make it pass —
   don't write implementation code first.
2. `tox run -e lint`, `tox run -e unit`, and `tox run -e static` — all MUST pass; show the output,
   don't assert success.
3. Do NOT run `charmcraft pack` to verify code changes — it is a 10–30 min Rust build; lint + unit
   are the verification gate. Pack only when a `.charm` artifact is explicitly needed.
4. Do NOT run `tox run -e integration` speculatively — it sudo-installs packages and downloads
   binaries *before* checking anything. Run it only with a bootstrapped controller
   (`juju controllers` works) and built charm + requirer-charm artifacts present.

## Essential commands

All linting/testing goes through `tox` + `poetry` (Python `^3.12`; dependency groups in `pyproject.toml`).

```bash
tox run -e lint          # ruff check + ruff format --check + codespell + shellcheck
tox run -e format        # auto-fix style; also runs `poetry lock` (needs network, mutates poetry.lock)
tox run -e unit          # pytest + ops Scenario with coverage. Fast, fully mocked — no Juju needed.
tox run -e static        # pyright type checks over src/. Fast — no Juju needed.

# Single unit test: use -k, NOT a path (the unit env appends tests/unit AFTER {posargs},
# so a path posarg would still run the whole dir).
tox run -e unit -- -k test_start_primary
# Fast iteration after the first run (skips poetry install + coverage):
.tox/unit/bin/pytest tests/unit -k test_start_primary

charmcraft pack          # or: charmcraftcache pack (faster, used in CI/spread). Long Rust build.

# Integration tests — need a bootstrapped Juju controller (microk8s or lxd), a built charm in the
# repo root, and the built requirer-charm. A test path IS honored here (unlike the unit env);
# --substrate is optional (vm|k8s, defaults to k8s) — pass it explicitly to target VM.
tox run -e integration -- tests/integration/test_charm.py --substrate k8s

# Deploy a locally-built charm. --trust is mandatory; the image resource is K8s-only — use the
# current `upstream-source` value from metadata.yaml as the tag.
juju deploy ./valkey_ubuntu@24.04-amd64.charm -n 3 \
  --resource valkey-image=<upstream-source from metadata.yaml> --trust
```

- `pyright` runs via `tox run -e static` (`[tool.pyright]` in `pyproject.toml` scopes it to
  `src/**` and `lib/**`; the env type-checks `src/`). Ruff rules (line length 99, pydocstyle,
  mccabe ≤10) also live in `pyproject.toml`. On lint failures, run `tox run -e format` first
  instead of hand-fixing style.
- Spread (`spread.yaml`, `tests/spread/`) is how CI runs the integration suite at scale; you rarely
  run it locally — run `tox -e integration` against your own model instead. Integration/spread
  specifics live in `tests/AGENTS.md`.

## Layers (strict — keep logic in its layer; the most important convention in the repo)

Dependency direction: `charm.py` → `events/` → `managers/` → `core/` + `workload_{vm,k8s}.py`.
All paths below are under `src/`.

- `charm.py` — thin wiring only: picks the workload by substrate, owns the `restart_workload`
  event and the rolling-restart handler. Add nothing else here.
- `core/` — state & data, NO behavior. `cluster_state.py`: `ClusterState`, the single source of
  truth (peer relations, networking, secrets). `models.py`: pydantic models over relation databags
  (`PeerAppModel`, `PeerUnitModel`; `RelationState.update()` DELETES keys whose value is falsy).
  `base_workload.py`: `WorkloadBase` ABC + `TLSPaths`.
- `managers/` — pure business logic, NO event handling; each takes (state, workload).
  `cluster.py`: health checks, replica sync, ACL reload. `config.py`: renders configs/ACL files,
  password generation, quorum math. `sentinel.py`: all Sentinel ops; on K8s also reconciles the
  primary/replicas Services and pod `role` labels. `backup.py`: RDB backup/restore policy.
  `tls.py`, `external_clients.py`, `topology.py` (observer subprocess lifecycle).
- `events/` — ops.Objects that observe Juju events and ORCHESTRATE managers, no low-level logic.
  `base_events.py`: startup state machine + scale-down. `tls.py`, `external_clients.py`.
- `workload_vm.py` — snap `valkey-charmed` (services `server`/`sentinel`, user `snap_daemon`,
  CLI `valkey-charmed.cli`). `workload_k8s.py` — Pebble in the `valkey` container (services
  valkey/sentinel/metrics-exporter, user `_daemon_`, CLI `valkey-cli`; owns the Pebble layer).
  Workloads expose file/exec/service primitives only — business logic belongs in `managers/`, and
  paths/ports/users come from `literals.py`, never hardcoded.
- `common/` — SDK/CLI adapters + shared plumbing: `client.py` (CliClient →
  ValkeyClient/SentinelClient), `locks.py`, `k8s_client.py` (lightkube, K8s only),
  `storage_backend.py` (object-store SDKs behind the `StorageBackend` Protocol; `build_backend`
  is the credentials-type → backend registry), `custom_events.py`, `exceptions.py` (ALL custom
  exceptions).
- `literals.py` — ALL constants and enums (ports, paths, relation names, snap revisions,
  `Substrate`, `StartState`, `TLSState`, `CharmUsers`, `CHARM_USERS_ROLE_MAP`, …).
- `statuses.py` — all `StatusObject`s, surfaced via data_platform_helpers' `StatusHandler` from
  each manager's `get_statuses()` (`ManagerStatusProtocol`).
- `scripts/topology_observer.py` — standalone subprocess (under `src/`), NOT a charm hook.

## Critical cross-cutting mechanisms (read these to avoid breaking things)

- **State lives in peer-relation databags**, typed via pydantic models: app-wide → `state.cluster`,
  per-unit → `state.unit_server` / `state.servers`. Sensitive values are NOT stored in the databag:
  secret fields in `core/models.py` use the `*Secret` type aliases annotated `Field(exclude=True)`
  + a secret-label suffix, which routes the value into a Juju secret (only the URI hits the
  databag). Copy that pattern for any new credential/key field — a bare `Field()` would write
  plaintext into relation data.
- **Locks serialize cluster operations** (`common/locks.py`): `StartLock`/`RestartLock` are databag
  locks arbitrated by the leader (`process()` grants to one unit at a time) — this is what makes
  start/restart a safe rolling operation. `ScaleDownLock` is a distributed lock stored inside
  Valkey itself (`SET ... NX PX`, 5-min TTL) because it must survive the unit going away. Reuse
  these for any operation that must not run concurrently across units.
- **Startup is a deferring state machine**, not a single function: `_on_start` emits
  `unit_fully_started`, which defers through `StartState` (`WAITING_FOR_PRIMARY_START` →
  `STARTING_WAITING_VALKEY` → `..._SENTINEL` → `..._REPLICA_SYNC` → `STARTED`), persisted in
  `PeerUnitModel.start_state`. Many handlers early-return unless `state.unit_server.is_active`.
- **The topology observer** is a long-lived subprocess the leader spawns
  (`TopologyManager.start_observer`). It watches Sentinel and on a primary change runs `juju-exec`
  to dispatch a custom `topology_changed` hook (handled in `events/external_clients.py` to
  re-point K8s Services / client endpoints). PID tracked in `PeerUnitModel.topology_observer_pid`.
- **TLS is two-tiered.** Peer/internal TLS is always on: the leader generates a self-signed CA and
  replication uses `tls-port` (`tls-replication yes`). Client TLS is optional via the
  `client-certificates` relation; once a provider is related, its cert replaces the self-signed one
  for replication too (single cert file). Enable/disable and CA rotation are explicit state
  machines (`TLSState`, `TLSCARotationState`) coordinated via databag flags in `events/tls.py`.
- **Users & ACLs.** Internal users are the `CharmUsers` enum with permissions in
  `CHARM_USERS_ROLE_MAP`; passwords auto-generated on `leader-elected` (or from the `system-users`
  secret config), written to on-disk ACL files by `ConfigManager`, applied with `acl load`.
  External client users come from the `valkey-client` relation. There are NO get/set-password
  actions — the only charm action is `status-detail`. `managers/config-template/` is the SOURCE OF
  TRUTH for valkey.conf / sentinel.conf defaults; managers read it from the repo, NOT from disk on
  the unit, then override keys.

## Substrate differences to keep in mind

| | VM | K8s |
|---|---|---|
| Workload | `valkey-charmed` snap (revisions pinned in `literals.py`) | OCI image + Pebble |
| Unit address (`endpoint`) | private IP (`bind_address`) | unit hostname (`<unit>.<app>-endpoints`) |
| Service discovery | Sentinel returns IPs | extra: lightkube-managed `*-primary`/`*-replicas` Services + pod `role` labels |
| File paths / user | `var/snap/...`, `snap_daemon` | `var/lib/valkey/...`, `_daemon_` |

Any code touching addresses, file paths, services, or networking must handle both. Branch on
`self.state.substrate == Substrate.K8S`.

## Conventions when editing

- Imports in `src/` are flat (`tox` sets `PYTHONPATH=src:lib`): `from managers.config import
  ConfigManager`. Unit tests import `from src.charm import ValkeyCharm` but patch via the flat
  path, e.g. `mocker.patch("workload_k8s.ValkeyK8sWorkload.write_file")`.
- New constant/port/path/enum → `literals.py`. New exception → `common/exceptions.py`. New status →
  a `StatusObject` in `statuses.py`, surfaced from the relevant manager's `get_statuses()`.
- Polling/health waits use `tenacity` retries (commonly `retry_if_result(lambda r: not r)`), not
  bare loops.
- Unit tests use `ops.testing` Scenario (`Context`, `State`, `Container`, `PeerRelation`,
  `Secret`) — not the legacy `Harness`. External effects are mocked autouse in
  `tests/unit/conftest.py`; pick the substrate via the `cloud_spec` (K8s) / `cloud_spec_vm`
  fixtures; assert statuses with `tests/unit/helpers.py::status_is`.
- User-facing behavior changes (ports, relations, TLS flow) → update the matching
  `docs/how-to/*.md` (Sphinx/Diátaxis, published to Read the Docs).

## Repo etiquette & CI

- The default branch is **`9/edge`** (not main). Never push to it directly — every change lands as
  a squash-merged PR with conventional-commit subjects (`type(scope): summary (#PR)`). **Merging
  to `9/edge` auto-releases to Charmhub `9/edge`** and tags `rev<N>` (`release.yaml`); docs-only
  changes (`docs/**`) are excluded from release.
- PR CI (`.github/workflows/ci.yaml`): lint → unit → lib-check → build → spread integration.
  Integration runs on PRs only (not branch pushes) and is skipped for docs-only changes.

## Gotchas

- `--trust` is mandatory (cloud-spec lookup at init raises without it; K8s also patches
  Services/pods). It grants cloud-admin credentials: only ever target a local, throwaway
  controller/model; never run destructive `juju`/`kubectl` commands (destroy-model,
  remove-application, delete) against a controller you did not create without explicit user
  confirmation.
- Base is Ubuntu 24.04; both amd64 and arm64 platforms build (`charmcraft.yaml`) — ARM integration
  tests are still a TODO. Channel is `9/edge`.
- Always invoke a specific env (`tox run -e <env>`); bare `tox` errors on an undefined `static` env
  reference (legacy `env_list` entry).
- `valkey-glide` is a released PyPI dependency (`^2.5`), pinned in both `poetry.lock` and
  `tests/integration/clients/requirer-charm/poetry.lock` — keep the two in step. It used to be a
  git fork (for upstream PR 5124, mTLS support, now released); the wheel installs in seconds where
  the fork cost ~4.5 min of Rust build per job. `poetry lock` (run by `format`) still needs network.
- The integration env sudo-installs `valkey-cli` to `/usr/local/bin` and, when `$CI` is unset,
  creates Juju model `testing` — delete that model between local runs or the re-run fails.
- Build requires the Rust toolchain (native dep: rpds-py). `# renovate:` comments in
  `charmcraft.yaml` pin pip/uv/poetry/python/rust for the build — keep that comment format if you
  bump them.
- Repo/dir is `valkey-operator`; the charm `name` is `valkey`; `metadata.yaml`/`config.yaml`/
  `actions.yaml` are kept as separate files (not folded into `charmcraft.yaml`) for
  `data-platform-workflows` compatibility.
- `lib/charms/` holds only the vendored `rolling_ops` lib, which is currently UNUSED (rolling
  restart is the custom `restart_workload`/`RestartLock` path, not rolling_ops).
- Integration tests deploy a companion requirer-charm (`tests/integration/clients/requirer-charm/`,
  the "glide-runner") that drives continuous writes with `valkey-glide` to validate HA scenarios.
