# SmokeHouse — working notes for agents

## Build & test
- Composite build: requires `../CSRBT` and `../SuperBeefSort` cloned as siblings.
  `./gradlew build` runs everything (Gradle 9 wrapper; JVM 17+ to run Gradle, 22+ additionally
  builds SuperBeefSort's optional Rust module — both fine).
- Tests are JUnit 5 with `@TempDir` stores; all seeded/deterministic. The oracle pattern
  (`TreeMap` reference in `SmokeHouseTest`) is the required style for any new store behavior.
- `./gradlew run` starts the shop window (Phase 4.4): `demo/StoreDashboard` + `dashboard.html`,
  SSE on `127.0.0.1:8079` (loopback only). Single driver thread owns all mutation; HTTP threads
  only read or call `compact()` (copy phase off-lock by contract). Auto-compaction is disabled
  there (`compactWhenGarbageAbove(0.0)`) so the Compact button owns the demo.

## Git is host-side
Same as the siblings: agent sandboxes cannot write `.git`. Run all git commands from a host
terminal (PowerShell). Stale `.git/index.lock` fix: `Remove-Item .git\index.lock -Force`.

## Architecture invariants (do not break)
- **The log is the only truth; every index is a cache.** Never persist index state as
  authoritative; recovery must always be able to rebuild from segments alone.
- **Single writer:** all mutation AND the internal pilot serialize on the store lock (CSRBT's
  control plane is single-threaded by contract). Log reads happen outside the lock — bytes at an
  offset never change once written.
- **Truth before cache:** on every mutation the log append happens before the index update.
- Key `equals`/`hashCode` must agree with the comparator (recovery's last-writer-wins map and
  the workload monitor's skew sketch both rely on it).
- Design record: `SuperBeefSort/docs/adr-smokehouse-ecosystem-ring.md` (phases 2–4 live there).

## Index tiers (Phase 3.5/4 surfaces)
- Four tiers: `STATIC` / `ADAPTIVE` (default) / `ENSEMBLE` / `EVOLUTION`. All are *born optimal*
  (`accessPolicy` + recovery-sort profile → `StrategyAdvisor`); non-RB bulk builds are
  health-gated with an RB fallback. ADAPTIVE clamps WRITE_HEAVY to Red-Black (WeightBalanced is
  outside CSRBT's morph family — `WorkloadAdaptation.attach` rejects it loudly).
- The ensemble-backed tiers (`ENSEMBLE`, `EVOLUTION`) reject `retainNewest` at open: the
  ensemble emits no per-member `Evict` events, so the garbage ledger can't be funded.
- The pilot also drives auto-compaction (`compactWhenGarbageAbove`, default 0.5): ratio check
  under the lock, `compact()` outside it, `compacting` re-entry guard. STATIC never auto-compacts.
- Order statistics (`countRange`/`nthKey`/`rankOf`/`medianKey`/`percentileKey`) are 1-indexed
  CLRS OS-SELECT/OS-RANK semantics; oracle tests live in `CsrbtUnlockTest`.
- `IndexedStore` (Phase 4.1) composes secondaries OVER the store — never bypass it to mutate
  the primary. Composite `(attribute, primaryKey)` entries; `AttrEntry`'s sentinel ranks make
  `byAttribute` an exact closed-range walk. Secondaries are memory-only (rebuilt from the
  primary on every `build()`); retention + secondaries is refused. Double-oracle tests in
  `IndexedStoreTest`.
- Interval index (Phase 4.2): `.interval(name, start, end)` on the same builder — CSRBT
  `IntervalAugmentor` tree (keys = distinct starts, tag = MAX end per start) + exact sidecar
  `start → end → keys`; the tag is an upper bound so pruning never loses a match, the sidecar
  makes duplicate starts/spans exact. Extractors validate BEFORE the primary write. Brute-force
  oracle in `IntervalIndexTest`.
- Typed interval tier (Phase 7): `.interval(name, order, start, end)` overload — same semantics
  over comparator-ordered endpoints (epoch-millis `Long`s etc.), backed by CSRBT's
  `GenericIntervalAugmentor` + a `TreeMap` sidecar (no `hashCode` contract, only
  equals-agrees-with-comparator). Typed `stab`/`overlapping` overloads; int calls on a typed
  index (and vice versa) fail loudly with cross-referencing messages. Oracle in
  `GenericIntervalIndexTest`.
