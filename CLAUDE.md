# SmokeHouse — working notes for agents

## Build & test
- Composite build: requires `../CSRBT` and `../SuperBeefSort` cloned as siblings.
  `./gradlew build` runs everything (Gradle 9 wrapper; JVM 17+ to run Gradle, 22+ additionally
  builds SuperBeefSort's optional Rust module — both fine).
- Tests are JUnit 5 with `@TempDir` stores; all seeded/deterministic. The oracle pattern
  (`TreeMap` reference in `SmokeHouseTest`) is the required style for any new store behavior.

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
