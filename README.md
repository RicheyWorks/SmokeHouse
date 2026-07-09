# SmokeHouse — where the beef is preserved

An embedded, log-structured **record store** for the JVM whose primary index is a live, adaptive
[CSRBT](https://github.com/RicheyWorks/CSRBT) tree and whose recovery engine is
[SuperBeefSort](https://github.com/RicheyWorks/SuperBeefSort). Third engine of the ecosystem:
CSRBT orders the world, SuperBeefSort feeds it, SmokeHouse is where the world actually lives.

One sentence is the entire consistency design:

> **The append-only segment log is the only truth; every index is a cache of it.**

A crash can never lose index state, because there is no index state to lose — recovery rebuilds
it from the log. Records carry CRC32, so a tail torn by a crash is truncated on reopen and
everything durably written before it survives by construction. Zero runtime dependencies beyond
the two sibling engines.

## Quick start

```java
var store = SmokeHouse.open(dir, SmokeHouseOptions.of(
        SpillSerializer.forLongs(), SpillSerializer.forStrings()));

store.put(42L, "brisket");               // append to the log, then index — truth before cache
store.get(42L);                          // one index walk + one positional log read
store.range(10L, 99L, (k, v) -> ...);    // key-ordered stream
store.delete(42L);                       // durable tombstone
store.compact();                         // reclaim dead bytes, crash-safely
store.stats();                           // keys, segments, garbage, index strategy, pilot verdict
store.close();                           // clean shutdown writes the warm-start checkpoint
```

Serializers are SuperBeefSort's `SpillSerializer` contract — `forLongs()`, `forIntegers()`,
`forStrings()` come ready-made; implement two methods for anything else.

## What you get

**An index that tunes itself.** By default the store's index is wired to CSRBT's control plane
and flown by an internal pilot: it watches your real access pattern — read/write mix, key skew,
realized walk depths — and re-shapes itself through anti-thrash gates. No knobs, no DBA.
`IndexTier.STATIC` opts out.

**A durability dial, not a durability sermon.** `Fsync.INTERVAL` (default) group-syncs every
50 ms — a small, explicit loss window at near-`OS` throughput. `ALWAYS` for nothing-ever-lost,
`OS` for fastest. No setting can corrupt the store; the dial only chooses how many
*acknowledged* recent writes a power loss may cost.

**Warm starts.** A clean shutdown writes a CRC-guarded hint checkpoint; the next open loads it
and scans only the segments written afterwards. Hints are optimizations, never truth: corrupt,
stale, or referencing replaced files → silently ignored, full scan, correct answer.

**Crash-safe compaction.** `compact()` merges all closed segments into one key-ordered segment
containing exactly the live records. The commit is a marker protocol — scratch file forced to
disk, then a durable marker (the point of no return), then the swap — and an interrupted commit
replays or rolls back idempotently on the next open. Dropped tombstones can never resurrect
anything, because compaction always covers a full prefix of the log: nothing older exists.
Reads continue during the copy; a read overlapping the commit retries once against the
repointed index.

**A retention tier.** `retainNewest(n)` keeps only the newest-written N keys — CSRBT's FIFO
window does the evicting (upserts re-enter at the tail, so eviction order is write order), and
evictions fund the garbage ledger with no tombstones needed: recovery re-derives newest-N from
log order, which *is* write order.

## The recovery story

Cold start is SuperBeefSort's feeding pipeline made load-bearing: scan the segments →
last-writer-wins → **sort the live entries with the full sort engine** →
`OrderedSet.fromSorted`, CSRBT's O(n) zero-rotation build. The demo path and the restart path
are the same code, which means the restart path is exercised constantly.

## Honest bounds

Read these before depending on it (they're the ADR's explicit trades, not accidents):

- **All keys live in memory** — the classic Bitcask trade. SmokeHouse is an embedded store for
  key-indexed record data, not a general database; the hard ceiling is CSRBT's 2³¹-key index.
- **Key `equals`/`hashCode` must agree with the comparator** (standard `TreeMap` + `HashMap`
  interop discipline; recovery's last-writer-wins and the index's skew detection rely on it).
- **Compaction is manual.** The garbage ledger (`garbageBytes()`, `stats()`) tells you when
  it's worth calling; auto-triggering is Phase 4 territory, alongside the live dashboard.
- **Single-writer.** All mutation serializes on one store lock (reads resolve the index under
  it and stream the log outside it). That's a contract inherited from the control plane, not a
  temporary limitation.

## Build

```bash
# Requires ../CSRBT and ../SuperBeefSort as siblings (Gradle composite build)
./gradlew build
```

Java 17+, Gradle 9 (wrapper included). Tests are deterministic and oracle-driven — every store
behavior is asserted against a `TreeMap` reference, including across crashes, compactions, and
reopens.

## Design

The full architecture — why Bitcask-style beat LSM here, the `IndexEntry` trick that turns a
set into a map through public API only, the compaction crash windows, and the roadmap
(Phase 3: file ingestion + trace replay; Phase 4: secondary & interval indexes, ensemble index
tier, live dashboard) — lives in the ecosystem ADR:
[`SuperBeefSort/docs/adr-smokehouse-ecosystem-ring.md`](https://github.com/RicheyWorks/SuperBeefSort/blob/main/docs/adr-smokehouse-ecosystem-ring.md).

## License

MIT.
