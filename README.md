# SmokeHouse — where the beef is preserved

The third engine of the ecosystem: a **log-structured record store** whose primary index is an
adaptive [CSRBT](https://github.com/RicheyWorks/CSRBT) set and whose recovery engine is
[SuperBeefSort](https://github.com/RicheyWorks/SuperBeefSort). CSRBT orders the world;
SuperBeefSort feeds it; SmokeHouse is where the world actually lives.

One sentence is the whole consistency design: **the append-only segment log is the only truth,
and every index is a cache of it.** A crash can never lose index state — recovery rebuilds the
index from the log via SuperBeefSort's pipeline (scan → last-writer-wins → sort →
`OrderedSet.fromSorted`, the O(n) zero-rotation build). Records carry CRC32; a torn tail is
truncated at recovery, and everything durably written before it survives by construction.

```java
var store = SmokeHouse.open(dir, SmokeHouseOptions.of(
        SpillSerializer.forLongs(), SpillSerializer.forStrings()));

store.put(42L, "brisket");
store.get(42L);                          // "brisket" — index walk + one positional log read
store.range(10L, 99L, (k, v) -> ...);    // key-ordered stream
store.delete(42L);                       // durable tombstone
store.stats();                           // keys, segments, index strategy, pilot verdict
```

The index is **adaptive by default**: CSRBT's control plane watches your access pattern (real
read/write mix, real key skew) and re-shapes the index through anti-thrash gates, driven by an
internal pilot — no tuning, no human. `IndexTier.STATIC` opts out.

**Phase 2 (durability + reclamation):** `Fsync.INTERVAL` group durability is the default (a
50 ms bounded loss window; `ALWAYS` and `OS` are the ends of the dial). Clean shutdowns write a
CRC-guarded **hint checkpoint**, so reopening scans only the delta — and a corrupt or stale hint
is simply ignored (truth never depends on it). `compact()` merges all closed segments into one
key-ordered segment containing exactly the live records, crash-safe via a marker protocol
(interrupted commits replay or roll back on the next open) — dropped tombstones can never
resurrect anything because compaction always covers a full prefix of the log.
`retainNewest(n)` is the retention tier: keep only the newest-written N keys, evictions funding
the per-segment garbage ledger with no tombstones needed (recovery re-derives newest-N from log
order, which *is* write order).

**Honest bounds** (see the ADR): all keys live in memory (the Bitcask trade — this is an
embedded store for key-indexed records, not a general database); key `equals` must agree with
the comparator; compaction is manual (`compact()`) — auto-triggering off the garbage ledger is
Phase 4 dashboard territory.

## Build

```bash
# Requires ../CSRBT and ../SuperBeefSort as siblings (composite build)
./gradlew build
```

Java 17+, Gradle 9 (wrapper included), zero runtime dependencies beyond the two sibling engines.

## Design

The full architecture — storage-layout trade-offs (Bitcask vs LSM), the `IndexEntry` trick that
turns a set into a map through public API, durability dials, and the four-phase roadmap
(compaction, ingestion, secondary/interval indexes, dashboard) — lives in
[`SuperBeefSort/docs/adr-smokehouse-ecosystem-ring.md`](https://github.com/RicheyWorks/SuperBeefSort/blob/main/docs/adr-smokehouse-ecosystem-ring.md).

## License

MIT.
