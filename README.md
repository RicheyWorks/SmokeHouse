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

**Honest bounds** (see the ADR): all keys live in memory (the Bitcask trade — this is an
embedded store for key-indexed records, not a general database); key `equals` must agree with
the comparator; Phase 1 has no compaction yet, so overwritten/deleted bytes stay on disk.

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
