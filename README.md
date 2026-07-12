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

**An index that is born optimal, then tunes itself.** Recovery doesn't hardcode a tree shape:
your declared `accessPolicy(...)` (READ_HEAVY, SKEWED, WRITE_HEAVY, BALANCED) plus the recovery
sort's own data profile pick the balancing strategy the index is *built* with, and the sort's
measurements prime the control plane's scorer. From there the default tier's internal pilot
watches your real access pattern — read/write mix, key skew, realized walk depths, range scans —
and re-shapes the index through anti-thrash gates. No knobs, no DBA. Four tiers:
`STATIC` (born optimal, never re-shapes), `ADAPTIVE` (the default: health-gated O(n) morphs),
`ENSEMBLE` (a mirrored RB+AVL+Splay trio; adaptation is an O(1) read-path promotion, with
failover/quarantine/heal), and `EVOLUTION` (CSRBT's evolution machine: a laboratory member
trials balancing policies against live traffic — an observability tier, on the record).

**Secondary indexes.** `IndexedStore` composes named secondaries over the primary: declare an
attribute extractor and a comparator, and `byAttribute(name, lo, hi)` answers value-attribute
ranges in one index walk (composite `(attribute, key)` entries — no post-filtering). Same
consistency story as everything else: secondaries are memory-only caches rebuilt from the log
on every open, so a crash can't lose them either. Indexed writes pay one extra primary read
(the old value must be retracted); v1 refuses `retainNewest` + secondaries.

```java
var store = IndexedStore.open(dir, opts)
        .secondary("price", Comparator.<Long>naturalOrder(), Order::price)
        .interval("window", Order::startMinute, Order::endMinute)
        .build();
store.byAttribute("price", 100L, 500L);   // keys of every order priced 100..500
store.stab("window", 1_440);              // keys of every order whose window covers minute 1440
store.overlapping("window", 900, 960);    // ...or overlapping 900..960
```

**An interval index** (same builder) answers *stabbing* and *overlap* queries over records
carrying an `(int start, int end)` span — CSRBT's CLRS-14.3 interval tree with subtree max-end
augmentation doing the pruning, and an exact sidecar resolving candidates, so duplicate starts
and even duplicate whole spans just work. Endpoints are `int` in v1 (epoch minutes/seconds —
that's the augmentor's honest bound, not ours).

**Order statistics for free.** The index maintains subtree sizes intrinsically, so
`countRange(lo, hi)`, `nthKey(rank)`, `rankOf(key)`, `medianKey()`, `percentileKey(pct)`,
`firstKey()` and `lastKey()` are all O(log n) — answers the log alone could never give without
a full scan, and they're correct immediately after recovery's bulk build.

**A durability dial, not a durability sermon.** `Fsync.INTERVAL` (default) group-syncs every
50 ms — a small, explicit loss window at near-`OS` throughput. `ALWAYS` for nothing-ever-lost,
`OS` for fastest. No setting can corrupt the store; the dial only chooses how many
*acknowledged* recent writes a power loss may cost.

**Warm starts.** A clean shutdown writes a CRC-guarded hint checkpoint; the next open loads it
and scans only the segments written afterwards. Hints are optimizations, never truth: corrupt,
stale, or referencing replaced files → silently ignored, full scan, correct answer.

**Crash-safe compaction — automatic by default.** On piloted tiers the pilot watches the
closed segments' garbage ratio and runs compaction itself once it crosses
`compactWhenGarbageAbove` (default 0.5; `0` disables — and the pilot-less `STATIC` tier is
always manual). `compact()` merges all closed segments into one key-ordered segment
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

**Bulk import — ingestion *is* recovery.** `SmokeHouse.importInto(dir, opts, source)` ingests a
whole [`RecordSource`](https://github.com/RicheyWorks/SuperBeefSort) (CSV, JSONL, or binary)
into a fresh store by appending every record straight to a bare log with **no index maintenance
at all**, then handing off to `open()` — whose recovery already sorts and builds the index in
O(n). An import is therefore a pre-compacted store, and it reuses the exact recovery muscle every
restart exercises. Duplicate keys resolve last-writer-wins; v1 targets an empty directory
(importing into a populated store is what `put` is for).

```java
try (var store = SmokeHouse.importInto(dir, opts,
        CsvSource.of(csv, /*keyCol*/0, /*valCol*/1, /*skipHeader*/true,
                Long::parseLong, s -> s))) {
    store.get(42L);                      // fully indexed, born from the CSV
}
```

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
- **Compaction is automatic only where a pilot flies.** Piloted tiers self-compact past the
  `compactWhenGarbageAbove` ratio; the `STATIC` tier has no pilot, so there the garbage ledger
  (`garbageBytes()`, `stats()`) tells you when `compact()` is worth calling.
- **The ensemble-backed tiers cost memory and refuse retention.** `ENSEMBLE`/`EVOLUTION`
  mirror every key into each member (~3× index memory for the trio) and surface no per-member
  eviction events, so `retainNewest` is rejected at open — by design, loudly.
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

## The shop window

```bash
./gradlew run     # then open http://127.0.0.1:8079/
```

A live SmokeHouse on exhibit: a built-in workload churns a temp-dir store through flipping
regimes (steady churn, hot-key overwrite, read-heavy, delete wave) while the page watches over
Server-Sent Events — keys/puts/gets/deletes ticking, the pilot's verdicts as they change, and
the **segment map**: one bar per log segment, its red fill the garbage ledger made visible.
Press *Compact now* and watch the map squash to a handful of clean segments while the workload
keeps running (compaction's copy phase is off the store lock by design) — *hot-key-overwrite* is
the garbage machine if you want the squash to be dramatic. Regime buttons switch the workload
live; *Pause* freezes it mid-scene. Binds loopback only — an exhibit, not a service.

## Design

The full architecture — why Bitcask-style beat LSM here, the `IndexEntry` trick that turns a
set into a map through public API only, the compaction crash windows, and every trade-off above
— lives in the ecosystem ADR:
[`SuperBeefSort/docs/adr-smokehouse-ecosystem-ring.md`](https://github.com/RicheyWorks/SuperBeefSort/blob/main/docs/adr-smokehouse-ecosystem-ring.md).
Its four phases (core store, durability + compaction, ingestion, the index ring + dashboard) are
all complete.

What comes next — benchmarking and the measured performance seams, crash-fuzz hardening,
backup/restore, a log-tail primitive feeding watchers and snapshots, tail-shipped read replicas,
and Maven Central release — is the successor ADR:
[`SuperBeefSort/docs/adr-ecosystem-outer-ring.md`](https://github.com/RicheyWorks/SuperBeefSort/blob/main/docs/adr-ecosystem-outer-ring.md).

## License

MIT.
