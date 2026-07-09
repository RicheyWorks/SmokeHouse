# Handoff: completing SmokeHouse Phases 3–4

**Audience:** whoever picks this up next — Richmond, or an agent session with the three sibling
repos mounted. **Prerequisite state:** Phases 1–2 built green and pushed (`SmokeHouse` repo:
segment log + CRC codec, `IndexEntry` index, SuperBeefSort-powered recovery, INTERVAL fsync,
hint checkpoints, crash-safe compaction, retention tier). The governing design is
`SuperBeefSort/docs/adr-smokehouse-ecosystem-ring.md` — read it first; this document is the
execution plan for its two remaining phases.

---

## Ground rules (non-negotiable, learned the hard way)

1. **The log is the only truth; every index is a cache.** No feature may persist index state as
   authoritative. If a design needs the index to survive on its own, the design is wrong.
2. **Truth before cache:** log append precedes index update on every mutation.
3. **Single writer:** all mutation AND the pilot serialize on the store lock. New background
   work (auto-compaction, dashboards) must either take that lock for control-plane/index access
   or touch only immutable closed segments.
4. **Compaction covers a full prefix of closed segments, always.** That prefix rule is the only
   reason dropping tombstones is safe. Never compact an arbitrary subset.
5. **Oracle tests or it didn't happen.** Every new store behavior is asserted against a
   `TreeMap`/reference implementation, including across close/reopen and across compaction.
   Deterministic seeds everywhere.
6. **Zero new runtime dependencies.** JDK + the two sibling engines. Document limitations
   instead of importing parsers.
7. **Workflow:** the agent sandbox cannot build (JRE 11) or run git. All builds
   (`.\gradlew build --console=plain`) and all git commands run host-side in PowerShell.
   Build **CSRBT → SuperBeefSort → SmokeHouse** when more than one repo changed. Stale
   `.git/index.lock` → `Remove-Item .git\index.lock -Force`.
8. **Loopback only** for any demo server; next free port in the family is **8079**.

Known pitfalls from this codebase: `OrderedSet.withNaturalOrder(new RedBlackStrategy<>())`
does **not** infer — write `new RedBlackStrategy<Integer>()` explicitly (plain `fromSorted`
infers fine). Guard `BeefSort` sorts with `if (list.size() > 1)`. `close()` must stay
idempotent. If the sandbox's `/mnt` view looks stale or "binary", trust the host-side file
tools (Read/Grep), not bash.

---

## Phase 3 — Ingestion + trace replay

**Lands in SuperBeefSort** (it is feeding apparatus), except `importFrom`, which lands in
SmokeHouse. Build/test SuperBeefSort green **before** starting the SmokeHouse half.

### 3.1 `RecordSource` (new package `io.github.richeyworks.superbeefsort.source`)

```java
public interface RecordSource<K, V> extends AutoCloseable {
    /** Next record, or null at end. Implementations are single-pass, streaming, order-preserving. */
    Record<K, V> next() throws IOException;
    record Record<K, V>(K key, V value) { }
    @Override void close() throws IOException;
}
```

Implementations, each with an explicit seed-free deterministic contract:

- **`CsvSource<K,V>`** — `of(Path, int keyColumn, int valueColumn, boolean skipHeader, Function<String,K>, Function<String,V>)`.
  Split on commas with a minimal quoted-field scanner (`"a,b"` stays one field; doubled quotes
  escape). Document: no multiline fields, no custom delimiters in v1.
- **`JsonlSource<K,V>`** — `of(Path, String keyField, String valueField, parsers…)`. A
  deliberately minimal top-level scanner: find `"field":` at nesting depth 1, extract string or
  number token. Document loudly: flat objects only, no nested/escaped-exotic extraction — a
  documented limitation, not a hidden dependency (ground rule 6).
- **`BinarySource<K,V>`** — length-prefixed records over `SpillSerializer<K>`/`SpillSerializer<V>`
  (reuse the existing contract; also provide the matching `BinarySink` writer so round-trip
  tests are self-contained).

Tests: `RecordSourceTest` — CSV quoting matrix, JSONL happy/limit cases, binary round-trip,
all via `@TempDir` files.

### 3.2 Iterator-based external sort (SuperBeefSort, `external` package)

Today `ExternalMergeSorter` starts from a `List`. Add streaming run generation **without
touching the existing list paths**:

- `private List<SpillFile<K>> generateRuns(Iterator<K> input)` — fill a `runSize` buffer, sort
  it with the existing engine call, spill, repeat.
- Public: `sortAndFeed(Iterator<K> input, CsrbtTarget<K> target, int maxSize)` and
  `sortToList(Iterator<K>)` (test convenience). Keep the Gap-8 rule: bounded + windowless
  target → throw.
- `BeefSort.ExternalSortBuilder` gains `feedFrom(Iterator<K>)` terminals mirroring the list ones.

Tests: iterator path equals list path on the same data (oracle = existing `sortToList(List)`);
a run-boundary-straddling case (`runSize` smaller than input).

### 3.3 Trace recorder/replayer (SuperBeefSort, `workload` package)

Trace = JSONL op log, one op per line: `{"op":"add|remove|contains","key":123,"t":17}`
(`t` = millis since trace start; integer keys in v1).

- **`TraceRecorder`** — wraps a `WorkloadAdaptation<Integer>` (same facade methods, delegates +
  appends a line via a `BufferedWriter`; `close()` flushes). Not thread-safe; say so.
- **`TraceReplayer`** — `replay(Path, WorkloadAdaptation<Integer>, double speed)`: parse lines
  (same minimal scanner as JsonlSource — share the helper), drive the facade; `speed <= 0`
  means as-fast-as-possible (the test mode), otherwise sleep to honor inter-op gaps scaled by
  `speed`.
- **`Workloads.fromTrace(Path path)`** — returns a `Regime` whose key stream and op mix replay
  the trace's keys/ops cyclically, so the **aquarium can eat real traces**: add a `"trace"` DJ
  booth slot that loads `docs/sample.trace` if present (skip the button if absent).

Tests: record 500 ops → replay into a fresh set → both sets' `inOrder()` equal; trace file
line count = op count.

### 3.4 `SmokeHouse.importFrom` (SmokeHouse repo)

The elegant design — **an import is a recovery**: append everything, then let `open()` do what
it already does.

```java
public static <K,V> SmokeHouse<K,V> importInto(Path dir, SmokeHouseOptions<K,V> opts,
                                               RecordSource<K,V> source) throws IOException
```

Implementation: open a bare `SegmentLog` (not a store), stream the source appending encoded
records (no index maintenance at all — this is the whole trick), close the log, then return
`SmokeHouse.open(dir, opts)` — recovery scans, LWW-resolves duplicates from the source, sorts
via SuperBeefSort, builds O(n). Fail loudly if the directory already contains segments (import
targets a fresh store in v1). Tests: CSV → importInto → oracle agreement; duplicate keys in
source → last one wins; a second open is a warm start.

**Phase 3 definition of done:** SBS + SmokeHouse both green; aquarium still runs; commit
messages: SBS `"Phase 3: RecordSource trio, iterator external sort, trace record/replay"`,
SmokeHouse `"Phase 3: importInto — ingestion as recovery"`. Update the ADR's action-item
checkboxes and the READMEs' feature lists.

---

## Phase 4 — The index ring + the shop window

**Lands in SmokeHouse.**

### 4.1 `IndexedStore` (secondary indexes)

Composition, not modification: `IndexedStore<K,V>` **owns** a `SmokeHouse<K,V>` plus named
secondaries; it has its own outer lock and is the only thing callers touch.

```java
public final class IndexedStore<K, V> implements Closeable {
    public static <K,V> Builder<K,V> open(Path dir, SmokeHouseOptions<K,V> opts);
    // Builder: .secondary(String name, Comparator<A> order, Function<V,A> extractor)
    public void put(K key, V value);            // store.put + fan out to secondaries
    public boolean delete(K key);               // needs old value: store.get first, then delete + retract
    public V get(K key);
    public List<K> byAttribute(String name, Object lo, Object hi);   // secondary range -> primary keys
}
```

- Secondary entry = composite `(attribute, primaryKey)` in its own CSRBT `OrderedSet`, ordered
  by attribute **then** key (the percentile service's codec lesson, generalized: composites
  keep equal attributes distinct). Probe-range trick for `byAttribute`: range from
  `(lo, MIN_KEY_SENTINEL)` to `(hi, MAX_KEY_SENTINEL)` — or simpler, range on attribute and
  filter; start simple, note the optimization.
- **Update path needs the old value** to retract stale secondary entries: `put` does
  `store.get(key)` first (one extra read per indexed put — document the cost).
- **Consistency stance (write it in the javadoc verbatim):** secondaries update in the same
  critical section as the primary; there is no cross-index transaction log; a crash loses
  nothing because **all** indexes rebuild from the log — `IndexedStore.open` rebuilds
  secondaries by scanning the primary (`range` full sweep → extract → sort via SuperBeefSort →
  `fromSorted`).
- Retention interplay: v1 refuses `retainNewest > 0` + secondaries (evictions bypass
  `IndexedStore`, so secondaries would go stale). Throw at build time; note as future work
  (needs an eviction callback seam).

Tests: oracle = primary `TreeMap` + manually maintained `TreeMap<attr, Set<K>>`; byAttribute
ranges agree; delete retracts; reopen rebuilds secondaries identically.

### 4.2 Interval index

**Read CSRBT's `IntervalAugmentor` javadoc and `AugmentorCoexistenceTest` before designing** —
it is `Integer`-bound and tag-driven; do not fight that in v1. Recommended shape: an
`IntervalIndex` secondary flavor for records exposing `(int start, int end)` (epoch
minutes/seconds), storing interval-start-keyed entries with the interval in the node tag and
`IntervalAugmentor` computing max-hi, answering `stab(int point)` / `overlapping(int lo, int hi)`.
If the tag/augmentor route fights back after a day, fall back honestly: a plain secondary on
`start` with a max-interval-length bound scan — document the trade and move on. Tests: stabbing
oracle via brute-force list filter.

### 4.3 Auto-compaction

`SmokeHouseOptions.compactWhenGarbageAbove(double ratio)` (0 = off, default 0.5): the existing
pilot thread, after its morph evaluation, computes
`garbageBytes(closed) / totalBytes(closed)` and, above threshold, runs `compact()` — which
already does its copying outside the lock, so the pilot thread doing it is safe. Guard against
re-entry (a `compacting` flag). Test: churn past the threshold with a short cadence, poll until
segment count drops, then oracle-verify.

### 4.4 The store dashboard (the shop window)

Port **8079**, loopback, JDK `HttpServer` — lift the SSE + page patterns from SuperBeefSort's
`AquariumServer`/`aquarium.html` (they're deliberately reusable). SmokeHouse needs the
`application` plugin added to `build.gradle.kts` with a `demo/StoreDashboard` main
(`./gradlew run`). Show: keys/puts/gets/deletes ticking; a **segment map** (one bar per
segment, fill = live vs garbage bytes — the garbage ledger finally visible); index strategy +
pilot verdict; a *Compact now* button (`/compact` endpoint → runs on an HTTP thread — safe,
see 4.3); and a built-in demo workload (steady churn with regime flips) toggleable like the
percentile service's. Drive it, watch a compaction visibly squash the segment map. That's the
Phase 4 money shot.

**Phase 4 definition of done:** all green including new oracle suites; dashboard runs and a
manual compaction visibly reclaims; READMEs + ADR checkboxes updated; commit
`"Phase 4: IndexedStore secondaries, interval index, auto-compaction, store dashboard"`.

---

## After Phase 4

The ADR ring is closed. The remaining ecosystem plays are audience, not organs: Maven Central
publishing (csrbt-core first), the evolution-machine essay, and — if the store grows real
users — the deferred designs recorded in the ADR: ensemble index tier, a CSRBT `replace` seam
if upsert profiling justifies it, generic interval endpoints, and the LSM ADR if anyone ever
outgrows keys-in-memory.
