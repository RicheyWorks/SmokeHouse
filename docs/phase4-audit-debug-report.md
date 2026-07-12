# Phase 4 Debug Audit — SmokeHouse

Fresh-eyes bug hunt over the newest, least-battle-tested code: the Phase 4 surfaces
(`IndexedStore`, the interval index, the `StoreDashboard`/SSE shop window) and the store's
concurrency and crash-recovery seams. Hunting for real correctness bugs, not style.

**Verdict:** three real bugs, all now **fixed, tested, and committed** on `main`; the build is
green. `RecordCodec`, `HintFile`, `IndexedStore`, and the read-path retry were inspected closely
and are clean.

| # | Bug | Severity | Fix commit | Regression test |
|---|-----|----------|------------|-----------------|
| 1 | Compaction recovery deletes the merged segment (data loss) | high | `c02851f` | `compactionSurvivesCrashAfterRenameBeforeMarkerCleared` |
| 2 | `compact()` has no concurrency guard | high | `47c6aae` | `concurrentCompactionsDoNotCorruptTheStore` |
| 3 | `close()` interrupts an in-flight fsync `force()` (flaky `ClosedChannelException`) | medium | `4e50663` | covered indirectly by the empty-store close test |

Test commit: `4070058`.

---

## Finding 1 — Compaction crash recovery deletes the merged segment (data loss) — FIXED

Severity **high** (silent, total loss of every compacted record). Fixed in
`SegmentLog.finishPendingCompaction` (`c02851f`); regression test added (`4070058`).

### Reproduction
- **Expected:** a crash at any point during a compaction commit replays or rolls back to a
  consistent state on the next `open`, losing nothing.
- **Actual (pre-fix):** a crash *after* the merged segment is renamed into place but *before* the
  `COMPACT_READY` marker is deleted caused recovery to delete the merged segment, destroying every
  live record the compaction had just preserved.
- **Trigger:** `commitCompaction` completes `Files.move(tmp → seg-maxId)` and dies before
  `Files.deleteIfExists(ready)`. On restart `COMPACT_READY` exists and `COMPACT_TMP` does not.

### Root cause
The original recovery loop deleted the whole inclusive range and only *then* looked for the scratch:

```java
for (int id = minId; id <= maxId; id++) {          // deletes seg-maxId too
    Files.deleteIfExists(dir.resolve(segmentName(id)));
}
if (Files.exists(tmp)) {                           // false — tmp already renamed away
    Files.move(tmp, dir.resolve(segmentName(maxId)), StandardCopyOption.ATOMIC_MOVE);
}
```

When the crash lands after the rename, `seg-maxId` *is* the merged replacement. The loop deletes
it, `tmp` no longer exists so nothing recreates it, and recovery rebuilds from whatever older
segments survive (none — deleted first), silently losing the compacted keys.

### Fix
`seg-maxId` is now only deleted when the scratch still needs renaming into it. A torn (partially
written) marker is also handled — deletes begin only after the marker is force-durable, so a torn
marker proves nothing was deleted yet.

```java
int minId, maxId;
try {
    String[] range = Files.readString(ready, StandardCharsets.UTF_8).trim().split(" ");
    minId = Integer.parseInt(range[0]);
    maxId = Integer.parseInt(range[1]);
} catch (RuntimeException tornMarker) {
    Files.deleteIfExists(tmp);                     // marker never committed: discard
    Files.deleteIfExists(ready);
    return;
}
for (int id = minId; id < maxId; id++) {           // maxId handled below
    Files.deleteIfExists(dir.resolve(segmentName(id)));
}
if (Files.exists(tmp)) {                           // crash before the rename: finish it
    Files.deleteIfExists(dir.resolve(segmentName(maxId)));   // the pre-compaction original
    Files.move(tmp, dir.resolve(segmentName(maxId)), StandardCopyOption.ATOMIC_MOVE);
}
// No tmp: the rename already committed, seg-maxId is the merged segment — keep it.
```

| On-disk state | Meaning | Recovery action |
|---|---|---|
| no marker | never committed | discard scratch, done |
| torn marker | crash mid-marker-write, before force | discard scratch, done (no delete happened) |
| marker + scratch | crash before/at rename | delete `minId..maxId` originals, rename scratch in |
| marker, **no** scratch | rename already committed | delete `minId..maxId-1` only, **keep `seg-maxId`** |

Consistent with `commitCompaction`, which force-durables the marker *before* the first delete —
the invariant the torn-marker branch relies on.

### Regression test
`Phase2Test.compactionSurvivesCrashAfterRenameBeforeMarkerCleared` reconstructs the
post-rename/pre-marker-clear state (merged `seg-maxId` on disk, no scratch, `COMPACT_READY`
present) and asserts the store still agrees with the oracle after reopen. It fails against the old
loop and passes against the fix.

---

## Finding 2 — `compact()` had no concurrency guard; overlapping compactions corrupt the store — FIXED

Severity **high** impact (store/index corruption or a spurious failure). Fixed in `SmokeHouse`
(`47c6aae`); regression test added (`4070058`).

### Reproduction
- **Expected:** at most one compaction runs at a time.
- **Actual (pre-fix):** two `compact()` calls could run concurrently. The `compacting` re-entry
  guard was set and cleared **only** inside `maybeAutoCompact`; the public `compact()` neither
  checked nor set it.
- **Reachable paths:** `StoreDashboard.serveCompact` calls `store.compact()` on HTTP worker
  threads — a double-click on *Compact now*, or two tabs, issues two concurrent compactions. Also
  `IndexedStore.primary().compact()` (or any app code) run while the pilot is auto-compacting.

### Root cause
The off-lock copy phase opens a single fixed scratch file:

```java
Files.deleteIfExists(dir.resolve(COMPACT_TMP));                 // races the other compaction
return FileChannel.open(dir.resolve(COMPACT_TMP),
        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
```

Both compactions gather the same victim range, release the lock, then race in the copy phase. The
second `openCompactionTmp` deletes the first's in-progress scratch — on POSIX the commit renames the
wrong file into `seg-maxId` and both threads repoint the index for the same victims (data loss +
ledger corruption); on Windows the delete of an open file throws `AccessDeniedException` (a spurious
500). Either way the single-writer contract is violated by a path the shipped dashboard exercises.

### Fix
`compact()` now owns the guard: it check-and-sets `compacting` inside its first critical section and
clears it in a `finally`, so overlapping callers serialize (the loser no-ops with `return 0`, the
established "skip if already compacting" idiom). `maybeAutoCompact` no longer manages the flag — it
relies on `compact()` to self-guard, so the pilot can't lock itself out.

```java
synchronized (lock) {
    if (compacting) {
        return 0;                              // a compaction is already in flight
    }
    // ... gather victims ...
    compacting = true;                         // serialize compactions across the off-lock copy
}
try {
    // ... copy phase + commit ...
} finally {
    compacting = false;
}
```

### Regression test
`Phase2Test.concurrentCompactionsDoNotCorruptTheStore` fires four `compact()` calls at once behind
a `CountDownLatch`, rethrows any per-call failure via `Future.get()`, and asserts the store still
agrees with the oracle. It catches the corruption on POSIX and the spurious throw on Windows.

---

## Finding 3 — `close()` interrupts an in-flight fsync `force()`, flaking `ClosedChannelException` — FIXED

Severity **medium** (intermittent close failure; surfaced as a flaky test). Fixed in
`SegmentLog.close` (`4e50663`).

### Reproduction
- **Symptom:** `CsrbtUnlockTest.orderStatisticsOnAnEmptyStore` failed intermittently with
  `java.nio.channels.ClosedChannelException` thrown from the try-with-resources `close()`.
- **Condition:** the default options use `Fsync.INTERVAL` at 50 ms (`SmokeHouseOptions.java:96`), so
  a background `smokehouse-fsync` daemon calls `active.force(false)` every 50 ms.

### Root cause
`SegmentLog.close()` called `fsyncDaemon.shutdownNow()`, which interrupts the daemon thread. If the
interrupt lands while the daemon is inside `FileChannel.force()`, the interrupt **closes** the
channel (a `FileChannel` is an `InterruptibleChannel`, so an interrupt during I/O closes it and
throws `ClosedByInterruptException`, swallowed by the daemon's `catch (IOException)`). `close()`'s
own follow-up `active.force(false)` then hits the now-closed channel and throws
`ClosedChannelException`. It only manifests when the timing overlaps — hence flaky.

### Fix
Shut the daemon down gracefully so the in-flight `force()` finishes instead of being interrupted,
and guard the final force against an already-closed channel:

```java
if (fsyncDaemon != null) {
    fsyncDaemon.shutdown();                        // let an in-flight force() finish...
    try {
        if (!fsyncDaemon.awaitTermination(5, TimeUnit.SECONDS)) {
            fsyncDaemon.shutdownNow();             // ...only interrupt a daemon that overran
        }
    } catch (InterruptedException interrupted) {
        fsyncDaemon.shutdownNow();
        Thread.currentThread().interrupt();
    }
}
synchronized (this) {
    if (active.isOpen()) {                         // an interrupted force() may have closed it
        active.force(false);
    }
    active.close();
}
```

`SmokeHouse.close()` already forces the log before `log.close()`, so data durability is unaffected;
this only removes the interrupt-closes-the-channel race.

---

## Minor — dashboard SSE escaping does not strip control characters (not fixed; demo-only)

`StoreDashboard.esc` escapes `\` and `"` but not newlines or other control characters. A newline
reaching the SSE stream — most plausibly via an `IOException` message routed through
`publish("... " + esc(storeTrouble.getMessage()) + " ...")` — would break the `data: ...\n\n` frame
and the surrounding JSON. Harmless for a loopback-only exhibit; if this plumbing is ever reused for
anything non-local, escape control characters (or JSON-encode properly) as well.

---

## Inspected and clean
- **`RecordCodec` / `HintFile`** — CRC-gated, length-validated; torn tails handled.
- **`IndexedStore`** — validate-before-write fan-out, correct old-entry retraction on put/delete,
  interval sidecar resolves candidates to exact keys in `(start, end, key)` order with the tag as a
  never-pruning upper bound, and the probe-rank comparator brackets ranges with no null-key NPE
  path. `retainNewest` + indexes correctly refused at build.
- **Read-path retry (`SmokeHouse.get`)** — index resolved under the lock, log read outside it, one
  retry to survive a compaction commit that repointed the entry mid-read; cached readers are closed
  before their segments are deleted, so an overlapping read fails cleanly and re-resolves. Correct
  given single-compaction discipline — which Finding 2's guard is what actually enforces.

---

## Status

All three fixes and both regression tests are committed on `main`; `./gradlew build` is green,
including the previously-flaky close test. No open items from this audit remain in the code.
