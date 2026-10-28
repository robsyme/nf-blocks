# Background store indexing in the explorer

Status: for review, 2026-10-27. Builder check done; open calls marked "orchestrator's call".

## 1. Why

`nextflow plugin nf-blocks:explore` catches the index up over every store
member before it prints its URL. A member with no Index Snapshot is indexed
from every block: on S3 that is one listing of `blocks/` and then a HEAD and
a GET per metadata block, one at a time. On a large member this takes long
enough that the explorer looks broken, and the only sign of progress is one
warning on stderr:

```
store member 'cloud' has no usable Index Snapshot (absent); indexing it from every block instead, which is slow on a large member. `nextflow plugin nf-blocks:snapshot` against it writes one.
```

The user wants the explorer to start straight away and stay usable, and to
index a large member only when they ask, with visible progress.

## 2. Goals

- `explore` serves at once. No full block scan happens at startup, or inside
  any request, or at exit, unless the user asked for that member to be
  indexed.
- A member that has neither an Index Snapshot nor a local index is
  unavailable: the page shows it as such, as it already does for a member with
  no snapshot, and the other members browse normally.
- The Storage page offers, per unavailable member, an Index action with two
  modes:
  - **Index locally**: scan the member into the local index cache and serve
    the page a snapshot built from it. Nothing is written to the member. Kept
    across restarts.
  - **Index and push**: the same, then write the snapshot into the member
    (`index/v7.sqlite`), so every user of that member gets it.
- While a job runs, the Storage page shows its progress and updates on its
  own. When it finishes, the member becomes available without a restart.
- A locally indexed member stays browsable after a restart, with a quick
  top-up (Store Log entries since the watermark) instead of a rescan.

## 3. Non-goals

- Making the full scan faster (parallel reads, resumable scans). Worth doing
  later; this design only moves it off the critical path and shows progress.
- The writable member. It is indexed at startup as today; Put, handoffs and
  the writable member's own snapshot depend on it. A large writable S3 member
  with no snapshot still blocks startup (see §8).
- Indexing several members at once. One job at a time.
- Background indexing in pipeline runs or other plugin verbs. Only `explore`
  changes; `nf-blocks:snapshot`, `reindex` and runs keep today's behaviour.

## 4. Anti-goals

- A request (save, export, copy plan, handoff) that silently starts a full
  scan of an unindexed member. That is the current failure in a new place.
- A member that looks browsable while its data is missing or partial. A
  member is either available with a complete index or unavailable with a
  reason; never half-loaded.
- Writing to a member the user did not ask to write to. Only "Index and push"
  writes: `index/v7.sqlite`, and the member's `store-id` when it has none
  (orchestrator's call).
- Messages tailored to one user's setup (a named AWS profile, `aws sso
  login`). Every message must read right for any user.

## 5. What the user sees

### 5.1 Startup

- `explore` prints its URL as soon as the writable member is caught up and
  its snapshot rewritten (as today), and every other member has been seeded
  from its snapshot or topped up from its watermark.
- A member with no usable snapshot and no local index is not scanned. stderr
  gets one line instead of today's warning, worded for any user:
  `store member 'cloud' has no Index Snapshot, so the explorer does not list it. Index it from the Storage page, or run nf-blocks:snapshot against it.`
- A locally indexed member (from an earlier session) is available at once
  from its kept local snapshot, which may be stale. A background top-up then
  catches it up from the watermark and rebuilds the local snapshot; the page
  picks up the new one when the top-up finishes (§6.6).

### 5.2 Storage page

Each member card shows its index state:

| State | Card says | Actions |
|---|---|---|
| Snapshot in the member | as today | none new |
| Local index (this machine) | "Indexed on this machine" and when | Push to store |
| Not indexed | "Not indexed: its runs and Selections are not listed" | Index locally, Index and push |
| Indexing | phase, progress, elapsed time | none (one job at a time) |
| Failed | the reason, in plain words | Retry |
| Not in this composition | "Not in `cas.resolve`, so the explorer does not index it" | none |

No Re-index (orchestrator's call): the top-up at each start (§6.6) adds new
Store Log entries, and a rescan can only add rows, never correct them.

- "Index and push" (and "Push to store") is disabled, with a reason, when the
  member cannot take a write: configured read-only (the copy judgement in
  `CopyTargets.judge`), or unreachable now (the member probe, as for copy
  destinations).
- While any job runs, the other members' Index buttons are disabled with
  "another store is being indexed".
- Progress text by phase, for example:
  - `Listing blocks…` then `Reading 1,240 of 8,615 metadata blocks · 2m 10s`
  - `Reading the Store Log…`, `Building the snapshot…`, `Pushing to cloud…`
- Unavailable members elsewhere in the page (sidebar Stores list, store
  switcher, copy destinations) keep today's "no snapshot" treatment; an
  indexing member reads "indexing…" there.

### 5.3 When a job ends

- Success: the member's card switches to its new state, and the page reloads
  that member's data (no full page reload needed).
- Failure: the card shows the reason and a Retry. Credentials failures use the
  same plain wording as the copy page (`S3Failures`). The local index keeps
  whatever was complete before the job; a scan that stopped part way leaves
  the member not indexed (a partial scan is never served).
- `explore` stopping mid-job ends the job; the member stays as it was before
  the job.

## 6. How it works (decided)

### 6.1 Which members a catch-up touches

`CasSession.catchUpIndex` gains a rule: a member that has never been indexed
in this cache (no watermark and no completed block scan) and has no usable
snapshot is left out, never scanned. Every caller in `explore` uses it:
startup `refresh`, Put, the samplesheet exporter, handoffs, the copy service
(plan and snapshot), and the exit refresh. The existing `leftOut` hook is the
place for this. Only the index job (§6.3) scans such a member.

`nf-blocks:snapshot`, `reindex` and pipeline runs are unchanged.

Which members count as indexed (orchestrator's call): the cache decides. A
member is indexed when this cache holds a completed block scan or a watermark
for it, whether that came from a snapshot seed, an earlier session's scan, a
pipeline run or `reindex`. A member whose own snapshot exists but cannot be
used (another incarnation, a watermark not in its log, unreadable) and that
the cache does not hold is not indexed: the page shows the reason and offers
Index, and once indexed its local snapshot is served over the member's own.

### 6.2 Index states and how the page learns them

`members.json` gains, per member, an `index` object:
`{ state: 'snapshot' | 'local' | 'none' | 'indexing' | 'failed', at, progress, reason }`,
where `progress` is `{ phase, done, total, startedAt }` while indexing. The
page polls `members.json` (or a small `api/index` status endpoint; builder's
choice) about once a second only while a job runs, and stops when none does.

### 6.3 The index job

- Started by `POST api/index` with `{ member, mode: 'local' | 'push' }`,
  behind the token and origin checks every POST already has. 409 when a job
  is running; 400 for an unknown member, the writable member, or push to a
  member that cannot take it.
- Runs on its own thread, with its own `Index` connection on explore's cache
  file, so Put is not held behind it. (A copy builds its own composition with
  the destination as writable member, so it uses a different cache file; see
  §6.8.)
- Steps: list blocks, read and ingest metadata blocks (the existing scan, with
  a progress callback added), read the Store Log and set the watermark, build
  the member's snapshot to the local snapshot file (§6.4), and in push mode
  write it into the member through that member's `SnapshotStorage`.
- Marks the scan complete only at the end, so an interrupted job leaves the
  member not indexed.

### 6.4 Local snapshots

- Kept next to the cache file, one per member, in a directory keyed like the
  cache (for example `<cache>.snapshots/<alias>/index/v7.sqlite`).
- Built with `IndexSnapshot.build` for that member, exactly as
  `nf-blocks:snapshot` would build it against that member: its presence check
  and its `sweep.lock` read (orchestrator's call).
- A member the cache holds but whose local snapshot file is missing (indexed
  before this change, by a run, by `reindex`, or the file was deleted) gets its
  local snapshot rebuilt by a background job at startup, with progress; it is
  unavailable on the page until that finishes. Startup itself never waits.
- Served by the explore server at the member's usual URL,
  `m/<alias>/index/v7.sqlite`, when the member has no usable snapshot of its
  own (§6.1), through
  a `MemberFiles` wrapper. The page then browses it unchanged; its Store Log
  tail still comes from the member.
- A member with a usable snapshot of its own always serves its own; the local
  one is never preferred over it.

### 6.5 Push

Builds the snapshot as §6.4 says and writes `index/v7.sqlite` into the
member (creating its `store-id` when absent, nothing else: no `index.html`),
with the same conditional write
the writable member's snapshot uses, with its existing guards (fewer runs,
older watermark, replaced meanwhile). A guard that refuses the write is a
failure with its reason; the local index is still kept.

### 6.6 Restart and top-up

On startup, a member with a local index and no snapshot of its own is served
from its kept local snapshot at once. A background top-up (log since the
watermark, then rebuild the local snapshot) runs as a job with progress like
any other. When the rebuilt snapshot replaces the old one, the page must
re-open that member: a file replaced under open reads fails with
`snapshot_changed`, so the page invalidates the member when the job ends.

### 6.7 Messages

All new text is generic. Credentials failures reuse `S3Failures` wording. No
profile names, commands for one auth method, or store names other than the
member's own alias.

### 6.8 Copies (decided by the user, 2026-10-27)

A copy builds its own composition with the destination as writable member,
so it uses its own cache file, not explore's.

- **Into a member that is not indexed:** the copy runs as today (blocks,
  records, Claims), but does not rebuild the destination's snapshot, since that
  would mean the full scan inside the request. The copy's result says the
  destination is not indexed yet and points at the Storage page's Index
  action. Its catch-up leaves the unindexed destination out (§6.1). The plan's
  "already in <destination>" figure cannot come from an index there; the
  builder decides whether the plan asks the destination directly or says the
  figure is unknown.
- **From a member indexed only on this machine:** the copy reads the source's
  runs, Selections and closures from the explorer's local index, so copying
  works without pushing first. How the copy's composition gets at those rows
  (seed its cache from the source's local snapshot, or read explore's cache)
  is the builder's choice; the copy must not scan the source.

## 7. Constraints

- SQLite: one writer at a time across connections (WAL, `busy_timeout`
  5000 ms). The job must write in transactions short enough that Put and
  copies do not hit `SQLITE_BUSY`.
- The cache file is per composition (keyed by member locations) and is wiped
  when a member's store-id changes; a local index and its local snapshot go
  with it. The local snapshot directory must follow the same lifetime.
- Explorer requests run on an 8-thread pool; the job must not occupy one.
- Tests run with scratch `XDG_CACHE_HOME`/`NXF_HOME`; nothing touches the real
  store.

## 8. Left to the builder

- Whether progress rides on `members.json` or a separate status endpoint.
- How progress is counted in the scan (callback shape, how often it reports).
- Transaction sizing for the scan to keep writers unblocked.
- Card layout: the cards are `<a>` elements today and cannot hold buttons;
  restructure as needed.
- Whether the writable member, when it has no snapshot and is on S3, should
  get the same treatment later (§3 leaves it out).
- How the page re-opens a member after a job (invalidate in place, or a soft
  reload of that member's views).
