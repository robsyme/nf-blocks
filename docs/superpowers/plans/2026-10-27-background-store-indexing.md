# Background Store Indexing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `nf-blocks:explore` prints its URL without scanning any member that has neither an Index Snapshot nor a local index; the Storage page offers Index locally, Index and push, and Push to store per such member, runs one job at a time on its own thread with visible progress, and makes the member browsable when the job ends, also across restarts.

**Architecture:** `CasSession` gains an opt-in rule (explore and the copies it runs turn it on) that leaves a non-writable member out of every catch-up when the cache does not hold it and its own snapshot cannot seed it. A new `IndexJobs` in the explore package owns each member's index state and one job thread. A job scans the member into a scratch Index file (never explore's cache, so no SQLite write lock is held across block reads), builds the member's snapshot from it, seeds explore's cache from that snapshot in one transaction under Put's monitor, and keeps the snapshot as the member's local snapshot beside the cache file; push mode then writes it into the member. The server serves the local snapshot at the member's usual URL, reports states in `members.json` and `GET /api/index`, and takes `POST /api/index`; the page polls once a second while a job runs and invalidates the member when it ends.

**Tech Stack:** Groovy 4 / Spock (`./gradlew test`), SQLite via JDBC (WAL), the JDK `HttpServer`, plain ES modules in `web/src` with `node:test` and the fake DOM in `web/test/dom.mjs`.

**Spec:** `docs/superpowers/specs/2026-10-27-background-store-indexing-design.md` (approved; binding). DESIGN.md is the project contract; Task D1 amends it.

## Global Constraints

- Only `explore` changes. `nf-blocks:snapshot`, `reindex`, `copy` from the CLI and pipeline runs keep today's behaviour: the CasSession rule is off unless `leaveUnindexedOut` is called, and `Index.catchUp`'s existing overloads still scan.
- The writable member is indexed at startup as today and is never left out (spec §3).
- No full block scan happens at startup, inside any request, or at exit (spec §2). Only an `IndexJobs` job calls `Index.indexMember`.
- A member is indexed when the cache holds `block_scan:<alias>` or `store_log_watermark:<alias>` (spec §6.1, orchestrator's call).
- Never half-loaded: explore's cache receives a scanned member's rows in one `seedFrom` transaction at the end of a successful scan, or not at all; a stopped or failed job leaves the member as it was (spec §4, §5.3).
- Only Index and push and Push to store write to a member: `index/v7.sqlite`, and `store-id` when the member has none; never `index.html` (spec §4, §6.5).
- One job at a time, on its own daemon thread `nf-blocks-index`, never on the server's 8-thread pool (spec §3, §7).
- `members.json` member key `index`, exactly: `{ state, at, progress, reason, mode, local }` with `state` one of `snapshot`, `local`, `none`, `indexing`, `failed`, `outside`; `progress` is `{ phase, done, total, startedAt }` while indexing, else null; `phase` one of `listing`, `reading`, `ingesting`, `log`, `snapshot`, `push`. `GET /api/index` answers `{"members": {"<alias>": <index>}}`.
- `POST /api/index` body `{ "member": "<alias>", "mode": "local" | "push" }`, behind `admitted()` (token, origin, `application/json`, 2 MiB). `202 {"member", "mode", "index"}`; `409 {"error": "index_running"}`; `400 {"error", "message"}` with `unknown_member`, `writable_member`, `not_writable`, `already_indexed` or `bad_request`.
- The page polls about once a second, only while some member's state is `indexing`.
- Local snapshot file: `<cache file>.snapshots/<alias>/index/v7.sqlite` (`IndexPaths.localSnapshotsDir`). Deleted with the cache's reset; served only while the cache holds the member; never served over a usable snapshot of the member's own (spec §6.4, §7).
- Startup line, verbatim (spec §5.1): `store member 'cloud' has no Index Snapshot, so the explorer does not list it. Index it from the Storage page, or run nf-blocks:snapshot against it.`
- All new text is generic (spec §4, §6.7): no AWS profile names, no `aws sso login`, no store name but the member's own alias. Credentials failures use `S3Failures` wording; other failures `CopyErrors.describe` (paths redacted). Writing style: plain, no em-dash runs, no bold-first bullets.
- Clean code over compatibility: no migrations or shims; the user is the only user of these formats.
- The real store `/tmp/cas-exploration/store` is never opened by anything that can write: no `sqlite3` without `-readonly`, no Index or CasSession, no `nf-blocks:*` command, no test pointed at it. Tests use temp dirs, local directory members and `MemoryS3Ops`/`FakeS3`. Any run that opens an Index sets `XDG_CACHE_HOME` and `NXF_HOME` to a scratch directory.
- Never symlink `web/node_modules` between worktrees; run `npm ci` in each.
- Quote heredoc delimiters (`<<'EOF'`) whenever the body has backticks or `$`.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Implementers commit on their own worktree branch; the orchestrator integrates by cherry-pick.
- Test commands: Spock `./gradlew test --tests '<FQCN>'`; web `cd web && npm ci && npm run gen && node --test test/<name>.test.mjs`; full `./gradlew test` and `cd web && npm test`.

## Review Focus

- A save, export or handoff while a large member is being indexed: it completes at once, with no `SQLITE_BUSY`, because the scan writes a scratch file and only the final seed touches explore's cache, under Put's monitor. Pinned in Task S1 (`a write to the cache while a job scans is not held behind it`).
- Credentials that expire part way through a scan (block reads fail after the listing worked): the job fails with the `S3Failures` wording and the member stays not indexed. Today `kindAt` swallows a read failure and `scanOnce` sets the watermark after a failed listing, so this would silently index a partial member. Pinned in Task C1 (`a read that fails mid-scan throws and marks nothing`) and Task S1 (`an S3 member whose reads are refused ends failed with the credentials wording`).
- Stopping explore during a job: the exit is bounded, the member stays as it was, and the next start lists it as `none`. Pinned in Task S1 (`stop() ends a running job and leaves the member as it was`) and Task S3 (`the exit stops a running job before the final refresh`).
- A member whose own snapshot is from another incarnation (its store was replaced): it reads `none` with that reason, Index works, and the local snapshot is then served over the stale own one, also after a restart. Pinned in Task S1 (`an own snapshot from another incarnation leaves the member none; once indexed, the local one is served`).
- A copy into a member that is not indexed, or from one indexed only on this machine: no scan of either, the result says the destination is not indexed, and the source's runs come from its local snapshot. Pinned in Task CP1 (`a copy into an unindexed destination does not scan it and says so`, `a copy from a member indexed only on this machine seeds from its local snapshot`).

---

## Decisions this plan makes (the spec's builder's choices)

- Progress rides on a separate `GET /api/index`, so polling never runs `members.json`'s member probe. `members.json` carries the same object for the first render.
- Progress is reported per block read through `IndexProgress.report(phase, done, total)`; the job's callback stores it in volatile fields and checks for interruption.
- Transaction sizing: the scan writes a scratch Index, not explore's cache. `Index.ingestRun` holds one transaction across all of a run's block reads (S3 GETs and HEADs inside `withTransaction`), so scanning into explore's cache on a second connection would hold SQLite's write lock for as long as a large run takes to read, and Put's 5 s `busy_timeout` would expire. The final `seedFrom` is local SQL only and runs under Put's monitor, so a save waits on the monitor instead of failing.
- Cards: each non-open member card becomes a `div` holding a head link (`a[data-storage-card-open]`, the old card's href and title) and an actions row.
- After a job, the page invalidates that member in place (`members.invalidate`, `sizes.forget`) and re-renders only when the Storage page is showing; no full reload.
- The writable member keeps today's treatment (spec §3); not revisited here.
- Push to store on a `local` member skips the scan: top-up, then write from explore's cache. Retry repeats the failed job's `mode`.
- A copy into a `local` member also skips the destination snapshot (writing one would be a push the user did not ask for) and queues a refresh of its local snapshot.
- `members.json` lists every configured store (`cas.stores`), not only `cas.resolve`; a member outside `cas.resolve` gets state `outside`, which the spec's enum lacks but its §5.2 table needs.
- The plan's "already in the destination" figure needs no change: `CopyService.plan` already leaves the destination out of its catch-up and asks it directly (`presentAt`, `destinationState`).

---

## Dependency graph

An arrow means "must be integrated before".

```
C1 Index: indexed, trySeed, indexMember ---> C2 CasSession rule and member snapshots ---+--> CP1 CopyService indexing --+
                                                                                       +--> S1 IndexJobs --------------+--> S2 ExploreServer --> S3 ExploreCommand
W1 indexing.js ---+--> W2 Storage cards
                  +--> W3 app wiring, switcher, nav, copy result
D1 DESIGN.md and README (no code dependency)
everything ---> Final acceptance (orchestrator checklist)
```

| Task | Needs | Files it owns |
|---|---|---|
| C1 | none | `core/Index.groovy`, `core/IndexPaths.groovy`, `core/IndexProgress.groovy` (new), `IndexMemberScanTest` (new), `IndexPathsTest` |
| C2 | C1 | `CasSession.groovy`, `CasSessionUnindexedTest` (new) |
| CP1 | C2 | `CopyService.groovy`, `CopyServiceIndexingTest` (new) |
| S1 | C2 | `explore/IndexJobs.groovy` (new), `IndexJobsTest` (new) |
| S2 | S1, CP1 | `explore/ExploreServer.groovy`, `explore/LocalSnapshotFiles.groovy` (new), `ExploreIndexTest` (new) |
| S3 | S2 | `explore/ExploreCommand.groovy`, `ExploreCommandTest` |
| W1 | none | `web/src/indexing.js` (new), `web/test/indexing.test.mjs` (new) |
| W2 | W1 | `web/src/views/storage.js`, `web/src/styles.js`, `web/test/storage-index.test.mjs` (new), `web/test/all-stores.test.mjs` |
| W3 | W1 | `web/src/app.js`, `web/src/store.js`, `web/src/nav.js`, `web/src/views/switcher.js`, `web/src/views/copy.js`, `web/test/store.test.mjs`, `web/test/nav.test.mjs`, `web/test/switcher.test.mjs`, `web/test/copy-view.test.mjs` |
| D1 | none | `DESIGN.md`, `README.md` |

No two tasks own the same file. Groovy paths are under `src/main/groovy/robsyme/cas/`, tests under `src/test/groovy/robsyme/cas/` in the same package as the class.

---

## Core lane

### Task C1: Index knows which members it holds, seeds without scanning, and scans one member with progress

**Files:**
- Create: `core/IndexProgress.groovy`
- Modify: `core/Index.groovy` (7-arg `catchUp` ~1191, `warnUnusable` ~1222, `reset` ~2035, `blocksOfKinds` ~2212, `kindAt` ~2236), `core/IndexPaths.groovy`
- Test: `core/IndexMemberScanTest.groovy` (new), `core/IndexPathsTest.groovy`

**Interfaces:**
- Produces:
  - `static Path IndexPaths.localSnapshotsDir(Path cacheFile)`: `cacheFile.resolveSibling(cacheFile.fileName.toString() + '.snapshots')`.
  - `interface IndexProgress` with `static final String LISTING = 'listing', READING = 'reading', INGESTING = 'ingesting', LOG = 'log', SNAPSHOT = 'snapshot', PUSH = 'push'` and `void report(String phase, long done, long total)` (`-1` for unknown). Whatever it throws propagates out of the scan.
  - `boolean Index.isIndexed(String member)`: the cache holds `store_log_watermark:<member>` or any `block_scan:<member>` value.
  - `String Index.trySeed(BlockStore store, StoreLog storeLog, String member, SnapshotStorage snapshots, Path tempDir)`: null once seeded (or when the member already has a watermark); else the `SnapshotUnusable` reason (`absent`, `unreadable`, `schema_version <n>`, or the two incarnation messages). Never scans, never warns. An exception other than `SnapshotUnusable` from `snapshots.fetch` returns `unreadable`.
  - `void Index.indexMember(BlockStore store, StoreLog storeLog, String member, BlockStore holders, IndexProgress progress) throws IOException`: reports `LISTING -1 -1`, lists, reports `READING 0 total` and then `READING i total` after each DAG-CBOR block (total = DAG-CBOR blocks listed), ingests the found kinds in `SCANNED_KINDS` order reporting `INGESTING i total` (total = records found), reports `LOG -1 -1` and runs the missing-block retries and the Store Log since null, then in one transaction sets the watermark (newest entry, when any) and `block_scan:<member>` (`done`, or `empty` when nothing was found). An exception from `listBlocks`, `size`, `open` or reading a block propagates and nothing is marked; a block that reads but does not decode is skipped as today.
  - `Index.reset()` also deletes `IndexPaths.localSnapshotsDir(file)` recursively after its transaction commits; a failed delete logs at debug.
  - The 7-arg `catchUp` calls `trySeed` and, on a reason, `warnUnusable` and the scan as today (behaviour unchanged).

- [ ] **Step 1: Write the failing tests** in `IndexMemberScanTest.groovy` (Spock; stores built with `LocalBlockStore` and `Fixtures` as `IndexSeedTest` does; a failing store is a delegating `BlockStore` like `IndexTest.Counting`)

```groovy
def 'a member is indexed once the cache holds its watermark or a block scan'() {
    expect: !index.isIndexed('cloud')
    when: index.catchUp(emptyCloud, StoreLog.of(emptyCloud), 'cloud')
    then: index.isIndexed('cloud')
}

def 'trySeed seeds from a usable snapshot and otherwise says why, reading no block'() {
    given: def counting = new IndexTest.Counting(cloudWithTwoRuns)
    expect:
    index.trySeed(counting, StoreLog.of(counting), 'cloud', new LocalSnapshotStorage(cloudRoot), tmp) == 'absent'
    counting.opens == 0 && index.countRows('run') == 0 && !index.isIndexed('cloud')
    when: writeSnapshotOf(cloudWithTwoRuns, cloudRoot)   // IndexSnapshot.write from a scratch index, as IndexSeedTest does
    then:
    index.trySeed(counting, StoreLog.of(counting), 'cloud', new LocalSnapshotStorage(cloudRoot), tmp) == null
    index.isIndexed('cloud') && index.countRows('run') == 2
}

def 'indexMember reports listing, every metadata block read, each record ingested, then the log'() {
    given: def seen = []
    when: index.indexMember(cloud, StoreLog.of(cloud), 'cloud', cloud, { p, d, t -> seen << [p, d, t] } as IndexProgress)
    then:
    seen*.getAt(0).unique() == ['listing', 'reading', 'ingesting', 'log']
    seen.findAll { it[0] == 'reading' }.last() == ['reading', dagCborBlocksIn(cloud), dagCborBlocksIn(cloud)]
    seen.findAll { it[0] == 'ingesting' }.last() == ['ingesting', 3L, 3L]          // two runs and a Selection
    index.isIndexed('cloud') && index.watermark('cloud') == StoreLog.read(cloud)[0].name
    index.metaValue('block_scan:cloud') == 'done'
}

def 'a read that fails mid-scan throws and marks nothing'() {
    given: def failing = failingAfter(cloud, 3, new IOException('expired'))   // open() throws from the 3rd call
    when: index.indexMember(failing, StoreLog.of(cloud), 'cloud', cloud, { p, d, t -> } as IndexProgress)
    then:
    thrown(IOException)
    !index.isIndexed('cloud') && index.watermark('cloud') == null && index.metaValue('block_scan:cloud') == null
}

def 'a progress callback that throws stops the scan and marks nothing'() {
    when: index.indexMember(cloud, StoreLog.of(cloud), 'cloud', cloud, { p, d, t -> if( p == 'reading' && d == 2 ) throw new CancellationException() } as IndexProgress)
    then: thrown(CancellationException); !index.isIndexed('cloud')
}

def 'reset removes the local snapshots kept beside the cache file'() {
    given: Files.createDirectories(IndexPaths.localSnapshotsDir(index.file).resolve('cloud/index'))
    when: index.reset()
    then: !Files.exists(IndexPaths.localSnapshotsDir(index.file))
}
```

In `IndexPathsTest`: `IndexPaths.localSnapshotsDir(Path.of('/c/nf-blocks/ab12.sqlite')) == Path.of('/c/nf-blocks/ab12.sqlite.snapshots')`.

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexMemberScanTest' --tests 'robsyme.cas.core.IndexPathsTest'`
Expected: FAIL, `isIndexed`, `trySeed`, `indexMember`, `IndexProgress` and `localSnapshotsDir` do not exist.

- [ ] **Step 3: Implement** in `core/IndexProgress.groovy`, `core/IndexPaths.groovy`, `core/Index.groovy`

`blocksOfKinds(store, kinds, progress)` gains the progress argument; with a null progress it is today's tolerant scan (`scanOnce`, `rebuild`), with one it calls a strict `kindAt` that catches only the decode failure and the size cap. `trySeed` is the body of today's 7-arg `catchUp` seeding branch, returning the reason instead of warning.

- [ ] **Step 4: Run to see them pass, plus the Index suites that cover catch-up and seeding**

Run: `./gradlew test --tests 'robsyme.cas.core.IndexMemberScanTest' --tests 'robsyme.cas.core.IndexPathsTest' --tests 'robsyme.cas.core.IndexSeedTest' --tests 'robsyme.cas.core.IndexTest' --tests 'robsyme.cas.core.IndexStoreIdTest' --tests 'robsyme.cas.CasSessionTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Index.groovy src/main/groovy/robsyme/cas/core/IndexPaths.groovy src/main/groovy/robsyme/cas/core/IndexProgress.groovy src/test/groovy/robsyme/cas/core/IndexMemberScanTest.groovy src/test/groovy/robsyme/cas/core/IndexPathsTest.groovy
git commit -m "index: say which members the cache holds, seed without scanning, scan one member with progress"
```

### Task C2: CasSession leaves unindexed members out and builds or pushes any member's snapshot

**Files:**
- Modify: `CasSession.groovy` (`catchUpIndex` 367-388, `snapshotWritable` 559-579, new methods beside them)
- Test: `CasSessionUnindexedTest.groovy` (new)

**Interfaces:**
- Consumes: `Index.isIndexed`, `Index.trySeed` (C1).
- Produces:
  - `void leaveUnindexedOut(Map<String, String> known)`: turns the rule on for this session; `known` (alias to reason) pre-fills what the rule has decided, so those members are skipped without a fetch.
  - The rule, inside the 3-arg `catchUpIndex`, for each non-writable member not in `leftOut`: when `index.isIndexed(alias)`, catch up as today; else when already decided, skip; else `index.trySeed(member, StoreLog.of(member), alias, seedSourceOf(alias), config.tmpDir)`; null means seeded, so catch up as today; a reason means record it, print `notListedLine(alias, reason)` through `notices.warn`, and skip. A skipped member is not in the returned failed set.
  - `Map<String, String> unindexed()`: a copy of the decided members (alias to reason).
  - `void seedFrom(String alias, SnapshotStorage storage)`: the rule seeds that member from `storage` instead of `snapshotsOf(alias)`.
  - `static String notListedLine(String alias, String reason)`: for `absent`, the Global Constraints line verbatim; else `store member '<alias>' has no usable Index Snapshot (<reason>), so the explorer does not list it. Index it from the Storage page, or run nf-blocks:snapshot against it.`
  - `BlockStore member(String alias)`: the composition member, or null.
  - `String ownSnapshotUnusable(String alias, Index index)`: null when `snapshotsOf(alias).base(tmpDir)` exists and `seedFrom` would accept it (its store id equals `index.storeIdOf(alias)`, or it has none and its watermark is null or in the member's Store Log); else the reason in `trySeed`'s words. Reads a HEAD (and a GET only for an uncounted S3 snapshot).
  - `String ensureStoreId(String alias)`: the member's store-id, created when absent; null when the member keeps none.
  - `IndexSnapshot.Built buildMemberSnapshot(Index index, String alias)`: `IndexSnapshot.build(index, alias, config.tmpDir, sweepGraceMillis(), present, 0L, sweepLock)` with that member's `has` and its `RetainedStore` retention storage, as `snapshotWritable` does for the writable member.
  - `IndexSnapshot.Result pushMemberSnapshot(Index index, String alias, SnapshotBase base, String storeId)`: `IndexSnapshot.write(index, alias, snapshotsOf(alias), 0L, base, config.tmpDir, storeId, false, sweepGraceMillis(), present, sweepLock)`; no page. `snapshotWritable` keeps its behaviour and shares the presence and sweep-lock helpers.

- [ ] **Step 1: Write the failing tests** in `CasSessionUnindexedTest.groovy` (composition `lab` writable local, `cloud` local with two logged runs and no snapshot; `cas.index.path` in the temp dir; `CasSession.forCommand(cfg, new PrintStream(err, true))`)

```groovy
def 'with the rule on, a member with no snapshot that the cache does not hold is left out, named once, never scanned'() {
    given: session.leaveUnindexedOut([:])
    when: 2.times { session.catchUpIndex(index) }
    then:
    err.toString('UTF-8').readLines() == ["store member 'cloud' has no Index Snapshot, so the explorer does not list it. Index it from the Storage page, or run nf-blocks:snapshot against it."]
    session.unindexed() == [cloud: 'absent']
    !index.isIndexed('cloud') && index.countRows('run') == labRuns
}

def 'with the rule on, a member whose snapshot seeds is caught up as before'()      // cloud gets index/v7.sqlite first: isIndexed, runs seeded, no line
def 'the writable member is never left out'()                                       // lab with runs and no snapshot: scanned, isIndexed('lab')
def 'a known member is skipped without a fetch, and a seed source override seeds from a local snapshot'() {
    // leaveUnindexedOut([cloud: 'absent']) with cloud holding index/v7.sqlite: still left out, no line.
    // A fresh session, seedFrom('cloud', new LocalSnapshotStorage(localRoot)) with the file under localRoot/index/v7.sqlite: seeded.
}
def 'without the rule, catch-up scans a member with no snapshot as before'()       // isIndexed('cloud'), the old warning line
def 'ownSnapshotUnusable: absent, another incarnation, usable'() {
    expect: session.ownSnapshotUnusable('cloud', index) == 'absent'
    // after writing a snapshot whose store_id differs from the cloud store-id file the cache recorded: starts with 'written for another incarnation'
    // after writing a matching one: null
}
def 'pushMemberSnapshot writes index/v7.sqlite and the store-id into a non-writable member, and no index.html'() {
    when:
    def id = session.ensureStoreId('cloud'); index.catchUp(...) /* cache holds cloud */; index.reconcileStoreIds([cloud: id], { a, w -> true })
    def r = session.pushMemberSnapshot(index, 'cloud', null, id)
    then:
    r.written && Files.isRegularFile(cloudRoot.resolve('index/v7.sqlite')) && Files.exists(cloudRoot.resolve('store-id'))
    !Files.exists(cloudRoot.resolve('index.html'))
}
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.CasSessionUnindexedTest'`
Expected: FAIL, `leaveUnindexedOut` and the other methods do not exist.

- [ ] **Step 3: Implement** the rule and methods in `CasSession.groovy`. The decided map is a `ConcurrentHashMap`; the rule flag is volatile (explore's job thread and request threads share the session).

- [ ] **Step 4: Run to see them pass, then the session and copy suites**

Run: `./gradlew test --tests 'robsyme.cas.CasSessionUnindexedTest' --tests 'robsyme.cas.CasSessionTest' --tests 'robsyme.cas.CasSessionS3Test' --tests 'robsyme.cas.CacheCoherenceTest' --tests 'robsyme.cas.CopyServiceTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/CasSession.groovy src/test/groovy/robsyme/cas/CasSessionUnindexedTest.groovy
git commit -m "session: an opt-in rule leaves unindexed members out of catch-up; build and push any member's snapshot"
```

### Task CP1: A copy follows the explorer's view of which members are indexed

**Files:**
- Modify: `CopyService.groovy` (`execute` ~158-170, `plan` ~305-316, `copy` ~896-912, the snapshot constants ~995)
- Test: `CopyServiceIndexingTest.groovy` (new; `CopyServiceTest`'s setup as the model)

**Interfaces:**
- Consumes: `CasSession.leaveUnindexedOut`, `seedFrom` (C2).
- Produces:
  - `@CompileStatic static final class CopyService.Indexing { final Map<String, String> unindexed; final Map<String, Path> localRoots }` with a two-argument constructor; `localRoots` maps an alias to the directory holding `index/v7.sqlite` built on this machine.
  - `static Outcome execute(Map config, Map request, boolean dryRun, PrintStream err, Supplier<Principal> signer, Indexing indexing)`; the 5-arg overload passes null, which changes nothing.
  - With `indexing`: the copy session calls `leaveUnindexedOut(indexing.unindexed)` and `seedFrom(alias, new LocalSnapshotStorage(root))` for each local root, before its first catch-up.
  - `static final String SNAPSHOT_NOT_INDEXED = 'not_indexed'`: the body's `snapshot` when `ask.to` is in `indexing.unindexed` or `indexing.localRoots`. The final catch-up and snapshot step are skipped, and err gets `nf-blocks:copy: '<to>' has no Index Snapshot, so this copy did not write one; index it from the explorer's Storage page`.

- [ ] **Step 1: Write the failing tests**

```groovy
def 'a copy into an unindexed destination does not scan it and says so'() {
    when: def out = CopyService.execute(config, [root: "selection:${sel}", to: 'cloud', from: 'lab'], false, errStream, null,
        new CopyService.Indexing([cloud: 'absent'], [:]))
    then:
    out.status == 200 && out.body.snapshot == 'not_indexed'
    !Files.exists(cloudRoot.resolve('index/v7.sqlite'))
    err.toString('UTF-8').contains("nf-blocks:copy: 'cloud' has no Index Snapshot, so this copy did not write one; index it from the explorer's Storage page")
    StoreLog.read(cloudStore).any { it.kind == StoreLogKind.TRANSFER }
    !copyCache().isIndexed('cloud')                    // copyCache(): Index.open on the copy composition's cache path
}

def 'a copy from a member indexed only on this machine seeds from its local snapshot'() {
    given: def root = buildLocalSnapshot('cloud')        // IndexSnapshot.build from a scratch index that indexed cloud, moved under root/index/v7.sqlite
    when: def out = CopyService.execute(config, [root: "run:${completion}", to: 'lab', from: 'cloud'], false, errStream, null,
        new CopyService.Indexing([:], [cloud: root]))
    then:
    out.status == 200
    copyCache().metaValue('seeded_from:cloud') != null   // seeded, not scanned
}

def 'a member the explorer lists as not indexed is left out of the copy catch-up'()   // other: 'absent' -> !copyCache().isIndexed('other')
def 'without explorer indexing a copy is as before'()                                   // 5-arg execute: snapshot 'written'
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.CopyServiceIndexingTest'`
Expected: FAIL, `CopyService.Indexing` does not exist.

- [ ] **Step 3: Implement** in `CopyService.groovy`.

- [ ] **Step 4: Run to see them pass, plus the copy suites**

Run: `./gradlew test --tests 'robsyme.cas.CopyServiceIndexingTest' --tests 'robsyme.cas.CopyServiceTest' --tests 'robsyme.cas.s3.S3CopyTest' --tests 'robsyme.cas.explore.ExploreCopyTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/CopyService.groovy src/test/groovy/robsyme/cas/CopyServiceIndexingTest.groovy
git commit -m "copy: leave members the explorer has not indexed out, seed from local snapshots, skip an unindexed destination's snapshot"
```

---

## Explore lane

### Task S1: IndexJobs, the index states and the one job thread

**Files:**
- Create: `explore/IndexJobs.groovy`
- Test: `explore/IndexJobsTest.groovy` (new)

**Interfaces:**
- Consumes: C1 (`IndexProgress`, `isIndexed`, `indexMember`, `seedFrom`, `IndexPaths.localSnapshotsDir`), C2 (`member`, `unindexed`, `ownSnapshotUnusable`, `ensureStoreId`, `buildMemberSnapshot`, `pushMemberSnapshot`, `catchUpIndex`, `openIndex`, `snapshotsOf`), `S3Failures.credentials`, `CopyErrors.describe`, `LocalSnapshotStorage`.
- Produces:
  - `IndexJobs(CasSession session, Index shared, Object putLock, Path localRoot, Map<String, ExploreServer.MemberInfo> info)`; `localRoot` is `IndexPaths.localSnapshotsDir(<cache file>)`; `putLock` is the explorer's `Put`. `@PackageScope Closure<Long> clock` and `@PackageScope Closure onReport` (called with each `(phase, done, total)` on the job thread; a test seam to observe or hold a job).
  - State constants `SNAPSHOT = 'snapshot'`, `LOCAL = 'local'`, `NONE = 'none'`, `INDEXING = 'indexing'`, `FAILED = 'failed'`, `OUTSIDE = 'outside'`; mode constants `MODE_LOCAL = 'local'`, `MODE_PUSH = 'push'`.
  - `Map<String, Object> stateOf(String alias)`: the Global Constraints `index` object, every key present (`at` and `startedAt` ISO millis via `Index.isoMillis`).
  - `Map<String, Map<String, Object>> states()`: every configured alias in config order.
  - `void classify()`: at startup, after the session's first catch-up. Writable: `snapshot`. Not in `cas.resolve`: `outside`. In `session.unindexed()`: `none` with that reason, and its local directory deleted. Held by the cache (`shared.isIndexed`): `snapshot` when `ownSnapshotUnusable` is null (local directory deleted); else `local` with `at` the file's mtime when its local snapshot exists, else `indexing` with `local` false; both queue a refresh. Anything else: `none` with reason `could not be read at start`.
  - `void startQueued()`: begins the queued refreshes on the job thread.
  - `IndexJobs.Answer start(String alias, String mode)`; `Answer { final int status; final Map<String, Object> body }`. 400 `unknown_member` (not configured), `writable_member`, `bad_request` (mode not `local`/`push`, or a member `outside`), `not_writable` (push and `info[alias].copyTarget` false; message `info.copyReason`), `already_indexed` (mode `local` on `local` or `snapshot`, or mode `push` on `snapshot`); 409 `index_running` (message `another store is being indexed; try again when it has finished`) while a job runs or is queued; else 202 `{member, mode, index}`.
  - Job steps. Index (state `none` or `failed` without a local snapshot): base = push ? `snapshotsOf(alias).base(tmp)` : null; storeId = push ? `ensureStoreId` : the member's id read; a scratch `Index.open(config.tmpDir.resolve("nf-blocks-index-${alias}-<random>.sqlite"))` recording that id (`reconcileStoreIds`); `scratch.indexMember(member, StoreLog.of(member), alias, session.store, progress)`; phase `snapshot`: `buildMemberSnapshot(scratch, alias)`; under `synchronized(putLock)`: `shared.seedFrom(built.file, alias, StoreLog.of(member))`; place the file with `new LocalSnapshotStorage(localRoot.resolve(alias)).replace(...)`; state `local`. Push then: phase `push`, `pushMemberSnapshot(scratch, alias, base, storeId)`; written: delete the local directory, state `snapshot`; refused: `failed`, `local` true, reason from the guard. Push on a `local` member: `ensureStoreId`, then under the put lock `session.catchUpIndex(shared)` (records the new id, tops up), then `pushMemberSnapshot` on a job-owned `session.openIndex()`. Refresh: under the put lock `session.catchUpIndex(shared)` (phase `log`), then `buildMemberSnapshot` on a job-owned connection (phase `snapshot`), place, state `local`. The scratch file (and `-wal`, `-shm`) and built files are deleted however the job ends.
  - Failure: state `failed`, `mode` kept, `at` now, `local` true when a local snapshot is still served, reason `reasonFor(e)`. `String reasonFor(Throwable e)`: `S3Failures.credentials(e)` when not null, else `CopyErrors.describe(e, CopyErrors.rootsOf(session.config.rawConfig))`. Guard reasons: `fewer_runs`: `the store's own snapshot lists more runs than this index, so it was kept`; `older_watermark`: `the store's own snapshot is newer than this index, so it was kept`; `replaced_meanwhile`: `another writer replaced the store's snapshot meanwhile, so it was kept`; `stale_store`: `the store was replaced while it was being indexed`; any other: the code.
  - Interruption (the progress callback throws `CancellationException` once the thread is interrupted, or an `InterruptedException` arrives): the state returns to what it was before the job; nothing is reported as failed.
  - `Path localSnapshot(String alias)`: the local snapshot file when it exists and the member's state serves it (`local`, or `indexing`/`failed` with `local` true); else null. `boolean servesOwn(String alias)`: state `snapshot` or `outside`.
  - `void copied(String alias)`: after a real copy into `alias`, queue a refresh when it serves a local snapshot.
  - `Map<String, String> unindexedForCopy()` (states `none`, `failed` without local, `indexing` without local, with their reasons) and `Map<String, Path> localRootsForCopy()` (members `localSnapshot` answers for, mapped to `localRoot.resolve(alias)`).
  - `void stop(long waitMillis)`: clears the queue, interrupts the running job, waits up to `waitMillis`. `@PackageScope boolean awaitIdle(long millis)` for tests.

- [ ] **Step 1: Write the failing tests** in `IndexJobsTest.groovy` (members: `lab` writable local; `cloud` local with two logged runs, no snapshot, no store-id; `ro` local, `readOnly: true`, no snapshot; `out` configured, not in `cas.resolve`; `cas.index.path` in the temp dir; the session from `CasSession.forCommand`, `leaveUnindexedOut([:])`, `catchUpIndex(shared)`, then `jobs.classify()`)

```groovy
def 'classify: the writable member snapshot, a member with no snapshot none with its reason, one outside cas.resolve outside'() {
    expect:
    jobs.stateOf('lab').state == 'snapshot'
    jobs.stateOf('cloud') == [state: 'none', at: null, progress: null, reason: 'absent', mode: null, local: false]
    jobs.stateOf('out').state == 'outside'
}

def 'index locally: each phase in order, then local; nothing is written to the member and the cache holds it'() {
    given: def phases = []; jobs.onReport = { p, d, t -> phases << p }
    when: def a = jobs.start('cloud', 'local'); jobs.awaitIdle(30_000)
    then:
    a.status == 202 && a.body.index.state == 'indexing'
    phases.unique() == ['listing', 'reading', 'ingesting', 'log', 'snapshot']
    jobs.stateOf('cloud').state == 'local' && jobs.stateOf('cloud').local
    Files.isRegularFile(localRoot.resolve('cloud/index/v7.sqlite')) && jobs.localSnapshot('cloud') != null
    !Files.exists(cloudRoot.resolve('index')) && !Files.exists(cloudRoot.resolve('store-id'))
    shared.isIndexed('cloud') && shared.countRows('run') == labRuns + 2
    !tmp.toFile().list().any { it.startsWith('nf-blocks-index-') }
}

def 'index and push writes index/v7.sqlite and a store-id into the member, no index.html, and serves its own'() {
    when: jobs.start('cloud', 'push'); jobs.awaitIdle(30_000)
    then:
    jobs.stateOf('cloud').state == 'snapshot' && jobs.localSnapshot('cloud') == null && jobs.servesOwn('cloud')
    Files.isRegularFile(cloudRoot.resolve('index/v7.sqlite')) && Files.exists(cloudRoot.resolve('store-id'))
    !Files.exists(cloudRoot.resolve('index.html')) && !Files.exists(localRoot.resolve('cloud'))
}

def 'push to store on a local member writes without scanning again'()            // phases == ['log', 'push'] or ['push'] (no 'listing'); state snapshot
def 'refusals: #alias #mode is #status #error'() {
    expect: jobs.start(alias, mode).with { [status, body.error] } == [status, error]
    where:
    alias   | mode    || status | error
    'nope'  | 'local' || 400    | 'unknown_member'
    'lab'   | 'local' || 400    | 'writable_member'
    'ro'    | 'push'  || 400    | 'not_writable'
    'cloud' | 'redo'  || 400    | 'bad_request'
    'out'   | 'local' || 400    | 'bad_request'
}
def 'one job at a time: a second start is 409 while the first runs'()           // onReport holds on a latch at 'reading'; start('ro', 'local') -> 409 index_running; 'already_indexed' after cloud is local

def 'a write to the cache while a job scans is not held behind it'() {
    given: def held = new CountDownLatch(1), release = new CountDownLatch(1)
    jobs.onReport = { p, d, t -> if( p == 'reading' ) { held.countDown(); release.await() } }
    jobs.start('cloud', 'local'); held.await()
    when: def t0 = System.nanoTime(); synchronized( putLock ) { shared.recordLogEntry(entry, 'lab') }
    then: (System.nanoTime() - t0) < 1_000_000_000L && !shared.isIndexed('cloud')
    cleanup: release.countDown(); jobs.awaitIdle(30_000)
}

def 'stop() ends a running job and leaves the member as it was'() {
    given: holds at 'reading' as above, without releasing
    when: def t0 = System.currentTimeMillis(); jobs.stop(5_000)
    then:
    System.currentTimeMillis() - t0 < 5_000
    jobs.stateOf('cloud').state == 'none' && !shared.isIndexed('cloud') && !Files.exists(localRoot.resolve('cloud'))
}

def 'an S3 member whose reads are refused ends failed with the credentials wording'() {
    // cloud is s3://b/cloud over a MemoryS3Ops subclass (CasSession.s3OpsFactory) whose get() throws
    // S3Exception.builder().statusCode(403).build() after the listing
    expect after the job: jobs.stateOf('cloud').state == 'failed' && jobs.stateOf('cloud').reason == S3Failures.REFUSED
    jobs.stateOf('cloud').mode == 'local' && !shared.isIndexed('cloud')
}

def 'restart: a locally indexed member is local at once and a refresh rebuilds its snapshot'()   // second IndexJobs on the same cache: classify -> local; startQueued; awaitIdle; file tag changed; local
def 'a member the cache holds without a local snapshot is indexing until its snapshot is built'() // cache caught up cloud with the rule off: classify -> indexing, local false; after awaitIdle -> local
def 'an own snapshot from another incarnation leaves the member none; once indexed, the local one is served'() {
    // cloud has a store-id file and an index/v7.sqlite whose store_id is another id
    expect: jobs.stateOf('cloud').state == 'none' && jobs.stateOf('cloud').reason.startsWith('written for another incarnation')
    when: jobs.start('cloud', 'local'); jobs.awaitIdle(30_000)
    then: jobs.localSnapshot('cloud') != null && !jobs.servesOwn('cloud')
    // and a second IndexJobs over the same cache classifies cloud local, not snapshot
}
def 'copied() refreshes a local member; the copy view lists unindexed members and local roots'()
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.explore.IndexJobsTest'`
Expected: FAIL, `IndexJobs` does not exist.

- [ ] **Step 3: Implement** `explore/IndexJobs.groovy`. One single-thread executor with a daemon thread named `nf-blocks-index`; a queue of pending refreshes; per-alias state held in a `ConcurrentHashMap` of immutable values, replaced whole, so `stateOf` never sees a half-updated state. The progress callback writes the current `(phase, done, total)` into the running state and throws `CancellationException` when `Thread.currentThread().isInterrupted()`.

- [ ] **Step 4: Run to see them pass**

Run: `./gradlew test --tests 'robsyme.cas.explore.IndexJobsTest'`
Expected: PASS, and no test takes over 30 s.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/explore/IndexJobs.groovy src/test/groovy/robsyme/cas/explore/IndexJobsTest.groovy
git commit -m "explore: IndexJobs keeps each member's index state and runs one index job at a time"
```

### Task S2: The server reports states, takes POST /api/index and serves local snapshots

**Files:**
- Create: `explore/LocalSnapshotFiles.groovy`
- Modify: `explore/ExploreServer.groovy` (`handle` 280-291, `route` 336-371, `membersJson` 384-399, `copies` 176-178, `runCopy` 551-563)
- Test: `explore/ExploreIndexTest.groovy` (new; `ExploreCopyTest`'s helpers as the model)

**Interfaces:**
- Consumes: `IndexJobs` (S1), `CopyService.Indexing` and the 6-arg `execute` (CP1).
- Produces:
  - `ExploreServer indexing(IndexJobs jobs)`.
  - `members.json`: each member gains `index: jobs.stateOf(alias)` when indexing is set; nothing changes without it.
  - `GET` (and `HEAD`) `/api/index`: `200 {"members": jobs.states()}`; 404 without indexing.
  - `POST /api/index`: 405 without indexing or token; `admitted(exchange, COPY_TYPES, 'application/json')`; a body that is not a JSON object with string `member` and `mode` is 400 `bad_request`; else `jobs.start(member, mode)`'s status and body as JSON. The 405 for other POST paths says `only /api/put, /api/copy, /api/handoff and /api/index take a POST`.
  - `copies(Map config, PrintStream err)` passes `new CopyService.Indexing(jobs.unindexedForCopy(), jobs.localRootsForCopy())` (null without indexing), built when each copy starts.
  - After a real copy answers 200, `jobs?.copied(request.to)`.
  - `class LocalSnapshotFiles implements MemberFiles`: `LocalSnapshotFiles(String alias, MemberFiles own, IndexJobs jobs)`. `open(rel)` for `IndexSnapshot.relativePath()`: `jobs.localSnapshot(alias)` when not null (opened as `new LocalMemberFiles(localRoot.resolve(alias)).open(rel)`), else `own.open(rel)` when `jobs.servesOwn(alias)`, else null. Every other `rel`, `list` and `describe` go to `own`.

- [ ] **Step 1: Write the failing tests** in `ExploreIndexTest.groovy` (a server over local members with a real `IndexJobs` as in S1's setup; `RawHttp`; `good()` headers carry the token, `Origin` and `Content-Type: application/json`)

```groovy
def 'members.json carries each member index object'() {
    expect: json(get('/members.json')).members.find { it.alias == 'cloud' }.index == [state: 'none', at: null, progress: null, reason: 'absent', mode: null, local: false]
}
def 'GET /api/index answers every member; a server without indexing answers 404'()
def 'POST /api/index starts a job: 202, then 409 index_running while it runs'()   // onReport holds; second POST for 'ro' -> 409
def 'POST /api/index refusals'() {
    // no token 403; text/plain 415; body [member: 'lab', mode: 'local'] 400 writable_member; [member: 'ro', mode: 'push'] 400 not_writable;
    // [member: 7] 400 bad_request; '[1]' 400 bad_request
}
def 'the snapshot URL of a member indexed on this machine is its local file; a member not indexed answers 404 even with a snapshot of its own'() {
    // cloud with an own index/v7.sqlite from another incarnation: GET /m/cloud/index/v7.sqlite -> 404 while none
    // after POST and awaitIdle: 200, and the bytes equal localRoot/cloud/index/v7.sqlite
}
def 'a real copy into a member indexed on this machine queues its refresh'()       // copies(stub Copier answering 200), request to: 'cloud' after cloud is local -> jobs.stateOf('cloud').state == 'indexing' or later 'local' with a newer at
def 'another POST path lists /api/index in its 405'()
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.explore.ExploreIndexTest'`
Expected: FAIL, `indexing(...)` and `LocalSnapshotFiles` do not exist.

- [ ] **Step 3: Implement** the routes, the members.json key, the copy hooks and `LocalSnapshotFiles`.

- [ ] **Step 4: Run to see them pass, then the server suites**

Run: `./gradlew test --tests 'robsyme.cas.explore.*'`
Expected: PASS (`ExploreServerTest`'s exact members.json assertion is for a server without indexing and is unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/explore/ExploreServer.groovy src/main/groovy/robsyme/cas/explore/LocalSnapshotFiles.groovy src/test/groovy/robsyme/cas/explore/ExploreIndexTest.groovy
git commit -m "explore server: index states in members.json and /api/index, POST /api/index, local snapshots at the member URL"
```

### Task S3: explore starts without scanning and stops a running job on exit

**Files:**
- Modify: `explore/ExploreCommand.groovy` (`Started` 36-46, `run` 49-67, `start` 69-115)
- Test: `explore/ExploreCommandTest.groovy`

**Interfaces:**
- Consumes: `CasSession.leaveUnindexedOut` (C2), `IndexJobs` (S1), `ExploreServer.indexing`, `LocalSnapshotFiles` (S2), `IndexPaths.localSnapshotsDir` (C1).
- Produces:
  - `start` order: `forCommand`, `checkClock`, `warnShadowed`, `session.leaveUnindexedOut([:])`, `refresh(session, err)`, `openIndex`, `newPut`, `jobs = new IndexJobs(session, index, put, IndexPaths.localSnapshotsDir(cachePath), memberInfoOf(cas))`, `jobs.classify()`, each `membersOf` entry wrapped in `LocalSnapshotFiles`, `.indexing(jobs)` on the server, the URL printed, then `jobs.startQueued()`.
  - `Started` gains `final IndexJobs jobs`.
  - `static void stop(Started started, PrintStream err)`: the exit hook's body, now `server.stop()`, then `jobs.stop(10_000)`, then the put-locked `index.close()`, then `refresh(cas, err)`. `run` calls it from the hook.

- [ ] **Step 1: Write the failing tests** in `ExploreCommandTest.groovy` (its `config()` gains `cloud`, a local member with a logged run and no snapshot)

```groovy
def 'a member with no snapshot is not scanned at start: the URL prints, stderr names it once, members.json says none'() {
    when: started = ExploreCommand.start(['--port', '0'], config(), new PrintStream(out, true), new PrintStream(err, true))
    then:
    out.toString().trim() ==~ /nf-blocks explorer: http:\/\/127\.0\.0\.1:\d+\/\?token=[a-z2-7]{26}/
    err.toString('UTF-8').readLines().count { it == "store member 'cloud' has no Index Snapshot, so the explorer does not list it. Index it from the Storage page, or run nf-blocks:snapshot against it." } == 1
    !started.index.isIndexed('cloud') && !Files.exists(tempDir.resolve('cloud/index'))
    json(RawHttp.send(started.server.port, 'GET', '/members.json')).members.find { it.alias == 'cloud' }.index.state == 'none'
}
def 'a member indexed in an earlier session is local at once after a restart, and its snapshot is served'()   // start, POST /api/index, awaitIdle, ExploreCommand.stop; start again: index.state in ['local', 'indexing'] with local true; GET /m/cloud/index/v7.sqlite 200
def 'the exit stops a running job before the final refresh'()      // started.jobs.onReport holds at 'reading'; ExploreCommand.stop(started, err) returns within 15 s; a fresh cache open says !isIndexed('cloud')
```

Update the existing tests: `members.json says each member kind...` expects `index: [state: 'snapshot', at: null, progress: null, reason: null, mode: null, local: false]` on `lab`; any stderr assertion now allows the `shared` member's line (it is an empty member with no snapshot).

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests 'robsyme.cas.explore.ExploreCommandTest'`
Expected: FAIL, cloud is scanned at start and members.json has no `index`.

- [ ] **Step 3: Implement** the wiring in `ExploreCommand.groovy`.

- [ ] **Step 4: Run to see them pass, then the whole Groovy suite**

Run: `./gradlew test --tests 'robsyme.cas.explore.ExploreCommandTest'` then `./gradlew test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy src/test/groovy/robsyme/cas/explore/ExploreCommandTest.groovy
git commit -m "explore: serve at once, leave unindexed members to the Storage page, stop a running job on exit"
```

---

## Web lane

### Task W1: indexing.js, the client, the progress words and the watch

**Files:**
- Create: `web/src/indexing.js`
- Test: `web/test/indexing.test.mjs` (new)

**Interfaces:**
- Consumes: the Global Constraints `index` object and `/api/index` shapes; `formatNumber` (`format.js`); `members.invalidate` (`members.js`).
- Produces:
  - `export function elapsedText(ms)`: `45s`, `2m 10s`, `1h 3m`.
  - `export function progressText(index, alias, now = Date.now())`: by phase, each followed by ` · ` and `elapsedText(now - Date.parse(startedAt))`: `Listing blocks…`, `Reading <done> of <total> metadata blocks`, `Indexing <done> of <total> records`, `Reading the Store Log…`, `Building the snapshot…`, `Pushing to <alias>…`; numbers through `formatNumber`.
  - `export class IndexError extends Error { code; status }`.
  - `export function indexClient({ endpoint, token, fetchFn })`: `{ start(alias, mode) }` posts `{ member, mode }` with `Content-Type: application/json` and `X-NF-Blocks-Token`; resolves the answer's `index`; throws `IndexError` with the server's `error` (else `index_failed`) and `message`.
  - `export function createIndexing(members, { client, fetchStatus, interval = 1000, setTimer = setTimeout, clearTimer = clearTimeout })`: `{ stateOf(alias), running(), start(alias, mode), onChange(fn), watch() }`. `stateOf` reads `member.index` (null for a server without it). `running()` is the alias whose state is `indexing`, else null. `start` posts, stores the answer's index and calls `watch()`. `watch()` polls `fetchStatus()` (resolving `{ members: { alias: index } }`) every `interval` while any state is `indexing`; for each alias whose object changed it sets `member.index`, and when its `state`, `at` or `local` changed and the new state is not `indexing` it calls `members.invalidate(alias)` before the listeners; listeners get `(alias, index)`. Three failed polls in a row stop the watch. `onChange` returns an unsubscribe.

- [ ] **Step 1: Write the failing tests**

```js
const t0 = Date.parse('2026-10-27T10:00:00.000Z')
const at = (phase, done = null, total = null) => ({ state: 'indexing', at: null, reason: null, mode: 'local', local: false,
  progress: { phase, done, total, startedAt: new Date(t0).toISOString() } })

test('progress text by phase, with the elapsed time', () => {
  const now = t0 + 130_000
  assert.equal(progressText(at('listing'), 'cloud', now), 'Listing blocks… · 2m 10s')
  assert.equal(progressText(at('reading', 1240, 8615), 'cloud', now), 'Reading 1,240 of 8,615 metadata blocks · 2m 10s')
  assert.equal(progressText(at('ingesting', 12, 340), 'cloud', now), 'Indexing 12 of 340 records · 2m 10s')
  assert.equal(progressText(at('log'), 'cloud', now), 'Reading the Store Log… · 2m 10s')
  assert.equal(progressText(at('snapshot'), 'cloud', now), 'Building the snapshot… · 2m 10s')
  assert.equal(progressText(at('push'), 'cloud', now), 'Pushing to cloud… · 2m 10s')
})

test('elapsed text', () => {
  assert.deepEqual([45_000, 130_000, 3_780_000].map(elapsedText), ['45s', '2m 10s', '1h 3m'])
})

test('the watch polls only while a job runs, and invalidates a member once when its job ends', async () => {
  // members: { list: [{ alias: 'cloud', index: at('reading', 1, 9) }], invalidate: (a) => invalidated.push(a) }
  // fetchStatus answers reading 5/9, then local; a manual setTimer runs ticks on demand
  // after three ticks: invalidated == ['cloud'], polls == 2, no timer pending, listeners heard reading then local
})

test('start posts the member and mode with the token; a 409 is index_running', async () => {
  // fetchFn records the request: method POST, headers['X-NF-Blocks-Token'] === 'tok', body {"member":"cloud","mode":"push"}
  // a 409 {"error":"index_running","message":"another store is being indexed; try again when it has finished"} rejects with IndexError code index_running
})

test('three failed polls in a row stop the watch', async () => {})
```

- [ ] **Step 2: Run to see them fail**

Run: `cd web && npm ci && npm run gen && node --test test/indexing.test.mjs`
Expected: FAIL, `../src/indexing.js` does not exist.

- [ ] **Step 3: Implement** `web/src/indexing.js`.

- [ ] **Step 4: Run to see them pass**

Run: `node --test test/indexing.test.mjs`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/src/indexing.js web/test/indexing.test.mjs
git commit -m "web: the index client, its progress words and the watch that polls while a job runs"
```

### Task W2: Storage cards show each member's index state and its actions

**Files:**
- Modify: `web/src/views/storage.js` (`storeCards` 243-273), `web/src/styles.js` (card actions)
- Test: `web/test/storage-index.test.mjs` (new), `web/test/all-stores.test.mjs` (the card href assertion)

**Interfaces:**
- Consumes: `progressText`, `IndexError` (W1); `ctx.indexing` with W1's `createIndexing` shape (`stateOf`, `running`, `start`, `onChange`), set by W3; members.json `copy_target`/`copy_reason`; `agoText` (`words.js`).
- Produces: every card a `div.panel.storage-card[data-storage-card=<alias>]`. A card not broken down below holds `a[data-storage-card-open]` (the old href and title) around its head, then its body. With `ctx.indexing` and the member's index object, the body and `[data-index-actions]`:

| state | body text | buttons |
|---|---|---|
| `snapshot` | figures, as today | none |
| `local` | `Indexed on this machine · <agoText(at)>`, then figures when the snapshot opens | `Push to store` (`button[data-index-mode=push]`) |
| `none` | `Not indexed: its runs and Selections are not listed`; when `reason` is not `absent`, also `Its own snapshot can't be used (<reason>).` | `Index locally` (`[data-index-mode=local]`), `Index and push` (`[data-index-mode=push]`) |
| `indexing` | `[data-index-progress]` with `progressText`, updated on each `onChange` for this alias | none |
| `failed` | `Indexing failed: <reason>` (and `Indexed on this machine` when `local`) | `Retry` (`[data-index-mode=<mode>]`) |
| `outside` | `Not in cas.resolve, so the explorer does not index it` | none |

  A push button is disabled with `title` and a `[data-index-why]` line `Can't push here: <copy_reason>` when `copy_target` is false. While `ctx.indexing.running()` names another member, every Index, Push and Retry button is disabled with `title` `another store is being indexed`. A click calls `ctx.indexing.start(alias, mode)`; an `IndexError` shows its message in the card. A card's `onChange` subscription ends once the card is no longer connected. Without `ctx.indexing` or an index object (an older server), cards are as today.

- [ ] **Step 1: Write the failing tests** in `storage-index.test.mjs` (the `world` and `STORAGE_MEMBERS` pattern of `all-stores.test.mjs`, members carrying `index`; `ctx.indexing` a fake recording `start` calls, with `running()` and `onChange` controlled by the test)

```js
test('a member not indexed offers Index locally and Index and push', async () => {
  const card = await cardOf('cloud', { index: none('absent') })
  assert.match(text(card), /Not indexed: its runs and Selections are not listed/)
  assert.deepEqual(card.querySelectorAll('[data-index-mode]').map(b => [b.textContent, b.dataset.indexMode]), [['Index locally', 'local'], ['Index and push', 'push']])
  card.querySelector('[data-index-mode=local]').click()
  assert.deepEqual(started, [['cloud', 'local']])
})
test('a member indexed on this machine says so and when, and offers Push to store', async () => {
  assert.match(text(await cardOf('cloud', { index: local(twoMinutesAgo) })), /Indexed on this machine · 2 min ago/)
})
test('push is disabled with the reason on a member a copy cannot write', async () => {
  const card = await cardOf('cloud', { index: none('absent'), copy_target: false, copy_reason: 'readOnly is set for this store' })
  const push = card.querySelector('[data-index-mode=push]')
  assert.equal(push.disabled, true)
  assert.equal(push.getAttribute('title'), 'readOnly is set for this store')
  assert.match(text(card.querySelector('[data-index-why]')), /Can't push here: readOnly is set for this store/)
})
test('while another store is being indexed, the Index buttons are disabled and say why', async () => {})   // running() === 'org-s3'
test('an indexing member shows its progress and updates it in place', async () => {})                      // onChange fires reading 9/10: [data-index-progress] text changes, the card node is the same
test('a failed job shows its reason and a Retry that repeats its mode', async () => {})
test('a member outside cas.resolve says so and offers nothing', async () => {})
test('a member whose own snapshot cannot be used says why', async () => {})                                // reason 'written for another incarnation ...'
test('without index objects the cards are as before', async () => {})
```

In `all-stores.test.mjs`, `cards[1].getAttribute('href')` becomes `cards[1].querySelector('a[data-storage-card-open]').getAttribute('href')`; the other card assertions stand.

- [ ] **Step 2: Run to see them fail**

Run: `node --test test/storage-index.test.mjs`
Expected: FAIL, the cards have no index text or buttons.

- [ ] **Step 3: Implement** the card body and actions in `storeCards`, and the actions row CSS in `styles.js` (buttons use the kit's `btn sm`).

- [ ] **Step 4: Run to see them pass, then the suite**

Run: `node --test test/storage-index.test.mjs test/all-stores.test.mjs test/storage-view.test.mjs` then `npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/src/views/storage.js web/src/styles.js web/test/storage-index.test.mjs web/test/all-stores.test.mjs
git commit -m "storage page: each store card shows its index state, with Index locally, Index and push, Push to store and Retry"
```

### Task W3: The page wires the watch in and says "indexing…" elsewhere

**Files:**
- Modify: `web/src/app.js` (`start` ~308-360, the ctx at ~106), `web/src/store.js` (`normalizeMember`), `web/src/nav.js` (`drawStores` ~59), `web/src/views/switcher.js` (`sizeText` ~71, its caller ~109), `web/src/views/copy.js` (result line ~742)
- Test: `web/test/store.test.mjs`, `web/test/nav.test.mjs`, `web/test/switcher.test.mjs`, `web/test/copy-view.test.mjs`

**Interfaces:**
- Consumes: `createIndexing`, `indexClient` (W1); CP1's `snapshot: 'not_indexed'`.
- Produces:
  - `normalizeMember` keeps `index: m.index ?? null`.
  - `app.js`: when the store is served with members and a token, `indexing = createIndexing(members, { client: indexClient({ endpoint: <page>/api/index, token }), fetchStatus: () => fetch('api/index', { cache: 'no-store' }).then(r => r.json()) })`, put on the render ctx as `ctx.indexing`; `indexing.onChange((alias, index) => ...)` calls `sizes.forget(alias)` and redraws the Stores section, and when `index.state !== 'indexing'` also `presence.invalidate()` and re-renders when the hash starts with `#/storage`; `indexing.watch()` once at start when any member is indexing.
  - `sizeText(figure, member = null)` in `switcher.js` and the `said` text in `drawStores`: `indexing…` when `member?.index?.state === 'indexing'` and the figure is not `open`; otherwise as today.
  - The copy result's Snapshot line for `not_indexed`: `not written: <to> is not indexed yet; index it from the Storage page`.

- [ ] **Step 1: Write the failing tests**

```js
// store.test.mjs
test('a members.json member keeps its index object, null when the server sends none', async () => {})
// switcher.test.mjs
test('a member being indexed reads indexing… instead of no snapshot', () => {
  assert.equal(sizeText({ state: 'no_snapshot', stored: null }, { index: { state: 'indexing' } }), 'indexing…')
  assert.equal(sizeText({ state: 'no_snapshot', stored: null }, { index: { state: 'none' } }), 'no snapshot')
})
// nav.test.mjs
test('the Stores section says indexing… for a member being indexed', () => {})
// copy-view.test.mjs
test('a copy into a member that is not indexed says so in its result', async () => {
  // a finished copy body with snapshot: 'not_indexed' and to 'cloud'
  // assert.match(text(result), /not written: cloud is not indexed yet; index it from the Storage page/)
})
```

- [ ] **Step 2: Run to see them fail**

Run: `node --test test/store.test.mjs test/switcher.test.mjs test/nav.test.mjs test/copy-view.test.mjs`
Expected: FAIL on the four new tests.

- [ ] **Step 3: Implement** the changes listed above.

- [ ] **Step 4: Run to see them pass, then build and the suite**

Run: `node --test test/store.test.mjs test/switcher.test.mjs test/nav.test.mjs test/copy-view.test.mjs` then `npm test` then `npm run build`
Expected: PASS, and the build succeeds (app.js has no unit test of its own; the build catches a broken import).

- [ ] **Step 5: Commit**

```bash
git add web/src/app.js web/src/store.js web/src/nav.js web/src/views/switcher.js web/src/views/copy.js web/test/store.test.mjs web/test/switcher.test.mjs web/test/nav.test.mjs web/test/copy-view.test.mjs
git commit -m "web: watch index jobs from the page, say indexing… in the Stores list and switcher, explain a copy into an unindexed store"
```

---

## Docs

### Task D1: DESIGN.md and README

**Files:**
- Modify: `DESIGN.md` prose (not the ipldsch block), `README.md`

**Interfaces:**
- Consumes: the decisions in this plan. Each as a dated (2026-10-27) amendment in the section's existing style, citing the spec path.

- [ ] **Step 1: Amend DESIGN.md**
  - §12 Index (from line 1966): `isIndexed`, `trySeed`, `indexMember` with `IndexProgress`, the strict scan (a read failure throws, nothing marked), and `reset()` deleting `<cache>.snapshots`.
  - §15 `nf-blocks:explore` (from line 2829): startup leaves unindexed members out with the verbatim line; the request table gains `GET /api/index` and `POST /api/index` with every status and error code; `members.json`'s `index` object; the local snapshot served at `m/<alias>/index/v7.sqlite` and the 404 for a member not indexed; the job thread, its phases, the scratch index and the seed under Put's monitor; the exit order.
  - §20 Cache coherence (from line 4324): the local snapshot directory's lifetime follows the cache file; served only while the cache holds the member.
  - §21 Copy (from line 4828): with the explorer's view, unindexed members are left out, local snapshots seed the copy's cache, and an unindexed or local destination gets `snapshot: "not_indexed"`.
- [ ] **Step 2: Amend README.md** where it describes `nf-blocks:explore`: a member with no Index Snapshot is listed as not indexed, and the Storage page indexes it on this machine or pushes a snapshot to it.
- [ ] **Step 3: Check**: `cd web && npm ci && npm run gen` succeeds (the ipldsch fence is untouched), and `grep -n "api/index" DESIGN.md` finds both requests in the §15 table.
- [ ] **Step 4: Commit**

```bash
git add DESIGN.md README.md
git commit -m "DESIGN, README: background store indexing in the explorer"
```

---

## Final: acceptance (orchestrator checklist)

Run after every task is integrated on `background-indexing`. Not an implementer task. Every command runs with a scratch `XDG_CACHE_HOME` and `NXF_HOME`; nothing opens `/tmp/cas-exploration/store` except `cp -R` reading it.

- [ ] Set up: `S=<scratchpad>/accept-$(date +%s)`; `mkdir -p "$S"`; `cp -R /tmp/cas-exploration/store "$S/roblab"`; `cp -R /tmp/cas-exploration/store "$S/cloud"` and `rm -rf "$S/cloud/index" "$S/cloud/index.html"` (a large member with no snapshot, standing in for S3); `cp -R "$S/cloud" "$S/cloud2"`; `mkdir "$S/lab" "$S/empty"`; `export XDG_CACHE_HOME="$S/cache" NXF_HOME="$S/nxf"`; build and install the plugin into a throwaway `NXF_PLUGINS_DIR` as `gate/smoke.sh` does. Config: `lab` writable, `roblab`, `cloud`, `cloud2`, `empty`, and `cloud` copied once more as `ro` with `readOnly = true`.
- [ ] 1. Start `nextflow plugin nf-blocks:explore`: the URL prints within a few seconds (time it); stderr has one `has no Index Snapshot, so the explorer does not list it` line per unindexed member and no `indexing it from every block` line; `curl -s <base>/api/index` shows `cloud`, `cloud2`, `empty`, `ro` as `none` and `roblab` as `snapshot`.
- [ ] 2. On the Storage page, `cloud`'s card offers Index locally and Index and push; `ro`'s Index and push is disabled with `readOnly is set for this store`. Click Index locally: the card shows Listing, then `Reading <n> of <m> metadata blocks · <elapsed>` advancing about once a second, then Indexing, the Store Log and Building the snapshot; the other cards' Index buttons say `another store is being indexed`. While it runs, a save from the page succeeds at once. At the end the card says `Indexed on this machine · just now`, `cloud`'s runs browse with `?member=cloud`, `$S/cloud/index` does not exist, and `$XDG_CACHE_HOME/nf-blocks/<hash>.sqlite.snapshots/cloud/index/v7.sqlite` does.
- [ ] 3. Stop explore (Ctrl-C) and start it again: the URL prints at once, no line for `cloud`, `cloud` is browsable at once, its card shows Building the snapshot briefly, then `Indexed on this machine`.
- [ ] 4. Push to store on `cloud`: `$S/cloud/index/v7.sqlite` and `$S/cloud/store-id` exist, `$S/cloud/index.html` does not, the card shows figures, and the local snapshot directory for `cloud` is gone.
- [ ] 5. Copy a Selection from `roblab` into `empty` (not indexed): the copy finishes without a long pause, its result says `not written: empty is not indexed yet; index it from the Storage page`, stderr has the `has no Index Snapshot, so this copy did not write one` line, and `$S/empty/index` does not exist.
- [ ] 6. Index `cloud2` locally, then copy one of its runs into `lab`: the copy succeeds, no scan of `cloud2` happens (no pause; the copy cache's `seeded_from:cloud2` meta is set, checked with `sqlite3 -readonly` on the scratch cache), and `lab`'s snapshot is rewritten.
- [ ] 7. Start Index locally on `ro`, and stop explore part way through Reading: explore exits within about 10 s; on restart `ro` is `none` again and nothing under `$S/ro` changed.
- [ ] 8. `./gradlew test` and `cd web && npm test` pass.
