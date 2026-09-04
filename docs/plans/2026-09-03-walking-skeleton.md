# Walking Skeleton Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or
> superpowers:subagent-driven-development when farming tasks out) to implement
> this plan task-by-task. Every task is bound by `DESIGN.md` in the repo root;
> when this plan and `DESIGN.md` disagree, `DESIGN.md` wins and this plan gets
> fixed.

**Goal:** A Nextflow plugin that, on released Nextflow 26.04.6 and the
existing Test Pipeline, publishes every output into a content-addressed store
through `outputDir = 'cas://lab'`, writes DAG-CBOR lineage blocks, indexes
them in SQLite, answers the three load-bearing queries, reads content back
through `lid://` and `cas://`, and passes an external Gate that hashes bytes
itself.

**Architecture:** Two released seams, `LinStoreFactory` and
`FileSystemPathFactory`, plus a `TraceObserverV2`. Hashing happens inside our
own `FileSystemTransferAware.upload()` as Nextflow publishes; the lineage join
happens at `onFlowComplete`. Everything under `robsyme.cas.core` has no
Nextflow dependency and is unit-tested in isolation; everything else is proven
by the Gate running real `nextflow`.

**Tech Stack:** Groovy 4 (`@CompileStatic`), Gradle 8.14 with
`io.nextflow.nextflow-plugin` 1.0.0-beta.15, Spock, `org.xerial:sqlite-jdbc`,
Java 21, Nextflow 26.04.6, Python 3 for Gate assertions.

**Sequencing rule (from the spec):** the Nextflow boundary first. Task 1 must
produce a plugin that loads and publishes through `cas://` on real Nextflow
before any design-heavy code lands. Nothing outside this plan is built until
the Gate is green.

**Waves (parallelisable):**

| Wave | Tasks | Depends on |
|---|---|---|
| 1 | 1 Build + boundary, 2 Core primitives, 3 Gate harness | — |
| 2 | 4 Records + Directory Manifest + Coordinates, 5 Index + Run Log | 1, 2 |
| 3 | 6 Provider (read, upload, download), 7 `CasLinStore` | 4 (5 for 7's search? no: 7 needs only 4) |
| 4 | 8 Observer + `fromStore` | 5, 6, 7 |
| 5 | 9 Run the Gate, fix until green; 10 Docker wrapper + dependency check in CI | 8 |

Each task ends with a commit on `main` in `nf-blocks/`. Tasks in the same
wave touch disjoint files; when a task needs an interface owned by a
concurrent task, code against `DESIGN.md` and a minimal stub in your own test
sources, never edit the other task's files.

---

### Task 1: Build, plugin identity, and the Nextflow boundary

**Files:**
- Modify: `build.gradle`, `settings.gradle`, `Makefile`, `README.md`, `.gitignore`
- Delete: `src/main/groovy/robsyme/plugin/*`, `src/test/groovy/robsyme/plugin/*`
- Create: `src/main/groovy/robsyme/cas/CasPlugin.groovy`
- Create: `src/main/groovy/robsyme/cas/CasConfig.groovy` (parses the `cas` scope per DESIGN §2)
- Create: `src/main/groovy/robsyme/cas/lineage/CasLinStoreFactory.groovy` (DESIGN §2, §10; `newInstance` may return a temporary store that delegates everything to `DefaultLinStore` at `<writable>/nf` until Task 7 replaces it)
- Create: `src/main/groovy/robsyme/cas/nio/CasPathFactory.groovy`, `CasFileSystemProvider.groovy`, `CasFileSystem.groovy`, `CasPath.groovy` — a **boundary stub** that mirrors coordinates onto a real directory `<writable>/coords/` exactly as the probe did (`.scratch/research/nf-casx-probe/src/main/groovy/casx/*`), implementing every `FileSystemProvider` method plus `FileSystemTransferAware`. Task 6 replaces the internals; keep the class names.
- Create: `src/test/groovy/robsyme/cas/CasConfigTest.groovy`, `src/test/groovy/robsyme/cas/lineage/CasLinStoreFactoryTest.groovy`
- Create: `gate/smoke.sh` — builds, installs into a temp `NXF_PLUGINS_DIR`, runs the Test Pipeline once with `lineage.store.location = 'cas://lab'` and `outputDir = 'cas://lab'`, and checks that `coords/aligned/A/A.bam` exists and that `nf/` holds a `WorkflowRun` record.

**Build facts you need:**
- The Gradle plugin adds only `compileOnly io.nextflow:nextflow:26.04.6`,
  `slf4j-api` and `pf4j`, and only `mavenCentral()`. Add
  `maven { url 'https://s3-eu-west-1.amazonaws.com/maven.seqera.io/releases' }`
  and `compileOnly 'io.nextflow:nf-lineage:26.04.6'` (plus `nf-commons` if
  not transitive). Spock: `testImplementation 'org.spockframework:spock-core:2.3-groovy-4.0'`
  and `testImplementation 'io.nextflow:nf-lineage:26.04.6'`.
- Runtime deps that must ship in the zip use `implementation` (Task 5 adds `sqlite-jdbc`).
- `extensionPoints = ['robsyme.cas.lineage.CasLinStoreFactory', 'robsyme.cas.nio.CasPathFactory']`
  (Task 8 appends the observer factory and the extension). This list is what
  generates `META-INF/extensions.idx`.
- `Plugin-Requires` is generated as `>=26.04.6`. Accept for now.
- Add a Gradle task `dependencyCheck` (wired into `check`) that fails if
  `build.gradle` or `settings.gradle` contains `includeBuild`, `files(`, or
  `/Users/`.
- `gradle installPlugin` honours `NXF_PLUGINS_DIR`.

**Nextflow facts you need:** `LinStoreFactory` source at
`modules/nf-lineage/src/main/nextflow/lineage/LinStoreFactory.groovy`;
`DefaultLinStore` in the same package; `FileSystemPathFactory` and
`FileSystemTransferAware` in `modules/nf-commons/src/main/nextflow/file/`;
`FileHelper.getOrInstallProvider`. `canOpen` must be total over a null
location (DESIGN §2). Copy the registration idiom from
`.scratch/research/nf-lineage-h2.md` §1.

**Steps:**
1. Write `CasConfigTest` (alias regex, default `asserted_by == 'anonymous'`,
   rejection of an alias that parses as a CID — use a 59-char `bafk…` string,
   missing `cas.stores.<alias>` → clear error). Run, watch it fail.
2. Implement `CasConfig`. Green.
3. Write `CasLinStoreFactoryTest`: `canOpen(new LineageConfig([:]))` is false
   and does not throw; `canOpen` with `store.location = 'cas://lab'` is true;
   `canOpen` with `'file:///x'` is false.
4. Implement the factory with `@Priority(-10)`. Green.
5. Port the probe provider classes into `robsyme.cas.nio` (drop the env-var
   switches and the log file; keep the behaviour that made the probe pass:
   `upload()` throws `FileAlreadyExistsException` when the target exists,
   directories are copied recursively inside `upload()`, `readAttributes`
   and `checkAccess` tell the truth). Back the coordinate tree at
   `<writable>/coords`.
6. `gradle check` green (unit tests + `dependencyCheck`).
7. Write and run `gate/smoke.sh`. It must pass on the installed
   `nextflow` (26.04.6, at `/Users/robsyme/bin/nextflow`). Keep the log.
8. Update `README.md` (Summary, Get Started with the DESIGN §2 config,
   Examples pointing at the Test Pipeline, License) and `Makefile`
   (`smoke` target).
9. Commit: `feat: plugin skeleton loads on 26.04.6 and publishes through cas://`.

---

### Task 2: Core primitives: Cid, DagCbor, Hashing, LocalBlockStore

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/Cid.groovy`, `Multibase.groovy` (base32 lower, RFC 4648 no padding), `Varint.groovy`, `DagCbor.groovy`, `Hashing.groovy`, `HashBufferPool.groovy`, `BlockStore.groovy`, `LocalBlockStore.groovy`, `CompositeStore.groovy`, `NoSuchBlockException.groovy`
- Test: `src/test/groovy/robsyme/cas/core/CidTest.groovy`, `DagCborTest.groovy`, `HashingTest.groovy`, `LocalBlockStoreTest.groovy`, `CompositeStoreTest.groovy`, `MemoryBoundTest.groovy`
- Modify: `build.gradle` — add a second `Test` task `memoryBoundTest` with `maxHeapSize = '48m'`, filtered to `MemoryBoundTest`, excluded from the default `test` task, wired into `check`.

**Steps (TDD, one commit per class is fine):**
1. `CidTest`: the three vectors in DESIGN §3; `parse` rejects a `bafk` string
   with a changed character, a CIDv0 (`Qm…`), a non-sha256 multihash;
   `toString(parse(s)) == s`; codec accessor.
2. `DagCborTest`: canonical map key ordering (`{"b":1,"a":2,"aa":3}` encodes
   keys in order `a`, `b`, `aa`); smallest-width ints (`0`, `23`, `24`,
   `255`, `256`, `65536`, `-1`, `2^32`); `Double` always 9 bytes; `null`,
   booleans, `byte[]`, nested lists; a `Cid` round-trips as tag 42 with the
   `0x00` prefix byte; map key `"/"` rejected on encode and decode; tag 43
   rejected on decode; indefinite-length item rejected on decode; the empty
   map encodes to `a0` and `cidOf` gives the DESIGN §3 vector; decode
   returns `Long` for every integer. Include one vector checked against an
   external implementation: `{"a": 1, "b": [true, null, "x"]}` → hex
   `a2616101616283f5f66178`.
3. `HashingTest`: `hashRaw` of `hello\n` gives the §3 vector; hashing a
   5 MiB random file gives the same cid as `MessageDigest` over the whole
   file; the buffer passed in is the only one used (pass a 1 MiB buffer and
   assert with a spy stream that no read exceeds its length).
4. `HashBufferPool`: 32 buffers, `borrow()` blocks when empty, `release()`.
5. `MemoryBoundTest` (runs under 48 MiB heap): generate a 256 MiB file with
   `RandomAccessFile.setLength` plus a few written bytes, `Hashing.hashRaw`
   it, `LocalBlockStore.putStreaming` it, `open` it and count bytes. All
   succeed. This test is the mechanised memory bound; never weaken it.
6. `LocalBlockStoreTest`: `putStreaming` places the block at
   `blocks/<last2>/<cid>` with mode `r--r--r--`; a second `putStreaming` of
   identical bytes is a no-op (mtime unchanged) and returns the same cid;
   `put(cid, …)` with bytes that do not hash to `cid` throws and leaves no
   block; `has`, `size`, `open`, `lastModifiedMillis` (stable across two
   calls); `listBlocks` enumerates; `putDagCbor` gives a `bafy…` cid;
   read-only store refuses `put`; concurrent `putStreaming` of the same bytes
   from 16 threads yields one block.
7. `CompositeStoreTest`: read resolves in order; write goes to member 0 even
   when member 1 has the block; member 0 read-only → exception at construction.
8. Commit each green step. Final commit: `feat(core): content identity, DAG-CBOR and local block store`.

---

### Task 3: Gate harness

**Files:**
- Create: `gate/gate.sh`, `gate/gate.config`, `gate/assert.py`, `gate/cas.py` (CID encoder + DAG-CBOR decoder + store helpers), `gate/test_cas.py` (unit tests for `cas.py`, run with `python3 -m unittest`), `gate/README.md`
- Modify: `Makefile` (`gate` target)

**Behaviour:**
- `gate.sh`: `set -euo pipefail`; `GATE_ROOT=$(mktemp -d)`; builds with
  `./gradlew -q assemble installPlugin` into `NXF_PLUGINS_DIR=$GATE_ROOT/plugins`;
  `XDG_CACHE_HOME=$GATE_ROOT/cache`; store at `$GATE_ROOT/store`; copies the
  Test Pipeline to `$GATE_ROOT/pipeline-a` (and `pipeline-b` for the
  two-launch-directory assertion); runs, from `pipeline-a`:
  `nextflow run . -c $GATE/gate.config -name cold`, `-name again`,
  `-name fail --fail` (expected non-zero exit), `-name resumed -resume cold`;
  then from `pipeline-b`: `-name elsewhere`. Each run's stdout/stderr and
  `.nextflow.log` are kept under `$GATE_ROOT/logs/<name>/`. `nextflow` is
  `${NEXTFLOW:-nextflow}` and must report `26.04.6`.
- `gate.config`: `plugins { id 'nf-blocks' }`, `lineage.enabled`,
  `lineage.store.location = 'cas://lab'`, `outputDir = 'cas://lab'`,
  `cas.stores.lab.location = "$GATE_STORE"` via `System.getenv`,
  `cas.asserted_by = 'gate'`.
- `assert.py GATE_ROOT`: implements the assertions from spec §1.2 that the
  skeleton must pass, each as a function returning pass/fail with a message,
  printed as a table, exit 1 if any fails. Assertions 1, 2, 3, 4 (output
  layer complete; task layer counted from `nf/`), 5, 7 (via a second
  pipeline `gate/consumer/main.nf` that uses `fromStore` and `lid://` and
  `cas://` inputs and writes the staged files' sha256 to its own output),
  and 10 (compare `pipeline-a` vs `pipeline-b` item and manifest cids; grep
  every `bafy…` block's decoded strings for `GATE_ROOT` and `$USER`).
  Assertions 6, 8, 9, 11, 12 are listed as `SKIP (not in skeleton)`.
- `cas.py`: `cid_raw(bytes) -> str`, `cid_dagcbor(bytes) -> str`,
  `decode(bytes) -> object` (DAG-CBOR incl. tag 42 → `Cid` object), `Store`
  class (`blocks(kind)`, `read(cid)`, `run_log()`), `nf_records(kind)`.
  Test vectors: the three in DESIGN §3 and the `a2616101616283f5f66178` map.
- Until Tasks 4–8 land, `gate.sh` runs and `assert.py` reports failures;
  that is the intended state. Make sure it fails *loudly and specifically*
  (which assertion, what was expected, what was found).

**Steps:** write `test_cas.py` first, then `cas.py`; write `assert.py` with
one assertion at a time against a hand-made fixture store directory in
`gate/fixtures/` (a few blocks you encode with `cas.py`, a `runs/` entry, an
`nf/` record); then `gate.sh`. Commit: `test(gate): external gate harness with independent hashing`.

---

### Task 4: Records, Directory Manifest, Coordinates

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/Records.groovy` (builders + decoders for every kind in DESIGN §6: `DirectoryManifest`, `OutputItem`, `Leaf`, `OutputCollection`, `RunManifest`, `RunCompletion`; each a small `@Canonical` class with `Map toCbor()` and `static X fromCbor(Map)`), `DirectoryManifestBuilder.groovy`, `Coordinates.groovy`, `CoordinateTree.groovy`, `StoreRef.groovy`, `Anomalies.groovy`
- Test: `src/test/groovy/robsyme/cas/core/RecordsTest.groovy`, `DirectoryManifestBuilderTest.groovy`, `CoordinatesTest.groovy`, `CoordinateTreeTest.groovy`

**Steps:**
1. `CoordinatesTest`: `key('cas://lab/a/./b/../c.txt') == 'cas://lab/a/c.txt'`;
   trailing slash dropped; a Store URI (cid authority) throws; same result
   for a `String` and for a `Path` whose `toUri()` is that string (use a
   tiny fake `Path` or `URI`-based overload).
2. `StoreRef` ⇄ `cas://<cid>/<name>`; name required for raw, optional for manifest.
3. `CoordinateTreeTest`: write then read; `exists`; overwrite replaces;
   parent directories created; `isDirectoryCoordinate` true when the pointer
   names a `bafy…` cid.
4. `DirectoryManifestBuilderTest` against a temp tree built in the test:
   files `b.txt`, `a.txt`, `Z.txt` sort as `Z.txt`, `a.txt`, `b.txt`
   (bytewise); an executable file gets `executable`; `nested/detail.txt`
   yields a nested manifest whose entry has a `bafy…` address and
   `mode: directory`; `alias.txt -> summary.txt` yields `symlink` with
   `target 'summary.txt'` and **no block for its content**; a link to an
   absolute path outside the tree is followed and stored as `regular`; a
   dangling link yields `unresolvable` and `Anomalies.unresolvable == 1`;
   an empty directory yields `entries: []`; the same tree built twice gives
   the same cid; building the tree with entries created in a different
   order gives the same cid; every regular file's block is present in the
   store afterwards. Re-encoding a decoded manifest reproduces its cid.
5. `RecordsTest`: each kind round-trips through `toCbor`/`fromCbor`;
   `OutputItem` with `[meta, path]` shape → `value` is a list whose second
   element is a Leaf map; an item whose meta contains `kind: 'Leaf'` at the
   top level is *not* mistaken for a leaf (only leaves inside `value`
   positions that were `Path`s become Leafs); two items with identical meta
   and identical addresses but different publish paths give the same cid;
   `OutputCollection.items` sorted by cid string with `paths` re-aligned;
   `RunCompletion` requires every anomaly counter; `asserted_by` present on
   the three run kinds and absent on `DirectoryManifest` and `OutputItem`.
6. Commit: `feat(core): records, directory manifest and coordinates`.

---

### Task 5: Index and Run Log

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/Index.groovy`, `RunLog.groovy`, `MetadataView.groovy` (flattening to `item_attr` rows)
- Test: `src/test/groovy/robsyme/cas/core/IndexTest.groovy`, `RunLogTest.groovy`, `MetadataViewTest.groovy`
- Modify: `build.gradle` — `implementation 'org.xerial:sqlite-jdbc:3.50.3.0'` (or the latest on Maven Central).

Fixtures: build blocks with `Records` from Task 4 if it has landed; otherwise
encode the DESIGN §6 maps directly with `DagCbor` and switch to `Records`
when it lands (same bytes either way, because the maps are specified).

**Steps:**
1. `MetadataViewTest`: `[meta, leaf]` list → view is `meta`; a Map item →
   itself; `{sample:'A', lane:1, single_end:false, nested:{kit:'truseq', ids:[1,2]}}`
   flattens to rows `sample string A`, `lane int 1`, `single_end bool false`,
   `nested.kit string truseq`, `nested.ids int 1`, `nested.ids int 2`; a
   2000-byte string → `sha256:…`, `truncated 1`.
2. `RunLogTest`: `append(store, completionCid, finishedAtMillis)` creates
   `runs/<13-digit>-<cid>`; newer entries list first; `entriesAfter(watermark)`.
3. `IndexTest`: schema created with WAL; `ingestRun` on a synthetic run
   (RunManifest, two collections `aligned` and `stats`, three items each,
   the three `stats` items sharing one content cid) populates `run`,
   `collection`, `item`, `collection_item`, `producer`, `item_attr`;
   `producersOf(statsCid)` returns three rows with distinct item cids;
   `latestSuccessfulRun('p')` skips a newer run with `status failed` and a
   newer one with `possibly_incomplete = 1`; `items(completion, 'aligned',
   [sample:'B'])` returns one item; `[sample:'B', lane:2]` intersects;
   `runByNextflowHash`; ingesting the same run twice is idempotent;
   `rebuild` from a store containing only blocks reproduces identical row
   counts; schema version mismatch triggers rebuild; a RunCompletion whose
   collection block is absent leaves a `missing` row and no crash;
   path derivation under `XDG_CACHE_HOME`.
4. Commit: `feat(core): sqlite index and run log`.

---

### Task 6: The provider, for real

**Files:**
- Modify: `src/main/groovy/robsyme/cas/nio/CasPath.groovy`, `CasFileSystem.groovy`, `CasFileSystemProvider.groovy`, `CasPathFactory.groovy`
- Create: `src/main/groovy/robsyme/cas/CasSession.groovy` (DESIGN §9), `src/main/groovy/robsyme/cas/nio/CasAttributes.groovy`
- Test: `src/test/groovy/robsyme/cas/nio/CasPathTest.groovy`, `CasFileSystemProviderTest.groovy` (unit tests drive the provider directly with a `LocalBlockStore` in a temp dir; they do not go through `FileSystems`/`installedProviders`, which is the green-tests-broken-runtime trap; the Gate covers the real path)
- Modify: `gate/smoke.sh` if the boundary stub's directory layout changed.

**Carried in from the Task 1 review (fix here, the files are yours):**
- The provider memoises `CasConfig` from `Global.session` on the JVM-singleton
  provider and never invalidates it; a second `Session` in one JVM would write
  into the first run's store. Reach state through `CasSession.of(session)`
  (DESIGN §9) instead; keep a test seam that does not leak.
- `robsyme.cas.CidSyntax` duplicates the §7 discriminator with a regex that
  disagrees with `Cid.parse` on corrupt strings. Delete it; use
  `robsyme.cas.core.Cid.isCid` in `CasConfig` and `CasPath`.
- `CasPath.resolve`, `relativize`, `endsWith`, `compareTo` silently absorb a
  foreign-filesystem or different-authority `Path` (`resolve` appends an
  absolute foreign path as segments). Throw `ProviderMismatchException` /
  `IllegalArgumentException` as `java.nio.file.Path` documents; `startsWith`
  alone stays false-not-throw. `toAbsolutePath()` on a relative path must not
  return a relative path.
- `checkAccess` for `WRITE` on a Store URI must be `AccessDeniedException`
  (the current branch is dead code).
- `isSameFile` must compare `Coordinates.key` (or cid) rather than `Path.equals`.
- `setAttribute` must throw `UnsupportedOperationException`, never pretend.
- `CasConfig`: `resolve` given as a `String` must be rejected, not split
  into characters.
- Unit tests for the provider are required (the review reproduced both
  load-bearing `upload()` behaviours in ten lines each against a test seam).

**Steps:**
1. `CasPathTest`: parse of Store URI vs Coordinate; `isCoordinate()`,
   `cid()`, `alias()`; `resolve`, `getParent`, `getFileName`, `relativize`,
   `normalize`, `iterator`; `startsWith(Paths.get('/tmp'))` returns false;
   `toUri().toString()` canonical; `equals`/`hashCode`; `toString()` is
   the URI (Nextflow logs and stage names use it).
2. Read side per DESIGN §8: `readAttributes` of `cas://<rawCid>/x.bam`
   (size, regular, stable mtime), of a manifest cid (directory), of an
   entry inside a manifest, of a coordinate that points at each;
   `newDirectoryStream` over a manifest and over a coordinate directory;
   `newInputStream` bytes equal the block; `checkAccess` NoSuchFile for an
   absent cid and for an absent coordinate; `WRITE` on a Store URI is
   `AccessDeniedException`.
3. `upload()`: existing coordinate → `FileAlreadyExistsException` before the
   source is opened (assert with a source `Path` whose `newInputStream` would
   throw); regular file → block present, pointer written,
   `CasSession.publishes[key]` filled with size and `head-node`; directory
   → manifest cid in the pointer, every file a block, internal symlink kept
   as `symlink`, dangling counted; a source that cannot be read →
   `IOException`, no pointer written. `canUpload` false for a Store URI.
4. `download()`: raw → target bytes equal the block; same-filesystem →
   symlink to a read-only block; manifest → materialised tree with
   `alias.txt` recreated as a relative symlink and `unresolvable` failing
   with the entry named; absent block → `NoSuchFileException` naming the cid.
5. Run `gate/smoke.sh` again: it must still pass with the real provider.
6. Commit: `feat(nio): cas:// provider with hashing upload and manifest download`.

---

### Task 7: `CasLinStore`

**Files:**
- Create: `src/main/groovy/robsyme/cas/lineage/CasLinStore.groovy`
- Modify: `src/main/groovy/robsyme/cas/lineage/CasLinStoreFactory.groovy`
- Test: `src/test/groovy/robsyme/cas/lineage/CasLinStoreTest.groovy`

**Carried in from the Task 1 review:** `CasLinStore` currently has no unit
tests at all (`CasLinStoreFactoryTest` never calls `newInstance`). Every
behaviour below gets a Spock test. Also guard `Makefile`'s `install` target so
it refuses to write into the real `~/.nextflow/plugins` without an explicit
`FORCE=1`.

**Steps:**
1. Test `open` creates `<writable>/nf`; `save`/`load` of a `TaskRun`
   round-trips through the delegate and is visible at `nf/<key>/.data.json`.
2. Test `save('abc', WorkflowRun)` records `'abc'` as the Nextflow run key
   in `CasSession`.
3. Test `save(key, FileOutput(path: 'cas://lab/aligned/A/A.bam'))` when
   `CasSession.publishes` holds that key: the stored record's `path` is
   `cas://<cid>/A.bam` and its `checksum.value` equals
   `Checksum.ofNextflow(FileHelper.asPath('cas://<cid>/A.bam')).value`
   (you will need the provider from Task 6 registered in the test JVM for
   `asPath`; if Task 6 is not yet merged, compute the fingerprint with a
   `CasPath` built directly). When the key is absent from memory but the
   Pointer File exists, the same rewrite happens. When neither exists,
   `AbortRunException`.
4. Test `search([type: ['FileOutput']])` returns the saved key and
   `getSubKeys('abc')` lists children.
5. Test an `IOException` from the delegate surfaces as `AbortRunException`.
6. Commit: `feat(lineage): CasLinStore rewrites FileOutput.path to Store URIs`.

---

### Task 8: Observer and `fromStore`

**Files:**
- Create: `src/main/groovy/robsyme/cas/trace/CasObserverFactory.groovy`, `CasObserver.groovy`, `src/main/groovy/robsyme/cas/trace/Join.groovy` (pure function: captured `WorkflowOutputEvent`s + `publishes` → items, collections, anomaly counts), `src/main/groovy/robsyme/cas/ext/CasExtension.groovy`, `src/main/groovy/robsyme/cas/CasConfigScope.groovy` (a `@ScopeName("cas")` `ConfigScope` declaring `stores`, `resolve`, `asserted_by`, `pipeline`, `index.path`, so `ConfigValidator` stops warning `Unrecognized config option`; see DESIGN §2)
- Modify: `build.gradle` (`extensionPoints` += `robsyme.cas.trace.CasObserverFactory`, `robsyme.cas.ext.CasExtension`, `robsyme.cas.CasConfigScope`)
- Test: `src/test/groovy/robsyme/cas/trace/JoinTest.groovy`, `CasObserverTest.groovy` (Spock `Mock(Session)`), `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy`

**Facts:** `WorkflowOutputEvent.value` for a channel output is a `List` of
items; each item is the original channel item (`[meta, path]` list, or a
Map) with every path replaced by the **publish target** `Path`
(`PublishOp.normalizeValue`). A declined publish arrives as `null` in place.
`FilePublishEvent.target` is the same target. Both are `CasPath`s under
`outputDir`, so `Coordinates.key()` joins them. `onFlowComplete` fires twice
on a failed run; write the `RunCompletion` once. `session.isSuccess()` and
`session.workflowMetadata` (`exitStatus`, `start`, `complete`, `projectName`,
`repository`, `revision`, `commitId`, `runName`, `sessionId`, `resume`,
`nextflow.version`) supply the RunManifest and RunCompletion fields. Read
`LinObserver.groovy` at `v26.04.6` for how it walks the value.

**Carried in from the Task 5 review (small, do them here):**
- Add `PRAGMA busy_timeout=<a few seconds>` to `Index`'s connection setup so two
  runs finishing at once wait rather than getting `SQLITE_BUSY` (the WAL choice
  was made precisely so concurrent runs under one user do not contend; without
  the pragma that is only half-delivered). Add a test that two threads each
  ingesting a run against one index file both succeed. This is the one place in
  `core/Index.groovy` Task 8 may touch; keep every signature.
- Have `Index.rebuild` carry the run-log watermark forward (set it to the newest
  entry seen) instead of leaving `meta` empty, so the next `catchUp` does not
  re-scan the whole log. Correctness-safe either way; this avoids the re-read.

**Steps:**
1. `JoinTest`: given two captured outputs and a `publishes` map, produce
   items with Leaf maps (name from the last path segment, address/size/
   provider from the publish), a `declined` Leaf for a null path, a
   `never_published` Leaf for a path with no publish, collections with items
   sorted by cid and `paths` aligned, anomaly counts. Identical bytes under
   three metas give three items with one content address (the Test Pipeline's
   `stats` fixture).
2. `CasObserverTest`: `onFlowBegin` writes a RunManifest block with
   `nf_run_hash` from `CasSession`; `onFlowComplete` twice writes one
   RunCompletion and one Run Log entry; failed run → `status failed`,
   `possibly_incomplete true`; index rows written; an index exception is
   logged and does not propagate; `outputDir` alias mismatch aborts at
   `onFlowCreate`.
3. `CasExtension.fromStore(Map)`: test with an index and store prepared in
   a temp dir: `run: 'latest', pipeline: 'p', output: 'aligned', where:
   [sample: 'B']` emits one item whose second element is a `CasPath`
   `cas://<cid>/B.bam`; `run: 'cas://<completionCid>'` and
   `run: 'lid://<nfHash>'` resolve; a run with no RunCompletion errors;
   an `unaddressed` leaf errors naming the item; a declined leaf emits null.
4. Commit: `feat: observer joins at onFlowComplete; fromStore channel factory`.

---

### Task 9: Make the Gate green

Run `make gate`. For every failing assertion, use
superpowers:systematic-debugging: read the Nextflow log, reproduce in a unit
test where possible, fix, re-run. Expected trouble spots, from the research:
`PublishDir` overwrite logic on directory coordinates; the `lid://` read-back
path warning about the fingerprint; `-resume` re-uploading (must be skipped by
`FileAlreadyExistsException`); the failed run's double `onFlowComplete`;
`fromStore` running inside a pipeline whose `outputDir` is a *different*
store directory (the consumer pipeline reads member `lab` read-only and
publishes to `cas://out`). Commit after each fix. Final commit:
`test(gate): walking skeleton passes the gate`.

---

### Task 10: CI shape

- `gate/Dockerfile`: `eclipse-temurin:21-jdk` + Nextflow 26.04.6 + Python 3;
  `gate/gate.sh` runs unchanged inside it. `make gate-docker`.
- `.github/workflows/gate.yml`: `gradle check` then the Docker Gate on every
  push and pull request, required to merge.
- Bound `Plugin-Requires` above as well as below (`>=26.04.0, <26.05.0`),
  because Nextflow accepts an unbounded lower bound on any later release and
  the store then fails at runtime with `AbstractMethodError` (this is what
  killed nf-lineage-h2). The Gradle plugin generates `>=<nextflowVersion>`;
  override the manifest attribute.
- Commit: `ci: gate runs real nextflow in docker on every commit`.

---

## Out of the skeleton (already decided, build after the Gate is green)

Projections (default `results/` tree and `latest`), sweep and trash, claims
and pins, Bundles, store composition beyond one writable member plus
read-only locals, the Input Set, Fusion `afterScript` provider, S3 member,
`onTaskCached` address reuse, small-file packing, attestations.
