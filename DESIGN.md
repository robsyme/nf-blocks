# nf-blocks: implementation contract for the Walking Skeleton

This file is the binding contract between the parallel pieces of the plugin.
It fixes names, byte formats, layouts and seams so that code written in
separate sessions fits together. The reasoning behind every rule is in the
architecture spec at `../.scratch/content-addressed-lineage/spec.md` and the
tickets under `../.scratch/content-addressed-lineage/issues/`. Glossary:
`../CONTEXT.md` (authoritative). Never write "checksum" unqualified: Nextflow's
field of that name is a metadata fingerprint over path, size and mtime.

Nextflow facts are read from the released source at tag `v26.04.6` in the
local clone `/Users/robsyme/dev/github.com/nextflow-io/nextflow`
(`git -C <clone> show v26.04.6:<path>`). The plugin builds against published
artifacts only.

## 0. Non-negotiable rules

1. Released Nextflow only. `build.gradle` may not contain `includeBuild`,
   `files('/Users/…')`, or any absolute path. A Gradle task `dependencyCheck`
   fails the build if it does. CI resolves from public repositories only:
   Maven Central plus `https://s3-eu-west-1.amazonaws.com/maven.seqera.io/releases`
   (the only source of `io.nextflow:nf-lineage:26.04.6`).
2. Memory bound: one fixed 1 MiB buffer per concurrent hash, never more than
   32 concurrent hashes, and no file content ever held in a byte array or
   `ByteArrayOutputStream`. Enforced by a test that hashes a 256 MiB file in a
   JVM whose max heap is 48 MiB.
3. Failures that would lose provenance throw `nextflow.exception.AbortRunException`
   (Nextflow swallows every other exception from a lineage store at debug level).
   Failures in derived structures (index, run log) log at warn and continue.
4. "Already exists" is success for every block write. Re-storing identical
   content is the normal case.
5. No command-line or web surface. No participation in task hashing or
   `-resume` identity.
6. The Gate's assertions never trust the plugin: they hash bytes themselves.

## 1. Plugin identity and layout

- Plugin id `nf-blocks`, entry class `robsyme.cas.CasPlugin`. Scheme `cas`.
  Config scope `cas`.
- Gradle: `io.nextflow.nextflow-plugin` `1.0.0-beta.15`, `nextflowVersion = '26.04.6'`.
  `extensionPoints` lists every extension class (that list is what generates
  `META-INF/extensions.idx`; `@Extension` alone registers nothing).
- Packages:
  - `robsyme.cas.core` — no Nextflow imports at all. CID, DAG-CBOR, hashing,
    block store, records, directory manifest, coordinates, index, run log.
  - `robsyme.cas.nio` — `CasFileSystemProvider`, `CasFileSystem`, `CasPath`,
    `CasPathFactory` (extends `nextflow.file.FileSystemPathFactory`).
  - `robsyme.cas.lineage` — `CasLinStoreFactory` (extends `nextflow.lineage.LinStoreFactory`),
    `CasLinStore` (implements `nextflow.lineage.LinStore`).
  - `robsyme.cas.trace` — `CasObserverFactory` (implements `nextflow.trace.TraceObserverFactoryV2`),
    `CasObserver` (implements `nextflow.trace.TraceObserverV2`).
  - `robsyme.cas.ext` — `CasExtension` (extends `nextflow.plugin.extension.PluginExtensionPoint`)
    exposing the `fromStore` channel factory.
  - `robsyme.cas.CasSession` — the one per-run shared object (see §9).
- Tests: Spock, under `src/test/groovy`, same packages. Groovy `@CompileStatic`
  on all main classes.

## 2. Configuration

```groovy
plugins { id 'nf-blocks' }

lineage.enabled = true
lineage.store.location = 'cas://lab'   // alias of the writable member
outputDir = 'cas://lab'                // publish through the store

cas {
    stores {
        lab { location = '/data/cas' }            // writable member (alias = outputDir authority)
        // shared { location = '/mnt/bundle' }    // read-only member (later)
    }
    resolve = ['lab']          // optional; default = every alias, writable first
    asserted_by = 'anonymous'  // optional opaque label; default 'anonymous'. Never defaults to the OS user name.
    index { path = null }      // optional override of the SQLite cache path (the Gate sets XDG_CACHE_HOME instead)
}
```

- The `cas` scope must be declared as a `ConfigScope` class annotated
  `@ScopeName("cas")` and listed in `extensionPoints`, or `ConfigValidator`
  warns `Unrecognized config option` for every key on every run
  (`modules/nextflow/src/main/groovy/nextflow/config/ConfigValidator.groovy:75,152`
  at v26.04.6). **Measured at v26.04.6:** the live interfaces are in
  `nextflow.config.spec` (`ConfigScope`, `ScopeName`, `ConfigOption`,
  `PlaceholderName`); the `nextflow.config.schema` names are deprecated aliases
  and `ConfigValidator` scans `config.spec`. `cas.stores.<alias>` is a
  `@PlaceholderName Map<String, <store scope>>`; `cas.index` is a nested scope.
  Implemented in `robsyme.cas.CasConfigScope`.
- `cas.pipeline` (optional string) overrides the Pipeline Identity.
- An alias matches `^[a-z][a-z0-9_-]{0,31}$` and must not parse as a CID.
- `CasLinStoreFactory.canOpen(config)` is `config?.store?.location?.startsWith('cas://') ?: false`.
  It must be total over every config, including `lineage.store.location` unset (null).
- `@Priority(-10)` on the factory.
- The writable member is the alias in `lineage.store.location`; `outputDir`
  must name the same alias (checked at run start, abort otherwise).
- In the Walking Skeleton a member location is a local directory path. The
  store abstraction (§5) is written so an S3 member can be added later.

## 3. Content identity

- Hash: SHA-256. Address: CIDv1, multibase base32 lower (prefix `b`),
  codec `raw` (0x55) for file content, `dag-cbor` (0x71) for metadata blocks,
  multihash `sha2-256` (0x12, length 0x20).
- Text form is always the base32 string: 59 characters, `bafk…` for raw,
  `bafy…` for dag-cbor. Class `robsyme.cas.core.Cid` with `parse(String)`,
  `of(codec, byte[] sha256)`, `toString()`, `bytes()`, `codec`, `digest`.
- Known vectors the tests must pass:
  - raw, empty input: `bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku`
  - raw, bytes `hello\n`: `bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am`
    (sha256 = `5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03`)
  - dag-cbor of the empty map (`0xa0`): `bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua`
    (sha256 of `a0` = `c19a797fa1fd590cd2e5b42d1cf5f246e29b91684e2f87404b81dc345c7a56a0`)
  - (All three recomputed independently with Python `hashlib` on 2026-09-03
    after an earlier revision of this file carried two wrong values.)
- Streaming hasher `robsyme.cas.core.Hashing`:
  `static Cid hashRaw(InputStream in, byte[] buffer)` using exactly the given
  1 MiB buffer; `static Cid hashRaw(Path file)` borrows a buffer from
  `HashBufferPool` (fixed pool of 32 × 1 MiB, blocking when exhausted).
- Verification is explicit, never on the read path.

## 4. DAG-CBOR

`robsyme.cas.core.DagCbor` with `static byte[] encode(Object)` and
`static Object decode(byte[])`. Strict DAG-CBOR:

- Map keys: strings only, sorted by length then bytewise (canonical DAG-CBOR
  order). Definite lengths everywhere. Smallest-width integer encoding.
- Supported values: `null`, `Boolean`, integers (`Integer`, `Long`,
  `BigInteger` within ±2^64), `Double` (always 64-bit, NaN/Infinity rejected),
  `String`, `byte[]`, `List`, `Map<String,?>`, `Cid` (tag 42 over
  `0x00 || cid.bytes()`).
- Any map with a key `"/"` is rejected on encode and decode. Any tag other
  than 42 is rejected. Floats other than 64-bit are rejected on decode.
- Decoding produces `LinkedHashMap`, `ArrayList`, `Long` for all integers,
  `Double`, `String`, `byte[]`, `Cid`, `Boolean`, `null`. Groovy `BigDecimal`
  and `Float` on encode: `Float` → `Double`; `BigDecimal` → `Double` (document
  as lossy; Nextflow Meta Maps measured contain `Integer`, `Long`, `Boolean`,
  `String`, nested maps and lists).
- `static Cid cidOf(byte[] encoded)` = `Cid.of(DAG_CBOR, sha256(encoded))`.

## 5. Block store

Interface `robsyme.cas.core.BlockStore`:

```groovy
interface BlockStore {
    String alias()
    boolean has(Cid cid)
    long size(Cid cid)                  // throws NoSuchBlockException
    InputStream open(Cid cid)           // throws NoSuchBlockException
    long lastModifiedMillis(Cid cid)    // stable per address once written
    /** Write bytes already known to hash to cid. No-op if present. */
    void put(Cid cid, InputStream in, long expectedSize)
    /** Hash while streaming to a temp file, then place under the computed address. */
    Cid putStreaming(InputStream in)
    Cid putDagCbor(Object value)        // encode, hash, put; returns the cid
    Stream<Cid> listBlocks()            // every block, for rebuild and sweep
    boolean isWritable()
}
```

`LocalBlockStore(Path root, String alias, boolean writable)`. Layout under
`root`:

```
blocks/<xx>/<cid>       xx = last two characters of the cid string. Files are chmod 0444.
                        Write = temp file (blocks/<xx>/.tmp-<random> when the cid is known up
                        front; blocks/.tmp-<random> for putStreaming, whose shard is unknown until
                        the hash completes), fsync the file, then link it into place with an
                        atomic create-if-absent (Files.createLink; never a rename that can replace
                        an existing block and move its mtime), then fsync the directory.
                        "Target already exists" is success.
runs/<rts>-<cid>        Run Log: empty file per RunCompletion. rts = String.format('%013d', 9999999999999L - finishedAtMillis)
                        so lexicographic order is newest first. cid = the RunCompletion cid.
coords/<publish path>   Publish Coordinate tree (§7). Real directories; leaves are Pointer Files.
nf/<key>/.data.json     Nextflow's own lineage records, DefaultLinStore layout. nf/.history is its history log.
```

`CompositeStore(List<BlockStore> members)`: reads resolve in order, first hit
wins; writes go only to `members[0]`, which must be writable; a write is never
skipped because a read-only member holds the block.

## 6. Block kinds

Every metadata block is a DAG-CBOR map with `kind` (string) and `schema`
(integer, `1`). Keys are `snake_case`. Links are `Cid` values (tag 42).
Timestamps are ISO-8601 UTC strings with millisecond precision, only ever as
facts about a run. Nothing store-local: no absolute paths, host names, user
names, member aliases, or surrogate ids.

Two content-derived kinds carry no `asserted_by`, so that identical content
yields one block regardless of who stored it: `DirectoryManifest` and
`OutputItem`. All other kinds carry `asserted_by` (string from config).

### DirectoryManifest
```
{ kind: "DirectoryManifest", schema: 1,
  entries: [ { name: string,                       // raw file name, one path segment
               mode: "regular"|"executable"|"symlink"|"directory"|"unresolvable",
               size: int,                          // bytes; for symlink/unresolvable, the byte length of target; for directory, 0
               address: Cid|null,                  // raw cid for regular/executable, dag-cbor cid for directory, null for symlink/unresolvable
               target: string|null } ] }           // link target text for symlink/unresolvable, else null
```
Entries sorted ascending by the UTF-8 bytes of `name`. Rules for a symlink
found while walking: if its target is relative and resolves inside the tree
being published, record `mode: "symlink"` with `target` (the relative target
string, which is portable and meaningful to a receiver); otherwise follow it
and store what it points at as regular/executable/directory; if it dangles or
cycles, `mode: "unresolvable"`. **A stored `target` is only ever a relative,
in-tree path.** An absolute target, or one that escapes the tree, is never
written into a block: the manifest carries no `asserted_by` and travels in a
Bundle, so a host path there both violates the "nothing store-local" rule and
makes the same tree hash differently under different launch prefixes. For such
an entry `target` is `"[redacted-location]"` (the same marker `scrub` uses), so
the fact of the broken link survives without its machine-local path. Cycle
detection and depth limit 64. Empty directory = `entries: []`.

### OutputItem
```
{ kind: "OutputItem", schema: 1,
  value: <the channel item structure> }
```
`value` mirrors the published channel item: a `Map` stays a map, a
tuple/list stays a list, scalars keep their types. Every file or directory
leaf is replaced by a **Leaf** map:
```
{ kind: "Leaf", name: string|null, address: Cid|null, size: int|null,
  provider: "head-node"|"fusion-node"|null,
  reason: null|"declined"|"never_published"|"unresolvable"|"unaddressed" }
```
`name` is the file name it was published under (last segment of the publish
path). `reason` is null when `address` is set and non-null otherwise; an absent
address is never an absent field. A `declined` leaf is what Nextflow hands us
as `null` in place of a path. A `never_published` leaf is a path in the item
that never received a publish event (a path outside the work dir).
Decoding rule: a map with `kind == "Leaf"` is a leaf. The item carries no run
reference and no publish path.

### OutputCollection
```
{ kind: "OutputCollection", schema: 1, asserted_by: string,
  run: Cid,                      // RunManifest
  name: string,                  // output name from the workflow output DSL
  items: [Cid|null, ...],        // OutputItem links sorted ascending by cid string; null only when Nextflow handed us a null item
  paths: [[string, ...], ...] }  // paths[i] = publish paths (relative to outputDir, '/'-joined) of item i's leaves in depth-first order; null for a leaf with no path
```

### RunManifest
```
{ kind: "RunManifest", schema: 1, asserted_by: string,
  pipeline: string,             // Pipeline Identity, first non-null of: cas.pipeline, manifest.name, session.workflowMetadata.projectName
                                // (measured: projectName is the literal "main.nf" for `nextflow run .`, so a local pipeline should set manifest.name)
  repository: string|null, revision: string|null, commit_id: string|null,
  run_name: string, nf_run_hash: string,   // Nextflow's WorkflowRun lid key (LinObserver.executionHash)
  session_id: string, resumed: bool,
  nextflow_version: string,
  params: Map, config: Map,     // scrubbed, see below
  script: Cid|null,             // raw block of the main script
  started_at: string }
```

**Portability scrub for `params` and `config`** (one function,
`Records.scrub(Object)`, unit-tested): drop the top-level config scopes
`cas`, `lineage`, `workDir`, `outputDir`, `launchDir`, `projectDir`, `homeDir`,
`configFiles`, `scriptFile`, `commandLine`, `runName` and `resume`; convert
`Path` values to strings; then replace every string value that starts with `/`
or with a `<scheme>://` other than `lid://`/`cas://` by `"[redacted-location]"`,
and every string equal to the OS user name by `"[redacted-user]"`. Measured at
v26.04.6: `session.config` contains `cas.stores.<alias>.location` (an absolute
host path), `outputDir` (a member alias) and `workDir`, so an unscrubbed copy
breaks §6's rule and Gate assertion 10.

### RunCompletion
```
{ kind: "RunCompletion", schema: 1, asserted_by: string,
  run: Cid,                             // RunManifest
  collections: [Cid, ...],              // OutputCollection links, sorted by output name
  input_set: Cid|null,                  // null in the skeleton
  status: "succeeded"|"failed",
  exit_status: int|null,
  possibly_incomplete: bool,            // true for every failed run (no barrier exists on that path)
  started_at: string, finished_at: string,
  anomalies: { unresolvable: int, unaddressed: int, declined: int, never_published: int },
  error: string|null }
```

### Claim, InputSet, Attestation
Specified in the spec; not built in the skeleton. Reserve the kind names.

## 7. URIs, coordinates and the join key

Two shapes share the `cas` scheme, told apart by whether the authority parses
as a CID (`Cid.parse` succeeds):

- **Store URI** `cas://<cid>[/<segment>...]`. Immutable, location-free.
  - raw cid: at most one trailing segment, the presented file name.
  - DirectoryManifest cid: traversal by entry names.
  - RunCompletion or OutputCollection cid: traversal by recorded publish
    paths (run-rooted reference). Skeleton: implement raw and manifest;
    run-rooted may be stubbed with `UnsupportedOperationException` until the
    observer exists.
- **Publish Coordinate** `cas://<alias>/<relative path>`. The write-side name
  Nextflow's `PublishDir` hands us. Persisted in the writable member as a
  Pointer File tree under `coords/`: intermediate segments are real
  directories, a leaf is a text file whose single line is the Store URI
  `cas://<cid>/<name>` (raw cid for a file, manifest cid for a directory).
  Not blocks, not roots, overwritten by the next run to the same coordinate.

`robsyme.cas.core.Coordinates`:
- `static String key(String uriOrPath)` and `static String key(Path)`: the
  **join key**, a canonical `cas://<alias>/<a/b/c>` with segments normalised
  (`.` and `..` resolved, no duplicate or trailing slashes, no percent
  escaping changes). Every place that maps a publish target to an address
  (`upload()`, `onFilePublish`, `onWorkflowOutput`, `save()`) uses this one
  function. Never `Path.equals`.
- `CoordinateTree(Path coordsRoot)`: `Optional<StoreRef> read(String relPath)`,
  `void write(String relPath, StoreRef ref)`, `boolean exists(String relPath)`,
  `boolean isDirectoryCoordinate(String relPath)`.
- `StoreRef(Cid cid, String name)` ⇄ `cas://<cid>/<name>`.
- `Cid` parsing of a `lid://…` is never attempted here; `lid://` is Nextflow's.

## 8. The provider (`robsyme.cas.nio`)

`CasPathFactory extends FileSystemPathFactory`, listed in `extensionPoints`:
- `parseUri(String)`: returns a `CasPath` for `cas://…`, else null.
- `toUriString(Path)`: `cas://…` for a `CasPath`, else null.
- `getBashLib`/`getUploadCmd`: return null (local executor only in the skeleton).

`CasFileSystemProvider extends FileSystemProvider implements nextflow.file.FileSystemTransferAware`.
Install with `nextflow.file.FileHelper.getOrInstallProvider(CasFileSystemProvider)`
from `CasPlugin.start()` (the idiom `nf-google` and the probe used). Scheme
`cas`. One `CasFileSystem` per JVM, backed by the `CompositeStore` from config.

`CasPath`: immutable, holds `authority` (alias or cid string, null for a
relative path) and a normalised segment list. `toUri()` yields the canonical
form. `startsWith(otherFs)` returns false rather than throwing.
`equals`/`hashCode` on `(authority, segments)`. `toString()` of an
**absolute** path is the URI string; a **relative** path (what `getFileName()`,
`getName(i)`, `subpath` and `relativize` return) stringifies as its bare
`/`-joined segments, because Nextflow uses `getFileName().toString()` as a
file name. A store root stringifies as `cas://<authority>` with no trailing
slash in both `toString()` and `toUri()`.

Read side (Store URIs and Coordinates alike), all must tell the truth:
- `readAttributes` (both overloads): size from the block or manifest entry,
  `isRegularFile`/`isDirectory` from the cid codec or coordinate kind,
  `lastModifiedTime` = the block file's mtime (stable per address). For a
  coordinate, resolve through the Pointer File then answer for the target.
- `checkAccess`: `NoSuchFileException` when absent, `AccessDeniedException`
  for `WRITE` on a Store URI.
- `newInputStream`, `newByteChannel` (read-only): stream the block.
- `newDirectoryStream`: entries of a manifest, or children of a coordinate directory.
- `isSameFile`, `isHidden`, `getFileStore` (unsupported), `getFileAttributeView`.

Write side (Coordinates only; any write to a Store URI is `AccessDeniedException`):
- `createDirectory`: create the coordinate directory under `coords/`.
- `newOutputStream` on a coordinate: allowed only so Nextflow's incidental
  writes (none expected in the skeleton) do not crash; implement as
  hash-on-close through a temp file, then write the pointer.
- `delete`/`deleteIfExists` on a coordinate: remove the Pointer File only.
  Never touches a block.
- `canUpload(source, target)`: `target instanceof CasPath && target.isCoordinate()`.
- `upload(source, target, options)`:
  1. `key = Coordinates.key(target)`. If the coordinate exists and
     `REPLACE_EXISTING` is absent, throw `FileAlreadyExistsException(key)`
     **before reading any byte** (this is what makes `-resume` cheap).
  2. Regular file: `cid = store.putStreaming(Files.newInputStream(source))`
     with the 1 MiB buffer, then write the Pointer File. Record
     `(key → StoreRef, size, provider 'head-node')` in `CasSession.publishes`.
  3. Directory: walk it yourself (Nextflow does not recurse), hash every
     file, build the `DirectoryManifest` recursively, put it, write the
     Pointer File pointing at the manifest cid. Never return normally with
     any child untransferred. Record the manifest and the anomaly counts.
- `canDownload(source, target)`: `source instanceof CasPath`.
- `download(source, target, options)`: raw → copy the block to `target`
  (symlink when both are on the same local filesystem and the block is
  read-only; otherwise stream and hash in flight, aborting on mismatch);
  manifest → materialise recursively, recreating `symlink` entries as
  relative symlinks, failing loudly on `unresolvable`. Absent block →
  `NoSuchFileException` naming the cid.

Measured behaviours to respect (see `.scratch/research/plugin-filesystem-schemes.md`
and the probe logs in `.scratch/research/nf-casx-probe/*.log`): Nextflow calls
`createDirectory` on the parent, `checkAccess`/`readAttributes` on the target
for overwrite logic, `upload()` once per published path including directories,
`readAttributes` twice per file for the fingerprint, and `newInputStream` when
`overwrite 'sha256'` is set.

## 9. Per-run shared state: `robsyme.cas.CasSession`

One instance per Nextflow `Session`, obtained by `CasSession.of(session)`
(a `ConcurrentHashMap<Session, CasSession>` in a static; the provider reaches
it through `Global.session`). Holds: the `CasConfig`, the `CompositeStore`,
the `CoordinateTree`, `asserted_by`, a `ConcurrentHashMap<String, Publish>`
keyed by join key (`Publish(StoreRef ref, long size, String provider,
Anomalies anomalies)`), the Nextflow run key once `save(<hash>, WorkflowRun)`
is seen, the RunManifest cid once written, the captured `WorkflowOutputEvent`s,
and a one-shot latch for `onFlowComplete`.

## 10. `CasLinStore` (`robsyme.cas.lineage`)

- `open(config)`: resolve the alias from `lineage.store.location`, build the
  composite store from the `cas` scope (`Global.session.config`), and open a
  delegated `nextflow.lineage.DefaultLinStore` at `<writable>/nf` for
  Nextflow's own records. Abort with `AbortOperationException` if the writable
  member cannot be created.
- `save(key, value)`:
  - `WorkflowRun` → record `key` as the Nextflow run key in `CasSession`.
  - `FileOutput` whose `path` is a Publish Coordinate → rewrite `path` to the
    Store URI from `CasSession.publishes[key]` (or the Pointer File if the
    JVM lost it), then recompute
    `checksum = Checksum.ofNextflow(FileHelper.asPath(newPath))` so
    `lid://` reads do not warn. If no address exists for a coordinate that
    was published, throw `AbortRunException`.
  - Everything else → delegate unchanged.
  - Any `IOException` → `AbortRunException`.
- `load`, `search`, `getSubKeys`, `getHistoryLog` → delegate. `search` must
  operate on decoded `LinSerializable` objects so `type: ['FileOutput']` works
  (this is what `channel.fromLineage` sends).
- `close()` is never called by Nextflow; nothing may depend on it.

## 11. `CasObserver` (`robsyme.cas.trace`)

- `onFlowCreate(session)`: bind `CasSession`, validate config (`outputDir`
  alias equals the lineage alias), open the index lazily.
- `onFlowBegin()`: write the `RunManifest` (the Nextflow run key is known
  because every observer's `onFlowCreate`, including `LinObserver`'s, has run).
- `onFilePublish(event)`: nothing to hash (our `upload()` already did). Look
  up `Coordinates.key(event.target)` in `publishes`; if absent, resolve
  through the Pointer File; if still absent, `AbortRunException`. Attach
  labels.
- `onWorkflowOutput(event)`: store the event (name, value) for the join. If
  `value` is null because an `index {}` block exists, record the anomaly.
- `onTaskCached(event)`: record the task hash so the task layer is not empty
  (skeleton: log only; address reuse by task hash is a later task).
- `onFlowComplete()`: one-shot. Build every `OutputItem` from the captured
  events (replace each `Path` leaf by a Leaf using `publishes`), each
  `OutputCollection` (items sorted by cid string, `paths` aligned), then the
  `RunCompletion` (status from `session.isSuccess()`, `exit_status` from
  `session.workflowMetadata.exitStatus`, `possibly_incomplete = !success`).
  Write the Run Log entry. Then update the index (§12) for this run; index
  failure logs and marks the index stale, never aborts.

## 12. Index (`robsyme.cas.core.Index`)

SQLite via `org.xerial:sqlite-jdbc` (bundled in the plugin zip), WAL mode.
Path: `cas.index.path` if set, else
`${XDG_CACHE_HOME:-$HOME/.cache}/nf-blocks/<first 16 hex of sha256(member locations joined by '\n')>.sqlite`.
Schema (spec §9), verbatim:

```
schema_version(version)
run(completion_cid PK, manifest_cid, pipeline, revision, commit_id,
    nf_run_hash, session_id, run_name, asserted_by,
    status, possibly_incomplete, finished_at, member)
  index (pipeline, status, finished_at DESC)
  index (nf_run_hash), index (manifest_cid)
collection(collection_cid PK, completion_cid, output_name)
item(item_cid PK)
collection_item(collection_cid, item_cid)
producer(content_cid, item_cid, collection_cid, completion_cid, filename)
  index (content_cid)
consumer(content_cid, completion_cid, name, how)
item_attr(item_cid, path, type, value, truncated)
  index (path, type, value)
claim_current(subject_cid, attribute, value, claim_cid, conflicted)
missing(have_cid, needed_cid)
nf_record(key PK, kind, workflow_run, task_run, labels_json, block_cid)
```

- `item_attr`: every scalar leaf of the item's **metadata view** (the item
  itself if a Map, else its first top-level Map) under its dotted path; array
  elements under the array's path; `type` ∈ `string|int|float|bool|null`;
  `value` is the text form; strings longer than 1024 bytes stored as
  `sha256:<hex>` with `truncated = 1`.
- `producer.filename` = the Leaf name.
- Rebuild: `Index.rebuild(store)` deletes and recreates from `bafy…` blocks
  only, into a temp file renamed into place. Schema mismatch → rebuild.
- Ingest: `Index.ingestRun(store, completionCid)` reads the RunCompletion
  and its closure. `Index.catchUp(store)` reads the Run Log past a watermark.
- Queries: `producersOf(Cid content) → List<ProducerRow>`;
  `latestSuccessfulRun(String pipeline) → Optional<Cid completion>`;
  `items(Cid completion, String outputName, Map<String,Object> where) → List<Cid item>`;
  `runByNextflowHash(String) → Optional<Cid completion>`;
  `runByManifest(Cid) → Optional<Cid completion>`.
  `successful` = `status == 'succeeded' AND possibly_incomplete = 0`
  (delete claims arrive later).

## 13. `fromStore` (`robsyme.cas.ext.CasExtension`)

`include { fromStore } from 'plugin/nf-blocks'`, then
`channel.fromStore(run: <ref>, output: 'aligned', where: [sample: 'B'])`.
`run` is a RunManifest or RunCompletion Store URI, a `lid://<runHash>`, or
`'latest'` together with `pipeline: '<Pipeline Identity>'`. Emits each
matching OutputItem restored to its published structure: a file leaf →
`CasPath` `cas://<cid>/<name>`; a **directory leaf → `CasPath` `cas://<cid>`**
(a dag-cbor manifest address the provider presents as a directory, per ticket
08); declined → `null`; an `unaddressed` leaf → error naming the item. A run
without a RunCompletion → error. Implemented with `@Factory`; resolve eagerly
so a bad run reference or an unaddressed item fails fast, but bind onto the
channel inside a `session.addIgniter` closure so a downstream subscriber is
attached first. **Measured at v26.04.6:** there is no `NF.dsl2` (DSL1 is gone)
and `CH.create()` is a non-buffering `DataflowBroadcast`, so eager binding
without the igniter would drop items.

## 14. The Gate (`gate/`)

`gate/gate.sh` builds and installs the plugin into a throwaway
`NXF_PLUGINS_DIR`, runs the Test Pipeline at
`../.scratch/content-addressed-lineage/test-pipeline` four ways (cold; again
into the same store; `--fail`; `-resume`) with `XDG_CACHE_HOME` and the store
under a fresh temp directory, then runs `gate/assert.py` (Python 3 standard
library only: `hashlib`, `sqlite3`, `json`, plus a small DAG-CBOR decoder and
CID encoder of its own). Exit non-zero on any failed assertion. The Gate
config overlay lives at `gate/gate.config`. A `gate/Dockerfile` wraps the same
script for CI.
