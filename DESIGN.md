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
   `-resume` identity. *Amended 2026-09-24: the command-line and web surface
   rule is lifted for the block explorer alone, specified in
   `../.scratch/block-explorer/spec.md`.*
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
  - `robsyme.cas.cli` — `CasCommands` (the `nextflow plugin nf-blocks:<verb>` dispatch, §15) and `Options`.
  - `robsyme.cas.explore` — `ExploreServer`, `MemberFiles` and its local and S3 implementations (§15).
- The page is an npm project under `web/`, built by Gradle into the plugin jar as
  `robsyme/cas/explorer/index.html` (block explorer spec section 2). Its output is not committed.
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
    snapshot { maxBytes = 64.MB }   // optional; a run writes the Index Snapshot only while it is under this (§15)
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
- `cas.snapshot.maxBytes` (a number of bytes, a `MemoryUnit`, or a string such as
  `'64 MB'`; default 64 MiB): the cap under which a run rewrites its member's
  Index Snapshot at `onFlowComplete` (§15).
- *Amended 2026-09-25:* a read-only member's `location` may be an S3 URI,
  `s3://<bucket>[/<prefix>]`. Only `nf-blocks:explore` reads such a member (§15).
  A run leaves S3 members out of its default `resolve` list and refuses one named
  in `cas.resolve`, because no S3 `BlockStore` exists yet. The writable member is
  always a local directory.

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
                        AMENDED 2026-09-24 (block explorer spec section 3, a v1 change): replaced by the Store Log,
                        log/<rts>-<kind>-<cid> with kind run|selection|claim, rts from the moment the entry is
                        written to this member rather than finishedAt, runs/ dropped with no compatibility read.
coords/<publish path>   Publish Coordinate tree (§7). Real directories; leaves are Pointer Files.
nf/<key>/.data.json     Nextflow's own lineage records, DefaultLinStore layout. nf/.history is its history log.
index/v<N>.sqlite       Index Snapshot of this member's rows, N = Index.SCHEMA_VERSION (§15). Derived,
                        rewritten whole by an atomic move, mode 0644. Not a block, not a root.
index.html              The explorer page, self-contained (§15). Rewritten when its bytes differ.
```

`CompositeStore(List<BlockStore> members)`: reads resolve in order, first hit
wins; writes go only to `members[0]`, which must be writable; a write is never
skipped because a read-only member holds the block.

## 6. Block kinds

Every metadata block is a DAG-CBOR map with `kind` (string) and `schema`
(integer, `1`). Keys are `snake_case`. Links are `Cid` values (tag 42).
Timestamps are ISO-8601 UTC strings with millisecond precision, only ever as
facts about a run, or as a Claim's advisory `timestamp`. Nothing store-local: no absolute paths, host names, user
names, member aliases, or surrogate ids.

Two content-derived kinds carry no `asserted_by`, so that identical content
yields one block regardless of who stored it: `DirectoryManifest` and
`OutputItem`. All other kinds carry `asserted_by` (string from config).
Confirmed by Rob 2026-09-24; the lineage spec and glossary now say the same.

### IPLD Schema (amended 2026-09-24)

Every kind below, and the block explorer's Selection and Claim, in IPLD
Schema DSL as one inline union on `kind` (block explorer spec section 12).
The per-kind notation that follows remains for its prose rules: sort orders,
the symlink and scrub rules, the Leaf decoding rule. Where the two disagree,
fix one; neither silently wins. `nullable` means the key is always present
and may hold null; nothing here is `optional`. Link targets are named for the
reader; a decoder checks the target's `kind` when it follows one.

```ipldsch
type Block union {
  | DirectoryManifest "DirectoryManifest"
  | OutputItem "OutputItem"
  | OutputCollection "OutputCollection"
  | RunManifest "RunManifest"
  | RunCompletion "RunCompletion"
  | Selection "Selection"
  | Claim "Claim"
} representation inline {
  discriminantKey "kind"
}
# Reserved kinds, specified in the lineage spec, not yet built: InputSet, Attestation.

type DirectoryManifest struct {
  schema Int
  entries [DirEntry]            # sorted by the UTF-8 bytes of name
}
type DirEntry struct {
  name String
  mode EntryMode
  size Int
  address nullable &Any         # raw cid for regular/executable, &DirectoryManifest for directory
  target nullable String        # relative in-tree target, or "[redacted-location]"
}
type EntryMode enum {
  | regular
  | executable
  | symlink
  | directory
  | unresolvable
}

type OutputItem struct {
  schema Int
  value Any                     # the channel item; every file leaf is a Leaf map (prose rule)
}
# Not a Block member: found inside OutputItem.value by its kind field.
type Leaf struct {
  kind String                   # always "Leaf"
  name nullable String
  address nullable &Any
  size nullable Int
  provider nullable Provider
  reason nullable LeafReason
}
type Provider enum {
  | HeadNode ("head-node")
  | FusionNode ("fusion-node")
}
type LeafReason enum {
  | declined
  | never_published
  | unresolvable
  | unaddressed
}

type OutputCollection struct {
  schema Int
  asserted_by String
  run &RunManifest
  name String
  items [nullable &OutputItem]  # sorted by cid string
  paths [[nullable String]]
}

type RunManifest struct {
  schema Int
  asserted_by String
  pipeline String
  repository nullable String
  revision nullable String
  commit_id nullable String
  run_name String
  nf_run_hash String
  session_id String
  resumed Bool
  nextflow_version String
  params {String:Any}
  config {String:Any}
  script nullable &Any
  started_at String
}

type RunCompletion struct {
  schema Int
  asserted_by String
  run &RunManifest
  collections [&OutputCollection]   # sorted by output name
  input_set nullable &Any
  status RunStatus
  exit_status nullable Int
  possibly_incomplete Bool
  started_at String
  finished_at String
  anomalies Anomalies
  error nullable String
}
type RunStatus enum {
  | succeeded
  | failed
}
type Anomalies struct {
  unresolvable Int
  unaddressed Int
  declined Int
  never_published Int
}

# Block explorer spec section 7.
type Selection struct {
  schema Int
  asserted_by String
  members [Member]              # sorted by member address; each address once
  derived_from [Bytes]          # binary cids, weak references, sorted
}
type Member union {
  | ItemMember "item"
  | SelectionLink "selection"
} representation keyed
type ItemMember struct {
  address &OutputItem
  via [&OutputCollection]       # sorted; followed for metadata only
}
type SelectionLink &Selection

# Block explorer spec section 8.
type Claim struct {
  schema Int
  asserted_by String
  subject &Any
  verb ClaimVerb
  attribute nullable String
  value nullable Any
  supersedes [&Claim]           # sorted by cid string
  timestamp String              # advisory, from the request
}
type ClaimVerb enum {
  | set
  | add
  | del
  | delete
}
```

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
  - *Amended 2026-09-24:* **Item Occurrence**
    `cas://<OutputCollection cid>/<OutputItem cid>[/<leaf name>]`, one item
    as it appeared in one run's output. Rule: when the root is an
    OutputCollection and the first segment parses as a CID listed in that
    collection's `items`, the URI names the occurrence; otherwise the
    segments are a publish path, as above. If neither resolves, the error
    names both readings. No reserved word, so no publish directory name can
    collide; the only ambiguity would be a publish directory named exactly
    after an item CID in the same collection. Without a leaf name the path
    is presented as a directory of the item's leaves by leaf name; with one,
    it is that file. Canonical form: CIDs in their string form, no trailing
    slash. Built 2026-09-25 (milestone 2, Task 7): a leaf name two leaves of
    one item share is refused, naming both positions.
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
  Write the Store Log entry (`run`, stamped with the write time). Then update
  the index (§12) for this run; index failure logs and marks the index stale,
  never aborts.
  *Amended 2026-09-25:* a failed run is notified twice, once on a Nextflow
  finalizer thread inside `Session.abort` and once from `Session.destroy` on
  main, and Nextflow interrupts the finalizer threads while the first is
  still writing. So the notification that claims the completion writes it on
  a plugin-owned thread (`nf-blocks-completion`) and waits for it without
  honouring interrupts, then restores the interrupt; the other notification
  waits up to 60 s for that write before returning, so the JVM cannot reach
  `System.exit` mid-write. Any failure writing the provenance, a checked
  exception included, reaches the caller as `AbortRunException` (§0 rule 3);
  before this change a checked exception was swallowed at debug level.

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
collection(collection_cid PK, kind, completion_cid, output_name, asserted_by)
item(item_cid PK)
collection_item(collection_cid, item_cid, via_cid)
selection_child(parent_cid, child_cid)
  index (child_cid)
selection_derived(selection_cid, derived_from_cid)
producer(content_cid, item_cid, collection_cid, completion_cid, filename)
  index (content_cid)
consumer(content_cid, completion_cid, name, how)
item_attr(item_cid, path, type, value, truncated)
  index (path, type, value)
log_entry(cid, kind, member, written_at)
  unique index (cid, member)
  index (kind, written_at DESC)
claim(claim_cid PK, subject_cid, verb, attribute, value, timestamp, asserted_by)
  index (subject_cid)
claim_supersedes(claim_cid, superseded_cid)
  index (superseded_cid)
claim_current(subject_cid, attribute, value, claim_cid, conflicted)
  index (subject_cid, attribute)
missing(have_cid, needed_cid)
nf_record(key PK, kind, workflow_run, task_run, labels_json, block_cid)
```

*Amended 2026-09-24 (block explorer spec section 13), v1 changes:*

```
index collection(completion_cid, output_name)   -- query 3's join; without it
index collection_item(collection_cid)           -- query 3 scans both tables
```

*Amended 2026-09-25 (block explorer milestone 2): schema 3, spec section 11,
plus the indexes of decision 3 of `docs/plans/2026-09-25-explorer-milestone-2.md`.*

Measured on a year-scale index (1,825 runs, 580 MB): query 3 fell from 105
page reads and 28.6 MB (2.2 s even warm) to 42 page reads and 180 KB. Catch-up
reads the Store Log past the watermark **plus a 10-minute overlap**, since an
entry can be written behind the watermark by a Bundle merge or a host with a
skewed clock; ingest is idempotent. Rebuild also reads the Store Log, to fill
the explorer's `log_entry` table. Selection tables: explorer spec section 11.

- `item_attr`: every scalar leaf of the item's **metadata view** (the item
  itself if a Map, else its first top-level Map) under its dotted path; array
  elements under the array's path; `type` ∈ `string|int|float|bool|null`;
  `value` is the text form; strings longer than 1024 bytes stored as
  `sha256:<hex>` with `truncated = 1`.
- `producer.filename` = the Leaf name.
- Rebuild: `Index.rebuild(store)` deletes and recreates from `bafy…` blocks
  only, into a temp file renamed into place. *Amended 2026-09-25:* a schema
  mismatch recreates the index empty on open; the first `catchUp` for each
  member then scans that member's blocks once (recorded as
  `block_scan:<member>` in `meta`), which is also how a store written before
  the Store Log keeps its runs. `rebuild` reads the Store Log only to carry
  the watermark forward.
- Ingest: `Index.ingestRun(store, completionCid)` reads the RunCompletion
  and its closure. `Index.catchUp(store, storeLog, member)` reads the Store
  Log past the watermark plus a 10-minute overlap (floor clamped to the local
  clock), skipping runs already indexed; retries every run recorded as
  `missing`; and records an unreachable block as `missing` rather than
  aborting the member. Every Store Log entry read is recorded in `log_entry`
  (earliest time per `(cid, member)`); rebuild fills it from the whole log.
  Selections are ingested from `selection` Store Log entries (spec section
  11); `Index.selectionItems` resolves nesting with `SQL_SELECTION_ITEMS` and
  refuses a partial answer.
- Queries: `producersOf(Cid content) → List<ProducerRow>`;
  `latestSuccessfulRun(String pipeline) → Optional<Cid completion>`;
  `items(Cid completion, String outputName, Map<String,Object> where) → List<Cid item>`;
  `runByNextflowHash(String) → Optional<Cid completion>`;
  `runByManifest(Cid) → Optional<Cid completion>`.
  `successful` = `status == 'succeeded' AND possibly_incomplete = 0`
  and no current `delete` Claim names the RunCompletion, a conflicted
  deletion included; `latestSuccessfulRun` warns once per conflicted run it
  leaves out. Claims are ingested from `claim` Store Log entries into `claim`
  and `claim_supersedes`; `claim_current` is `ClaimState` (decision 5 of the
  milestone 2 plan) per subject.
- *Amended 2026-09-25:* the three load-bearing queries' SQL is held in public
  constants (`Index.SQL_PRODUCERS_OF`, `SQL_LATEST_SUCCESSFUL_RUN`,
  `SQL_ITEMS_BASE`, `SQL_ITEMS_PREDICATE`, `SQL_ITEMS_PREDICATE_NULL`,
  `SQL_ITEMS_ORDER`, `SQL_COLLECTIONS_OF`), and the page's copy in
  `web/src/queries.json` is pinned equal to them by `ExplorerQueriesTest`, which
  also refuses any page query whose plan scans a table other than `run` through
  a covering index (§15).

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

## 15. The block explorer (milestone 1)

*Status 2026-09-25: milestone 1 accepted; Gate browser tier A 7 of 7 (local 5, cloud 2).*

Specified in `../.scratch/block-explorer/spec.md`; this section fixes the names,
paths and seams its pieces share. Plan: `docs/plans/2026-09-25-explorer-milestone-1.md`.

### Index Snapshot

- Path `<member>/index/v<Index.SCHEMA_VERSION>.sqlite`, today `index/v3.sqlite`.
  Class `robsyme.cas.core.IndexSnapshot`.
- Rows: every `run` the member's Store Log announced (`log_entry.member`) or
  its blocks held at the first scan (`run.member`), and the rows reached from
  those runs, plus the member's `log_entry` rows with `member` NULL.
  Claims: the `claim` and `claim_supersedes` rows of the Claims whose
  `log_entry` names the member, with `claim_current` recomputed from those
  alone. Selections: those whose `log_entry` names the member, with their
  `collection_item`, `selection_child` and `selection_derived` rows. An item
  another member produced keeps its membership row and has no `item_attr`
  rows; the page shows it as held elsewhere.
- Same DDL as the cache index (`Index.ddl()`), same `schema_version`.
- `meta` holds exactly `store_log_watermark` (the member's watermark in the
  cache index, a Store Log entry name; absent when the member has no log) and
  `snapshot_written_at` (ISO-8601 UTC, milliseconds).
- Written by inserting into a fresh database, `PRAGMA page_size=4096`, then
  `VACUUM INTO` a temp file in `<member>/index/`, then an atomic move over the
  old file. One rollback-journal file, no sidecars.
- Writers: a run's `onFlowComplete`, after indexing, only while the existing
  snapshot is under `cas.snapshot.maxBytes` and only if the new one is too;
  `nf-blocks:snapshot` at any size; `nf-blocks:explore` at start and at exit, at
  any size. Every writer writes only the writable member's snapshot.
- `<member>/index.html` is the page from the plugin jar
  (`/robsyme/cas/explorer/index.html`), written beside the snapshot whenever a
  snapshot is written and its bytes differ.

### Plugin verbs

`CasPlugin implements nextflow.cli.PluginExecAware` and delegates to
`robsyme.cas.cli.CasCommands`. It does not use `PluginAbstractExec`, which
swallows every exception and returns 0 (v26.04.6,
`modules/nextflow/src/main/groovy/nextflow/cli/PluginAbstractExec.groovy`). The
config comes from `ConfigBuilder` over the launch directory and `-c`, exactly as
`PluginAbstractExec` builds it; no `Session` is created.

```
nextflow [-c <config>] plugin nf-blocks:snapshot
nextflow [-c <config>] plugin nf-blocks:explore [--port <n>]
nextflow [-c <config>] plugin nf-blocks:put <file> [--dry-run]
```

`CmdPlugin` turns `--name value` into the argument pair `--name`, `value` after
the positional arguments. Exit 0 on success, 1 on a failure the verb reports, 2
on a usage error.

`put` prints the response or error body (DAG-JSON) on stdout, exit 0 or 1.
Nextflow 26.04.6's launcher refuses a bare `-` (`Unknown option: -`), so read
stdin as `/dev/stdin`; `-` works for in-process callers. A bare `--dry-run`
arrives as `--dry-run`, `true`.

A **published** plugin needs none of what follows: once `nf-blocks` is on the
plugin registry, an unpinned `nextflow plugin nf-blocks:<verb>` resolves and
starts it the ordinary way, offline or not. Driving a **locally built,
unpublished** plugin's verbs from the real CLI needs one extra step in
v26.04.6, because `CmdPlugin.run()` starts the plugin unpinned
(`Plugins.start('nf-blocks')`) and, with `NXF_OFFLINE=true`, an unpinned,
unregistered plugin always fails with `Cannot find version for nf-blocks
plugin -- plugin versions MUST be specified in offline mode`
(`PluginUpdater.groovy:346`): the plugin manager the plain launcher selects
(`LocalPluginManager`, whenever `NXF_HOME` is set) discovers plugins from a
fresh per-process directory, never `NXF_PLUGINS_DIR`, so it never already
knows about a plugin that is merely unpacked there. Pinning the version on
the command line instead (`nf-blocks@0.1.0:snapshot`) does not help either --
`CmdPlugin` looks the started plugin back up with that same versioned
string, which does not match the bare id (`nf-blocks`) the plugin registers
itself under, and aborts with `Cannot find target plugin: nf-blocks@0.1.0`.

The invocation that works leaves `NXF_OFFLINE` unset and points
`NXF_PLUGINS_TEST_REPOSITORY` (`PluginUpdater.customRepos()`, only added to
the repository list when not offline) at a `file://` URL for a small
`plugins.json` describing the built zip:

```json
[{"id":"nf-blocks","releases":[{"version":"0.1.0","date":"2026-09-25T00:00:00Z",
  "url":"file:///abs/path/to/build/distributions/nf-blocks-0.1.0.zip",
  "requires":">=26.04.6","sha512sum":"<sha512 of that zip>"}]}]
```

```bash
export NXF_PLUGINS_DIR=<plugins dir> XDG_CACHE_HOME=<cache dir>
export NXF_PLUGINS_TEST_REPOSITORY="file:///abs/path/to/plugins.json"
nextflow [-c <config>] plugin nf-blocks:snapshot   # NXF_OFFLINE left unset
```

`<plugins dir>` must hold the current build (`installPlugin`, or a fresh
`unzip` of `build/distributions/nf-blocks-<version>.zip`) -- an install that
predates `CasPlugin implements PluginExecAware` fails with `Invalid target
plugin`, not the errors above. This still reaches the real plugin registry
once, for a dependency-resolution call the custom repository does not
replace (`HttpPluginRepository`, seen in the debug log as `GET
https://registry.nextflow.io/api/v1/plugins/dependencies?plugins=nf-blocks&
nextflowVersion=26.04.6`); the plugin's own version and location come from
`plugins.json`, and since the zip is already unpacked at
`<plugins dir>/nf-blocks-<version>`, nothing is downloaded.
`gate/browser/plugin-repo.sh <repo> <out dir>` writes that `plugins.json` for
the installed build; the Gate's browser tier uses it.

`nextflow run` is unaffected: `Plugins.load(config)` installs the version
pinned in the `plugins {}` block directly, never through
`Plugins.start(target)`, so the Gate's `id 'nf-blocks@0.1.0'` in `gate.config` needs none of this.

### What a member serves

Relative to a member's base URL:

```
index/v3.sqlite            the Index Snapshot, read with single-range GETs
index.html                 the page
blocks/<xx>/<cid>          a block, xx = the cid's last two characters
log/                       the Store Log listing (one of the three forms below)
```

The page lists `log/` in the first form that answers:

1. `GET <base>log/` answering `200` with `Content-Type: application/json`:
   `{"entries": ["<rts>-<kind>-<cid>", ...]}`, sorted ascending (newest first).
   This is what `nf-blocks:explore` serves.
2. `GET <base>log/` answering `200` with HTML: a static server's directory
   index; entry names are the `href` values that parse as Store Log names.
3. S3 `ListObjectsV2` at `<origin>/?list-type=2&prefix=<base path>log/`,
   following `NextContinuationToken`, stopping once a page's last key is older
   than the overlap floor. Virtual-hosted-style bucket URLs only.

If none answers, the tail is empty and the page says the Store Log is not
readable (`#stale[data-log="unreadable"]`) rather than showing "0 runs newer".

A directly browsed bucket needs the policy and CORS rule of spec section 6:
`s3:GetObject` and `s3:ListBucket` for `Principal "*"`, Block Public Access's
`BlockPublicPolicy` and `RestrictPublicBuckets` off, and a CORS rule allowing
`GET` and `HEAD` with `AllowedHeaders` including `range` and `ExposeHeaders`
`Content-Range`, `Content-Length`, `Accept-Ranges`, `ETag`.

### `nf-blocks:explore`

A JDK `HttpServer` bound to `InetAddress.getLoopbackAddress()`, port `--port`
or ephemeral. Prints `nf-blocks explorer: http://127.0.0.1:<port>/?token=<token>`
on stdout once it is listening, then blocks until the JVM is interrupted.

| Request | Answer |
|---|---|
| `GET /`, `GET /index.html` | the page |
| `GET /members.json` | `{"members": [{"alias", "writable", "base": "m/<alias>/"}, ...], "write": <bool>}`, writable first |
| `GET` or `HEAD /m/<alias>/index/v<N>.sqlite` | the file, honouring one `Range`, with an `ETag` |
| `GET` or `HEAD /m/<alias>/blocks/<xx>/<cid>` | the block, honouring one `Range`; `xx` must equal the cid's last two characters |
| `GET /m/<alias>/log/` | listing form 1 |
| `POST /api/put[?dry_run=true]` | the same `Put` as `nf-blocks:put` (block explorer spec sections 9.2 and 9.5): `403` without the right `X-NF-Blocks-Token` header, `415` for a content type other than `application/json` or `application/vnd.ipld.dag-json` (parameters such as `charset` ignored), `413` over `Put.MAX_REQUEST_BYTES` (2 MiB), else the builder's status and DAG-JSON body with `Content-Type: application/vnd.ipld.dag-json` |
| anything else | `404`; a method other than `GET`, `HEAD` (or `POST` on `/api/put`) is `405` |

`Host` must be `127.0.0.1:<port>` or `localhost:<port>`, and `Origin`, when
sent, `http://127.0.0.1:<port>` or `http://localhost:<port>`; otherwise `403`.
No CORS headers are sent. No other path under a member is ever served:
`coords/` and `nf/` hold host paths. Members are every configured store
(`cas.stores`), local ones read from disk, S3 ones with the AWS SDK default
credential chain (`AWS_PROFILE`, SSO) and ranged `GetObject`.

Every snapshot and block answer carries a strong `ETag` taken from the file as
opened for that answer, never from a second look at the path: size,
modification time and file key (inode) for a local member, the object's own
`ETag` for an S3 one, whose reads are `GetObject` with `If-Match` on it (a
replaced object is a `500`, never another object's bytes).

The exit rewrite runs in a shutdown hook. Stop a backgrounded `explore` with
`SIGTERM`: a job `&`-backgrounded from a non-interactive shell (a script, CI,
the Gate) inherits `SIGINT` ignored, HotSpot leaves it ignored, and no hook
runs. An interactive Ctrl-C, or a shell with `set -m`, is unaffected.

### The page

- Store: `?store=<base URL>` if given; else, if `./members.json` answers, the
  member `?member=<alias>` or the first listed, at `./m/<alias>/`; else the
  page's own directory (spec section 5.1).
- `?cap=<bytes>` overrides the 64 MiB whole-file cap.
- Routes (location hash): `#/` home, `#/idle` (opens the snapshot and does
  nothing else), `#/pipeline/<name>[?offset=<n>]`, `#/run/<completion>`,
  `#/collection/<collection>[?offset=<n>]`, `#/item/<collection>/<item>`,
  `#/content/<cid>` (query 1), `#/latest/<pipeline>` (query 2),
  `#/items/<completion>/<output>?where=<JSON [[path, type, value], ...]>`
  (query 3, per run; `type` is one of `string`, `int`, `float`, `bool`, `null`).
- Pages: a pipeline's runs 50 at a time, a collection's items 500 at a time;
  `offset` counts snapshot rows. The Store Log tail's runs are all on the
  first page and counted in its span and in the total (`runCount` plus the
  tail); a stale collection's total is its block's item count.
- SQL: only the statements in `web/src/queries.json`.
- Blocks: fetched from `<base>blocks/<xx>/<cid>`, SHA-256 checked against the
  requested CID, then decoded and checked against the IPLD Schema of §6
  (extracted from this file at build time), before use.
- Snapshot version: the probe records the snapshot's `ETag` (else its
  `Last-Modified`) and the `Content-Range` total, and every later range is
  checked against both, from the headers it already carries. A mismatch fails
  that query, and every later one, with `snapshot_changed`: the snapshot was
  rewritten under the open page (every run and `explore` rewrite it), and
  pages of two files must never answer (spec section 4).
- Stale runs: Store Log `run` entries since the snapshot's watermark minus the
  10-minute overlap (floor clamped to the local clock), less those the snapshot
  holds. For each, the RunCompletion and its RunManifest are fetched; a run's
  collections and items only when it is opened or a query needs them.

The DOM the Gate reads, and nothing else it may rely on:

| Selector | Meaning |
|---|---|
| `body[data-state]` | `loading`, `ready` or `error`, for the current render |
| `body[data-render]` | a counter, incremented when a render finishes |
| `body[data-route]` | the current route's name |
| `#snapshot-mode[data-mode]` | `range` or `whole` |
| `#stale[data-stale-count]` | runs newer than the snapshot; `#stale [data-command]` once past the notice thresholds |
| `#stale[data-log]` | `read` when a Store Log listing answered in full, `unreadable` when none did (or an S3 listing failed part way): the tail is then unknown, and `data-stale-count` counts only the stale runs actually found |
| `[data-run]` | one run: `data-run` completion cid, `data-pipeline`, `data-status`, `data-source` (`snapshot` or `tail`) |
| `[data-collection]` | one output of a run: `data-collection` cid, `data-output` |
| `[data-page]` | on the pipeline and collection views, when the list is not empty: `data-first` and `data-last` (1-based, inclusive; `data-first` is 0 on a page past the end) and `data-total`; `[data-page-next]` and `[data-page-prev]` link to the pages either side |
| `[data-producer]` | one query 1 row: `data-content`, `data-item`, `data-collection`, `data-completion`, `data-filename` |
| `[data-latest]` | query 2's answer, a completion cid or empty |
| `[data-item-result]` | one query 3 item cid |
| `[data-error]` | an error: `data-error` code, `data-cid` when a block is to blame |
| `window.__nfBlocks.verified` | every cid whose bytes the page hashed and accepted |

Error codes: `no_snapshot`, `no_range_over_cap`, `cors_headers`,
`fetch_failed`, `query_failed`, `worker_failed`, `snapshot_changed`, `hash_mismatch`,
`schema_invalid`, `block_missing`, `not_found`, `bad_route`, `bad_predicate`,
`file_protocol` (the store resolved to a `file://` URL, which a browser will
not fetch from: the page must be served, by any static server or `explore`).

The query views (query 1, 2 and 3) read the snapshot only through their own
statement, so Gate assertion 2's counts are the query's cost.

### Decisions made where the spec is silent (2026-09-25)

1. Milestone 1 selects snapshot rows by `run.member` (above). `run.member` is
   last-writer-wins: a RunCompletion present in two members keeps only the
   member it was last ingested from (`Index.insertRun`, `INSERT OR REPLACE`),
   so it can drop out of the other member's snapshot and, once that
   snapshot's watermark passes it, out of that member's tail too. Milestone
   2's `log_entry.member` fixes it.
2. `explore` rewrites the snapshot at start as well as at exit, so opening a
   member through it clears the stale notice (spec section 5.4).
3. The tail fetches a stale run's RunManifest with its RunCompletion, for the
   pipeline name.
4. Query 3 is per run; matching across runs waits for milestone 2's
   `collection_item(item_cid)` index.
5. Run-list anomalies come from each visible run's RunCompletion, fetched lazily.
6. The page is one self-contained `index.html`.
7. The whole-file cap is 64 MiB, `?cap=` per load.
8. The launch token (decision 12 of the milestone 2 plan) guards `POST`;
   `GET`/`HEAD` stay token-free behind the `Host`/`Origin` check.
9. S3 members are read-only and explore-only.
10. Nothing is filtered by `delete` Claims until Claims exist (milestone 2).
