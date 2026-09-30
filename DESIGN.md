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
   `../.scratch/block-explorer/spec.md`.* *Widened 2026-09-27: the lifting
   also covers `nf-blocks:items`, a read-only verb that lists a run's items
   from the plugin's own index so a downstream workflow can use them (§15,
   §16 Milestone 3). `put` stays the only command-line write path.*
   *Amended 2026-09-28 (ticket 07):* a verb exists only for an operation the
   spec names that a run cannot perform itself, and each lands in the
   milestone that also adds the Gate assertion exercising it. Milestone 4
   adds none; `sweep`, `prune`, `untrash`, `bundle`, `merge`, `verify` and
   `project` wait for their milestones.
6. The Gate's assertions never trust the plugin: they hash bytes themselves.

## 1. Plugin identity and layout

- Plugin id `nf-blocks`, entry class `robsyme.cas.CasPlugin`. Scheme `cas`.
  Config scope `cas`.
- Gradle: `io.nextflow.nextflow-plugin` `1.0.0-beta.15`, `nextflowVersion = '26.04.6'`.
  `extensionPoints` lists every extension class (that list is what generates
  `META-INF/extensions.idx`; `@Extension` alone registers nothing).
  `requirePlugins = ['nf-amazon@>=3.9.2']`; the AWS SDK is `compileOnly`
  through `io.nextflow:nf-amazon:3.9.2`, so the zip carries none and nf-blocks
  links against the classes nf-amazon loads (ticket 02). Nextflow 26.04.6
  does not download a required plugin for a plugin that is already unpacked
  in `NXF_PLUGINS_DIR` (a local install, the Gate), so nf-amazon must be
  installed beside it there (`nextflow plugin install nf-amazon@3.9.2`);
  `gate/gate.sh` does so itself.
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
  - `robsyme.cas.s3`: the one S3 seam (`S3Ops`, `SdkS3Ops`, `S3Access`) and
    the S3 member (`S3BlockStore`, `S3StoreLogStorage`, `S3CoordinateTree`,
    `S3SnapshotStorage`). The only package that imports
    `software.amazon.awssdk.*` or `nextflow.cloud.aws.*`.
- The page is an npm project under `web/`, built by Gradle into the plugin jar as
  `robsyme/cas/explorer/index.html` (block explorer spec section 2). Its output is not committed.
- Tests: Spock, under `src/test/groovy`, same packages. Groovy `@CompileStatic`
  on all main classes.
- What is built next, and in what order: `../.scratch/post-gate/roadmap.md`
  (2026-09-29). Milestone 4 (cloud) is this contract's §17; milestones 5 to 8
  (nf-core pipelines in the store, retention, input-side lineage, portability)
  are decided there, each gap linked to its ticket; projections are parked.

## 2. Configuration

```groovy
plugins { id 'nf-blocks' }

lineage.enabled = true
lineage.store.location = 'cas://lab'   // alias of the writable member
outputDir = 'cas://lab'                // optional: unset means the writable member's alias

cas {
    stores {
        lab { location = 's3://bucket/cas' }      // writable member: a local directory or s3://<bucket>[/<prefix>]
        // shared { location = '/mnt/bundle' }    // read-only member
    }
    resolve = ['lab']          // optional; default = every alias, writable first
    asserted_by = 'anonymous'  // optional opaque label; default 'anonymous'. Never defaults to the OS user name.
    index { path = null }      // optional override of the SQLite cache path (the Gate sets XDG_CACHE_HOME instead)
    snapshot { maxBytes = 64.MB }   // optional; a run writes the Index Snapshot only while it is under this (§15)
    tmpDir = null              // optional; scratch for S3 uploads of unknown length, default java.io.tmpdir
    nodeHash = null            // optional; node-side hashing, default fusion.enabled
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
- The writable member is the alias in `lineage.store.location`. *Amended
  2026-09-27:* `outputDir` unset means the writable member's alias; set to
  anything else, the run aborts (checked at run start, in
  `CasObserver.onFlowCreate`). `CasObserverFactory.create(session)` sets
  `session.outputDir = FileHelper.toCanonicalPath('cas://<alias>')` (as
  `Session.groovy:418` does) when lineage is enabled,
  `lineage.store.location` is a bare `cas://<alias>`, and the config has no
  `outputDir`, and logs at info `outputDir not set; publishing to
  cas://<alias>` through `nextflow.cas`, so it is on the terminal too
  (§16, the slf4j paragraph). It runs before `Session.groovy:472` copies
  `session.outputDir` into `WorkflowMetadata`, so `workflow.outputDir`, the
  lineage WorkflowRun record and Platform payloads agree. The RunManifest
  records the config as written because it records
  `session.resolvedConfig`, the text Nextflow rendered from the config files
  before any observer ran, which never sees this default (`session.config`
  is not changed either). An
  `outputDir` from `-output-dir`, or `cas://<alias>/sub`, is explicit: the
  factory leaves it to `onFlowCreate`'s check.
- *Amended 2026-09-28 (milestone 4, ticket 02):* any member, the writable
  one included, is a local directory or `s3://<bucket>[/<prefix>]` (a bucket
  root allowed; a trailing slash dropped, so `s3://bkt/p/` is `s3://bkt/p`).
  Another `<scheme>://`, or a malformed S3 URI, is refused naming the store.
  Local locations go through `FileHelper.asPath`. `cas.resolve` may name S3
  members; its default is every configured alias, writable first. The S3
  client is nf-amazon's, built from the `aws` scope (`S3Access`): credentials,
  profile and SSO, region (us-east-1 when none is set), endpoint, HTTP
  settings; `aws.client.storageClass`, `storageEncryption`,
  `storageKmsKeyId` and `requesterPays` go on each request. With an S3
  writable member, `GLACIER` and `DEEP_ARCHIVE` are refused (blocks must stay
  readable, and archive tiers need packing); `STANDARD_IA`, `ONEZONE_IA` and
  `INTELLIGENT_TIERING` warn once that each block pays the per-object
  minimum; any class nf-amazon does not accept (`GLACIER_IR`) warns that it is
  ignored and blocks are written as `STANDARD`. The storage class is read
  from `aws.client.storageClass`, else `uploadStorageClass`, and judged only
  when the writable member is on S3.
- `cas.tmpDir` holds a stream of unknown length on its way to an S3 member
  (`putStreaming` spools while hashing, rule 2): an S3 member needs scratch
  disk the size of the largest such output. That is an object over 5 GiB
  published from S3 without a node digest, and also any S3-sourced output,
  at any size, whose server-side copy failed and fell back to the head-node
  read (§8); under the optional hardening bucket policy of §5 every
  S3-to-S3 copy fails, so every S3-sourced output spools. The Index
  Snapshot is built there too before it is uploaded (§15).
- `cas.nodeHash` (Boolean; default `fusion.enabled`) turns on node-side
  hashing (§11): the `afterScript` default and the `.command.cas` read. Any
  value other than `true` or `false` is refused.
- `cas.snapshot.maxBytes` (a number of bytes, a `MemoryUnit`, or a string such as
  `'64 MB'`; default 64 MiB): the cap under which a run rewrites its member's
  Index Snapshot at `onFlowComplete` (§15).

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

*Amended 2026-09-28 (milestone 4):* `S3BlockStore(ops, prefix, alias,
writable, tmpDir)` keeps the local layout byte for byte under the member
prefix: `blocks/<xx>/<cid>`, `log/`, `coords/`, `nf/`, `index/v3.sqlite`,
`index.html`, plus `tmp/` (staging keys of the `s3-copy` provider, §8). A
block of 1 MiB or more is looked for with `HeadObject` first (S3 reads a
whole body before answering a conditional PUT's 412, ticket 14), and
`put(cid, in, size)` makes that HEAD before it reads the caller's stream, so
a known large block is never read at all; every write carries
`If-None-Match: *` and a 412 is success; a write that meets a 409
`ConditionalRequestConflict` is tried up to 3 times in all, a multipart
upload restarting whole (a Store Log entry too, after which the run warns and
continues). The storage class, SSE and requester-pays fields are set by
`SdkS3Ops` on every write; `aws.client.s3Acl` is not applied. Up to 5 GiB a
block is one `PutObject` with `ChecksumAlgorithm SHA256`, and the
`ChecksumSHA256` S3 returns must equal the CID digest (a mismatch, a source
file changed between hash and upload, deletes the object and fails the
write; when the delete is refused, as under the hardening policy below, the
publish aborts naming the key to remove by hand, as a mismatched copy does,
§8); above, a multipart upload of `max(64 MiB, ceil(size/10000))` parts,
each a file-channel range through `RequestBody.fromContentProvider` with its
own SHA-256, completed with `If-None-Match: *` and aborted on any failure.
The whole-object SHA-256 S3 returns for a multipart upload is composite
(`<base64>-<parts>`) and is not compared. `put` and `putStreaming` spool
through `cas.tmpDir` while hashing (rule 2); `putFile` for a file on the
default filesystem hashes it in place and uploads from it; `putDagCbor`
uploads its encoded bytes. Blocks carry
`Cache-Control: public, max-age=31536000, immutable`. Immutability on S3 is
the conditional writes; the documented hardening is a bucket policy denying
`PutObject` on `blocks/*` without `s3:if-none-match` and `DeleteObject` on
`blocks/*` except to a sweep role, and a lifecycle rule expiring `tmp/` after
a day and aborting incomplete multipart uploads after a day. The deny needs
`s3:ObjectCreationOperation` so `UploadPart` and `UploadPartCopy`, which take
no conditional header, still pass (README). AWS documents that a bucket
enforcing conditional writes refuses `CopyObject` into the enforced prefix
(403 without the header, 501 with it); under this policy every `s3-copy`
into `blocks/` would then fail and fall back to the head-node read (§8).
Not measured; tier two runs without the policy. The Store Log is
empty objects under `log/`, written `If-None-Match: *`; `ListObjectsV2` is
lexicographic, so newest first holds. `StoreLog` finds a member's log through
`LoggedStore`.

`CoordinateTree` is an interface (`LocalCoordinateTree`, `S3CoordinateTree`),
held to one contract (`CoordinateTreeContract`). On S3 a pointer is the
object `coords/<rel>` with the local body and there are no directory
markers; before a write the tree HEADs each ancestor (a pointer at `a`
refuses `a/b`, `FileAlreadyExistsException`) and lists `coords/<rel>/` with
max-keys 1 (a non-empty `a/` refuses a pointer at `a`,
`DirectoryNotEmptyException`), the local outcomes. Last write wins on one
coordinate. A pointer that two writers raced under an ancestor pointer is
shadowed: `read`, `exists`, `isDirectory` and `children` treat it as absent,
and `explore` lists up to 20 shadowed pointers on stderr. No locking
anywhere.

Snapshot and page storage is `SnapshotStorage` (`LocalSnapshotStorage`,
`S3SnapshotStorage`), §15.

## 6. Block kinds

Every metadata block is a DAG-CBOR map with `kind` (string) and `schema`
(integer: `1`, except DirectoryManifest, OutputItem and RunCompletion, `2` since 2026-09-28). Keys are `snake_case`. Links are `Cid` values (tag 42).
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
  address nullable &Any         # raw cid for regular, &DirectoryManifest for directory
  target nullable String        # relative in-tree target, or "[redacted-location]"
}
# `executable` appears only in a DirectoryManifest at schema 1, written before
# 2026-09-28, and reads as regular; schema 2 never holds it.
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
  reason nullable LeafReason
}
# The Leaf of an OutputItem at schema 1, written before 2026-09-28. Readers
# ignore its provider; the run records providers in its RunCompletion.
type LeafV1 struct {
  kind String
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
  index optional OutputIndex     # 2026-09-29 (ticket 26): the Output Index File of an output with index {}
}

type OutputIndex struct {
  leaf Leaf
  path String
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
  config RunConfig
  script nullable &Any
  started_at String
}
# The resolved config as text; a RunManifest written before 2026-09-27 holds
# the scrubbed config map instead.
type RunConfig union {
  | String string
  | ConfigMap map
} representation kinded
type ConfigMap {String:Any}

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
  providers optional {String:[&Any]}  # schema 2: provider name -> every Leaf address of the run it supplied, sorted; absent at schema 1
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
  unjoined optional Int  # 2026-09-29 (ticket 19): publishes that joined no item; absent reads as 0
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
{ kind: "DirectoryManifest", schema: 2,
  entries: [ { name: string,                       // raw file name, one path segment
               mode: "regular"|"symlink"|"directory"|"unresolvable",
               size: int,                          // bytes; for symlink/unresolvable, the byte length of target; for directory, 0
               address: Cid|null,                  // raw cid for regular, dag-cbor cid for directory, null for symlink/unresolvable
               target: string|null } ] }           // link target text for symlink/unresolvable, else null
```
Entries sorted ascending by the UTF-8 bytes of `name`. Rules for a symlink
found while walking: if its target is relative and resolves inside the tree
being published, record `mode: "symlink"` with `target` (the relative target
string, which is portable and meaningful to a receiver); otherwise follow it
and store what it points at as regular/directory; if it dangles or
cycles, `mode: "unresolvable"`. **A stored `target` is only ever a relative,
in-tree path.** An absolute target, or one that escapes the tree, is never
written into a block: the manifest carries no `asserted_by` and travels in a
Bundle, so a host path there both violates the "nothing store-local" rule and
makes the same tree hash differently under different launch prefixes. For such
an entry `target` is `"[redacted-location]"` (the same marker `scrub` uses), so
the fact of the broken link survives without its machine-local path. Cycle
detection and depth limit 64. Empty directory = `entries: []`.

*Amended 2026-09-28 (ticket 15 addendum, Rob):* a manifest records no execute
bit. An object store keeps none, so the bit let the storage backend change a
manifest address. A reader accepts schema 1 and reads its `executable` entries
as `regular`; materialising a manifest sets no execute permission.

### OutputItem
```
{ kind: "OutputItem", schema: 2,
  value: <the channel item structure> }
```
`value` mirrors the published channel item: a `Map` stays a map, a
tuple/list stays a list, scalars keep their types. Every file or directory
leaf is replaced by a **Leaf** map:
```
{ kind: "Leaf", name: string|null, address: Cid|null, size: int|null,
  reason: null|"declined"|"never_published"|"unresolvable"|"unaddressed" }
```
`name` is the file name it was published under (last segment of the publish
path). `reason` is null when `address` is set and non-null otherwise; an absent
address is never an absent field. A `declined` leaf is what Nextflow hands us
as `null` in place of a path. A `never_published` leaf is a path in the item
that never received a publish event (a path outside the work dir).
Decoding rule: a map with `kind == "Leaf"` is a leaf. The item carries no run
reference and no publish path.

*Amended 2026-09-28 (ticket 16):* the Leaf carries no provider, so the same
content published by different Address Providers is one OutputItem. An
OutputItem is written at `schema: 2`; a reader accepts `schema: 1`, whose
Leaves carry `provider`, and ignores it. The RunCompletion records providers.

### OutputCollection
```
{ kind: "OutputCollection", schema: 1, asserted_by: string,
  run: Cid,                      // RunManifest
  name: string,                  // output name from the workflow output DSL
  items: [Cid|null, ...],        // OutputItem links sorted ascending by cid string; null only when Nextflow handed us a null item
  paths: [[string, ...], ...],   // paths[i] = publish paths (relative to outputDir, '/'-joined) of item i's leaves in depth-first order; null for a leaf with no path
  index: { leaf: Leaf, path: string }|absent }  // 2026-09-29 (ticket 26): the Output Index File of an output with `index {}`; absent when the output declares none
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
  params: Map,                  // scrubbed, see below
  config: string,               // the resolved config as text, scrubbed; see below (a Map before 2026-09-27)
  script: Cid|null,             // raw block of the main script
  started_at: string }
```

**Portability scrub for `params` and `config`** (`Records.scrub(Object)` for
`params`, `Records.scrubText(String)` for `config`, both unit-tested).
`scrub`: drop the top-level config scopes
`cas`, `lineage`, `workDir`, `outputDir`, `launchDir`, `projectDir`, `homeDir`,
`configFiles`, `scriptFile`, `commandLine`, `runName` and `resume`; convert
`Path` values to strings, and any other value dag-cbor cannot encode (a
`MemoryUnit`, a `Duration`, a closure) to its `toString()`; then replace every
string value that starts with `/`
or with a `<scheme>://` other than `lid://`/`cas://` by `"[redacted-location]"`,
and every string equal to the OS user name by `"[redacted-user]"`. Measured at
v26.04.6: `session.config` contains `cas.stores.<alias>.location` (an absolute
host path), `outputDir` (a member alias) and `workDir`, so an unscrubbed copy
breaks §6's rule and Gate assertion 10.

*Amended 2026-09-27:* `config` is text. A config map could not be recorded
whenever it held a value dag-cbor has no type for, and strict syntax allows
both kinds nf-core uses: closures as dynamic process directives
(`ext.args = { ... }`, `VariableScopeVisitor.java:170-180` at v26.04.6) and
unit literals (`memory = 8.GB`). Every such run aborted writing its
RunManifest. The text is `session.resolvedConfig`, which `CmdRun` builds
whenever `lineage.enabled` is set (`CmdRun.groovy:419-422`): canonical config
with closures as their source, the text Platform receives as `configText`
(`ConfigBuilder.resolveConfig`, `ConfigBuilder.groovy:897-915`). Nextflow
masks the values of keys matching `SecretHelper.SECRET_KEYS`
(`^AWS.+|.*TOKEN.*|.*PASSWORD.*|.*SECRET.*|.*accessKey.*`) in it. The
fallback, when it is null, is `ConfigHelper.toCanonicalString(session.config)`,
which masks nothing. *Amended 2026-09-27 (final review):* either text then
goes through `Records.scrubConfigText`, which redacts exactly this:

- the value of any assignment or map entry (`key = value`, `key: value`)
  whose key contains, case-insensitively, `key`, `secret`, `token`,
  `password`, `passwd` or `credential`, or has a segment (split at `.`, `_`,
  `-` and camelCase) that is exactly `pat`, becomes `'[secret]'`. This
  covers `env { FOO_API_KEY = '...' }`, `GITHUB_PAT`, `azure.storage.accountKey`
  and `azure.batch.accountKey`, which Nextflow's pattern misses; `path` and
  `pattern` are not secrets. The value is the quoted string or the bare word
  after the separator, so a secret written across several lines keeps its
  later lines.
- then `scrubText`, token by token (a token is a run of non-whitespace, with
  leading quotes and brackets and trailing punctuation set aside): a token
  that is an absolute path or a URI in a scheme other than `lid://` or
  `cas://` becomes `[redacted-location]`, and one equal to the OS user name
  becomes `[redacted-user]`. Otherwise the token is split at its first `=`
  or `:` and each side judged the same way, so `--volume=/home/x:/data`,
  `TMPDIR=/scratch/x` and `--account=<user>` keep their shape with the path
  or name redacted. A path followed by `:` (a mount, a path list) has each
  path redacted.

So `workDir = '/x'` keeps its line with the path redacted, and the dropped
scopes stay in the text, with their paths redacted. Not redacted: a host
name, a user name other than the OS user's, a relative path, and a secret
under a key that names none of the words above. Both passes are idempotent.
Nothing reads `config` by machine: it is provenance for people.

### RunCompletion
```
{ kind: "RunCompletion", schema: 2, asserted_by: string,
  run: Cid,                             // RunManifest
  collections: [Cid, ...],              // OutputCollection links, sorted by output name
  input_set: Cid|null,                  // null in the skeleton
  status: "succeeded"|"failed",
  exit_status: int|null,
  possibly_incomplete: bool,            // true for every failed run (no barrier exists on that path)
  started_at: string, finished_at: string,
  anomalies: { unresolvable: int, unaddressed: int, declined: int, never_published: int, unjoined: int },  // unjoined added 2026-09-29 (ticket 19); absent on an older block reads as 0
  error: string|null,
  providers: { <provider>: [Cid, ...] } }   // schema 2: every Leaf address the run published (each file leaf and each directory leaf's manifest) under the provider that supplied it; not the files inside a directory (final review I5)
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
  - *Amended 2026-09-28 (final review I1):* **Item Leaf**
    `cas://<OutputItem cid>/<leaf name>[/<entry>...]`, one leaf of an item by
    its name, with no collection: a raw leaf is that file, a directory leaf
    that directory, and further segments traverse its manifest. A leaf name
    two leaves of the item share is refused, as for an occurrence. With no
    segment it is an error asking for a leaf name. `fromStore` emits a
    directory leaf this way (§13), because a bare `cas://<manifest>` has no
    segment to be staged under.
  - A Store URI with no segments has its CID as its file name
    (`CasPath.getFileName()`), so Nextflow stages a bare `cas://<cid>` under
    `<cid>`. With no file name, FilePorter stages into its cache directory
    itself and retries its integrity check without end (Task 14). A
    coordinate root still has no file name.
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
  `boolean isDirectoryCoordinate(String relPath)`. *Amended 2026-09-28:*
  `CoordinateTree` is now an interface; the class above is
  `LocalCoordinateTree`, and `S3CoordinateTree` holds an S3 member's
  coordinates (§5). `CasSession.coordinatesOf(alias)` gives any configured
  member's tree, one outside `cas.resolve` included, built on first use.
- `StoreRef(Cid cid, String name)` ⇄ `cas://<cid>/<name>`.
- `Cid` parsing of a `lid://…` is never attempted here; `lid://` is Nextflow's.

## 8. The provider (`robsyme.cas.nio`)

`CasPathFactory extends FileSystemPathFactory`, listed in `extensionPoints`:
- `parseUri(String)`: returns a `CasPath` for `cas://…`, else null.
- `toUriString(Path)`: `cas://…` for a `CasPath`, else null.
- `getBashLib`/`getUploadCmd`: return null. No task script ever touches the
  scheme: tasks unstage to the work dir as usual and the head node publishes
  from there (measured on Batch, ticket 05).

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
*Amended 2026-09-28 (final review I3):* only the writable member's
coordinates are written. `upload`, `newOutputStream`, `createDirectory`,
`delete` and `deleteIfExists` on `cas://<alias>/...` for any other member
throw `AccessDeniedException` naming both aliases, before anything is read or
written: a pointer written there would name blocks only this run's writable
member holds.
- `createDirectory`: create the coordinate directory under `coords/`.
- `newOutputStream` on a coordinate (*amended 2026-09-30, Task 6a; narrowed
  2026-09-30 by the milestone 5 final review*): Nextflow writes a CSV Output
  Index File as a delete and then one append per piece (`CsvWriter.apply`,
  v26.04.6), so an `APPEND` close is not the end of the file. Every stream
  writes to a spool file under `cas.tmpDir`. A stream opened without
  `APPEND` has a spool of its own and is stored when it closes, as before
  Task 6a: one block, one Pointer File write and one `recordPublish`
  (`head-node`, the byte count); a pending spool for the key is dropped
  first. A stream opened with `APPEND` continues the key's pending spool
  (`CasSession` holds it, keyed by the join key), or else a new one seeded
  by streaming the coordinate's current block (empty when it names no
  file; a directory is refused), and its `close()` hashes nothing. A
  pending spool is finalised exactly once, into one block, one Pointer File
  write and one `recordPublish`, then deleted: when the provider next
  resolves the coordinate or a directory above it (a read, attributes,
  access, a listing, an `upload`), when `onFilePublish` names it, and for
  every spool left at the join (`finalizeAllPending`, before `Join.join`).
  `finalizeAllPending` also seals the session: from then on an `APPEND`
  close is stored at close too, since nothing sweeps again and observers
  that run after nf-blocks (nf-prov) still write into `outputDir`. A later
  `APPEND` seeds a new spool from the published content.
  `delete`/`deleteIfExists` drop a pending spool. A spool whose stream is
  still open at the join (a publish thread racing a failed run's
  completion) is left out of the `RunCompletion` with a console warning
  naming the coordinate, and stays pending, so its close stores it; the rest
  of the run is recorded. A failure to hash or write a closed spool from the
  observer aborts the run (rule 3). Spool operations for one key are
  serialised on a per-key lock.
- `delete`/`deleteIfExists` on a coordinate: remove the Pointer File only.
  Never touches a block.
- `canUpload(source, target)`: `target instanceof CasPath && target.isCoordinate()`.
- `upload(source, target, options)`:
  1. `key = Coordinates.key(target)`. If the coordinate exists and
     `REPLACE_EXISTING` is absent, throw `FileAlreadyExistsException(key)`
     **before reading any byte** (this is what makes `-resume` cheap).
  2. Regular file: the address comes through `PublishAddresser` (the Address
     Provider seam, spec §3), then the Pointer File is written, in this order:
     the task node's digest in `<task dir>/.command.cas` when `cas.nodeHash`
     is on (task dir = the first two path segments under `workDir`, both
     compared as real paths; the file is read once per task directory per
     run, ignored with a warning above 16 MiB; lines that do not parse are
     skipped); for an S3 source into an S3 member, `S3BlockStore.copyFrom`
     (below); else the head node streams the file with the 1 MiB buffer (a
     default-filesystem file into an S3 member is hashed in place and
     uploaded from it; any other source spools through `cas.tmpDir`). A node
     digest and a computed address that differ abort the run, whether the
     computed one is the head node's or S3's SHA-256 of a copy; the copy is
     deleted first. Under the optional hardening bucket policy, which denies
     `DeleteObject` on `blocks/`, that delete is refused, and the mismatch
     becomes an abort whose message names the key to remove by hand. A copy
     that fails for any other reason (an SDK refusal, no full-object SHA-256
     in the answer) warns and falls back to the head-node read. A staging
     copy under `tmp/` that cannot be deleted is left to the `tmp/` lifecycle
     rule. A full `cas.tmpDir` aborts naming it and the bytes needed. Record
     `(key -> StoreRef, size, provider)` in `CasSession.publishes`:
     `fusion-node` when the node digest named a block the writable member
     already held or drove an `UploadPartCopy`, `s3-copy` when S3 returned
     the SHA-256 of a copy, `head-node` when the head node read the bytes.
  3. Directory: walk it yourself (Nextflow does not recurse), address every
     file through the same addresser, build the `DirectoryManifest`
     recursively, put it, write the Pointer File pointing at the manifest
     cid. Never return normally with any child untransferred. Record the
     manifest and the anomaly counts. The manifest itself is encoded on the
     head node and recorded `head-node`. The provider of each file inside is
     counted by the addresser for the run's summary line (silent decision 8)
     and not recorded in a block: `RunCompletion.providers` lists Leaf
     addresses only, so a directory of millions of files cannot push the
     RunCompletion past what the index reads (final review I5, reverting
     pre-flight F30). `toRealPath` is used only where the
     provider has it (an object store has no links to resolve, ticket 05).
     From an object store, a directory holding a `.fusion.symlinks` object
     has each listed name decoded as a link whose target is its object's body
     (ticket 15; rules in §6), the sidecar left out; every file is `regular`.
- `canDownload(source, target)`: `source instanceof CasPath`.
- `download(source, target, options)`: raw → copy the block to `target`
  (symlink when both are on the same local filesystem and the block is
  read-only; otherwise stream and hash in flight, aborting on mismatch);
  manifest → materialise recursively, recreating `symlink` entries as
  relative symlinks, failing loudly on `unresolvable`. Absent block →
  `NoSuchFileException` naming the cid.

`S3BlockStore.copyFrom(sourceBucket, sourceKey, size, expected)` (ticket 16):
with a node digest, `HEAD` the final key first (present: `fusion-node`, at
any size). Up to 5 GiB, with a node digest, `CopyObject` straight to the
final key with `If-None-Match: *` and SHA-256 (a 412 is `fusion-node`),
compared with the digest (mismatch: delete, abort); without one, copy to
`tmp/<uuid>` with SHA-256, `HEAD` the final key, copy staging to final with
`If-None-Match: *` (412 is success), delete staging. Above 5 GiB, with a node
digest, `UploadPartCopy` to the final key, the node digest trusted as the
address (it is asserted); without one, null, and the head node reads. Copies
meeting a 409 are tried up to 3 times. The head node's byte count and each
provider's count are logged at info at `onFlowComplete` (`nf-blocks: the
head node read <n> bytes to address <m> file(s); head-node <a>, fusion-node
<b>, s3-copy <c>`).

*Amended 2026-09-28 (milestone 4):* `download` into an S3 target of a block
an S3 member holds, up to 5 GiB, is a `CopyObject` with SHA-256 from the
member's key, checked against the CID (a mismatch, or an answer with no
full-object SHA-256, deletes the target and aborts); a copy the SDK
refuses falls back to streaming. A local member
staging into S3 streams through nf-amazon's output stream as before. A
`symlink` manifest entry staged onto any non-default filesystem becomes a
copy of what it names inside the tree (ticket 15 decision 7), the target
resolved segment by segment as POSIX does, so a `..` after a directory link
climbs from where the link landed (`dirlink -> sub/deep`, `l -> dirlink/../f`
gives `sub/f`; final review I7); a link that names a directory already being
materialised (`up -> ..`) aborts rather than recursing. Nothing is made executable: manifests carry no execute bit.

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
keyed by join key (`Publish(StoreRef ref, long size, String provider)`;
*amended 2026-09-28, final review I5:* the `contents` field of pre-flight F30
is gone), a directory's anomalies keyed the same way, the run's `PublishAddresser`, the Nextflow run key once `save(<hash>, WorkflowRun)`
is seen, the RunManifest cid once written, the captured `WorkflowOutputEvent`s,
and a one-shot latch for `onFlowComplete`. *Amended 2026-09-28:* also each
member's `CoordinateTree` and `SnapshotStorage` (local or S3, built beside
its block store; `coordinatesOf(alias)`, `snapshotsOf(alias)`), and the
writable S3 member's clock check (`checkClock`, §11). The `PublishAddresser`
is built on first publish over the composite and the writable member, with
`cas.nodeHash` and the session's `workDir`.

## 10. `CasLinStore` (`robsyme.cas.lineage`)

- `open(config)`: resolve the alias from `lineage.store.location`, build the
  composite store from the `cas` scope (`Global.session.config`), and open a
  delegated `nextflow.lineage.DefaultLinStore` at `<writable>/nf` for
  Nextflow's own records. Abort with `AbortOperationException` if the writable
  member cannot be created.
  *Amended 2026-09-28 (ticket 02 decision 7):* `DefaultLinStore` is opened on
  the location string `FilesEx.toUriString` gives, so an S3 writable member's
  records are at `s3://<bucket>/<prefix>/nf` through nf-amazon's filesystem
  (no directory is made first; S3 has none). `nextflow lineage find` then
  costs one GET per record. A read-only member joins the reader chain only
  when both `nf/` and `nf/.history` already exist, because
  `DefaultLinStore.open` creates its location and `DefaultLinHistoryLog`
  creates `.history` when missing (`DefaultLinHistoryLog.groovy:37-40` at
  v26.04.6), and a read-only member is never written; an S3 member that
  cannot be looked at is skipped with a warning.
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
  alias equals the lineage alias), open the index lazily. Then, for an S3
  writable member, one `HEAD` on its snapshot key and a comparison of the
  local clock with the response's `Date`: above 1 minute a warning on the
  terminal, above 5 minutes `ClockSkewException` (an `AbortRunException`)
  naming the skew and NTP (ticket 03 decision 2). A `HEAD` that fails warns
  and the run continues. `put` and `explore` check the same at start and
  exit 1 above 5 minutes.
- `onFlowBegin()`: write the `RunManifest` (the Nextflow run key is known
  because every observer's `onFlowCreate`, including `LinObserver`'s, has run).
- `onFilePublish(event)`: nothing to hash (our `upload()` already did). Look
  up `Coordinates.key(event.target)` in `publishes`; if absent, resolve
  through the Pointer File; if still absent, `AbortRunException`. Attach
  labels. *Amended 2026-09-29 (milestone 5, ticket 19):* every `cas://` key
  an event names is also recorded in `publishedKeys`, whether or not a
  workflow output ever joins it. `onFlowComplete` subtracts `Join`'s
  `joinedKeys` from `publishedKeys` and every key in `publishes` to count
  `unjoined` (below).
- `onWorkflowOutput(event)`: store the event (name, value) for the join. If
  `value` is null because an `index {}` block exists, record the anomaly.
  *Amended 2026-09-29 (milestone 5, ticket 26):* that premise is wrong. At
  26.04.6, `PublishOp.onComplete` (`PublishOp.groovy:219-229`) sends the
  published value and the index path together, so `index {}` never nulls
  the value; a null value is instead a value channel that emitted nothing,
  and `Join` builds an empty collection for it, not an `unaddressed`
  anomaly. This bullet also captures `event.index`, the Output Index
  File's `cas://` coordinate, when the output declares one; `Join` links
  it into the `OutputCollection` as an `index` Leaf (name, address, size)
  and its publish path, addressed only from a publish made in this run
  (`CasSession.publishFor`), never an existing Pointer File (§6).
- `onTaskCached(event)`: record the task hash so the task layer is not empty
  (skeleton: log only; address reuse by task hash is a later task).
- `onFlowComplete()`: one-shot. Build every `OutputItem` from the captured
  events (replace each `Path` leaf by a Leaf using `publishes`), each
  `OutputCollection` (items sorted by cid string, `paths` aligned), then the
  `RunCompletion` (status from `session.isSuccess()`, `exit_status` from
  `session.workflowMetadata.exitStatus`, `possibly_incomplete = !success`).
  Write the Store Log entry (`run`, stamped with the write time). Then update
  the index (§12) for this run; index failure logs and marks the index stale,
  never aborts. *Amended 2026-09-28:* the RunCompletion is written at
  schema 2 with `providers` (§6), and the addresser's byte and provider
  counts are logged at info (§8). The snapshot base is taken before the
  catch-up; the snapshot is not rewritten when the writable member's
  catch-up threw (`catch_up_failed`) or a guard of §15 holds.
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
  *Amended 2026-09-27:* when `session.error` is already set, `onFlowComplete`
  catches its own failures and logs them at warn instead of throwing, since
  an `AbortRunException` there skips `notifyError` (`Session.groovy:1125-1128`)
  for every observer, losing the user's `onError` and the hint below. A clean
  run keeps the abort.
  *Amended 2026-09-29 (milestone 5, ticket 19):* `RunCompletion.anomalies`
  gains `unjoined`: `publishedKeys` minus `Join`'s `joinedKeys` (every key
  an `onFilePublish` event named that no item or `index` Leaf claimed),
  counted and, when non-empty, warned once with the count, the first three
  coordinates and a pointer to the output DSL. The run's status is
  unchanged. *Amended 2026-09-30 (final review C1):* the count is
  `(publishedKeys ∪ publishes.keySet()) - joinedKeys`, so a file stored in
  `cas://` with no publish event (sarek's `collectFile(storeDir:)` into
  `csv/` and `pipeline_info/`, a stream written by a script or observer) is
  counted too; `publishedKeys` still covers a resumed publish known only by
  its Pointer File. The warning reads "N file(s) stored this run are in no
  workflow output".
- `onFlowError(event)` (2026-09-27): when `session.error`, or a cause of it,
  is a `MissingMethodException` whose method is `Channel.fromStore` (untyped),
  or `fromStore` on a receiver of type `nextflow.dataflow.ChannelNamespace` or
  `nextflow.script.types.Channel` (typed), log at warn the hint
  `FromStoreHint.of(error)` builds (§13); anything else, nothing. The hint
  goes to the logger `nextflow.cas` (`robsyme.cas.trace.ConsoleLog`), whose
  name Nextflow's console filter admits, so it is printed on the terminal
  just above the launcher's error, and written to `.nextflow.log`.
- `CasObserverFactory.create` (2026-09-28) also installs node-side hashing
  when `cas.nodeHash` (default `fusion.enabled`) is on:
  `NodeHash.install(session.config)` puts `node-hash.sh` ahead of every
  `afterScript` string in `process` and in each `withName:`/`withLabel:`
  selector (the process scope's default when none is set); a closure
  `afterScript` is left alone with a terminal warning naming the selector,
  and those tasks fall back to the head node. The script works in
  `${NXF_CHDIR:-$PWD}` and does nothing without `.command.run`; it reads
  `.command.run`'s `### outputs:` patterns, expands them (`nullglob`, and
  `globstar` where the shell has it; brace patterns do not expand), skips
  links and absent names, hashes files, and the files under directories,
  with `sha256sum` (or `shasum -a 256`) into `.command.cas` through
  `.command.cas.tmp`, and never fails the task. `afterScript` is not in the
  task hash (rule 5).
- `onProcessCreate(process)` (2026-09-28): `NodeHash.install` (above) only ever
  reaches a process through Nextflow's process-scope/selector config
  defaults, which `ProcessConfigBuilder.applyConfigDefaults` skips for any
  process whose own body sets `afterScript` directly; that process gets none
  of ours chained ahead, so it is not hashed on the node though
  `cas.nodeHash`/`fusion.enabled` says it should be. When node hashing is
  enabled, this warns once per process name, on the `nextflow.cas` logger,
  when the process's effective `afterScript` does not start with
  `NodeHash.script()`; that process's outputs are still addressed on the
  head node, same as any other run without node hashing.
  *Amended 2026-09-29 (milestone 5, ticket 19):* before that node-hash
  check, `onProcessCreate` warns once per process name whose config
  declares `publishDir` while `outputDir` is `cas://`, independent of
  `cas.nodeHash`: lineage comes from workflow outputs only, so that
  process's files are stored but referenced by no run's `RunCompletion`.
  The warning names the process and points at the output DSL.

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
- *Amended 2026-09-28 (ticket 04):* `catchUp(store, log, member, snapshots,
  tempDir)` seeds first when the cache has no `store_log_watermark:<m>`, the
  only trigger (ticket 04 decision 6): it fetches the member's snapshot, and
  when its `schema_version` matches copies each run, Selection and Claim it
  does not hold with their rows (`run.member` and `log_entry.member` set to
  the alias), recomputes `claim_current` for every seeded subject, adopts
  the snapshot's watermark, marks `block_scan:<m>` done and records
  `seeded_from:<m>` = its `snapshot_written_at`; then the tail is read with
  the usual overlap. A Store Log entry the snapshot's writer could not read
  (it held only `missing(NULL, cid)`, which a snapshot does not carry) is
  seeded as that `missing` row, so it is retried once its block arrives. No
  usable snapshot (absent, another `schema_version`, unreadable) means the
  full scan, with a warning on the `nextflow.cas` logger naming the member,
  the reason and `nf-blocks:snapshot` when the member's Store Log is not
  empty. `cas.index.path` on persistent disk (EFS, FSx; one file per head
  node, since WAL needs shared memory) skips seeding; SQLite on S3 is not
  supported. The cache file is named by the members' location texts, S3 URIs
  included.
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
08; *amended 2026-09-28, final review I1:* now the Item Leaf
`cas://<item>/<leaf name>` of §7, so a task stages it under its published
name, and `cas://<manifest>` only when another leaf of the item shares its
name, staged then under the manifest's CID); declined → `null`; an `unaddressed` leaf → error naming the item. A run
without a RunCompletion → error. Implemented with `@Factory`; resolve eagerly
so a bad run reference or an unaddressed item fails fast, but bind onto the
channel inside a `session.addIgniter` closure so a downstream subscriber is
attached first. **Measured at v26.04.6:** there is no `NF.dsl2` (DSL1 is gone)
and `CH.create()` is a non-buffering `DataflowBroadcast`, so eager binding
without the igniter would drop items.

`channel.fromStore(selection: <cid or cas://cid>)` (block explorer spec
section 10) emits every distinct item the Selection reaches through nesting
(`Index.selectionItems`), sorted by item CID, restored as above; a Selection
hidden by a current `delete` Claim emits with a warning; a nested Selection
the composition lacks fails the call, naming it. `run`, `output`, `where` and
`pipeline` are refused beside `selection`.

*Amended 2026-09-27 (milestone 3):*

- `records: true` (default `false`, in every script mode, beside `selection:`
  and beside `run:` with `output:`) restores every map that is not a Leaf, at
  any depth and inside lists, as `nextflow.util.RecordMap`; Leaves become
  paths as above and lists stay lists. Any value other than `true` or
  `false` is refused, naming `records`. A restored record is immutable:
  `item + [x: 1]` returns a new map, and `put` throws Nextflow's
  `UnsupportedOperationException`. `RecordMap` exists from Nextflow
  26.01.1-edge; the plugin requires 26.04.6.
- In a typed script (`nextflow.enable.types = true`) a plugin factory cannot
  be reached as `channel.fromStore` or `Channel.fromStore` in 26.04.6:
  `channel` is `nextflow.dataflow.ChannelNamespace`, which has a fixed
  factory list (nextflow-io/nextflow#7694). The spelling that works keeps the
  include and calls `nextflow.Channel.fromStore(..., records: true)`, which
  a record-typed process input accepts without TaskProcessor's `invalid
  argument type` warning. Gate tier B18 runs it.
- A run reference (`run:`) is resolved by `robsyme.cas.core.RunRef.resolve`,
  which `nf-blocks:items --run` shares (§15); its errors say "run reference".
- With none of `selection`, `run` or `output`, the call fails with
  "`fromStore` takes `selection: <address>`, or `run: <ref>` with `output:
  <name>`; optionally `where: [...]` and `records: true`."
- A run that fails because `fromStore` cannot be found ends with a warning
  from `CasObserver.onFlowError` (§11), chosen by the exception, not the
  script mode. Untyped (method `Channel.fromStore`): "`fromStore` comes from
  the nf-blocks plugin: add `include { fromStore } from 'plugin/nf-blocks'`
  at the top of the script." Typed (method `fromStore`, type
  `nextflow.dataflow.ChannelNamespace` or `nextflow.script.types.Channel`):
  "In a typed script, `channel.fromStore` can't be reached until
  nextflow-io/nextflow#7694 is fixed. Add the include and call
  `nextflow.Channel.fromStore(...)`, with `records: true` for record-typed
  inputs."

## 14. The Gate (`gate/`)

`gate/gate.sh` builds and installs the plugin into a throwaway
`NXF_PLUGINS_DIR`, installs `nf-amazon@3.9.2` beside it (§1: a fresh
`GATE_ROOT` otherwise fails before any pipeline runs), and runs the Test
Pipeline at `../.scratch/content-addressed-lineage/test-pipeline` five ways
(cold; again into the same store with `gate/node-hash.config`, so its files
are addressed from `.command.cas`; `--fail`; `-resume`; from a second launch
directory), then the consumer, then the consumer twice more on a deleted
cache (`consumer-seeded`, with the metadata blocks of the producer's runs at
or before the snapshot's watermark unreadable; `consumer-scan`, with the
snapshot moved aside too), with `XDG_CACHE_HOME` and the store under a fresh
temp directory, then runs `gate/assert.py` (Python 3 standard library only:
`hashlib`, `sqlite3`, `json`, plus a small DAG-CBOR decoder and CID encoder
of its own). Exit non-zero on any failed assertion. The Gate config overlay
lives at `gate/gate.config`. Lineage tier: 15 PASS, 0 FAIL, 6 SKIP.
Assertion 13, "a cold cache seeds from the Index Snapshot", takes its locked
runs from the Store Log entries at or before the snapshot's watermark, and
counts a permission failure when the seeded run's log says "could not be
read" or "could not be decoded as <Kind>" together with a locked path (the
plugin reports a locked RunCompletion as undecodable).

*Tier two* (`make gate-tier2`, `gate/tier2/`, on demand): the scidev Batch
queue, a throwaway S3 member, T1-T6 and T2b (`gate/tier2/README.md`); run
before a milestone that touches the S3 store, the Fusion provider or the
cloud publish path is accepted. It is the only part of the Gate that talks
to AWS, from a person's SSO session; its own unit tests
(`python3 -m unittest discover -s gate/tier2`) do not.

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
  `VACUUM INTO` a temp file. One rollback-journal file, no sidecars.
  *Amended 2026-09-28:* the build is local (`IndexSnapshot.build`, in
  `cas.tmpDir`); `SnapshotStorage.replace` puts it in place: locally a copy
  into `<member>/index/` and an atomic move, on S3 one `PutObject` with
  `Cache-Control: no-cache`, `x-amz-meta-runs: <run rows>` and `If-Match` on
  the ETag it replaces (`If-None-Match: *` when there was none); a 412, a 409
  `ConditionalRequestConflict` (not retried: another writer is replacing it,
  and the snapshot is derived), or a 404 (the snapshot deleted meanwhile)
  skips the rewrite (`replaced_meanwhile`).
- Writers: a run's `onFlowComplete`, `nf-blocks:snapshot` (any size),
  `nf-blocks:explore` (start and exit, any size). Each takes the base (`HEAD`,
  or a local stat) before its catch-up and writes only the writable member's
  snapshot, and none writes when the old snapshot is at or over
  `cas.snapshot.maxBytes` or the new one is over it (`over_cap`, the run
  only), when the new one has fewer `run` rows than the old (`fewer_runs`; an
  S3 snapshot without `x-amz-meta-runs` is downloaded once and counted), when
  another writer replaced it (`replaced_meanwhile`), or when the writable
  member's catch-up failed (`catch_up_failed`). Locally `replace` does not
  re-check the base; the move is atomic. Skips log at info; the verb prints
  them and exits 0, and `explore` prints them on stderr.
- `<member>/index.html` is the page from the plugin jar
  (`/robsyme/cas/explorer/index.html`), written beside the snapshot whenever a
  snapshot is written and its bytes differ; a page that cannot be written
  warns and the snapshot stands.

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
nextflow [-c <config>] plugin nf-blocks:put <file|/dev/stdin> [--dry-run] [--name <name>]
nextflow [-c <config>] plugin nf-blocks:items <output> [<path>=<value> ...] --run <ref>[,<ref>...]
                                              [--pipeline <id>] [--format csv|json|occurrences|selection]
```

`CmdPlugin` turns `--name value` into the argument pair `--name`, `value` after
the positional arguments. Exit 0 on success, 1 on a failure the verb reports, 2
on a usage error.

`put` prints the response or error body (DAG-JSON) on stdout, exit 0 or 1.
Nextflow 26.04.6's launcher refuses a bare `-` (`Unknown option: -`), so read
stdin as `/dev/stdin`; `-` works for in-process callers. A bare `--dry-run`
arrives as `--dry-run`, `true`.

`put --name <name>` (milestone 3) takes Selection requests only. Once the
Selection is written or found, a second request through the same `Put`
builder writes a `set name` Claim, timestamped by the verb's clock,
superseding the dry run's `name_claims`; nothing is written when the one
current name already equals `<name>`. Both responses are printed. When the
Claim fails the verb prints "saved, naming failed" and exits 1. With
`--dry-run` it prints the dry run and the Claim it would send. Renaming is a
re-put with a new `--name`; there is no `rename` verb.

`items` (milestone 3) is read-only. It lists the items of one output of one
or more runs from the plugin's own index, caught up first with
`cas.openIndex()` as `put` and `snapshot` do; it never reads an Index
Snapshot and never writes. `<ref>` is anything `fromStore(run:)` takes
(`latest` needs `--pipeline`), resolved by `RunRef`; several runs are
comma-joined, because `CmdPlugin` keeps one value per flag. A condition is a
positional `<path>=<value>` split at the first `=` (`a=b=c` is the path `a`
and the value `b=c`); it matches an `item_attr` row of that path whose value
text equals `<value>`, whatever its type, so `lane=2` finds the integer 2 and
the string "2" (`Index.itemHitsByText`). Every condition must hold. `=x` or
an argument without `=` is a usage error naming it, exit 2. Rows are sorted
by item CID, then collection CID. `--format csv` (the default) and `json` are
`Samplesheet.of(store, items, occurrences)` (§16) with a leading `occurrence`
column, `cas://<collection>/<item>`; a Meta Map key named `occurrence`
becomes `meta.occurrence`, and a file position named `occurrence` becomes
`file.occurrence` (the samplesheet's existing collision rule). `occurrences`
prints one occurrence per line. `selection` prints a complete `put` request
whose members are those occurrence strings, the union across the runs, so
the command-line route to a named Selection is `nextflow -q plugin
nf-blocks:items ... --format selection | nextflow -q plugin nf-blocks:put
/dev/stdin --name <name>`. Both launchers take `-q`: Nextflow's console log
writes to stdout, so a "Downloading plugin" line (the first use of a version)
or the `NXF_PLUGINS_TEST_REPOSITORY` banner would otherwise reach `put` ahead
of the request.

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
`Plugins.start(target)`, so the Gate's `id 'nf-blocks@0.2.0-beta.1'` in `gate.config` needs none of this.

### What a member serves

Relative to a member's base URL:

```
index/v3.sqlite            the Index Snapshot, read with single-range GETs
index.html                 the page
blocks/<xx>/<cid>          a block, xx = the cid's last two characters
log/                       the Store Log listing (one of the three forms below)
```

Upload `index/v<N>.sqlite` with `Cache-Control: no-cache` (a revalidation per
load; blocks may be `immutable`): a snapshot uploaded without it can be served
stale from a browser's heuristic cache after it is rewritten, and the page
then fails with `snapshot_changed` rather than answering wrongly, but a user
sees an error they did not need. `nf-blocks:explore` sends `no-cache` for the
snapshot and `public, max-age=31536000, immutable` for a block
(`ExploreServer.groovy`); `gate/cloud/s3tier.py`'s `upload` sends the same
pair for a directly browsed bucket. *Amended 2026-09-28:* a writable S3
member's own writes set the same headers (§5, above), and on S3 the page is
`no-cache` and rewritten only when its stored `ChecksumSHA256` differs.

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
| `GET` or `HEAD /api/samplesheet/<selection cid>.csv` or `.json` | a Selection's items as a samplesheet (decisions 1 and 18 of `docs/plans/2026-09-25-explorer-milestone-2.md`): `Content-Type: text/csv; charset=utf-8` or `application/json`, `Content-Disposition: attachment; filename="selection-<first 16 chars of the cid>.<ext>"`; `404` naming the reason when the address is not a Selection the index holds, or reaches one it does not |
| anything else | `404`; a method other than `GET`, `HEAD` (or `POST` on `/api/put`) is `405` |

`Host` must be `127.0.0.1:<port>` or `localhost:<port>`, and `Origin`, when
sent, `http://127.0.0.1:<port>` or `http://localhost:<port>`; otherwise `403`.
No CORS headers are sent. No other path under a member is ever served:
`coords/` and `nf/` hold host paths. Members are every configured store
(`cas.stores`), local ones read from disk, S3 ones through `S3Ops` over
nf-amazon's client built from the loaded config's `aws` scope
(`S3Access`, §2: profile and SSO, region with the us-east-1 fallback) and
ranged `GetObject`. *Amended 2026-09-28:* with a writable S3 member,
`explore` checks the clock at start (§11) and prints on stderr up to 20
coordinates shadowed by a pointer above them (§5); a listing that fails
warns and the explorer starts.

Every snapshot and block answer carries a strong `ETag` taken from the file as
opened for that answer, never from a second look at the path: size,
modification time and file key (inode) for a local member, the object's own
`ETag` for an S3 one, whose reads are `GetObject` with `If-Match` on it (a
replaced object is a `500`, never another object's bytes). Every response
`explore` sends carries `X-Content-Type-Options: nosniff` (final review
finding 7).

The exit rewrite runs in a shutdown hook. `ExploreServer.stop()` stops
accepting new exchanges and waits (up to a grace period) for one already in
flight to finish before it touches the executor's threads, so a write or an
export under way, both holding `Put`'s monitor, completes rather than being
interrupted mid-write; only then does the hook take that same monitor and
close the index (final review finding 5). Stop a backgrounded `explore` with
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
  (query 3, per run; `type` is one of `string`, `int`, `float`, `bool`, `null`),
  `#/item/-/<item>` (an item with no collection, such as a Selection member
  picked by a query), `#/selections[?offset=<n>][&deleted=1]` (this member's
  Selections, 50 a page; `deleted=1` lists the deleted ones, each with undo),
  `#/selection/<cid>` (one Selection: names, deletion, members, rename,
  delete, undo, samplesheet links) and `#/compose` (the tray, and saving it
  as a Selection) (milestone 2, §16).
- Writing: only when the page was opened through `explore` with its `?token=`,
  and `members.json` has `"write": true` and a writable member. The page posts
  DAG-JSON to `POST /api/put` with the token in `X-NF-Blocks-Token`; composing
  first asks with `?dry_run=true` and offers the existing Selection instead of
  writing when the writable member holds it (`here`), or a copy of it when
  only another member does (decision 21). A Selection is written to the
  writable member, and the page opens it there, carrying the write's outcome
  across that navigation in `sessionStorage` (one key). Rename, delete and
  undo are offered only while the page views the writable member; elsewhere
  `[data-unavailable]` links to the Selection there, and, on the deleted
  Selections list (`#/selections?deleted=1`), where Undo is likewise offered
  only there, to that list in the writable member instead. One write runs at a
  time: a second attempt while one runs is ignored, and the clicked button is
  disabled until it ends. After a write the page re-lists that member's Store
  Log, so it sees its own write; if that refresh fails the outcome is still
  recorded and the status asks for a reload.
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
| `[data-output-index]` | a collection's Output Index File, when it was published: `data-output-index` its address, holding an `<a download>` to its block (ticket 26) |
| `[data-output-index-missing]` | a collection's Output Index File, when it was never written: `data-output-index-missing` the publish path it was to have (ticket 26) |
| `[data-page]` | on the pipeline, collection and Selections views, when the list is not empty: `data-first` and `data-last` (1-based, inclusive; `data-first` is 0 on a page past the end) and `data-total`; `[data-page-next]` and `[data-page-prev]` link to the pages either side |
| `[data-producer]` | one query 1 row: `data-content`, `data-item`, `data-collection`, `data-completion`, `data-filename` |
| `[data-latest]` | query 2's answer, a completion cid or empty |
| `[data-item-result]` | one query 3 item cid |
| `[data-preview-for]` | the Meta Map pills container of one item row: `data-preview-for` the item cid. Empty (no pills) when the item has no Meta Map (milestone 3) |
| `[data-pick-all]` | "Add all N to the tray" on query results and collection pages: `data-via` the collection, `data-count` N; adds every item of the query or collection, not only the page (milestone 3) |
| `body[data-write]` | `available` or `unavailable` |
| `body[data-write-seq]`, `body[data-write-outcome]` | a counter bumped when a write attempt ends, and how: `written`, `exists`, `elsewhere`, or an error code; a save whose naming or restoring failed after the Selection saved records `written` (decision 23) |
| `body[data-written]` | the address the last successful write made |
| `#tray[data-count]` | items in the tray |
| `#tray-unsaved` | shown, with a plain-prose note, while the tray's last write to `sessionStorage` failed |
| `[data-pick]` | a button adding `data-pick` (an address) with `data-via` (space-separated collections) and `data-kind` (`item` or `selection`); on query 3 results `data-via` is the run's Output Collection (milestone 3) |
| `[data-tray-entry]` | one tray entry on `#/compose`: `data-tray-entry` address, `data-kind` |
| `#compose-name`, `#compose-save` | the new Selection's name, and save |
| `[data-exists]` | the dry run found the Selection in the writable member (`here`): `data-exists` address, `data-names` JSON, `data-deletion` the composition's deletion state (decision 23) |
| `#exists-restore` | "Restore", on `[data-exists]` when the composition's `deletion` is not `none`: one `del` superseding `deletion_claims` (decision 23) |
| `[data-held-elsewhere]` | the dry run found the Selection only in another member: `data-held-elsewhere` its address, `data-names` the composition's current names as JSON (decision 21), `data-deletion` the composition's deletion state (decision 23) |
| `#compose-copy` | "Save a copy here", or "Restore a copy here" when the dry run's deletion is not `none`: the write, the name Claim superseding `name_claims`, then a `del` superseding `deletion_claims` (decision 23) |
| `[data-write-failed]` | the banner on the saved Selection's page after a partly failed save: `data-write-failed` the failed steps (`naming`, `restoring`), space-separated; `data-banner-for` the Selection's address; each error in a `[data-error]` (decision 23); a failed Retry adds its own `[data-error]` inside the banner's `#retry-status`, separate from the failed steps' |
| `#retry-restore` | "Retry restore", in `[data-write-failed]` when restoring failed: resends the `del` with the same `supersedes` |
| `[data-unavailable]` | why composing, rename and delete are unavailable (on the Selection view of a non-writable member, with a link to it in the writable member), and, the same way, why Undo is unavailable on the deleted Selections list (`#/selections?deleted=1`) |
| `[data-selection]` | one row of `#/selections`: `data-selection` cid, `data-deletion`, `data-source`, `data-names` JSON |
| `[data-selection-view]` | the Selection view: `data-selection-view` cid, `data-deletion` |
| `[data-name]` | one current name: `data-name` value, `data-claim`, `data-conflicted` when in conflict |
| `[data-deletion-claim]` | one current deletion Claim: `data-deletion-claim` cid, `data-verb` |
| `[data-member]` | one member: `data-member` address, `data-kind`; `data-held` (`here` or `elsewhere`) once that member's own lookup answers -- the view renders before every member's lookup does (final review finding 2), so a row may briefly have none, and one whose lookup fails never gets it, showing `[data-error]` in its "held in" cell instead |
| `#rename-name`, `#rename-save`, `#delete`, `#undo` | the actions on the Selection view |
| `[data-undo]` | an Undo button on a row of `#/selections?deleted=1`: `data-undo` the Selection's cid |
| `[data-refresh-failed]` | the write succeeded but the page could not refresh afterwards |
| `[data-samplesheet]` | `csv` or `json` export link (served by `explore` only) |
| `[data-snippet]` | the `<code>` holding one consumer call line on the Selection and run pages: `untyped`, `channel.fromStore(...)`, or `typed`, `nextflow.Channel.fromStore(..., records: true)`. The mode is the bare string in `localStorage` key `nf-blocks.snippets` (milestone 3) |
| `[data-snippet-mode]` | one of the two toggle buttons, `untyped` or `typed`, that switch every snippet on the page; `aria-pressed` marks the current one (milestone 3) |
| `[data-hidden-runs]` | on a pipeline page, how many runs a delete Claim hides |
| `[data-error]` | an error: `data-error` code, `data-cid` when a block is to blame |
| `window.__nfBlocks.verified` | every cid whose bytes the page hashed and accepted |

Error codes: `no_snapshot`, `no_range_over_cap`, `cors_headers`,
`fetch_failed`, `query_failed`, `worker_failed`, `snapshot_changed`, `hash_mismatch`,
`schema_invalid`, `block_missing`, `not_found`, `bad_route`, `bad_predicate`,
`file_protocol` (the store resolved to a `file://` URL, which a browser will
not fetch from: the page must be served, by any static server or `explore`).
A write can also show, in its status and in `body[data-write-outcome]`, the
transport refusals `forbidden` (`403`), `unsupported_media_type` (`415`),
`too_large` (`413`) and `write_failed` (the server could not be reached, or
answered with a status and body it does not document), and every `PutError`
code: `not_found`, `wrong_kind`, `not_in_via`, `empty`, `stale_supersedes`,
`clock_skew`, `too_large`, `not_writable`, `invalid`. The page itself uses
`invalid` for a rename with no name typed.

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
9. S3 members are read-only and explore-only. *Superseded 2026-09-28:* S3
   members may be writable and resolvable (§2).
10. Nothing is filtered by `delete` Claims until Claims exist (milestone 2).

## 16. Selections (milestones 2 and 3)

*Status 2026-09-26: milestone 2 accepted; Gate browser tier B 6 of 6 (all
local); cloud tier A6-A7 (`make gate-cloud`) passed on the merged tree
(`50f677e`), A6 at 7 / 43 requests.*

Specified in `../.scratch/block-explorer/spec.md` (sections 5.6, 7, 8 and 9);
plan `docs/plans/2026-09-25-explorer-milestone-2.md`. The page's routes and
DOM contract for Selections are in §15 above, amended in place as this
milestone landed.

### Block kinds a client builds; the write endpoint

A client (`nf-blocks:put`, the endpoint, or the page) may build exactly two
kinds: `Selection` and `Claim` (§6 has both schemas). A request is DAG-JSON,
links as `{"/": "bafy…"}`, the block's own content without `kind`, `schema` or
`asserted_by` (the server fills those three); `kind` in the request is
checked against the two buildable kinds. The server normalises (sorts,
dedupes), validates, encodes DAG-CBOR, writes to the writable member, appends
a Store Log entry and ingests into its local index; it never rewrites the
Index Snapshot on a write (the page sees its own write through the tail,
decision 14).

Response: `{"address": {"/": <cid>}, "block": <canonical block as DAG-JSON>,
"entry": <Store Log entry name>, "written": <bool>}`. Dry run
(`?dry_run=true`, decision 10): `{"address", "exists", "here", "names",
"name_claims", "deletion", "deletion_claims"}`, and writes nothing; `exists`
is true when any member of the composition holds the block, `here` when the
writable member does (decision 21). `names` and `name_claims` are the
composition's current name Claims' values and addresses, in claim-address
order. `deletion` and `deletion_claims` are the composition's deletion state
(`none`, `deleted` or `conflicted`) and its current deletion Claims'
addresses, in claim-address order; a current `del` can leave
`deletion_claims` non-empty while `deletion` is `none` (decision 23).

Errors are DAG-JSON `{"error": <code>, "message": <text>, "at": <JSON
pointer into the request>}`, `400` (`409` for `not_writable`). Eight codes
from spec section 9.4 -- `not_found`, `wrong_kind`, `not_in_via`, `empty`,
`stale_supersedes`, `clock_skew`, `too_large`, `not_writable` -- plus a
ninth, `invalid` (decision 7): the request is not DAG-JSON or does not have
the block's shape (a missing or mistyped field, an unknown key, a `/` map
that is not a link or bytes, `verb: "add"`). Transport refusals stay plain
text, never this body: `403` (no or wrong `X-NF-Blocks-Token`, or a foreign
`Host`/`Origin`), `415` (a content type other than `application/json` or
`application/vnd.ipld.dag-json`; parameters such as `charset` ignored),
`413` (a body over `Put.MAX_REQUEST_BYTES`, 2 MiB). The CLI prints the same
error body and exits 1. The page itself uses `invalid` for a rename with no
name typed.

### `nf-blocks:put`

```
nextflow [-c <config>] plugin nf-blocks:put <file|/dev/stdin> [--dry-run] [--name <name>]
```

Builds and writes one Selection or Claim from a DAG-JSON file, sharing
`Put`'s one builder with `POST /api/put` (spec section 9.2). Prints the response
or error body (DAG-JSON) on stdout, exit 0 or 1. `put` reads a path
(decision 11). Nextflow 26.04.6's launcher refuses a bare `-` before any
plugin runs (`Unknown option: -`), so from a shell stdin is `/dev/stdin`;
the verb reads `/dev/stdin` (and `-`, when called in-process) from its own
stdin stream, and reads any other path with a plain read loop that never
sizes or seeks it, so a pipe, a FIFO or a redirect all work. A bare `--dry-run`
reaches the verb as `--dry-run`, `true` (`Launcher.normalizeArgs` appends
`=true`). Staleness is member-scoped (decision 22): superseding a Claim
that only a read-only member has already superseded succeeds and leaves a
conflict; the dry run reports `here` and `name_claims`, and `deletion`, `deletion_claims`.
A member may be written as an Item Occurrence string, `cas://<collection>/<item>`,
meaning that item via that collection (`Put.groovy:162-168`); `items --format
selection` writes its members that way. `--name` is in §15.

### Samplesheet export

`GET` or `HEAD /api/samplesheet/<selection cid>.csv` or `.json`, served by `explore`
only (decision 1; no new verb for it; milestone 3's `items`, §15, prints the same columns from the command line), linked
from the page's Selection view. One row per distinct item, sorted ascending
by item CID (decision 18).

- **Meta Map columns** come out in DAG-CBOR canonical key order (length,
  then bytes), not the order the pipeline author wrote them in: a stored Meta
  Map is decoded in that order (a Groovy `Map` built by `DagCbor.decode`),
  and the samplesheet reads columns from the decoded map, never re-sorting
  them. Nested maps are flattened to dotted names.
- **File columns** are in first-seen order, named by the leaf's structural
  path in the item -- tuple index or record key, nested positions joined by
  `.` (`1` for `[meta, bam]`, `1.0` for the first of `[meta, [chunks...]]`)
  -- never by file name. A position that collides with a Meta Map column name
  is written `file.<position>`.
- **Cell values**: an addressed raw leaf is `cas://<cid>/<name>` (the form
  `fromStore` restores, §13); a directory leaf is `cas://<manifest cid>`; any
  other leaf is blank. In CSV a list or map value is its JSON text, a scalar
  is the index's text form (`MetadataView.scalar`); a string, however long,
  exports in full -- never the index's sha256 digest, which is a storage
  artifact of the index, not a value to carry into an export. JSON keeps the
  Meta Map's own nesting, types and absence, then adds the file columns.
- **CSV quoting is RFC 4180**: a field is quoted (its own quotes doubled)
  only when it holds a comma, a quote, a CR or an LF. Non-ASCII text is
  never quoted for that reason alone; it round-trips as UTF-8, unmangled, in
  both CSV and JSON.

### Decisions made where the spec is silent, as built

1. The samplesheet export is served by `nf-blocks:explore` at `GET
   /api/samplesheet/<selection cid>.csv` and `.json` (above). No new verb.
2. Snapshot runs are those the member's Store Log announced, plus those found
   in its blocks (`run.member`, for a store written before the Store Log,
   §12). Fixes milestone 1's decision 1: a RunCompletion in two members now
   appears in both snapshots.
3. Four indexes beyond spec section 11, so the page's queries pass
   `ExplorerQueriesTest`'s no-scan guard: `log_entry(cid, member)` unique,
   `log_entry(kind, written_at DESC)`, `claim(subject_cid)`,
   `claim_current(subject_cid, attribute)`. `log_entry.written_at` is
   ISO-8601 UTC with milliseconds; `log_entry` keeps the earliest entry per
   `(cid, member)` ("first seen in this member").
4. `claim.value` and `claim_current.value` hold a string value as itself,
   null as NULL, any other value as its DAG-JSON text.
5. Current state (spec section 8) is one set of rules implemented twice, in
   `ClaimState.groovy` and `web/src/claims.js`, both pinned by
   `web/test/fixtures/claim-vectors.json`: the current Claims of a subject
   are those no Claim of that subject supersedes; they group by `attribute`,
   with `delete` and `del` (attribute null) forming the deletion group; a
   group with more than one current Claim is conflicted. Names are the
   values of the current `set name` Claims, in claim-address order.
   Deletion is `deleted` when the deletion group's one current Claim is a
   `delete`, `conflicted` when the group has more than one, `none`
   otherwise. Only `deleted` hides.
6. Claim request rules, checked by `Put` before writing: `set` needs an
   attribute and a non-null value; `delete` has neither; `del` has no value
   and supersedes at least one Claim; a `name` value is a non-empty string of
   at most 256 characters; `add` is refused (out of the slice); every
   superseded address must be about the same subject (`wrong_kind`
   otherwise), present and not already superseded (`stale_supersedes`
   otherwise, counting only Claims logged in the writable member; decision 22).
7. Error code `invalid`, a ninth code beside spec section 9.4's eight (above).
   `DagJson.decode` refuses, as `invalid`, naming the field: a string (a
   value or a map key) holding a lone surrogate, reachable only through a
   `\uXXXX` escape, which `DagCbor.encode` cannot turn into UTF-8; an integer
   literal outside the range `DagCbor.encode` carries (-2^64 to 2^64-1); and
   an integer literal of more than 40 digits, before `BigInteger` ever parses
   it (final review findings 1 and 6). `Put` also wraps its own
   `DagCbor.encode` call as a backstop, for a request built as a `Map`
   directly rather than decoded from DAG-JSON.
8. `derived_from` is accepted as links or as bytes in a request and always
   stored as bytes (binary CIDs), sorted by the CID's string form.
9. Idempotence first. `Put` computes the address before any semantic check.
   A block the writable member already holds is success with `"written":
   false`, without re-checking `supersedes`, the clock or membership, and
   reuses the block's existing Store Log entry (one is appended only if none
   exists). A retried request therefore never fails with `stale_supersedes`
   or `clock_skew` against itself.
10. `dry_run` on the endpoint is the query parameter `?dry_run=true`, since it
    is not block content. `exists` is true when any member of the
    composition holds the block.
11. `put` reads a path (above).
12. The launch token is 26 base32 characters, printed as `?token=<t>` in the
    URL `explore` prints, and sent by the page as the header
    `X-NF-Blocks-Token`. Only `POST` needs it.
13. A run's delete Claim names its RunCompletion. Query 2's SQL excludes runs
    with a current `delete` Claim, conflicted deletions included, and
    `Index.latestSuccessfulRun` warns about each conflicted one it leaves
    out; the page's query 2 view does the same, re-checking a tail Claim
    each time rather than trusting a cached filter. The page's run *lists*
    (a pipeline's runs) hide only runs whose deletion is `deleted`, not
    conflicted ones, filtering the page of rows it shows and saying how many
    it hid. The UI offers no run deletion (spec section 5.6).
14. The page sees its own write through the tail: after a write it re-lists
    the writable member's Store Log. Composing works from any member's view;
    the Selection is written to the writable member. Rename, delete and undo
    are offered only when the page views the writable member; elsewhere
    `[data-unavailable]` links to the Selection there, since a Claim about a
    Selection the writable member lacks cannot be seen there afterwards.
    Composing from another member still navigates to the writable member
    after saving, carrying the write's outcome across that navigation in
    `sessionStorage` (one key), restored on load.
15. The page keeps the tray of picked items in `sessionStorage` (per tab,
    wrapped in `try`), so picks survive switching member; the page works
    without it. When a write is refused (no storage, or a full quota after
    "Add all"), the header shows `#tray-unsaved`, "The tray could not be
    saved in this browser, so it will be lost on reload.", and the "Added N
    to the tray." status repeats it (final review, 2026-09-27).
16. `fromStore(selection:)` and the samplesheet resolve nesting through the
    index's recursive CTE (spec section 11), after catch-up, and fail naming
    any nested Selection the index does not hold rather than emitting a
    partial set. Items come out sorted by CID string. `run`, `output` and
    `where` are refused beside `selection`. Both callers ensure a Selection
    copied into the composition without its Store Log entry is indexed
    before that CTE runs, through the one shared method,
    `Index.ensureSelectionIndexed` (final review finding 4): the samplesheet
    export used to skip this, so its export of such a Selection failed
    `not_found` where `fromStore(selection:)` succeeded.
17. An Item Occurrence without a leaf name is a directory of the item's
    leaves by leaf name; a leaf name that two leaves of one item share is
    refused, naming both positions. Publish-path traversal of a collection
    root stays unsupported, and the error names both readings (§7).
18. Samplesheet specifics (above).
19. Output Collection rows now carry `kind = 'output'` and `asserted_by`;
    Selection rows carry `kind = 'selection'`, `completion_cid` and
    `output_name` NULL.
20. Three of milestone 1's four parked minors are fixed where their code is
    touched: dead `Explorer.closures` removed (Task 13), the pager's label
    past the end fixed (Task 14), `Cache-Control: no-cache` documented and
    set on uploaded snapshots (this task, §15 "What a member serves" and
    `gate/cloud/s3tier.py`). The fourth was already fixed; see Resolved,
    below.
21. A Selection held only in a read-only member (ticket 09). The dry run's
    `here` separates "held here" from "held somewhere". With `exists` and
    not `here`, the page offers "Save a copy here": the ordinary write,
    which copies the block into the writable member, logs and ingests it.
    The name field is prefilled when the composition has exactly one
    current name, and the name Claim written with the copy supersedes every
    current name Claim in `name_claims`, so the composition ends with one
    name rather than a same-value conflict.
22. Staleness across members (ticket 09). The "already superseded" check
    counts only Claims logged in the writable member, the ones the page can
    see there. A superseder held only in a read-only member no longer blocks
    a rename; the composition then has two current names, reported as a
    conflict in claim-address order. The presence check stays
    composition-wide, so `nf-blocks:put` can supersede a Claim another
    member holds: that is how a person settles a conflict between members.
23. A Selection deleted in another member (ticket 10). The dry run reports
    the composition's `deletion` and `deletion_claims`. Held only elsewhere
    and `deleted` or `conflicted` there, the page labels the copy "Restore a
    copy here": after the copy and its name Claim (when a name is set) it
    writes a `del` Claim superseding every Claim in `deletion_claims`,
    whether or not a name is set, so the Selection is live across the
    composition. The `del` is tried even when naming fails. A save whose
    naming or restoring failed opens the saved Selection with a banner naming
    each failure and, for the `del`, Retry restore (ticket 11). Held here but
    deleted in the composition, the "already exists" message names the
    deletion and offers Restore (one `del` superseding `deletion_claims`;
    ticket 11). Both messages say "in this composition", since the dry run
    cannot tell which member holds a deletion. The Selection view still reads
    only this member's Claims (decision 22). After a `del` restores a Selection,
    `deletion_claims` still names that current `del` Claim although
    `deletion` is `none`; clients act on `deletion`, not on whether
    `deletion_claims` is empty.
24. Deferred minors from milestone 2's reviews (ticket 11). `Put`'s
    idempotent path ingests any Store Log entry it appends; DAG-JSON float
    literals over 64 characters are refused, as over-long integers are. On
    the page: the latest successful run reads the snapshot's runs in pages
    (1, then 50 at a time) until a visible one appears, so a tail deletion
    of more than 50 runs cannot hide an older live run; tail Claim values
    are normalised to the snapshot's text form, matching `Index.valueText`
    for strings, null, integers, booleans and maps with ASCII keys, but not
    byte for byte for floats (Groovy writes `Double.toString`, e.g. `1.0`,
    `@ipld/dag-json` writes `1`) or for maps with non-BMP keys (UTF-16 vs
    UTF-8 key order); the page writes neither. `runPage` skips the
    supersedes query when there are no Claims; Copy says when it fails; a
    Selection member picked by query copies as its bare address; the deleted
    list says why Undo is unavailable; `page-smoke.mjs` is retired in favour
    of tier B.

### Gate browser tier B

Twelve assertions (spec section 1.3, tier B), all local: a Selection made in
the page has the Gate's own address (8); `fromStore(selection:)` receives each
distinct item once, nested included (9); rename, delete and undo are Claims at
the Gate's addresses, and a replay writes nothing (10); two sessions renaming
from one view surface a conflict, not an overwrite (11); a POST without the
token, from another Origin, or as `text/plain` is refused, and writes nothing
(12); the samplesheet lists exactly the Selection's items, and its cells stage
(13); a read-only member's Selection is copied and named (14); a Claim in
another member does not lock a rename (15); a copy deleted in another member is
restored (16); a per-run query pick keeps its collection, and Add all adds
every item with it (17); the typed consumer receives records (18);
`items --format selection` piped into `put /dev/stdin --name` through the
real launcher writes the named Selection at the Gate's address (19). B9's and
B18's consumers run the call line the Selection view shows (`[data-snippet]`)
verbatim, so a broken snippet fails the Gate. B12 probes with a Selection no
step has written, so the assertion can actually fail if a refusal ever let a
block or a Store Log entry through; the earlier draft replayed an already-
written Selection, which could not distinguish "refused" from "written".

### Resolved

- **"Stale worker error code"**, parked by milestone 1's final review: the
  VFS kept `lastError` across worker requests, so a later, unrelated failure
  could carry an earlier read's code and cause. Already fixed in milestone 1
  (`65e63e8`): the worker calls `clearError()` at the start of every request,
  and a unit test pins it. What remains is narrow and accepted: within one
  request, a failed read that SQLite retries past, followed by a failure of
  another kind, reports the read's code rather than `query_failed`.

### Milestone 3: picking items for a downstream workflow

*Status 2026-09-27: milestone 3 accepted; Gate lineage 11/0/6, browser tier A
5/5, tier B 12/12 (all local).*

Plan `docs/plans/2026-09-27-explorer-milestone-3.md`, from the UX map
`../.scratch/block-explorer/ux/map.md`; each ticket's `## Answer` holds the
reasoning.

1. An unset `outputDir` means the writable member's alias, set by
   `CasObserverFactory` before `WorkflowMetadata` copies it (§2).
   [02](../.scratch/block-explorer/ux/issues/02-unset-outputdir.md), research
   [01](../.scratch/block-explorer/ux/issues/01-outputdir-read-before-flowcreate.md).
2. A run that fails because `fromStore` cannot be found ends with a warning
   naming the fix, untyped and typed apart, chosen by the exception (§11,
   §13). [06](../.scratch/block-explorer/ux/issues/06-missing-fromstore-exception-chain.md),
   [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q1.
3. On a run that is already failing, `onFlowComplete` logs its own failures
   instead of throwing, so `notifyError` still reaches every observer (§11).
   [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q2.
4. `fromStore(..., records: true)` restores every non-Leaf map as
   `RecordMap`; default `false` in every script mode (§13).
   [09](../.scratch/block-explorer/ux/issues/09-opt-in-record-restore.md).
5. `fromStore` with no `selection`, `run` or `output` says what it takes
   (§13). [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q3.
6. One run-reference resolver, `RunRef`, for `fromStore(run:)` and
   `items --run` (§13, §15).
   [05](../.scratch/block-explorer/ux/issues/05-items-verb-and-put-name.md) Q2.
7. `nf-blocks:items`, a fourth verb, read-only, over the caught-up local
   index; conditions match as text; formats `csv`, `json`, `occurrences`,
   `selection` (§0 rule 5, §15).
   [05](../.scratch/block-explorer/ux/issues/05-items-verb-and-put-name.md) Q1-Q4, Q6.
8. `put --name <name>` names a Selection with one `set name` Claim (§15).
   [05](../.scratch/block-explorer/ux/issues/05-items-verb-and-put-name.md) Q5.
9. Query results, collection pages, the tray and Selection members share one
   item row: a label of up to three paths (list-valued paths skipped, strings
   first, then most distinct values among loaded previews, ties by path),
   file-name chips, the pick control, the Meta Map as `key | value` pills
   coloured by type (numbers blue, `true` green, `false` red, null grey
   italic; a number-like string quoted), and the shortened item CID. A pill's
   value runs query 3 with that pair added.
   [03](../.scratch/block-explorer/ux/issues/03-item-previews.md),
   [10](../.scratch/block-explorer/ux/issues/10-meta-map-pairs.md).
10. Previews are OutputItem blocks fetched through `blocks.ofKind`
    (hash-checked, cached), at most 6 at once, the first 100 rows of a view
    without a click; no §12 change and no snapshot query.
    [03](../.scratch/block-explorer/ux/issues/03-item-previews.md) Q3.
11. Runs are shown as `<run_name> / <output>`, the CID in `title`: a tray
    entry's "picked from", a Selection member's via, the item page's
    "produced by". [03](../.scratch/block-explorer/ux/issues/03-item-previews.md) Q4.
12. A per-run query pick records its Output Collection as `via`; "Add all N
    to the tray" (`[data-pick-all]`) adds every item of the query or
    collection; row checkboxes add the checked ones; no client-side size
    check, since the dry run's `too_large` covers it (spec section 5.6).
    [04](../.scratch/block-explorer/ux/issues/04-query-results-keep-run.md).
13. The Selection page shows the `include` line and the
    `fromStore(selection:)` call; the run page shows its `lid://` and, per
    output, the `fromStore(run:, output:)` call; one untyped | typed toggle
    for every snippet, remembered per viewer (§15 `[data-snippet]`).
    [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q4.
14. The Gate runs the snippets: tier B9's consumer and B18's typed consumer
    take their call line verbatim from the page (§14, Gate browser tier B).
    [07](../.scratch/block-explorer/ux/issues/07-fromstore-discoverable.md) Q6.

`slf4j-api` (bundled transitively through the AWS SDK until milestone 4 made
the SDK `compileOnly`; the exclusion stays so no other dependency brings it
back) is excluded from the plugin zip (Task 1b, controller-added fix), so a plugin `log.*` call reaches
`nextflow.log` instead of a NOP logger. That is as far as a plugin logger
named `robsyme.cas.*` gets: Nextflow's console appender admits only loggers
whose names start with a configured package, `nextflow` among them
(`LoggerHelper.ConsoleLoggerFilter`, `LoggerHelper.groovy:408-441` at
v26.04.6). So the lines written for the person at the terminal, §11's
`onFlowError` hint and §2's `outputDir not set` line (and, since milestone
4, the clock-skew warning, the node-hashing warnings of §11 and §12's
seeding fallback), go through the logger `nextflow.cas` and appear on the
terminal as well; every other plugin log line, §0 rule 3's warnings
included, is in `.nextflow.log` only. Gate
assertion 6 requires the `outputDir` line in the consumer's `nextflow.log`
and on its console (`logs/consumer/stdout.log`).

Left out, per the map: plugin factories on the typed `channel` namespace
(nextflow-io/nextflow#7694, upstream), self-registration of `fromStore` so
the include is optional, and a "one of" condition in the page and in
`fromStore(where:)`.

## 17. Milestone 4: cloud (2026-09-28)

*Status 2026-09-28: built on `feat/cloud-m4`. `./gradlew check`: 1,029
unit tests and 4 `memoryBoundTest` features (the S3 multipart case
included) pass, `dependencyCheck` green, `webTest` 181 of 181. `make gate` on
a fresh `GATE_ROOT`: lineage 12 PASS, 0 FAIL, 6 SKIP; browser tier A 5/5;
tier B 12/12; the Gate's own unit tests 284 OK. Tier two's unit tests 31 OK.
Not yet run, for Rob (they need his SSO session): `make gate-cloud`
(A6-A7), `make gate-tier2` (T1-T6, T2b), one `explore` against a
writable S3 member in a bucket he names, and, at release, a clean-machine
install from the registry confirming nf-amazon is fetched (Task 1 ruling).*

*Status 2026-09-28: accepted with Rob's SSO session on `9b0c7f2`: `make gate`
12/0/6, A 5/5, B 12/12; `make gate-cloud` A6-A7 2/0; `make gate-tier2` T1-T6
7/0/0 (70 Batch jobs, the head node read 0 bytes in every run); one
`explore` against a writable S3 member. Merged to `main` at `0b32a0e` and
released as 0.2.0-beta.1.*

Plan `docs/plans/2026-09-27-cloud-milestone-4.md`, from the map
`../.scratch/post-gate/map.md`; each ticket's `## Answer` (and addendum)
holds the reasoning, and the execution ledger is
`.superpowers/sdd/2026-09-27-cloud-milestone-4/progress.md`.

1. There is one S3 client, nf-amazon's: the SDK is `compileOnly` through
   `io.nextflow:nf-amazon:3.9.2`, `requirePlugins = ['nf-amazon@>=3.9.2']`,
   `explore` builds its client through the same factory, and
   `aws.client.s3Acl` is not applied (§1, §2).
   [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q1,
   [01](../.scratch/post-gate/issues/01-s3-facts-for-a-writable-store.md) Q1.
2. A block of 1 MiB or more is looked for with `HeadObject` first, and every
   write carries `If-None-Match: *` with a 412 as success (§5).
   [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q2,
   [14](../.scratch/post-gate/issues/14-measure-s3-write-behaviour.md) item 1.
3. Up to 5 GiB a block is one `PutObject` with SHA-256, above it a
   hand-driven multipart upload of `max(64 MiB, ceil(size/10000))` parts
   streamed from a file channel, and `putStreaming` spools to `cas.tmpDir`
   (§2, §5). [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q3,
   [14](../.scratch/post-gate/issues/14-measure-s3-write-behaviour.md) items 3, 4.
4. An S3 member keeps the local layout byte for byte under its prefix, plus
   `tmp/` for staging copies (§5).
   [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q4.
5. Immutability on S3 is the conditional writes alone; the deny-overwrite and
   deny-delete bucket policy is documented hardening (§5, README).
   [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q5.
6. Coordinate conflicts on S3 have the local outcomes, checked with a `HEAD`
   per ancestor and one max-keys-1 listing; last write wins, a raced pointer
   is shadowed, and nothing locks (§5).
   [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q6,
   [03](../.scratch/post-gate/issues/03-concurrent-writers-on-one-s3-member.md) Q3-4.
7. `nf/` is `DefaultLinStore` on the member's URI string, and
   `nextflow lineage find` costs a GET per record (§10).
   [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q7,
   [01](../.scratch/post-gate/issues/01-s3-facts-for-a-writable-store.md) Q6.
8. The snapshot is built locally and uploaded with one conditional
   `PutObject` (`no-cache`, `x-amz-meta-runs`, `If-Match` on the ETag it
   replaces), a 412 skipping the rewrite; the page is rewritten only when its
   `ChecksumSHA256` differs (§15).
   [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q8,
   [03](../.scratch/post-gate/issues/03-concurrent-writers-on-one-s3-member.md) Q1.
9. Storage class and SSE come from the `aws.client` settings; `GLACIER` and
   `DEEP_ARCHIVE` are refused, infrequent-access classes warn, and
   `GLACIER_IR`, which nf-amazon 3.9.2 drops, warns that blocks go to
   `STANDARD` (§2).
   [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q9.
10. Any member may be local or `s3://<bucket>[/<prefix>]`, the writable one
    and `cas.resolve` included, through `FileHelper.asPath`; a malformed S3
    URI and any other scheme are refused (§2).
    [02](../.scratch/post-gate/issues/02-s3-blockstore-write-semantics.md) Q10.
11. The local clock is compared with S3's `Date`: over 1 minute warns, over 5
    minutes aborts (§11).
    [03](../.scratch/post-gate/issues/03-concurrent-writers-on-one-s3-member.md) Q2.
12. Blocks from two writers are conditional and idempotent, a 409 retried up
    to 3 times in all, and the page's `put` behaves the same (§5).
    [03](../.scratch/post-gate/issues/03-concurrent-writers-on-one-s3-member.md) Q5.
13. `provider` leaves the Leaf: the RunCompletion's `providers` records it,
    OutputItem goes to schema 2, and schema-1 readers ignore the old field
    (§6). [16](../.scratch/post-gate/issues/16-s3-copy-address-provider.md) Q1-2.
14. `s3-copy` is the default for every S3-to-S3 publish up to 5 GiB, checked
    against a node digest when there is one; above 5 GiB a node digest drives
    `UploadPartCopy`, else the head node reads (§8).
    [16](../.scratch/post-gate/issues/16-s3-copy-address-provider.md) Q3-6.
15. `DirectoryManifestBuilder` calls `toRealPath` only where the provider has
    it (§8). [05](../.scratch/post-gate/issues/05-cloud-executor-publish-on-batch.md).
16. Fusion's links are content: a directory's `.fusion.symlinks` is decoded
    under the §6 link rules and left out of the manifest; without Fusion an S3
    work dir's links arrive as copies (§8).
    [15](../.scratch/post-gate/issues/15-fusion-symlinks-in-a-published-directory.md) Q1-6.
17. A link staged onto an object store is a copy of its target (§8).
    [15](../.scratch/post-gate/issues/15-fusion-symlinks-in-a-published-directory.md) Q7.
18. The Fusion Address Provider is a chained `afterScript` default writing
    `.command.cas`, read once per task at publish, with a fallback on a miss
    (§8, §11). Spec §14,
    [06](../.scratch/post-gate/issues/06-remeasure-fusion-on-batch.md).
19. A cache with no watermark for a member seeds from that member's Index
    Snapshot, else scans with a warning; a run rewrites the snapshot only
    after a clean catch-up and without losing `run` rows (§12, §15).
    [04](../.scratch/post-gate/issues/04-index-cache-on-an-ephemeral-head-node.md) Q1-10.
20. No new verbs; rule 5 is reworded (§0).
    [07](../.scratch/post-gate/issues/07-post-gate-cli-verbs.md).
21. Gate tier one gains assertion 13 (a seeded and a scanned cold cache give
    the same answer) and assertion 2's check that `head-node` and
    `fusion-node` publishes give one OutputItem address (§14).
    [04](../.scratch/post-gate/issues/04-index-cache-on-an-ephemeral-head-node.md) Q11,
    [16](../.scratch/post-gate/issues/16-s3-copy-address-provider.md) Q1.
22. Gate tier two, `make gate-tier2`, runs on demand on the scidev Batch
    queue into a throwaway bucket: T1-T6, with T2 expecting `s3-copy` in a
    fresh member and T2b `fusion-node` in T1's (§14, `gate/tier2/README.md`).
    [11](../.scratch/post-gate/issues/11-gate-tier-two-harness.md),
    [03](../.scratch/post-gate/issues/03-concurrent-writers-on-one-s3-member.md) Q6.
23. Directory Manifests carry no execute bit (schema 2), so no backend can
    change a manifest address (§6).
    [15](../.scratch/post-gate/issues/15-fusion-symlinks-in-a-published-directory.md) addendum.

### Decisions made where the tickets are silent, as built

1. RunCompletion is at schema 2 too; schema 1 reads as `providers: {}`, and
   in the IPLD Schema `providers` is the one `optional` field, which the page
   requires on a schema-2 RunCompletion.
2. The schema-1 Leaf survives as `LeafV1`; the page validates leaves by the
   item's `schema`, and Groovy refuses an OutputItem or RunCompletion whose
   `schema` is not 1 or 2.
3. `providers` covers every Leaf address the run published: each file leaf,
   and each directory leaf by its manifest, which is always `head-node`; one
   address may appear under two providers. Pre-flight F30 had it list the
   files inside published directories too (`Publish.contents`, merged by
   `Join`). The final review (I5) reverted that: at about 41 bytes of
   DAG-CBOR per CID, a run publishing about 1.6 million files inside
   directories writes a RunCompletion over the index's 64 MiB block limit,
   so that run would never be indexed. The files inside a directory are
   counted per provider in the summary line (8) instead. The cost: `verify`
   cannot target an asserted file inside a directory from a block; a
   separate linked block could add that later without changing this one.
   For Rob.
4. `fusion-node` is recorded when the node digest named a block the writable
   member already held (a `HEAD` hit, or a 412 on the direct copy) or drove an
   `UploadPartCopy`; `s3-copy` when S3 returned a copy's SHA-256; `head-node`
   when the head node read the bytes, including an S3 source into a local
   member, whose hash must then equal the node digest.
5. `cas.nodeHash`, a Boolean defaulting to `fusion.enabled`, turns on both
   halves; tier one sets it for `again`; the name stays `fusion-node`.
6. The hashing script is `src/main/resources/robsyme/cas/node-hash.sh`, as
   described in §11; a closure `afterScript` is left alone with a warning. A
   process whose own body sets `afterScript` gets no config default, so
   `onProcessCreate` warns once per such process (Task 9 review).
7. `.command.cas` is parsed with coreutils' escaping, lines that do not parse
   skipped, a file over 16 MiB ignored with a warning; the task directory is
   the first two segments under `workDir`, both compared as real paths; it is
   read once per task directory per run and kept by the addresser.
8. The head-node byte count and the provider counts are one info line at
   `onFlowComplete` (§8).
9. Staging out of an S3 member into S3 is a `CopyObject` checked by SHA-256,
   up to 5 GiB; an SDK refusal streams instead, a mismatch or a missing
   full-object SHA-256 aborts (§8).
10. The client factory is `new AwsClientFactory(new AwsConfig(aws),
    aws.resolveS3Region()).getS3Client(S3SyncClientConfiguration.create(props),
    global)`, needing no session and skipping the wrapper's `listBuckets`.
11. Every S3 request goes through `S3Ops` (`SdkS3Ops`; `MemoryS3Ops` in
    tests), and `S3MemberFiles` reads through it too.
12. The single-request limit is 5 GiB (5,368,709,120 bytes).
13. A `PutObject`'s `ChecksumSHA256` is compared with the CID (a composite
    multipart one is not); `put(cid, in, size)` HEADs a block of 1 MiB or
    more before it reads the stream (Task 5 review) and spools through
    `cas.tmpDir`; `putDagCbor` uploads its bytes; `putFile` hashes in place.
14. A 409 is tried up to 3 times in all, a multipart upload restarting
    whole; the Store Log's `putEntry` then throws and the observer warns.
15. Cache-Control: blocks `public, max-age=31536000, immutable`, snapshot and
    page `no-cache`, nothing on `coords/`, `log/`, `nf/` or `tmp/`.
16. The snapshot base is taken before the catch-up; the run-count guard
    applies to every writer; an S3 snapshot without `x-amz-meta-runs` is
    counted once; a 409 on the snapshot PUT skips as `replaced_meanwhile`
    without a retry, and so does a 404 on its `If-Match` (Task 7, Task 11
    reviews); skips log at info.
17. Seeding copies per run, Selection and Claim not already held, tagging
    `run.member` and `log_entry.member`, and carries a logged block the
    snapshot's writer could not read as `missing(NULL, cid)` (Task 8
    review); the fallback warning is on `nextflow.cas` and only for a
    non-empty Store Log.
18. Assertion 13 locks the RunManifest, RunCompletion and OutputCollection
    blocks of the producer's runs at or before the snapshot's watermark,
    taken from the Store Log, and requires the seeded run to succeed with the
    same answer and no permission failure in its log; the matcher accepts
    "could not be read" or "could not be decoded as <Kind>" with a locked
    path, since the plugin reports a locked RunCompletion as undecodable
    (Task 13 ruling).
19. The clock check is one `HEAD` on the writable S3 member's snapshot key;
    a failing `HEAD` warns and continues; `put` and `explore` refuse to start
    above 5 minutes.
20. On S3 a coordinate under an ancestor pointer answers absent to `read`,
    `exists`, `isDirectory` and `children`; `explore` prints up to 20.
21. S3 coordinates have no directory markers; the pointer body is the local
    one.
22. `cas.tmpDir` is a path string; a trailing slash on an S3 location is
    dropped; the storage class is read from `aws.client.storageClass`, else
    `uploadStorageClass`, and judged only for a writable S3 member.
23. A `.fusion.symlinks` over 1 MiB, not UTF-8, holding NUL, or with an empty
    or `/`-bearing name is unparseable (one `unresolvable`); a listed name
    with no object is `unresolvable`. A decoded link is chased to the end of
    its chain, so a dangling chain or a cycle is `unresolvable` as the local
    walk records it (ticket 15 decisions 1 and 3 disagree here; Rob to
    confirm, pre-flight F31). A target is resolved segment by segment, as
    POSIX does: a segment that is a decoded link is chased before the next,
    and a later `..` climbs from where it landed, so a target through a
    decoded directory link records `symlink` as a local run does (Task 3
    ruling, fixed in the final wave, I6). A sidecar that lists its own name
    does not enter the manifest.
24. Tier two's details are in `gate/tier2/README.md`. T4 passes the directory
    as an Item Occurrence `cas://<collection>/<item>/A_qc` rather than
    `cas://<manifest>`, because staging a bare manifest URI had no file name
    and Nextflow's FilePorter retried its integrity check forever (Task 14
    finding). Fixed in the final wave (I1): a bare Store URI is named by its
    CID, and `fromStore` emits a directory leaf as `cas://<item>/<leaf name>`
    (§7, §13).

Added during execution:

25. A read-only lineage member is opened only when `nf/` and `nf/.history`
    both exist, so a pure read never writes it (§10, Task 11 review).
26. `gate/gate.sh` installs `nf-amazon@3.9.2` into its `NXF_PLUGINS_DIR`,
    since Nextflow 26.04.6 does not fetch a required plugin for one already
    unpacked there; a fresh `GATE_ROOT` works (§1, §14, Task 1 review).
27. A copy whose SHA-256 disagrees with the node digest, and whose delete is
    refused (the hardening policy), aborts naming the key, never falls back
    (§8, Task 10 ruling, for Rob).
28. `CasSession.coordinatesOf` builds the tree of a configured alias left out
    of `cas.resolve` on first use (§7).
29. `web/src/generated/schema.json` stays gitignored; `cd web && npm test`
    regenerates it (Task 2 ruling).
30. A staging link that names a directory already being materialised aborts
    rather than recursing (§8, Task 12 review).
31. Found while documenting (Task 15, from the AWS guide "Enforce
    conditional writes on Amazon S3 buckets"): the deny-overwrite policy of
    carried decision 5 needs an `s3:ObjectCreationOperation` exemption for
    multipart parts, and a bucket enforcing conditional writes refuses
    `CopyObject` into the enforced prefix, so under it `s3-copy` falls back
    to the head-node read (§5). For Rob: keep the policy as optional
    hardening with that cost, or narrow it.

## 18. Milestone 5: nf-core pipelines in the store (2026-09-29)

*Status 2026-09-30, after the final review's fix wave: built on
`feat/m5-nfcore`. `./gradlew check`: 1,087 unit tests and 4
`memoryBoundTest` features pass, `dependencyCheck` green, `webTest` 187 of
187. Gate unit tests 296 OK (1 skipped). `make gate` on a fresh
`GATE_ROOT`: lineage 15 PASS, 0 FAIL, 6 SKIP; browser tier A 5/5; tier B
12/12 (no flake this run). Tier two's own unit tests, `python3 -m unittest
discover -s gate/tier2`, 43 OK.
Not yet run, for Rob (needs his SSO session): `make gate-tier2 GATE_ROOT=<the
same root>` (T1-T6, T2b, TS) against nf-core/sarek 3.10.0 in its own member;
then merge `feat/m5-nfcore` to `main`.*

Plan `docs/plans/2026-09-29-nfcore-milestone-5.md`, from the map
`../.scratch/post-gate/roadmap.md` ("Milestone 5"); each ticket's `## Answer`
holds the reasoning, and the execution ledger is
`.superpowers/sdd/2026-09-29-nfcore-milestone-5/progress.md`.

1. Lineage comes from workflow outputs only: a `publishDir` publish into
   `cas://` still gets a block and a Publish Coordinate — the provider
   cannot tell a `publishDir` write from an output-DSL one — but no Output
   Collection refers to it (§11).
   [19](../.scratch/post-gate/issues/19-publishdir-publishes-in-the-closure.md).
2. This is loud, not silent: `onProcessCreate` warns once per process whose
   config declares `publishDir` while `outputDir` is `cas://`, and
   `onFlowComplete` counts every file stored in `cas://` this run, with or
   without a publish event, that joined no item or index Leaf
   in a new `RunCompletion.anomalies.unjoined`, warning once with the count,
   the first few coordinates and a pointer to the output DSL (§11, §6).
   [19](../.scratch/post-gate/issues/19-publishdir-publishes-in-the-closure.md).
3. `index {}` does not null the workflow output event's value at 26.04.6:
   `PublishOp.onComplete` (`PublishOp.groovy:219-229`) sends the published
   value and the index path together. A null value is instead a value
   channel that emitted nothing, and `Join` builds an empty collection for
   it rather than an `unaddressed` anomaly (§11).
   [26](../.scratch/post-gate/issues/26-workflow-outputs-with-an-index-block.md).
4. `OutputCollection` gains an optional `index` field, an `OutputIndex`
   (a `Leaf` and its publish path), addressed only from a publish this run
   itself made, never an existing Pointer File. Never written (a CSV
   `header: true` on a tuple channel, say), it reads `never_published`;
   written after the `RunCompletion` (seen only on an aborted run), it stays
   unreferenced. The explorer offers it as a download from the collection
   page (§6, §11, §15).
   [26](../.scratch/post-gate/issues/26-workflow-outputs-with-an-index-block.md).
5. Found by the Gate, not the original plan (Task 6a): Nextflow's
   `CsvWriter` (v26.04.6) writes a CSV Output Index File as one delete and
   several appends, and `cas://`'s `newOutputStream` did not honour
   `APPEND` — each append replaced the coordinate, so a CSV index was
   stored as one byte. Fixed with a per-coordinate spool file, finalised
   once, that `CasSession` holds (§8, decision 1 below).
6. Gate tier one gains `gate/outputs` (a JSON-indexed tuple output and a CSV
   `header: true`-indexed record output, both joining with Meta Maps, and a
   `LEGACY` process publishing through `publishDir` alone) and
   `gate/outputs-badindex` (a CSV index Nextflow's `CsvWriter` throws
   writing, on a tuple channel, while the run still exits 0), each in their
   own store; assertions 14-16 (§14, `gate/README.md`).
   [19](../.scratch/post-gate/issues/19-publishdir-publishes-in-the-closure.md),
   [26](../.scratch/post-gate/issues/26-workflow-outputs-with-an-index-block.md).
7. Tier two gains nf-core/sarek 3.10.0's test profile
   (`gate/tier2/tier2.sh`, `gate/tier2/sarek.config`, `gate/tier2/gatk4-quay.config`),
   the longest run in the tier (about 15 min), started first and in the
   background into its own member. Its check (TS) verifies the run
   succeeded, `multiqc`'s items carry a Meta Map and independently
   re-hashed, addressed Leaves, the `index` Leaf hashes independently to
   the member's block that `coords/multiqc/index.json` names, and
   `anomalies.unjoined` matches
   the `coords/` pointers no item or index Leaf claims; the log names the
   `publishDir` warning (`gate/tier2/README.md`).
   [18](../.scratch/post-gate/issues/18-measure-sarek-on-the-queue.md).

### Decisions made where the tickets are silent, as built

1. Task 6a: `cas://`'s `newOutputStream` honours `APPEND` through a
   per-coordinate spool file `CasSession` holds under `cas.tmpDir`, keyed by
   the join key. Without `APPEND` the spool starts empty, replacing a
   pending one; with it, the write continues the pending spool or seeds one
   by streaming the coordinate's current block. A pending spool is
   finalised exactly once, into one block, one Pointer File write and one
   `recordPublish`, when the provider next resolves the coordinate or a
   directory above it, when `onFilePublish` names it, or at the join (every
   spool still pending, `finalizeAllPending`). A spool still open at the
   join races a failing run's own publish thread; it is left out of the
   `RunCompletion` with a console warning naming the coordinate rather than
   aborting the whole `RunCompletion` over one file, since a failed run has
   no publish barrier (§8). A finalise-per-close design for appends was
   rejected: it would store one orphan block and one write per append (two
   per CSV row) and re-hash O(n²) bytes, where the deferred spool stores
   exactly one block. *Narrowed by the final review:* only `APPEND` streams
   are deferred. A stream without `APPEND` is stored at close, and once the
   join has run every close is, so a write after the join (an observer
   after nf-blocks, such as nf-prov) or before a head node dies keeps its
   Pointer File. Cost if wrong: a CSV built by appends after the join
   stores its intermediate blocks (orphans, sweepable).
2. Gate assertion 15 reads `store-outputs`'s own `coords/tuples/index.csv`
   directly rather than trusting only the plugin-reported leaf address, so
   a plugin bug that left a stray coordinate on disk while still marking
   the leaf `never_published` would fail it (Task 6 review round 1).
3. Tier two's TS check verifies every Leaf independently: it fetches and
   re-hashes the member's own block at the claimed address, cross-checked
   against the standalone `coords/` pointer, rather than trusting the
   `RunCompletion`'s recorded address field alone. The Output Index File is
   looked up the same way, against the member, not a task work directory,
   since `PublishOp` writes it from the head node straight to the output
   directory, never as a task's own output (Task 7 review round 1, fixing a
   Critical finding that would have failed TS on every real run).
4. Adding nf-blocks to a pipeline you don't own with a `-c` config file
   replaces that pipeline's `plugins { }` block rather than adding to it:
   measured against nf-core/sarek 3.10.0, whose `nf-schema@2.7.2` pin was
   silently replaced by that day's `nf-schema 3.0.0` release, and the run
   failed until the pipeline's pins were repeated beside nf-blocks's own.
   Documented in README (ticket 18 finding, for Rob).
