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
   `project` wait for their milestones. *Landed 2026-09-30 (milestone 6):*
   `sweep`, `prune` and `untrash` land with Gate assertion 9 (§14, §15, §19);
   `bundle`, `merge`, `verify` and `project` still wait. *Landed 2026-10-09
   (§20):* `reindex`, which rebuilds the cache index from the store and
   writes no block, entry or Claim. `sweep` and `prune`
   are dry runs unless `--apply true`; `untrash` acts on its first call.
   `put` stays the only way to write a pin or a `retain` Claim by hand:
   `sweep` writes Trash ledgers and index rows, and `prune --apply` writes
   only the one fixed-shape `set retain "lineage"` Claim, through the same
   `Put` builder `put` uses.
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
    *Since milestone 6:* retention, too — `RetentionStorage`
    (`LocalRetentionStorage` the local implementation), `RetainedStore`,
    `SweepLock`, `LiveRegistry`, `LiveWriter`, `Mark`, `Sweep`, `SweepPolicy`,
    `SweepReport`, `TrashLedger`, `Prune`, `PruneDecision`, `ClaimState`'s
    `retain` and `pin` groups (§19).
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
    `S3SnapshotStorage`, and, since milestone 6, `S3RetentionStorage`: the
    sweep lock, `live/` registrations and Trash ledgers as S3 objects, §19).
    The only package that imports `software.amazon.awssdk.*` or
    `nextflow.cloud.aws.*`.
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
    asserted_by = 'gate'       // optional; overrides every other source. Unset: Seqera Platform, NXF_BLOCKS_USER, git user.email, then 'anonymous', resolved on first use (docs/identity.md)
    index { path = null }      // optional override of the SQLite cache path (the Gate sets XDG_CACHE_HOME instead)
    snapshot { maxBytes = 64.MB }   // optional; a run writes the Index Snapshot only while it is under this (§15)
    tmpDir = null              // optional; scratch for S3 uploads of unknown length, default java.io.tmpdir
    nodeHash = null            // optional; node-side hashing, default fusion.enabled
    sweep {
        ageFloor = '14d'       // optional; how old a dead block must be before sweep trashes it, default 14d (§19)
        grace = '14d'          // optional; how long a trashed block stays in Trash before deletion, default 14d (§19)
    }
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
- `cas.sweep.ageFloor` and `cas.sweep.grace` (each a `Duration` string such as
  `'14d'` or `'12h'`; default 14 days each, §19 plan decision 8): the age a
  dead block must reach before a sweep trashes it, and how long a trashed
  block then waits before a later sweep deletes it. `ageFloor` must be at
  least the Live Writer stale threshold (10 minutes, §19); `grace` may be 0.
  Either under a day warns once, on stderr, from the `sweep` verb.
- *Amended 2026-10-25 (explorer v0.3 P3, decision D1):*
  `cas.stores.<alias>.readOnly` (Boolean, default false). A value other than
  `true` or `false` is refused naming the store, and `readOnly = true` on the
  writable member (the alias in `lineage.store.location`) is refused at
  config time. A member with `readOnly = true` is never a copy target (§21):
  the copy verb (below) and the explorer never write to it.
  `CasConfig.readOnly(alias)` reads it. `robsyme.cas.CopyTargets.judge(config,
  alias)` judges each member alone and returns `{target, reason}`: `readOnly`
  gives the reason `readOnly is set for this store`; a local member whose
  directory (or, when it does not exist yet, its nearest existing ancestor)
  this process cannot write gives `this process can't write the directory`;
  an S3 member is always a target, since there is no cheap write probe and a
  denied write fails the copy itself. Excluding the source member is the
  request's business, not the judgement's.

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
sweep.lock              Since milestone 6 (§19): one file, held by a sweep for its whole run. Local:
                        create-exclusive to take, compare-then-move to heartbeat and to release.
live/<session>          A run's Live Writer registration, one file per session, heartbeated every 60 s,
                        deleted at run end; a sweep treats one with no heartbeat for 10 min as dead.
trash/<deadline>-<id>   A Trash ledger: the addresses one sweep trashed, JSON, plan decision 14. Blocks
                        named in a ledger stay in place and readable until a later sweep, past the
                        deadline and still dead in its own fresh mark, deletes them.
store-id                Since 2026-10-09 (§20): one line, 32 hex characters, the id of this
                        incarnation of the member. Created once, never rewritten, not a block.
```

Nothing under `sweep.lock`, `live/` or `trash/` is a block: they are not
content-addressed, not swept themselves, and a dry run writes none of them.

*Amended 2026-10-25 (explorer v0.3 P3):* the Store Log kinds are `run`,
`selection`, `claim`, `forget` (§20) and `transfer`
(`StoreLogKind.TRANSFER`): `log/<rts>-transfer-<cid>`, the cid of a Transfer
block (§6). A copy writes it into its destination member last, after every
block and every ordinary entry it made (§21), so a `transfer` entry never
names content the member lacks. A build older than P3 warns once per entry
that its kind is unknown and ignores it (§19 has what that means for its
sweep).

*Amended 2026-10-25 (explorer v0.3 P4, §22):* two more kinds, `project`
(`log/<rts>-project-<cid>`, a Project block) and `handoff`
(`log/<rts>-handoff-<cid>`, a Handoff block). Put writes the first, `POST
/api/handoff` the second, each last, after the blocks it names. A build
older than P4 ignores both, so its sweep trashes Project and Handoff blocks
(§22). `StoreLog.list()` returns the entries this build reads and, apart,
the names of well-formed entries whose kind it does not know; a sweep
refuses to apply beside any (§19, §22). The plugin reads `nf/` only
through `LineageRecords` (`LocalLineageRecords` over `<member>/nf/<key>/.data.json`,
`S3LineageRecords` over `<prefix>nf/<key>/.data.json`), which never writes.

`CompositeStore(List<BlockStore> members)`: reads resolve in order, first hit
wins; writes go only to `members[0]`, which must be writable; a write is never
skipped because a read-only member holds the block.

*Amended 2026-09-28 (milestone 4):* `S3BlockStore(ops, prefix, alias,
writable, tmpDir)` keeps the local layout byte for byte under the member
prefix: `blocks/<xx>/<cid>`, `log/`, `coords/`, `nf/`, `index/v<N>.sqlite`,
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
a day and aborting incomplete multipart uploads after a day. Since milestone
6 the sweep role also needs, on the member prefix: `s3:DeleteObject` on
`blocks/*`, `log/*`, `live/*`, `trash/*` and `tmp/*` (a sweep deletes trashed
blocks, dangling Store Log entries, stale Live Writer registrations and old
scratch), plus `s3:AbortMultipartUpload` and `s3:ListBucketMultipartUploads`
(an abandoned multipart upload older than the age floor, README). The deny needs
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
  | Forget "Forget"
  | Transfer "Transfer"
  | InputSet "InputSet"
  | ItemLineage "ItemLineage"
  | Project "Project"
  | Handoff "Handoff"
} representation inline {
  discriminantKey "kind"
}
# Reserved kind, specified in the lineage spec, not yet built: Attestation.

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
  providers optional {String:[&Any]}  # schema 2 and 3: provider name -> every Leaf address of the run it supplied, sorted; absent at schema 1
  item_lineage optional nullable &ItemLineage  # schema 3: null when it could not be made; absent at schema 1 and 2
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

type Forget struct {
  schema Int
  asserted_by String
  deleted [&Any]                # sorted by cid string, no duplicates (§20)
}

# Explorer v0.3 P3 (§21): what one copy put into its destination member.
type Transfer struct {
  schema Int                    # 1
  asserted_by String            # the copying principal
  roots [&Any]                  # the roots asked for, sorted by cid string, no duplicates
  from String                   # source member alias
  to String                     # destination member alias
  depth String                  # "results" | "lineage" | "all"
  claims [String]               # the claim categories carried, sorted, no duplicates
  runs [&RunCompletion]         # every run whose records the copy carried, sorted by cid string, no duplicates (D8)
  blocks Int                    # blocks in the plan
  copied Int                    # blocks written (the rest were already there)
  bytes Int                     # bytes written
  timestamp String              # ISO-8601 UTC, when the copy finished
}

# Explorer v0.3 P4 (§22): the files a run read, linked from RunCompletion.input_set.
type InputSet struct {
  schema Int                    # 1 or 2; 2 adds Input.from
  asserted_by String
  inputs [Input]                # sorted by (role, name, address, sample, reason, size, from), nulls first; no two equal
}
type Input struct {
  role String                   # "samplesheet" (params.input), "param:<name>", "asset" (under projectDir) or "task"
  name nullable String          # the file's base name, never a path
  address nullable &Any         # the content's raw cid, computed for a local file even when the store does not keep the bytes
  size nullable Int
  sample nullable String        # meta.id of the task input tuple that staged it
  reason nullable String        # why address is null: "unaddressed", "unreadable", "remote", "unfinished"
  from optional [Origin]        # schema 2: the fromStore items that gave the task this file; absent when none
}
type Origin struct {
  item &OutputItem               # the upstream item fromStore emitted
  collection nullable &OutputCollection  # the output it was reached through; null when a Selection picked it by query
  selection nullable &Selection  # the Selection named in the fromStore call; null for run:/output:
}
# Origins are sorted by (item, collection, selection) as CID bytes, nulls first; no two equal.

# Cross-run item lineage: which inputs made which item, linked from RunCompletion.item_lineage.
type ItemLineage struct {
  schema Int                     # 1
  asserted_by String
  input_set &InputSet            # the InputSet whose positions `inputs` names
  items [Derived]                # sorted by item CID bytes; one per item with at least one input or one publishing task
}
type Derived struct {
  item &OutputItem               # an OutputItem of this run
  inputs [Int]                   # positions in input_set.inputs, ascending, no duplicates
  tasks [TaskRef]                # the tasks whose outputs this item's files are; sorted by hash; may be empty
}
type TaskRef struct {
  hash String                    # Nextflow task hash, 32 lowercase hex; lid://<hash> in Nextflow's lineage
  process String                 # fully qualified process name
  tag nullable String
}

# Explorer v0.3 P4 (§22): a project's root record. Its name and policies change by Claims.
type Project struct {
  schema Int                    # 1
  asserted_by String            # who created it
  name String                   # 1..256 characters, the name at creation
  description String            # may be empty, at most 4,096 characters
  created_at String             # ISO-8601 UTC with milliseconds, from the request
}

# Explorer v0.3 P4 (§22): one hand-off to a downstream pipeline, rooting the files it generated.
type Handoff struct {
  schema Int                    # 1
  asserted_by String
  template String               # e.g. "nf-core/differentialabundance"
  selection &Selection
  project nullable &Project
  files [Leaf]                  # the generated files, sorted by name; each reason is null
  params {String:String}        # the downstream params, scrubbed; a generated file as cas://<cid>/<name>
  timestamp String              # ISO-8601 UTC with milliseconds, from the request
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
as `null` in place of a path. *Amended 2026-10-24:* `Join` no longer writes
declined leaves. Nextflow's `WorkflowOutputEvent` carries no type (the output
declaration's type is dropped at compile time and `PublishOp.normalizeValue`
rebuilds records as plain maps), so a `null` cannot be told apart from a null
string, and on nf-core/rnaseq most were optional outputs or fields that did
not apply. A `null` now stays a `null` in the item: no leaf, no `paths` entry,
not counted in `anomalies.declined` (which new runs report as 0). Readers
treat a null field as an absent key (§12). An item with a null field
therefore has a different address than the same output stored before this
change. Declined leaves in items written earlier still decode, and
`fromStore` still restores them to `null`. A `never_published` leaf is a path in the item
that never received a publish event (a path outside the work dir).
*Amended 2026-09-30 (patch 0.3.0-beta.2):* that includes a path that is not
`cas://` at all, such as an https or local input FASTQ a record carries
(nf-core/rnaseq): `Join` makes it a `never_published` Leaf named by the
path's file name, puts `null` in its `paths` entry, gives it no join key, and
counts it in `anomalies.never_published`. Before this, `Coordinates.key`
threw on it and the whole RunCompletion was lost. A malformed `cas://` path
is still refused. Recording such inputs by address is milestone 7
(input-side lineage).
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

*Amended 2026-10-25 (explorer v0.3 P3, plan section 1.6):* the scrub has
three more rules, found by the audit below and built test-first
(`ScrubTest`, `RunManifestAuditTest`).

- Credential-shaped values are replaced by `[secret]` wherever they sit, in
  `params` strings and in the config text, before any other rule
  (`Leaks.redactCredentials`): an AWS access key id (`AKIA` or `ASIA` and 16
  upper-case letters or digits), a GitHub token (`ghp_`, `gho_`, `ghu_`,
  `ghs_` or `ghr_` and at least 20 letters or digits, or `github_pat_` and
  at least 20 letters, digits or underscores), a Seqera Platform token (`eyJ0aWQi`, base64 of `{"tid"`,
  and at least 6 more characters), and a whole PEM private key block
  (`-----BEGIN ... PRIVATE KEY-----` to its `-----END ... PRIVATE KEY-----`).
  Before this, such a value under a key naming no secret word survived into
  the block.
- In `params` (at any depth), a value under a secret-named key (the
  `isSecretKey` rule the config text uses: a key containing `key`, `secret`,
  `token`, `password`, `passwd` or `credential`, or with a segment `pat`)
  becomes `[secret]` when it is a non-empty string, a number or a `Path`. An
  empty string, a boolean and null are left as they are, and a list or map
  under such a key is scrubbed by its contents only. As in the config text
  the word match is a substring, so a value under `keyfile` or
  `public_key_id` goes too.
- A `params` string that is wholly an absolute path or a URI in a scheme
  other than `lid://` or `cas://` (once credentials are redacted) is still
  replaced whole, since it may hold spaces. Any other string is scrubbed
  token by token exactly as the config text is (`scrubText`), so
  `--ref /data/g.fa` becomes `--ref [redacted-location]` and
  `-v /home/<user>:/data` has both paths redacted. Before this a string was
  judged whole, so a path inside a longer string stayed.

So the RunManifest of a new run gets a different address from the same run
recorded before this change wherever its `params` or config held such a
value. Blocks already written are not rewritten. *Amends the "Not redacted"
sentence above:* not redacted are a host name, a user name other than the
OS user's, a relative path, and a secret of no recognised shape under a key
that names none of the secret words (in `params`, also one inside a list or
map under a secret-named key).

*Amended 2026-10-25 (P3 final fix wave, item 10; `Records.scrubSecret`):*
in `params`, each string, number or `Path` element of a list or array under
a secret-named key becomes `[secret]` too, in nested lists as well, so
`params.tokens = ['abc']` is recorded as `['[secret]']`. A map inside such a
list keeps the map rule: its own keys decide. This replaces "a list ...
under such a key is scrubbed by its contents only" and the list case of the
"Not redacted" sentence above. Open gap: a list in the config TEXT under a
secret-named key (`tokens = ['a']`) is not redacted. `scrubConfigText`
redacts the quoted string or bare word right after the separator, and a `[`
is neither, so the elements stay; the audit's text rule uses the same
pattern and does not flag them either.

`RunManifestAudit.audit(Cid, Map manifestBlock)` (in `robsyme.cas.core`,
pure, no I/O) reads a RunManifest block as stored and returns
`List<Finding>`, each `{cid, kind: 'RunManifest', where, what}`
(`Finding.toMap()` for JSON). It walks `params` recursively and the
`config`: text line by line and token by token, or, for a config Map (a
block from before 2026-09-27), the way it walks `params`. `where` is the
dotted path (`params.outdir`, `params.inputs[2]`, `config.a.b`) or
`config:<line>` for text, where findings are kept once per line and `what`.
`what` is one of `absolute path`, `uri`, `os user`, `aws key id`,
`private key`, `github token`, `seqera token` and `secret assignment`. A
value under a secret-named key that is a number or a non-empty string other
than `[secret]` is a `secret assignment`, or a `seqera token` when the key
names a token and `tower` or `seqera`. In config text, secret assignments
are matched over the whole text (a value may follow its key on the next
line), PEM blocks are found whole and blanked before the lines are read,
and a PEM header without its footer is still a `private key`. The markers
`[redacted-location]`, `[redacted-user]` and `[secret]`, and `cas://` and
`lid://` references, are clean. The audit and the scrub share one
classifier, `robsyme.cas.core.Leaks` (the patterns, `classify`,
`scanToken`): the scrub maps a kind to its marker, the audit maps it to a
finding, so the two cannot drift. A RunManifest built through
`RunManifest.toCbor()` from params and config holding every shape above
audits clean (`RunManifestAuditTest`); the same values unscrubbed are each
found. Over the roblab store's 4 RunManifests the audit finds nothing
(`RoblabAuditTest`, opt-in by `-Droblab.store=`, over a copy of `blocks/`).
The copy verb (below) audits the RunManifest of every run it would carry
(§21). *Amended 2026-10-25 (fix wave item 10):* each element of a list
under a secret-named `params` key that is a number or a non-empty string
other than `[secret]` is a finding of its own, `where` `params.tokens[0]`,
`what` `secret assignment` (or `seqera token`); an empty value under a
secret-named key is no finding.

*Amended 2026-10-25 (P3 final review, item 3; `Leaks.scanToken`,
`Records.scrubConfigText`, `Records.secretListLiterals`):* the scrub and the
audit find four more shapes, in `params` strings and config text alike, and
both use the same code for each.

- A piece is split at its first `=`, `:`, `,`, `;` or `@` (was `=` or `:`),
  and each side judged again, so `a.csv,/home/x/b.csv` becomes
  `a.csv,[redacted-location]`, `--bind a,/scratch/x` keeps `--bind a,`, and
  `@/home/x/args.txt` becomes `@[redacted-location]`.
- A path glued to a short flag, a `-`, one letter or digit, then `/`
  (`-B/home/x`, `-I/usr/include`), has the path redacted and the flag kept.
- The OS user name is redacted where it stands as a word of its own inside
  a piece, bounded by characters other than letters, digits and `_`
  (`<user>@example.org` becomes `[redacted-user]@example.org`; `~<user>/data`
  is a home path and becomes `[redacted-location]`, see Paths below); a
  longer word that only contains the name is kept. Before this only a piece equal to the name went.
  A name directly after an `@` is a handle (`github:'@<user>'` in a
  pipeline's manifest contributors), so it is neither redacted nor flagged;
  a path after the `@` is still a path.
- In config text, each string or number in a list assigned to a
  secret-named key (`params.passwords = ['a', "b"]`, `process.secrets =
  ['x']`, nested lists and maps in it included, a map key excepted) becomes
  `'[secret]'`, keeping its quote; an empty string stays. The audit reports
  each such element that is not `[secret]` as a `secret assignment` (or
  `seqera token`) on its own line. This closes the open gap above.

Still not found, by either: a Windows path (`C:\Users\x`), and tokens of
shapes §1.6 does not list (`Bearer ...`, Slack `xoxb-...`, GitLab
`glpat-...`), and a secret under a key with none of the secret words
(`env.API_AUTH`). The final review's probe printed these; they were not in
its fix list. *Replaced by the F2 amendment below, which finds all of them.*

*Amended 2026-10-25 (explorer v0.3 P3 follow-up, F2, its fix rounds 1 to 5,
and F6; `Leaks.RULES`, `Leaks.keyNames`, `Records.ASSIGNMENT`,
`Records.secretAssignments`):* the scrub and the audit share one ordered
list of secret rules, `Leaks.RULES`. The scrub applies it with
`Leaks.redactCredentials`, the audit with `Leaks.maskCredentials`, which is
the same code except that each marker keeps the line breaks of what it
replaced and each secret is recorded as a hit, so the audit finds exactly
what the scrub removes. A secret becomes `[secret]`, in the quotes it had;
a rule that names its secret (a flag, a property, a key) keeps the name.
The rules apply to `params` strings and to the config text alike (the config
text reaches them through `scrubText`), in this order:

- A whole PEM private key block, as before.
- A `--<flag>` and its value (`Leaks.SECRET_FLAG`). With the value after
  `=`, the flag counts when its name is secret-named (`isSecretKey`,
  below): `--api-key=abc`. With the value in the next token (after spaces
  or tabs), it counts only when the name's last segment is a secret word:
  `key`, `secret`, `token`, `password`, `passwd`, `credential`,
  `credentials`, `pat` or `auth`. So `--password hunter2` goes and
  `--sort-keys in.json` stays. A last `auth` right after `skip`, `no`,
  `disable`, `disabled` or `without` is a switch, so `--skip-auth x.bam`
  stays. The value is a single- or double-quoted string, which keeps its
  quotes, or a run of non-space characters. A value that starts with `-`
  (another flag), `$` (a variable) or `[` (a marker), quoted or not, is no
  value. A secret flag takes `Bearer <token>` or `Basic <token>` whole,
  whatever the token (`--auth Basic Statistics`), quoted or not. A `Bearer`
  or `Basic` with no token after it (what follows is a flag, a variable or
  a marker) is the value on its own, so `--proxy-auth basic --threads 4`
  keeps `--threads 4`. A flag that does not count is searched again from
  the start of its value, so `--extra "foo --password x"` loses `x`.
- A Java system property, `-D<name>=<value>` (`Leaks.SYSTEM_PROPERTY`),
  when the name is secret-named. A quoted value keeps its quotes. A bare
  value runs to a space or a quote and takes commas, stopping only before
  `,-D`: `-Dfoo.password=hunter,2` goes whole, and
  `-Dfoo.password=a,-Dbar.token=b` is two properties. A value that starts
  with `[` or `$` is no value. A property whose name is not secret-named is
  searched again from the start of its value, so a secret inside it is
  still found (`-Ddb.props=user=a,password=x` loses `x` by the next rule).
- A bare `name=value` inside a string (`Leaks.BARE_SECRET`), as a params
  string or a quoted config value holds one: `user=bob password=hunter2`, a
  libpq `host=h password=x`, `-Dx=a,token=abc`. The name's last segment
  must be one of the secret words above, with the same negations, so
  `SLIDINGWINDOW:4:15`, `ktrim=r` and `max_tokens=10` stay. A name right
  after a word character, `.`, `-`, `/`, `$` or a marker's `]` is part of
  something else (a flag, a property, a path, a variable). A value that
  starts like a flag, a variable, a marker, a path or a URI is left to its
  own rule. A bare value takes a `,` or `;` unless a space, a quote, the
  end or another `name=` follows it, and a closing bracket only inside it
  (`(token=x)` keeps its `)`). This rule runs last of all, so a token shape
  in the value (`password=AKIA...`) is reported by its shape.
- A quoted secret-named key, a `:` and its value, as JSON writes one
  (`Leaks.JSON_SECRET`): `{"token": "abc123"}`. `true`, `false` and `null`
  stay.
- The credential shapes, wherever they stand:
  - an AWS access key id: `AKIA` or `ASIA` and 16 upper-case letters or
    digits;
  - a GitHub token: `ghp_`, `gho_`, `ghu_`, `ghs_` or `ghr_` and 20 or more
    letters or digits, or `github_pat_` and 20 or more letters, digits or
    underscores;
  - a Seqera Platform token: `eyJ0aWQi` and 6 or more characters;
  - `Bearer <token>`, in any case. The token has 8 or more characters and
    holds a digit, `+`, `/` or `=`, or an upper-case letter after its
    first character, so prose (`the bearer instruments`) stays. The word
    `Bearer` stays;
  - `Basic <value>`, in any case, when the value (4 or more base64
    characters, padding optional) decodes to RFC 7617 user-pass
    (`Leaks.isUserPass`): well-formed UTF-8 (a strict decode), no control
    character, and a `:` with at least one character before it. So
    `Basic QualityControl`, `Basic Statistics` and `Basic Overview` stay
    (a capitalised word whose bytes start 0x80 or more fails the strict
    decode), and a non-ASCII credential (`jörg:pässwörd`) goes. The word
    `Basic` stays;
  - a JWT: `eyJ` and three dot-separated base64url segments;
  - Slack `xoxa-`, `xoxb-`, `xoxp-` or `xoxr-` and 10 or more characters;
    GitLab `glpat-` and 20 or more; Hugging Face `hf_` and 30 or more
    letters or digits;
  - the value of an Azure SAS `sig=` after a `?` or `&`; `sig=` stays;
  - the AWS secret-key shape, last of the shapes: exactly 40 characters of
    `[A-Za-z0-9/+=]` with an upper-case letter, a lower-case letter, and a
    digit, `/` or `+`, standing alone. A hex digest (a git SHA, md5, sha1)
    has one case and a CID is lower case, so both stay; a run that starts
    with `/` is a path and is judged as one.

`isSecretKey` moved to `Leaks` (`Records.isSecretKey` calls it) and counts
`auth` as a whole segment, as `pat` is: `env.API_AUTH` and `basicAuth` are
secret-named, `author` and `env.AUTHOR` are not.

*Amended 2026-10-25 (explorer v0.3 P4, L1; `Leaks.isSecretKey`,
`Leaks.endsInSecretWord`, `Leaks.BARE_SECRET`, `Leaks.withoutCredentials`):*
secret names are matched by segment, not substring. A name splits at any
run of characters other than letters and digits and at a camelCase
boundary. A segment names a secret when it is one of `key`, `secret`,
`token`, `password`, `passwd`, `passphrase`, `pass`, `pwd`, `credential`,
`credentials`, `pat`, `auth`, `authorization`; in a map or config key only,
a plural (`keys`, `tokens`, `secrets`, `passwords`, `passphrases`); one
lower-case word ending in `token`, `secret`, `password`, `passwd`,
`passphrase` or `credential(s)` (`accesstoken`, `dbpassword`); or `key`
after a known prefix (`api`, `access`, `secret`, `private`, `ssh`,
`account`, `master`, `client`, `auth`, `encryption`, `signing`, `license`,
`storage`, `shared`, `sas`). It does not count right after a qualifier
(`skip`/`no`/`disable` before `auth`; `sort`, `keep`, `max`, `num`, `join`,
`primary` and the like before `key` or `token`; `low`, `high`, `two`,
`multi` and the like before `pass`) or right before a reference word
(`file`, `path`, `dir`, `filename`, ...). So `api_key`, `apiKey`,
`ssh-key`, `DB_PASS`, `MYSQL_PWD` are secret-named, and `monkey_reads`,
`keystone`, `turkey`, `keyword`, `bypass_qc`, `passage`, `keep_tokens`,
`max_tokens`, `sort_keys`, `keyfile` and `password_file_name` are not; a
`*_file` value is judged as any value, so an absolute path still goes as a
location (`--secret-file=/x` is now `[redacted-location]`, was `[secret]`).
`--passphrase`, `--pass` and `--pwd` lose their value.

A bare `name: value` in a string is judged as `name=value` is, by the
name's last segment; a `Bearer`, `Basic`, `token` or `Digest` scheme word
after the colon stays, matched all or nothing. So `password: hunter2`,
`-pass pass:hunter2`, `Authorization: token abc`, `-H 'x-api-key: abc'` and
`-H "X-Auth-Token: abc"` lose their value, while `SLIDINGWINDOW:4:15`,
`sort_key: asc` and `12:30` stay. Three more credential shapes, before the
AWS secret-key shape: a Google API key (`AIza` and 35 of
`[0-9A-Za-z_-]`), an Anthropic key (`sk-ant-` and 20 or more), an OpenAI
key (`sk-proj-`, `sk-svcacct-`, `sk-admin-` and 20 or more, or `sk-` and 32
or more letters and digits holding both), each its own audit kind.
Accepted over-redaction: the colon form reads prose in error text too
(`Missing key: sample` loses `sample`); `--pass 2` loses `2`. Still open:
`mysql -p x` and `-px`, a bare base64 `user:pass`, an AWS session token,
and one-word compounds such as `PGPASS` (closed by R2, below). A RunManifest a new build writes
from values newly redacted, or newly kept, gets a new address; blocks
already written are not rewritten. Over roblab's 4 RunManifests the audit
still finds nothing.

*Amended 2026-10-25 (explorer v0.3 P4 follow-up, R2; `Leaks.PASS_COMPOUND`,
`Leaks.PASS_VERDICT`, `Leaks.SSHPASS`, `Leaks.COOKIE`, `Leaks.NUCLEOTIDES`):*
three more shapes go and four false positives stay, in the scrub and the
audit alike.

- A segment that is one lower-case word made of a database or login tool's
  name and `pass` or `pwd` names a secret; the tool names are `pg`,
  `mysql`, `ssh`, `db`, `sql`, `mongo`, `redis`, `ldap`, `smb`, `ftp`,
  `sftp`, `rsync`, `admin`, `root`. So `PGPASS`, `SSHPASS`, `MYSQLPWD` and
  `DBPASS` are secret-named; `compass`, `bypass`, `overpass` and `oldpwd`
  are not.
- `pass` does not count right before or after a QC or data word
  (`filter(s|ed)`, `only`, `fail(ed)`, `qc`, `read(s)`, `fastq`, `fq`,
  `bam`, `cram`, `sam`, `vcf`, `bcf`, `pod5`, `fast5`): MinKNOW's
  `fastq_pass`, `bam_pass`, `pod5_pass`, and `pass_filter` and
  `vcf_pass_only` keep their values.
- A flag, a bare `name=value` or `name: value`, or a JSON key whose only
  secret word is `pat` does not count when its unquoted value is a word of
  upper-case IUPAC nucleotide codes or of lower-case `acgtun`: `--pat GATC`
  stays, `--pat abc123` and a token shape go. A `pat` map or config key
  still loses any value.
- The value of `sshpass -p` (after spaces or glued, `-v` flags allowed
  before it) becomes `[secret]`; a `-p` of any other command stays.
- The `name=value` pairs of a `Cookie` or `Set-Cookie` header, any case,
  become `[secret]` together, up to a quote, a line end, or a space not
  after a `;`; a header value that does not start with `name=` stays. Both
  new rules are `secret assignment` findings and run after `SECRET_FLAG`,
  before the token shapes.
- Kept as they are: `id_key` is secret-named; `Missing key: sample` loses
  `sample`. Still open: `mysql -p x`, `-px`, a bare base64 `user:pass`, an
  AWS session token, `curl --cookie`/`-b` values.
- The integrator's probe 3 (62 rows) is a test table beside probe 2. Over
  roblab's RunManifests, audited on a copy, the audit finds nothing.

*Amended 2026-10-25 (explorer v0.3 P4, second follow-up, B1;
`Leaks.PASS_DATA`, `Leaks.PASS_VERDICT`, `Leaks.keepsValue`,
`Leaks.CURL_COOKIE`):* secrets beat false positives.

- A filter, only or read word (`filter(s|ed)`, `only`, `read(s)`) makes
  `pass` a QC verdict only right after it (`pass_filter`, `vcf_pass_only`,
  `pass_reads`). Before it the word qualifies nothing, so `DB_READ_PASS`,
  `READ_ONLY_PASS`, `db_read_pass` and `FILTER_PASS` are secret-named again
  (R2 had let them leak). The data words (`fail(ed)`, `qc`, `fastq`, `fq`,
  `bam`, `cram`, `sam`, `vcf`, `bcf`, `pod5`, `fast5`) still count on
  either side.
- One predicate, `Leaks.keepsValue(name, value)`, decides in the scrub and
  the audit alike when a value under a secret name is no secret, for flags,
  bare `name=value`, JSON keys, `-D` properties, params and config keys and
  each element of a list under one. Such a value is judged as any other. It
  is one of: a plain boolean or number (`true`, `False`, `0.05`, `-1`,
  `1e-5`, a whole number of at most three digits: a longer one can be a PIN
  or a numeric password, so `password=12345678` and `apiKey: 12345` still
  go, while `--pass 2` and `filter_pass = true` stay); one of the scrub's
  own markers; a location the path rule replaces whole (`PWD=/home/x`
  becomes `[redacted-location]`, reported as a location; `/x:hunter2` is
  not whole and becomes `[secret]`); or, under a name that is `pat` alone
  after its last `.`, a nucleotide word. The R2 sentences "A `pat` map or
  config key still loses any value" and "`--pass 2` loses `2`" no longer
  hold; a longer name ending in `pat` (`GITHUB_PAT`, `adapter-pat`) loses any
  value.
- `sshpass` allows a `-P <prompt>` option before `-p`. A `Cookie` or
  `Set-Cookie` header may be a quoted JSON key with a quoted value. curl's
  `-b <value>`, `--cookie <value>` and `--cookie=<value>` lose the value
  when it opens with `name=`; a cookie file (`-b cookies.txt`) and another
  tool's `-b b.bed` stay.
- The reviewer's probe 4 is the test table `ScrubProbe4Test` (100 rows).
  Still open: `mysql -p x`, a bare base64 `user:pass`, an AWS session
  token, and `password=/abc:hunter2` in a params string (the path rule
  splits at the `:`).

Paths. `Leaks.locationKind` also takes as an absolute path a home path
(`~/x`, `~user/x`), a Windows drive path (`C:\x`) and a UNC path
(`\\host\share`, host and share 2 or more characters each, so regex escapes
such as `\\d\\d` and `\\bword\\b` stay), with the doubled backslashes config
text writes. It applies to values and to map keys. `~<user>/data` is now a
home path and becomes `[redacted-location]` (it was
`~[redacted-user]/data`). A short flag glued to a path is a letter only
(`-[A-Za-z]/`), so `-1/3` stays.

Map keys (`Leaks.keyNames`, `Leaks.keyMarker`). A map key the scrub would
change, in `params` at any depth and in a config Map, is written under the
marker for what it held: `[secret]` for a credential, else
`[redacted-location]` for a path or a URI, else `[redacted-user]` for the
OS user name. A second key with the same marker becomes `<marker>-2`, then
`-3`, skipping any key the map keeps, so no entry is lost. A value under a
key renamed to `[secret]` is a secret too, so a second scrub changes
nothing. The audit judges each key as it judges a string, and names a
finding by the key the scrub would write
(`params.files.[redacted-location]`).

Config text (`Records.ASSIGNMENT`). The assignment rule above (`key =
value`, `key: value`, the key secret-named) reads its value so:

- a quoted value may hold backslash escapes;
- under a key that follows a single `-` (a `-D` property inside a
  `'''`/`"""` string), a bare value takes the `-D` form above, to a space
  or a quote and stopping only before `,-D`, when the `=` follows a key of
  256 characters or fewer;
- any other bare value keeps its commas except before a space, a quote, a
  bracket, the end, `-D`, or another key and separator (punctuation between
  them allowed), so `password=hunter,2` goes whole and `a=1,password=x`
  keeps `a=1`. A `;` ends it only before a key and separator, the same way;
- after its first character, a `(` or `{` ends a bare value when a key and
  separator follow it, so a call's named argument (`x = f(token: 'y')`) is
  matched on its own, and a whole `(...)` or `{...}` with no key at its
  start is part of the value (`password=p(a)ss`). The first character is
  never refused, so `password = (a:b)` goes whole. A URI's scheme is not a
  key, so `token=(http://u:p@h/x)` keeps its value whole;
- a key right after `--` is a flag and goes to the flag rule;
- every form is possessive, so a value of any length is matched without
  backtracking or recursion.

`Records.secretAssignments` yields the matches whose key is secret-named.
Under a key that is not, it searches again from the start of a `-D` value
(`-Dx=a,token=abc` yields `token=abc`) and from just inside a bare value
that opens with `(` or `{` (`x = (token=abc)` yields `token=abc`).

One pass (`Leaks.scanPiece`). A piece is judged from its start, then from
each place it splits (`=`, `:`, `,`, `;`, `@`), left to right in one loop,
and the credential rules run once over the whole piece. Before this, each
split recursed and classified the rest again, so a 50 KB comma-separated
list took 20 to 40 s and `a,` 12,000 times overflowed the stack in the
scrub; both now take under a second, with the same output (a 1.5M-token
differential fuzz found no difference).

The audit's `what` adds `aws secret key`, `bearer token`, `basic auth`,
`jwt`, `slack token`, `gitlab token`, `hugging face token` and `azure sas
signature`; a secret flag, property, JSON key or bare `name=value` is a
`secret assignment` (`seqera token` when its name names a token and
`tower` or `seqera`).

Kept, as tests that the scrub leaves them unchanged and the audit finds
nothing, through `RunManifest.toCbor()` and a DAG-CBOR round trip
(`ScrubTest`, `RunManifestAuditTest`): an ordinary sample name; a relative
path; `-1/3`; an email that is not the OS user's (`someone@example.org`);
an IP address before a path (the path goes, the IP stays);
`--account=projX`; a git SHA in either case; an md5; `cas://` and `lid://`
references; `bafy`/`bafk` CIDs alone and in `cas://<cid>/name`;
`sha256:<hex>` alone and as `image@sha256:<hex>`; a 40-character lower-case
string; 40 letters of mixed case with no digit, `/` or `+`; `author` and
`env.AUTHOR`; prose holding `bearer` and `hf_`; `~1.2`, `a~b/c`, `\d+` and
`x:1/3`; `Basic QualityControl` and `Basic Overview`.

Over-redaction accepted by ruling (the value is lost from the RunManifest;
nothing leaks):

- an R formula such as `~genotype/treatment` reads as a home path;
- a dot-free relative path of exactly 40 characters, mixed case, with a
  `/` matches the AWS secret-key shape;
- a `--<x>-auth` flag followed by a positional takes the positional as its
  value (`--proxy-auth sample.bam`);
- a `-D` property's bare value runs to `,-D`, so
  `-Dfoo.password=abc,user=bob` loses `,user=bob` too;
- `--key 2` (GNU `sort`) and `--token-limit=100` read as secret flags;
- a regex with a doubled-backslash run, such as `\\bSRR\\d+` in config
  text, can still read as a UNC path;
- config text's key rule is a substring match, so `skip_auth == false` in
  a closure and some `;`-joined config statements lose a value.

The address. A RunManifest a new build writes from params or config that
held any shape above gets a different address from the same run recorded
before; blocks already written are not rewritten. Over the roblab store's
4 RunManifests the audit still finds nothing, and scrubbing them again
changes nothing.

*Amends the "Not redacted" sentences above.* Not redacted, and open:

- a host name, a user name other than the OS user's, a relative path, and a
  secret of no recognised shape under a name that names no secret word;
- a JSON value with an escaped quote (`{"token": "ab\"cd"}`) keeps what
  follows the escape;
- a SAS `sig=` at the very start of a string, with no `?` or `&` before it;
- a bare secret value that holds `,<key>=` keeps that tail
  (`password=abc,user=x` reads as two entries), and in config text one
  that runs into a call's `(` keeps what follows (`password=x(a=1)` keeps
  `(a=1)`);
- a `-D` key longer than 256 characters, or `-Dx.password = v` with spaces,
  in config text, gets the ordinary bare form, which stops at a comma
  before a key;
- a path glued after a marker and its quote (`'[secret]'/home/x`) is not
  split off and stays.

Also open: on a few adversarial junk strings found by fuzzing (none of
them holding a secret), a second scrub still changes the text, so "both
passes are idempotent" holds for the configs we have seen, not for every
string; and some config lines scrub in
quadratic time: a 50 KB line of chained `-D` properties takes about 3.4 s,
an `x=-D` line about 4.5 s per pass, and an `a-a-a...` line 12 to 26 s.

### RunCompletion
```
{ kind: "RunCompletion", schema: 3, asserted_by: string,
  run: Cid,                             // RunManifest
  collections: [Cid, ...],              // OutputCollection links, sorted by output name
  input_set: Cid|null,                  // null in the skeleton
  status: "succeeded"|"failed",
  exit_status: int|null,
  possibly_incomplete: bool,            // true for every failed run (no barrier exists on that path)
  started_at: string, finished_at: string,
  anomalies: { unresolvable: int, unaddressed: int, declined: int, never_published: int, unjoined: int },  // unjoined added 2026-09-29 (ticket 19); absent on an older block reads as 0
  error: string|null,
  providers: { <provider>: [Cid, ...] },    // schema 2: every Leaf address the run published (each file leaf and each directory leaf's manifest) under the provider that supplied it; not the files inside a directory (final review I5)
  item_lineage: Cid|null }                  // schema 3: ItemLineage link; null when it could not be made; absent at schema 1 and 2
```

*Amended 2026-10-26 (cross-run item lineage, spec
`docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md`):*
RunCompletion is written at schema 3 and `RunCompletion.READABLE` is
`[1, 2, 3]`. `item_lineage` links the run's ItemLineage (below), written
just after the InputSet. It is null when the ItemLineage could not be made,
and there is never one without an InputSet: `input_set: null` implies
`item_lineage: null`. A run where no item qualifies still links an
ItemLineage, with empty `items`, so null keeps meaning "could not be made".
Schema 1 and 2 blocks have no `item_lineage` field and read as before.

### Claim, InputSet, Attestation
Specified in the spec; not built in the skeleton. Reserve the kind names.
*Amended 2026-10-25 (explorer v0.3 P4):* InputSet is built (§22). Every
RunCompletion a P4 or later build writes links one in `input_set`, an empty
one when the run read nothing, so `input_set: null` means a run recorded
before P4. Attestation stays reserved.

### Project, InputSet, Handoff
*Added 2026-10-25 (explorer v0.3 P4, `robsyme.cas.core.Project`,
`InputSet`, `Handoff`):* the shapes are in the schema above; the rules are
in §22. Each class's constructor refuses what its schema forbids: a Project
name that is blank or over 256 characters, a description over 4,096 or
null; an input with neither address nor reason, or both; a Handoff file
without a name or address, or named twice. InputSet sorts its inputs by
(role, name, address, sample, reason, size), nulls first, and drops
repeats; Handoff sorts its files by name and its params by key. The same
content built in any order is one block.

### InputSet schema 2, Origin, ItemLineage
*Amended 2026-10-26 (cross-run item lineage, spec
`docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md`
sections 5.1 and 5.2; the shapes are in the schema above):*

- InputSet is written at schema 2 and readers accept 1 and 2. `Input.from`
  lists the Origins of a file a task received through `fromStore`: the
  upstream OutputItem, the OutputCollection it was reached through (null
  when a Selection picked it by query) and the Selection the call named
  (null for `run:` and `output:`). It is absent at schema 1 and, at schema
  2, when the list would be empty. Origins are sorted by (item, collection,
  selection) as CID bytes, nulls first, with no two equal. The `Input` sort
  key gains `from` last, and "no two equal" covers it, so two directory
  inputs from two items holding identical directories stay two inputs that
  differ only in `from`. Two Origins on one input happen only when two
  emitted items held the same bytes.
- ItemLineage is a new kind, linked from `RunCompletion.item_lineage`.
  `items` is sorted by item CID bytes and holds one Derived per OutputItem
  of the run with at least one input or one publishing task; an item of
  values only is left out. `Derived.inputs` are positions in
  `input_set.inputs`, ascending, no duplicates, so an input with no address
  can still be named; the InputSet is sorted and content-addressed, so a
  position is stable for one InputSet CID. `Derived.tasks` are the tasks
  that published the item's files, sorted by hash, possibly empty.
  `TaskRef.hash` is the Nextflow task hash as 32 lowercase hex,
  `TaskRef.process` the fully qualified process name
  (`task.processor.name`), and `TaskRef.tag` goes through
  `Records.scrubText`. A cached task's reference is the original
  execution's hash, so the ItemLineage CID is the same on `-resume`.
- `inputs` is the closure at the run boundary: the walk passes through the
  run's intermediate tasks and records only run inputs. Same-run lineage
  stays with `TaskLineage` (§21).

### Transfer
*Added 2026-10-25 (explorer v0.3 P3, plan section 1.5 and decision D8;
`robsyme.cas.core.Transfer`):*
```
{ kind: "Transfer", schema: 1, asserted_by: string,   // the copying principal
  roots: [Cid, ...],             // the roots asked for, sorted by cid string, no duplicates, at least one
  from: string, to: string,      // source and destination member aliases
  depth: string,                 // "results" | "lineage" | "all"
  claims: [string, ...],         // the claim categories carried (names, pins, projects, annotations), sorted, no duplicates
  runs: [Cid, ...],              // every RunCompletion whose records the copy carried, sorted by cid string, no duplicates
  blocks: int,                   // blocks in the plan at that depth
  copied: int,                   // blocks written; the rest were already there
  bytes: int,                    // bytes written
  timestamp: string }            // ISO-8601 UTC with milliseconds, when the copy finished
```
One block per copy, written into the destination member by the copy verb
(below) after everything it names, then announced by a `transfer` Store Log
entry (§5, §21). The constructor sorts `roots`, `runs` and `claims` and drops
repeats, so the same copy built in any order is one block. It refuses an
empty `asserted_by`, `from`, `to`, `depth` or `timestamp`, no roots, a null
address in `roots` or `runs`, and a negative count; `runs` and `claims` may
be empty. `fromCbor` checks `kind`, `schema` 1 and the shape of every field,
and refuses a wrong one with `IllegalArgumentException`. `depth` and the
`claims` values are not checked on read, so a later depth or category stays
readable. *Amended 2026-10-25 (fix wave item 7):* with one exception: a
claim category that is null, blank or holds a `,` is refused, by the
constructor and so by `fromCbor` too, since the index keeps the categories comma-joined
(§12); catch-up warns once and skips such a block, as any unreadable
Transfer. The copied blocks keep their own `asserted_by` and `timestamp`
(they travel verbatim); only the Transfer carries the copier.

`from` and `to` are member aliases on purpose, a stated exception to "nothing
store-local" above. A Transfer records a move between two configured
members, and the explorer's "Copied here from" line names the source by
its alias. Dedup across copies is not a goal: the `timestamp` already makes
each copy a distinct block.

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

*Note for users, 2026-10-25 (explorer v0.3 P4, second follow-up):* with
Nextflow's default `workflow.output.overwrite = 'standard'`, every `-resume`
re-copies every published file into `cas://`, and the plugin hashes each
one again, cached tasks included: the provider reports a block's own mtime,
so no published file ever looks unchanged (`copyAttributes` does not help,
and `overwrite 'sha256'` reads the source bytes to compare). The cost is a
read and a hash of the run's published bytes per resume; nothing is stored
twice. `overwrite = false` avoids the re-copy, but then a task that re-runs
on resume never republishes its changed output, so a resumed run would
record the old files. The Gate's `gate/resume.config` sets it for its
`resumed` run only, where every task is cached by construction (§14); it is
not a recommendation for pipelines.

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

*Amended 2026-10-25 (explorer v0.3 P3 follow-up, F3 and F6;
`robsyme.cas.lineage.NfRecordScrub`):* "Everything else -> delegate
unchanged" no longer holds. A member's `nf/` is read by everyone who can
read the member, so every record `save` hands on except a `FileOutput`
first loses the launching machine's paths, home directory and user name.
Strings are judged by the RunManifest's rules (`Records.scrub`,
`Records.scrubText` and `Records.isSecretKey`, so `Leaks`, §6). Only
`nf-lineage` reads these records at 26.04.6.

`FileOutput` records are never scrubbed, so the `nf/` records in a shared
lineage store still carry host paths in `FileOutput.path`: the work-dir
path of every task output (`s3://bucket/work/...` on S3), and any path
outside the store. Nextflow needs them: `FileOutput.path` is the file a
`lid://` read opens and `nextflow lineage check` hashes, and
`channel.fromLineage` stages it. Measured on 26.04.6: with a work-dir path
redacted, `lineage check` exits 1 and a downstream `lid://` read fails with
"Target path [redacted-location] does not exist". A published
`FileOutput.path` is still rewritten to `cas://<cid>/...` as above.

Scrubbed, each field replaced on the record (values copied, never changed
in place, since a TaskRun's `val` inputs are the run's own objects):

- `WorkflowRun.metadata`: every string in it, so `homeDir`, `launchDir`,
  `workDir`, `projectDir`, `scriptFile` and `configFiles` become
  `[redacted-location]`, and the manifest's URLs go as non-portable URIs
  do in a RunManifest. `commandLine` is scrubbed token by token
  (`Records.scrubText`), so a command line that starts with the launcher's
  path keeps the rest of its shape. `userName` becomes `[redacted-user]`
  whatever its value (a head job's user is often not the person who
  launched it). A local container image (`/x/bwa.sif`, `file:...`) becomes
  `[redacted-location]`; a registry image stays.
- `WorkflowRun.workflow.scriptFiles`: each path, unless it is under the
  run's repository URL; checksums stay.
- `WorkflowRun.params` and `WorkflowRun.config`: every string judged as a
  RunManifest's `params` strings are, and a value under a secret-named key
  becomes `[secret]`; no scope is dropped.
- `TaskRun`: the paths of its `input` DataPaths, its `val` values (copied,
  no secret-name rule on an input's name, since a `val(key)` input is a
  sample key), `conda`, `spack`, `globalVars` (with the secret-name rule:
  Nextflow records each `params.x` a script reads as
  `globalVars['params.x']`, so `params.db_password` and `task.ext.token`
  become `[secret]`), the paths of `binEntries`, a local `container`, and
  `script` (below). Any string in any of these records (a `val` input, a
  `globalVars` value, a DataPath path, an output value, WorkflowRun config
  and metadata) that equals what the scrub redacted in the run's params
  becomes that marker, so `val(params.db_password)` goes; as in a script, a
  secret a name carried counts only from 8 characters. Known limit: this
  step does not exempt `lid://` and `cas://` addresses, so a secret-named
  param that holds one (`params.genome_key = 'lid://…'`) also turns that
  address into `[secret]` where a task input or output repeats it.
- `TaskOutput.output` and `WorkflowOutput.output`: every value, as a
  TaskRun's inputs.

Keys and parameter names go through `Leaks.keyNames`, as a RunManifest's
map keys do (§6): a map key, or the `name` of a top-level `Parameter` in
`params`, `input` or `output`, that the scrub would change becomes the
marker for what it held, numbered `-2`, `-3` past any key the map keeps.
Every other key keeps its text. A key under the run's repository URL
stays and takes no number.

Kept: record keys, `lid://` and `cas://` references, checksums,
`repository`, `commitId` and `revision` (the pipeline's identity, as the
RunManifest keeps it), ids, numbers, booleans and dates, and any path
under the run's repository URL (a public source link). A field a newer
Nextflow adds passes through unscrubbed.

Scripts. A TaskRun's `script` is not scrubbed token by token, since
`Records.scrubText` would read the `/.* /` in `sed 's/.* //'` as a path and
the record would misreport what ran. Three kinds of literal text are
replaced, longest first, where each stands as a whole path or word: not
after a letter, digit, `_`, `.`, `-`, `/`, `}`, `)` or `$` (text after
`}`, `)` or `$` continues an expansion such as `${PWD}/tmp`), and not
before a letter, digit, `_` or `-`.

- What the scrub redacted in the run's `params` (from `session.params`),
  with its marker.
- What it redacted in this task's own values, with its marker.
- The run's host directories (launch, project, work and home, and the
  session's work directory), as plain paths and as `file://` or `s3://`
  URIs, with `[redacted-location]`.

A redacted piece is looked for only from 3 characters. A secret that a
name carried (a value under a secret-named key, or one the flag,
assignment, JSON-key, `-D` or bare `name=value` rules found) is looked for
only from 8 characters, and a number never, because `--key sample` or
`max_tokens = 1000` is as likely to be code; such a secret stays in the
script while the record's params hold `[secret]` (ruling: `nf/` is outside
the RunManifest's bar). A secret found by its own shape (a token, a key
id, a Bearer or Basic credential, a PEM key) is looked for from 3. A piece
that is a marker, holds one, or is a marker's inner word is never looked
for. Any other absolute path a script spells stays, as does a password
spelled alone in a script whose params held it inside a URL (the URL was
redacted whole).

The unscrubbed fallback. `NfRecordScrub` computes every replacement before
it assigns any field, and `CasLinStore` catches anything it throws other
than a `VirtualMachineError` (a newer Nextflow whose model changed shape
throws a `LinkageError`), and a `StackOverflowError`, the one VM error a
regex scrub raises on a long enough value (`OutOfMemoryError` and the other
VM errors are rethrown). It warns "could not scrub lineage record '<key>'
(...); saving it unscrubbed" and saves the record as Nextflow handed it
over. Losing the record would be worse: `Session` swallows the exception
at debug level, and a lost WorkflowRun leaves the run with no history
line and its TaskRuns naming a run that is not there.

Per session. `CasLinStore` builds the scrub from `Global.session`
(`NfRecordScrub.forSession`: the repository, the host directories, the
params) and keeps it while `Global.session` is that same session and the
session has WorkflowMetadata; a new session builds a new one, and a bare
session without metadata builds one per save.

Not covered: `nf/` records written before this change are not rewritten.
Nextflow computes the run key over the unscrubbed WorkflowRun. `nextflow
lineage diff` no longer shows differences that were only in a redacted
path, `lineage find` on a scrubbed field no longer matches the original
value, and `lineage render` merges inputs that differ only in a redacted
path.

The gate check. `gate/nf-scrub/check.sh <SCRATCH_ROOT>` runs released
Nextflow 26.04.6 with `NXF_HOME`, `NXF_PLUGINS_DIR` and `XDG_CACHE_HOME`
under the scratch root (never `~/.nextflow`; the framework jar is copied
from there when present, a read) and AWS credentials cut off. It runs the
producer twice, then checks that `nextflow lineage list`, `view`, `find`,
`diff` and `check` work, that a downstream run stages files through
`lid://` and through `channel.fromLineage`, and that no record but a
`FileOutput`'s `path` holds a host string (`records.py leftovers`). Run it
directly: there is no make target. It refuses a scratch root that is not
empty and holds no `.nf-scrub-root` marker.

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
  *Amended 2026-10-26 (cross-run item lineage, spec
  `docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md`
  section 5.2):* the observer keeps what the ItemLineage walk needs, in
  memory, with no file system access on the task thread.
  - `onFilePublish` keeps, per publish join key, `event.source` (the
    work-dir file). Nextflow passes it on the already-exists path too, so a
    resumed publish still reports it. The Output Index File event has no
    source and is skipped.
  - `onTaskComplete` and `onTaskCached` keep, keyed by task hash, a note
    of the task's work directory, process name, tag and staged inputs
    (`InputRecorder.Staged`, which gains `stageName`, the name the file is
    staged under). Paths and strings only; holding the TaskRun would pin
    its values. A cached task's note is the original execution's.
  - At completion, for each joined OutputItem, each leaf's source maps to
    the task whose work directory holds it, and then to that task's inputs.
    An input the InputRecorder kept as a run input is a hit. An input under
    another task's work directory is followed the same way, each task once
    per item, so a cycle ends. The tasks whose work directories held the
    sources are the item's `tasks`, not the intermediate ones the walk
    passed through.
  - Passthrough rule: a published file that is itself one of the task's
    staged inputs (`<workDir>/<stageName>` is the staged link) maps to that
    one input, not to every input the task staged.
  - An item whose source maps to no task (published from outside a task)
    gets no entry. A failed run writes what it resolved. A cached task, or
    a publish that finds its target already there, still feeds the walk, so
    the InputSet and ItemLineage CIDs of a fully cached `-resume` equal the
    first run's.
  - The ItemLineage is written between the InputSet and the RunCompletion.
    When the InputSet is null so is `item_lineage`; `item_lineage` is also
    null when the ItemLineage could not be made.
  - `InputRecorder` reads `params.input` only when
    `params.containsKey('input')`. `params.get('input')` warns `Access to
    undefined parameter 'input'` in a typed script whose `params {}` block
    has none, and throws under `nextflow.enable.strict`.
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
  *Amended 2026-09-30 (patch 0.3.0-beta.2):* whatever stops the RunCompletion
  being written is also logged at error on `nextflow.cas` (`ConsoleLog`),
  naming the run and the cause, before it is rethrown or (on a failing run)
  logged at warn as above. `Session.notifyEvent` logs any other exception
  than `AbortRunException` at debug (`Session.groovy:1125-1128`), so without
  this line a run could go unrecorded with nothing on the terminal.
  *Amended 2026-09-30 (patch 0.3.0-beta.2, aborted runs):* from Nextflow
  26.08.0-edge (`14d5f26c4`, #7349, "Fix race condition calling workflow
  onComplete twice") `Session.shutdown0` runs once (a `compareAndSet` on
  `shutdownInitiated`), so an aborted run is notified only on the thread
  that called `Session.abort`, after its shutdown callbacks (the user's
  `onComplete`, `TaskPollingMonitor.cleanup` killing tasks), and
  `Session.destroy` on main no longer notifies at all. Main goes straight on
  to `Plugins.stop()` and `System.exit` (`ScriptRunner.shutdown`,
  `ScriptRunner.groovy:243-248`), and the aborting thread's write was lost
  (agitated_hodgkin on 26.09.0-edge: "Session aborted" at 13:01:33.996,
  nf-blocks stopped at 13:01:34.092, no RunCompletion). `CasPlugin.stop()`
  therefore calls `CasObserver.finishPending()`: for each observer of the
  JVM whose RunCompletion is not written, if the latch is claimed it waits
  up to 60 s for the write; if not, and `session.isAborted()`, it runs
  `onFlowComplete()` itself on main (failing-run rules above). The latch
  still keeps one RunCompletion per run; a notification that arrives after
  is the loser and returns once the write is done. On 26.04.6 the second
  notification from `Session.destroy` still waits, and the stop finds
  nothing left to do. A completion written from the stop may precede
  `WorkflowMetadata.invokeOnComplete` (a shutdown callback on the aborting
  thread), so a failed run's `error` falls back to `session.error`'s
  message when the metadata has none, scrubbed as any `error` is
  (`Records.scrubText`); `exit_status` and `finished_at` then have no such
  source and stay null and the write time.
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
  elements under the array's path; `type` ∈ `string|int|float|bool|null`
  (*amended 2026-10-24:* a null value writes no row, so a null field counts
  as an absent key, and `Index.items` refuses a null predicate value instead
  of matching nothing; `null` is no longer written);
  `value` is the text form; strings longer than 1024 bytes stored as
  `sha256:<hex>` with `truncated = 1`.
  *Amended 2026-10-24 (ranges):* a `where:` key ending in `>`, `>=`, `<` or
  `<=` (`'meta.depth >='`, `Conditions.splitKey`) compares rows of type `int`
  or `float` by `CAST(value AS REAL)` (`Index.SQL_ITEMS_RANGE`, one clause per
  operator, mirrored in the page's `queries.json` as `itemsRange`). The bound
  must be a number; text never matches a range. `nf-blocks:items` takes
  `path>=n` and the like and keys them the same way. No schema change: the
  `(path, type, value)` index still narrows by path.
  *Amended 2026-10-24 (not equal):* a `where:` key ending in `!=`
  (`'meta.strandedness !='`) keeps every item with no row equal on path, type
  and value, so an item without the path matches (`Index.SQL_ITEMS_NOT_EQUAL`,
  a `NOT EXISTS` over the same index, mirrored in `queries.json` as
  `itemsNotEqual`). A value past 1024 bytes is refused, since a truncated row
  cannot be compared. `nf-blocks:items` takes `path!=value`, matched as text
  like `=`. The explorer's filter picker offers `==` and `!=` on every field.
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
  `runByManifest(Cid) → Optional<Cid completion>`;
  `pipelines() → List<String>` (distinct pipelines any run names, sorted);
  `runsOf(String pipeline) → List<IndexedRun>` (that pipeline's runs, newest
  `finished_at` first, ties by completion cid). `prune` reads both (§19); no
  other caller does.
  `successful` = `status == 'succeeded' AND possibly_incomplete = 0`
  and no current `delete` Claim names the RunCompletion, a conflicted
  deletion included; `latestSuccessfulRun` warns once per conflicted run it
  leaves out. Claims are ingested from `claim` Store Log entries into `claim`
  and `claim_supersedes`; `claim_current` is `ClaimState` (decision 5 of the
  milestone 2 plan) per subject. *Since milestone 6:* `ClaimState` also groups
  the `retain` attribute (`released` when its one current Claim is `set
  retain "lineage"`, `conflicted` with more than one, `none` otherwise) and
  the `pin` attribute (an `add pin "<note>"` group, exempt from the
  one-current rule like `name`; a subject is pinned while any `add pin` on it
  is current) — §19.
- `Index.forget(Collection<Cid> cids)` removes every row keyed by these cids
  as a run, collection, item, Selection or Claim, and their `claim_current`,
  in one transaction; producer rows stay (a forgotten run's content still
  shows who produced it). A sweep calls it with the cids it deleted, then
  recomputes the writable member's Index Snapshot, so a cold reader is not
  seeded with runs whose blocks are gone (plan decision 7, §19).
  *Since 2026-10-09 (§20):* the cache records each member's `store-id`
  and resets (every table emptied, `meta` cleared except
  `block_present_generation`, which moves on by one) when one changes, and
  catch-up applies the `forget` entries a sweep writes, so every cache
  forgets what any sweep deleted, not only the sweeper's.
- *Amended 2026-09-25:* the three load-bearing queries' SQL is held in public
  constants (`Index.SQL_PRODUCERS_OF`, `SQL_LATEST_SUCCESSFUL_RUN`,
  `SQL_ITEMS_BASE`, `SQL_ITEMS_PREDICATE`, `SQL_ITEMS_PREDICATE_NULL`,
  `SQL_ITEMS_ORDER`, `SQL_COLLECTIONS_OF`), and the page's copy in
  `web/src/queries.json` is pinned equal to them by `ExplorerQueriesTest`, which
  also refuses any page query whose plan scans a table other than `run` through
  a covering index (§15).
- *Amended 2026-10-25 (explorer v0.3 P3, plan section 1.5 and D8):* two
  tables for `transfer` Store Log entries, last in `Index.ddl()` as
  `Index.TRANSFER_DDL`, so the cache and the Index Snapshot both have them:

  ```
  transfer(cid PK, from_member, to_member, asserted_by, depth, claims,
           blocks, copied, bytes, at)
  transfer_root(transfer_cid, root_cid, role)
    index transfer_root_root (root_cid)
  ```

  One `transfer` row per Transfer block (§6): `from_member` and `to_member`
  are its `from` and `to`, `claims` its categories comma-joined, `at` its
  `timestamp`, the rest its fields of the same name. `transfer_root` has a
  row per root asked for (`role = 'root'`) and per RunCompletion in its
  `runs` (`role = 'run'`). *Amended 2026-10-25 (explorer v0.3 P4, schema 5, now 6):* the
  three objects are part of the base DDL (schema 5, now 6), so every cache and
  snapshot has them.

  Ingest (`ingestTransfer`): a `transfer` entry is read in catch-up and in
  the `missing` retry like a `claim` entry, recorded in `log_entry` with kind
  `transfer`, and ingested unless the Transfer is already indexed
  (`isTransferIndexed`). The first block scan of a member and `rebuild` also
  ingest every Transfer block they find (`Records.TRANSFER` is in
  `SCANNED_KINDS`, after Claims), so a cache may hold a Transfer whose entry
  a stopped copy never wrote; such a Transfer has no `log_entry`, so no
  snapshot carries it and no sweep roots it (§19). Ingest deletes the
  Transfer's rows and writes them again, so it is idempotent across the
  catch-up overlap. A Transfer block not yet arrived is a `missing(NULL,
  cid)` row, retried by later catch-ups; a read that fails is recorded the
  same way. A block that is there but is not a Transfer this build reads (a
  wrong shape, another kind, over the size cap) is warned once and skipped,
  not retried, as a malformed Forget is. A Transfer changes no retention,
  so its ingest leaves `block_present` alone. `Index.forget` removes the
  `transfer` row and the `transfer_root` rows keyed by a forgotten Transfer
  cid; `transfer_root` rows whose `root_cid` is forgotten stay, as producer
  rows do. A seed copies the snapshot's `transfer` and `transfer_root` rows
  of the Transfers the cache does not hold, before the other seed
  statements. The seed's `missing(NULL, cid)` rule for unread Store Log
  entries covers `transfer` entries too. A reset empties both tables with
  the rest.


  *Amended 2026-10-25 (P3 final review, item 4; `Index.open`):* two
  openers of one cache file never delete it under each other. The explore
  server opens a cache per request, so two dry runs and a copy could open
  one at once, and the second could read a schema the first had not yet
  written, call it stale, and delete the file and its `-wal` and `-shm`
  under the first.

  - Openers in one JVM take turns per file (a monitor per absolute path).
  - A new cache is built under a temporary name beside the file
    (`<name>.creating-<token>`), its whole schema and the version row in
    one transaction, then hard-linked into place, so the file at the path
    always has its schema. When another process linked one there first,
    that one is opened. A file system without hard links gets a rename,
    made only when the path is still free.
  - A file that reads as stale (another version, or no `schema_version`
    table) or cannot be opened is asked again holding the write lock
    (`BEGIN IMMEDIATE`, which waits the busy timeout for a connection
    still committing a schema) before it is recreated. When the lock or
    the first look stays `SQLITE_BUSY` past the busy timeout, `open`
    throws `IllegalStateException` ("busy ... try again") and deletes
    nothing. Callers already treat an index failure as derived (§11).

  Tested by `IndexOpenRaceTest`: two threads opening a new cache 20 times
  over get one whole database; a cache whose schema another connection is
  still committing is waited for; a stale cache another connection holds
  past the timeout is left in place and the open throws. Not closed:
  `rebuild` still replaces the file under other connections, and two
  processes recreating one stale file at the same moment can still race
  between the second look and the delete.

  *Amended 2026-10-25 (explorer v0.3 P4, schema 5, then 6; replaces P3's F1
  hook and P4's own rescan):* the index schema is version 6 (5 for the
  P4 tables; 6 so that a v5 cache that ingested a Handoff before its files
  were recorded as content is rebuilt). `Index.ddl()`
  creates every table in one list: the base tables, then `transfer` and
  `transfer_root` (P3), then `project`, `project_stats`, `run_input`,
  `handoff` and the `claim_current_attribute_value` index (P4). A cache
  whose `schema_version` is not 5 is deleted and recreated on open, and
  the next catch-up of each member scans its blocks and reads its whole
  Store Log, so Transfers, Projects, Handoffs and every run's InputSet are
  ingested whatever an older build skipped. There is no in-place
  migration: no tables are added to an existing cache and no member is
  marked for a rescan; only the cache-only `block_absent` and
  `block_present` are created if absent. The user's ruling for v0.3
  ("I'm the only user so far, so always prioritize clean code over
  backwards compatibility") replaced P3's `AddedTables` hook (which kept
  schema 4 and added tables in place) and with it the problems it carried:
  the snapshot hole (a session without the added tables could publish a
  snapshot with empty tables and a watermark past their entries) and the
  busy-session paths. Measured on a scratch copy of roblab: a cold v5
  catch-up in 279 ms (including the open) and a snapshot write in 232 ms.
  Tested by `IndexSchemaVersionTest` (a cache of an older version is recreated and its catch-up
  ingests a Transfer, a Project, a Handoff and `run_input`; a v4 snapshot
  is not seeded from; a run found only by the block scan gets its
  `run_input` rows) and `TransferAfterV4Test` (P3's REV6 case through a
  recreated cache: org counts 11 B in 2 blocks).

  *Amended 2026-10-26 (cross-run item lineage, schema 7):* the index schema
  is version 7 and the snapshot is `index/v7.sqlite`. As at 6, a cache of
  another version is recreated and a snapshot of another version is not
  seeded from; there is no migration. The tables and their ingest are in
  §22 "Index (§12)".
- *Amended 2026-10-25 (explorer v0.3 P3):* lookups for the copy closure
  (§21), each over a public SQL constant (no schema change; tested by
  `IndexCopyQueriesTest`):
  - `blockSize(Cid) -> Long` (`SQL_BLOCK_SIZE`): the `block` table's size, or
    null.
  - `attrsOf(Cid item, String path) -> List<AttrRow>` (`SQL_ITEM_ATTR`): an
    item's untruncated `item_attr` rows at one path, by type and value.
  - `runItemsWith(Cid completion, String path, String type, String value) ->
    List<Cid>` (`SQL_RUN_ITEMS_WITH_ATTR`): every item of the run, in any of
    its outputs, with an untruncated row equal on all three, by cid.
  - `outputCollectionsOf(Cid item) -> Map<Cid collection, Cid completion>`
    (`SQL_OUTPUT_COLLECTIONS_OF_ITEM`): the OutputCollections (kind
    `output`) that list the item, with their runs.
  - `claimsLoggedIn(String member) -> Map<Cid subject, List<ClaimState.Row>>`
    (`SQL_CLAIMS_LOGGED_IN`, `SQL_CLAIM_SUPERSEDES_LOGGED_IN`): every Claim a
    `claim` entry of that member's log announced, grouped by subject, each
    with every link its `supersedes` makes, whoever logged the Claim it
    names.
  - `claimOf(Cid) -> Claim` (`SQL_CLAIM`, plus an inline query of
    `claim_supersedes`): one Claim as the index holds it, value as text;
    null when it is not indexed.
- *Amended 2026-10-27 (background store indexing in the explorer, spec
  `docs/superpowers/specs/2026-10-27-background-store-indexing-design.md`,
  §6.1 and §6.3; plan
  `docs/superpowers/plans/2026-10-27-background-store-indexing.md`, task
  C1):* the index can say which members it holds, seed without scanning, and
  scan one member with progress. No schema change.
  - `Index.isIndexed(member)` is true when the cache holds
    `store_log_watermark:<member>` or any `block_scan:<member>` value,
    whatever put it there: a snapshot seed, an earlier scan, a pipeline run
    or `reindex`.
  - `Index.trySeed(store, storeLog, member, snapshots, tempDir)` is the
    seeding branch of the 7-argument `catchUp`, returning the reason instead
    of warning. It returns null once the member is seeded, or when the
    member already has a watermark; otherwise the `SnapshotUnusable` reason
    (`absent`, `unreadable`, `schema_version <n>`, or one of the two
    incarnation messages). It reads no block and never scans. An exception
    from `snapshots.fetch` other than `SnapshotUnusable` counts as
    `unreadable`. The 7-argument `catchUp` calls it and, on a reason, warns
    and scans as before, so `snapshot`, `reindex` and pipeline runs behave as
    they did.
  - `Index.indexMember(store, storeLog, member, holders, progress)` scans one
    member and reports through `IndexProgress.report(phase, done, total)`
    (`-1` for unknown; phases `listing`, `reading`, `ingesting`, `log`,
    `snapshot`, `push`). It reports `listing`, lists, reports `reading 0 <n>`
    and then `reading <i> <n>` after each DAG-CBOR block (`n` is the count
    listed), ingests the kinds it found in `SCANNED_KINDS` order reporting
    `ingesting <i> <records>`, reports `log`, runs the missing-block retries
    and reads the Store Log from the start, and then in one transaction sets
    the watermark and `block_scan:<member>` (`done`, or `empty` when it found
    nothing). Whatever the callback throws propagates out of the scan.
  - The scan with a progress callback is strict. A failure to list, size,
    open or read a block throws and nothing is marked, where the tolerant
    scan (`scanOnce`, `rebuild`) treats an unreadable block as missing and
    sets the watermark after a failed listing. A block that reads but does
    not decode, or is over the size cap, is skipped as before. This is what
    keeps credentials that expire part way through from indexing half a
    member.
  - `Index.reset()` also deletes `IndexPaths.localSnapshotsDir(file)`
    recursively once its transaction has committed; a delete that fails logs
    at debug (§20, 2026-10-27 amendment, and §15 for what lives there).
    `IndexPaths.localSnapshotsDir(cacheFile)` is the cache file's name plus
    `.snapshots`, beside it.

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

*Amended 2026-10-26 (cross-run item lineage, spec
`docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md`
section 5.1):*

- While `fromStore` resolves its items (all of them, before any task runs)
  it fills a per-run map on `CasSession`: each emitted Path to the set of
  Origins that gave it (InputSet, §22). Nothing goes into the path, so task
  hashes do not change. `Index.selectionVias` gives each item a Selection
  reaches with the collections it was picked through, and `Index.itemRefs`
  each item of a run's output with its collection; `Index.selectionItems`
  and `Index.items` are unchanged.
- A Selection item reached through several OutputCollections
  (`collection_item.via_cid`) gives one Origin per collection. The Selection
  recorded is the one the call named: for nested Selections the outer one,
  not the inner Selection that held the item. A Selection item with no
  `via` gives `collection` null. `run:` and `output:` record the run's
  OutputCollection and a null Selection.
- On Nextflow 26.10.4 `channel.fromStore` in a typed script still fails
  with MissingProcessException (checked 2026-10-26), so the
  `nextflow.Channel.fromStore` spelling above holds. Record-collection
  inputs (`Bag<Record>`) stage their files from 26.10.4.

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
lives at `gate/gate.config`. Lineage tier: 18 PASS, 0 FAIL, 5 SKIP.
Assertion 17, since 2026-10-09 (§20): a member deleted and written again at
the same location, with the same cache, leaves the cache, the member's
snapshot and a cache seeded from it holding only the new run, with one
`store-id` in all three. Assertion 18, since 2026-10-09 (§20): after a sweep
deletes a run's metadata through one cache, a second cache that had indexed
the run holds no row keyed by any cid in the sweep's `forget` entry and keeps
the other run whole, and a cold cache seeded from the rewritten snapshot
holds the kept run and not the swept one.
Assertion 9, since milestone 6 (§19, ticket 21 answer 7): a dry-run sweep
right after a run reports an empty dead set; after `set retain "lineage"` on
one run, a real sweep trashes exactly its unshared content and none of its
metadata, a pinned item in it and content it shares with another run
survive; `del retain` before the deadline brings the content back as live
with no `untrash`; a later sweep past the deadline deletes what is still
released. Assertion 13, "a cold cache seeds from the Index Snapshot", takes its locked
runs from the Store Log entries at or before the snapshot's watermark, and
counts a permission failure when the seeded run's log says "could not be
read" or "could not be decoded as <Kind>" together with a locked path (the
plugin reports a locked RunCompletion as undecodable).

*Tier two* (`make gate-tier2`, `gate/tier2/`, on demand): the scidev Batch
queue, a throwaway S3 member, T1-T6, T2b and TS (`gate/tier2/README.md`); run
before a milestone that touches the S3 store, the Fusion provider or the
cloud publish path is accepted. Since milestone 6, T7 exercises the
retention sequence (sweep, the lock and the Trash ledger) on the S3 member,
overlapping T6 and the background nf-core/sarek run TS (§19). It is the only
part of the Gate that talks to AWS, from a person's SSO session; its own unit
tests (`python3 -m unittest discover -s gate/tier2`) do not.

*Amended 2026-10-25 (explorer v0.3 P4 follow-up, R5, issue 17):* the
`resumed` run resumes `cold` by its session UUID (read from
`pipeline-a/.nextflow/history`), because Nextflow 26.04.6's `-resume
<run name>` takes only `last` or a UUID and otherwise resumes the last
session, which was `fail`'s: which of `fail`'s tasks had finished was a
race, and so was A1. It also sets `workflow.output.overwrite = false`
(`gate/resume.config`, for that run only), so PublishDir re-copies nothing
into existing `cas://` coordinates on resume; `copyAttributes` does not
help there (the cas provider reports a block's own mtime) and `overwrite
'sha256'` reads the source bytes. Assertion 4c now holds when the resumed
run carries `cold`'s session, every `cold` task is a cache hit, no
PublishDir copy fails with every published source at mode 000, the run
exits 0, and the plugin's addresser summary reports 0 bytes for 0 files.
Because the plugin addresses a Leaf only from a publish made in the same
run, the resumed run records every leaf `never_published` and is never a
producer of any content (A1).

*Amended 2026-10-25 (explorer v0.3 P4, second follow-up, S1 and S2):*
assertion 4c also fails unless the resumed run's RunCompletion has
`anomalies.never_published` equal to the number of Leaves in `cold`'s
items (21 on the Test Pipeline), empty `providers`, and the same number of
Leaves of its own, none addressed; and unless no line of the resumed
`nextflow.log` holds `AccessDenied`, `Permission denied` or a locked source
path. When a publish of a locked source fails, the Gate names the reader
from the stack trace: the plugin's `upload()` (a `robsyme.cas` frame under
`PublishDir.processFile:418`, the first copy) or PublishDir's re-copy
(under `:432`, after `shouldOverwrite`; line numbers of 26.04.6). gate.sh
exits 2 when `cold`'s session UUID is missing instead of falling back to
`last`. Assertion 19 covers a resume after a failed run, in its own store,
cache and launch directory: `recover-fail` runs `--fail` with
`errorStrategy 'finish'` on MAYBE_FAIL and 16 local cpus (so every other
task finishes; one that finishes after the session cancels still never
reaches the workflow outputs, so which cached tasks the failed run
published remains a race), and `recover-resumed` resumes that session by UUID
without `--fail` and with the default overwrite: 12 tasks are cached and
MAYBE_FAIL's 3 re-run. Each re-run task's `.report` must be addressed in
the reports Leaf, the coords pointer and a store block, at the address the
Gate computes from the work file; every Leaf the failed run recorded must
keep its address, a Leaf only the resumed run has (a cached task the failed
run finished after it cancelled) must be addressed, and none may be
`never_published`. It reports, and does not
assert, the cost §8 describes: 24 files addressed, 21 of them cached tasks'.

## 15. The block explorer (milestone 1)

*Status 2026-09-25: milestone 1 accepted; Gate browser tier A 7 of 7 (local 5, cloud 2).*

Specified in `../.scratch/block-explorer/spec.md`; this section fixes the names,
paths and seams its pieces share. Plan: `docs/plans/2026-09-25-explorer-milestone-1.md`.

### Index Snapshot

- Path `<member>/index/v<Index.SCHEMA_VERSION>.sqlite`, today `index/v7.sqlite`
  (`index/v3.sqlite` before explorer v0.3 P2, v4 before P4, v5 before its
  second follow-up, v6 before cross-run item lineage; the page reads only the current one and says an older
  snapshot is too old). Class `robsyme.cas.core.IndexSnapshot`.
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
  cache index, a Store Log entry name; absent when the member has no log),
  `snapshot_written_at` (ISO-8601 UTC, milliseconds) and, since 2026-10-09
  (§20), `store_id` (the member's `store-id`; absent when the member has
  none).
- *Amended 2026-10-24 (explorer v0.3 P2, storage accounting;
  `docs/plans/2026-10-24-explorer-v0.3-p2.md`, report
  `docs/design/reports/p2.md`):* schema 4 adds content and storage tables.

  The cache index fills two of them at ingest:
  - `block(cid, size, codec)` holds raw file blocks, with sizes from Leaf and
    DirectoryManifest entry records, and DirectoryManifests at their encoded size.
  - `block_ref(owner_cid, cid, name)` holds item → Leaf and manifest → entry edges.

  The snapshot copies both for its member's closure, so a seeded cache keeps
  them.

  The snapshot build then derives `root` (one row per run, Selection, and
  pinned subject and asserter, with states `held`, `released`, `deleted` and
  `pinned`, by `Mark`'s rules) and computes these per-store aggregates with
  `StorageStats`: `root_stats`, `root_shared`, `store_stats`, `asserter_stats`,
  `pipeline_stats`, `collection_size`, `item_size`, `kind_stats` and
  `pinned_overlap`.

  *Amended 2026-10-25 (explorer v0.3 P4 follow-up, R3):* a Handoff its
  member logged is a root too (kind `handoff`), numbered after Selections
  and before pins, by cid. It reaches its files and the items of its
  Selection (through child Selections), whatever the Selection's own
  state, and is `deleted` on a clean current `delete` from the member's own
  Claims, `held` otherwise, as a Selection is. So a live Handoff's files
  count in Stored, nothing it reaches is releasing, and `store_stats`
  releasing is what a sweep of the member would take. The cache records a
  Handoff's files at ingest as `block_ref(handoff, file, name)` with each
  file's `block` row sized by its Leaf; the snapshot copies them with the
  content edges, from the member's `handoff` rows as well as its items. A
  Handoff is not an item: no `item_size` row; its files appear in
  `kind_stats` and, since the second follow-up (below), in
  `referenced_bytes`. The page sees `root.kind =
  'handoff'` in `root_shared` rows. A Handoff whose Selection is logged
  only in another member reaches only its files here, the per-store limit
  of a cross-member release.

  *Amended 2026-10-25 (explorer v0.3 P4, second follow-up, S3):*
  `referenced_bytes` is the sum of the raw file references under every
  holder, multiplicity kept and directories expanded. The holders are each
  (output collection, item) membership (P2 R2, unchanged); each Handoff
  root, one reference per file Leaf, whatever its state; and, once each,
  every other cid a root starts from that no holder of the first two kinds
  reaches (an item of another member's run that a Selection or Handoff
  here holds, R8, or a pinned cid outside them). A cid under another holder
  of the third kind is not a holder itself, so a pin, or a Selection of an
  item this store's runs already list, adds nothing. Every stored block is
  under some holder, so `referenced_bytes >= stored_bytes` for any content
  and the page's dedup (`referenced / stored`, `web/src/model.js`
  `storeStats`) is never below 1x. Tested by `IndexSnapshotHandoffStatsTest`
  (a Handoff, a pinned Selection and a released run: stored 47 = referenced
  47, was 38), the R8 case in `IndexSnapshotStorageTest` (referenced 51
  against stored 47, was 19) and `StorageStatsTest`; roblab's figures are
  unchanged (no Handoff, no R8 item).

  Stored counts the raw blocks reachable from every root of the member, of
  every kind. Two kinds of block are checked for presence (the writable
  member's `has`, 16 at a time, a lookup that throws counting as present)
  when the snapshot is built:
  - a block reachable only through released or deleted roots, because a sweep
    may have removed it;
  - a block recorded absent that a held or pinned root now reaches, because a
    Claim (`del retain`, `add pin`) brings no bytes back. A present answer
    drops the record.

  A block found absent leaves every storage figure, whatever its roots'
  states. The cache remembers it per member in the cache-only table
  `block_absent(member, cid)` (§20). Records that are no longer candidates
  for either check are dropped.

  `meta` gains:
  - `sweep_grace_ms`: `cas.sweep.grace` when the writer knew it.
  - `storage_omitted`, on a lean snapshot, one that leaves out the content
    and storage tables (`block`, `block_ref`, `store_stats` and the other
    aggregates), so the page explains why its storage figures are missing.
    It takes one of three values:
    - `over_cap`: the full snapshot was estimated, as the lean part plus
      `block_ref` rows × the learned bytes per reference, to pass
      `cas.snapshot.maxBytes`.
    - `out_of_memory`: computing the figures ran out of memory.
    - `seeded_lean`: the cache that wrote it was seeded from a lean snapshot,
      so it has no content edges for those runs. `nextflow plugin
      nf-blocks:reindex --seed false` rebuilds it from the store.

    A `seeded_lean` build never replaces a base that has storage figures: it
    is skipped as `seeded_lean_base_full`. It also never replaces a base it
    could not read: that skip is `seeded_lean_base_unknown`.
  - `snapshot_max_bytes`: on a lean snapshot, the cap in force, as decimal
    text. Absent when no cap applied.

  On S3 the snapshot object also carries `x-amz-meta-storage: full|lean`, so
  a writer can tell from the `HEAD` whether the base has storage figures. An
  object without it, written before explorer v0.3, counts as unknown.
- *Amended 2026-10-25 (explorer v0.3 P3, plan section 1.5):* the snapshot
  carries the `transfer` rows of the Transfers whose `log_entry` names the
  member (kind `transfer`), and their `transfer_root` rows
  (`IndexSnapshot.COPY_TRANSFERS`). A Transfer the cache holds from a block
  scan alone, or from another member's log, is left out. Seeding copies
  them back (§12).
- *Amended 2026-10-25 (explorer v0.3 P3, decision D8):* a copy carries every
  record of each run it touches (§21) but only the files its depth reaches,
  so a destination can hold a run whose other items' files are elsewhere.
  So every root and run a snapshot's `transfer_root` rows name (both roles;
  these are only the Transfers the member's own log announces) is exposed
  in the P2 sense, like a run or Selection ever released or deleted
  (`IndexSnapshot.exposedSubjects`): a block that a held or pinned root
  reaches under it, with no presence record either way, is looked up with the writable
  member's `has` (16 at a time) and recorded in `block_absent` when gone
  or `block_present` when there. The files a partial copy left behind then
  leave every storage figure as swept ones do. A Transfer logged only in
  another member exposes nothing here. Tested by
  `IndexSnapshotTransferPresenceTest`: the destination's `store_stats`
  counts only the copied item's files, and the same layout without the
  Transfer counts them all.

  Known cost, kept from P2 (ruling 2026-10-25): any Claim the cache ingests
  drops every `block_present` row (§20 decision 4 amendment), and every
  copied file is now an exposed candidate, so the next destination build
  after any Claim re-asks every copied file once (on S3 one `HEAD` each, 16
  at a time: about 625 rounds for 10,000 files). Recorded absences a held
  root reaches are re-asked at every build (P2's rule: only a present answer
  drops one), so each build of a partial copy's destination re-asks every
  file the copy left behind. Neither changes a figure; both cost lookups.
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
  member's catch-up failed (`catch_up_failed`). *Since 2026-10-09
  (§20):* `meta` gains `store_id`; seeding and the guard check it, the guard
  adds `stale_store` and `older_watermark`, and `fewer_runs` counts runs a
  `forget` entry removed. Locally `replace` does not
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
nextflow [-c <config>] plugin nf-blocks:sweep [--apply true] [--wait true] [--budget <size>] [--format text|json]
nextflow [-c <config>] plugin nf-blocks:prune --keep-last <n> | --keep-newer <period> [--pipeline <id>] [--apply true]
nextflow [-c <config>] plugin nf-blocks:untrash <cid> [<cid> ...] | --sweep <id>
nextflow [-c <config>] plugin nf-blocks:reindex [--seed false]
nextflow [-c <config>] plugin nf-blocks:copy <selection:<cid>|run:<ref>|<cid>> --to <alias> [--from <alias>]
                                             [--depth results|lineage|all] [--claims names,pins,projects,annotations|none]
                                             [--dry-run] [--accept-audit true] [--format text|json]
```

`sweep` and `prune` default to a dry run; `--apply true` acts. `sweep
--apply` exits 0 only when it applied, 1 on a lost lock, a refused start (a
fresh Live Writer, without `--wait`) or a delete failure; `--wait true`
waits instead of refusing, polling and printing why. `--budget <size>` (a
`MemoryUnit` string such as `'10 GB'`) caps the bytes newly trashed, in
address order; what a budget-capped run deletes was capped when it was
trashed, not when it is deleted. `prune --apply` exits 1 only on a Claim
write failure, naming how many of the planned releases it wrote first.
`untrash` always acts (there is no dry run) and exits 1 when another sweep
holds `sweep.lock`, or when nothing named is in any Trash ledger. §19 has
the full contract and plan decisions 6 and 8 to 13. `reindex` (§20 decision
12) deletes this config's cache file and catches every member up cold, then
writes the writable member's snapshot even when the stored one has a newer
watermark or more runs (§20 build decision 19), printing both run counts; it
exits 1 when a member could not be caught up. Blocks removed outside the
plugin need `--seed false`, since the default seeds from the snapshot that
still lists them.

*Amended 2026-09-30 (patch 0.3.0-beta.2):* Nextflow 26.08.0-edge (nextflow
`1dc8cf68f`, "Separate CLI from runtime", #5971) changed the interface to
`int exec(String pluginId, String cmd, List<String> args)`: the Launcher, and
with it every launcher option, is gone, and a plugin without that method
fails with "does not define or inherit an implementation of the resolved
method". `CasPlugin` implements both; the plugin compiles against 26.04.6,
so the 3-argument one carries no `@Override`, and each Nextflow calls the
one its interface names. Both reach `CasCommands.run`. The new
`PluginAbstractExec` builds its config from `NXF_WORK` and
`NXF_CLOUDCACHE_PATH` alone and `CmdPlugin.executePluginCommand` passes the
plugin nothing else, so on the 3-argument path `CasCommands` reads the files
`ConfigCmdAdapter.resolveConfigFiles` reads when no `-c` is given
(`robsyme.cas.cli.LaunchConfig`): `$NXF_HOME/config` (`Const.APP_HOME_DIR`),
then `./nextflow.config`, or the file `NXF_CONFIG_FILE` names, each when it
exists, later overriding earlier, with the `standard` profile, parsed with
`ConfigParserFactory`, whose API is the same on both versions. Measured on
26.09.1-edge: `./nextflow.config` and `NXF_CONFIG_FILE=<file>` work;
`nextflow -c <file> plugin nf-blocks:<verb>` is accepted by the launcher and
never reaches the plugin, so the verb sees no store unless one of the
default files names it (use `NXF_CONFIG_FILE` instead). `-C` does not
reach it either. On 26.04.6 the 4-argument path and `-c`
are unchanged. So that a verb never acts on another store unannounced, the
3-argument path first checks the verb against `VERBS` (a usage error, exit
2, reads no config), then prints on stderr the config files it read, or
that it read none, and that `-c` does not reach plugin verbs on 26.08.0-edge
and later (use `NXF_CONFIG_FILE` or `./nextflow.config`). stdout is
untouched, so `items ... | put /dev/stdin` still pipes.

`CmdPlugin` turns `--name value` into the argument pair `--name`, `value` after
the positional arguments. Exit 0 on success, 1 on a failure the verb reports, 2
on a usage error.

*Amended 2026-10-25 (explorer v0.3 P3, plan section 1.1; `cli/CopyCommand`):*
`copy` plans a copy of a Selection or a run from one configured member into
another and, without `--dry-run`, makes it (§21 has the contract). A bare
`--dry-run` arrives as `--dry-run true`, as `put`'s does; `--dry-run` and
`--accept-audit` take `true` or `false`, and any other value is a usage
error. `--claims` is comma-joined, default `names,projects,annotations`;
`none` carries no Claims. Exit codes:

- 0: a plan, or a finished copy, including one that wrote nothing;
- 2: a usage error: an option `copy` does not take, not exactly one root,
  no `--to`, a `--format` other than `text` or `json`, or one of the 400
  codes `bad_root`, `unknown_member`, `same_member`, `bad_depth`,
  `bad_claims` and `bad_request` (`CopyService.USAGE_ERRORS`);
- 1: the 400 codes `not_a_target`, `root_not_found` and `bad_config`; a
  real copy refused before any write (409): a root or run released or
  deleted at the destination (D9, §21; stderr names each, as `<role>
  <name> <cid>: <state> in <to>`, and says to restore them there first),
  or audit findings without `--accept-audit true`; a copy that failed part
  way (500); and anything thrown, such as a config that does not parse or
  a member that cannot be read.

`--format json` prints the §21 body, or the 400 body, as one line on
stdout. `--format text` (the default) prints the plan as the explorer
design doc's section 7 words it, a line per group, a line per run, the
Claim counts, the audit, a line per root or run the destination already
knows (`in <to>: <role> <name> <cid> is <state>`, D9), and for a real copy
a last line with what was copied, the Transfer and the snapshot outcome.
Every message goes to stderr.

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
`Plugins.start(target)`, so the Gate's `id 'nf-blocks@0.10.0-beta.2'` in `gate.config` needs none of this.

### What a member serves

Relative to a member's base URL:

```
index/v7.sqlite            the Index Snapshot, read with single-range GETs (v3 before explorer v0.3, v4/v5 before P4's follow-ups, v6 before cross-run item lineage)
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
That line is all it prints on stdout. Right after it listens it resolves the
identity on a background thread and prints the `signing as` line, or the
warning, on stderr when that finishes; `members.json`, a write and (since
P3) a real copy about to write its Transfer wait for it, and nothing else
does. A write builds its block, which asks for the name,
before it takes the lock it shares with a samplesheet export, so an export
never waits on the resolution behind it (§16, the slf4j paragraph;
docs/identity.md).

| Request | Answer |
|---|---|
| `GET /`, `GET /index.html` | the page |
| `GET /members.json` | `{"members": [{"alias", "writable", "base": "m/<alias>/", "kind", "location", "copy_target", "copy_reason"}, ...], "write": <bool>, "identity": {"principal", "source"}, "copy": <bool>}`, writable first; `identity` is who the session signs as (docs/identity.md) and is absent when the server was built without one; the per-member copy fields and `copy` are in the P3 amendment below, and the per-member `index` object (a server with indexing only) in the 2026-10-27 amendment |
| `GET` or `HEAD /m/<alias>/index/v<N>.sqlite` | the file, honouring one `Range`, with an `ETag` |
| `GET` or `HEAD /m/<alias>/blocks/<xx>/<cid>` | the block, honouring one `Range`; `xx` must equal the cid's last two characters |
| `GET /m/<alias>/log/` | listing form 1 |
| `POST /api/put[?dry_run=true]` | the same `Put` as `nf-blocks:put` (block explorer spec sections 9.2 and 9.5): `403` without the right `X-NF-Blocks-Token` header, `415` for a content type other than `application/json` or `application/vnd.ipld.dag-json` (parameters such as `charset` ignored), `413` over `Put.MAX_REQUEST_BYTES` (2 MiB), else the builder's status and DAG-JSON body with `Content-Type: application/vnd.ipld.dag-json` |
| `GET` or `HEAD /api/samplesheet/<selection cid>.csv` or `.json` | a Selection's items as a samplesheet (decisions 1 and 18 of `docs/plans/2026-09-25-explorer-milestone-2.md`): `Content-Type: text/csv; charset=utf-8` or `application/json`, `Content-Disposition: attachment; filename="selection-<first 16 chars of the cid>.<ext>"`; `404` naming the reason when the address is not a Selection the index holds, or reaches one it does not |
| `POST /api/copy[?dry_run=true]` | one `CopyService` run (§21), synchronous, added 2026-10-25 (P3 plan section 1.7). `/api/put`'s transport refusals first, in plain text: `403` without the right `X-NF-Blocks-Token`, `415` for any content type other than `application/json` (DAG-JSON too; parameters ignored), `413` over 2 MiB. Every other answer is JSON with `Content-Type: application/json`: `200` the plan, or the finished copy; `400` `{"error", "message"}`, `bad_request` for a body that is not a JSON object, else a CopyService code (`bad_root`, `unknown_member`, `same_member`, `not_a_target`, `root_not_found`, `bad_depth`, `bad_claims`, `bad_request`, `bad_config`); `409` with the plan (`dry_run` false, no copy fields) and `"refused"` saying why, for a real copy refused before any write: `"not_kept"` when a root or run is released or deleted at the destination (D9, §21), else `"audit"` for audit findings without `"accept_audit": true`; `409` `{"error": "copy_running", "message"}` for a real copy while another runs here; `409` `{"error": "superseded", "message"}` for a dry run a newer one replaced while it waited (D10, below); `503` `{"error": "stopping", "message"}` for a dry run still waiting when the server stops; `500` in two shapes: the §21 body with its `failures` for a copy that failed part way, or `{"error": "copy_failed", "message"}` when CopyService threw. `405` when the server was started without copying |
| `GET` or `HEAD /api/index` | `200 {"members": {"<alias>": <index>, ...}}`, every configured store, in config order; `<index>` is the object in the 2026-10-27 amendment below; `404` when the server was started without indexing. Polling it never runs `members.json`'s member probe |
| `POST /api/index` | start an index job, added 2026-10-27. Body `{"member": "<alias>", "mode": "local" or "push"}`. `/api/put`'s transport refusals first, in plain text: `403` without the right `X-NF-Blocks-Token`, `415` for any content type other than `application/json`, `413` over 2 MiB. Every other answer is JSON: `202 {"member", "mode", "index"}` with the member's new state; `409 {"error": "index_running", "message"}` while a job runs or is queued; `400 {"error", "message"}` with `unknown_member` (not configured), `writable_member`, `not_writable` (push to a member that cannot take a write; the message is its `copy_reason`), `already_indexed` (`local` on a member that is `local` or `snapshot`, or `push` on one that is `snapshot`) or `bad_request` (a body that is not a JSON object with string `member` and `mode`, a mode other than `local` or `push`, or a member outside `cas.resolve`). `405` when the server was started without indexing or without the token |
| anything else | `404`; a method other than `GET`, `HEAD` (or `POST` on `/api/put`, `/api/copy` or `/api/index`) is `405`, with `Allow: GET, HEAD`; the `405` for another POST path says `only /api/put, /api/copy, /api/handoff and /api/index take a POST` |

*Amended 2026-10-25 (explorer v0.3 P3, plan section 1.7, decision D1):*
each `members.json` member also has `kind` (`local` or `s3`), `location`
(an S3 member's `s3://<bucket>[/<prefix>]`; null for a local member, whose
path is never exposed), `copy_target` and `copy_reason` (§2,
`CopyTargets.judge`: null for a target, else `readOnly is set for this
store` or `this process can't write the directory`), from
`ExploreCommand.memberInfoOf`. A server built without that information
reports a member as local and a copy target. The top level gains `copy`:
true when the server can run copies, which takes a copier and the token.
`nf-blocks:explore` always has both, so it is true there; a server built
without them answers `POST /api/copy` with `405` and reports false.

*Amended 2026-10-25 (explorer v0.3 P3, plan sections 1.7 and D6, as
built):* `POST /api/copy` runs `CopyService` over the loaded config, so
each request builds its own copy composition and index (§21) and never
touches the server's own index or `Put`'s monitor: writes and exports go
on while a copy runs. One real copy at a time per server: it holds the
copy lock (`ExploreServer.copying`) for its whole run, and a real copy that
arrives meanwhile is `409 copy_running`. Dry runs take no lock and share
two slots (`ExploreServer.DRY_RUNS`); a dry run may run beside a real
copy. *Amended 2026-10-25 (P3 final review item 5, decision D10; replaces
"a third waits for a slot and is never refused"):* one more dry run may
wait for a slot, and only one. A newer dry run takes the waiting place,
and the one it replaced answers at once `409 {"error": "superseded",
"message"}` without running; the page debounces its re-plans and ignores
`superseded`. So re-plans never queue up on the server's eight threads.
When `stop()` begins, a dry run still waiting answers `503 {"error":
"stopping", "message"}` at once, and an interrupt ends its wait the same
way, so it never holds the stop up. A
copy signs its Transfer with this server's principal, the one
`members.json` shows (§21, signing), so it resolves no identity of its own
and prints no `signing as` line. Each copy's notices go to the server's
stderr. After a copy the page re-reads the destination's snapshot and log;
the server keeps nothing of the copy.

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

*Amended 2026-10-25 (explorer v0.3 P3, fix wave item 14):* a real copy in
flight gets the copy grace, `copyGraceSeconds` (30 minutes), in place of
the 10-second one, since a copy takes minutes and is worth finishing.
`stop()` then prints on stderr that a copy is running, how long it waits,
and how to stop now: `kill -9 <pid>`, with the server's pid, after which
the destination keeps the blocks and entries already written and gets no
Transfer, and running the copy again finishes it. Once `server.stop`
returns, `stop()` asks the copy lock again before it waits on the
executor, so a copy that took the lock during the stop also gets the copy
grace and the notice. A copy still running when the grace ends is cut off,
with a second line saying the destination has no Transfer for it. A dry
run waiting for a slot ignores the stop's interrupt
(`acquireUninterruptibly`).

*Amended 2026-10-27 (background store indexing, spec
`docs/superpowers/specs/2026-10-27-background-store-indexing-design.md`,
plan `docs/superpowers/plans/2026-10-27-background-store-indexing.md`,
tasks C2, S1, S2, S3):* `explore` no longer scans a member at start, and a
member that has no Index Snapshot is indexed on request.

Startup. `ExploreCommand.start` runs `forCommand`, `checkClock`,
`warnShadowed`, then `session.leaveUnindexedOut([:])`, which turns on a
rule in `CasSession.catchUpIndex` (off everywhere else, so `snapshot`,
`reindex`, `copy` from the CLI and pipeline runs keep scanning as before).
Then it runs the first refresh, opens the index, builds `Put`, builds
`IndexJobs` over the session, the index, `Put` and the local snapshot
directory, calls `jobs.classify()`, wraps each member's files in
`LocalSnapshotFiles`, gives the server `indexing(jobs)`, prints the URL, and
only then calls `jobs.startQueued()`. The rule: for each non-writable
member, when the cache holds it (`Index.isIndexed`) catch up as before; else
`trySeed` from the member's snapshot (`seedSourceOf`, or a local snapshot
when a copy supplies one); on null the member is seeded and caught up; on a
reason the member is left out of that catch-up and named once on stderr
through `notices.warn`, then skipped on every later catch-up of the session
(startup refresh, Put, the samplesheet exporter, handoffs, the copy service,
the exit refresh), without another fetch. It is not counted among the
members whose catch-up failed. For `absent` the line is, verbatim:
`store member 'cloud' has no Index Snapshot, so the explorer does not list it. Index it from the Storage page, or run nf-blocks:snapshot against it.`
For another reason it reads `store member '<alias>' has no usable Index
Snapshot (<reason>), so the explorer does not list it. Index it from the
Storage page, or run nf-blocks:snapshot against it.` The writable member is
indexed at start as before and never left out; a large writable S3 member
with no snapshot still holds startup up (spec §3).

`members.json`. It lists every configured store (`cas.stores`), not only
those in `cas.resolve`, and when the server has indexing each member also
carries `index`, exactly `{ "state", "at", "progress", "reason", "mode",
"local" }`. `state` is one of `snapshot` (the member's own snapshot is
served, or the member is the writable one), `local` (indexed on this
machine, its local snapshot served), `none` (not indexed; `reason` says
why, `absent` for no snapshot), `indexing`, `failed` and `outside` (a
configured store that is not in `cas.resolve`, so the explorer does not
index it; the spec's enum lacks it and its §5.2 table needs it). `at` is
an ISO-8601 millisecond time, null when there is none. `progress` is
`{ "phase", "done", "total", "startedAt" }` while the state is `indexing`
and null otherwise; `phase` is one of `listing`, `reading`, `ingesting`,
`log`, `snapshot`, `push`, and `done` and `total` are `-1` when unknown.
`mode` is `local` or `push`, the mode of the job that last ran or is
running, and a Retry repeats it. `local` is true while a local snapshot is
served. A server without indexing adds no `index` key, so its `members.json`
is as it was. `GET /api/index` answers the same objects keyed by alias, so
the page can poll for progress without `members.json`'s member probe; the
page polls it about once a second, only while some member is `indexing`.

The local snapshot. A job's snapshot lives at
`<cache file>.snapshots/<alias>/index/v7.sqlite` (`IndexPaths.localSnapshotsDir`),
and the server serves it at the member's usual URL,
`m/<alias>/index/v7.sqlite`, through `LocalSnapshotFiles`, a `MemberFiles`
wrapper. For that one path it answers, in order: the local file when
`IndexJobs.localSnapshot(alias)` has one (state `local`, or `indexing` or
`failed` with `local` true); else the member's own snapshot when the state
is `snapshot` or `outside`; else `404`. So a member that is not indexed
answers `404` for its snapshot even when it holds one of its own that the
cache cannot use (another incarnation, an unreadable file), and a member
indexed here whose own snapshot is unusable serves the local one over it,
after a restart too. A member with a usable snapshot of its own serves its
own and the local one is never preferred. Every other path, listings and
descriptions go to the member as before. The Store Log tail still comes from
the member. The local file is replaced whole when a refresh finishes; a page
that had the old one open fails with `snapshot_changed`, so the page
invalidates that member when a job ends and reads it again, in place.

The job. `IndexJobs` runs one job at a time on one daemon thread named
`nf-blocks-index`, never on the server's eight-thread pool, with a queue
for the refreshes a restart or a copy asks for. A start while a job runs or
is queued is `409 index_running`. Each member's state is an immutable value
replaced whole, so a reader never sees half of one. Classifying at start:
the writable member is `snapshot`; a member outside `cas.resolve` is
`outside`; a member the session left out is `none` with its reason, and any
local directory it has is deleted; a member the cache holds is `snapshot`
when its own snapshot would seed (`ownSnapshotUnusable` is null), else
`local` with `at` the file's modification time when a local snapshot
exists, else `indexing` with `local` false until one is built, and both of
those queue a refresh; anything else is `none` with `could not be read at
start`.

Index, in `local` or `push` mode, for a member that is `none`, or `failed`
without a local snapshot:

1. In push mode the base is the member's own snapshot (`base`) and its
   `store-id` is created when it has none (`ensureStoreId`); in local mode
   the member's id is only read.
2. The scan writes a scratch index, `nf-blocks-index-<alias>-<random>.sqlite`
   in the session's temp directory, which records that id, through `Index.indexMember`, and
   never explore's cache. `Index.ingestRun` holds one transaction across all
   of a run's block reads, so a scan into the cache on a second connection
   would hold SQLite's write lock for as long as a large run takes to read
   and `Put`'s 5 s `busy_timeout` would expire.
3. Phase `snapshot`: `buildMemberSnapshot` builds the member's snapshot from
   the scratch index, as `nf-blocks:snapshot` would against that member, with
   its presence check and its `sweep.lock` read.
4. Under `Put`'s monitor, `shared.seedFrom` copies the scratch rows into the
   cache in one transaction, then the built file replaces the local one
   (`LocalSnapshotStorage`), and the state is `local`. This is the only
   moment a job touches the cache, it is local SQL only, and a save, export
   or handoff meanwhile waits on the monitor rather than failing. The cache
   gets a member's rows whole, at the end of a successful scan, or not at all.
5. Push mode then runs phase `push`: `pushMemberSnapshot` writes
   `index/v7.sqlite` into the member with the writable member's conditional
   write and its guards, and the member's `store-id` when it had none; it
   never writes `index.html`. When written, the local directory is deleted
   and the state is `snapshot`. When a guard refuses, the state is `failed`
   with `local` true, and the local index stays. Guard reasons read:
   `fewer_runs`, `the store's own snapshot lists more runs than this index,
   so it was kept`; `older_watermark`, `the store's own snapshot is newer
   than this index, so it was kept`; `replaced_meanwhile`, `another writer
   replaced the store's snapshot meanwhile, so it was kept`; `stale_store`,
   `the store was replaced while it was being indexed`; any other code as it
   is.
6. The scratch file, its `-wal` and `-shm`, and the built files are deleted
   however the job ends.

Push to store on a member that is already `local` skips the scan: it creates
the `store-id` when absent, tops the cache up under `Put`'s monitor
(`catchUpIndex`, which also records the new id), and pushes from a
job-owned connection. A refresh, for a `local` member at start or after a
copy into it, tops the cache up under the monitor (phase `log`), rebuilds
the local snapshot on a job-owned connection (phase `snapshot`), and
replaces the file; the member keeps serving the old file until then.

A failed job leaves the state `failed`, `mode` kept, `at` now, `local` true
when a local snapshot is still served, and the reason from `reasonFor`:
`S3Failures.credentials` wording when there is one, else
`CopyErrors.describe` with the config's roots, so paths are redacted. A scan
that fails part way, such as credentials that expire after the listing,
marks nothing, so the member stays not indexed. An interruption (the
progress callback throws `CancellationException` once the thread is
interrupted, or an `InterruptedException` arrives) returns the member to
the state it had before the job, and reports nothing as failed. A copy
into a member that serves a local snapshot queues a refresh of it
(`copied`). All of this text is generic: no profile names or login
commands, and no store name but the member's own alias.

The exit. The hook calls `ExploreCommand.stop(started, err)`: `server.stop()`
first, so a write or copy in flight finishes; then `jobs.stop(10_000)`,
which clears the queue, interrupts the running job and waits up to ten
seconds; then the put-locked `index.close()`; then the final refresh of the
members. The exit is bounded, nothing is scanned at exit, and a member whose
job was stopped stays as it was, so the next start lists it as `none`.

Copies. `POST /api/copy` builds each copy with
`CopyService.Indexing(unindexed, localRoots)` from `IndexJobs.unindexedForCopy`
and `localRootsForCopy`, taken when the copy starts, and §21 says what it
does with them.

### The page

- Store: `?store=<base URL>` if given; else, if `./members.json` answers, the
  member `?member=<alias>` or the first listed, at `./m/<alias>/`; else the
  page's own directory (spec section 5.1).
- `?cap=<bytes>` overrides the 64 MiB whole-file cap.
- Routes (location hash): `#/` home, `#/idle` (opens the snapshot and does
  nothing else), `#/pipeline/<name>[?offset=<n>]`, `#/run/<completion>`,
  `#/collection/<collection>[?offset=<n>]`, `#/item/<collection>/<item>`,
  `#/content/<cid>` (query 1), `#/block/<cid>` (any block by its address),
  `#/latest/<pipeline>` (query 2),
  `#/items/<completion>/<output>?where=<JSON [[path, type, value], ...]>`
  (query 3, per run; `type` is one of `string`, `int`, `float`, `bool`; `null`
  was dropped 2026-10-24, since nulls are not indexed; *amended 2026-10-24:*
  an entry may be `[path, type, value, op]` with `op` one of `>`, `>=`, `<`,
  `<=`, a numeric range run with `itemsRange[op]`),
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
| `[data-block]` | the block page; its value the CID (2026-10-14) |
| `[data-block-check]` | on the block page: `verified`, `unchecked` (a raw block over 1 MiB), `refused` (hash mismatch) or `missing` |
| `[data-address]` | an address link to `#/block/<cid>`; its value the CID |
| `[data-icon]` | a kind's icon; its value the kind |
| `[data-output-index]` | a collection's Output Index File, when it was published: `data-output-index` its address, holding an `<a download>` to its block (ticket 26); since 2026-10-14 under the collection page's Details, as Nextflow wrote it |
| `[data-index-stable]` | the button that saves a collection's Output Index File with each Publish Coordinate replaced by its address (run page and collection page, 2026-10-14); `[data-index-status]` beside it says how many paths were left as written, or why it failed |
| `[data-output-index-missing]` | a collection's Output Index File, when it was never written: `data-output-index-missing` the publish path it was to have (ticket 26) |
| `[data-page]` | on the pipeline, collection and Selections views, when the list is not empty: `data-first` and `data-last` (1-based, inclusive; `data-first` is 0 on a page past the end) and `data-total`; `[data-page-next]` and `[data-page-prev]` link to the pages either side |
| `[data-producer]` | one query 1 row: `data-content`, `data-item`, `data-collection`, `data-completion`, `data-filename` |
| `[data-latest]` | query 2's answer, a completion cid or empty |
| `[data-item-result]` | one query 3 item cid |
| `[data-preview-for]` | the items table row: `data-preview-for` the item cid (item rows dropped their pills on 2026-10-14) |
| `[data-pick-all]` | "Pick all N" on query results and collection pages: `data-via` the collection, `data-count` N; adds every item of the query or collection, not only the page (milestone 3; labelled "Add all N to the tray" before layout B) |
| `body[data-write]` | `available` or `unavailable` |
| `body[data-write-seq]`, `body[data-write-outcome]` | a counter bumped when a write attempt ends, and how: `written`, `exists`, `elsewhere`, or an error code; a save whose naming or restoring failed after the Selection saved records `written` (decision 23) |
| `body[data-written]` | the address the last successful write made |
| `#tray[data-count]` | items in the tray, shown as "Picked (N)" (layout B) |
| `#tray-unsaved` | shown, with a plain-prose note, while the tray's last write to `sessionStorage` failed |
| `[data-pick]` | a button adding `data-pick` (an address) with `data-via` (space-separated collections) and `data-kind` (`item` or `selection`); on query 3 results `data-via` is the run's Output Collection (milestone 3) |
| `[data-tray-entry]` | an entry of the panel's picked list: `data-tray-entry` address, `data-kind` (on `#/compose`'s page before layout B) |
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
| `[data-snippet]` | the `<code>` holding one consumer call line, on the panel; at most one per mode on a page: `untyped`, `channel.fromStore(...)`, or `typed`, `nextflow.Channel.fromStore(..., records: true)`. The mode is the bare string in `localStorage` key `nf-blocks.snippets` (milestone 3; on the Selection and run pages before layout B) |
| `[data-snippet-mode]` | one of the two toggle buttons, `untyped` or `typed`, that switch every snippet on the page; `aria-pressed` marks the current one (milestone 3) |
| `[data-panel-state]` | the "Use in a workflow" panel's section: `none`, `whole`, `filtered`, `picked` or `saved`, the panel's state (layout B spec section 6.1) |
| `[data-where]` | on the panel's call `<code>` when the items view is filtered: the chips as JSON `[[path, type, value], ...]`, the same conditions the call's `where:` carries (layout B) |
| `[data-run-mode]` | the panel's run switch: `this` or `latest` (layout B) |
| `[data-columns]` | on the items table: its column paths as JSON (layout B) |
| `[data-fold]` | a folded section (`<details>`): `storage`, `details`, `lineage`, `filter`, `meta` or `config`; whether it is open survives a re-render for the life of the page (layout B) |
| `[data-anomaly]` | one Lineage check line on the run overview: `data-anomaly` the anomaly's key (layout B) |
| `[data-hidden-runs]` | on a pipeline page, how many runs a delete Claim hides |
| `[data-error]` | an error: `data-error` code, `data-cid` when a block is to blame |
| `window.__nfBlocks.verified` | every cid whose bytes the page hashed and accepted |

Error codes: `no_snapshot`, `no_range_over_cap`, `cors_headers`,
`fetch_failed`, `query_failed`, `worker_failed`, `snapshot_changed`, `hash_mismatch`,
`schema_invalid`, `block_missing`, `not_found`, `bad_route`, `bad_predicate`,
`file_protocol` (the store resolved to a `file://` URL, which a browser will
not fetch from: the page must be served, by any static server or `explore`),
`server_gone` (a read or write failed and the server that served the page no
longer answers a `HEAD /`, as when `explore` is stopped; it replaces whatever
code the failure had). A write can also show, in its status and in `body[data-write-outcome]`, the
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

*Amended 2026-10-25 (explorer v0.3 P3):* `POST /api/copy` (§15, §21) has
its own error body, plain JSON `{"error": <code>, "message": <text>}` with
no `at`, and its own codes: CopyService's 400 codes (`bad_root`,
`unknown_member`, `same_member`, `not_a_target`, `root_not_found`,
`bad_depth`, `bad_claims`, `bad_request`, `bad_config`), `copy_running`
(`409`, a real copy while another runs on the same server),
`superseded` (`409`, a dry run a newer one replaced while it waited, D10),
`stopping` (`503`, a dry run still waiting when the server stops) and
`copy_failed` (`500`, CopyService threw; a copy that failed part way is a
`500` with the §21 body instead). A real copy refused before any write is
a `409` whose body is the §21 plan with `refused` `not_kept` or `audit`,
not this error body. Its transport refusals are the plain-text
`403`, `415` and `413` above, except that it takes `application/json`
only.

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

Fourteen assertions (spec section 1.3, tier B), all local: a Selection made in
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
real launcher writes the named Selection at the Gate's address (19); the run
page's Pin, Release content and Restore content write the Claims they
describe (20); and, since layout B, the items view's filtered call, run
verbatim, stages exactly the items the page listed (21). B9's and
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
seeding fallback; since P1b, the identity lines and the storage-class
warning), go through the logger `nextflow.cas` and appear on the
terminal as well; every other plugin log line, §0 rule 3's warnings
included, is in `.nextflow.log` only. Gate
assertion 6 requires the `outputDir` line in the consumer's `nextflow.log`
and on its console (`logs/consumer/stdout.log`).

That is a run. A `nextflow plugin nf-blocks:<verb>` command, `explore`
included, writes no `.nextflow.log` and keeps stdout for its output, so its
`CasSession` is a command session (`CasSession.forCommand(config, err)`):
the same notices (the identity warnings, the `signing as` line, the
storage-class warning, the clock check's, and the index's missing-snapshot
and rebuild lines) go to the command's stderr, once each, and
`--format json` stdout stays one JSON value. `explore`'s stdout is the
`nf-blocks explorer: <url>` line and nothing else; its identity notices are
on stderr. The identity itself is resolved on first use (docs/identity.md),
so the verbs that ask Platform or git are the ones that sign (`put`,
`sweep --apply`, `prune --apply`) and `explore`, which resolves once it is
listening whether or not anything is written.

Left out, per the map: plugin factories on the typed `channel` namespace
(nextflow-io/nextflow#7694, upstream), self-registration of `fromStore` so
the include is optional, and a "one of" condition in the page and in
`fromStore(where:)`.

### Layout B (2026-09-30)

*Status: merged to main at `fc4f001` (PR #2) and released as 0.5.0-beta.1.*

Spec `docs/superpowers/specs/2026-09-30-explorer-layout-b-design.md`, plan
`docs/plans/2026-09-30-explorer-layout-b.md`. The page has three regions: a
left column (store status, `#members`, pipelines, recent Selections), the
middle view, and the "Use in a workflow" panel (`[data-panel-state]`, §15).
The panel replaces `#/compose`'s page: the picked list, `#compose-name`,
`#compose-save` and `#compose-copy` live in its `picked` state, and
`#/compose` renders the home view with the panel open. The consumer calls
(`[data-snippet]`) moved from the Selection and run pages into the panel.

Plan decisions, where the spec is silent or the Gate forced a refinement:

- P1. The left column queries the snapshot at load, and otherwise only on a
  reader's click or on the `#/run` and `#/pipeline` routes, so Gate assertion
  2's query-phase counts for `#/items` and `#/content` do not grow.
- P2. The panel names a run from its RunCompletion and RunManifest blocks
  (`Explorer.runIdentity`), not from the snapshot.
- P3. The run switch shows whenever the run's pipeline is known; "latest good
  run" runs `latestSuccessfulRun` only when clicked, and says so when there is
  none.
- P4. The `filtered` state says "M items of <output>, where ..." without the
  unfiltered total; the items view does not count rows the filter hides.
- P5. `#/compose` renders home with the panel open, with no redirect (tier B
  waits for exactly one render).
- P6. Each items row keeps a "Pick" button carrying `[data-pick]`, and gets
  an "Open" link to the item page; the row itself is not a link.
- P7. Folds remember whether they are open for the life of the page, so a
  write's re-render keeps Storage open (`B.retain` opens it with one click).
- P8. Float values in `where:` get a `d` suffix (`1.0E-5d`), so they type
  back to the index's `Double.toString` text.
- P9. (Retired 2026-10-23 for the items page, which now shows one column per
  file key with a pill per file:
  docs/superpowers/specs/2026-10-23-items-file-columns-design.md.) File chips
  dropped the prefix one item's files share, up to its last `.`.
- P10. The "same for every item" line needs at least two loaded items.
- P11. The item page has no "View" for text files: Download, and a "where
  else" link to the content page.
- P12. `[data-member]` stays on the Selection view (a Selection's member
  row); the store member links (`#members`) moved to the left column. The
  spec's section 10 is amended to say so.
- P13. The Collection, Content, Latest and Idle views stay, restyled into the
  middle column with breadcrumbs; Collection and Content get the Storage fold.
- P14. Panel and left-column failures show as `.warn` text without
  `[data-error]`, since tier A reads every `[data-error]` as the view's.
- P15. The panel's `saved` state has no Undo; delete and undo stay on the
  Selection view.
- P16. Only the same key at two paths is a duplicate column (`id` and
  `meta.id`); equal values alone never merge keys.

Controller rulings during the build that change what the page does:

- While a save runs, the panel holds redraws on the hash the save started on
  and draws once when it settles; a draw on a new hash proceeds.
- The panel's write status (`#write-status`) is kept across states after a
  write that did not navigate, and cleared on a draw whose hash differs from
  the one it was written on.
- The header's toggle reads "Use · N": the item count in `whole` and
  `filtered`, the picked list's size in `picked`, the Selection's members in
  `saved`, plain "Use" otherwise.
- The stylesheet is bundled into the page's script as strings and appended
  at start, not a separate file, so the page can show unstyled for a moment
  before the script runs.

Gate: tier B check 21 runs the items view's filtered call (step `B.where`,
`[data-panel-state=filtered]`, `[data-snippet][data-where]`) verbatim in
`gate/selection` and requires it to stage exactly the one item the page
listed in `[data-item-result]`.

### Stable references (2026-10-14)

The page hands out addresses, never a name that a later run or another
reader's config can point elsewhere. `run: 'latest'` stays, because the
reader picks it knowing it moves.

- The run page's outputs table has a "fields" column, every Meta Map path
  on the sampled items (a key at two paths once; three, then +N), in place
  of "keyed by"; "files" shows the distinct suffixes over the sampled items
  rather than one item's file names.
- The panel's "this run" call is `fromStore(run: 'cas://<RunCompletion>',
  ...)`, not `run: 'lid://<hash>'` (amends milestone 3 decision 13 and the
  layout B spec's run switch). A `lid://` is looked up in the reader's
  composition, newest first (`Index.runByNextflowHash`); the RunCompletion
  address names one run wherever it is read. A run with no lineage ID gets
  the same call. The `lid://` stays under Run details.
- An Output Index File names each file by its Publish Coordinate,
  `cas://<alias>/<path>` (§7), which the next run to that path overwrites.
  "index file" on the run page and the collection page saves it with each
  coordinate replaced by the address `fromStore` emits for that leaf (§13):
  `cas://<cid>/<name>` for a file, the Item Leaf for a directory
  (`web/src/outputindex.js`). The page reads every item of the collection,
  matches a coordinate to a publish path in `paths` at a segment boundary,
  so an `outputDir` below the alias still matches, and changes nothing else
  in the text. A coordinate it cannot match is left and counted. Nextflow's
  own file stays under the collection page's Details.
- `OutputCollection.paths[i]` is in the order `Join.build` walked the
  channel item, a `LinkedHashMap`'s, while a stored map comes back in
  canonical key order (§4), so for an item with leaves under two or more
  map keys `paths[i]` and the decoded item's leaves can disagree by
  position. Nothing in the plugin reads `paths`; the page pairs a path with
  a leaf by file name within the item, or directly when the item has one
  path and one addressed leaf. A path two items claim pairs with neither.

### Visual language and the block page (2026-10-14)

*Status: merged to main at `41bc018` (PR #4) and released as 0.7.0-beta.1. `./gradlew check` green; `cd web && npm test` 336 of 336; gate unit tests 333 OK (1 skipped). `make gate` on a fresh root: lineage 18 PASS, 0 FAIL, 5 SKIP; browser tier A (local) 6/6 including the new A8; tier B 14/14. `make gate-cloud` on the same root: browser tier A (cloud) 2/2 (A6, A7). Tier two not run against this release.*

Spec `docs/superpowers/specs/2026-10-14-explorer-visual-language-design.md`,
plan `docs/superpowers/plans/2026-10-14-explorer-visual-language.md`.

- One grey icon per kind (`web/src/icons.js`): pipeline, run, output, item,
  file, directory, Selection, Claim, address, store. An icon is a span
  painted by a CSS mask (`ICON_CSS`), so the test DOM needs no SVG.
  `ref(kind, label, href)` names a thing with its icon.
- `address(cid)` (in `icons.js`, since it draws an icon; the spec placed it
  in `html.js`) replaces `cid()`: a quiet link to `#/block/<cid>`. The short
  form is `shortCid`'s first 10 and last 6 characters, such as
  `bafyreian3...ac6uye`.
- `#/block/<cid>` shows any block: its CID decoded, the browser's hash
  check, its kind with a link to its friendly page, and its contents as
  DAG-JSON with each link an address. `[data-block-check]` takes four values:
  `verified`, `unchecked`, `refused` and `missing`. A RunManifest links to
  its run only when the Store Log tail names the run: the snapshot has no
  query by manifest, and adding one would change `Index.groovy`'s mirror.
- Block reads: `BlockFetcher.partial` reads the first 4 KiB by Range. A
  server that ignores Range answers 200 and the body is cancelled, unread.
  HTTP 416 means the block is empty (the size comes from
  `Content-Range: bytes */n`), and any other error status is `fetch_failed`.
  An empty raw block is `verified` with size 0. A raw block over 1 MiB is
  previewed by Range and left unchecked; `partial` never records it as
  verified, so A4 holds.
- `friendlyOf` trusts a string `kind` only when it is an own key of its
  table. Any other string kind, `__proto__` included, gets the generic line
  "a block of kind <kind>" with no link; a `kind` that is not a string gets
  "a block of kind unknown".
- Directory leaves are recognised by a dag-cbor address, read from a CID
  object or from its text.
- Item rows are two lines: the item and its label, then its files, "from"
  its run and output, and its address. Pills are on the item page and the
  items table only. A row says "not in this store" for an item held
  elsewhere and nothing when it is held here: the page knows only here or
  elsewhere, not which other store.
- The Selection page leads with its name, one quiet line of who named it
  and when and its address, and lists "Items".
- Gate tier A check A8 walks from a collection page to an item's block and
  on to its file's block.

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
Accepted 2026-09-30 with Rob's SSO session on `9f5463a`: `make gate-tier2`
8 PASS, 0 FAIL, 0 SKIP (T1-T6, T2b, TS), 93 Batch jobs. TS: nf-core/sarek
3.10.0 exited 0; `multiqc` holds 3 items, each verified against the member;
its index leaf hashes to `coords/multiqc/index.json`'s block;
`anomalies.unjoined` 40 equals the 40 `coords/` keys no leaf references. The
head node read 0 bytes in every Test Pipeline run, and 40,984 bytes for 172
files in sarek's (s3-copy 159, fusion-node 12, head-node 1). Merged to `main`.*

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

## 19. Milestone 6: retention (2026-09-30)

*Status: merged to main at `934c934` (PR #1) and released as 0.4.0-beta.1. Accepted on `e5fc807` after the whole-branch review and its fix wave: `./gradlew check` 1,270 unit tests and 4 `memoryBoundTest` features pass, `dependencyCheck` green; `web`, `node --test`: 205 of 205; Gate unit tests 312 OK (1 skipped) under `gate` and 53 OK under `gate/tier2`; `make gate` on a fresh `GATE_ROOT`: lineage 16 PASS, 0 FAIL, 5 SKIP, browser tier A 5/5, tier B 13/13. Tier two on 0.4.0-beta.1 (`e34b3bc`, 2026-10-01, `T2_TIMEOUT=3600`): 9 PASS, 0 FAIL, 0 SKIP (T1 to T7, T2b, TS; 93 Batch jobs). T7 on a real S3 member: `sweep.lock` taken with `If-None-Match` and released with `If-Match`, the ledgers objects under `trash/`, one `DeleteObjects` request for the released blocks, and `live/gate-fake` deleted once 662 s old by S3's clock; T7 waited 96 s for its blocks to pass the 10-minute age floor. Run tier two with `T2_TIMEOUT=3600`: T7 adds several minutes.*

Plan `docs/plans/2026-09-30-retention-milestone-6.md`; tickets
[20](../.scratch/post-gate/issues/20-retention-on-s3.md) and
[21](../.scratch/post-gate/issues/21-pins-as-content-roots.md) hold the
reasoning (each `## Answer` numbered), and the execution ledger is
`.superpowers/sdd/2026-09-30-retention-milestone-6/progress.md`.

Every run registers as a Live Writer in its writable member's `live/`
(ticket 20 answers 1 and 5): `CasObserver.onFlowCreate` calls
`CasSession.startLiveWriter` right after `checkClock()`, which writes
`live/<session>`, starts a 60 s heartbeat, and then blocks while a fresh
`sweep.lock` is held (polling every 30 s), so a sweep and a run always see
each other. `completeRun` calls `CasSession.stopLiveWriter` in its
`finally`, after `completionWritten()`, on the winning notification only,
deleting the registration.

This can precede every `fromStore` read and every publish because
`Session.start()` calls `notifyFlowCreate()` before returning, ahead of the
script (`Session.groovy:606` at v26.04.6, confirmed with
`git -C nextflow show v26.04.6:modules/nextflow/src/main/groovy/nextflow/Session.groovy`):

    Session start() {
        ...
        // signal start to trace observers
        notifyFlowCreate()

        return this
    }

1. A sweep never runs beside a Live Writer: it refuses to start (or, with
   `--wait`, waits) while any `live/` registration is fresh, and re-checks
   before every trash batch, every delete batch and before writing a ledger;
   a registration with no heartbeat for 10 minutes counts as dead. A run
   that cannot write its own registration warns and continues rather than
   aborting (DESIGN §0 rule 3 covers provenance, not this). It still reads
   `sweep.lock` and waits while a fresh one is held, without heartbeating
   (it has no registration to renew), so a sweep already running is waited
   out; a sweep that starts later cannot see it. The age floor protects a
   block such a run writes, but not an old block it dedups onto: one
   already in a ledger past its deadline, and dead in that sweep's mark, can
   be deleted under the run. [20] answers 1, 6, 7; plan decision 11.
2. Trash is a ledger, on both backends, never a tag, a copy or a rename:
   `trash/<deadline>-<sweep id>` lists the addresses one sweep trashed and
   their sizes; blocks stay in place and readable; a later sweep deletes
   only ledgered blocks past their deadline that are still dead in its own
   fresh mark, so a block that becomes reachable again is simply live, no
   `untrash` needed (§5). [20] answer 2; plan decision 14.
3. The age floor is judged against the store's own clock, stable per address
   because writes are conditional (S3 `LastModified`, local mtime); a sweep
   also clears `tmp/` staging keys and incomplete multipart uploads to keys
   under the member's `blocks/` or `tmp/` past the age floor, and no other
   upload, so a member at the bucket root (empty prefix) leaves other tools'
   uploads alone. `sweep.lock`, `live/` and `trash/` objects are written as
   STANDARD whatever `aws.client.storageClass` gives the blocks. [20] answer 3.
4. The mark (`Mark.of`, ticket 20 answer 4, ticket 21 answers 1 to 4; plan
   decisions 1 to 6) reads blocks level by level, in windows of about
   `threads * 8` blocks, expanding and dropping each decoded map before the
   next window (§0 rule 2). It reads, never trusts, the Index: deletion is
   where correctness beats speed. A run's content root is every OutputItem
   and OutputCollection Leaf address, every collection's `index` Leaf, and
   everything under a Leaf that is a Directory Manifest; its metadata root
   is the RunCompletion, its RunManifest and the RunManifest's `script`
   block. `set retain "lineage"` on a RunCompletion releases its content and
   keeps its metadata; `del retain` restores the content; a conflicted
   `retain` group keeps everything, never consent to sweep.
5. A pin (`add pin "<note>"`) keeps its subject's whole content closure — a
   run's every Leaf, a collection's or item's Leaves, a Directory Manifest
   and everything under it — plus, for a pinned run, collection or item, its
   own metadata too, so the page can still explain pinned content that
   `delete` hid. Several current `add pin` Claims on one subject are a set,
   not a conflict (`add` groups are exempt from the one-current rule, like
   `name`); `del pin` supersedes one. A pin beats `retain "lineage"` for what
   it pins and beats `delete` for content (the run stays hidden; the
   explorer warns "hidden but pinned"); pins cannot conflict with each
   other. [21] answers 1 to 4; plan decisions 1 to 6.
6. `prune` reads the Index (`pipelines()`, `runsOf(pipeline)`) and writes
   `set retain "lineage"` per Pipeline Identity through the same `Put`
   builder `put` uses, one `Put` per release, in one `--apply` call: with
   both `--keep-last` and `--keep-newer`, a run is kept when either keeps
   it; without `--pipeline`, every pipeline is pruned separately; a run
   whose `retain` group is already conflicted is skipped and reported; a
   pinned run releases as usual, since its pins hold regardless. [21] answer
   5; plan decision 12.
7. `sweep`, `prune` and `untrash` land in the CLI (§0 rule 5, §15): dry runs
   by default, `--apply true` to act; `untrash` always acts, and exits 1
   when another sweep holds `sweep.lock` or when nothing named is in any
   Trash ledger. `put` stays the only way to write a pin or a `retain`
   Claim by hand. [07], [20] and [21] throughout.
8. Gate assertion 9 (§14) and tier two's T7 (`gate/tier2/README.md`) exercise
   the sequence above end to end, T7 on an S3 member, also exercising the
   lock and the ledger. [21] answer 7.

### Decisions made where the tickets are silent, as built

Recorded here per the plan (`plan-decisions.md`, `.superpowers/sdd/2026-09-30-retention-milestone-6/`);
a reviewer could reject any of them.

1. A run's closure, for the mark: metadata is the RunCompletion, its
   RunManifest, the RunManifest's `script` block, every OutputCollection and
   every OutputItem; content is every Leaf address of every item and each
   collection's `index` Leaf, and, for a Leaf that is a Directory Manifest,
   the manifest and everything under it. A Directory Manifest is content,
   not metadata: it goes when the run's content is released.
2. A pinned run keeps its whole closure, metadata included, even hidden by
   `delete` (ticket 21 answer 3 names "the subject's own metadata block");
   a pinned collection keeps itself, its RunManifest, its items and their
   content; a pinned item keeps itself and its content.
3. A Selection with a clean `delete` is not a root, the same rule as a run.
   A conflicted deletion keeps it. Its via collections are kept with their
   RunManifest, metadata only (block explorer spec section 7.3).
4. Claims are live exactly when their subject is live (glossary Store Log:
   "a Claim is listed but kept only while its subject is"), superseded ones
   included; they are small and are the history. A Claim is also live when
   its subject is held by a read-only member (checked with `has()` on
   members after the first): the sweep cannot remove that subject, so
   removing its Claims would change its meaning (un-deleting it). A Claim
   block this build cannot parse (a field missing, or of the wrong shape,
   such as a `supersedes` that is not a list) is kept live and reported
   (`unreadableClaims`), never swept — wherever Claims disagree, content is
   kept. A RunCompletion's `input_set`, when it is a link (milestone 7
   writes one), is metadata of the run, kept with the RunManifest.
   *Amended 2026-10-25 (P4 R1):* the InputSet is kept as itself only. Its
   input addresses are not followed: a run does not own what it read, so
   an input nothing else reaches is trashed even while the run is held.
   *Amended 2026-10-26 (cross-run item lineage):* a non-null
   `RunCompletion.item_lineage` is kept as itself the same way, as run
   metadata, and nothing under it is followed. Its links name this run's
   items and InputSet, which the run keeps anyway. `Input.from` is not
   followed either: an Origin names another run's records, possibly in
   another member, and a consumer run does not keep upstream content alive.
   A build that predates this change and sweeps a member holding schema 3
   RunCompletions trashes ItemLineage blocks; there is no shim, since one
   build is in use.
5. Only `set retain "lineage"` releases. A `set retain` with any other value
   keeps content, so a later value cannot be misread as consent by this
   build.
   *Amended 2026-10-25 (explorer v0.3 P4, F5, behaviour change):* a pin may
   name a Selection. Ticket 21 answer 6 is "pins on any subject"; Put's
   `PINNABLE` had left Selections out as an implementation choice, not a
   decision, and the P2 page's own Pin… on a Selection was refused with 400
   `wrong_kind`. Put now accepts `add pin` and `del pin` on a RunCompletion,
   OutputCollection, OutputItem, DirectoryManifest or Selection, and on any
   raw block the composition holds. It refuses a pin on a Claim, Handoff,
   RunManifest, InputSet or unknown kind as `wrong_kind`, on a Project as
   `invalid` at /attribute, and on an absent raw block as `not_found`. A
   pinned Selection keeps its whole closure even after a clean `delete`, as
   a pinned run does, judged by D2 (a pin in any member's log keeps it); a
   `del pin` superseding the last pin, then a clean `delete`, lets a sweep
   take it. Mark and the storage figures needed no change: they already
   judged pins per subject whatever its kind, so a pinned Selection is a
   pin root seeded with its items and counts as pinned in `store_stats`,
   `root_stats`, `asserter_stats`, `pinned_overlap` and `project_stats`.
   The snapshot format is unchanged.
6. A mark that cannot read a metadata block it needs refuses `--apply` (exit
   1, naming up to 20), because the blocks under it are unknown. A missing
   content block (raw or Directory Manifest) is reported, not refused: it is
   already gone, and what was under a gone manifest is protected only by
   other roots. (*Amended 2026-10-25, found in P4:* as built, only a missing
   Directory Manifest is reported. Mark never reads a raw block, so a
   missing raw file, under an item or a Handoff, goes unreported.) To get past the refusal, either restore the missing block
   (from a copy, or by re-running what wrote it), or, if what needs it
   should go, `put` a `delete` Claim on the root that reaches it: the
   report's `needed by` names the block linking to it, which leads up to a
   RunCompletion or a Selection. A root with a clean `delete` and no pin is
   not walked, so its missing metadata no longer blocks the sweep, which
   then trashes the rest (a pinned root also needs its pins removed with
   `del pin`, and any other root still reaching the block keeps needing
   it). A Store Log entry whose own block is in no member is
   dangling: not a root, reported, and deleted from the writable member's
   log by `--apply`, but only once older than the age floor (a conservative
   narrowing of the plan: a fresh dangling entry could be one a concurrent
   writer has not finished with).
7. After deleting a block the sweep deletes its Store Log entries in the
   writable member, then removes the index rows keyed by it
   (`Index.forget`) and rewrites the writable member's Index Snapshot.
   Producer rows for released content stay: the lineage says who produced
   the bytes that were released.
8. Trash timing: one sweep writes at most one ledger, after its mark, so an
   interrupted sweep before that write changed nothing. The age floor and
   the grace period default to 14 days each. The age floor may not be under
   the Live Writer stale threshold (10 minutes), because a run whose
   heartbeat stopped counts as dead after that and may still reference what
   it wrote; the grace may be 0. Either under a day warns. A block trashed
   by two ledgers (in two members, or across a lock loss and a retry) goes
   by the later deadline, not the earlier one — a conservative narrowing.
   A ledgered block that becomes live while no sweep runs, and is released
   again before the next one, is still in its old ledger, so that next
   sweep deletes it once it is dead and past the ledger's original
   deadline, with no fresh grace: only a sweep that sees it live drops it
   from its ledger. Pin it, or `untrash` it, to restart the clock.
9. `--budget <size>` caps the bytes a sweep newly trashes, in address order.
   What it deletes was capped when it was trashed. No `--budget` means no
   cap; `--budget 0` (or any size that is 0 bytes) is a usage error, exit 2,
   rather than a second way to say "no cap".
10. Before each delete batch and before writing the ledger the sweep
    re-checks: its own lock (a heartbeat that throws counts as lost, the
    same as one that is refused — a transient store error must stop
    deletion rather than continue unlocked), live registrations (a fresh
    one stops it, releasing the lock so the run proceeds), and the Store Log
    of every member (entries written since the mark are marked into the
    live set, never out of it). After a lost lock the ledgers already on
    disk are left untouched rather than rewritten — a conservative
    narrowing that can leave a ledger listing a block another sweep has
    since deleted, harmless since deletion is idempotent. Due Claims are
    deleted last, only after every other due block went (no stop, no failed
    batch, no block S3 would not delete), and a due Claim whose subject is
    still in the writable member and was not deleted in the same pass is
    held back in its ledger: a Claim deleted ahead of its subject (a
    `delete`, or a `del pin` on a hidden run) would make the next mark read
    that subject as undeleted, a content root with half its metadata gone,
    refusing every later `--apply`. A stop request (Ctrl-C) is also checked
    between the mark's windows, so a sweep interrupted while marking ends
    there and releases the lock; if a sweep has not ended 30 s after Ctrl-C,
    the stop hook releases the lock itself before the JVM exits, and the
    sweeping thread's next heartbeat then fails, stopping it before its next
    step. A `--wait` that is still waiting ends at Ctrl-C too.
11. A run that cannot write its registration warns and continues; a Claim or
    Selection written during a sweep is caught by the Store Log re-check
    unless it lands between the last re-check and a delete batch — that gap
    is documented, not closed (see below). Heartbeat failures on the run
    side (the Live Writer, not the sweep) warn once on the first failure and
    once on recovery, never throw, matching "registration failure warns".
12. `prune` reads the Index; with both `--keep-last` and `--keep-newer`, a
    run is kept when either keeps it. Without `--pipeline`, every Pipeline
    Identity is pruned separately. A run whose `retain` group is in conflict
    is skipped and reported; a pinned run is released as usual (its pins
    hold).
13. `untrash` takes the sweep lock, exiting 1 if another sweep already holds
    it, removes the named addresses (or a whole sweep with `--sweep <id>`)
    from the ledgers, and says that a block still unreachable is trashed
    again by the next sweep, so the way to keep it is a pin or `del retain`.
14. Ledger format, not a block: JSON
    `{"sweep": <id>, "trashed_at": <iso>, "deadline": <iso>, "blocks": [{"cid": <text>, "size": <int>}, ...]}`,
    blocks sorted by cid, named `trash/<13-digit deadline epoch millis>-<sweep id>`
    so a listing sorts by deadline. Sweep id `<yyyyMMdd'T'HHmmss'Z'>-<8 base32 chars>`.
15. Lock body: JSON `{"sweep": <id>, "state": "held"|"released", "started_at": <iso>, "beat": <int>}`.
    A registration body: JSON `{"session", "run_name", "pipeline", "started_at"}`.
16. Task 8: `prune` counts every successful run toward `--keep-last N`
    whatever its Claims (a released or pinned run still occupies a slot,
    since the count is about how many runs exist, not how many still hold
    content).
17. Task 10: the Live Writer registration is written at `onFlowCreate`
    (`Session.groovy:606`, above), not at first publish, so it covers every
    `fromStore` read too, and a pipeline that only reads a member is still
    unregistered there (below).
18. `sweep --apply --format json` prints one JSON object on stdout, the
    forget-and-snapshot outcome folded in as a `snapshot` field rather than
    a second line, so a `--format json` reader never has to skip a trailing
    text line; text mode keeps the separate `snapshot   ...` line. `--apply`
    exits 0 only when it applied.
19. The mark reads each level in windows of about `threads * 8` blocks
    (`Mark.WINDOW_FACTOR`), expanding and dropping each decoded map before
    the next window, rather than a whole level at once (§0 rule 2 outranks
    the plan's level-at-once sketch).
20. The Gate's browser tier-B check for this milestone is assertion 20, not
    "B13" (13 was already taken by an earlier tier-B row); tier B is
    reported as 13/13.
21. Tier two's T7 runs its sweep steps after T6 and before `wait "$TS_PID"`,
    overlapping the background nf-core/sarek run (TS), rather than after TS;
    its retention closures are computed only at the after-runs checkpoint.
    `T2_TIMEOUT` (ticket 11 decision 6) stays 2700 s, cheaper than raising
    the watchdog; it can be raised by environment if T7 makes a run tight.

**Not protected, documented rather than closed:** a pipeline that only
reads a member (`fromStore`, no publish) cannot register as a Live Writer
there, so a sweep does not wait for it and a mid-run input can vanish under
it (ticket 20 answer 7); and a Claim or Selection written in the narrow gap
between a sweep's last re-check and the delete batch it guards is not
caught by that re-check (plan decision 10, decision 11).

*Amended 2026-10-25 (P3 final review item 2):* `nf-blocks:copy` writes
like a run, so a real copy (never a dry run) registers as a Live Writer in
its destination too: after `checkClock`, `CasSession.startLiveWriter` with
`run_name` `nf-blocks:copy`, waiting out a held `sweep.lock`, and
`stopLiveWriter` when the copy ends. A sweep's report lists it beside runs.
The copy then asks `has` again of the blocks its plan counted present
(§21, write order).

### Across members (explorer v0.3 P3, decision D2, 2026-10-25)

*Amended 2026-10-25 (plan section 1.8, as narrowed while building;
`Mark.verdictOf`, `CrossMemberReleaseTest`):* before P3 the mark read every
member's Claims as one set, so a release logged only in member B let a
sweep of member A, from a session that included B, take content that A's
own storage figures (§15, computed from A's own Claims) called held. The
rule now:

- A run or Selection that A's own Store Log lists (a `run` or `selection`
  entry in the writable member's log) is released, or deleted, only when
  both the whole composition's Claims and the Claims A's own log lists say
  so.
- A run or Selection logged only in other members is judged by the
  composition's Claims alone, as before P3. A's figures do not cover it,
  and judging it by A's (empty) own Claims would keep A's copies of its
  content forever, with no way to release them from A.
- Pins count from any view, for every subject: a subject is pinned when the
  composition's Claims pin it, when A's own do, or when the Claims any one
  member's log lists do. A `del pin` logged in one member does not unpin
  what another member's log still pins.
- A Claim cid logged in two members counts in both members' views. A Claim
  already read that a later re-check (`extend`) finds newly listed in
  another member's log, or in A's, re-roots its subject, and so does a run
  or Selection that appears in A's log after the mark first saw it in
  another member's.

So, for what A's log lists, a sweep of A never takes what A's figures call
held. Roots logged in other members still protect A's blocks during a sweep
(unchanged). Two mismatches remain, and both are safe (the sweep keeps more
than the figures say): A's "freed on release" can include a block that a
pin in another member still keeps; and when A's own release is superseded
elsewhere (a `del retain` logged in B), A's figures say freed while the
sweep keeps the content. A copy never carries `retain` or deletion Claims
and carries pins only when asked (§21), so copying does not move a
retention decision between members.

*Amended 2026-10-25 (plan section 1.5):* a `transfer` Store Log entry roots
its own Transfer block only, as a `forget` entry does (§20 decision 8). Its
`roots` and `runs` are not followed: the `run`, `selection` and `claim`
entries the copy wrote root those. A `transfer` entry whose block is in no
member is dangling, like any other root entry. A Transfer block with no
entry (a copy stopped between the two) is not a root, so a sweep trashes it
like any other dead block. Format note: a build older than P3 ignores
`transfer` entries (§5), so its sweep would trash every Transfer block and
leave its entry dangling; a later P3 sweep reports that entry and deletes
it once past the age floor.

## 20. Cache coherence (2026-10-09)

*Status: merged to main at `4a3294f` (PR #3) and released as 0.6.0-beta.1.
Built on `design/cache-coherence` (2026-10-09), with the final
review's fixes (build decisions 19 to 25). `./gradlew check` passes (1335
tests, 0 failures); `cd web && npm test` passes (281 tests, and the schema
test still finds 24 types); `python3 -m unittest discover -s gate` runs 327
tests OK (1 skipped) and `gate/tier2` 53 OK. `make gate` on a fresh
`GATE_ROOT` (commit 04c0121): lineage tier 18 PASS, 0 FAIL, 5 SKIP; browser
tier A 5/5, tier B 14/14. Tier two on 0.6.0-beta.1 (`8a75f5f`, 2026-10-14,
`T2_TIMEOUT=3600`): 9 PASS, 0 FAIL, 0 SKIP (T1 to T7, T2b, TS; 93 Batch
jobs), teardown clean. Where the build
differs from the decisions below, the decision carries an "Amended by" note
and the reason is under Decisions made while building.*

The cache index (§12) is per user and lives outside the store, and catch-up
only ever adds: it reads the Store Log past its watermark and ingests what
it finds. Two things the store can lose are never seen by a cache that
indexed them first.

- The whole member. `rm -rf` a local member and run again at the same
  location: the cache file name is a hash of the location texts, so the same
  cache opens, keeps every old row, reads the one new entry, and the next
  snapshot writer exports the old runs into the new store's
  `index/v3.sqlite` (found 2026-10-09 in `scratch-workflow/`: 7 runs in the
  cache, 1 entry in the member's log).
- Part of a member. A sweep (§19 decision 7) calls `Index.forget` on its own
  cache only. Every other cache (another person, another machine, a CI job,
  a project whose config holds the member read-only) keeps the rows of the
  runs, collections, items, Selections and Claims the sweep deleted. Those
  runs are hidden there already, since a run is only swept after a clean
  `delete` Claim the other caches caught up on, but their rows point at
  blocks that are gone and travel into the next snapshot those caches write.

A third gap is in the snapshot guard. `fewer_runs` (§15) keeps the old
snapshot whenever the new one has fewer `run` rows, so the sweep's own
rewrite is refused whenever it deleted a RunCompletion, and a stale cache
with more rows than the store still holds is let through (found by reading
`IndexSnapshot.write` and `RetentionCommands.forgetAndSnapshot`; Gate
assertion 9 releases content only, so no sweep in the Gate deletes a run).

Three changes close the first two gaps and the guard. Blocks removed outside
the plugin (an S3 lifecycle rule, a manual `rm` under `blocks/`) stay out of
scope: nothing finds them without listing every block, so the answer is the
`reindex` verb (decision 12).

### Store id

1. Each member holds `store-id`, at its root beside `index.html`: one line,
   a random 128-bit id as 32 lowercase hex characters, then `\n`. Not a block,
   not a root, not swept, never rewritten. It names one incarnation of the
   member: a member deleted and written again at the same location gets a
   new one.
2. The first write to a writable member creates it (`CasSession`, before the
   Live Writer registration, so before anything else the run writes there):
   locally a temp file linked into place with create-if-absent, the block
   write's rule (§5); on S3 a `PutObject` with `If-None-Match: *`. Losing the
   race is success; the writer then reads the winner's id. `sweep`, `put`,
   `snapshot` and `explore` create it the same way when they hold the member
   writable. A read-only member is never written; its id is read if present.
3. *Amended by decisions 1 to 4 below.* The cache records `store_id:<member alias>` in `meta` the first time it
   catches up a member that has one. Every `catchUp` reads the member's
   `store-id` first (one small read per member per open; catch-up already
   lists the log, so the cost does not change in kind) and compares:
   - same id: catch up as now;
   - a different id, or the cache holds one and the member no longer has
     one: the cache drops the member's rows (decision 4), then catches up the
     member as a cold cache does (§12, ticket 04), seeding from the snapshot
     only under decision 9;
   - the cache holds none and the member has one (a cache from before this
     section, or a member another user just upgraded): adopt it only when the
     cache's `store_log_watermark:<m>` entry is still in the member's log
     (one `exists` on the entry name); otherwise drop and catch up cold. A
     watermark entry a sweep has deleted (§19 decision 7) costs a rebuild,
     never a wrong answer;
   - neither holds one (a read-only member nobody has written since the
     upgrade): catch up as now. This is the one case left unprotected.
4. *Amended by decision 1 below: a changed id resets the whole cache, not
   only the member's rows.* Dropping a member's rows reuses `Index.forget`: the cids of every `run`
   with `run.member` = the alias and every Selection and Claim whose only
   `log_entry` row names the member, and the member's `log_entry` rows, then
   `store_log_watermark:<m>`, `block_scan:<m>`, `seeded_from:<m>` and
   `store_id:<m>` from `meta`. A row another member also announced stays.
   Logged at info on `nextflow.cas`, naming the member and both ids.

   *Amended 2026-10-25 (explorer v0.3 P2):* the cache also keeps, per member,
   the cache-only table `block_absent(member, cid)` (§15, the presence
   check), and these `meta` keys, which the Index Snapshot writer reads and
   writes:
   - `snapshot_bytes_per_ref:<m>`: the learned cost per `block_ref` row of the
     content and storage tables, from a full build of at least 1,000 refs. The
     estimate starts at 400 B.
   - `snapshot_over_cap:<m>`: "cap bytes runs omitted" of a lean snapshot that
     was still over the cap. Later writes skip the build while the cap is the
     same and the member has no fewer runs.
   - `snapshot_out_of_memory:<m>`: "refs heap" of a build whose figures ran
     out of the Java heap ("Java heap space" or "GC overhead" only). At as many
     references and no more heap, the next build goes lean at once.
   - `snapshot_seeded_lean:<m>`: set when the cache was seeded from a lean
     snapshot. Its value is that snapshot's `storage_omitted`.
   - `snapshot_base_figures:<m>`: whether an S3 base snapshot without
     `x-amz-meta-storage` has storage figures, keyed by its ETag, so a
     lean-seeded cache reads such a base once and not at every write.

   A second cache-only table, `block_present(member, cid)`, remembers blocks
   found present among the ever-exposed ones. Those are blocks reachable from a
   run or Selection whose Claim history holds a release or deletion, now also
   reached by a held or pinned root. With it, a cold cache (after `reindex`,
   say) checks them once rather than counting a swept one again.

   A row goes:
   - when its block becomes releasing-only, which is checked as before;
   - when no held or pinned root reaches it.

   Every member's rows go together:
   - whenever the cache ingests any Claim. Any Claim may supersede a pin or a
     release, whatever its attribute, so any Claim can change what a sweep
     may take. This includes a Claim first logged in this member after another
     member's log already brought it into the cache: catch-up records the
     new `log_entry` and drops the rows in one transaction;
   - at a seed;
   - at a forget.

   Each such drop bumps `block_present_generation` in `meta`. A build reads the
   generation before it reads the roots, and records presences only if the
   generation is still the same, in the transaction that inserts them. So a
   process that ingests a Claim mid-build cannot have its drop undone.

   A build also records no presences while the writable member's sweep lock
   says a sweep may be deleting (`SweepLock.mayBeSweeping`, through
   `RetentionStorage.readLock()`). That means a lock that isn't released, or one
   released less than 60 seconds ago, which covers an abandoned sweep's
   in-flight batch. It is checked at the build's start and again just before it
   records. A lock that can't be read counts as held. A release rewrites
   `sweep.lock` and never deletes it, so a lock that merely exists is not a
   sign of a sweep. This covers a sweep whose mark came before a restore and is
   still deleting: the build sees the restored block present just before the
   sweep's batch deletes it. A sweep that takes the lock after the build
   started marks from a Store Log that already holds the restore, so it keeps
   the block.

   A reset empties both tables and `meta` with the rest of the cache, except
   `block_present_generation`, which it keeps and moves on by one, so a build
   that began before the reset cannot write a memo into the emptied cache.
   An unreadable generation counts as 0.
5. The Index Snapshot's `meta` gains `store_id` (§15 lists `meta` as
   exactly two keys; it becomes three). The S3 snapshot also carries it as
   `x-amz-meta-store-id`, so the guard can read it from the `HEAD` it
   already makes.

### Forget entries

6. *Amended by decision 6 below: `deleted` lists every dag-cbor block, and a
   sweep of raw blocks only writes no entry.* New Store Log kind `forget`: `log/<rts>-forget-<cid>`, the cid of a new
   block kind, written to the writable member only by a sweep:

   ```
   type Forget struct {
     schema Int                     # 1
     asserted_by String
     deleted [&Any]                 # sorted by cid string, no duplicates
   }
   ```

   added to the `Block` union as `Forget "Forget"`. No timestamp and no sweep
   id (§6: nothing store-local); two sweeps that delete the same set write one
   block. `deleted` lists only blocks of the kinds the index keys rows by
   (RunCompletion, RunManifest, OutputCollection, OutputItem, Selection,
   Claim). The mark has decoded each of them, so the sweep knows the kind
   without a read; content blocks and DirectoryManifests have no rows to drop
   (producer rows stay, §12) and are left out, so a content-only sweep, Gate
   assertion 9's, writes no `forget` entry.
7. *Amended by decisions 7, 8 and 24 below: the Store Log entries are
   deleted once, after every batch, the sweeper's cache forgets only what
   could not be announced, and a batch whose delete fails is announced whole.* Order, per delete batch: delete the batch, then delete its Store Log
   entries (§19 decision 7), then `putDagCbor` the batch's Forget block and
   append its `forget` entry, before the next batch's re-check. A sweep that
   stops between a batch and its entry leaves rows behind in other caches;
   the next sweep cannot repair them (the blocks are gone from its mark), so
   `reindex` does (decision 12). The sweep's own `forget` call then reads
   from its own catch-up of these entries, not from the report, so the
   sweeper and every other cache take the same path.
8. *Amended by decisions 9, 15, 22 and 23 below: a cid is kept when any
   member of the composition holds it or the lookup fails, only cids the
   cache has rows for are looked up, and a `forget` entry whose block is gone
   is dangling.* Catch-up ingests a `forget` entry from member `m` by reading its block
   and calling `Index.forget` on each listed cid that no other member of
   this cache holds (`has` on each other member; a one-member cache checks
   nothing). The entry goes into `log_entry` like any other, and is retried
   as `missing` when its block cannot be read. Ingest is idempotent, so the
   10-minute overlap re-reads it harmlessly. A Forget block is a sweep root
   while its entry exists, and a sweep never deletes a `forget` entry as
   dangling. Entries are kept forever: one per delete batch, under 200 bytes
   of key and a block of about 40 bytes per listed cid. A reader older than
   this section warns once per entry ("unknown kind 'forget'") and ignores
   it, which is today's behaviour.

### Snapshot guard and seeding

9. A cold cache seeds from a member's snapshot only when the snapshot's
   `store_id` equals the member's `store-id`. A snapshot without `store_id`
   (written before this section) seeds only when its `store_log_watermark`
   entry is still in the member's log, the adoption rule of decision 3; a
   snapshot written from a stale cache, like the one in `scratch-workflow/`,
   names an entry from the old incarnation and fails it. A refused snapshot
   means the full scan, with the warning §12 already gives.

   *Amended 2026-10-25 (explorer v0.3 P4, schema 5, now 6):* a seed accepts only
   a snapshot whose `schema_version` is 6, at `index/v6.sqlite`; any other
   is unusable and the member is caught up in full (§12). A snapshot always
   carries the member's transfer, project, handoff and run_input rows, and a
   seed always copies them. *Amended 2026-10-26 (cross-run item lineage,
   now 7):* the snapshot is `index/v7.sqlite` and a seed accepts only a
   snapshot whose `schema_version` is 7; it also carries and seeds the
   `input_origin`, `item_input` and `item_task` rows of its runs.
10. *Amended by decisions 10, 12, 19 to 21 below: no `forgotten` table, the
    guards run in a fixed order, a base from another incarnation is not
    compared, and `reindex` and a sweep with unannounced deletes waive
    `older_watermark` and `fewer_runs`.* A writer refuses to replace a snapshot when:
    - its cache's `store_id:<m>` differs from the member's `store-id` after
      its own catch-up (`stale_store`; it cannot happen after decision 3
      ran, so it guards a writer whose catch-up was skipped or failed);
    - the base snapshot's `store_log_watermark` is newer than the new one's
      (`older_watermark`; Store Log names compare by their rts prefix,
      smaller is newer);
    - `fewer_runs`, kept against a reader that cannot read some runs (Gate
      assertion 13's locked blocks), but counted after adding back the runs
      that `forget` entries newer than the base's watermark removed. The
      cache records which `forget` entry removed each run, as
      `forgotten(cid, entry)`, a new table, not in the snapshot.

    `over_cap`, `replaced_meanwhile` and `catch_up_failed` stay as §15 has
    them. With these, a sweep that deletes runs rewrites the snapshot, and a
    cache that missed a sweep cannot export the swept runs: its catch-up
    applies the `forget` entries before it builds.
11. *See decision 13 below.* The page reads `store_id` from the snapshot's `meta` and shows nothing
    new; it is there for the guard and for a person reading the file.
    `web/schema-gen.mjs` regenerates for the Forget kind, and the page's
    block validation accepts it.

### `reindex`

12. *Built as written, with the snapshot written as a rebuild; see decisions 14 and 19 below.* `nextflow [-c <config>] plugin nf-blocks:reindex [--seed false]` deletes
    the cache file (and its `-wal`, `-shm`) for this config's members and
    catches up every member cold, seeding under decision 9 unless
    `--seed false`, then writes the writable member's snapshot. It is the
    answer for blocks removed outside the plugin and for a sweep stopped
    between a batch and its `forget` entry. Exit 0 when every member caught
    up, 1 otherwise, naming the member.

### Gate

13. Assertion 17, "a recreated member shows only its own runs": after the
    lineage runs, the Gate moves the store aside, runs the Test Pipeline
    once into a fresh store at the same location with the same
    `XDG_CACHE_HOME`, then runs `nf-blocks:snapshot`. Pass when the cache
    holds exactly the new run's `run` row, `meta` holds the new
    `store-id`, the member's `index/v3.sqlite` holds that one run with the
    same `store_id`, and a third cache seeded from it holds one run too. The
    moved-aside store and the old cache come back for the assertions after
    it.
14. Assertion 18, "a sweep reaches every cache", on a copy of assertion 9's
    `store-retention` after its last checkpoint, holding runs R1 (`--tag a`)
    and R2 (`--tag b`), with two caches, A and B, both caught up on it.
    Through A: `delete` Claim on R1, backdate with `--retention-age` as
    assertion 9 does, then `sweep --apply` under `gate/retention/grace0.config`
    twice (trash, then delete), so R1's metadata is deleted. Pass when the member's log
    holds a `forget` entry whose block lists R1's RunCompletion; A's
    snapshot rewrite reports written, not `fewer_runs`; then B, on its next
    `nf-blocks:snapshot`, holds no row keyed by any cid in that list, keeps
    R2 whole, and leaves the snapshot without R1; and a cold cache C seeded
    from the snapshot holds R2 and not R1.
15. Unit tests carry the cases the Gate does not: the adoption rule of
    decision 3 both ways, a forget entry naming a cid another member holds
    (rows stay), a stop between a batch and its entry, an S3 `store-id`
    race (two writers, one id), and each new guard reason.

### Open questions

16. Whether a sweep should refuse to start when the member has no
    `store-id` (it would always create one, so this is about a read-only
    sweep role on S3 without `PutObject` on the member root; the hardening
    policy in §5 only restricts `blocks/*`, so it allows the write).
17. Whether `reindex` belongs here or in the verify work §1 still lists.
    Answered in part: the verb landed here (decision 12, built decision 14
    below), because it is the only repair for the two cases §20 leaves out
    of scope. Whether a later `verify` should also offer it is still open.

### Decisions made while building

Numbers here restart at 1; "decision N" in an "Amended by" note above points
into this list, and "§20 decision N" below points back at the numbered
entries above. Each entry says what was built.

1. A changed store id resets the whole cache, not only that member's rows
   (changes §20 decision 4). Dropping one member's rows needs the closure of
   every run (collections, items, attrs, producers) minus the rows another
   member still announces, and most tables have no per-member ownership. A
   reset (every table emptied, `meta` cleared except
   `block_present_generation`, which moves on by one) followed by the usual cold
   catch-up of every member is always right, and the other members reseed
   from their snapshots. An id changes only when a member is deleted and
   recreated, so the cost is rare.
2. Ids are reconciled for every member before any member is caught up
   (`CasSession.catchUpIndex`), so a reset triggered by the second member
   never discards rows the first one has just caught up.
3. A member whose `store-id` cannot be read (an S3 403, the network) is left
   out of reconciliation for that call: no reset and no adoption. Failing to
   look is not evidence that the member was replaced.
4. The cache adopts an id only when it can show its rows came from this
   incarnation: it holds nothing for the member (no watermark and no
   `block_scan` mark), or its watermark entry is still in the member's log.
   A cache with rows and no watermark (a member written before the Store
   Log, then scanned) cannot show it, so it resets. When the log check
   itself fails, the member is left out, as in decision 3.

   *Amended 2026-10-25 (explorer v0.3 P3, plan task T10 and fix wave item
   18; `Index.reconcileStoreIds`, `attributesRowsTo`):* the cache holds
   nothing for a member when it has no watermark for it and no row it
   attributes to it, whatever the `block_scan` mark says. A block scan now
   records `block_scan:<m>` as `empty` when it found no RunCompletion,
   Selection, Claim or Transfer, and as `done` otherwise; a seed and a
   rebuild record `done`. The attributed rows are:
   - the member's `log_entry` rows, whatever the mark says. Only a
     catch-up of the member writes them, so an `empty` scan whose log
     catch-up threw before it set a watermark still leaves rows that a
     replaced member must not keep;
   - after a `done` scan only, a `run` row with `run.member` equal to the
     member, and any Selection, Claim or Transfer row that no `log_entry`
     row announces, anywhere in the cache, since nothing records which
     member's scan found it. This errs toward a reset. A session without
     the transfer tables (§12) leaves Transfers out of this check.

   So a member scanned while empty and with no `store-id` (a new copy
   destination) adopts its first id with no reset, and the first copy
   into it (§21) resets no cache that scanned it. Before this, such a
   cache had the scan mark and no watermark, could not show its rows were
   from this incarnation, and reset: the explore server's own cache, and
   every composition sharing `cas.index.path`. A member still resets when
   the cache holds a run or a log entry of it and its watermark is not in
   its log. Rows a writer ingested itself (a run's `ingestRun`, a `put`)
   are not counted, because the writer created the member's id before it
   wrote (§20 decision 2). That leaves a window: a row ingested when the
   id could not be created (`writableStoreId` warns once and the run goes
   on), or a Selection `ensureSelectionIndexed` indexed after an `empty`
   scan, is not attributed, so if the member were then replaced the cache
   would adopt the new id and keep that row. Both need an earlier failure
   or a block present without its entry. Tested by `IndexStoreIdTest`
   and, end to end, by `CopyServiceTest`: an explore-style session over
   `[lab, org]` sharing `cas.index.path` catches up while `org` is absent,
   a real copy goes into `org`, and the next catch-up prints no rebuild
   notice, records `store_id:org` and leaves `lab`'s rows as they were.
5. `CasSession.writableStoreId()` creates the writable member's id. It is
   called from `catchUpIndex` and from `CasObserver.onFlowCreate` before the
   Live Writer registration. Every verb that holds the member writable goes
   through `catchUpIndex`, so §20 decision 2 needed no per-verb code. `items`
   on a writable config therefore creates the id too. Its "read-only"
   description (§0 rule 5) means it writes no block, entry, claim or
   snapshot.
6. A Forget block lists every deleted block whose CID codec is dag-cbor
   (changes §20 decision 6). The deleted blocks are dead, so the mark never
   decoded them and the sweep does not know their kinds without reading
   each one. The codec filter costs nothing. It lets DirectoryManifests in;
   they have no index rows, so forgetting them is a no-op. A sweep that
   deletes only raw blocks writes no entry. Gate assertion 9 deletes
   released DirectoryManifests, so it now writes `forget` entries, which
   changes the last sentence of §20 decision 6; its checkpoints ignore
   Forget blocks.
7. Store Log entries of deleted blocks are still deleted once, after every
   batch (changes §20 decision 7's per-batch order). Reading the whole log
   per batch is a listing per batch on S3. The `forget` entry is written
   right after its batch, which is the part other caches depend on.
8. The sweeper's own cache forgets through catch-up, as §20 decision 7
   says, except for cids whose `forget` entry could not be written
   (`SweepReport.unannounced`). `RetentionCommands.forgetAndSnapshot`
   forgets those directly, and the sweep warns that other caches need
   `reindex`.
9. Catch-up applies a `forget` entry once per member, recorded in `meta` as
   `forget:<member>:<forget cid>` = `<entry name> <runs forgotten>`. A cid
   that any member of the composition holds now (`CompositeStore.has`) is
   not forgotten: it is still held elsewhere, or was written again since the
   sweep (an OutputItem a later run reproduced). This widens §20 decision 8's
   "other member" to the whole composition. It costs one `has` per listed
   cid per cache, once (changed by decision 22: only cids the cache has
   rows for).
10. No schema version bump (changes §20 decision 10). The forgotten-run
    counts live in `meta`; the `forgotten` table decision 10 named was not
    built. A new table changes `Index.ddl()`, which is also the snapshot's
    DDL, and would have forced `index/v4.sqlite` on the page and the Gate.
    `fewer_runs` adds back the counts of the `forget:<member>:` entries
    newer than the base snapshot's watermark (`Index.forgottenRunsSince`).
11. `SnapshotBase` carries `storeId` and `watermark`, and
    `SnapshotStorage.replace` takes a `SnapshotBase` describing what it
    writes: `replace(Path built, SnapshotBase written, SnapshotBase base)`.
    On S3 they ride as `x-amz-meta-store-id` and `x-amz-meta-watermark`
    beside `x-amz-meta-runs`. An object without `runs` is still downloaded
    once, and then all three are read from the file.
12. The snapshot guards run in this order: `over_cap` (base), `stale_store`,
    then the build, `over_cap` (built), `older_watermark`, `fewer_runs`,
    `replaced_meanwhile`. Watermarks compare by their 13-digit reverse
    timestamp: a smaller one is newer, and equal is not newer. A base with a
    watermark beats a build with none.
13. The page does not apply `forget` entries. Its Store Log regex keeps
    ignoring unknown kinds, and it reads the snapshot, which the sweep and
    every later writer rebuild after applying them. Only
    `web/src/generated/schema.json` changed (24 types).
14. `reindex --seed false` passes no snapshot storage to the cold catch-up,
    so every member is scanned. `reindex` also removes a `-journal` sidecar
    beside the cache file, and prints `reindexed <cache file>` and then
    either `wrote <snapshot> (<n> runs)` or `snapshot not rewritten: <reason>`
    (changed by decision 19: `(<n> runs, was <m>)`).
15. A `forget` entry whose Forget block is gone is dangling like any other
    root entry, and a sweep deletes it once it is past the age floor
    (changes §20 decision 8's "a sweep never deletes a `forget` entry as
    dangling"). The block is written before its entry, so this happens only
    after an external delete, and no cache can apply an entry without its
    block.
16. A Forget block that exists but does not decode as a Forget is warned
    once and recorded as applied with 0 runs, so it never fails a member's
    catch-up (`Index.applyForget`). Decision 23 extends this to a block
    that is there but of another kind or over the size cap.
17. `Put`'s own catch-up asks the whole composition (`holders`) before
    forgetting, as `CasSession.catchUpIndex` does: the four-argument
    `Index.catchUp(store, storeLog, member, holders)`.
18. Gate assertions 17 and 18 record their preconditions through
    `assert.py --coherence-checkpoint`: the cache held the first
    incarnation's run after `recreate-1`, and cache B held run a before the
    sweep. An assertion fails when its precondition is missing, so a pass
    cannot come from a cache that never held the rows it is said to have
    forgotten.

Decisions 19 to 25 come from the final review of the branch (2026-10-09).

19. `reindex` writes the snapshot as a rebuild: `IndexSnapshot.write(...,
    boolean rebuild)`, reached through `CasSession.snapshotWritable(...,
    boolean rebuild)`, skips `older_watermark` and `fewer_runs`. It still
    applies `stale_store`, `catch_up_failed` and `replaced_meanwhile`, and
    `reindex` writes at any size, as before. Without the waiver, `--seed
    false` got the cache right but the snapshot write was refused, so the
    snapshot kept the dead runs and the next cold cache seeded them again.
    The output line is `wrote <snapshot> (<n> runs, was <m>)`, where `<m>`
    is the base's count, `none` without a base, or `uncounted`. The default
    `--seed true` seeds from the snapshot, which still lists runs whose
    blocks were removed by hand or by a lifecycle rule, and writes them back
    (the output shows `<n>` equal to `<m>`); for those, the README says to
    use `--seed false`. It also says to stop `explore` around a reindex: a
    process with the cache open keeps writing to the old, unlinked file.
20. A sweep with `SweepReport.unannounced` cids writes its snapshot as a
    rebuild too (`RetentionCommands.forgetAndSnapshot`). Those runs were
    deleted by this sweep but no `forget` entry counts them, so
    `fewer_runs` cannot add them back and would refuse the write.
21. A base whose `store_id` is set and differs from the member's is from
    another incarnation of the member, so `older_watermark` and
    `fewer_runs` are not judged against it (`IndexSnapshot.write`, when it
    is given the member's id). Its watermark names an entry of the old log,
    and its run count describes a store that no longer exists.
22. `applyForget` looks up only the listed cids this cache has a row for:
    a `run`, `collection`, `item` or `claim` row keyed by the cid, or its
    `log_entry` (one SQLite query per cid). Forgetting any other cid is a
    no-op, so DirectoryManifests and blocks the cache never indexed cost no
    `has`. A `has` that throws anything (an S3 403 on a missing key without
    `s3:ListBucket` is an unchecked SDK exception) counts as held: the
    cid's rows stay, and one warning per `forget` entry gives the count of
    cids it could not check. Before this, the exception escaped `catchUp`
    before the watermark was set, on every catch-up.
23. `applyForget` tells an absent Forget block from one that is there but
    unusable. Absent (`has` is false) is `missing` and retried, as before.
    A block that is there but does not decode, is of another kind, or is
    over the size cap is warned once and recorded as applied with 0 runs,
    as decision 16 does for a malformed Forget; before this it was recorded
    `missing` and read again on every catch-up. An IO error while reading a
    block that is there is still `missing` and retried, so a transient
    failure never records an entry as applied.
24. A batch whose `deleteBlocks` throws is announced whole (`Sweep.deleteDue`)
    before the sweep stops. Which blocks went is unknown; catch-up asks the
    composition before it forgets a cid, so the blocks that survived keep
    their rows.
25. Known limit, not fixed: a reset (decision 1) in one process can race a
    catch-up of the same cache file in another process. The other process
    can record its watermark after the reset has emptied the cache; the
    cold catch-up that follows the reset then finds a watermark, skips the
    seed and the scan, and reads only the log tail. The cache is missing
    older rows until the next `reindex`. It takes a member replaced at the
    moment another process is catching up the same composition.
26. Known limit, not fixed (found on 0.6.0-beta.1 in `scratch-workflow/`,
    2026-10-14): decision 21 does not cover a base without `store_id`. A
    snapshot that a build before this section wrote from a stale cache,
    after the member was recreated, keeps its old runs: the cache resets
    and declines to seed from it (decision 9), but the writer still judges
    it, its watermark is newer than the rebuilt cache's, and
    `older_watermark` refuses every rewrite. The page goes on showing the
    old runs. `reindex --seed false` replaces it once; every snapshot
    written since carries `store_id`, so it cannot recur unless
    `writableStoreId()` fails to create the member's `store-id` (it warns).
    The fix, if one is wanted, is decision 9's rule in the writer: a base
    without `store_id` whose watermark entry is not in the member's Store
    Log is another incarnation's.
27. *Added 2026-10-25 (P3 final review item 4).* Opening a cache never
    deletes it under another opener: openers in one JVM take turns per
    file, a new cache is linked into place whole, and a file that looks
    stale is asked again under the write lock before it is recreated; a
    lock that stays busy makes `open` throw rather than delete (§12, the
    item 4 amendment). Decision 25's cross-process race is unchanged.
28. *Added 2026-10-27 (background store indexing in the explorer, spec
    `docs/superpowers/specs/2026-10-27-background-store-indexing-design.md`
    §7).* The explorer keeps a local snapshot for each member it indexed
    on this machine, in `<cache file>.snapshots/<alias>/index/v7.sqlite`
    (§15). That directory has the cache file's lifetime: `Index.reset()`
    deletes it after its transaction commits, so a member whose store-id
    changed, or a cache recreated for another schema version, loses its
    local snapshots with its rows. A local snapshot is served only while the
    cache holds the member (`Index.isIndexed`). At start the explorer
    deletes the directory of a member it left out as not indexed, and of a
    member whose own snapshot is usable, which it serves instead.

## 21. Copy with provenance (explorer v0.3 P3, 2026-10-25)

*Status: in progress on `explorer/p3-copy`. Spec
`docs/plans/2026-10-24-explorer-v0.3-p3.md` sections 1 and 2 (decisions D1
to D8); the plugin half's plan is
`docs/plans/2026-10-24-explorer-v0.3-p3-plugin.md`, and the execution ledger
`.superpowers/sdd/2026-10-24-explorer-v0.3-p3-plugin/progress.md`. Landed:
the `readOnly` option and copy targets (§2), the `transfer` Store Log
kind (§5), the Transfer block, the scrub change and `RunManifestAudit`
(§6), the transfer tables and the closure's lookups (§12), Transfers in the
Index Snapshot and their presence check (§15), `members.json`'s copy
fields and `POST /api/copy` (§15), the copy error codes (§16), the
cross-member release rule and the Transfer root (§19), the store-id rule
for a member that held nothing (§20 decision 4), the closure, and the copy
verb (below), with the final fix wave's 18 items. The final whole-branch
review's fix wave (its items 1 to 8, 12, 13, 14 and 16; decisions D9 and
D10, interface commit `9caddbd`) is marked "final review item N" below.*

`nf-blocks:copy` copies a Selection or a run, with what it needs, from one
configured member to another, and records the copy in the destination as a
Transfer block (§6) announced by a `transfer` entry (§5). Copied blocks
travel verbatim, so their addresses, `asserted_by` and `timestamp` do not
change.

### The closure (`robsyme.cas.core.CopyClosure`)

`CopyClosure.compute(BlockStore source, Index index, String fromAlias, Root
root, String depth, Set<String> claimCategories, LineageRecords nf) ->
Result`. `Root` is `{kind: 'selection' | 'run', cid}`, a Selection or a
RunCompletion; turning the verb's root argument into one is the verb's job.
`depth` is `results`, `lineage` or `all`; `claimCategories` is a subset of
`names`, `pins`, `projects` and `annotations` (empty for none); `nf` reads
the source member's `nf/` records, or is null when it has none to offer.

Reads and lookups. Blocks are read from `source`, the source member's own
`BlockStore` and never the composite, 16 at a time in a pool that `compute`
shuts down before it returns, so `missingAtSource` is the source's truth: a
closure cid the source cannot open, or, for a block never read, one its
`has` says it lacks. A block the source lacks is asked for once: a
DirectoryManifest it lacks is given no children, so later expansions do
not read it again. Lookups (`runByManifest`, `outputCollectionsOf`,
`producersOf`, `attrsOf`, `runItemsWith`, `claimsLoggedIn`, `claimOf`,
`blockSize`, §12) go to `index`, the copy session's cache, which the caller
catches up first (the copy verb, below).

Runs. For a `run` root, the run itself. For a `selection` root, every run
that produced one of its items: each item member's `via` collections give
their RunManifest (`run`) and `Index.runByManifest` its RunCompletion (the
index's `collection` row stands in when the source lacks the collection);
an item with no `via` (picked by a query) is reached through every
OutputCollection of kind `output` the index says lists it, and their runs.

D7, whole run metadata. At every depth, each such run's RunCompletion, its
RunManifest, the manifest's `script` block, a non-null `input_set`, and
every OutputCollection and OutputItem record the run links (records only)
travel. Content (raw blocks and DirectoryManifests, with everything under
them) follows the depth. A destination holding only the copy is then
self-consistent: `Mark` can walk the run, a sweep there is not refused for
missing metadata, and the items whose files did not travel show as
missing. These records count in `run_records` unless an earlier group
already has them. *Amended 2026-10-25 (fix wave items 1 to 3):* `Mark`
follows every metadata link (§19 decision 4), so `run_records` also takes
two cases the first build missed:

- a `via` collection the source holds whose RunManifest no indexed
  RunCompletion names (a run that never finished, or one the index lacks):
  that RunManifest and its `script` travel, at `results`, though no run is
  listed for it in `runs`. *Amended (final review item 7):* every `via`
  collection the source holds brings its RunManifest and `script` this
  way, also when the index knows the run: the source may lack that run's
  RunCompletion, and then the run's records bring nothing (the
  RunCompletion is missing at the source and the run is listed with no
  manifest);
- an item the lineage rule or the join key adds that no source collection
  lists (the index lists it for the run): its OutputItem record travels at
  `lineage`, the depth its files got.

Groups, in the order of the plan's section 1.3: `root`, `items`, `files`,
`collections`, `run_records`, `lineage`, `run_outputs`, `inputs`,
`handoffs` (P4), `claims`. Each block is in the earliest group any rule
gives it.

- `root`: the root Selection and every Selection nested under it (ruling:
  they are roots of the tree asked for); for a `run` root, its RunCompletion.
- `items`: the item members of that Selection tree; for a `run` root, every
  item of the run.
- `files`: the root's own files, at every depth: each Leaf address of those
  items, and each DirectoryManifest with everything under it; for a `run`
  root, also each collection's `index` Leaf.
- `collections`: the collections the root's items were picked through
  (records only; the other items they name stay references unless D7 brings
  their records); for a `run` root, the run's collections.
- `run_records`: D7, above.
- `lineage` (lineage and all): the files of the items the lineage rule
  adds. Their records are in `run_records` (D7, above).
- `run_outputs` (all only): the files of every other item of the producing
  runs, and each collection's `index` Leaf.
- `inputs`: reserved for input content once milestone 7 records it; empty
  in this build. A non-null `RunCompletion.input_set` is run metadata and
  goes in `run_records` (D7, and the ruling below).
- `handoffs` (explorer v0.3 P4, follow-up R4): each Handoff citing a
  Selection of the tree (the root or a nested one) that a `handoff` entry
  of the source member's own log announced (`Index.handoffsLoggedIn`),
  with its files (each Leaf address, sized by the Leaf), at every depth.
  The Handoff is the Selection's citation and its command reads its files
  (`--input cas://<cid>/samplesheet.csv`, the file's own cid; a path
  through the Handoff's cid, `cas://<handoff-cid>/samplesheet.csv`, does not
  resolve and is not meant to: the Handoff is a record, not a directory),
  so a copied Selection's
  hand-off resolves at the destination. A Handoff whose current Claims
  among the source's own are a clean deletion with no pin stays behind
  (deletions never travel, so it would arrive undeleted). Its `project`
  link is not followed: a Project travels only through a carried
  membership. A Handoff or a file the source lacks is listed in
  `missingAtSource`. A `run` root carries none. A Handoff written into a
  member other than the source is not carried, since a copy reads blocks
  from the source member only.
- `claims`: the carried Claims.

Depths nest: `results` is inside `lineage`, which is inside `all`. One walk
computes the `all` closure and tags every block and Claim with the
shallowest depth that includes it. `Result.at(depth)` filters that walk,
and `Result.depths` gives each depth's totals (`blocks`, `bytes`,
`unsized`) from it, so a dry run answers all three depths at once. A block
that a shallower rule reaches later moves to that depth, and a
DirectoryManifest passes the better placement down to what is under it.

Lineage. Measured on roblab (plan, "Lineage depth"): a run's writable
member keeps Nextflow's records under `nf/` (1,734 FileOutput, 235 TaskRun,
235 TaskOutput, 4 WorkflowRun). A published FileOutput
(`nf/<nf_run_hash>/<publish path>`, path `cas://<cid>/<name>`) names its
`source`, `lid://<task>/<file>`, and that task's TaskRun `input` names the
files it read in the same form. Those are work-dir files (1,905 of 1,905 resolved inputs
on that store, which predates cross-run item lineage; *amended 2026-10-26:*
a task fed by `fromStore` has `cas://` paths in its TaskRun inputs, and
`TaskLineage` stops at them by design: they name another run's content, and
`lineage` depth stays within a run), but many have a published
counterpart: a FileOutput of the same run whose `source` is that lid. 366
published sources have a published upstream (the plan counts 784 published
sources; the reader's recount found 1,173 (run, source lid) pairs over the
4 runs). The rule, per run:

- Task rule (`runs[].lineage = "tasks"`): when `nf` is given, the run's
  RunManifest has an `nf_run_hash` other than `unknown`, and the source
  holds records under `nf/<nf_run_hash>/`. *Amended (final review item
  8):* the hash must be hex digits (`[0-9a-f]+`), since it comes from block
  content and names a directory; any other hash takes the join key. The
  local `nf/` reader also refuses an absolute key, a backslash or a `..`
  segment, so a key never resolves outside `nf/`. `TaskLineage.upstream(nf,
  nfRunHash, addresses) -> Result{available, upstream}` reads every
  FileOutput of the run, starts from the tasks that produced the run's
  results Leaf addresses, and walks TaskRun inputs upstream, transitively
  (each TaskRun read at most once, so cycles end); every input lid with a
  published counterpart adds that address. It is called once per run with
  every results address of the run; on S3 that is one GET per FileOutput
  of the run, made even for a results copy. Lineage then adds each
  OutputItem of that run holding such an address (the index's `producer`
  rows, kept to that RunCompletion) that is not a results item.
- Join-key fallback (`runs[].lineage = "join_key"`): when the task rule
  does not apply (no `nf`, no usable hash, or `available` false). Lineage
  adds every item of the same run, in any output, with an untruncated
  `meta.id` `item_attr` row equal on path, type and value to one of that
  run's results items (ruling: the same run only, as the task rule is).
- `nf/` records never travel: they hold host paths (work-dir files) and are
  not blocks, and the closure only reads them. So lineage depth from a
  member that holds a run only as a copy always takes the fallback.
- For a `run` root every item is already a results item: the plan names the
  rule that would apply and adds nothing. A results view reports
  `runs[].lineage` as null.

Measured on a scratch copy of roblab (`RoblabCopyClosureTest`, opt-in by
`-Droblab.store=`), the wigshop Selection
`bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce`, one run
(`distraught_murdock`): results 98 blocks and 1,349,065 bytes, lineage 174
and 20,842,160, all 1,043 and 78,681,431. By group: root 1 (375 B), items 3
(3,645 B), files 9 (1,135,852 B), collections 1 (1,041 B), run_records 83
(207,975 B), lineage 76 (19,493,095 B), run_outputs 869 (57,839,271 B),
claims 1 (177 B). The task rule adds 18 items; the join key on the same run
would add 23 items, 303 blocks and 23,102,286 bytes. Nothing unsized or
missing at the source; unaddressed Leaves 0, 3 and 5 by depth.

Claims. Only Claims a `claim` entry of the source member's own log
announced count (`Index.claimsLoggedIn(fromAlias)`); every block in the
closure is a subject. Per subject, the current Claims are `ClaimState` over
those, and each falls in a category by its attribute: `name` is `names`,
`pin` is `pins`, `project` is `projects`, `retain` is retain, a null
attribute (a deletion) is deletion, and any other is `annotations`.

- A current `retain` or deletion Claim never travels: it is about the
  source's retention. It is counted in `skippedClaims` (`retain`,
  `deletion`) at its subject's depth and not listed. So a Project or
  Selection deleted at the source arrives live at the destination when the
  copy carries it (a Project through a carried membership, §22); delete it
  there too.
- Every current Claim of the four categories is listed, with every Claim it
  supersedes, transitively (read from the source, else from the index;
  `supersedes` is empty when neither holds the Claim). A superseded Claim
  takes the category, the carried flag and the depth of the current one it
  is history of. `current` is true for the current Claims and false for
  their history. `carried` is true when the category was asked for.
  Uncarried Claims are listed so the page can show a category's Claims
  before it is ticked. *Amended (final review item 14):* a current Claim
  the source lacks (its log and the index still name it) is not carried,
  and neither is its history: a destination that logged what it
  supersedes without it would take an older value as current.
- Only carried Claims are blocks (group `claims`) and count in the totals.
  A Claim's depth is its subject's shallowest depth. Claims whose subject
  is itself a Claim are not looked for.

Sizes, in this order: the cache's `block` table; the Leaf's or directory
entry's `size` for a raw block; the encoded length of a record the walk
read; else the source's `size(cid)`. Every block the walk never read is
checked with the source's `has`, 16 at a time, and sized there when nothing
above gave a size. A block with no size is `unsized`, and byte totals
are then "at least". `unaddressed` counts Leaves with no address in the
items whose files the depth copies.

`input_set` ruling (2026-10-25, ledger; built in fix wave item 3): a
non-null `RunCompletion.input_set` link is run metadata, as `Mark` treats
it (§19 decision 4), so it travels in `run_records` at every depth, read
like a record, with nothing under it followed, since InputSet is a reserved
kind. The `inputs` group is for input content once milestone 7 records it.
`input_set` is null in every RunCompletion written today, so no copy
carries one yet.

*Amended 2026-10-26 (cross-run item lineage):* a non-null
`RunCompletion.item_lineage` travels in `run_records` at every depth the
same way, read like a record, with nothing under it followed (its links
name this run's items and InputSet, and `Input.from` names other runs'
records). A copied consumer run therefore shows the same lineage.
`lineage` depth still stays within a run.

Known gaps, as built: `blockSize` is one query per block; on S3 a block the
walk never read costs a `HEAD` for `has` and, when nothing else sized it,
another for `size`. The audit covers every RunManifest in the copy,
including one that travels only through a `via` collection (the first D7
case above) and so has no `runs` entry.

### The copy verb and `POST /api/copy`

*Added 2026-10-25 (plan tasks T6, T8 and T10, and the final fix wave).*

`robsyme.cas.CopyService` is the one implementation. `nf-blocks:copy`
(`cli/CopyCommand`, §15 has its flags and exit codes) and the explore
server's `POST /api/copy` (§15) only turn their input into a request and
its answer into an exit code or a response. `CopyService.execute(Map
config, Map request, boolean dryRun, PrintStream err)` returns
`Outcome{status, body}`; a fifth argument, `Supplier<Principal> signer`,
signs the Transfer (below). The request has the endpoint's JSON keys:
`root`; `from`, null for the default; `to`; `depth`, default `results`;
`claims`, a list, default `names`, `projects`, `annotations`, or
`['none']`; and `accept_audit`, a boolean. The status is 200 (a plan or a
finished copy), 400 (`{error, message}`), 409 (`CopyService.REFUSED`: a
real copy refused before any write, the body the plan with `refused`
`not_kept` or `audit`, below) or 500 (a copy that failed part way). Anything
else, such as a config that does not parse or a member that cannot be
read, is thrown; the CLI then exits 1 and the endpoint answers
`copy_failed`.

The 400 codes:

- `bad_root`: no root; a root in none of the forms below; a `selection:`
  that names another kind; a bare cid of a kind other than Selection,
  RunCompletion or RunManifest; `run:latest` (copy has no `--pipeline`); a
  `run:` reference `RunRef` refuses as malformed.
- `bad_request`: no `to`; a field of the wrong JSON type; an
  `accept_audit` that is not a boolean. The endpoint also uses it for a
  body that is not a JSON object.
- `bad_depth`: a depth other than `results`, `lineage` or `all`.
- `bad_claims`: `claims` not a list, an empty list, `none` beside another
  category, or a category not among the four (`retain` and `deletion` say
  they never travel).
- `unknown_member`: `to`, or a given `from`, is not under `cas.stores`.
- `same_member`: `from` and `to` are the same.
- `bad_config` (fix wave item 11): `readOnly` on any configured member is
  something other than `true` or `false`. `CasConfig` would throw on it.
- `not_a_target`: `CopyTargets.judge` refuses `to` (§2).
- `root_not_found`: no member holds the root; the named `from` does not
  hold it; no member other than the destination does; a RunManifest that
  no RunCompletion names (the run did not finish); a `run:` reference
  `RunRef` cannot find.

The copy composition. A copy of the config whose `cas.resolve` is the
configured list (every `cas.stores` alias when unset) with `from` appended
when missing, opened with the destination as its writable member:
`CasConfig.from(composition, 'cas://<to>')`, then `CasSession.forCommand`.
So the destination is writable for this copy only (D1), and the
composition's index sees the source and every resolve member. Its members
are the destination, then the `cas.resolve` order.

Roots. `selection:<cid>` must name a Selection; `run:<ref>` takes what
`RunRef` resolves (`run:<cid>`, `run:cas://<cid>`, `run:lid://<hash>`), a
bare cid read as `cas://<cid>`; a bare cid is taken by its kind: a
Selection, a RunCompletion, or a RunManifest, whose RunCompletion
`Index.runByManifest` finds. The kind is read through the composite, so
any member holding the block will do. The source is `from` when given,
and it must hold the root; otherwise it is the first member after the
destination, in `cas.resolve` order, that holds it. The source is read
through its own `BlockStore`, never the composite, and its `nf/` records
through `LocalLineageRecords` or `S3LineageRecords`.

Plans are write-free (ruling, 2026-10-25). Every plan, the dry run's and
the real copy's, catches up the composition's index with the destination
left out (`CasSession.catchUpIndex(index, true, [to])`): the destination is
not caught up into the index, its `store-id` is neither read nor created,
and nothing is written there or at the source. The closure is computed
once at `all` and viewed at the depth asked for (above). Presence is the
destination's own `has`, 16 at a time, for every closure block and every
listed Claim, carried or not. The plan never reads presence from the
cache. *Amended (final review item 13):* the cache can still hold the
destination's rows from an earlier real copy (whose snapshot step catches
the destination up), and from any other session over a composition that
includes it; the plan reads only the cache's Claim rows from them (D9,
below). The audit reads each RunManifest in the view from the source
(`RunManifestAudit`, §6). A dry run, and a refused real copy, end here.
Tested: a dry run into a local member that does not exist leaves it
absent, and one into an S3 member sends it only `HEAD`, `GET` and `LIST`.

Kept at the destination (D9, final review item 1). Present is not kept: a
destination holds the blocks of a run it has released until its next
sweep (trashed blocks stay in `blocks/` during the grace), and a copy that
found them present, or wrote them, logged nothing new, so that sweep took
them, and the page then offered to release the source's copy too. So every
plan reads the destination's own state, write-free: its Store Log listing
(a directory listing, or an S3 `LIST`; a member that does not exist yet
reads as an empty log), and the Claim blocks that log lists, each from the
cache when it holds that Claim (a Claim is its content) and otherwise from
the destination (`GET`). Only Claims whose subject is a root or a run of
the plan count. `plan.destination_state` has one entry per root (role
`root`: the root asked for, then each nested Selection) and per run (role
`run`: each `runs[].completion`; a `run:` root is listed once, as the root),
`{cid, role, name, state}`. `name` is the run's `run_name`, or the
Selection's current name among the source's Claims the plan lists, or
null. `state` is `ClaimState` over the destination's own logged Claims on
that subject, plus the Claims this copy carries on it that `announce`
will log there, so it describes the destination after the copy (a carried
`del pin` superseding the destination's pin on a run it released makes
that run `released`); `absent` still looks at the destination's own Claims
alone. Read as Mark reads them:

- `pinned`: a current pin (a pin wins for content, so a pinned run that is
  also released or deleted is `pinned`);
- `deleted`: a clean current deletion;
- `released`: a run (only a run) whose `retain` group is a clean `set
  retain "lineage"`;
- `absent`: none of those, no Claim on it at all, and no `run-` or
  `selection-` entry for it in the destination's log;
- `held`: anything else, a conflicted group included (Mark keeps on a
  conflict).

A subject with no log entry but a releasing or deleting Claim reads as
`released` or `deleted`, not `absent` (ruling while building: once the copy
logs it, that Claim decides). `plan.kept` is false when any entry is
`released` or `deleted`. Copy never writes a restore Claim: the person
restores in the destination first.

Refusals. A real copy with `kept` false writes nothing and answers 409
with `refused` `not_kept`, whatever the audit says (ruling: `not_kept`
wins, since restoring at the destination comes first). Else a real copy
whose audit has findings writes nothing and answers 409 with `refused`
`audit`, unless `accept_audit` is true. Every other body has `refused`
null. A dry run reports both and answers 200. Tested
(`CopyServiceTest`, the D9 cases): a released run and a deleted Selection
at the destination refuse the copy and leave the destination's files as
they were; a held run, a pinned one, a pinned and released one and a
pinned and deleted one are kept; a conflicted `retain` group holds; an
absent destination reads `absent` throughout and stays absent; a release
logged before the run itself refuses; an S3 destination is read with
`HEAD`, `GET` and `LIST` only.

The body is the plan's section 1.3 body. Each group also carries
`present_bytes`, `transfer` and `transfer_bytes`; `transfer` counts blocks
the destination lacks and the source holds. Claims not carried are listed
but count in no total. `command` is the equivalent CLI line, with `from`
resolved. A real copy adds `copied`, `copied_bytes`, `transfer_entry`,
`failures` and `snapshot`. The 409 body is the plan with `dry_run` false,
`refused` set, and none of those fields.

Write order. After the refusals, `checkClock` (§11): a clock far from S3's
would misdate the entries. *Amended (final review item 2):* then the copy
registers as a Live Writer in the destination
(`CasSession.startLiveWriter`, §19 rule 1, with `run_name`
`nf-blocks:copy`), which waits out a sweep holding `sweep.lock`, and
deregisters (`stopLiveWriter`) when it ends, in a `finally`. That sweep
may have deleted trashed, past-deadline blocks the plan counted present,
so after registering the copy asks the destination's `has` again of every
block the plan counted present; any now gone is written like the rest,
and the body's figures are those of the second look. Tested: a sweep that
holds the lock and takes a present block before letting go; the copy
waits, writes that block again, and leaves no registration. Then each
block the destination lacks and the
source holds, in seven steps: raw content; DirectoryManifests;
OutputItems; OutputCollections; RunManifests with their scripts;
RunCompletions; Selections, Projects, Handoffs and Claims. A step's blocks go 16 at a time in
no fixed order, submitted in reverse closure order so a nested Selection
or DirectoryManifest tends to go before the one naming it; only the steps
are ordered. Each is `put(cid, source.open(cid), size)`, which hashes and
refuses a mismatch (§5). A failed put stops the copy at the end of its
step and nothing more is written: the answer is 500 with `copied`,
`copied_bytes`, `failures` (`[{cid, error}]`), `transfer_entry` null and
`snapshot` null. Running the copy again finishes it, since the blocks
already written now count as present.

Then one read of the destination's Store Log, and an entry of the existing
kind for each root-like block the log lacks: `run-<completion>` per run of
the view, `selection-<cid>` per Selection in it, `project-<cid>` per
Project, `handoff-<cid>` per Handoff, `claim-<cid>` per carried
Claim, leaving out a block the source lacked and the destination does not
hold. Then the Transfer block (§6), with `roots` the one root, `runs`
every run of the view whose RunCompletion the source held (D8: the runs
whose records the copy carried; final review item 6), `blocks` the view's
block count, `copied` the
blocks written and `bytes` the bytes read from the source for them; then
its `transfer-` entry, last. Each entry is stamped `max(clock, previous +
1)`, starting after the newest entry already in the log, so the `transfer`
entry sorts newest even when an entry there is stamped in the future.

No-op copies (ruling, 2026-10-25). When the copy wrote no block and the
log lacks no entry, nothing is appended and no Transfer is written:
`copied` 0, `transfer_entry` null, `failures` empty, `snapshot` `skipped`,
and stderr says the destination already holds everything. A copy that
writes only missing entries, or only a newly carried Claim, writes a
Transfer. *Amended 2026-10-25 (explorer v0.3 P3 follow-up, F4;
`CopyService.anythingToWrite`):* such a copy registers no Live Writer.
After the refusals, a real copy whose view has no block the destination
lacks and the source holds, and whose destination log (read once more)
lacks none of the run, Selection and carried Claim entries, goes straight
to the copy step: no `checkClock`, no Live Writer and no second `has`. So a
no-op copy into an S3 member sends no `PUT` and no `DELETE` under `live/`
(tested against `MemoryS3Ops`). It reads the destination's log a second
time to decide.

*Amended 2026-10-25 (explorer v0.3 P3 follow-up, F4;
`robsyme.cas.CopyErrors`):* a copy's error text never says where on the
operator's disk the stores, the index cache or the spool directory are.
`CopyErrors.describe(e, roots)` is `<ExceptionClass>: <message>` (the
simple class name, the full name for an anonymous class, the bare name
when there is no message), with each credential in the message replaced by
`[secret]` (`Leaks.redactCredentials`, as the RunManifest scrub does) and
the rest redacted by `CopyErrors.redact`, in this order:

- a `file:` URI (`file:///p`, `file:/p`) is read as the path `p`;
- each configured member's root, longest first, becomes `<alias>:`: a
  local location as an absolute, normalized path (so `/data/./lab` matches
  `/data/lab`), an S3 one as written
  (`s3://bucket/prefix`), matched as a whole path, never inside a longer
  path or a URL. `CopyErrors.rootsOf(config)` reads them from
  `cas.stores`;
- a Windows path (`C:\x`, `C:/x`, `\\host\x`) becomes `[local path]`,
  before the home directory is looked for, so `C:/Users/<user>` goes whole;
- a path under the home directory, the temp directory or the working
  directory becomes `[local path]` up to the next terminator: a quote, a
  backquote, a comma, a semicolon, a parenthesis, a line end, a `: ` (a
  reason follows, as in `<path>: Not a directory`) or a space before a
  `scheme://` URL. Such a path may hold spaces and names (`OneDrive -
  Org/x`, `Screen Shot 2024.png`), so prose after it may go too (ruling:
  over-redacting a trailing word beats leaking a name);
- every other absolute POSIX path becomes `[local path]`, up to a space,
  a quote, a bracket, a comma, a `;` or a `:`; a full stop that ends the
  sentence stays. A URL, a relative path, `alias:/...`, `1/3` and `and/or`
  stay. This rule and the home-directory one also take a path right after
  `<word>:` (`jar:file:/x`, `jdbc:sqlite:/x`, `location:/x`), unless the
  word is a member alias or the colon starts `://`.

This text is each `failures[].error`, the snapshot outcome `failed:
<ExceptionClass>: <message>` and its stderr line, and the 500
`copy_failed` message. `CopyService.run` rethrows anything but a
`Refused` or a `ClockSkewException` as `CopyService.Failed`, whose message
is already redacted (the cause stays for the log); the CLI prints it and
the explore server answers it. Another exception reaching the server is
described with no member roots, so its paths become `[local path]`.
Before this, `failures[].error` was the bare message (`the bucket went
away`), and a message could name a local path.

Not covered: an absolute path outside home, temp and the working
directory that holds a space is redacted only to the space; a terminator
inside a path under home (`<home>/Smith, John/x`, `Photos (old)/x`,
`Notes: 2024/x`) leaves the rest of that directory name after it, and so
does ` b://` inside a path name (ruling: the home prefix and the user
name never leak, part of a directory name can); a relative member
location is matched only as its absolute form; a `ClockSkewException` is
not wrapped.

The snapshot. After the Transfer, the composition's index is caught up in
full, which reads the destination's log and creates its `store-id` when
it has none, and the destination's Index Snapshot is written at any size
(`snapshotWritable(index, 0, base, failed)`). The snapshot base is taken
before the plan's catch-up, so the guard covers the whole copy.
`snapshot` is exactly `written`, `skipped` or `failed: <message>`; the
reason for a skip, with `IndexSnapshot.skipAdvice`, goes to stderr (ruling:
the page compares the value with `skipped`). The copy has succeeded
whatever it says (§0 rule 3). The first copy into a member with no
`store-id` resets no cache that scanned it while it was empty (§20
decision 4, amended). *Amended (final review item 12):* a session whose
cache opened without its transfer tables (§12) skips the snapshot, with
`snapshot` `skipped` and the reason on stderr, since its snapshot would
hold no Transfer rows and a build from it would skip D8.

Signing. The CLI signs the Transfer as a command does
(`CasSession.assertedBy`, docs/identity.md). Decision (P3 lead,
2026-10-25; fix wave item 12): a copy through `POST /api/copy` signs with
the explore server's own principal, the one `members.json` shows, passed
as `CopyService`'s signer. It is asked only when the Transfer is written,
so a dry run never waits for identity, and a copy resolves none of its own
and prints no `signing as` line. A null answer falls back to the config's
resolution. The copied blocks keep their own `asserted_by`.

Concurrency on the explore server: one real copy at a time, and dry runs
share two slots, with at most one more waiting; a newer dry run supersedes
the waiting one (D10, §15). Each request opens the copy composition's
cache, and openers of one file take turns (§12, final review item 4).

The cache file. The copy composition's cache file is named by its members'
locations, writable first (§12). Unless `cas.index.path` is set, it is the
configured composition's file, and the explore server's, only when the two
list the same members in the same order, as when the destination is the
configured writable member and `cas.resolve` already lists the source.
Otherwise the first copy into that destination from that config builds a
cache of its own cold, seeding from the members' snapshots, and later
copies catch it up; a server whose cache is another file sees the copy at
its next catch-up of the destination's log. With `cas.index.path` set,
every composition shares one file, and §20 decision 25's race between a
reset and a catch-up of the same file applies to it. Measured (T8,
local members): a cold dry run 15.6 ms for 10 items and 454 ms for 3,000
items (about 6,000 blocks), a warm one 8.2 and 224 ms; a real copy 148 ms
for the 10 items and 14.1 s for 3,005 blocks (about 4.7 ms a block). The
P3 lead's local acceptance run at `daac0b0` copied 98 blocks, 1,349,065
bytes (the wigshop `results` figures above); a second dry run found all 98
present, and `fromStore` read the 9 files from the destination alone.

*Amended 2026-10-27 (background store indexing, spec
`docs/superpowers/specs/2026-10-27-background-store-indexing-design.md`
§6.8, plan `docs/superpowers/plans/2026-10-27-background-store-indexing.md`
task CP1):* with the explorer's view of which members are indexed, a copy
never scans a member the explorer has not indexed.
`CopyService.execute(config, request, dryRun, err, signer, indexing)` takes
a `CopyService.Indexing` of two maps: `unindexed` (alias to reason) and
`localRoots` (alias to the directory holding an `index/v7.sqlite` built on
this machine); the five-argument overload passes null and changes nothing.
With it, the copy session calls `leaveUnindexedOut(indexing.unindexed)`, so
those members are skipped by every catch-up of the copy without a fetch,
and `seedFrom(alias, new LocalSnapshotStorage(root))` for each local root,
so the copy's cache seeds from the local snapshot in place of the member's
own, all before its first catch-up. A copy from a member indexed only on
this machine therefore reads that member's runs, Selections and closures
from the local snapshot. The plan's "already in the destination" figure
needs no change, since `CopyService.plan` leaves the destination out of its
catch-up and asks it directly (`presentAt`, `destinationState`). When the destination is in
`unindexed` or `localRoots`, the final catch-up and the snapshot step are
skipped, since writing the snapshot would mean scanning the destination
inside the request, or a push the user did not ask for. The body's
`snapshot` is then `not_indexed` (`CopyService.SNAPSHOT_NOT_INDEXED`) and
stderr gets `nf-blocks:copy: '<to>' has no Index Snapshot, so this copy did
not write one; index it from the explorer's Storage page`. The copy has
succeeded and its Transfer entry is written whatever the snapshot says (§0
rule 3). After a real copy into a member
served from a local snapshot, the explorer queues a refresh of that snapshot
(§15).

## 22. Projects, input sets and hand-offs (explorer v0.3 P4, 2026-10-25)

*Status: on `explorer/p4-projects`, plugin half. Contract
`docs/plans/2026-10-24-explorer-v0.3-p4.md` sections 1 and 2; the plugin
plan, with rulings R1 to R9, is
`docs/plans/2026-10-25-explorer-v0.3-p4-plugin.md`. The explorer's side
(the project pages, sample identity, the hand-off template) is the web
half's, under `web/`.*

A project is a small root record whose name, members, policies and
project-scoped annotations are Claims. A run's InputSet records the files
it read, by content address, so two runs that read the same bytes can be
told to be the same sample. A Handoff records one hand-off to a downstream
pipeline and roots the files generated for it.

### Format changes

- New block kinds Project, InputSet and Handoff (§6), and Store Log kinds
  `project` and `handoff` (§5).
- New RunCompletions link an InputSet. Old ones (`input_set: null`) read as
  before everywhere.
- A build older than P4 ignores `project` and `handoff` entries, so its
  sweep trashes Project and Handoff blocks, and the files and Selection
  only a Handoff reached, and leaves the entries dangling. It keeps an
  InputSet (its Mark already kept `input_set` as run metadata). From this
  build on, a sweep refuses to apply beside an entry kind it does not know
  (below), so later kinds are safe against this build.
- The index schema is version 6 (§12): every table in one DDL, the
  snapshot at `index/v6.sqlite`. A cache of an older version is recreated and such a snapshot
  is not seeded from; both are rebuilt from the Store Log.

### Projects and the Claims about them

`Put` builds a third kind: `{kind: "Project", name, description,
created_at}`. The server fills `schema` and `asserted_by`. The name is
non-blank and at most 256 characters, the description a string of at
most 4,096 (empty allowed), `created_at` ISO-8601 UTC with milliseconds;
a bad shape is `invalid` at the field, and a `created_at` more than 10
minutes from the server clock is `clock_skew`, checked after "already
written" as a Claim's timestamp is. The Project is logged as `project`.
`nf-blocks:put` takes the same request; the request's `kind` picks what
is built.

Claims, amending §16 decision 6:

| Claim | subject | verb, attribute | value |
|---|---|---|---|
| Membership | RunCompletion, Selection or OutputItem | `add project` | link to a Project |
| Sample in the project | Project | `add sample` | SampleRef `{pipeline, id}` |
| Annotation | Project | `add annotation` | `{sample: SampleRef, name, value}` |
| Same sample as | Project | `add same_sample` | `{a: Occurrence, b: Occurrence, same: Bool}`, Occurrence `{pipeline, id, run: link}` |
| Policies | Project | `set policy.default_store`, `set policy.retention`, `add policy.people` | a member alias; `"pin_selections"` or `"none"`; a principal |

- `add` is for pin, project, sample, annotation, same_sample and
  policy.people; `set` of any of these is `invalid` at /attribute. `add
  pin` keeps its rules. Any other `add` may supersede Claims. A Claim
  whose attribute is project, sample, annotation, same_sample or
  `policy.*` supersedes only Claims of the same subject and attribute, and
  is superseded only by a Claim of the same attribute (`wrong_kind` at
  /supersedes/N, either way). So a `set name`, a bare `del` or a `delete`
  cannot remove a membership, a sample or a policy; a bare `del` still
  undoes a `delete`. An edit is an `add` superseding the old value; a
  removal is a `del` with the attribute.
- Value shapes are checked before the store is read, `invalid` at /value:
  SampleRef strings non-empty and at most 256 characters; annotation
  `name` matching `[A-Za-z_][A-Za-z0-9_.]{0,63}` and `value` at most 256
  characters; policy strings non-empty and at most 256;
  `policy.retention` one of its two values. The pins the page writes for
  `"pin_selections"` are `add pin` Claims on Selections, which Put accepts
  since F5 (§19 item 5). `policy.default_store` is not
  checked against `members.json`: a project travels between explorers with
  different members, and the policy never overrides a member's `readOnly`
  (R9).
- Against the store: a held Project subject takes only those attributes,
  `name`, and `delete` with its `del` (else `invalid` at /attribute). The
  project-scoped attributes need a held Project subject; `add project`
  needs a held run, Selection or item subject and a held Project value;
  each Occurrence's `run` is a held RunCompletion (`not_found` or
  `wrong_kind` at the field).
- Sample, annotation and same-sample Claims hang off the Project because
  a Claim's subject is a cid and a sample has none (plan D1). A global
  sample identity, outside any project, waits on input addresses (below)
  and would need a sample subject.
- The page groups occurrences into samples itself (plan 1.6, D2); the
  index supplies `run_input`.

### InputSet: what a run read (`robsyme.cas.trace.InputRecorder`)

`CasObserver.onTaskComplete` and `onTaskCached` pass each task to
`InputRecorder`. A file in the task's `inputFiles` is an input when its
source is a Path and neither its staged path nor its source lies under the
run's work directory (compared as given and through `toRealPath`), except
the `stage-*` subtree, where Nextflow copies foreign (http, s3) files.
Files the run's own tasks made are not inputs, nor is a file under the
`storeDir` of any task the run recorded (`task.config.storeDir`, as given
and through `toRealPath`), whichever task is recorded first. `params.input` is recorded
at `onFlowBegin`, idempotently again at completion.

- Role: `samplesheet` when the base name equals `params.input`'s; else
  `param:<name>` for the first top-level param, in name order, whose value
  has a URI scheme, contains `/` or names an existing file, and has that
  base name; else `task`, except that a `task` file under the pipeline's
  `projectDir` is `asset` with no sample (nf-core stages its
  `multiqc_config.yml` in a tuple with meta id `multiqc_report`, which
  would otherwise make any two runs of the pipeline share a sample's input).
- Sample (R3): from the task's declared inputs, typed processes through
  `ProcessConfigV2` (records, tuples, plain inputs), classic processes
  grouped by `BaseParam.index`. A record's sample is `meta.id`, else its
  `id`. A tuple's is the `id` of its component named `meta`, else its `id`
  component; a map component under any other name (`meta2`, `meta3`,
  `ref_meta`) gives none, since nf-core passes a reference as `tuple
  val(meta2), path(fasta)` with `meta2.id` naming the reference. A record
  field other than `meta` or `id` gives none either. A file gets the sample
  of each input that holds it; a file only a separate `path` input holds
  gets null, so a reference staged beside per-sample reads names no sample.
- Address: a `cas://` source keeps its cid, unhashed. Which local path to
  read is chosen from the store path, then the source, by text only: a
  path on the default file system whose text is not under `/fusion`
  (Fusion's mount, where a read is a download). On the pool, each
  candidate's real path is resolved (`toRealPath`, links followed) first;
  the first real path not under `/fusion` is read, and when every
  candidate leads into `/fusion` (a local link into Fusion's mount) the file
  is `remote` and its real path only stat'ed. A candidate with no real path
  (realpath fails, or gives no absolute path) is judged by one stat that
  follows its links: a failed stat (a missing file, a dangling link, a
  parent this user cannot search) is `unreadable`; a regular file is
  `unreadable` with its size (without a real path it cannot be told from a
  link into `/fusion`); anything else is `unaddressed`. Only a regular file
  is opened, and only to hash it (raw, §3), with its size. A directory, a
  FIFO or a device is `unaddressed` with no size and is never opened. This
  covers `--input <(...)` and a piped `/dev/stdin`: on macOS `/dev/fd/N`
  resolves to a FIFO, on Linux it is a link to `pipe:[n]` or `socket:[n]`
  that realpath cannot follow. So a pipe with no writer cannot hold
  completion, and a piped `params.input` is never raced for its bytes. The
  `stage-*` copy of a foreign file is a regular local file Nextflow already
  downloaded for the task, so it is hashed and nothing is downloaded for
  hashing; a local source staged to a remote work directory is hashed from
  that source. Any other file (s3, gs, az, http(s), `/fusion`),
  `params.input` included, is never opened (orchestrator ruling, P4): it
  is `remote`, with its size when a stat answers. When a task staged
  `params.input` itself, the hashed `stage-*` copy stands for it and the
  unaddressed `params.input` entry of the same name is dropped.
- Name: the base name of the path only. A URI's query and fragment are
  dropped, and its userinfo whole. When the URI's first segment (up to the
  first `/`) holds a userinfo (an `@`, or a `:` that starts no port number)
  and a later segment holds an `@`, the userinfo runs to the last `@` of
  the first such segment, so a password holding an unescaped `/` or `@`
  never becomes the name: `https://user:p@a/ss@host` gives `host`, and
  `https://user:pa/ss@host/dir/a@b.fq` gives `a@b.fq`. Such URIs are
  ambiguous, and this reading costs a plain userinfo followed by an `@` in
  the path its text up to that `@` (`https://user:pass@host/data/x@1.fq`
  gives `1.fq`). Any other userinfo is dropped by `Leaks.withoutCredentials`,
  and a URI with no path gives its host. A remote name is also cut at its
  first `?`, `#` or `;` (path parameters such as `;jsessionid=`); a local
  name keeps them. On a Box shared link (host `box.com` or under it, last
  segment directly under `/s/` or `/shared/static/`) the stem (the name up
  to its first `.`) becomes `[secret]` whatever its shape, with its
  extension kept: `https://app.box.com/s/abcdef` is named `[secret]`.
  Elsewhere a stem of 24 or more letters and digits that is neither all
  hex nor a CID becomes `[secret]` the same way when it switches between
  letters and digits often enough: at least twice in a remote URL path,
  where tokens are handed out (a random 32-character token passes with
  probability 1 - 6.8e-5, and a separator-free remote name with a number
  inside, such as `run20261025combinedreads`, is redacted too), and at least
  four times in a local file name, so that same name on local disk stays
  readable (orchestrator ruling; a random token passes with probability
  1 - 1.3e-3). Known token shapes are caught anywhere by the next step. Last, the name goes
  through `Leaks.redactCredentials`, so a GitHub token in a file name
  becomes `[secret]`. The same rule gives the param base names and the
  samplesheet name, so roles still match by name.
- Cost: the task thread touches no file system. Each input's stat, then its
  hash, run on 4 daemon threads, once per path per run; a storeDir's
  symlinks are resolved on the same pool. At `onFlowComplete` the InputSet
  is written just before the RunCompletion that links it. A successful run
  waits at most `cas.inputs.waitSeconds` for the stats, hashes and storeDir
  resolution together, a failed run at most 30 s of it. An input still
  being stat'ed or hashed then has reason `unfinished`, with its size if
  the stat had answered: an InputSet is partial exactly when an input has
  reason `unfinished` (a new value of `reason`, no new field). The console
  says how many and names the option, and the run's status and
  RunCompletion are unchanged. The pool's daemon threads are stopped
  without waiting, so a read blocked on a hung mount holds neither the
  plugin stop nor the JVM exit. If the InputSet cannot be made, the console
  says so once and the RunCompletion links `input_set: null`; a failure
  writing the InputSet block aborts the run (rule 3). Measured on an M3 Pro
  with a warm page cache: `Hashing.hashRaw` runs at 1.8 GiB/s on one thread
  and 5.6 GiB/s on four, and a run with 4 GiB of local inputs waited 0.38
  to 0.43 s for hashing at completion.
- `cas.inputs.hash` (Boolean, default true; anything else refused; `cas.inputs`
  must be a scope): false reads nothing. Local inputs keep name, size, role
  and sample with reason `unaddressed`; `cas://` inputs keep their address;
  remote files, `params.input` included, stay `remote`.
- `cas.inputs.waitSeconds` (a whole number, 0 or more, default 600;
  anything else refused): how long a completing run waits for the stats and
  hashes of its inputs; 0 waits for nothing still queued. The measured cost
  above is for a warm page cache. A cold read of large local inputs on
  shared storage, or a resume where every task is cached and the run ends
  at once, pays the full read at completion, up to this bound, and every
  run hashes its inputs again (nothing is remembered across runs).
- `params.input` stays `[redacted-location]` in the RunManifest; the
  InputSet has it under role `samplesheet`, with its address when it is a
  local file.
- The same reads give the same InputSet block: a resumed run, and the same
  pipeline written as classic or typed processes, give one cid.

### Index (§12)

Tables, part of schema 6's base DDL (§12; an older cache is recreated and
rebuilt from the Store Log):

```
project(project_cid PRIMARY KEY, name, description, created_by, created_at)
project_stats(project_cid PRIMARY KEY, reachable_bytes, pinned_bytes, held_bytes, releasing_bytes, roots)
run_input(completion_cid, role, name, address, size, sample)
  index run_input_address (address, role, completion_cid)   -- covers the page's shared-input query
  index run_input_completion (completion_cid)
handoff(cid PRIMARY KEY, template, selection_cid, project_cid, asserted_by, at)
index claim_current_attribute_value ON claim_current(attribute, value)
```

- A `project` entry fills `project` (`created_by` is the block's
  `asserted_by`); a `handoff` entry fills `handoff` (`project_cid` NULL
  without a project, `at` the Handoff's `timestamp`) and records its files
  as content, `block_ref(handoff, file, name)` and `block(file, size,
  'raw')`, once per Handoff (§15, R3). Both are read in
  catch-up, the missing retry, the first block scan and `rebuild`, exactly
  as Transfers are: idempotent; an absent block waits as `missing(NULL,
  cid)`; one that does not read as its kind is warned about once and
  skipped.
- `ingestRun` reads a linked InputSet: one `run_input` row per input
  (address as the cid string or NULL) and one `consumer(content_cid,
  completion_cid, name, how)` row per distinct addressed (address, name,
  role), `how` the role. An input address is never a `missing`, `block` or
  `block_ref` row (R2): input content the store lacks is not missing
  metadata and is not this store's storage. An InputSet absent or
  unreadable for now is never a `missing` row either, since it says nothing
  of stored bytes and must not make `store_stats` or `root_stats` partial.
  It leaves a wait record, meta `inputset_wait:<completion>` = the
  InputSet's cid. `retryMissingInputSets` asks about each record of the
  member's runs at every catch-up, one `has` each, and ingests the InputSet
  once it is there. The record is cleared by that ingest, by a re-ingest of
  the run, by `forget` and by `reset`. A malformed InputSet is warned about
  once, leaves no record, and the run is indexed without inputs. A snapshot
  carries its runs' wait records in its meta, and a seed copies them for
  the seeded runs.
- `forget` removes `run_input` and `consumer` by completion, `project` and
  `handoff` by cid; `reset` empties them.
- Mixed builds: a pre-v6 build recreates a v6 cache it opens (and a v6
  build recreates an older one), so two builds sharing one cache file rebuild
  it in turn; give each build its own `cas.index.path`.
- Lookups: `isProjectIndexed`, `isHandoffIndexed`, `runInputs(completion)`.
  The page's P4 queries are in `ExplorerQueriesTest`'s no-scan check.

*Amended 2026-10-26 (cross-run item lineage, spec
`docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md`
section 5.4; schema 7, snapshot `index/v7.sqlite`, rebuilt from the Store
Log with no migration):*

```
run_input(completion_cid, role, name, address, size, sample, position)
  -- run_input gains a position column, last: the input's index in the InputSet's sorted list
input_origin(completion_cid, position, item_cid, collection_cid, selection_cid)
  index input_origin_item (item_cid)
  index input_origin_completion (completion_cid, position)
item_input(completion_cid, item_cid, positions)
  index item_input_item (item_cid)
  index item_input_completion (completion_cid)
item_task(completion_cid, item_cid, task_hash, process, tag)
  index item_task_item (item_cid)
  index item_task_completion (completion_cid)
```

- `input_origin` has one row per (input, Origin), keyed by the input's
  position in the run's InputSet. A join on address and name would attach
  the wrong Origins to a directory input (two items' identical directories
  differ only in `from`) and could not join an input with no address. The
  input's name, role and address come from `run_input`.
- `item_input` has one row per Derived item, `positions` a packed JSON
  array. One row per item, not per (item, input), because shared references
  (a genome, an index) fan out to every item: an estimated 6,000 to 10,000
  rows for a 100-sample rnaseq run as separate rows, about 1,000 packed.
- `item_task` has one row per (Derived item, TaskRef). No page reads it in
  this work; it exists so a later "made by" line is a UI change only.
- `item_input_completion` and `item_task_completion` exist for
  `usedByOf` and `clearRun`, which look rows up by run rather than by item;
  `input_origin_completion` also carries `position` for the join to
  `run_input`.
- `ingestRun` reads the ItemLineage the RunCompletion links, with the same
  wait-and-retry as the InputSet: an absent or unreadable one leaves a wait
  record, meta `itemlineage_wait:<completion>` = its cid, and is never a
  `missing`, `block` or `block_ref` row. `input_origin` rows come from the
  InputSet's `from` lists (written with `run_input`) and `item_input` and
  `item_task` from the ItemLineage.
  An ItemLineage whose `input_set` is not the RunCompletion's `input_set`
  is skipped with one warning, since its positions would point into the
  wrong list: no `item_input` or `item_task` rows. `forget` and `reset`
  clear all of it by completion, as for `run_input`; a snapshot carries
  its runs' wait records and a seed copies them.
- Used by (`usedByOf`) reads `input_origin` by `item_cid` (the upstream
  item), then `item_input` of that run for the derived items whose
  `positions` contain the input's position (`json_each`). It reads
  snapshots only, not tail runs' blocks: a tail run's derived items appear
  once the member's snapshot catches up. Page queries are in
  `web/src/queries.json` and in `ExplorerQueriesTest`'s no-scan check.

Snapshot (§15). A snapshot copies the `project` and `handoff` rows its
member's Store Log announced and the `run_input`, `input_origin`,
`item_input`, `item_task` and `consumer` rows of its runs (`completion_cid
IN main.run`), always; a seed copies them for the seeded runs.

`project_stats` is written by each snapshot that has storage figures, from
the current `add project` Claims in the cache's `claim_current`: the whole
composition's, whichever member logged them, read before the cache is
detached. A member counts in this store when it is a run or Selection root
here (any state) or an OutputItem of one of this store's output
collections. A run reaches its output items, a Selection its items, an item
itself, with the closures `root_stats` uses, so a one-member project's
figures equal that root's, and a block a sweep took is left out. Per
project, each raw block a member here reaches counts once: pinned when a
pin root reaches it, else held when a held root does, else releasing; the
`store_stats` partition restricted to the project. `reachable_bytes` is the
sum of the three, so a released member's bytes count, as releasing; this
differs from `asserter_stats`, whose reachable counts content roots only.
Root states come from the member's own Claims, as in P2. `roots` is how
many distinct members are here; a project with none here has no row. A
lean snapshot has no rows (`project_stats` is one of the tables a lean
snapshot empties). Cost, on `gen_year.py`'s year
(547,500 references, 41 projects, 4,209 memberships, one project of all
1,825 runs): about +0.3 to 0.5 s on a 2 s figures computation, with no
extra pass over `block_ref`. There is no `partial` column, so a project that
reaches a missing item shows no "at least".

### `POST /api/handoff` (`robsyme.cas.core.HandoffWriter`)

Same transport as `/api/put` (§16): 403 without the token or with a foreign
Host/Origin, 415 for anything but `application/json`, 413 over 2 MiB, 405
when the explorer has no writable member; those answers are plain text.
Every other answer is JSON. The writer runs under Put's monitor.

```
request:  { template, selection: "<cid>" | {"/": "<cid>"}, project: "<cid>" | {"/": "<cid>"} | null,
            files: [ { name, text } ], params: { <name>: "<string>" | { "file": "<name>" } }, timestamp }
200:      { address, files: [ { name, address, size } ], params, entry, written }
refusals: { error, message }: 400 bad_request | clock_skew | not_found | wrong_kind | too_large; 409 not_writable
```

- Refused as `bad_request`: unknown fields; a template outside
  `[A-Za-z0-9._/-]{1,256}` (a pipeline name); no files; a file name outside `[A-Za-z0-9._-]{1,128}`, `.`,
  `..` or repeated; text that is not a string; a param that is neither a
  string nor `{file: <one of files>}`; a lone surrogate; a timestamp not
  ISO-8601 UTC with milliseconds. A selection that is not a held Selection,
  or a project that is not a held Project, is `not_found` or `wrong_kind`.
- Each file is a raw block of its text's UTF-8 bytes, unchanged, in a Leaf
  with `reason: null`. The Handoff's `params` resolve file references to
  `cas://<cid>/<name>` and then go through `Records.scrub`, as RunManifest
  params do: a local path becomes `[redacted-location]`, a credential is
  redacted, and store-local top-level keys (`workDir`, `outputDir`, ...)
  are left out. The response's `params` are resolved and not scrubbed;
  they are never stored, and the page builds the run command from them.
- Order, as Put's: shape and address, dry run (writes nothing, `entry:
  null`), "already written" (success, `written: false`, the existing entry),
  then the clock, the Selection and the Project. Then raw blocks the
  writable member lacks, the Handoff, the `handoff` entry last, and ingest.
  `asserted_by` is the server's signer.

### Sweep (§19)

- A `project` entry roots its Project and nothing under it. A `handoff`
  entry roots its Handoff, which keeps its files as content and its
  Selection whole, as a nested Selection member is kept; its `project` link
  is not followed (R5, R6). Both follow the Selection rule, across members
  too (§19, D2): a clean current `delete` frees one; a `del` superseding the
  delete, a conflicted deletion or a pin keeps it. The report counts them
  (`projects`, `deleted_projects`, `handoffs`, `deleted_handoffs` under
  `roots`). The snapshot's storage figures follow the same rule (§15, R3),
  so a store's releasing figure is what its sweep takes.
- Unknown entry kinds (R8). An entry name is a Store Log entry when it
  starts with 13 digits and `-`. One this build cannot read (an unknown
  kind, a hyphenated kind such as `input-set`, or a cid form it cannot
  parse) is of an unknown kind; its kind is the text between the first `-`
  and the last `-`, or the whole rest when there is no other `-`. Other
  names in `log/` are ignored. A sweep dry run lists, per member, the kinds
  of the Store Log entries this build does not know (`unknown_kinds`
  in JSON, an `unknown` line in text). An applied sweep that finds any, in
  any member's log, the writable one or a read-only one, refuses before it
  marks and exits 1: "the Store Log of member <alias> holds entries of
  kind(s) <list> that this build does not know, so a sweep could delete
  blocks they keep; upgrade nf-blocks". A re-check that finds one logged
  during the sweep stops it the same way before its next step. Such entries
  are never deleted, even as dangling. The CLI `sweep` verb is the only
  caller that starts a sweep.
- *Amended 2026-10-26 (cross-run item lineage):* Mark keeps a run's
  `item_lineage` as metadata, as itself, and follows nothing under it, as
  for `input_set`. `Input.from` is not followed (§19 decision 4).

### Copy (§21)

`CopyClosure.categoryOf`: `project` and any `policy.*` attribute are
`projects`; `sample`, `annotation` and `same_sample` are `annotations`;
`name` stays `names`. With `projects` carried, each Project named by a
carried current `add project` Claim whose value is a link travels too: in
the `claims` group with kind `Project`, at the depth of its shallowest
membership, sized like any block. It is then a Claim subject like any
closure block, so its name Claims, policies and project-scoped annotations
travel with their history, and a destination opened alone can name the
project. Claims on a Project are not searched for further memberships. A
Project the source lacks is listed in `missingAtSource`, and its membership
still travels. The copy writes a carried Project with the Selections and
Claims and gives it a `project` entry when the destination's log lacks one.
Deletions never travel (§21), so a Project with a clean `delete` at the
source that a carried membership names arrives undeleted at the
destination; delete it there too, or remove the membership before copying.
A carried Selection brings the Handoffs that cite it and that the source's
log announced, with their files, in the `handoffs` group at every depth,
and the copy logs each with a `handoff` entry before the Transfer (§21). A
Handoff deleted at the source (clean, unpinned) does not travel.

*Amended 2026-10-26 (cross-run item lineage):* `item_lineage` travels in
`run_records` at every depth, read like a record, with nothing under it
followed (§21 `input_set` ruling).

### Explorer: Lineage and Used by (2026-10-26)

Spec `docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md`
sections 5.5 and 5.6; the code is `web/src/views/item.js`. The ItemLineage
and InputSet blocks are read by the page only for tail runs (a run the
snapshot has not caught up with); otherwise the panels read the snapshot's
`item_input` and `input_origin`.

- The Lineage panel keeps its thread, inputs on top and the item
  highlighted, and gains upstream nodes. An input with Origins shows each
  upstream item above its file: the item linked to
  `#/item/<collection>/<item>`, the run chip and the Selection's name from
  its Claims (short CID when unnamed). Several Origins on one file show as
  several upstream nodes. An input with no Origin shows the file only,
  dashed when no member holds its bytes. The page never names an upstream
  item it did not read from a record.
- An Origin with a null `collection` does not name a run, since the item
  CID alone matches every run that published identical content. Its node
  links to `#/item/-/<item>`, reads "picked by query", and shows the
  Selection with no run chip.
- An upstream item held by a member is linked there (`hrefIn`). When no
  member holds it the node shows its short CID and "not in any store here"
  with no link, and the run and Selection labels when they can be read.
- Run scoping: lineage is a fact about one run. With a collection in the
  URL the panel shows that run's lineage only, so an older run's page is
  unchanged even when a newer run recorded lineage for the same item CID.
  With `-` in the URL the panel shows every run that recorded lineage for
  the item, grouped under a run chip per run, ungrouped when there is one.
- Runs without an ItemLineage (written before this change, or
  `item_lineage: null`) show the panel as before. The generic block page
  names the new kind "a block of kind ItemLineage".
- Used by lists the downstream items whose runs recorded an Origin naming
  this item, each as `output · title` under its run chip, with the
  Selection the consumer read it through when there was one. Other members
  are asked too (`fromEach`), and a row from another member links there.
  The panel is absent when nothing used the item. It is scoped like
  Lineage: a collection in the URL keeps the rows whose `via` is that
  collection; `-` keeps all of them, so a consumer that picked the item by
  query is listed only there.
