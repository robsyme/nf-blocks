# Milestone 4 (Cloud: a Writable S3 Member, Publishing from S3, the Fusion Address Provider) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Every task is bound by `DESIGN.md`; when this plan and `DESIGN.md` disagree, `DESIGN.md` wins and this plan gets fixed.

**Goal:** A run on AWS Batch, with or without Fusion, publishes its outputs into a writable S3 member with real Content Addresses and no head-node bytes where S3 or the task node can supply the address; a consumer on a fresh head node seeds its index from the member's Index Snapshot instead of scanning a year of blocks; and the Gate proves it, tier one on every commit and a new tier two on demand against the scidev queue.

**Architecture:** Wave 0 lays four independent foundations: one S3 seam (`S3Ops`) over nf-amazon's own SDK client, the schema change that moves `provider` out of the Leaf into the RunCompletion, a Directory Manifest walk that works on an object store and decodes Fusion's link encoding, and a config that accepts S3 members anywhere. Wave 1 builds the S3 member's parts on that seam: the block store and Store Log, the coordinate tree (behind a new interface, local and S3 held to one conflict contract), and snapshot storage with the conditional upload and the run-count guard. Wave 2 adds the three behaviours that use them: index seeding from snapshots, the node-side hashing `afterScript`, and the publish addresser (`.command.cas`, `s3-copy`, head-node fallback). Wave 3 wires members into `CasSession`, `CasLinStore`, the observer and the verbs, and makes staging out of an S3 member a server-side copy. Wave 4 is the Gate (tier one additions, the tier-two harness); wave 5 the documents.

**Tech Stack:** Groovy 4 (`@CompileStatic`), Spock, Nextflow 26.04.6, nf-amazon 3.9.2 and the AWS SDK for Java 2.46.8 it bundles (compile-only), `org.xerial:sqlite-jdbc` 3.50.3.0, Node 24 and `@ipld/schema` 7.0.12 for the page, Python 3 stdlib for tier one, Python 3 with boto3 for tier two, bash.

**Spec:** the wayfinder map `../.scratch/post-gate/map.md` and the `## Answer` of each ticket under `../.scratch/post-gate/issues/` it cites: 01, 02, 03, 04, 05, 06, 07, 11, 14, 15, 16 (each answer is binding; this plan does not reopen them). Research: `../.scratch/research/s3-writable-store.md`, `s3-write-measurements.md`, `batch-publish.md`, `fusion-batch-remeasure.md`, `fusion-nextflow-integration.md`. Background contract: `../.scratch/content-addressed-lineage/spec.md` §1.2, §3, §14 and `DESIGN.md` §0, §2, §5, §6, §8, §11, §12, §14, §15. Throwaway probes that show working code (read with `git show <branch>:<path>`, never merge): `prototype/05-batch-publish` (`prototype/05/batch.config`, `fusion.config`, `run.sh`, `check.py`, the `realOf` fallback in `DirectoryManifestBuilder`), `prototype/06-fusion-remeasure` (`prototype/06/main.nf`, `nextflow.config`), `prototype/14-s3-writes` (`prototype/14/Probe.java`: conditional PUT, hand-driven multipart over `fromContentProvider`, `CopyObject` with SHA-256).

**Branch:** `feat/cloud-m4`, from `main` at `8dc1acb` (0.1.0-beta.2). The version stays `0.1.0-beta.2` until the release that follows acceptance; `gate/gate.config`'s pinned plugin id follows `build.gradle`.

## Decisions this plan carries (each from its ticket)

The ticket in brackets holds the reasoning; Task 15 writes each into the `DESIGN.md` section it amends.

1. **One S3 client, nf-amazon's** [02 Q1, 01 Q1]. The AWS SDK becomes `compileOnly` against `io.nextflow:nf-amazon:3.9.2`, `nextflowPlugin.requirePlugins = ['nf-amazon@>=3.9.2']`; the explorer's class-loader shim and URL-connection client go. `explore` builds its client through the same factory from its loaded config and keeps the us-east-1 fallback when no region is set. SSE-KMS, storage class and requester pays are per-request fields nf-blocks sets itself. `aws.client.s3Acl` is not applied to member writes (ticket 02 decision 9 does not list it); the README says so.
2. **Existence** [02 Q2, 14 item 1]. A block of 1 MiB or more is checked with `HeadObject` first; below that a conditional PUT alone. Every write carries `If-None-Match: *`; a 412 is success (rule 4).
3. **Upload** [02 Q3, 14 items 3 and 4]. Known length up to the single-request limit is one `PutObject` with `ChecksumAlgorithm SHA256`; above it a hand-driven multipart upload with per-part SHA-256, parts `max(64 MiB, ceil(size/10000))`, streamed from a file channel with `RequestBody.fromContentProvider`. `putStreaming` spools to `cas.tmpDir` (default `java.io.tmpdir`) while hashing, then uploads.
4. **Layout** [02 Q4]. Byte-for-byte the local keys under the member prefix: `blocks/<xx>/<cid>`, `log/`, `coords/`, `nf/`, `index/v3.sqlite`, `index.html`, plus `tmp/` (staging keys of decision 13).
5. **Immutability on S3** [02 Q5] is the conditional writes only; a bucket policy (deny `PutObject` on `blocks/*` without `s3:if-none-match`, deny `DeleteObject` on `blocks/*` except to a sweep role) is documented as hardening.
6. **Coordinate conflicts** [02 Q6, 03 Q3-4]. The same outcome as the local store, checked explicitly: `HEAD` each ancestor pointer before writing `coords/a/b`; list `coords/a/` with max-keys 1 before writing `coords/a`. One test pins the outcome on both backends. Last write wins on one coordinate; an ancestor pointer that slipped past the check shadows everything below it, and `explore` shows shadowed pointers as a warning. No locking anywhere.
7. **`nf/`** [02 Q7, 01 Q6]. `DefaultLinStore` as is, with `CasLinStore` passing `FilesEx.toUriString(location)`; `nextflow lineage find` costs O(records) GETs on S3, documented.
8. **Snapshot and page on S3** [02 Q8, 03 Q1]. Built locally, uploaded with one `PutObject`: `Cache-Control: no-cache` for the snapshot, `immutable` for blocks; the snapshot's run count stored as `x-amz-meta-runs`; `index.html` rewritten only when its stored `ChecksumSHA256` differs. The snapshot upload is conditional on the ETag it replaced (`If-Match: <etag>`, or `If-None-Match: *` when absent); a 412 skips the rewrite with an info log. Locally, the atomic move stays.
9. **Storage class and SSE** [02 Q9] come from `aws.client.storageClass`, `storageEncryption`, `storageKmsKeyId` and requester pays. `GLACIER` and `DEEP_ARCHIVE` are refused at config time; infrequent-access classes and `GLACIER_IR` are allowed with a warning naming packing.
10. **Config** [02 Q10]. The refusals at `CasConfig.groovy:149` (writable S3) and `:193` (S3 in `cas.resolve`) go; member locations go through `FileHelper.asPath`. Still rejected: a malformed S3 URI, a scheme other than a local path or `s3://`, an archive storage class. A bucket-root member (`s3://bucket`) is allowed.
11. **Clock skew** [03 Q2]. The local clock is compared with the `Date` header of the first S3 response; a skew above 1 minute warns, above 5 minutes aborts the run. Local members unchanged.
12. **Blocks from two writers** [03 Q5]: conditional and idempotent, 409 `ConditionalRequestConflict` retried. The page's `put` behaves the same.
13. **`provider` leaves the Leaf** [16 Q1-2]. The RunCompletion gains `providers: {<provider>: [<address>, ...]}`, addresses sorted; OutputItem goes to `schema: 2`; readers accept schema 1 and ignore its `provider`; the IPLD Schema and `web/src/generated/schema.json` are regenerated. `head-node` is self-computed; `fusion-node` and `s3-copy` are asserted. Existing beta stores' items change address on their next publish.
14. **`s3-copy`** [16 Q3-6]. The default for every S3-to-S3 publish up to the single-request limit, any region. With a digest in advance (a `fusion-node` line): `HEAD` the final key (exists = success), else `CopyObject` straight to it with `If-None-Match: *` and SHA-256, compare, and on mismatch delete it and abort. Without one: copy to `<member>/tmp/<uuid>` with SHA-256, `HEAD` the final key, copy staging to final with `If-None-Match: *` (412 = success), delete staging. With a `fusion-node` digest the copy still runs with SHA-256 and the two must agree; disagreement aborts the run. The provider recorded is `s3-copy` whenever S3 returned the digest. Above the limit: with a `fusion-node` digest, `UploadPartCopy` to the final key, provider `fusion-node`; otherwise the head-node streaming read (spooling to scratch), provider `head-node`. Never unaddressed by default.
15. **The toRealPath fix** [05]. `DirectoryManifestBuilder` stops calling `toRealPath` on a path whose provider does not support it; an object store has no links to resolve, so the absolute normalised path is the real one.
16. **Fusion's links are content** [15 Q1-6]. When publishing a directory from an object store that holds a `.fusion.symlinks` object, each listed name is a link whose target is its object's body; the DESIGN §6 link rules apply; the sidecar is left out of the manifest. Targets by key arithmetic: relative in-tree gives `symlink` if the target key exists (object, prefix or decoded link), else `unresolvable`; relative escaping is followed and stored if the key exists, else `unresolvable` with `[redacted-location]`; absolute `/fusion/s3/<bucket>/<key>` becomes `s3://<bucket>/<key>`, then as escaping; any other absolute is `unresolvable`, `[redacted-location]`. Chains follow with depth 64 and cycle detection. A listed name with no object, or a body over 4096 bytes or holding NUL, is `unresolvable`; an unparseable listing records the directory literally, counts an anomaly and warns. Never on a local directory. Without Fusion, an S3 work dir's links arrive flattened to copies.
17. **Links staged into an S3 work dir** [15 Q7] are materialised as copies of their targets.
18. **Fusion Address Provider** [spec §14, 06]. A `process.afterScript` config default, chained ahead of the user's, reads the `### outputs:` block of `.command.run` (patterns: it expands globs and skips absent optional outputs), hashes only the declared outputs, writes `.command.cas` in `sha256sum` format and never exits non-zero. `.command.cas` is fetched once per task at publish; a miss falls back.
19. **Index cache seeding** [04 Q1-10]. When the cache has no watermark for a member, it downloads that member's `index/v3.sqlite`, copies its rows (`INSERT OR IGNORE`), adopts its `store_log_watermark`, marks `block_scan:<member>` done, then catches up the Store Log tail with the 10-minute overlap. No usable snapshot (absent, other `schema_version`, unreadable) falls back to the full scan, warning with the member, the reason and `nf-blocks:snapshot`. `claim_current` is never copied: it is recomputed for every seeded subject. `meta` records `seeded_from:<member>` = the snapshot's `snapshot_written_at`. A run rewrites the snapshot only if the writable member's catch-up in this run finished without error and the new snapshot has at least as many `run` rows as the old one. Keep the 64 MiB cap; any member with a snapshot is seeded; seeding is triggered only by an absent watermark; `cas.index.path` on persistent disk is the documented way to skip it; no remote ranged-GET queries from a run.
20. **No new verbs** [07]. Rule 5 becomes: a verb exists only for an operation the spec names that a run cannot perform itself, and each lands in the milestone that also adds the Gate assertion exercising it. Milestone 4 adds none.
21. **Gate tier one** [04 Q11, 16 Q1]: a cold-cache consumer run seeded from the producer's snapshot gives the same `fromStore` answer, reads no metadata block of a run at or before the snapshot's watermark, and leaves a snapshot whose `run` count did not fall; a second run with the snapshot removed falls back to the full scan with the same answer and prints the warning. Assertion 2 adds: the same items published with `head-node` and with `fusion-node` have identical OutputItem addresses.
22. **Gate tier two** [11, 03 Q6]. `make gate-tier2` (`gate/tier2/tier2.sh`), on demand, from the laptop with Rob's `scidev` SSO, reusing a GATE_ROOT that passed tier one. Work dir `s3://scidev-playground-us-east-1/robsyme/nf-blocks-gate/<run-id>/work/`; the writable S3 member a throwaway us-east-1 bucket per run at prefix `s3://<bucket>/cas`, created and torn down under `cloud.sh`'s trap pattern; the work prefix deleted and open multipart uploads aborted on every exit. T1 (assertion 12), T2 (11), T3 (5, cloud), T4 (6, cloud), T5 (ticket 04 on S3), T6 (two writers). *Rob, 2026-09-28:* T2 publishes into a fresh member and expects `s3-copy` on every file, each copy's SHA-256 having agreed with the file's `.command.cas` digest; T2b, a smaller Fusion run into the member T1 already filled, expects `fusion-node`. Head-node bytes logged and reported, not asserted. 45-minute timeout, no dollar ceiling; the harness prints the Batch job count and total job seconds.
23. **Directory Manifests carry no execute bit** [ticket 15 addendum, Rob 2026-09-28]. `executable` leaves the entry modes, so no backend can change a manifest address: DirectoryManifest goes to `schema: 2` with modes `regular`, `symlink`, `directory`, `unresolvable`; readers accept schema 1 and read `executable` as `regular`; local walks stop recording the bit. Materialising a manifest no longer sets execute permission. Gate assertion 5 expects `regular` where the tree has an executable file. Task 2 (records, IPLD Schema, page) and Task 3 (the walk).

## Decisions made where the tickets are silent

Rob reviews these. Each names the task that implements it.

1. **RunCompletion goes to `schema: 2` too** (Task 2). It gains a field, so a reader must know whether to expect it; schema 1 reads as `providers: {}`. In the IPLD Schema `providers` is the one `optional` field, because a struct cannot vary its fields by the value of `schema`; the page checks that a schema-2 RunCompletion carries it.
2. **The schema-1 Leaf survives as `LeafV1`** in the IPLD Schema (Task 2). The page validates an OutputItem's leaves as `Leaf` or `LeafV1` by the item's `schema`; any other `schema` is refused. Groovy refuses an OutputItem or RunCompletion whose `schema` is not 1 or 2.
3. **`providers` covers every address the run published** (Tasks 2, 3, 10; ticket 16 decision 1): each file leaf, each directory leaf, and each file inside a published directory. A directory leaf's address (its manifest) is always `head-node`, because the manifest is encoded on the head node whatever addressed the files inside it. The files inside a directory carry the provider their addresser returned: `DirectoryManifestBuilder.Result.providers` collects `provider -> addresses` during the walk (Task 3), `CasFileSystemProvider` records it on the `Publish` (Task 10), and `Join` merges it into the RunCompletion (Task 2). One address may appear under two providers in one run (published twice, two ways). Keys are checked against `head-node`, `fusion-node`, `s3-copy` in Groovy and in the page; the schema types the map `{String:[&Any]}`.
4. **When each provider is recorded** (Task 10). `fusion-node`: the node digest named a block the writable member already held (no bytes moved), or an `UploadPartCopy` above the single-request limit. `s3-copy`: S3 returned a SHA-256 from a copy. `head-node`: the head node streamed the bytes. With a local member and an S3 source, a node digest for a block the member lacks still needs the bytes, so the head node streams them, compares its hash with the node digest (disagreement aborts) and records `head-node`. Consequence for tier two (Rob, 2026-09-28): T2 publishes into a fresh member, so every file leaf is `s3-copy` and each copy's SHA-256 is checked against the file's `.command.cas` digest; T2b, a smaller Fusion run into the member T1 filled, finds every block present and records `fusion-node`.
5. **`cas.nodeHash`** (Tasks 4, 9, 10), a Boolean, defaults to `fusion.enabled`. It turns on both halves of the Fusion provider: the `afterScript` default and the `.command.cas` read. Tier one sets it on the local executor for the `again` run, so assertion 2's addition runs on every commit. The recorded name stays `fusion-node`: it means "the task node's digest", Fusion or not.
6. **The hashing script** (Task 9) lives in `src/main/resources/robsyme/cas/node-hash.sh`: task directory `${NXF_CHDIR:-$PWD}` (Fusion always sets `NXF_CHDIR`); nothing is written when `.command.run` is not there (a `scratch true` task on a grid executor falls back); `sha256sum`, else `shasum -a 256`, else nothing; `nullglob` always and `globstar` when the shell has it; no word splitting of a pattern; a matched symbolic link is skipped (staged inputs are links); a matched directory is hashed with `find <dir> -type f`; the output goes to `.command.cas.tmp` and is renamed. A config `afterScript` that is a closure is left alone, with one warning naming the process selector, and those tasks fall back.
7. **`.command.cas` reading** (Task 10): parsed with coreutils' escaping (a leading `\`), `*` or space as the mode character, lines that do not parse ignored; a file over 16 MiB is ignored with a warning. The task directory is the first two path segments under `session.workDir`; a source outside `workDir` (a `storeDir`) gets no node digest. Fetched once per task directory per run and kept in `CasSession`.
8. **Head-node bytes** (Task 10): the publish addresser counts the bytes it streams and logs one info line at `onFlowComplete`, `nf-blocks: the head node read <n> bytes to address <m> file(s); head-node <a>, fusion-node <b>, s3-copy <c>`. Tier two reports it.
9. **Staging out of an S3 member into an S3 work dir is a server-side copy** (Task 12): `CopyObject` with SHA-256 from the member's block key to the target, the returned digest compared with the CID (mismatch deletes the target and aborts). A local member staging into S3 keeps today's stream through nf-amazon's output stream.
10. **The client factory** (Task 1) is `new AwsClientFactory(new AwsConfig(aws), aws.resolveS3Region()).getS3Client(S3SyncClientConfiguration.create(props), global)`, the calls `S3FileSystemProvider.createFileSystem` makes, and not the `S3FileSystem` accessor chain: it needs no session (so `explore` and a run share it), and skips the wrapper's `listBuckets` probe. `resolveS3Region()` is the us-east-1 fallback.
11. **One seam, `S3Ops`** (Task 1): every S3 request nf-blocks makes goes through it, bucket-scoped. `SdkS3Ops` is the SDK; the test double `MemoryS3Ops` holds objects in memory and counts requests and body bytes. `S3MemberFiles` reads through it too, so an explorer test can read what an S3 writable member wrote.
12. **The single-request limit is 5 GiB** (5,368,709,120 bytes), S3's limit for `PutObject` and `CopyObject`.
13. **A `PutObject` response's `ChecksumSHA256` is compared with the CID** (Task 5); a mismatch (a source file changed between hash and upload) deletes the object and fails the write. `put(cid, in, size)` on S3 spools through `cas.tmpDir` (rule 2); `putDagCbor` uploads its encoded bytes directly (metadata, bounded); `putFile(Path)` for a default-filesystem source hashes the file, then uploads from it (two reads, no spool).
14. **409 `ConditionalRequestConflict`** (Task 5): a write is tried up to 3 times in all; for a multipart upload the whole upload restarts. The Store Log's `putEntry` uses the same count and then throws, and its caller warns and continues (rule 3).
15. **Cache-Control on S3** (Tasks 5, 7): blocks `public, max-age=31536000, immutable`; `index/v3.sqlite` and `index.html` `no-cache`. `coords/`, `log/`, `nf/` and `tmp/` get none.
16. **The snapshot guard's order** (Tasks 7, 11): the old snapshot is looked at (`HEAD`, or a local stat) before the catch-up, so the ETag guard covers the catch-up window; the run-count guard applies to every writer (a run, `nf-blocks:snapshot`, `explore`); an S3 snapshot without `x-amz-meta-runs` (uploaded by another tool) is downloaded once and counted. Skips log at info with one of `over_cap`, `fewer_runs`, `replaced_meanwhile`, `catch_up_failed`; `nf-blocks:snapshot` prints the skip and exits 0.
17. **Seeding's row rules** (Task 8): rows are copied per run, Selection and Claim not already indexed, tagging `run.member` and `log_entry.member` with the member's alias, so seeding a second member into one cache duplicates nothing. The fallback warning goes to the `nextflow.cas` logger (terminal and `.nextflow.log`) and fires only when the member's Store Log is not empty.
18. **Tier one's "counting method"** (Task 13): during the seeded consumer run, the Gate sets mode 000 on every RunManifest, RunCompletion and OutputCollection block of the producer's runs at or before the snapshot's watermark (`fromStore` reads only OutputItem blocks, which stay readable). The assertion requires the run to succeed, its answer to be unchanged and its log to name no permission failure on the store. It is assertion 13, "a cold cache seeds from the Index Snapshot".
19. **The clock check** (Task 11) is one `HEAD` on the writable S3 member's snapshot key at `onFlowCreate`; `put` and `explore` check at start and refuse to start above 5 minutes. The warning and the abort name the skew and the fix (NTP).
20. **Shadowed pointers** (Tasks 6, 11): an S3 read through a coordinate with an ancestor pointer answers absent; `explore` lists the writable S3 member's `coords/` at start and prints up to 20 shadowed pointers and their count on stderr.
21. **Coordinates on S3** (Task 6): no directory marker objects; the pointer body is the local one (`cas://<cid>/<name>\n`).
22. **Config details** (Task 4): `cas.tmpDir` (string path); a trailing slash on an S3 location is dropped, so `s3://bkt/p` and `s3://bkt/p/` are one member and one cache file; the storage class is read from `aws.client.storageClass` (else `uploadStorageClass`) and judged only when an S3 member is writable.
23. **Fusion listing limits** (Task 3): a `.fusion.symlinks` over 1 MiB, not UTF-8, holding NUL, or with an empty or `/`-bearing name is unparseable, counted as one `unresolvable`; a listed name with no object is `unresolvable` with a null target. Ticket 15 decision 3 ("relative in-tree gives `symlink` if the target key exists (object, prefix or decoded link)") and decision 1 (a Fusion run and a local run give one Directory Manifest) disagree for a chain that ends nowhere (`a -> b -> missing`) and for a cycle: read literally, decision 3 makes `a` a `symlink` because `b`'s key exists. The plan reads it as decision 1 requires: a decoded link is chased to the end of its chain, and a relative in-tree link is `symlink` only when the chain reaches an object or prefix; a dangling chain or a cycle is `unresolvable`, as the local walk (which resolves through `toRealPath`) records it. Rob to confirm (pre-flight F31); if decision 3's literal reading wins, both walks change.
24. **Tier two's details** (Task 14): the consumer's writable member is `s3://<bucket>/cas-out`, T2 uses a fresh `s3://<bucket>/cas-t2`, T2b runs `gate/tier2/small` (the Test Pipeline's ALIGN and QC_DIR, their scripts and sample A's Meta Map copied verbatim, so its `aligned` items are T1's and its `qc` items t2's) into T1's `s3://<bucket>/cas`, and T6 uses a fresh `s3://<bucket>/cas-t6`; T2 and T2b compare `aligned` items with t1's and `qc` items with each other and with tier one's local `cold` `qc` item, since t1 flattens `alias.txt` (ticket 15 decision 5); T4 passes a directory input `--dir cas://<the qc/A/A_qc manifest of cas after t2b>`, whose `alias.txt` is a `symlink` entry, and asserts that first; job count and seconds come from `-with-trace`; the timeout is a watchdog that sends TERM to every recorded nextflow PID and then the harness, each nextflow running in the background under `wait`; T5's evidence of seeding is the cache's `seeded_from:lab` equal to the S3 snapshot's `snapshot_written_at`; T6's stale-ETag evidence is a deterministic boto3 probe (PutObject with If-Match on a replaced ETag must get 412) plus a best-effort race that starts two `nf-blocks:snapshot` verbs together on cold caches and requires exactly one to write, retrying the pair up to 3 times when both wrote without overlapping, and SKIPs when they never overlap.

## Contradictions found

Recorded for the parent session; the plan's resolution is in the decision named.

1. Ticket 11 T2 says every published file under Fusion has provider `fusion-node`; ticket 16 decision 3 says the provider is `s3-copy` whenever S3 returned a digest, which it does for every block copied into an S3 member. *Ruled by Rob, 2026-09-28:* T2 publishes into a fresh member and expects `s3-copy` with every copy's digest agreeing with `.command.cas`; T2b, into the member T1 filled, expects `fusion-node` (carried decision 22).
2. Ticket 15 decision 1 (a Fusion run and a local run give one Directory Manifest) held only for trees without executable files, since an object store has no execute bit. *Ruled by Rob, 2026-09-28:* manifests drop the execute bit (carried decision 23).
3. Ticket 02 decision 9 allows `GLACIER_IR` with a warning, but nf-amazon 3.9.2's `AwsS3Config.parseStorageClass` accepts only `STANDARD`, `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING` and `REDUCED_REDUNDANCY`, and turns anything else into null after its own warning. Since nf-blocks inherits the parsed value, `GLACIER_IR` never reaches a request: blocks go to `STANDARD`. Task 4 warns that nf-amazon ignores it (kept by Rob, 2026-09-28); the refusal of `GLACIER` and `DEEP_ARCHIVE` stays as decided even though nf-amazon would drop them too.
4. Spec §14 says the head node fetches `.command.cas` "at `onFilePublish`"; the address is needed inside `upload()`, which runs before `onFilePublish`, to write the Pointer File. Task 10 fetches it in `upload()`; Task 15 rewords spec §14.
5. Spec §1.2 calls tier two "nightly"; the map decided on demand. Task 15 rewords spec §1.2.
6. `DESIGN.md` §2 ("In the Walking Skeleton a member location is a local directory path", the 2026-09-25 amendment that S3 members are read-only), §15 decision 9 ("S3 members are read-only and explore-only"), §6 ("schema (integer, `1`)") and §8 (`upload()` records provider `head-node`) are superseded by the tickets; Task 15 amends each.

## Pre-flight corrections

Findings of `.superpowers/sdd/2026-09-27-cloud-milestone-4/preflight.md`, applied as ruled in `progress.md`'s "Pre-flight scan".

- F1: S3 test buckets of 3+ characters (`s3://bkt`): Task 4 tests, Review Focus 5, silent decision 22, Task 15 §2 text.
- F2: `withSpool` names `cas.tmpDir` only for spool failures; body exceptions propagate: Task 5 `S3BlockStore`.
- F3: `writable.has(node)`, not the composite: Task 10 `PublishAddresser.address`.
- F4: `S3Copied` is a top-level `@Canonical` class in `S3Types.groovy`: Task 10 (Files, code, `git add`), shared interfaces, File Structure.
- F5: unescape is one left-to-right scan; test adds the `a\\nb` line: Task 10 `NodeDigests` and `NodeDigestsTest`.
- F6: a copy failing with an `IOException` other than a mismatch warns and falls back; null staging SHA-256 throws: Task 10 `address`, `copyFrom`, new `PublishAddresserTest` feature, Task 15 §8.
- F7: a copy's `BlockMismatchException` becomes `AbortRunException` naming the file and `.command.cas`: Task 10 `address`, new `PublishAddresserTest` feature.
- F8: provider test asserts through `session.addresser.counts` and the recorded `Publish`, no spy seam: Task 10 `CasFileSystemProviderTest`.
- F9: `_permission_failures` matches `could not be read` plus a locked path; fixture in the real format: Task 13.
- F10: `relock` restores 444: Task 13 `gate.sh`.
- F11: T2 and T2b compare `aligned` items with t1's, `qc` items with t2's and tier one's `cold`: Task 14 table, T2, T2b, small pipeline note, unit tests, silent decision 24.
- F12: `produce`/`consume` record a failing status with gate.sh's `|| status=$?` pattern: Task 14 `tier2.sh`.
- F13: every nextflow runs backgrounded under `wait` with its PID recorded; the watchdog TERMs them, then the harness: Task 14 `tier2.sh` (`run_nf`), silent decision 24.
- F14: `refs` passes the `qc/A/A_qc` manifest of `cas` after t2b (its `alias.txt` is `symlink`) and T4 asserts that first; T3 reads t1's manifest through its RunCompletion: Task 14, silent decision 24.
- F15 (ruling): race kept as best-effort SKIP; added a deterministic boto3 `IfMatch` probe that must get 412: Task 14 `s3gate.py if-match`, harness, T6, README prerequisite, unit tests, silent decision 24.
- F16: Task 2 Files and `git add` gain `DirectoryManifestBuilder`(+Test), `CasExtensionTest`, `CasOccurrenceTest`; `RecordsTest:34` moves to `regular`: Task 2.
- F17: Tasks 2 and 3 sequenced in wave 0; multi-task file list completed: Waves.
- F18: `HeadNodeAddresser` uses `Providers.HEAD_NODE`: Task 3.
- F19: Task 2 deletes the ternary, `isExecutable` and its imports; Task 3's sentence dropped: Tasks 2, 3.
- F20: "tried up to 3 times in all"; test uses 2 and 3 conflicts: silent decision 14, Task 5 test, Task 15 §5.
- F21: `putEntry` loops `ATTEMPTS`, then throws; the observer warns: Task 5 `S3StoreLogStorage` and its test.
- F22: `S3BlockStore` takes no `S3WriteOptions`: shared interfaces, Task 5, Task 10 test, Task 11, Task 15 §5.
- F23: `seedFrom` turns an `IllegalStateException` from the seed into `SnapshotUnusable('unreadable')`: Task 8.
- F24 (ruling): an absent watermark alone triggers seeding: Task 8 intro and `catchUp`, Task 15 §12.
- F25: the `IOException` branch uses the same condition and message, reason `unreadable`: Task 8 `warnUnusable`.
- F26: `checkClock` catches a failing `HEAD`, warns and returns: Task 11 code and note.
- F27 (ruling): `explore`/`gate-cloud` accepted broken between Tasks 4 and 11, `gate-cloud` run only at acceptance: Task 4 intro.
- F28: `COPYOUT` added to the `calls` vocabulary: Task 1 Interfaces.
- F29: ACL dropped from carried decision 1; README says `aws.client.s3Acl` is not applied: decision 1, Task 15 README and §5.
- F30 (ruling): providers cover every published address, files inside directories included: silent decision 3, shared interfaces, Task 2 (`Publish.contents`, `Join`, JoinTest), Task 3 (`Result.providers`, both walks, new test), Task 10 (directory `Publish`), Task 15 §8.
- F31 (ruling): the chain reading (dangling chains and cycles `unresolvable`) recorded in silent decision 23, which Task 15 copies into §17 (silent decision 17 is seeding, so the Fusion-link decision holds it).
- F32: `NodeDigests.parse` reads line by line counting bytes, null over the cap, warned in `load`: Task 10, shared interfaces.
- F33: JoinTest retitled; the directory-is-`head-node` rule pinned in Task 10's provider test: Tasks 2, 10.
- F34: Task 5 feature retitled to a conditional PUT answered 412: Task 5.
- F35: clock features split: over 5 minutes throws, 90 s does not, a failing HEAD continues, a local member makes no request: Task 11 `CasSessionS3Test`.
- F36: the absent declared directory is exercised and asserted: Task 9 `NodeHashScriptTest`.
- F37: Task 6 stages named paths: Task 6 Files and `git add`.
- F38: `@Requires` non-root on the `setReadable`/`setWritable` features: Task 9, Task 10.
- F39: assertion 11's SKIP text points at tier two T2/T2b: Task 13 Step 3.
- F40: node digests compare real paths of the work dir and the file's parent: Task 10 `PublishAddresser`.
- F41: IPLD `DirEntry.address` comment says "raw cid for regular": Task 2 Step 5b.

## Global Constraints

- Released artifacts only (DESIGN §0 rule 1): `io.nextflow:nextflow:26.04.6`, `io.nextflow:nf-lineage:26.04.6`, `io.nextflow:nf-amazon:3.9.2` (the version bundled with 26.04.6), all `compileOnly`; the plugin zip carries no AWS SDK jar. `dependencyCheck` stays green.
- `requirePlugins = ['nf-amazon@>=3.9.2']`. nf-blocks code outside `robsyme.cas.s3` never imports `software.amazon.awssdk.*` or `nextflow.cloud.aws.*`; `robsyme.cas.core` still imports nothing from Nextflow.
- Memory bound (rule 2): one 1 MiB buffer per concurrent hash, at most 32; no file content in a byte array or `ByteArrayOutputStream`, S3 included. `memoryBoundTest` (48 MiB heap) gains an S3 multipart upload of a 256 MiB file.
- Rule 3: a provenance write that fails aborts; index, Store Log, snapshot, page and coordinate-shadow warnings log at warn and continue. Rule 4: already exists is success, including a 412. Rule 6: Gate assertions hash bytes themselves.
- No new command-line verb (ticket 07). `put` stays the only command-line write path.
- Unit tests never reach AWS: S3 behaviour is tested through `MemoryS3Ops`, and SDK request shapes through an execution interceptor that stops before transmission. Tier two is the only thing that talks to AWS, and only when a person runs it.
- Any edit to the ```` ```ipldsch ```` block in `DESIGN.md` §6 is followed by `cd web && npm test` (which regenerates `web/src/generated/schema.json`); the regenerated file is committed with it.
- Stay on Nextflow 26.04.6; every Nextflow fact is read from `git -C /Users/robsyme/dev/github.com/nextflow-io/nextflow show v26.04.6:<path>`.
- Never write "checksum" unqualified: Nextflow's field of that name is a path+size+mtime fingerprint. Say "S3's SHA-256" or "`ChecksumSHA256`" for the S3 object checksum.
- Plain prose in every user-facing string and document: no bold-first bullets, no em-dash chains.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A work-bucket output above 5 GiB with no node digest** (the common Batch case without Fusion). The copy cannot run; the head node streams the object once into `cas.tmpDir` and uploads it multipart. A full `cas.tmpDir` must fail the publish with an `AbortRunException` naming `cas.tmpDir` and the size needed, never record a partial block. Pinned in Task 10 (`PublishAddresserTest`, with the limit lowered through the constructor).
2. **The same bytes published twice in one run by two providers** (one leaf from a local path, one from S3). Both OutputItems keep one address; `providers` lists the address under both names. Pinned in Task 2 (`JoinTest`).
3. **A `.command.cas` line with a space, a backslash or a newline in the path, a `*` binary marker, or garbage.** Parsed or skipped; the publish never aborts on a bad line and falls back for that file. Pinned in Task 10 (`NodeDigestsTest`).
4. **A Fusion link chain and a cycle** (`a -> b`, `b -> target.txt`; `x -> y`, `y -> x`). The chain is followed to `symlink` (in tree) or to stored content (escaping); the cycle is `unresolvable` and counted, never a stack overflow. Pinned in Task 3 (`ObjectStoreManifestTest`).
5. **An S3 member at the bucket root, and one written with a trailing slash** (`s3://bkt`, `s3://bkt/cas/`). Keys have no leading slash (`blocks/..`, not `/blocks/..`); `s3://bkt/cas/` and `s3://bkt/cas` name one member and one cache file. Pinned in Tasks 4 (`CasConfigTest`) and 5 (`S3BlockStoreTest`).

---

## File Structure

```
nf-blocks/
  build.gradle                                  nf-amazon compileOnly, requirePlugins, SDK out of the zip (Task 1);
                                                S3 memory-bound test in memoryBoundTest (Task 5)
  DESIGN.md                                     §6 IPLD Schema and Leaf/OutputItem/RunCompletion prose (Task 2);
                                                every other amendment (Task 15)
  README.md                                     S3 member, Batch, Fusion, cache on an ephemeral head node (Task 15)
  src/main/resources/robsyme/cas/node-hash.sh   the afterScript (Task 9)
  src/main/groovy/robsyme/cas/
    s3/S3Ops.groovy                             the one S3 seam, bucket-scoped (Task 1)
    s3/S3Types.groovy                           S3Head, S3Body, S3PutOptions, S3Written, S3Part, S3Listed, S3WriteOptions (Task 1);
                                                S3Copied (Task 10)
    s3/SdkS3Ops.groovy                          S3Ops over nf-amazon's SDK client (Task 1; copyOut in Task 12)
    s3/S3Access.groovy                          the client factory from a config map (Task 1)
    s3/S3BlockStore.groovy                      blocks on S3 (Task 5; copyFrom Task 10; copyOut Task 12)
    s3/S3StoreLogStorage.groovy                 log/ on S3 (Task 5)
    s3/S3CoordinateTree.groovy                  coords/ on S3 (Task 6)
    s3/S3SnapshotStorage.groovy                 index/v3.sqlite and index.html on S3 (Task 7)
    core/Providers.groovy                       head-node, fusion-node, s3-copy (Task 2)
    core/Records.groovy                         Leaf without provider; OutputItem, RunCompletion and DirectoryManifest
                                                schema 2; no executable mode (Task 2)
    core/FileAddresser.groovy                   FileAddresser, Addressed, HeadNodeAddresser (Task 3)
    core/FusionLinks.groovy                     parse a .fusion.symlinks body (Task 3)
    core/DirectoryManifestBuilder.groovy        no execute bit (Task 2); realOf, the addresser, the object-store walk,
                                                Result.providers (Task 3)
    core/LoggedStore.groovy                     a block store that knows its Store Log storage (Task 5)
    core/LocalBlockStore.groovy                 implements LoggedStore (Task 5)
    core/StoreLog.groovy                        storageOf via LoggedStore (Task 5)
    core/CoordinateTree.groovy                  now an interface (Task 6)
    core/LocalCoordinateTree.groovy             the old class, renamed (Task 6)
    core/SnapshotStorage.groovy                 SnapshotStorage, SnapshotBase (Task 7)
    core/LocalSnapshotStorage.groovy            index/ and index.html on disk (Task 7)
    core/IndexSnapshot.groovy                   build to a temp file, then storage.replace with the guards (Task 7)
    core/Index.groovy                           seedFrom, catchUp with a SnapshotStorage (Task 8)
    core/NodeDigests.groovy                     parse .command.cas; task directory of a source (Task 10)
    core/ClockSkew.groovy                       judge a skew (Task 11)
    CasConfig.groovy, CasConfigScope.groovy     S3 anywhere, tmpDir, nodeHash, storage class (Task 4)
    CasSession.groovy                           Publish.contents (Task 2); coordinatesOf (Task 6); addresser (Task 10); members, seeding,
                                                snapshot guard, clock (Task 11)
    nio/CasFileSystemProvider.groovy            coordinate calls through the interface (Task 6); upload through the
                                                addresser (Task 10); download to an S3 target (Task 12)
    nio/PublishAddresser.groovy                 node digest, s3-copy, head-node, cross-check, byte count (Task 10)
    trace/Join.groovy                           providers (Task 2)
    trace/CasObserver.groovy                    providers (Task 2); head-node line (Task 10); clock, guard (Task 11)
    trace/NodeHash.groovy                       install the afterScript default (Task 9)
    trace/CasObserverFactory.groovy             call NodeHash.install (Task 9)
    lineage/CasLinStore.groovy                  nf/ by URI string, S3 members (Task 11)
    explore/S3MemberFiles.groovy                over S3Ops (Task 1)
    explore/ExploreCommand.groovy               members through S3Ops (Task 1); S3 writable, clock, shadows (Task 11)
    cli/CasCommands.groovy                      snapshot guard, put clock check (Task 11)
  src/test/groovy/robsyme/cas/
    s3/MemoryS3Ops.groovy                       the test double (Task 1)
    s3/*Test.groovy, core/*Test.groovy, ...     per task
  web/src/blocks.js                             leaves by OutputItem schema; RunCompletion providers (Task 2)
  web/src/generated/schema.json                 regenerated (Task 2)
  web/test/schema.test.mjs                      22 types; schema 1 and 2 (Task 2)
  gate/
    node-hash.config                            cas.nodeHash = true for the `again` run (Task 13)
    gate.sh, assert.py, test_assert.py,         assertion 2 addition, assertion 13 (seeding), steps (Task 13)
    cas.py, README.md
    consumer/main.nf                            optional --dir branch (Task 14)
    tier2/tier2.sh, tier2/s3gate.py,            the tier-two harness (Task 14)
    tier2/assert_tier2.py, tier2/test_assert_tier2.py,
    tier2/batch.config, tier2/fusion.config,
    tier2/member.config, tier2/consumer.config,
    tier2/README.md
  Makefile                                      gate-tier2 (Task 14)
```

## Interfaces shared across tasks

Groovy, package `robsyme.cas` unless named. A task sees only its own section, so every name another task uses is here.

S3 (package `robsyme.cas.s3`, Task 1):

- `interface S3Ops`: `String getBucket()`; `S3Head head(String key)` (null when absent); `InputStream get(String key, String ifMatch, long start, long length)` (null when absent; `length < 0` reads to the end; `ifMatch` may be null; a 412 throws `S3PreconditionFailed`); `S3Written put(String key, S3Body body, S3PutOptions options)`; `String createMultipart(String key, S3PutOptions options)`; `S3Part uploadPart(String key, String uploadId, int partNumber, S3Body body)`; `S3Part uploadPartCopy(String key, String uploadId, int partNumber, String sourceBucket, String sourceKey, long first, long last)`; `S3Written completeMultipart(String key, String uploadId, List<S3Part> parts, boolean ifNoneMatch)`; `void abortMultipart(String key, String uploadId)`; `S3Written copy(String sourceBucket, String sourceKey, String key, S3PutOptions options)`; `S3Written copyOut(String key, String targetBucket, String targetKey)` (Task 12 adds it); `List<S3Listed> list(String prefix, int maxKeys)` (lexicographic; `maxKeys <= 0` means all); `void delete(String key)`; `Long firstServerDateMillis()` (the `Date` header of the first response, null before one); `String describe()`.
- `S3Head(long size, String etag, String sha256, Map<String,String> metadata, long lastModifiedMillis)`; `sha256` is base64, null when S3 holds none.
- `S3Body`: `long getLength()`, `InputStream open()` (a fresh stream per call); `static S3Body ofFile(Path file, long offset, long length)`, `static S3Body ofBytes(byte[] bytes)`.
- `S3PutOptions`: fields `boolean ifNoneMatch`, `String ifMatch`, `boolean sha256`, `String cacheControl`, `String contentType`, `Map<String,String> metadata`; built with `S3PutOptions.create()` and chained setters (`ifNoneMatch()`, `ifMatch(String)`, `sha256()`, `cacheControl(String)`, `contentType(String)`, `meta(String, String)`).
- `S3Written(Status status, String etag, String sha256)`, `enum Status { WRITTEN, EXISTS, CONFLICT }` (EXISTS is S3's 412, CONFLICT its 409).
- `S3Part(int partNumber, String etag, String sha256)`; `S3Listed(String key, long size)`.
- `S3WriteOptions(String storageClass, String sse, String kmsKeyId, boolean requesterPays)`, `S3WriteOptions.NONE`.
- `class S3PreconditionFailed extends IOException`.
- `S3Access.open(Map config, String bucket) -> S3Ops`; `S3Access.writeOptions(Map config) -> S3WriteOptions`.
- `S3Location.parse(String uri) -> S3Location` with `bucket`, `prefix` (`''` or ending in `/`), `toString()` (`s3://bucket` or `s3://bucket/p`, no trailing slash) (Task 4 creates it in `robsyme.cas`, since `CasConfig` needs it and it holds no SDK type).
- `S3BlockStore(S3Ops ops, String prefix, String alias, boolean writable, Path tmpDir, long singleRequestMax = S3BlockStore.SINGLE_REQUEST_MAX)` (the aws per-request fields are applied by `SdkS3Ops`, so the store takes no `S3WriteOptions`) implements `BlockStore`, `LoggedStore`; `String key(Cid)`; `Cid putFile(Path file)`; `S3Ops getOps()`; `String getPrefix()` (Task 5). `S3Copied copyFrom(String sourceBucket, String sourceKey, long size, Cid expected)` returns `S3Copied(Cid cid, String provider)` (a top-level `@Canonical` class in `S3Types.groovy`, Task 10), or null when neither a copy nor a part copy can address it (Task 10). `void copyOut(Cid cid, String targetBucket, String targetKey)` (Task 12).
- `S3StoreLogStorage(S3Ops ops, String prefix)` implements `StoreLogStorage` (Task 5).
- `S3CoordinateTree(S3Ops ops, String prefix)` implements `CoordinateTree`; `List<String> shadowedPointers(int limit)` (Task 6).
- `S3SnapshotStorage(S3Ops ops, String prefix)` implements `SnapshotStorage` (Task 7).

Core (package `robsyme.cas.core`):

- `Providers`: `HEAD_NODE = 'head-node'`, `FUSION_NODE = 'fusion-node'`, `S3_COPY = 's3-copy'`, `List<String> ALL`, `static boolean isKnown(String)` (Task 2).
- `Leaf.of(String name, Cid address, Long size)` (no provider); `Leaf` has no `provider` field; `OutputItem.SCHEMA = 2`; `RunCompletion` gains `Map<String, List<Cid>> providers` (constructor key `providers`), `RunCompletion.SCHEMA = 2`; `DirectoryManifest.SCHEMA = 2`, `ManifestEntry.executable(...)` is gone and `ManifestEntry.fromCbor` reads `executable` as `regular` (Task 2).
- `interface FileAddresser { Addressed address(Path file, long size) }`; `Addressed(Cid cid, long size, String provider)`; `HeadNodeAddresser(BlockStore store) implements FileAddresser` (Task 3).
- `DirectoryManifestBuilder(BlockStore store)`, `DirectoryManifestBuilder(BlockStore store, FileAddresser addresser, Closure<Path> objectPath)`; `objectPath` maps `s3://<bucket>/<key>` to a `Path`; `Result` gains `Map<String, List<Cid>> providers`, the provider of every file inside the tree to its addresses (Task 3).
- `FusionLinks.parse(byte[] body) -> FusionLinks.Parsed` with `boolean ok`, `Set<String> names`, `String problem` (Task 3).
- `interface LoggedStore { StoreLogStorage storeLogStorage() }` (Task 5).
- `interface CoordinateTree`: `Optional<StoreRef> read(String rel)`, `void write(String rel, StoreRef ref)`, `boolean exists(String rel)`, `boolean isDirectory(String rel)`, `boolean isDirectoryCoordinate(String rel)`, `List<String> children(String rel)`, `boolean delete(String rel)`, `void createDirectories(String rel)`, `long lastModifiedMillis(String rel)`; `LocalCoordinateTree(Path coordsRoot)` keeps `pointerPath(String)` and `getRoot()` (Task 6).
- `interface SnapshotStorage`: `SnapshotBase base(Path tempDir)` (null when there is no snapshot); `Path fetch(Path tempDir)` (null when absent; the caller deletes the file); `boolean replace(Path built, int runs, SnapshotBase base)` (false when another writer replaced it since `base`); `boolean writePage(byte[] page)`; `String describe()`. `SnapshotBase(String tag, long bytes, int runs)` (`runs` is -1 when the old file could not be counted). `LocalSnapshotStorage(Path memberRoot)` (Task 7).
- `IndexSnapshot.build(Index index, String member, Path tempDir) -> IndexSnapshot.Built(Path file, long bytes, int runs, String watermark)`; `IndexSnapshot.write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir) -> Result`; the old `IndexSnapshot.write(Index, String, Path, long)` stays as a wrapper; `Result.skipped` is null or one of `IndexSnapshot.OVER_CAP`, `FEWER_RUNS`, `REPLACED_MEANWHILE`, `CATCH_UP_FAILED` (Task 7).
- `Index.catchUp(BlockStore store, StoreLog log, String member, SnapshotStorage snapshots, Path tempDir)` (the 3-argument form stays, without seeding); `boolean Index.seedFrom(Path snapshotFile, String member)` (throws `Index.SnapshotUnusable` with `reason`); `String Index.meta(String key)` becomes public as `metaValue(String key)` (Task 8).
- `NodeDigests.parse(InputStream in, long maxBytes) -> Map<String, Cid>` (rel path to raw CID; null when more than `maxBytes` arrive); `NodeDigests.taskDirOf(String sourceUri, String workDirUri) -> NodeDigests.TaskPath` with `taskDir` and `rel`, or null; `NodeDigests.MAX_BYTES = 16 MiB` (Task 10).
- `ClockSkew.judge(long localMillis, long serverMillis) -> ClockSkew.Verdict` (`OK`, `WARN`, `ABORT`) and `ClockSkew.describe(long localMillis, long serverMillis) -> String` (Task 11).

Plugin (packages `robsyme.cas`, `nio`, `trace`):

- `CasConfig`: `boolean isRemote(String alias)`, `S3Location remoteOf(String alias)`, `Path locationOf(String alias)` (local only, else null), `String locationText(String alias)`, `List<String> locationTexts()` (replaces `localLocations()`), `Path pathOf(String alias)` (`FileHelper.asPath` of the text), `Path tmpDir`, `Boolean nodeHashSetting`, `Map rawConfig`, `static boolean nodeHashEnabled(Map sessionConfig)`, `String storageClassWarning` (null or the text to warn once) (Task 4).
- `CasSession.Publish` gains `Map<String, List<Cid>> contents` (default empty; for a directory, its files' providers), merged into `RunCompletion.providers` by `Join` (Task 2; filled by Task 10). `CasSession.coordinatesOf(String alias) -> CoordinateTree` (Task 6); `CasSession.addresser -> PublishAddresser` (Task 10); `static Closure<S3Ops> s3OpsFactory` (a test seam, `{ Map config, String bucket -> S3Access.open(config, bucket) }`), `SnapshotStorage snapshotsOf(String alias)`, `SnapshotBase snapshotBase()`, `Set<String> catchUpIndex(Index index)` (the aliases whose catch-up threw), `IndexSnapshot.Result snapshotWritable(Index index, long maxBytes, SnapshotBase base, Set<String> failed)`, `void checkClock()` (throws `ClockSkewException`) (Task 11).
- `PublishAddresser(BlockStore store, BlockStore writable, boolean nodeHash, Path workDir, long singleRequestMax = S3BlockStore.SINGLE_REQUEST_MAX)` implements `FileAddresser`; `long getHeadNodeBytes()`, `Map<String,Integer> getCounts()`, `String summary()` (Task 10).
- `NodeHash.RESOURCE = '/robsyme/cas/node-hash.sh'`, `NodeHash.script() -> String`, `NodeHash.install(Map config) -> List<String>` (the selectors left alone because their `afterScript` is a closure) (Task 9).

Page (`web/src/`, Task 2): `blocks.js` validates an OutputItem's leaves with `validLeaf` (schema 2) or `validLeafV1` (schema 1), refuses any other `schema`, and requires a schema-2 RunCompletion to carry `providers` with only the three provider names; `schema.js` exports `validLeafV1`.

Gate (Task 13): `assert.py` `assertion(13, ...)`; `gate.sh` writes `logs/consumer-seeded/`, `logs/consumer-scan/`, `logs/consumer-seeded/locked`, `seeding.json` (the snapshot's watermark, run count and `snapshot_written_at` before the seeded run). Tier two (Task 14): `gate/tier2/assert_tier2.py <T2_ROOT> <GATE_ROOT>` prints a table like `assert.py` and exits 1 on a FAIL.

## Waves

| Wave | Tasks | Depends on |
|---|---|---|
| 0, Foundations | 1 S3 seam; 2 provider out of the Leaf, no execute bit; 3 object-store manifests and Fusion links (after 2); 4 config | `main` at `8dc1acb` |
| 1, The S3 member | 5 block store and Store Log; 6 coordinates; 7 snapshot storage | Task 1 (all three) |
| 2, Behaviours | 8 index seeding; 9 node-side hashing script; 10 publish addresser | Tasks 3, 5, 7 (8 on 7; 10 on 3 and 5); Task 4 (9, 10) |
| 3, Wiring | 11 session, observer, lineage store, verbs; 12 staging out of an S3 member | waves 0-2 |
| 4, Gate | 13 tier one; 14 tier two | waves 0-3 |
| 5, Documents | 15 DESIGN, spec, README, acceptance | waves 0-4 |

Tasks within a wave touch disjoint files, so subagents may run them in parallel, each committing only its own paths. The one exception is Tasks 2 and 3 in wave 0: both edit `DirectoryManifestBuilder.groovy` and `DirectoryManifestBuilderTest.groovy`, so Task 3 runs after Task 2 commits (Task 1 and Task 4 may run beside either). Files edited by more than one task, otherwise always in different waves: `CasFileSystemProvider.groovy` (6, 10, 12), `CasFileSystemProviderTest.groovy` (6, 10), `CasOccurrenceTest.groovy` (2, 6), `CasSession.groovy` (2, 6, 10, 11), `CasObserver.groovy` (2, 10, 11), `CasConfig.groovy` (4, 11), `S3BlockStore.groovy` (5, 10, 12), `SdkS3Ops.groovy` and `S3Ops.groovy` (1, 12), `SdkS3OpsTest.groovy` (1, 12), `MemoryS3Ops.groovy` (1, 12), `S3Types.groovy` (1, 10), `S3CopyTest.groovy` (10, 12), `ExploreCommand.groovy` (1, 11), `build.gradle` (1, 5), `DESIGN.md` (2, 15).

Every Groovy task ends with `./gradlew test` green, and Tasks 1 and 5 with `./gradlew memoryBoundTest dependencyCheck` green too; Task 2 with `cd web && npm test` green; Tasks 2, 3, 10, 11, 12 and 13 with `make gate` (lineage 11/0/6 before Task 13, 12/0/6 after it; browser tier A 5/5; tier B 12/12). Run the Gate with `GATE_ROOT` in the session scratchpad, and rerun once before debugging an assertion 4 failure: it is intermittent by design (`gate/README.md`). Task 14 ends with `make gate-tier2` passing T1-T6, run by Rob, since it needs his SSO session and creates a bucket.

---

### Task 1: One S3 seam over nf-amazon's client

Tickets 01 and 02 (decision 1), silent decisions 10 and 11. nf-blocks stops bundling the AWS SDK and links against the copy nf-amazon loads, so an `S3Client` built by nf-amazon's `AwsClientFactory` is usable from nf-blocks classes (with a bundled copy it would fail with a `ClassCastException` or `LinkageError`: pf4j loads a plugin's own jars first, `s3-writable-store.md` §1). Every S3 request nf-blocks makes goes through one bucket-scoped interface, `S3Ops`: `SdkS3Ops` is the SDK behind it, and `MemoryS3Ops`, a test double, lets every later task test S3 behaviour without AWS. `S3MemberFiles` (the explorer's read side) moves onto `S3Ops`, which deletes the class-loader shim and the URL-connection client.

The factory is the one `S3FileSystemProvider.createFileSystem` uses (`plugins/nf-amazon/src/main/nextflow/cloud/aws/nio/S3FileSystemProvider.java:756-780` at v26.04.6): `AwsConfig(Map)` (`config/AwsConfig.groovy:76-83`), `resolveS3Region()` (`:110-113`, us-east-1 when nothing names a region), `AwsClientFactory.getS3Client(S3SyncClientConfiguration, boolean global)` (`AwsClientFactory.groovy:212-236`), `getS3LegacyProperties()` (`AwsConfig.groovy:163`) for the HTTP settings. The per-request fields come from `AwsS3Config` (`storageClass`, `storageEncryption`, `storageKmsKeyId`, `requesterPays`, `config/AwsS3Config.groovy:139-175, 246-252`).

**Files:**
- Modify: `build.gradle`
- Create: `src/main/groovy/robsyme/cas/s3/S3Ops.groovy`
- Create: `src/main/groovy/robsyme/cas/s3/S3Types.groovy`
- Create: `src/main/groovy/robsyme/cas/s3/SdkS3Ops.groovy`
- Create: `src/main/groovy/robsyme/cas/s3/S3Access.groovy`
- Modify: `src/main/groovy/robsyme/cas/explore/S3MemberFiles.groovy`
- Modify: `src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy` (`start` and `membersOf` only)
- Create: `src/test/groovy/robsyme/cas/s3/MemoryS3Ops.groovy` (test double)
- Create: `src/test/groovy/robsyme/cas/s3/MemoryS3OpsTest.groovy`
- Create: `src/test/groovy/robsyme/cas/s3/SdkS3OpsTest.groovy`
- Create: `src/test/groovy/robsyme/cas/s3/S3AccessTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/explore/S3MemberFilesTest.groovy`, `src/test/groovy/robsyme/cas/explore/ExploreCommandTest.groovy`

**Interfaces:**
- Consumes: `nextflow.cloud.aws.AwsClientFactory`, `nextflow.cloud.aws.config.AwsConfig`, `nextflow.cloud.aws.nio.util.S3SyncClientConfiguration` (nf-amazon 3.9.2).
- Produces: everything under "S3" in "Interfaces shared across tasks" except `S3Location`, `S3BlockStore`, `S3StoreLogStorage`, `S3CoordinateTree`, `S3SnapshotStorage` and `copyOut`. `ExploreCommand.membersOf(CasConfig config, Closure<S3Ops> s3Ops)`, the closure taking the bucket name. The test double `MemoryS3Ops(String bucket)` with `objects` (key to `MemoryS3Ops.Obj`), `calls` (`"HEAD <key>"`, `"GET <key>"`, `"PUT <key>"`, `"COPY <src bucket>/<src key> <key>"`, `"MPU <key>"`, `"PART <key> <n>"`, `"PARTCOPY <key> <n>"`, `"COMPLETE <key>"`, `"ABORT <key>"`, `"LIST <prefix>"`, `"DELETE <key>"`, and from Task 12 `"COPYOUT <key> <target bucket>/<target key>"`), `pulledBytes`, `serverDateMillis` (Long, null for "no Date header"), `discard` (hash bodies, keep none), `conflicts` (`Map<String,Integer>`: op name to how many 409s to answer first), `peers` (other buckets by name, for copy sources and targets), `Obj.text()`, `putText(String key, String text)`.

- [ ] **Step 1: Move the SDK to compile-only against nf-amazon**

In `build.gradle`, inside `nextflowPlugin { ... }` after `className`, add:

```groovy
    // S3 goes through the SDK nf-amazon loads (DESIGN.md §1, ticket 02): a
    // bundled SDK would be a second copy of every class, and nf-amazon's
    // client would not be assignable to it. Nextflow installs the bundled
    // nf-amazon for a core plugin whatever the constraint says, then pf4j
    // checks the constraint (PluginUpdater.groovy:407-434 at v26.04.6).
    requirePlugins = ['nf-amazon@>=3.9.2']
```

Replace the dependency block's SDK lines

```groovy
    // Private S3 members for nf-blocks:explore (DESIGN.md §15). URL-connection
    // client only: the Apache and Netty clients would add megabytes for nothing.
    implementation platform('software.amazon.awssdk:bom:2.46.7')
    implementation 'software.amazon.awssdk:s3'
    implementation 'software.amazon.awssdk:sso'
    implementation 'software.amazon.awssdk:ssooidc'
    implementation 'software.amazon.awssdk:url-connection-client'
```

with

```groovy
    // nf-amazon 3.9.2 is the one Nextflow 26.04.6 bundles; its POM brings the
    // AWS SDK 2.46.8 it was built with. Compile-only: nothing of it lands in
    // the zip, and at run time the classes come from nf-amazon's loader.
    compileOnly 'io.nextflow:nf-amazon:3.9.2'
```

add `testImplementation 'io.nextflow:nf-amazon:3.9.2'` beside the other `testImplementation` lines, and delete the whole `configurations.configureEach { ... exclude ... apache-client ... }` block with its comment. Keep `configurations.runtimeClasspath { exclude group: 'org.slf4j' }`, and change the first line of its comment to "Nothing bundled may carry slf4j-api: a copy in the zip's lib/ would win the plugin's own".

Run: `./gradlew dependencies --configuration compileClasspath | grep -E 'nf-amazon|awssdk:s3:'`
Expected: `io.nextflow:nf-amazon:3.9.2` and `software.amazon.awssdk:s3:2.46.8` appear.

Run: `./gradlew assemble && unzip -l build/distributions/nf-blocks-0.1.0-beta.2.zip | grep -c awssdk`
Expected: `0`. (Compilation fails in `S3MemberFiles` and the two explore tests until Step 5; that is the next steps' work, so run `assemble` after Step 5 if it fails here.)

- [ ] **Step 2: Write the failing tests for the double and the SDK's request shapes**

```groovy
// src/test/groovy/robsyme/cas/s3/MemoryS3OpsTest.groovy
package robsyme.cas.s3

import java.security.MessageDigest

import spock.lang.Specification

/** The test double behaves as the measured S3 does (ticket 14), or later tests prove nothing. */
class MemoryS3OpsTest extends Specification {

    MemoryS3Ops s3 = new MemoryS3Ops('member-bucket')

    private static String sha256b64(byte[] bytes) {
        Base64.encoder.encodeToString(MessageDigest.getInstance('SHA-256').digest(bytes))
    }

    def 'a conditional PUT onto an existing key reads the whole body, then answers EXISTS (ticket 14 item 1)'() {
        given:
        s3.put('k', S3Body.ofBytes('one'.bytes), S3PutOptions.create().ifNoneMatch())
        final long before = s3.pulledBytes

        when:
        final S3Written second = s3.put('k', S3Body.ofBytes('two!'.bytes), S3PutOptions.create().ifNoneMatch())

        then:
        second.status == S3Written.Status.EXISTS
        s3.pulledBytes - before == 4
        s3.objects['k'].text() == 'one'
    }

    def 'If-Match replaces only the version it names'() {
        given:
        final String etag = s3.put('k', S3Body.ofBytes('v1'.bytes), S3PutOptions.create()).etag

        expect:
        s3.put('k', S3Body.ofBytes('v2'.bytes), S3PutOptions.create().ifMatch('"stale"')).status == S3Written.Status.EXISTS
        s3.put('k', S3Body.ofBytes('v2'.bytes), S3PutOptions.create().ifMatch(etag)).status == S3Written.Status.WRITTEN
        s3.objects['k'].text() == 'v2'
    }

    def 'a PUT with sha256 stores and returns the full-object SHA-256 (ticket 14 item 4)'() {
        when:
        final S3Written w = s3.put('k', S3Body.ofBytes('hello\n'.bytes), S3PutOptions.create().sha256())

        then:
        w.sha256 == sha256b64('hello\n'.bytes)
        s3.head('k').sha256 == w.sha256
    }

    def 'a copy with sha256 returns the SHA-256 of a multipart source (ticket 14 item 2)'() {
        given:
        final MemoryS3Ops work = new MemoryS3Ops('work-bucket')
        s3.peers['work-bucket'] = work
        final String id = work.createMultipart('src', S3PutOptions.create())
        final S3Part p1 = work.uploadPart('src', id, 1, S3Body.ofBytes('abc'.bytes))
        final S3Part p2 = work.uploadPart('src', id, 2, S3Body.ofBytes('def'.bytes))
        work.completeMultipart('src', id, [p1, p2], true)

        when:
        final S3Written copied = s3.copy('work-bucket', 'src', 'dst', S3PutOptions.create().sha256().ifNoneMatch())

        then:
        work.objects['src'].text() == 'abcdef'
        copied.status == S3Written.Status.WRITTEN
        copied.sha256 == sha256b64('abcdef'.bytes)
        s3.copy('work-bucket', 'src', 'dst', S3PutOptions.create().ifNoneMatch()).status == S3Written.Status.EXISTS
    }

    def 'an injected 409 is answered first, then the write succeeds'() {
        given:
        s3.conflicts['PUT'] = 1

        expect:
        s3.put('k', S3Body.ofBytes('x'.bytes), S3PutOptions.create().ifNoneMatch()).status == S3Written.Status.CONFLICT
        s3.put('k', S3Body.ofBytes('x'.bytes), S3PutOptions.create().ifNoneMatch()).status == S3Written.Status.WRITTEN
    }

    def 'list is lexicographic under a prefix and honours maxKeys; a ranged get with a stale If-Match fails'() {
        given:
        ['log/2', 'log/1', 'logx', 'log/3'].each { s3.putText(it, '') }
        s3.putText('blob', '0123456789')

        expect:
        s3.list('log/', 0)*.key == ['log/1', 'log/2', 'log/3']
        s3.list('log/', 2)*.key == ['log/1', 'log/2']
        s3.get('blob', s3.head('blob').etag, 2, 3).text == '234'
        s3.get('absent', null, 0, -1) == null

        when:
        s3.get('blob', '"other"', 0, -1)

        then:
        thrown(S3PreconditionFailed)
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/s3/SdkS3OpsTest.groovy
package robsyme.cas.s3

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.interceptor.Context
import software.amazon.awssdk.core.interceptor.ExecutionAttributes
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.http.SdkHttpRequest
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.S3Exception
import spock.lang.Specification

/**
 * The headers SdkS3Ops puts on the wire, captured after signing and before
 * transmission, so nothing reaches a network. S3's answers are tier two's.
 */
class SdkS3OpsTest extends Specification {

    static class Stop extends RuntimeException {}

    final List<SdkHttpRequest> sent = []

    private SdkS3Ops ops(S3WriteOptions options = S3WriteOptions.NONE) {
        final ExecutionInterceptor capture = new ExecutionInterceptor() {
            @Override
            void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes attributes) {
                sent << context.httpRequest()
                throw new Stop()
            }
        }
        final S3Client client = S3Client.builder()
            .region(Region.US_EAST_1)
            .endpointOverride(URI.create('http://127.0.0.1:9'))
            .forcePathStyle(true)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create('a', 'b')))
            .overrideConfiguration { it.addExecutionInterceptor(capture) }
            .build()
        return new SdkS3Ops(client, 'member', options)
    }

    private String header(String name) {
        sent.last().firstMatchingHeader(name).orElse(null)
    }

    def 'a block PUT is conditional, asks for SHA-256, and carries the aws scope fields'() {
        given:
        final SdkS3Ops s3 = ops(new S3WriteOptions('STANDARD_IA', 'aws:kms', 'key-1', true))

        when:
        s3.put('cas/blocks/am/x', S3Body.ofBytes('x'.bytes),
            S3PutOptions.create().ifNoneMatch().sha256().cacheControl('public, max-age=31536000, immutable'))

        then:
        thrown(SdkClientException)
        sent.last().method().name() == 'PUT'
        sent.last().encodedPath() == '/member/cas/blocks/am/x'
        header('If-None-Match') == '*'
        header('x-amz-sdk-checksum-algorithm') == 'SHA256'
        header('x-amz-storage-class') == 'STANDARD_IA'
        header('x-amz-server-side-encryption') == 'aws:kms'
        header('x-amz-server-side-encryption-aws-kms-key-id') == 'key-1'
        header('x-amz-request-payer') == 'requester'
        header('Cache-Control') == 'public, max-age=31536000, immutable'
    }

    def 'a snapshot PUT is conditional on the ETag it replaces and records the run count'() {
        when:
        ops().put('cas/index/v3.sqlite', S3Body.ofBytes('s'.bytes),
            S3PutOptions.create().ifMatch('"e1"').cacheControl('no-cache').meta('runs', '7'))

        then:
        thrown(SdkClientException)
        header('If-Match') == '"e1"'
        header('If-None-Match') == null
        header('x-amz-meta-runs') == '7'
        header('Cache-Control') == 'no-cache'
    }

    def 'a copy into the member names its source, asks for SHA-256, is conditional and replaces the metadata'() {
        when:
        ops().copy('work', 'w/ab/c/A.bam', 'cas/tmp/u', S3PutOptions.create().sha256().ifNoneMatch().cacheControl('no-cache'))

        then:
        thrown(SdkClientException)
        header('x-amz-copy-source') == 'work/w/ab/c/A.bam'
        header('x-amz-checksum-algorithm') == 'SHA256'
        header('If-None-Match') == '*'
        header('x-amz-metadata-directive') == 'REPLACE'
    }

    def 'HEAD asks for the stored SHA-256 (x-amz-checksum-mode)'() {
        when:
        ops().head('cas/index.html')

        then:
        thrown(SdkClientException)
        sent.last().method().name() == 'HEAD'
        header('x-amz-checksum-mode') == 'ENABLED'
    }

    def 'S3 status codes map to written, exists and conflict'() {
        expect:
        SdkS3Ops.statusOf(S3Exception.builder().statusCode(412).build()) == S3Written.Status.EXISTS
        SdkS3Ops.statusOf(S3Exception.builder().statusCode(409).build()) == S3Written.Status.CONFLICT
        SdkS3Ops.statusOf(S3Exception.builder().statusCode(403).build()) == null
    }

    def 'the Date of the first response is kept and parsed'() {
        expect:
        SdkS3Ops.parseDate('Sun, 27 Sep 2026 10:00:00 GMT') == 1790503200000L
        SdkS3Ops.parseDate('garbage') == null
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/s3/S3AccessTest.groovy
package robsyme.cas.s3

import spock.lang.Specification

/** The aws scope reaches nf-blocks' S3 requests through nf-amazon's own parsing (ticket 02 decision 9). */
class S3AccessTest extends Specification {

    def 'the aws scope gives the per-request fields; building the client touches no network'() {
        given:
        final Map config = [aws: [accessKey: 'a', secretKey: 'b', region: 'eu-west-1',
                                  client: [storageClass: 'ONEZONE_IA', storageEncryption: 'AES256', requesterPays: true]]]

        when:
        final S3Ops ops = S3Access.open(config, 'member-bucket')

        then:
        ops instanceof SdkS3Ops
        ops.bucket == 'member-bucket'
        ((SdkS3Ops) ops).options == new S3WriteOptions('ONEZONE_IA', 'AES256', null, true)
    }

    def 'no aws scope: no per-request fields'() {
        expect:
        S3Access.writeOptions([:]) == S3WriteOptions.NONE
    }
}
```

The double itself (test source, written in this step because the tests compile against it):

```groovy
// src/test/groovy/robsyme/cas/s3/MemoryS3Ops.groovy
package robsyme.cas.s3

import java.security.MessageDigest

/**
 * An S3 bucket in memory, answering as the measured S3 does (ticket 14): a
 * conditional PUT reads the whole body before its 412, a copy or a PUT with
 * sha256 returns the full-object SHA-256, a multipart upload is checked only
 * at Complete. Counts every request and every body byte read.
 */
class MemoryS3Ops implements S3Ops {

    static class Obj {
        byte[] bytes
        String etag
        String sha256
        Map<String, String> metadata = [:]
        String cacheControl
        long lastModified
        String text() { new String(bytes, 'UTF-8') }
    }

    final String bucket
    final Map<String, Obj> objects = new TreeMap<>()
    final Map<String, Map<Integer, byte[]>> uploads = [:]
    final List<String> calls = []
    final Map<String, Integer> conflicts = [:]
    final Map<String, MemoryS3Ops> peers = [:]
    long pulledBytes = 0
    Long serverDateMillis = null
    boolean discard = false
    private long clock = 1_790_000_000_000L
    private int nextId = 0

    MemoryS3Ops(String bucket) { this.bucket = bucket }

    void putText(String key, String text) { store(key, text.getBytes('UTF-8'), S3PutOptions.create()) }

    private MemoryS3Ops bucketNamed(String name) { name == bucket ? this : peers[name] }

    private boolean conflict(String op) {
        final Integer left = conflicts[op]
        if( !left ) return false
        conflicts[op] = left - 1
        return true
    }

    private byte[] drain(S3Body body) {
        final ByteArrayOutputStream out = discard ? null : new ByteArrayOutputStream()
        final MessageDigest md = MessageDigest.getInstance('SHA-256')
        final byte[] buf = new byte[65536]
        body.open().withCloseable { InputStream in ->
            int n
            while( (n = in.read(buf)) > 0 ) { pulledBytes += n; md.update(buf, 0, n); out?.write(buf, 0, n) }
        }
        lastDigest = md.digest()
        return out == null ? new byte[0] : out.toByteArray()
    }
    private byte[] lastDigest

    private Obj store(String key, byte[] bytes, S3PutOptions o, byte[] digest = null) {
        final byte[] d = digest ?: MessageDigest.getInstance('SHA-256').digest(bytes)
        final Obj obj = new Obj(bytes: bytes, etag: '"' + (++nextId) + '-' + key.hashCode() + '"',
            sha256: o.sha256 ? Base64.encoder.encodeToString(d) : null,
            metadata: new LinkedHashMap<String, String>(o.metadata ?: [:]), cacheControl: o.cacheControl,
            lastModified: ++clock)
        objects[key] = obj
        return obj
    }

    private S3Written refused(String key, S3PutOptions o) {
        final Obj existing = objects[key]
        if( o.ifNoneMatch && existing != null ) return new S3Written(S3Written.Status.EXISTS, null, null)
        if( o.ifMatch != null && (existing == null || existing.etag != o.ifMatch) ) return new S3Written(S3Written.Status.EXISTS, null, null)
        return null
    }

    @Override S3Head head(String key) {
        calls << "HEAD ${key}".toString()
        final Obj o = objects[key]
        return o == null ? null : new S3Head((long) o.bytes.length, o.etag, o.sha256, o.metadata, o.lastModified)
    }

    @Override InputStream get(String key, String ifMatch, long start, long length) {
        calls << "GET ${key}".toString()
        final Obj o = objects[key]
        if( o == null ) return null
        if( ifMatch != null && ifMatch != o.etag ) throw new S3PreconditionFailed("${key} is no longer ${ifMatch}")
        final int from = (int) start
        final int to = length < 0 ? o.bytes.length : (int) Math.min(o.bytes.length, start + length)
        return new ByteArrayInputStream(Arrays.copyOfRange(o.bytes, from, to))
    }

    @Override S3Written put(String key, S3Body body, S3PutOptions o) {
        calls << "PUT ${key}".toString()
        final byte[] bytes = drain(body)
        if( conflict('PUT') ) return new S3Written(S3Written.Status.CONFLICT, null, null)
        final S3Written no = refused(key, o)
        if( no != null ) return no
        final Obj obj = store(key, bytes, o, lastDigest)
        return new S3Written(S3Written.Status.WRITTEN, obj.etag, obj.sha256)
    }

    @Override String createMultipart(String key, S3PutOptions o) {
        calls << "MPU ${key}".toString()
        final String id = "upload-${++nextId}".toString()
        uploads[id] = new TreeMap<Integer, byte[]>()
        mpuOptions[id] = o
        return id
    }
    private final Map<String, S3PutOptions> mpuOptions = [:]

    @Override S3Part uploadPart(String key, String uploadId, int n, S3Body body) {
        calls << "PART ${key} ${n}".toString()
        final byte[] bytes = drain(body)
        uploads[uploadId][n] = bytes
        return new S3Part(n, '"p' + n + '"', Base64.encoder.encodeToString(lastDigest))
    }

    @Override S3Part uploadPartCopy(String key, String uploadId, int n, String srcBucket, String srcKey, long first, long last) {
        calls << "PARTCOPY ${key} ${n}".toString()
        final Obj src = bucketNamed(srcBucket).objects[srcKey]
        uploads[uploadId][n] = Arrays.copyOfRange(src.bytes, (int) first, (int) last + 1)
        return new S3Part(n, '"c' + n + '"', null)
    }

    @Override S3Written completeMultipart(String key, String uploadId, List<S3Part> parts, boolean ifNoneMatch) {
        calls << "COMPLETE ${key}".toString()
        if( conflict('COMPLETE') ) return new S3Written(S3Written.Status.CONFLICT, null, null)
        final S3PutOptions o = mpuOptions[uploadId]
        if( ifNoneMatch && objects.containsKey(key) ) return new S3Written(S3Written.Status.EXISTS, null, null)
        final ByteArrayOutputStream all = new ByteArrayOutputStream()
        parts.each { S3Part p -> all.write(uploads[uploadId][p.partNumber]) }
        uploads.remove(uploadId)
        // Multipart SHA-256 is composite on S3 (ticket 01): none is returned.
        final Obj obj = store(key, all.toByteArray(), S3PutOptions.create().cacheControl(o?.cacheControl))
        return new S3Written(S3Written.Status.WRITTEN, obj.etag, null)
    }

    @Override void abortMultipart(String key, String uploadId) {
        calls << "ABORT ${key}".toString()
        uploads.remove(uploadId)
    }

    @Override S3Written copy(String srcBucket, String srcKey, String key, S3PutOptions o) {
        calls << "COPY ${srcBucket}/${srcKey} ${key}".toString()
        if( conflict('COPY') ) return new S3Written(S3Written.Status.CONFLICT, null, null)
        final Obj src = bucketNamed(srcBucket)?.objects?.get(srcKey)
        if( src == null ) throw new FileNotFoundException("no such source s3://${srcBucket}/${srcKey}")
        final S3Written no = refused(key, o)
        if( no != null ) return no
        final Obj obj = store(key, src.bytes, o)
        return new S3Written(S3Written.Status.WRITTEN, obj.etag, obj.sha256)
    }

    @Override S3Written copyOut(String key, String targetBucket, String targetKey) {
        calls << "COPYOUT ${key} ${targetBucket}/${targetKey}".toString()
        final Obj src = objects[key]
        if( src == null ) throw new FileNotFoundException("no such key ${key}")
        final Obj obj = bucketNamed(targetBucket).store(targetKey, src.bytes, S3PutOptions.create().sha256())
        return new S3Written(S3Written.Status.WRITTEN, obj.etag, obj.sha256)
    }

    @Override List<S3Listed> list(String prefix, int maxKeys) {
        calls << "LIST ${prefix}".toString()
        final List<S3Listed> out = objects.findAll { k, v -> k.startsWith(prefix) }
            .collect { k, v -> new S3Listed(k, (long) v.bytes.length) }
        return maxKeys > 0 ? out.take(maxKeys) : out
    }

    @Override void delete(String key) {
        calls << "DELETE ${key}".toString()
        objects.remove(key)
    }

    @Override Long firstServerDateMillis() { serverDateMillis }

    @Override String describe() { "memory://${bucket}" }
}
```

(`copyOut` is on the interface only from Task 12. Leave it out of `MemoryS3Ops` in this task and add it with Task 12; it is shown here so the double is in one place.)

Update `S3MemberFilesTest`: replace the `UrlConnectionHttpClient` import and `.httpClientBuilder(UrlConnectionHttpClient.builder())` with `software.amazon.awssdk.http.apache.ApacheHttpClient` and `.httpClientBuilder(ApacheHttpClient.builder())`; build every `S3MemberFiles` as `new S3MemberFiles(new SdkS3Ops(client, 'bucket', S3WriteOptions.NONE), 'member/')`; in "an opened object reads only the version it opened" replace `thrown(Exception)` with `thrown(S3PreconditionFailed)`. In `ExploreCommandTest`, replace the `noNetworkClient` feature's body with:

```groovy
        given:
        final CasConfig config = CasConfig.from([cas: [stores: [
            lab : [location: tempDir.resolve('lab').toString()],
            priv: [location: 's3://bucket/member'],
        ]]], 'cas://lab')
        final List<String> asked = []

        when:
        final LinkedHashMap<String, MemberFiles> members = ExploreCommand.membersOf(config,
            { String bucket -> asked << bucket; new MemoryS3Ops(bucket) } as Closure<S3Ops>)

        then:
        members.keySet().toList() == ['lab', 'priv']
        members['lab'] instanceof LocalMemberFiles
        members['priv'] instanceof S3MemberFiles
        asked == ['bucket']
```

and drop its SDK imports.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.s3.*' --tests 'robsyme.cas.explore.*'`
Expected: compilation FAILS: `unable to resolve class S3Ops`, `S3Body`, `SdkS3Ops`, `S3Access`.

- [ ] **Step 4: Write the seam**

```groovy
// src/main/groovy/robsyme/cas/s3/S3Ops.groovy
package robsyme.cas.s3

import groovy.transform.CompileStatic

/**
 * Every S3 request nf-blocks makes, for one bucket (DESIGN.md §5). Keys are
 * relative to the bucket and never start with '/'. A 412 and a 409 are
 * answers, not failures: they come back as S3Written statuses. Anything else
 * S3 refuses is thrown.
 */
@CompileStatic
interface S3Ops {

    String getBucket()

    /** Null when the key is absent. */
    S3Head head(String key)

    /** The object's bytes from start, at most length (all when length < 0); null when absent. */
    InputStream get(String key, String ifMatch, long start, long length)

    S3Written put(String key, S3Body body, S3PutOptions options)

    String createMultipart(String key, S3PutOptions options)

    S3Part uploadPart(String key, String uploadId, int partNumber, S3Body body)

    S3Part uploadPartCopy(String key, String uploadId, int partNumber, String sourceBucket, String sourceKey, long first, long last)

    S3Written completeMultipart(String key, String uploadId, List<S3Part> parts, boolean ifNoneMatch)

    void abortMultipart(String key, String uploadId)

    /** A server-side copy into this bucket; no byte passes through the JVM. */
    S3Written copy(String sourceBucket, String sourceKey, String key, S3PutOptions options)

    /** Keys under prefix in lexicographic order, at most maxKeys (all when maxKeys <= 0). */
    List<S3Listed> list(String prefix, int maxKeys)

    void delete(String key)

    /** The Date header of the first response this instance saw, in epoch millis; null before one. */
    Long firstServerDateMillis()

    String describe()
}
```

```groovy
// src/main/groovy/robsyme/cas/s3/S3Types.groovy
package robsyme.cas.s3

import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/** What HeadObject says of an object. sha256 is base64, null when S3 holds none. */
@Canonical
@CompileStatic
class S3Head {
    long size
    String etag
    String sha256
    Map<String, String> metadata
    long lastModifiedMillis
}

/**
 * A request body the SDK may read more than once (a retry reopens it), so it
 * is never a stream held in memory: a file range, or a small byte array of
 * metadata (never file content, DESIGN.md §0 rule 2).
 */
@CompileStatic
abstract class S3Body {

    abstract long getLength()

    /** A fresh stream over the whole body. */
    abstract InputStream open()

    static S3Body ofFile(Path file, long offset, long length) {
        return new S3Body() {
            @Override long getLength() { length }
            @Override InputStream open() {
                final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)
                channel.position(offset)
                return new BoundedInputStream(new BufferedInputStream(Channels.newInputStream(channel), 65536), length)
            }
        }
    }

    static S3Body ofBytes(byte[] bytes) {
        return new S3Body() {
            @Override long getLength() { (long) bytes.length }
            @Override InputStream open() { new ByteArrayInputStream(bytes) }
        }
    }

    /** At most n bytes of an underlying stream. */
    @CompileStatic
    static class BoundedInputStream extends FilterInputStream {
        private long left
        BoundedInputStream(InputStream in, long n) { super(in); this.left = n }
        @Override int read() throws IOException {
            if( left <= 0 ) return -1
            final int b = super.read()
            if( b >= 0 ) left--
            return b
        }
        @Override int read(byte[] b, int off, int len) throws IOException {
            if( left <= 0 ) return -1
            final int n = super.read(b, off, (int) Math.min((long) len, left))
            if( n > 0 ) left -= n
            return n
        }
    }
}

/** How a write is made. Built with create() and chained setters. */
@CompileStatic
class S3PutOptions {
    boolean ifNoneMatch
    String ifMatch
    boolean sha256
    String cacheControl
    String contentType
    Map<String, String> metadata = [:]

    static S3PutOptions create() { new S3PutOptions() }
    S3PutOptions ifNoneMatch() { this.ifNoneMatch = true; this }
    S3PutOptions ifMatch(String etag) { this.ifMatch = etag; this }
    S3PutOptions sha256() { this.sha256 = true; this }
    S3PutOptions cacheControl(String value) { this.cacheControl = value; this }
    S3PutOptions contentType(String value) { this.contentType = value; this }
    S3PutOptions meta(String key, String value) { this.metadata.put(key, value); this }
}

/** A write's outcome: EXISTS is S3's 412 (a precondition refused), CONFLICT its 409. */
@Canonical
@CompileStatic
class S3Written {
    enum Status { WRITTEN, EXISTS, CONFLICT }
    Status status
    String etag
    String sha256
}

@Canonical
@CompileStatic
class S3Part {
    int partNumber
    String etag
    String sha256
}

@Canonical
@CompileStatic
class S3Listed {
    String key
    long size
}

/** The per-request fields of the aws scope (ticket 01 Q1): nf-amazon's client carries none of them. */
@Canonical
@CompileStatic
class S3WriteOptions {
    static final S3WriteOptions NONE = new S3WriteOptions(null, null, null, false)
    String storageClass
    String sse
    String kmsKeyId
    boolean requesterPays
}

/** A read made with If-Match on a version the object no longer is. */
@CompileStatic
class S3PreconditionFailed extends IOException {
    S3PreconditionFailed(String message) { super(message) }
}
```

```groovy
// src/main/groovy/robsyme/cas/s3/SdkS3Ops.groovy
package robsyme.cas.s3

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.atomic.AtomicReference

import groovy.transform.CompileStatic
import software.amazon.awssdk.core.SdkResponse
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.ContentStreamProvider
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

/**
 * S3Ops over the SDK client nf-amazon builds (S3Access). Adds the aws scope's
 * per-request fields to every write, asks HeadObject for the stored SHA-256,
 * and keeps the Date of the first response for the clock check (ticket 03 Q2).
 */
@CompileStatic
class SdkS3Ops implements S3Ops {

    final String bucket
    final S3WriteOptions options
    private final S3Client client
    private final AtomicReference<Long> firstDate = new AtomicReference<>()

    SdkS3Ops(S3Client client, String bucket, S3WriteOptions options) {
        this.client = client
        this.bucket = bucket
        this.options = options ?: S3WriteOptions.NONE
    }

    /** A 412 or a 409 as a write outcome; null for anything else. */
    static S3Written.Status statusOf(S3Exception e) {
        if( e.statusCode() == 412 ) return S3Written.Status.EXISTS
        if( e.statusCode() == 409 ) return S3Written.Status.CONFLICT
        return null
    }

    static Long parseDate(String text) {
        if( !text ) return null
        try {
            return ZonedDateTime.parse(text, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli()
        }
        catch( DateTimeParseException e ) {
            return null
        }
    }

    private void note(SdkHttpResponse http) {
        if( http == null || firstDate.get() != null ) return
        final Long date = parseDate(http.firstMatchingHeader('Date').orElse(null))
        if( date != null ) firstDate.compareAndSet(null, date)
    }

    private void note(SdkResponse r) { note(r?.sdkHttpResponse()) }

    private void note(S3Exception e) { note(e.awsErrorDetails()?.sdkHttpResponse()) }

    private RequestPayer payer() { options.requesterPays ? RequestPayer.REQUESTER : null }

    private static RequestBody bodyOf(S3Body body, String contentType) {
        return RequestBody.fromContentProvider({ -> body.open() } as ContentStreamProvider, body.length,
            contentType ?: 'application/octet-stream')
    }

    @Override
    S3Head head(String key) {
        try {
            final HeadObjectResponse r = client.headObject(HeadObjectRequest.builder()
                .bucket(bucket).key(key).checksumMode(ChecksumMode.ENABLED).requestPayer(payer()).build())
            note(r)
            return new S3Head(r.contentLength(), r.eTag(), r.checksumSHA256(),
                r.metadata() ?: Collections.<String, String> emptyMap(), r.lastModified()?.toEpochMilli() ?: 0L)
        }
        catch( NoSuchKeyException e ) {
            note(e)
            return null
        }
        catch( S3Exception e ) {
            note(e)
            if( e.statusCode() == 404 ) return null
            throw e
        }
    }

    @Override
    InputStream get(String key, String ifMatch, long start, long length) {
        final GetObjectRequest.Builder b = GetObjectRequest.builder().bucket(bucket).key(key).requestPayer(payer())
        if( ifMatch != null ) b.ifMatch(ifMatch)
        if( length >= 0 ) b.range("bytes=${start}-${start + length - 1}".toString())
        else if( start > 0 ) b.range("bytes=${start}-".toString())
        try {
            final def stream = client.getObject(b.build())
            note(stream.response())
            return stream
        }
        catch( NoSuchKeyException e ) {
            note(e)
            return null
        }
        catch( S3Exception e ) {
            note(e)
            if( e.statusCode() == 404 ) return null
            if( e.statusCode() == 412 ) throw new S3PreconditionFailed("s3://${bucket}/${key} is no longer ${ifMatch}")
            throw e
        }
    }

    @Override
    S3Written put(String key, S3Body body, S3PutOptions o) {
        final PutObjectRequest.Builder b = PutObjectRequest.builder().bucket(bucket).key(key)
            .storageClass(options.storageClass).serverSideEncryption(options.sse).ssekmsKeyId(options.kmsKeyId)
            .requestPayer(payer()).cacheControl(o.cacheControl).contentType(o.contentType)
        if( o.metadata ) b.metadata(o.metadata)
        if( o.ifNoneMatch ) b.ifNoneMatch('*')
        if( o.ifMatch != null ) b.ifMatch(o.ifMatch)
        if( o.sha256 ) b.checksumAlgorithm(ChecksumAlgorithm.SHA256)
        try {
            final PutObjectResponse r = client.putObject(b.build(), bodyOf(body, o.contentType))
            note(r)
            return new S3Written(S3Written.Status.WRITTEN, r.eTag(), r.checksumSHA256())
        }
        catch( S3Exception e ) {
            note(e)
            final S3Written.Status status = statusOf(e)
            if( status == null ) throw e
            return new S3Written(status, null, null)
        }
    }

    @Override
    String createMultipart(String key, S3PutOptions o) {
        final CreateMultipartUploadRequest.Builder b = CreateMultipartUploadRequest.builder().bucket(bucket).key(key)
            .storageClass(options.storageClass).serverSideEncryption(options.sse).ssekmsKeyId(options.kmsKeyId)
            .requestPayer(payer()).cacheControl(o.cacheControl).contentType(o.contentType)
        if( o.sha256 ) b.checksumAlgorithm(ChecksumAlgorithm.SHA256)
        final CreateMultipartUploadResponse r = client.createMultipartUpload(b.build())
        note(r)
        return r.uploadId()
    }

    @Override
    S3Part uploadPart(String key, String uploadId, int partNumber, S3Body body) {
        final UploadPartResponse r = client.uploadPart(UploadPartRequest.builder().bucket(bucket).key(key)
            .uploadId(uploadId).partNumber(partNumber).contentLength(body.length)
            .checksumAlgorithm(ChecksumAlgorithm.SHA256).requestPayer(payer()).build(), bodyOf(body, null))
        note(r)
        return new S3Part(partNumber, r.eTag(), r.checksumSHA256())
    }

    @Override
    S3Part uploadPartCopy(String key, String uploadId, int partNumber, String sourceBucket, String sourceKey, long first, long last) {
        final UploadPartCopyResponse r = client.uploadPartCopy(UploadPartCopyRequest.builder()
            .sourceBucket(sourceBucket).sourceKey(sourceKey).destinationBucket(bucket).destinationKey(key)
            .uploadId(uploadId).partNumber(partNumber).copySourceRange("bytes=${first}-${last}".toString())
            .requestPayer(payer()).build())
        note(r)
        return new S3Part(partNumber, r.copyPartResult().eTag(), r.copyPartResult().checksumSHA256())
    }

    @Override
    S3Written completeMultipart(String key, String uploadId, List<S3Part> parts, boolean ifNoneMatch) {
        final List<CompletedPart> completed = parts.collect { S3Part p ->
            CompletedPart.builder().partNumber(p.partNumber).eTag(p.etag).checksumSHA256(p.sha256).build()
        }
        final CompleteMultipartUploadRequest.Builder b = CompleteMultipartUploadRequest.builder().bucket(bucket).key(key)
            .uploadId(uploadId).requestPayer(payer())
            .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build())
        if( ifNoneMatch ) b.ifNoneMatch('*')
        try {
            final CompleteMultipartUploadResponse r = client.completeMultipartUpload(b.build())
            note(r)
            return new S3Written(S3Written.Status.WRITTEN, r.eTag(), r.checksumSHA256())
        }
        catch( S3Exception e ) {
            note(e)
            final S3Written.Status status = statusOf(e)
            if( status == null ) throw e
            return new S3Written(status, null, null)
        }
    }

    @Override
    void abortMultipart(String key, String uploadId) {
        client.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(bucket).key(key)
            .uploadId(uploadId).requestPayer(payer()).build())
    }

    @Override
    S3Written copy(String sourceBucket, String sourceKey, String key, S3PutOptions o) {
        final CopyObjectRequest.Builder b = CopyObjectRequest.builder()
            .sourceBucket(sourceBucket).sourceKey(sourceKey).destinationBucket(bucket).destinationKey(key)
            .storageClass(options.storageClass).serverSideEncryption(options.sse).ssekmsKeyId(options.kmsKeyId)
            .requestPayer(payer())
        if( o.cacheControl != null || o.contentType != null )
            b.metadataDirective(MetadataDirective.REPLACE).cacheControl(o.cacheControl)
                .contentType(o.contentType ?: 'application/octet-stream')
        if( o.ifNoneMatch ) b.ifNoneMatch('*')
        if( o.sha256 ) b.checksumAlgorithm(ChecksumAlgorithm.SHA256)
        try {
            final CopyObjectResponse r = client.copyObject(b.build())
            note(r)
            return new S3Written(S3Written.Status.WRITTEN, r.copyObjectResult().eTag(), r.copyObjectResult().checksumSHA256())
        }
        catch( S3Exception e ) {
            note(e)
            final S3Written.Status status = statusOf(e)
            if( status == null ) throw e
            return new S3Written(status, null, null)
        }
    }

    @Override
    List<S3Listed> list(String prefix, int maxKeys) {
        final ListObjectsV2Request.Builder b = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).requestPayer(payer())
        final List<S3Listed> out = new ArrayList<S3Listed>()
        if( maxKeys > 0 ) {
            final ListObjectsV2Response r = client.listObjectsV2(b.maxKeys(maxKeys).build())
            note(r)
            for( S3Object o : r.contents() ) out.add(new S3Listed(o.key(), o.size()))
            return out
        }
        for( ListObjectsV2Response page : client.listObjectsV2Paginator(b.build()) ) {
            note(page)
            for( S3Object o : page.contents() ) out.add(new S3Listed(o.key(), o.size()))
        }
        return out
    }

    @Override
    void delete(String key) {
        note(client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).requestPayer(payer()).build()))
    }

    @Override
    Long firstServerDateMillis() { firstDate.get() }

    @Override
    String describe() { "s3://${bucket}" }
}
```

```groovy
// src/main/groovy/robsyme/cas/s3/S3Access.groovy
package robsyme.cas.s3

import groovy.transform.CompileStatic
import nextflow.cloud.aws.AwsClientFactory
import nextflow.cloud.aws.config.AwsConfig
import nextflow.cloud.aws.nio.util.S3SyncClientConfiguration
import software.amazon.awssdk.services.s3.S3Client

/**
 * The S3 client for one bucket, built the way nf-amazon builds its own
 * (S3FileSystemProvider.createFileSystem, S3FileSystemProvider.java:756-780 at
 * v26.04.6), from the config map a run or a verb loaded: the aws scope's
 * credentials, profile (SSO included), region, endpoint and HTTP settings.
 * No session is needed, and the S3Client wrapper's listBuckets probe is
 * skipped. With no region anywhere, resolveS3Region() answers us-east-1 and
 * the client is cross-region.
 */
@CompileStatic
class S3Access {

    private S3Access() {}

    static S3Ops open(Map config, String bucket) {
        final AwsConfig aws = awsConfig(config)
        final Properties props = new Properties()
        for( Map.Entry e : (aws.getS3LegacyProperties() as Map).entrySet() )
            if( e.value != null )
                props.setProperty(e.key.toString(), e.value.toString())
        // As S3FileSystemProvider.java:763-766: `global` would override a custom endpoint.
        final boolean global = !aws.s3Config.isCustomEndpoint()
        final AwsClientFactory factory = new AwsClientFactory(aws, aws.resolveS3Region())
        final S3Client client = factory.getS3Client(S3SyncClientConfiguration.create(props), global)
        return new SdkS3Ops(client, bucket, writeOptions(aws))
    }

    static S3WriteOptions writeOptions(Map config) {
        return writeOptions(awsConfig(config))
    }

    private static S3WriteOptions writeOptions(AwsConfig aws) {
        final def s3 = aws.s3Config
        if( !s3.storageClass && !s3.storageEncryption && !s3.storageKmsKeyId && !s3.requesterPays )
            return S3WriteOptions.NONE
        return new S3WriteOptions(s3.storageClass, s3.storageEncryption, s3.storageKmsKeyId, s3.requesterPays ?: false)
    }

    private static AwsConfig awsConfig(Map config) {
        final Object scope = config?.get('aws')
        return new AwsConfig(scope instanceof Map ? (Map) scope : Collections.emptyMap())
    }
}
```

`S3MemberFiles`: replace the class body with one over `S3Ops`, keeping `bucketAndPrefix` as it is:

```groovy
// src/main/groovy/robsyme/cas/explore/S3MemberFiles.groovy (imports: groovy.transform.CompileStatic, robsyme.cas.s3.S3Head, robsyme.cas.s3.S3Listed, robsyme.cas.s3.S3Ops)
/**
 * A member in a bucket, read with the user's own credentials so the page
 * never holds any (block explorer spec section 1.4), through the one S3 seam
 * (DESIGN.md §15). Every read is a ranged GetObject with If-Match on the ETag
 * the object had when opened, so a replaced object fails the read
 * (S3PreconditionFailed) instead of answering with the new object's bytes.
 */
@CompileStatic
class S3MemberFiles implements MemberFiles {

    private final S3Ops ops
    private final String prefix

    S3MemberFiles(S3Ops ops, String prefix) {
        this.ops = ops
        this.prefix = prefix
    }

    // bucketAndPrefix(URI) unchanged

    @Override
    MemberFiles.Opened open(String rel) {
        final String key = prefix + rel
        final S3Head head = ops.head(key)
        return head == null ? null : new Opened(ops, key, head.size, head.etag)
    }

    @CompileStatic
    private static class Opened implements MemberFiles.Opened {
        private final S3Ops ops
        private final String key
        final long size
        final String tag
        Opened(S3Ops ops, String key, long size, String tag) { this.ops = ops; this.key = key; this.size = size; this.tag = tag }
        @Override InputStream read(long start, long length) { ops.get(key, tag, start, length) }
        @Override void close() {}
    }

    @Override
    List<String> list(String dirRel) {
        final String under = prefix + dirRel.replaceAll('/+$', '') + '/'
        final List<String> names = []
        for( S3Listed object : ops.list(under, 0) ) {
            final String name = object.key.substring(under.length())
            if( name && !name.contains('/') )
                names.add(name)
        }
        return names
    }

    @Override
    String describe() { "s3://${ops.bucket}/${prefix}" }
}
```

Delete `open(URI)`, `defaultClient()`, `defaultRegion()` and `withPluginLoader`, and every `software.amazon.awssdk` import. In `ExploreCommand`, replace `membersOf` with

```groovy
    /**
     * Every configured member the explorer serves, writable first. The S3Ops
     * for a remote alias comes from {@code s3Ops}, called once per remote alias
     * with its bucket; production passes {@code S3Access.open(config, bucket)}.
     */
    static LinkedHashMap<String, MemberFiles> membersOf(CasConfig config, Closure<S3Ops> s3Ops) {
        final LinkedHashMap<String, MemberFiles> members = new LinkedHashMap<>()
        for( String alias : config.configuredAliases ) {
            if( config.isRemote(alias) ) {
                final List<String> bucketAndPrefix = S3MemberFiles.bucketAndPrefix(config.remoteLocationOf(alias))
                members.put(alias, new S3MemberFiles(s3Ops.call(bucketAndPrefix[0]), bucketAndPrefix[1]))
            }
            else {
                members.put(alias, new LocalMemberFiles(config.locationOf(alias)))
            }
        }
        return members
    }
```

and in `start`, `membersOf(cas)` becomes `membersOf(cas, { String bucket -> S3Access.open(config, bucket) } as Closure<S3Ops>)`. Replace the `software.amazon.awssdk.services.s3.S3Client` import with `robsyme.cas.s3.S3Access` and `robsyme.cas.s3.S3Ops`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.s3.*' --tests 'robsyme.cas.explore.*'`
Expected: PASS, including `S3MemberFilesTest`'s end-to-end serve through the real SDK against `FakeS3`.

Run: `./gradlew test memoryBoundTest dependencyCheck && ./gradlew assemble && unzip -l build/distributions/nf-blocks-0.1.0-beta.2.zip | grep -c awssdk`
Expected: BUILD SUCCESSFUL; `0` SDK jars in the zip.

- [ ] **Step 6: Confirm the plugin still starts and `explore` still reads a local member**

Run: `make gate` with `GATE_SKIP_BROWSER=` unset.
Expected: lineage 11/0/6, browser tier A 5/5, tier B 12/12. The Gate's `explore` start is the evidence that `requirePlugins` resolves nf-amazon for a verb. Also run `make gate-cloud GATE_ROOT=<same root>` if Rob has an SSO session (A6-A7 read a private S3 member through the new client); otherwise list it in Task 15's acceptance.

- [ ] **Step 7: Commit**

```bash
git add build.gradle src/main/groovy/robsyme/cas/s3 src/main/groovy/robsyme/cas/explore/S3MemberFiles.groovy \
  src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy src/test/groovy/robsyme/cas/s3 \
  src/test/groovy/robsyme/cas/explore/S3MemberFilesTest.groovy src/test/groovy/robsyme/cas/explore/ExploreCommandTest.groovy
git commit -m "feat(s3): one S3 seam over nf-amazon's SDK client; no SDK in the zip

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: `provider` leaves the Leaf; Directory Manifests lose the execute bit (schema 2)

Ticket 16 decisions 1-2, carried decision 23 (ticket 15 addendum, Rob 2026-09-28), silent decisions 1-3. The same content published by `head-node` in one run and by `fusion-node` or `s3-copy` in another must be one OutputItem block, so the provider moves out of the content-derived block into the RunCompletion. The page validates every block against DESIGN §6's IPLD Schema (`web/schema-gen.mjs` extracts it; `web/src/blocks.js:63` checks leaves), so the schema edit, the regenerated `schema.json` and the page's reader change land together. The same reasoning drops `executable` from Directory Manifests (Step 5b): an object store has no execute bit, so the bit let the backend change a manifest address. This task changes the records and the schema; Task 3 stops the walk recording the bit, so Task 3 starts after this task commits.

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/Providers.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/Records.groovy` (`Records.head`, `Leaf`, `OutputItem`, `RunCompletion`, `ManifestEntry`, `DirectoryManifest`)
- Modify: `src/main/groovy/robsyme/cas/trace/Join.groovy`
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (`writeCompletion` only)
- Modify: `DESIGN.md` §6 (the ```` ```ipldsch ```` block, the OutputItem and RunCompletion prose blocks, the first paragraph's "`schema` (integer, `1`)")
- Modify: `web/src/schema.js`, `web/src/blocks.js`, `web/src/generated/schema.json` (regenerated)
- Modify: `web/test/schema.test.mjs`, `web/test/blocks.test.mjs`
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (`Publish` only: the `contents` property)
- Modify: `src/main/groovy/robsyme/cas/core/DirectoryManifestBuilder.groovy` (the `executable` call, `isExecutable` and its imports only; Task 3 rewrites the rest)
- Modify: `src/test/groovy/robsyme/cas/core/RecordsTest.groovy`, `src/test/groovy/robsyme/cas/trace/JoinTest.groovy`, `src/test/groovy/robsyme/cas/core/Fixtures.groovy` (adds `leaf2`, `outputItem2`), `src/test/groovy/robsyme/cas/core/DirectoryManifestBuilderTest.groovy` (the executable feature), `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy` (`Leaf.of` at line 108), `src/test/groovy/robsyme/cas/nio/CasOccurrenceTest.groovy` (`Leaf.of` at lines 50 and 52)

**Interfaces:**
- Consumes: `CasSession.Publish.provider` (a string, `'head-node'` from every writer today).
- Produces: `DirectoryManifest.SCHEMA = 2`, `DirectoryManifest.READABLE = [1, 2]`; `ManifestEntry` modes `regular`, `symlink`, `directory`, `unresolvable` (the constant `ManifestEntry.EXECUTABLE` stays, only so `fromCbor` can recognise a schema-1 entry and `CasFileSystemProvider` compiles until Task 12); `Providers` (see shared interfaces); `Leaf(String name, Cid address, Long size, String reason)`, `Leaf.of(String, Cid, Long)`; `OutputItem.SCHEMA = 2`, `OutputItem.READABLE = [1, 2]`; `RunCompletion.SCHEMA = 2`, `RunCompletion.providers` (`Map<String, List<Cid>>`, keys sorted, each list sorted by CID string, no duplicates); `Join.Result.providers` (same type), merging each leaf's provider and each directory Publish's `contents`; `CasSession.Publish.contents` (`Map<String, List<Cid>>`, provider name to the addresses of the files inside a published directory, empty by default and for a file; Task 10 fills it from `DirectoryManifestBuilder.Result.providers`); `Records.head(String kind, int schema)`; `web/src/schema.js` `validLeafV1`.

- [ ] **Step 1: Write the failing Groovy tests**

Replace the Leaf features of `RecordsTest` (the ones at lines 159-202 that pass or assert a provider) with:

```groovy
    // ---- Leaf, schema 2: no provider (ticket 16) ----

    def 'a leaf carries no provider, and a schema-1 leaf that does still decodes'() {
        given:
        final Leaf leaf = Leaf.of('A.bam', raw('a'), 12L)

        expect:
        leaf.toCbor() == [kind: 'Leaf', name: 'A.bam', address: raw('a'), size: 12L, reason: null]
        Leaf.fromCbor(roundTrip(leaf.toCbor())) == leaf
        Leaf.fromCbor([kind: 'Leaf', name: 'A.bam', address: raw('a'), size: 12L, provider: 'head-node', reason: null]) == leaf
        Leaf.declined().toCbor() == [kind: 'Leaf', name: null, address: null, size: null, reason: 'declined']
    }

    def 'a leaf reason comes from a closed set, and an address excludes a reason'() {
        when:
        Leaf.without('A.bam', 'lost')
        then:
        thrown(IllegalArgumentException)

        when:
        new Leaf('A.bam', raw('a'), 1L, 'declined')
        then:
        thrown(IllegalArgumentException)
    }

    // ---- OutputItem, schema 2 ----

    def 'an OutputItem is written at schema 2; schema 1 is read; any other schema is refused'() {
        given:
        final OutputItem item = OutputItem.of([[sample: 'A'], Leaf.of('A.bam', raw('bam'), 3L)])

        expect:
        item.toCbor().schema == 2L
        OutputItem.fromCbor(roundTrip(item.toCbor())) == item
        OutputItem.fromCbor([kind: 'OutputItem', schema: 1L, value: [[sample: 'A'],
            [kind: 'Leaf', name: 'A.bam', address: raw('bam'), size: 3L, provider: 'fusion-node', reason: null]]]) == item

        when:
        OutputItem.fromCbor([kind: 'OutputItem', schema: 3L, value: 1L])
        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('schema 3')
    }

    def 'the same content and metadata is one OutputItem address whoever addressed it (ticket 16)'() {
        given:
        final Map v1Head = [kind: 'OutputItem', schema: 1L, value: [[sample: 'A'],
            [kind: 'Leaf', name: 'A.bam', address: raw('bam'), size: 3L, provider: 'head-node', reason: null]]]
        final Map v1Fusion = [kind: 'OutputItem', schema: 1L, value: [[sample: 'A'],
            [kind: 'Leaf', name: 'A.bam', address: raw('bam'), size: 3L, provider: 'fusion-node', reason: null]]]

        expect: 'schema 1 split one item into two addresses; schema 2 cannot'
        DagCbor.cidOf(DagCbor.encode(v1Head)) != DagCbor.cidOf(DagCbor.encode(v1Fusion))
        OutputItem.fromCbor(v1Head).toCbor() == OutputItem.fromCbor(v1Fusion).toCbor()
    }

    // ---- RunCompletion, schema 2: providers ----

    private static Map completionArgs(Map overrides = [:]) {
        final Map args = [assertedBy: 'gate', run: dag('m'), collections: [], inputSet: null,
                          status: 'succeeded', exitStatus: 0, possiblyIncomplete: false,
                          startedAt: '2026-09-28T10:00:00.000Z', finishedAt: '2026-09-28T10:05:00.000Z',
                          anomalies: Anomalies.NONE, error: null]
        args.putAll(overrides)
        return args
    }

    def 'providers are sorted by name, each list by address, without duplicates, and written at schema 2'() {
        given:
        final RunCompletion rc = new RunCompletion(completionArgs(providers: [
            's3-copy'  : [raw('b'), raw('a'), raw('b')],
            'head-node': [dag('dir')],
        ]))

        expect:
        rc.toCbor().schema == 2L
        (rc.toCbor().providers as Map).keySet().toList() == ['head-node', 's3-copy']
        rc.providers['s3-copy'] == [raw('a'), raw('b')].sort { it.toString() }
        RunCompletion.fromCbor(roundTrip(rc.toCbor())) == rc
    }

    def 'an unknown provider is refused'() {
        when:
        new RunCompletion(completionArgs(providers: [laptop: [raw('a')]]))
        then:
        final IllegalArgumentException e = thrown()
        e.message.contains("'laptop'")
    }

    def 'a schema-1 RunCompletion has no providers field and reads as none'() {
        given:
        final Map v1 = new RunCompletion(completionArgs()).toCbor()
        v1.schema = 1L
        v1.remove('providers')

        expect:
        RunCompletion.fromCbor(v1).providers == [:]

        when: 'a schema-2 block without the field'
        v1.schema = 2L
        RunCompletion.fromCbor(v1)
        then:
        thrown(IllegalArgumentException)
    }
```

(`raw(String)` and `roundTrip(Map)` are the spec's existing helpers; add `private static Cid dag(String s) { Cid.of(Cid.DAG_CBOR, Hashing.sha256(s.getBytes('UTF-8'))) }` if the spec has no dag-cbor helper.)

In `JoinTest`, delete the line `leavesByName['A.bam'].provider == 'head-node'`, and add:

```groovy
    def 'providers maps each provider to its leaf addresses'() {
        given:
        final cidA = rawCid(1)
        final cidB = rawCid(2)
        final dir = dagCid(3)
        final session = sessionWith([
            'cas://lab/aligned/A/A.bam': new CasSession.Publish(new StoreRef(cidA, 'A.bam'), 1L, 's3-copy'),
            'cas://lab/aligned/B/B.bam': new CasSession.Publish(new StoreRef(cidB, 'B.bam'), 1L, 'fusion-node'),
            'cas://lab/qc/A/A_qc'      : new CasSession.Publish(new StoreRef(dir, 'A_qc'), 1L, 'head-node'),
        ], dagCid(9))

        when:
        final result = Join.join([
            aligned: [[[sample: 'A'], coord('cas://lab/aligned/A/A.bam')], [[sample: 'B'], coord('cas://lab/aligned/B/B.bam')]],
            qc     : [[[sample: 'A'], coord('cas://lab/qc/A/A_qc')]],
        ] as Map<String, Object>, session)

        then:
        result.providers == ['fusion-node': [cidB], 'head-node': [dir], 's3-copy': [cidA]]
    }

    def 'the same bytes published by two providers in one run: one item address, the address under both (Review Focus 2)'() {
        given:
        final cid = rawCid(7)
        final session = sessionWith([
            'cas://lab/a/x.txt': new CasSession.Publish(new StoreRef(cid, 'x.txt'), 5L, 'head-node'),
            'cas://lab/b/x.txt': new CasSession.Publish(new StoreRef(cid, 'x.txt'), 5L, 's3-copy'),
        ], dagCid(9))

        when:
        final result = Join.join([
            a: [[[id: 1], coord('cas://lab/a/x.txt')]],
            b: [[[id: 1], coord('cas://lab/b/x.txt')]],
        ] as Map<String, Object>, session)

        then:
        result.outputs[0].collection.items == result.outputs[1].collection.items
        result.providers == ['head-node': [cid], 's3-copy': [cid]]
    }

    def 'providers also covers the files inside a published directory (ticket 16 decision 1)'() {
        given:
        final dir = dagCid(3)
        final inA = rawCid(4)
        final inB = rawCid(5)
        final session = sessionWith([
            'cas://lab/qc/A/A_qc': new CasSession.Publish(new StoreRef(dir, 'A_qc'), 1L, 'head-node',
                ['s3-copy': [inA], 'head-node': [inB]]),
        ], dagCid(9))

        when:
        final result = Join.join([qc: [[[sample: 'A'], coord('cas://lab/qc/A/A_qc')]]] as Map<String, Object>, session)

        then:
        result.providers == ['head-node': [dir, inB].sort { it.toString() }, 's3-copy': [inA]]
    }
```

In `Fixtures`, keep `leaf` and `outputItem` as they are (schema 1, so the index tests keep proving that readers accept old blocks) and add:

```groovy
    /** A schema-2 Leaf (ticket 16): no provider. */
    static Map leaf2(String name, Cid address, long size) {
        return [kind: 'Leaf', name: name, address: address, size: size, reason: null]
    }

    static Map outputItem2(Object value) {
        return [kind: 'OutputItem', schema: 2, value: value]
    }
```

Run: `./gradlew test --tests 'robsyme.cas.core.RecordsTest' --tests 'robsyme.cas.trace.JoinTest'`
Expected: FAIL: `Leaf.of` takes four arguments, `toCbor()` still has `provider`, `schema == 1`, `Result.providers` is missing.

- [ ] **Step 2: Implement the records**

```groovy
// src/main/groovy/robsyme/cas/core/Providers.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The Address Providers a run may record (DESIGN.md §6, ticket 16). The
 * provider is recorded by the run, in the RunCompletion, never in a
 * content-derived block, so no choice of provider changes an address.
 */
@CompileStatic
final class Providers {
    /** The head node streamed the bytes and hashed them: self-computed. */
    static final String HEAD_NODE = 'head-node'
    /** The task node hashed its outputs (.command.cas): asserted. */
    static final String FUSION_NODE = 'fusion-node'
    /** S3 computed the SHA-256 during a server-side copy: asserted. */
    static final String S3_COPY = 's3-copy'

    static final List<String> ALL = [HEAD_NODE, FUSION_NODE, S3_COPY].asImmutable()

    private Providers() {}

    static boolean isKnown(String name) { ALL.contains(name) }
}
```

In `Records`, replace `SCHEMA` and `head`:

```groovy
    /** The schema a kind is written at unless it says otherwise (DESIGN.md §6). */
    static final int SCHEMA = 1

    static Map<String, Object> head(String kind) {
        return head(kind, SCHEMA)
    }

    static Map<String, Object> head(String kind, int schema) {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('kind', kind)
        map.put('schema', (long) schema)
        return map
    }

    /** The block's schema, refused unless the reader knows it. */
    static int schemaOf(Map block, Collection<Integer> readable) {
        final int schema = (int) number(require(block, 'schema'), 'schema')
        if( !readable.contains(schema) )
            throw new IllegalArgumentException("${kindOf(block)} schema ${schema} is not one this build reads (${readable.join(', ')})")
        return schema
    }
```

Replace `class Leaf` with:

```groovy
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Leaf {

    static final String DECLINED = 'declined'
    static final String NEVER_PUBLISHED = 'never_published'
    static final String UNRESOLVABLE = 'unresolvable'
    static final String UNADDRESSED = 'unaddressed'

    private static final Set<String> REASONS = [DECLINED, NEVER_PUBLISHED, UNRESOLVABLE, UNADDRESSED] as Set

    final String name
    final Cid address
    final Long size
    final String reason

    Leaf(String name, Cid address, Long size, String reason) {
        if( address == null && !reason )
            throw new IllegalArgumentException("a leaf without an address needs a reason (${name ?: 'unnamed'})")
        if( address != null && reason )
            throw new IllegalArgumentException("a leaf addressed as $address cannot also carry the reason '$reason'")
        if( reason && !REASONS.contains(reason) )
            throw new IllegalArgumentException("unknown leaf reason '$reason'")
        this.name = name
        this.address = address
        this.size = size
        this.reason = reason
    }

    /** An addressed leaf. Who addressed it is the run's record (RunCompletion.providers), not the leaf's. */
    static Leaf of(String name, Cid address, Long size) {
        return new Leaf(name, address, size, null)
    }

    static Leaf without(String name, String reason) {
        return new Leaf(name, null, null, reason)
    }

    static Leaf declined() {
        return without(null, DECLINED)
    }

    boolean isAddressed() { address != null }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = new LinkedHashMap<String, Object>()
        map.put('kind', Records.LEAF)
        map.put('name', name)
        map.put('address', address)
        map.put('size', size)
        map.put('reason', reason)
        return map
    }

    // isLeaf(Object) unchanged

    /** A schema-1 leaf also carries `provider`; it is ignored (ticket 16). */
    static Leaf fromCbor(Map map) {
        Records.expectKind(map, Records.LEAF)
        final Object size = Records.require(map, 'size')
        return new Leaf(
            Records.string(Records.require(map, 'name'), 'name'),
            Records.cid(Records.require(map, 'address'), 'address'),
            size == null ? null : (Long) Records.number(size, 'size'),
            Records.string(Records.require(map, 'reason'), 'reason'))
    }
}
```

In `OutputItem`, add `static final int SCHEMA = 2` and `static final List<Integer> READABLE = [1, 2].asImmutable()`; `toCbor` uses `Records.head(Records.OUTPUT_ITEM, SCHEMA)`; `fromCbor` calls `Records.schemaOf(block, READABLE)` after `expectKind`. Keep the class comment's first paragraph and add: "Written at schema 2 (2026-09-28): its leaves carry no provider. A schema-1 item decodes to the same value, and re-encodes at schema 2 under a different address; nothing re-encodes a block it read."

In `RunCompletion`, add `static final int SCHEMA = 2`, `static final List<Integer> READABLE = [1, 2].asImmutable()`, a field `final Map<String, List<Cid>> providers`, set in the constructor after `anomalies`:

```groovy
        this.providers = normaliseProviders((Map) args.get('providers') ?: Collections.emptyMap())
```

with

```groovy
    /** Keys in name order, each list sorted by cid string and without repeats; every key a known provider. */
    private static Map<String, List<Cid>> normaliseProviders(Map raw) {
        final TreeMap<String, List<Cid>> out = new TreeMap<String, List<Cid>>()
        for( Object e : raw.entrySet() ) {
            final String name = String.valueOf(((Map.Entry) e).key)
            if( !Providers.isKnown(name) )
                throw new IllegalArgumentException("unknown Address Provider '${name}' (known: ${Providers.ALL.join(', ')})")
            final TreeMap<String, Cid> byText = new TreeMap<String, Cid>()
            for( Object c : (Collection) ((Map.Entry) e).value )
                byText.put(c.toString(), (Cid) c)
            out.put(name, Collections.unmodifiableList(new ArrayList<Cid>(byText.values())))
        }
        return Collections.unmodifiableMap(out)
    }
```

`toCbor` becomes `Records.head(Records.RUN_COMPLETION, SCHEMA)` and ends with

```groovy
        final Map<String, Object> byProvider = new LinkedHashMap<String, Object>()
        providers.each { String name, List<Cid> cids -> byProvider.put(name, new ArrayList<Object>(cids)) }
        map.put('providers', byProvider)
```

and `fromCbor` reads it:

```groovy
        final int schema = Records.schemaOf(block, READABLE)
        final Map providers = schema == 1 ? Collections.emptyMap() : (Map) Records.require(block, 'providers')
```

passing `providers: providers.collectEntries { k, v -> [(k): ((List) v).collect { Object c -> Records.cid(c, 'providers') }] }` into the constructor map.

- [ ] **Step 3: The join records providers; the observer writes them**

In `Join`, `Result` gains `final Map<String, List<Cid>> providers` (constructor third argument). In `join`, create `final Map<String, TreeMap<String, Cid>> byProvider = new TreeMap<String, TreeMap<String, Cid>>()` beside `counters`, pass it through `build` to `leafFor`, and in `leafFor` replace `return Leaf.of(name, address, size, publish.provider)` with

```groovy
        final String provider = publish.provider ?: Providers.HEAD_NODE
        byProvider.computeIfAbsent(provider, { String k -> new TreeMap<String, Cid>() }).put(address.toString(), address)
        // The files inside a published directory are addresses the run published too (ticket 16 decision 1).
        publish.contents?.each { String p, List<Cid> cids ->
            final TreeMap<String, Cid> held = byProvider.computeIfAbsent(p, { String k -> new TreeMap<String, Cid>() })
            cids.each { Cid c -> held.put(c.toString(), c) }
        }
        return Leaf.of(name, address, size)
```

In `CasSession.Publish` add a fourth property after `provider`:

```groovy
        /**
         * For a published directory, the providers of the files inside it
         * (provider name to addresses), so RunCompletion.providers covers every
         * address the run published (ticket 16 decision 1). Empty for a file.
         */
        Map<String, List<Cid>> contents = [:]
```

`@Canonical`'s tuple constructor keeps the three-argument form, with `contents` defaulting to the empty map, so no existing caller changes.

and return `new Result(outputs, counters.toAnomalies(), byProvider.collectEntries { String k, TreeMap<String, Cid> v -> [(k): new ArrayList<Cid>(v.values())] } as Map<String, List<Cid>>)`. The `build` signature gains the `byProvider` parameter in every recursive call.

In `CasObserver.writeCompletion`, add `providers: joined.providers,` to the `RunCompletion` map after `anomalies`.

Run: `./gradlew test`
Expected: PASS once the other callers of the four-argument `Leaf.of` drop the provider argument: `src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy:108` and `src/test/groovy/robsyme/cas/nio/CasOccurrenceTest.groovy:50,52` (confirm with `grep -rn 'Leaf.of(\|new Leaf(' src`; no main-code caller remains).

- [ ] **Step 4: Write the failing page tests**

In `web/test/schema.test.mjs`, change the count test to 22 types and add:

```js
test('a schema-2 Leaf has no provider; a schema-1 Leaf keeps its own', () => {
  assert.equal(validLeaf({ kind: 'Leaf', name: 'a', address: null, size: null, reason: 'declined' }), true)
  assert.equal(validLeaf({ kind: 'Leaf', name: 'a', address: null, size: null, provider: 'head-node', reason: 'declined' }), false)
  assert.equal(validLeafV1({ kind: 'Leaf', name: 'a', address: null, size: null, provider: 'head-node', reason: 'declined' }), true)
  assert.equal(validLeafV1({ kind: 'Leaf', name: 'a', address: null, size: null, provider: 'laptop', reason: null }), false)
})
```

(import `validLeafV1` beside `validLeaf`; delete the old `validLeaf` lines with `provider` from "every fixture block is valid").

In `web/test/blocks.test.mjs` add:

```js
import * as dagCbor from '@ipld/dag-cbor'
import { block, rawCid } from './fixture.mjs'

const served = (...values) => {
  const blocks = new Map(values.map(v => { const b = block(v); return [b.cid.toString(), b.bytes] }))
  return { cids: [...blocks.keys()], fetcher: new BlockFetcher('http://h/', { fetchFn: blockFetch(blocks) }) }
}
const leafV2 = { kind: 'Leaf', name: 'A.bam', address: rawCid('a'), size: 1, reason: null }
const leafV1 = { ...leafV2, provider: 'head-node' }

test('an OutputItem is read at schema 2 with bare leaves and at schema 1 with provider leaves, and no other way (ticket 16)', async () => {
  const { cids, fetcher } = served(
    { kind: 'OutputItem', schema: 2, value: [{ sample: 'A' }, leafV2] },
    { kind: 'OutputItem', schema: 1, value: [{ sample: 'A' }, leafV1] },
    { kind: 'OutputItem', schema: 2, value: [{ sample: 'A' }, leafV1] },
    { kind: 'OutputItem', schema: 3, value: [{ sample: 'A' }, leafV2] })
  assert.equal((await fetcher.get(cids[0])).value.schema, 2)
  assert.equal((await fetcher.get(cids[1])).value.schema, 1)
  await assert.rejects(fetcher.get(cids[2]), e => e.code === 'schema_invalid')
  await assert.rejects(fetcher.get(cids[3]), e => e.code === 'schema_invalid')
})

test('a schema-2 RunCompletion carries providers with known names; schema 1 carries none', async () => {
  const { blocks, runs } = await buildMember()
  const v1 = dagCbor.decode(blocks.get(runs.R1.completion))
  const { cids, fetcher } = served(
    { ...v1, schema: 2, providers: { 'head-node': [rawCid('a')], 's3-copy': [] } },
    { ...v1, schema: 2 },
    { ...v1, schema: 2, providers: { laptop: [] } },
    { ...v1, schema: 1, providers: {} })
  assert.equal((await fetcher.get(cids[0])).value.kind, 'RunCompletion')
  for (const cid of cids.slice(1))
    await assert.rejects(fetcher.get(cid), e => e.code === 'schema_invalid')
})
```

Run: `cd web && npm test`
Expected: FAIL: 21 types; `validLeafV1` is not exported; the schema-1 fixture items still validate only because their leaves carry `provider`.

- [ ] **Step 5: Amend the IPLD Schema and the page**

In `DESIGN.md` §6, in the ```` ```ipldsch ```` block, replace

```
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
```

with

```
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
```

and in `type RunCompletion struct`, after `error nullable String`, add

```
  providers optional {String:[&Any]}  # schema 2: provider name -> every address of the run it supplied, sorted; absent at schema 1
```

In the prose, replace the OutputItem block's `{ kind: "OutputItem", schema: 1,` with `{ kind: "OutputItem", schema: 2,` and the Leaf block

```
{ kind: "Leaf", name: string|null, address: Cid|null, size: int|null,
  provider: "head-node"|"fusion-node"|null,
  reason: null|"declined"|"never_published"|"unresolvable"|"unaddressed" }
```

with

```
{ kind: "Leaf", name: string|null, address: Cid|null, size: int|null,
  reason: null|"declined"|"never_published"|"unresolvable"|"unaddressed" }
```

adding after the Leaf paragraph: "*Amended 2026-09-28 (ticket 16):* the Leaf carries no provider, so the same content published by different Address Providers is one OutputItem. An OutputItem is written at `schema: 2`; a reader accepts `schema: 1`, whose Leaves carry `provider`, and ignores it. The RunCompletion records providers." In the RunCompletion prose block replace `{ kind: "RunCompletion", schema: 1, asserted_by: string,` with `{ kind: "RunCompletion", schema: 2, asserted_by: string,` and add a last field line `  providers: { <provider>: [Cid, ...] } }   // schema 2: every address the run published (leaves and the files inside published directories) under the provider that supplied it` (moving the closing brace). In §6's first paragraph replace "`schema` (integer, `1`)" with "`schema` (integer: `1`, except OutputItem and RunCompletion, `2` since 2026-09-28)".

```js
// web/src/schema.js
import { create } from '@ipld/schema/typed.js'
import schema from './generated/schema.json' with { type: 'json' }

const block = create(schema, 'Block')
const leaf = create(schema, 'Leaf')
const leafV1 = create(schema, 'LeafV1')

export const validBlock = (value) => block.toTyped(value) !== undefined
export const validLeaf = (value) => leaf.toTyped(value) !== undefined
export const validLeafV1 = (value) => leafV1.toTyped(value) !== undefined
```

In `web/src/blocks.js`, import `validLeafV1`, and replace the `if (!validBlock(value) || ...)` line with

```js
    if (!validBlock(value) || !validKind(value))
```

adding at module level:

```js
const PROVIDERS = new Set(['head-node', 'fusion-node', 's3-copy'])

// What the IPLD Schema cannot say (DESIGN.md §6, ticket 16): an OutputItem's
// leaves are Leaf at schema 2 and LeafV1 at schema 1; a RunCompletion carries
// providers exactly when it is at schema 2, with known provider names.
function validKind(value) {
  if (value.kind === 'OutputItem') {
    if (value.schema === 2) return leavesOf(value.value).every(validLeaf)
    if (value.schema === 1) return leavesOf(value.value).every(validLeafV1)
    return false
  }
  if (value.kind === 'RunCompletion') {
    if (value.schema === 1) return value.providers === undefined
    return value.schema === 2 && value.providers !== undefined && Object.keys(value.providers).every(k => PROVIDERS.has(k))
  }
  return true
}
```

- [ ] **Step 5b: Directory Manifests at schema 2, without `executable`**

Failing tests first, in `RecordsTest`:

```groovy
    def 'a manifest is written at schema 2 and has no executable mode; a schema-1 executable entry reads as regular (ticket 15 addendum)'() {
        given:
        final Cid a = raw('tool')
        final Map v1 = [kind: 'DirectoryManifest', schema: 1L,
                        entries: [[name: 'tool.sh', mode: 'executable', size: 4L, address: a, target: null]]]

        expect:
        new DirectoryManifest([ManifestEntry.regular('tool.sh', a, 4L)]).toCbor().schema == 2L
        DirectoryManifest.fromCbor(v1).entries == [ManifestEntry.regular('tool.sh', a, 4L)]

        when:
        new ManifestEntry('tool.sh', 'executable', 4L, a, null)
        then:
        thrown(IllegalArgumentException)

        when:
        DirectoryManifest.fromCbor([kind: 'DirectoryManifest', schema: 2L,
            entries: [[name: 'tool.sh', mode: 'executable', size: 4L, address: a, target: null]]])
        then:
        thrown(IllegalArgumentException)
    }
```

and in `web/test/blocks.test.mjs`:

```js
test('a schema-2 manifest has no executable entry; schema 1 may (ticket 15 addendum)', async () => {
  const e = (mode) => ({ name: 't', mode, size: 1, address: rawCid('t'), target: null })
  const { cids, fetcher } = served(
    { kind: 'DirectoryManifest', schema: 2, entries: [e('regular')] },
    { kind: 'DirectoryManifest', schema: 1, entries: [e('executable')] },
    { kind: 'DirectoryManifest', schema: 2, entries: [e('executable')] })
  assert.equal((await fetcher.get(cids[0])).value.schema, 2)
  assert.equal((await fetcher.get(cids[1])).value.schema, 1)
  await assert.rejects(fetcher.get(cids[2]), e => e.code === 'schema_invalid')
})
```

Then in `Records`: `ManifestEntry`'s `MODES` becomes `[REGULAR, SYMLINK, DIRECTORY, UNRESOLVABLE]`, the `executable(...)` factory is deleted, the constructor's regular check reads `mode == REGULAR`, and `fromCbor` gains a `schema` argument:

```groovy
    /** A schema-1 manifest may say `executable`; since 2026-09-28 that is `regular` (ticket 15 addendum). */
    static ManifestEntry fromCbor(Map entry, int schema) {
        final String mode = Records.string(Records.require(entry, 'mode'), 'mode')
        return new ManifestEntry(
            Records.string(Records.require(entry, 'name'), 'name'),
            schema == 1 && mode == EXECUTABLE ? REGULAR : mode,
            Records.number(Records.require(entry, 'size'), 'size'),
            Records.cid(Records.require(entry, 'address'), 'address'),
            Records.string(Records.require(entry, 'target'), 'target'))
    }
```

`DirectoryManifest` gains `static final int SCHEMA = 2` and `READABLE = [1, 2]`, writes `Records.head(Records.DIRECTORY_MANIFEST, SCHEMA)`, and its `fromCbor` reads `final int schema = Records.schemaOf(block, READABLE)` and passes it to each `ManifestEntry.fromCbor(e, schema)`. Update the class comment of `ManifestEntry` ("No permission bits beyond the executable one" becomes "No permission bits at all: identity must not depend on the umask, or on whether the backend keeps an execute bit").

In `DESIGN.md` §6: in the IPLD Schema, replace the `EntryMode` enum's comment line (add one above it) with

```
# `executable` appears only in a DirectoryManifest at schema 1, written before
# 2026-09-28, and reads as regular; schema 2 never holds it.
```

keeping the member (a schema-1 block must still validate). In `type DirEntry struct`, change the `address` comment `# raw cid for regular/executable, &DirectoryManifest for directory` to `# raw cid for regular, &DirectoryManifest for directory`. Replace the DirectoryManifest prose block's first line with `{ kind: "DirectoryManifest", schema: 2,` and its mode line with `mode: "regular"|"symlink"|"directory"|"unresolvable",`, the address comment with `// raw cid for regular, dag-cbor cid for directory, null for symlink/unresolvable`, and add after the symlink rules paragraph: "*Amended 2026-09-28 (ticket 15 addendum, Rob):* a manifest records no execute bit. An object store keeps none, so the bit let the storage backend change a manifest address. A reader accepts schema 1 and reads its `executable` entries as `regular`; materialising a manifest sets no execute permission."

In `web/src/blocks.js`'s `validKind`, add before `return true`:

```js
  if (value.kind === 'DirectoryManifest') {
    if (value.schema === 1) return true
    return value.schema === 2 && value.entries.every(e => e.mode !== 'executable')
  }
```

In `RecordsTest`'s 'every mode has its own shape' (line 34), replace `ManifestEntry.executable('run.sh', raw('x'), 1L).toCbor().mode == 'executable'` with `ManifestEntry.regular('run.sh', raw('x'), 1L).toCbor().mode == 'regular'`.

In `DirectoryManifestBuilder`, the local walk still calls `ManifestEntry.executable`: replace the whole `return isExecutable(path) ? ManifestEntry.executable(...) : ManifestEntry.regular(...)` ternary with `return ManifestEntry.regular(name, address, attrs.size())`, delete the private `isExecutable(Path)` method and its comment, and delete the imports it alone used (`java.nio.file.attribute.PosixFileAttributeView`, `java.nio.file.attribute.PosixFilePermission`). Leave the rest of the walk to Task 3.

Run: `./gradlew compileGroovy`, then `./gradlew test --tests 'robsyme.cas.core.RecordsTest'` and `cd web && npm test`.
Expected: PASS. `DirectoryManifestBuilderTest`'s executable feature now fails: change its expectation to `regular` here.

- [ ] **Step 6: Run the page tests and the Gate**

Run: `cd web && npm test`
Expected: PASS; `src/generated/schema.json: 22 types`; `git diff --stat web/src/generated/schema.json` shows it changed.

Run: `./gradlew test` then `make gate`
Expected: PASS; lineage 11/0/6, tier A 5/5, tier B 12/12 (the page reads the Gate's schema-2 blocks; B8's Selection address is unchanged because Selections did not change). Assertion 5 still passes: the Test Pipeline's `*_qc` tree holds no executable file, so only the manifest's `schema` changed; `gate/assert.py`'s walk (`_walk`, `_compare_manifest`) is updated in Task 13 to expect `regular` for an executable file.

- [ ] **Step 7: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Providers.groovy src/main/groovy/robsyme/cas/core/Records.groovy \
  src/main/groovy/robsyme/cas/trace/Join.groovy src/main/groovy/robsyme/cas/trace/CasObserver.groovy DESIGN.md \
  web/src/schema.js web/src/blocks.js web/src/generated/schema.json web/test/schema.test.mjs web/test/blocks.test.mjs \
  src/test/groovy/robsyme/cas/core/RecordsTest.groovy src/test/groovy/robsyme/cas/trace/JoinTest.groovy \
  src/test/groovy/robsyme/cas/core/Fixtures.groovy src/main/groovy/robsyme/cas/core/DirectoryManifestBuilder.groovy \
  src/test/groovy/robsyme/cas/core/DirectoryManifestBuilderTest.groovy src/main/groovy/robsyme/cas/CasSession.groovy \
  src/test/groovy/robsyme/cas/ext/CasExtensionTest.groovy src/test/groovy/robsyme/cas/nio/CasOccurrenceTest.groovy
git commit -m "feat(records): provider moves to RunCompletion.providers; manifests lose the execute bit (schema 2)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```


### Task 3: Directory manifests from an object store, and Fusion's links

Tickets 05 and 15 (decisions 15, 16, 23), silent decision 23. Starts after Task 2 commits (Task 2 removes `ManifestEntry.executable`). Two things break a directory published from an S3 work dir today: `DirectoryManifestBuilder` calls `toRealPath` (lines 67, 120, 176), which `S3Path` does not support (`S3Path.java:432` throws `UnsupportedOperationException`), and it reads Fusion's link encoding as data (`batch-publish.md` §5). This task gives the builder an object-store walk that decodes `.fusion.symlinks`, and a `FileAddresser` seam so Task 10 can address each file inside a directory the way it addresses a lone file (`s3-copy`, node digest, head-node). The local walk keeps its behaviour and its tests.

The encoding, measured on Fusion 2.5.14 (`fusion-batch-remeasure.md` §6): a link is an object at the link's own key whose body is the target text, byte for byte, no trailing newline; each directory holding links gets one `.fusion.symlinks` object listing their names separated by `\n`, no trailing newline, any order; a link to a directory is stored the same way with nothing copied below it; no metadata marks a link.

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/FileAddresser.groovy` (`FileAddresser`, `Addressed`, `HeadNodeAddresser`)
- Create: `src/main/groovy/robsyme/cas/core/FusionLinks.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/DirectoryManifestBuilder.groovy`
- Create: `src/test/groovy/robsyme/cas/core/FusionLinksTest.groovy`
- Create: `src/test/groovy/robsyme/cas/core/ObjectStoreManifestTest.groovy`

**Interfaces:**
- Consumes: `BlockStore.putStreaming`, `BlockStore.putDagCbor`, `DirectoryManifest`, `ManifestEntry` factories, `Anomalies.unresolvable(int)`.
- Produces: `FileAddresser`, `Addressed`, `HeadNodeAddresser` (shared interfaces); `DirectoryManifestBuilder(BlockStore)` (head-node addresser, `objectPath` resolving nothing), `DirectoryManifestBuilder(BlockStore, FileAddresser, Closure<Path> objectPath)`; `DirectoryManifestBuilder.Result.providers` (`Map<String, List<Cid>>`: each provider the addresser returned for a file inside the tree, to those files' addresses, sorted by CID string, no repeats; the manifests themselves are not in it); `static Path DirectoryManifestBuilder.realOf(Path)`; `FusionLinks.SIDECAR`, `FusionLinks.parse(byte[])`, `FusionLinks.target(byte[]) -> String` (null when the body is not a storable target).

The tests use the JDK's zip filesystem as the object store: it is not the default filesystem (so the builder takes the object-store walk), it has directories and plain files but no links, which is exactly what Fusion leaves in a bucket.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/FusionLinksTest.groovy
package robsyme.cas.core

import spock.lang.Specification

class FusionLinksTest extends Specification {

    def 'a listing is names separated by newlines, with or without a trailing newline'() {
        expect:
        FusionLinks.parse('rel.txt\ndangling.txt\ndirlink'.bytes).names == ['rel.txt', 'dangling.txt', 'dirlink'] as Set
        FusionLinks.parse('up.txt\n'.bytes).names == ['up.txt'] as Set
        FusionLinks.parse(new byte[0]).ok
        FusionLinks.parse('name with space.txt'.bytes).names == ['name with space.txt'] as Set
    }

    def 'an unparseable listing says why (#why)'() {
        expect:
        !FusionLinks.parse(body).ok
        FusionLinks.parse(body).problem.contains(why)

        where:
        why            | body
        'NUL'          | 'a\u0000b'.bytes
        'UTF-8'        | [0x61, 0xff, 0x62] as byte[]
        'empty name'   | 'a\n\nb'.bytes
        'path segment' | 'a/b'.bytes
        'path segment' | '..'.bytes
        'bytes'        | new byte[FusionLinks.MAX_SIDECAR_BYTES + 1]
    }

    def 'a link body is a target only when short, UTF-8 and free of NUL'() {
        expect:
        FusionLinks.target('../target.txt'.bytes) == '../target.txt'
        FusionLinks.target(('x' * 4097).bytes) == null
        FusionLinks.target('a\u0000'.bytes) == null
        FusionLinks.target(new byte[0]) == null
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/core/ObjectStoreManifestTest.groovy
package robsyme.cas.core

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/**
 * A directory as Fusion leaves it in a bucket (ticket 06 §6), walked from a
 * non-default filesystem, against the same tree built with real links on
 * local disk (ticket 15 decision 1: the backend is not provenance).
 */
class ObjectStoreManifestTest extends Specification {

    @TempDir Path work
    LocalBlockStore store
    FileSystem zip

    def setup() {
        store = new LocalBlockStore(work.resolve('store'), 'lab', true)
        zip = FileSystems.newFileSystem(URI.create('jar:' + work.resolve('bucket.zip').toUri()), [create: 'true'])
    }

    def cleanup() { zip?.close() }

    private Path obj(String key, String body) {
        final Path p = zip.getPath('/' + key)
        Files.createDirectories(p.parent)
        Files.writeString(p, body)
        return p
    }

    private DirectoryManifest read(Cid cid) {
        store.open(cid).withCloseable { DirectoryManifest.fromCbor((Map) DagCbor.decode(it.bytes)) }
    }

    /** The prototype-06 tree, minus the host-absolute link (decision 16 makes it unresolvable here, content locally). */
    private Path fusionTree() {
        obj('w/d/target.txt', 'target\n')
        obj('w/d/nested/deeper/deep.txt', 'deep\n')
        obj('w/d/name with space.txt', 'x\n')
        obj('w/d/rel.txt', 'target.txt')
        obj('w/d/nested/up.txt', '../target.txt')
        obj('w/d/dirlink', 'nested/deeper')
        obj('w/d/escape.txt', '../../../escape.txt')
        obj('w/d/dangling.txt', 'missing.txt')
        obj('w/d/other/spaced.txt', 'name with space.txt')
        obj('w/d/.fusion.symlinks', 'rel.txt\ndangling.txt\ndirlink\nescape.txt')
        obj('w/d/nested/.fusion.symlinks', 'up.txt')
        obj('w/d/other/.fusion.symlinks', 'spaced.txt')
        return zip.getPath('/w/d')
    }

    private Path localTree() {
        final Path d = work.resolve('local/w/d')
        Files.createDirectories(d.resolve('nested/deeper'))
        Files.createDirectories(d.resolve('other'))
        Files.writeString(d.resolve('target.txt'), 'target\n')
        Files.writeString(d.resolve('nested/deeper/deep.txt'), 'deep\n')
        Files.writeString(d.resolve('name with space.txt'), 'x\n')
        Files.createSymbolicLink(d.resolve('rel.txt'), Path.of('target.txt'))
        Files.createSymbolicLink(d.resolve('nested/up.txt'), Path.of('../target.txt'))
        Files.createSymbolicLink(d.resolve('dirlink'), Path.of('nested/deeper'))
        Files.createSymbolicLink(d.resolve('escape.txt'), Path.of('../../../escape.txt'))
        Files.createSymbolicLink(d.resolve('dangling.txt'), Path.of('missing.txt'))
        Files.createSymbolicLink(d.resolve('other/spaced.txt'), Path.of('name with space.txt'))
        return d
    }

    def 'a Fusion tree and the same tree on local disk are one Directory Manifest (ticket 15 decision 1)'() {
        when:
        final def fusion = new DirectoryManifestBuilder(store).build(fusionTree())
        final def local = new DirectoryManifestBuilder(store).build(localTree())

        then:
        fusion.cid == local.cid
        fusion.anomalies == local.anomalies
        final DirectoryManifest m = read(fusion.cid)
        m.entry('.fusion.symlinks') == null
        m.entry('rel.txt').mode == 'symlink' && m.entry('rel.txt').target == 'target.txt'
        m.entry('dirlink').mode == 'symlink'
        m.entry('dangling.txt').mode == 'unresolvable' && m.entry('dangling.txt').target == 'missing.txt'
        m.entry('escape.txt').target == Records.REDACTED_LOCATION
    }

    def 'an escaping link that exists is followed and stored as content; /fusion/s3 targets resolve by key'() {
        given:
        obj('w/outside.txt', 'out\n')
        obj('other-bucket/k/far.txt', 'far\n')
        obj('w/d/a.txt', '../outside.txt')
        obj('w/d/b.txt', '/fusion/s3/other-bucket/k/far.txt')
        obj('w/d/c.txt', '/etc/hostname')
        obj('w/d/.fusion.symlinks', 'a.txt\nb.txt\nc.txt')
        final Closure<Path> objectPath = { String uri ->
            uri.startsWith('s3://other-bucket/') ? zip.getPath('/other-bucket/' + uri.substring('s3://other-bucket/'.length())) : null
        }

        when:
        final def r = new DirectoryManifestBuilder(store, new HeadNodeAddresser(store), objectPath).build(zip.getPath('/w/d'))
        final DirectoryManifest m = read(r.cid)

        then:
        m.entry('a.txt').mode == 'regular' && store.open(m.entry('a.txt').address).text == 'out\n'
        m.entry('b.txt').mode == 'regular' && store.open(m.entry('b.txt').address).text == 'far\n'
        m.entry('c.txt').mode == 'unresolvable' && m.entry('c.txt').target == Records.REDACTED_LOCATION
        r.anomalies.unresolvable == 1
    }

    def 'a chain is followed, a cycle is unresolvable and counted (Review Focus 4)'() {
        given:
        obj('w/d/target.txt', 't\n')
        obj('w/d/a', 'b')
        obj('w/d/b', 'target.txt')
        obj('w/d/x', 'y')
        obj('w/d/y', 'x')
        obj('w/d/.fusion.symlinks', 'a\nb\nx\ny')

        when:
        final def r = new DirectoryManifestBuilder(store).build(zip.getPath('/w/d'))
        final DirectoryManifest m = read(r.cid)

        then:
        m.entry('a').mode == 'symlink' && m.entry('a').target == 'b'
        m.entry('b').mode == 'symlink'
        m.entry('x').mode == 'unresolvable' && m.entry('x').target == 'y'
        r.anomalies.unresolvable == 2
    }

    def 'a listed name without an object, a long body and an unparseable listing (ticket 15 decision 4)'() {
        given:
        obj('w/d/long.txt', 'x' * 5000)
        obj('w/d/.fusion.symlinks', 'long.txt\nghost.txt')
        obj('w/e/f.txt', 'f')
        obj('w/e/.fusion.symlinks', 'f.txt\u0000')

        when:
        final def d = new DirectoryManifestBuilder(store).build(zip.getPath('/w/d'))
        final def e = new DirectoryManifestBuilder(store).build(zip.getPath('/w/e'))

        then:
        read(d.cid).entry('ghost.txt').mode == 'unresolvable' && read(d.cid).entry('ghost.txt').target == null
        read(d.cid).entry('long.txt').target == Records.REDACTED_LOCATION
        d.anomalies.unresolvable == 2
        read(e.cid).entry('.fusion.symlinks').mode == 'regular'
        read(e.cid).entry('f.txt').mode == 'regular'
        e.anomalies.unresolvable == 1
    }

    def 'every file is regular, from an object store or local disk, and each goes through the addresser'() {
        given:
        obj('w/d/tool.sh', '#!/bin/sh\n')
        final Path localDir = Files.createDirectories(work.resolve('local-tool/d'))
        final Path localTool = Files.writeString(localDir.resolve('tool.sh'), '#!/bin/sh\n')
        localTool.toFile().setExecutable(true)
        final List<String> asked = []
        final FileAddresser counting = { Path f, long size -> asked << f.fileName.toString(); new HeadNodeAddresser(store).address(f, size) } as FileAddresser

        when:
        final def r = new DirectoryManifestBuilder(store, counting, null).build(zip.getPath('/w/d'))
        final def l = new DirectoryManifestBuilder(store, counting, null).build(localDir)

        then:
        read(r.cid).entry('tool.sh').mode == 'regular'
        read(l.cid).entry('tool.sh').mode == 'regular'
        r.cid == l.cid
        asked == ['tool.sh', 'tool.sh']
    }

    def 'the result lists every file address under the provider that supplied it, in both walks (ticket 16 decision 1)'() {
        given:
        obj('w/d/a.txt', 'a\n')
        obj('w/d/sub/b.txt', 'b\n')
        final Path localDir = Files.createDirectories(work.resolve('local-prov/d/sub'))
        Files.writeString(localDir.parent.resolve('a.txt'), 'a\n')
        Files.writeString(localDir.resolve('b.txt'), 'b\n')
        final FileAddresser split = { Path f, long size ->
            final Addressed a = new HeadNodeAddresser(store).address(f, size)
            f.fileName.toString() == 'a.txt' ? new Addressed(a.cid, a.size, Providers.S3_COPY) : a
        } as FileAddresser
        final Cid cidA = store.putStreaming(new ByteArrayInputStream('a\n'.bytes))
        final Cid cidB = store.putStreaming(new ByteArrayInputStream('b\n'.bytes))

        when:
        final def r = new DirectoryManifestBuilder(store, split, null).build(zip.getPath('/w/d'))
        final def l = new DirectoryManifestBuilder(store, split, null).build(localDir.parent)

        then:
        r.providers == [(Providers.HEAD_NODE): [cidB], (Providers.S3_COPY): [cidA]]
        l.providers == r.providers
        !r.providers.values().flatten().contains(r.cid)
    }

    def 'realOf falls back to the absolute path where toRealPath is unsupported (ticket 05)'() {
        given:
        final Path norm = Stub(Path)
        final Path abs = Stub(Path) { normalize() >> norm }
        final Path s3 = Stub(Path) {
            toRealPath(*_) >> { throw new UnsupportedOperationException() }
            toAbsolutePath() >> abs
        }

        expect:
        DirectoryManifestBuilder.realOf(s3).is(norm)
    }
}
```

Run: `./gradlew test --tests 'robsyme.cas.core.FusionLinksTest' --tests 'robsyme.cas.core.ObjectStoreManifestTest'`
Expected: compilation FAILS on `FusionLinks`, `FileAddresser`, `HeadNodeAddresser`, `realOf`.

- [ ] **Step 2: The seam and the parser**

```groovy
// src/main/groovy/robsyme/cas/core/FileAddresser.groovy
package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * The one seam through which a published file's address arrives (spec §3,
 * Address Providers). The implementation stores the content in the writable
 * member, or finds it already there, and says which provider supplied the address.
 */
@CompileStatic
interface FileAddresser {
    Addressed address(Path file, long size)
}

@Canonical
@CompileStatic
class Addressed {
    Cid cid
    long size
    String provider
}

/** The provider that always works: the head node streams the file through one hash buffer. */
@CompileStatic
class HeadNodeAddresser implements FileAddresser {
    private final BlockStore store
    HeadNodeAddresser(BlockStore store) { this.store = store }

    @Override
    Addressed address(Path file, long size) {
        final InputStream input = Files.newInputStream(file)
        try {
            return new Addressed(store.putStreaming(input), size, Providers.HEAD_NODE)
        }
        finally {
            input.close()
        }
    }
}
```

```groovy
// src/main/groovy/robsyme/cas/core/FusionLinks.groovy
package robsyme.cas.core

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * Fusion's link encoding in an object store (ticket 06 §6, ticket 15). Read
 * because it is part of a directory's content; no other Fusion internal is.
 */
@CompileStatic
class FusionLinks {

    static final String SIDECAR = '.fusion.symlinks'
    static final int MAX_SIDECAR_BYTES = 1 << 20
    static final int MAX_TARGET_BYTES = 4096

    @Canonical
    static class Parsed {
        boolean ok
        Set<String> names
        String problem
    }

    static Parsed parse(byte[] body) {
        if( body.length > MAX_SIDECAR_BYTES )
            return bad("the listing is over ${MAX_SIDECAR_BYTES} bytes")
        final String text = utf8(body)
        if( text == null )
            return bad('the listing is not UTF-8')
        if( text.indexOf('\u0000') >= 0 )
            return bad('the listing holds NUL')
        final String trimmed = text.endsWith('\n') ? text.substring(0, text.length() - 1) : text
        final Set<String> names = new LinkedHashSet<String>()
        if( trimmed.isEmpty() )
            return new Parsed(true, names, null)
        for( String name : trimmed.split('\n', -1) ) {
            if( name.isEmpty() )
                return bad('the listing has an empty name')
            if( name.contains('/') || name == '.' || name == '..' )
                return bad("'${name}' is not one path segment")
            names.add(name)
        }
        return new Parsed(true, names, null)
    }

    /** A link object's body as a target a manifest may hold, or null. */
    static String target(byte[] body) {
        if( body.length == 0 || body.length > MAX_TARGET_BYTES )
            return null
        final String text = utf8(body)
        return text == null || text.indexOf('\u0000') >= 0 ? null : text
    }

    private static String utf8(byte[] body) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body)).toString()
        }
        catch( CharacterCodingException e ) {
            return null
        }
    }

    private static Parsed bad(String problem) { new Parsed(false, Collections.<String> emptySet(), problem) }
}
```

- [ ] **Step 3: The builder**

In `DirectoryManifestBuilder`: add `@Slf4j`; fields `private final FileAddresser addresser`, `private final Closure<Path> objectPath` and `private final Map<String, TreeMap<String, Cid>> tally = new TreeMap<String, TreeMap<String, Cid>>()` (the providers of the build in progress; every caller constructs one builder per build, so one instance never runs two builds at once); `Result` gains `final Map<String, List<Cid>> providers` as a third constructor argument (`toString` unchanged); constructors

```groovy
    DirectoryManifestBuilder(BlockStore store) {
        this(store, new HeadNodeAddresser(store), null)
    }

    DirectoryManifestBuilder(BlockStore store, FileAddresser addresser, Closure<Path> objectPath) {
        this.store = store
        this.addresser = addresser
        this.objectPath = objectPath
    }
```

replace `build` with

```groovy
    Result build(Path directory) {
        if( !Files.isDirectory(directory) )
            throw new NotDirectoryException(directory.toString())
        tally.clear()
        final int[] unresolvable = new int[1]
        final Cid cid
        if( directory.fileSystem == FileSystems.default ) {
            final Path root = directory.toRealPath()
            cid = walk(root, root, 1, new LinkedHashSet<Path>([root]), unresolvable)
        }
        else {
            final Path root = realOf(directory)
            cid = new ObjectWalk(root, unresolvable).walk(root, 1, new LinkedHashSet<String>([keyOf(root)]))
        }
        final Map<String, List<Cid>> providers = new TreeMap<String, List<Cid>>()
        tally.each { String p, TreeMap<String, Cid> cids -> providers.put(p, Collections.unmodifiableList(new ArrayList<Cid>(cids.values()))) }
        return new Result(cid, Anomalies.unresolvable(unresolvable[0]), Collections.unmodifiableMap(providers))
    }

    /**
     * Every file inside the tree is addressed here, in both walks, so the
     * result can say which provider supplied each address (ticket 16 decision 1:
     * RunCompletion.providers covers every address the run published).
     */
    private Cid addressOf(Path file, long size) {
        final Addressed a = addresser.address(file, size)
        tally.computeIfAbsent(a.provider, { String k -> new TreeMap<String, Cid>() }).put(a.cid.toString(), a.cid)
        return a.cid
    }

    /** toRealPath where the provider has it; an object store has no links to resolve (ticket 05). */
    static Path realOf(Path p) {
        try {
            return p.toRealPath()
        }
        catch( UnsupportedOperationException e ) {
            return p.toAbsolutePath().normalize()
        }
    }

    private static String keyOf(Path p) { realOf(p).toString() }
```

replace `putFile(path)` in `contentEntry` with `addressOf(path, attrs.size())` (and delete `putFile`), change `resolvesInside`'s `link.toRealPath()` to `realOf(link)` and `contentEntry`'s `path.toRealPath()` to `realOf(path)`, then add the object walk as an inner class:

```groovy
    /**
     * The walk of a directory in an object store (ticket 15). Keys are compared
     * as the paths' string forms, which are absolute on every object store
     * provider nf-amazon and the JDK's zip provider have. A directory holding
     * a parseable `.fusion.symlinks` has its listed names decoded as links; the
     * sidecar never enters the manifest. Every file is regular: an object has
     * no execute bit (nf-amazon's checkAccess(EXECUTE) always throws).
     */
    private class ObjectWalk {
        final Path root
        final String rootKey
        final int[] unresolvable
        final Map<String, Set<String>> linksByDir = new HashMap<String, Set<String>>()

        ObjectWalk(Path root, int[] unresolvable) {
            this.root = root
            this.rootKey = keyOf(root)
            this.unresolvable = unresolvable
        }

        Cid walk(Path dir, int depth, LinkedHashSet<String> ancestors) {
            if( depth > MAX_DEPTH )
                throw new IOException("directory tree is deeper than $MAX_DEPTH levels at $dir")
            final Map<String, Path> children = new TreeMap<String, Path>()
            Files.newDirectoryStream(dir).withCloseable { stream ->
                for( Path child : stream ) children.put(child.fileName.toString().replaceAll('/+$', ''), child)
            }
            final Set<String> links = linksIn(dir, children)
            final List<ManifestEntry> entries = new ArrayList<ManifestEntry>()
            for( String name : links )
                if( !children.containsKey(name) )
                    entries.add(unresolvable(name, null))
            for( Map.Entry<String, Path> e : children.entrySet() ) {
                if( links.contains(e.key) )
                    entries.add(linkEntry(dir, e.key, e.value, depth, ancestors))
                else
                    entries.add(content(e.key, e.value, depth, ancestors))
            }
            return store.putDagCbor(new DirectoryManifest(entries).toCbor())
        }

        /** The decoded link names of a directory being walked; a parsed sidecar leaves `children`. */
        private Set<String> linksIn(Path dir, Map<String, Path> children) {
            final Path sidecar = children.get(FusionLinks.SIDECAR)
            if( sidecar == null || Files.isDirectory(sidecar) )
                return remember(dir, Collections.<String> emptySet())
            final FusionLinks.Parsed parsed = FusionLinks.parse(readAtMost(sidecar, FusionLinks.MAX_SIDECAR_BYTES + 1))
            if( !parsed.ok ) {
                unresolvable[0]++
                log.warn("${sidecar}: ${parsed.problem}; the directory is recorded as its objects, links as files")
                return remember(dir, Collections.<String> emptySet())
            }
            children.remove(FusionLinks.SIDECAR)
            return remember(dir, parsed.names)
        }

        private Set<String> remember(Path dir, Set<String> names) {
            linksByDir.put(keyOf(dir), names)
            return names
        }

        /** Whether p is a decoded link, reading its directory's sidecar once. */
        private boolean isLink(Path p) {
            final Path parent = p.parent
            if( parent == null ) return false
            Set<String> names = linksByDir.get(keyOf(parent))
            if( names == null ) {
                final Path sidecar = parent.resolve(FusionLinks.SIDECAR)
                final FusionLinks.Parsed parsed = Files.isRegularFile(sidecar)
                    ? FusionLinks.parse(readAtMost(sidecar, FusionLinks.MAX_SIDECAR_BYTES + 1)) : null
                names = remember(parent, parsed?.ok ? parsed.names : Collections.<String> emptySet())
            }
            return names.contains(p.fileName.toString())
        }

        private ManifestEntry linkEntry(Path dir, String name, Path child, int depth, LinkedHashSet<String> ancestors) {
            final String target = Files.isDirectory(child) ? null : FusionLinks.target(readAtMost(child, FusionLinks.MAX_TARGET_BYTES + 1))
            if( target == null )
                return unresolvable(name, Files.isDirectory(child) ? null : Records.REDACTED_LOCATION)
            final boolean absolute = target.startsWith('/')
            final boolean textInTree = !absolute && within(dir.resolve(target).normalize())
            final Path found = chase(dir, target, new HashSet<String>([keyOf(child)]), 0)
            if( found == null )
                return unresolvable(name, textInTree ? target : Records.REDACTED_LOCATION)
            if( !absolute && within(found) )
                return ManifestEntry.symlink(name, target)
            return followed(name, found, depth, ancestors)
        }

        /** The non-link a target leads to, or null when it is missing, cyclic or not resolvable by key. */
        private Path chase(Path fromDir, String target, Set<String> seen, int hops) {
            if( hops >= MAX_DEPTH ) return null
            final Path p = target.startsWith('/') ? fusionPath(target) : fromDir.resolve(target).normalize()
            if( p == null ) return null
            if( isLink(p) ) {
                if( !seen.add(keyOf(p)) ) return null
                final String next = FusionLinks.target(readAtMost(p, FusionLinks.MAX_TARGET_BYTES + 1))
                return next == null ? null : chase(p.parent, next, seen, hops + 1)
            }
            return Files.exists(p) ? p : null
        }

        /** `/fusion/s3/<bucket>/<key>` is `s3://<bucket>/<key>`; any other absolute target has no key. */
        private Path fusionPath(String target) {
            final java.util.regex.Matcher m = target =~ /^\/fusion\/s3\/([^\/]+)\/(.+)$/
            return m.matches() && objectPath != null ? objectPath.call("s3://${m.group(1)}/${m.group(2)}".toString()) : null
        }

        private boolean within(Path p) {
            final String key = keyOf(p)
            return key == rootKey || key.startsWith(rootKey + '/')
        }

        private ManifestEntry followed(String name, Path p, int depth, LinkedHashSet<String> ancestors) {
            if( Files.isDirectory(p) && ancestors.contains(keyOf(p)) )
                return unresolvable(name, Records.REDACTED_LOCATION)
            return content(name, p, depth, ancestors)
        }

        private ManifestEntry content(String name, Path p, int depth, LinkedHashSet<String> ancestors) {
            final BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes)
            if( attrs.isDirectory() ) {
                final LinkedHashSet<String> deeper = new LinkedHashSet<String>(ancestors)
                deeper.add(keyOf(p))
                return ManifestEntry.directory(name, walk(p, depth + 1, deeper))
            }
            return ManifestEntry.regular(name, addressOf(p, attrs.size()), attrs.size())
        }

        private ManifestEntry unresolvable(String name, String target) {
            unresolvable[0]++
            return ManifestEntry.unresolvable(name, target)
        }
    }

    /** At most max bytes of a small object: a sidecar or a link body, never file content. */
    private static byte[] readAtMost(Path p, int max) {
        Files.newInputStream(p).withCloseable { InputStream in -> in.readNBytes(max) }
    }
```

(`FileSystems` and `BasicFileAttributes` imports as needed.)

- [ ] **Step 4: Run the tests**

Run: `./gradlew test --tests 'robsyme.cas.core.*'`
Expected: PASS, including the unchanged `DirectoryManifestBuilderTest` (the local walk).

Run: `./gradlew test && make gate`
Expected: PASS; lineage 11/0/6 (assertion 5 still sees `alias.txt` as a link locally).

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/FileAddresser.groovy src/main/groovy/robsyme/cas/core/FusionLinks.groovy \
  src/main/groovy/robsyme/cas/core/DirectoryManifestBuilder.groovy \
  src/test/groovy/robsyme/cas/core/FusionLinksTest.groovy src/test/groovy/robsyme/cas/core/ObjectStoreManifestTest.groovy
git commit -m "feat(manifest): walk an object-store directory, decode Fusion's links, stop calling toRealPath on S3

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Config for S3 members anywhere

Ticket 02 decisions 9 and 10, silent decisions 5 and 22, Contradictions 3. `CasConfig` stops refusing a writable S3 member (`CasConfig.groovy:148-149`) and an S3 member in `cas.resolve` (`:192-193`), keeps the S3 URI check, refuses any other `<scheme>://`, passes local locations through `FileHelper.asPath` (`:145` used `Path.of`), and gains `cas.tmpDir`, `cas.nodeHash` and the storage-class judgement. Since the refusal goes, the default `resolve` list is every configured alias again, writable first, S3 members included (DESIGN §2's original rule). This task changes no caller: `locationOf`, `writableLocation`, `remoteLocationOf` and `localLocations()` keep their meaning for local members, and Task 11 moves the callers to the new names. That holds only for all-local configs: from this task until Task 11, the default member list includes S3 aliases that the old `CasSession.buildStore` (`CasSession.groovy:121-127`) builds as `LocalBlockStore(null)`, so `explore` with a private S3 member, and with it `make gate-cloud`, is broken between Tasks 4 and 11. Accepted (pre-flight F27): Task 1's Step 6 runs `gate-cloud` before this task, and it runs next only at acceptance (Task 15); nobody runs it in waves 1-3.

**Files:**
- Create: `src/main/groovy/robsyme/cas/S3Location.groovy`
- Modify: `src/main/groovy/robsyme/cas/CasConfig.groovy`
- Modify: `src/main/groovy/robsyme/cas/CasConfigScope.groovy`
- Modify: `src/test/groovy/robsyme/cas/CasConfigTest.groovy`, `src/test/groovy/robsyme/cas/CasConfigScopeTest.groovy`

**Interfaces:**
- Consumes: `nextflow.file.FileHelper.asPath(String)`.
- Produces: `S3Location` and the `CasConfig` members listed under "Plugin" in the shared interfaces. `localLocations()` stays as a deprecated alias of `locationTexts()` until Task 11 deletes it. `writableLocation` is null when the writable member is remote.

- [ ] **Step 1: Write the failing tests**

Replace the three features "an S3 location is a read-only member that runs leave out and explore serves", "naming an S3 member in cas.resolve is refused for runs" and "the writable member cannot be on S3" with:

```groovy
    def 'S3 members resolve like local ones, writable first; texts name the cache file'() {
        given:
        final Map cfg = [cas: [stores: [lab: [location: 's3://bucket/cas/'], shared: [location: '/mnt/shared'], priv: [location: 's3://other']]]]

        when:
        final CasConfig config = CasConfig.from(cfg, 'cas://lab')

        then:
        config.members == ['lab', 'shared', 'priv']
        config.isRemote('lab') && config.isRemote('priv') && !config.isRemote('shared')
        config.remoteOf('lab').bucket == 'bucket'
        config.remoteOf('lab').prefix == 'cas/'
        config.remoteOf('priv').prefix == ''
        config.writableLocation == null
        config.locationOf('shared') == Path.of('/mnt/shared')
        config.locationTexts() == ['s3://bucket/cas', '/mnt/shared', 's3://other']
        config.remoteLocationOf('lab') == URI.create('s3://bucket/cas')
    }

    def 's3://bkt/p and s3://bkt/p/ are one member and one cache name (Review Focus 5)'() {
        expect:
        CasConfig.from([cas: [stores: [lab: [location: 's3://bkt/p']]]], 'cas://lab').locationTexts() ==
            CasConfig.from([cas: [stores: [lab: [location: 's3://bkt/p/']]]], 'cas://lab').locationTexts()
    }

    def 'a location in another scheme is refused, naming the store'() {
        when:
        CasConfig.from([cas: [stores: [lab: [location: 'gs://bucket/cas']]]], 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('cas.stores.lab.location')
        e.message.contains('gs://bucket/cas')
    }

    def 'archive storage classes are refused for a writable S3 member; infrequent access warns about packing'() {
        when:
        CasConfig.from([aws: [client: [storageClass: cls]], cas: [stores: [lab: [location: 's3://bkt']]]], 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains(cls)
        e.message.contains('packing')

        where:
        cls << ['GLACIER', 'DEEP_ARCHIVE']
    }

    def 'the storage-class warning (#cls)'() {
        expect:
        CasConfig.from([aws: [client: [storageClass: cls]], cas: [stores: [lab: [location: 's3://bkt']]]], 'cas://lab')
            .storageClassWarning?.contains(fragment) ?: fragment == null

        where:
        cls                   | fragment
        'STANDARD_IA'         | 'packing'
        'INTELLIGENT_TIERING' | 'packing'
        'GLACIER_IR'          | 'nf-amazon ignores'
        'STANDARD'            | null
    }

    def 'a local writable member ignores the storage class'() {
        expect:
        CasConfig.from([aws: [client: [storageClass: 'GLACIER']], cas: [stores: [lab: [location: '/data/cas']]]], 'cas://lab').storageClassWarning == null
    }

    def 'cas.tmpDir defaults to java.io.tmpdir; cas.nodeHash defaults to fusion.enabled'() {
        expect:
        CasConfig.from(storeConfig(), 'cas://lab').tmpDir == Path.of(System.getProperty('java.io.tmpdir'))
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas']], tmpDir: '/scratch']], 'cas://lab').tmpDir == Path.of('/scratch')
        !CasConfig.nodeHashEnabled([:])
        CasConfig.nodeHashEnabled([fusion: [enabled: true]])
        !CasConfig.nodeHashEnabled([fusion: [enabled: true], cas: [nodeHash: false]])
        CasConfig.nodeHashEnabled([cas: [nodeHash: true]])
    }

    def 'a cas.nodeHash that is not a boolean is refused'() {
        when:
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas']], nodeHash: 'yes']], 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('cas.nodeHash')
    }
```

In `CasConfigScopeTest`, assert the scope declares `tmpDir` and `nodeHash` (follow the existing feature's reflection pattern).

Run: `./gradlew test --tests 'robsyme.cas.CasConfig*'`
Expected: FAIL (`remoteOf`, `locationTexts`, `tmpDir`, `nodeHashEnabled` unknown; the refusals still fire).

- [ ] **Step 2: Implement**

```groovy
// src/main/groovy/robsyme/cas/S3Location.groovy
package robsyme.cas

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

/** An S3 member's location: a bucket and a key prefix that is empty or ends in '/' (DESIGN.md §2). */
@CompileStatic
@EqualsAndHashCode
class S3Location {
    final String bucket
    final String prefix

    private S3Location(String bucket, String prefix) { this.bucket = bucket; this.prefix = prefix }

    /** From an already validated s3://<bucket>[/<prefix>] (CasConfig.S3_LOCATION); a trailing slash is dropped. */
    static S3Location parse(String uri) {
        final String rest = uri.substring('s3://'.length())
        final int slash = rest.indexOf('/')
        if( slash < 0 )
            return new S3Location(rest, '')
        final String path = rest.substring(slash + 1).replaceAll('/+$', '')
        return new S3Location(rest.substring(0, slash), path ? path + '/' : '')
    }

    @Override
    String toString() { prefix ? "s3://${bucket}/${prefix[0..-2]}" : "s3://${bucket}" }
}
```

In `CasConfig`:

- `remotes` becomes `Map<String, S3Location>`; `remoteLocationOf(alias)` returns `URI.create(remotes.get(alias).toString())`; add `S3Location remoteOf(String alias)`.
- In `from`, for a location starting `s3://`: after the pattern check, `remotes.put(name, S3Location.parse(location))`. Otherwise, refuse `~/^[A-Za-z][A-Za-z0-9+.-]*:\/\//` with `"cas.stores.${name}.location must be a local directory or s3://<bucket>[/<prefix>] -- offending value: ${location}"`, and store `FileHelper.asPath(location).toAbsolutePath().normalize()`.
- Delete the writable-remote refusal; the missing-store check becomes `!locations.containsKey(alias) && !remotes.containsKey(alias)`.
- `memberList(alias, scope.get('resolve'), locations.keySet() + remotes.keySet())`: delete the S3 refusal loop and the `remotes` parameter.
- New fields, set in `from`: `final Map rawConfig` (the session config map as given), `final Path tmpDir` (`scope.tmpDir` through `FileHelper.asPath`, else `Path.of(System.getProperty('java.io.tmpdir'))`), `final Boolean nodeHashSetting` (refusing anything but null, `true`, `false`, naming `cas.nodeHash`), `final String storageClassWarning` (from `judgeStorageClass(sessionConfig, remotes.containsKey(alias))`).
- New methods:

```groovy
    String locationText(String alias) {
        return remotes.containsKey(alias) ? remotes.get(alias).toString() : locations.get(alias)?.toString()
    }

    /** Every resolvable member's location text, writable first: what IndexPaths names the cache file by. */
    List<String> locationTexts() { members.collect { String a -> locationText(a) } }

    /** Deprecated: Task 11 moves its one caller to locationTexts(). */
    List<String> localLocations() { locationTexts() }

    /** The member's location as a Path through FileHelper.asPath; an S3 one needs nf-amazon started. */
    Path pathOf(String alias) {
        return remotes.containsKey(alias) ? FileHelper.asPath(remotes.get(alias).toString()) : locations.get(alias)
    }

    /** Node-side hashing (DESIGN.md §11): cas.nodeHash when set, else fusion.enabled. */
    static boolean nodeHashEnabled(Map sessionConfig) {
        final Object cas = sessionConfig?.get(SCHEME)
        final Object explicit = cas instanceof Map ? ((Map) cas).get('nodeHash') : null
        if( explicit instanceof Boolean )
            return (Boolean) explicit
        final Object fusion = sessionConfig?.get('fusion')
        return fusion instanceof Map && ((Map) fusion).get('enabled') == Boolean.TRUE
    }

    private static final Set<String> ARCHIVE = ['GLACIER', 'DEEP_ARCHIVE'] as Set
    private static final Set<String> INFREQUENT = ['STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING'] as Set
    // nf-amazon 3.9.2 AwsS3Config.parseStorageClass keeps only these (and REDUCED_REDUNDANCY, STANDARD).
    private static final Set<String> NF_AMAZON_ACCEPTS = ['STANDARD', 'STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING', 'REDUCED_REDUNDANCY'] as Set

    /**
     * Ticket 02 decision 9: blocks inherit aws.client.storageClass. One object
     * per block pays each class's per-object minimum, which packing (spec §3)
     * exists to avoid, so archive classes are refused and infrequent-access
     * ones warn. Judged only when the writable member is on S3.
     */
    private static String judgeStorageClass(Map sessionConfig, boolean writableIsRemote) {
        if( !writableIsRemote ) return null
        final Object aws = sessionConfig?.get('aws')
        final Object client = aws instanceof Map ? ((Map) aws).get('client') : null
        final String cls = client instanceof Map ? (((Map) client).get('storageClass') ?: ((Map) client).get('uploadStorageClass')) as String : null
        if( !cls ) return null
        if( ARCHIVE.contains(cls) )
            throw new IllegalArgumentException("aws.client.storageClass = '${cls}' would archive every block of the writable S3 member; blocks must stay readable, and archive tiers need packing (spec §3), which nf-blocks does not do yet")
        if( INFREQUENT.contains(cls) )
            return "aws.client.storageClass = '${cls}': every block is its own object, so each pays that class's per-object minimum; packing (spec §3), which would avoid it, is not built yet".toString()
        if( !NF_AMAZON_ACCEPTS.contains(cls) )
            return "aws.client.storageClass = '${cls}': nf-amazon ignores this class (AwsS3Config.parseStorageClass), so blocks are written as STANDARD".toString()
        return null
    }
```

Update the class comment and the `members` field comment ("Every resolvable member alias, the writable one first, local or S3"). In `CasConfigScope`, change `CasStoreScope.location`'s description to `'Where this member lives: a local directory or s3://<bucket>[/<prefix>].'` and add to `CasConfigScope`:

```groovy
    @ConfigOption
    @Description('Scratch directory for content of unknown length on its way to an S3 member. Defaults to java.io.tmpdir.')
    String tmpDir

    @ConfigOption
    @Description('Hash declared outputs on the task node and publish their addresses from .command.cas. Defaults to fusion.enabled.')
    Boolean nodeHash
```

- [ ] **Step 3: Run the tests**

Run: `./gradlew test`
Expected: PASS. `CasSession`, `CasLinStore` and `ExploreCommand` compile unchanged; with only local members, every existing test sees what it saw.

- [ ] **Step 4: Commit**

```bash
git add src/main/groovy/robsyme/cas/S3Location.groovy src/main/groovy/robsyme/cas/CasConfig.groovy \
  src/main/groovy/robsyme/cas/CasConfigScope.groovy src/test/groovy/robsyme/cas/CasConfigTest.groovy \
  src/test/groovy/robsyme/cas/CasConfigScopeTest.groovy
git commit -m "feat(config): S3 members anywhere, cas.tmpDir, cas.nodeHash, storage-class judgement

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: The S3 block store and Store Log

Ticket 02 decisions 2-5, ticket 03 decision 5, ticket 14, silent decisions 12-15. `S3BlockStore` is the local store's contract (DESIGN §5) on `S3Ops`: presence is a `HEAD`, placement a conditional write, a 412 success, a 409 retried. A known-length body up to the single-request limit is one `PutObject` with SHA-256, whose returned `ChecksumSHA256` must equal the CID digest; above it, a multipart upload from file-channel ranges. The memory bound is kept by construction: every body is an `S3Body` over a file range or over encoded metadata bytes, never file content in heap. The Store Log's entries become empty objects under `log/`. `StoreLog.storageOf` stops asking `instanceof LocalBlockStore` and asks the store (`LoggedStore`).

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/LoggedStore.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/LocalBlockStore.groovy` (implements `LoggedStore`)
- Modify: `src/main/groovy/robsyme/cas/core/StoreLog.groovy` (`storageOf`)
- Create: `src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy`
- Create: `src/main/groovy/robsyme/cas/s3/S3StoreLogStorage.groovy`
- Modify: `build.gradle` (`test` excludes, `memoryBoundTest` includes `robsyme.cas.s3.S3MemoryBoundTest`)
- Create: `src/test/groovy/robsyme/cas/s3/S3BlockStoreTest.groovy`, `src/test/groovy/robsyme/cas/s3/S3StoreLogStorageTest.groovy`, `src/test/groovy/robsyme/cas/s3/S3MemoryBoundTest.groovy`

**Interfaces:**
- Consumes: `S3Ops`, `S3Body`, `S3PutOptions`, `S3Written`, `MemoryS3Ops` (Task 1) (`S3WriteOptions` is applied inside `SdkS3Ops`, so the store never sees it); `HashBufferPool`, `Cid`, `DagCbor`, `BlockMismatchException`, `NoSuchBlockException`.
- Produces: `LoggedStore`; `S3BlockStore` and `S3StoreLogStorage` as in the shared interfaces, plus `S3BlockStore.SINGLE_REQUEST_MAX = 5L << 30`, `HEAD_FIRST_BYTES = 1L << 20`, `MIN_PART = 64L << 20`, `MAX_PARTS = 10_000`, `IMMUTABLE = 'public, max-age=31536000, immutable'`, `static long partSize(long size)`.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/s3/S3BlockStoreTest.groovy
package robsyme.cas.s3

import java.nio.file.Files
import java.nio.file.Path

import robsyme.cas.core.BlockMismatchException
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Hashing
import robsyme.cas.core.NoSuchBlockException
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

class S3BlockStoreTest extends Specification {

    @TempDir Path tmp
    MemoryS3Ops s3 = new MemoryS3Ops('member')

    private S3BlockStore store(String prefix = 'cas/', long limit = S3BlockStore.SINGLE_REQUEST_MAX) {
        new S3BlockStore(s3, prefix, 'lab', true, tmp, limit)
    }

    private Path file(String name, byte[] bytes) { Files.write(tmp.resolve(name), bytes) }

    private static byte[] bytes(int n) { (0..<n).collect { (byte) (it * 31) } as byte[] }

    def 'the layout is the local one under the prefix; a bucket-root member has no leading slash (Review Focus 5)'() {
        given:
        final Cid cid = Hashing.hashRaw(new ByteArrayInputStream('hello\n'.bytes), new byte[1024])

        expect:
        store('cas/').key(cid) == "cas/blocks/${cid.toString()[-2..-1]}/${cid}"
        store('').key(cid) == "blocks/${cid.toString()[-2..-1]}/${cid}"
    }

    def 'putStreaming spools, uploads once with SHA-256 and immutable caching; under 1 MiB a second put is a conditional PUT answered 412'() {
        given:
        final S3BlockStore b = store()

        when:
        final Cid cid = b.putStreaming(new ByteArrayInputStream('hello\n'.bytes))
        final Cid again = b.putStreaming(new ByteArrayInputStream('hello\n'.bytes))

        then:
        cid.toString() == 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'
        again == cid
        s3.objects[b.key(cid)].text() == 'hello\n'
        s3.objects[b.key(cid)].cacheControl == S3BlockStore.IMMUTABLE
        s3.calls.count { it.startsWith('PUT ') } == 2          // under 1 MiB: conditional PUT alone (ticket 02 decision 2)
        b.has(cid) && b.size(cid) == 6L
        b.open(cid).text == 'hello\n'
        Files.list(tmp).count() == 0                            // the spool file is gone
    }

    def 'a block of 1 MiB or more is looked for before any body is sent (ticket 14 item 1)'() {
        given:
        final S3BlockStore b = store()
        final Path f = file('big', bytes(2 << 20))
        final Cid cid = b.putFile(f)
        final long pulled = s3.pulledBytes

        when:
        b.putFile(f)

        then:
        s3.pulledBytes == pulled
        s3.calls.last() == "HEAD ${b.key(cid)}".toString()
    }

    def 'a write that meets a 409 is tried up to three times in all'() {
        given:
        s3.conflicts['PUT'] = 2

        expect: 'two 409s, then the third try writes'
        store().putDagCbor([kind: 'x']) == DagCbor.cidOf(DagCbor.encode([kind: 'x']))

        when: 'three 409s use up the three tries'
        s3.conflicts['PUT'] = 3
        store().putDagCbor([kind: 'y'])

        then:
        thrown(IOException)
    }

    def 'put verifies the announced address and size before anything is uploaded'() {
        when:
        store().put(Cid.of(Cid.RAW, new byte[32]), new ByteArrayInputStream('x'.bytes), 1L)

        then:
        thrown(BlockMismatchException)
        !s3.calls.any { it.startsWith('PUT ') }
    }

    def 'a returned ChecksumSHA256 that differs from the address deletes the object and fails (silent decision 13)'() {
        given:
        final MemoryS3Ops lying = new MemoryS3Ops('member') {
            @Override S3Written put(String key, S3Body body, S3PutOptions o) {
                final S3Written w = super.put(key, body, o)
                return new S3Written(w.status, w.etag, Base64.encoder.encodeToString(new byte[32]))
            }
        }
        final S3BlockStore b = new S3BlockStore(lying, 'cas/', 'lab', true, tmp)

        when:
        b.putFile(file('a', 'abc'.bytes))

        then:
        thrown(BlockMismatchException)
        lying.objects.isEmpty()
    }

    def 'above the single-request limit: multipart from file ranges, each part hashed, completed conditionally'() {
        given:
        final S3BlockStore b = store('cas/', 1L << 20)
        final byte[] content = bytes(3 << 20)

        when:
        final Cid cid = b.putFile(file('large', content))

        then:
        s3.objects[b.key(cid)].bytes == content
        s3.calls.count { it.startsWith('PART ') } == 1          // max(64 MiB, ceil(3 MiB / 10000)): one part
        s3.calls.count { it.startsWith('COMPLETE ') } == 1
        S3BlockStore.partSize(1L << 40) == (long) Math.ceil((1L << 40) / 10000.0d)
        S3BlockStore.partSize(100L << 30) == S3BlockStore.MIN_PART
    }

    def 'a multipart upload that loses the race at Complete is aborted and counts as written'() {
        given:
        final byte[] content = bytes(2 << 20)
        final Cid cid = Hashing.hashRaw(new ByteArrayInputStream(content), new byte[1 << 20])
        final MemoryS3Ops racing = new MemoryS3Ops('member') {
            @Override String createMultipart(String key, S3PutOptions o) {
                putText(key, 'another writer, same address')   // lands after our HEAD
                return super.createMultipart(key, o)
            }
        }
        final S3BlockStore b = new S3BlockStore(racing, 'cas/', 'lab', true, tmp, 1L << 20)

        when:
        b.putFile(file('large', content))

        then:
        noExceptionThrown()
        racing.calls.count { it.startsWith('ABORT ') } == 1
        racing.uploads.isEmpty()
        racing.objects[b.key(cid)].text() == 'another writer, same address'
    }

    def 'a read-only member refuses writes; an absent block is NoSuchBlockException'() {
        given:
        final S3BlockStore ro = new S3BlockStore(s3, 'cas/', 'shared', false, tmp)

        when:
        ro.putDagCbor([kind: 'x'])
        then:
        thrown(IllegalStateException)

        when:
        ro.size(Cid.of(Cid.RAW, new byte[32]))
        then:
        thrown(NoSuchBlockException)
    }

    def 'listBlocks lists blocks/ only; the Store Log is empty objects under log/'() {
        given:
        final S3BlockStore b = store()
        final Cid a = b.putDagCbor([kind: 'a'])
        s3.putText('cas/tmp/stray', 'x')

        when:
        StoreLog.append(b, StoreLogKind.RUN, a, 1_000L)
        StoreLog.append(b, StoreLogKind.RUN, a, 1_000L)

        then:
        b.listBlocks().toList() == [a]
        StoreLog.read(b)*.cid == [a]
        s3.list('cas/log/', 0).size() == 1
    }
}
```

`S3StoreLogStorageTest` pins `putEntry` as a conditional empty PUT (412 is success), tried up to `S3BlockStore.ATTEMPTS` times on a 409 (`conflicts['PUT'] = 2` writes, `= 3` throws `IOException`), and `listEntries` as the names under `<prefix>log/` without the prefix, ignoring keys with a further `/`. `S3MemoryBoundTest` (run only by `memoryBoundTest`, heap 48 MiB):

```groovy
// src/test/groovy/robsyme/cas/s3/S3MemoryBoundTest.groovy
package robsyme.cas.s3

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §0 rule 2 on S3: a 256 MiB file through spool, hash and multipart, under a 48 MiB heap. */
class S3MemoryBoundTest extends Specification {

    @TempDir Path tmp

    def 'putStreaming a 256 MiB stream to S3 holds no file content in heap'() {
        given:
        final MemoryS3Ops s3 = new MemoryS3Ops('member')
        s3.discard = true
        // 64 MiB parts: the lowered limit forces the multipart path.
        final S3BlockStore store = new S3BlockStore(s3, 'cas/', 'lab', true, tmp, 64L << 20)
        final InputStream zeros = new InputStream() {
            long left = 256L << 20
            @Override int read() { left-- > 0 ? 0 : -1 }
            @Override int read(byte[] b, int off, int len) {
                if( left <= 0 ) return -1
                final int n = (int) Math.min(len, left); Arrays.fill(b, off, off + n, (byte) 0); left -= n; n
            }
        }

        when:
        store.putStreaming(zeros)

        then:
        s3.pulledBytes == 256L << 20
        s3.calls.count { it.startsWith('PART ') } == 4
        Runtime.runtime.maxMemory() < (256L << 20)
    }
}
```

In `build.gradle`: `test { filter { excludeTestsMatching 'robsyme.cas.core.MemoryBoundTest'; excludeTestsMatching 'robsyme.cas.s3.S3MemoryBoundTest' } }` and add `includeTestsMatching 'robsyme.cas.s3.S3MemoryBoundTest'` to `memoryBoundTest`'s filter.

Run: `./gradlew test --tests 'robsyme.cas.s3.*'`
Expected: compilation FAILS on `S3BlockStore`.

- [ ] **Step 2: Implement**

```groovy
// src/main/groovy/robsyme/cas/core/LoggedStore.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/** A block store that knows where its member's Store Log lives (DESIGN.md §5). */
@CompileStatic
interface LoggedStore {
    StoreLogStorage storeLogStorage()
}
```

`LocalBlockStore implements BlockStore, LoggedStore` with `StoreLogStorage storeLogStorage() { new LocalStoreLogStorage(root) }`. `StoreLog.storageOf` becomes:

```groovy
    private static StoreLogStorage storageOf(BlockStore store) {
        if( store instanceof LoggedStore )
            return ((LoggedStore) store).storeLogStorage()
        if( store instanceof CompositeStore )
            return storageOf(((CompositeStore) store).getMembers()[0])
        throw new IllegalArgumentException("no store log storage for ${store?.getClass()?.name}")
    }
```

```groovy
// src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy
package robsyme.cas.s3

import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.stream.Stream

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import robsyme.cas.core.*

/**
 * A member's blocks in S3 (DESIGN.md §5, ticket 02): blocks/<xx>/<cid> under
 * the member prefix. "Already exists" is success (rule 4): a HEAD first at
 * 1 MiB and up, because S3 reads a whole body before a 412 (ticket 14), and
 * If-None-Match on every write for the race. Immutability is those
 * conditional writes; nothing is ever replaced, so LastModified is stable.
 */
@Slf4j
@CompileStatic
class S3BlockStore implements BlockStore, LoggedStore {

    static final long SINGLE_REQUEST_MAX = 5L << 30
    static final long HEAD_FIRST_BYTES = 1L << 20
    static final long MIN_PART = 64L << 20
    static final int MAX_PARTS = 10_000
    static final int ATTEMPTS = 3
    static final String IMMUTABLE = 'public, max-age=31536000, immutable'

    final S3Ops ops
    final String prefix
    private final String alias
    private final boolean writable
    private final Path tmpDir
    private final long singleRequestMax

    /** Storage class, SSE and requester pays are the S3Ops' own (SdkS3Ops applies them to every write). */
    S3BlockStore(S3Ops ops, String prefix, String alias, boolean writable, Path tmpDir,
                 long singleRequestMax = SINGLE_REQUEST_MAX) {
        this.ops = ops; this.prefix = prefix ?: ''; this.alias = alias; this.writable = writable
        this.tmpDir = tmpDir; this.singleRequestMax = singleRequestMax
    }

    static long partSize(long size) { Math.max(MIN_PART, (long) Math.ceil(size / (double) MAX_PARTS)) }

    String key(Cid cid) {
        final String text = cid.toString()
        return "${prefix}blocks/${text.substring(text.length() - 2)}/${text}".toString()
    }

    @Override String alias() { alias }
    @Override boolean isWritable() { writable }
    @Override StoreLogStorage storeLogStorage() { new S3StoreLogStorage(ops, prefix) }

    @Override boolean has(Cid cid) { ops.head(key(cid)) != null }

    private S3Head headOrThrow(Cid cid) {
        final S3Head h = ops.head(key(cid))
        if( h == null ) throw new NoSuchBlockException(cid, alias)
        return h
    }

    @Override long size(Cid cid) { headOrThrow(cid).size }
    @Override long lastModifiedMillis(Cid cid) { headOrThrow(cid).lastModifiedMillis }

    @Override
    InputStream open(Cid cid) {
        final InputStream in = ops.get(key(cid), null, 0L, -1L)
        if( in == null ) throw new NoSuchBlockException(cid, alias)
        return in
    }

    /** Bytes already said to hash to cid: spooled and checked first (rule 2), then placed. */
    @Override
    void put(Cid cid, InputStream input, long expectedSize) {
        checkWritable()
        withSpool(input) { Path spool, Cid actual, long written ->
            if( !Arrays.equals(actual.digest, cid.digest) )
                throw new BlockMismatchException(cid, "the bytes hash to ${Cid.of(cid.codec, actual.digest)}")
            if( expectedSize >= 0 && written != expectedSize )
                throw new BlockMismatchException(cid, "$written bytes arrived, not the announced $expectedSize")
            place(cid, S3Body.ofFile(spool, 0L, written))
            return cid
        }
    }

    @Override
    Cid putStreaming(InputStream input) {
        checkWritable()
        return withSpool(input) { Path spool, Cid cid, long written ->
            place(cid, S3Body.ofFile(spool, 0L, written))
            return cid
        }
    }

    /** A file on the default filesystem: hashed where it is, uploaded from it, no spool (silent decision 13). */
    Cid putFile(Path file) {
        checkWritable()
        final Cid cid = Hashing.hashRaw(file)
        place(cid, S3Body.ofFile(file, 0L, Files.size(file)))
        return cid
    }

    @Override
    Cid putDagCbor(Object value) {
        checkWritable()
        final byte[] encoded = DagCbor.encode(value)
        final Cid cid = DagCbor.cidOf(encoded)
        place(cid, S3Body.ofBytes(encoded))
        return cid
    }

    @Override
    Stream<Cid> listBlocks() {
        final String under = "${prefix}blocks/".toString()
        return ops.list(under, 0).stream()
            .map { S3Listed o -> o.key.substring(o.key.lastIndexOf('/') + 1) }
            .filter { String name -> Cid.isCid(name) }
            .map { String name -> Cid.parse(name) }
    }

    /**
     * Hashes input into a file in tmpDir through one pool buffer, hands it to body, deletes it.
     * Only a failure to create or write the spool names cas.tmpDir; what body throws
     * (a BlockMismatchException, a 409 IOException) propagates unchanged.
     */
    private <T> T withSpool(InputStream input, Closure<T> body) {
        Path spool = null
        try {
            final long[] written = new long[1]
            final byte[] digest
            try {
                Files.createDirectories(tmpDir)
                spool = Files.createTempFile(tmpDir, 'nf-blocks-', '.spool')
                final Path into = spool
                digest = HashBufferPool.shared().withBuffer { byte[] buffer ->
                    final MessageDigest md = MessageDigest.getInstance('SHA-256')
                    FileChannel.open(into, StandardOpenOption.WRITE).withCloseable { FileChannel ch ->
                        final java.nio.ByteBuffer view = java.nio.ByteBuffer.wrap(buffer)
                        int n
                        while( (n = input.read(buffer, 0, buffer.length)) != -1 ) {
                            if( n == 0 ) continue
                            md.update(buffer, 0, n)
                            view.limit(n).position(0)
                            while( view.hasRemaining() ) ch.write(view)
                            written[0] += n
                        }
                    }
                    return md.digest()
                } as byte[]
            }
            catch( IOException e ) {
                throw new IOException("could not spool to cas.tmpDir (${tmpDir}): ${e.message}", e)
            }
            return body.call(spool, Cid.of(Cid.RAW, digest), written[0])
        }
        finally {
            if( spool != null ) Files.deleteIfExists(spool)
        }
    }

    /** Puts the body at the block's key unless it is there; a 412 is success, a 409 is retried. */
    private void place(Cid cid, S3Body body) {
        final String key = key(cid)
        if( body.length >= HEAD_FIRST_BYTES && ops.head(key) != null )
            return
        for( int attempt = 1; attempt <= ATTEMPTS; attempt++ ) {
            final S3Written w = body.length <= singleRequestMax ? single(key, body) : multipart(key, body)
            if( w.status == S3Written.Status.EXISTS )
                return
            if( w.status == S3Written.Status.WRITTEN ) {
                checkDigest(cid, key, w.sha256)
                return
            }
            log.debug("409 ConditionalRequestConflict writing ${key}; attempt ${attempt} of ${ATTEMPTS}")
        }
        throw new IOException("S3 answered 409 ConditionalRequestConflict ${ATTEMPTS} times writing ${ops.describe()}/${key}")
    }

    private S3Written single(String key, S3Body body) {
        return ops.put(key, body, S3PutOptions.create().ifNoneMatch().sha256().cacheControl(IMMUTABLE))
    }

    private S3Written multipart(String key, S3Body file) {
        // file is an S3Body.ofFile over the spool or the source; parts are ranges of it.
        final String id = ops.createMultipart(key, S3PutOptions.create().sha256().cacheControl(IMMUTABLE))
        try {
            final long part = partSize(file.length)
            final List<S3Part> parts = new ArrayList<S3Part>()
            long offset = 0
            for( int n = 1; offset < file.length; n++ ) {
                final long length = Math.min(part, file.length - offset)
                parts.add(ops.uploadPart(key, id, n, ranged(file, offset, length)))
                offset += length
            }
            final S3Written done = ops.completeMultipart(key, id, parts, true)
            if( done.status != S3Written.Status.WRITTEN )
                ops.abortMultipart(key, id)
            return done
        }
        catch( Exception e ) {
            ops.abortMultipart(key, id)
            throw e
        }
    }

    /** A range of a file body, reopened per attempt so a retried part is re-read, never buffered. */
    private static S3Body ranged(S3Body whole, long offset, long length) {
        return new S3Body() {
            @Override long getLength() { length }
            @Override InputStream open() {
                final InputStream in = whole.open()
                in.skipNBytes(offset)
                return new S3Body.BoundedInputStream(in, length)
            }
        }
    }

    /** S3 validated and stored a SHA-256 of what it received; it must be the address (silent decision 13). */
    private void checkDigest(Cid cid, String key, String sha256) {
        if( sha256 == null ) return          // multipart: composite, nothing to compare
        if( Base64.decoder.decode(sha256) != cid.digest ) {
            ops.delete(key)
            throw new BlockMismatchException(cid, "S3 stored bytes whose SHA-256 is ${sha256}; the source changed while it was read")
        }
    }

    private void checkWritable() {
        if( !writable ) throw new IllegalStateException("store '$alias' is read-only")
    }

    @Override String toString() { "S3BlockStore[$alias at ${ops.describe()}/${prefix}]" }
}
```

(`ranged` reopens and skips; since every multipart body is a file, passing the `Path` into `multipart` and using `S3Body.ofFile(path, offset, length)` per part is equivalent and avoids the skip. Either is fine.)

```groovy
// src/main/groovy/robsyme/cas/s3/S3StoreLogStorage.groovy
package robsyme.cas.s3

import groovy.transform.CompileStatic
import robsyme.cas.core.StoreLogStorage

/** log/<rts>-<kind>-<cid> as empty objects; ListObjectsV2 is lexicographic, so newest first holds (DESIGN.md §5). */
@CompileStatic
class S3StoreLogStorage implements StoreLogStorage {
    private final S3Ops ops
    private final String under

    S3StoreLogStorage(S3Ops ops, String prefix) { this.ops = ops; this.under = "${prefix ?: ''}log/".toString() }

    /**
     * Write-once: If-None-Match, and a 412 is the same entry already there. A 409 is
     * tried up to S3BlockStore.ATTEMPTS times, as blocks are, then thrown; the
     * observer's appendStoreLog warns and continues (rule 3).
     */
    @Override
    void putEntry(String name) {
        for( int attempt = 1; attempt <= S3BlockStore.ATTEMPTS; attempt++ ) {
            final S3Written w = ops.put(under + name, S3Body.ofBytes(new byte[0]), S3PutOptions.create().ifNoneMatch())
            if( w.status != S3Written.Status.CONFLICT )
                return
        }
        throw new IOException("S3 answered 409 ConditionalRequestConflict ${S3BlockStore.ATTEMPTS} times writing ${ops.describe()}/${under}${name}")
    }

    @Override
    List<String> listEntries() {
        final List<String> names = []
        for( S3Listed o : ops.list(under, 0) ) {
            final String name = o.key.substring(under.length())
            if( name && !name.contains('/') ) names.add(name)
        }
        return names
    }
}
```

- [ ] **Step 3: Run the tests**

Run: `./gradlew test memoryBoundTest dependencyCheck`
Expected: PASS; `S3MemoryBoundTest` passes under `-Xmx48m`.

- [ ] **Step 4: Commit**

```bash
git add build.gradle src/main/groovy/robsyme/cas/core/LoggedStore.groovy src/main/groovy/robsyme/cas/core/LocalBlockStore.groovy \
  src/main/groovy/robsyme/cas/core/StoreLog.groovy src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy \
  src/main/groovy/robsyme/cas/s3/S3StoreLogStorage.groovy src/test/groovy/robsyme/cas/s3/S3BlockStoreTest.groovy \
  src/test/groovy/robsyme/cas/s3/S3StoreLogStorageTest.groovy src/test/groovy/robsyme/cas/s3/S3MemoryBoundTest.groovy
git commit -m "feat(s3): S3 block store and Store Log: HEAD first, conditional writes, streamed multipart

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: Coordinates behind an interface; coordinates on S3

Ticket 02 decision 6, ticket 03 decisions 3-4, silent decisions 20-21. `CoordinateTree` becomes an interface; the class it was is renamed `LocalCoordinateTree`; `S3CoordinateTree` puts Pointer Files at `coords/<rel>` with no directory markers. S3 has no directories, so both local conflicts (a pointer at `a` blocks `a/b`; a non-empty `a/` blocks a pointer at `a`) are checked explicitly. One abstract Spock specification, `CoordinateTreeContract`, runs against both, so the outcomes are pinned equal. The provider stops reaching into the local tree (`pointerPath`, `Files.isDirectory`, `Files.getLastModifiedTime`) and uses interface calls, which is the only change to `CasFileSystemProvider` here.

Local outcomes the contract pins (measured on the JDK's default provider): writing `a/b` over a pointer at `a` throws `FileAlreadyExistsException` from `Files.createDirectories`; writing `a` over a non-empty directory `a/` throws `DirectoryNotEmptyException` from `Files.move(..., REPLACE_EXISTING)`; writing `a` over an empty directory `a/` replaces it.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/CoordinateTree.groovy` (now the interface)
- Create: `src/main/groovy/robsyme/cas/core/LocalCoordinateTree.groovy` (the old class body)
- Create: `src/main/groovy/robsyme/cas/s3/S3CoordinateTree.groovy`
- Modify: `src/main/groovy/robsyme/cas/nio/CasFileSystemProvider.groovy` (`coordsFor`, `resolveCoordinate`, `createDirectory`)
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (the constructor's `new CoordinateTree(...)`; add `coordinatesOf`)
- Rename: `src/test/groovy/robsyme/cas/core/CoordinateTreeTest.groovy` to `LocalCoordinateTreeTest.groovy`
- Create: `src/test/groovy/robsyme/cas/core/CoordinateTreeContract.groovy`, `src/test/groovy/robsyme/cas/core/LocalCoordinateTreeContractTest.groovy`, `src/test/groovy/robsyme/cas/s3/S3CoordinateTreeTest.groovy`
- Modify: every test that constructs `new CoordinateTree(`: `new LocalCoordinateTree(`. At `8dc1acb` these are `src/test/groovy/robsyme/cas/nio/CasOccurrenceTest.groovy` and `src/test/groovy/robsyme/cas/nio/CasFileSystemProviderTest.groovy` (confirm with `grep -rln 'new CoordinateTree(' src/test`; add any other it finds to the `git add` below)

**Interfaces:**
- Consumes: `S3Ops`, `MemoryS3Ops` (Task 1); `StoreRef`.
- Produces: `CoordinateTree`, `LocalCoordinateTree`, `S3CoordinateTree` (shared interfaces); `CasSession.coordinatesOf(String alias)`: the writable member's tree for its alias, else a `LocalCoordinateTree` at `<location>/coords` (Task 11 adds S3 members), `IllegalArgumentException` for an unknown alias (the message `coordsFor` has today).

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/CoordinateTreeContract.groovy
package robsyme.cas.core

import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException

import spock.lang.Specification

/** What every CoordinateTree does, so the S3 member and a local one cannot drift (ticket 02 decision 6). */
abstract class CoordinateTreeContract extends Specification {

    abstract CoordinateTree tree()

    static final StoreRef FILE = StoreRef.parse('cas://bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am/A.bam')
    static final StoreRef DIR = StoreRef.parse('cas://bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua/A_qc')

    def 'a written pointer reads back; the last write to one coordinate wins (ticket 03 decision 3)'() {
        given:
        final CoordinateTree t = tree()

        when:
        t.write('aligned/A/A.bam', DIR)
        t.write('aligned/A/A.bam', FILE)

        then:
        t.read('aligned/A/A.bam') == Optional.of(FILE)
        t.exists('aligned/A/A.bam') && t.exists('aligned/A') && t.exists('aligned')
        t.isDirectory('aligned/A') && !t.isDirectory('aligned/A/A.bam')
        t.children('aligned') == ['A']
        t.children('aligned/A') == ['A.bam']
        !t.exists('nope') && t.read('nope') == Optional.empty()
    }

    def 'a directory coordinate is a pointer at a manifest'() {
        given:
        final CoordinateTree t = tree()
        t.write('qc/A/A_qc', DIR)

        expect:
        t.isDirectoryCoordinate('qc/A/A_qc')
        t.isDirectoryCoordinate('qc/A')
        !t.isDirectoryCoordinate('qc/B')
    }

    def 'a pointer at a blocks a pointer at a/b'() {
        given:
        final CoordinateTree t = tree()
        t.write('a', FILE)

        when:
        t.write('a/b', FILE)

        then:
        thrown(FileAlreadyExistsException)
        t.read('a') == Optional.of(FILE)
    }

    def 'a non-empty a/ blocks a pointer at a'() {
        given:
        final CoordinateTree t = tree()
        t.write('a/b', FILE)

        when:
        t.write('a', FILE)

        then:
        thrown(DirectoryNotEmptyException)
        t.read('a/b') == Optional.of(FILE)
    }

    def 'delete removes a pointer only, and refuses a directory'() {
        given:
        final CoordinateTree t = tree()
        t.write('d/x', FILE)

        expect:
        !t.delete('d/y')
        t.delete('d/x')
        !t.exists('d/x')

        when:
        t.write('e/f', FILE)
        t.delete('e')

        then:
        thrown(IOException)
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/core/LocalCoordinateTreeContractTest.groovy
package robsyme.cas.core

import java.nio.file.Path
import spock.lang.TempDir

class LocalCoordinateTreeContractTest extends CoordinateTreeContract {
    @TempDir Path dir
    @Override CoordinateTree tree() { new LocalCoordinateTree(dir.resolve('coords')) }
}
```

```groovy
// src/test/groovy/robsyme/cas/s3/S3CoordinateTreeTest.groovy
package robsyme.cas.s3

import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.CoordinateTreeContract
import robsyme.cas.core.StoreRef

class S3CoordinateTreeTest extends CoordinateTreeContract {

    MemoryS3Ops s3 = new MemoryS3Ops('member')

    @Override CoordinateTree tree() { new S3CoordinateTree(s3, 'cas/') }

    def 'pointers are cas/coords/<rel> with the local body and no directory markers'() {
        when:
        tree().write('aligned/A/A.bam', FILE)

        then:
        s3.objects.keySet() == ['cas/coords/aligned/A/A.bam'] as Set
        s3.objects['cas/coords/aligned/A/A.bam'].text() == FILE.toString() + '\n'
    }

    def 'a pointer shadowed by an ancestor pointer (written past the check) reads as absent and is reported (ticket 03 decision 4)'() {
        given:
        s3.putText('cas/coords/a', FILE.toString() + '\n')
        s3.putText('cas/coords/a/b', FILE.toString() + '\n')
        s3.putText('cas/coords/a/c/d', FILE.toString() + '\n')
        final S3CoordinateTree t = new S3CoordinateTree(s3, 'cas/')

        expect:
        t.read('a/b') == Optional.empty()
        t.read('a') == Optional.of(FILE)
        t.shadowedPointers(20) == ['a/b', 'a/c/d']
        t.shadowedPointers(1) == ['a/b']
    }
}
```

Run: `./gradlew test --tests '*CoordinateTree*'`
Expected: compilation FAILS on `LocalCoordinateTree` and `S3CoordinateTree`.

- [ ] **Step 2: Implement**

`git mv src/main/groovy/robsyme/cas/core/CoordinateTree.groovy src/main/groovy/robsyme/cas/core/LocalCoordinateTree.groovy`; in it rename the class to `LocalCoordinateTree implements CoordinateTree`, mark the existing public methods `@Override`, and add:

```groovy
    @Override
    boolean isDirectory(String relPath) { Files.isDirectory(pointerPath(relPath)) }

    @Override
    void createDirectories(String relPath) { Files.createDirectories(pointerPath(relPath)) }

    @Override
    long lastModifiedMillis(String relPath) {
        try { return Files.getLastModifiedTime(pointerPath(relPath)).toMillis() }
        catch( IOException e ) { return 0L }
    }
```

Then write the interface:

```groovy
// src/main/groovy/robsyme/cas/core/CoordinateTree.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The Publish Coordinate tree of one member (DESIGN.md §5, §7): Pointer Files
 * at relative paths, overwritten by the next run to the same coordinate,
 * never blocks and never roots. A published directory is one pointer at a
 * DirectoryManifest. Local and S3 trees are held to one contract
 * (CoordinateTreeContract), conflicts included.
 */
@CompileStatic
interface CoordinateTree {
    Optional<StoreRef> read(String relPath)
    void write(String relPath, StoreRef ref)
    /** A pointer, or a directory on the way to one. */
    boolean exists(String relPath)
    /** A directory on the way to pointers (not a pointer at a manifest). */
    boolean isDirectory(String relPath)
    /** A directory on the way, or a pointer at a DirectoryManifest. */
    boolean isDirectoryCoordinate(String relPath)
    /** Names directly under a directory, sorted; empty for anything else. */
    List<String> children(String relPath)
    /** Removes a pointer only; an IOException for a directory. */
    boolean delete(String relPath)
    void createDirectories(String relPath)
    /** 0 when unknown. */
    long lastModifiedMillis(String relPath)
}
```

```groovy
// src/main/groovy/robsyme/cas/s3/S3CoordinateTree.groovy
package robsyme.cas.s3

import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException

import groovy.transform.CompileStatic
import robsyme.cas.core.CoordinateTree
import robsyme.cas.core.StoreRef

/**
 * coords/ on S3 (ticket 02 decision 6). A pointer is the object coords/<rel>,
 * body `cas://<cid>/<name>\n` as locally; a directory is any key under
 * coords/<rel>/, no marker objects. S3 would hold both a/ and a/b, so both
 * local conflicts are checked before a write: a HEAD per ancestor, and one
 * LIST with max-keys 1. Two writers can still race past the checks; then the
 * ancestor pointer shadows what is under it (ticket 03 decision 4).
 */
@CompileStatic
class S3CoordinateTree implements CoordinateTree {

    private final S3Ops ops
    private final String root

    S3CoordinateTree(S3Ops ops, String prefix) {
        this.ops = ops
        this.root = "${prefix ?: ''}coords/".toString()
    }

    private static List<String> segments(String rel) {
        final List<String> out = []
        for( String s : (rel ?: '').split('/') ) {
            if( !s || s == '.' ) continue
            if( s == '..' ) {
                if( out.isEmpty() ) throw new IllegalArgumentException("coordinate path escapes the coordinate tree: '$rel'")
                out.remove(out.size() - 1)
                continue
            }
            out.add(s)
        }
        return out
    }

    private String key(String rel) { root + segments(rel).join('/') }

    private boolean hasChildren(String rel) { !ops.list(key(rel) + '/', 1).isEmpty() }

    /** The first ancestor of rel that is itself a pointer, or null. */
    private String ancestorPointer(String rel) {
        final List<String> segs = segments(rel)
        for( int i = 1; i < segs.size(); i++ ) {
            final String up = segs.subList(0, i).join('/')
            if( ops.head(root + up) != null ) return up
        }
        return null
    }

    @Override
    Optional<StoreRef> read(String rel) {
        final InputStream in = ops.get(key(rel), null, 0L, -1L)
        if( in == null ) return Optional.empty()
        final String text = in.withCloseable { it.getText('UTF-8') }
        if( ancestorPointer(rel) != null ) return Optional.empty()     // shadowed
        try {
            return Optional.of(StoreRef.parse(text.trim()))
        }
        catch( IllegalArgumentException e ) {
            throw new IOException("pointer file for '${segments(rel).join('/')}' does not hold a store uri: ${e.message}", e)
        }
    }

    @Override
    void write(String rel, StoreRef ref) {
        if( ref == null ) throw new IllegalArgumentException("no store reference for coordinate '${rel}'")
        if( segments(rel).isEmpty() ) throw new IllegalArgumentException('the root of the coordinate tree is not a coordinate')
        final String blocking = ancestorPointer(rel)
        if( blocking != null ) throw new FileAlreadyExistsException(blocking, rel, 'a Pointer File is there')
        if( hasChildren(rel) ) throw new DirectoryNotEmptyException(segments(rel).join('/'))
        ops.put(key(rel), S3Body.ofBytes((ref.toString() + '\n').getBytes('UTF-8')), S3PutOptions.create().contentType('text/plain; charset=utf-8'))
    }

    @Override boolean exists(String rel) { ops.head(key(rel)) != null || hasChildren(rel) }

    @Override boolean isDirectory(String rel) { segments(rel).isEmpty() || (ops.head(key(rel)) == null && hasChildren(rel)) }

    @Override
    boolean isDirectoryCoordinate(String rel) {
        if( isDirectory(rel) ) return true
        return read(rel).map { StoreRef r -> r.isDirectory() }.orElse(false)
    }

    @Override
    List<String> children(String rel) {
        final String under = segments(rel).isEmpty() ? root : key(rel) + '/'
        final TreeSet<String> names = new TreeSet<String>()
        for( S3Listed o : ops.list(under, 0) ) {
            final String rest = o.key.substring(under.length())
            final String first = rest.contains('/') ? rest.substring(0, rest.indexOf('/')) : rest
            if( first && !first.startsWith('.tmp-') ) names.add(first)
        }
        return new ArrayList<String>(names)
    }

    @Override
    boolean delete(String rel) {
        if( ops.head(key(rel)) == null ) {
            if( hasChildren(rel) ) throw new IOException("'${segments(rel).join('/')}' is a coordinate directory, not a pointer file")
            return false
        }
        ops.delete(key(rel))
        return true
    }

    @Override void createDirectories(String rel) { }   // S3 has no directories to make

    @Override long lastModifiedMillis(String rel) { ops.head(key(rel))?.lastModifiedMillis ?: 0L }

    /** Pointers under another pointer, in key order, at most limit: what explore warns about (silent decision 20). */
    List<String> shadowedPointers(int limit) {
        final List<String> pointers = ops.list(root, 0)*.key.collect { String k -> k.substring(root.length()) }
        final Set<String> all = new HashSet<String>(pointers)
        final List<String> out = []
        for( String p : pointers ) {
            final List<String> segs = p.split('/') as List<String>
            for( int i = 1; i < segs.size(); i++ )
                if( all.contains(segs.subList(0, i).join('/')) ) { out.add(p); break }
            if( out.size() >= limit ) break
        }
        return out
    }
}
```

In `CasFileSystemProvider`: `coordsFor(p)` becomes `session().coordinatesOf(p.alias())`; in `resolveCoordinate` replace the `pointerPath`/`Files.isDirectory`/`getLastModifiedTime` lines with

```groovy
        if( tree.isDirectory(rel) )
            return new CasNode(present: true, directory: true, size: 0L, mtime: tree.lastModifiedMillis(rel))
```

and `createDirectory` calls `coordsFor(p).createDirectories(relOf(p))`. In `CasSession`: the constructor's tree becomes `new LocalCoordinateTree(config.writableLocation.resolve('coords'))`, and add

```groovy
    /** The coordinate tree of a member by alias (DESIGN.md §7). Task 11 adds S3 members. */
    CoordinateTree coordinatesOf(String alias) {
        if( alias == config.writableAlias )
            return coordinates
        final Path location = config.locationOf(alias)
        if( location == null )
            throw new IllegalArgumentException("Unknown store alias '${alias}' -- configured stores: ${config.members.join(', ')}")
        return new LocalCoordinateTree(location.resolve('coords'))
    }
```

- [ ] **Step 3: Run the tests**

Run: `./gradlew test`
Expected: PASS: the contract passes on both trees (10 features), the renamed local tests and the provider tests unchanged in behaviour.

- [ ] **Step 4: Commit**

```bash
git add -A src/main/groovy/robsyme/cas/core/CoordinateTree.groovy src/main/groovy/robsyme/cas/core/LocalCoordinateTree.groovy \
  src/main/groovy/robsyme/cas/s3/S3CoordinateTree.groovy src/main/groovy/robsyme/cas/nio/CasFileSystemProvider.groovy \
  src/main/groovy/robsyme/cas/CasSession.groovy \
  src/test/groovy/robsyme/cas/core/CoordinateTreeTest.groovy src/test/groovy/robsyme/cas/core/LocalCoordinateTreeTest.groovy \
  src/test/groovy/robsyme/cas/core/CoordinateTreeContract.groovy src/test/groovy/robsyme/cas/core/LocalCoordinateTreeContractTest.groovy \
  src/test/groovy/robsyme/cas/s3/S3CoordinateTreeTest.groovy \
  src/test/groovy/robsyme/cas/nio/CasOccurrenceTest.groovy src/test/groovy/robsyme/cas/nio/CasFileSystemProviderTest.groovy
git commit -m "feat(coords): CoordinateTree interface; S3 coordinates held to the local conflict contract

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

(The paths are named because Tasks 5 and 7 write tests under `src/test/groovy/robsyme/cas` in the same wave; `-A` on the old and new names of the renamed test stages the rename.)

### Task 7: Snapshot storage, the conditional upload and the run-count guard

Ticket 02 decision 8, ticket 03 decision 1, ticket 04 decision 3, silent decisions 15-16. `IndexSnapshot.write` today builds and moves into a local path in one step. It splits into `build` (a local file, always; SQLite needs one) and `SnapshotStorage.replace` (an atomic move locally; a conditional `PutObject` on S3). The guards live in the new `write`: the existing cap, `fewer_runs` (the new snapshot has fewer `run` rows than the one it replaces) and `replaced_meanwhile` (another writer replaced it since `base` was taken). `catch_up_failed` is the caller's (Task 11). The old four-argument `write` stays as a wrapper, so `CasSession` compiles unchanged until Task 11.

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/SnapshotStorage.groovy` (`SnapshotStorage`, `SnapshotBase`)
- Create: `src/main/groovy/robsyme/cas/core/LocalSnapshotStorage.groovy`
- Create: `src/main/groovy/robsyme/cas/s3/S3SnapshotStorage.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy`
- Create: `src/test/groovy/robsyme/cas/core/SnapshotGuardTest.groovy`, `src/test/groovy/robsyme/cas/s3/S3SnapshotStorageTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/core/IndexSnapshotTest.groovy` (only if a feature asserts the temp-file names)

**Interfaces:**
- Consumes: `Index`, `Index.ddl()`, `IndexSnapshot.buildAndVacuum`; `S3Ops`, `MemoryS3Ops`.
- Produces: `SnapshotStorage`, `SnapshotBase`, `LocalSnapshotStorage`, `S3SnapshotStorage`, `IndexSnapshot.build`, `IndexSnapshot.Built`, the six-argument `IndexSnapshot.write`, `IndexSnapshot.OVER_CAP = 'over_cap'`, `FEWER_RUNS = 'fewer_runs'`, `REPLACED_MEANWHILE = 'replaced_meanwhile'`, `CATCH_UP_FAILED = 'catch_up_failed'`, `static int IndexSnapshot.countRuns(Path sqlite)` (-1 when unreadable).

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/SnapshotGuardTest.groovy
package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** Ticket 04 decision 3 and ticket 03 decision 1, local member. */
class SnapshotGuardTest extends Specification {

    @TempDir Path tmp
    LocalBlockStore store
    Index index

    def setup() {
        store = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    def cleanup() { index?.close() }

    /** One run in the store, logged and indexed (the IndexSnapshotTest helpers do the same). */
    private void addRun(String name) {
        final Cid m = store.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: name))
        final Cid rc = store.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(store, StoreLogKind.RUN, rc, System.currentTimeMillis())
        index.catchUp(store, StoreLog.of(store), 'lab')
    }

    def 'a snapshot is written and counted; its base says how many runs it has'() {
        given:
        addRun('r1'); addRun('r2')
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)

        when:
        final IndexSnapshot.Result r = IndexSnapshot.write(index, 'lab', storage, 0L, storage.base(tmp), tmp)

        then:
        r.written && r.runs == 2
        storage.base(tmp).runs == 2
        IndexSnapshot.countRuns(IndexSnapshot.pathIn(store.root)) == 2
    }

    def 'a snapshot with fewer runs than the one it would replace is not written (fewer_runs)'() {
        given:
        addRun('r1'); addRun('r2')
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)
        final Index fresh = Index.open(tmp.resolve('fresh.sqlite'))

        when: 'a cache that saw only an empty log'
        final IndexSnapshot.Result r = IndexSnapshot.write(fresh, 'lab', storage, 0L, storage.base(tmp), tmp)

        then:
        !r.written
        r.skipped == IndexSnapshot.FEWER_RUNS
        storage.base(tmp).runs == 2

        cleanup:
        fresh.close()
    }

    def 'the cap still keeps a large snapshot'() {
        given:
        addRun('r1')
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(store.root)
        IndexSnapshot.write(index, 'lab', storage, 0L, null, tmp)

        expect:
        IndexSnapshot.write(index, 'lab', storage, 1L, storage.base(tmp), tmp).skipped == IndexSnapshot.OVER_CAP
    }

    def 'no temp file is left in the member or in tempDir'() {
        given:
        addRun('r1')

        when:
        IndexSnapshot.write(index, 'lab', new LocalSnapshotStorage(store.root), 0L, null, tmp.resolve('t'))

        then:
        Files.list(store.root.resolve('index')).toList()*.fileName*.toString() == ['v3.sqlite']
        Files.list(tmp.resolve('t')).count() == 0
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/s3/S3SnapshotStorageTest.groovy
package robsyme.cas.s3

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

import robsyme.cas.core.SnapshotBase
import spock.lang.Specification
import spock.lang.TempDir

/** Ticket 02 decision 8, ticket 03 decision 1. */
class S3SnapshotStorageTest extends Specification {

    @TempDir Path tmp
    MemoryS3Ops s3 = new MemoryS3Ops('member')
    S3SnapshotStorage storage = new S3SnapshotStorage(s3, 'cas/')

    private Path built(String text) { Files.writeString(tmp.resolve("b-${text}.sqlite"), text) }

    def 'no snapshot: base is null and replace is If-None-Match, no-cache, with the run count'() {
        expect:
        storage.base(tmp) == null
        storage.replace(built('one'), 3, null)
        s3.objects['cas/index/v3.sqlite'].cacheControl == 'no-cache'
        s3.objects['cas/index/v3.sqlite'].metadata == [runs: '3']
        storage.base(tmp).runs == 3
        storage.base(tmp).tag == s3.objects['cas/index/v3.sqlite'].etag
    }

    def 'replace is conditional on the ETag it replaces; a stale one is refused and changes nothing'() {
        given:
        storage.replace(built('one'), 1, null)
        final SnapshotBase stale = storage.base(tmp)
        storage.replace(built('two'), 2, stale)

        expect:
        !storage.replace(built('three'), 3, stale)
        s3.objects['cas/index/v3.sqlite'].text() == 'two'
    }

    def 'a snapshot uploaded without x-amz-meta-runs is downloaded once and counted'() {
        given:
        final Path real = tmp.resolve('real.sqlite')
        java.sql.DriverManager.getConnection("jdbc:sqlite:${real}").withCloseable { c ->
            c.createStatement().execute('CREATE TABLE run(completion_cid TEXT)')
            c.createStatement().execute("INSERT INTO run VALUES ('a'), ('b')")
        }
        s3.objects['cas/index/v3.sqlite'] = new MemoryS3Ops.Obj(bytes: Files.readAllBytes(real), etag: '"x"', metadata: [:])

        expect:
        storage.base(tmp).runs == 2
        s3.calls.count { it == 'GET cas/index/v3.sqlite' } == 1
    }

    def 'the page is rewritten only when its stored SHA-256 differs, with no-cache and a text/html type'() {
        given:
        final byte[] page = '<html>1</html>'.bytes

        expect:
        storage.writePage(page)
        !storage.writePage(page)
        storage.writePage('<html>2</html>'.bytes)
        s3.objects['cas/index.html'].cacheControl == 'no-cache'
        s3.calls.count { it == 'PUT cas/index.html' } == 2
    }

    def 'fetch downloads to a local file, or answers null'() {
        expect:
        storage.fetch(tmp) == null

        when:
        storage.replace(built('one'), 1, null)
        final Path f = storage.fetch(tmp)

        then:
        f.text == 'one'
    }
}
```

Run: `./gradlew test --tests '*SnapshotGuardTest' --tests '*S3SnapshotStorageTest'`
Expected: compilation FAILS on `SnapshotStorage`, `LocalSnapshotStorage`, `S3SnapshotStorage`, `IndexSnapshot.build`.

- [ ] **Step 2: Implement**

```groovy
// src/main/groovy/robsyme/cas/core/SnapshotStorage.groovy
package robsyme.cas.core

import java.nio.file.Path

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * Where a member keeps its Index Snapshot and page (DESIGN.md §15). The
 * snapshot is always built locally (IndexSnapshot.build); this puts it in
 * place. A writer takes base() before its catch-up and replaces only that
 * version (ticket 03 decision 1, silent decision 16).
 */
@CompileStatic
interface SnapshotStorage {
    /** The snapshot there now, or null when there is none. */
    SnapshotBase base(Path tempDir)
    /** A local copy for reading, or null when there is none; the caller deletes it. */
    Path fetch(Path tempDir)
    /** Puts built in place if the snapshot is still base (null: still absent); false when another writer got there first. */
    boolean replace(Path built, int runs, SnapshotBase base)
    /** Writes the page when its bytes differ; true when it wrote. */
    boolean writePage(byte[] page)
    String describe()
}

/** A version of a snapshot: an opaque tag (an ETag, or size:mtime:inode), its size, its run rows (-1 when uncountable). */
@Canonical
@CompileStatic
class SnapshotBase {
    String tag
    long bytes
    int runs
}
```

```groovy
// src/main/groovy/robsyme/cas/core/LocalSnapshotStorage.groovy
package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.PosixFilePermissions

import groovy.transform.CompileStatic

/** index/v<N>.sqlite and index.html in a local member: an atomic move, as before (ticket 03 decision 1). */
@CompileStatic
class LocalSnapshotStorage implements SnapshotStorage {

    final Path memberRoot

    LocalSnapshotStorage(Path memberRoot) { this.memberRoot = memberRoot }

    private Path target() { IndexSnapshot.pathIn(memberRoot) }

    @Override
    SnapshotBase base(Path tempDir) {
        if( !Files.isRegularFile(target()) ) return null
        final BasicFileAttributes a = Files.readAttributes(target(), BasicFileAttributes)
        return new SnapshotBase("${a.size()}:${a.lastModifiedTime().toMillis()}:${a.fileKey()}".toString(), a.size(), IndexSnapshot.countRuns(target()))
    }

    @Override
    Path fetch(Path tempDir) {
        if( !Files.isRegularFile(target()) ) return null
        Files.createDirectories(tempDir)
        final Path copy = Files.createTempFile(tempDir, 'nf-blocks-snapshot-', '.sqlite')
        Files.copy(target(), copy, StandardCopyOption.REPLACE_EXISTING)
        return copy
    }

    /** Locally the move is atomic and the count guard is the caller's; base is not re-checked. */
    @Override
    boolean replace(Path built, int runs, SnapshotBase base) {
        Files.createDirectories(target().parent)
        final Path staged = target().resolveSibling(".tmp-${UUID.randomUUID()}.sqlite")
        try {
            Files.copy(built, staged)
            if( FileSystems.default.supportedFileAttributeViews().contains('posix') )
                Files.setPosixFilePermissions(staged, PosixFilePermissions.fromString('rw-r--r--'))
            Files.move(staged, target(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            return true
        }
        finally {
            Files.deleteIfExists(staged)
        }
    }

    @Override boolean writePage(byte[] page) { IndexSnapshot.writePage(memberRoot, page) }

    @Override String describe() { target().toString() }
}
```

```groovy
// src/main/groovy/robsyme/cas/s3/S3SnapshotStorage.groovy
package robsyme.cas.s3

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.MessageDigest

import groovy.transform.CompileStatic
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.SnapshotBase
import robsyme.cas.core.SnapshotStorage

/**
 * The snapshot and page of an S3 member (ticket 02 decision 8, ticket 03
 * decision 1): one PutObject each, the snapshot `no-cache` with its run
 * count as x-amz-meta-runs and If-Match on the ETag it replaces (If-None-Match
 * when there was none), the page `no-cache` and rewritten only when its
 * stored SHA-256 differs (an ETag is no MD5 under SSE-KMS or multipart).
 */
@CompileStatic
class S3SnapshotStorage implements SnapshotStorage {

    static final String NO_CACHE = 'no-cache'
    private final S3Ops ops
    private final String snapshotKey
    private final String pageKey

    S3SnapshotStorage(S3Ops ops, String prefix) {
        this.ops = ops
        this.snapshotKey = (prefix ?: '') + IndexSnapshot.relativePath()
        this.pageKey = (prefix ?: '') + IndexSnapshot.PAGE_NAME
    }

    @Override
    SnapshotBase base(Path tempDir) {
        final S3Head head = ops.head(snapshotKey)
        if( head == null ) return null
        final String runs = head.metadata?.get('runs')
        if( runs?.isInteger() ) return new SnapshotBase(head.etag, head.size, runs as int)
        // Uploaded by something that does not record it (gate/cloud/s3tier.py): count it once.
        final Path copy = download(head.etag, tempDir)
        try {
            return new SnapshotBase(head.etag, head.size, copy == null ? -1 : IndexSnapshot.countRuns(copy))
        }
        finally {
            if( copy != null ) Files.deleteIfExists(copy)
        }
    }

    @Override
    Path fetch(Path tempDir) { download(null, tempDir) }

    private Path download(String etag, Path tempDir) {
        final InputStream in = ops.get(snapshotKey, etag, 0L, -1L)
        if( in == null ) return null
        Files.createDirectories(tempDir)
        final Path copy = Files.createTempFile(tempDir, 'nf-blocks-snapshot-', '.sqlite')
        in.withCloseable { Files.copy(it, copy, StandardCopyOption.REPLACE_EXISTING) }
        return copy
    }

    @Override
    boolean replace(Path built, int runs, SnapshotBase base) {
        final S3PutOptions o = S3PutOptions.create().sha256().cacheControl(NO_CACHE)
            .contentType('application/vnd.sqlite3').meta('runs', String.valueOf(runs))
        if( base == null ) o.ifNoneMatch() else o.ifMatch(base.tag)
        final S3Written w = ops.put(snapshotKey, S3Body.ofFile(built, 0L, Files.size(built)), o)
        return w.status == S3Written.Status.WRITTEN
    }

    @Override
    boolean writePage(byte[] page) {
        final String sha = Base64.encoder.encodeToString(MessageDigest.getInstance('SHA-256').digest(page))
        if( ops.head(pageKey)?.sha256 == sha ) return false
        ops.put(pageKey, S3Body.ofBytes(page), S3PutOptions.create().sha256().cacheControl(NO_CACHE).contentType('text/html; charset=utf-8'))
        return true
    }

    @Override String describe() { "${ops.describe()}/${snapshotKey}" }
}
```

In `IndexSnapshot`, add the four `skipped` constants (and use `OVER_CAP` where `'over_cap'` appears), and:

```groovy
    @CompileStatic
    static final class Built {
        final Path file; final long bytes; final int runs; final String watermark
        Built(Path file, long bytes, int runs, String watermark) { this.file = file; this.bytes = bytes; this.runs = runs; this.watermark = watermark }
    }

    /** The member's snapshot as a local file in tempDir; the caller deletes it. */
    static Built build(Index index, String member, Path tempDir) {
        Files.createDirectories(tempDir)
        final String token = token()
        final Path buildFile = tempDir.resolve(".tmp-${token}.build")
        final Path vacuumed = tempDir.resolve("nf-blocks-snapshot-${token}.sqlite")
        try {
            final String watermark = index.watermark(member)
            final int runs = buildAndVacuum(buildFile, vacuumed, index.file, member, watermark)
            return new Built(vacuumed, Files.size(vacuumed), runs, watermark)
        }
        finally {
            for( String suffix : ['', '-journal', '-wal', '-shm'] )
                Files.deleteIfExists(buildFile.resolveSibling(buildFile.fileName.toString() + suffix))
        }
    }

    /**
     * Writes the member's snapshot into storage unless a guard keeps the old
     * one (DESIGN.md §15, ticket 04 decision 3): the cap (maxBytes > 0), fewer
     * run rows than base, or another writer's version in place of base.
     */
    static Result write(Index index, String member, SnapshotStorage storage, long maxBytes, SnapshotBase base, Path tempDir) {
        if( maxBytes > 0 && base != null && base.bytes >= maxBytes )
            return new Result(false, null, base.bytes, -1, null, OVER_CAP)
        final Built built = build(index, member, tempDir)
        try {
            if( maxBytes > 0 && built.bytes > maxBytes )
                return new Result(false, null, built.bytes, built.runs, built.watermark, OVER_CAP)
            if( base != null && base.runs > built.runs )
                return new Result(false, null, built.bytes, built.runs, built.watermark, FEWER_RUNS)
            if( !storage.replace(built.file, built.runs, base) )
                return new Result(false, null, built.bytes, built.runs, built.watermark, REPLACED_MEANWHILE)
            return new Result(true, null, built.bytes, built.runs, built.watermark, null)
        }
        finally {
            Files.deleteIfExists(built.file)
        }
    }

    /** The old entry point: a local member, its own base, temp files beside it. */
    static Result write(Index index, String member, Path memberRoot, long maxBytes) {
        final LocalSnapshotStorage storage = new LocalSnapshotStorage(memberRoot)
        final Path temp = pathIn(memberRoot).parent
        final Result r = write(index, member, storage, maxBytes, storage.base(temp), temp)
        return new Result(r.written, pathIn(memberRoot), r.bytes, r.runs, r.watermark, r.skipped)
    }

    static int countRuns(Path sqlite) {
        try {
            return java.sql.DriverManager.getConnection("jdbc:sqlite:file:${sqlite.toAbsolutePath()}?mode=ro".toString()).withCloseable { c ->
                count(c, 'SELECT count(*) FROM run')
            }
        }
        catch( java.sql.SQLException e ) {
            return -1
        }
    }
```

Delete the old `write` body (its build-and-move logic now lives in `build` and `LocalSnapshotStorage.replace`).

- [ ] **Step 3: Run the tests**

Run: `./gradlew test`
Expected: PASS: the two new specs and the unchanged `IndexSnapshotTest`, `ExploreServerTest` and `CasObserverTest`, which still go through the four-argument `write`.

- [ ] **Step 4: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/SnapshotStorage.groovy src/main/groovy/robsyme/cas/core/LocalSnapshotStorage.groovy \
  src/main/groovy/robsyme/cas/s3/S3SnapshotStorage.groovy src/main/groovy/robsyme/cas/core/IndexSnapshot.groovy \
  src/test/groovy/robsyme/cas/core/SnapshotGuardTest.groovy src/test/groovy/robsyme/cas/s3/S3SnapshotStorageTest.groovy
git commit -m "feat(snapshot): storage behind an interface; conditional S3 upload; run-count guard

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Seeding the index from a member's snapshot

Ticket 04 decisions 1-10, silent decision 17. A cache that has never caught a member up (no `store_log_watermark:<member>` in `meta`; ticket 04 decision 6 makes an absent watermark the only trigger, whatever `block_scan:<member>` says) copies the member's snapshot rows instead of reading every run's closure, then reads only the Store Log tail. The snapshot is written from this very DDL (`Index.ddl()`, same `schema_version`, DESIGN §15), so the copy is `INSERT ... SELECT` over an attached database. Its `run.member` and `log_entry.member` are NULL (an alias is a local label), so seeding writes the member's alias into both. `claim_current` is recomputed with `ClaimCurrent.rewrite`, the ingest path's own code. Seeding copies each run, Selection and Claim only when the cache does not hold it yet, so two members' snapshots seeded into one cache duplicate no row.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (`seedFrom`, `SnapshotUnusable`, the five-argument `catchUp`, `metaValue`)
- Create: `src/test/groovy/robsyme/cas/core/IndexSeedTest.groovy`

**Interfaces:**
- Consumes: `SnapshotStorage.fetch(Path)` (Task 7), `IndexSnapshot.build` (Task 7, in the test), `ClaimCurrent.rewrite(Connection, String)`, `StoreLog`, `IndexSnapshot.WATERMARK_KEY`, `IndexSnapshot.WRITTEN_AT_KEY`.
- Produces: `Index.catchUp(BlockStore, StoreLog, String member, SnapshotStorage snapshots, Path tempDir)`; `boolean Index.seedFrom(Path, String)`; `static class Index.SnapshotUnusable extends Exception { final String reason }`; `String Index.metaValue(String key)`; the meta key `seeded_from:<member>`; the console warning (logger `nextflow.cas`) `store member '<m>' has no usable Index Snapshot (<reason>); indexing it from every block instead, which is slow on a large member. `nextflow plugin nf-blocks:snapshot` against it writes one.`

- [ ] **Step 1: Write the failing test**

```groovy
// src/test/groovy/robsyme/cas/core/IndexSeedTest.groovy
package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

/** Ticket 04: a cold cache seeds from the member's snapshot and reads only the tail. */
class IndexSeedTest extends Specification {

    @TempDir Path tmp
    LocalBlockStore lab
    List<Cid> opened = []

    /** Counts every metadata block the index reads, which is what seeding must avoid. */
    BlockStore counting

    ListAppender<ILoggingEvent> console
    int runs = 0

    def setup() {
        lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        counting = new CompositeStore([lab]) {
            @Override InputStream open(Cid cid) { opened << cid; super.open(cid) }
        }
        console = new ListAppender<ILoggingEvent>(); console.start()
        ((Logger) LoggerFactory.getLogger('nextflow.cas')).addAppender(console)
    }

    def cleanup() { ((Logger) LoggerFactory.getLogger('nextflow.cas')).detachAppender(console) }

    /** A succeeded run with one item, logged at `at`; returns its completion. */
    private Cid run(String name, String sample, long at) {
        final Cid content = lab.putStreaming(new ByteArrayInputStream("bam-${sample}".bytes))
        final Cid item = lab.putDagCbor(Fixtures.outputItem2([[sample: sample], Fixtures.leaf2("${sample}.bam", content, 5)]))
        final Cid m = lab.putDagCbor(Fixtures.runManifest(run_name: name, nf_run_hash: name))
        final Cid coll = lab.putDagCbor(Fixtures.outputCollection(m, 'aligned', [[item, ["aligned/${sample}.bam".toString()]]]))
        // finished_at in call order, so the last run() is the latest run.
        final Cid rc = lab.putDagCbor(Fixtures.runCompletion(m, [coll], [finished_at: String.format('2026-09-28T10:%02d:00.000Z', runs++)]))
        StoreLog.append(lab, StoreLogKind.RUN, rc, at)
        return rc
    }

    private Index producer() {
        final Index index = Index.open(tmp.resolve("producer-${UUID.randomUUID()}.sqlite"))
        index.catchUp(lab, StoreLog.of(lab), 'lab')
        return index
    }

    def 'a cold cache seeds, answers like the producer, and reads no block of a run the snapshot holds'() {
        given:
        final long now = System.currentTimeMillis()
        run('r1', 'A', now - 3_600_000L)
        final Cid r2 = run('r2', 'B', now - 3_000_000L)
        final Index source = producer()
        IndexSnapshot.write(source, 'lab', new LocalSnapshotStorage(lab.root), 0L, null, tmp.resolve('t'))
        final Cid r3 = run('r3', 'C', now)                        // after the snapshot: the tail
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        opened.clear()
        cold.catchUp(counting, StoreLog.of(lab), 'lab', new LocalSnapshotStorage(lab.root), tmp.resolve('t'))

        then: 'the same answers as a full ingest'
        cold.items(r2, 'aligned', [sample: 'B']).size() == 1
        cold.latestSuccessfulRun('p') == Optional.of(r3)
        cold.isRunIndexed(r2) && cold.isRunIndexed(r3)
        cold.metaValue('seeded_from:lab') != null
        cold.metaValue('block_scan:lab') == 'done'

        and: 'only the tail run was read (ticket 04 decision 11)'
        !opened.contains(r2)
        opened.contains(r3)
        console.list.isEmpty()

        cleanup:
        source?.close(); cold?.close()
    }

    def 'claim_current is recomputed, never copied (ticket 04 decision 9)'() {
        given:
        final Cid rc = run('r1', 'A', System.currentTimeMillis())
        final Cid claim = lab.putDagCbor(Fixtures.claim(rc, 'delete', null, null, []))
        StoreLog.append(lab, StoreLogKind.CLAIM, claim, System.currentTimeMillis())
        final Index source = producer()
        IndexSnapshot.write(source, 'lab', new LocalSnapshotStorage(lab.root), 0L, null, tmp.resolve('t'))
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.seedFrom(IndexSnapshot.pathIn(lab.root), 'lab')

        then:
        cold.claimState(rc).deleted
        cold.latestSuccessfulRun('p') == Optional.empty()

        cleanup:
        source?.close(); cold?.close()
    }

    def 'seeding the same snapshot twice, or two members, duplicates no row'() {
        given:
        run('r1', 'A', System.currentTimeMillis())
        final Index source = producer()
        IndexSnapshot.write(source, 'lab', new LocalSnapshotStorage(lab.root), 0L, null, tmp.resolve('t'))
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.seedFrom(IndexSnapshot.pathIn(lab.root), 'lab')
        cold.seedFrom(IndexSnapshot.pathIn(lab.root), 'lab')

        then:
        cold.countRows('collection_item') == source.countRows('collection_item')
        cold.countRows('item_attr') == source.countRows('item_attr')

        cleanup:
        source?.close(); cold?.close()
    }

    def 'no usable snapshot (#why): full scan, same answer, one warning naming the member and the fix'() {
        given:
        final Cid rc = run('r1', 'A', System.currentTimeMillis())
        prepare.call(lab.root)
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.catchUp(lab, StoreLog.of(lab), 'lab', new LocalSnapshotStorage(lab.root), tmp.resolve('t'))

        then:
        cold.isRunIndexed(rc)
        cold.metaValue('seeded_from:lab') == null
        console.list.size() == 1
        console.list[0].formattedMessage.contains("'lab'")
        console.list[0].formattedMessage.contains(why)
        console.list[0].formattedMessage.contains('nf-blocks:snapshot')

        cleanup:
        cold?.close()

        where:
        why                        | prepare
        'absent'                   | { Path root -> }
        'unreadable'               | { Path root -> Files.createDirectories(root.resolve('index')); Files.writeString(IndexSnapshot.pathIn(root), 'not sqlite') }
    }

    def 'an empty member warns about nothing'() {
        given:
        final Index cold = Index.open(tmp.resolve('cold.sqlite'))

        when:
        cold.catchUp(lab, StoreLog.of(lab), 'lab', new LocalSnapshotStorage(lab.root), tmp.resolve('t'))

        then:
        console.list.isEmpty()

        cleanup:
        cold?.close()
    }
}
```

Add a package-private helper `int countRows(String table)` to `Index` for this test (validated against `ddl()` table names). A snapshot with another `schema_version` gives the reason `schema_version <n>`; cover it with a third `where:` row that rewrites the snapshot's `schema_version` row with `sqlite-jdbc` before the catch-up.

Run: `./gradlew test --tests 'robsyme.cas.core.IndexSeedTest'`
Expected: compilation FAILS on the five-argument `catchUp`, `seedFrom`, `metaValue`, `countRows`.

- [ ] **Step 2: Implement**

In `Index`:

```groovy
    private static final org.slf4j.Logger CONSOLE = org.slf4j.LoggerFactory.getLogger('nextflow.cas')
    private static final String META_SEEDED = 'seeded_from'

    /** Why a snapshot could not seed the cache: absent, schema_version <n>, unreadable (ticket 04 decision 2). */
    static class SnapshotUnusable extends Exception {
        final String reason
        SnapshotUnusable(String reason, Throwable cause = null) { super(reason, cause); this.reason = reason }
    }

    String metaValue(String key) { meta(key) }

    /**
     * catchUp, seeding first when this index has never caught `member` up
     * (ticket 04 decisions 1, 6): its snapshot's rows, its watermark, and the
     * block scan marked done, so only the Store Log tail (with the 10-minute
     * overlap) is read. Without a usable snapshot, the full scan, with a
     * warning when the member's Store Log is not empty.
     */
    void catchUp(BlockStore store, StoreLog storeLog, String member, SnapshotStorage snapshots, Path tempDir) {
        // Ticket 04 decision 6: an absent watermark alone triggers seeding.
        if( snapshots != null && meta(watermarkKey(member)) == null ) {
            Path file = null
            try {
                file = snapshots.fetch(tempDir)
                if( file == null ) throw new SnapshotUnusable('absent')
                seedFrom(file, member)
            }
            catch( SnapshotUnusable e ) {
                warnUnusable(storeLog, member, e.reason)
            }
            catch( IOException e ) {
                warnUnusable(storeLog, member, 'unreadable')
                log.debug("fetching the Index Snapshot of '${member}' failed", e)
            }
            finally {
                if( file != null ) Files.deleteIfExists(file)
            }
        }
        catchUp(store, storeLog, member)
    }

    /** Silent decision 17: one warning, and only when the member's Store Log is not empty. */
    private static void warnUnusable(StoreLog storeLog, String member, String reason) {
        if( !storeLog.read().isEmpty() )
            CONSOLE.warn("store member '${member}' has no usable Index Snapshot (${reason}); indexing it from every block instead, which is slow on a large member. `nextflow plugin nf-blocks:snapshot` against it writes one.")
    }

    /** The per-owner copies, each limited to what this index does not hold yet (silent decision 17). */
    private static final List<String> SEED = [
        'CREATE TEMP TABLE seed_run(cid TEXT PRIMARY KEY)',
        'CREATE TEMP TABLE seed_coll(cid TEXT PRIMARY KEY)',
        'CREATE TEMP TABLE seed_item(cid TEXT PRIMARY KEY)',
        'CREATE TEMP TABLE seed_claim(cid TEXT PRIMARY KEY)',
        'INSERT INTO seed_run SELECT completion_cid FROM snap.run WHERE completion_cid NOT IN (SELECT completion_cid FROM main.run)',
        '''INSERT INTO main.run SELECT completion_cid, manifest_cid, pipeline, revision, commit_id, nf_run_hash, session_id,
             run_name, asserted_by, status, possibly_incomplete, finished_at, :member FROM snap.run WHERE completion_cid IN seed_run''',
        'INSERT INTO seed_coll SELECT collection_cid FROM snap.collection WHERE collection_cid NOT IN (SELECT collection_cid FROM main.collection)',
        'INSERT INTO main.collection SELECT * FROM snap.collection WHERE collection_cid IN seed_coll',
        'INSERT INTO main.collection_item SELECT * FROM snap.collection_item WHERE collection_cid IN seed_coll',
        'INSERT INTO seed_item SELECT item_cid FROM snap.item WHERE item_cid NOT IN (SELECT item_cid FROM main.item)',
        'INSERT INTO main.item SELECT item_cid FROM seed_item',
        'INSERT INTO main.item_attr SELECT * FROM snap.item_attr WHERE item_cid IN seed_item',
        'INSERT INTO main.producer SELECT * FROM snap.producer WHERE completion_cid IN seed_run',
        'INSERT INTO main.consumer SELECT * FROM snap.consumer WHERE completion_cid IN seed_run',
        'INSERT INTO main.missing SELECT * FROM snap.missing WHERE have_cid IN seed_run OR have_cid IN seed_coll',
        'INSERT INTO main.selection_child SELECT * FROM snap.selection_child WHERE parent_cid IN seed_coll',
        'INSERT INTO main.selection_derived SELECT * FROM snap.selection_derived WHERE selection_cid IN seed_coll',
        'INSERT INTO seed_claim SELECT claim_cid FROM snap.claim WHERE claim_cid NOT IN (SELECT claim_cid FROM main.claim)',
        'INSERT INTO main.claim SELECT * FROM snap.claim WHERE claim_cid IN seed_claim',
        'INSERT INTO main.claim_supersedes SELECT * FROM snap.claim_supersedes WHERE claim_cid IN seed_claim',
        'INSERT OR IGNORE INTO main.log_entry SELECT cid, kind, :member, written_at FROM snap.log_entry',
    ]

    /**
     * Copies a member's Index Snapshot into this index (ticket 04 decision 1).
     * Trusted when its schema_version matches (decision 10); claim_current is
     * recomputed for every seeded subject (decision 9). True when it seeded.
     */
    boolean seedFrom(Path snapshotFile, String member) throws SnapshotUnusable {
        try {
            update('ATTACH DATABASE ? AS snap', [(Object) "file:${snapshotFile.toAbsolutePath()}?mode=ro".toString()])
        }
        catch( Exception e ) {
            throw new SnapshotUnusable('unreadable', e)
        }
        try {
            final List<Integer> versions = []
            try {
                query('SELECT version FROM snap.schema_version', []) { ResultSet rs -> versions.add(rs.getInt(1)) }
            }
            catch( Exception e ) {
                throw new SnapshotUnusable('unreadable', e)
            }
            if( versions != [SCHEMA_VERSION] )
                throw new SnapshotUnusable("schema_version ${versions ? versions[0] : 'missing'}")
            final Map<String, String> snapMeta = [:]
            try {
                query('SELECT key, value FROM snap.meta', []) { ResultSet rs -> snapMeta.put(rs.getString(1), rs.getString(2)) }
                withTransaction {
                    for( String sql : SEED )
                        update(sql.replace(':member', '?'), sql.contains(':member') ? [(Object) member] : [])
                    final List<String> subjects = []
                    query('SELECT DISTINCT subject_cid FROM main.claim WHERE claim_cid IN seed_claim', []) { ResultSet rs -> subjects.add(rs.getString(1)) }
                    for( String subject : subjects )
                        ClaimCurrent.rewrite(connection, subject)
                    if( snapMeta[IndexSnapshot.WATERMARK_KEY] )
                        setMeta(watermarkKey(member), snapMeta[IndexSnapshot.WATERMARK_KEY])
                    setMeta(META_SCANNED + ':' + member, 'done')
                    setMeta(META_SEEDED + ':' + member, snapMeta[IndexSnapshot.WRITTEN_AT_KEY] ?: 'unknown')
                    for( String t : ['seed_run', 'seed_coll', 'seed_item', 'seed_claim'] )
                        update("DROP TABLE temp.${t}".toString(), [])
                }
            }
            catch( IllegalStateException e ) {
                // Index.update and Index.query wrap every SQLException (Index.groovy:946-979).
                throw new SnapshotUnusable('unreadable', e)
            }
            return true
        }
        finally {
            update('DETACH DATABASE snap', [])
        }
    }
```

(`ATTACH` cannot run inside a transaction, which is why it precedes `withTransaction`; `ClaimCurrent.rewrite` takes the same connection the ingest path passes it. If `withTransaction` fails half way it rolls back, taking the temp tables with it. `Index.update` and `Index.query` wrap every `SQLException` in an `IllegalStateException` (`Index.groovy:946-979`), so that is what a half-failed seed throws; `seedFrom` turns it into `SnapshotUnusable('unreadable', e)`, and `catchUp` falls back to the full scan instead of letting it escape to `CasSession.catchUpIndex`, which would mark the member `catch_up_failed`.)

- [ ] **Step 3: Run the tests**

Run: `./gradlew test`
Expected: PASS; the existing `IndexTest` and `IndexSnapshotTest` are unaffected (nothing calls the five-argument `catchUp` yet; Task 11 wires it).

- [ ] **Step 4: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Index.groovy src/test/groovy/robsyme/cas/core/IndexSeedTest.groovy
git commit -m "feat(index): seed a cold cache from a member's Index Snapshot, then read only the tail

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: Node-side hashing, the `afterScript` default

Spec §14, ticket 06, carried decision 18, silent decisions 5-6. Under Fusion the task node is the cheapest Address Provider: `afterScript` runs inside the container with the mount live, after the script and before `.exitcode` (measured on Batch, `fusion-batch-remeasure.md` §2; order in `command-run.txt`'s `nxf_main`, where `{{after_script}}` follows `{{unstage_cmd}}`). The script reads the `### outputs:` block `BashWrapperBuilder.getTaskMetadata` writes (`BashWrapperBuilder.groovy:519-522`: one `### - '<pattern>'` line per declared output, patterns as written, optional outputs listed whether or not they exist), expands the patterns, hashes what exists and writes `.command.cas`. `afterScript` is not part of the task hash (`TaskHasher.groovy`), so the default changes no `-resume` identity (DESIGN §0 rule 5). The installer runs in `CasObserverFactory.create`, inside `Session.init`, before any process reads its config.

**Files:**
- Create: `src/main/resources/robsyme/cas/node-hash.sh`
- Create: `src/main/groovy/robsyme/cas/trace/NodeHash.groovy`
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserverFactory.groovy`
- Create: `src/test/groovy/robsyme/cas/trace/NodeHashTest.groovy`, `src/test/groovy/robsyme/cas/trace/NodeHashScriptTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/trace/CasObserverFactoryTest.groovy`

**Interfaces:**
- Consumes: `CasConfig.nodeHashEnabled(Map)` (Task 4); `ConsoleLog.LOG` (logger `nextflow.cas`).
- Produces: `NodeHash` (shared interfaces); the file `<task dir>/.command.cas`, lines `<64 hex>  <path relative to the task dir>` in `sha256sum` text format (coreutils escaping for names holding `\` or a newline), which Task 10 parses.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/trace/NodeHashScriptTest.groovy
package robsyme.cas.trace

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

/** Runs the real script with bash in a fake task directory (ticket 06 §3). */
@Requires({ new File('/bin/bash').canExecute() })
class NodeHashScriptTest extends Specification {

    @TempDir Path task

    private static String hex(byte[] b) { MessageDigest.getInstance('SHA-256').digest(b).encodeHex().toString() }

    private void put(String rel, String text) {
        final Path p = task.resolve(rel); Files.createDirectories(p.parent); Files.writeString(p, text)
    }

    private int runScript(Map<String, String> env = [:]) {
        final ProcessBuilder pb = new ProcessBuilder('/bin/bash', '-c', 'set -u\n' + NodeHash.script() + '\necho after=$?')
            .directory(task.toFile()).redirectErrorStream(true)
        pb.environment().remove('NXF_CHDIR')
        pb.environment().putAll(env)
        final Process p = pb.start()
        final String out = p.inputStream.text
        assert out.contains('after=0')
        return p.waitFor()
    }

    private Map<String, String> cas() {
        Files.readAllLines(task.resolve('.command.cas')).collectEntries { String l -> [(l.substring(66)): l.substring(0, 64)] }
    }

    def 'declared outputs are hashed: globs expanded, directories walked, absent optionals and links skipped'() {
        given:
        put('.command.run', """#!/bin/bash
### ---
### name: 'LINKS'
### outputs:
### - 'd'
### - '*.glob.txt'
### - 'maybe.txt'
### - 'name with space.txt'
### ...
""")
        put('d/target.txt', 'target\n')
        put('d/nested/deep.txt', 'deep\n')
        Files.createSymbolicLink(task.resolve('d/rel.txt'), Path.of('target.txt'))
        put('a.glob.txt', 'a\n'); put('b.glob.txt', 'b\n')
        put('name with space.txt', 'x\n')
        put('input.bam', 'staged input')
        Files.createSymbolicLink(task.resolve('c.glob.txt'), task.resolve('input.bam'))
        put('undeclared.txt', 'nope')

        when:
        runScript()

        then:
        cas() == [
            'd/target.txt'       : hex('target\n'.bytes),
            'd/nested/deep.txt'  : hex('deep\n'.bytes),
            'a.glob.txt'         : hex('a\n'.bytes),
            'b.glob.txt'         : hex('b\n'.bytes),
            'name with space.txt': hex('x\n'.bytes),
        ]
        !Files.exists(task.resolve('.command.cas.tmp'))
    }

    def 'it never fails the task: no .command.run, a declared directory that is absent'() {
        when:
        runScript()

        then:
        !Files.exists(task.resolve('.command.cas'))

        when:
        put('.command.run', "### outputs:\n### - 'gone'\n### - 'here.txt'\n### ...\n")
        put('here.txt', 'h\n')
        runScript()

        then: 'the absent directory gets no line; the rest is hashed'
        cas() == ['here.txt': hex('h\n'.bytes)]
    }

    // setReadable(false) does nothing for root, so the file would be hashed and prove nothing.
    @Requires({ System.getProperty('user.name') != 'root' })
    def 'it never fails the task: an unreadable output'() {
        given:
        put('.command.run', "### outputs:\n### - 'locked.txt'\n### ...\n")
        put('locked.txt', 'x')
        task.resolve('locked.txt').toFile().setReadable(false)

        when:
        runScript()

        then:
        notThrown(AssertionError)
    }

    def 'NXF_CHDIR names the task directory when the shell is elsewhere (Fusion)'() {
        given:
        put('.command.run', "### outputs:\n### - 'out.txt'\n### ...\n")
        put('out.txt', 'o\n')
        final Path elsewhere = Files.createTempDirectory('elsewhere')

        when:
        final ProcessBuilder pb = new ProcessBuilder('/bin/bash', '-c', NodeHash.script())
            .directory(elsewhere.toFile())
        pb.environment().put('NXF_CHDIR', task.toString())
        pb.start().waitFor()

        then:
        cas() == ['out.txt': hex('o\n'.bytes)]
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/trace/NodeHashTest.groovy
package robsyme.cas.trace

import spock.lang.Specification

/** Chained ahead of the user's afterScript, in every selector (spec §14). */
class NodeHashTest extends Specification {

    def 'no afterScript anywhere: ours becomes the default'() {
        given:
        final Map config = [process: [cpus: 1]]

        when:
        NodeHash.install(config)

        then:
        config.process.afterScript == NodeHash.script()
    }

    def "the user's afterScript runs after ours, at the top level and in each selector"() {
        given:
        final Map config = [process: [afterScript: 'echo user', 'withName:FOO': [afterScript: 'echo foo'], 'withLabel:big': [cpus: 8]]]

        when:
        final List<String> skipped = NodeHash.install(config)

        then:
        config.process.afterScript == NodeHash.script() + '\necho user'
        config.process['withName:FOO'].afterScript == NodeHash.script() + '\necho foo'
        !config.process['withLabel:big'].containsKey('afterScript')
        skipped.isEmpty()
    }

    def 'a closure afterScript is left alone and reported'() {
        given:
        final Closure dynamic = { -> 'echo dyn' }
        final Map config = [process: ['withName:BAR': [afterScript: dynamic]]]

        when:
        final List<String> skipped = NodeHash.install(config)

        then:
        config.process['withName:BAR'].afterScript.is(dynamic)
        skipped == ['withName:BAR']
        config.process.afterScript == NodeHash.script()
    }

    def 'installing twice chains once'() {
        given:
        final Map config = [process: [afterScript: 'echo user']]

        when:
        NodeHash.install(config); NodeHash.install(config)

        then:
        config.process.afterScript == NodeHash.script() + '\necho user'
    }
}
```

In `CasObserverFactoryTest`, add a feature: with `cas.nodeHash = true` in the mock session's config, `create` leaves `config.process.afterScript == NodeHash.script()`; with neither `cas.nodeHash` nor `fusion.enabled`, `config.process` gains nothing.

Run: `./gradlew test --tests 'robsyme.cas.trace.NodeHash*'`
Expected: compilation FAILS on `NodeHash`.

- [ ] **Step 2: The script**

```bash
# src/main/resources/robsyme/cas/node-hash.sh
# nf-blocks: hash this task's declared outputs on the node (DESIGN.md §11, spec §14).
# Installed as a process.afterScript default, ahead of the user's. Reads the
# "### outputs:" block of .command.run, expands each pattern, and writes
# .command.cas in sha256sum format, paths relative to the task directory.
# It never fails the task: every error leaves .command.cas absent or partial,
# and the head node then addresses the file itself.
nf_blocks_cas() {
  local dir="${NXF_CHDIR:-$PWD}"
  [ -f "$dir/.command.run" ] || return 0
  local -a sum
  if command -v sha256sum >/dev/null 2>&1; then sum=(sha256sum)
  elif command -v shasum >/dev/null 2>&1; then sum=(shasum -a 256)
  else return 0; fi
  (
    cd "$dir" || exit 0
    shopt -s nullglob
    shopt -s globstar 2>/dev/null || true
    : > .command.cas.tmp || exit 0
    sed -n "s/^### - '\(.*\)'\$/\1/p" .command.run | while IFS= read -r pattern; do
      local IFS=
      for f in $pattern; do
        [ -L "$f" ] && continue
        if [ -d "$f" ]; then find "$f" -type f -exec "${sum[@]}" {} + 2>/dev/null
        elif [ -f "$f" ]; then "${sum[@]}" "$f" 2>/dev/null
        fi
      done
    done >> .command.cas.tmp
    mv -f .command.cas.tmp .command.cas
  ) || true
  rm -f "$dir/.command.cas.tmp" 2>/dev/null || true
  return 0
}
nf_blocks_cas || true
```

(`local IFS=` inside the loop stops word splitting of a pattern while keeping pathname expansion; `nullglob` makes an unmatched glob expand to nothing, and a literal name that does not exist fails the `-f`/`-d` tests, which is how absent optional outputs are skipped. `local` in a pipeline subshell is legal inside a function body.)

- [ ] **Step 3: The installer and its call**

```groovy
// src/main/groovy/robsyme/cas/trace/NodeHash.groovy
package robsyme.cas.trace

import groovy.transform.CompileStatic

/**
 * Installs node-side hashing as a process.afterScript default (spec §14,
 * DESIGN.md §11), ahead of the user's own afterScript in the process scope
 * and in each withName/withLabel selector. A closure afterScript is left
 * alone (its tasks fall back to the head node) and returned for a warning.
 */
@CompileStatic
class NodeHash {

    static final String RESOURCE = '/robsyme/cas/node-hash.sh'

    private static final String SCRIPT = NodeHash.getResourceAsStream(RESOURCE).withCloseable { InputStream in -> in.getText('UTF-8') }.trim()

    static String script() { SCRIPT }

    /** Chains ours into config.process; the selectors whose afterScript is a closure, left alone. */
    static List<String> install(Map config) {
        Object process = config.get('process')
        if( !(process instanceof Map) ) {
            process = new LinkedHashMap<String, Object>()
            config.put('process', process)
        }
        final Map scope = (Map) process
        final List<String> skipped = []
        chain(scope, 'process', skipped, true)
        for( Object key : new ArrayList<Object>(scope.keySet()) ) {
            final String name = String.valueOf(key)
            if( (name.startsWith('withName:') || name.startsWith('withLabel:')) && scope.get(key) instanceof Map )
                chain((Map) scope.get(key), name, skipped, false)
        }
        return skipped
    }

    private static void chain(Map scope, String name, List<String> skipped, boolean top) {
        final Object current = scope.get('afterScript')
        if( current == null ) {
            if( top ) scope.put('afterScript', SCRIPT)
            return
        }
        if( !(current instanceof CharSequence) ) {
            skipped.add(name)
            return
        }
        final String text = current.toString()
        if( !text.startsWith(SCRIPT) )
            scope.put('afterScript', SCRIPT + '\n' + text)
    }
}
```

In `CasObserverFactory.create`, after `defaultOutputDir(session)`:

```groovy
        installNodeHash(session)
```

with

```groovy
    /** cas.nodeHash, default fusion.enabled (silent decision 5): the afterScript half of the Fusion provider. */
    static void installNodeHash(Session session) {
        final Map config = session?.config
        if( config == null || !CasConfig.nodeHashEnabled(config) )
            return
        try {
            for( String selector : NodeHash.install(config) )
                ConsoleLog.LOG.warn("nf-blocks: ${selector} sets a dynamic afterScript, so its tasks are not hashed on the node; the head node addresses their outputs")
        }
        catch( Exception e ) {
            log.warn("could not install node-side hashing; outputs are addressed on the head node: ${e.message}", e)
        }
    }
```

- [ ] **Step 4: Run the tests**

Run: `./gradlew test`
Expected: PASS (the script test runs where `/bin/bash` exists, including macOS's bash 3.2, which has no `globstar`; the `2>/dev/null || true` keeps it going).

- [ ] **Step 5: Commit**

```bash
git add src/main/resources/robsyme/cas/node-hash.sh src/main/groovy/robsyme/cas/trace/NodeHash.groovy \
  src/main/groovy/robsyme/cas/trace/CasObserverFactory.groovy src/test/groovy/robsyme/cas/trace/NodeHashTest.groovy \
  src/test/groovy/robsyme/cas/trace/NodeHashScriptTest.groovy src/test/groovy/robsyme/cas/trace/CasObserverFactoryTest.groovy
git commit -m "feat(fusion): hash declared outputs on the task node through an afterScript default

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10: The publish addresser: node digests, `s3-copy`, the head-node fallback

Ticket 16 decisions 3-6, spec §14, silent decisions 4, 7, 8, 12. Every published file, lone or inside a directory, gets its address through one `FileAddresser`, `PublishAddresser`, in the order spec §3 gives: the task node's digest (`.command.cas`), S3's own SHA-256 from a server-side copy, the head node's streamed read. `CasFileSystemProvider.upload` and `DirectoryManifestBuilder` (Task 3's seam) both use it, so a directory from an S3 work dir costs no head-node bytes either. `S3BlockStore.copyFrom` holds the copy logic of carried decision 14. The provider recorded per publish feeds `RunCompletion.providers` (Task 2) through `CasSession.Publish.provider`, and for a directory the providers of the files inside it through `CasSession.Publish.contents` (silent decision 3).

How an upload reaches here: for an S3 source and a `cas://` target the providers differ, S3's `canDownload` needs a default-filesystem target, so `FileHelper.copyPath` calls our `upload(source, target)` (`FileHelper.groovy:992-1019` at v26.04.6, `fusion-nextflow-integration.md` §4).

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/NodeDigests.groovy`
- Create: `src/main/groovy/robsyme/cas/nio/PublishAddresser.groovy`
- Modify: `src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy` (`copyFrom`)
- Modify: `src/main/groovy/robsyme/cas/s3/S3Types.groovy` (adds the top-level `S3Copied`)
- Modify: `src/main/groovy/robsyme/cas/nio/CasFileSystemProvider.groovy` (`upload`, `newOutputStream`)
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (`addresser`)
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (the head-node line in `writeCompletion`)
- Create: `src/test/groovy/robsyme/cas/core/NodeDigestsTest.groovy`, `src/test/groovy/robsyme/cas/nio/PublishAddresserTest.groovy`, `src/test/groovy/robsyme/cas/s3/S3CopyTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/nio/CasFileSystemProviderTest.groovy`

**Interfaces:**
- Consumes: `FileAddresser`, `Addressed`, `DirectoryManifestBuilder(BlockStore, FileAddresser, Closure<Path>)` (Task 3); `S3BlockStore`, `putFile`, `key` (Task 5); `Providers` (Task 2); `CasConfig.nodeHashEnabled` (Task 4); `nextflow.file.FilesEx.toUriString(Path)`, `nextflow.file.FileHelper.asPath(String)`, `Session.workDir`.
- Produces: `NodeDigests`, `PublishAddresser`, `S3BlockStore.copyFrom`, `S3Copied(Cid cid, String provider)` (shared interfaces; a top-level class in `S3Types.groovy`); `CasSession.Publish.contents` filled for a directory from `DirectoryManifestBuilder.Result.providers`; `CasSession.getAddresser()` (built lazily from `Global.session.workDir` and `CasConfig.nodeHashEnabled(session config)`); the info line of silent decision 8.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/NodeDigestsTest.groovy
package robsyme.cas.core

import spock.lang.Specification

class NodeDigestsTest extends Specification {

    static final String A = 'a' * 64
    static final String B = 'b' * 64

    private static Map<String, Cid> parse(String text, long max = NodeDigests.MAX_BYTES) {
        NodeDigests.parse(new ByteArrayInputStream(text.getBytes('UTF-8')), max)
    }

    def 'sha256sum lines, text and binary mode, with coreutils escaping; garbage is skipped (Review Focus 3)'() {
        when:
        final Map<String, Cid> d = parse("${A}  d/target.txt\n${B} *bin.dat\n\\${A}  back\\\\slash\\nline\n\\${B}  a\\\\nb\nnot a line\n${A}  name with space.txt\n")

        then: 'escapes are read left to right: an escaped backslash before n is a backslash, then n'
        d.keySet() == ['d/target.txt', 'bin.dat', 'back\\slash\nline', 'a\\nb', 'name with space.txt'] as Set
        d['d/target.txt'] == Cid.of(Cid.RAW, A.decodeHex())
        d['bin.dat'].digest == B.decodeHex()
    }

    def 'a file over the cap is ignored whole'() {
        expect:
        parse("${A}  x\n", 10L) == null
    }

    def 'the task directory is the first two segments under workDir'() {
        expect:
        NodeDigests.taskDirOf('s3://w/work/ab/cdef/d/x.txt', 's3://w/work').taskDir == 's3://w/work/ab/cdef'
        NodeDigests.taskDirOf('s3://w/work/ab/cdef/d/x.txt', 's3://w/work/').rel == 'd/x.txt'
        NodeDigests.taskDirOf('/tmp/work/ab/cdef/A.bam', '/tmp/work').rel == 'A.bam'
        NodeDigests.taskDirOf('/data/store/A.bam', '/tmp/work') == null
        NodeDigests.taskDirOf('/tmp/work/ab/A.bam', '/tmp/work') == null
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/s3/S3CopyTest.groovy
package robsyme.cas.s3

import java.nio.file.Path
import java.security.MessageDigest

import robsyme.cas.core.BlockMismatchException
import robsyme.cas.core.Cid
import spock.lang.Specification
import spock.lang.TempDir

/** Ticket 16 decisions 3-6 on the double. */
class S3CopyTest extends Specification {

    @TempDir Path tmp
    MemoryS3Ops member = new MemoryS3Ops('member')
    MemoryS3Ops work = new MemoryS3Ops('work')
    S3BlockStore store

    def setup() {
        member.peers['work'] = work
        store = new S3BlockStore(member, 'cas/', 'lab', true, tmp, 1L << 20)
        work.putText('w/ab/cd/A.bam', 'bam-A')
    }

    private static Cid cidOf(String text) { Cid.of(Cid.RAW, MessageDigest.getInstance('SHA-256').digest(text.bytes)) }

    def 'no digest in advance: copy to tmp/ with SHA-256, then to the final key, staging deleted; provider s3-copy'() {
        when:
        final S3Copied c = store.copyFrom('work', 'w/ab/cd/A.bam', 5L, null)

        then:
        c == new S3Copied(cidOf('bam-A'), 's3-copy')
        member.objects[store.key(c.cid)].text() == 'bam-A'
        member.objects.keySet().every { !it.startsWith('cas/tmp/') }
        member.calls.count { it.startsWith('COPY ') } == 2
        work.pulledBytes == 0
    }

    def 'a node digest: one copy straight to the final key, compared; s3-copy'() {
        expect:
        store.copyFrom('work', 'w/ab/cd/A.bam', 5L, cidOf('bam-A')) == new S3Copied(cidOf('bam-A'), 's3-copy')
        member.calls.count { it.startsWith('COPY ') } == 1
    }

    def 'a node digest for a block already there: a HEAD and nothing else; fusion-node'() {
        given:
        store.copyFrom('work', 'w/ab/cd/A.bam', 5L, null)
        member.calls.clear()

        expect:
        store.copyFrom('work', 'w/ab/cd/A.bam', 5L, cidOf('bam-A')) == new S3Copied(cidOf('bam-A'), 'fusion-node')
        member.calls == ["HEAD ${store.key(cidOf('bam-A'))}".toString()]
    }

    def 'a node digest that disagrees with S3: the copy is deleted and the publish fails'() {
        when:
        store.copyFrom('work', 'w/ab/cd/A.bam', 5L, cidOf('something else'))

        then:
        thrown(BlockMismatchException)
        member.objects.isEmpty()
    }

    def 'above the limit: UploadPartCopy with a node digest (fusion-node); without one, null'() {
        given:
        work.putText('w/big', 'x' * (3 << 20))

        expect:
        store.copyFrom('work', 'w/big', 3L << 20, null) == null
        store.copyFrom('work', 'w/big', 3L << 20, cidOf('x' * (3 << 20))) == new S3Copied(cidOf('x' * (3 << 20)), 'fusion-node')
        member.calls.count { it.startsWith('PARTCOPY ') } == 1
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/nio/PublishAddresserTest.groovy
package robsyme.cas.nio

import java.nio.file.Files
import java.nio.file.Path

import nextflow.exception.AbortRunException
import robsyme.cas.core.*
import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

class PublishAddresserTest extends Specification {

    @TempDir Path tmp
    LocalBlockStore lab

    def setup() { lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true) }

    private Path taskFile(String rel, String text) {
        final Path f = tmp.resolve('work/ab/cdef').resolve(rel)
        Files.createDirectories(f.parent); Files.writeString(f, text); f
    }

    private void nodeDigest(String rel, String text) {
        final String hex = java.security.MessageDigest.getInstance('SHA-256').digest(text.bytes).encodeHex().toString()
        Files.writeString(tmp.resolve('work/ab/cdef/.command.cas'), "${hex}  ${rel}\n")
    }

    def 'nodeHash off: the head node streams and hashes; bytes are counted'() {
        given:
        final PublishAddresser a = new PublishAddresser(lab, lab, false, tmp.resolve('work'))

        when:
        final Addressed r = a.address(taskFile('A.bam', 'bam-A'), 5L)

        then:
        r.provider == Providers.HEAD_NODE
        a.headNodeBytes == 5L
        a.counts == ['head-node': 1]
    }

    // setReadable(false) does nothing for root: a read would succeed and the feature prove nothing.
    @Requires({ System.getProperty('user.name') != 'root' })
    def 'a node digest for a block the member holds: no byte read; fusion-node'() {
        given:
        lab.putStreaming(new ByteArrayInputStream('bam-A'.bytes))
        final Path f = taskFile('A.bam', 'bam-A')
        nodeDigest('A.bam', 'bam-A')
        final PublishAddresser a = new PublishAddresser(lab, lab, true, tmp.resolve('work'))
        f.toFile().setReadable(false)     // a read would fail

        expect:
        a.address(f, 5L).provider == Providers.FUSION_NODE
        a.headNodeBytes == 0L
    }

    def 'a node digest for a block the member lacks: the head node reads, and must agree'() {
        given:
        final Path f = taskFile('A.bam', 'bam-A')
        nodeDigest('A.bam', 'bam-B')

        when:
        new PublishAddresser(lab, lab, true, tmp.resolve('work')).address(f, 5L)

        then:
        final AbortRunException e = thrown()
        e.message.contains('.command.cas')
        e.message.contains('A.bam')
    }

    def '.command.cas is read once per task directory'() {
        given:
        final Path x = taskFile('x', 'x'); final Path y = taskFile('y', 'y')
        final int[] reads = [0]
        final PublishAddresser a = new PublishAddresser(lab, lab, true, tmp.resolve('work')) {
            @Override protected InputStream openDigests(Path file) { reads[0]++; super.openDigests(file) }
        }

        when:
        a.address(x, 1L); a.address(y, 1L)

        then:
        reads[0] == 1
    }

    // setWritable(false) does nothing for root, so the spool would succeed.
    @Requires({ System.getProperty('user.name') != 'root' })
    def 'over the limit, no node digest, S3 member: the object is spooled through cas.tmpDir, and an unwritable tmpDir aborts naming it (Review Focus 1)'() {
        given:
        final Path readOnly = Files.createDirectories(tmp.resolve('ro'))
        readOnly.toFile().setWritable(false)
        final robsyme.cas.s3.MemoryS3Ops s3 = new robsyme.cas.s3.MemoryS3Ops('member')
        s3.peers['work'] = new robsyme.cas.s3.MemoryS3Ops('work')
        s3.peers['work'].putText('w/big', 'x' * (2 << 20))
        final robsyme.cas.s3.S3BlockStore member = new robsyme.cas.s3.S3BlockStore(s3, 'cas/', 'lab', true,
            readOnly, 1L << 20)
        final Path big = taskFile('big', 'x' * (2 << 20))
        // The local file stands in for s3://work/w/big: copyFrom answers null (over the limit, no digest).
        final PublishAddresser a = new PublishAddresser(member, member, false, tmp.resolve('work'), 1L << 20) {
            @Override protected List<String> copySource(Path file) { ['work', 'w/big'] }
        }

        when:
        a.address(big, 2L << 20)

        then:
        final AbortRunException e = thrown()
        e.message.contains('cas.tmpDir')
        e.message.contains(String.valueOf(2L << 20))
        s3.objects.isEmpty()

        cleanup:
        readOnly.toFile().setWritable(true)
    }

    private robsyme.cas.s3.S3BlockStore s3Member(robsyme.cas.s3.MemoryS3Ops s3, String text) {
        s3.peers['work'] = new robsyme.cas.s3.MemoryS3Ops('work')
        s3.peers['work'].putText('w/ab/cdef/A.bam', text)
        return new robsyme.cas.s3.S3BlockStore(s3, 'cas/', 'lab', true, tmp.resolve('spool'))
    }

    def 'a copy whose SHA-256 disagrees with .command.cas aborts the run, naming the file and .command.cas (ticket 16 decision 3)'() {
        given:
        final robsyme.cas.s3.MemoryS3Ops s3 = new robsyme.cas.s3.MemoryS3Ops('member')
        final robsyme.cas.s3.S3BlockStore member = s3Member(s3, 'bam-A')
        final Path f = taskFile('A.bam', 'bam-A')
        nodeDigest('A.bam', 'bam-B')
        final PublishAddresser a = new PublishAddresser(member, member, true, tmp.resolve('work')) {
            @Override protected List<String> copySource(Path file) { ['work', 'w/ab/cdef/A.bam'] }
        }

        when:
        a.address(f, 5L)

        then:
        final AbortRunException e = thrown()
        e.message.contains('A.bam')
        e.message.contains('.command.cas')
        e.cause instanceof BlockMismatchException
    }

    def 'a copy that fails for another reason warns and falls back to the head-node read'() {
        given:
        final robsyme.cas.s3.MemoryS3Ops s3 = new robsyme.cas.s3.MemoryS3Ops('member') {
            @Override robsyme.cas.s3.S3Written copy(String sb, String sk, String key, robsyme.cas.s3.S3PutOptions o) {
                throw new IOException('S3 is having a day')
            }
        }
        final robsyme.cas.s3.S3BlockStore member = s3Member(s3, 'bam-A')
        final Path f = taskFile('A.bam', 'bam-A')
        final PublishAddresser a = new PublishAddresser(member, member, false, tmp.resolve('work')) {
            @Override protected List<String> copySource(Path file) { ['work', 'w/ab/cdef/A.bam'] }
        }

        when:
        final Addressed r = a.address(f, 5L)

        then:
        r.provider == Providers.HEAD_NODE
        a.headNodeBytes == 5L
        s3.objects[member.key(r.cid)].text() == 'bam-A'
    }
}
```

In `CasFileSystemProviderTest`, add one feature, with no new seam: publish a directory of three files through `upload` on the test `CasSession`, then assert that `session.addresser.counts == ['head-node': 3]` (every file inside went through the addresser), that the recorded `session.publishFor(key).provider == 'head-node'` (a directory leaf's address, its manifest, is always `head-node`: silent decision 3, the rule `JoinTest` no longer claims), and that `session.publishFor(key).contents == ['head-node': <the three file CIDs, sorted by string>]` (silent decision 3, ticket 16 decision 1).

Run: `./gradlew test --tests 'robsyme.cas.core.NodeDigestsTest' --tests 'robsyme.cas.s3.S3CopyTest' --tests 'robsyme.cas.nio.*'`
Expected: compilation FAILS on `NodeDigests`, `copyFrom`, `PublishAddresser`.

- [ ] **Step 2: `NodeDigests` and `copyFrom`**

```groovy
// src/main/groovy/robsyme/cas/core/NodeDigests.groovy
package robsyme.cas.core

import java.nio.charset.StandardCharsets

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/** The task node's .command.cas (spec §14, silent decision 7). */
@CompileStatic
class NodeDigests {

    static final long MAX_BYTES = 16L << 20
    private static final java.util.regex.Pattern LINE = ~/^(\\?)([0-9a-f]{64}) [ *](.*)$/

    @Canonical
    static class TaskPath { String taskDir; String rel }

    /**
     * rel path to raw CID; unparseable lines skipped. Read line by line, counting
     * bytes, so the file is never held whole (rule 2's spirit for metadata);
     * null once more than maxBytes have arrived: the caller warns and ignores it.
     */
    static Map<String, Cid> parse(InputStream input, long maxBytes) {
        final long[] seen = [0L] as long[]
        final InputStream counted = new FilterInputStream(input) {
            @Override int read() { final int b = super.read(); if( b >= 0 ) seen[0]++; return b }
            @Override int read(byte[] b, int off, int len) { final int n = super.read(b, off, len); if( n > 0 ) seen[0] += n; return n }
        }
        final BufferedReader reader = new BufferedReader(new InputStreamReader(counted, StandardCharsets.UTF_8))
        final Map<String, Cid> out = new HashMap<String, Cid>()
        String line
        while( (line = reader.readLine()) != null ) {
            if( seen[0] > maxBytes ) return null
            final java.util.regex.Matcher m = LINE.matcher(line)
            if( !m.matches() ) continue
            final String name = m.group(1) ? unescape(m.group(3)) : m.group(3)
            if( name ) out.put(name, Cid.of(Cid.RAW, m.group(2).decodeHex()))
        }
        return seen[0] > maxBytes ? null : out
    }

    /** coreutils' escaping in one left-to-right pass: a backslash then n is a newline, two backslashes one backslash. */
    private static String unescape(String s) {
        final StringBuilder out = new StringBuilder(s.length())
        for( int i = 0; i < s.length(); i++ ) {
            final char c = s.charAt(i)
            if( c == (char) '\\' && i + 1 < s.length() ) {
                final char next = s.charAt(++i)
                out.append(next == (char) 'n' ? (char) '\n' : next)
            }
            else {
                out.append(c)
            }
        }
        return out.toString()
    }

    /** The task directory and relative path of a source under workDir, or null. */
    static TaskPath taskDirOf(String sourceUri, String workDirUri) {
        final String base = workDirUri.replaceAll('/+$', '') + '/'
        if( !sourceUri.startsWith(base) ) return null
        final String[] segs = sourceUri.substring(base.length()).split('/', 3)
        if( segs.length < 3 || !segs[2] ) return null
        return new TaskPath(base + segs[0] + '/' + segs[1], segs[2])
    }
}
```

(The unescape is a single left-to-right scan: two `replace` calls get an escaped backslash before `n` wrong, and the test's `a\\nb` line fails them.)

In `S3Types.groovy`, beside the other value types, add the top-level class (so `import robsyme.cas.s3.S3Copied` and the bare name in `S3CopyTest` both resolve):

```groovy
/** What a server-side copy into the member addressed, and which provider supplied the address (Task 10). */
@Canonical
@CompileStatic
class S3Copied {
    Cid cid
    String provider
}
```

(`S3Types.groovy` imports `robsyme.cas.core.Cid` for it.) In `S3BlockStore` add:

```groovy
    /**
     * An S3 source into this member without the bytes leaving S3 (ticket 16).
     * Up to the single-request limit, CopyObject with SHA-256: straight to the
     * final key when the node's digest names it (compared, deleted and refused
     * on mismatch), else through tmp/<uuid>. Above it, UploadPartCopy when a
     * node digest names the key; otherwise null, and the caller reads the bytes.
     */
    S3Copied copyFrom(String sourceBucket, String sourceKey, long size, Cid expected) {
        checkWritable()
        if( expected != null && ops.head(key(expected)) != null )
            return new S3Copied(expected, Providers.FUSION_NODE)
        if( size > singleRequestMax )
            return expected == null ? null : new S3Copied(partCopy(sourceBucket, sourceKey, size, expected), Providers.FUSION_NODE)
        if( expected != null ) {
            final S3Written w = copyRetrying(sourceBucket, sourceKey, key(expected))
            if( w.status == S3Written.Status.WRITTEN && Base64.decoder.decode(w.sha256) != expected.digest ) {
                ops.delete(key(expected))
                throw new BlockMismatchException(expected, ".command.cas says ${expected}, S3 hashed s3://${sourceBucket}/${sourceKey} as ${w.sha256}")
            }
            return new S3Copied(expected, w.status == S3Written.Status.WRITTEN ? Providers.S3_COPY : Providers.FUSION_NODE)
        }
        final String staging = "${prefix}tmp/${UUID.randomUUID()}".toString()
        try {
            final S3Written staged = copyRetrying(sourceBucket, sourceKey, staging)
            if( staged.sha256 == null )
                throw new IOException("S3 returned no SHA-256 copying s3://${sourceBucket}/${sourceKey}")
            final Cid cid = Cid.of(Cid.RAW, Base64.decoder.decode(staged.sha256))
            if( ops.head(key(cid)) == null )
                copyRetrying(ops.bucket, staging, key(cid))
            return new S3Copied(cid, Providers.S3_COPY)
        }
        finally {
            ops.delete(staging)
        }
    }

    private S3Written copyRetrying(String bucket, String srcKey, String dstKey) {
        for( int attempt = 1; attempt <= ATTEMPTS; attempt++ ) {
            final S3Written w = ops.copy(bucket, srcKey, dstKey, S3PutOptions.create().ifNoneMatch().sha256().cacheControl(IMMUTABLE))
            if( w.status != S3Written.Status.CONFLICT ) return w
        }
        throw new IOException("S3 answered 409 ConditionalRequestConflict ${ATTEMPTS} times copying to ${ops.describe()}/${dstKey}")
    }

    private Cid partCopy(String bucket, String srcKey, long size, Cid expected) {
        final String k = key(expected)
        final String id = ops.createMultipart(k, S3PutOptions.create().cacheControl(IMMUTABLE))
        try {
            final long part = partSize(size)
            final List<S3Part> parts = []
            long first = 0
            for( int n = 1; first < size; n++ ) {
                final long last = Math.min(first + part, size) - 1
                parts.add(ops.uploadPartCopy(k, id, n, bucket, srcKey, first, last))
                first = last + 1
            }
            if( ops.completeMultipart(k, id, parts, true).status != S3Written.Status.WRITTEN )
                ops.abortMultipart(k, id)
            return expected
        }
        catch( Exception e ) {
            ops.abortMultipart(k, id)
            throw e
        }
    }
```

(The staging copy to the final key can answer EXISTS when a racing writer placed it: that is success. The staging copy's own `If-None-Match` never matters, the uuid key being new. A `sha256` of null from the staging copy is an S3 that returned no SHA-256: `copyFrom` throws `IOException` naming the source, and `PublishAddresser.address` catches it, warns and falls back to the head-node read.)

- [ ] **Step 3: `PublishAddresser`, and the provider through it**

```groovy
// src/main/groovy/robsyme/cas/nio/PublishAddresser.groovy
package robsyme.cas.nio

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortRunException
import nextflow.file.FileHelper
import nextflow.file.FilesEx
import robsyme.cas.core.*
import robsyme.cas.s3.S3BlockStore
import robsyme.cas.s3.S3Copied

/**
 * The Address Provider seam for a run's publishes (spec §3, ticket 16): the
 * node's digest, S3's SHA-256 from a copy, the head node's read, in that order.
 * A node digest and a computed address must agree, or the run aborts (rule 3).
 */
@Slf4j
@CompileStatic
class PublishAddresser implements FileAddresser {

    private final BlockStore store
    private final BlockStore writable
    private final boolean nodeHash
    private final String workDirUri
    private final long singleRequestMax
    private final ConcurrentHashMap<String, Map<String, Cid>> digests = new ConcurrentHashMap<>()
    private final AtomicLong headNodeBytes = new AtomicLong()
    private final ConcurrentHashMap<String, AtomicLong> counts = new ConcurrentHashMap<>()

    PublishAddresser(BlockStore store, BlockStore writable, boolean nodeHash, Path workDir,
                     long singleRequestMax = S3BlockStore.SINGLE_REQUEST_MAX) {
        this.store = store; this.writable = writable; this.nodeHash = nodeHash
        // Compared as real paths: a work dir under a symlink (macOS /var) must still match (pre-flight F40).
        this.workDirUri = workDir == null ? null : FilesEx.toUriString(DirectoryManifestBuilder.realOf(workDir))
        this.singleRequestMax = singleRequestMax
    }

    long getHeadNodeBytes() { headNodeBytes.get() }

    Map<String, Integer> getCounts() { counts.collectEntries { k, v -> [(k): (int) v.get()] } as Map<String, Integer> }

    String summary() {
        final Map<String, Integer> c = getCounts()
        final int files = (int) c.values().sum(0)
        return "nf-blocks: the head node read ${headNodeBytes.get()} bytes to address ${files} file(s); " +
            Providers.ALL.collect { "${it} ${c.get(it) ?: 0}" }.join(', ')
    }

    @Override
    Addressed address(Path file, long size) {
        final Cid node = nodeHash ? nodeDigest(file) : null
        final List<String> source = copySource(file)
        if( writable instanceof S3BlockStore && source != null ) {
            try {
                final S3Copied c = ((S3BlockStore) writable).copyFrom(source[0], source[1], size, node)
                if( c != null ) return counted(new Addressed(c.cid, size, c.provider))
            }
            catch( BlockMismatchException e ) {
                // Ticket 16 decision 3, rule 3: the node's digest and S3's disagree.
                throw new AbortRunException("${file}: .command.cas says ${node}, but S3's SHA-256 of the copy disagrees (${e.message}); the output changed after the task hashed it", e)
            }
            catch( IOException e ) {
                log.warn("server-side copy of ${file} failed (${e.message}); the head node reads it instead")
            }
        }
        // The writable member, not the composite: a block held only by a read-only member must still be written (DESIGN §5).
        if( node != null && writable.has(node) )
            return counted(new Addressed(node, size, Providers.FUSION_NODE))
        final Cid cid = headNodeRead(file, source != null)
        if( node != null && node != cid )
            throw new AbortRunException("${file}: .command.cas says ${node}, the head node hashed ${cid}; the output changed after the task hashed it")
        headNodeBytes.addAndGet(size)
        return counted(new Addressed(cid, size, Providers.HEAD_NODE))
    }

    /** [bucket, key] of an S3 source, or null; a seam for tests. */
    protected List<String> copySource(Path file) {
        final String uri = FilesEx.toUriString(file)
        if( !uri.startsWith('s3://') ) return null
        final int slash = uri.indexOf('/', 5)
        return slash < 0 ? null : [uri.substring(5, slash), uri.substring(slash + 1)]
    }

    /** A local file into an S3 member needs no spool; an object-store source is spooled (rule 2). */
    private Cid headNodeRead(Path file, boolean objectSource) {
        try {
            if( writable instanceof S3BlockStore && !objectSource && file.fileSystem == FileSystems.default )
                return ((S3BlockStore) writable).putFile(file)
            final InputStream in = Files.newInputStream(file)
            try { return writable.putStreaming(in) } finally { in.close() }
        }
        catch( IOException e ) {
            if( e.message?.contains('cas.tmpDir') )
                throw new AbortRunException("${file}: ${e.message}; S3 cannot copy it server-side (over ${singleRequestMax} bytes, no node digest), so it passes through cas.tmpDir, which needs ${Files.size(file)} bytes free", e)
            throw e
        }
    }

    private Addressed counted(Addressed a) {
        counts.computeIfAbsent(a.provider, { new AtomicLong() }).incrementAndGet()
        return a
    }

    private Cid nodeDigest(Path file) {
        if( workDirUri == null ) return null
        final Path real = file.parent == null ? file : DirectoryManifestBuilder.realOf(file.parent).resolve(file.fileName.toString())
        final NodeDigests.TaskPath tp = NodeDigests.taskDirOf(FilesEx.toUriString(real), workDirUri)
        if( tp == null ) return null
        return digests.computeIfAbsent(tp.taskDir, { String dir -> load(dir) }).get(tp.rel)
    }

    private Map<String, Cid> load(String taskDir) {
        try {
            final InputStream in = openDigests(FileHelper.asPath(taskDir + '/.command.cas'))
            try {
                final Map<String, Cid> d = NodeDigests.parse(in, NodeDigests.MAX_BYTES)
                if( d == null ) {
                    log.warn("${taskDir}/.command.cas is over ${NodeDigests.MAX_BYTES} bytes and is ignored; the head node addresses that task's outputs")
                    return Collections.<String, Cid> emptyMap()
                }
                if( d.isEmpty() ) log.debug("no usable node digests in ${taskDir}/.command.cas")
                return d
            }
            finally { in.close() }
        }
        catch( NoSuchFileException e ) {
            return Collections.emptyMap()
        }
        catch( IOException e ) {
            log.warn("could not read ${taskDir}/.command.cas (${e.message}); the head node addresses that task's outputs")
            return Collections.emptyMap()
        }
    }

    /** A seam: where .command.cas is opened. */
    protected InputStream openDigests(Path file) { Files.newInputStream(file) }
}
```

(A `.command.cas` over `MAX_BYTES` parses to null and is warned about once per task directory, per silent decision 7.)

In `CasSession`, add

```groovy
    private volatile PublishAddresser addresser

    /** The run's Address Provider seam (DESIGN.md §8), built on first publish. */
    PublishAddresser getAddresser() {
        if( addresser == null ) synchronized( this ) {
            if( addresser == null ) {
                final Session s = Global.session as Session
                addresser = new PublishAddresser(store, members()[0], CasConfig.nodeHashEnabled(s?.config ?: config.rawConfig), s?.workDir)
            }
        }
        return addresser
    }
```

In `CasFileSystemProvider.upload`, the directory branch builds `new DirectoryManifestBuilder(store(), session().addresser, { String uri -> FileHelper.asPath(uri) } as Closure<Path>)` and records the directory with `Providers.HEAD_NODE` and the walk's providers: `new CasSession.Publish(new StoreRef(result.cid, name), store().size(result.cid), Providers.HEAD_NODE, result.providers)`, so `RunCompletion.providers` covers the files inside it (silent decision 3); the file branch becomes

```groovy
            final Addressed a = session().addresser.address(source, Files.size(source))
            tree.write(rel, new StoreRef(a.cid, name))
            session().recordPublish(key, new CasSession.Publish(new StoreRef(a.cid, name), a.size, a.provider))
            log.debug "cas: published file ${source} as block ${a.cid} at ${key} (${a.provider})"
```

`newOutputStream`'s close records `Providers.HEAD_NODE` instead of the literal. In `CasObserver.writeCompletion`, after `appendStoreLog(completion)`: `log.info(cas.addresser.summary())`.

- [ ] **Step 4: Run the tests**

Run: `./gradlew test`
Expected: PASS.

Run: `make gate`
Expected: lineage 11/0/6, tier A 5/5, tier B 12/12. Every Gate publish is local-to-local with `nodeHash` off, so each is `head-node`, as before; the new info line is in each run's `nextflow.log`.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/NodeDigests.groovy src/main/groovy/robsyme/cas/nio/PublishAddresser.groovy \
  src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy src/main/groovy/robsyme/cas/s3/S3Types.groovy \
  src/main/groovy/robsyme/cas/nio/CasFileSystemProvider.groovy \
  src/main/groovy/robsyme/cas/CasSession.groovy src/main/groovy/robsyme/cas/trace/CasObserver.groovy \
  src/test/groovy/robsyme/cas/core/NodeDigestsTest.groovy src/test/groovy/robsyme/cas/nio/PublishAddresserTest.groovy \
  src/test/groovy/robsyme/cas/s3/S3CopyTest.groovy src/test/groovy/robsyme/cas/nio/CasFileSystemProviderTest.groovy
git commit -m "feat(publish): node digest, s3-copy and head-node read behind one addresser

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: Wiring members into the session, the observer, the lineage store and the verbs

Tickets 02 (decisions 1, 7, 8), 03 (decisions 1, 2), 04 (decisions 1-3, 5), silent decisions 16, 19, 20. Every piece now exists; this task makes a run, `explore`, `put`, `snapshot` and `items` use them. `CasSession` builds each member from `CasConfig` (a `LocalBlockStore` or an `S3BlockStore` over `S3Access.open`), with its coordinate tree and snapshot storage beside it; its catch-up seeds from snapshots; its snapshot write takes the base before the catch-up and skips on a failed catch-up. The observer checks the writable S3 member's clock at `onFlowCreate`. `CasLinStore` passes `nf/` as a URI string, so `DefaultLinStore` works on `s3://` (`s3-writable-store.md` §6). The explorer keeps working against an S3 writable member because its `put` path writes through `CasSession.newPut`, whose writable member is now whatever the config names.

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/ClockSkew.groovy`, `src/main/groovy/robsyme/cas/ClockSkewException.groovy`
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy`
- Modify: `src/main/groovy/robsyme/cas/CasConfig.groovy` (delete `localLocations()`)
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (`onFlowCreate`, `indexRun`, `writeSnapshot`)
- Modify: `src/main/groovy/robsyme/cas/lineage/CasLinStore.groovy` (`open`, `openRecords`)
- Modify: `src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy` (`start`, `refresh`, `membersOf`'s factory)
- Modify: `src/main/groovy/robsyme/cas/cli/CasCommands.groovy` (`snapshot`, `put`)
- Create: `src/test/groovy/robsyme/cas/core/ClockSkewTest.groovy`, `src/test/groovy/robsyme/cas/CasSessionS3Test.groovy`
- Modify: `src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy`, `src/test/groovy/robsyme/cas/lineage/CasLinStoreTest.groovy`, `src/test/groovy/robsyme/cas/explore/ExploreWriteTest.groovy`, `src/test/groovy/robsyme/cas/cli/CasCommandsTest.groovy`

**Interfaces:**
- Consumes: `CasConfig` (Task 4), `S3Access`, `MemoryS3Ops` (Task 1), `S3BlockStore` (5), `S3CoordinateTree` (6), `SnapshotStorage`, `LocalSnapshotStorage`, `S3SnapshotStorage`, the six-argument `IndexSnapshot.write` (7), the five-argument `Index.catchUp` (8), `PublishAddresser` (10), `S3Ops.firstServerDateMillis()`.
- Produces: the `CasSession` members listed in the shared interfaces; `ClockSkew`; `class ClockSkewException extends nextflow.exception.AbortRunException` (in `robsyme.cas`); `CasLinStore.recordsLocation(CasConfig, String alias) -> String`.

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/core/ClockSkewTest.groovy
package robsyme.cas.core

import spock.lang.Specification

/** Ticket 03 decision 2: warn above 1 minute, abort above 5. */
class ClockSkewTest extends Specification {

    def 'skew of #seconds s is #verdict'() {
        expect:
        ClockSkew.judge(1_000_000_000L + seconds * 1000L, 1_000_000_000L) == verdict
        ClockSkew.judge(1_000_000_000L - seconds * 1000L, 1_000_000_000L) == verdict

        where:
        seconds | verdict
        0       | ClockSkew.Verdict.OK
        60      | ClockSkew.Verdict.OK
        61      | ClockSkew.Verdict.WARN
        300     | ClockSkew.Verdict.WARN
        301     | ClockSkew.Verdict.ABORT
    }

    def 'the description names the direction, the size and the fix'() {
        expect:
        ClockSkew.describe(1_000_400_000L, 1_000_000_000L).contains('400 s ahead of')
        ClockSkew.describe(1_000_000_000L, 1_000_400_000L).contains('400 s behind')
        ClockSkew.describe(0L, 1L).contains('NTP')
    }
}
```

```groovy
// src/test/groovy/robsyme/cas/CasSessionS3Test.groovy
package robsyme.cas

import java.nio.file.Path

import robsyme.cas.core.*
import robsyme.cas.s3.*
import spock.lang.Specification
import spock.lang.TempDir

/** A composition with a writable S3 member, on the test double (no AWS). */
class CasSessionS3Test extends Specification {

    @TempDir Path tmp
    Map<String, MemoryS3Ops> buckets = [:]
    Closure<S3Ops> saved

    def setup() {
        saved = CasSession.s3OpsFactory
        CasSession.s3OpsFactory = { Map config, String bucket -> buckets.computeIfAbsent(bucket) { new MemoryS3Ops(bucket) } } as Closure<S3Ops>
    }

    def cleanup() { CasSession.s3OpsFactory = saved }

    private CasSession session(Map extra = [:]) {
        final Map cfg = [cas: [stores: [lab: [location: 's3://member/cas'], shared: [location: tmp.resolve('shared').toString()]],
                               index: [path: tmp.resolve('cache.sqlite').toString()], tmpDir: tmp.resolve('t').toString()]]
        cfg.putAll(extra)
        return new CasSession(CasConfig.from(cfg, 'cas://lab'))
    }

    def 'members are built from config: S3 writable first, local read-only after'() {
        when:
        final CasSession s = session()

        then:
        s.members()*.class == [S3BlockStore, LocalBlockStore]
        s.members()[0].isWritable() && !s.members()[1].isWritable()
        s.coordinatesOf('lab') instanceof S3CoordinateTree
        s.coordinatesOf('shared') instanceof LocalCoordinateTree
        s.snapshotsOf('lab') instanceof S3SnapshotStorage
    }

    def 'a Selection put through the session lands in the bucket with its Store Log entry, and the snapshot follows'() {
        given:
        final CasSession s = session()
        final Cid item = s.members()[0].putDagCbor(Fixtures.outputItem2([[sample: 'A']]))
        final Index index = s.openIndex()

        when:
        final PutResult r = s.newPut(index).put(DagJson.encode([members: [[item: [address: item, via: []]]], derived_from: []]), false)
        final SnapshotBase base = s.snapshotBase()
        final Set<String> failed = s.catchUpIndex(index)
        final IndexSnapshot.Result snap = s.snapshotWritable(index, 0L, base, failed)

        then:
        buckets['member'].objects.keySet().any { it.startsWith('cas/log/') && it.contains('-selection-') }
        snap.written
        buckets['member'].objects['cas/index/v3.sqlite'].metadata.runs == '0'

        cleanup:
        index?.close()
    }

    def 'a failed catch-up of the writable member keeps the old snapshot (catch_up_failed)'() {
        given:
        final CasSession s = session()
        final Index index = s.openIndex()

        expect:
        s.snapshotWritable(index, 0L, s.snapshotBase(), ['lab'] as Set).skipped == IndexSnapshot.CATCH_UP_FAILED

        cleanup:
        index?.close()
    }

    def 'the clock check: a skew over 5 minutes throws'() {
        given:
        final CasSession s = session()
        buckets['member'].serverDateMillis = System.currentTimeMillis() - 400_000L

        when:
        s.checkClock()

        then:
        final ClockSkewException e = thrown()
        e.message.contains('behind') || e.message.contains('ahead of')
    }

    def 'the clock check: a skew over 1 minute and under 5 does not throw'() {
        given:
        final CasSession s = session()
        buckets['member'].serverDateMillis = System.currentTimeMillis() - 90_000L

        when:
        s.checkClock()

        then:
        noExceptionThrown()
        buckets['member'].calls.contains('HEAD cas/' + IndexSnapshot.relativePath())
    }

    def 'the clock check: a HEAD that fails (403, network) warns and continues'() {
        given:
        buckets['member'] = new MemoryS3Ops('member') {
            @Override S3Head head(String key) { throw new IOException('403 Forbidden') }
        }
        final CasSession s = session()

        when:
        s.checkClock()

        then:
        noExceptionThrown()
    }

    def 'the clock check: a local writable member makes no request'() {
        given:
        final CasSession s = new CasSession(CasConfig.from([cas: [stores: [lab: [location: tmp.resolve('local').toString()]],
            index: [path: tmp.resolve('local.sqlite').toString()]]], 'cas://lab'))

        when:
        s.checkClock()

        then:
        noExceptionThrown()
        buckets.isEmpty()
    }

    def 'the cache file is named by the location texts, S3 ones included'() {
        expect:
        session().config.locationTexts() == ['s3://member/cas', tmp.resolve('shared').toString()]
    }
}
```

(`MemoryS3Ops.serverDateMillis` answers `firstServerDateMillis()`; `checkClock` makes one `head` first so a real `SdkS3Ops` has seen a response.)

Add to `CasObserverTest`: `onFlowCreate` with an S3 writable member whose double reports a 400 s skew throws `AbortRunException`; `onFlowComplete` with an index whose `catchUp` of the writable member throws writes no snapshot and logs `catch_up_failed` at info. Add to `CasLinStoreTest`: `CasLinStore.recordsLocation(config, 'lab') == 's3://member/cas/nf'` for an S3 writable member and `== <dir>/nf` for a local one. Add to `ExploreWriteTest` one feature: the write endpoint over an S3 writable member (a `CasSession` built with the `s3OpsFactory` seam, `ExploreCommand.membersOf(config, factory)`) writes a Selection whose block and Store Log entry are in the double's bucket, and `GET /m/lab/blocks/<xx>/<cid>` serves it back. Add to `CasCommandsTest`: `snapshot` prints `not rewritten: fewer_runs` and exits 0 when the stored snapshot has more runs than the cache.

Run: `./gradlew test --tests 'robsyme.cas.core.ClockSkewTest' --tests 'robsyme.cas.CasSessionS3Test'`
Expected: compilation FAILS (`ClockSkew`, `s3OpsFactory`, `snapshotsOf`, `snapshotBase`, `checkClock`).

- [ ] **Step 2: `ClockSkew` and the session**

```groovy
// src/main/groovy/robsyme/cas/core/ClockSkew.groovy
package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * The writer's clock against S3's (ticket 03 decision 2). Store Log entries
 * carry the writer's clock and a catch-up re-reads 10 minutes behind a
 * watermark, so an entry more than that behind others' could be missed by
 * fromStore(latest); 5 minutes leaves room for two writers skewed opposite ways.
 */
@CompileStatic
class ClockSkew {
    enum Verdict { OK, WARN, ABORT }

    static final long WARN_MILLIS = 60_000L
    static final long ABORT_MILLIS = 300_000L

    static Verdict judge(long localMillis, long serverMillis) {
        final long skew = Math.abs(localMillis - serverMillis)
        return skew > ABORT_MILLIS ? Verdict.ABORT : skew > WARN_MILLIS ? Verdict.WARN : Verdict.OK
    }

    static String describe(long localMillis, long serverMillis) {
        final long s = Math.abs(localMillis - serverMillis).intdiv(1000L)
        final String way = localMillis >= serverMillis ? 'ahead of' : 'behind'
        return "this machine's clock is ${s} s ${way} S3's (the Date header of its first response); Store Log entries " +
            'are stamped with the local clock, and a skew over 5 minutes can hide a run from fromStore(latest). Set the clock by NTP.'
    }
}
```

In `CasSession`:

```groovy
    /** A test seam: the S3Ops for a bucket, from the loaded config map. */
    static Closure<S3Ops> s3OpsFactory = { Map config, String bucket -> S3Access.open(config, bucket) } as Closure<S3Ops>

    private final Map<String, CoordinateTree> trees = new LinkedHashMap<>()
    private final Map<String, SnapshotStorage> snapshots = new LinkedHashMap<>()

    CasSession(CasConfig config) {
        this.config = config
        this.assertedBy = config.assertedBy
        final List<BlockStore> members = new ArrayList<>()
        for( String alias : config.members ) {
            final boolean writable = alias == config.writableAlias
            if( config.isRemote(alias) ) {
                final S3Location at = config.remoteOf(alias)
                final S3Ops ops = s3OpsFactory.call(config.rawConfig, at.bucket)
                members.add(new S3BlockStore(ops, at.prefix, alias, writable, config.tmpDir))
                trees.put(alias, new S3CoordinateTree(ops, at.prefix))
                snapshots.put(alias, new S3SnapshotStorage(ops, at.prefix))
            }
            else {
                final Path root = config.locationOf(alias)
                members.add(new LocalBlockStore(root, alias, writable))
                trees.put(alias, new LocalCoordinateTree(root.resolve('coords')))
                snapshots.put(alias, new LocalSnapshotStorage(root))
            }
        }
        this.store = new CompositeStore(members)
        this.coordinates = trees.get(config.writableAlias)
        if( config.storageClassWarning ) warnOnce(config.storageClassWarning)
    }
```

(The test-seam constructor `CasSession(CasConfig, BlockStore, CoordinateTree)` also fills `trees` and `snapshots` for its writable alias, a `LocalSnapshotStorage` over the store's root when it is a `LocalBlockStore`.) Then:

```groovy
    CoordinateTree coordinatesOf(String alias) {
        final CoordinateTree t = trees.get(alias)
        if( t == null )
            throw new IllegalArgumentException("Unknown store alias '${alias}' -- configured stores: ${config.members.join(', ')}")
        return t
    }

    SnapshotStorage snapshotsOf(String alias) { snapshots.get(alias) }

    /** Taken before the catch-up, so the guard covers it (silent decision 16). Null when there is none, or it cannot be looked at. */
    SnapshotBase snapshotBase() {
        try {
            return snapshotsOf(config.writableAlias).base(config.tmpDir)
        }
        catch( Exception e ) {
            log.warn("could not look at the Index Snapshot of '${config.writableAlias}'; it is derived: ${e.message}")
            return null
        }
    }

    /** Catches up every member, seeding cold ones from their snapshots (ticket 04). The aliases that failed. */
    Set<String> catchUpIndex(Index index) {
        final Set<String> failed = new LinkedHashSet<String>()
        for( BlockStore member : members() ) {
            try {
                index.catchUp(member, StoreLog.of(member), member.alias(), snapshotsOf(member.alias()), config.tmpDir)
            }
            catch( Exception e ) {
                failed.add(member.alias())
                log.warn("could not catch up the index from store member '${member.alias()}'; it is derived: ${e.message}", e)
            }
        }
        return failed
    }

    Index openIndex() {
        return Index.open(IndexPaths.cachePath(config.locationTexts(), config.indexOverride))
    }

    /** DESIGN.md §15 and ticket 04 decision 3; maxBytes <= 0 writes at any size. */
    IndexSnapshot.Result snapshotWritable(Index index, long maxBytes, SnapshotBase base, Set<String> failed) {
        if( failed?.contains(config.writableAlias) )
            return new IndexSnapshot.Result(false, null, 0L, -1, null, IndexSnapshot.CATCH_UP_FAILED)
        final SnapshotStorage storage = snapshotsOf(config.writableAlias)
        final IndexSnapshot.Result result = IndexSnapshot.write(index, config.writableAlias, storage, maxBytes, base, config.tmpDir)
        if( result.written ) {
            final byte[] page = IndexSnapshot.bundledPage()
            if( page != null ) storage.writePage(page)
            else warnOnce('this build of nf-blocks carries no explorer page; the snapshot was written without index.html')
        }
        return result
    }

    /** The writable S3 member's clock check (ticket 03 decision 2); nothing for a local member. */
    void checkClock() {
        final BlockStore writable = members()[0]
        if( !(writable instanceof S3BlockStore) ) return
        final S3BlockStore s3 = (S3BlockStore) writable
        try {
            s3.ops.head(s3.prefix + IndexSnapshot.relativePath())
        }
        catch( Exception e ) {
            // A 403 on a missing key without s3:ListBucket, or the network: the check is advice, and
            // a real outage is reported by the first write (silent decision 19).
            ConsoleLog.LOG.warn("could not check this machine's clock against S3 (${e.message}); continuing")
            return
        }
        final Long server = s3.ops.firstServerDateMillis()
        if( server == null ) return
        final long now = System.currentTimeMillis()
        switch( ClockSkew.judge(now, server) ) {
            case ClockSkew.Verdict.ABORT: throw new ClockSkewException(ClockSkew.describe(now, server))
            case ClockSkew.Verdict.WARN: ConsoleLog.LOG.warn(ClockSkew.describe(now, server)); break
            default: break
        }
    }
```

Delete `buildStore` and the two-argument `snapshotWritable`. `ClockSkewException extends AbortRunException` lives in `src/main/groovy/robsyme/cas/ClockSkewException.groovy`. `ConsoleLog` is `robsyme.cas.trace.ConsoleLog`. In `CasConfig`, delete `localLocations()`.

- [ ] **Step 3: The observer, the lineage store and the verbs**

`CasObserver.onFlowCreate`, after `validateOutputDir()`:

```groovy
        // Ticket 03 decision 2: one HEAD on the writable S3 member, before any entry is stamped.
        cas.checkClock()
```

(`ClockSkewException` is an `AbortRunException`, so the run aborts with its message; any other failure of the `HEAD` is caught inside `checkClock`, which warns and returns, so the observer, `put` and `explore` all continue without a catch of their own.) `indexRun` becomes:

```groovy
    private void indexRun(Cid completion) {
        Index index = null
        try {
            final SnapshotBase base = cas.snapshotBase()
            index = openIndex()
            index.ingestRun(cas.store, completion, cas.config.writableAlias)
            final Set<String> failed = cas.catchUpIndex(index)
            writeSnapshot(index, base, failed)
        }
        // catch and finally unchanged
    }

    private void writeSnapshot(Index index, SnapshotBase base, Set<String> failed) {
        try {
            final IndexSnapshot.Result r = cas.snapshotWritable(index, cas.config.snapshotMaxBytes, base, failed)
            if( r.skipped == IndexSnapshot.OVER_CAP )
                log.info("the Index Snapshot of store '${cas.config.writableAlias}' is over cas.snapshot.maxBytes " +
                    "(${cas.config.snapshotMaxBytes} bytes) and was not rewritten; `nextflow plugin nf-blocks:snapshot` rewrites it at any size")
            else if( r.skipped )
                log.info("the Index Snapshot of store '${cas.config.writableAlias}' was not rewritten (${r.skipped}); it is derived, and the next writer rewrites it")
        }
        catch( Exception e ) {
            log.warn("the Index Snapshot of store '${cas.config.writableAlias}' could not be written; it is derived: ${e.message}", e)
        }
    }
```

`CasLinStore`:

```groovy
    /** nf/ of a member as the location string DefaultLinStore resolves (ticket 02 decision 7): s3://... or a local path. */
    static String recordsLocation(CasConfig config, String alias) {
        return config.isRemote(alias)
            ? "${config.remoteOf(alias)}/${NEXTFLOW_RECORDS}".toString()
            : FilesEx.toUriString(config.locationOf(alias).resolve(NEXTFLOW_RECORDS))
    }
```

`open` uses it: for a local writable member it still creates `nf/` (and aborts with `AbortOperationException` when it cannot); for an S3 one it creates nothing (S3 needs no directory). `delegate = openRecords(recordsLocation(casConfig, writable))`; the reader chain adds every other member, local ones only when `Files.isDirectory(<location>/nf)`, S3 ones always (a missing prefix yields no records). `openRecords(String location)` passes the string straight into `LineageConfig([enabled: true, store: [location: location]])`. `getRecordsLocation()` keeps returning `delegate?.location`.

`ExploreCommand.start`: after building the session, `session.checkClock()` (a `ClockSkewException` is printed as `nf-blocks:explore: <message>` and the verb exits 1, through `CasCommands.run`'s existing catch); then, when the writable member is S3, print the shadowed pointers of silent decision 20:

```groovy
        final CoordinateTree coords = session.coordinatesOf(cas.writableAlias)
        if( coords instanceof S3CoordinateTree ) {
            final List<String> shadowed = ((S3CoordinateTree) coords).shadowedPointers(21)
            if( shadowed )
                err.println("nf-blocks:explore: ${shadowed.size() > 20 ? 'more than 20' : shadowed.size()} coordinate(s) in '${cas.writableAlias}' are shadowed by a pointer above them, and read as absent: ${shadowed.take(20).join(', ')}")
        }
```

`membersOf(cas, ...)` gets `{ String bucket -> CasSession.s3OpsFactory.call(config, bucket) } as Closure<S3Ops>`. `refresh(CasSession cas, PrintStream err)` becomes `base = cas.snapshotBase()`, open, `failed = cas.catchUpIndex(index)`, `cas.snapshotWritable(index, 0L, base, failed)`, printing `nf-blocks:explore: the Index Snapshot was not rewritten (<skipped>)` when skipped. The exporter's `session.catchUpIndex(index)` is unchanged (its result is ignored).

`CasCommands.snapshot` does the same and prints either today's success line or `nf-blocks:snapshot: not rewritten: <skipped>` (exit 0: the snapshot is derived and the old one stands). `put` calls `cas.checkClock()` after building its `CasSession` (exit 1 on `ClockSkewException`, with the message). `items` is read-only and does not check.

- [ ] **Step 4: Run the tests and the Gate**

Run: `./gradlew test`
Expected: PASS. Fix every remaining caller the compiler reports: `snapshotWritable(index, maxBytes)` (tests of the old shape move to four arguments with `cas.snapshotBase()` and `[] as Set`), `localLocations()`.

Run: `make gate`
Expected: lineage 11/0/6, tier A 5/5, tier B 12/12. The consumer's first catch-up of `lab` now seeds from the producer's snapshot (its `nextflow.log` has no fallback warning); tier B's `put` and `explore` behave as before on local members.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/ClockSkew.groovy src/main/groovy/robsyme/cas/ClockSkewException.groovy \
  src/main/groovy/robsyme/cas/CasSession.groovy src/main/groovy/robsyme/cas/CasConfig.groovy \
  src/main/groovy/robsyme/cas/trace/CasObserver.groovy src/main/groovy/robsyme/cas/lineage/CasLinStore.groovy \
  src/main/groovy/robsyme/cas/explore/ExploreCommand.groovy src/main/groovy/robsyme/cas/cli/CasCommands.groovy \
  src/test/groovy/robsyme/cas/core/ClockSkewTest.groovy src/test/groovy/robsyme/cas/CasSessionS3Test.groovy \
  src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy src/test/groovy/robsyme/cas/lineage/CasLinStoreTest.groovy \
  src/test/groovy/robsyme/cas/explore/ExploreWriteTest.groovy src/test/groovy/robsyme/cas/cli/CasCommandsTest.groovy
git commit -m "feat(session): S3 members in a run, explore and the verbs; seeding, snapshot guard, clock check

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 12: Staging out of an S3 member; links staged as copies

Ticket 15 decision 7, carried decisions 17 and 23, silent decision 9. A Batch consumer's `cas://` inputs are staged by the head node through Nextflow's `FilePorter` into the work bucket (`batch-publish.md` §4), which lands in our `download(source, target)`. Three changes: a raw block held by an S3 member is copied server-side into an S3 target (`CopyObject` with SHA-256, the digest checked against the CID), so no byte passes through the head node; a `symlink` manifest entry staged onto any non-default filesystem is materialised as a copy of its target inside the manifest (S3 has no links; `Files.createSymbolicLink` on an `S3Path` throws); and the `executable` branches go, since manifests no longer carry the bit.

**Files:**
- Modify: `src/main/groovy/robsyme/cas/s3/S3Ops.groovy` (`copyOut`), `src/main/groovy/robsyme/cas/s3/SdkS3Ops.groovy` (`copyOut`), `src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy` (`copyOut`)
- Modify: `src/test/groovy/robsyme/cas/s3/MemoryS3Ops.groovy` (add Task 1's `copyOut`)
- Modify: `src/main/groovy/robsyme/cas/nio/CasFileSystemProvider.groovy` (`download`, `materialiseFile`, `materialiseDirectory`, the manifest-entry node, `makeExecutable`)
- Create: `src/test/groovy/robsyme/cas/nio/StageToObjectStoreTest.groovy`
- Modify: `src/test/groovy/robsyme/cas/s3/SdkS3OpsTest.groovy`, `src/test/groovy/robsyme/cas/s3/S3CopyTest.groovy`

**Interfaces:**
- Consumes: `S3BlockStore.key`, `S3Ops.copy` semantics (Task 1), `DirectoryManifest`, `ManifestEntry`.
- Produces: `S3Ops.copyOut(String key, String targetBucket, String targetKey) -> S3Written` (a `CopyObject` from this bucket into another, `x-amz-checksum-algorithm: SHA256`, the aws scope's write fields); `S3BlockStore.copyOut(Cid cid, String targetBucket, String targetKey)` (throws `BlockMismatchException` on a digest mismatch; the provider then deletes the target).

- [ ] **Step 1: Write the failing tests**

```groovy
// src/test/groovy/robsyme/cas/nio/StageToObjectStoreTest.groovy
package robsyme.cas.nio

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

import nextflow.Global
import nextflow.Session
import robsyme.cas.CasConfig
import robsyme.cas.CasSession
import robsyme.cas.core.*
import spock.lang.Specification
import spock.lang.TempDir

/** download() into a non-default filesystem: links become copies (ticket 15 decision 7). */
class StageToObjectStoreTest extends Specification {

    @TempDir Path tmp
    LocalBlockStore store
    Session session
    FileSystem zip
    CasFileSystemProvider provider = new CasFileSystemProvider()

    def setup() {
        store = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        final CasConfig config = CasConfig.from([cas: [stores: [lab: [location: store.root.toString()]]]], 'cas://lab')
        session = Mock(Session); Global.session = session
        CasSession.bind(session, new CasSession(config, store, new LocalCoordinateTree(store.root.resolve('coords'))))
        zip = FileSystems.newFileSystem(URI.create('jar:' + tmp.resolve('work.zip').toUri()), [create: 'true'])
    }

    def cleanup() { zip?.close(); CasSession.unbind(session); Global.session = null }

    def 'a manifest with in-tree links stages onto an object store with each link as a copy of its target'() {
        given:
        final Path src = tmp.resolve('A_qc')
        Files.createDirectories(src.resolve('nested'))
        Files.writeString(src.resolve('summary.txt'), 'summary\n')
        Files.writeString(src.resolve('nested/n.txt'), 'n\n')
        Files.createSymbolicLink(src.resolve('alias.txt'), Path.of('summary.txt'))
        Files.createSymbolicLink(src.resolve('dirlink'), Path.of('nested'))
        final Cid manifest = new DirectoryManifestBuilder(store).build(src).cid
        final Path target = zip.getPath('/stage/A_qc')

        when:
        provider.download(provider.getPath(URI.create("cas://${manifest}")), target)

        then:
        Files.readString(target.resolve('alias.txt')) == 'summary\n'
        Files.readString(target.resolve('dirlink/n.txt')) == 'n\n'
        !Files.isSymbolicLink(target.resolve('alias.txt'))
    }

    def 'staged locally, links stay links'() {
        given:
        final Path src = tmp.resolve('B_qc')
        Files.createDirectories(src)
        Files.writeString(src.resolve('summary.txt'), 's\n')
        Files.createSymbolicLink(src.resolve('alias.txt'), Path.of('summary.txt'))
        final Cid manifest = new DirectoryManifestBuilder(store).build(src).cid

        when:
        provider.download(provider.getPath(URI.create("cas://${manifest}")), tmp.resolve('staged'))

        then:
        Files.isSymbolicLink(tmp.resolve('staged/alias.txt'))
    }
}
```

In `S3CopyTest` (Task 10's spec), add: `store.copyOut(cid, 'work', 'stage/A.bam')` puts the bytes in the peer bucket and records `COPYOUT` in the member's calls with no `GET`; a double whose `copyOut` returns a wrong `sha256` makes `copyOut` throw `BlockMismatchException`. In `SdkS3OpsTest`, add the header feature for `copyOut`: `x-amz-copy-source: member/<key>`, the destination path `/work/stage/A.bam`, `x-amz-checksum-algorithm: SHA256`.

Run: `./gradlew test --tests 'robsyme.cas.nio.StageToObjectStoreTest' --tests 'robsyme.cas.s3.*'`
Expected: FAIL: `createSymbolicLink` is unsupported on the zip filesystem; `copyOut` does not exist.

- [ ] **Step 2: Implement**

`S3Ops` gains `S3Written copyOut(String key, String targetBucket, String targetKey)`; `SdkS3Ops.copyOut` is `copy`'s body with source and destination swapped (`sourceBucket(bucket).sourceKey(key).destinationBucket(targetBucket).destinationKey(targetKey)`), always `checksumAlgorithm(SHA256)`, never conditional (a stage target is overwritten, as `FilePorter` expects); add the `MemoryS3Ops.copyOut` from Task 1. `S3BlockStore`:

```groovy
    /** A block into another bucket without the bytes leaving S3, checked by S3's SHA-256 (silent decision 9). */
    void copyOut(Cid cid, String targetBucket, String targetKey) {
        final S3Written w = ops.copyOut(key(cid), targetBucket, targetKey)
        if( w.sha256 == null || Base64.decoder.decode(w.sha256) != cid.digest )
            throw new BlockMismatchException(cid, "S3 copied it to s3://${targetBucket}/${targetKey} as ${w.sha256}")
    }
```

In `CasFileSystemProvider`:

- `materialiseFile(Cid cid, Path target, boolean allowSymlink)` (the `executable` parameter and `makeExecutable` go): first the local symlink case as today; then, when `FilesEx.toUriString(target)` starts with `s3://` and `holderOf(cid)` (the first member with `has(cid)`) is an `S3BlockStore`, `copyOut(cid, bucket, key)` (on `BlockMismatchException`: `Files.deleteIfExists(target)`, then `AbortRunException` as `streamAndVerify` does); else `streamAndVerify`.
- `materialiseDirectory(Cid manifestCid, Path dir)` becomes `materialiseDirectory(Cid manifestCid, Path dir, Cid root, List<String> at)`, carrying the root manifest and the directory's segments, and its `SYMLINK` case:

```groovy
                case ManifestEntry.SYMLINK:
                    if( child.fileSystem == FileSystems.default ) {
                        Files.deleteIfExists(child)
                        Files.createSymbolicLink(child, child.fileSystem.getPath(entry.target))
                    }
                    else {
                        // No links on an object store: stage what the link names, from inside the tree (ticket 15 decision 7).
                        final ManifestEntry resolved = resolveInTree(root, at, entry.target, 0)
                        if( resolved == null )
                            throw new AbortRunException("cas: manifest ${manifestCid}: '${entry.name}' -> '${entry.target}' does not resolve inside the tree; cannot stage it as a copy")
                        if( resolved.isDirectory() ) materialiseDirectory(resolved.address, child, root, segmentsOf(at, entry.target))
                        else materialiseFile(resolved.address, child, false)
                    }
                    break
```

with `resolveInTree(Cid root, List<String> at, String target, int hops)` normalising `at + target` (`..` pops, refusing to climb above the root), walking the manifests entry by entry from `root`, following a `symlink` entry met on the way with `hops + 1` (null past `DirectoryManifestBuilder.MAX_DEPTH`), and returning the final entry or null. Delete both `case ManifestEntry.EXECUTABLE:` branches (the entry-node branch and the materialise branch): a schema-1 `executable` entry decodes as `regular` (Task 2). Callers pass `(manifest, target, manifest, [])` at the top.

- [ ] **Step 3: Run the tests and the Gate**

Run: `./gradlew test`
Expected: PASS, including the existing download tests (local staging keeps symlinks, and hashes in flight when not on the same filesystem).

Run: `make gate`
Expected: lineage 11/0/6, tier A 5/5, tier B 12/12 (tier-one staging is local, so assertion 6 and B13's staged cells behave as before).

- [ ] **Step 4: Commit**

```bash
git add src/main/groovy/robsyme/cas/s3/S3Ops.groovy src/main/groovy/robsyme/cas/s3/SdkS3Ops.groovy \
  src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy src/test/groovy/robsyme/cas/s3/MemoryS3Ops.groovy \
  src/main/groovy/robsyme/cas/nio/CasFileSystemProvider.groovy src/test/groovy/robsyme/cas/nio/StageToObjectStoreTest.groovy \
  src/test/groovy/robsyme/cas/s3/SdkS3OpsTest.groovy src/test/groovy/robsyme/cas/s3/S3CopyTest.groovy
git commit -m "feat(stage): S3 member to S3 target by server-side copy; links staged onto an object store as copies

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 13: Gate tier one: assertion 2's addition, assertion 13 (seeding), assertion 5's walk

Carried decisions 21 and 23, silent decisions 5 and 18. Three additions to the local Gate, each independent of the plugin (rule 6):

- The `again` run gets `-c gate/node-hash.config` (`cas.nodeHash = true`), so every file it republishes is addressed by the task node's `.command.cas` and recorded `fusion-node`, while `cold` recorded `head-node`. Assertion 2 already requires identical OutputItem addresses across the two runs; it now also requires the two runs' `providers` to say so, every `.command.cas` line of `again`'s tasks to equal the Gate's own SHA-256 of that file, and no Leaf to carry `provider` (ticket 16's addition).
- After the consumer, two more consumer runs (`consumer-seeded`, `consumer-scan`) on a deleted cache. For the first, every RunManifest, RunCompletion and OutputCollection block of the producer's runs at or before `lab`'s snapshot watermark is at mode 000: a catch-up that read one would log a permission failure, and one that skipped them without seeding would lose the runs and change the `fromStore` answer. For the second, `lab`'s snapshot is moved aside too. Assertion 13 checks both.
- `_walk` stops reading the execute bit: a manifest never records one (ticket 15 addendum).

**Files:**
- Create: `gate/node-hash.config`
- Modify: `gate/gate.sh`, `gate/assert.py`, `gate/test_assert.py`, `gate/cas.py` (`Store.metadata_blocks_of_runs`), `gate/README.md`

**Interfaces:**
- Consumes: the RunCompletion `providers` field (Task 2); `.command.cas` (Task 9); seeding and its console warning text (Task 8), which contains `no usable Index Snapshot` and `nf-blocks:snapshot`; `meta` keys `seeded_from:lab`, `store_log_watermark`, `snapshot_written_at`.
- Produces: `assertion(13, "a cold cache seeds from the Index Snapshot")`; `logs/consumer-seeded/`, `logs/consumer-scan/` (each `stdout.log`, `stderr.log`, `nextflow.log`, `exit`), `logs/consumer-seeded/locked` (the paths set to 000), `seeding.json` (`{"watermark", "runs", "written_at", "out_runs_before"}`).

- [ ] **Step 1: Write the failing unit tests**

In `gate/test_assert.py` add, using the existing `StoreBuilder`:

```python
class ProvidersTest(unittest.TestCase):
    def test_addresses_by_provider(self):
        a, b = str(cas.cid_raw(b"a")), str(cas.cid_raw(b"b"))
        rc = {"providers": {"head-node": [cas.Cid(a), cas.Cid(b)], "fusion-node": [cas.Cid(b)]}}
        self.assertEqual(gate_assert._provider_of(rc), {a: {"head-node"}, b: {"head-node", "fusion-node"}})

    def test_command_cas_lines(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "x.txt"), "wb") as f:
                f.write(b"x\n")
            digest = cas.sha256_of_file(os.path.join(d, "x.txt"))
            with open(os.path.join(d, ".command.cas"), "w") as f:
                f.write("%s  x.txt\n%s  gone.txt\n" % (digest, "0" * 64))
            self.assertEqual(gate_assert._command_cas_problems(d), ["%s: .command.cas names gone.txt, which is not in the task directory" % d])


class WalkTest(unittest.TestCase):
    def test_an_executable_file_is_regular(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "tool.sh")
            with open(p, "w") as f:
                f.write("#!/bin/sh\n")
            os.chmod(p, 0o755)
            self.assertEqual(gate_assert._walk(d, d)["tool.sh"]["mode"], "regular")


class SeedingTest(unittest.TestCase):
    def test_permission_failures_name_a_locked_block(self):
        # Index.groovy:507 logs "<kind> <cid> could not be read (<e.message>)", and an
        # AccessDeniedException's message is the path alone.
        locked = ["/g/store/blocks/yx/bafyx"]
        log = "WARN nextflow.cas - run bafyx could not be read (/g/store/blocks/yx/bafyx); it will be retried"
        self.assertEqual(gate_assert._permission_failures(log, locked), [log])
        self.assertEqual(gate_assert._permission_failures("run bafyz could not be read (/g/store/blocks/yz/bafyz); it will be retried", locked), [])
        self.assertEqual(gate_assert._permission_failures("fine", locked), [])
```

Run: `python3 -m unittest discover -s gate`
Expected: FAIL (`_provider_of`, `_command_cas_problems`, `_permission_failures` missing; `_walk` says `executable`).

- [ ] **Step 2: `node-hash.config` and `gate.sh`**

```groovy
// gate/node-hash.config
// The `again` run only: node-side hashing on the local executor (DESIGN.md
// §11, cas.nodeHash). Every republished file's address then comes from its
// task's .command.cas and is recorded fusion-node; assertion 2 requires the
// OutputItem addresses to equal cold's, whose files were head-node.
cas.nodeHash = true
```

In `gate.sh`, `run "$GATE_ROOT/pipeline-a" again` becomes `run "$GATE_ROOT/pipeline-a" again -c "$REPO/gate/node-hash.config"`. After the consumer block (before `lineage=0`), add:

```bash
# --------------------------------------------------------------------------
# Assertion 13: a cold cache seeds from the Index Snapshot (ticket 04 decision 11)
# --------------------------------------------------------------------------

consumer_cache_delete() {
    python3 "$REPO/gate/assert.py" "$GATE_ROOT" --delete-consumer-cache
}

consumer_again() {   # <name>: the consumer, as above, under another run name
    local name="$1" log="$GATE_ROOT/logs/$1" status=0
    mkdir -p "$log"
    echo "--- run $name"
    ( cd "$GATE_ROOT/consumer" && "$NEXTFLOW" run . -name "$name" "${consumer_args[@]+"${consumer_args[@]}"}" ) \
        > "$log/stdout.log" 2> "$log/stderr.log" || status=$?
    echo "$status" > "$log/exit"
    cp "$GATE_ROOT/consumer/.nextflow.log" "$log/nextflow.log" 2> /dev/null || true
    echo "    exit $status  -> $log"
}

python3 "$REPO/gate/assert.py" "$GATE_ROOT" --seeding-before > "$GATE_ROOT/seeding.json"
consumer_cache_delete
python3 "$REPO/gate/assert.py" "$GATE_ROOT" --seeding-lock > "$GATE_ROOT/logs/seeding-lock.txt"
mkdir -p "$GATE_ROOT/logs/consumer-seeded"
cp "$GATE_ROOT/logs/seeding-lock.txt" "$GATE_ROOT/logs/consumer-seeded/locked"
relock() {
    while IFS= read -r f; do [[ -n "$f" ]] && chmod 444 "$f" 2> /dev/null || true; done < "$GATE_ROOT/logs/consumer-seeded/locked"
}
trap relock EXIT
while IFS= read -r f; do [[ -n "$f" ]] && chmod 000 "$f"; done < "$GATE_ROOT/logs/consumer-seeded/locked"
consumer_again consumer-seeded
relock
trap - EXIT

consumer_cache_delete
snap="$(ls "$GATE_STORE"/index/v*.sqlite)"
mv "$snap" "$GATE_ROOT/snapshot-aside.sqlite"
consumer_again consumer-scan
mv "$GATE_ROOT/snapshot-aside.sqlite" "$snap"
```

(The browser tiers run afterwards and read `store/`, so both the unlock and the restore happen before them, and `relock` also runs on any early exit. `relock` restores mode 444, the `r--r--r--` that `LocalBlockStore` writes blocks with (`LocalBlockStore.groovy:38, 226`), so the browser tiers see blocks exactly as the plugin left them.)

- [ ] **Step 3: `assert.py`**

Add the three flags to `main`'s `known` set and their handlers:

- `--seeding-before`: print `json.dumps({"watermark": ..., "runs": ..., "written_at": ..., "out_runs_before": ...})` from `store/index/v3.sqlite`'s `meta` and `run` count and `store-out/index/v3.sqlite`'s `run` count (0 when absent), opened read-only with `sqlite3`.
- `--delete-consumer-cache`: `cas.Index.locate_for_pipeline(<cache>, CONSUMER_IDENTITY)`, then remove it and its `-wal`/`-shm`; print the path.
- `--seeding-lock`: print, one per line, the block paths of every RunManifest, RunCompletion and OutputCollection reachable from the `run` rows of `store/index/v3.sqlite` (the snapshot's own list of runs at or before its watermark), from `gate.store.metadata_blocks_of_runs(completions)` in `cas.py`, which follows `run` and `collections` links from each RunCompletion.

Then:

```python
def _provider_of(completion):
    """{address text: {provider, ...}} from a RunCompletion's providers."""
    out = {}
    for name, links in (completion.get("providers") or {}).items():
        for link in links:
            out.setdefault(_address_text(link), set()).add(name)
    return out


def _command_cas_problems(task_dir):
    """Every .command.cas line must name a file in the task dir that hashes to its digest."""
    path = os.path.join(task_dir, ".command.cas")
    problems = []
    with open(path, encoding="utf-8") as f:
        for line in f.read().splitlines():
            digest, name = line[:64], line[66:]
            full = os.path.join(task_dir, name)
            if not os.path.isfile(full):
                problems.append("%s: .command.cas names %s, which is not in the task directory" % (task_dir, name))
            elif cas.sha256_of_file(full) != digest:
                problems.append("%s: .command.cas says %s is %s; it hashes to %s" % (task_dir, name, digest, cas.sha256_of_file(full)))
    return problems


def _permission_failures(log_text, locked_paths):
    """Lines where the plugin could not read one of the locked blocks (Index.groovy:507's format)."""
    return [line for line in log_text.splitlines()
            if "could not be read" in line and any(p in line for p in locked_paths)]
```

In `assert_two`, before the final `if problems:` add:

```python
    cold_rc, again_rc = gate.run("cold").completion or {}, gate.run("again").completion or {}
    for name, rc in (("cold", cold_rc), ("again", again_rc)):
        if rc.get("schema") != 2 or "providers" not in rc:
            problems.append("run %s's RunCompletion is schema %r without providers; ticket 16 moves them there" % (name, rc.get("schema")))
    cold_p, again_p = _provider_of(cold_rc), _provider_of(again_rc)
    raw_leaves = {_address_text(leaf.get("address")) for _cid, item in gate.run("again").items(gate)
                  for leaf in _leaves(item.get("value")) if leaf.get("address") is not None
                  and cas.cid_codec(_address_text(leaf.get("address"))) == cas.RAW}
    not_node = sorted(a for a in raw_leaves if "fusion-node" not in again_p.get(a, set()))
    if not_node:
        problems.append("run again (cas.nodeHash = true) has %d file leaf address(es) not under fusion-node: %s" % (len(not_node), not_node[:3]))
    not_head = sorted(a for a in raw_leaves if a in cold_p and "head-node" not in cold_p[a])
    if not_head:
        problems.append("run cold has file leaves not under head-node: %s" % not_head[:3])
    with_provider = [cid for cid, item in gate.run("again").items(gate) for leaf in _leaves(item.get("value")) if "provider" in leaf]
    if with_provider:
        problems.append("%d OutputItem(s) of again still carry a Leaf provider: %s" % (len(with_provider), with_provider[:3]))
    for task_dir in gate.work_dirs("pipeline-a", None):
        if os.path.isfile(os.path.join(task_dir, ".command.cas")):
            problems.extend(_command_cas_problems(task_dir)[:3])
```

(`gate.work_dirs(launch, name)` returns every task directory when `name` is None; extend it so.) Append to the PASS message: `"; again's %d file leaves came from .command.cas (fusion-node), cold's from the head node, one item address each"`.

Assertion 13:

```python
@assertion(13, "a cold cache seeds from the Index Snapshot")
def assert_thirteen(gate):
    before = json.load(open(os.path.join(gate.root, "seeding.json")))
    problems = []
    baseline = _consumer_hashes_of_run(gate, "consumer")
    for name in ("consumer-seeded", "consumer-scan"):
        if gate.exit_code(name) != 0:
            problems.append("%s exited %s; see logs/%s/" % (name, gate.exit_code(name), name))
            continue
        got = _consumer_hashes_of_run(gate, name)
        if got != baseline:
            problems.append("%s staged %s, the first consumer %s: the answer changed" % (name, got, baseline))
    seeded_log = _read(os.path.join(gate.root, "logs", "consumer-seeded", "nextflow.log"))
    locked = _read(os.path.join(gate.root, "logs", "consumer-seeded", "locked")).split()
    denied = _permission_failures(seeded_log, locked)
    if denied:
        problems.append("the seeded run read a locked metadata block of a run the snapshot holds: %s" % denied[:2])
    if "no usable Index Snapshot" in seeded_log:
        problems.append("the seeded run fell back to the full scan although lab's snapshot was there")
    scan_out = _read(os.path.join(gate.root, "logs", "consumer-scan", "stdout.log")) + _read(os.path.join(gate.root, "logs", "consumer-scan", "nextflow.log"))
    if not ("no usable Index Snapshot" in scan_out and "'lab'" in scan_out and "nf-blocks:snapshot" in scan_out):
        problems.append("consumer-scan printed no fallback warning naming lab and nf-blocks:snapshot")
    out_runs = _snapshot_runs(os.path.join(gate.store_out.root, "index", "v3.sqlite"))
    if out_runs < before["out_runs_before"]:
        problems.append("store-out's snapshot fell from %d to %d runs" % (before["out_runs_before"], out_runs))
    if problems:
        return FAIL, "; ".join(problems)
    return PASS, ("with the cache deleted and %d metadata blocks of lab's %d snapshot runs unreadable, the consumer "
                  "seeded from the snapshot and staged the same bytes; with the snapshot gone too it scanned, warned and "
                  "staged them again; store-out's snapshot has %d runs" % (len(open(os.path.join(gate.root, "logs", "consumer-seeded", "locked")).read().split()), before["runs"], out_runs))
```

with `_consumer_hashes_of_run(gate, name)` reading, from `store-out`, the `hashes` OutputCollection of the RunCompletion whose RunManifest's `run_name` is `name`, and the sha256 text of each item's leaf (the same parse `_consumer_hashes` does from coordinates, but per run, since all three consumers publish to the same coordinates); `_snapshot_runs(path)` a read-only `count(*)` of `run` (0 when the file is absent); `_read(path)` the file's text or `""`. In `_walk`, replace the `mode = "executable" if ... else "regular"` line with `mode = "regular"`, and in `_count_manifest` drop the `"executable"` counter. In the SKIP table's assertion 11 entry (`assert.py:1320-1323`), replace "Under Fusion, published files carry provider `fusion-node`, `.command.cas` verifies with `sha256sum -c`, and recorded addresses equal hashes the test computes." with "Under Fusion, each published file's address comes from the task node's `.command.cas` or from S3's SHA-256 of a server-side copy (`s3-copy`), both checked against hashes the test computes; tier two runs it (`make gate-tier2`, T2 and T2b)." (ticket 16 made `s3-copy` the provider for a fresh S3 member.)

- [ ] **Step 4: README, run the Gate**

In `gate/README.md`: the assertion 2 row adds "and `again`, run with `gate/node-hash.config`, records every file `fusion-node` from `.command.cas` while `cold` recorded `head-node`: the same OutputItem addresses either way (ticket 16)"; a new row 13 "a cold cache seeds from the Index Snapshot: `consumer-seeded` (cache deleted, the producer's run metadata blocks at mode 000) and `consumer-scan` (snapshot removed too) stage the same bytes as the consumer; the first reads no locked block, the second prints the fallback warning; store-out's snapshot run count does not fall"; the assertion 5 row notes that a manifest records no execute bit.

Run: `python3 -m unittest discover -s gate && make gate`
Expected: lineage 12 PASS, 0 FAIL, 6 SKIP (assertion 13 added); browser tier A 5/5; tier B 12/12.

- [ ] **Step 5: Commit**

```bash
git add gate/node-hash.config gate/gate.sh gate/assert.py gate/test_assert.py gate/cas.py gate/README.md
git commit -m "test(gate): node-hashed republish keeps item addresses; a cold cache seeds from the snapshot (assertion 13)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 14: Gate tier two, `make gate-tier2`

Tickets 11 and 03 (decision 6), carried decision 22 (with Rob's T2/T2b ruling), silent decision 24. On demand, from the laptop, with Rob's `scidev` SSO session (`AWS_PROFILE=scidev`) and a Python with boto3 (`GATE_PYTHON`; `/usr/local/bin/aws` does not run on this machine). It reuses a GATE_ROOT that passed tier one: its built plugin (`$GATE_ROOT/plugins`), its `plugins.json` recipe and its local `store/` (for T3's cross-backend comparison). Everything it creates is under one run id and deleted on every exit. The assertions hash the work bucket's bytes themselves, as `prototype/05/check.py` did; no assertion believes the plugin.

Runs, in order (queue and CE `TowerForge-3skcexigeJwK0Jb71pThbJ`, us-east-1; configs from `prototype/05-batch-publish:prototype/05/batch.config` and `fusion.config`):

| Run | Pipeline | Fusion | Writable member | Checks |
|---|---|---|---|---|
| t1 | Test Pipeline | off | `s3://<b>/cas` (`lab`) | T1, T3 (flattened links) |
| t2 | Test Pipeline | on | `s3://<b>/cas-t2` (`lab`, fresh) | T2 (`s3-copy`, digests agree), T3 (decoded links) |
| t2b | `gate/tier2/small` | on | `s3://<b>/cas` (T1's) | T2b (`fusion-node`; `aligned` items equal T1's, `qc` items equal t2's) |
| t4 | consumer, on Batch | off | `s3://<b>/cas-out` (`out`), reading `lab` = `s3://<b>/cas` | T4 |
| t5 | consumer again, cache deleted | off | as t4 | T5 |
| t6a, t6b | Test Pipeline from two launch dirs, started together | off | `s3://<b>/cas-t6` (fresh) | T6 |

**Files:**
- Create: `gate/tier2/tier2.sh`, `gate/tier2/s3gate.py`, `gate/tier2/assert_tier2.py`, `gate/tier2/test_assert_tier2.py`
- Create: `gate/tier2/batch.config`, `gate/tier2/fusion.config`, `gate/tier2/member.config`, `gate/tier2/consumer.config`, `gate/tier2/small/main.nf`, `gate/tier2/small/nextflow.config`, `gate/tier2/README.md`
- Modify: `gate/consumer/main.nf` (an optional `--dir` branch), `gate/consumer/nextflow.config` (`params.dir = null`)
- Modify: `Makefile` (`gate-tier2`)

**Interfaces:**
- Consumes: `gate/cas.py` (CID, DAG-CBOR decode); tier one's `gate/assert.py --refs`; the plugin's info line `nf-blocks: the head node read ...` (Task 10), reported only; `nextflow plugin nf-blocks:items` and `nf-blocks:snapshot` (existing verbs) through `gate/browser/plugin-repo.sh`.
- Produces: `$T2_ROOT` (`$GATE_ROOT/tier2/<run id>/`) holding `logs/<run>/`, `trace/<run>.txt`, `ids.env` (`BUCKET`, `WORK`, `RUN_ID`), `pids` (every nextflow the harness started), `evidence/` (downloaded snapshots, coords, `.command.cas` files, `if-match.json`); `assert_tier2.py` output in `assert.py`'s table format, exit 1 on a FAIL.

- [ ] **Step 1: The S3 helper and its unit tests**

```python
# gate/tier2/s3gate.py
"""Tier two's view of S3, independent of the plugin (rule 6): boto3 only.

    s3gate.py setup <run-id>            prints the bucket it created (us-east-1)
    s3gate.py teardown <bucket> <work-prefix>
    s3gate.py if-match <bucket>          prints {"status": ..., "body": ...} of the stale If-Match probe
Everything else is imported by tier2.sh's assertions.
"""
import hashlib, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import cas  # noqa: E402

REGION = "us-east-1"
WORK_BUCKET = "scidev-playground-us-east-1"


def client():
    import boto3
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "scidev")).client("s3", region_name=REGION)


def setup(run_id):
    s3 = client()
    bucket = "nf-blocks-t2-%s" % run_id
    s3.create_bucket(Bucket=bucket)   # us-east-1 takes no LocationConstraint
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={"Rules": [
        {"ID": "staging", "Filter": {"Prefix": ""}, "Status": "Enabled",
         "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}},
        {"ID": "tmp", "Filter": {"Prefix": "cas/tmp/"}, "Status": "Enabled", "Expiration": {"Days": 1}}]})
    return bucket


def _empty(s3, bucket, prefix=""):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
    for page in s3.get_paginator("list_multipart_uploads").paginate(Bucket=bucket, Prefix=prefix):
        for u in page.get("Uploads", []):
            s3.abort_multipart_upload(Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"])


def teardown(bucket, work_prefix):
    """Both halves always run; a failure in either is loud (cloud.sh's rule)."""
    s3, failed = client(), False
    for step in (lambda: _empty(s3, WORK_BUCKET, work_prefix),
                 lambda: (_empty(s3, bucket), s3.delete_bucket(Bucket=bucket)) if bucket else None):
        try:
            step()
        except Exception as exc:
            sys.stderr.write("tier two teardown: %s\n" % exc)
            failed = True
    return 1 if failed else 0


class Member(object):
    """A member in S3 read as gate/cas.Store reads a local one."""

    def __init__(self, s3, bucket, prefix):
        self.s3, self.bucket, self.prefix = s3, bucket, prefix.rstrip("/") + "/" if prefix else ""

    def keys(self, under):
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix + under):
            for o in page.get("Contents", []):
                yield o["Key"][len(self.prefix):]

    def read(self, rel):
        return self.s3.get_object(Bucket=self.bucket, Key=self.prefix + rel)["Body"].read()

    def block(self, cid):
        return self.read("blocks/%s/%s" % (cid[-2:], cid))

    def decoded(self, cid):
        return cas.decode(self.block(cid))

    def blocks(self):
        return [k.rsplit("/", 1)[1] for k in self.keys("blocks/")]

    def log(self):
        return [k[len("log/"):] for k in self.keys("log/")]

    def completions(self):
        return [name.split("-", 2)[2] for name in self.log() if name.split("-", 2)[1] == "run"]

    def coords(self):
        return {k[len("coords/"):]: self.read(k).decode().strip() for k in self.keys("coords/")}

    def head(self, rel):
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=self.prefix + rel, ChecksumMode="ENABLED")
        except self.s3.exceptions.ClientError:
            return None


def work_objects(s3, prefix):
    """{path in task: [keys]} for every task output in the work prefix, .command.* and .exitcode left out."""
    out = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=WORK_BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):].split("/", 2)
            if len(rel) == 3 and not rel[2].startswith(".command") and rel[2] != ".exitcode":
                out.setdefault(rel[2], []).append(o["Key"])
    return out


def stale_if_match(s3, bucket, key="probe/if-match"):
    """Upload an object, replace it, then PutObject with If-Match on the first ETag: S3 must answer 412
    and keep the second body (ticket 03 decision 6; S3's If-Match was not measured in ticket 14)."""
    stale = s3.put_object(Bucket=bucket, Key=key, Body=b"first")["ETag"]
    s3.put_object(Bucket=bucket, Key=key, Body=b"second")
    status = 200
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"third", IfMatch=stale)
    except s3.exceptions.ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    s3.delete_object(Bucket=bucket, Key=key)
    return {"status": status, "body": body}


def sha256_of_object(s3, bucket, key):
    h = hashlib.sha256()
    for chunk in s3.get_object(Bucket=bucket, Key=key)["Body"].iter_chunks(1 << 20):
        h.update(chunk)
    return h.hexdigest()


if __name__ == "__main__":
    if sys.argv[1:2] == ["setup"]:
        print(setup(sys.argv[2]))
    elif sys.argv[1:2] == ["teardown"]:
        sys.exit(teardown(sys.argv[2], sys.argv[3]))
    elif sys.argv[1:2] == ["if-match"]:
        import json
        print(json.dumps(stale_if_match(client(), sys.argv[2])))
    else:
        sys.stderr.write(__doc__)
        sys.exit(2)
```

`gate/tier2/test_assert_tier2.py` (stdlib `unittest`, no AWS: a `FakeS3` class answering `get_paginator`, `get_object`, `head_object` from a dict) pins `Member.completions()` parsing, `work_objects` leaving `.command.*` out, and each assertion function of Step 3 on a hand-built member: a T1 member whose coordinates name the work bytes' CIDs passes and one with a wrong CID fails; a T2 RunCompletion with a `fusion-node` file leaf fails T2 and passes T2b; T2's item comparison passes when the `aligned` items equal t1's and the `qc` items equal tier one's local `cold` `qc` item although t1's `qc` item differs, and fails when an `aligned` item differs; the `refs` choice of `T2_DIR` names a manifest whose `alias.txt` is `symlink`; T3's expected manifest for a flattened `alias.txt` (a regular file with `summary.txt`'s bytes) and for a Fusion one (decoded to `symlink` `summary.txt`); T6's stale-ETag verdict with one and two writes, and its If-Match verdict on `{"status": 412, "body": "second"}` (PASS) and `{"status": 200, "body": "third"}` (FAIL).

Run: `$GATE_PYTHON -m unittest discover -s gate/tier2`
Expected: FAIL until Step 3 exists (`assert_tier2` has no `t1`..`t6`), then PASS.

- [ ] **Step 2: Configs, the small pipeline, the consumer's directory branch**

```groovy
// gate/tier2/batch.config -- applied after gate/gate.config (producer) or tier2/consumer.config.
plugins {
    id "nf-blocks@${System.getenv('T2_PLUGIN_VERSION')}"
    id 'nf-amazon'
}
workDir = "s3://scidev-playground-us-east-1/robsyme/nf-blocks-gate/${System.getenv('T2_RUN_ID')}/work"
process {
    executor = 'awsbatch'
    queue = 'TowerForge-3skcexigeJwK0Jb71pThbJ'
    container = 'public.ecr.aws/docker/library/ubuntu:24.04'
}
aws {
    region = 'us-east-1'
    profile = System.getenv('AWS_PROFILE') ?: 'scidev'
    batch { cliPath = '/usr/local/aws-cli/v2/current/bin/aws'; volumes = '/usr/local/aws-cli' }
}
trace { enabled = true; overwrite = true; file = System.getenv('T2_TRACE'); fields = 'task_id,hash,native_id,name,status,exit,realtime,workdir' }
```

```groovy
// gate/tier2/fusion.config -- Fusion on (needs Wave, through the Platform token in ~/.nextflow/config).
wave.enabled = true
fusion.enabled = true
aws.batch.cliPath = null
aws.batch.volumes = null
```

```groovy
// gate/tier2/member.config -- the producer's writable member in S3 (T2_MEMBER is cas, cas-t2 or cas-t6).
cas.stores.lab.location = "s3://${System.getenv('T2_BUCKET')}/${System.getenv('T2_MEMBER')}"
```

```groovy
// gate/tier2/consumer.config -- the consumer: its own S3 member, the producer's read-only.
includeConfig "${System.getenv('GATE_REPO')}/gate/consumer/nextflow.config"
cas.stores.out.location = "s3://${System.getenv('T2_BUCKET')}/cas-out"
cas.stores.lab.location = "s3://${System.getenv('T2_BUCKET')}/cas"
```

`gate/tier2/small/main.nf` copies `ALIGN` and `QC_DIR` from the Test Pipeline verbatim (scripts, outputs), feeds them `channel.of([[sample: 'A', single_end: false, lane: 1, nested: [kit: 'truseq', ids: [1, 2]]], 10])`, and publishes `aligned` and `qc` with the Test Pipeline's `output {}` entries for those two; its header comment says it must stay byte-for-byte in step with the Test Pipeline, and T2b fails loudly (its `aligned` item addresses differ from T1's, or its `qc` items from t2's) if it drifts. `gate/tier2/small/nextflow.config` holds one line, `cas.pipeline = 'cas-tier2-small'`: a different Pipeline Identity (DESIGN §6: `cas.pipeline` wins over `manifest.name`), so T4's `fromStore(run: 'latest', pipeline: 'cas-test-pipeline')` still finds t1 and its sample B, while the `aligned` OutputItems, which carry no pipeline, stay T1's (the `qc` items are t2's: under Fusion `alias.txt` stays a link, where t1 flattened it).

In `gate/consumer/main.nf`, add `params.dir = null` to the consumer's params (in `nextflow.config`) and, in the workflow:

```groovy
    // Tier two T4 only: a directory input, to measure what staging a manifest into S3 does (ticket 15 decision 7).
    ch_dir = params.dir
        ? channel.fromPath(params.dir, type: 'dir').map { d -> tuple('dir', d) }
        : channel.empty()
```

with a process `HASH_DIR` (input `tuple val(source), path(staged)`, output `tuple val(source), path("${staged.name}.sha256")`, script `find -L '${staged}' -type f | LC_ALL=C sort | while read -r f; do sha256sum "$f" 2>/dev/null || shasum -a 256 "$f"; done > '${staged.name}.sha256'`) mixed into `hashes`. Tier one passes no `--dir`, so it is unchanged.

In `Makefile`:

```make
# Gate tier two (gate/tier2/README.md): the scidev Batch queue, a throwaway bucket,
# Rob's SSO session. Run after `make gate` with the same GATE_ROOT.
.PHONY: gate-tier2
gate-tier2:
	./gate/tier2/tier2.sh "$(GATE_ROOT)"
```

- [ ] **Step 3: The harness and its assertions**

```bash
#!/usr/bin/env bash
# gate/tier2/tier2.sh -- Gate tier two (ticket 11), on demand:
#   GATE_PYTHON=<python with boto3> AWS_PROFILE=scidev gate/tier2/tier2.sh <GATE_ROOT>
# <GATE_ROOT> must have passed tier one (it reuses the plugin built there).
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GATE_ROOT="$(cd "$1" && pwd)"
PY="${GATE_PYTHON:-python3}"
NEXTFLOW="${NEXTFLOW:-nextflow}"
export AWS_PROFILE="${AWS_PROFILE:-scidev}" GATE_REPO="$REPO" NXF_ANSI_LOG=false
export NXF_PLUGINS_DIR="$GATE_ROOT/plugins"
export T2_PLUGIN_VERSION="$(sed -n "s/^version = '\(.*\)'/\1/p" "$REPO/build.gradle")"
[[ -d "$NXF_PLUGINS_DIR/nf-blocks-$T2_PLUGIN_VERSION" ]] || { echo "tier two: run make gate on $GATE_ROOT first" >&2; exit 2; }
"$PY" -c 'import boto3' 2> /dev/null || { echo "tier two: $PY has no boto3; set GATE_PYTHON" >&2; exit 2; }

export T2_RUN_ID="t2-$(date -u +%Y%m%d-%H%M%S)-$RANDOM"
T2="$GATE_ROOT/tier2/$T2_RUN_ID"; mkdir -p "$T2/logs" "$T2/trace" "$T2/evidence"
WORK_PREFIX="robsyme/nf-blocks-gate/$T2_RUN_ID/"
export T2_BUCKET=''

# The trap first: no exit path leaves a bucket, a work prefix, an open upload or the watchdog behind.
WATCHDOG=''
PIDS="$T2/pids"; : > "$PIDS"
cleanup() {
    [[ -n "$WATCHDOG" ]] && kill "$WATCHDOG" 2> /dev/null || true
    "$PY" "$REPO/gate/tier2/s3gate.py" teardown "$T2_BUCKET" "$WORK_PREFIX" \
        || { echo "tier two: teardown FAILED; delete s3://$T2_BUCKET and s3://scidev-playground-us-east-1/$WORK_PREFIX by hand" >&2; exit 1; }
}
trap cleanup EXIT
trap 'exit 124' TERM                                 # a TERM from the watchdog is an ordinary exit, so cleanup runs
# Ticket 11 decision 6: 45 minutes. Signalling only the shell would wait for the foreground nextflow and let it
# keep submitting Batch jobs after the bucket is gone, so every nextflow runs in the background with its PID
# in $PIDS (run_nf), and the watchdog stops those first.
( sleep 2700; echo "tier two: 45-minute timeout" >&2
  while read -r pid; do kill -TERM "$pid" 2> /dev/null || true; done < "$PIDS"
  sleep 30; kill -TERM $$ ) & WATCHDOG=$!

run_nf() {   # <log> <dir> <env args and command...>: runs it in <dir>, backgrounded and recorded; returns its status
    local log="$1" dir="$2"; shift 2
    local status=0 pid
    ( cd "$dir" && exec env "$@" ) > "$log" 2>&1 &
    pid=$!
    echo "$pid" >> "$PIDS"
    wait "$pid" || status=$?
    return "$status"
}

T2_BUCKET="$("$PY" "$REPO/gate/tier2/s3gate.py" setup "$T2_RUN_ID")"
printf 'BUCKET=%s\nWORK=%s\nRUN_ID=%s\n' "$T2_BUCKET" "$WORK_PREFIX" "$T2_RUN_ID" > "$T2/ids.env"
echo "tier two: bucket $T2_BUCKET, work s3://scidev-playground-us-east-1/$WORK_PREFIX"

# gate.sh's pattern: a failing run is recorded in its exit file, never the end of the harness (set -e).
produce() {   # <run> <member prefix> <pipeline dir> [extra -c ...]
    local run="$1" member="$2" src="$3"; shift 3
    local launch="$T2/$run" status=0; rm -rf "$launch"; mkdir -p "$launch" "$T2/logs/$run"
    cp "$src"/main.nf "$launch/"; [[ -f "$src/nextflow.config" ]] && cp "$src/nextflow.config" "$launch/"
    run_nf "$T2/logs/$run/stdout.log" "$launch" T2_MEMBER="$member" T2_TRACE="$T2/trace/$run.txt" XDG_CACHE_HOME="$T2/cache-$run" \
      "$NEXTFLOW" -log "$T2/logs/$run/nextflow.log" run . -name "$run" -c "$REPO/gate/gate.config" \
        -c "$REPO/gate/tier2/member.config" -c "$REPO/gate/tier2/batch.config" "$@" || status=$?
    echo "$status" > "$T2/logs/$run/exit"
    echo "--- $run exit $status"
}
consume() {   # <run> <cache dir>
    local run="$1" cache="$2" launch="$T2/$1" status=0; rm -rf "$launch"; mkdir -p "$launch" "$T2/logs/$run"
    cp "$REPO/gate/consumer/main.nf" "$launch/"
    eval "$("$PY" "$REPO/gate/tier2/assert_tier2.py" refs "$T2" "$GATE_ROOT")"   # T2_LID, T2_CAS, T2_DIR
    run_nf "$T2/logs/$run/stdout.log" "$launch" T2_TRACE="$T2/trace/$run.txt" XDG_CACHE_HOME="$cache" \
      "$NEXTFLOW" -log "$T2/logs/$run/nextflow.log" run . -name "$run" -c "$REPO/gate/tier2/consumer.config" \
        -c "$REPO/gate/tier2/batch.config" --lid "$T2_LID" --cas "$T2_CAS" --dir "$T2_DIR" || status=$?
    echo "$status" > "$T2/logs/$run/exit"
    echo "--- $run exit $status"
}

TP="$REPO/../.scratch/content-addressed-lineage/test-pipeline"
produce t1  cas    "$TP"
produce t2  cas-t2 "$TP" -c "$REPO/gate/tier2/fusion.config"
produce t2b cas    "$REPO/gate/tier2/small" -c "$REPO/gate/tier2/fusion.config"
consume t4 "$T2/cache-consumer"
rm -rf "$T2/cache-consumer"                          # T5: the head node starts cold
consume t5 "$T2/cache-consumer"

# T6: two writers into one fresh member, started together from two launch dirs.
produce t6a cas-t6 "$TP" & A=$!
produce t6b cas-t6 "$TP" & B=$!
wait "$A" "$B"
plugins_json="$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$T2")"
verb() {   # <log> <cache> <nf-blocks verb and args...>
    local log="$1" cache="$2"; shift 2
    run_nf "$log" "$T2/t6a" -u NXF_OFFLINE T2_MEMBER=cas-t6 XDG_CACHE_HOME="$cache" \
      NXF_PLUGINS_TEST_REPOSITORY="file://$plugins_json" \
      "$NEXTFLOW" -q -c "$REPO/gate/gate.config" -c "$REPO/gate/tier2/member.config" plugin "nf-blocks:$@" || true
}
eval "$("$PY" "$REPO/gate/tier2/assert_tier2.py" t6-refs "$T2")"      # T6_RUNS=lid://a,lid://b
verb "$T2/logs/t6-items.txt" "$T2/cache-t6-items" items aligned --run "$T6_RUNS" --format occurrences
verb "$T2/logs/t6-snapshot.txt" "$T2/cache-t6-snap" snapshot
for attempt in 1 2 3; do                                             # the stale-ETag step (silent decision 24)
    rm -rf "$T2/cache-t6-x" "$T2/cache-t6-y"
    verb "$T2/logs/t6-race-$attempt-x.txt" "$T2/cache-t6-x" snapshot & X=$!
    verb "$T2/logs/t6-race-$attempt-y.txt" "$T2/cache-t6-y" snapshot & Y=$!
    wait "$X" "$Y"
    grep -l 'not rewritten: replaced_meanwhile' "$T2"/logs/t6-race-$attempt-*.txt > /dev/null && break
done
# The deterministic half of ticket 03 decision 6 on AWS itself: a PutObject whose If-Match names a replaced
# ETag is refused with 412 (the plugin's skip on that 412 is pinned by S3SnapshotStorageTest, Task 7).
"$PY" "$REPO/gate/tier2/s3gate.py" if-match "$T2_BUCKET" > "$T2/evidence/if-match.json" || true

"$PY" "$REPO/gate/tier2/assert_tier2.py" check "$T2" "$GATE_ROOT"
```

```python
# gate/tier2/assert_tier2.py -- the tier-two assertions (ticket 11), independent of the plugin.
#   assert_tier2.py refs <T2_ROOT> <GATE_ROOT>    shell lines T2_LID, T2_CAS, T2_DIR for the consumer; T2_DIR is
#                                                 cas://<the qc/A/A_qc manifest of cas>, t2b's, whose alias.txt is symlink
#   assert_tier2.py t6-refs <T2_ROOT>             T6_RUNS
#   assert_tier2.py check <T2_ROOT> <GATE_ROOT>   the table; exit 1 on any FAIL
```

Each check is a function `tN(ctx) -> (status, message)` over a `ctx` holding the boto3 client, `Member` objects for `cas`, `cas-t2`, `cas-out`, `cas-t6`, the work prefix, `$T2` and the tier-one `cas.Store`:

- **T1 (assertion 12).** `t1` exited 0. For every `coords/` pointer in `cas` whose leaf is a raw CID, the CID equals `cas.cid_from_sha256(sha256_of_object(work key), RAW)` for the object under the task directory the pointer's file name came from (`work_objects`, as `prototype/05/check.py` matched them), and the block exists in the member and hashes to its CID. Every `FileOutput` record under `cas/nf/` names a `cas://` path. t1's RunCompletion (`Member.completions()`, the one whose manifest's `run_name` is `t1`) lists every raw file leaf under `s3-copy`. No `.command.run` of t1 (the work prefix's `t1` tasks, found through `trace/t1.txt`'s `workdir`) contains `cas:` (spec's "getBashLib and getUploadCmd are exercised": nothing on a node ever needs the scheme).
- **T2 (assertion 11).** `t2` exited 0. Every raw file leaf of t2's RunCompletion is under `s3-copy` in `cas-t2`. Every task directory of t2 has a `.command.cas` (downloaded into `evidence/`) whose every line's digest equals the Gate's hash of the work object it names (the `sha256sum -c` of the spec, done by the Gate), and every published file's recorded CID equals its `.command.cas` digest. t2's `aligned` OutputItem addresses equal t1's (the assertion 2 addition, in the cloud); its `qc` item addresses equal tier one's local `cold` `qc` items and not t1's, because t1 flattened `alias.txt` and t2 keeps it a link (ticket 15 decision 5, T3).
- **T2b.** `t2b` exited 0; every raw file leaf of its RunCompletion is under `fusion-node`; its `aligned` item addresses are members of t1's, and its `qc` item addresses are members of t2's (and of tier one's local `cold` `qc` items).
- **T3 (assertion 5, cloud).** For sample A's `qc` manifest of t1 (read through t1's RunCompletion's `qc` item, since t2b later rewrites the `qc/A/A_qc` coordinate in `cas`) and of t2 (the `qc/A/A_qc` coordinate in `cas-t2`), decode the manifest and compare it with an independent walk of the work bucket's objects under that task's `A_qc/`: t1 expects `alias.txt` `regular` with `summary.txt`'s CID (nxf_s3_upload flattened it, ticket 15 decision 5); t2 expects the Gate's own decoding of `.fusion.symlinks` (a name listed there is a `symlink` with its object's body as target; the sidecar is not an entry). t2's manifest CID equals tier one's local `cold` manifest CID for `qc/A/A_qc` (`GATE_ROOT/store/coords/qc/A/A_qc`): the backend is not provenance.
- **T4 (assertion 6, cloud).** First, the manifest `refs` passed as `T2_DIR` (the `qc/A/A_qc` coordinate of `cas` after t2b, t2b's Fusion manifest, equal to tier one's local `cold` one) has `alias.txt` as a `symlink` entry, so the staging below exercises a link (ticket 15 decision 7; FAIL otherwise). Then `t4` exited 0; its `hashes/` coordinates in `cas-out` give `lid` and `cas` digests equal to A.bam's work-bucket SHA-256, `fromstore` equal to B.bam's, and `dir`'s listing equal to the Gate's own listing of A_qc with `alias.txt` holding `summary.txt`'s bytes (a link staged as a copy, ticket 15 decision 7).
- **T5 (ticket 04 on S3).** `t5` exited 0 with `hashes` equal to t4's; its cache (`$T2/cache-consumer/nf-blocks/*.sqlite`, the one whose `run` table has `cas-gate-consumer`) has `meta` `seeded_from:lab` equal to `cas/index/v3.sqlite`'s `snapshot_written_at` (downloaded), and no fallback warning in `logs/t5/nextflow.log`; `cas-out`'s snapshot run count (`x-amz-meta-runs`, and a `count(*)` of the downloaded file) is not lower after t5 than after t4 (the harness records the HEAD after t4 in `evidence/`).
- **T6 (ticket 03 decision 6).** Both t6 runs exited 0 and both RunCompletions are in `cas-t6`'s Store Log; every block of the member hashes to its CID (download and hash each; the Test Pipeline's blocks are a few hundred KB); `t6-items.txt` lists occurrences from both runs' `aligned` collections; after `t6-snapshot.txt`, the member's snapshot has as many `run` rows as `run` entries in its Store Log; every `coords/` pointer names a block the member holds; `evidence/if-match.json` says `{"status": 412, "body": "second"}`: S3 refused the PutObject whose If-Match named the replaced ETag and kept the newer object (FAIL otherwise; this is the deterministic evidence for ticket 03 decision 6 on AWS, and `S3SnapshotStorageTest` pins the plugin's skip on that 412); and in the race step exactly one of the two verbs of the last attempt printed `not rewritten: replaced_meanwhile` (best-effort evidence of the plugin path: SKIP, not FAIL, with "the two verbs did not overlap in 3 attempts; ticket 03 decision 6 was not observed through the plugin on AWS" when none did).

At the end the harness prints, from `trace/*.txt`, the Batch job count (rows with a `native_id`) and the total job seconds (sum of `realtime`), and each producer's `nf-blocks: the head node read ...` line from its `nextflow.log`, noting that the head node is a laptop nearest ca-central-1, so every byte it reads crosses regions.

`gate/tier2/README.md` lists the prerequisites (a tier-one GATE_ROOT, `GATE_PYTHON` with a boto3 recent enough to pass `IfMatch` to `put_object`, an SSO login, the Platform token for Wave), what is created and destroyed, the six checks as above, and "rerun rather than debug" for T6's race SKIP.

- [ ] **Step 4: Run it (Rob)**

Run: `make gate GATE_ROOT=<root>` then `GATE_PYTHON=<venv>/bin/python AWS_PROFILE=scidev make gate-tier2 GATE_ROOT=<root>`
Expected: T1-T6 and T2b PASS (T6's If-Match probe must PASS; only its race step may SKIP; rerun once); the bucket and the work prefix are gone afterwards (`aws s3 ls` through boto3 shows neither). This step needs Rob's SSO session and creates a bucket: the implementing agent stops here and asks Rob to run it.

- [ ] **Step 5: Commit**

```bash
git add gate/tier2 gate/consumer/main.nf gate/consumer/nextflow.config Makefile
git commit -m "test(gate): tier two on the scidev Batch queue: S3 member, Fusion, cold cache, two writers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 15: `DESIGN.md`, spec, README, and acceptance

Every decision of this plan's header lands in the document it amends. `DESIGN.md` gains a §17 "Milestone 4: cloud" that lists the carried decisions with their tickets and the silent decisions as built. Each step quotes the text it replaces where the text is fixed; keep facts only. §6's schema and record prose were already amended by Task 2.

**Files:**
- Modify: `DESIGN.md` (§0 rule 5, §1, §2, §5, §8, §11, §12, §14, §15, new §17)
- Modify: `README.md` (status, an S3 member, Batch and Fusion, a cold head node)
- Modify, in place (outside the repo): `../.scratch/content-addressed-lineage/spec.md` (§1.2, §3, §14)

- [ ] **Step 1: `DESIGN.md` §0, §1, §2**

§0 rule 5: append after the 2026-09-27 widening

```
   *Amended 2026-09-28 (ticket 07):* a verb exists only for an operation the
   spec names that a run cannot perform itself, and each lands in the
   milestone that also adds the Gate assertion exercising it. Milestone 4
   adds none; `sweep`, `prune`, `untrash`, `bundle`, `merge`, `verify` and
   `project` wait for their milestones.
```

§1: add to the package list: "`robsyme.cas.s3`: the one S3 seam (`S3Ops`, `SdkS3Ops`, `S3Access`) and the S3 member (`S3BlockStore`, `S3StoreLogStorage`, `S3CoordinateTree`, `S3SnapshotStorage`). The only package that imports `software.amazon.awssdk.*` or `nextflow.cloud.aws.*`." and after the Gradle line: "`requirePlugins = ['nf-amazon@>=3.9.2']`; the AWS SDK is `compileOnly` through `io.nextflow:nf-amazon:3.9.2`, so the zip carries none and nf-blocks links against the classes nf-amazon loads (ticket 02)."

§2: in the example block, replace `lab { location = '/data/cas' }            // writable member (alias = outputDir authority)` with `lab { location = 's3://bucket/cas' }      // writable member: a local directory or s3://<bucket>[/<prefix>]`, and add `tmpDir = null              // optional; scratch for S3 uploads of unknown length, default java.io.tmpdir` and `nodeHash = null            // optional; node-side hashing, default fusion.enabled`. Replace the two paragraphs "In the Walking Skeleton a member location is a local directory path..." and "*Amended 2026-09-25:* a read-only member's `location` may be an S3 URI..." with:

```
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
  ignored and blocks are written as `STANDARD`.
- `cas.tmpDir` holds a stream of unknown length on its way to an S3 member
  (`putStreaming` spools while hashing, rule 2): an S3 member needs scratch
  disk the size of the largest such output (in practice an object over 5 GiB
  published from S3 without a node digest).
- `cas.nodeHash` (Boolean; default `fusion.enabled`) turns on node-side
  hashing (§11): the `afterScript` default and the `.command.cas` read.
```

- [ ] **Step 2: §5, §8**

§5: after the `CompositeStore` paragraph add

```
*Amended 2026-09-28 (milestone 4):* `S3BlockStore(ops, prefix, alias,
writable, tmpDir)` keeps the local layout byte for byte under the
member prefix: `blocks/<xx>/<cid>`, `log/`, `coords/`, `nf/`,
`index/v3.sqlite`, `index.html`, plus `tmp/` (staging keys of the `s3-copy`
provider, §8). A block of 1 MiB or more is looked for with `HeadObject` first
(S3 reads a whole body before answering a conditional PUT's 412, ticket 14);
every write carries `If-None-Match: *` and a 412 is success; a write that
meets a 409 `ConditionalRequestConflict` is tried up to 3 times in all (a
Store Log entry too, after which the run warns and continues). The storage
class, SSE and requester-pays fields are set by `SdkS3Ops` on every write;
`aws.client.s3Acl` is not applied. Up to 5 GiB a block
is one `PutObject` with `ChecksumAlgorithm SHA256`, and the `ChecksumSHA256`
S3 returns must equal the CID digest (a mismatch deletes the object and fails
the write); above, a multipart upload of `max(64 MiB, ceil(size/10000))`
parts, each a file-channel range through `RequestBody.fromContentProvider`,
completed with `If-None-Match: *` and aborted on any failure. Blocks carry
`Cache-Control: public, max-age=31536000, immutable`. Immutability on S3 is
the conditional writes; the documented hardening is a bucket policy denying
`PutObject` on `blocks/*` without `s3:if-none-match` and `DeleteObject` on
`blocks/*` except to a sweep role, and a lifecycle rule expiring `tmp/` after
a day and aborting incomplete multipart uploads after a day. The Store Log is
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
shadowed: it reads as absent, and `explore` lists up to 20 shadowed pointers
on stderr.

Snapshot and page storage is `SnapshotStorage` (`LocalSnapshotStorage`,
`S3SnapshotStorage`), §15.
```

§8: replace `- getBashLib/getUploadCmd: return null (local executor only in the skeleton).` with `- getBashLib/getUploadCmd: return null. No task script ever touches the scheme: tasks unstage to the work dir as usual and the head node publishes from there (measured on Batch, ticket 05).` Replace upload steps 2 and 3 with:

```
  2. Regular file: the address comes through `PublishAddresser` (the Address
     Provider seam, spec §3), in this order: the task node's digest in
     `<task dir>/.command.cas` when `cas.nodeHash` is on (task dir = the first
     two path segments under `workDir`; the file is read once per task
     directory per run, ignored above 16 MiB); for an S3 source into an S3
     member, `S3BlockStore.copyFrom` (below); else the head node streams the
     file (a default-filesystem file into an S3 member is hashed in place and
     uploaded from it; any other source spools through `cas.tmpDir`). A node
     digest and a computed address that differ abort the run, whether the
     computed one is the head node's or S3's SHA-256 of a copy; a copy that
     fails for any other reason warns and falls back to the head-node read. Record
     `(key -> StoreRef, size, provider)`: `fusion-node` when the node digest
     named a block already held or drove an `UploadPartCopy`, `s3-copy` when
     S3 returned the SHA-256 of a copy, `head-node` when the head node read
     the bytes.
  3. Directory: `DirectoryManifestBuilder` with the same addresser for every
     file inside; the manifest itself is encoded on the head node and
     recorded `head-node`, and the provider of each file inside travels with
     it (`Publish.contents`), so `RunCompletion.providers` covers every
     address the run published. `toRealPath` is used only where the provider has
     it (an object store has no links to resolve, ticket 05). From an object
     store, a directory holding a `.fusion.symlinks` object has each listed
     name decoded as a link whose target is its object's body (ticket 15;
     rules in §6), the sidecar left out; every file is `regular`.
```

and add after it:

```
`S3BlockStore.copyFrom(sourceBucket, sourceKey, size, expected)` (ticket 16):
up to 5 GiB, with a node digest, `HEAD` the final key (present: `fusion-node`),
else `CopyObject` straight to it with `If-None-Match: *` and SHA-256, compared
with the digest (mismatch: delete, abort); without one, copy to
`tmp/<uuid>` with SHA-256, `HEAD` the final key, copy staging to final with
`If-None-Match: *` (412 is success), delete staging. Above 5 GiB, with a node
digest, `UploadPartCopy` to the final key; without one, null, and the head
node reads. The head node's byte count and each provider's count are logged
at info at `onFlowComplete`.

`download` into an S3 target of a block an S3 member holds is a `CopyObject`
with SHA-256 from the member's key, checked against the CID; a `symlink`
manifest entry staged onto any non-default filesystem becomes a copy of what
it names inside the tree (ticket 15 decision 7). Nothing is made executable:
manifests carry no execute bit.
```

- [ ] **Step 3: §11, §12, §14, §15**

§11, `onFlowCreate`: append "Then, for an S3 writable member, one `HEAD` on its snapshot key and a comparison of the local clock with the response's `Date`: above 1 minute a warning on the terminal, above 5 minutes `ClockSkewException` (an `AbortRunException`) naming the skew and NTP (ticket 03 decision 2). `put` and `explore` check the same at start and exit 1 above 5 minutes." `onFlowComplete`: append "The RunCompletion is written at schema 2 with `providers` (§6). The snapshot base is taken before the catch-up; the snapshot is not rewritten when the writable member's catch-up threw (`catch_up_failed`) or a guard of §15 holds." Add a bullet for the factory: "`CasObserverFactory.create` also installs node-side hashing when `cas.nodeHash` (default `fusion.enabled`) is on: `NodeHash.install(session.config)` puts `node-hash.sh` ahead of every `afterScript` string in `process` and in each `withName:`/`withLabel:` selector (the process scope's default when none is set); a closure `afterScript` is left alone with a terminal warning, and those tasks fall back to the head node. The script reads `.command.run`'s `### outputs:` patterns, expands them (`nullglob`, `globstar` where the shell has it), skips links and absent names, hashes files and the files under directories with `sha256sum` (or `shasum -a 256`) into `.command.cas`, and never fails the task. `afterScript` is not in the task hash (rule 5)."

§12: after the "Rebuild" bullet add

```
- *Amended 2026-09-28 (ticket 04):* `catchUp(store, log, member, snapshots,
  tempDir)` seeds first when the cache has no `store_log_watermark:<m>`
  (the only trigger, ticket 04 decision 6): it fetches the member's snapshot, and when its
  `schema_version` matches copies each run, Selection and Claim it does not
  hold with their rows (`run.member` and `log_entry.member` set to the
  alias), recomputes `claim_current` for every seeded subject, adopts the
  snapshot's watermark, marks `block_scan:<m>` done and records
  `seeded_from:<m>` = its `snapshot_written_at`; then the tail is read with
  the usual overlap. No usable snapshot (absent, another schema_version,
  unreadable) means the full scan, with a terminal warning naming the member,
  the reason and `nf-blocks:snapshot` when the member's Store Log is not
  empty. `cas.index.path` on persistent disk (EFS, FSx; one file per head
  node, since WAL needs shared memory) skips seeding; SQLite on S3 is not
  supported. The cache file is named by the members' location texts, S3 URIs
  included.
```

§14: replace the section with the four-way description extended: "...runs the Test Pipeline five ways (cold; again into the same store with `gate/node-hash.config`, so its files are addressed from `.command.cas`; `--fail`; `-resume`; from a second launch directory), the consumer, then the consumer twice more on a deleted cache (`consumer-seeded`, with the producer's run metadata blocks unreadable; `consumer-scan`, with the snapshot removed too), then `gate/assert.py` ... Lineage tier: 12 PASS, 0 FAIL, 6 SKIP. *Tier two* (`make gate-tier2`, `gate/tier2/`, on demand): the scidev Batch queue, a throwaway S3 member, T1-T6 and T2b (`gate/tier2/README.md`); run before a milestone that touches the S3 store, the Fusion provider or the cloud publish path is accepted."

§15, "Index Snapshot": replace the "Written by inserting ... atomic move over the old file." bullet's last sentence with "The build is local (`IndexSnapshot.build`); `SnapshotStorage.replace` puts it in place: locally an atomic move, on S3 one `PutObject` with `Cache-Control: no-cache`, `x-amz-meta-runs: <run rows>` and `If-Match` on the ETag it replaces (`If-None-Match: *` when there was none); a 412 skips the rewrite (`replaced_meanwhile`)." Replace the "Writers:" bullet with: "Writers: a run's `onFlowComplete`, `nf-blocks:snapshot` (any size), `nf-blocks:explore` (start and exit, any size). Each takes the base before its catch-up and writes only the writable member's snapshot, and none writes when the old snapshot is over `cas.snapshot.maxBytes` (the run only), when the new one has fewer `run` rows than the old (`fewer_runs`; an S3 snapshot without `x-amz-meta-runs` is downloaded once and counted), when another writer replaced it (`replaced_meanwhile`), or when the writable member's catch-up failed (`catch_up_failed`). Skips log at info; the verb prints them and exits 0." In "What a member serves", add "On S3 the page is `no-cache` and rewritten only when its stored `ChecksumSHA256` differs." Decision 9 of "Decisions made where the spec is silent (2026-09-25)" gets "*Superseded 2026-09-28:* S3 members may be writable and resolvable (§2)."

- [ ] **Step 4: §17 and the spec**

Add `## 17. Milestone 4: cloud (2026-09-28)` after §16: status line (filled at acceptance), the plan path, the map, then the 23 carried decisions of this plan's header as a numbered list, each one sentence with its ticket link (`../.scratch/post-gate/issues/<nn>-*.md`), then "Decisions made where the tickets are silent, as built" copying this plan's 24 with any change the review made.

In `../.scratch/content-addressed-lineage/spec.md`:

§1.2, replace `**Tier two, nightly**:` with `**Tier two, on demand** (before a milestone touching the cloud paths is accepted; \`make gate-tier2\`):`, and after its sentence add "Milestone 4 carries the cloud-executor publish into an S3 member (T1), Fusion (T2, T2b), directory manifests from the work bucket (T3), read-back on Batch (T4), a cold head node (T5) and two concurrent writers (T6); sarek, the sweep and the portability run come with their milestones."

§3, replace

```
an object store's native whole-object digest where one is cryptographic,
which today none is, since S3's SHA-256 is composite-only for multipart objects
and only CRCs linearise; and the head-node streaming read, which always works.
```

with

```
an object store's cryptographic digest of the whole object where it computes
one: S3 does for a single PutObject or CopyObject with SHA256, up to 5 GiB,
even when the copy's source was a multipart upload (measured 2026-09-27,
ticket 14), so a server-side copy into an S3 member yields a real address
(provider `s3-copy`, asserted) with no head-node bytes; and the head-node
streaming read, which always works.
```

and replace

```
On S3 the default streams through
`upload()` and hashes in flight; a server-side copy is an opt-in that yields
Unaddressed Records, because cheap transfer and a real address are mutually
exclusive there today.
```

with

```
On S3 the default for an S3-to-S3 publish up to 5 GiB is the server-side
copy with SHA-256; above it, a node digest drives a part copy, and otherwise
the head node streams the bytes. The provider is recorded by the run, in its
RunCompletion, never in a content-derived block, so no choice of provider
changes an address (ticket 16).
```

§14, replace "At `onFilePublish` the head node fetches `<taskdir>/.command.cas` once per task; provider `fusion-node`; a miss falls back to the head-node read." with "When publishing, inside `upload()` (the address is needed before the Pointer File is written), the head node fetches `<taskdir>/.command.cas` once per task; `### outputs:` lists patterns, not files, so the script expands globs and skips absent optional outputs (measured on Batch, ticket 06). The digest then names the block (`fusion-node` when the member already holds it) or is cross-checked against S3's own SHA-256 of the copy (`s3-copy`), and a miss falls back to the head-node read." Replace the last sentence ("The xattr sidecar ... is a Fusion internal.") with "nf-blocks reads Fusion's link encoding (`.fusion.symlinks` and the link objects), because it is part of a directory's content, and no other Fusion internal: the xattr sidecar, also measured to persist, is not used. Tier two pins the encoding, measured at Fusion 2.5.14 (ticket 15)."

- [ ] **Step 5: README**

Update the status paragraph (milestone 4: S3 members, Batch, Fusion). Add a section "## Publishing into S3" with: the config (a writable `s3://` member, an S3 `workDir`, `aws { region; profile }`), the scratch note for `cas.tmpDir`, the storage-class rule, the hardening policy and lifecycle rule (as JSON), that `aws.client.s3Acl` is not applied to member writes (storage class, SSE, KMS key and requester pays are), that under Fusion `cas.nodeHash` is on by default and outputs are hashed on the node, that without Fusion an S3 work dir flattens links to copies, and that `nextflow lineage find` on an S3 member costs a GET per record. Add "### A head node that starts cold" (a Batch head job, Platform launches): the first catch-up seeds from the member's Index Snapshot; `nextflow plugin nf-blocks:snapshot` on a schedule keeps a large shared member's snapshot fresh; `cas.index.path` on EFS or FSx skips seeding (one file per head node).

- [ ] **Step 6: Acceptance**

Run, in order, and paste each result into §17's status line:

1. `./gradlew check` (unit tests, `memoryBoundTest` with the S3 multipart case, `dependencyCheck`, `webTest`).
2. `make gate` with a fresh `GATE_ROOT`: lineage 12/0/6, browser tier A 5/5, tier B 12/12.
3. `make gate-cloud GATE_ROOT=<root>` (Rob): A6-A7, the explorer reading a private S3 member through nf-amazon's client.
4. `make gate-tier2 GATE_ROOT=<root>` (Rob): T1-T6, T2b.
5. By hand (Rob), `explore` against the tier-two member before teardown is not possible (the harness deletes it), so once: a config with a writable `s3://` member in a bucket Rob names, `nextflow plugin nf-blocks:explore`, make and name a Selection in the page, and confirm with `nf-blocks:items` that it is there; then delete the bucket.

- [ ] **Step 7: Commit**

```bash
git add DESIGN.md README.md
git commit -m "docs: milestone 4 in DESIGN.md (S3 members, providers, Fusion, seeding, tier two) and README

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

(`spec.md` lives outside this repository; it is edited in place and not committed here.)
