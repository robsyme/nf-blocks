# P4 report, plugin half: projects, input sets and hand-offs

Lead: `p4-plugin`, reporting to the P4 lead, who folds this into the phase report. Branch `explorer/p4-projects`, base `461533a`. Contract: `docs/plans/2026-10-24-explorer-v0.3-p4.md` sections 1 and 2, as amended after my review (`2649b1f`). Plugin plan, with rulings R1 to R9: `docs/plans/2026-10-25-explorer-v0.3-p4-plugin.md`. The rules as built are DESIGN.md §22, with notes in §5, §6 and §19.

Status: done, including the fix wave from the whole-branch review, three later items from the orchestrator ("After the first report"), the last round after P3's follow-up merged ("Last round"), the follow-up round after P4 merged ("Follow-up round"), the second follow-up ("Second follow-up") and the third ("Third follow-up"). `make test` at `f13852c`: 2,538 tests, 0 failures, 0 errors, 8 skipped (4 opt-in roblab tests, 4 pending scrub probe rows).

## Format changes (flag to the orchestrator)

- New block kinds Project, InputSet and Handoff (DESIGN.md §6 schema, 29 types).
- New Store Log kinds `project` (`log/<rts>-project-<cid>`) and `handoff` (`log/<rts>-handoff-<cid>`).
- Every RunCompletion a P4 build writes links an InputSet in `input_set`, an empty one when the run read nothing. Old RunCompletions (`input_set: null`) read as before everywhere. `params.input` stays `[redacted-location]` in the RunManifest.
- An older build's sweep would trash Project and Handoff blocks, the files a Handoff generated, and anything only a Handoff's Selection reached, and leave their entries dangling: a pre-P4 build ignores `project` and `handoff` entries, as a pre-P3 build ignores `transfer`. It keeps InputSets: its Mark already kept `input_set` as run metadata (R1).
- From this build on, a sweep refuses to apply beside a Store Log entry it cannot read (R8, the orchestrator's addition), so later kinds are safe against this build.
- Index: schema version 5 (last round). Every table in one DDL, P3's `transfer` tables and P4's `project`, `project_stats`, `run_input`, `handoff` and the `claim_current_attribute_value` index included; the snapshot is `index/v5.sqlite`. A v4 cache is recreated and a v4 snapshot is not seeded from; both are rebuilt from the Store Log. `web/test/fixtures/schema.sql` is unchanged.
- Config: `cas.inputs.hash` (Boolean, default true) and `cas.inputs.waitSeconds` (whole number, default 600).
- InputSet `reason` values: `unaddressed`, `unreadable`, `remote`, and `unfinished` (an input still being stat'ed or hashed at the wait limit; a set is partial exactly when one has it). No other field marks a partial set.
- Behaviour change in P2's code: Put accepts `add pin` and `del pin` on a Selection (F5, below). Mark and the storage figures already handled pinned subjects of any kind, so only `Put.PINNABLE` changed; the snapshot format is unchanged.
- Explore server: `POST /api/handoff`. `POST /api/put` builds a third kind, Project, and validates the P4 Claims.

## What was built, by interface section

| Interface | What | Commits |
|---|---|---|
| 1.1 Project | `Project` block; `Put` builds it from `{kind: "Project", name, description, created_at}` with `invalid` and `clock_skew` as for Claims; `project` log kind; `nf-blocks:put` takes it (the request's `kind` picks; there is no `--kind` option) | `07de1e9`, `3ad74f9` |
| 1.2 Claims | `add` for project, sample, annotation, same_sample, policy.people; value shapes; subject and value kinds against the store; P4 attributes supersede and are superseded only within their own attribute; `claim_current_attribute_value` | `3ad74f9`, `5f43c87`, `5c215f8` |
| 1.3 InputSet | `InputRecorder`: what a task staged from outside the run, roles samplesheet > param > task, `asset` for files under projectDir, sample from the `meta` component only, hashing on 4 daemon threads with a bounded wait, `cas.inputs.hash` and `cas.inputs.waitSeconds` | `8428b1b`, `7973f94`, `da82560`, `175c5b6` |
| 1.3 index | `run_input` and `consumer` from each run's InputSet; inputs never `missing`, `block` or `block_ref` rows (R2); wait records for an InputSet not yet readable | `83774f3`, `1a92954` |
| 1.4 | `project` and `handoff` rows from their entries; `project_stats` at snapshot time; snapshot copy and seed of the P4 rows; the migration rescan (R7) | `5c215f8`, `83774f3`, `b0af4fd`, `1a92954` |
| 1.5 | `Handoff` block; `POST /api/handoff` (`HandoffWriter`), raw blocks for the generated files, the `handoff` log entry last, params scrubbed in the block and unscrubbed in the response | `07de1e9`, `5b723af`, `d836d52` |
| sweep | Project and Handoff roots (R5, R6); InputSet kept as itself (R1); the unknown-kind refusal and its re-check (R8) | `4a87241`, `bf63dc0` |
| 1.8 | `CopyClosure.categoryOf`; a carried membership brings its Project, with its Claims; `CopyService` logs it as `project` at the destination | `4ecdf6f`, `c49508d` |
| page support | the web half's P4 queries in `ExplorerQueriesTest`'s no-scan check; `run_input_address` covering the shared-input query | `5dcff26`, `2c19b0a`, `643c5fe` |
| real data | `gate/p4/input-sandbox.sh`, a sandbox store with populated InputSets | `016db3a` |
| docs | DESIGN.md §22, and §5, §6, §19 | `ce535c3`, `c5761a1`, `529f385`, `9d2c500`, `62dc4c9`, `c5f73c8` |

## After the first report

Three items reached me through the P4 lead after the report at `25fb538`.

1. Orchestrator ruling: never download a remote input just to hash it (F4, `c399db2`, `47beb76`, DESIGN.md `03d7c4e`).

   | Input | Content read? | Recorded |
   |---|---|---|
   | local file | yes, stat and hash on the pool | address, size |
   | https/s3 file Nextflow staged into a local `work/stage-*` | yes, the local copy Nextflow already downloaded for the task; nothing is downloaded for hashing | address, size |
   | local source staged into a remote work dir | yes, the local source | address, size |
   | s3/gs/az/http(s) path, e.g. any task input with an s3 work dir (Batch, Fusion) | no; one stat on the pool | reason `remote`, size when the stat answers |
   | `/fusion/...` path (Fusion's mount, which looks local) | no | reason `remote` |
   | `cas://` input | no; its cid is known | address, size |
   | remote `params.input` | no (the earlier 16 MiB read is removed) | role samplesheet, reason `remote`; when a task staged it, the hashed `stage-*` copy stands for it instead (`47beb76`) |
   | local `params.input` | yes | address, size |
   | local directory | no; stat only | reason `unaddressed` |

   Names are the base name of the path, with a URI's query, fragment and userinfo dropped, so no presigned S3 signature, Azure SAS token or basic-auth password reaches an InputSet. Tests cover each, asserting none of `X-Amz`, `sig=`, `user`, `pass` or `AKIA` appears in the block. The helper is local and marked to switch to P3's shared `Leaks` scrub when P3's follow-up merges.

   The task thread no longer touches the file system: each input's stat, then its hash, runs on the pool, once per path per run, and a storeDir's symlinks are resolved there too. Stats, hashes and storeDir resolution share one `cas.inputs.waitSeconds` deadline (a failed run: at most 30 s of it). What is still pending then gets reason `unfinished`, with its size if the stat had answered. The run finishes with its own status and its RunCompletion is written; tests hang a hash and a stat and check that completion returns within about the bound.

2. Orchestrator: no credential may reach an InputSet, and the per-file stats share the wait limit. Both are in F4 above.

3. Orchestrator ruling on a bug the acceptance run found: the page writes `add pin` on Selections (the retention policy, a hand-off's Save, "Add and pin", and P2's own Selection page Pin…), and Put refused it with 400 `wrong_kind` because `PINNABLE` excluded Selections. Ticket 21 answer 6 is "pins on any subject". F5 (`3889f27`, `6520b8f`, `3f98a33`, DESIGN.md `888bdab`):
   - Put accepts `add pin` and `del pin` on a Selection. Pins on a Claim, Handoff, RunManifest, InputSet or unknown kind stay `wrong_kind`; on a Project, `invalid`; on an absent raw block, `not_found`.
   - Mark, the sweep and P2's figures needed no code change: they already judged pins per subject whatever its kind. New tests pin it: a pinned Selection keeps its closure through a clean `delete` and an applied sweep (with D2 across members); `del pin` then frees it; `store_stats`, `asserter_stats`, `pinned_overlap`, freed-on-release and `project_stats` count it as pinned. Each new test was shown to fail against a mutant Mark or IndexSnapshot that drops pinned Selections.
   - `ExploreWriteTest` sends the exact DAG-JSON `write.js` `pin` and `unpin` send to the real explore server: 400 `wrong_kind` before the fix, 200 after.
   - No snapshot or cache format change; figures change in value only, and only when a Selection is pinned. The integrator should review the retention reasoning in the F5 tests, since this is P2's code.

## Last round

After P3's follow-up merged into `explorer/v0.3` (`30b8ef5`), the P4 lead merged it into this branch (`d65bbe8`); I resolved `Index.groovy`, the one plugin file in conflict, keeping both sides. Then the user ruled: "I'm the only user so far, so always prioritize clean code over backwards compatibility." The orchestrator turned it into a schema bump in place of migrations. Commits, in order:

| Item | What | Commits |
|---|---|---|
| v5 | `Index.SCHEMA_VERSION = 5`, every table in the base DDL. Removed: P3's `Index.AddedTables` hook (`ADDED_TABLES`, the rescan marks, `rescanOnce`, the reingest paths), P4's own `P4_DDL`, `gainLateTables`, `projectTables`, `p4Rescan`, the busy-session special cases, the snapshot's DROP_P4 path, `hasTransferTables`/`hasProjectTables` and CopyService's skipped snapshot, and their tests. `wasRead`'s allowance for an older build's oversized rows went too. New `IndexSchemaV5Test`: a v4 cache is recreated and its catch-up ingests a Transfer, a Project, a Handoff and `run_input`; a v4 snapshot is not seeded from; a run found only by the block scan gets its `run_input`. P3's `TransferRescanTest` became `TransferAfterV4Test` (REV6 still gives 11 B in 2 blocks). | `0f9d85b`, `c067589`, `a3b9fe0`, `980571d` |
| gate | every gate file on `index/v5.sqlite`; `gen_year.py` writes schema 5 | `43ceee9` |
| scrub (item 5) | secret names by segment (`monkey_reads`, `keep_tokens`, `keyfile` kept; `api_key`, `DB_PASS`, `MYSQL_PWD` redacted); `--passphrase`/`--pass`/`--pwd`; `name: value` and header forms in strings; Google, Anthropic and OpenAI key shapes; the integrator's 101 probe rows as tests; InputRecorder names through the shared `Leaks.withoutCredentials` | `521b646`, `9107a9d` |
| not_kept wording (item 6) | the CLI names the explorer route with a separate config file (`NXF_CONFIG_FILE`), never the pipelines' config | `23f59d9` |
| onWorkflowOutput race | `capturedOutputs`/`capturedIndexes` were plain maps written from Nextflow's actor threads; now under one lock. The test (16 threads, 6,400 outputs) kept 5 before the fix. This explains the Gate's intermittent checks 14 and 16 | `963cfc7` |
| docs | DESIGN.md §6, §12, §15, §22 | `115edfa`, `f13852c` |

Moot under the ruling: S1, the snapshot hole (a session without P3's added tables could publish a snapshot with empty tables and a watermark past their entries), HOLE1 and HOLE1b, and item 4 (an immutable `ADDED_TABLES`). There are no added tables any more: every v5 cache and snapshot has every table.

Kept on purpose (store data, not old builds): reading RunCompletions with `input_set: null` and old block and log formats; the InputSet wait records; the retry of missing logged blocks; the sweep's refusal on unknown log kinds; the block scan of a store written before the Store Log; `IndexSnapshot.baseOf`'s tolerance of a snapshot without meta or `store_stats` (a hand-made or damaged file); and the fetch that tells whether a base has storage figures when its S3 metadata is missing (an object copied by a tool that drops object metadata).

Measured: the full v5 rebuild of a scratch copy of roblab, a cold catch-up with no seeding, 279 ms including the open, and a snapshot write of 232 ms (1,806,336 B; 4 runs, 1,385 blocks, 2,030 block refs). roblab's `index/` held only `v3.sqlite`, not a v4 snapshot. The P2 acceptance figures still hold on v5 (142,134,100 B stored, dedup 1.547).

Scrub, accepted and open: the `name: value` form also reads error prose (`Missing key: sample` loses `sample`), and `--pass 2` loses `2`; nothing leaks. Still missed (4 `@PendingFeature` rows): `mysql -p x`, `-px`, a bare base64 `user:pass`, an AWS session token; also one-word compounds such as `PGPASS`. RunManifests a new build writes get a new address where a value is newly redacted or newly kept. The roblab audit on a scratch copy: 4 RunManifests, 0 findings.

## Follow-up round

After P4 merged (`explorer/v0.3` @ `95dec3c`), the integrator's review gave nine must-fix items and a list of should-fixes. The plugin half took items 1, 2, 3, the CLI half of 4, 6, 8 and 9, the should-fixes in `src/`, and one more from the orchestrator (the roblab test). Implementers worked in worktrees from `95dec3c`, and their commits were cherry-picked onto the branch. Each change was test-first.

| Item | What | Commits |
|---|---|---|
| Must 1, special files | Only a regular file is opened. A FIFO, `/dev/stdin`, `/dev/fd/N` (`--input <(...)`), `/dev/zero` or a directory is `unaddressed` and never opened. A FIFO with no writer can't hold completion, and a FIFO `params.input` is never raced. The reviewer's ReviewInputProbeTest is now `InputRecorderSpecialFilesTest`. | `ee13103`, `ccc7d8e` |
| Must 2, links into /fusion | A candidate's real path is resolved on the pool before deciding local or remote. A local link into `/fusion` is `remote` and only stat'ed. A dangling link is `unreadable`. | `ccc7d8e` |
| Must 3, names | A remote name is cut at `;`. A token-shaped stem becomes `[secret]` with its extension kept, and the name goes through `Leaks.redactCredentials`. A userinfo password containing `/` is dropped. `;jsessionid=`, `ghp_` in a path or a name, a Box shared token and `user:pa/ss@host` are all tested against the encoded block. | `ccc7d8e` |
| Must 4, CLI half | The not_kept route says to make a config file beside `nextflow.config` holding `includeConfig 'nextflow.config'` (or the `cas.stores` block) and the lineage line, then start explore with `NXF_CONFIG_FILE`. Checked on 26.04.6 and 26.10.3: `includeConfig` resolves relative to the including file, and `NXF_CONFIG_FILE` replaces the launch directory's `nextflow.config`. | `da67f87`, `4046d9a` |
| Must 6 (S3), Handoff storage roots | A logged Handoff is a storage root, as Mark makes it one. Its files are recorded as `block_ref`/`block` at ingest. The snapshot's releasing figure is what a sweep takes, tested against an applied sweep in five delete/pin combinations. The page sees `root.kind = 'handoff'`. | `75bc6ba`, `1f994d4` |
| Must 8 (S5), copy carries Handoffs | A copied Selection brings the Handoffs that cite it (announced in the source's log), with their files, in a new `handoffs` group, plus a `handoff` entry at the destination. A Handoff deleted at the source stays behind. The copied samplesheet resolves from the destination alone. | `493b73b` |
| Must 9, Gate A1 flake (issue 17) | The cause: `-resume cold` doesn't take a run name on 26.04.6, so the resumed run resumed `fail`'s session, and which tasks re-ran was a race. The Gate now resumes `cold` by its session UUID, with `workflow.output.overwrite = false` (`gate/resume.config`, resumed run only). Measured against `cas://`, `copyAttributes` and `'sha256'` both still re-copy. Assertion 4c now checks instead of skipping: cold's session, every task cached, no PublishDir failure with all sources at mode 000, exit 0, and 0 bytes addressed. | `ab80c5c` |
| should: HandoffWriter order | It signs as Put does: shape on a probe, then writable plus the held Selection and Project, then the signer. A refused hand-off never asks who signs. The class comment is fixed. | `279e31d` |
| should: PutTest table | P4 rows in "never asks who signs": sample, membership and same_sample on a non-Project, pin and retain on a Project | `e1a3e5f` |
| should: scrub | Now redacted: `PGPASS`/`SSHPASS`/`MYSQLPWD`-style names (a closed list of tool prefixes), `sshpass -p`, and `Cookie:`/`Set-Cookie:` values. Now kept: `pass_filter`, `vcf_pass_only`, MinKNOW `fastq_pass`, and `--pat GATC` (a nucleotide value). Probe 3 (62 rows) is a test table. | `5705c08` |
| should: deleted Project | DESIGN §21 and §22 say a Project or Selection deleted at the source arrives live, since deletions never travel. | `430d2b9`, `529f385` |
| roblab test (orchestrator) | RoblabStorageTest asserts the acceptance runs' reachable figures, that their frees-on-release can only fall, and store invariants (stored = raw blocks counted once = pinned + held + releasing; referenced ≥ stored). The historical totals are in a comment. It passes on today's copy and on a copy with the 2026-10-25 16:50 run removed, which reproduces 142,134,100 B / 1,285 / 219,874,873 / dedup 1.547 exactly. | `4396e08` |
| docs | DESIGN.md §6, §14, §15, §21, §22 | `132b657`, `25de372`, `e5ba491`, `7e363d7` |

roblab changed on 2026-10-25 at 16:50: a fifth run, `crazy_miescher` (pipeline `scratch-workflow`, Nextflow 26.09.2-edge, `asserted_by` anonymous). The orchestrator identified it as the user's own `make run`. It is not from P4. The opt-in RoblabStorageTest's acceptance totals moved by its 539 B in 12 blocks (stored 142,134,100 → 142,134,639, blocks 1,285 → 1,297, referenced 219,874,873 → 219,875,443). Per the orchestrator's ruling, the test no longer asserts whole-store totals (above). RoblabAuditTest: 5 RunManifests, 0 findings. RoblabCopyClosureTest is unchanged apart from the new `handoffs: 0` group.

Tests at `7e363d7` (plugin), then `17c6be9` (the web side's last commit):

```
$ make test          # at 7e363d7
BUILD SUCCESSFUL in 3m 28s
(build/test-results: tests 2728, failures 0, errors 0, skipped 8: 4 opt-in roblab, 4 pending scrub probe rows)

ExplorerQueriesTest at 17c6be9: 73/73 (rootShared's new join on handoff is a primary-key search)
$ cd web && npm test # at 17c6be9
tests 1066, pass 1066, fail 0
$ cd gate && python3 -I -m unittest discover
OK (skipped=1)
```

The final solo Gate pair at `17c6be9` ran back to back on fresh `GATE_ROOT`s, with Nextflow 26.04.6, a scratch `NXF_HOME` and AWS/Tower/Seqera env unset. Nothing else of P4's was running. The user's idle `nf-blocks:explore` (port 8123, from the explorer-v0-3 worktree) was up throughout. The Gate takes OS-assigned ports, so it never bound 8123.

| | run 1 | run 2 |
|---|---|---|
| gate.sh exit | 0 | 0 |
| Lineage | 19 PASS, 0 FAIL, 4 SKIP | 19 PASS, 0 FAIL, 4 SKIP |
| 4c, resumed run re-hashed nothing | PASS: cold's session, all 15 tasks cached | PASS: the same |
| resumed PublishDir failures | 0 | 0 |
| Browser A | 6/6 | 6/6 |
| A1 | PASS, 4 producer rows (again, cold, elsewhere, fail) | PASS, 3 producer rows (again, cold, elsewhere) |
| A2 | producers 8 req / 32,768 B, latest 4 req / 16,384 B, items 43 req / 176,128 B | the same |
| Browser B | 14/14 | 14/14 |

4c went from SKIP to PASS, so the lineage tier is 19/0/4 where it was 18/0/5. The 4 SKIPs are 4b, 8, 11 and 12. A1's 3 or 4 producer rows is `fail`'s own race (whether it published before exiting), which A1's rule accepts (gate/README.md). Logs: `scratchpad/gate-final-1.log`, `gate-final-2.log`.

Skipped or left open:
- Items 5, 7 and 10, the copy.js half of 4, the "Projects" label and the claimTag title are the web half's.
- Remote-input fingerprints (ETag or version id) are still not recorded, so runs on s3, Fusion or http storage get no input-based identity.
- The token-stem rule for names is a heuristic. A separator-free name such as `sample20261025run1lane2tile3` would be redacted.
- A Handoff whose Selection is logged only in another member counts only its files in this member's figures, the per-store limit of a cross-member release. A Handoff written into a member other than the copy's source is not carried.
- On resume the plugin records every leaf `never_published`, even when the coordinate's pointer names the address. A1 now relies on that, and would flip if the plugin took the address from the pointer.
- Scrub, still open: `mysql -p x`, `-px`, a bare base64 `user:pass`, an AWS session token, and `curl --cookie`/`-b`. `id_key` is secret-named. `Missing key: sample` loses `sample`.
- The scratch worktrees and `p4p/*` branches of every round are left for the user. Every one is patch-equivalent to the branch (`git cherry`), except `p4p/t3-index`, which differs by one resolved conflict.

## Second follow-up

The integrator blocked `af2abfe` on one regression (B1) and listed should-fixes S1 to S4 plus minors. The P4 lead merged P1a's follow-up (`explorer/v0.3` @ `5d40f0a`) into the branch as `39c1a6b`. Four implementers worked from `af2abfe`, their commits were cherry-picked, and each change was tested first.

| Item | What | Commits |
|---|---|---|
| B1 (blocking) | `read`, `reads`, `only` and `filter` no longer qualify `pass` when they come before it, so `DB_READ_PASS`, `READ_ONLY_PASS`, `db_read_pass` and `FILTER_PASS` are redacted again. So is a fifth regression, `GITHUB_PAT=GATTACA`. One predicate, `Leaks.keepsValue`, serves the scrub and the audit: plain booleans and numbers, the scrub's markers, whole locations, and a nucleotide word under `pat` keep their values. The reviewer's probe 4 is the test table `ScrubProbe4Test` (100 rows). Probes 2 and 3 are unchanged, and all 25 rows the review listed as improved stay improved. | `91529b9` |
| B1, a cut I added | Taken literally, "a plain number is never a secret" left `password=12345678` and `apiKey: 12345` readable. A plain whole number now has at most 3 digits, so `--pass 2` and `threads 8` stay readable. The P4 lead flagged this to the orchestrator. | `b74d50e` |
| scrub minors | `sshpass -P … -p x`, `{"Cookie": "sid=…"}`, and curl `-b`/`--cookie name=…` are redacted. `pat: 'GATC'` and `PWD=/home/x` stay readable (PWD becomes a location). | `91529b9` |
| S1, 4c with teeth | 4c also requires the resumed RunCompletion's `never_published` to equal `cold`'s Leaf count (21), with no providers and no addressed Leaf of its own. It fails on `AccessDenied`, `Permission denied` or a locked path in the log. A failed publish names its reader (the plugin's `upload()` or PublishDir's re-copy). Also: no truncation at 160 characters, gate.sh exits 2 when `cold`'s session is missing, the README counts are fixed, and 4c gets its own SKIP reason and reports the exit code first. | `bfa7602` |
| S2 | Assertion 19 resumes after a failed run in its own store. It measured that the default `overwrite 'standard'` re-copies and re-hashes the cached tasks' outputs on resume: 24 files, 21 of them cached, on 26.04.6. It reports that cost rather than asserting it away. DESIGN §8 tells users, and says `gate/resume.config` is not advice. | `9d85783`, `6a79992` |
| S2, a race in 19 | The first live pair at `c10a9ce` failed 19 once. A task that finished 71 ms after the failed run's session cancelled never reached its workflow outputs, so its Leaf existed only in the resumed run, which is correct. 19 now requires every Leaf the failed run recorded to keep its address, and a Leaf only the resumed run has to be addressed; the PASS line names such a Leaf. | `63006de` |
| S3, dedup ≥ 1x | `referenced_bytes` counts every holder: collection memberships, each Handoff's file Leaves, and items a Selection here holds from another member's run (R8). So referenced ≥ stored for any content, and the page's dedup (`web/src/model.js:941`) needs no change. RoblabStorageTest lost its tautology and its duplicate line, and checks the page's formula on the snapshot's figures. New fixtures cover a Handoff, a pinned Selection and a released run (stored 47 = referenced 47, was 38). | `61a06ac`, `7ff871a` |
| S4, Box and tokens | On a Box `/s/` or `/shared/static/` link the stem is always `[secret]`. Per the orchestrator's ruling, 2 letter/digit switches mark a token in a remote URL path, while a local file name needs 4, so `run20261025combinedreads.fastq.gz` on local disk stays readable. | `be2a491`, `05715b0` |
| 6, schema v6 | `SCHEMA_VERSION = 6` and `index/v6.sqlite`, so a v5 cache that ingested a Handoff before its files were recorded is rebuilt. The P4 lead approved changing `web/src/config.js` and the web fixtures in the same commit. The gate paths moved in `688b985`. | `9cd4f2b`, `688b985` |
| 7, names | `https://user:p@a/ss@host` is named `host`, and `user:pa/ss@host/dir/a@b.fq` is named `a@b.fq`. The literal rule in the review still left `ss`. The cost, pinned: `user:pass@host/data/x@1.fq` is named `1.fq`. | `be2a491` |
| Linux pipes | A realpath of `pipe:[n]` or `socket:[n]`, or a failed one, is judged by one stat, so a FIFO is `unaddressed`. Tested with a faked realpath; nothing has been run on a real Linux host. | `be2a491` |
| DESIGN | `cas://<handoff-cid>/samplesheet.csv` not resolving is by design. Also §6, §8, §14, §15, §22. | `6a79992`, `a140c26`, `c10a9ce` |

Tests at `c10a9ce`. The machine's load average was 5 to 8, from the user's desktop apps, not ours.

```
$ make test          # 30 min wall clock under that load (3.5-5 min before)
tests 2933, failures 0, errors 0, skipped 8 (4 opt-in roblab, 4 pending scrub shapes)
$ cd web && npm test # 1088/1088; npm run build OK
$ gate unit tests    # gate OK (1 skipped), tier2 OK, cloud OK (1 skipped)
```

At `63006de` the gate unit tests are OK; that commit changes only gate/ and DESIGN.md.

Opt-in roblab tests on a fresh `cp -R` copy: Audit 5 RunManifests, 0 findings; CopyClosure pass; Storage 2/2. Stored 142,134,639 B in 1,297 blocks, referenced 219,875,443 B, dedup 1.547. A v6 cold catch-up took 163 ms and the snapshot write 80 ms.

The final solo Gate pair at `63006de` ran back to back on fresh `GATE_ROOT`s, with Nextflow 26.04.6, a scratch `NXF_HOME`, nothing else of ours running, and load averages of 4.1 to 7.0:

| | run 1 | run 2 |
|---|---|---|
| gate.sh exit | 0 | 0 |
| Lineage | 20 PASS, 0 FAIL, 4 SKIP | 20 PASS, 0 FAIL, 4 SKIP |
| 4c | PASS: cold's session, 15/15 cached, exit 0 with 72 sources at mode 000, 0 re-copies, no lock line, 0 files addressed, 21 of 21 cold Leaves never_published, no providers | PASS, the same figures |
| 19 | PASS: 12 cached, MAYBE_FAIL 1-3 re-run, 3 reports match the Leaves, pointers and blocks, 18 cached Leaves kept, none "addressed now"; 24 files (357 B) addressed, 21 cached | PASS, the same figures |
| Browser A | 6/6 | 6/6 |
| A1 | 4 producer rows (again, cold, elsewhere, fail) | 4 rows, the same |
| A2 | producers 8 req / 32,768 B, latest 4 req / 16,384 B, items 43 req / 176,128 B | the same |
| Browser B | 14/14 | 14/14 |

The earlier pair at `c10a9ce`: run 1 passed every tier; run 2 failed 19 on the race fixed in `63006de`. Re-evaluated read-only, both now pass. Logs: `scratchpad/gate-f3-1.log`, `gate-f3-2.log` (this pair), and `gate-f2-*.log` (the earlier one).

Skipped or open:
- Scrub: `mysql -p x`, `-px`, a bare base64 `user:pass` and an AWS session token are still missed, and so is `password=/abc:hunter2` in a params string, because the path rule splits at the `:`.
- The three-digit cut for plain numbers is my call; the orchestrator may want a different one.
- Remote-input fingerprints are still not recorded.
- The resumed run records every leaf `never_published`, and 4c relies on that.
- The `p4p/*` scratch branches and worktrees are left for the user.

## Third follow-up

The integrator blocked `41d68c5` on one regression, B2. The user is on paid credits, so this round was kept small. I did the four plugin items myself, with no implementers and no whole-branch re-review. The P4 lead did item 4 (the page's `OLDER_SNAPSHOT_PATHS`). Each item was tested first.

| Item | What | Commits |
|---|---|---|
| 1, B2 | PLAIN's exponent form is `[-+]?(?:[0-9]{1,3}\|[0-9]*\.[0-9]+)[eE][-+]?[0-9]{1,3}`, and a decimal needs a digit after its point. So `api_key: '1234e567'`, `'0123E4567'`, `password=12345e678` and `12345678.` go, while `1e-5`, `1e-10` and `2.5e-8` stay. The integrator's probe 6 is `ScrubProbe6Test`, which asserts that each secret is gone from the stored block and reported by the audit, and that plain values stay. 13 of its rows were red before the fix. | `d1d0a9e` |
| 2, S8 | After `=`, a leading `-` no longer stops `SECRET_FLAG` or `BARE_SECRET`, so `password=-hunter2`, `--password=-hunter2` and `password=-12345678` go. After a space, a `-` can still be the next flag. | `d1d0a9e` |
| 3, S7 | An integral Double, Float or BigDecimal of 1000 or more counts as a whole number, so `12345678d`, `1234567f` and `BigDecimal('1.2345678E+7')` go. One `LeaksTest` row of mine (`2.5e9d` kept) changed accordingly. | `d1d0a9e` |
| 5, S2 | DESIGN (§12, §15, §22, the snapshot path line and "What a member serves"), README and test texts (IndexSchemaVersionTest's doc, IndexTest's feature name, TransferAfterV4Test's doc) now name schema 6. | `2564239` |

Verified at `2564239`, which includes the P4 lead's item 4 (`9ffc353`):

```
$ make test          # 3m 36s
tests 2962, failures 0, errors 0, skipped 8 (4 opt-in roblab, 4 pending scrub shapes)
$ cd web && npm test # 1088/1088; npm run build OK (index.html 1,954,750 B)
$ gate unit tests    # OK (skipped=1)
```

One solo Gate at `2564239` on a fresh `GATE_ROOT`, with Nextflow 26.04.6 and a scratch `NXF_HOME`. Nothing else of ours was running, and the load average was 4.4 to 5.9 from the user's desktop apps. Exit 0, 2 m 39 s. Log: `scratchpad/gate-f4.log`.

| Tier or assertion | Result |
|---|---|
| Lineage | 20 PASS, 0 FAIL, 4 SKIP |
| 4c | PASS: cold's session, 15/15 cached, exit 0 with 72 sources at mode 000, 0 re-copies, no lock line, 0 files addressed, 21 of 21 cold Leaves never_published, no providers |
| 19 | PASS: 12 cached, MAYBE_FAIL 1-3 re-run, 3 reports match their Leaves, pointers and blocks, and 17 cached Leaves kept their addresses. One Leaf recover-fail never published (its task finished after the cancel) is addressed now: `qc C_qc`. The 63006de race allowance was used live. Per parked S5, the allowance doesn't yet confirm that the task finished after the cancel or hash the Leaf. 24 files (357 B) were addressed, 21 of them for cached tasks. |
| Browser A | 6/6. A1: 4 producer rows. A2: producers 8 req / 32,768 B, latest 4 req / 16,384 B, items 43 req / 176,128 B |
| Browser B | 14/14 |

### Parked

As the integrator wrote them (S1 and S2 are this round's items 4 and 5, and S7 and S8 are items 3 and 2):

- S3. A copyChoice that never settles leaves Copy to… dead with aria-busy and no href (addproject.js:146-153); there's no fetch timeout anywhere in web/src. Bound the wait, then aim at the copy page with every project named, so nothing is preselected.
- S4. 4c doesn't check OutputCollection paths. Join gives the same never_published Leaf to a value that was never a cas:// path as to a cas:// coordinate that was never published; only `paths` differs. The reviewer's case 04, with all 21 resumed paths nulled, still PASSes 4c. Fix: require resumed paths equal to cold's per output, no nulls; prototype in p4g-r3/probe/paths_check.py.
- S5. Assertion 19's race allowance can pass vacuously.
  - A "late" Leaf only needs some address. Its task is never confirmed to have finished after the cancel, and it's never hashed.
  - Cases 21 and 22 (drop collections from recover-fail) PASS with "0 cached Leaves kept their addresses", although recover-fail's log shows every good task completed before "Execution cancelled".
  - Case 24 (drop chunks, point chunk_1 at chunk_2's block) PASSes.
  - Fix options: accept a missing Leaf only for tasks whose "Task completed" comes after "Execution cancelled"; or hash late Leaves against the cached task's work file; and require len(before) > 0; or remove the race with errorStrategy 'ignore' + workflow.failOnIgnore = true, if 26.04.6 has it.
- S6. 4c's log check can't see the likeliest warn-only read: InputRecorder.submit logs a failed hash at DEBUG (InputRecorder.groovy:480-482), and plugin DEBUG never reaches nextflow.log (0 such lines in the whole root). The test uses a DEBUG line that can't occur in a real log. No gap today, since the Test Pipeline has no file inputs, but the claim is overstated in the assert.py docstring, README:358, DESIGN §14 and browser_assert:155. Fix: -trace robsyme.cas on resumed, or fail on an `unreadable` InputSet entry.

Minor (the "stale test text" item is fixed by item 5, `2564239`):

- Inputs (all already at af2abfe):
  - https://user:p?ss@host/x.fq gives the fallbackName "user:p", because withoutCredentials cuts the query before the '@'. This only applies when params.input doesn't resolve (InputRecorder.groovy:273).
  - https://user:1234/ss@host with no path gives ss@host; holdsUserinfo reads it as host:port.
  - x_ghp_<36>.txt keeps the token: GITHUB's lookbehind (Leaks.groovy:55) excludes '_'.
  - APP.BOX.COM/S/ in upper case isn't treated as Box (InputRecorder.groovy:92).
  - Confirm s3 keys count as "remote URL path"; the reviewer read it that way.
- A signed list element: ['12345678', 123, -1234] stores -'[secret]'. Nothing leaks.
- Assertion 19's PASS text says "re-copies every cached file" even when the cached count is 0, and "18 Leaf … is addressed". The cost (24 addressed, 21 cached) is recorded, not asserted.
- fail.config's comment overstates "all 15 submitted at once": submission is serial, about 6 ms apart, and QC_DIR went after MAYBE_FAIL had started. The risk is a flaky FAIL, not a false PASS. The comment doesn't mention 63006de's publish race.
- make_fixture.py regenerates a different gate/fixtures/root/cache sqlite set from the one checked in.
- copyChoice rejecting falls back to the unaimed href, i.e. P3's first target: the Q1 issue in the error case.
- aria-busy on role=link isn't reliably announced. aria-disabled, or a status line, would be clearer.
- StorageStatsTest.pageDedup is a Groovy copy of model.js's formula, so on roblab it can't catch lost multiplicity. The fixtures do catch it.
- Stale test text: the IndexSchemaVersionTest class doc says schema 5 and tests only v4 as the older version. IndexTest:729 is named "schema 5 carries…" but asserts 6. TransferAfterV4Test:27 says "(schema 5)".
- referenced() drops both seeds of a ref cycle (StorageStats.groovy:357-361). That's impossible with content addressing, but unguarded, unlike closure().
- Pre-existing undercounts: a pin on a raw block no owner indexes is kept by Mark but not in stored. A held Selection of a swept run's item reads 0 B while the store still has it.

## Measurements

Hashing cost (design doc §4 experiment 4). Apple M3 Pro, JDK 21, warm page cache (the files had just been written; the cache could not be dropped without sudo). Fixture: 8 fake FASTQ files of 512 MiB, 4,295,033,032 bytes.

| What | Result |
|---|---|
| `Hashing.hashRaw`, 1 thread | 1,784 and 1,851 MiB/s |
| `Hashing.hashRaw`, 4 threads | 5,673 and 5,574 MiB/s |
| a 4-sample run reading 4 GiB of local inputs, `cas.inputs.hash = true` | 4.32, 3.28, 3.48 s wall; waited 421, 380, 427 ms for hashing at completion |
| the same, `cas.inputs.hash = false` | 3.39, 3.10, 2.93 s wall |

Hashing stays on by default. Not measured: a cold read on shared storage. Such a read, or a resume where every task is cached and the run ends at once, pays the full read at completion, bounded by `cas.inputs.waitSeconds` (default 600 s). Nothing is remembered across runs, so every run hashes its inputs again.

project_stats cost. On `gen_year.py`'s year (1,825 runs, 547,500 file references) with 41 projects and 4,209 memberships, one of them all 1,825 runs: `StorageStats.compute` went from about 2.0 s to 2.3 to 2.5 s, with no extra pass over `block_ref`. Measured with a 4 GiB test heap; at the default heap the year's figures run out of memory and the snapshot goes lean whatever P4 adds (pre-existing).

Sandbox store (`gate/p4/input-sandbox.sh`). Real nf-core/rnaseq runs: `rnaseq-output-records`, the same fork of the pipeline (typed processes, workflow outputs) that made roblab's runs, `-profile test,docker,arm64`, Nextflow 26.10.3, nf-blocks 0.9.0-beta.1. Store at `<scratchpad>/p4-sandbox/store`, alias `p4sandbox`. Results of the last build, at `47beb76` (F5 does not change what the sandbox records):

| Run | How | Wall |
|---|---|---|
| p4-rnaseq-1 | fresh | 232 s |
| p4-rnaseq-2 | fresh, own work dir | 238 s |
| p4-rnaseq-wt-short | `-resume` of run 1, WT_REP1's reads cut to their first 40,000 lines | 95 s |
| p4-tiny-reads | a small local pipeline | 3 s |

- Runs 1 and 2 have the same InputSet block and share 11 task-role input addresses with a sample: exactly the reads.
- p4-rnaseq-wt-short shares 7 with each. Its WT_REP1 shares 0; the other four samples share 1 or 2 each.
- The tiny run shares nothing with any rnaseq run.
- Every run's InputSet equals its `run_input` rows, and `params.input` is `[redacted-location]` in every RunManifest. Every input was addressed, the https reads included (hashed from Nextflow's staged copies). Runs 1 and 2's https samplesheet was never downloaded by the plugin; its address comes from the copy the pipeline staged, the same in both runs.
- Reference params still carry a sample in this pipeline (`param:gtf` has `gtf`, `param:salmon_index` has `salmon_index`, ...): the fork passes references as typed records with their own `id`, which the `meta`-only rule cannot tell from a sample record. Their role is `param:*`, which the page's identity and suggestions ignore.
- The first build at `c2ab9a8` found nf-core's `multiqc_config.yml` recorded as a task input with sample `multiqc_report`, which would have linked any two rnaseq runs; `da82560` made files under projectDir role `asset` with no sample.

## Decisions

The P4 lead accepted my interface review (`2649b1f`): inputs not held are never `missing` rows; `sample` comes only from the input holding the file; role precedence; the `handoff` table; Handoff params scrubbed in the block and unscrubbed in the response. The orchestrator ruled R8 and, via the P4 lead, R9 (a default-store policy never overrides `readOnly`; sample, annotation and same-sample Claims hang off the Project; DESIGN.md says a global sample identity waits on input addresses and would need a sample subject).

My rulings, each with what it costs if wrong:

- R1. Mark keeps an InputSet as itself and does not follow its input addresses: a run does not own what it read. Cost: an input nothing else reaches is swept while its run is held.
- R5. A Handoff keeps its files and its Selection whole; its project link is not followed. Cost: a hand-off keeps the Selection it cites after that Selection is deleted, until the Handoff itself is deleted.
- R6. A Project is rooted like a Selection, itself only.
- R7 (superseded in the last round by schema v5). No schema version bump; a one-time rescan per member when a cache gains the tables.
- A file under the pipeline's projectDir is role `asset` with no sample, and a file under a task's `storeDir` is not an input. Cost: real sample data shipped inside a pipeline's directory (rare; nf-core uses remote test datasets) loses its sample.
- A sample comes from a tuple component named `meta` (or a typed record's `meta.id` or `id`). `meta2` and other names give none, since nf-core names references that way. Cost: a pipeline that names its sample map something else gets no input-based identity.
- Every run writes an InputSet, an empty one when it read nothing, so null means "before P4".
- A snapshot whose cache can't vouch for its P4 rows (no tables this session, or a rescan pending) is written without `project`, `handoff` and `run_input`, so the page tells "not known" from "none". `project_stats` stays.
- An InputSet not yet readable is a wait record in meta, not a `missing` row, so it never marks storage figures partial and is retried for its own member only.
- `project_stats`: memberships from the whole composition's Claims; root states from the member's own Claims, as P2. No row for a project with nothing here. `reachable_bytes` includes releasing bytes, unlike `asserter_stats`.
- Put: a held Project subject accepts only the P4 attributes, `name` and `delete`/`del` (`invalid` at /attribute otherwise); a P4 Claim supersedes and is superseded only within its attribute.
- `/api/handoff`: plain JSON with cid strings or `{"/": cid}`; checks in Put's order, so a dry run checks only the shape; a template is `[A-Za-z0-9._/-]{1,256}`; raw files the writable member already holds are not rewritten. The block's params go through `Records.scrub`, which also drops store-local top-level keys (`outputDir`, `resume`, ...).
- The unknown-kind refusal: any `<13 digits>-...` name this build cannot read counts, including a known kind with a cid it can't parse; a refused sweep stops before marking, so its report has no figures (a dry run has them).
- Process: implementers worked in their own worktrees from the shared branch head; I applied their commits onto the shared branch as patches with the same messages, committing only my paths.

## The whole-branch review

A `superpowers:code-reviewer` pass over `461533a..c5761a1` found no Critical issues, 3 Important and 11 Minor. All Important items and 8 Minor items were fixed test-first in one wave; the other 3 were documentation.

| Item | Fix |
|---|---|
| I1 one unreadable RunCompletion left the rescan pending forever | caught per run, retried through a wait record (`1a92954`) |
| I2 a successful run waited with no limit for hashing | `cas.inputs.waitSeconds`, daemon threads stopped without waiting; cold-cache cost stated in §22 (`175c5b6`, `c5f73c8`) |
| I3 nf-core `meta2` reference tuples gave a reference a sample | sample only from `meta` (`175c5b6`) |
| retry asked about every missing row; InputSet rows marked storage partial; the busy-session race at upgrade | wait records in meta; seeds in a no-tables session mark the member; `consumer` kept (`1a92954`) |
| supersede check one way only | both ways (`5f43c87`) |
| an InputSet failure would lose the RunCompletion | `input_set: null` and one console warning (`175c5b6`) |
| R8 missed hyphenated kinds and unparseable cids | any unreadable entry name counts (`bf63dc0`) |
| storeDir outputs recorded as inputs | excluded (`175c5b6`) |
| Handoff template free text | restricted (`d836d52`) |
| a deleted Project comes back through a copy; mixed builds on one cache; the §6 InputSet comment | documented in §22 and §6 (`529f385`) |

## Tests

```
$ make test          # at f13852c (last round)
BUILD SUCCESSFUL in 4m 48s
(build/test-results: tests 2538, failures 0, errors 0, skipped 8: 4 opt-in roblab, 4 pending scrub probe rows)

$ cd web && npm test # at f13852c
tests 1057, pass 1057, fail 0

$ cd web && npm test # in the shared worktree, which held the P4 lead's uncommitted web work at the time
tests 1028, pass 1028, fail 0
```

The 3 skipped tests are the opt-in roblab tests, not changed by P4.

The solo Gate (`gate/gate.sh`) at `f13852c`, on a fresh `GATE_ROOT`, Nextflow 26.04.6, scratch `NXF_HOME`, nothing else running, in a separate worktree (the same results as the earlier run at `f994e8a`):

| Tier | Result |
|---|---|
| Lineage | 18 PASS, 0 FAIL, 5 SKIP (as P3) |
| Browser A | 6 / 6. A2 unchanged: producers 8 req / 32,768 B, latest 4 req / 16,384 B, items 43 req / 176,128 B |
| Browser B | 14 / 14 |

Nothing was written under `~/.nextflow`.

New test classes: `IndexProjectTablesTest`, `P4BlocksTest`, `IndexProjectTest`, `IndexRunInputTest`, `IndexSnapshotProjectStatsTest`, `PutProjectTest`, `HandoffWriterTest`, `ExploreHandoffTest`, `InputRecorderTest`. Extended: `ExplorerQueriesTest`, `CopyClosureTest`, `CopyServiceTest`, `MarkTest`, `SweepTest`, `StoreLogTest`, `CrossMemberReleaseTest`, `RetentionCommandsTest`, `StorageStatsTest`, `CasObserverTest`, `CasConfigTest`, `CasConfigScopeTest`, `CasCommandsPutTest`, `ExploreCommandTest`.

Every index, query, schema, Put, sweep and recorder change was written test-first and seen red, with one exception: `IndexProjectTablesTest` (the first DDL commit) was written before the code but not run red first. The implementers' red runs are in their task reports; the review fixes each have a failing test that reproduces the finding.

## Open questions

- Closed in the last round: the move onto P3's `AddedTables` hook is replaced by the v5 schema bump (no hook, no rescan); a block-scan-only run gets its `run_input` on a fresh cache (`IndexSchemaV5Test`), and the roblab rebuild is measured above.
- Cold hashing cost on shared storage is not measured.
- Gate lineage check 8 ("Input Set from four consumption shapes") is still a skeleton SKIP. P4 now writes InputSets, so the check can be built: a second pipeline consuming the first run's outputs through `cas://`, `lid://`, `fromStore` and a copied file, asserting the InputSet's addresses.
- No ETag or version-id fingerprint for remote inputs (option (b) of the ruling): inputs read through s3, Fusion or http carry no address, so runs on such storage get no input-based identity. A fingerprint field would be a further format change.
- project_stats has no `partial` column, so a project that reaches a missing item shows no "at least". Root states use only the member's own Claims, as P2's figures do.
- A deleted Project comes back undeleted through a copy (deletions never travel, §21).
- The sweep's refusal message says "kind(s) run that this build does not know" for a known kind with an unreadable cid.
- A typed-record reference with its own `id` that no param names (so role `task`) would still carry a sample and could feed a false project suggestion. None occurs in the sandbox.
- Mark never reads a raw block, so a sweep does not report a missing raw file, under an item or a Handoff; DESIGN.md §19 decision 6 said it did and now says what is built.
- Scrub: the 4 pending probe shapes and `PGPASS`-style compounds; the colon form's over-redaction of error prose (`Missing key: sample`).
- Two builds sharing one cache file (`cas.index.path`) across the v5 bump recreate it in turn; each build needs its own cache path.
- The 13 `p4p/*` scratch branches and worktrees from the first rounds, and the two from this round (`p4p/v5-index`, `p4p/l1-scrub`, integrated by cherry-pick, so `git cherry` shows them all `-`), are left for the user to delete (see the P4 phase report).
