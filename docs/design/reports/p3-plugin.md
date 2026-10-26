# P3 report, plugin half: copy with provenance

Lead: `p3-plugin`, reporting to the P3 lead, who folds this into the phase report. Branch `explorer/p3-copy`, base `270f03a`. Spec and contract: `docs/plans/2026-10-24-explorer-v0.3-p3.md` sections 1 and 2, as amended during the phase (D2 narrowed, D7, D8, `run_outputs`, `runs[].lineage`, `claims[].carried/current/supersedes`). Plugin plan: `docs/plans/2026-10-24-explorer-v0.3-p3-plugin.md`.

Status: done, including the fix wave from the P3 lead's whole-branch review and the follow-up round after the merge (see the last section). Code and DESIGN.md are complete at `604e374` (T12, the last change); this report is the commit after it. `make test` at `604e374`: 1,867 tests, 0 failures, 0 errors, 3 skipped (the opt-in roblab tests). `cd web && npm test`: 876 tests, 876 pass.

## What was built, by interface section

| Interface | What | Commits |
|---|---|---|
| 1.1 CLI | `nextflow plugin nf-blocks:copy <root> --to <alias> [--from] [--depth] [--claims] [--dry-run] [--accept-audit true] [--format text\|json]`, exit 0/1/2 | `00c8fe6`, `2fbc879` |
| 1.2, D1 | `cas.stores.<alias>.readOnly`; `CopyTargets.judge` (read-only, or a local directory this process cannot write) | `808bd9b`, `8bbd9f4` |
| 1.3 | the plan and result body, shared by the CLI's `--format json` and the endpoint (`CopyService`) | `00c8fe6`, `2fbc879` |
| 1.4, D7 | `CopyClosure`: one walk, every block tagged with its shallowest depth, groups in 1.3 order, whole run metadata at every depth, task-level lineage with a join-key fallback, per-source-log claims | `ba17bea`, `6edc507` |
| 1.4 lineage | `LineageRecords` (local and S3) and `TaskLineage`, which read a member's `nf/` records | `4bc8c49` |
| 1.5, D8 | the `Transfer` block kind (with `runs`), the `transfer` Store Log kind, `transfer` and `transfer_root(..., role)` index tables, snapshot rows and seeding, Mark keeping the Transfer block | `2857018`, `904f3fd`, `c3129f3`, `1be351e` |
| D8 | a destination's snapshot presence-checks content under transferred runs and roots | `3e71eef` |
| 1.6 | `RunManifestAudit`, a scrub that now catches credential shapes, secret-named params and paths inside params strings, one shared classifier (`Leaks`) | `0e21aba`, `db6ddfd`, `1d5a018` |
| 1.7 | `members.json` per-member `kind`, `location`, `copy_target`, `copy_reason`, top-level `copy`; `POST /api/copy` | `808bd9b`, `8bbd9f4`, `daac0b0` |
| 1.8, D2 | the cross-member re-release rule in `Mark` | `d2668f5`, `f5f2ff7` |
| page support | the page's three transfer queries in `ExplorerQueriesTest`'s no-scan check | `37cb106` |
| docs | DESIGN.md §2, §5, §6, §12, §15, §19, new §21 | `05c0434`, `93b2e38`, `d9abb7e`, `55f291e`, `2f0ffea` |

Fixes after the first round of tasks:

| Item | What | Commits |
|---|---|---|
| T10 | a first store-id on a member the cache holds no row of resets nothing; T8 found that the first copy into a new destination made every cache that had scanned it rebuild | `eee01aa` |
| fix wave | 18 deferred review findings, from these task reviews: T1 the Transfer index, T2 the scrub and audit, T3 the lineage reader, T4 the closure, T6 the copy service, T7 the presence check, T8 the endpoint, T10. Main items: the D7 gaps (a via manifest with no indexed run; lineage-added item records; input_set), list secrets, a busy cache never deleted, explore copies signed by the server, two dry-run slots, the shutdown re-check | `8caf21d`, `353e1b1`, `42d3bbb`, `19907ca`, `3f3f464` |
| T11 | the audit covers every RunManifest in the copy, including one reached only through a via collection | `a7a367c`, `d9abb7e` |
| T12 | from the P3 lead's acceptance run. The final review made the OS-user rule stricter, and it then flagged nf-core/rnaseq's own `contributors` entry `github:'@robsyme'`, so the wigshop copy exited 1. The fix: a user name directly after `@` is a handle, not a leak, in the classifier the scrub and the audit share. `RoblabAuditTest` now asserts 0 findings; run as `robsyme` it audits 4 RunManifests with 0 findings | `aa85891`, `604e374` |
| final review | fixes from the P3 lead's whole-branch review:<br>- D9 kept-at-destination (Critical)<br>- a copy registers as a Live Writer<br>- scrub and audit shapes after `,` `;` `@`, glued short flags, `~user`, and secret lists in config text<br>- the Index create/open race<br>- D10<br>- Transfer runs that were actually carried<br>- via manifests always travel<br>- hex-only `nf_run_hash`<br>- history carried only with its head<br>- no snapshot without the transfer tables<br>- audit test gaps | `84638ae`..`2f0ffea` |

Every task had a review against its brief and the interface. The P3 lead's whole-branch review found one Critical (D9) and four Important items. One wave fixed them with a scoped re-review, and a last small round closed two of that re-review's Minors. Fix rounds were T2 (1), T5 (1, the D2 narrowing) and T6 (1), each closed by a scoped re-review. The fix wave had one scoped re-review. The P3 lead runs the whole-branch review.

## Measurements on roblab

All on scratch copies of `/tmp/cas-exploration/store`. The real store was only read.

Copying the wigshop Selection `bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce`, claims names, projects and annotations. It has one run, distraught_murdock. The closure was computed in 163 ms after catch-up (`RoblabCopyClosureTest`, opt-in).

| group | results | lineage | all |
|---|---|---|---|
| root | 1 / 375 | 1 / 375 | 1 / 375 |
| items | 3 / 3,645 | 3 / 3,645 | 3 / 3,645 |
| files | 9 / 1,135,852 | 9 / 1,135,852 | 9 / 1,135,852 |
| collections | 1 / 1,041 | 1 / 1,041 | 1 / 1,041 |
| run_records | 83 / 207,975 | 83 / 207,975 | 83 / 207,975 |
| lineage | 0 | 76 / 19,493,095 | 76 / 19,493,095 |
| run_outputs | 0 | 0 | 869 / 57,839,271 |
| claims | 1 / 177 | 1 / 177 | 1 / 177 |
| total | 98 / 1,349,065 | 174 / 20,842,160 | 1,043 / 78,681,431 |

Cells are blocks / bytes.
- The `files` figure matches the interface's own 1.3 example.
- The run's whole metadata (D7) adds about 80 records and about 156 KB.
- The task-level lineage rule adds 18 items. The join-key fallback on the same run would add 23 items (303 blocks, 23.1 MB).
- unsized 0, missing_at_source 0. unaddressed 0, 3 and 5 at results, lineage and all.
- The P3 lead's `gate/p3/local-accept.sh` at `daac0b0` copied 98 blocks, 1,349,065 bytes, matching the results column. Its second dry run found all 98 present.

The audit over roblab's RunManifests (`RoblabAuditTest`, opt-in, over a copy of `blocks/`): 4 RunManifests, 0 findings, both before and after the scrub changes.

Lineage records in roblab's `nf/`: 1,734 FileOutput, 235 TaskRun, 235 TaskOutput, 4 WorkflowRun, 3 WorkflowOutput. TaskRun inputs always name work-dir files, never `cas://` ones (1,905 of 1,905 resolved). Of the 784 distinct published sources, 366 have a published upstream. Counted per (run, source) pair, it is 366 of 1,173.

## Format and behaviour changes (flag to the orchestrator)

- New block kind `Transfer` (DESIGN.md §6 union and struct). Its `from` and `to` are member aliases, a stated exception to "nothing store-local": the block is the provenance of one copy.
- New Store Log kind `transfer`, `<rts>-transfer-<cid>`, written last by a copy. A build older than P3 ignores the entry (`StoreLog.parse` warns once). An older build's sweep therefore does not root the Transfer block, so it would trash it and leave the entry dangling.
- Index: new tables `transfer(cid PK, from_member, to_member, asserted_by, depth, claims, blocks, copied, bytes, at)` and `transfer_root(transfer_cid, root_cid, role)` with index `transfer_root_root`. No schema version bump: a v4 cache gains the tables on open, and a snapshot carries its member's Transfers. web/test/fixtures/schema.sql gained the same three statements (`c3129f3`, with the P3 lead's OK).
- Config: `cas.stores.<alias>.readOnly` (Boolean, default false; refused on the writable member; a non-Boolean is refused).
- RunManifest scrub: it now also redacts:
  - credential-shaped values (AWS key ids, GitHub tokens, Seqera tokens, PEM private keys) wherever they sit;
  - a params or Map-config value under a secret-named key;
  - absolute paths and non-portable URIs inside params strings, token by token as config text already was.

  **RunManifests that new runs write get a new address wherever such a value appears.** Existing blocks are unchanged.
- Sweep (D2): see Decisions. It keeps at least as much as before P3 in every case.
- Snapshot (D8): a destination's storage figures count only the files it holds after a partial copy.
- Explore server: `POST /api/copy`, and `members.json` gains `kind`, `location`, `copy_target`, `copy_reason` per member and a top-level `copy`.

## Decisions

Every ruling made during the phase, with what it costs if wrong. The P3 lead approved D2's change, D7, D8, the lineage rule, `run_outputs`, the write-free dry run and the no-op copy. The rest are mine.

Decisions the P3 lead approved:
- D2, narrowed (1.8). A sweep of member A releases or deletes a run or Selection that A's own Store Log lists only when both A's own Claims and the whole composition's Claims say so. A run or Selection logged only in other members is judged by the composition's Claims, as before P3. Anything pinned in any member, or in the composition, is kept. The "counts only where logged" first draft would have dropped other members' pins and a `del retain` in B superseding A's release, so it was not strictly safer than the old rule. Two mismatches remain, both safe: A's "freed on release" can include a block that a pin in another member keeps, and one that a `del retain` logged elsewhere keeps. Cost if wrong: a run logged only in B and released only in B lets A's sweep reclaim A's copies of its content (pre-P3 behaviour).
- D7. Every run in a copy travels with its whole metadata: RunCompletion, RunManifest, script, every OutputCollection and OutputItem, and a non-null `input_set`. A destination holding only the copy can then be swept without a missing-metadata refusal (tested: `sweep` over the destination alone reports none). Cost if wrong: about 80 extra small records per run (roblab).
- D8. The Transfer names its `runs`, and `transfer_root.role` is `root` or `run`. A destination's snapshot presence-checks content under them through P2's existing exposed-roots lookup. Cost if wrong: one more field in a new block kind.
- Lineage depth. Task-level lineage from `nf/` records, per run, with a join-key fallback (`meta.id`, the same run only) when the source member has no records for the run. `nf/` records never travel: they hold host paths and are not blocks. So lineage from a store that received a copy always uses the fallback. `runs[].lineage` says which rule each run used.
- Plans write nothing. A dry run, and the plan of a real copy, leave the destination out of the index catch-up and ask it only `has`. The page dry-runs on load.
- A copy that changes nothing writes no Transfer (`copied: 0`, `transfer_entry: null`, `snapshot: "skipped"`). Carrying a new Claim is a change.
- Explore copies sign with the server's own principal, the identity `members.json` shows. A copy never resolves the identity again. The CLI resolves as every verb does.
- D9, kept and not just present (from the final whole-branch review). The plan reports `destination_state` for each root and run, judged by the destination's own Claims plus the Claims the copy will carry:
  - The states are absent, held, pinned, released and deleted.
  - "Absent" means no log entry and no Claim of its own.
  - A `run:` root is listed once, with role "root".
  - `kept` is false when any root or run is released or deleted there. A copy that would not be kept is refused: CLI exit 1, or endpoint 409 with `refused: "not_kept"`. The audit's 409 carries `refused: "audit"`, and `not_kept` wins when both apply.
  - Copy never writes a restore Claim.
  - Reading the destination's state stays write-free.
- D10, dry runs supersede (final review; replaces "never 409"). The explore server runs two dry runs and holds one more waiting. A newer dry run replaces the waiting one, which answers 409 `superseded`. A dry run still waiting at shutdown answers 503 `stopping`. Real copies run one at a time; a second gets 409 `copy_running`.
- A real copy registers as a Live Writer at the destination before it writes, so no sweep runs beside it. After registering it asks the destination `has` again for the blocks it counted present.
- `Index.open` answers "busy, try again" instead of deleting a cache that stays busy. A new cache is built aside and linked into place, and opens of one path are serialised in the JVM.
- P2's cache-wide `block_present` drop on any Claim is kept. D8 extends it to all copied content, so the first snapshot build of a destination after any Claim re-asks every copied file (one HEAD each on S3). The P3 lead measures it in the acceptance run.

My rulings, each with what it costs if wrong:
- The scrub redacts, and the audit flags, a params or Map-config value under a secret-named key, each element of a list under one, and paths inside params strings (token by token). 1.6 requires that no unredacted path or credential reaches a RunManifest. Cost: new runs' RunManifests change address where such values occur.
- One classifier (`Leaks`) serves the scrub and the audit. Cost: none.
- A first store-id on a member the cache holds no row of resets nothing (T10, item 18). Before this, the first copy into a new destination made every cache that had scanned it rebuild, the explore server's own included. Cost if wrong: a cache keeps rows of a replaced member it could not attribute; the rule requires that it attributes none.
- Nested Selections count in the `root` group. Cost: a count moves between groups.
- The join-key fallback stays within one run. Cost: lineage misses same-sample items of another run in the closure.
- The block scan indexes a Transfer that has no log entry, as it does Claims (not snapshotted, not rooted). Cost: a cache may show a Transfer the store never announced, until a sweep forgets it.
- When the transfer tables cannot be added to a cache because another process holds it busy, the session runs without them and keeps the cache. Any other failure still recreates it. Cost: see the open questions.
- `snapshot` is exactly `"written"`, `"skipped"` or `"failed: <message>"`, with the reason on stderr. The page compares the value.
- Process: implementers worked in isolated worktrees, and I cherry-picked their commits onto the shared branch, which only moved forward. The harness created those worktrees from `main`, which I caught after the first four tasks. Each was rebased before review. Cost: none in the result.

## Tests

Final, at `604e374`, in a scratch worktree of the branch:

```
$ make test
BUILD SUCCESSFUL in 3m 6s
12 actionable tasks: 8 executed, 4 up-to-date
(build/test-results: tests 1867, failures 0, errors 0, skipped 3)

$ cd web && npm test
tests 876, pass 876, fail 0, skipped 0
```

The 3 skipped tests are the opt-in roblab tests, run separately on scratch copies:
- `./gradlew test --tests robsyme.cas.core.RoblabAuditTest -Droblab.store=/tmp/cas-exploration/store` (copies `blocks/` to a temp directory) printed "roblab RunManifests audited: 4, findings: 0".
- `RoblabCopyClosureTest` (copies `blocks/`, `log/`, `nf/` and `store-id`) gave the measurements above.
- `RoblabStorageTest` is P2's and was not changed.

New test classes:
- `TransferTest`
- `IndexTransferTest`
- `IndexCopyQueriesTest`
- `CopyClosureTest`
- `TaskLineageTest`
- `RunManifestAuditTest`
- `CrossMemberReleaseTest`
- `IndexSnapshotTransferPresenceTest`
- `CopyTargetsTest`
- `CopyServiceTest`
- `CopyCommandTest`
- `ExploreCopyTest`
- `RoblabAuditTest`
- `RoblabCopyClosureTest`

Extended: `MarkTest`, `ScrubTest`, `IndexStoreIdTest`, `ExplorerQueriesTest`, `CasConfigTest`, `CasConfigScopeTest`, `ExploreServerTest`, `ExploreCommandTest`, `MemoryS3OpsTest`.

Index, closure, Mark and store-id work was test-first; each task report holds its red-first output. The P3 lead's `gate/p3/local-accept.sh` passed at `daac0b0`. The MinIO acceptance run is the P3 lead's.

The final-review wave added `IndexOpenRaceTest`. It also added D9, D10 and Live Writer cases to `CopyServiceTest`, `CopyCommandTest` and `ExploreCopyTest`, and the probe's path shapes to `RunManifestAuditTest` and `ScrubTest`. Every item was red before its fix, except item 12, which has no test (see Open questions). `audit.checked` counts the manifests actually read and decoded.

## Open questions

Checked against the code at `10b1c1d`. Items fixed during the phase are listed under "Fixed since first listed" below.

- Presence cost after a Claim (D8 with P2's memo drop). See Decisions; the P3 lead measures it on S3 in the acceptance run. A fix would scope the drop to subjects the Claim can affect.
- Recorded absences are re-asked at every build. That is P2's rule, so files a partial copy left behind are re-asked once each per destination snapshot build (top-level entries only).
- The lost-update race between a cache reset and a concurrent catch-up (two processes, one cache file) predates P3. T10 removes the one reset copy caused, but not the race.
- A snapshot written from a cache opened without the transfer tables (a pre-P3 cache that another process kept busy) has no transfer rows, so that build skips D8's presence check. The copy skips its snapshot in that case (`CopyService.groovy:952`). The other writers do not: a run's `onFlowComplete`, `nf-blocks:snapshot`, `explore` and `sweep`. No test covers it, since it needs a cache held busy for about 10 s.
- `Index.open` gaps across processes:
  - `rebuild` still replaces the file under other connections (`Index.groovy:1891`).
  - On a file system without hard links, the rename fallback is check-then-act (`Index.groovy:366`).
  - A non-busy failure while adding the transfer tables still recreates the cache without the stale re-check (`Index.groovy:276`), which predates P3.
- A RunManifest `script` that is a DirectoryManifest is placed but not expanded. The schema says the script is raw, so this is unreachable today.
- Older builds and `transfer` entries. An older build ignores them, so its sweep would trash Transfer blocks and leave their entries dangling. Mixed-version sweeps of a destination need P3 or later.
- Explore shutdown (final review item 11). `stop()` can wait up to two copy graces (3,600 s) while its notice says 1,800 s: `server.stop(copyGraceSeconds)` is followed by `awaitTermination(copyGraceSeconds)`, at `ExploreServer.groovy:219` and `:226`.
- The explore server never closes its per-request copy sessions or their S3 clients (final review item 10).
- The audit's "os user" check (final review item 9) uses the auditing user's `user.name`, not the name of the user who wrote the block. A short or common OS user name ("root", "runner") over-redacts and over-flags, for example `root.txt` and `root@example.org`.
- The `@` handle rule (T12) also keeps `@<user>/...` (the GitHub `@org/team` form) and `host@<user>.lab`. A user name of `user` matches inside `[redacted-user]`, so the scrub is not idempotent for that one name (this predates P3's changes).
- Shapes the scrub and audit still miss: Windows paths, `Bearer` tokens, Slack `xoxb-` and GitLab `glpat-` tokens, and an `env.API_AUTH` value. Two shapes over-redact: `SHORT_FLAG_PATH` turns `-1/3` into a redacted path, and a quoted map key inside a secret list in config text becomes `[secret]`.
- After the copy registers as a Live Writer and re-asks `has`, it rebuilds the body but does not recompute `destination_state` or `kept`. That only matters if a sweep removes a Claim block the plan counted present in that window.
- D9 reads each destination Claim the cache does not know with one GET per plan. A Claim block at the destination that will not decode makes every plan into that store fail with 500. `CopyService.manifestAt` does not catch decode errors.
- `nf/` traversal on S3 reads every FileOutput record of a run in sequence (1,734 for roblab's run) on every plan, `--depth results` included, because one walk serves all three depths.
- A dry run's cold cache. The first dry run for each new (to, from) composition catches up a fresh cache file: 454 ms for about 6,000 local blocks, more on S3 without a snapshot.
- Local-to-local copy speed was about 4.7 ms per block (3,005 blocks in 14 s, one fsync per block). It was not tuned.

Fixed since first listed:
- SQLITE_BUSY at open, fixed in `fcc6553` (final review item 4). A busy cache now throws "busy, try again" and deletes nothing. A stale file is checked again under `BEGIN IMMEDIATE` before it is deleted.
- Config-text lists under a secret-named key, fixed in `b15108c` (final review item 3). They are redacted and flagged: `ScrubTest:369`, `RunManifestAuditTest:67`.
- The copy's own snapshot from a cache without the transfer tables, fixed in `9ac5697` (final review item 12). It is skipped. The other writers are still listed above.

## Follow-up round (after the merge at 461533a)

Brief: the P3 lead's p3-plugin-followup.md and the orchestrator's p3-followup.md. Plan: `docs/plans/2026-10-24-explorer-v0.3-p3-plugin.md`, "Follow-up round". There were four parallel tasks in isolated worktrees, each reviewed, re-reviewed after every fix round, and then landed by cherry-pick. A last small task (F6) took the rulings made at F2's fix-round cap and F3's deferred items.

| Item | What | Commits |
|---|---|---|
| F1 lost Transfers | A pre-P3 build skips `transfer` entries but moves its watermark past them, and its snapshots have no transfer tables. A cache that first gains a registered table set, or is seeded from a snapshot without it, now marks each member. Its next catch-up lists the whole Store Log once and ingests those kinds through the ordinary path. The integrator's REV6 case now gives org 11 B / 2 blocks with the Transfer rows, where it gave 26 B / 4. Built as a general hook for P4: `Index.AddedTables(name, ddl, kinds, reingest = false)` in `ADDED_TABLES`. | `0292eba`, `5b2af5c`, `9863f46` |
| F2 scrub and audit (hard bar) | Every probe row is redacted by the scrub and reported by the audit, through the real RunManifest path: secret-named `--flags` (`=` or the next token, quoted values, `auth` unless negated), `-D<secret>=`, JSON-ish secret keys, AWS access and secret keys, Bearer, Basic (decoded as RFC 7617 user-pass), JWT, Slack, GitLab, Hugging Face, Azure SAS `sig=`, Windows, UNC and `~` paths in values and map keys. False-positive tests cover sample names, relative paths, `-1/3`, CIDs, Docker digests, git SHAs, `--sort-keys x`, `Basic Statistics` and others. Two pre-existing failures are gone: a 5 KB quoted config value overflowed the stack, and a 50 KB comma list took 20-50 s. Roblab audit: 4 RunManifests, 0 findings. | `91aa117`..`ce55e1f`; F6 `0164013`..`bdc76b0` (the scrub parts) |
| F3 nf/ lineage records | CasLinStore scrubs WorkflowRun, TaskRun, TaskOutput and WorkflowOutput records before Nextflow's DefaultLinStore writes them. Scrubbed: host dirs, commandLine, config files, userName, params, config values, task inputs, binEntries, conda, spack, globalVars, and in scripts the run's dirs plus every value redacted elsewhere. A failed scrub saves the record unscrubbed with a warning. `nextflow lineage list/view/find/diff/check`, a `lid://` read and `channel.fromLineage` still work on 26.04.6: run `gate/nf-scrub/check.sh <SCRATCH_ROOT>` directly, which reports 28 ok, PASS. | `3d8693c`, `a0bdbbd`, `92b553e`, `b876cdd`; F6 (the nf/ parts); F7 |
| F6 cap rulings and deferred items | A comma or `;` followed by punctuation no longer hides a secret assignment. A non-secret `-D` value or a value opening with `(` or `{` is looked inside. A call's named argument is judged. A bare secret-named `name=value` is a secret in params strings and quoted config values (`Leaks.BARE_SECRET`). A map key that is a credential, the user name or a path is renamed and reported (`Leaks.keyNames`, `keyMarker`). nf/ map keys and top-level parameter names are named the same way. nf/ scripts keep ordinary code: name-carried secrets need 8 characters and numbers are never looked for. | `0164013`..`bdc76b0` |
| F7 round review | The one round-level re-review found that a params secret stayed verbatim in `TaskRun.globalVars`. Fixed (secret rule there, and exact-match replacement of redacted param values). Also: a StackOverflowError in a scrub saves the record unscrubbed. Copy errors now catch paths after `word:`, `C:/…` drive paths and credentials, and normalise store roots. The gate check now passes a secret param and fails on the pre-F7 scrub. | `05c1320` and its four predecessors, `558250c` |
| F4 copy errors, no-op copy | Every copy error reads `<Class>: <message>`, with each member root replaced by its alias and any other absolute local path by `[local path]`. A home, tmp or cwd path is redacted through to the next quote, comma, semicolon, parenthesis, `: `, URL or line end. This covers `failures[].error`, the snapshot "failed: …" and `/api/copy` 500 bodies. A copy with nothing to transfer and no entry to add registers no Live Writer, so there is no PUT or DELETE on S3. | `e6a1a97`, `b19ec4d`, `62287fe`, `c7c30b3` |

Stated plainly, as the P3 lead asked: `nf/` records in a shared lineage store still carry host paths in `FileOutput.path`, because Nextflow's `lid://` reads and `nextflow lineage check` open and hash that path. With it redacted, both fail on 26.04.6 with "Target path [redacted-location] does not exist". Records written before this round are not rewritten.

Follow-up rulings, each with its cost if wrong:
- F1 is a registry hook, not a Transfer-only case, at the orchestrator's request. Its `reingest` flag re-runs a kind already indexed, for tables P4 derives from Claims or runs. An unchanged Claim keeps the presence memos, and a re-ingested run keeps its member and its absences. Cost: one full re-ingest of that kind per member on upgrade.
- F2 rulings (all approved by the P3 lead, plus my own):
  - `auth` counts as a secret last segment of a flag unless negated (skip, no, disable, disabled, without). Cost: `--x-auth <positional>` loses its argument.
  - A secret flag takes `Bearer`/`Basic <token>` whole.
  - Basic is judged by decoding, with strict UTF-8 and a non-empty user. Cost: a Latin-1 user-pass is not caught.
  - R-style formulas such as `~genotype/treatment` and dot-free 40-character mixed-case relative paths are over-redacted (as a home path and as an AWS secret). Cost: those params lose their value in the RunManifest.
- F2 reached its five-round cap. At the cap I ruled two small regex changes in (F6): a comma followed by punctuation no longer hides a secret, and a `-D` property inside `'''` takes its whole value.
- F3 keeps `FileOutput.path`, scrubs scripts literally (no token scan), and saves unscrubbed rather than losing a record. The Makefile target was dropped at the P3 lead's ruling.
- F4: redact home, tmp and cwd paths through to a terminator, accepting the loss of trailing prose. The `: ` stop keeps "<path>: Not a directory". Cost: a directory name under home containing `,` `(` `)` or `: ` shows its tail after that character, and a file name containing `: ` would too.

Follow-up tests: Final, at `558250c`, in a scratch worktree of the branch:

```
$ make test
BUILD SUCCESSFUL in 3m 14s
12 actionable tasks: 8 executed, 4 up-to-date
(build/test-results: tests 2000, failures 0, errors 0, skipped 3)

$ cd web && npm test
src/generated/schema.json: 25 types
tests 883, pass 883, fail 0, skipped 0
```

The roblab audit, run opt-in over a scratch copy of `blocks/`, reports "roblab RunManifests audited: 4, findings: 0". Re-scrubbing roblab's manifests with the final rules changes nothing.

The nf/ gate check, `gate/nf-scrub/check.sh <SCRATCH_ROOT>`, ran on Nextflow 26.04.6 with scratch NXF_HOME and NXF_PLUGINS_DIR: 28 ok, PASS at `bdc76b0` (round reviewer) and PASS at F7 (implementer). It fails against the pre-F7 scrub with "host string in TaskRun spec.globalVars.params.db_password".

New test classes this round:
- `TransferRescanTest`
- `IndexAddedTablesTest`
- `CopyErrorsTest`
- `NfRecordScrubTest`

Every Index, scrub, audit and copy-error change was test-first. Each task's red output is in its report under `.superpowers/sdd/2026-10-24-explorer-v0.3-p3-plugin/` (git-ignored scratch).

Reviews:
- every task had a review, with scoped re-reviews after each fix round;
- F2 used all five fix rounds; the rulings at the cap went into F6;
- one round-level re-review produced F7, and F7 had one scoped re-review.

F2 was also checked against the probe harnesses and a 60k-case differential fuzz. No change lost a finding kind.

Follow-up open questions:
- A pre-P3 build still sharing a cache after P3 added the tables can skip `transfer` entries again, with no re-mark. Dev caches migrated by a P3 build before F1 are never rescanned. `reindex` fixes both.
- In config text, the ASSIGNMENT key part restarts after every `-`, so 50 KB of `a-a-a-…` scrubs in 12-26 s. This predates P3. Config scrub is not idempotent on about 1% of adversarial strings; this also predates P3.
- Credential shapes still not caught: a quoted token after a secret flag's `Bearer` when the token has spaces, an AWS session token, and credentials used as map keys. Windows paths are now caught.
- nf/: work-dir FileOutput paths stay, as above. Script paths that are neither the run's dirs nor redacted elsewhere stay. `lineage diff` hides path-only differences, `find` on scrubbed fields stops matching, and `render` merges redacted inputs.
- The 1000 ms timing checks in RunManifestAuditTest have measured at most 349 ms under load. There is no CI here.
- Three residual Minors from F7's re-review, parked with rulings (no second fix wave):
  - NfRecordScrub's exact-match step turns a `lid://` or `cas://` address held by a secret-named param into `[secret]` where a task input or output repeats it. DESIGN §10 states the limit.
  - CopyErrors spares a backslash drive path after a one-letter store alias (`c:\Users\…`).
  - A copy error keeps URL userinfo and `X-Amz-Signature` (the RunManifest scrub redacts the whole URL).
- Slow pathological config lines, all pre-existing: 50 KB of chained `-Da=` takes 3-5 s, and 50 KB of `a-a-a-…` takes 12-26 s per config scrub. `x = foo(token: …)`-style closure arguments are now caught.
- A password of 7 characters or fewer passed as `--password x` stays in nf/ task scripts. The RunManifest still redacts it.
- Map keys in config text and params are now judged, but a key that is merely secret-named (`api_key: x`) still keeps its name. Only its value is redacted.
