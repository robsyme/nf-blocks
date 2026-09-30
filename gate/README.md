# The Gate

The Gate is nf-blocks' acceptance test. Real Nextflow, the real Test Pipeline,
a real `cas://` store on disk, and assertions written in Python that hash bytes
themselves. Nothing in `assert.py` asks the plugin what it stored.

```
make gate                       # or: gate/gate.sh
GATE_ROOT=/tmp/g gate/gate.sh   # reuse a root while developing
python3 -m unittest discover -s gate     # unit tests for cas.py and assert.py
python3 gate/assert.py gate/fixtures/root --offline   # assertions, no Nextflow
```

Python 3.9+ standard library only: `hashlib`, `sqlite3`, `json`, `base64`,
`struct`. No pip, no venv, no third-party CBOR or CID library — a bug in a
dependency we did not write is a bug we cannot tell apart from a bug in the
plugin.

## Why the assertions are independent

Every prior attempt at this verified the store against its own idea of what it
had stored. This one does not. `cas.py` carries its own CIDv1 encoder and its
own strict DAG-CBOR decoder, and:

- addresses come from `hashlib.sha256` over the bytes the pipeline left in
  `pipeline-a/work`, never from a field the plugin wrote;
- `Store.read` re-hashes every block it reads and refuses one that does not
  match the address it was filed under, and assertion 0 does that over the
  whole store *and* re-encodes every metadata block, so an unreadable,
  mis-addressed or non-canonical block is a FAIL rather than a block quietly
  missing from the set the other assertions run over;
- the published directory in assertion 5 is compared against a walk this file
  performs, entry by entry, mode by mode, size by size, address by address;
- the consumer pipeline hashes whatever Nextflow actually staged and publishes
  the digest, so a wrong, truncated or empty stage is caught by bytes.

The decoder is strict on purpose: definite lengths only, minimal integer
widths, 64-bit floats only, tag 42 only, string map keys, no `"/"` key, no
trailing bytes, and canonical key order (length, then bytewise). A block that
violates DAG-CBOR is a plugin bug, and it surfaces as a decode error naming the
block, not as a silently different value. Order violations raise
`DagCborOrderError` so they can be told apart from corruption.

## What gate.sh does

1. Checks `python3` and that `${NEXTFLOW:-nextflow} -version` reports `26.04.6`.
2. `./gradlew -q assemble installPlugin` into `NXF_PLUGINS_DIR=$GATE_ROOT/plugins`
   (falling back to unzipping `build/distributions/nf-blocks-*.zip` if
   `installPlugin` ignores the variable).
3. `nextflow plugin install nf-amazon@3.9.2` into the same `NXF_PLUGINS_DIR`: nf-blocks'
   `requirePlugins` (Task 1) names a dependency Nextflow never auto-downloads for a
   plugin already unpacked on disk at its pinned version, so a fresh `GATE_ROOT` needs it
   installed directly once.
4. Exports `XDG_CACHE_HOME=$GATE_ROOT/cache`, `GATE_STORE=$GATE_ROOT/store`,
   `GATE_STORE_OUT=$GATE_ROOT/store-out`.
5. Copies the Test Pipeline to `$GATE_ROOT/pipeline-a` and `$GATE_ROOT/pipeline-b`
   and this directory's `consumer/` to `$GATE_ROOT/consumer`.
6. Runs, all with `-c gate/gate.config`, keeping stdout, stderr, the exit code
   and `.nextflow.log` under `$GATE_ROOT/logs/<name>/`:

   | run | where | why |
   |---|---|---|
   | `cold` | pipeline-a | the baseline; snapshot to `blocks-after-cold.txt` |
   | `again` | pipeline-a | same store again, plus `-c gate/node-hash.config` (`cas.nodeHash = true`); snapshot to `blocks-after-again.txt` |
   | `fail` | pipeline-a | `--fail`, MAYBE_FAIL exits 7 for sample B; non-zero exit is expected and does not stop the script |
   | `resumed` | pipeline-a | `-resume cold`, with every published source file at mode 000 |
   | `elsewhere` | pipeline-b | a second launch directory into the same store |
   | `outputs` | gate/outputs | milestone 5: `tuples`/`records` with a JSON and a CSV index, and `LEGACY`'s `publishDir`; its own store, `GATE_STORE=$GATE_ROOT/store-outputs` |
   | `outputs-badindex` | gate/outputs-badindex | milestone 5: a CSV `index { header true }` on a tuple channel, which Nextflow fails to write while the run still exits 0; same `store-outputs` |
   | `consumer` | consumer | reads back through `lid://`, `cas://` and `fromStore` |
   | `consumer-seeded` | consumer | the consumer again with its cache deleted and the metadata blocks of the runs in `store/`'s snapshot at mode 000 (restored to 444 after) |
   | `consumer-scan` | consumer | the consumer again with its cache deleted and `store/`'s snapshot moved aside (put back after) |

7. Snapshots the whole store after `cold` and again after `again`, through
   `assert.py --snapshot`: block cids, run-log entries, `nf/` record keys and
   every `coords/` pointer with its text.
8. Reads the read-back references out of the store (`assert.py --refs`) and
   passes them to the consumer as `--lid` and `--cas`, because neither exists
   before the producer has run.
9. `python3 gate/assert.py "$GATE_ROOT"`, then browser tier A
   (`gate/browser/tier.sh`) and browser tier B (`gate/browser/tier_b.sh`),
   both below. Exits non-zero when any tier fails.

Reusing a `GATE_ROOT` wipes `store/`, `store-out/`, `store-outputs/`, `cache/`, `logs/`,
`browser/`, `browser-b/`, `selection/`, `selection-typed/` and the snapshots first. Every one of them is evidence, and stale
evidence is worse than none. The plugin in `$GATE_ROOT/plugins` is replaced by
the zip just built on every run that builds.

`GATE_SKIP_BUILD=1` reuses whatever is already in `$GATE_ROOT/plugins`.

Two things `gate.config` has to say that are not obvious:

- `plugins { id 'nf-blocks@0.2.0-beta.1' }` — the version must be pinned. An unpinned
  id sends Nextflow to the plugin registry, which has never heard of nf-blocks,
  and the run dies with `Cannot find latest version of nf-blocks plugin` before
  anything is loaded.
- `manifest.name = 'cas-test-pipeline'` — the Pipeline Identity. DESIGN §6 takes
  the first non-null of `cas.pipeline`, `manifest.name` and `projectName`, and
  `nextflow run .` sets `projectName` to the literal string `main.nf`, which
  identifies nothing. The consumer names `cas-test-pipeline` literally in its
  `fromStore` call and `assert.py` checks the plugin recorded that name.

## The browser tier (block explorer)

Tier A, local part, of the block explorer spec (section 1.3): the page in
Playwright's own pinned Chromium against the store this Gate run wrote. It
needs Node; `tier.sh` runs `npm ci` and `npx playwright install chromium` in
`gate/browser` (log in `$GATE_ROOT/browser/setup.log`). `GATE_SKIP_BROWSER=1`
skips it.

After `fail`, gate.sh keeps that run's Index Snapshot as
`snapshot-after-fail.sqlite`. `browser_assert.py prepare` then lays out four
member copies under `$GATE_ROOT/browser/site/stores/`, each holding what a
member publishes (`blocks/`, `log/`, `index/v3.sqlite`, `index.html`, never
`coords/` or `nf/`):

| store | snapshot | for |
|---|---|---|
| `current` | the store's own, after `elsewhere` | A1, A5 |
| `stale` | the one kept after `fail`, two runs behind | A3 |
| `tampered` | as `stale`, with one byte of `elsewhere`'s RunCompletion changed | A4 |
| `year` | only `index/v3.sqlite`: `gen_year.py`'s year of 1,825 runs | A2, A5 |

The year snapshot takes minutes to generate, so it is cached in
`GATE_YEAR_CACHE` (default `$TMPDIR/nf-blocks-gate-year`), keyed by
`gen_year.py`'s source and the schema.

`tier.sh` serves them three ways, all in one script:

| server | what |
|---|---|
| `range` | `gate/browser/serve.py` over `site/`, honouring `Range` |
| `plain` | the same with `--no-range`: every GET is a `200` of the whole file |
| `explore` | `nextflow plugin nf-blocks:explore` in `pipeline-a`, stopped with SIGTERM |

`explore` is started for the locally built plugin through a `plugins.json`
naming the zip (`NXF_PLUGINS_TEST_REPOSITORY`, `NXF_OFFLINE` unset; DESIGN.md
section 15), which asks the plugin registry once for dependencies.

`drive.mjs` plays `scenario.json`, one cold browser context per step,
recording every request from Playwright's network events and reading only
the DOM contract of DESIGN.md section 15 into `observed.json`.
`browser_assert.py check` compares that with `expected.json`, which comes
from the Gate's own hashes and block reads, and `sqlite3` for the year file:

- A1: the page answers all three load-bearing queries, each equal to the
  Gate's own answer, through `range` and through `explore`.
- A2: against the year snapshot, cold, a point query costs at most 8 requests
  and 64 KB, and query 3 at most 50 requests and 512 KB.
- A3: with the snapshot two runs stale, the run list shows both runs, the
  stale notice counts two, and query 3 over one of them returns the right
  items after fetching its closure.
- A4: every block the page fetches has its hash verified in the browser, and
  the tampered block is refused with `hash_mismatch`.
- A5: against `plain`, the page downloads a snapshot under the cap whole and
  refuses one over it with `no_range_over_cap` shown.

A failing line: read `browser/observed.json` for that step and
`browser/drive.log`. Assertions 6 and 7 are the cloud part.

## Browser tier B (Selections)

Tier B, milestones 2 and 3, assertions 8 to 19 of spec section 1.3. It is local:
`gate/browser/tier_b.sh` runs after tier A, reuses its `npm ci` and
Playwright, and needs no network beyond what `explore` itself asks for.
`GATE_SKIP_BROWSER=1` skips it with tier A.

The page writes, so it never touches the store the other tiers read.
`browser_b_assert.py prepare` copies `store/` (without `index/` and
`index.html`) to `browser-b/store`, and `tier_b.sh` serves that copy as the
one writable member `lab` through `nf-blocks:explore` (`asserted_by = 'gate'`,
its own index under `browser-b/cache`). `prepare` picks, from the Gate's own
read of the blocks, item A and B of `cold`'s `aligned`, B again through
`again`'s `aligned` (the same OutputItem in another collection) and C of
`stats`, and hashes `A.bam`, `B.bam` and `C.stats` in `pipeline-a/work`.
It also builds a second, read-only member `shared` in `browser-b/shared`
(only `blocks/` and `log/`, written with the Gate's own encoder): S3 = {A via
`aligned`} named `from-shared` there and held nowhere else, and a Claim
`shared-name` superseding `lab`'s name `lab-name` of S4 = {C via `stats`},
which `prepare` puts in `lab`. `shared` also holds S5 = {B via `aligned`},
named `restored-name` and deleted there and held nowhere else, and a
deletion of S6 = {B via `again`}, which `prepare` puts in `lab`.

`drive.mjs` then plays eleven steps with the launch token `explore` printed:
compose `first` = {A, B}; compose `second` = {`first`, B, C}; rename
`second`; delete it and undo; two pages renaming it from the same view;
compose {A} and save the copy the page offers; rename S4 to `lab-renamed`;
compose S5 and restore the copy the page offers; compose S6; pick A from
query 3 over `cold`'s `aligned` (`sample` is `A`), add every item of `cold`'s
`stats` with Add all, and save them as `picked`; and read the Selection view
of `second` in both snippet modes, untyped and typed. While `explore` is still up, `browser_b_assert.py probe`
replays the page's own rename bytes, sends three POSTs that must be refused,
dry-runs S4's request, and fetches the samplesheet of `second` as CSV and
JSON. `explore` stops. `tier_b.sh` then pipes one launcher into another over
the same copy, `nextflow -q plugin nf-blocks:items aligned sample=A --run
lid://<cold>,cas://<again's RunCompletion> --format selection | nextflow -q
plugin nf-blocks:put /dev/stdin --name from-the-cli` (`-q` keeps
Nextflow's console output, here the `NXF_PLUGINS_TEST_REPOSITORY` banner, off
`items`' stdout, as the documented command does for a user's first-run
"Downloading plugin" line), and
`gate/selection` runs in `$GATE_ROOT/selection` over the copy (member `lab`)
and its own `browser-b/store-out` (member `out`), staging `second` through
the page's untyped snippet and through the CSV's `1` column, and publishing
the sha256 of each staged file. `browser_b_assert.py consumer` copies the
pipeline first, putting the page's `[data-snippet]` text verbatim on the line
of `main.nf` that ends in `// @snippet`, so a broken snippet fails the Gate.
`gate/selection-typed` then runs the page's typed snippet the same way in
`$GATE_ROOT/selection-typed`, into its own `browser-b/store-typed` and with
no `outputDir` line: a typed script (`nextflow.enable.types = true`) whose
process takes each item as `tuple(meta: Sample, kit: Kit, staged: Path)`.
`browser_b_assert.py check` recomputes every address with `gate/dagjson.py`
and the Gate's DAG-CBOR encoder:

- B8: each Selection the page wrote has the Gate's address for the page's
  own request, the endpoint answered it, the page shows it, and the block
  in the store reads back equal to the Gate's; `second` has exactly the
  members {`first`}, B via `again` and C via `stats`.
- B9: the pipeline exits 0 and `fromStore(selection:)` staged exactly
  `A.bam`, `B.bam` and `C.stats`, one HASH task each (counted from each
  task's `.command.run`, since a file staged twice publishes one hash), each
  hashing as its work file does.
- B10: every Claim the page wrote (names, rename, delete, undo) has the
  Gate's address and is in the store, the rename, delete and undo are about
  the Gate's `second`, and the undo supersedes the delete; the page lists the deleted Selection
  only as deleted, and the undo clears it; the replayed rename answers
  `200`, `written: false` and the same address, writes no block and leaves
  one `log/` entry for it.
- B11: of the two racing sessions the first is `written`, the second
  `stale_supersedes` and shown as `[data-error]`; the Gate's own current
  state of `second` (`dagjson.claim_state`) is the one name `third-a`.
- B12: no token `403`, a foreign `Origin` `403`, `text/plain` `415`. The
  three send a Selection no step wrote ({A via `aligned`, C via `stats`}, its
  Gate address in `probes.json`); the store must not hold it afterwards, and the
  block and log counts must not change.
- B13: both exports answer `200`, their `1` cells are exactly
  `cas://<cid>/<name>` for A, B and C (by item CID), and the cells stage
  once each and hash as B9's files do.
- B14: a Selection that only the read-only member `shared` holds (built by the
  Gate, with a name Claim there) is offered as "Save a copy here" with its name
  prefilled; one click writes it into `lab` at the Gate's address, with one
  Store Log entry, and a name Claim superseding `shared`'s, so both members
  together show one current name.
- B15: `shared` holds a Claim superseding `lab`'s name for a Selection in `lab`;
  renaming it from the page still succeeds, the composition reports the two
  names as a conflict, and the endpoint's dry run answers `here` with both.
- B16: `shared` names and deletes a Selection only it holds; composing it in the
  page offers "Restore a copy here" with the name prefilled, and one click
  writes it into `lab` with a name Claim and a `del` superseding `shared`'s
  deletion, so both members together show it live and named. Composing a
  Selection `lab` holds and `shared` deleted names the deletion, and its
  Restore writes one `del` superseding `shared`'s, so S6 is live across both
  members.
- B17: query 3's `[data-pick]` for A carries `cold`'s `aligned` in
  `data-via`; `[data-pick-all]` names the query's collection with count 1 on
  the query page and `stats` with its item count on the collection page; Add
  all fills the tray with every item of `stats`; and the saved Selection has
  the Gate's address, with A via `aligned` and each `stats` item via `stats`.
- B18: the typed snippet calls `nextflow.Channel.fromStore` with `records:
  true` and names `second`; the typed consumer ran it verbatim, exited 0,
  logged no `invalid argument type` warning in its `.nextflow.log`, ran one
  task per file, and each published digest matches pipeline-a's work file.
  B9 likewise requires `gate/selection` to have run the untyped snippet,
  naming `second`.
- B19: both launchers of the `items | put` pipe exit 0; `put` answers the
  Gate's address for {A via `cold`'s `aligned` and via `again`'s `aligned`},
  the block in the store reads back equal to the Gate's, and the Gate's
  `dagjson.claim_state` of it is the one current name `from-the-cli`.

Logs are in `browser-b/`: `explore.log`, `drive.log`, `probe.log`,
`cli-items.err`, `cli-put.out`, `cli-put.err` and `cli.exit` (B19),
`selection.log` (and `selection-nextflow.log`), `selection-typed.log` (and
`selection-typed-nextflow.log`), beside `scenario.json`,
`observed.json`, `probes.json`, `expected.json` and the two samplesheets.
A failing line: read that step in `observed.json` and `drive.log` first.

## The cloud browser tier

`gate/cloud/cloud.sh`, assertions 6 and 7: real S3 (CORS, ranged GETs,
`ListObjectsV2`) from a page on another origin, and a private bucket readable
only through `nf-blocks:explore`. Needs Rob's `scidev` SSO session, so it is
not part of `gate.sh` and is run on demand: before each milestone is accepted
and whenever the HTTP reader or the serving code changes.

```
GATE_ROOT=/tmp/g make gate                              # local tier must pass first
GATE_PYTHON=/path/to/venv/bin/python make gate-cloud GATE_ROOT=/tmp/g
```

It creates two tagged, throwaway buckets (`nf-blocks-gate-pub-*`,
`nf-blocks-gate-priv-*`) in `AWS_PROFILE` (default `scidev`) and
`GATE_CLOUD_REGION` (default `ca-central-1`), uploads `browser/site`'s stores
into them, and deletes both on exit, success or failure. `GATE_PYTHON` picks a
Python with `boto3` installed, since the Gate itself needs none. After a run,
confirm no `nf-blocks-gate-` bucket remains with that Python's `boto3`.

Known limit: a private-bucket member whose policy grants `s3:GetObject` but not
`s3:ListBucket` answers `403` for a missing key, which `explore` surfaces as
`500`. This tier's buckets are created by their own owner, so it does not
arise here; it can with a member on someone else's bucket.

## The GATE_ROOT tree

```
plugins/                 NXF_PLUGINS_DIR for these runs only
cache/nf-blocks/*.sqlite the indexes; the producer's is selected by pipeline
store/                   the cas:// member `lab`: blocks/ log/ coords/ nf/
store-out/               the consumer's member `out`
store-outputs/           milestone 5's `outputs`/`outputs-badindex` runs, seen by nothing else
browser/ browser-b/      browser tiers A and B: inputs, observations, logs
selection/               tier B's selection pipeline launch directory
selection-typed/         tier B's typed consumer launch directory
pipeline-a/ pipeline-b/  two launch directories of the Test Pipeline
outputs/ outputs-badindex/  milestone 5's launch directories
consumer/                the second pipeline
logs/<name>/             stdout.log stderr.log nextflow.log exit
blocks-after-cold.txt    find blocks -type f, after `cold`
blocks-after-again.txt   the same, after `again`; assertion 2 diffs them
seeding.json             lab's snapshot watermark and run counts before assertion 13's runs
```

## The assertions

Numbers are the spec's, `.scratch/content-addressed-lineage/spec.md` §1.2.
`assert.py` prints one line per assertion, `PASS`/`FAIL`/`SKIP`, and exits 1 if
any line is `FAIL`. A `SKIP` never fails the Gate.

| # | what it proves | how it stays independent |
|---|---|---|
| 0 | every block decodes, re-encodes to its own address, and every run exited as the Gate drove it | re-hashes each block and re-runs the canonical encoder over each metadata block; reads `logs/<name>/exit` and requires `fail` non-zero and every other run zero |
| 1 | three byte-identical `.stats` get one Content Address, three Output Items, and three *different* Nextflow fingerprints | hashes the three files itself, then opens the three `item_cid`s the `producer` rows name and requires each to be an `OutputItem` with one Leaf addressing that content under the name `<sample>.stats` |
| 2 | `again` writes no new content block and loses no record; Output Item addresses are identical, and `again`, run with `gate/node-hash.config`, records every file `fusion-node` from `.command.cas` while `cold` recorded `head-node`: the same OutputItem addresses either way (ticket 16) | diffs blocks, run-log entries, `nf/` keys and `coords/` pointer *text* between the two snapshots, and re-hashes every block in the store; reads both RunCompletions' `providers`, requires no Leaf to carry `provider`, and hashes every file each task's `.command.cas` names, comparing against the digest on its line |
| 3 | the failed run is marked failed, is partial and says so, and is not `latest` | requires `status: failed`, `possibly_incomplete: true`, an `anomalies` map, a `reports` collection with no `sample == 'B'` item and at most 2 items, and agreement between the index and the Store Log that `latest` is some other run |
| 4 | the resumed run's output layer is complete | collection names exactly `{aligned, stats, qc, chunks, reports}`, each with 3 items |
| 4 | the resumed run's task layer is populated through our own `onTaskCached` | `SKIP`: filling the task layer on resume is deferred out of the Walking Skeleton; native Nextflow leaves it empty (7 not 15, issue 17) |
| 4 | the resumed run re-hashed nothing | proved by the filesystem: `gate.sh` sets every published source file in `pipeline-a/work` to mode 000 for the duration of the run, so anything that re-reads one to re-address it gets `AccessDenied`. No counter is trusted. See the caveat below |
| 5 | the published directory is a Directory Manifest matching an independent walk, with the internal symlink recorded as a link | walks `pipeline-a/work/**/A_qc` and compares names, modes, sizes and per-file raw CIDs, recursing into `nested/`; a manifest records no execute bit, so the walk calls every file `regular`; the PASS message states how many of each kind were compared |
| 5 | every recorded publish path resolves | for each `paths[i][j]` in every `cold` collection, requires a `coords/` pointer whose Store URI is that leaf's own address and name |
| 6 | `lid://` and `cas://` each stage into a second pipeline and hash to the expected address; the consumer, with no `outputDir` line, publishes into its store | requires exactly one file under each of `hashes/lid/` and `hashes/cas/` in `store-out`, compares its digest against the Gate's own sha256 of `A.bam`, and requires the consumer's lineage `WorkflowRun` (`store-out/nf/*/.data.json`, `name` `consumer`) to have `metadata.outputDir` `cas://out`; it also requires the consumer's `nextflow.log` to carry the plugin's `outputDir not set` line, which is also the proof that the plugin's own logging reaches Nextflow's log at all, and its console (`stdout.log` or `stderr.log`) to show the same line, which the plugin logs through `nextflow.cas` so Nextflow's console filter prints it |
| 7 | `fromStore` with `where: [sample: 'B']` returns exactly one item | requires exactly one file under `hashes/fromstore/` digesting to the Gate's sha256 of `B.bam` |
| 10 | two launch directories give identical Output Item and Directory Manifest addresses, and nothing store-local leaks into a block | compares the two closures; searches every decoded `bafy…` block, whole and without exemption, for the `GATE_ROOT` path and the OS user name, naming the JSON path of any hit |
| 13 | a cold cache seeds from the Index Snapshot: `consumer-seeded` (cache deleted, the producer's run metadata blocks at mode 000) and `consumer-scan` (snapshot removed too) stage the same bytes as the consumer; the first reads no locked block, the second prints the fallback warning; store-out's snapshot run count does not fall | reads each consumer run's own `hashes` collection in `store-out` and the sha256sum text its leaves address; the locked set (`logs/consumer-seeded/locked`) is computed by the Gate from the snapshot's `run` rows and the RunCompletions' own links, and any `could not be read` or `could not be decoded as` line in `nextflow.log` naming one of those paths fails it (a locked RunCompletion read logs the second); the snapshot run counts are `count(*)` over `run`, read-only, before (`seeding.json`) and after |
| 14 | run `outputs`'s `tuples` and `records` collections join with their Meta Maps, and each output's `index {}` file is linked by address | reads `store-outputs` (its own store) directly; every item's Meta Map (via `metadata_view`) must carry `id`; for each collection, hashes the bytes at its `index.path` coordinate itself and requires that hash to equal both the coords pointer's CID and the index leaf's own recorded address; `tuples/index.json` must parse as a 2-row JSON array, `records/index.csv` a header row and 2 data rows |
| 15 | run `outputs-badindex` exits 0 and is marked `succeeded` although its CSV index (`header true` on a tuple channel) was never written | requires the `tuples` collection to hold 2 items and its index leaf to carry `reason: never_published` and no `address`, and `anomalies.never_published >= 1`; never reads a `tuples/index.csv` coordinate, since the leaf's own reason is what proves the write failed |
| 16 | run `outputs`'s `LEGACY` process (`publishDir`, no workflow output) warns once about the process and once about its 2 unjoined files, and `anomalies.unjoined == 2` | counts each warning text's occurrences in `logs/outputs/nextflow.log`; walks every item and index Leaf address of every collection into one set, then requires `legacy/A.legacy` and `legacy/B.legacy` (hashed from their own coords bytes) to be addressed coordinates absent from that set |
| 8, 9, 11, 12 | — | `SKIP (not in skeleton)`, printed with the spec's own wording |

**Assertion 4c is inconclusive under `mode 'copy'`, and says so.** Measured on
26.04.6: with the sources locked, the resumed run exits 1 because Nextflow's own
`PublishDir` re-copies every published file on a resume (issue 17 measured the
inode changing) and a copy needs read access, so the lock is hit upstream of the
plugin. The assertion greps the resumed run's log: an exit caused by
`PublishDir - Failed to publish` reports `SKIP` naming that cause, any other
non-zero exit is a `FAIL`, and an exit of 0 is the `PASS` that proves no content
byte was read. Assertion 0's exit check makes the same distinction. Clearing
this properly needs the Test Pipeline to offer a non-copy publish mode for the
resumed run, which is not the Gate's file.

**Browser assertion A1's producer count is 3 or 4, and that is expected.** A1
compares the page's producers of `A.bam`'s content with the Gate's own set, so
the page must match exactly whichever it is. The set moves because of `fail`:
Nextflow notifies a workflow output only when its channel closes, and `fail`
aborts on `MAYBE_FAIL`'s exit 7 before some channels have, so its RunCompletion
holds `aligned` in some runs and nothing at all in others (six runs on
2026-09-26: four with `fail`, one without, one not captured). `resumed` is
never a producer: its `aligned` collection is complete, but the locked source
stops PublishDir's copy, so its `A.bam` leaf is never-published and has no
address. A1 therefore also requires the Gate's set to include `cold`, `again`
and `elsewhere` and to exclude `resumed`, and its PASS line names the runs it
matched. This is a different race from assertion 4's, and the Test Pipeline is
left as it is: making `MAYBE_FAIL` wait on `ALIGN` would narrow the race
without closing it.

Assertion 10's leak scan has **no exemptions**: every dag-cbor block is searched
whole, `RunManifest.params` and `RunManifest.config` included. DESIGN §6
specifies a portability scrub for those two — it drops the `cas`, `lineage`,
`workDir`, `outputDir`, `launchDir`, `projectDir`, `homeDir`, `configFiles`,
`scriptFile`, `commandLine`, `runName` and `resume` scopes, replaces
absolute-path and non-`lid`/`cas` URI strings with `[redacted-location]`, and
replaces the OS user name with `[redacted-user]` — so a launch path surviving
into a block is a scrub bug, and the assertion reports it as
`RunManifest block bafy… contains the GATE_ROOT path at $.config.env.HOME`.
The fixture's RunManifests carry a scrubbed config, so the fixture exercises the
shape the scrub is meant to produce.

One reading worth knowing about:

- **Assertion 6 covers two of the three URI shapes.** The run-rooted
  `cas://<runCid>/aligned/A/A.bam` and the glob over a manifest are stubbed in
  the skeleton (DESIGN §7), so the assertion says so in its message rather than
  pretending to cover them.

## Developing offline: gate/fixtures

`gate/fixtures/root` is a hand-made GATE_ROOT: five runs across all five
outputs (including the multi-leaf `chunks` items and the partial `reports` of
the failed run), work trees with the real bytes, `qc` directories with a nested
subdirectory and an internal symlink, a Store Log, `nf/` records, coords pointers
for every publish path, a consumer store covering the three read-back sources,
and a populated SQLite index. It was built to what DESIGN said a correct
plugin produced before milestone 4, and it has not been regenerated since
(Task 13 parked it), so it no longer passes every assertion:

```
python3 gate/fixtures/make_fixture.py                  # regenerate (deterministic)
python3 gate/assert.py gate/fixtures/root --offline    # 8 PASS, 3 FAIL, 7 SKIP
python3 gate/assert.py gate/fixtures/root              # 9 PASS, 4 FAIL, 5 SKIP
```

The failures are the fixture's age, not the plugin's. Assertion 2 fails
because its RunCompletions are schema 1 without `providers` and its work
dirs hold no `.command.cas` (ticket 16, milestone 4). Assertion 3 fails
because its RunManifest records `config` as a map, where DESIGN §6 records
the resolved config text. Assertion 13 fails because the fixture has no
`seeding.json` or Index Snapshot (milestone 4). Without `--offline`,
assertion 6 also fails: the fixture's consumer store has no WorkflowRun
named `consumer` under `store-out/nf`. A real `make gate` run is the evidence; this
fixture still exercises the PASS branches of the other assertions offline.
Bringing it up to date means teaching `make_fixture.py` schema 2, the
config text, `.command.cas` lines and a snapshot.

`--offline` skips only the two assertions that need a real consumer run.
The fixture is generated with `cas.py`'s own encoder, which the unit tests pin
to the published CID vectors, so it cannot drift away from the real format
without `test_cas.py` noticing.

## The CID vectors

`test_cas.py` pins the three vectors from DESIGN.md §3 and, in
`test_every_vector_encodes_the_digest_it_claims`, re-derives each one from the
bytes with `hashlib` rather than comparing string to string. That check earned
its place: an earlier revision of DESIGN gave `raw` of `b"hello\n"` as
`bafkreigyhb6gpc…`, which decodes to
`d8387c678ba3d4783d0277d429240129b754ca069bcf1a938002436b81760c30` — a digest
nothing hashes to. The correct value, and the one DESIGN now carries, is
`bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am`.
