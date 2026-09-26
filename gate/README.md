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
3. Exports `XDG_CACHE_HOME=$GATE_ROOT/cache`, `GATE_STORE=$GATE_ROOT/store`,
   `GATE_STORE_OUT=$GATE_ROOT/store-out`.
4. Copies the Test Pipeline to `$GATE_ROOT/pipeline-a` and `$GATE_ROOT/pipeline-b`
   and this directory's `consumer/` to `$GATE_ROOT/consumer`.
5. Runs, all with `-c gate/gate.config`, keeping stdout, stderr, the exit code
   and `.nextflow.log` under `$GATE_ROOT/logs/<name>/`:

   | run | where | why |
   |---|---|---|
   | `cold` | pipeline-a | the baseline; snapshot to `blocks-after-cold.txt` |
   | `again` | pipeline-a | same store again; snapshot to `blocks-after-again.txt` |
   | `fail` | pipeline-a | `--fail`, MAYBE_FAIL exits 7 for sample B; non-zero exit is expected and does not stop the script |
   | `resumed` | pipeline-a | `-resume cold`, with every published source file at mode 000 |
   | `elsewhere` | pipeline-b | a second launch directory into the same store |
   | `consumer` | consumer | reads back through `lid://`, `cas://` and `fromStore` |

6. Snapshots the whole store after `cold` and again after `again`, through
   `assert.py --snapshot`: block cids, run-log entries, `nf/` record keys and
   every `coords/` pointer with its text.
7. Reads the read-back references out of the store (`assert.py --refs`) and
   passes them to the consumer as `--lid` and `--cas`, because neither exists
   before the producer has run.
8. `python3 gate/assert.py "$GATE_ROOT"`, then browser tier A
   (`gate/browser/tier.sh`) and browser tier B (`gate/browser/tier_b.sh`),
   both below. Exits non-zero when any tier fails.

Reusing a `GATE_ROOT` wipes `store/`, `store-out/`, `cache/`, `logs/`,
`browser/`, `browser-b/`, `selection/` and the snapshots first. Every one of them is evidence, and stale
evidence is worse than none. The plugin in `$GATE_ROOT/plugins` is replaced by
the zip just built on every run that builds.

`GATE_SKIP_BUILD=1` reuses whatever is already in `$GATE_ROOT/plugins`.

Two things `gate.config` has to say that are not obvious:

- `plugins { id 'nf-blocks@0.1.0' }` — the version must be pinned. An unpinned
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

Tier B, milestone 2, assertions 8 to 13 of spec section 1.3. It is local:
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

`drive.mjs` then plays five steps with the launch token `explore` printed:
compose `first` = {A, B}; compose `second` = {`first`, B, C}; rename
`second`; delete it and undo; and two pages renaming it from the same view.
While `explore` is still up, `browser_b_assert.py probe` replays the page's
own rename bytes, sends three POSTs that must be refused, and fetches the
samplesheet of `second` as CSV and JSON. `explore` stops, and
`gate/selection` runs in `$GATE_ROOT/selection` over the copy (member `lab`)
and its own `browser-b/store-out` (member `out`), staging `second` through
`fromStore(selection:)` and through the CSV's `1` column, and publishing the
sha256 of each staged file. `browser_b_assert.py check` recomputes every
address with `gate/dagjson.py` and the Gate's DAG-CBOR encoder:

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
  three send a Selection no step wrote ({C via `stats`} alone, its Gate
  address in `probes.json`); the store must not hold it afterwards, and the
  block and log counts must not change.
- B13: both exports answer `200`, their `1` cells are exactly
  `cas://<cid>/<name>` for A, B and C (by item CID), and the cells stage
  once each and hash as B9's files do.

Logs are in `browser-b/`: `explore.log`, `drive.log`, `probe.log`,
`selection.log` (and `selection-nextflow.log`), beside `scenario.json`,
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
browser/ browser-b/      browser tiers A and B: inputs, observations, logs
selection/               tier B's selection pipeline launch directory
pipeline-a/ pipeline-b/  two launch directories of the Test Pipeline
consumer/                the second pipeline
logs/<name>/             stdout.log stderr.log nextflow.log exit
blocks-after-cold.txt    find blocks -type f, after `cold`
blocks-after-again.txt   the same, after `again`; assertion 2 diffs them
```

## The assertions

Numbers are the spec's, `.scratch/content-addressed-lineage/spec.md` §1.2.
`assert.py` prints one line per assertion, `PASS`/`FAIL`/`SKIP`, and exits 1 if
any line is `FAIL`. A `SKIP` never fails the Gate.

| # | what it proves | how it stays independent |
|---|---|---|
| 0 | every block decodes, re-encodes to its own address, and every run exited as the Gate drove it | re-hashes each block and re-runs the canonical encoder over each metadata block; reads `logs/<name>/exit` and requires `fail` non-zero and every other run zero |
| 1 | three byte-identical `.stats` get one Content Address, three Output Items, and three *different* Nextflow fingerprints | hashes the three files itself, then opens the three `item_cid`s the `producer` rows name and requires each to be an `OutputItem` with one Leaf addressing that content under the name `<sample>.stats` |
| 2 | `again` writes no new content block and loses no record; Output Item addresses are identical | diffs blocks, run-log entries, `nf/` keys and `coords/` pointer *text* between the two snapshots, and re-hashes every block in the store |
| 3 | the failed run is marked failed, is partial and says so, and is not `latest` | requires `status: failed`, `possibly_incomplete: true`, an `anomalies` map, a `reports` collection with no `sample == 'B'` item and at most 2 items, and agreement between the index and the Store Log that `latest` is some other run |
| 4 | the resumed run's output layer is complete | collection names exactly `{aligned, stats, qc, chunks, reports}`, each with 3 items |
| 4 | the resumed run's task layer is populated through our own `onTaskCached` | `SKIP`: filling the task layer on resume is deferred out of the Walking Skeleton; native Nextflow leaves it empty (7 not 15, issue 17) |
| 4 | the resumed run re-hashed nothing | proved by the filesystem: `gate.sh` sets every published source file in `pipeline-a/work` to mode 000 for the duration of the run, so anything that re-reads one to re-address it gets `AccessDenied`. No counter is trusted. See the caveat below |
| 5 | the published directory is a Directory Manifest matching an independent walk, with the internal symlink recorded as a link | walks `pipeline-a/work/**/A_qc` and compares names, modes, sizes and per-file raw CIDs, recursing into `nested/`; the PASS message states how many of each kind were compared |
| 5 | every recorded publish path resolves | for each `paths[i][j]` in every `cold` collection, requires a `coords/` pointer whose Store URI is that leaf's own address and name |
| 6 | `lid://` and `cas://` each stage into a second pipeline and hash to the expected address | requires exactly one file under each of `hashes/lid/` and `hashes/cas/`, and compares its digest against the Gate's own sha256 of `A.bam` |
| 7 | `fromStore` with `where: [sample: 'B']` returns exactly one item | requires exactly one file under `hashes/fromstore/` digesting to the Gate's sha256 of `B.bam` |
| 10 | two launch directories give identical Output Item and Directory Manifest addresses, and nothing store-local leaks into a block | compares the two closures; searches every decoded `bafy…` block, whole and without exemption, for the `GATE_ROOT` path and the OS user name, naming the JSON path of any hit |
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
and a populated SQLite index. It is what DESIGN says a correct plugin must
produce, so it exercises every PASS branch of `assert.py` before the plugin
can.

```
python3 gate/fixtures/make_fixture.py                  # regenerate (deterministic)
python3 gate/assert.py gate/fixtures/root --offline    # 10 PASS, 0 FAIL, 7 SKIP
python3 gate/assert.py gate/fixtures/root              # 12 PASS, 0 FAIL, 5 SKIP
```

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
