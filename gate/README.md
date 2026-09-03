# The Gate

The Gate is nf-blocks' acceptance test. Real Nextflow, the real Test Pipeline,
a real `cas://` store on disk, and assertions written in Python that hash bytes
themselves. Nothing in `assert.py` asks the plugin what it stored.

```
make gate                       # or: gate/gate.sh
GATE_ROOT=/tmp/g gate/gate.sh   # reuse a root while developing
python3 -m unittest discover -s gate     # unit tests for cas.py
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
  match the address it was filed under;
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
   | `resumed` | pipeline-a | `-resume cold` |
   | `elsewhere` | pipeline-b | a second launch directory into the same store |
   | `consumer` | consumer | reads back through `lid://`, `cas://` and `fromStore` |

6. Reads the two read-back URIs out of the store (`assert.py --refs`) and
   passes them to the consumer as `--lid` and `--cas`, because neither exists
   before the producer has run.
7. Runs `nextflow lineage find` in pipeline-a and keeps the output as evidence
   that native lineage still works.
8. `python3 gate/assert.py "$GATE_ROOT"` and exits with its status.

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

## The GATE_ROOT tree

```
plugins/                 NXF_PLUGINS_DIR for these runs only
cache/nf-blocks/*.sqlite the index; exactly one file is expected
store/                   the cas:// member `lab`: blocks/ runs/ coords/ nf/
store-out/               the consumer's member `out`
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
| 1 | three byte-identical `.stats` get one Content Address, three Output Items, and three *different* Nextflow fingerprints | hashes the three files itself; reads `producer` rows and the `nf/` FileOutput records |
| 2 | `again` writes no new content block and loses no record; Output Item addresses are identical | diffs the two `find blocks` snapshots by cid |
| 3 | the failed run has `status: failed`, `possibly_incomplete: true`, partial collections, and is not `latest` | decodes the RunCompletion; resolves `latest` from the index, falling back to the store's run log |
| 4 | the resumed run's collections each hold 3 items | decoded from the OutputCollections |
| 4 | the resumed run's task layer is populated through our own `onTaskCached` | counts `TaskRun` records naming the resumed run; **expects 15, and fails until the plugin implements `onTaskCached` — released Nextflow measured 7** (issue 17). Reported on its own line so the two halves of assertion 4 can be read apart. |
| 5 | the published directory is a Directory Manifest matching an independent walk, with the internal symlink recorded as a link | walks `pipeline-a/work/**/A_qc` and compares names, modes, sizes and per-file raw CIDs, recursing into `nested/`; `alias.txt` must be `mode: symlink`, `target: summary.txt` |
| 6 | `lid://` and `cas://` each stage into a second pipeline and hash to the expected address | compares the consumer's `hashes/` output against its own sha256 of `A.bam` |
| 7 | `fromStore` with `where: [sample: 'B']` returns exactly one item; `fromLineage` still works | compares against its own sha256 of `B.bam`; reads `logs/consumer/lineage-find.txt` |
| 10 | two launch directories give identical Output Item and Directory Manifest addresses, and nothing store-local leaks into a block | compares the two closures; searches every decoded `bafy…` block for the `GATE_ROOT` path and `$USER` |
| 8, 9, 11, 12 | — | `SKIP (not in skeleton)`, printed with the spec's own wording |

Two readings worth knowing about, both chosen for consistency with `DESIGN.md`:

- **Assertion 10's leak scan exempts `RunManifest.params` and
  `RunManifest.config`.** DESIGN §6 forbids store-local strings in blocks but
  the RunManifest schema in the same section carries `session.params` and
  `session.config` verbatim, and the config necessarily holds
  `cas.stores.lab.location`, which is inside `GATE_ROOT`. Every other field of
  every other block — OutputItem, DirectoryManifest, OutputCollection,
  RunCompletion — is scanned in full.
- **Assertion 6 covers two of the three URI shapes.** The run-rooted
  `cas://<runCid>/aligned/A/A.bam` and the glob over a manifest are stubbed in
  the skeleton (DESIGN §7), so the assertion says so in its message rather than
  pretending to cover them.

## Developing offline: gate/fixtures

`gate/fixtures/root` is a hand-made GATE_ROOT: five runs, work trees with the
real bytes, a `qc` directory with a nested subdirectory and an internal
symlink, a run log, `nf/` records, coords pointers, a consumer store and a
populated SQLite index. It is what DESIGN says a correct plugin must produce,
so it exercises every PASS branch of `assert.py` before the plugin can.

```
python3 gate/fixtures/make_fixture.py                  # regenerate (deterministic)
python3 gate/assert.py gate/fixtures/root --offline    # 7 PASS, 0 FAIL, 6 SKIP
python3 gate/assert.py gate/fixtures/root              # 9 PASS, 0 FAIL, 4 SKIP
```

`--offline` skips only the two assertions that need a real consumer run.
The fixture is generated with `cas.py`'s own encoder, which the unit tests pin
to the published CID vectors, so it cannot drift away from the real format
without `test_cas.py` noticing.

## A correction to DESIGN.md section 3

Two of the three CID vectors in DESIGN §3 are correct. The third is not:

- `raw` of `b""` → `bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku`
  decodes to digest `e3b0c442…b855`, which is `sha256(b"")`. Correct.
- `dag-cbor` of `0xa0` → `bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua`
  decodes to `c19a797fa1fd590cd2e5b42d1cf5f246e29b91684e2f87404b81dc345c7a56a0`,
  which is `sha256(b"\xa0")`. Correct — but DESIGN's parenthetical hex for it,
  `c19a7817da1fd2c0…`, is a garbled transcription of that same digest.
- `raw` of `b"hello\n"` → DESIGN gives
  `bafkreigyhb6gpc5d2r4d2atx2qusiajjw5kmubu3z4njhaacinvyc5qmga`, which decodes
  to `d8387c678ba3d4783d0277d429240129b754ca069bcf1a938002436b81760c30`. That is
  not `sha256(b"hello\n")`. DESIGN's own parenthetical for the same vector,
  `5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03`, *is*
  `sha256(b"hello\n")` (`printf 'hello\n' | shasum -a 256`), and it encodes to
  **`bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am`**.

`test_cas.py` asserts the digest-derived value and pins the bad string in
`test_designs_hello_string_encodes_a_different_digest`, so the discrepancy
cannot be quietly lost. Any Java test that hardcodes DESIGN's string will fail
against a correct implementation.
