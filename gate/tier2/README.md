# Gate tier two

`gate/tier2/tier2.sh` (`make gate-tier2`, ticket 11) runs the Test Pipeline and
the consumer on the scidev AWS Batch queue against a writable S3 member, then
checks the results with boto3 and hashlib alone. It is run on demand, by a
person: before milestone 4 is accepted, and whenever the S3 block store, the
Fusion provider or the cloud publish path changes. Unit tests never reach AWS;
this is the only part of the Gate that does.

```
GATE_ROOT=/tmp/g make gate                                    # tier one must pass on this root first
GATE_PYTHON=/path/to/venv/bin/python AWS_PROFILE=scidev make gate-tier2 GATE_ROOT=/tmp/g
```

## Prerequisites

A GATE_ROOT that passed tier one. Tier two reuses its built plugin
(`$GATE_ROOT/plugins`), its `plugins.json` recipe (`gate/browser/plugin-repo.sh`,
so `build/distributions` must still hold the zip) and its local `store/`, whose
`qc/A/A_qc` manifest T3 compares with the S3 one.

`GATE_PYTHON`, a Python with a boto3 recent enough to pass `IfMatch` to
`put_object` (boto3 1.35.x from late 2024 or later). The Gate itself needs no
third-party Python; only this tier and `make gate-cloud` do.

An SSO login for `AWS_PROFILE` (default `scidev`): `aws sso login --profile scidev`
from a working AWS CLI. The head node is the laptop; no Batch head job runs.

The Seqera Platform token in `~/.nextflow/config` (`tower.accessToken`), which
Wave needs to build the Fusion containers for t2 and t2b. The registry must be
reachable, since nf-wave is downloaded into `$GATE_ROOT/plugins` on first use.

## What it creates and destroys

One run id, `t2-<UTC date>-<time>-<random>`, names everything:

    s3://nf-blocks-t2-<run id>/                     the members: cas, cas-t2, cas-out, cas-t6 (us-east-1)
    s3://scidev-playground-us-east-1/robsyme/nf-blocks-gate/<run id>/work/
                                                    the Batch work dir (the instance role allows only that bucket)
    $GATE_ROOT/tier2/<run id>/                      logs, traces, evidence; kept

The exit trap stops any nextflow still running (TERM, which cancels its Batch
jobs, then up to a minute's wait), empties and deletes the bucket, empties the
work prefix and aborts open multipart uploads, on every exit: success, a
failed check, an error, Ctrl-C, or the 45-minute watchdog (ticket 11 decision
6), which stops the recorded nextflow processes before it stops the harness. A
teardown that fails says so and exits 1; delete the two names it prints by
hand. After a run, confirm with `GATE_PYTHON`'s boto3 that neither the bucket
nor the work prefix remains.

## The runs

| Run | Pipeline | Fusion | Writable member |
|---|---|---|---|
| t1 | Test Pipeline | off | `cas` (`lab`) |
| t2 | Test Pipeline | on | `cas-t2` (`lab`, fresh) |
| t2b | `gate/tier2/small` | on | `cas` (T1's) |
| t4 | consumer, on Batch | off | `cas-out` (`out`), reading `lab` = `cas` |
| t5 | consumer again, its cache deleted | off | as t4 |
| t6a, t6b | Test Pipeline from two launch dirs, started together | off | `cas-t6` (fresh) |

After t6 the harness runs `nf-blocks:items` and `nf-blocks:snapshot` against
`cas-t6`, then up to three attempts at two snapshot verbs started together, and
a boto3 probe of S3's own If-Match refusal.

`gate/tier2/small` is ALIGN and QC_DIR of the Test Pipeline, verbatim, for
sample A, under the Pipeline Identity `cas-tier2-small`. It must stay in step
with the Test Pipeline; if the two drift apart T2b fails.

## The checks

`assert_tier2.py check` prints one line per check and exits 1 if any is FAIL.

T1 (spec assertion 12). t1 exited 0. Every raw `coords/` pointer in `cas` names
the CID of an object of that file name in the work bucket, hashed by the Gate,
and the member's block hashes to it. Every workflow-output `FileOutput` record
under `cas/nf/` names a `cas://` path. t1's RunCompletion lists every file leaf
under `s3-copy`. No `.command.run` of t1's tasks contains `cas:`.

T2 (assertion 11). t2 exited 0; every file leaf of its RunCompletion is under
`s3-copy` in the fresh member. Every t2 task directory has a `.command.cas`
whose every digest equals the Gate's hash of the object it names (copied to
`evidence/command-cas/`), and every published file, including those inside
published directories, carries its `.command.cas` digest. t2's `aligned` item
addresses equal t1's; its `qc` item addresses equal tier one's local `cold`
ones and not t1's, because t1 flattened `alias.txt` and t2 keeps it a link
(ticket 15 decision 5).

T2b. t2b exited 0; every file leaf is under `fusion-node`, since T1 already
placed each block; its `aligned` items are among t1's, its `qc` items among
t2's and tier one's `cold` ones.

T3 (assertion 5 in the cloud). Sample A's `qc` manifest of t1 (read through
t1's RunCompletion, because t2b rewrites the coordinate in `cas`) equals the
manifest the Gate builds from the work bucket's objects, with `alias.txt` a
regular file holding `summary.txt`'s bytes. t2's (the `qc/A/A_qc` coordinate
of `cas-t2`) equals the Gate's decoding of `.fusion.symlinks` (a listed name is
a `symlink` whose target is the object's body; the sidecar is no entry) and
tier one's local `cold` manifest: the backend is not provenance.

T4 (assertion 6 in the cloud). The `--dir` the consumer was given names t2b's
`A_qc` manifest, whose `alias.txt` is a `symlink`, so staging it exercises a
link (ticket 15 decision 7). It is passed as the Item Occurrence
`cas://<collection>/<item>/A_qc`: a directory's Store URI `cas://<manifest>`
has no file name, and Nextflow's FilePorter then stages it into the cache
directory itself and retries its integrity check forever, while
`cas://<manifest>/A_qc` names an entry `A_qc` inside the manifest, which does
not exist. t4 exited 0; the `lid` and `cas` digests equal A.bam's work-bucket
SHA-256, `fromstore`'s equals B.bam's, and the `dir` listing equals the Gate's
listing of t2b's `A_qc` with `alias.txt` holding `summary.txt`'s bytes.

T5 (ticket 04 on S3). t5 exited 0 and staged t4's bytes; its fresh cache's
`seeded_from:lab` equals the `snapshot_written_at` of `cas/index/v3.sqlite`;
no fallback warning in `logs/t5/`; `cas-out`'s snapshot run count
(`x-amz-meta-runs` and a `count(*)` of the downloaded file) did not drop from
after t4 to after t5.

T6 (ticket 03 decision 6). Both t6 runs exited 0 and are in `cas-t6`'s Store
Log; every block hashes to its CID; `t6-items.txt` lists occurrences of both
runs' `aligned` collections; the snapshot has as many `run` rows as the Store
Log has `run` entries; every coordinate names a block the member holds;
`evidence/if-match.json` is `{"status": 412, "body": "second"}`, S3 refusing a
PutObject whose If-Match names a replaced ETag and keeping the newer object.
That probe is the deterministic evidence on AWS; `S3SnapshotStorageTest` pins
the plugin's skip on that 412. The race step is best effort: when exactly one
of the last attempt's two verbs printed `not rewritten: replaced_meanwhile` it
passes, and when the verbs never overlapped in three attempts T6 is SKIP with
"ticket 03 decision 6 was not observed through the plugin on AWS". Rerun
rather than debug a SKIP.

After the table the harness prints the Batch job count and total job seconds
from `trace/*.txt`, and each producer's `nf-blocks: the head node read ...`
line. Those are reported, never asserted. The head node is a laptop nearest
ca-central-1, so every byte it reads crosses regions.

## The evidence tree

```
ids.env                  BUCKET, WORK, RUN_ID
pids                     every nextflow the harness started
logs/<run>/              stdout.log nextflow.log exit (refs.log for t4, t5)
logs/t6-*.txt            the plugin verbs' output
trace/<run>.txt          Nextflow's trace: task_id hash native_id name status exit realtime workdir
evidence/                refs-t4.env refs-t5.env, if-match.json, cas-out-after-t4.json, downloaded
                         snapshots (*.sqlite), coords-cas.json, command-cas/t2/*.command.cas
<run>/                   each run's launch directory
cache-<run>/, cache-consumer/   each head-node index cache
```
