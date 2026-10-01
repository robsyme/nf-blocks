# nf-blocks

## Summary

`nf-blocks` is a Nextflow plugin that publishes pipeline outputs into a
content-addressed store and records their lineage there.

It owns the `cas` URI scheme, so `outputDir = 'cas://lab'` sends every
published file through the plugin instead of onto a `results/` tree, and it
provides a lineage store for `lineage.store.location = 'cas://lab'`, so
Nextflow's own `lid://` records live in the same store.

It is built and tested against released Nextflow 26.04.6. A run writes
DAG-CBOR lineage records into the store beside Nextflow's own `lid://`
records, and keeps a SQLite index of them; another pipeline reads the outputs
back with the `fromStore` channel factory (below), and `nf-blocks:explore`
browses them and saves Selections. `DESIGN.md` is the contract the code is
built against.

### Status: beta

This is a beta (`0.4.0-beta.1`). Pin the version in `plugins { }`, as the
examples below do. What it supports today:

- Milestone 4 (cloud): any member, the writable one included, is a local
  directory or an S3 location. A run on AWS Batch, with or without Fusion,
  publishes into a writable S3 member (below). Under Fusion the task node
  hashes its own outputs; between S3 buckets S3 computes the SHA-256 of a
  server-side copy, so most files never pass through the head node.
- Milestone 5 (nf-core pipelines): nf-blocks records lineage from workflow
  outputs (`output { }`) only. A process that publishes with `publishDir`
  is warned about once; its files are still stored in the content-addressed
  tree, but counted as `unjoined` rather than recorded against any run. An
  output that declares `index { }` records its Output Index File too, linked
  from its Output Collection. See "Pipelines that use publishDir" below.
- Milestone 6 (retention): every run's content is a root by default and is
  kept until you release it; a sweep only ever deletes what is released,
  unpinned, and past its grace period. See "Getting space back" below.
- nf-blocks requires the `nf-amazon` plugin (`>=3.9.2`). If you install
  nf-blocks by unpacking a local build into `NXF_PLUGINS_DIR` rather than
  from the registry, Nextflow 26.04.6 does not fetch that dependency, so
  install it alongside: `nextflow plugin install nf-amazon@3.9.2`.
- In a typed script (`nextflow.enable.types = true`) Nextflow does not reach
  plugin channel factories as `channel.fromStore` until
  [nextflow-io/nextflow#7694](https://github.com/nextflow-io/nextflow/issues/7694)
  is fixed. Call `nextflow.Channel.fromStore(..., records: true)` instead.
- The on-disk format (block kinds, the index, the Store Log) may still change
  before 1.0. A change to the blocks will come with a migration, since a
  block's address is its content and existing stores keep their blocks.
- Warnings from the plugin go to `.nextflow.log`. These also print on the
  terminal: the missing-`fromStore` hint, the default `outputDir` line, a
  clock skew against S3, a process that node hashing cannot reach, a member
  indexed without a usable Index Snapshot, a process that declares
  `publishDir`, and a run whose published files joined no workflow output.

## Get Started

Enable the plugin and point both the lineage store and the output directory at
the same store alias:

```groovy
plugins {
    id 'nf-blocks@0.4.0-beta.1'
}

lineage.enabled = true
lineage.store.location = 'cas://lab'   // alias of the writable member
outputDir = 'cas://lab'                // optional: unset, it is the lineage alias

cas {
    stores {
        lab {
            location = '/data/cas'     // the writable member
        }
    }
    resolve = ['lab']                  // optional; default is every alias, writable first
    asserted_by = 'anonymous'          // optional opaque label; never the OS user name
}
```

An alias matches `^[a-z][a-z0-9_-]{0,31}$` and must not parse as a content
address. `outputDir` may be left out: unset, it means the alias in
`lineage.store.location`. When set, it must name that alias, or the run
aborts.

Nextflow downloads the plugin from the Nextflow Registry the first time the
pipeline runs.

To add nf-blocks to someone else's pipeline, such as an nf-core one, with a
`-c` config file: a `plugins { }` block in that file replaces the pipeline's
own plugin list rather than adding to it. Repeat the pipeline's pinned plugins
beside nf-blocks, or Nextflow installs the latest release of each one the
pipeline includes (measured with nf-core/sarek 3.10.0, whose `nf-schema@2.7.2`
pin was replaced by nf-schema 3.0.0, and the run failed).

## Examples

Any pipeline with a workflow `output` block publishes into the store. The
pipeline used to exercise the plugin lives at
`../.scratch/content-addressed-lineage/test-pipeline`; with the configuration
above, running it writes each published path under the store's coordinate
tree:

```
/data/cas/coords/aligned/A/A.bam
/data/cas/coords/qc/A/A_qc/summary.txt
/data/cas/nf/<run hash>/aligned/A/A.bam/.data.json
```

and the lineage record for a published file names its coordinate rather than a
work directory path:

```json
{"kind":"FileOutput","spec":{"path":"cas://lab/aligned/A/A.bam", ...}}
```

## Pipelines that use publishDir

Most nf-core pipelines still publish with `publishDir` rather than a
workflow `output { }` block, and nf-blocks only builds lineage from workflow
outputs. Point one at `cas://` and its `publishDir` files are still stored
(they get a block and a Publish Coordinate, so they read back and stage like
any other content), but no run's RunCompletion links them: `fromStore` and
the explorer's per-run Output Collections do not reach them, and the
milestone 6 sweep will treat them as unreferenced content.

This is loud, not silent. A process that declares `publishDir` while
`outputDir` is `cas://` is warned about once, on the console and in
`.nextflow.log`:

```
nf-blocks: process 'NAME' uses publishDir; the files it publishes are stored but no run records them. Declare them as workflow outputs (output { }) to keep their lineage
```

and at the end of a run, every file the run stored in `cas://` that no
workflow output claimed (a `publishDir` copy, a `collectFile(storeDir:)`
file, any other write into the store) is counted in
`RunCompletion.anomalies.unjoined` and warned once with the count and the
first few coordinates:

```
nf-blocks: N file(s) stored this run are in no workflow output, so no run records them: cas://lab/..., cas://lab/..., cas://lab/..., .... Declare them as workflow outputs (output { }) to keep their lineage
```

To keep a pipeline's lineage, migrate its publishes to the [workflow output
definition](https://www.nextflow.io/docs/latest/workflow.html#publishing-outputs).
An output that declares `index { }` is joined like any other, and its Output
Index File is linked from the Output Collection and offered as a download
from the explorer's collection page.

Adding nf-blocks to a pipeline you don't own, such as an nf-core one, with a
`-c` config file needs care too: see the plugin-pins note in "Get Started"
above. Measured against nf-core/sarek 3.10.0: only its `multiqc` output uses
`output { }`, so a default run records lineage for `multiqc` (its
`multiqc/index.json` Output Index File is linked from the collection) and
counts everything else (`reports/`, `preprocessing/`, `csv/`,
`variant_calling/`, `pipeline_info/`) as `unjoined`.

## Publishing into S3

A writable S3 member takes an `s3://<bucket>[/<prefix>]` location; the S3
client is nf-amazon's, configured from the `aws` scope. With a cloud executor
the work dir is on S3 too:

```groovy
plugins {
    id 'nf-blocks@0.4.0-beta.1'
}

lineage.enabled = true
lineage.store.location = 'cas://lab'

workDir = 's3://my-work-bucket/work'
process.executor = 'awsbatch'
process.queue = 'my-queue'

aws {
    region = 'us-east-1'
    profile = 'my-profile'             // or any credential source nf-amazon accepts, SSO included
}

cas {
    stores {
        lab { location = 's3://my-cas-bucket/cas' }
    }
    tmpDir = '/scratch/nf-blocks'      // optional; default java.io.tmpdir
}
```

The head node needs scratch disk. An output whose bytes the head node must
read from S3 is spooled to `cas.tmpDir` while it is hashed, then uploaded,
so `cas.tmpDir` needs room for the largest such output. That is an output
over 5 GiB with no node digest, and also any S3-sourced output, whatever its
size, whose server-side copy failed and fell back to the head-node read. A full `cas.tmpDir` stops the run with a message naming
it and the bytes needed.

Blocks are written with `aws.client.storageClass`, `storageEncryption`,
`storageKmsKeyId` and `requesterPays`. `aws.client.s3Acl` is not applied to
member writes. Every block is its own object, so `GLACIER` and
`DEEP_ARCHIVE` are refused (blocks must stay readable), `STANDARD_IA`,
`ONEZONE_IA` and `INTELLIGENT_TIERING` warn that each block pays the
per-object minimum, and a class nf-amazon ignores (`GLACIER_IR`) warns that
blocks go to `STANDARD`.

Immutability comes from conditional writes (`If-None-Match: *`). A bucket
policy can harden it, denying unconditional writes and deletes of blocks to
everyone but a sweep role (bucket, prefix, account and role are
placeholders):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BlocksOnlyIfAbsent",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::my-cas-bucket/cas/blocks/*",
      "Condition": {
        "Null": { "s3:if-none-match": "true" },
        "Bool": { "s3:ObjectCreationOperation": "true" }
      }
    },
    {
      "Sid": "OnlySweepDeletesBlocks",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:DeleteObject",
      "Resource": "arn:aws:s3:::my-cas-bucket/cas/blocks/*",
      "Condition": {
        "ArnNotEquals": { "aws:PrincipalArn": "arn:aws:iam::111122223333:role/nf-blocks-sweep" }
      }
    }
  ]
}
```

That policy has two costs. AWS documents that a bucket enforcing
conditional writes refuses `CopyObject` into the enforced prefix, so every
server-side copy into `blocks/` fails and the head node reads the bytes
instead (untested), so every S3-sourced output then passes through the head
node and spools through `cas.tmpDir`, which needs room for the largest of
them. And when a copy's SHA-256 disagrees with the task
node's digest, nf-blocks cannot delete the wrong object, so the run stops
naming the key to remove with the sweep role.

A lifecycle rule clears staging copies under `tmp/` and abandoned multipart
uploads:

```json
{
  "Rules": [
    {
      "ID": "nf-blocks-tmp",
      "Filter": { "Prefix": "cas/tmp/" },
      "Status": "Enabled",
      "Expiration": { "Days": 1 }
    },
    {
      "ID": "nf-blocks-multipart",
      "Filter": { "Prefix": "cas/" },
      "Status": "Enabled",
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 1 }
    }
  ]
}
```

Under Fusion (`fusion.enabled = true`), `cas.nodeHash` is on by default:
an `afterScript`, chained ahead of any you set, hashes each task's declared
outputs on the node into `.command.cas`, and the head node uses those
digests. A process whose own body sets `afterScript` is not hashed on the
node, and the run warns naming it. Without Fusion, an S3 work dir flattens
symbolic links to copies, so a published directory's internal link arrives
as a regular file. A run also compares this machine's clock with S3's: over
a minute it warns, over five minutes it stops and asks for NTP.

`nextflow lineage find` against an S3 member costs one GET per record.

### A head node that starts cold

A head node without the index cache (a Batch head job, a Seqera Platform
launch) would have to read every block of a large member once. Instead the
first catch-up seeds the cache from the member's Index Snapshot
(`index/v3.sqlite`) and reads only the Store Log entries written after it.
Runs keep the snapshot current while it is under `cas.snapshot.maxBytes`
(64 MiB by default); for a large shared member, run
`nextflow plugin nf-blocks:snapshot` against it on a schedule. With no usable
snapshot the run warns and scans. Setting `cas.index.path` to persistent
disk (EFS, FSx) skips seeding altogether; use one file per head node, since
SQLite's WAL mode needs shared memory.

## Reading outputs in another pipeline

A second pipeline reads a run's outputs, or a Selection, back out of the
store with `fromStore`. Its config names the producer's store as a read-only
member beside its own writable one:

```groovy
plugins {
    id 'nf-blocks@0.4.0-beta.1'
}

manifest.name = 'downstream'           // or cas.pipeline = 'downstream'

lineage.enabled = true
lineage.store.location = 'cas://mine'

cas {
    stores {
        mine { location = '/data/cas-downstream' }   // writable: this pipeline's own outputs
        lab  { location = '/data/cas' }              // the producer's store, read-only here
    }
    resolve = ['mine', 'lab']
}
```

No `outputDir` line is needed; unset, it is `cas://mine`. Set
`manifest.name` (or `cas.pipeline`) so the consuming runs are filed under a
Pipeline Identity of their own rather than as `main.nf`.

`fromStore` comes from the plugin, so the script includes it:

```nextflow
include { fromStore } from 'plugin/nf-blocks'

workflow {
    main:
    picked  = channel.fromStore(selection: '<selection address>')
    aligned = channel.fromStore(run: 'latest', pipeline: 'cas-test-pipeline', output: 'aligned', where: [sample: 'B'])

    picked.mix(aligned).view()
}
```

Each item arrives in the shape the producer published it, here
`[meta, bam]`, with every file a `cas://` path Nextflow stages like any
other. `run:` also takes a `lid://<run hash>` or a `cas://` RunCompletion.

In a typed script (`nextflow.enable.types = true`), Nextflow 26.04.6 cannot
reach a plugin factory through `channel.` (nextflow-io/nextflow#7694). Keep
the include and call it through `nextflow.Channel`, with `records: true` so a
record-typed input receives records rather than plain maps:

```nextflow
nextflow.enable.types = true

include { fromStore } from 'plugin/nf-blocks'

record Sample {
    sample: String
    lane: Integer
}

process COUNT_LINES {
    input:
    tuple(meta: Sample, bam: Path)

    output:
    stdout()

    script:
    """
    printf '%s\\t' '${meta.sample}'
    wc -l < '${bam}'
    """
}

workflow {
    COUNT_LINES(nextflow.Channel.fromStore(selection: '<selection address>', records: true))
        .view()
}
```

A restored record is immutable: `meta + [x: 1]` returns a new map, and
`meta.put('x', 1)` throws `UnsupportedOperationException`. Without
`records: true` every map arrives as an ordinary mutable map, typed script
or not.

The explorer's Selection and run pages show these call lines for what you
are looking at, with a toggle between the untyped and typed forms. A run that
fails because the include is missing prints a warning saying what to add,
just above Nextflow's "Missing process or function" error.

## Browsing a store

`nextflow plugin nf-blocks:explore` serves a live view of every configured
member (`cas.stores`) at `http://127.0.0.1:<port>/`, port `--port` or
ephemeral; `nextflow plugin nf-blocks:snapshot` writes a member's Index
Snapshot and explorer page without starting a server. Once a snapshot exists,
serving the member's directory through any static file server and opening its
`index.html` browses it too. Opening that file straight from disk does not
work: a browser will not fetch from `file://`, and the page says so
(`file_protocol`). A member served straight from a bucket needs the bucket
policy and CORS rule in `DESIGN.md` §15.

Until the plugin is published, `nextflow plugin` finds a local build only
through `NXF_PLUGINS_TEST_REPOSITORY`. After `make install FORCE=1`, `make
explore CONFIG=<config naming your cas stores> ARGS='--port 8123'` sets it and
starts the explorer; `make plugins-json` prints the `export` line for running
`snapshot` or `put` by hand.

## Selections

A Selection curates Output Items across runs into one named group, without
copying anything: composing, renaming, deleting and undoing all happen from
the page opened through `nf-blocks:explore` (the page needs its own `?token=`
to write, so it must be opened through `explore`, not a plain static server).
`nextflow plugin nf-blocks:put <file|/dev/stdin> [--dry-run] [--name <name>]` builds and writes the
same Selection or Claim blocks from a DAG-JSON file on the command line,
sharing the endpoint's one builder. `/dev/stdin` reads the request from a pipe
or a redirect (Nextflow 26.04.6 refuses a bare `-` as an argument).
Staleness is member-scoped (decision 22): superseding a Claim that only a
read-only member has already superseded succeeds and leaves a conflict; the
dry run reports `here` and `name_claims`.

A Selection's items are consumed with `fromStore(selection: <address>)`
(nested Selections flattened, each item once), or exported as a samplesheet
from the page's Selection view: `GET
/api/samplesheet/<selection cid>.csv` or `.json` from `explore`, one row per
item, file columns holding `cas://<cid>/<name>` so the CSV only needs
`-plugins nf-blocks` to stage. See `DESIGN.md` §16 for the full contract.

From the command line, `nf-blocks:items` finds items and `put --name` saves
them as a named Selection:

```bash
nextflow plugin nf-blocks:items aligned nested.kit=truseq --run latest --pipeline cas-test-pipeline
nextflow -q plugin nf-blocks:items aligned lane=2 --run lid://<run hash>,lid://<other run hash> --format selection \
  | nextflow -q plugin nf-blocks:put /dev/stdin --name lane-2
```

Both halves of the pipe take `-q`. Without it, anything Nextflow prints while
`items` runs (a "Downloading plugin" line the first time a version is used,
or the banner a local build prints through `NXF_PLUGINS_TEST_REPOSITORY`)
goes into the pipe ahead of the request, and `put` refuses it.

`items` never writes. By default it prints the samplesheet above with a
leading `occurrence` column; `--format json` prints the same rows as JSON,
`--format occurrences` one `cas://<collection>/<item>` per line, and
`--format selection` a complete `put` request. A condition is
`<path>=<value>`, split at the first `=`, with dotted paths for nested keys;
it matches the value as text whatever its type, so `lane=2` finds the number
and the string. In a `put` request a member may be written as such an
occurrence string instead of `{"item": {"address": ..., "via": [...]}}`,
meaning the item as picked from that collection. `put --name` writes the name
Claim after the Selection, and nothing when the Selection already has that
one name; renaming is the same command with a new name.

## Getting space back

Every run's content is a root by default, so nothing a sweep can delete until
you release it.

`put` a `set retain "lineage"` Claim on a run's RunCompletion to release its
content, or use the explorer's Release content button on the run page. The
run's own metadata stays (its RunManifest, its Output Collections and Items
are still listed and browsable); only the bytes become eligible for a sweep
to delete once nothing else needs them. `del retain`, or the explorer's
Restore content button, undoes it at any time before a sweep has actually
deleted the blocks; after that, `untrash` within the grace period is what
brings them back.

`put` an `add pin "<note>"` Claim on a run, a collection, an item, or a raw
block to pin it, or use the explorer's Pin button. A pin keeps its subject's
whole content closure regardless of `retain` or `delete`: a pinned run's
content is never swept even after it is released, and a pinned item inside a
deleted run keeps that run visible in the explorer as "hidden but pinned".
`del pin`, or Unpin, removes one pin; several pins on the same subject do not
conflict with each other.

`prune` writes those `set retain "lineage"` Claims for you, per pipeline:
`--keep-last <n>` releases every run of that pipeline except the newest
`<n>` (failed and incomplete runs count); `--keep-newer <period>` releases
every run finished before the cutoff. Without `--pipeline` it prunes every
pipeline separately. It never deletes anything and never sweeps; a pinned
run is released like any other (its pins still protect its content), and a
run whose retain state is already in conflict is skipped and reported.

`sweep` is what actually deletes bytes, and only content that is released
(or was never registered as a root) and has been dead for at least the age
floor (`cas.sweep.ageFloor`, default 14 days). It never deletes straight
away: a sweep first moves newly dead content into a Trash ledger, and only
deletes blocks from an older ledger once they have sat there for the grace
period (`cas.sweep.grace`, default 14 days) and are still unreachable in a
fresh check. With no `--apply` it is a dry run that reports what it would do
and touches nothing. A sweep never runs alongside a running pipeline: it
refuses to start while a run is registered, or with `--wait` waits for it,
and a run that starts first makes any later sweep wait in turn.

```bash
nextflow plugin nf-blocks:prune --keep-last 3            # what would be released
nextflow plugin nf-blocks:prune --keep-last 3 --apply true

nextflow plugin nf-blocks:sweep                          # dry run: roots, live, dead, Trash
nextflow plugin nf-blocks:sweep --apply true             # trash the newly dead; delete what is past its grace
```

`sweep --apply` also takes `--wait true`, to wait for a running pipeline
instead of refusing, and `--budget <size>` (for example `--budget '10 GB'`),
to cap how many bytes it newly trashes in one run.

`untrash` takes content back out of Trash before its grace period ends:
`nextflow plugin nf-blocks:untrash <cid> [<cid> ...]` for specific addresses,
or `--sweep <id>` for everything one sweep trashed. It is a stopgap, not a
retention decision: whatever it restores is trashed again by the next sweep
unless you also pin it or `del retain` its run.

A run waits for a sweep the same way a sweep waits for a run, so the two
never act on the store at once. If a run's own registration fails to write,
the run warns and carries on; the age floor and grace period still give it
room before anything it wrote could be swept.

An S3 sweep role needs `s3:DeleteObject` on `blocks/*`, `log/*`, `live/*`,
`trash/*` and `tmp/*` under the member prefix, plus
`s3:AbortMultipartUpload` and `s3:ListBucketMultipartUploads` for abandoned
uploads older than the age floor. See "Publishing into S3" above for the
bucket policy that restricts deletes to this role.

See `DESIGN.md` §19 for the full contract, including what a sweep does not
protect: a pipeline that only reads a member (`fromStore`, no publish)
cannot register there, so a mid-run input can in principle vanish under it,
and a Claim or Selection written in the narrow window between a sweep's last
re-check and its delete batch is not caught by that check.

## Plugin development

This project was created from the [Nextflow plugin template](https://www.nextflow.io/docs/latest/guides/gradle-plugin.html#gradle-plugin-create).

### Building

```bash
make assemble
```

### Testing

```bash
make test    # unit tests
make check   # unit tests plus the dependency check and the memory bound
make smoke   # builds, installs into a temp NXF_PLUGINS_DIR and runs a real pipeline
```

`make smoke` needs Nextflow 26.04.6 on the path; override the binary with
`NEXTFLOW=/path/to/nextflow` and the pipeline with `PIPELINE_SRC=...`. It keeps
its store, its config and its `.nextflow.log` under a temp directory and prints
the path.

### Publishing

Plugins can be published to a central Nextflow registry to make them accessible to the Nextflow community.

Follow these steps to publish the plugin to the Nextflow Registry:

1. Create a file named `$HOME/.gradle/gradle.properties`, where `$HOME` is your home directory. Add the following properties:
    * `npr.apiKey`: Your Nextflow Registry access token.
2. Package your plugin and publish it to the registry: `make release`.

## License

Apache License 2.0. See the [`COPYING`](COPYING) file for details.
