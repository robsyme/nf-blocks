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

## Get Started

Enable the plugin and point both the lineage store and the output directory at
the same store alias:

```groovy
plugins {
    id 'nf-blocks@0.1.0'
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

## Reading outputs in another pipeline

A second pipeline reads a run's outputs, or a Selection, back out of the
store with `fromStore`. Its config names the producer's store as a read-only
member beside its own writable one:

```groovy
plugins {
    id 'nf-blocks@0.1.0'
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
