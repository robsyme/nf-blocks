# nf-blocks

## Summary

`nf-blocks` is a Nextflow plugin that publishes pipeline outputs into a
content-addressed store and records their lineage there.

It owns the `cas` URI scheme, so `outputDir = 'cas://lab'` sends every
published file through the plugin instead of onto a `results/` tree, and it
provides a lineage store for `lineage.store.location = 'cas://lab'`, so
Nextflow's own `lid://` records live in the same store.

This is the walking skeleton. The boundary with Nextflow is in place and
proven on released Nextflow 26.04.6: the plugin loads, publishes through
`cas://`, and keeps `lid://` reads working. The block store itself, the
DAG-CBOR lineage records, the SQLite index and the `fromStore` channel factory
arrive in the tasks that follow; see `DESIGN.md` for the contract they are
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
outputDir = 'cas://lab'                // publish through the store

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
address. The alias in `outputDir` must be the same one as in
`lineage.store.location`.

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

## Selections

A Selection curates Output Items across runs into one named group, without
copying anything: composing, renaming, deleting and undoing all happen from
the page opened through `nf-blocks:explore` (the page needs its own
`?token=` to write, so it must be opened through `explore`, not a plain
static server). `nextflow plugin nf-blocks:put <file|-> [--dry-run]` builds and writes the
same Selection or Claim blocks from a DAG-JSON file on the command line,
sharing the endpoint's one builder; `-` reads stdin in-process, and from a
real shell (Nextflow 26.04.6 refuses a bare `-`) use `/dev/stdin` instead.

A Selection's items are consumed with `fromStore(selection: <address>)`
(nested Selections flattened, each item once), or exported as a samplesheet
from the page's Selection view: `GET
/api/samplesheet/<selection cid>.csv` or `.json` from `explore`, one row per
item, file columns holding `cas://<cid>/<name>` so the CSV only needs
`-plugins nf-blocks` to stage. See `DESIGN.md` §16 for the full contract.

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
