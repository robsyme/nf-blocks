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
