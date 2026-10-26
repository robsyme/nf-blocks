# ADR 0001: fromStore samples: for filters across outputs

## Status

Proposed (2026-10-24). The explorer's Samples tab shows this call, marked as a
proposal in the snippet itself. The plugin does not accept it yet.

## Context

A run of nf-core/rnaseq publishes one item per sample in many outputs
(`quant_salmon`, `markdup`, `preprocessed`, ...), joined by `meta.id`. The
fields a reader filters on often live in a different output from the files
they want: the trimmed read count is on `preprocessed`, while the files to
hand on are in `quant_salmon`. The Samples tab joins the outputs on the join
key, so a filter such as `preprocessed:num_trimmed_reads >= 50000` picks
samples, and those samples then pick items in any output.

Today's call cannot say that. `channel.fromStore(run:, output:, where:)` reads
one output, and `Index.items(completion, output, where)` matches `where`
against that output's own `item_attr` rows. A reader who wants the
`quant_salmon` items of the samples whose `preprocessed` count passes has two
choices: save a Selection (a fixed list, which is right for citing but does
not follow a re-run), or write a join in the script by hand.

## Decision

Add a `samples:` option to `fromStore(run:)` that returns a samples handle
rather than a channel:

```groovy
samples = channel.fromStore(
  run: 'cas://<RunCompletion>',
  samples: [where: ['preprocessed:num_trimmed_reads >=': 50000]])

quant_salmon = samples.output('quant_salmon')
```

`samples:` takes one of two forms. `[where: [...]]` is a filter with the same
key grammar as today's `where:` (`path`, `'path >='`, `'path !='`), plus an
optional output qualifier: `'output:path op'`. A qualified key matches only
that output's items; an unqualified key matches any output of the run. The
qualifier is recognised only when the text before the first `:` is an output
of the run, so a path that contains `:` keeps working. `[ids: [...]]` names
the samples by their join key value, for a list ticked by hand.

The handle resolves the sample set once. `.output(name)` returns a channel of
that output's items whose join key is in the set, in the order today's call
uses. The join key is the pipeline's, defaulting to `meta.id`, then `id`, the
first of these an item holds as a single string.

The sample set is decided per output. An output of two or more items, each
with a join key, is per-sample when at least one of its keys also keys an item
in another output, or when it is the only output of two or more items that
carries join keys. All of a per-sample output's keys are then samples, so a
sample whose later steps failed stays listed: reads keyed S1, S2 and S3 beside
bams keyed S1 and S2 give three samples. A one-item output is per-sample when
its key is one of the run's samples, so a keyed `multiqc_report` or
`all_samples` item is run-level and takes no samples away. The contrasts of a
differential-abundance output (`treated_vs_control`, `ko_vs_wt`) are not
samples when they are keyed in that one output beside per-sample outputs,
since no contrast keys another output. They are listed as samples once one
contrast key also keys another output, even a one-item output: the contrasts
output is then per-sample, and all its keys with it. Any other output with
items is run-level (one item for the whole run, such as `quant_merged`, items
keyed by something that is not a sample, or an output where some item has no
key). A run-level output's items belong to no sample. A run with no output of
two or more items has no samples.

A sample passes a condition according to its items. A qualified key reads the
sample's items in that output; an unqualified key reads its items in every
per-sample output. For `=` and the range operators, the sample passes when
at least one of those items matches, so a sample with no item there fails. For
`!=`, the sample passes when none of those items holds the value, so a sample
with no item there passes. The `!=` set is therefore the run's samples minus
the `=` set for the same key and value. A qualified key that names a run-level
output reads no items for any sample: it matches no sample for `=` and the
ranges, and every sample for `!=`. Conditions intersect, as `where:` keys do
now, and `[where: [:]]` has no conditions, so it means every sample.

It maps onto today's index without a schema change. For an `=` or range
condition, `Index.items(completion, output, [path op: value])` over the
qualified output (or each per-sample output for an unqualified key) gives the
matching items, and their join key values, read from `item_attr`, are the
samples that pass. A `!=` condition runs the same lookup with `=` and takes
the run's samples that are not in the result; calling `Index.items` with `!=`
directly would give a different answer, since it keeps an item that lacks the
value even when the same sample has another item that holds it. `.output(name)`
is then `Index.items(completion, name, [:])` filtered to items whose join key
value is in the set. The explorer's `sampleMatches` (web/src/samples.js), with
the sample table from `buildSamples`, computes exactly these rules over the
same `item_attr` rows through `metadata.matches`, so the plugin can reuse its
test cases.

Run-level outputs are not filtered by samples. The snippet reads them with
today's `fromStore(run:, output:)`.

## Alternatives considered

A `samples:` option on each per-output call,
`fromStore(run:, output: 'quant_salmon', samples: [where: ...])`. This keeps
`fromStore` returning a channel every time, but repeats the filter for every
output, and two outputs could drift apart if one copy is edited.

Qualified keys in today's `where:`, with `output:` naming the output to emit.
This reuses the existing option, but an unqualified key would then mean this
output or any output depending on whether another key in the same map is
qualified, and nothing in the call shows which.

Leaving it to Selections. A Selection already records the exact items, and
the explorer saves one from the same rail. It does not follow the filter to a
re-run of the pipeline, which is the point of a filter.

## Consequences

`fromStore` would return either a channel or a handle depending on its
options, which is new for a channel factory. The handle needs its own type
with a clear error when used as a channel.

The join key becomes part of the plugin's contract, not only the explorer's
config. A pipeline that keys samples differently needs a way to say so, likely
`samples: [where: ..., key: 'meta.sample']`.

Until this lands, the explorer labels the call as a proposal in the snippet's
first comment line and keeps it out of `[data-snippet]`, so the Gate never
runs it. The saved-Selection call stays the one that works today.
