# nf-blocks explorer — UX review

Reviewed: live explorer, store `roblab` (single writable member), 2026-10-24.
Data: `nf-core/rnaseq` × 3 runs (`distraught_murdock` ✓, `sad_yonath` ✓, `sharp_magritte` ✗), `main.nf` × 1, one Selection (`wigshop`).

## The task

> As a computational biologist, take the latest nf-core/rnaseq run, keep samples with ≥ 50,000 reads, and hand their counts to nf-core/differentialabundance.

### What I actually had to do

| # | Step | Outcome |
|---|------|---------|
| 1 | Pipelines → `nf-core/rnaseq` → `distraught_murdock` | Easy. Run list, status, "latest good run" are clear. |
| 2 | Guess which of 24 outputs holds read counts | Hard. `fields` column shows `id, meta.has_genome_bam +4`; no hint that `preprocessed` holds `num_trimmed_reads`. Found it by opening outputs one at a time. |
| 3 | `preprocessed` → Add filter → `num_trimmed_reads >= 50000` | Good. Typed operators, value suggestions with counts, URL keeps the filter, and the `fromStore(... where: [...])` snippet updates as you go. Result: WT_REP1, RAP1_UNINDUCED_REP2. |
| 4 | Apply the same filter to `quant_salmon` | **Blocked.** `quant_salmon` items don't carry `num_trimmed_reads`. The filter can't cross outputs. Had to note the IDs and pick them by hand. |
| 5 | Find the counts matrix | `quant_merged` → one item, `all_samples`, 12 TSV/RDS files. No preview, no column subset, no sample list. |
| 6 | Pick → Picked (2) → Save as Selection | Works, but the Picked panel lists CIDs (`bafyreiea4…o74sru`), not sample names, and `#/compose` renders the Pipelines table in the main pane. |
| 7 | Samplesheet export | Exists (CSV/JSON) but is 46 columns of repeated `meta.*` per channel element. No `condition` column, nothing in differentialabundance's shape (`sample`, `condition`, contrasts). |
| 8 | Know what I'm about to move / how big it is | Not possible. No sizes on runs, outputs, Selections or the store. |

**Bottom line:** the explorer is organised around how the plugin *stores* data (run → output → item → block). The biologist thinks in *samples* and *hand-offs*. Every place those two models diverge is where the task got hard.

## What makes it easy (keep these)

1. **Typed filter builder.** Field search, operator chips, value suggestions with item counts. URL-encoded state (`?where=[...]`) makes filters shareable.
2. **Live code snippet.** The right rail turns the current view into `channel.fromStore(run:…, output:…, where:…)`, with toggles for "this run" vs "latest good run" and typed vs untyped. This connects the explorer to the workflow better than anything else on the page.
3. **"Same for every item"** strip collapses constant fields into one line with one-click filters.
4. **Deduplication made visible.** "Same files also in: sharp_magritte / preprocessed, sad_yonath / preprocessed" and "Every run that produced this file" show content addressing working in a way users can see.
5. **Pin with a reason** ("why keep this? e.g. figure"). Good habit to build in.
6. **Lineage check** ("5 referenced files not stored here, such as input files") is honest about what's missing.
7. **Selections are immutable, addressed lists** with samplesheet export. This is the right primitive for hand-off.

## What makes it hard

### Model mismatches (highest impact)

1. **No sample-centric view.** Sample metadata is spread across outputs (`preprocessed.num_trimmed_reads`, `bam_qc.*.inferred_strandedness`, `quant_salmon.meta.*`). There is no row-per-sample pivot, so "samples where X" can't drive "give me output Y".
2. **Filters don't join across outputs.** The `where` clause is scoped to one output's items. The natural join key (`meta.id`) is right there.
3. **Merged artefacts are opaque.** `quant_merged/all_samples` is the thing differentialabundance wants, but it's one item with 12 files, no preview, and no knowledge of which samples (columns) it contains.
4. **No hand-off shape.** The samplesheet is a raw flattening of the channel record, not the input schema of the downstream pipeline. There's no place to add the `condition` annotation DE needs.

### Information design

5. **Deep meta paths dominate.** `dupradar.meta.salmon_strand_analysis.forwardFragments +34` in the outputs table; long dotted column headers in item grids. No promoted "key metrics", and no grouping by tool.
6. **Unstable column order.** `num_trimmed_reads` moves between views as filters change. `id` is sometimes pushed off-screen.
7. **Numbers shown raw.** `99566.0` (should read 99,566), `89.62899137102112` (should read 89.6%).
8. **Icon-only file grid.** Rows of identical 📄 glyphs per tool column. You only learn which file is which by hovering. `+` (pick) and `{}` (open) are cryptic.
9. **IDs vs names are inconsistent.** Run page shows `distraught_murdock`, the file page shows `bafyreibim…26ocxq` for the same run. Picked panel shows CIDs only.
10. **Sample ID looks like a link but adds a filter.** Clicking `WT_REP1` filters instead of opening the sample.
11. **Empty outputs get equal weight.** 7 of 24 outputs have 0 items and sit in the same table.
12. **Run details is a raw config dump** (~300 lines of `params`/`genomes`). Useful, but should be a searchable diff against defaults or the previous run.
13. **Rendering glitch:** collapsed `<details>` sections (Run details / Storage / Lineage) render as empty table rows at the bottom of the run page.

### Storage, identity, sharing

14. **No sizes anywhere.** Not on runs, outputs, items-in-aggregate, Selections, or the store. "Release content" doesn't say how many bytes it frees.
15. **No identity.** Selections are "named by anonymous". Without `asserted_by`, "mine" can't be computed and pins can't be attributed.
16. **One store at a time.** The footer shows `roblab (writable) · snapshot read by range`, but there's no idea of other stores, of where else a block lives, or of moving data between stores.
17. **Health copy is jargon.** "5 published files that are in no output" repeats identically on every run and doesn't say whether to act.
18. **Search is disabled** ("Search samples, runs… (later)"). Search by sample ID across runs would have saved several steps here.

## Severity summary

| Severity | Items |
|----------|-------|
| Blocks the task | 1, 2, 3, 4 |
| Slows / confuses | 5, 6, 8, 9, 10, 14, 15, 18 |
| Polish | 7, 11, 12, 13, 17 |
