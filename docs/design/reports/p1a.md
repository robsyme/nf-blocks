# P1a report: run Samples tab, cross-output join (explorer v0.3)

Branch `explorer/p1a-samples`, based on `explorer/v0.3` at `185a2f3` (P0 merged). Code head `2262733` before the merge of `explorer/v0.3` (P1b) at `23c4c91`. The merge is `adbf2b9`, and `6d69a07` adds P1b's claim tags to search. This report and the refreshed walk files are committed on top.
Plan: `docs/plans/2026-10-24-explorer-v0.3-p1a.md`. Spec: `docs/design/03-agent-prompts.md` (preamble and P1a), the P1a addendum from the P0 integration review, and mockups 07 and 12.

## What was built

**Samples tab** (`web/src/samples.js`, `web/src/views/samples.js`, `web/src/views/run.js`).
- When the run's items share a join key, Samples is the first and default tab: `Samples · Outputs · Provenance · Storage`. Otherwise Outputs stays the default, and `?tab=samples` shows an empty state that links to Outputs.
- There is one row per sample. Each item's key is the first of the pipeline's `joinKey` paths it holds (default `['meta.id', 'id']`; lint_* outputs carry only `id`).
- The samples are the key values of outputs that hold two or more items. An output is per-sample when every item's key is a sample, and run-level otherwise. On distraught_murdock that gives 5 samples, 11 per-sample outputs, and 6 run-level outputs: deseq2, deseq2_pseudo, multiqc, pipeline_info, quant_merged, quant_merged_pseudo.
- Each column records its source output and path. The header shows the label with the source in mono under it. A path that two outputs give different values for splits into one column per output.
- Columns: promoted fields, per-output "has" chips (quant, BAM, bigWig, QC), and Size, the unique bytes over all of the sample's items. Everything else is in a column picker grouped by output, with paths shared by several outputs under "Shared". The line under the table reads "47 more fields from 8 outputs in the column picker" on roblab.
- Filters apply to samples, and the URL `where` takes an output qualifier: `#/run/<cid>?tab=samples&where=[["preprocessed:num_trimmed_reads","int","50000",">="]]`.
  - A qualified key reads the sample's items in that output; an unqualified key reads all of them.
  - Matching uses the same `metadata.matches` rows as `Explorer.items`. So `=` and ranges fail a sample with no item in that output, and `!=` passes it.
  - Typing a value rewrites the URL with `replaceState` and keeps focus.
- "Show N excluded" reveals failed rows dimmed. A manual tick or untick marks the row `manual`, and a filter change clears the manual marks, as in the mockup script. "Use N samples" opens the rail.
- Loading reads every item block once (typed view, so a stored `99566.0` stays a float), under `SAMPLES_ITEM_CAP = 2000`. Over the cap, the probe answers false and the run opens on Outputs. Leaving the page stops the reads, including during the default-tab probe.

**Hand-off rail** (`web/src/handoff.js`, `web/src/panel.js`).
- A new `samples` panel state. On the Samples tab it shows even when the Picked tray holds items, with a "N picked · Open the picked list" line.
- "Hand off N samples" lists Per sample outputs, then Run-level outputs, each with a tick and `N · bytes` for the current samples. Configured outputs come first, in config order. The default ticks come from config (`quant_salmon`, `quant_merged`). Then Total.
- "Pick N items" adds exactly the ticked items to the tray, each with its collection as via. "Save as Selection" saves the same members through the existing save flow (dry run, held elsewhere, name claim) without touching the tray. Both buttons wait until Total shows its bytes, and both are off with no sample selected.
- Merged note, from evidence. When the config names a matrix file (`salmon.merged.gene_counts.tsv`), the rail reads its header row and counts the run's samples among the columns: "Holds 5 samples as columns; your 2 are among them. Preview". Preview opens the file page with `?highlight=[…]`, which marks those columns. Without such evidence the note reads "One item for the whole run."
- "Use in a workflow" shows the proposed snippet, marked as a proposal in its first comment line. It is kept out of every `data-snippet*` attribute, so the Gate never runs it. With manual ticks it uses `samples: [ids: [...]]`. After a save, the reader is taken to the new Selection page, which shows the working `fromStore(selection:)` call.

**ADR** `docs/design/adr/0001-fromstore-samples.md` (status Proposed). It proposes `channel.fromStore(run:, samples: [where: [...] | ids: [...]])`, which returns a handle with `.output(name)`, with keys qualified as `'output:path op'`, and maps the proposal onto today's `Index.items`. The plugin DSL is unchanged.

**Search v1** (`web/src/views/search.js`, `Explorer.searchSamples` / `searchAddresses`, five page queries).
- The top-bar box is enabled, and `/` focuses it. A typeahead shows the top 3 per group. Enter opens `#/search?q=`.
- Sample IDs are searched by exact value and by prefix over the id paths, as a range on `item_attr(path, type, value)`, not LIKE. Each sample row shows its run, promoted values ("99,566 reads (trimmed) · 89% mapped"), output count and date. It links to that run's Samples tab, filtered to the sample on the run's join path.
- Address prefixes (13 characters or more) are searched over runs, outputs, Selections, items and files. An address with exactly one match opens its page.
- The page says that file, output, run and Selection names are not searchable yet. It keeps the mockup's address tip.

**Items grid follow-ups (addendum).**
- S1: once a promoted column has been shown for an output in this page session, it stays a column, and a filtered view never moves one to the strip.
- Multi-file chips open a menu of their files, each linking to its file page. A one-file chip still links straight to the file.
- The hover funnel has its own slot in every cell, so it never covers text.

## P1a items and acceptance

| Item | Status | Evidence |
|---|---|---|
| Samples tab, default when items share a join key; key configurable | Met | `pipelineConfig(p).joinKey`. distraught_murdock opens on Samples; the walk's step 1 reads "default tab samples" |
| Per-run sample table, one row per key; columns from every output's item_attr, each with source output and path | Met | `buildSamples`; header small text names the source; split columns on disagreement (samples.test) |
| Promoted columns for rnaseq plus per-output "has" chips; the rest in a picker grouped by output | Met, with the Strand ruling below | Reads (trimmed)/preprocessed, % mapped/aligned, Layout/meta.single_end, Strand/meta.strandedness; quant BAM bigWig QC chips |
| Filters apply to samples; URL `where` with output qualifier | Met | The walk's step 2 hash is `[["preprocessed:num_trimmed_reads","int","50000",">="]]` |
| "Use N samples" opens the hand-off: outputs with item counts and sizes; merged-output note | Met | walk step 3; note from the matrix header |
| Right-rail snippet expresses the cross-output filter; ADR; no DSL change | Met | `samplesSnippet`, ADR 0001; no plugin code changed |
| **Acceptance:** distraught_murdock, threshold 50,000 gives WT_REP1 and RAP1_UNINDUCED_REP2 | **Met** | walk: `filtered rows ["RAP1_UNINDUCED_REP2","WT_REP1"] match 2 of 5 samples` |
| **Acceptance:** picking quant_salmon from the drawer yields exactly those two items | **Met** | walk: `#/compose` shows "2 items · 56.5 KB, distraught_murdock / quant_salmon, WT_REP1, RAP1_UNINDUCED_REP2" |
| Mockup 07 acceptance: quant_salmon + quant_merged gives 3 items, 143 KB | Met for 3 items. Measured 140 KB | quant_merged's `salmon.merged.tx2gene.tsv` and `…tx2gene_augmented.tsv` are the same block. Stored unique bytes are 83,338; the mockup counted that block twice (86,305) |
| Mockup 07/12: all numbers formatted, no CIDs as labels | Met | walk audits at all 6 steps: `cidOnly []`, `floats []`, `blank 0`, no page errors |
| Mockup 12: search groups, a sample result opens the Samples tab filtered to it, address jump, in-browser only | Met for v1 scope | walk steps 5–6; `#/search?q=WT_REP1` gives 3 sample rows and 33 items; search v2 items below |
| Addendum S1 column stability | Met | table.test and items-view tests, including open filtered then unfiltered |
| Addendum: promoted.js output-qualified keys | Met | fields carry `output`; `fieldKey` gives `preprocessed:num_trimmed_reads` |
| Addendum: labels settled | Met (ruling) | "Strand" = `meta.strandedness` on every page; inferred strandedness is an ordinary field |
| Addendum: multi-file chips, funnel overlap, walk float check | Met | file menu; funnel slot; the walk is served on `localhost` and its float check reads live text |

## Deliberate differences from the mockups
- Sample rows are sorted by sample id. The mockup listed WT_REP1 first.
- Size per sample counts unique bytes over every output (WT_REP1 17.1 MB, RAP1_UNINDUCED_REP2 7.16 MB). The mockup summed five outputs (16.5 MB, 6.53 MB).
- The rail lists all 17 outputs: 11 per-sample and 6 run-level. The mockup showed 7.
- Only active filters have chips. The mockup also shows an empty "% mapped ≥ any" chip.
- "Show N excluded" is hidden when nothing is excluded.
- Run-level notes come from evidence only. Only quant_merged gets "Holds 5 samples as columns"; multiqc reads "One item for the whole run".
- "Hand off to differentialabundance →" is left out (P4).
- The snippet names the run as `cas://<completion cid>` in full, because the plugin rejects a bare CID. Its first line marks it as a proposal.
- Search has an Addresses tab instead of Files, since file names need v2. Sample rows show "11 outputs" (the outputs that hold the sample); the mockup showed the run's 17.
- `% mapped` reads `89%` where the mockup shows `89.0%`. Numbers drop trailing zeros (user ruling, 2026-10-25; see the follow-up round).
- No Google Fonts fetch, as in P0.

## Screenshots
`docs/design/screenshots/p1a/`: `mock-07-run-samples-{light,dark}.png` next to `build-07-run-samples-{light,dark}.png` (`#/run/<distraught_murdock>?tab=samples&where=[threshold]`), and `mock-12-search-*` next to `build-12-search-*` (`#/search?q=WT_REP1`). All are 1440×900 on roblab. The walk's step screenshots are `walk-1-samples.png` … `walk-6-search-to-samples.png`, and the walk output is in `walk.log`. The scripts are `walk.mjs` and `shoot.mjs`; they import Playwright from this checkout's `gate/browser/node_modules`, because the main checkout's copy was gone during this phase.

## Tests

On the merged branch (P1a + P1b, at `6d69a07`):
```
$ cd web && npm test
ℹ tests 650
ℹ pass 650
ℹ fail 0

$ cd web && npm run build
dist/index.html 1752997 bytes

$ make test
BUILD SUCCESSFUL in 1m 18s     (1409 Gradle tests, 0 failures, 0 errors)

$ python3 -m unittest discover -s gate
Ran 333 tests
OK (skipped=1)

$ node docs/design/screenshots/p1a/walk.mjs http://localhost:8731/index.html docs/design/screenshots/p1a
(walk.log: same acceptance results as below, every step clean, no page errors)
```

Before the merge (P1a alone, at `2262733`):
```
$ cd web && npm test
ℹ tests 608
ℹ pass 608
ℹ fail 0                      (523 at the P0 base)

$ cd web && npm run build
dist/index.html 1744163 bytes

$ make test
BUILD SUCCESSFUL in 53s       (1378 Gradle tests, 0 failures, 0 errors, per build/test-results; 1373 at the P0 base,
                               plus 5 ExplorerQueriesTest no-scan rows for the search queries)

$ python3 -m unittest discover -s gate
Ran 333 tests in 9.572s
OK (skipped=1)

$ node docs/design/screenshots/p1a/walk.mjs http://localhost:8731/index.html docs/design/screenshots/p1a
(output in walk.log: every step cidOnly [], floats [], blank 0; page errors [])
```
I did not run the Gate's browser tiers or `make check`. No Gate step reads the run page's default tab; B.retain opens `?tab=storage`, which is unchanged.

## Process
Five tasks, each built by one implementer subagent and gated by a task review. Tasks 2, 4 and 5 ran in parallel in isolated worktrees after Task 1, and Task 3 ran beside Task 2's fix round. Then came a whole-branch review on Opus, one fix wave of 10 commits, and a scoped re-review, which found the branch ready to merge. Fix rounds: Task 2, Task 3, Task 4 and Task 5 each had one; Task 1 had none.

## Merging alongside P1b and P2
At the orchestrator's request, `explorer/v0.3` at `23c4c91` (P1b merged) is merged into this branch (`adbf2b9`). Four files conflicted, resolved as follows:
- `web/src/panel.js`: P1a's `samples` branch in `draw` is kept. The other branch keeps P1b's claims section after the main section, and both import lines are kept.
- `web/src/app.js`: P1b's pipeline route (`parsePipelineQuery`), `ctx.viewer`/`ctx.identity` and seg-toggle work are kept, with P1a's run, content and search routes and the search wiring in `start()`. Against `explorer/v0.3`, app.js differs only by those P1a lines, and kit-css.js and components.js are unchanged.
- `web/src/views/index.js`: both sets of exports are kept.
- `web/src/styles.js`: adjacent appended blocks, all kept.

After the merge, search shows P1b's claim tag (avatar, "named", age, and `conflict` when names disagree) beside every Selection name it renders: the Selections panel, the Addresses panel and the typeahead (`build-12-typeahead-selection-light.png`). Nothing else P1a renders is a claim. Run names come from the RunManifest, and sample IDs and outputs are facts.

Against `explorer/p2-storage`, only `web/src/queries.json` conflicts, where both appended queries at the end. P2's schema 4 keeps the indexes the search queries use (`item_attr_path_type_value`, `producer_content_cid`, `collection_item_item`), and keeps `itemFileMap`, which the hand-off's bytes use. After merging, run `./gradlew test --tests robsyme.cas.core.ExplorerQueriesTest` on the merged queries.

One P0 rule changed outside an append. The task 5 overlap fix deleted the six absolutely positioned funnel rules in P0's items grid block of `styles.js` instead of overriding them.

## Schema and format changes
None. The index schema (v3), block formats and log formats are unchanged. Five page queries were added to `web/src/queries.json` (`searchSamples`, `searchRunCids`, `searchCollectionCids`, `searchItemCids`, `searchContentCids`). Each has a row in `ExplorerQueriesTest`'s no-scan table, and each plan uses an existing index or primary key. P2's schema bump is not touched. The follow-up round replaced `searchSamples` with two queries; see that section.

## Search v2: what the remaining search needs

Each of these needs an index that the v3 schema lacks. They should land on top of P2's schema bump. Each query is a prefix range, so `ExplorerQueriesTest`'s rule (no `SCAN` except `run` through a covering index, `json_each` or a constant row) holds with a `SEARCH … USING INDEX` plan. Case-insensitive search would declare the index `COLLATE NOCASE` and compare with `COLLATE NOCASE`, which SQLite can still answer from the index.

| Search | Table | New index | Query it serves | Rule it satisfies |
|---|---|---|---|---|
| Run names | `run` | `run(run_name, completion_cid)` | `SELECT completion_cid, run_name, pipeline, status, finished_at FROM run WHERE run_name >= ? AND run_name < ? LIMIT ?` | Today this is `SCAN run` without a covering index, which the guard refuses. With the index it is `SEARCH run USING INDEX run_run_name` |
| File names (Leaf names) | `producer` | `producer(filename, content_cid)` | `SELECT content_cid, min(filename) … FROM producer WHERE filename >= ? AND filename < ? GROUP BY content_cid LIMIT ?` | `SEARCH producer USING (COVERING) INDEX`. Today `producer` has only `producer_content_cid` |
| File names inside directories | new `dir_entry(dir_cid, name, content_cid)` | `dir_entry(name)` | the same shape, for DirectoryManifest entry names (spec 12 lists them) | needs ingest work: directory entries are not in any v3 table |
| Output names | `collection` | `collection(output_name, completion_cid)` | `SELECT collection_cid, completion_cid, output_name FROM collection WHERE kind = 'output' AND output_name >= ? AND output_name < ? LIMIT ?` | `SEARCH collection USING INDEX`. Today the only index leads with `completion_cid` |
| Selection names | `claim_current` | `claim_current(attribute, value, subject_cid)` | `SELECT subject_cid, value FROM claim_current WHERE attribute = 'name' AND value >= ? AND value < ? LIMIT ?`, joined to the `collection` PK with `kind = 'selection'` | `SEARCH claim_current USING COVERING INDEX`. Today `claim_current_subject` leads with `subject_cid` |
| Substring search (any of the above) | an FTS5 table | `fts(name, kind, cid)` | `… WHERE fts MATCH ?` | needs `SCAN fts VIRTUAL TABLE` added to `ALLOWED_SCANS`, with a budget check on the year snapshot |

Sample search over runs newer than the snapshot would need their closures built per keystroke; v1 searches the snapshot only for sample IDs. Address search includes newer runs and Selections.

## Rulings made during the phase
Each one says what it costs if it's wrong. They are in the order they were made.
- Scope, agreed with the orchestrator: no schema change. Search v1 covers sample IDs and addresses. Run, file, output and Selection names wait for v2 on top of P2's schema bump. I first proposed run names for v1, then found the no-scan guard refuses a run_name scan, and corrected it. If wrong, search does less than mockup 12 until v2.
- Join key `['meta.id', 'id']` per pipeline. The samples are the key values of outputs with two or more items, and any output whose items are not all samples is run-level. If wrong, a single-sample run gets no Samples tab, and a stray keyless item demotes its whole output. Failed reads are dropped first.
- The rail contract is `{ completion, output: null, samples: { table, selected, where, manual }, sizeOf }`. If wrong, a field name changes.
- `samples.js` keeps every multi-output path in `columns`. The tab shows only promoted columns by default, and the rest go in the picker. If wrong, nothing beyond the picker grouping.
- Tasks 2, 4 and 5 ran in parallel in isolated worktrees. If wrong, only merge work, which is done.
- Value-cell funnels count as part of the addendum's overlap item. If wrong, a few CSS lines.
- Address search starts at 13 characters: 6 past the 7-character codec head (`bafyrei`, `bafkrei`) that every CID of a codec shares. Mockup 12 says "6 chars after the multibase prefix". Read literally, `bafyrei` would match every dag-cbor block. If wrong, change one constant.
- Promoted values on search rows were first held back, while `promoted.js` was being rewritten, then added. This cost one fix round.
- "Strand" is `meta.strandedness`, the mockup 07 header source, and not the spec's `meta.salmon_strand_analysis.inferred_strandedness`. On roblab the inferred path exists only on WT_REP1, so the spec's column showed "—" for 4 of 5 samples. The inferred path is an ordinary field. This also settles P0's open "Strandedness vs Strand" question: "Strand" on every page. If wrong, change one config line: `{ path: 'meta.strandedness', label: 'Strand' }` in `web/src/promoted.js:13`. The user confirmed it on 2026-10-25.
- Non-integral floats kept 3 significant figures, trailing zeros included (88.99 read "89.0"). The user overruled this on 2026-10-25: numbers drop trailing zeros everywhere. See the follow-up round.
- The proposed snippet names the run as `cas://<completion cid>`, since the plugin rejects a bare CID and the run-level line must run today. If wrong, nothing.
- A save from the rail opens the new Selection page, as every other save does. The rail's saved-call view shows on Back. If wrong, the rail would have to stay put after a save.
- On the Samples tab the hand-off rail shows even when the Picked tray holds items, with a "N picked · Open the picked list" line. Mockup 07 shows both together. If wrong, the picked list is one click further away there.
- P1a does not merge `explorer/v0.3` (with P1b) into its branch. The resolution is described above for the integrator. If wrong, the integrator resolves four small conflicts.
- After the final review, the file menu's clipping was fixed together with its deferred Escape and outside-click close, the snippet's `channel` guard and the search menu's `aria-activedescendant`. Other deferred minors were left, per the final reviewer's triage.

Parked after the final re-review, none blocking:
- the fixed file menu measures height with `innerHeight`
- a menu removed by a same-page redraw keeps its listeners until the next click or scroll
- the ADR's index-mapping paragraph could say a run-level qualifier needs no lookup
- one vacuous assertion in samples.test
- the items filter bar's document listener lives for the page, which predates P1a
- on narrow screens the rail toggle reads "Use" on the Samples tab

Smaller deferred items are in the plan workspace ledger:
- the search page's Selections lookups run one per item, capped at 50
- a numeric column shifts by the funnel's width in multi-valued cells
- the Samples tab's probe re-reads collection counts
- `data-state=ready` comes before the Samples tab body on a bare run URL. Revisit this if a Gate step ever opens `#/run/<cid>` with no tab.

## Follow-up round (2026-10-25)

This round started by merging `explorer/v0.3` at `95dec3c` (P0, P1a, P1b, P2, P3, P4) into this branch (`b813a5d`, no conflicts). It then took the integrator's review items and the user's rulings. One implementer built the items, then two review rounds followed and fixes were made. The commits run from `fa94d68` to `7005151`.

| Item | Commits | What changed |
|---|---|---|
| S1: sample search lost the newest runs once LIMIT applied | `fa94d68`, `1bdb593`, `54f9815`, `9b87235`, `b665ac2`, `7005151` | The single `searchSamples` query is replaced by two. `searchSampleValues` lists the distinct IDs in the prefix range from the covering index (`SEARCH item_attr USING COVERING INDEX`, DISTINCT streams, so LIMIT stops the walk). `searchSampleRows` reads one exact ID's rows newest run first (`ORDER BY r.finished_at DESC, r.completion_cid, a.item_cid, c.collection_cid, a.path`). Its sort covers only that ID's rows, and the order is total, so a longer read starts with what a shorter one returned. An ID equal to the query reads first, with the whole limit. Other IDs share what is left, then the remainder goes newest first, so the typeahead for `WT_` offers both WT_REP1 and WT_REP2. `more` is true only when something was cut. Both queries are in ExplorerQueriesTest's no-scan table. On roblab, the old query for WT_REP1 with LIMIT 30 gave distraught_murdock 8, sad_yonath 11, sharp_magritte 11; the new one gives distraught_murdock 19, then sad_yonath 11 |
| S2: numbers drop trailing zeros (user ruling) | `643ecb6`, `b2ee5c5`, `7926338` | `formatNumber` keeps 3 significant figures with no minimum: 0.5, 89%, 95.9. As the orchestrator extended the ruling, `bytes()` passes its text through a new exported `trimZeros`: 1 MB, 21 MB, 25 MB, 1.5 MB, while 5.29 MB and 142 MB are unchanged. This departs from the mockup kit, which writes `1.00 MB`. `trimZeros` trims only plain decimal text, so `1.2e+30` is safe. The dedup ratio was left for the final cleanup; the cleanup round routes it through `trimZeros` (see that section) |
| A bare `#/run/<cid>` was ready before any tab body | `d711e37` | The run view waits for the default-tab probe and the tab it opens, guarded by `alive()`, before it resolves. The header no longer draws ahead of the body |
| Multi-contrast outputs added fake sample rows | `e01fe32`, `bb0613e` | Rule, after two rulings: only outputs holding two or more items decide. A key from such an output is a sample when it keys items in at least two outputs, or when only one output of two or more items carries keys. A contrasts output beside per-sample outputs is run-level. A run with fastqc plus a keyed multiqc keeps its samples. A single-output run keeps its samples, so P4's project Samples tab and hand-off (`runSampleTable`) are unchanged, and their tests pass untouched. ADR 0001 says so, and notes that contrasts keyed in two multi-item outputs would still be listed. The empty state read "No output holds two or more items with a sample id, or none of those ids appears in a second output." The cleanup round replaced this rule with a per-output one (see that section) |
| Rail toggle and "Use N samples" disagreed | `b1db6ff` | On the Samples tab the narrow-screen toggle reads `Use · 2 samples` |
| Search is case-sensitive until v2 | `973e280` | The scope note says "Sample IDs match from the start and are case-sensitive." |
| Route parsing and '+' | `53f9136`, `42cf505` | New `web/src/routequery.js` `queryOf` decodes with `decodeURIComponent`, so a hand-typed '+' stays '+'. The run, content and search routes and the search box use it. `routequery.test.mjs` round-trips an output-qualified `where` holding '+', '&', '=' and a space |

Notes the integrator asked for:
- Pick and Save enable with "size unknown" when the size walk fails. That is deliberate: the reader sees that the bytes are unknown before committing, and the action isn't blocked forever.
- Manual-tick marks differ slightly from mockup 07. The mark is a `manual` kind chip after the sample name. It clears when any filter changes, as in the mockup script, and it is kept when "Show excluded" is toggled.

Verification at `7005151`:
- `cd web && npm test`: 1078 tests, 1078 pass, 0 fail (1058 at the merge base `b813a5d`). `npm run build` succeeds.
- `./gradlew test --tests robsyme.cas.core.ExplorerQueriesTest`: 74 tests, 0 failures.
- The acceptance walk on roblab is clean at every step, with the same results as before: threshold 50,000 gives RAP1_UNINDUCED_REP2 and WT_REP1; picking quant_salmon puts those 2 items in compose; quant_salmon + quant_merged gives 3 items, 140 KB.
- The screenshots under `docs/design/screenshots/p1a/` were re-shot with the new number format.
- This build reads only a v5 snapshot. The roblab store holds a v3 one, and nothing here writes to it. The screenshots and the walk therefore serve a v5 snapshot built read-only from the store by `RoblabStorageTest` with `-Droblab.out`, while blocks come from the store itself.

- `make test`: BUILD SUCCESSFUL. 2552 Gradle tests, 0 failures, 0 errors, 8 skipped; the skips are the opt-in roblab tests.
- `python3 -m unittest discover -s gate`: 339 tests, OK (1 skipped).
- Solo Gate (`gate/gate.sh`) at `7005151`: Nextflow 26.04.6 in a scratch `NXF_HOME`, a fresh `GATE_ROOT`, nothing else running in this worktree. Exit 0. Lineage 18 PASS, 0 FAIL, 5 SKIP. Browser A 6/6, with A2 unchanged: producers 8 req / 32,768 B, latest 4 req / 16,384 B, items 43 req. Browser B 14/14. Nothing was written under `~/.nextflow`.

Things found on the way:
- `RoblabStorageTest` (opt-in, `-Droblab.store`) fails its pinned figure on the merged branch: stored bytes 142,134,639 against 142,134,100. The roblab store now also lists a `scratch-workflow` pipeline, so another session has written to it since P2 pinned that figure. P1a doesn't touch storage accounting.
- The first solo Gate run of this round, at `b2ee5c5`, failed browser A1, because the `resumed` run exited 1 on Nextflow's PublishDir copy and its A.bam was never published. The runs at `7926338` and `7005151` passed A1. The Gate's resumed run is in P4's follow-up this round.

## Cleanup round (2026-10-25)

The final round before the draft PR. The branch was fast-forwarded to `explorer/v0.3` at `bc29671`, with every phase merged. Three groups followed, each from one implementer with a task review: A (web) on this branch, B (identity) on `cleanup-b`, merged as its own merge commit, and C (personal data) on this branch after A and B. One whole-branch review followed, and one fix wave.

Group A, web:

| Item | Commits | What changed |
|---|---|---|
| A1: the default tab follows the samples rule | `4a9cae8`, `391f27c`, `5d3aed9` | `hasSamples` is gone. On a bare `#/run/<cid>`, `joinRun` loads the items as the Samples tab does (under `SAMPLES_ITEM_CAP` and `alive()`) and runs `buildSamples`. The page opens on Samples only when the table has samples, so it never opens on an empty tab, and the loaded table is handed to the tab, so nothing is read twice. While it reads, the app's progress line shows "fetched N of M blocks". On a named tab (`?tab=outputs` and so on), Samples-tab visibility comes from a cheap first-item check (lead ruling), and opening the tab runs the real join |
| A2: samples are decided per output, so failed samples stay | `a249d00`, `906a455`, `c8f8700` | An output of two or more items, every item keyed, is per-sample when one of its keys also keys another output (one-item outputs count), or when it is the only such output carrying keys (lead ruling, which keeps single-output runs and P4's project pages). All its keys are samples, so reads {S1, S2, S3} beside bams {S1, S2} gives S1 to S3. A one-item output is per-sample when its key is a sample, and every other output is run-level. Contrasts beside per-sample outputs drop out, unless a contrast key also keys another output, even a one-item one. ADR 0001 and the JSDoc say so. The empty state reads "No output holds two or more items, each with a sample id, or none of those ids appears in a second output." |
| A3: route parsing is tested | `21ec7aa`, `357b970` | `ROUTES` and the parse helpers moved from app.js to `web/src/routes.js`, which has no side effects at import. `routes.test.mjs` runs the real routes. Reverting the run, content or search route to `URLSearchParams` fails a test. The P4 and `?release=1` route tests also draw the route's view, so a swapped argument fails |
| A4: dedup ratios drop trailing zeros | `b743b89` | `ratioText` in format.js (through `trimZeros`) for home.js and storage.js: "3.1×", "1×". seal.js is untouched: its `toFixed(2)` is Seal geometry, pinned byte for byte to the mockup kit, and no ratio |
| A5: "Also held by" names hand-offs | `b05b036`, `17797e6`, `000f5d0` | The run's Storage tab lists hand-offs among the sharers, with their own links. An unnamed one reads "an unnamed hand-off", and one with no Selection to link to is drawn unlinked. The Release confirmation's words follow the kinds named, including "no other run, Selection, hand-off or pin holds these files". A TypeError when no sharer was named is fixed |
| A6: '+' in list routes | `3c3cb3d`, `89c27d7` | `listquery.js` and copy.js's `parseCopyQuery` read through `queryOf`, so a hand-typed '+' in `#/handoff`, `#/copy` and the other list routes stays '+' |
| A7: search minors | `5de41ed` and the fix wave | The first reads per ID run in parallel. IDs with no row to show (a truncated digest, items only in Selections, hidden runs) take no share and are refilled, for up to 4 walks |
| run.js draws the header after the body | `d711e37` (follow-up round) | Deliberate: a bare run URL resolves only once its tab body exists, so the page is never "ready" with an empty tab area. The header no longer draws ahead of the body |

Group B, identity (merged from `cleanup-b`, then `3bc5ada`):
- docs/identity.md says that a write refused for its shape or size is refused whatever name would sign it. The signer isn't resolved for it, and the size is measured on a probe signed with a one-character name. The not-writable and block checks run before signing, on a real write only, and `POST /api/handoff` keeps the same order. (`6709a83`, `3bc5ada`, plus a clause in the fix wave for a block that crosses the limit only by its signer's bytes.)
- PutTest: a write to a read-only member never resolves the signer (`6e84d71`).
- IdentityTest: a dangling git symlink on PATH is skipped (`b0b1919`).
- `MACOS_SHIM` is kept, with a comment saying why (`4282857`). It is needed on real macOS today, because Apple's `/usr/bin/git` stub opens the Command Line Tools install dialog, so nf-blocks asks `xcode-select -p` first.
- The second `requireWritable()` in `Put.write()` and in `HandoffWriter`, which could never refuse, is removed (`0abc630`).

Group C, personal data (`33e38db`, `84dbe14`):
- The user's real addresses became `ada.lovelace@example.org`, `ada.lovelace@example.com` or `ada@example.org` in tests, fixtures, the #/kit page, README, docs/identity.md, reports, plans and `live.sh`. Fake addresses on real domains (`lab.org`, `x.io`, `x.org`) became example.org.
- The design pack (docs/design/02-design-doc.md, blockprint.html, mockups/) is byte-identical to `bc29671`.
- Two non-reserved hosts stay on purpose: `pass@app.box.com` in InputRecorderTest, where the box.com redaction needs that host, and a GCP service-account address in RunManifestAuditTest's secret-scrubbing fixture.
- Derived test values moved with the address. For example, initials RS became AL.

Fix wave after the whole-branch review: `ca303b0`, `8e51fa6` and `c4075c7`. The sample search now looks up claims once per walk, not once per ID: the first rows of all IDs are read together, there is one batched hidden-run lookup, and a deeper re-read happens only for an ID whose first row is hidden (a test counts the lookups). `SEARCH_VALUE_ROUNDS` is module-local. docs/identity.md now names the one size edge that is refused after signing: a block that crosses 1 MiB only by the bytes the signer's name adds. The p2 report and p1b's live.sh no longer name an address for what the old screenshots show. The P1b plan's names match the new example address. Comments in README, run.js and the ADR are realigned. The scoped re-review approved it. Two cosmetic minors are parked: inner variables in `showing` shadow outer ones, and one long line at model.js:1488.

Parked:
- Committed PNG screenshots showed the old real address in the identity chip: p1b's build-01-kit, build-08-selection, build-identity, build-pipeline-all, build-pipeline-mine and build-selections-mine, and p2's build-09-storage-viewer, light and dark. They were re-shot as alice@example.org at the commits that first took them (`9152fdd`, `28d9a31`, `429b7e9`), over a roblab copy without the one run the real store gained afterwards; only the identity and claim times and ages differ. The mockup renders (`mock-*`) keep the design pack's address. The p3 shots show p3-accept@example.org, and the p4 shots the developer's example.org address.
- Git metadata: commits carry a Signed-off-by trailer with the author's address, and the author address itself. That is outside the round, and it will show in the PR's commit list.
- Commit `a249d00`'s message misdescribes the rule: one-item outputs whose key is a sample are per-sample. Fix the wording if the PR is squashed.
- A bare run URL reads up to `SAMPLES_ITEM_CAP` item blocks before it draws (ruling A1). Worth one timing on a large S3 store.
- A named tab can show a Samples tab whose join finds no samples, the accepted gap of the first-item check. The empty state covers it.
- An output of several items with one unkeyed item is run-level, as before. Revisit if a real pipeline emits a stray unkeyed item into a per-sample channel.
- Single-label fake hosts (`ben@x`, `g@x`, `ADMIN@IO.X`) aren't registrable domains and were left.

Verification at `c4075c7`.
- `cd web && npm test`: 1114 tests, 1114 pass, 0 fail. `npm run build` succeeds.
- `make test`: BUILD SUCCESSFUL. 2966 Gradle tests, 0 failures, 0 errors, 8 skipped (the opt-in roblab tests).
- `python3 -m unittest discover -s gate`: 379 tests, OK (1 skipped).
- Solo Gate (`gate/gate.sh`), Nextflow 26.04.6 in a scratch `NXF_HOME`, fresh `GATE_ROOT` each time, nothing else running in this worktree:
  - The first run at `c4075c7` exited 1. Browser A 6/6 and B 14/14 passed. Lineage gave 18 PASS, 2 FAIL (checks 0 and 4), 4 SKIP. Both failures are the `resumed` run exiting 1: the Gate's own diagnosis is "the plugin's upload() read a locked source on PublishDir's first copy, before refusing a target cold already wrote" (`CasFileSystemProvider.upload`, line 422). Nothing this round changed reaches that code: the commits since the green run at `84dbe14` touch only web/ and docs.
  - A second run at the same head passed in full: exit 0, lineage 20 PASS, 0 FAIL, 4 SKIP, browser A 6/6 with A2 unchanged (producers 8 req / 32,768 B, latest 4 req / 16,384 B, items 43 req), browser B 14/14. The run at `84dbe14` also passed in full.
  - So the `resumed` failure is intermittent. It is parked as a plugin follow-up: PublishDir's first copy of a mode-000 source goes through `upload()`.
- Nothing was written under `~/.nextflow`.

## Open questions
- The proposed `fromStore(samples:)` API needs a decision from whoever owns the plugin DSL (ADR 0001).
- On roblab the rail lists 17 outputs. A future config could fold lint_* and *_pseudo outputs under one "more" row.
- `meta.salmon_strand_analysis.inferred_strandedness` exists on only 1 of 5 roblab samples (WT_REP1). This looks like rnaseq's auto-strandedness ran on one sample. If that is a plugin capture gap rather than pipeline behaviour, it needs a look.
