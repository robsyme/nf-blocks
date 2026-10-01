# Explorer layout B: browse, pick, use

Date: 2026-09-30. Status: design approved in conversation, spec awaiting review.
Mockups: `.superpowers/brainstorm/5517-1790814981/content/` (`layout.html`,
`middle-column.html`, `use-panel.html`, `nav-item-narrow.html`; gitignored,
local only).

## 1. Purpose

The explorer page (`nextflow plugin nf-blocks:explore`) is redesigned around
what its readers come to do, in this order:

1. Get outputs into a downstream workflow.
2. Browse what the store holds: pipelines, runs, outputs, items, files.
3. Check a run's lineage health, and manage storage (pin, release).

The reader is a bioinformatician who writes Nextflow but has never read
DESIGN.md. The page leads with run, output, sample and file; Store URIs,
CIDs, `lid://` references, member URLs and anomaly names are behind "Details"
disclosures or in the store status line.

Taking outputs downstream comes in three shapes, all supported:

- (a) a whole output of one run;
- (b) a subset of one output, described by Meta Map values;
- (c) items picked by hand, or from several outputs or runs.

A reader usually arrives by opening the explorer and finding their way
(pipeline, run, output). Search by sample or run name is the expected next
request; this design reserves its place and builds nothing for it.

## 2. What is wrong today

Observed on run `high_jang` (nf-core/rnaseq, 24 outputs):

- The header is store plumbing (member URL, "snapshot read by range",
  "0 runs newer than the snapshot"); the title links nowhere; there is no
  home page.
- The run page opens with two raw identifiers and an anomalies line in store
  terms ("declined 439, never published 5, unjoined 5") with no indication of
  which counts matter.
- "Release content" sits above the outputs.
- Outputs are 24 near-identical two-line snippets that say nothing about
  their content: no item count, file types or keys.
- An items row repeats its label ("multiqc_report · multiqc_report") and its
  `id` pill, because the item's metadata holds `id` at two paths.
- Filtering starts from an empty key / type / value form.

## 3. Layout

Three regions, built once by the app shell; the router fills only the middle.

```
+------------------+-------------------------------+----------------------+
| Left column      | Middle column                 | Use in a workflow    |
| pipelines > runs | home | run | items | item |    | (the panel, §6)      |
| Selections       | selection | selections        |                      |
| store status     |                               |                      |
+------------------+-------------------------------+----------------------+
```

The left column and the panel stay mounted across routes, so picks and
scroll position in the left column survive navigation.

### 3.1 Narrow screens

- 900 to 1200 px wide: the left column collapses to a ☰ drawer.
- Under 900 px: the panel also leaves the screen and becomes a "Use · N"
  button in the top bar (N = items the panel would use) that opens it as a
  sheet. The middle column takes the full width.

No horizontal page scroll at any width; wide tables scroll inside their own
container.

## 4. Left column

From top to bottom:

1. A search box, disabled, placeholder "Search samples, runs… (later)". It
   reserves the space; §11 says what it will need.
2. **Pipelines**, A–Z, each with its run count. Expanding one lists its runs
   newest first, with a dot for status (green: succeeded and not possibly
   incomplete; red: failed or possibly incomplete) and the finish date. At
   most 10 runs, then "all runs…", which opens `#/pipeline/<name>`. The
   current run is highlighted. Runs hidden by a delete Claim are not listed,
   as today.
3. **Selections**: the five most recent by name, then "all selections…"
   (`#/selections`).
4. **Store status**, small and muted: the member name and whether it is
   writable, and whether the page is up to date. Everything the header shows
   today (`#snapshot-mode`, `#stale`, the member list and its links) moves
   here unchanged in behaviour; the stale notice and its command keep their
   current thresholds.

## 5. Middle column views

Every view starts with a breadcrumb (`nf-core/rnaseq / high_jang / markdup /
WT_REP1`), each segment a link.

### 5.1 Home, `#/`

Each pipeline with its run count, the date of its latest run, and a link to
its latest good run (query `latestSuccessfulRun`). An empty store shows how
to publish into it (the existing empty-state text).

### 5.2 Pipeline, `#/pipeline/<name>`

Today's paged run table, restyled; columns run, status, finished, and a
plain-words health summary in place of the raw anomalies column.

### 5.3 Run overview, `#/run/<cid>`

- Title: the run name. One line under it: status in words with its dot, the
  finish time in the reader's locale, and "latest good run of <pipeline>"
  when it is.
- **Outputs**, A–Z, one table row each: name, item count, the keys that tell
  its items apart (§5.4, from the loaded previews; blank until loaded), and
  file suffix chips (at most two, then "+N"). Clicking a row opens
  `#/items/<run>/<output>`. When a collection has an Output Index File, the
  row carries its download link, as today.
- Folded sections at the bottom, closed by default:
  - **Run details**: pipeline revision, `lid://` reference, run hash,
    RunCompletion and RunManifest addresses, the configuration text.
  - **Storage**: pinned or not, Pin (with its reason box), Release files, and
    the existing explanation of what releasing does. Unchanged behaviour.
  - **Lineage check**: one line per non-zero anomaly, in the words of §8.
    The section's summary line shows "worth a look" when `unjoined` or
    `unresolvable` or `unaddressed` is non-zero, else "nothing to check".

The run page shows no snippets of its own; the panel (§6) shows the call for
whatever output the reader opens.

### 5.4 Items, `#/items/<run>/<output>`

A table of the output's items (query 3 under the filter chips), with a
checkbox per row and a header checkbox for all visible rows.

Column rules, computed by `table.js` from the previews loaded so far:

- Paths are the index's `item_attr` paths (`pairsOf`). Two paths whose values
  are equal on every loaded item are shown once, under the shorter path
  (ties: the first in sort order). This removes the doubled `id`.
- A path whose value is the same on every loaded item is not a column; it
  goes in one line above the table, "Same for every item: key value · …".
- The remaining paths are columns, ordered as `labelPaths` orders them
  (strings first, then most distinct values, then by path). At most six; the
  rest are reachable from the item page.
- A list value shows as its values joined by ", " and is not clickable.
- Files: the item's file names with their longest shared prefix (with all
  other items of the output, cut at a `.` boundary) removed, as suffix chips,
  at most three, then "+N".

Filtering:

- Every scalar value in the table and in the "same for every item" line is a
  link that adds a `path = value` filter chip (the existing `filterHref`,
  which already carries the type).
- Chips show above the table, each with ✕. Several chips mean all must hold,
  as today.
- "+ filter" opens today's key / type / value form, for values not on screen
  and for typed numbers and nulls.
- Rows hidden by the filter are counted under the table ("2 hidden by the
  filter").

Clicking a row (outside its checkbox and values) opens the item.

### 5.5 Item, `#/item/…`

Today's item page, rearranged:

- Title: the item's label (`labelText`). The Meta Map as chips, at most
  eight, then "+N more".
- Files: name, size (human units), and Download, or View for a text file the
  existing preview rules allow.
- "Add to picked" (§6.3).
- **Produced by**: the run that produced it, and the other runs whose items
  hold the same files ("same files also in: …"), from query 1 as today.
  The producing task's name is optional: add it only if it comes from the
  `nf_record` table without a request per file (§11).
- Folded **Details**: item address, leaf addresses, the raw Meta Map.

### 5.6 Selections, `#/selections` and `#/selection/<cid>`

Today's views and behaviour (rename, delete, undo, deleted list,
held-elsewhere and read-only notes, samplesheet CSV and JSON), restyled into
the middle column. The Selection view's snippet moves into the panel
(state `saved`, §6.1). `#/compose` is retired: the picked list lives in the
panel (§6.3), and `#/compose` redirects to `#/` with the panel open.

## 6. The panel: Use in a workflow

### 6.1 States

The panel has one state at a time, computed by `panel.js`, a pure function
of: the route, the active filter chips, the checked rows of the current
items view, and the picked list.

| state      | when                                                                                   | shows |
|------------|----------------------------------------------------------------------------------------|-------|
| `none`     | not on an items, item or Selection view, and the picked list is empty                   | "Open an output to use it in a workflow." |
| `whole`    | on an items or item view, no filter chips, picked list empty                            | "All N items of <output>", the run switch, the `run:`/`output:` call |
| `filtered` | on an items view, one or more chips, picked list empty                                  | "M of N items, where …", the run switch, the call with `where:` |
| `picked`   | not on a Selection view, and the picked list is non-empty                               | the picked list grouped by output and run, a name box, Save as Selection, Clear |
| `saved`    | on a Selection view                                                                     | the Selection's name and count, the `selection:` call, samplesheet CSV/JSON, Undo |

Rows are checked by default. Unchecking rows does not change the state: the
panel stays in `whole` or `filtered`, keeps the call for the whole (filtered)
output, and adds "M of N checked" with a "Pick these M" button that moves
them into the picked list. The call on screen therefore always describes
exactly what it returns. While the picked list is non-empty it takes the
panel (`picked`) on every view but a Selection's; Clear or a save empties it.

Every state with a call has Copy and the untyped | typed switch
(`snippets.js`, one choice for the page, remembered as today).

### 6.2 The call

`snippets.js` gains:

- `where:` from the chips, as a Groovy map literal in chip order. A key that
  is not a Groovy identifier is single-quoted (`['meta.id': 'WT_REP1']`).
  Values by type: string quoted with the existing `quote`, int and float as
  their index text, bool `true`/`false`, null `null`.
- The run switch, "this run | latest good run", shown only when the run's
  pipeline has a latest good run. Default **this run** (`run: 'lid://…'`).
  "latest good run" gives `run: 'latest', pipeline: '<name>'` and adds the
  line "latest may match different items after the next run". The choice is
  per page view and not remembered.

`fromStore(where:)` runs the same `item_attr` path / type / value predicate
as the page's filter (`Index.items`, `Index.groovy:997`), so the `filtered`
call returns the rows on screen.

### 6.3 The picked list

The picked list is today's Tray (`tray.js`), unchanged in storage (per tab,
`sessionStorage`, survives switching member, warns when it could not be
saved). The UI calls it "picked". Items are added by:

- "Pick these M" on an items view (the checked rows);
- "Pick all N" on an items view (today's "Add all N to the tray");
- "Add to picked" on an item page.

The list shows each source (output · run · count) with ✕ to remove that
group, and a total. Save as Selection runs today's save flow (dry run,
exists here / held elsewhere, partial failure banner); on success the page
opens the new Selection (`saved`) and the picked list empties.

On a member that is not writable the Save button is replaced by today's
`[data-unavailable]` note and its link to the same page in the writable
member.

## 7. Visual style

- Light and dark themes from CSS custom properties, following
  `prefers-color-scheme`.
- System UI font for text, `ui-monospace` for code and addresses.
- One accent colour for actions and links; green, amber and red only for
  status and health.
- The styles live in one stylesheet built by `build.mjs` into the page;
  no CSS framework, no new runtime dependency.

## 8. Words

| store term                 | page says                                                       | marked |
|----------------------------|-----------------------------------------------------------------|--------|
| succeeded                  | Succeeded                                                       | |
| failed / possibly_incomplete | Failed / Failed, may be incomplete                            | |
| `declined` N               | N empty optional slots (the pipeline passed no file)            | expected |
| `never_published` N        | N referenced files not stored here, such as input files         | expected |
| `unjoined` N               | N published files that are in no output                         | worth a look |
| `unresolvable` N           | N links whose target could not be found                          | worth a look |
| `unaddressed` N            | N files with no stored content                                   | worth a look |
| Tray                       | Picked                                                           | |
| Collection                 | Output                                                           | |
| member                     | store                                                            | |
| CID, Store URI, lid        | only inside Details, labelled "address" / "run reference"       | |

DESIGN.md §6 is the source for each anomaly's meaning; a wording change that
alters meaning needs DESIGN.md first.

## 9. Code structure

All in `web/src`, vanilla JS with the `h()` builder, as today.

| file | change |
|------|--------|
| `app.js` | the three-region shell; the router renders into the middle region only; panel and left column subscribe to route and pick changes |
| `views.js` | split into `views/home.js`, `views/pipeline.js`, `views/run.js`, `views/items.js`, `views/item.js`, `views/selection.js` (with `selections`); shared helpers stay in `html.js` |
| `nav.js` (new) | the left column |
| `panel.js` (new) | `panelState(input) → {state, …}` (pure) and its renderer |
| `table.js` (new) | column, constant-line, de-duplication and filename-prefix rules of §5.4 (pure) |
| `words.js` (new) | §8's table and the health summary (pure) |
| `snippets.js` | `where:`, the run switch, key quoting |
| `tray.js` | unchanged storage; callers renamed in the UI only |
| `index.html` + a stylesheet | the theme of §7; inline styles removed |

No change to `model.js` queries, `queries.json`, the index schema, the
plugin's Groovy code or `fromStore`.

## 10. The page contract and the Gate

The Gate drives the page through the `data-*` contract in DESIGN.md §15–16
(38 attributes), not through layout. Rules:

- Every existing attribute stays on the element that means the same thing.
  `body[data-state]`, `[data-render]`, `[data-route]`, `#snapshot-mode`,
  `#stale`, `[data-member]` move with the store status into the left column.
- `[data-snippet]` and `[data-snippet-mode]` are in the panel. There is at
  most one `[data-snippet="untyped"]` on a page, so tier B's
  `querySelector` reads the panel's call. A tier B step that read a run
  page's snippet opens `#/items/<run>/<output>` first.
- `[data-pick]` and `[data-pick-all]` keep their attributes on "Add to
  picked" and "Pick all N".
- `#/compose`'s `[data-tray-entry]`, `#compose-name` and `#compose-copy`
  move to the panel's picked list, name box and save button, keeping those
  ids and attributes; tier B navigates to any items view instead of
  `#/compose`.
- New: `[data-panel-state]` on the panel (`none|whole|filtered|picked|saved`);
  `[data-where]` on the call's `<code>`, holding the chips as JSON
  `[[path, type, value], …]`; `[data-run-mode]` (`this|latest`) on the run
  switch; `[data-columns]` on the items table, the column paths as JSON.
- DESIGN.md §16 gains a "Layout B" entry listing these moves and the new
  attributes, in the same commit as the code that makes them.

New Gate check (tier B): on the Gate's producer run, filter one output by one
value, copy the `filtered` call, run it in the consumer, and assert the
consumer receives exactly the item CIDs the page's table showed
(`[data-item-result]`).

## 11. Later, not built

- **Search.** A value search under a known key is one indexed lookup today
  (`item_attr(path, type, value)`). Matching a value under any key needs an
  index led by `value` (an index schema bump) or a scan; decide when search
  is built.
- **Producing task name.** Needs a check that `nf_record` (`task_run`,
  `labels_json`) yields the process name per item without a block fetch per
  file. If it does, add it to §5.5 then.
- **Output grouping by topic.** Not recorded in the store; A–Z for now.
- **Remembering the run switch.** Per view for now.

## 12. Testing

- Unit (`node --test`): `panel.js` over every row of §6.1's table and the
  "M of N checked" case; `table.js` against a fixture shaped like rnaseq's
  `markdup` (two `id` paths, sparse `inferred_strandedness`, constant
  `has_genome_bam`, six files per item with a shared prefix); `words.js`
  over every anomaly; `snippets.js` for `where:` types, key quoting and the
  run switch.
- `views.test.mjs` split to follow `views/`; the existing assertions move
  with their view.
- `make gate` green at every commit; tier A and tier B green, with the new
  tier B check.
- Manual: one pass on `high_jang` in a desktop window and one under 900 px,
  covering shapes (a), (b) and (c) end to end, with the steps written by
  visible label.

## 13. Success

On `high_jang`, a reader who has never seen the explorer can, without
reading DESIGN.md:

- copy the call for all of `quant_salmon`;
- copy a call for the `markdup` items with `single_end = false`, and get
  exactly those items in a downstream run;
- pick `markdup` items from `high_jang` and `happy_rubens`, save them as a
  Selection, and copy its call;
- tell from the run page whether its lineage needs attention.
