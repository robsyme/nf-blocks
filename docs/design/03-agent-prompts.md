# nf-blocks explorer: implementation prompts (v0.3)

Each prompt is self-contained and meant for its own agent session in the nf-blocks repo. Give each session:

- `01-ux-review.md` and `02-design-doc.md`
- the `mockups/` folder: copy it into the repo, e.g. `docs/design/mockups/`. Each mockup is a standalone HTML file and is the **visual target**.

Shared preamble (paste at the top of every prompt):

> You're working on the nf-blocks Nextflow plugin's web explorer: a single-page app served by the plugin. It reads a store's snapshot index (`index/v3.sqlite`, read by HTTP range) plus blocks under `m/<member>/blocks/`, and writes claims and Selections to the store log. Read `02-design-doc.md` §1 for the concept model: facts vs claims, stores as places, `cas://` and `lid://` references. Keep these invariants: never show a filesystem path as an object's identity; every claim shows who/when; any action that keeps or frees data shows the bytes before commit. Match existing code style. Add tests for any index or query change. Don't change the on-disk block or log formats without flagging it.
>
> **Visual target:** `docs/design/mockups/`. Open `00-index.html` first, then `01-components.html`. Port the tokens and component classes from `mockups/src/kit.css`. Port the Seal algorithm from `mockups/src/kit.js` exactly (same CID → same glyph). Each mockup's top-of-file comment and dashed "Spec notes" panel list behaviour and acceptance criteria. Treat them as part of this prompt. Match layout, hierarchy, copy, number formatting and states. The spec panel and the Light/Dark/Spec-pins buttons are mockup chrome; don't ship them. When done, screenshot your build next to the mockup at 1440×900 in light and dark, and list any deliberate differences.

---

## P0: polish pass

**Mockups:** `01-components`, `02-pipelines`, `03-run-outputs`, `04-output-items`, `05-item`, `06-file`, `08-selection`.

> Start by porting the mockup kit (tokens, app shell, components, Seal) so later phases build on it.
> Implement the "Polish" and "Slows / confuses" fixes from `01-ux-review.md` that need no schema change:
> 1. Show sizes (sum of unique file blocks) on run rows, the output table, items grids and Selection pages. Format bytes as KB/MB/GB.
> 2. Show human names wherever a run or item is shown by CID (content page "run" column, Picked panel). Keep the short CID as secondary mono text.
> 3. Give file chips in items grids a label (extension or short role, e.g. `zip`, `html`, `log`) instead of identical icons. Rename the `+` / `{}` controls to "Pick" / "Open", or give them visible tooltips and aria-labels.
> 4. Format numbers: integers with thousands separators and no `.0`; floats to 3 significant figures.
> 5. Make column order deterministic: `id` first, then promoted fields, then the rest alphabetically. Don't reorder when filters change.
> 6. Clicking a sample ID should open that item. Move "filter to this value" into a small secondary affordance.
> 7. Collapse 0-item outputs into a single "7 empty outputs" row.
> 8. Fix collapsed `<details>` sections (Run details / Storage / Lineage) rendering as empty table rows on the run page.
> 9. `#/compose` should render the picked list in the main pane, not the Pipelines table.
> 10. Rewrite the health copy ("5 published files that are in no output") as an actionable sentence, shown once per pipeline when it repeats on every run.
> Acceptance: walk Pipelines → nf-core/rnaseq → latest run → preprocessed → filter `num_trimmed_reads >= 50000` → pick → compose. No CID-only labels, no raw floats, no blank rows.

## P1a: run Samples tab (cross-output join)

**Mockups:** `07-run-samples` (interactive: its threshold, tick and snippet behaviour is the spec), `12-search`.

> Add a **Samples** tab as the default on the run page when the run's items share a join key (`meta.id`; make the key configurable).
> - Build a per-run sample table: one row per key value, with columns from every output's `item_attr`. Each column records its source output and path.
> - Promoted columns: a small per-pipeline config (start with nf-core/rnaseq: `preprocessed.num_trimmed_reads` → "Reads (trimmed)", `meta.single_end` → "Layout", `meta.salmon_strand_analysis.inferred_strandedness` → "Strand"), plus per-output "has" chips. Everything else goes in a column picker grouped by output.
> - Filters on this tab apply to samples. Extend the URL `where` encoding with an output qualifier, e.g. `["preprocessed:num_trimmed_reads","int","50000",">="]`.
> - "Use N samples" opens the hand-off drawer: a checklist of outputs with item counts and sizes for the filtered samples. A merged output (one item covering many samples) gets a note naming how many of its samples are selected.
> - Update the right-rail snippet to express the cross-output filter. Propose the `fromStore` API extension in a short ADR. Don't change the plugin DSL in this PR.
> Acceptance: on `distraught_murdock`, threshold 50,000 gives WT_REP1 and RAP1_UNINDUCED_REP2; picking `quant_salmon` from the drawer yields exactly those two items.

## P1b: identity

**Mockups:** avatars and claim tags in `01-components` (including the anonymous state), the claim header in `08-selection`.

> Make `asserted_by` a real principal. Resolve it in order: Seqera Platform identity (if a token is configured) → `NXF_BLOCKS_USER` → `git config user.email` → `anonymous` (with a visible warning). Show initials avatar + email on every claim and Selection. Add a "Mine" filter to runs and Selections. Document the resolution order.

## P2: storage accounting

**Mockups:** `09-storage`, `10-run-storage`. The roblab numbers in them are real, so your implementation should reproduce them on the same store: 142 MB stored, 220 MB referenced, 1.55× dedup; freed on release: distraught_murdock 63.7 MB, sad_yonath 15.6 MB, sharp_magritte 0 B.

> Implement §3 of `02-design-doc.md`.
> - Index: add `block(cid, size, codec)` and `root(cid, kind, asserted_by, state)`. At snapshot build time, compute reachability from roots and write `store_stats` and `asserter_stats(asserted_by, reachable_bytes, exclusive_bytes, pinned_bytes, releasing_bytes)`. Bump the schema version, and keep the reader backward compatible for one version.
> - **Every number is per store.** Pins and cleanup are per store, so never sum bytes across stores, including in project views (one bar per store, side by side).
> - Fill `block.size` for raw blocks from item `Leaf {name, size, address}` records. They already carry sizes.
> - UI: a Storage view with one card per store: stored, referenced, dedup ratio, a stacked bar (pinned/held/releasing), and a separate thin "Yours" bar with `bytes · %`. No overlays or brackets. Breakdowns per store by pipeline, person (`Reachable from their roots`, `Freed on release (this store)`, `Pinned`) and file kind.
> - Bytes at decision points, scoped to the store: Release shows bytes freed *here* and what stays (pinned or shared here); Pin shows bytes kept and how much is already pinned here by others.
> - Test on a fixture with three asserters sharing blocks across two stores with different pins. Verify reachable and freed-on-release by hand, per store.

## P3: multiple stores + copy with provenance

**Mockups:** `11-copy`, plus presence dots and the store switcher in every page's shell.

> Implement §7 of `02-design-doc.md`.
> - Explorer: load several members from `members.json`. Add a store switcher with an "All stores" mode, and presence dots (filled = held, ring = referenced) wherever an address appears.
> - Copy: a plugin command `nf-blocks:copy <root> --to <member> --depth results|lineage|all --claims names,pins,projects,annotations --dry-run`. The closure always includes the producing RunCompletion and RunManifest (params, config, script) at every depth. This is a decided requirement, not an option. Add a regression test that no unredacted absolute path or credential-like string appears in a RunManifest. Claim blocks are copied verbatim. Write a `transfer` log entry in the destination (roots, source member, principal, depth, claim set). It's a block.
> - The explorer's Copy flow mirrors the command: dry-run plan (blocks in plan / already at destination / to transfer / bytes), then copy, then offer "Release here (kept in <dest>)".
> - Before enabling copy to shared stores, audit manifest and config blocks for local paths and secrets.
> Acceptance: copy a Selection from a local store to a MinIO-backed S3 store. The same `fromStore(selection: …)` call resolves from the destination with only store config changed, and the run's lineage page works there.

## P4: projects + hand-off templates

**Mockups:** `13-project`, `14-handoff-de` (interactive: its validation behaviour is the spec).

> Implement §4 and the hand-off template part of §5.
> - Project root record and membership claims (`attribute: project`, add/remove via supersede). Project-scoped annotation claims. Policies: default store, retention ("pin every Selection").
> - Cross-run sample identity: start with `meta.id` + pipeline, plus a "same sample as" claim for manual merge/split. Input CIDs are **not recorded today**: `RunCompletion.input_set` is null, the `consumer`/`missing` tables are empty, and `params.input` is redacted (see design doc §4). As a separate plugin task, populate `input_set` with `{role, cid, size}` for files a run reads, including ones that aren't stored. Then switch identity to input CIDs and suggest a project when runs share inputs.
> - UI: Projects first in the left nav; project page tabs Overview / Samples / Runs / Selections / Activity; "Add to project" on runs, samples, Selections and items.
> - Hand-off template for nf-core/differentialabundance: annotation grid for `condition`; validation (≥ 2 replicates per condition); generates a samplesheet *stored as a block* plus the `nextflow run` command with `--input cas://…`, `--matrix`, `--contrasts`. First check whether differentialabundance subsets the matrix to samplesheet samples, and pick the default accordingly.

---

## Design-system prompt (after visual feedback)

> Build the "nf-blocks" design system as tokens + components from the Blockprint trial v0.2 (link). Decided: Seqera product identity; the Seal is a core primitive. Components: Seal (deterministic CID glyph, square cells = raw blocks, round = records), AddressChip, FactTag, ClaimTag (luggage-tag shape: full border, punched hole at left, avatar, verb, value, age; no clip-path), PresenceDots, DurabilityBadge, StorageBar (single store only: stacked pinned/held/releasing plus a separate thin "Yours" bar), LineageThread, StoreCard. Tokens: Seqera base (ink `#201637`, brand green `#31C9AC`, deep green `#065647`, Inter, JetBrains Mono), plus a categorical store set and a semantic durability set that share no hues. Light and dark themes. Document each component with its rule from the Blockprint page.
