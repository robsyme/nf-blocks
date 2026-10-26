# nf-blocks explorer — redesign design doc (v0.2)

Companion to `01-ux-review.md`. Visual language: the published "nf-blocks Blockprint" design-system trial (v0.2).

**Decisions from review round 1:** keep the seal · keep the fact/claim split · all storage numbers are per store (no "only yours" headline) · run config always travels with copies · Seqera product identity · input CIDs aren't recorded today, so cross-run sample identity starts from `meta.id` (§4).

---

## 1. The block store, from first principles

The explorer's job is to make one idea feel obvious:

> **Data has an address, not a location.** An address is computed from the bytes. A store is just a place that holds some addresses. Links built from addresses can't break when data moves, because they never named a place.

What the plugin already has, as I read it from the index schema (`v3.sqlite`) and the UI:

| Layer | What it is | Mutable? | Examples in the index |
|---|---|---|---|
| **Block** | Bytes + CID. Raw (`bafk…`) for files, structured (`bafy…`, dag-cbor) for records | Never | `blocks/xq/bafyrei…` |
| **Record** | Structured blocks that link other blocks: RunManifest, RunCompletion, Collection (one per output), Item (meta + file links), Selection | Never | `run`, `collection`, `collection_item`, `item_attr`, `selection_child` |
| **Claim** | A block that *states something about* another CID: name, pin, release, delete. Claims supersede earlier claims and can conflict | Append-only; the "current" view changes | `claim`, `claim_current`, `claim_supersedes` |
| **Log** | Per-store append-only list of what was written (run, selection, claim) | Append-only | `log/`, `log_entry` |
| **Store** (member) | A place holding blocks + log + snapshot index. Local dir today; S3 next | — | `members.json` → `roblab` |
| **Lineage** | Which completion produced or consumed which content | Derived | `producer`, `consumer`, `missing` |
| **Reference** | `cas://<cid>/<filename>` for content, `lid://<hash>` for a run | Never | samplesheet export, Run details |

There are two kinds of thing, and the UI should keep them visually separate:

- **Facts:** immutable, addressed, identical in every store. Runs, outputs, items, files, Selections.
- **Claims:** human statements about facts. Names, pins, releases, project membership, sample annotations. Always attributed (who, when, why) and always traceable.

Everything the user can *change* is a claim. Everything they can *cite* is a fact.

### Design principles

1. **Address first, name second.** Every fact shows a recognisable address mark (the *seal*, see §6) beside its human name. The name is a claim and can change. The seal can't.
2. **Location is a property, not an identity.** Never show a path as *the* thing. Show "present in: ● roblab ● org-s3 ○ scratch".
3. **Every claim is signed.** Who, when, and (for pins) why. "anonymous" is a bug.
4. **Speak samples, keep storage one click down.** Default views are samples and hand-offs. Collections and blocks stay available for people who want them.
5. **Bytes at the point of decision.** Pin, release, copy and select all show the bytes they affect *before* you commit.
6. **Provenance travels with the data.** Copying something copies the records that explain it.

---

## 2. Information architecture

```
Top bar:   [store switcher ● roblab ▾]   [ search samples, runs, files, CIDs ]   [Picked · 2]  [RS]
Left nav:  Projects  ·  Pipelines  ·  Selections  ·  Storage
Right rail: Hand-off drawer (picked items → Selection → snippet / samplesheet / copy-to-store)
```

| View | Primary question it answers | Notes |
|---|---|---|
| **Storage** (new) | How full is each store, how much is pinned, how much is mine? | §3 |
| **Project** (new) | What belongs to this study, who's working on it, what's its state? | §4 |
| **Pipeline** | Which runs exist, which is the latest good one? | Mostly as today, plus sizes |
| **Run** → tabs **Samples** (new, default) · **Outputs** · **Provenance** · **Storage** | What did this run produce, for which samples? | §5 |
| **Output items** | Today's items grid, with labelled file chips and stable columns | |
| **Item / File** | What is this, where else does it live, who made it? | Add preview for TSV/CSV/HTML/log |
| **Selection** | What exactly is in this frozen list, and how do I use it? | Add size, owner, presence, "Copy to…" |
| **Copy to store** (new flow) | What will move, what's already there, what provenance comes along? | §7 |

The search box should work as soon as possible. It's the cheapest fix for most navigation problems in the review. Sample IDs, run names, file names, and CID prefixes are all indexed already.

---

## 3. Storage accounting

### Definitions (per store)

| Measure | Definition | Why it matters |
|---|---|---|
| **Stored** (physical) | Σ size of unique blocks in the store | What the bucket bill sees |
| **Referenced** (logical) | Σ size over every reference from every record | What it would cost without dedup; Stored ÷ Referenced = dedup saving |
| **Pinned** | Unique bytes reachable from current `pin` claims | Will never be swept |
| **Held by live runs** | Unique bytes reachable from un-released runs or Selections, not pinned | Safe for now, can be released |
| **Releasable** | Released, past or inside grace period, not reachable from any root | Will be reclaimed at next sweep (show date) |
| **Missing** | Referenced but not present here (e.g. inputs) | Show where it *is* present if another store has it |
| **Yours** | Unique bytes *in this store* reachable from roots you asserted (your runs, pins, Selections) | "How much of this store am I using?" |
| **Freed on release** | Bytes in this store that releasing a given root (or all of yours) would reclaim, after other people's roots and pins are taken into account | Shown only at the Release action, never as a standing number |

**Decision (review round 1): every storage number is per store.** Pins and cleanup happen independently in each store, so a pin in `org-s3` doesn't protect the same block in `roblab`. No view adds bytes across stores, including project views, which show one bar per store side by side. The earlier "only yours" figure is dropped from bars and summaries. Its useful version, "what would this release free *here*", appears only where you'd act on it.

### UI

- **Store card** (Storage view, store switcher popover): one stacked bar per store, with segments Pinned · Held · Releasing. Below it is a separate thin **Yours** bar labelled `212 GB · 12%` (no overlay or bracket, which was unclear). Text: `1.82 TB stored · 5.6 TB referenced · 3.1× dedup`.
- **Project storage:** one bar per store the project touches, never summed, with the caption "Each store keeps its own pins and runs its own cleanup."
- **Breakdown tables** below, per store: by pipeline, by project, by person (`Reachable from their roots`, `Freed on release (this store)`, `Pinned`), and by output type.
- **At decision points:** Release ("frees 3.4 GB *in roblab* after 7-day grace; 1.1 GB stays because it's pinned or shared here"), Pin ("keeps 212 MB in org-s3; 180 MB already pinned here by Ana"), Copy ("transfers 1.2 GB; 3.8 GB already at destination").

### Index work needed

- `block(cid, size, codec)`. Sizes exist for files (shown on item pages) but not as a queryable table.
- `root(cid, kind, asserted_by, state)` for runs, Selections, pins, projects.
- Snapshot-time aggregation, per store: for each block, the set of asserters whose roots reach it. Then `store_stats` and `asserter_stats(asserted_by, reachable_bytes, exclusive_bytes, pinned_bytes)`. `exclusive_bytes` backs "Freed on release" and isn't a headline number. Full reachability is precomputed when the snapshot is built, not in the browser.
- File sizes are already in the item records: each `Leaf` carries `{name, size, address}`, so `block.size` for raw blocks can be filled from items without re-reading the bytes.
- **Identity:** `asserted_by` must be a stable principal. Proposal, in order of preference: Seqera Platform identity if a token is present → `NXF_BLOCKS_USER` → `git config user.email`. Show initials avatar + email on hover. Claims could be signed later.

---

## 4. Projects

### Why

Pipelines → runs is how the *plugin* is organised. People organise work by study, grant, collaborator or paper. Selections are deliberately frozen, which suits citing and handing off. They don't work as a folder you keep adding to.

### Model

- A **Project** is a small root record (`name`, `description`, `created_by`, `created_at`) with its own CID. Renaming is a name claim, as for Selections.
- **Membership is claims.** `{subject: <run|selection|sample|item>, attribute: "project", value: <project cid>, verb: "add"}`. Removing supersedes. The current view is `claim_current`, and the history is the audit trail.
- **Cross-store by construction.** A project lists CIDs, so it can include things held in any store. The explorer resolves each CID against the stores it knows and shows presence.
- **Project-scoped annotations.** `condition = treated` is a claim scoped to *this* project, because the same sample can be a control in one study design and a case in another.
- **Policies** (claims on the project):
  - *default store*: where "Copy to…" points by default
  - *retention*: e.g. "pin every Selection in this project"
  - *people*: who can add to it (advisory at first)

### Sample identity

A sample is the join key that makes everything else work.

1. **Within a run:** `meta.id` across all outputs of one completion. Cheap, available now.
2. **Across runs, today:** `meta.id` plus pipeline, with a "same sample as" claim to merge or split by hand.
3. **Across runs, target:** the CIDs of the sample's **input reads**. Two runs that consumed the same FASTQ bytes are processing the same sample, whatever `meta.id` says. Use it to suggest projects ("these 3 runs share 12 input files") and to show "this sample was also processed in…".

**Checked against `roblab` (2026-10-24): input CIDs are not recorded yet.**

| Where | What's there |
|---|---|
| `RunCompletion.input_set` | `null` on all 4 runs. The schema has the slot (`link → Any`, nullable) but nothing fills it |
| `consumer`, `missing` tables | 0 rows |
| `RunManifest.params.input` | `"[redacted-location]"`: the samplesheet path is redacted and has no CID |
| `RunCompletion.providers["head-node"]` | 376 CIDs: content the head node hashed. These are published outputs, not inputs |
| Item `reads` field (`preprocessed`) | `[null, null]`: FASTQs not published, so no address |
| `anomalies` | `unjoined: 5, never_published: 5`. This is probably where "5 referenced files not stored here" comes from |

So input-based identity needs plugin work: populate `input_set` with `{role, cid, size}` for files the run read, even when they aren't stored. Experiments that would show the current behaviour (Rob offered to run them):

1. A run whose input comes from `channel.fromStore(selection: 'bafyreib2r…id5hce')` (the `wigshop` bigWigs). Does `input_set` or `consumer` get populated when inputs are already addressed?
2. nf-core/rnaseq with a samplesheet whose `fastq_1`/`fastq_2` are `cas://` references, if the plugin resolves them.
3. The same rnaseq test run twice, unchanged. Do any records link the two runs beyond `pipeline` + `meta.id`?
4. A run with large local FASTQ inputs, timed with and without input hashing, to size the cost of hashing inputs that aren't stored.

### UI

- Left nav **Projects** first. Project page tabs: **Overview** (people, storage card, recent activity from the log), **Samples** (cross-run, annotations editable), **Runs**, **Selections**, **Activity**.
- "Add to project" is available on run, sample, Selection and item.
- An unassigned bucket ("Not in a project") keeps the old Pipelines view reachable.

### Projects vs Selections

| | Selection | Project |
|---|---|---|
| Mutable | No: a fixed list of exact CIDs | Yes: membership is claims |
| Purpose | Cite, hand off, reproduce | Organise, browse, collaborate |
| Address | Its CID *is* the content | Stable root CID; contents change |
| Pin semantics | Pin the Selection | Retention policy pins its Selections |

---

## 5. The sample-first run view (fixes the task)

**Run → Samples tab** (default when a run has a `meta.id` join key):

- One row per sample. Columns are **promoted metrics** pulled from wherever they live: reads (raw / trimmed), % mapped, strandedness, single/paired, plus per-output "has" chips (BAM, quant, bigWig). A column picker exposes the long tail (`dupradar.meta.…`) grouped by output → tool.
- Each cell carries its source (`preprocessed · num_trimmed_reads`). Hover shows it, and the filter chip shows it.
- **Filters operate on the sample table** and so apply across outputs. `reads_trimmed ≥ 50,000` → 2 of 5 samples.
- **"Use 2 samples →"** opens the hand-off drawer:
  1. Choose outputs to include, each with size: ☑ `quant_salmon` (2 items, 4.1 MB) ☐ `markdup` (2 items, 1.2 GB) …
  2. Merged outputs get special handling. `quant_merged` → "contains 5 samples; you selected 2". Options: *pass whole matrix + subset samplesheet* (default when the downstream pipeline subsets by samplesheet), or *subset in workflow* (snippet).
  3. Choose a **hand-off template**. `nf-core/differentialabundance` knows its inputs: `--input` samplesheet (`sample`, `condition`, …), `--matrix`, `--contrasts`. The drawer shows an annotation grid for the missing `condition` column. Values are saved as project-scoped claims.
  4. **Save** → a Selection (frozen) + a generated samplesheet stored *as a block* (so `--input cas://…` is itself an address) + the run command / `fromStore` snippet.

The filter → snippet bridge stays, extended across outputs:

```groovy
include { fromStore } from 'plugin/nf-blocks'
samples = channel.fromStore(run: 'latest:nf-core/rnaseq', samples: [where: ['reads_trimmed >=': 50000]])
quant   = samples.output('quant_salmon')
```

(API shape illustrative. It needs a decision from whoever owns the plugin DSL.)

**To check:** whether differentialabundance subsets the matrix to the samplesheet's samples during validation. If it doesn't, "whole matrix + subset samplesheet" isn't safe, and the template must default to a subset step.

---

## 6. Visual language: summary

Full trial in the "nf-blocks — Blockprint" page. Core ideas:

- **Seal:** a small deterministic glyph derived from the CID's bytes (a 4×4 cell pattern inside a rounded square). Same bytes → same seal, in every store, every screen, every export. It's how a human recognises "that's the same file" without reading a hash. It always sits beside the short CID in mono (`bafk…gz3oym`).
- **Facts are solid, claims are tagged.** Facts get a hairline frame and the seal. Claims get a luggage-tag shape: full border, warm fill, a punched hole at the left, then avatar, verb, value and age (`pinned · RS · 2d · "fig 3"`). (v0.1 used a clip-path notch, which cut off the left border.)
- **Presence dots:** one colour per store, filled = present, ring = referenced but absent. Placed wherever an address appears.
- **Durability states** (semantic colour, separate from store colour): Pinned · Held · Releasing (with date) · Missing.
- **Lineage thread:** a thin connector line in the left gutter linking produced-by / consumed-by.
- **Decision: Seqera product identity.** Palette and type come from the Seqera system (dark brand ink, brand green as the single accent, Inter + JetBrains Mono). Store colours are a separate categorical set chosen for lightness contrast.
- **Decision: the seal stays** as a core primitive.

---

## 7. Copy to store, keeping provenance

### User story

> Rob's rnaseq run is in `roblab` (his laptop). The lab wants the counts and QC in `org-s3` (shared bucket) so Ana can run DE next week, and Rob wants to free his disk.

### Flow (explorer)

1. **Start from a root:** Selection, run, or project → **Copy to…** → pick destination store (only stores you can write; read-only ones greyed with reason).
2. **Choose depth:**
   - *Results:* the root, its items, their files, and the **producing run's records** (RunCompletion, RunManifest with its params, config and script). Small, and **always included at every depth (decision, review round 1)**: without them provenance is lost.
   - *Results + lineage:* also the upstream outputs those results were derived from (e.g. BAMs behind counts).
   - *Everything stored:* also inputs, if this store holds them.
3. **Choose claims to carry:** names ✓, pins ✓, project membership ✓, annotations ✓. Claims are blocks, so they're copied **verbatim**. Original authorship and timestamps survive unchanged, with no re-assertion.
4. **Plan** (dry run, always shown):
   `1,284 blocks · 812 already in org-s3 · 472 to transfer · 1.2 GB · 31 inputs referenced but not stored (stay as references)`
5. **Copy.** The destination verifies every block against its CID on write (inherent to content addressing). The destination log gets a **transfer** entry: `{roots, from: roblab, by: rob.syme@seqera.io, at, depth, claim set}`. The transfer entry is itself a block, so "how did this get here?" has an addressable answer.
6. **After:** presence dots update. The source offers **Release here (kept in org-s3)** and shows the bytes that frees.

### What stays valid

- Every `cas://` and `lid://` reference: they name content, not a place.
- Every `fromStore(selection: 'bafy…')` call: only the store *configuration* changes, not pipeline code.
- Lineage: the run records travelled with the results.

### CLI parity

The explorer should generate the equivalent command, as it does `fromStore` snippets today:

```
nextflow plugin nf-blocks:copy selection:bafyreib2r…id5hce \
    --to org-s3 --depth results --claims names,pins,projects --dry-run
```

### Open questions

- Pushing to a store you can read but not write: request flow, or "propose" by writing to a staging prefix?
- Do pins carry by default? Pins express *your* intent in *your* store. In a shared store they may need re-confirming.
- Config always travels (decided). The existing `[redacted-location]` handling covers paths. A regression test should check that no unredacted absolute path or credential-like string appears in a RunManifest.

---

## 8. Phasing

| Phase | Scope | Unblocks |
|---|---|---|
| **P0: polish** | Sizes on runs/outputs/Selections; names over CIDs everywhere; labelled file chips; number formatting; stable column order; fix collapsed-section rendering; fix `#/compose` main pane; de-emphasise empty outputs | Readability |
| **P1: samples + identity** | Run → Samples tab with cross-output filters; `asserted_by` identity; global search | The rnaseq → DE task |
| **P2: storage** | `block` sizes + snapshot aggregation; Storage view; bytes at decision points | "How much is mine / pinned?" |
| **P3: multi-store + copy** | Multiple members in one explorer, presence dots, Copy-to flow, transfer log entries | Sharing to org S3 |
| **P4: projects + hand-off templates** | Project roots and membership claims; project-scoped annotations; differentialabundance template | Organising and repeatable hand-off |
