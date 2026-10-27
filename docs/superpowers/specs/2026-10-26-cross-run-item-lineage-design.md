# Cross-run item lineage

Date: 2026-10-26. Status: draft for review.
Changes DESIGN.md §6 (IPLD Schema), §12 (Index), §13 (`fromStore`), §19 and
§22 (sweep), §21 (copy closure) and §22 (InputSet), and the item page of the
explorer (`web/src/views/item.js`).

## 1. Why

A second pipeline can read a first pipeline's outputs with `fromStore`. The
test case is `scratch-consumer-workflow`: it reads the Selection
`bafyreih4xlqr6dec4bqqilk7kv6brh6ux5ggkod3cot4znfr7k6lhrbpf4`, four
`PersonWithData` items from `scratch-workflow`'s `people` output (run
`infallible_spence`), and publishes one `intros` item per person and one
`circle` item from all four.

On the explorer's page for the `intros · Dev` item, the Lineage panel shows
one node, the item itself. The user wants to follow that item back to what
it was made from: the file `Dev_Patel.dat`, the `people · Dev` item that
held it, the Selection it was picked through, and the `scratch-workflow` run
that made it. From there, the upstream item's own page continues the chain.

Two pieces of information are missing from the store today:

1. Which inputs made which item. The run's InputSet lists the four
   datafiles the run read, but not which task, and so which output item,
   used which file. Only Nextflow's TaskRun records under `nf/` know, and
   those are not blocks: the explorer never reads them and copies never
   carry them.
2. Which upstream item an input came from. An input is recorded by its
   content address only. `Dev_Patel.dat` has the same bytes in three
   `scratch-workflow` runs, so matching by address gives three candidates
   for one true origin. `fromStore` knew the exact item when it emitted the
   record, and drops that knowledge.

## 2. Goals

- An InputSet input that a task received through `fromStore` records the
  upstream OutputItem it came from, the OutputCollection it was reached
  through, and the Selection named in the `fromStore` call, when there was
  one.
- Each run records, for each OutputItem it published, which of the run's
  inputs that item's files were made from, following task inputs back
  through the run's own intermediate tasks to the run's inputs.
- The item page's Lineage panel shows, above the item, the inputs it was
  made from and, for each input that came through `fromStore`, the
  upstream item with its run and the Selection, each a link. Opening the
  upstream item's page shows the next step back.
- The recorded facts survive a copy between members, so a copied consumer
  run shows the same lineage.

## 3. Non-goals

- Lineage between items of the same run (an item made from another output
  of the same run). The walk passes through such tasks to the run's inputs
  but records only the run's inputs. `TaskLineage` keeps covering same-run
  lineage for copy depth.
- Backfilling runs written before this change. They have no record and show
  the Lineage panel as today. Rebuilding the index does not invent one.
- A multi-hop graph on one page. Each item page shows one step back; the
  links do the rest.
- Copy depth following lineage into upstream runs. `lineage` depth stays
  within a run (§21 "Task rule").
- Inputs a task read without `fromStore` (a `cas://` path typed into a
  param, a `lid://`, a local file) get no upstream item. They still show as
  inputs, as now.

## 4. Anti-goals

- Task hashes must not change. A consumer run must resume across this
  change, and two items holding identical bytes must still give tasks the
  same hash. This rules out putting the item into the emitted `cas://` path.
- A consumer run must not keep upstream content alive. Sweep must not
  follow any upstream link these records hold (same rule as InputSet today,
  §19 decision 4 amended 2026-10-25).
- No measurable cost on the task thread, and no extra reads at completion
  beyond what InputRecorder already does. Everything comes from objects the
  plugin already sees in memory.
- The explorer must not name a wrong upstream item. When the record cannot
  say which item, the page shows the file with no upstream item rather
  than a guess from content address.

## 5. Design

### 5.1 InputSet: where an input came from

`Input` gains one optional field:

```ipldsch
type Input struct {
  role String
  name nullable String
  address nullable &Any
  size nullable Int
  sample nullable String
  reason nullable String
  from optional [Origin]        # schema 2: the fromStore items that gave the task this file; absent when none
}
type Origin struct {
  item &OutputItem               # the upstream item fromStore emitted
  collection nullable &OutputCollection  # the output it was reached through; null when a Selection picked it by query
  selection nullable &Selection  # the Selection named in the fromStore call; null for run:/output:
}
```

- `InputSet.schema` becomes 2. Readers accept 1 and 2. `from` is absent at
  schema 1 and absent at schema 2 when the list would be empty. Origins are
  sorted by (item, collection, selection) as CID bytes, nulls first, with no
  two equal. The `Input` sort key and its "no two equal" rule gain `from`
  last.
- `fromStore` fills a per-run map on `CasSession` while it resolves items
  (it resolves all of them before any task runs): emitted Path to the set
  of Origins. A Selection item reached through several OutputCollections
  (`collection_item.via_cid`) gives one Origin per collection. Nested
  Selections give the Selection the call named, not the inner one (builder
  may record the inner one instead if it is as cheap; say which in DESIGN.md).
  `selectionItems` and `items` drop the via and collection columns today;
  they return them instead.
- `InputRecorder` keys its entries by the Path the task was given
  (`FileHolder.sourceObj`, its `origin`). At `collect`, an entry whose
  origin is in the map gets that Origin set. When the same file came from
  two emitted items (two items holding identical bytes, both emitted), the
  input lists both Origins. That is the only case with more than one, and
  the explorer shows both.
- Links in `from` are not followed by Mark or by the copy closure. They
  name another run's records, possibly in another member.

### 5.2 ItemLineage: which inputs made which item

A new record kind, written by the observer at completion just after the
InputSet and linked from the RunCompletion:

```ipldsch
type ItemLineage struct {
  schema Int                     # 1
  asserted_by String
  input_set &InputSet            # the InputSet whose positions `inputs` names
  items [Derived]                # sorted by item CID bytes; one per item with at least one input
}
type Derived struct {
  item &OutputItem               # an OutputItem of this run
  inputs [Int]                   # positions in input_set.inputs, ascending, no duplicates
}
```

and on RunCompletion, schema 3:

```ipldsch
  item_lineage optional nullable &ItemLineage  # schema 3: null when it could not be made; absent at schema 1 and 2
```

- Positions, not addresses, so an input with no address (unaddressed,
  remote, unreadable) can still be named. The InputSet is sorted and
  content-addressed, so a position is stable for a given InputSet CID.
- No ItemLineage without an InputSet. When the InputSet could not be made,
  `item_lineage` is null.
- An item none of whose files traces to a run input is left out (an item
  of values only, or made by tasks that read nothing from outside the run).
  A run where no item has inputs writes an ItemLineage with empty `items`,
  so `item_lineage: null` keeps meaning "could not be made".

`inputs` is the closure at the run boundary: the walk passes through the
run's intermediate tasks and records only run inputs. A later task-level
record (a Task Record block, a process table; see the tradeoffs page
https://claude.ai/artifact/1GwJVqeWJJCMsXC1vJ21LF) refines this rather
than contradicting it.

How the observer builds it, in memory:

1. `onFilePublish` keeps, per publish join key, `event.source` (the
   work-dir file; Nextflow passes it on the already-exists path too, so a
   resumed publish still reports it). The Output Index File event has no
   source and is skipped.
2. `onTaskComplete` and `onTaskCached` keep, keyed by task hash, the
   task's work directory, process name, tag and staged input sources (the
   list `InputRecorder.staged` already computes). Copy out paths and
   strings only; holding the TaskRun would pin its values. Keying by task
   hash (orchestrator's call, from the tradeoffs analysis) lets the same
   structure feed task references or a Task Record later.
3. At completion, for each joined OutputItem (the Join already gives item
   to leaf join keys): source path to the task whose work directory holds
   it, then that task's inputs. An input that is a run input (an entry the
   InputRecorder kept) is a hit. An input under another task's work
   directory is followed the same way. Each task is visited once per item,
   so cycles end. A published file that is itself one of the task's staged
   inputs (a link the task passed through) maps to that one input, not to
   all of the task's inputs.
4. A failed run writes what it resolved. An item whose source never maps to
   a task (published from outside a task) gets no entry.

### 5.3 Sweep and copy

- Mark follows `item_lineage` as run metadata, kept as itself, nothing under
  it followed: same treatment as `input_set`. ItemLineage's own links point
  at this run's items and InputSet, which the run keeps anyway.
- The copy closure carries `item_lineage` in `run_records` at every depth,
  read like a record, nothing under it followed, as for `input_set`.
- A build that predates this change and sweeps a member holding schema-3
  RunCompletions would trash ItemLineage blocks. The user is the only user
  and runs one build; no compatibility shim (clean over compat).

### 5.4 Index

Schema version 7 (cache and snapshot `index/v7.sqlite`; the web adds v6 to
the older paths). Rebuild, no migration. Two tables, both keyed by the
input's position in the run's InputSet (orchestrator's call): a join on
address and name would attach the wrong Origins to a directory input,
which `fromStore` emits as `cas://<item>/<name>` so two items' identical
directories differ only in `from`, and could not join an input with no
address.

- `input_origin(completion_cid, position, item_cid, collection_cid,
  selection_cid)`, one row per (input, Origin), index on `item_cid` and on
  `completion_cid`. The input's name, role and address come from
  `run_input`, which gains a `position` column.
- `item_input(completion_cid, item_cid, positions)`, one row per Derived
  item, `positions` a JSON array, index on `item_cid`. One row per item
  rather than per (item, input) because shared references (genome, index)
  fan out to every item: about 6,000 to 10,000 rows per 100-sample rnaseq
  run as separate rows (estimate from the tradeoffs analysis), about 1,000
  packed.

Both ingested with the InputSet / ItemLineage the RunCompletion links, with
the same wait-and-retry and `clearRun` handling `run_input` has, copied into
the snapshot by `completion_cid IN main.run`, and seeded back. Neither
writes `block`, `block_ref` or `missing` rows. Page queries go in
`web/src/queries.json` and into `ExplorerQueriesTest`'s scan table.

### 5.5 Explorer: the Lineage panel

The panel keeps its thread (inputs on top, the item highlighted). Above the
item it lists the item's inputs from `item_input` (snapshot) or the run's
ItemLineage and InputSet blocks (tail run). Per input:

```
○ people · Dev                          ← upstream item, link to #/item/<collection>/<item>
    scratch-workflow · infallible_spence  via Selection "friends"
○ Dev_Patel.dat                          ← the file, link to #/content/<address>
● intros · Dev                           ← this item
```

- An input with Origins shows each upstream item above its file, with the
  run chip (`runFact`) and the Selection's name from its Claims (short CID
  when unnamed). Several Origins for one file show as several upstream
  nodes; `circle` shows four.
- An input with no Origin shows the file only, as an input node; dashed
  (`ext`) when no member holds its bytes.
- The never-published inputs the panel shows today stay.
- Lineage is a fact about one run. With a collection in the URL the page
  shows that run's lineage only; an older run's page stays as today even
  when a newer run recorded lineage for the same item CID. With `-` in the
  URL (picked by a query across runs), the page shows the lineage of every
  run that recorded one for the item, grouped under a run chip per run, and
  no grouping when there is one (orchestrator's call).
- An Origin with a null `collection` (a Selection picked the item by query)
  does not name a run: the item CID alone matches every run that published
  identical content. The node shows the upstream item linked to
  `#/item/-/<item>`, labelled "picked by query", with the Selection and no
  run chip; that page's "Produced by" already lists every run that made it
  (orchestrator's call, per the no-guessing anti-goal).
- The upstream item may live in another member or in none. Use presence
  to find a member that holds the item block and link there
  (`hrefIn`); when none does, show the item's short CID and "not in any
  store here" without a link, and still show the run and Selection when
  their labels can be read.
- Runs without ItemLineage (older runs, or `item_lineage: null`) show the
  panel as today.
- `ItemLineage` and the new `Input.from` field are added to DESIGN.md's
  ipldsch block as `optional`, so the browser's schema check accepts new
  blocks and older blocks still validate. The generic block page lists the
  new kind as "a block of kind ItemLineage", like InputSet.

### 5.6 Small fixes in the same area

- `InputRecorder` reads `params.input` with `params.get('input')`, which
  warns `Access to undefined parameter 'input'` in a typed script whose
  `params {}` block has no `input`, and throws under
  `nextflow.enable.strict`. Read it only when `params.containsKey('input')`.
- README "Reading outputs in another pipeline": the note that a typed
  script must call `nextflow.Channel.fromStore` still holds on Nextflow
  26.10.4 (checked 2026-10-26: `channel.fromStore` fails with
  MissingProcessException). Say so, and that record-collection inputs
  (`Bag<Record>`) stage files from 26.10.4.
- DESIGN.md's lineage measurement (~line 4813) says task inputs are "never
  `cas://` ones". Amend: `fromStore` inputs are, and `TaskLineage` stops at
  them by design.

## 6. Acceptance

On a scratch copy of the roblab store (never the real one), with the
plugin built from this branch and Nextflow 26.10.4:

1. Run `scratch-consumer-workflow` against the copy. The RunCompletion is
   schema 3, links an InputSet (schema 2) and an ItemLineage. Each of the
   four datafile inputs has exactly one Origin: the `people` item from
   `infallible_spence`, its `people` OutputCollection, and the Selection.
   ItemLineage lists five items: each `intros` item names one input, the
   `circle` item names all four.
2. Resume the same run (`-resume`): every task is cached, and the InputSet
   and ItemLineage CIDs are the same as in step 1.
3. `nextflow plugin nf-blocks:explore` on the copy: the `intros · Dev`
   item page shows `people · Dev` (linked, with run `infallible_spence` and
   the Selection) above `Dev_Patel.dat` above the item. Clicking
   `people · Dev` opens that item's page.
4. Two members. Member A is a fresh, empty scratch store, the consumer's
   writable member. Member B is the scratch copy of roblab, read-only,
   holding the producer runs and the Selection. Run the consumer with
   `resolve = ['A', 'B']`; `explore` over the composition of both: the
   upstream node links into B (`?member=B`) and the run chip and Selection
   name are read from B. Then `explore` with A alone: the node shows the
   short CID and "not in any store here", and the file node is dashed.
   No blocks are deleted by hand (orchestrator's call; a sweep could not
   remove the upstream run from B while the Selection holds it).
5. A run written by the previous build shows the Lineage panel unchanged.
6. The typed consumer run prints no `Access to undefined parameter 'input'`
   warning.
7. `make test` (Spock and web) passes; the Gate's typed consumer
   (`gate/selection-typed`) asserts Origins on its InputSet.

## 7. Left to the builder

- Names of Groovy classes and methods, and the in-memory structures for 5.2.
- Whether `fromStore` records the outer or inner Selection for nested
  Selections (5.1), stated in DESIGN.md.
- Exact wording and layout of the Lineage nodes within the visual language
  spec (`docs/superpowers/specs/2026-10-14-explorer-visual-language-design.md`).
- How the Gate checks Origins and ItemLineage.
