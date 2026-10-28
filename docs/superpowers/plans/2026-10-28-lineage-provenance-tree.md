# Lineage Provenance Tree Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The item page's Lineage panel draws a tree with one group per upstream run (run chip, then one line per output and Selection, then a row per input position), ending on the item, and spans the main column above Produced by and Used by.

**Architecture:** A pure module `web/src/lineage.js` turns `Explorer.itemLineage`'s inputs plus the Origin reads `readOrigin` makes into groups, lines and rows. A new kit component `tree()` in `web/src/components.js` draws any such structure, with its CSS in the kit (`kit.css` then `kit-css.js`). `web/src/views/item.js` reads the Origins as today, groups them, asks the holding member for the upstream pipeline (`runIdentity`) and the collection's item count (new `Explorer.collectionItemCount`), and builds the tree's cells.

**Tech Stack:** Plain ES modules in `web/src`, built with esbuild (`web/build.mjs`); tests with `node:test` and the fake DOM in `web/test/dom.mjs`; sqlite-wasm snapshots in model tests (`web/test/helpers.mjs`, `web/test/fixture.mjs`); Playwright from `gate/browser/node_modules` and the Range server `gate/browser/serve.py` for the visual check.

**Spec:** `docs/superpowers/specs/2026-10-28-lineage-provenance-tree-design.md`. The agreed picture is `docs/superpowers/specs/2026-10-28-lineage-provenance-tree/mockup.png` (drawn by `mockup.js` there). Where the picture and the text differ, the text wins; spec §6 lists the known differences.

## Global Constraints

- Code reads like the surrounding code: the `h()` DOM builder (`web/src/html.js`), one-line `/** ... */` doc comments on functions, comments only where the code does not say it, names in the file's existing style.
- No backwards-compatibility shims: the user is the only user. `originNode` and `inputNodes` in `views/item.js` are deleted, not kept beside the new code. `thread()` and its `.thread` CSS stay, for the kit page's Lineage thread row (it mirrors `docs/design/mockups/01-components.html`); nothing else calls it afterwards.
- `Explorer.itemLineage` keeps its shape (spec §3). The only model change is the new `collectionItemCount`.
- Never name a run the records do not name (spec §4): a row sits under a run only when one of its input's Origins names a collection of that run. Rows may repeat across groups; the header counts inputs, not rows.
- User-visible copy, exactly: `Lineage`; `<R> upstream run` / `<R> upstream runs`; `<N> input` / `<N> inputs`, joined by ` · `; `<C> runs recorded lineage`; `<k> of <m> items` (`item` when m is 1) or `<k> item` / `<k> items`, followed by `, via` when the line has a Selection; `"<name>"` (several names joined ` / `); `picked by query`; `not in any store here`; `run not in any store here`; `Read by path`; `unnamed input`; `also`; `<n> other runs`; `made by`; `+<n> more`. The age is what `ago(finished_at, now)` returns (`2d`), not "2 days ago".
- Kit CSS: `docs/design/mockups/src/kit.css` is the source of truth and is edited first; the same rules go into `web/src/kit-css.js` by hand (no build step copies them). `docs/design/mockups/src/build.py` inlines kit.css into `docs/design/mockups/*.html`, which are in sync today; rerun it after editing kit.css.
- Tests use the fake DOM (`web/test/dom.mjs`): `querySelectorAll` takes compound selectors only (`tag`, `.class`, `[attr]`, `[attr=value]`, combined with no spaces), no descendant combinators or `:has`, and returns an Array. An element appended a second time is not moved, so build fresh elements for every row and every item node.
- Test commands run from `web/`: one file with `cd web && node --test test/<name>.test.mjs`; the suite with `cd web && npm test` (it runs `node schema-gen.mjs` first, then every `test/**/*.test.mjs`). In a fresh worktree run `cd web && npm ci` first. Never symlink `web/node_modules` from another checkout: `npm ci` wipes the symlink's target.
- Quote every heredoc delimiter (`<<'EOF'`) so backticks and `$` in the body are not run by the shell.
- `/tmp/cas-exploration/store` is the user's store and read-only for agents: never open, serve or point anything at it. Copy it with `cp -R` to a scratch directory first; plain reads (`ls`, `cat`) are the only thing allowed on the original. Any `sqlite3` on a real store (even a copy) uses `-readonly`.
- Anything that runs nf-blocks sets `XDG_CACHE_HOME` to a scratch directory, so no index cache lands in `~/.cache/nf-blocks`.
- Commits: messages in the repo's `area: sentence` style (`explorer: ...`, `kit: ...`, `docs: ...`), and every message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Commit only the files your task lists. Stay on branch `lineage-provenance-tree` unless the orchestrator gives you another.

## Shared interfaces

Every task's implementer reads this section; the tasks refer to it rather than repeat it.

### `web/src/lineage.js` (Task 2)

```js
/** The key readOrigin's answers are cached under: `${item_cid} ${collection_cid} ${selection_cid}`. */
export function originKey(origin) -> string

/** Inputs and their Origin reads as run groups, lines and rows (lineage provenance tree spec §5.3, §5.6). */
export function groupLineage(inputs, reads, leaves = []) -> { groups: Group[], runs: number, inputs: number }

/** The panel's or a tree's meta: `<R> upstream run(s) · <N> input(s)`, `<N> input(s)` when R is 0, null when N is 0. */
export function countsText(runs, inputs) -> string | null

/** A line's count: `<k> of <m> item(s)` when m is a positive number, else `<k> item(s)`. Never "of 0". */
export function itemsText(k, m) -> string
```

Shapes:

```
Input = { position, name, address, size, origins }        itemLineage's input; role and sample pass through unread; origins may be [] or missing
Origin = { item_cid, collection_cid, selection_cid }       any of the last two may be null
Read  = { itemCid, collection, selection, holder, reader, title, label, names }
        readOrigin's answer: holder is a member alias or null; reader is that member's Explorer (opaque here);
        label is Explorer.runLabel(collection) or null; names are the Selection's names from Claims ([] when none).
        An Origin with no entry in `reads` is grouped as the Read { itemCid: item_cid, collection: collection_cid,
        selection: selection_cid, holder: null, reader: null, title: null, label: null, names: [] }.
leaves = string[]                                          the item's never-published leaf names, in leaf order

Group = { kind: 'run' | 'collection' | 'query' | 'path', key, completion, collection, selection, names, label, holder, reader, lines: Line[] }
  run:        key `run:<label.completion>`; completion, label, holder, reader from the group's first row's Read; collection, selection, names null
  collection: key `collection:<collection>` (an Origin with a collection whose run label is null); label null
  query:      key `query:<selection ?? '-'>` (an Origin with collection null); selection, names, holder, reader from its first Read; one line
  path:       key `path`; every field but kind, key and lines null; one line
Line  = { key, collection, selection, names, label, holder, reader, items, rows: Row[] }
  key `<collection ?? '-'>:<selection ?? '-'>`; the path line's key is `path`.
  label, holder, reader, names from the line's first row's Read; items = distinct read.itemCid among its rows (0 on the path line).
Row   = { position, input, leaf, read, also }
  An Origin row: position = input.position, input, leaf null, read, also = Also[].
  A path row for an Origin-less input: position, input, leaf = the merged leaf name or null, read null, also [].
  A leaf row: position null, input null, leaf = name, read null, also [].
Also  = { completion, label, holder }    another run group holding this row's position, from that group's fields
```

Rules `groupLineage` follows:

- An Origin's Read goes to a run group when `read.label?.completion` is set, else to a collection group when `read.collection` is set, else to a query group keyed by `read.selection`.
- Walk the inputs by position and each input's Origins in their given order; a group, and a line within it, is created the first time it is seen. So run groups come in order of the lowest position they hold, ties by Origin order. The query and collection groups follow all run groups, in the same first-seen order among themselves. The path group is last.
- Rows within a line are by position, then item CID.
- `also` on a row lists the run groups other than the row's own group's completion that hold the same position, in group order. A row in a query or collection group lists every run group holding its position. Two lines of the same run (two outputs) never name their own run.
- The path group holds Origin-less inputs (origins empty or missing) by position, then the leaves. A leaf whose name equals an Origin-less input's name is that input's row (`leaf` set on it), not a row of its own. Every other leaf gets a leaf row, in leaf order. The path group exists only when it has rows.
- `runs` is the number of run groups. `inputs` is the number of distinct positions in `inputs`; leaves never count.

### `tree(groups, here)` in `web/src/components.js` (Task 3)

```js
/**
 * A provenance tree: each group a head on the main rail with a hollow node, its lines indented under it,
 * each line's rows on a branch rail with a tick per row (dashed when `ext`); ends on `here`, the filled node.
 */
export function tree(groups, here) -> HTMLElement
// groups: [{ key, head, lines: [{ key, head, rows, folded = [], foldKey }] }]
// row:    { key, cells: [item, file, tag, size], ext = false }   each cell a Node, string, array or null
// head, line head, here: a Node, string or array; a line head may be null
```

### `Explorer.collectionItemCount(cid)` in `web/src/model.js` (Task 1)

```js
/** How many items a collection holds: the snapshot's count, else its OutputCollection block's (a tail collection), else null. Reads no OutputItem. */
async collectionItemCount(cid) -> number | null
```

### DOM contract (Tasks 3 and 4; the tests rely on it exactly)

```
div.tree
  div.tree-group[data-group=<group key>]                 one per group, in order
    div.tree-head                                        the group head
    div.tree-line[data-line=<line key>]                  one per line
      div.tree-sub                                       the line head; absent when the line head is null
      div.tree-rows                                      class also `no-item` when every row and folded row has a null item cell
        div.tree-row[data-row=<row key>]                 class also `ext` when the row is dashed
          span.tree-item  span.tree-file  span.tree-tag  span.tree-size     always all four, empty when the cell is null
        details.fold[data-fold=lineage]                  only when `folded` is non-empty: fold(foldKey, '+<n> more', ...)
          summary                                        '+<n> more'
          div.tree-more                                  the folded div.tree-row elements
  div.tree-here                                          last child: the item
```

On the item page (Task 4):

```
section.panel with h2 'Lineage'; its .ph has span.muted = the meta (absent when the meta is null)
  one tree:      .pb > div.tree
  every-run view (two or more consumer runs):
                 .pb > div > div[data-lineage-run=<consumer completion>] > p.muted.small[data-lineage-counts] (absent when countsText is null), div.tree
row key: String(position) for an input row; 'leaf' for a leaf row
group head children:
  run:        a.fact.sealed-fact (runFact, href = the run page in the group's holder), span.muted.small '<pipeline> · <age>' (absent when both are unknown)
  collection: code.cid[title=<collection>] short CID, span.muted.small 'run not in any store here'
  query:      span.ref (the Selection ref; absent when the Origin has no Selection), span.muted.small 'picked by query'
  path:       span.muted 'Read by path'
line head (run and collection groups; query and path lines have none):
  span.ref output ref (absent when the label has no output), span.muted.small '<itemsText>' + ', via' when there is a Selection, span.ref Selection ref (absent without a Selection)
tag cell: span.muted.small 'also', then a.also (one other run: seal 12px + run name, href = its run page in its holder)
          or span.also[title=<names joined ', '>] '<n> other runs' (two or more, not a link)
here children: span.ref (item ref, its text in b: '<output> · <title>'), then span.muted.small 'made by' and the run's runFact, those two absent when no run is known
```

## Review Focus

- Two outputs of one upstream run feeding the same input (two lines in one group): each row's `also` tag must not name its own run. Pinned in Task 2 (`two outputs of one run make two lines, and neither row names its own run`).
- `collectionItemCount` rejecting (an unreadable block, a failed query): the line falls back to `<k> items, via` and the page still renders. Pinned in Task 4 (`the line says k of m ...`, the rejecting case).
- A long Read by path group (an item made from dozens of files read by path): it folds like a line, so the panel does not run on for screens. Pinned in Task 4 (`Read by path folds like a line`).
- One run group whose items are held in different members: every row links its own holder, not the group head's. Pinned in Task 4 (`rows of one run held in two members each link their own member`).
- An Origin naming neither a collection nor a Selection: a `picked by query` group with no Selection ref and no "null" or "undefined" text. Pinned in Task 2 (`... an Origin naming neither goes in query:-`) and Task 4 (`an Origin naming neither collection nor Selection ...`).

---

## Files

| File | Task | What changes |
|---|---|---|
| `web/src/model.js`, `web/test/model.test.mjs` | 1 | `collectionItemCount` and its test |
| `web/src/lineage.js` (new), `web/test/lineage-groups.test.mjs` (new) | 2 | the pure grouping |
| `web/src/components.js`, `docs/design/mockups/src/kit.css`, `web/src/kit-css.js`, `docs/design/mockups/*.html` (rebuilt), `web/src/views/kit.js`, `web/test/components.test.mjs` | 3 | `tree()`, its CSS, the kit row |
| `web/src/views/item.js`, `web/test/item-lineage-view.test.mjs`, `web/test/item-view.test.mjs`, `web/test/pages.test.mjs`, `DESIGN.md`, `docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md` | 4 | the page wiring, its tests, the docs |
| scratch only; CSS fixes in `kit.css`/`kit-css.js` if needed | 5 | visual check |

Tasks 1, 2 and 3 are independent. Task 4 needs all three. Task 5 needs Task 4.

---

### Task 1: `Explorer.collectionItemCount`

**Files:**
- Modify: `web/src/model.js` (add the method next to `allItems`, near line 1153)
- Test: `web/test/model.test.mjs` (add after the `allItems` test, near line 221)

**Interfaces:**
- Consumes: `SQL.collectionByCid`, `SQL.collectionItemCount` (`web/src/queries.json`), `this.blocks.ofKind(cid, 'OutputCollection')`, `BlockError`; the test's `openBig()`, `bigCollCid`, `open()`, `rawCid` already in `model.test.mjs`.
- Produces: `Explorer.collectionItemCount(cid) -> Promise<number | null>` (see Shared interfaces).

- [ ] **Step 1: Write the failing test**

```js
test('collectionItemCount counts a snapshot collection without a block read, a tail collection from its block, and is null for one neither has', async () => {
  const big = await openBig()
  assert.equal(await big.collectionItemCount(bigCollCid), 1200)
  assert.equal(big.blocks.fetches, 0, 'a snapshot collection is counted by the SQL alone')
  const { explorer, member } = await open()
  assert.equal(await explorer.collectionItemCount(member.runs.R1.collection), 2)
  assert.equal(await explorer.collectionItemCount(member.runs.R2.collection), 2, 'a tail collection: the SQL would say 0, the block says 2')
  assert.equal(await explorer.collectionItemCount(rawCid('held elsewhere').toString()), null)
})
```

- [ ] **Step 2: Run it to see it fail**

Run: `cd web && node --test test/model.test.mjs`
Expected: FAIL, `explorer.collectionItemCount is not a function`.

- [ ] **Step 3: Implement `async collectionItemCount(cid)` in `web/src/model.js`**

Same branch as `allItems`: when `SQL.collectionByCid` finds a row, answer `SQL.collectionItemCount`'s `n`; otherwise count the OutputCollection block's `items.filter(Boolean)`; a `BlockError` from that read answers null, anything else is rethrown. The doc comment is the one in Shared interfaces.

- [ ] **Step 4: Run it to see it pass**

Run: `cd web && node --test test/model.test.mjs`
Expected: PASS, every test in the file.

- [ ] **Step 5: Commit**

```bash
git add web/src/model.js web/test/model.test.mjs
git commit -F - <<'EOF'
explorer: Explorer.collectionItemCount counts a collection's items without reading them

A snapshot collection is counted by its collection_item rows; a tail
collection, which the SQL would count as 0, by its OutputCollection block;
one neither holds is null.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 2: The pure grouping, `web/src/lineage.js`

**Files:**
- Create: `web/src/lineage.js`
- Test: `web/test/lineage-groups.test.mjs`

**Interfaces:**
- Consumes: nothing from other tasks. The module imports nothing.
- Produces: `originKey`, `groupLineage`, `countsText`, `itemsText`, exactly as in Shared interfaces (signatures, shapes and rules).

- [ ] **Step 1: Write the failing tests**

File header: `// web/test/lineage-groups.test.mjs` then `// groupLineage over plain objects: inputs and their Origin reads as run groups, lines and rows, with no DOM.`

Fixtures:

```js
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { countsText, groupLineage, itemsText, originKey } from '../src/lineage.js'

const label = (run_name, completion, output = 'people') => ({ run_name, output, completion, status: 'succeeded' })
const FAB = label('fabulous_bartik', 'run-fab')
const SLEEPY = label('sleepy_williams', 'run-sleepy')
const KEEN = label('keen_hopper', 'run-keen')
const LABELS = { 'coll-fab': FAB, 'coll-sleepy': SLEEPY, 'coll-keen': KEEN, 'coll-fab-2': label('fabulous_bartik', 'run-fab', 'friends') }
const from = (item_cid, collection_cid = 'coll-fab', selection_cid = 'sel-community') => ({ item_cid, collection_cid, selection_cid })
const input = (position, name, origins = []) => ({ position, role: 'path', name, address: `bafk-${name}`, size: 10, sample: null, origins })
/** The reads readOrigin would make: every item held in the open member `lab`, each collection labelled from LABELS. */
const readsOf = (inputs) => new Map(inputs.flatMap(i => i.origins ?? []).map(o => [originKey(o), { itemCid: o.item_cid, collection: o.collection_cid,
  selection: o.selection_cid, holder: 'lab', reader: null, title: o.item_cid, label: LABELS[o.collection_cid] ?? null, names: ['Community'] }]))
/** Each group's key with its lines' keys and their rows' positions (a leaf row by its name). */
const shape = ({ groups }) => groups.map(g => [g.key, g.lines.map(l => [l.key, l.rows.map(r => r.position ?? r.leaf)])])
```

Tests (names and assertions):

```js
test('one upstream run: one group, one line, a row per position, the items counted', () => {
  const inputs = ['Amina', 'Dev', 'Femi', 'Greta'].map((n, i) => input(i, `${n}.dat`, [from(n)]))
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(shape(out), [['run:run-fab', [['coll-fab:sel-community', [0, 1, 2, 3]]]]])
  assert.equal(out.groups[0].kind, 'run')
  assert.equal(out.groups[0].completion, 'run-fab')
  assert.equal(out.groups[0].label, FAB)
  assert.equal(out.groups[0].lines[0].items, 4)
  assert.deepEqual(out.groups[0].lines[0].names, ['Community'])
  assert.deepEqual([out.runs, out.inputs], [1, 4])
})

test('groups by the lowest position they hold, rows by position; a row under two runs names the other', () => {
  const inputs = [input(2, 'Bo.dat', [from('Bo')]), input(0, 'Cy.dat', [from('Cy', 'coll-sleepy')]), input(1, 'Ana.dat', [from('Ana'), from('Ana', 'coll-sleepy')])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(shape(out), [['run:run-sleepy', [['coll-sleepy:sel-community', [0, 1]]]], ['run:run-fab', [['coll-fab:sel-community', [1, 2]]]]])
  const ana = (g) => out.groups[g].lines[0].rows.find(r => r.position === 1)
  assert.deepEqual(ana(0).also, [{ completion: 'run-fab', label: FAB, holder: 'lab' }])
  assert.deepEqual(ana(1).also, [{ completion: 'run-sleepy', label: SLEEPY, holder: 'lab' }])
  assert.deepEqual(out.groups[0].lines[0].rows[0].also, [])
  assert.deepEqual([out.runs, out.inputs], [2, 3])
})

test('an input three runs name lists the two others, in group order', () => {
  const inputs = [input(0, 'Dev.dat', [from('Dev')]), input(1, 'Ana.dat', [from('Ana'), from('Ana', 'coll-sleepy'), from('Ana', 'coll-keen')])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(out.groups.map(g => g.key), ['run:run-fab', 'run:run-sleepy', 'run:run-keen'], 'a tie on position keeps Origin order')
  assert.deepEqual(out.groups[0].lines[0].rows[1].also.map(a => a.completion), ['run-sleepy', 'run-keen'])
  assert.deepEqual(out.groups[2].lines[0].rows[0].also.map(a => a.completion), ['run-fab', 'run-sleepy'])
})

test('two outputs of one run make two lines, and neither row names its own run', () => {
  const inputs = [input(0, 'Dev.dat', [from('Dev'), from('Dev', 'coll-fab-2')])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(shape(out), [['run:run-fab', [['coll-fab:sel-community', [0]], ['coll-fab-2:sel-community', [0]]]]])
  assert.deepEqual(out.groups[0].lines.flatMap(l => l.rows.map(r => r.also)), [[], []])
  assert.equal(out.groups[0].lines[1].label.output, 'friends')
  assert.deepEqual([out.runs, out.inputs], [1, 1])
})

test('two positions reading one item count it once', () => {
  const inputs = [input(0, 'Dev.dat', [from('Dev')]), input(1, 'again.dat', [from('Dev')])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.equal(out.groups[0].lines[0].items, 1)
  assert.deepEqual(out.groups[0].lines[0].rows.map(r => r.input.name), ['Dev.dat', 'again.dat'])
})

test('no collection goes in a query group by its Selection, an unreadable run label in a collection group, both after the run groups; an Origin naming neither goes in query:-', () => {
  const inputs = [input(0, 'Q.dat', [from('Q', null, 'sel-friends')]), input(1, 'U.dat', [from('U', 'coll-gone')]),
    input(2, 'F.dat', [from('F')]), input(3, 'N.dat', [from('N', null, null)])]
  const out = groupLineage(inputs, readsOf(inputs))
  assert.deepEqual(shape(out), [['run:run-fab', [['coll-fab:sel-community', [2]]]], ['query:sel-friends', [['-:sel-friends', [0]]]],
    ['collection:coll-gone', [['coll-gone:sel-community', [1]]]], ['query:-', [['-:-', [3]]]]])
  assert.deepEqual(out.groups.map(g => g.kind), ['run', 'query', 'collection', 'query'])
  assert.equal(out.groups[1].selection, 'sel-friends')
  assert.deepEqual(out.groups[1].names, ['Community'])
  assert.deepEqual([out.runs, out.inputs], [1, 4])
})

test('an Origin with no read is grouped as nothing read', () => {
  const inputs = [input(0, 'Dev.dat', [from('Dev')])]
  const out = groupLineage(inputs, new Map())
  assert.deepEqual(shape(out), [['collection:coll-fab', [['coll-fab:sel-community', [0]]]]])
  const { read } = out.groups[0].lines[0].rows[0]
  assert.deepEqual([read.itemCid, read.holder, read.label, read.title, read.names], ['Dev', null, null, null, []])
  assert.equal(out.runs, 0)
})

test('Origin-less inputs and never-published leaves make the last group; a leaf named like such an input is that row', () => {
  const inputs = [input(0, 'a.dat', [from('A')]), { ...input(1, 'genome.fa'), origins: undefined }, input(2, 'b.dat')]
  const out = groupLineage(inputs, readsOf(inputs), ['b.dat', 'extra.txt', 'a.dat'])
  assert.deepEqual(shape(out), [['run:run-fab', [['coll-fab:sel-community', [0]]]], ['path', [['path', [1, 2, 'extra.txt', 'a.dat']]]]])
  const rows = out.groups[1].lines[0].rows
  assert.deepEqual(rows.map(r => r.leaf), [null, 'b.dat', 'extra.txt', 'a.dat'])
  assert.deepEqual(rows.map(r => r.input?.name ?? null), ['genome.fa', 'b.dat', null, null])
  assert.equal(out.groups[1].kind, 'path')
  assert.equal(out.groups[1].lines[0].items, 0)
  assert.deepEqual([out.runs, out.inputs], [1, 3], 'leaves are not inputs')
})

test('no inputs and no leaves: no groups; leaves alone: the path group, no inputs', () => {
  assert.deepEqual(groupLineage([], new Map()), { groups: [], runs: 0, inputs: 0 })
  const out = groupLineage([], new Map(), ['in.fq'])
  assert.deepEqual(shape(out), [['path', [['path', ['in.fq']]]]])
  assert.equal(out.inputs, 0)
})

test('countsText', () => {
  assert.equal(countsText(1, 4), '1 upstream run · 4 inputs')
  assert.equal(countsText(2, 9), '2 upstream runs · 9 inputs')
  assert.equal(countsText(1, 1), '1 upstream run · 1 input')
  assert.equal(countsText(0, 2), '2 inputs')
  assert.equal(countsText(0, 1), '1 input')
  assert.equal(countsText(0, 0), null)
})

test('itemsText never says of 0', () => {
  assert.equal(itemsText(4, 9), '4 of 9 items')
  assert.equal(itemsText(1, 1), '1 of 1 item')
  assert.equal(itemsText(4, null), '4 items')
  assert.equal(itemsText(1, null), '1 item')
  assert.equal(itemsText(2, undefined), '2 items')
  assert.equal(itemsText(3, 0), '3 items')
})
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd web && node --test test/lineage-groups.test.mjs`
Expected: FAIL, `Cannot find module '../src/lineage.js'`.

- [ ] **Step 3: Implement `web/src/lineage.js`**

Header: `// web/src/lineage.js` and two lines saying it groups an item's inputs and their Origin reads for the Lineage panel (lineage provenance tree spec §5.3, §5.6), with no DOM so it is tested alone. The four exports, their doc comments and the rules are in Shared interfaces. `groupLineage` does one pass over the inputs sorted by position (Maps keyed by group key and line key keep first-seen order), sorts each line's rows by position then item CID, then fills `also` from a position-to-run-groups map.

- [ ] **Step 4: Run them to see them pass**

Run: `cd web && node --test test/lineage-groups.test.mjs`
Expected: PASS, 11 tests.

- [ ] **Step 5: Commit**

```bash
git add web/src/lineage.js web/test/lineage-groups.test.mjs
git commit -F - <<'EOF'
explorer: lineage.js groups an item's inputs into one group per upstream run

A pure function over itemLineage's inputs and readOrigin's answers:
run groups by lowest position, a line per (collection, Selection), a row
per position naming the other runs that hold it; picked-by-query and
unlabelled collections after them, Read by path last with the item's
never-published leaves merged by name.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 3: The `tree()` kit component, its CSS and the kit row

**Files:**
- Modify: `web/src/components.js` (add `tree` after `thread`, near line 245; import `fold` from `./folds.js`)
- Modify: `docs/design/mockups/src/kit.css` (a `/* ---------- Lineage tree ---------- */` block right after the Lineage thread block, near line 263, followed by one blank line)
- Modify: `web/src/kit-css.js` (the same block, verbatim, right after its Lineage thread block, near line 270)
- Regenerate: `docs/design/mockups/*.html` with `python3 -I docs/design/mockups/src/build.py`
- Modify: `web/src/views/kit.js` (a `Lineage tree` row right after the `Lineage thread` row)
- Test: `web/test/components.test.mjs`

**Interfaces:**
- Consumes: `fold(key, summary, ...body)` from `web/src/folds.js`; `h` from `./html.js`.
- Produces: `tree(groups, here)` and the DOM contract in Shared interfaces; CSS classes `.tree`, `.tree-group`, `.tree-head`, `.tree-line`, `.tree-sub`, `.tree-rows`, `.tree-rows.no-item`, `.tree-row`, `.tree-row.ext`, `.tree-item`, `.tree-file`, `.tree-tag`, `.tree-size`, `.tree-more`, `.tree .also`, `.tree-here`.

- [ ] **Step 1: Write the failing tests** in `web/test/components.test.mjs`

Add `tree` to the components import, and `import { resetFolds } from '../src/folds.js'`, `import { KIT_CSS } from '../src/kit-css.js'`, `import { kit } from '../src/views/kit.js'`, `import { readFileSync } from 'node:fs'`.

```js
const treeRow = (key, item, file, ext = false) => ({ key, cells: [item, file, null, '10 B'], ext })

test('tree: groups, lines and rows carry data-group, data-line and data-row; four cells a row; the item last', () => {
  installDom(); resetFolds()
  const node = tree([
    { key: 'run:r1', head: 'fabulous_bartik', lines: [{ key: 'c1:s1', head: 'people', rows: [treeRow('0', 'Amina', 'Amina.dat'), treeRow('1', 'Dev', 'Dev.dat', true)] }] },
    { key: 'path', head: 'Read by path', lines: [{ key: 'path', head: null, rows: [treeRow('leaf', null, 'in.fq', true)] }] },
  ], 'circle · 4')
  assert.ok(node.classList.contains('tree'))
  assert.deepEqual(node.children.map(c => c.getAttribute('class')), ['tree-group', 'tree-group', 'tree-here'])
  const [run, path] = node.querySelectorAll('[data-group]')
  assert.deepEqual([run.getAttribute('data-group'), path.getAttribute('data-group')], ['run:r1', 'path'])
  assert.equal(run.querySelector('.tree-head').textContent, 'fabulous_bartik')
  assert.equal(run.querySelector('.tree-line').getAttribute('data-line'), 'c1:s1')
  assert.equal(run.querySelector('.tree-sub').textContent, 'people')
  const rows = run.querySelectorAll('.tree-row')
  assert.deepEqual(rows.map(r => r.getAttribute('data-row')), ['0', '1'])
  assert.deepEqual(rows[0].children.map(c => c.getAttribute('class')), ['tree-item', 'tree-file', 'tree-tag', 'tree-size'])
  assert.deepEqual(rows[0].children.map(c => c.textContent), ['Amina', 'Amina.dat', '', '10 B'])
  assert.deepEqual(rows.map(r => r.classList.contains('ext')), [false, true])
  assert.equal(path.querySelector('.tree-sub'), null, 'a line with no head has no sub line')
  assert.ok(path.querySelector('.tree-rows').classList.contains('no-item'))
  assert.equal(run.querySelector('.tree-rows').classList.contains('no-item'), false)
  assert.equal(node.querySelector('.tree-here').textContent, 'circle · 4')
  assert.equal(node.querySelector('details'), null, 'nothing folded, no fold')
})

test('tree: folded rows sit behind +n more in a lineage fold, after the shown rows', () => {
  installDom(); resetFolds()
  const rows = Array.from({ length: 7 }, (_, i) => treeRow(String(i), `P${i}`, `P${i}.dat`))
  const node = tree([{ key: 'run:r1', head: 'r1', lines: [{ key: 'c1:s1', head: 'people', rows: rows.slice(0, 2), folded: rows.slice(2), foldKey: 'lineage:c1:s1' }] }], 'x')
  const box = node.querySelector('.tree-rows')
  assert.deepEqual(box.children.map(c => c.tagName), ['DIV', 'DIV', 'DETAILS'])
  const more = box.querySelector('details')
  assert.equal(more.getAttribute('data-fold'), 'lineage')
  assert.equal(more.querySelector('summary').textContent, '+5 more')
  assert.ok(more.querySelector('.tree-more'))
  assert.deepEqual(more.querySelectorAll('.tree-row').map(r => r.getAttribute('data-row')), ['2', '3', '4', '5', '6'])
})

test('the Lineage tree rules in kit-css.js are kit.css\'s, verbatim', () => {
  const css = readFileSync(new URL('../../docs/design/mockups/src/kit.css', import.meta.url), 'utf8')
  const block = css.match(/\/\* -+ Lineage tree -+ \*\/\n[\s\S]*?\n\n/)?.[0]
  assert.ok(block, 'kit.css has a Lineage tree block followed by a blank line')
  assert.ok(KIT_CSS.includes(block))
})

test('the kit page shows a Lineage tree row', () => {
  installDom(); resetFolds()
  const page = kit()
  assert.ok(page.querySelectorAll('h2').some(t => t.textContent === 'Lineage tree'))
  assert.equal(page.querySelectorAll('.tree').length, 1)
})
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd web && node --test test/components.test.mjs`
Expected: FAIL, `tree` is not exported by `../src/components.js`.

- [ ] **Step 3: Implement `tree(groups, here)` in `web/src/components.js`**

The doc comment is in Shared interfaces. Build the DOM contract exactly: four cell spans per row (`h('span', { class: 'tree-item' }, cell)`, an empty span for null); `no-item` on a line's `.tree-rows` when every row in `rows` and `folded` has `cells[0] == null`; the fold, when `folded` is non-empty, is `fold(foldKey, `+${folded.length} more`, h('div', { class: 'tree-more' }, ...))` appended inside `.tree-rows` after the shown rows.

- [ ] **Step 4: Write the CSS in `docs/design/mockups/src/kit.css`, then the same block in `web/src/kit-css.js`**

Start from the CSS in `docs/superpowers/specs/2026-10-28-lineage-provenance-tree/mockup.js` (the `CSS` constant), renamed: `.ptree` to `.tree`, `.pg` to `.tree-group`, `.gh` to `.tree-head`, `.sub` to `.tree-sub` (the kit already has a `.sub`), `.pitems` to `.tree-rows`, `.pitem` to `.tree-row`, `.tag` to `.tree-tag`, `.sz` to `.tree-size`, `.also` to `.tree .also`, `.ptree .here` to `.tree-here`. Drop the mockup's page chrome (`body`, `.wrap`, `.cap`, `.lbl`, `.col`, `.ghost`). Decisions the mockup does not make:

- Columns align within a line: `.tree-rows` is a grid with `grid-template-columns: minmax(90px,max-content) minmax(140px,max-content) minmax(0,1fr) auto`, and each `.tree-row` is `grid-column: 1/-1; display: grid; grid-template-columns: subgrid`. `.tree-rows.no-item` drops the first column (`minmax(140px,max-content) minmax(0,1fr) auto`) and hides `.tree-item`.
- The fold: `.tree-rows > .fold` spans every column; `.tree-more` repeats the same column template (a `<details>` cannot pass a subgrid through, so folded rows align among themselves only; accepted). `.tree-more` has no margin, padding or border, so its rows' ticks meet the line's rail.
- `.tree-row.ext::before` has a dashed tick; `.tree-row.ext .tree-file` and its links are `var(--ink-3)`.
- `.tree-file` is `var(--font-mono)` at 12px; `.tree-size` right-aligned, muted, tabular numerals.
- The group head's hollow node and `.tree-here`'s filled node (`var(--accent)`, border `var(--accent-ink)`) sit on the main rail as in the mockup.

The block in kit.css ends with one blank line before the next section; `kit-css.js` gets the identical text (the third test checks this).

- [ ] **Step 5: Add the kit row in `web/src/views/kit.js`**

Right after the `Lineage thread` row: `row('Lineage tree', 'tree(groups, here)', 'One group per upstream run: its run chip, then each output and Selection it was read through, then a row per input. Ends on the item. Dashed tick: no store holds the file.', h('div', { class: 'kit-block' }, tree(...)))`, with demo values from the mockup's first example (a `fabulous_bartik` run chip via `runFact`, a `people` output ref and `"Friends"` Selection ref with `4 of 9 items, via`, rows Amina and Dev with `.dat` files and sizes, one row `ext`, Dev's tag `also` plus an `a.also` naming `sleepy_williams`, and `here` as `circle · 4` made by `goofy_dubinsky`). Use the CIDs already defined at the top of kit.js (`RUN`, `SELECTION`, `MANIFEST`, `RAW`, `RAW_2`); import `ref` from `../icons.js` and `runFact`, `tree` from `../components.js`.

- [ ] **Step 6: Run the tests to see them pass**

Run: `cd web && node --test test/components.test.mjs`
Expected: PASS, every test in the file.

- [ ] **Step 7: Rebuild the mockup pages and check the diff**

Run: `python3 -I docs/design/mockups/src/build.py && git diff --stat docs/design/mockups`
Expected: the `*.html` files change only by the added Lineage tree rules (check one with `git diff docs/design/mockups/01-components.html`). If anything else in them changes, restore them with `git checkout -- docs/design/mockups/*.html` and say so in your report.

- [ ] **Step 8: Run the whole web suite**

Run: `cd web && npm test`
Expected: PASS, no failures.

- [ ] **Step 9: Commit**

```bash
git add web/src/components.js docs/design/mockups/src/kit.css web/src/kit-css.js docs/design/mockups/*.html web/src/views/kit.js web/test/components.test.mjs
git commit -F - <<'EOF'
kit: tree(), the Lineage panel's provenance tree, with its CSS and a kit row

Groups on a main rail, lines indented under each, rows on a branch rail
with a tick each (dashed when no store holds the file), columns aligned
within a line; long lines fold behind "+n more". kit.css first, the same
rules in kit-css.js, the mockup pages rebuilt.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 4: The item page draws the tree

**Files:**
- Modify: `web/src/views/item.js` (`readOrigin`, delete `originNode` and `inputNodes`, the Lineage assembly at lines 391-408, the page section at line 445, move `now` up from line 424)
- Test: `web/test/item-lineage-view.test.mjs` (rewrite the Lineage tests; Used by tests unchanged)
- Modify: `web/test/item-view.test.mjs` (the `.fact` count at line 95) and `web/test/pages.test.mjs` (the `.fact` list at line 51)
- Modify: `DESIGN.md` (the "Explorer: Lineage and Used by (2026-10-26)" section, near line 6068)
- Modify: `docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md` (a note under `### 5.5`)

**Interfaces:**
- Consumes: `originKey`, `groupLineage`, `countsText`, `itemsText` from `../lineage.js` (Task 2); `tree` from `../components.js` (Task 3); `Explorer.collectionItemCount` (Task 1), called as `reader.collectionItemCount?.(collection)` so a fake without it works. All shapes and the DOM contract are in Shared interfaces.
- Produces: the item page DOM in Shared interfaces.

- [ ] **Step 1: Rework the test fixtures in `web/test/item-lineage-view.test.mjs`**

Keep `lineageEx`, `ctxWith`, `onePresenceCtx`, `usedByEx` and the Used by tests. Change:

```js
import { strayTexts } from './dom.mjs'   // beside installDom
import { shortCid } from '../src/icons.js'

const SLEEPY_PEOPLE = `bafyreisleepypeople${'x'.repeat(40)}`
const SLEEPY_RUN = `bafyreisleepyrun${'x'.repeat(43)}`
const KEEN_PEOPLE = `bafyreikeenpeople${'x'.repeat(42)}`
const KEEN_RUN = `bafyreikeenrun${'x'.repeat(45)}`
const MANY = Array.from({ length: 13 }, (_, i) => [`P${i}`, `bafyreimany${String(i).padStart(2, '0')}${'x'.repeat(46)}`])
// LABELS gains:
//   [SLEEPY_PEOPLE]: { run_name: 'sleepy_williams', output: 'people', completion: SLEEPY_RUN, status: 'succeeded' },
//   [KEEN_PEOPLE]:   { run_name: 'keen_hopper', output: 'people', completion: KEEN_RUN, status: 'succeeded' },
// TITLES gains ...Object.fromEntries(MANY.map(([n, c]) => [c, n]))

const from = (item_cid, collection_cid = PEOPLE, selection_cid = FRIENDS) => ({ item_cid, collection_cid, selection_cid })
const input = (position, name, origins = []) => ({ position, role: 'path', name, address: DEV_DAT, size: 10, sample: null, origins })
```

`lineageEx` gains two options, `inputs = null` (when given, every item's lineage inputs; otherwise today's `inputsOf`) and `neverPublished = []` (names added to the leaves of `INTRO_DEV` and `CIRCLE` as `{ kind: 'Leaf', name, size: null, address: null, reason: 'never_published' }`).

Replace the helpers `thread`, `nodesOf`, `anchors`, `hrefs` with:

```js
const lineagePanel = (node) => node.querySelectorAll('section.panel').find(p => p.querySelector('h2')?.textContent === 'Lineage')
const metaOf = (node) => lineagePanel(node).querySelector('.ph').querySelector('.muted')?.textContent ?? null
const groupsOf = (node) => lineagePanel(node).querySelectorAll('[data-group]')
const groupKeys = (node) => groupsOf(node).map(g => g.getAttribute('data-group'))
const rowsOf = (el) => el.querySelectorAll('.tree-row')
const cells = (row) => row.children.map(c => c.textContent)
const subOf = (el) => el.querySelector('.tree-sub').children.map(c => c.textContent)
const hereOf = (el) => el.querySelectorAll('.tree-here').map(n => n.children.map(c => c.textContent))
const hrefsIn = (el) => el.querySelectorAll('a').map(a => a.getAttribute('href'))
```

- [ ] **Step 2: Rewrite today's Lineage tests against the tree**

Each keeps its subject; the assertions below replace the thread ones.

```js
test('intros · Dev: one group under infallible_spence, the people line via "friends", Dev and Dev_Patel.dat, ending on the item made by consumer_run', async () => {
  const node = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: { [PEOPLE_DEV]: ['roblab'], [DEV_DAT]: ['roblab'] } }))
  assert.deepEqual(groupKeys(node), [`run:${PEOPLE_RUN}`])
  const [group] = groupsOf(node)
  assert.match(group.querySelector('.tree-head').textContent, /infallible_spence/)
  const line = group.querySelector('.tree-line')
  assert.equal(line.getAttribute('data-line'), `${PEOPLE}:${FRIENDS}`)
  assert.deepEqual(subOf(line), ['people', '1 item, via', '"friends"'])
  assert.ok(hrefsIn(line.querySelector('.tree-sub')).includes(`#/collection/${PEOPLE}`))
  assert.ok(hrefsIn(line.querySelector('.tree-sub')).includes(`#/selection/${FRIENDS}`))
  const [row] = rowsOf(line)
  assert.equal(row.getAttribute('data-row'), '0')
  assert.deepEqual(cells(row), ['Dev', 'Dev_Patel.dat', '', '10 B'])
  assert.ok(hrefsIn(row).includes(`#/item/${PEOPLE}/${PEOPLE_DEV}`))
  assert.ok(hrefsIn(row).includes(`#/content/${DEV_DAT}`))
  assert.equal(row.classList.contains('ext'), false)
  assert.deepEqual(hereOf(lineagePanel(node)), [['intros · Dev', 'made by', 'consumer_run']])
  assert.equal(metaOf(node), '1 upstream run · 1 input')
  assert.deepEqual(strayTexts(lineagePanel(node)), [])
})
```

- `circle: four rows under one line, by position`: `groupsOf(node).length === 1`; `rowsOf(lineagePanel(node)).map(r => r.getAttribute('data-row'))` is `['0','1','2','3']`; item cells `['Dev','Ana','Bo','Cy']`; `metaOf(node) === '1 upstream run · 4 inputs'`.
- `an upstream item held only in member B links there, and its group asks B for the pipeline`: as today's B test (the open member's Explorer is `lineageEx({ reads: c => c !== PEOPLE_DEV })`), with B's Explorer able to read it: `explorers: { B: Object.assign(lineageEx(), { runIdentity: async () => ({ pipeline: 'b-pipeline' }) }) }`; the row's item href is `?member=B#/item/${PEOPLE}/${PEOPLE_DEV}`; `groupsOf(node)[0].querySelector('.tree-head').children[1].textContent === 'b-pipeline'`.
- `the run chip of an upstream item held only in member B opens the run there`: `groupsOf(node)[0].querySelector('.sealed-fact').getAttribute('href') === `?member=B#/run/${PEOPLE_RUN}``.
- `held nowhere: short CID and "not in any store here", unlinked; the run still heads the group, opening here; the row is dashed` (fixture as today's): group key `run:${PEOPLE_RUN}`; its `.sealed-fact` href `#/run/${PEOPLE_RUN}`; the row's `children[0].textContent` includes `shortCid(PEOPLE_DEV)` and matches `/not in any store here/`; no href in the row contains `#/item/`; the row has class `ext`.
- `a Selection pick by query: a group headed by the Selection, "picked by query", no run chip; the row links #/item/-/`: `lineageEx({ collection: null })`; `groupKeys` is `[`query:${FRIENDS}`]`; the head's children text `['"friends"', 'picked by query']`; no `.sealed-fact` and no `.tree-sub` in the group; the group's hrefs include `#/item/-/${PEOPLE_DEV}`; `metaOf(node) === '1 input'`.
- `lineage is scoped to the URL run; - draws a tree per run in its own wrapper`: today's `calls.itemLineage` and `[data-lineage-run]` count assertions, plus `metaOf(scoped) === '1 upstream run · 1 input'`, `metaOf(across) === '2 runs recorded lineage'`; for each wrapper, `children.length === 2`, `children[0].hasAttribute('data-lineage-counts')` with text `1 upstream run · 1 input`, `children[1].classList.contains('tree')`; `hereOf` of the CONSUMER wrapper is `[['item · Dev', 'made by', 'consumer_run']]` and of the CONSUMER_2 wrapper `[['item · Dev', 'made by', 'second_run']]`.
- `a run without lineage shows the item alone` and `an Explorer without itemLineage shows the item alone`: `groupsOf(node).length === 0`; `hereOf(lineagePanel(node))` is `[['intros · Dev', 'made by', 'consumer_run']]`; `metaOf(node) === null`.
- `an input with no Origin goes under Read by path, file only`: `groupKeys` is `['path']`; the head text `Read by path`; `cells(row)` is `['', 'Dev_Patel.dat', '', '10 B']`; the line's `.tree-rows` has class `no-item`; `metaOf(node) === '1 input'`.
- `an input with neither name nor address gets a fixed label`: the path row's `children[1].textContent === 'unnamed input'` and the row has class `ext`.
- `a run whose lineage cannot be drawn shows only its item; the page and the other run still render`: two wrappers; CONSUMER's has `[data-group]` keys `[`run:${PEOPLE_RUN}`]`; CONSUMER_2's has no `[data-group]`, no `[data-lineage-counts]`, and `hereOf` `[['item · Dev', 'made by', 'second_run']]`.
- `both members hold the upstream item: the link stays in the open member even when it is listed second`: today's assertions over `hrefsIn(lineagePanel(node))`.
- `a file only member B holds is not dashed; one no member holds is`: `lineagePanel(n).querySelectorAll('.tree-row.ext').length` is 0, then 1.
- The two real-presence tests: the row whose `children[1].textContent === 'Dev_Patel.dat'` has (does not have) class `ext`.
- `an Origin repeated across inputs is read once`: unchanged.
- `two inputs sharing an Origin are two rows under one line, the item counted once`: rows `['0','1']`, file cells `['Dev_Patel.dat','again.dat']`, `subOf(line)[1] === '1 item, via'`, `metaOf(node) === '1 upstream run · 2 inputs'`, and `rowsOf(p)[0].children[0].children[0] !== rowsOf(p)[1].children[0].children[0]` (different elements).

- [ ] **Step 3: Add the new tests**

```js
test('two upstream runs: a group each, by lowest position; a row under both names the other run', async () => {
  const inputs = [input(0, 'Dev.dat', [from(PEOPLE_DEV)]), input(1, 'Ana.dat', [from(PEOPLE_ITEMS[1][1]), from(PEOPLE_ITEMS[1][1], SLEEPY_PEOPLE)]),
    input(2, 'Bo.dat', [from(PEOPLE_ITEMS[2][1])]), input(3, 'Cy.dat', [from(PEOPLE_ITEMS[3][1], SLEEPY_PEOPLE)])]
  const node = await page(lineageEx({ inputs }), CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld }))
  assert.deepEqual(groupKeys(node), [`run:${PEOPLE_RUN}`, `run:${SLEEPY_RUN}`])
  assert.deepEqual(groupsOf(node).map(g => rowsOf(g).map(r => r.getAttribute('data-row'))), [['0', '1', '2'], ['1', '3']])
  const tag = (g, position) => rowsOf(groupsOf(node)[g]).find(r => r.getAttribute('data-row') === position).querySelector('.tree-tag')
  assert.equal(tag(0, '1').children[0].textContent, 'also')
  assert.equal(tag(0, '1').querySelector('a.also').textContent, 'sleepy_williams')
  assert.equal(tag(0, '1').querySelector('a.also').getAttribute('href'), `#/run/${SLEEPY_RUN}`)
  assert.equal(tag(1, '1').querySelector('a.also').textContent, 'infallible_spence')
  assert.equal(tag(0, '0').textContent, '')
  assert.equal(metaOf(node), '2 upstream runs · 4 inputs')
})

test('three runs: the tag counts the two others, their names in its title, and is not a link', async () => {
  const ana = PEOPLE_ITEMS[1][1]
  const inputs = [input(0, 'Dev.dat', [from(PEOPLE_DEV)]), input(1, 'Ana.dat', [from(ana), from(ana, SLEEPY_PEOPLE), from(ana, KEEN_PEOPLE)])]
  const node = await page(lineageEx({ inputs }), CONSUMER_CIRCLE, CIRCLE, ctxWith({ held: allHeld }))
  assert.deepEqual(groupKeys(node), [`run:${PEOPLE_RUN}`, `run:${SLEEPY_RUN}`, `run:${KEEN_RUN}`])
  const box = (g) => rowsOf(groupsOf(node)[g]).find(r => r.getAttribute('data-row') === '1').querySelector('.tree-tag')
  assert.equal(box(0).querySelector('a.also'), null)
  assert.equal(box(0).querySelector('span.also').textContent, '2 other runs')
  assert.equal(box(0).querySelector('span.also').getAttribute('title'), 'sleepy_williams, keen_hopper')
  assert.equal(box(2).querySelector('span.also').getAttribute('title'), 'infallible_spence, sleepy_williams')
})
```

- `the line says k of m from the collection's count; without one, k items; never of 0`: `circle` with `ex.collectionItemCount = async (c) => (c === PEOPLE ? 9 : null)` gives `subOf(line)[1] === '4 of 9 items, via'`; with `async () => 0`, with `async () => { throw new Error('unreadable') }`, and with no method, `'4 items, via'` and the page renders.
- `the count is asked of the member that holds the collection`: the B fixture (open member `lineageEx({ reads: c => c !== PEOPLE_DEV })`, `held: { [PEOPLE_DEV]: ['B'] }`) with `explorers: { B: Object.assign(lineageEx(), { collectionItemCount: async () => 9 }) }` and no method on the open member's Explorer gives `'1 of 9 items, via'`.
- `an unnamed Selection on the line shows its short CID`: `ex.claimStates = async (cids) => new Map(cids.map(c => [c, claimState([])]))`; `subOf(line)[2] === shortCid(FRIENDS)`; `line.querySelector('.tree-sub').querySelector('code').getAttribute('title') === FRIENDS`; the sub line's text has no `"`.
- `the group head names the upstream pipeline and the age; without the pipeline, the age alone`: `ex.runLabel` answers PEOPLE with `{ ...LABELS[PEOPLE], finished_at: new Date(Date.now() - 2 * 86_400_000).toISOString() }` (others as before); the head's `children[1].textContent === 'p · 2d'`; with `ex.runIdentity` rejecting for `PEOPLE_RUN` (and answering `{ run_name: 'consumer_run', pipeline: 'p' }` otherwise), `'2d'`.
- `never-published leaves: one named like a path input is that row; the others follow, and none counts as an input`: `lineageEx({ inputs: [input(0, 'Dev_Patel.dat')], neverPublished: ['Dev_Patel.dat', 'extra.txt'] })`; `groupKeys` `['path']`; rows `data-row` `['0', 'leaf']`; file cells `['Dev_Patel.dat', 'extra.txt']`; the leaf row has class `ext`; `metaOf(node) === '1 input'`. Then `lineageEx({ runs: [], neverPublished: ['in.fq'] })` gives one path row `leaf` with file `in.fq` and `metaOf(node) === null`.
- `a line with 13 rows shows 8 and folds 5 behind +5 more; 12 rows fold none`: inputs `MANY.slice(0, n).map(([name, cid], i) => input(i, `${name}.dat`, [from(cid)]))`; for 13, the line's `.tree-rows` has 8 direct `.tree-row` children (`box.children.filter(c => c.classList.contains('tree-row'))`), its `details` has `data-fold` `lineage`, summary `+5 more` and 5 `.tree-row`; for 12, no `details` and 12 rows.
- `Read by path folds like a line`: 13 Origin-less inputs (`MANY.map(([name], i) => input(i, `${name}.dat`))`) give the path line 8 shown rows and a `+5 more` fold.
- `rows of one run held in two members each link their own member`: circle with `reads: c => c !== PEOPLE_DEV`, `held: { ...allHeld, [PEOPLE_DEV]: ['B'] }`, `hrefIn: (alias, h) => (alias === 'roblab' ? h : `?member=${alias}${h}`)`; row 0's item href starts with `?member=B#/item/`, row 1's is `#/item/${PEOPLE}/${PEOPLE_ITEMS[1][1]}`.
- `an Origin naming neither collection nor Selection: picked by query alone, no stray text`: `inputs: [input(0, 'Dev_Patel.dat', [from(PEOPLE_DEV, null, null)])]`; `groupKeys` `['query:-']`; head children text `['picked by query']`; `strayTexts(lineagePanel(node))` is `[]`.
- `Lineage spans the column above a grid-2 of Produced by and Used by; without Used by, Produced by alone`:

```js
  const node = await page(usedByEx(), PEOPLE, PEOPLE_DEV, ctxWith({ held: allHeld, members: ['roblab'] }))
  const lineage = lineagePanel(node)
  const grid = node.children.find(c => c.classList.contains('grid-2'))
  assert.ok(node.children.includes(lineage), 'Lineage is a child of the page, not of the grid')
  assert.ok(node.children.indexOf(lineage) < node.children.indexOf(grid))
  assert.deepEqual(grid.children.map(p => p.querySelector('h2').textContent), ['Produced by', 'Used by'])
  const alone = await page(lineageEx(), CONSUMER_INTROS, INTRO_DEV, ctxWith({ held: allHeld }))
  assert.deepEqual(alone.children.find(c => c.classList.contains('grid-2')).children.map(p => p.querySelector('h2').textContent), ['Produced by'])
```

- [ ] **Step 4: Run the file to see the Lineage tests fail**

Run: `cd web && node --test test/item-lineage-view.test.mjs`
Expected: FAIL on every Lineage test (no `.tree` yet); the Used by tests pass.

- [ ] **Step 5: Implement in `web/src/views/item.js`**

- `readOrigin` also returns `reader` (the Explorer it read from) and no longer returns `coll`. Its cache key is `originKey(origin)`.
- Delete `originNode` and `inputNodes`. Keep `viaSelection` and `itemLinkText`, which Used by uses. Replace the `thread` import with `tree`; import `originKey`, `groupLineage`, `countsText`, `itemsText` from `../lineage.js`.
- `/** A hash opened in the member that holds it, or here when none does. */ const inHolder = (ctx, holder, hash) => (holder ? hrefIn(ctx, holder, hash) : hash)`. Every link the tree makes to an upstream run, collection, Selection or item goes through it with the Read's (or group's, or Also's) holder; `#/content/` links stay plain, as today.
- `async function lineageTree(ex, ctx, pres, run, { leaves, here, cache, now, itemCid }) -> { node, runs, inputs }`, where `cache = { origins, identities, counts, held }` are Maps shared by every tree on the page (Origin reads by `originKey`; `runIdentity` answers by `${holder} ${completion}`; `collectionItemCount` answers by `${holder} ${collection}`; held-anywhere by address, from `pres.states(address, { hereHeld: null })` as `inputNodes` did). It reads every Origin of `run.inputs`, calls `groupLineage(run.inputs, reads, leaves)`, then fetches, per run group, `reader.runIdentity?.(completion)` and, per line with a collection, `reader.collectionItemCount?.(collection)`, each `.catch(() => null)`. A null `run` (no lineage) groups `[]`. When reading or grouping throws (a run whose lineage cannot be drawn), it falls back to `groupLineage([], new Map(), leaves)`.
- Cells and heads follow the DOM contract in Shared interfaces:
  - run head `[runFact(label, completion, null, { href: inHolder(ctx, holder, `#/run/${completion}`) }), meta]`, where, with `age = ago(label?.finished_at, now)`, meta is `h('span', { class: 'muted small' }, pipeline, pipeline && age ? ' · ' : null, age)`, and null when both are missing (so `p`, `2d` or `p · 2d`, never a dangling ` · `);
  - line head `[label?.output ? ref('output', label.output, inHolder(ctx, holder, `#/collection/${collection}`), { cid: collection }) : null, h('span', { class: 'muted small' }, `${itemsText(items, m)}${selection ? ', via' : ''}`), selection ? selectionRef : null]`; the Selection ref is `ref('selection', names.length ? `"${names.join(' / ')}"` : h('code', { class: 'cid', title: selection }, shortCid(selection)), inHolder(ctx, holder, `#/selection/${selection}`), { cid: selection })`;
  - item cell `ref('item', read.title ?? shortCid(read.itemCid), inHolder(ctx, read.holder, `#/item/${read.collection ?? '-'}/${read.itemCid}`), { cid: read.itemCid })` when `read.holder` is set, else the unlinked `ref('item', h('code', { class: 'cid', title: itemCid }, shortCid(itemCid)), null, { cid: itemCid })` and `h('span', { class: 'muted small' }, 'not in any store here')`; path and leaf rows have a null item cell;
  - file cell `ref('file', name ?? shortCid(address), `#/content/${address}`, { cid: address })` when there is an address, else the name, the leaf's name, or `unnamed input`; `ext` when there is no address or no member holds it;
  - tag cell `[h('span', { class: 'muted small' }, 'also'), tag]`: one Also is `h('a', { class: 'also', href: inHolder(ctx, also.holder, `#/run/${also.completion}`) }, seal(also.completion, { size: 12 }), also.label?.run_name ?? 'unnamed run')`; two or more are `h('span', { class: 'also', title: <run names joined ', '> }, `${n} other runs`)`; null when `also` is empty;
  - size cell `bytes(input.size)` when the input has a size, else null.
- Each line folds as the metadata does: when `rows.length - SHOWN_PAIRS >= MIN_FOLDED`, the first `SHOWN_PAIRS` rows are shown and the rest passed as `folded`, with `foldKey` `lineage:${line.key}`, or `lineage:path:${itemCid}` for the path line.
- `here` is built fresh for each tree: `[ref('item', h('b', {}, `${from?.output ?? 'item'} · ${title}`), null, { cid: itemCid }), ...madeBy]`, `madeBy` being `[h('span', { class: 'muted small' }, 'made by'), chip]` or `[]`. The chip is `runFact(from, from.completion)` when the URL names a run; in the `-` view, each tree's chip is `runFact(identity, run.completion_cid)` from `ex.runIdentity` for that consumer run (caught to null), as the wrapper's chip was. With no run known there is no `made by`.
- Assembly: leaves are `it.leaves.filter(l => l.reason === 'never_published' && l.name).map(l => l.name)`. With fewer than two lineage runs, one tree over `lineageRuns[0] ?? null`, and the panel's meta is `countsText(runs, inputs)`. With two or more, a tree per run, each in `h('div', { 'data-lineage-run': completion }, countsLine, tree)` where `countsLine` is `h('p', { class: 'muted small', 'data-lineage-counts': '' }, text)` or null when `countsText` is null; the panel's meta is `${lineageRuns.length} runs recorded lineage`. Move `const now = ex.now?.() ?? Date.now()` above the Lineage code.
- Page: `head, projects.node, filesPanel, sheet, lineage, h('div', { class: 'grid-2' }, producedBy, usedBy), inSelections, ...`; the separate `usedBy` after the grid goes.

- [ ] **Step 6: Run the file to see it pass**

Run: `cd web && node --test test/item-lineage-view.test.mjs`
Expected: PASS, every test.

- [ ] **Step 7: Fix the two page tests that count every `.fact` on the page**

The Lineage item node now carries a run chip, so `web/test/item-view.test.mjs` (`item page: Produced by names the runs ...`, line 95) and `web/test/pages.test.mjs` (`the item page: a de-duplicated title ...`, line 51) take their `.fact` list from the Produced by panel only: `page.querySelectorAll('section.panel').find(p => p.querySelector('h2')?.textContent === 'Produced by').querySelectorAll('.fact')`. Their assertions stay as they are.

Run: `cd web && npm test`
Expected: PASS, no failures. Any other failure is a test reading the old thread or the old grid; fix it the same way and name it in your report.

- [ ] **Step 8: Update DESIGN.md and the cross-run spec**

In `DESIGN.md`, section "Explorer: Lineage and Used by (2026-10-26)": after the line naming the 2026-10-26 spec, add that the Lineage panel's layout is `docs/superpowers/specs/2026-10-28-lineage-provenance-tree-design.md`. Replace the first five bullets (thread, null collection, held by a member, run scoping, runs without ItemLineage) with bullets in the section's voice that say:

- the panel spans the main column above Produced by and Used by, and draws a tree: one group per upstream run (its run chip, linked in the member that holds it, then the pipeline and age), under it a line per output and Selection the run's rows came through (`<k> of <m> items, via` and the Selection's name from its Claims, short CID when unnamed), a row per input position (the upstream item's title, the file, an `also` tag naming the one other run or counting two or more, the size), ending on the item `made by` its run. The header says `<R> upstream run(s) · <N> input(s)`; rows may repeat across groups, the header counts inputs. Groups are ordered by their lowest input position. A line longer than the metadata fold allows folds behind `+<n> more`.
- an Origin with a null collection names no run: its rows go in a group headed by the Selection and `picked by query`, linking `#/item/-/<item>`.
- an upstream item held by no member keeps its run group when the run label can be read, its row showing the short CID and `not in any store here` unlinked; when the run label cannot be read either, its group is headed by the collection's short CID and `run not in any store here`.
- run scoping as now; with `-`, a tree per run that recorded lineage, each in its own wrapper with its own counts and its own `made by`, the panel saying `<C> runs recorded lineage`; one run draws one tree.
- inputs with no Origin and the item's never-published leaves go in a last `Read by path` group, a leaf named like such an input being that input's row; leaves do not count as inputs. Runs without an ItemLineage, or whose lineage cannot be drawn, show only that group and the item. The generic block page sentence about ItemLineage stays.

Leave the Used by bullet as it is.

In `docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md`, directly under `### 5.5 Explorer: the Lineage panel`, add:

```
*Layout superseded on 2026-10-28 by
`docs/superpowers/specs/2026-10-28-lineage-provenance-tree-design.md`: the
panel draws one group per upstream run instead of one chain per input. The
rules below about what may be named (no guessing, picked by query, held
nowhere, run scoping) still hold.*
```

- [ ] **Step 9: Run the whole suite again**

Run: `cd web && npm test`
Expected: PASS (`npm test` regenerates `web/src/generated/schema.json` from DESIGN.md's ipldsch block, which this task does not touch, so `git status` shows no change there).

- [ ] **Step 10: Commit**

```bash
git add web/src/views/item.js web/test/item-lineage-view.test.mjs web/test/item-view.test.mjs web/test/pages.test.mjs DESIGN.md docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md
git commit -F - <<'EOF'
explorer: the Lineage panel draws one group per upstream run, across the main column

Each upstream run is named once, with its pipeline and age; under it a
line per output and Selection ("4 of 9 items, via") and a row per input
(item, file, an "also" tag for the other runs that hold it, size). The
item ends the tree, made by its run. Picked-by-query, unlabelled and
Read by path groups follow. Lineage leaves the grid-2 row, which now
holds Produced by and Used by.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 5: Visual check against mockup.png

**Files:**
- Scratch only (screenshots, the store copy, the Playwright script). Fixes, if any, in `docs/design/mockups/src/kit.css` then `web/src/kit-css.js` (and `docs/design/mockups/*.html` rebuilt), or in `web/src/views/item.js` / `web/src/components.js` with their tests.

**Interfaces:**
- Consumes: the built page (`web/dist/index.html`), Task 4's DOM, `gate/browser/serve.py`, Playwright in `gate/browser/node_modules`.
- Produces: four screenshots in scratch, compared with `docs/superpowers/specs/2026-10-28-lineage-provenance-tree/mockup.png`.

Hard rules for this task:

- Never open, serve or point anything at `/tmp/cas-exploration/store` itself. Copy it with `cp -R` to a scratch directory first; plain reads are the only thing allowed on the original.
- Any `sqlite3` on a real store, the copy included, uses `-readonly`.
- Set `XDG_CACHE_HOME` to a scratch directory for anything that runs nf-blocks. (This task runs none; the copied store already holds `index/v7.sqlite`, so nothing indexes and nothing writes to `~/.cache`.)
- The Range server is required; `python -m http.server` will not do.
- Screenshots go in the scratch directory, never in the repo.
- Kill the Range server afterwards and confirm the port is free with `lsof -iTCP:<port> -sTCP:LISTEN` (no output).

- [ ] **Step 1: Copy the store, build, serve (spec §7's recipe, verbatim)**

Pick a scratch directory `S` (your session scratchpad, or `S=$(mktemp -d)`), and a port that `lsof -iTCP:<port> -sTCP:LISTEN` shows free. From the repository root:

```
S=<scratch dir>
cp -R /tmp/cas-exploration/store "$S/store"
(cd web && npm run build) && cp web/dist/index.html "$S/store/index.html"
python3 -I gate/browser/serve.py "$S/store" <port> &
# open http://127.0.0.1:<port>/index.html#/item/<collection>/<item>
# with Playwright from gate/browser/node_modules, wait ~4s, screenshot
```

Note the server's PID (`$!`) for Step 5.

- [ ] **Step 2: Screenshot both example items at 1280px and 900px**

Write `$S/shoot.mjs` (outside the repo) with a quoted heredoc:

```bash
cat > "$S/shoot.mjs" <<'EOF'
// Screenshots the item page's two lineage examples at 1280px and 900px: the whole page and the Lineage panel.
import { pathToFileURL } from 'node:url'
const [repo, port, out] = process.argv.slice(2)
const { chromium } = await import(pathToFileURL(`${repo}/gate/browser/node_modules/playwright/index.mjs`).href)
const ITEMS = {
  circle4: 'bafyreidntpqvqzc6ebpiqh63byqjomrqrk7nf7afqwwxsp65rj6vc5osjm/bafyreiez7zewl7goyaaclrefrzs3rbwfycz4rmxydrpfmzijaujdihr76i',
  circle9: 'bafyreih2xrnc7rm4bbj3fvrxsmdr6j35nqzlmytzpdwzfx5kg7sgqrs32q/bafyreidgx4yv434qpepw3z6qzpzihsupde25g5xwbaz3wevewhkizy4oum',
}
const browser = await chromium.launch()
const errors = []
for (const width of [1280, 900]) {
  for (const [name, path] of Object.entries(ITEMS)) {
    const page = await browser.newPage({ viewport: { width, height: 900 } })
    page.on('pageerror', e => errors.push(`${name}@${width}: ${e}`))
    await page.goto(`http://127.0.0.1:${port}/index.html#/item/${path}`)
    await page.waitForTimeout(4000)
    await page.screenshot({ path: `${out}/${name}-${width}.png`, fullPage: true })
    await page.locator('section.panel', { has: page.locator('h2', { hasText: /^Lineage$/ }) }).screenshot({ path: `${out}/${name}-${width}-lineage.png` })
    await page.close()
  }
}
await browser.close()
if (errors.length) { console.error(errors.join('\n')); process.exit(1) }
console.log('wrote 8 screenshots to', out)
EOF
node "$S/shoot.mjs" "$PWD" <port> "$S"
```

Expected: `wrote 8 screenshots to <S>`, no page errors.

- [ ] **Step 3: Compare with mockup.png**

Look at `mockup.png` and each screenshot with the Read tool. What should match, from the spec:

- `circle · 4`: meta `1 upstream run · 4 inputs`; one group `fabulous_bartik` with `scratch-workflow · <age>`; the line `people`, `4 of 9 items, via`, `"Friends"`; rows Amina, Dev, Femi, Greta with their `.dat` files and sizes (34 B, 28 B, 32 B, 31 B) right-aligned; the tree ends on `circle · 4` `made by` `goofy_dubinsky`.
- `circle · 9`: meta `2 upstream runs · 9 inputs`; `fabulous_bartik` with nine rows (no fold), Amina, Dev and Femi tagged `also` `sleepy_williams`; then `sleepy_williams` with `3 of 9 items, via` `"Community"` and Amina, Dev, Femi tagged `also` `fabulous_bartik`; ends on `circle · 9` `made by` `magical_ritchie`.
- Both: Lineage spans the main column; below it, a `grid-2` row with Produced by (and Used by when the item has one); rails, hollow and filled nodes, ticks, the boxed `also` tag and aligned columns as in the picture. At 900px the grid is one column (below 1180px) and the rows stay readable with no horizontal scroll inside the panel.

Known, accepted differences (spec §6): the age reads `2d` (or however long ago the run now is), not "2 days ago"; in the real data Amina shows `also sleepy_williams`, not the mockup's `3 other runs` (keen_hopper and brave_lovelace are made up).

If what you see differs from the real data described above (an item or Selection missing from the copy), stop and report it; do not edit data.

- [ ] **Step 4: Fix differences, then shoot again**

CSS differences: change `docs/design/mockups/src/kit.css` first, then the same text in `web/src/kit-css.js`, then `python3 -I docs/design/mockups/src/build.py`. DOM differences: change `web/src/views/item.js` or `web/src/components.js` and keep their tests passing. After each change: `cd web && npm test` (PASS), repeat the build and copy lines of Step 1 (the server can keep running; it serves the new `index.html`), then Step 2 and Step 3.

- [ ] **Step 5: Stop the server and confirm the port is free**

Run: `kill <pid> ; lsof -iTCP:<port> -sTCP:LISTEN`
Expected: no output from `lsof`. Then `rm -rf "$S/store"` (the copy only). Keep the screenshots in `$S` and give their absolute paths in your report.

- [ ] **Step 6: Commit (only if Step 4 changed anything)**

```bash
git add <the files you changed>
git commit -F - <<'EOF'
kit: the Lineage tree matches the agreed picture at 1280px and 900px

<one line per fix>

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

Report what you compared, what you fixed, and anything left different from the picture with the reason.
