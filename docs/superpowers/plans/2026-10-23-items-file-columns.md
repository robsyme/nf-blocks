# Items Table File Columns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the items page's single "files" chip column with one column per top-level file key (or tuple position), each file drawn as a fixed-size pill that links to its block page and shows its field path, name and size on hover.

**Architecture:** A new pure module `web/src/slots.js` walks an OutputItem's value into file slots (column key, field path, Leaf details). `Previews` stores those slots with each preview. `table.js` gains two pure functions that decide the file columns across loaded previews and the slots one cell shows. `views/items.js` draws the columns and pills; `fileChips` is removed.

**Tech Stack:** Plain ES modules in `web/src`, `multiformats` CID, `node:test` with the fake DOM in `web/test/dom.mjs`. Run tests from `web/` with `npm test` (339 passing on `main` at 7ac32f8).

**Spec:** `docs/superpowers/specs/2026-10-23-items-file-columns-design.md`

## Global Constraints

- No new request per row, cell or hover: everything comes from the item block `Previews` already loads.
- A pill link is `#/block/<address>` with no `download` attribute.
- At most 5 pills per cell, then `+N` linking to `#/item/<collection or '-'>/<item>`.
- File columns: record keys sorted alphabetically, then tuple positions `[i]` sorted numerically; no cap on their number.
- A cell with no slot shows `—` (muted). A slot with no address is a non-link pill with a dashed outline.
- Field paths: dotted keys, list positions as `[i]` (`samtools.stats`, `ballgown[3]`, `[1]`, `[0].ref`).
- Hover text (`title` and `aria-label`, lines joined by `\n`): addressed slot `path`, `name`, `humanBytes(size)`; address-less slot `path`, `leafReasonText(reason)`, `name`; empty parts dropped.
- `table.js` and `slots.js` stay pure (no DOM, no fetch).
- The run page's suffix column, the item page's file table and the metadata columns are unchanged.

## Review Focus

- A record key holding an empty list or a plain `null` (rnaseq's `aligned` has `wig: []`) yields no slot and so no column of its own; pinned in Task 1.
- A Leaf address arriving as a CID object or as text must give the same `address` text and the same directory flag; pinned in Task 1.
- A Leaf inside a tuple's meta map (`[{ sample, ref: Leaf }, bam]`) gives position `[0]` a column, with path `[0].ref`; pinned in Task 1.
- Rows still loading, or whose preview failed, must span the file columns as well, so the table does not shear; pinned in Task 3.
- `+N` from an item reached with no collection (`via` empty) links to `#/item/-/<item>`, as the Open link does; pinned in Task 3.

---

### Task 1: File slots from an item value, carried by Previews

**Files:**
- Create: `web/src/slots.js`
- Modify: `web/src/previews.js` (the `done.set` in `pump`, ~line 68)
- Test: `web/test/slots.test.mjs` (new), `web/test/previews.test.mjs`

**Interfaces:**
- Consumes: `isLeaf` from `web/src/metadata.js`; `DAG_CBOR` from `web/src/cid.js`.
- Produces:
  - `fileSlots(value: any) -> Slot[]`, in `web/src/slots.js`, where
    `Slot = { column: string, path: string, name: string|null, size: number|null, address: string|null, dir: boolean, reason: string|null }`.
    Walk order is the decoded value's own order (`Object.entries` for maps, index order for lists).
  - Each loaded preview gains `slots: Slot[]` (`[]` when the item has no `value`), next to the existing `pairs`, `files`, `dirs`.

- [ ] **Step 0: Branch and commit the spec**

```bash
git switch -c web/file-columns
git add docs/superpowers/specs/2026-10-23-items-file-columns-design.md docs/superpowers/plans/2026-10-23-items-file-columns.md
git commit -m "docs: spec and plan for the items table's file columns"
```

- [ ] **Step 1: Write the failing tests** in `web/test/slots.test.mjs`

```js
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { fileSlots } from '../src/slots.js'
import { block, rawCid } from './fixture.mjs'

const leaf = (name, address, size = 10, reason = null) => ({ kind: 'Leaf', name, address, size, reason })
const bam = rawCid('bam'); const bai = rawCid('bai'); const st = rawCid('stats')

test('record: one column per top-level key, dotted paths, list positions in brackets', () => {
  const slots = fileSlots({ id: 'WT_REP1', meta: { id: 'WT_REP1' }, bam: leaf('a.bam', bam), bai: leaf('a.bai', bai),
    samtools: { stats: leaf('a.stats', st) }, ballgown: [leaf('e.ctab', bam), leaf('t.ctab', bai)] })
  assert.deepEqual(slots.map(s => [s.column, s.path]), [
    ['bam', 'bam'], ['bai', 'bai'], ['samtools', 'samtools.stats'], ['ballgown', 'ballgown[0]'], ['ballgown', 'ballgown[1]']])
  assert.deepEqual(slots[0], { column: 'bam', path: 'bam', name: 'a.bam', size: 10, address: bam.toString(), dir: false, reason: null })
})

test('tuple: one column per position that holds a Leaf; the meta map holds none', () => {
  const slots = fileSlots([{ sample: 'A', lane: 1 }, leaf('A.bam', bam), [leaf('A.bai', bai)]])
  assert.deepEqual(slots.map(s => [s.column, s.path]), [['[1]', '[1]'], ['[2]', '[2][0]']])
})

test('a Leaf inside a tuple meta map gives position [0] a column (Review Focus 3)', () => {
  assert.deepEqual(fileSlots([{ sample: 'A', ref: leaf('ref.fa', bam) }, leaf('A.bam', bai)]).map(s => s.path), ['[0].ref', '[1]'])
})

test('a bare Leaf is column [0]', () => {
  assert.deepEqual(fileSlots(leaf('x.txt', bam)).map(s => [s.column, s.path]), [['[0]', '[0]']])
})

test('empty lists, nulls and scalars give no slot (Review Focus 1)', () => {
  assert.deepEqual(fileSlots({ id: 'x', wig: [], junction: null, n: 3 }), [])
  assert.deepEqual(fileSlots(undefined), [])
})

test('an address-less Leaf is a slot with its reason; a dag-cbor address, as object or text, is a directory (Review Focus 2)', () => {
  const [declined] = fileSlots({ json_info: leaf(null, null, null, 'declined') })
  assert.deepEqual(declined, { column: 'json_info', path: 'json_info', name: null, size: null, address: null, dir: false, reason: 'declined' })
  const dir = block({ kind: 'DirectoryManifest' }).cid
  for (const given of [dir, dir.toString()]) {
    const [s] = fileSlots({ qualimap: leaf('qc', given) })
    assert.equal(s.dir, true)
    assert.equal(s.address, dir.toString())
  }
})
```

Add to `web/test/previews.test.mjs`:

```js
test('a preview carries the item value\'s file slots, or none when the item has no value', async () => {
  const ex = { item: async (c, i) => (i === 'i1'
    ? { view: null, leaves: [], value: { bam: { kind: 'Leaf', name: 'a.bam', address: rawCid('a'), size: 3, reason: null } } }
    : { view: null, leaves: [] }) }
  const p = new Previews(ex)
  p.ask('coll', 'i1'); p.ask('coll', 'i2')
  await tick()
  assert.deepEqual(p.get('i1').slots.map(s => s.path), ['bam'])
  assert.deepEqual(p.get('i2').slots, [])
})
```

- [ ] **Step 2: Run to see them fail**

Run (from `web/`): `node --test test/slots.test.mjs test/previews.test.mjs`
Expected: FAIL, `Cannot find module '../src/slots.js'`.

- [ ] **Step 3: Implement `fileSlots(value)` in `web/src/slots.js`; add `slots: fileSlots(it.value)` to the record `Previews.pump` stores**

The column key is the first path segment: a map key, or `[i]` for a top-level list position, or `[0]` for a bare Leaf. `address` is `String(leaf.address)` when present, else `null`. `dir` is true when the address's CID code is `DAG_CBOR`; parse with `CID.asCID(a) ?? CID.parse(String(a))` and treat a parse failure as `false` (the same rule as `codeOf` in `previews.js`, which can then import it from `slots.js` instead of keeping its own copy). Header comment in the file's existing style: what the module decides and the spec section (§6.1).

- [ ] **Step 4: Run to see them pass, then the whole suite**

Run: `node --test test/slots.test.mjs test/previews.test.mjs` then `npm test`
Expected: PASS; full suite 346 tests, 0 failing.

- [ ] **Step 5: Commit**

```bash
git add web/src/slots.js web/src/previews.js web/test/slots.test.mjs web/test/previews.test.mjs
git commit -m "feat(web): file slots of an item, carried by its preview"
```

---

### Task 2: File columns and cell contents (pure)

**Files:**
- Modify: `web/src/table.js` (add after `fieldPaths`)
- Test: `web/test/table.test.mjs`

**Interfaces:**
- Consumes: `Slot` from Task 1 (`preview.slots`).
- Produces, in `web/src/table.js`:
  - `fileColumns(previews: Preview[]) -> string[]`: union of `slot.column` over previews with `slots`; names sorted with the file's `byText`, then `[i]` keys sorted by `i`.
  - `cellSlots(preview: Preview, column: string, max = 5) -> { shown: Slot[], more: number }`: the preview's slots with that column, in order; `shown` is the first `max`, `more` the count beyond.

- [ ] **Step 1: Write the failing tests** in `web/test/table.test.mjs`

```js
const slot = (column, path) => ({ column, path, name: null, size: null, address: null, dir: false, reason: null })

test('fileColumns: the union over loaded previews, names sorted, then positions by number', () => {
  const a = { slots: [slot('samtools', 'samtools.stats'), slot('bam', 'bam')] }
  const b = { slots: [slot('bai', 'bai'), slot('[10]', '[10]'), slot('[2]', '[2]')] }
  assert.deepEqual(fileColumns([a, b, undefined, { error: 'block_missing' }]), ['bai', 'bam', 'samtools', '[2]', '[10]'])
  assert.deepEqual(fileColumns([]), [])
})

test('cellSlots: the column\'s slots in order, at most five, and how many more', () => {
  const p = { slots: [...Array(9)].map((_, i) => slot('mqc_files', `mqc_files[${i}]`)).concat([slot('bam', 'bam')]) }
  const { shown, more } = cellSlots(p, 'mqc_files')
  assert.deepEqual(shown.map(s => s.path), ['mqc_files[0]', 'mqc_files[1]', 'mqc_files[2]', 'mqc_files[3]', 'mqc_files[4]'])
  assert.equal(more, 4)
  assert.deepEqual(cellSlots(p, 'bam'), { shown: [p.slots[9]], more: 0 })
  assert.deepEqual(cellSlots(p, 'bai'), { shown: [], more: 0 })
})
```

- [ ] **Step 2: Run to see them fail**

Run: `node --test test/table.test.mjs`
Expected: FAIL, `fileColumns` / `cellSlots` not exported.

- [ ] **Step 3: Implement `fileColumns` and `cellSlots` in `web/src/table.js`**, each with a one-line doc comment in the file's style citing spec §6.1 / §6.2. Leave `fileChips` in place (Task 3 removes it with its caller).

- [ ] **Step 4: Run to see them pass, then the whole suite**

Run: `node --test test/table.test.mjs` then `npm test`
Expected: PASS; 348 tests, 0 failing.

- [ ] **Step 5: Commit**

```bash
git add web/src/table.js web/test/table.test.mjs
git commit -m "feat(web): file columns and cell slots for the items table"
```

---

### Task 3: Draw the file columns and pills on the items page

**Files:**
- Modify: `web/src/views/items.js` (`itemsTable`, ~lines 86–150; imports)
- Modify: `web/src/styles.js` (the line with `.ext`, ~line 39)
- Modify: `web/src/table.js` (remove `fileChips`), `DESIGN.md` (P9, ~line 2238)
- Test: `web/test/items-view.test.mjs`, `web/test/table.test.mjs` (drop the P9 test)

**Interfaces:**
- Consumes: `fileColumns`, `cellSlots` (Task 2); `preview.slots` (Task 1); `icon` from `icons.js`; `humanBytes`, `leafReasonText` from `words.js`; `link`, `h` from `html.js`.
- Produces (DOM contract the tests and Gate may select on):
  - `table.items-table` has `data-file-columns` = JSON of `fileColumns(...)`, set with `data-columns` when the shape changes.
  - One `th` per file column after the metadata `th`s, text = column key, `data-file-column` = key.
  - One `td[data-file-cell]` per file column in a drawn row, `data-file-cell` = key.
  - Addressed pill: `a.pill[data-pill=<path>]`, `href="#/block/<address>"`, `title` and `aria-label` per Global Constraints, containing `icon('directory')` when `dir`, else `icon('file')`.
  - Address-less pill: `span.pill.pill-empty[data-pill=<path>]`, `role="img"`, `title` and `aria-label`, no icon.
  - Overflow: `a.pill-more` with text `+N`, `href="#/item/<via[0] ?? '-'>/<item>"`.
  - No slot: `span.muted` with text `—`.

- [ ] **Step 1: Update the fake `ex` and write the failing tests** in `web/test/items-view.test.mjs`

Give `fakeEx().item` a record `value` beside its `view` and `leaves`: for `i1` and `i2`, `{ id, bam: Leaf, bai: Leaf, samtools: { stats: Leaf, flagstat: Leaf }, json_info: <declined Leaf, name null> }`; for `i3`, the same without `bai`. Use `rawCid` from `./fixture.mjs` for addresses; the `bam` Leaf is named `${id}.markdup.sorted.bam` with `size: 2048`. Replace the first test's two file-chip assertions (`/\.bam/` and the `WT_REP1.markdup.sorted` one) with the ones below, and add the new tests:

```js
test('one column per file key after the metadata columns; pills link to the block page with no download', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.fileColumns), ['bai', 'bam', 'json_info', 'samtools'])
  assert.deepEqual(page.querySelectorAll('th[data-file-column]').map(t => t.textContent), ['bai', 'bam', 'json_info', 'samtools'])
  const row = page.querySelectorAll('[data-item-result]')[0]
  const bam = row.querySelector('a[data-pill="bam"]')
  assert.equal(bam.getAttribute('href'), `#/block/${BAM.toString()}`)
  assert.equal(bam.getAttribute('download'), null)
  assert.equal(bam.getAttribute('title'), 'bam\nWT_REP1.markdup.sorted.bam\n2.0 KB')
  assert.equal(bam.getAttribute('aria-label'), bam.getAttribute('title'))
  assert.ok(bam.querySelector('[data-icon="file"]'))
  assert.deepEqual(row.querySelector('[data-file-cell="samtools"]').querySelectorAll('[data-pill]').map(p => p.dataset.pill),
    ['samtools.stats', 'samtools.flagstat'])
})

test('a missing key is a dash; an address-less slot is a pill that is not a link', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const rows = page.querySelectorAll('[data-item-result]')
  assert.equal(rows[2].querySelector('[data-file-cell="bai"]').textContent, '—')
  const empty = rows[0].querySelector('[data-pill="json_info"]')
  assert.equal(empty.tagName, 'SPAN')
  assert.equal(empty.getAttribute('title'), 'json_info\nno file (an optional output was empty)')
})

test('more than five slots shows five and +N to the item page, "-" when there is no collection (Review Focus 5)', async () => {
  installDom()
  const ex = fakeEx()
  ex.items = async () => ({ items: ['i1'], collection: null })
  ex.item = async () => ({ view: { id: 'S' }, leaves: [], value: { id: 'S', mqc: [...Array(9)].map((_, i) => leaf(`f${i}`, rawCid(`f${i}`))) } })
  const page = await items(ex, 'bafyrun', 'bam_qc', '[]', ctx())
  await frame(); await frame()
  const cell = page.querySelector('[data-file-cell="mqc"]')
  assert.equal(cell.querySelectorAll('[data-pill]').length, 5)
  assert.equal(cell.querySelector('.pill-more').textContent, '+4')
  assert.equal(cell.querySelector('.pill-more').getAttribute('href'), '#/item/-/i1')
})

test('loading and failed rows span the file columns too (Review Focus 4)', async () => {
  installDom()
  const ex = fakeEx()
  const item = ex.item
  ex.item = async (c, i) => { if (i === 'i3') throw Object.assign(new Error('gone'), { code: 'block_missing' }); return item(c, i) }
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const columns = JSON.parse(page.querySelector('table').dataset.columns).length
  const files = JSON.parse(page.querySelector('table').dataset.fileColumns).length
  const failed = page.querySelectorAll('[data-item-result]')[2]
  assert.equal(failed.querySelectorAll('td')[1].getAttribute('colspan'), String(columns + files))
})
```

(`BAM` is the `rawCid` the fake uses for `bam`; `leaf` is a local helper building `{ kind: 'Leaf', name, address, size: 10, reason: null }`.)

Delete the `fileChips` test in `web/test/table.test.mjs` and `fileChips` from its import.

- [ ] **Step 2: Run to see them fail**

Run: `node --test test/items-view.test.mjs`
Expected: FAIL on `dataset.fileColumns` being undefined and on missing `[data-pill]`.

- [ ] **Step 3: Implement in `web/src/views/items.js`**

- In `itemsTable`, compute `files = fileColumns(loaded previews)` in `redraw` alongside `itemTable`; include it in the `shapeKey` JSON so a new file key redraws the header; set `table.dataset.fileColumns`.
- Header: metadata `th`s, then `h('th', { 'data-file-column': key }, key)` per file column, then the empty `th`. The initial header before any preview loads has no file columns.
- `drawRow`: after the metadata cells, one `td[data-file-cell]` per file column from `cellSlots(p, key)`: pills, then `+N`, or `—` when `shown` is empty. The loading and error cells use `colspan = max(1, shape.columns.length + files.length)` (today's `shape.columns.length + 1` counted the old files column).
- Add `pill(slot) -> Element` in `items.js` building the two pill forms of the Interfaces block; the hover text is a small `slotTitle(slot) -> string` beside it implementing the Global Constraints rule.
- Remove the `fileChips` import and use.

In `web/src/table.js` delete `fileChips` and its doc comment.

In `web/src/styles.js`, beside `.ext`, add (values are the builder's within this shape; the mockup used 22×18px):
`.pill` (inline-flex, centred, fixed width and height, `1px solid var(--line)` border, full radius, small right margin, `vertical-align: middle`, colour inherited), `.pill:hover, .pill:focus-visible` (accent border), `.pill .icon { pointer-events: none }` (so the pill's `title` shows, not the icon's kind name), `.pill-empty` (dashed border, reduced opacity, `cursor: help`), `.pill-more` (11px, `var(--muted)`).

- [ ] **Step 4: Run to see them pass, then the whole suite and the build**

Run: `node --test test/items-view.test.mjs test/table.test.mjs`, then `npm test`, then `npm run build`
Expected: PASS; full suite 0 failing (348 − 1 removed + 4 added = 351); build writes the bundle with no errors.

- [ ] **Step 5: Update DESIGN.md P9**

Replace the P9 entry's text with: `P9. (Retired 2026-10-23 for the items page, which now shows one column per file key with a pill per file: docs/superpowers/specs/2026-10-23-items-file-columns-design.md.) File chips dropped the prefix one item's files share, up to its last '.'.`

- [ ] **Step 6: Commit**

```bash
git add web/src/views/items.js web/src/styles.js web/src/table.js web/test/items-view.test.mjs web/test/table.test.mjs DESIGN.md
git commit -m "feat(web): items table shows one column per file key, a pill per file"
```

---

### Task 4: Check against the rnaseq run

**Files:** none changed unless the check finds a defect (fix it in the owning task's files, with a test, and commit).

- [ ] **Step 1: Build the plugin and start the explorer on the scratch store**

Run from the nf-blocks root: `NXF_CONFIG_FILE=$PWD/../scratch-workflow/cas.config make explore ARGS='--port 8123'`. The target builds the plugin from this checkout (`gate/browser/plugin-repo.sh`). The config goes through `NXF_CONFIG_FILE` because Nextflow 26.08+ does not pass `-c` to plugin verbs.
Expected: the explorer serves at `http://127.0.0.1:8123/`.

- [ ] **Step 2: Check the pages the spec names**

Open the latest `nf-core/rnaseq` run, then `markdup`, `bam_qc`, `quant_salmon` and `stringtie`. Expected:
- `markdup`: columns `bai`, `bam`, `metrics`, `samtools`; `samtools` has 3 pills.
- `bam_qc`: 6 file columns; `rseqc` shows 5 pills and `+N`; `qualimap` is one directory pill.
- `quant_salmon`: `json_info` and `lib_format_counts` are dashed pills with the "optional output was empty" reason on hover; `quant_dir` is a directory pill.
- Clicking a BAM pill opens its block page with a Range preview and does not download.
- The browser's network panel shows no request on hover.

- [ ] **Step 3: Run the Gate's unit tests**

Run from the nf-blocks root: `python3 -m unittest discover -s gate`
Expected: OK (the Gate reads only `[data-item-result]` from this page, `gate/browser/drive.mjs:43,81`).
