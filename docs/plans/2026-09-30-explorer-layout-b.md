# Explorer layout B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the explorer page as three regions (a left column of pipelines, runs and Selections; a middle view; a "Use in a workflow" panel) so that a Nextflow user who has never read DESIGN.md can take a whole output, a filtered subset, or hand-picked items into a downstream workflow.

**Architecture:** The app shell in `web/src/app.js` mounts the left column (`nav.js`) and the panel (`panel.js`) once; the router renders only the middle region. Views report what they show through `ctx.use(view)` (panel input) and `ctx.mark(where)` (left column). New pure modules (`words.js`, `table.js`, `panelState` in `panel.js`, the `where:` builder in `snippets.js`) hold every rule, so the rules are unit-tested without a DOM. `views.js` is split into `views/*.js` first, as a move with no change in behaviour.

**Tech Stack:** Vanilla ES modules with the `h()` DOM builder (`web/src/html.js`), esbuild (`web/build.mjs`), `node --test` with the fake DOM in `web/test/dom.mjs`, the Gate (`make gate`; browser tiers in `gate/browser/`, Playwright).

**Spec:** `docs/superpowers/specs/2026-09-30-explorer-layout-b-design.md` (commit 82dec04). Read it before any task; this plan argues from it.

## Global Constraints

- No new runtime dependency, no CSS framework, no frontend framework (spec §7, §9).
- No change to `web/src/queries.json`, the index schema, any Groovy code or `fromStore` (spec §9).
- Every existing `data-*` attribute in DESIGN.md §15–16 stays on the element that means the same thing (spec §10).
- A consumer call shown on the page is one line: tier B substitutes `[data-snippet]` text into a one-line `// @snippet` assignment (`gate/browser_b_assert.py` `substitute`, which refuses more than one line).
- Plain words on the page per spec §8; Store URIs, CIDs and `lid://` appear only in Details folds, the store status, and the panel's call.
- Outputs are listed A–Z (spec §5.3). The run switch defaults to "this run" (spec §6.2).
- Run the web tests with `cd web && npm test`; run the Gate with `make gate` from the repo root. Both green at every commit.
- Commits are signed off (`git commit -s`) and end with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Work on branch `feat/explorer-layout-b` from `spec/explorer-layout-b` (82dec04).

## Plan decisions (where the spec is silent or the Gate forces a refinement)

Rob to confirm before execution. Each is built as written unless he says otherwise.

- **P1. The left column queries the snapshot at load, and otherwise only on a reader's click or on the `#/run` and `#/pipeline` routes.** Gate assertion 2 counts every snapshot read in the query phase of `#/items` and `#/content` and holds them to 8 requests / 64 KB and 50 / 512 KB (`gate/browser_assert.py` `POINT_LIMIT`, `QUERY3_LIMIT`). Loading `pipelines()` and five Selections before the first render puts that cost in the open phase, which is not measured.
- **P2. The panel names a run from its RunCompletion and RunManifest blocks (new `Explorer.runIdentity`), not from the snapshot,** for the same reason: block fetches are not snapshot reads.
- **P3. The run switch is shown whenever the run's pipeline is known; "latest good run" runs `latestSuccessfulRun` only when clicked.** The spec says to show it only when a latest good run exists, which would cost a query on every items view. When none exists, the panel says so and stays on "this run".
- **P4. The `filtered` state says "M items of <output>, where …" without the unfiltered total,** and the items view does not count rows hidden by the filter (spec §5.4). Both need `collectionItemCount` in the query phase.
- **P5. `#/compose` renders the home view with the panel open (no redirect).** Tier B navigates to `#/compose` and waits for exactly one render; a redirect renders twice.
- **P6. Each items row keeps a "Pick" button carrying `[data-pick]`.** Tier B step `B.picks` clicks `[data-pick="<item>"]` on an items view. Rows also get an "Open" link to the item page, instead of making the whole row clickable, since every value in the row is already a filter link.
- **P7. Folds (`<details>`) remember whether they are open for the life of the page,** so a write's re-render keeps Storage open. Tier B `B.retain` gains one click on the Storage summary before filling `#pin-note`.
- **P8. Float values in `where:` get a `d` suffix (`1.0E-5d`).** `MetadataView.scalar` types Groovy's `BigDecimal` as float with `BigDecimal.toString()` (`0.000010` for `1.0E-5`), which differs from the index's `Double.toString` text; a `Double` literal round-trips.
- **P9. File chips drop the prefix one item's files share, up to its last `.`** (`WT_REP1.markdup.sorted.bam` gives `.bam`). The spec's "shared with all other items of the output" strips nothing on rnaseq, where sample names differ. An item with one file shows its whole name.
- **P10. The "same for every item" line needs at least two loaded items;** with one item (multiqc) every key is a column.
- **P16. Only the same key at two paths is a duplicate** (`id` and `meta.id`, as rnaseq's items carry). `has_genome_bam` and `has_transcriptome_bam` are both `false` on every markdup item, yet are two facts; equal values alone never merge keys.
- **P11. The item page has no "View" for text files.** Nothing in the page previews file content today; the spec's §5.5 said "as today". Files get Download and a "where else" link to the existing content page.
- **P12. Spec §10 is wrong about `[data-member]`:** it marks a Selection's member row (DESIGN.md §15 table), not a store member. It stays on the Selection view. The store member links (`#members`) move to the left column. Task 13 corrects the spec.
- **P13. The Collection, Content, Latest and Idle views stay,** restyled into the middle column with breadcrumbs; Collection and Content get the Storage fold. The spec does not mention them.
- **P14. Panel and left-column failures show as `.warn` text without `[data-error]`.** Tier A reads every `[data-error]` on the page as the view's errors (`EXTRACT.errors`).
- **P15. The panel's `saved` state has no Undo;** delete and undo stay on the Selection view, where they are today (spec §6.1 listed Undo).

## Review Focus

1. **Gate assertion 2 on `#/items`.** Rendering an items view and its panel must issue no snapshot query beyond `Explorer.items` itself. A reasonable person expects the year-scale query to cost what it cost before. Pinned in Task 11 by a test whose fake explorer throws on every snapshot-backed method except `items`.
2. **One item in an output (multiqc).** Expect a table with the item's keys as columns and no "same for every item" line. Pinned in Task 2.
3. **A copied call that Groovy or `fromStore` reads differently from the page.** Expect a one-line call whose `where:` matches the rows on screen: a dotted key quoted, a numeric-looking string kept a string, a float kept a `Double`, a Groovy keyword as a key quoted. Pinned in Task 3.
4. **The left column or panel leaking into the Gate's DOM contract.** Expect no `[data-run]`, `[data-selection]` or `[data-error]` in the left column or panel, so tier A's run and error lists are the view's own. Pinned in Tasks 7 and 8.
5. **Two snippets on one page.** Tier B reads the first `[data-snippet="untyped"]`; a Selection view must show exactly one, the panel's. Pinned in Task 12.

---

### Task 1: Plain words (`words.js`)

**Files:**
- Create: `web/src/words.js`
- Test: `web/test/words.test.mjs`

**Interfaces:**
- Consumes: nothing.
- Produces: `anomalyLines(anomalies) -> [{ key, count, text, concern }]`, `healthSummary(anomalies) -> 'worth a look' | 'nothing to check'`, `healthText(anomalies) -> string`, `statusText({ status, possibly_incomplete }) -> string`, `statusTone({ status, possibly_incomplete }) -> 'good' | 'bad'`, `humanBytes(n) -> string`, `whenText(iso) -> string`, `leafReasonText(reason) -> string`.

- [ ] **Step 1: Write the failing test**

```js
// web/test/words.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { anomalyLines, healthSummary, healthText, humanBytes, leafReasonText, statusText, statusTone, whenText } from '../src/words.js'

const RNASEQ = { unresolvable: 0, unaddressed: 0, declined: 439, never_published: 5, unjoined: 5 }

test('anomaly lines: non-zero only, concerns first, in the spec\'s words', () => {
  assert.deepEqual(anomalyLines(RNASEQ), [
    { key: 'unjoined', count: 5, concern: true, text: '5 published files that are in no output' },
    { key: 'declined', count: 439, concern: false, text: '439 empty optional slots (the pipeline passed no file)' },
    { key: 'never_published', count: 5, concern: false, text: '5 referenced files not stored here, such as input files' },
  ])
  assert.equal(anomalyLines({ unjoined: 1 })[0].text, '1 published file that is in no output')
  assert.deepEqual(anomalyLines({}), [])
  assert.deepEqual(anomalyLines(undefined), [])
})

test('an older block without unjoined reads it as 0', () => {
  assert.deepEqual(anomalyLines({ unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }), [])
})

test('health: worth a look when any concern is non-zero', () => {
  assert.equal(healthSummary(RNASEQ), 'worth a look')
  assert.equal(healthSummary({ declined: 3, never_published: 2 }), 'nothing to check')
  assert.equal(healthSummary({ unresolvable: 1 }), 'worth a look')
  assert.equal(healthSummary({ unaddressed: 1 }), 'worth a look')
  assert.equal(healthText({ declined: 3 }), 'nothing to check')
  assert.equal(healthText(RNASEQ), '5 published files that are in no output')
})

test('status in words, and its tone', () => {
  assert.equal(statusText({ status: 'succeeded', possibly_incomplete: false }), 'Succeeded')
  assert.equal(statusText({ status: 'failed', possibly_incomplete: true }), 'Failed, may be incomplete')
  assert.equal(statusText({ status: 'failed', possibly_incomplete: false }), 'Failed')
  assert.equal(statusText({ status: 'succeeded', possibly_incomplete: 1 }), 'Succeeded, may be incomplete')
  assert.equal(statusTone({ status: 'succeeded', possibly_incomplete: 0 }), 'good')
  assert.equal(statusTone({ status: 'succeeded', possibly_incomplete: 1 }), 'bad')
  assert.equal(statusTone({ status: 'failed', possibly_incomplete: true }), 'bad')
})

test('sizes in decimal units; a BigInt, null and empty are handled', () => {
  assert.equal(humanBytes(0), '0 B')
  assert.equal(humanBytes(999), '999 B')
  assert.equal(humanBytes(1200), '1.2 KB')
  assert.equal(humanBytes(412_000_000), '412 MB')
  assert.equal(humanBytes(3n * 1000n ** 3n), '3.0 GB')
  assert.equal(humanBytes(null), '')
  assert.equal(humanBytes(''), '')
})

test('a time reads in the viewer\'s locale; text that is not a time is shown as it is', () => {
  assert.notEqual(whenText('2026-09-30T21:10:38.345Z'), '2026-09-30T21:10:38.345Z')
  assert.equal(whenText('not a time'), 'not a time')
  assert.equal(whenText(null), '')
})

test('a leaf with no address says why, in words', () => {
  assert.equal(leafReasonText('declined'), 'no file (an optional output was empty)')
  assert.equal(leafReasonText('never_published'), 'not stored here (such as an input file)')
  assert.equal(leafReasonText('unresolvable'), 'a link whose target could not be found')
  assert.equal(leafReasonText('unaddressed'), 'no stored content')
  assert.equal(leafReasonText('something new'), 'something new')
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/words.test.mjs`
Expected: FAIL, `Cannot find module '../src/words.js'`.

- [ ] **Step 3: Write minimal implementation**

```js
// web/src/words.js
// The page's plain words for store terms (explorer layout B spec §8). DESIGN.md
// §6 defines each anomaly and Leaf reason: change a meaning there first.

const counted = (n, one, many) => `${n} ${n === 1 ? one : many}`

// Concerns first, then the expected ones; each line's text is the spec §8 table's.
const ANOMALIES = [
  { key: 'unjoined', concern: true, text: (n) => `${counted(n, 'published file', 'published files')} that ${n === 1 ? 'is' : 'are'} in no output` },
  { key: 'unresolvable', concern: true, text: (n) => `${counted(n, 'link', 'links')} whose target could not be found` },
  { key: 'unaddressed', concern: true, text: (n) => `${counted(n, 'file', 'files')} with no stored content` },
  { key: 'declined', concern: false, text: (n) => `${counted(n, 'empty optional slot', 'empty optional slots')} (the pipeline passed no file)` },
  { key: 'never_published', concern: false, text: (n) => `${counted(n, 'referenced file', 'referenced files')} not stored here, such as input files` },
]

/** One line per non-zero anomaly, concerns first. A missing count (unjoined on an older block) reads as 0. */
export function anomalyLines(anomalies) {
  return ANOMALIES.map(a => ({ key: a.key, count: Number(anomalies?.[a.key] ?? 0), concern: a.concern, text: null, a }))
    .filter(l => l.count > 0)
    .map(({ a, ...l }) => ({ ...l, text: a.text(l.count) }))
}

/** The Lineage check summary line. */
export const healthSummary = (anomalies) => (anomalyLines(anomalies).some(l => l.concern) ? 'worth a look' : 'nothing to check')

/** The pipeline page's health cell: the concerns, or "nothing to check". */
export function healthText(anomalies) {
  const concerns = anomalyLines(anomalies).filter(l => l.concern)
  return concerns.length ? concerns.map(l => l.text).join('; ') : 'nothing to check'
}

const capital = (s) => (s ? s[0].toUpperCase() + s.slice(1) : '')

export function statusText({ status, possibly_incomplete }) {
  const base = capital(String(status ?? 'unknown'))
  return possibly_incomplete ? `${base}, may be incomplete` : base
}

export const statusTone = ({ status, possibly_incomplete }) => (status === 'succeeded' && !possibly_incomplete ? 'good' : 'bad')

const UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

/** Decimal units, as file browsers show them. */
export function humanBytes(n) {
  if (n === null || n === undefined || n === '') return ''
  let v = Number(n)
  let i = 0
  while (v >= 1000 && i < UNITS.length - 1) { v /= 1000; i++ }
  return i === 0 ? `${v} B` : `${v < 10 ? v.toFixed(1) : Math.round(v)} ${UNITS[i]}`
}

export function whenText(iso) {
  if (!iso) return ''
  const d = new Date(iso)
  return Number.isNaN(d.getTime()) ? String(iso) : d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
}

const REASONS = {
  declined: 'no file (an optional output was empty)',
  never_published: 'not stored here (such as an input file)',
  unresolvable: 'a link whose target could not be found',
  unaddressed: 'no stored content',
}

export const leafReasonText = (reason) => REASONS[reason] ?? String(reason ?? '')
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd web && node --test test/words.test.mjs`
Expected: PASS, 7 tests.

- [ ] **Step 5: Commit**

```bash
git add web/src/words.js web/test/words.test.mjs
git commit -s -m "feat(web): plain words for anomalies, status and sizes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: The items table's shape (`table.js`)

**Files:**
- Create: `web/src/table.js`
- Test: `web/test/table.test.mjs`

**Interfaces:**
- Consumes: the preview shape from `web/src/previews.js`: `{ pairs: [{ path, type, values, types }], files: [string] }` (or `{ error }`), `pairs` as `pairsOf` in `web/src/pairs.js` makes them.
- Produces: `itemTable(previews, { maxColumns = 6 }) -> { columns: [path], constant: [pairsEntry], duplicates: Map<path, keptPath> }`, `cellOf(preview, path) -> pairsEntry | null`, `fileChips(files) -> { prefix, chips: [string] }`, `withoutDuplicates(pairs, duplicates) -> pairs`.

- [ ] **Step 1: Write the failing test**

The fixture copies the shape of rnaseq's `markdup` in run `high_jang`: `id` at two paths with equal values, `has_genome_bam` the same everywhere, `inferred_strandedness` on one item only, six files per item.

```js
// web/test/table.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { cellOf, fileChips, itemTable, withoutDuplicates } from '../src/table.js'

const e = (path, type, value) => ({ path, type, values: [value], types: [type] })
const files = (s) => ['.markdup.sorted.bam.bai', '.markdup.sorted.bam', '.markdup.sorted.metrics.txt',
  '.markdup.sorted.bam.stats', '.markdup.sorted.bam.flagstat', '.markdup.sorted.bam.idxstats'].map(x => s + x)
const item = (id, singleEnd, extra = []) => ({
  pairs: [e('has_genome_bam', 'bool', 'false'), e('has_transcriptome_bam', 'bool', 'false'), e('id', 'string', id), e('meta.id', 'string', id),
    e('single_end', 'bool', singleEnd), e('strandedness', 'string', 'reverse'), ...extra],
  files: files(id),
})
const MARKDUP = [
  item('RAP1_UNINDUCED_REP1', 'true'),
  item('WT_REP1', 'false', [e('inferred_strandedness', 'string', 'reverse')]),
  item('RAP1_IAA_30M_REP1', 'false'),
  item('WT_REP2', 'false'),
  item('RAP1_UNINDUCED_REP2', 'true'),
]

test('markdup: id once, constants on one line, varying keys as columns (strings first, then most distinct)', () => {
  const shape = itemTable(MARKDUP)
  assert.deepEqual(shape.duplicates, new Map([['meta.id', 'id']]))
  assert.deepEqual(shape.constant.map(c => c.path), ['has_genome_bam', 'has_transcriptome_bam', 'strandedness'],
    'two keys with equal values are not one key: only the same key at two paths is a duplicate')
  assert.deepEqual(shape.columns, ['id', 'inferred_strandedness', 'single_end'])
})

test('a key missing on some items is a column, not a constant', () => {
  const shape = itemTable([item('A', 'false'), item('B', 'false', [e('lane', 'int', '1')])])
  assert.ok(shape.columns.includes('lane'))
  assert.ok(!shape.constant.some(c => c.path === 'lane'))
})

test('one item (multiqc): every key is a column and nothing is "same for every item" (P10)', () => {
  const shape = itemTable([{ pairs: [e('id', 'string', 'multiqc_report'), e('meta.id', 'string', 'multiqc_report')], files: ['multiqc_report.html'] }])
  assert.deepEqual(shape.constant, [])
  assert.deepEqual(shape.columns, ['id'])
  assert.deepEqual(shape.duplicates, new Map([['meta.id', 'id']]))
})

test('previews still loading or failed are ignored; none loaded gives an empty shape', () => {
  assert.deepEqual(itemTable([undefined, { error: 'block_missing' }]), { columns: [], constant: [], duplicates: new Map() })
  assert.deepEqual(itemTable([undefined, MARKDUP[0], MARKDUP[1]]).columns, itemTable([MARKDUP[0], MARKDUP[1]]).columns)
})

test('at most maxColumns columns', () => {
  const wide = [0, 1].map(i => ({ pairs: 'abcdefgh'.split('').map(k => e(k, 'string', `${k}${i}`)), files: [] }))
  assert.equal(itemTable(wide, { maxColumns: 6 }).columns.length, 6)
})

test('a list value can be a column, and the same key at two paths equal only on some items is not a duplicate', () => {
  const list = (path, values) => ({ path, type: 'string', values, types: values.map(() => 'string') })
  const shape = itemTable([{ pairs: [list('tags', ['x', 'y']), e('x.k', 'string', '1'), e('y.k', 'string', '1')], files: [] },
    { pairs: [list('tags', ['z']), e('x.k', 'string', '2'), e('y.k', 'string', '3')], files: [] }])
  assert.ok(shape.columns.includes('tags'))
  assert.equal(shape.duplicates.size, 0)
})

test('cellOf and withoutDuplicates', () => {
  assert.equal(cellOf(MARKDUP[0], 'id').values[0], 'RAP1_UNINDUCED_REP1')
  assert.equal(cellOf(MARKDUP[0], 'nope'), null)
  assert.equal(cellOf(undefined, 'id'), null)
  const { duplicates } = itemTable(MARKDUP)
  assert.ok(!withoutDuplicates(MARKDUP[0].pairs, duplicates).some(p => p.path === 'meta.id'))
})

test('file chips drop the prefix an item\'s files share, up to its last dot (P9)', () => {
  assert.deepEqual(fileChips(files('WT_REP1')), { prefix: 'WT_REP1.markdup.sorted',
    chips: ['.bam.bai', '.bam', '.metrics.txt', '.bam.stats', '.bam.flagstat', '.bam.idxstats'] })
  assert.deepEqual(fileChips(['a.bam', 'a.bam.bai']), { prefix: 'a', chips: ['.bam', '.bam.bai'] })
  assert.deepEqual(fileChips(['multiqc_report.html']), { prefix: '', chips: ['multiqc_report.html'] })
  assert.deepEqual(fileChips(['x.tsv', 'y.rds']), { prefix: '', chips: ['x.tsv', 'y.rds'] })
  assert.deepEqual(fileChips([]), { prefix: '', chips: [] })
  assert.deepEqual(fileChips(undefined), { prefix: '', chips: [] })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/table.test.mjs`
Expected: FAIL, `Cannot find module '../src/table.js'`.

- [ ] **Step 3: Write minimal implementation**

```js
// web/src/table.js
// The items table's shape (explorer layout B spec §5.4), decided from the
// previews loaded so far: which Meta Map paths are columns, which collapse into
// the "same for every item" line, which repeat another path's values, and each
// item's file suffixes. Pure: the views draw what it decides.

const byText = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
const sig = (entry) => JSON.stringify([entry.types, entry.values])
const keyOf = (path) => path.split('.').at(-1)

/** The entry at `path` in one preview, or null. */
export const cellOf = (preview, path) => preview?.pairs?.find(e => e.path === path) ?? null

/**
 * `previews`: previews in any state; only those with pairs count. The same key
 * at two paths (`id`, `meta.id`), present on every loaded item with equal values
 * on each, is one column, kept under the shorter path (ties: sort order); two
 * different keys are never merged, whatever their values. A path with one value on every
 * loaded item is "constant" once two or more items have loaded (P10). The
 * rest are columns: strings first, then the most distinct values, then by path.
 */
export function itemTable(previews, { maxColumns = 6 } = {}) {
  const items = (previews ?? []).filter(p => p?.pairs)
  const n = items.length
  const stats = new Map()
  for (const p of items) {
    for (const entry of p.pairs) {
      const s = stats.get(entry.path) ?? { present: 0, sigs: new Set(), string: true, first: entry }
      s.present++
      s.sigs.add(sig(entry))
      if (entry.type !== 'string') s.string = false
      stats.set(entry.path, s)
    }
  }
  const everywhere = (path) => stats.get(path).present === n
  const paths = [...stats.keys()].sort((a, b) => (a.length - b.length) || byText(a, b))
  const duplicates = new Map()
  for (let i = 0; i < paths.length; i++) {
    const keep = paths[i]
    if (duplicates.has(keep) || !everywhere(keep)) continue
    for (const other of paths.slice(i + 1)) {
      if (duplicates.has(other) || !everywhere(other) || keyOf(other) !== keyOf(keep)) continue
      if (items.every(p => sig(cellOf(p, keep)) === sig(cellOf(p, other)))) duplicates.set(other, keep)
    }
  }
  const kept = paths.filter(p => !duplicates.has(p))
  const constantPaths = new Set(n >= 2 ? kept.filter(p => everywhere(p) && stats.get(p).sigs.size === 1) : [])
  const constant = [...constantPaths].sort(byText).map(p => stats.get(p).first)
  const columns = kept.filter(p => !constantPaths.has(p))
    .sort((a, b) => (Number(stats.get(b).string) - Number(stats.get(a).string))
      || (stats.get(b).sigs.size - stats.get(a).sigs.size) || byText(a, b))
    .slice(0, maxColumns)
  return { columns, constant, duplicates }
}

/** `pairs` without the paths `itemTable` found to repeat another. */
export const withoutDuplicates = (pairs, duplicates) => (pairs ?? []).filter(e => !duplicates.has(e.path))

/**
 * An item's files as chips (P9): with two or more, the prefix they share up to
 * its last '.' is dropped, each chip keeping its leading '.'; one file, or no
 * shared prefix with a '.', keeps the whole names.
 */
export function fileChips(files) {
  if (!files?.length) return { prefix: '', chips: [] }
  if (files.length === 1) return { prefix: '', chips: [files[0]] }
  let common = files[0]
  for (const f of files.slice(1)) {
    let i = 0
    while (i < common.length && i < f.length && common[i] === f[i]) i++
    common = common.slice(0, i)
  }
  const cut = common.lastIndexOf('.')
  if (cut <= 0) return { prefix: '', chips: [...files] }
  return { prefix: common.slice(0, cut), chips: files.map(f => f.slice(cut)) }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd web && node --test test/table.test.mjs`
Expected: PASS, 8 tests. If `markdup` columns come out in another order, check the sort: `id` and `inferred_strandedness` are strings (string first), then `id` (5 distinct) before `inferred_strandedness` (1 distinct), then `single_end` (bool).

- [ ] **Step 5: Commit**

```bash
git add web/src/table.js web/test/table.test.mjs
git commit -s -m "feat(web): items table shape, duplicate paths and file chips

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `where:`, latest and Groovy literals in `snippets.js`

**Files:**
- Modify: `web/src/snippets.js` (`snippetLines`, `snippetBlock`)
- Test: `web/test/snippets.test.mjs`

**Interfaces:**
- Consumes: `where` as the route carries it: `[[path, type, value], ...]`, `type` one of `string|int|float|bool|null`, `value` the index's text (`web/src/metadata.js` `predicateRow`).
- Produces: `snippetLines(spec, mode)` now also takes `{ kind: 'run', lid, output, where? }` and `{ kind: 'latest', pipeline, output, where? }`; `groovyKey(path)`, `groovyValue(type, value)`, `whereLiteral(where)`. `snippetBlock(spec)` sets `data-where` (JSON of `spec.where`) on the call `<code>` when `spec.where` is non-empty.

- [ ] **Step 1: Write the failing tests** (append to `web/test/snippets.test.mjs`; extend its import line to `import { INCLUDE_LINE, SNIPPET_KEY, groovyKey, groovyValue, setSnippetMode, snippetLines, snippetMode, whereLiteral } from '../src/snippets.js'`)

```js
test('where: one condition per chip, in chip order, as a Groovy map literal', () => {
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: 'markdup', where: [['single_end', 'bool', 'false'], ['id', 'string', 'WT_REP1']] }, 'untyped').call,
    `channel.fromStore(run: '${LID}', output: 'markdup', where: [single_end: false, id: 'WT_REP1'])`)
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: 'markdup', where: [] }, 'untyped').call,
    `channel.fromStore(run: '${LID}', output: 'markdup')`)
})

test('latest good run names the pipeline instead of the run', () => {
  assert.equal(snippetLines({ kind: 'latest', pipeline: 'nf-core/rnaseq', output: 'markdup', where: [['single_end', 'bool', 'false']] }, 'typed').call,
    "nextflow.Channel.fromStore(run: 'latest', pipeline: 'nf-core/rnaseq', output: 'markdup', where: [single_end: false], records: true)")
})

test('keys: identifiers bare; dotted, odd and Groovy keywords quoted (Review Focus 3)', () => {
  assert.equal(groovyKey('sample'), 'sample')
  assert.equal(groovyKey('_x1'), '_x1')
  assert.equal(groovyKey('meta.id'), "'meta.id'")
  assert.equal(groovyKey('read-group'), "'read-group'")
  assert.equal(groovyKey('1st'), "'1st'")
  assert.equal(groovyKey('in'), "'in'")
  assert.equal(groovyKey('class'), "'class'")
  assert.equal(groovyKey("it's"), "'it\\'s'")
})

test('values by type: a numeric-looking string stays a string, a float stays a Double (P8)', () => {
  assert.equal(groovyValue('string', '10'), "'10'")
  assert.equal(groovyValue('string', "o'brien"), "'o\\'brien'")
  assert.equal(groovyValue('int', '42'), '42')
  assert.equal(groovyValue('int', '-7'), '-7')
  assert.equal(groovyValue('int', '123456789012345678901234567890'), '123456789012345678901234567890')
  assert.equal(groovyValue('float', '10.371008628978885'), '10.371008628978885d')
  assert.equal(groovyValue('float', '1.0E-5'), '1.0E-5d')
  assert.equal(groovyValue('float', '-0.0'), '-0.0d')
  assert.equal(groovyValue('bool', 'true'), 'true')
  assert.equal(groovyValue('null', null), 'null')
  assert.throws(() => groovyValue('date', 'x'), /unknown type/)
})

test('whereLiteral', () => {
  assert.equal(whereLiteral([]), null)
  assert.equal(whereLiteral([['meta.id', 'string', 'A'], ['depth', 'float', '1.5'], ['x', 'null', null]]), "['meta.id': 'A', depth: 1.5d, x: null]")
})

test('every call is one line, whatever the filter holds (the Gate substitutes it into one line)', () => {
  const where = [['a\nb', 'string', 'line\nbreak'], ['c', 'string', 'tab\there']]
  for (const mode of ['untyped', 'typed']) {
    const { call } = snippetLines({ kind: 'run', lid: LID, output: 'o', where }, mode)
    assert.ok(!call.includes('\n'), call)
  }
  assert.equal(groovyValue('string', 'line\nbreak'), "'line\\nbreak'")
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/snippets.test.mjs`
Expected: FAIL, `groovyKey` is not exported.

- [ ] **Step 3: Implement.** Replace `quote` and `snippetLines` in `web/src/snippets.js`, and add the `data-where` attribute in `snippetBlock`:

```js
// A Groovy single-quoted string: backslash, quote and line breaks escaped, so a call stays one line.
const quote = (text) => `'${String(text).replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')}'`

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/
// Groovy's reserved words, quoted as map keys to be safe.
const KEYWORDS = new Set(['abstract', 'as', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char', 'class', 'const', 'continue',
  'def', 'default', 'do', 'double', 'else', 'enum', 'extends', 'false', 'final', 'finally', 'float', 'for', 'goto', 'if', 'implements',
  'import', 'in', 'instanceof', 'int', 'interface', 'long', 'native', 'new', 'null', 'package', 'private', 'protected', 'public',
  'return', 'short', 'static', 'strictfp', 'super', 'switch', 'synchronized', 'this', 'threadsafe', 'throw', 'throws', 'trait',
  'transient', 'true', 'try', 'var', 'void', 'volatile', 'while', 'yields', 'record', 'sealed', 'permits', 'non-sealed'])

/** A Meta Map path as a `where:` key: bare when it is a plain identifier, else quoted. */
export const groovyKey = (path) => (IDENTIFIER.test(path) && !KEYWORDS.has(path) ? path : quote(path))

/**
 * A condition's value as the Groovy literal fromStore(where:) types back to the
 * same item_attr row (MetadataView.scalar): a float gets `d`, since a BigDecimal
 * literal's toString differs from Double's (P8).
 */
export function groovyValue(type, value) {
  switch (type) {
    case 'string': return quote(value)
    case 'int': return String(value)
    case 'float': return `${value}d`
    case 'bool': return String(value)
    case 'null': return 'null'
    default: throw new Error(`unknown type '${type}'`)
  }
}

/** `[k: v, ...]` for the chips, or null when there are none. */
export const whereLiteral = (where) => (where?.length ? `[${where.map(([p, t, v]) => `${groovyKey(p)}: ${groovyValue(t, v)}`).join(', ')}]` : null)

/** The two lines of a snippet: the include, and the call for `mode`. */
export function snippetLines(spec, mode) {
  const where = whereLiteral(spec.where)
  const args = spec.kind === 'selection' ? `selection: ${quote(spec.cid)}`
    : spec.kind === 'latest' ? `run: 'latest', pipeline: ${quote(spec.pipeline)}, output: ${quote(spec.output)}`
      : `run: ${quote(spec.lid)}, output: ${quote(spec.output)}`
  const full = where ? `${args}, where: ${where}` : args
  return { include: INCLUDE_LINE, call: mode === 'typed' ? `nextflow.Channel.fromStore(${full}, records: true)` : `channel.fromStore(${full})` }
}
```

In `snippetBlock`, change the first line to

```js
  const call = h('code', spec.where?.length ? { 'data-where': JSON.stringify(spec.where) } : {})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd web && node --test test/snippets.test.mjs`
Expected: PASS, the original 6 tests and the 6 new ones.

- [ ] **Step 5: Commit**

```bash
git add web/src/snippets.js web/test/snippets.test.mjs
git commit -s -m "feat(web): fromStore calls with where:, latest, and exact Groovy literals

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `Explorer.runIdentity`

**Files:**
- Modify: `web/src/model.js` (constructor; new method after `completionOf`)
- Test: `web/test/model.test.mjs`

**Interfaces:**
- Consumes: `this.completionOf(cid)`, `this.blocks.ofKind(cid, 'RunManifest')`, the module's `text` helper.
- Produces: `runIdentity(completionCid) -> Promise<{ completion, manifest, pipeline, run_name, nf_run_hash, lid, revision, config, status, possibly_incomplete }>`, memoized per completion; a failure is not memoized.

- [ ] **Step 1: Write the failing test** (append to `web/test/model.test.mjs`; add `memberWithUnjoinedRun` to its `./fixture.mjs` import)

```js
test('runIdentity names a run from its blocks alone, once per run, and asks again after a failure (P2)', async () => {
  const { ex, ids } = await memberWithUnjoinedRun(0)
  const queries = []
  const query = ex.db.query.bind(ex.db)
  ex.db.query = (sql, params) => { queries.push(sql); return query(sql, params) }
  const id = await ex.runIdentity(ids.completion)
  assert.deepEqual({ ...id, config: undefined, manifest: undefined }, {
    completion: ids.completion, manifest: undefined, pipeline: 'demo', run_name: 'R4', nf_run_hash: 'hash-R4', lid: 'lid://hash-R4',
    revision: null, config: undefined, status: 'succeeded', possibly_incomplete: false })
  assert.match(id.manifest, /^bafy/)
  assert.deepEqual(queries, [], 'no snapshot query')
  assert.equal(await ex.runIdentity(ids.completion), id)
  await assert.rejects(ex.runIdentity('bafyreinotarealblockxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'))
  assert.equal(ex.identities.size, 1)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/model.test.mjs`
Expected: FAIL, `ex.runIdentity is not a function`.

- [ ] **Step 3: Implement.** In the constructor add `this.identities = new Map()` after `this.runLabels = new Map()`. After `completionOf`:

```js
  /**
   * A run's name, lineage ID and pipeline from its RunCompletion and
   * RunManifest blocks, with no snapshot query, so the panel and breadcrumbs
   * add nothing to query 3's measured cost (Gate assertion 2; layout B plan P2).
   * One lookup per run; a failure is asked again next time.
   */
  runIdentity(completionCid) {
    if (!this.identities.has(completionCid)) {
      const lookup = (async () => {
        const completion = await this.completionOf(completionCid)
        const manifestCid = text(completion.run)
        const manifest = (await this.blocks.ofKind(manifestCid, 'RunManifest')).value
        return { completion: completionCid, manifest: manifestCid, pipeline: manifest.pipeline ?? null, run_name: manifest.run_name ?? null,
          nf_run_hash: manifest.nf_run_hash ?? null, lid: manifest.nf_run_hash ? `lid://${manifest.nf_run_hash}` : null,
          revision: manifest.revision ?? null, config: manifest.config ?? null,
          status: completion.status, possibly_incomplete: !!completion.possibly_incomplete }
      })()
      this.identities.set(completionCid, lookup.catch((e) => { this.identities.delete(completionCid); throw e }))
    }
    return this.identities.get(completionCid)
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd web && node --test test/model.test.mjs`
Expected: PASS. If `ex.db` is not the property name the fixture's `explorerOf` sets, read `web/test/fixture.mjs` `explorerOf` and wrap the property it uses.

- [ ] **Step 5: Commit**

```bash
git add web/src/model.js web/test/model.test.mjs
git commit -s -m "feat(web): Explorer.runIdentity reads a run's name from its blocks

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Folds that stay open (`folds.js`)

**Files:**
- Create: `web/src/folds.js`
- Test: `web/test/folds.test.mjs`

**Interfaces:**
- Consumes: `h` from `web/src/html.js`.
- Produces: `fold(key, summary, ...body) -> <details data-fold="<key before ':'>">`; open state remembered per full `key` for the life of the page (P7). `resetFolds()` for tests.

- [ ] **Step 1: Write the failing test**

```js
// web/test/folds.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { fold, resetFolds } from '../src/folds.js'

test('a fold starts closed, names its kind, and remembers being opened across a redraw (P7)', async () => {
  installDom()
  resetFolds()
  const first = fold('storage:bafyrun', 'Storage', 'body')
  assert.equal(first.tagName, 'DETAILS')
  assert.equal(first.dataset.fold, 'storage')
  assert.equal(first.hasAttribute('open'), false)
  assert.equal(first.querySelector('summary').textContent, 'Storage')
  first.open = true
  await first.fire('toggle')
  assert.equal(fold('storage:bafyrun', 'Storage').hasAttribute('open'), true)
  assert.equal(fold('storage:other', 'Storage').hasAttribute('open'), false)
  const again = fold('storage:bafyrun', 'Storage')
  again.open = false
  await again.fire('toggle')
  assert.equal(fold('storage:bafyrun', 'Storage').hasAttribute('open'), false)
})

test('a fold can start open', () => {
  installDom()
  resetFolds()
  assert.equal(fold('details:x', 'Details', { open: true }).hasAttribute('open'), true)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/folds.test.mjs`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement**

```js
// web/src/folds.js
// A <details> section whose open state survives a re-render for the life of
// the page (layout B plan P7): a write re-renders its view, and the reader's
// open Storage section, with its Pin and Release buttons, must stay open.
import { h } from './html.js'

const remembered = new Map()

/** Forgets every fold's state (tests). */
export function resetFolds() { remembered.clear() }

/**
 * `key` is `<kind>:<subject>`; `data-fold` carries the kind. A first argument
 * after `summary` that is `{ open: true }` opens it the first time it is drawn.
 */
export function fold(key, summary, ...body) {
  const options = body.length && body[0] && typeof body[0] === 'object' && !(body[0] instanceof Node) && 'open' in body[0] ? body.shift() : {}
  const open = remembered.has(key) ? remembered.get(key) : !!options.open
  const el = h('details', { class: 'fold', 'data-fold': key.split(':')[0], open }, h('summary', {}, summary), ...body)
  el.addEventListener('toggle', () => { remembered.set(key, !!el.open) })
  return el
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd web && node --test test/folds.test.mjs`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add web/src/folds.js web/test/folds.test.mjs
git commit -s -m "feat(web): folds that remember being open across a re-render

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Split `views.js` into `views/` (move only) and add breadcrumbs

**Files:**
- Create: `web/src/views/common.js`, `views/home.js`, `views/pipeline.js`, `views/run.js`, `views/collection.js`, `views/items.js`, `views/item.js`, `views/selection.js`, `views/index.js`
- Delete: `web/src/views.js`
- Modify: `web/src/app.js:16` (`import * as views from './views/index.js'`), `web/test/views.test.mjs:9` (import from `'../src/views/index.js'`)
- Test: `web/test/views.test.mjs` (unchanged assertions), new `web/test/crumbs.test.mjs`

**Interfaces:**
- Consumes: everything `views.js` imports today.
- Produces: the same exported names as `views.js` today, from `views/index.js`; plus `crumbs(...parts)` and `runCrumbs(ex, completionCid, tail)` in `views/common.js`. Later tasks import shared helpers from `views/common.js`.

This task moves code without changing it. Every function body is copied verbatim from `web/src/views.js`; only imports change. Imports inside `views/` use `'../html.js'` etc.

- [ ] **Step 1: Write the failing breadcrumb test**

```js
// web/test/crumbs.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { crumbs, runCrumbs } from '../src/views/common.js'

test('crumbs: linked segments joined by " / ", the last one plain', () => {
  installDom()
  const node = crumbs({ text: 'nf-core/rnaseq', href: '#/pipeline/nf-core%2Frnaseq' }, { text: 'high_jang', href: '#/run/c' }, { text: 'markdup' })
  assert.equal(node.textContent, 'nf-core/rnaseq / high_jang / markdup')
  assert.equal(node.querySelectorAll('a').length, 2)
  assert.equal(node.getAttribute('aria-label'), 'breadcrumb')
})

test('runCrumbs fills pipeline and run name from runIdentity, and shows the tail meanwhile', async () => {
  installDom()
  const ex = { runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'high_jang' }) }
  const node = runCrumbs(ex, 'bafyrun', [{ text: 'markdup' }])
  assert.equal(node.textContent, 'run / markdup')
  await new Promise(r => setTimeout(r, 0))
  assert.equal(node.textContent, 'nf-core/rnaseq / high_jang / markdup')
  const quiet = runCrumbs({}, 'bafyrun', [])
  assert.equal(quiet.textContent, 'run')
})
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd web && node --test test/crumbs.test.mjs`
Expected: FAIL, module not found.

- [ ] **Step 3: Move the code.** Put each function of `web/src/views.js` in the file below, verbatim:

| file | functions and constants (from `views.js`) |
|------|-------------------------------------------|
| `views/common.js` | `enc`, `errorNode`, `table`, `copyText`, `undoNote`, `unreadableRuns`, `pager`, `unavailableNote`, `retentionPanel`, `flag`, `pickButton`, `shortCid`, `runLabelText`, `runLabelNode`, `pickAll`, `added`, `markInTray`, `nextFrame`, `rowOf`, `itemRow`, `fillRow`, `watchRows`, `itemRows` |
| `views/home.js` | `idle`, `home` |
| `views/pipeline.js` | `pipeline`, `latest` |
| `views/run.js` | `run` |
| `views/collection.js` | `indexLine`, `collection`, `content` |
| `views/items.js` | `items`, `TYPES`, `whereForm` |
| `views/item.js` | `item` |
| `views/selection.js` | `undoUnavailableNote`, `selections`, `undoButton`, `selection`, `actions`, `failureBanner`, `deletedNote`, `compose` |

`views/common.js` exports every name in its row (add `export` to the ones that lacked it: `enc`, `table`, `unreadableRuns`, `pager`, `unavailableNote`, `retentionPanel`, `flag`, `shortCid`, `runLabelNode`, `added`, `markInTray`, `nextFrame`, `fillRow`, `watchRows`). Its header:

```js
// web/src/views/common.js
// What more than one view draws (DESIGN.md §15–16): errors, tables, pagers,
// the retention panel, picks, item rows, breadcrumbs. The data-* attributes
// are the contract the Gate reads; the rest is for people.
import { h, link, cid, copyOutcome } from '../html.js'
import { Previews } from '../previews.js'
import { labelPaths, labelText, pairsNode, pairsOf } from '../pairs.js'
import { trayNote } from '../tray.js'
```

and append the breadcrumb helpers:

```js
/** `a / b / c`: each part `{ text, href? }`, linked when it has an href. */
export function crumbs(...parts) {
  return h('nav', { class: 'crumbs', 'aria-label': 'breadcrumb' },
    parts.filter(Boolean).map((p, i) => [i ? ' / ' : null, p.href ? link(p.href, p.text) : h('span', {}, p.text)]))
}

/**
 * `pipeline / run / ...tail` for a run, named from its blocks (Explorer.runIdentity,
 * no snapshot query). Shows `run / ...tail` until the names arrive, and keeps
 * that when they cannot be read.
 */
export function runCrumbs(ex, completionCid, tail = []) {
  const node = crumbs({ text: 'run', href: `#/run/${completionCid}` }, ...tail)
  const lookup = ex.runIdentity?.(completionCid)
  lookup?.then((id) => {
    node.replaceChildren(...crumbs(
      id.pipeline ? { text: id.pipeline, href: `#/pipeline/${enc(id.pipeline)}` } : null,
      { text: id.run_name ?? 'run', href: `#/run/${completionCid}` }, ...tail).childNodes)
  }, () => {})
  return node
}
```

Each view file starts with a two-line header naming its routes and imports what its functions use, for example:

```js
// web/src/views/item.js
// #/item/<collection>/<item> (DESIGN.md §15).
import { h, link, cid } from '../html.js'
import { pairsNode, pairsOf } from '../pairs.js'
import { errorNode, pickButton, retentionPanel, runLabelNode, runLabelText, table } from './common.js'
```

`views/selection.js` also imports `saveChoice` from `'../save-choice.js'`, `saveSequence, retryRestore` from `'../save-flow.js'`, `snippetBlock, snippetToggle` from `'../snippets.js'`, `copyOutcome` from `'../html.js'` and `Previews` from `'../previews.js'`. `views/run.js` imports `snippetBlock, snippetToggle` from `'../snippets.js'`.

`views/index.js`:

```js
// web/src/views/index.js
// Every route's view (DESIGN.md §15), and the helpers tests and app.js use.
export { copyOutcome } from '../html.js'
export * from './common.js'
export { home, idle } from './home.js'
export { pipeline, latest } from './pipeline.js'
export { run } from './run.js'
export { collection, content } from './collection.js'
export { items } from './items.js'
export { item } from './item.js'
export { selections, selection, failureBanner, compose } from './selection.js'
```

Delete `web/src/views.js`. Change `web/src/app.js` line 16 and `web/test/views.test.mjs` line 9 to import from the new path.

- [ ] **Step 4: Run every web test, then build**

Run: `cd web && npm test && node build.mjs`
Expected: PASS for every test file (the existing views tests unchanged), and `dist/index.html` written. A failure here is a missing import in one of the new files; the error names the identifier.

- [ ] **Step 5: Commit**

```bash
git add -A web/src/views web/src/views.js web/src/app.js web/test/views.test.mjs web/test/crumbs.test.mjs
git commit -s -m "refactor(web): split views.js into views/, add breadcrumbs

Moves every view verbatim; no behaviour change.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: The panel (`panel.js`)

**Files:**
- Create: `web/src/panel.js`
- Test: `web/test/panel.test.mjs`

**Interfaces:**
- Consumes: `snippetBlock`, `snippetToggle` (`snippets.js`, Task 3); `saveChoice` (`save-choice.js`); `saveSequence` (`save-flow.js`); `trayNote` (`tray.js`); `shortCid`, `crumbs` not needed; `valueText` (`pairs.js`); `Explorer.runIdentity` (Task 4), `latestSuccessfulRun`, `runLabel`; `ctx = { tray, write, trayChanged }` as `app.js` builds it.
- Produces: `panelState({ route, view, picked }) -> { state: 'none'|'whole'|'filtered'|'picked'|'saved', ... }` (pure) and `createPanel({ el, ex, ctx }) -> { draw(input?) -> Promise<void> }`. A view's `view` object (passed to `ctx.use`) is `{ completion, output, where, count, checked, pickChecked() }` on `items`, `{ completion, output, where: [], count }` on `collection` and `item`, `{ selection, members, names }` on `selection`.

The `picked` state's save controls are `compose()`'s from `web/src/views/selection.js`, moved with the same ids (`#compose-name`, `#compose-save`, `#compose-copy`, `#exists-restore`, `#write-status`) and attributes (`[data-exists]`, `[data-held-elsewhere]`, `[data-tray-entry]`, `[data-unavailable]`), so tier B's clicks find them.

- [ ] **Step 1: Write the failing tests**

```js
// web/test/panel.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { createPanel, panelState } from '../src/panel.js'

const ITEMS = { completion: 'bafyrun', output: 'markdup', where: [], count: 5, checked: 5 }

test('panelState: every row of spec §6.1', () => {
  assert.deepEqual(panelState({ route: 'run', view: null, picked: 0 }), { state: 'none' })
  assert.equal(panelState({ route: 'items', view: ITEMS, picked: 0 }).state, 'whole')
  assert.equal(panelState({ route: 'items', view: { ...ITEMS, where: [['single_end', 'bool', 'false']], count: 3, checked: 3 }, picked: 0 }).state, 'filtered')
  assert.equal(panelState({ route: 'item', view: { completion: 'c', output: 'o', where: [], count: null }, picked: 0 }).state, 'whole')
  assert.equal(panelState({ route: 'collection', view: { completion: 'c', output: 'o', where: [], count: 9 }, picked: 0 }).state, 'whole')
  assert.equal(panelState({ route: 'collection', view: { completion: null, output: 'o', where: [], count: 9 }, picked: 0 }).state, 'none')
  assert.equal(panelState({ route: 'items', view: ITEMS, picked: 2 }).state, 'picked')
  assert.equal(panelState({ route: 'compose', view: null, picked: 1 }).state, 'picked')
  assert.deepEqual(panelState({ route: 'selection', view: { selection: 'bafysel', members: 3 }, picked: 2 }), { state: 'saved', selection: 'bafysel', members: 3 })
})

test('unchecking rows keeps the call for the whole output and offers to pick the checked ones', () => {
  const s = panelState({ route: 'items', view: { ...ITEMS, checked: 3 }, picked: 0 })
  assert.equal(s.state, 'whole')
  assert.deepEqual(s.partial, { checked: 3, count: 5 })
  assert.equal(panelState({ route: 'items', view: ITEMS, picked: 0 }).partial, null)
})

function world({ lid = 'lid://hash-R4', latest = 'bafyrun', write = { available: true } } = {}) {
  installDom()
  const el = document.createElement('div')
  const tray = new Tray(null)
  const asked = []
  const ex = {
    runIdentity: async (c) => { asked.push(['runIdentity', c]); return { completion: c, pipeline: 'nf-core/rnaseq', run_name: c === 'bafyrun' ? 'high_jang' : 'newer', lid } },
    latestSuccessfulRun: async (p) => { asked.push(['latest', p]); return latest },
    runLabel: async () => ({ run_name: 'high_jang', output: 'markdup', completion: 'bafyrun' }),
  }
  const ctx = { tray, write: { served: true, ...write, hrefFor: (h) => h, run: async () => {} }, trayChanged: () => panel.draw() }
  const panel = createPanel({ el, ex, ctx })
  return { el, tray, ex, ctx, panel, asked }
}

test('whole: the run/output call, with no snapshot query (P2)', async () => {
  const { el, panel, asked } = world()
  await panel.draw({ route: 'items', view: ITEMS })
  assert.equal(el.querySelector('[data-panel-state]').dataset.panelState, 'whole')
  assert.equal(el.querySelector('[data-snippet="untyped"]').textContent, "channel.fromStore(run: 'lid://hash-R4', output: 'markdup')")
  assert.match(el.textContent, /All 5 items of markdup/)
  assert.equal(el.querySelector('[data-run-mode]').dataset.runMode, 'this')
  assert.deepEqual(asked, [['runIdentity', 'bafyrun']])
})

test('filtered: where: from the chips, and data-where for the driver', async () => {
  const { el, panel } = world()
  const where = [['single_end', 'bool', 'false']]
  await panel.draw({ route: 'items', view: { ...ITEMS, where, count: 3, checked: 3 } })
  const call = el.querySelector('[data-snippet="untyped"]')
  assert.equal(call.textContent, "channel.fromStore(run: 'lid://hash-R4', output: 'markdup', where: [single_end: false])")
  assert.deepEqual(JSON.parse(call.dataset.where), where)
  assert.match(el.textContent, /3 items of markdup, where single_end = false/)
})

test('latest good run: asked only when chosen; a newer run is named; none keeps "this run" (P3)', async () => {
  const w = world({ latest: 'bafynewer' })
  await w.panel.draw({ route: 'items', view: ITEMS })
  assert.ok(!w.asked.some(([k]) => k === 'latest'))
  const latestButton = w.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'latest good run')
  await latestButton.click()
  await new Promise(r => setTimeout(r, 0))
  assert.equal(w.el.querySelector('[data-run-mode]').dataset.runMode, 'latest')
  assert.equal(w.el.querySelector('[data-snippet="untyped"]').textContent,
    "channel.fromStore(run: 'latest', pipeline: 'nf-core/rnaseq', output: 'markdup')")
  assert.match(w.el.textContent, /latest good run is now newer/)
  const none = world({ latest: null })
  await none.panel.draw({ route: 'items', view: ITEMS })
  await none.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'latest good run').click()
  await new Promise(r => setTimeout(r, 0))
  assert.equal(none.el.querySelector('[data-run-mode]').dataset.runMode, 'this')
  assert.match(none.el.textContent, /no good run/)
})

test('the run switch goes back to "this run" for another output', async () => {
  const w = world({ latest: 'bafyrun' })
  await w.panel.draw({ route: 'items', view: ITEMS })
  await w.el.querySelector('[data-run-mode]').querySelectorAll('button').find(b => b.textContent === 'latest good run').click()
  await new Promise(r => setTimeout(r, 0))
  await w.panel.draw({ route: 'items', view: { ...ITEMS, output: 'quant' } })
  assert.equal(w.el.querySelector('[data-run-mode]').dataset.runMode, 'this')
})

test('M of N checked offers "Pick these M"', async () => {
  const { el, panel } = world()
  let picked = 0
  await panel.draw({ route: 'items', view: { ...ITEMS, checked: 3, pickChecked: () => { picked++ } } })
  const button = el.querySelector('#pick-checked')
  assert.equal(button.textContent, 'Pick these 3')
  await button.click()
  assert.equal(picked, 1)
})

test('picked: grouped by output and run, each entry keeps [data-tray-entry], and the save controls keep their ids', async () => {
  const { el, panel, tray } = world()
  tray.addMany([{ address: 'i1', via: ['coll'] }, { address: 'i2', via: ['coll'] }, { address: 'sel', kind: 'selection' }])
  await panel.draw({ route: 'compose', view: null })
  await new Promise(r => setTimeout(r, 0))
  assert.equal(el.querySelector('[data-panel-state]').dataset.panelState, 'picked')
  assert.equal(el.querySelectorAll('[data-tray-entry]').length, 3)
  assert.match(el.textContent, /markdup · high_jang · 2/)
  for (const id of ['#compose-name', '#compose-save', '#write-status']) assert.ok(el.querySelector(id), id)
  assert.ok(!el.querySelector('[data-snippet]'))
})

test('picked: removing a group, and Clear', async () => {
  const { el, panel, tray } = world()
  tray.addMany([{ address: 'i1', via: ['coll'] }, { address: 'i2', via: ['other'] }])
  await panel.draw({ route: 'compose', view: null })
  await el.querySelectorAll('button').find(b => b.getAttribute('aria-label') === 'remove these').click()
  assert.equal(tray.size, 1)
  await el.querySelectorAll('button').find(b => b.textContent === 'Clear').click()
  assert.equal(tray.size, 0)
})

test('picked while writing is unavailable says why and offers no Save', async () => {
  const { el, panel, tray } = world({ write: { available: false, reason: 'read only' } })
  tray.add({ address: 'i1', via: ['coll'] })
  await panel.draw({ route: 'compose', view: null })
  assert.equal(el.querySelector('[data-unavailable]').textContent, 'read only')
  assert.ok(el.querySelector('#compose-save').disabled)
})

test('saved: the Selection call and its samplesheets', async () => {
  const { el, panel } = world()
  await panel.draw({ route: 'selection', view: { selection: 'bafysel', members: 4, names: ['treated BAMs'] } })
  assert.equal(el.querySelector('[data-snippet="untyped"]').textContent, "channel.fromStore(selection: 'bafysel')")
  assert.ok(el.querySelector('[data-samplesheet="csv"]'))
  assert.match(el.textContent, /treated BAMs/)
})

test('the panel never carries [data-error], [data-run] or [data-selection] (P14, Review Focus 4)', async () => {
  const { el, panel, ex } = world()
  ex.runIdentity = async () => { throw Object.assign(new Error('gone'), { code: 'block_missing' }) }
  await panel.draw({ route: 'items', view: ITEMS })
  assert.ok(!el.querySelector('[data-error]'))
  assert.match(el.textContent, /gone/)
  assert.ok(!el.querySelector('[data-run]') && !el.querySelector('[data-selection]'))
})

test('a slower draw that finishes after a newer one does not overwrite it', async () => {
  const { el, panel, ex } = world()
  let release
  ex.runIdentity = (c) => (c === 'slow' ? new Promise(r => { release = () => r({ pipeline: 'p', lid: 'lid://slow' }) }) : Promise.resolve({ pipeline: 'p', lid: 'lid://fast' }))
  const slow = panel.draw({ route: 'items', view: { ...ITEMS, completion: 'slow' } })
  await panel.draw({ route: 'items', view: { ...ITEMS, completion: 'fast' } })
  release()
  await slow
  assert.match(el.querySelector('[data-snippet="untyped"]').textContent, /lid:\/\/fast/)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/panel.test.mjs`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement**

```js
// web/src/panel.js
// The "Use in a workflow" panel (explorer layout B spec §6). panelState picks
// one state from the route, what the view reported (ctx.use) and the picked
// list; createPanel draws it. A call on screen always returns what it says.
// The panel reads runs from their blocks only (Explorer.runIdentity), and
// shows failures without [data-error] (plan P2, P14).
import { h, link } from './html.js'
import { snippetBlock, snippetToggle } from './snippets.js'
import { saveChoice } from './save-choice.js'
import { saveSequence } from './save-flow.js'
import { trayNote } from './tray.js'
import { valueText } from './pairs.js'

const OUTPUT_ROUTES = new Set(['items', 'item', 'collection'])
const counted = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`
const short = (text) => (text.length > 20 ? `${text.slice(0, 10)}...${text.slice(-6)}` : text)
const whereText = (where) => where.map(([p, t, v]) => `${p} = ${valueText(t, v)}`).join(', ')

/** Spec §6.1. `view` is what the current view passed to ctx.use; `picked` the tray's size. */
export function panelState({ route, view = null, picked = 0 }) {
  if (route === 'selection' && view?.selection) return { state: 'saved', selection: view.selection, members: view.members ?? null }
  if (picked > 0) return { state: 'picked' }
  if (OUTPUT_ROUTES.has(route) && view?.completion && view.output) {
    const where = view.where ?? []
    const count = view.count ?? null
    const checked = view.checked ?? count
    return { state: where.length ? 'filtered' : 'whole', completion: view.completion, output: view.output, where, count,
      partial: count !== null && checked !== null && checked < count ? { checked, count } : null }
  }
  return { state: 'none' }
}

const deletedNote = (deletion, where) => deletion === 'deleted' ? ` It is deleted ${where}.`
  : deletion === 'conflicted' ? ` Its deletion is in conflict ${where}.` : ''

export function createPanel({ el, ex, ctx }) {
  let drawn = 0
  let last = { route: 'open', view: null }
  let mode = 'this'
  let modeKey = null
  let note = null

  async function draw(input = last) {
    last = input
    const mine = ++drawn
    const s = panelState({ ...input, picked: ctx.tray.size })
    const key = s.completion ? `${s.completion}/${s.output}` : null
    if (key !== modeKey) { mode = 'this'; note = null; modeKey = key }
    let body
    try {
      body = await bodyOf(s, input.view)
    } catch (e) {
      body = h('p', { class: 'warn' }, `Could not prepare the call: ${e?.message ?? e}`)
    }
    if (mine !== drawn) return
    el.replaceChildren(h('section', { 'data-panel-state': s.state }, h('h2', {}, 'Use in a workflow'), body))
  }

  async function bodyOf(s, view) {
    if (s.state === 'saved') return saved(s, view)
    if (s.state === 'picked') return picked()
    if (s.state === 'none') return h('p', { class: 'muted' }, 'Open an output to use it in a workflow.')
    return output(s, view)
  }

  async function output(s, view) {
    const id = await ex.runIdentity(s.completion)
    const what = s.state === 'filtered' ? `${counted(s.count ?? 0, 'item')} of ${s.output}, where ${whereText(s.where)}`
      : s.count === null ? `Every item of ${s.output}` : `All ${counted(s.count, 'item')} of ${s.output}`
    const spec = mode === 'latest' ? { kind: 'latest', pipeline: id.pipeline, output: s.output, where: s.where }
      : id.lid ? { kind: 'run', lid: id.lid, output: s.output, where: s.where } : null
    return [
      h('p', {}, what),
      id.pipeline ? runSwitch(id) : null,
      spec ? [snippetBlock(spec), h('p', { class: 'muted' }, 'Script kind: ', snippetToggle())]
        : h('p', { class: 'muted' }, 'This run has no lineage ID; read it as the latest good run instead.'),
      mode === 'latest' ? h('p', { class: 'muted' }, '"latest" may match different items after the next run.') : null,
      note ? h('p', { class: 'muted' }, note) : null,
      s.partial ? h('p', {}, `${s.partial.checked} of ${s.partial.count} checked. `,
        h('button', { type: 'button', id: 'pick-checked', disabled: s.partial.checked === 0, onclick: () => view?.pickChecked?.() },
          `Pick these ${s.partial.checked}`)) : null,
    ]
  }

  function runSwitch(id) {
    const choose = async (next) => {
      try {
        if (next === 'latest') {
          const best = await ex.latestSuccessfulRun(id.pipeline)
          if (!best) { note = `${id.pipeline} has no good run, so "latest" would find nothing.`; return draw() }
          note = best === id.completion ? null : `The latest good run is now ${(await ex.runIdentity(best)).run_name ?? best}, not this one.`
        } else {
          note = null
        }
        mode = next
      } catch (e) {
        note = `Could not find the latest good run: ${e?.message ?? e}`
      }
      return draw()
    }
    return h('p', { class: 'run-switch', role: 'group', 'aria-label': 'which run', 'data-run-mode': mode },
      ['this', 'latest'].map(m => h('button', { type: 'button', 'aria-pressed': String(mode === m), onclick: () => choose(m) },
        m === 'this' ? 'this run' : 'latest good run')))
  }

  function saved(s, view) {
    const name = view?.names?.length ? `Selection "${view.names.join(' / ')}"` : 'This Selection'
    return [
      h('p', {}, name, s.members === null ? '' : ` · ${counted(s.members, 'member')}`),
      snippetBlock({ kind: 'selection', cid: s.selection }),
      h('p', { class: 'muted' }, 'Script kind: ', snippetToggle()),
      ctx.write.served ? h('p', {}, 'Samplesheet: ',
        h('a', { href: `api/samplesheet/${s.selection}.csv`, download: '', 'data-samplesheet': 'csv' }, 'CSV'), ' ',
        h('a', { href: `api/samplesheet/${s.selection}.json`, download: '', 'data-samplesheet': 'json' }, 'JSON')) : null,
    ]
  }

  function picked() {
    const entries = ctx.tray.entries()
    const groups = new Map()
    for (const e of entries) {
      const key = e.kind === 'selection' ? `selection:${e.address}` : `via:${e.via[0] ?? '-'}`
      groups.set(key, [...(groups.get(key) ?? []), e])
    }
    const removeAll = (list) => { for (const e of list) ctx.tray.remove(e.address); ctx.trayChanged() }
    const list = h('ul', { class: 'picked' }, [...groups].map(([key, members]) => {
      const label = h('span', {}, key.startsWith('selection:') ? 'Selection' : key === 'via:-' ? 'picked by a query across runs' : 'an output')
      if (key.startsWith('via:') && key !== 'via:-') {
        ex.runLabel(key.slice(4)).then((l) => { if (l) label.textContent = `${l.output} · ${l.run_name ?? 'unnamed run'}` }, () => {})
      }
      return h('li', {}, label, ` · ${members.length} `,
        h('button', { type: 'button', 'aria-label': 'remove these', onclick: () => removeAll(members) }, '✕'),
        h('ul', { class: 'picked-entries' }, members.map(e => h('li', { 'data-tray-entry': e.address, 'data-kind': e.kind },
          h('code', { class: 'cid', title: e.address }, short(e.address))))))
    }))
    const status = h('div', { id: 'write-status' })
    const name = h('input', { id: 'compose-name', placeholder: 'A name, e.g. treated BAMs' })
    const blocked = !ctx.write.available || entries.length === 0 || ctx.tray.onlyOneSelection()
    const save = h('button', { type: 'button', id: 'compose-save', class: 'primary', disabled: blocked,
      onclick: (event) => ctx.write.run(status, () => saveFlow(status, name), event.currentTarget) }, 'Save as Selection')
    const clear = h('button', { type: 'button', onclick: () => { ctx.tray.clear(); ctx.trayChanged() } }, 'Clear')
    return [
      h('p', {}, `Picked: ${counted(entries.length, 'item')}`),
      list,
      trayNote(ctx.tray) ? h('p', { class: 'warn' }, trayNote(ctx.tray)) : null,
      ctx.tray.onlyOneSelection() ? h('p', { class: 'muted' }, 'A Selection whose only member is another Selection is legal, but the explorer does not make one: add an item too.') : null,
      h('p', { class: 'muted' }, 'A filter cannot describe these, so save them as a Selection: a fixed list of these exact items.'),
      ctx.write.available ? null : h('p', { 'data-unavailable': '', class: 'muted' }, ctx.write.reason),
      h('p', {}, name), h('p', {}, save, ' ', clear),
      status,
    ]
  }

  // compose()'s save, moved (DESIGN.md §16 decisions 9, 23): dry run, then
  // "exists here", "held elsewhere", or save and name.
  async function saveFlow(status, name) {
    const members = ctx.tray.toMembers()
    const saveAndName = async (choice) => {
      const { address, failures } = await saveSequence(ctx.write.writer, members, choice, name.value,
        { onSaved: () => { ctx.tray.clear(); ctx.trayChanged() } })
      return { address, href: `#/selection/${address}`, failures }
    }
    const dry = await ctx.write.writer.selection(members, { dryRun: true })
    const choice = saveChoice(dry)
    const cancel = h('button', { type: 'button', onclick: () => status.replaceChildren() }, 'cancel')
    if (choice.state === 'here') {
      const named = dry.names.length === 0 ? ', unnamed'
        : dry.names.length === 1 ? ` as ${dry.names[0]}` : ` as ${dry.names.join(', ')} (in conflict)`
      status.replaceChildren(h('p', { 'data-exists': dry.address, 'data-deletion': choice.deletion, 'data-names': JSON.stringify(dry.names) },
        `This Selection already exists${named}.${deletedNote(choice.deletion, 'in this composition')} `,
        link(ctx.write.hrefFor(`#/selection/${dry.address}`), 'Open it to rename it'),
        choice.restore.length ? [', ', h('button', { type: 'button', id: 'exists-restore', onclick: (e) => ctx.write.run(status, async () => {
          await ctx.write.writer.undo(dry.address, choice.restore)
          return { address: dry.address, href: `#/selection/${dry.address}` }
        }, e.currentTarget) }, 'Restore')] : null,
        ' or ', cancel, '.'))
      return { outcome: 'exists' }
    }
    if (choice.state === 'elsewhere') {
      if (!name.value.trim() && choice.prefill) name.value = choice.prefill
      const named = choice.names.length === 0 ? ', unnamed'
        : choice.names.length === 1 ? ` as ${choice.names[0]}` : ` as ${choice.names.join(', ')} (in conflict)`
      status.replaceChildren(h('p', { 'data-held-elsewhere': dry.address, 'data-deletion': choice.deletion, 'data-names': JSON.stringify(choice.names) },
        `This Selection is already held in another member${named}.${deletedNote(choice.deletion, 'in this composition')} `,
        h('button', { type: 'button', id: 'compose-copy', onclick: (e) => ctx.write.run(status, () => saveAndName(choice), e.currentTarget) },
          choice.restore.length ? 'Restore a copy here' : 'Save a copy here'), ' or ', cancel, '.'))
      return { outcome: 'elsewhere' }
    }
    return saveAndName(choice)
  }

  return { draw }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd web && node --test test/panel.test.mjs`
Expected: PASS, 13 tests. The fake DOM (`web/test/dom.mjs`) has no descendant combinator, which is why the tests chain `querySelector(...).querySelectorAll(...)`; its `querySelectorAll` returns an array, so `.find` works on it.

- [ ] **Step 5: Commit**

```bash
git add web/src/panel.js web/test/panel.test.mjs
git commit -s -m "feat(web): the Use in a workflow panel and its five states

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: The left column (`nav.js`)

**Files:**
- Create: `web/src/nav.js`
- Test: `web/test/nav.test.mjs`

**Interfaces:**
- Consumes: `Explorer.pipelines()`, `Explorer.runPage(name, { limit })`, `Explorer.selectionPage({ limit })` (its rows carry `cid` and `state.names`); `statusTone`, `whenText` (Task 1).
- Produces: `createNav(ex, { el }) -> { load(), reloadSelections(), mark({ pipeline, run, expand }) }`. It writes `data-nav-run` and `data-nav-selection`, never `data-run`, `data-selection` or `data-error` (P14, Review Focus 4).

- [ ] **Step 1: Write the failing test**

```js
// web/test/nav.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { createNav } from '../src/nav.js'

function fakeEx() {
  const asked = []
  const ex = {
    asked,
    pipelines: async () => { asked.push('pipelines'); return [{ pipeline: 'nf-core/sarek', runs: 3 }, { pipeline: 'nf-core/rnaseq', runs: 12 }] },
    selectionPage: async ({ limit }) => { asked.push(`selections ${limit}`); return { rows: [{ cid: 'bafysel', state: { names: ['treated BAMs'] } }, { cid: 'bafyunnamed', state: { names: [] } }] } },
    runPage: async (name, { limit }) => {
      asked.push(`runs ${name} ${limit}`)
      return { rows: [{ completion_cid: 'bafygood', run_name: 'high_jang', status: 'succeeded', possibly_incomplete: 0, finished_at: '2026-09-30T21:10:38.345Z' },
        { completion_cid: 'bafybad', run_name: 'happy_rubens', status: 'failed', possibly_incomplete: 1, finished_at: '2026-09-30T16:55:59.157Z' }], total: 12 }
    },
  }
  return ex
}

test('load lists pipelines A–Z and five Selections, and reads no runs (P1)', async () => {
  installDom()
  const el = document.createElement('div')
  const ex = fakeEx()
  const nav = createNav(ex, { el })
  await nav.load()
  assert.deepEqual(ex.asked.sort(), ['pipelines', 'selections 5'])
  const names = el.querySelectorAll('[data-nav-pipeline]').map(n => n.dataset.navPipeline)
  assert.deepEqual(names, ['nf-core/rnaseq', 'nf-core/sarek'])
  assert.deepEqual(el.querySelectorAll('[data-nav-selection]').map(n => n.textContent), ['treated BAMs', 'unnamed'])
  assert.ok(el.querySelector('input[disabled]'), 'the search box is reserved, disabled')
})

test('mark with expand reads ten runs of that pipeline; without expand it reads nothing', async () => {
  installDom()
  const el = document.createElement('div')
  const ex = fakeEx()
  const nav = createNav(ex, { el })
  await nav.load()
  await nav.mark({ pipeline: 'nf-core/rnaseq', run: 'bafygood' })
  assert.ok(!ex.asked.some(a => a.startsWith('runs')))
  await nav.mark({ pipeline: 'nf-core/rnaseq', run: 'bafygood', expand: true })
  assert.ok(ex.asked.includes('runs nf-core/rnaseq 10'))
  const runs = el.querySelectorAll('[data-nav-run]')
  assert.deepEqual(runs.map(r => r.dataset.navRun), ['bafygood', 'bafybad'])
  assert.equal(runs[0].getAttribute('aria-current'), 'page')
  assert.ok(runs[0].querySelector('.dot-good') && runs[1].querySelector('.dot-bad'))
  assert.ok(el.querySelectorAll('a').some(a => a.textContent === 'all runs…' && a.getAttribute('href') === '#/pipeline/nf-core%2Frnaseq'))
  await nav.mark({ pipeline: 'nf-core/rnaseq', run: 'bafybad', expand: true })
  assert.equal(ex.asked.filter(a => a.startsWith('runs')).length, 1, 'one read per pipeline')
})

test('clicking a pipeline expands it', async () => {
  installDom()
  const el = document.createElement('div')
  const ex = fakeEx()
  const nav = createNav(ex, { el })
  await nav.load()
  await el.querySelector('[data-nav-pipeline="nf-core/sarek"]').querySelector('button').click()
  await new Promise(r => setTimeout(r, 0))
  assert.ok(ex.asked.includes('runs nf-core/sarek 10'))
})

test('failures show as warnings, never [data-error]; no [data-run] or [data-selection] anywhere (P14)', async () => {
  installDom()
  const el = document.createElement('div')
  const ex = fakeEx()
  ex.pipelines = async () => { throw Object.assign(new Error('range read failed'), { code: 'query_failed' }) }
  const nav = createNav(ex, { el })
  await nav.load()
  assert.match(el.textContent, /range read failed/)
  assert.ok(!el.querySelector('[data-error]'))
  const okEl = document.createElement('div')
  const ok = createNav(fakeEx(), { el: okEl })
  await ok.load()
  await ok.mark({ pipeline: 'nf-core/rnaseq', expand: true })
  assert.ok(okEl.querySelector('[data-nav-run]'), 'runs are listed')
  assert.ok(!okEl.querySelector('[data-run]') && !okEl.querySelector('[data-selection]') && !okEl.querySelector('[data-error]'))
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/nav.test.mjs`
Expected: FAIL, module not found.

- [ ] **Step 3: Implement**

```js
// web/src/nav.js
// The left column (explorer layout B spec §4): a reserved search box,
// pipelines A–Z with their newest runs, and recent Selections; app.js keeps
// the store status beneath it. It reads the snapshot at load, on a reader's
// click, and when the run or pipeline view asks (mark with expand), so query
// 3's measured cost stays the query's own (Gate assertion 2, plan P1). Its
// attributes are data-nav-*, never the Gate's data-run or data-selection.
import { h, link } from './html.js'
import { statusTone, whenText } from './words.js'

const NAV_RUNS = 10
const NAV_SELECTIONS = 5
const enc = encodeURIComponent
const warn = (what, e) => h('p', { class: 'warn' }, `${what}: ${e?.message ?? e}`)

export function createNav(ex, { el }) {
  let pipelines = []
  let selections = []
  let pipelinesError = null
  let selectionsError = null
  const open = new Set()
  const runs = new Map()       // pipeline -> { rows, total } once read
  const runErrors = new Map()
  const asking = new Map()
  let here = { pipeline: null, run: null }

  function readRuns(name) {
    if (runs.has(name)) return Promise.resolve()
    if (!asking.has(name)) {
      asking.set(name, ex.runPage(name, { limit: NAV_RUNS }).then(
        (page) => { runs.set(name, page); runErrors.delete(name) },
        (e) => { runErrors.set(name, e) }).finally(() => asking.delete(name)))
    }
    return asking.get(name)
  }

  function runList(name) {
    if (runErrors.has(name)) return warn('Could not list its runs', runErrors.get(name))
    const page = runs.get(name)
    if (!page) return h('p', { class: 'muted' }, 'loading...')
    return h('ul', { class: 'nav-runs' },
      page.rows.map(r => h('li', { 'data-nav-run': r.completion_cid, 'aria-current': r.completion_cid === here.run ? 'page' : null },
        link(`#/run/${r.completion_cid}`, h('span', { class: `dot dot-${statusTone(r)}`, 'aria-hidden': 'true' }, '●'), ' ',
          r.run_name ?? 'unnamed run', ' ', h('span', { class: 'muted' }, whenText(r.finished_at).split(',')[0])))),
      page.total > page.rows.length ? h('li', {}, link(`#/pipeline/${enc(name)}`, 'all runs…')) : null)
  }

  function draw() {
    el.replaceChildren(
      h('input', { type: 'search', disabled: true, placeholder: 'Search samples, runs… (later)', 'aria-label': 'search (not yet available)' }),
      h('h2', { class: 'nav-head' }, 'Pipelines'),
      pipelinesError ? warn('Could not list pipelines', pipelinesError)
        : pipelines.length === 0 ? h('p', { class: 'muted' }, 'No runs yet.')
          : h('ul', { class: 'nav-pipelines' }, pipelines.map(p => h('li', { 'data-nav-pipeline': p.pipeline },
            h('button', { type: 'button', class: 'nav-toggle', 'aria-expanded': String(open.has(p.pipeline)), onclick: () => {
              if (open.has(p.pipeline)) open.delete(p.pipeline)
              else { open.add(p.pipeline); readRuns(p.pipeline).then(draw) }
              draw()
            } }, open.has(p.pipeline) ? '▾ ' : '▸ ', p.pipeline, ' ', h('span', { class: 'muted' }, String(p.runs))),
            open.has(p.pipeline) ? runList(p.pipeline) : null))),
      h('h2', { class: 'nav-head' }, 'Selections'),
      selectionsError ? warn('Could not list Selections', selectionsError)
        : h('ul', { class: 'nav-selections' },
          selections.map(s => h('li', {}, h('a', { href: `#/selection/${s.cid}`, 'data-nav-selection': s.cid },
            s.state?.names?.length ? s.state.names.join(' / ') : 'unnamed'))),
          h('li', {}, link('#/selections', 'all selections…'))))
  }

  async function readSelections() {
    try {
      selections = (await ex.selectionPage({ limit: NAV_SELECTIONS })).rows
      selectionsError = null
    } catch (e) {
      selectionsError = e
    }
  }

  return {
    async load() {
      await Promise.all([
        ex.pipelines().then((p) => { pipelines = [...p].sort((a, b) => (a.pipeline < b.pipeline ? -1 : a.pipeline > b.pipeline ? 1 : 0)); pipelinesError = null },
          (e) => { pipelinesError = e }),
        readSelections(),
      ])
      draw()
    },
    async reloadSelections() {
      runs.clear()
      await readSelections()
      for (const name of open) await readRuns(name)
      draw()
    },
    async mark({ pipeline = null, run = null, expand = false } = {}) {
      here = { pipeline, run }
      if (expand && pipeline) {
        open.add(pipeline)
        await readRuns(pipeline)
      }
      draw()
    },
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd web && node --test test/nav.test.mjs`
Expected: PASS, 4 tests. `whenText(...).split(',')[0]` keeps the date part of the locale string; if the test machine's locale formats without a comma the whole string shows, which is acceptable.

- [ ] **Step 5: Commit**

```bash
git add web/src/nav.js web/test/nav.test.mjs
git commit -s -m "feat(web): the left column of pipelines, runs and Selections

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: The shell: three regions, styles, narrow screens

**Files:**
- Create: `web/src/styles.js`
- Modify: `web/src/index.html` (whole body and head), `web/src/app.js` (`ROUTES` compose entry, `render`, `updateTray`, `runWrite`, `start`), `web/src/views/selection.js` (delete `compose` and `deletedNote`), `web/src/views/index.js` (drop `compose`), `web/test/views.test.mjs` (move the compose test)
- Test: `web/test/views.test.mjs`, `web/test/panel.test.mjs`, manual build check

**Interfaces:**
- Consumes: `createPanel` (Task 7), `createNav` (Task 8), `views.home`.
- Produces: `ctx.use(view)` and `ctx.mark({ pipeline, run, expand })` for every view (both may be absent in tests, so views call `ctx.use?.(...)` and `ctx.mark?.(...)`); the panel is drawn before `finish('ready')`, so a driver waiting on `body[data-render]` sees the panel's call. `#tray[data-count]`, `#tray-unsaved`, `#stale`, `#snapshot-mode`, `#members`, `#where` keep their ids.

- [ ] **Step 1: Move the compose test.** In `web/test/views.test.mjs`, delete the test `'the compose tray counts only item entries toward the preview cap, so a Selection entry does not push an item past it'` and remove `compose` from the import. Its rule (item entries count toward the preview cap) no longer applies: the panel lists entries by address and fetches no previews. Run `cd web && npm test`; expected PASS.

- [ ] **Step 2: Write `web/src/styles.js`**

```js
// web/src/styles.js
// The page's stylesheet (explorer layout B spec §7): tokens for light and dark,
// the three-region layout, and the narrow-screen drawer and sheet (spec §3.1).
// app.js appends it to <head> at start, beside pairs.js's PAIRS_CSS.
export const APP_CSS = `
  :root { color-scheme: light dark; --fg: #1d1d1f; --bg: #ffffff; --side: #f6f7f9; --muted: #6e6e73; --line: #d2d2d7;
    --accent: #2563eb; --accent-fg: #ffffff; --good: #15803d; --warn: #9a5b00; --bad: #b3261e; --num: #0550ae; --true: #116329;
    --false: #cf222e; --pill-key: #f6f8fa; --chip: #eef1ff; --code: #f6f8fa; }
  @media (prefers-color-scheme: dark) { :root { --fg: #f5f5f7; --bg: #161617; --side: #1d1d1f; --muted: #a1a1a6; --line: #3a3a3c;
    --accent: #6ea8ff; --accent-fg: #0b1220; --good: #56d364; --warn: #ffd180; --bad: #ff8a80; --num: #79c0ff; --true: #56d364;
    --false: #ff7b72; --pill-key: #2c2c2e; --chip: #2a2f45; --code: #1f1f22; } }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 system-ui, sans-serif; color: var(--fg); background: var(--bg); }
  a { color: inherit; } h1 { font-size: 1.35rem; margin: .4rem 0 .2rem; } h2 { font-size: 1.02rem; }
  code, .cid { font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; overflow-wrap: anywhere; }
  button { font: inherit; } button.primary { background: var(--accent); color: var(--accent-fg); border: 1px solid var(--accent); border-radius: 4px; padding: .15rem .6rem; }
  #bar { position: sticky; top: 0; z-index: 4; display: flex; gap: 1rem; align-items: center; padding: .5rem 16px;
    background: var(--bg); border-bottom: 1px solid var(--line); }
  #bar .brand { font-weight: 600; text-decoration: none; } #bar .spacer { flex: 1; }
  #nav-toggle, #use-toggle { display: none; }
  #shell { display: grid; grid-template-columns: 15rem minmax(0, 1fr) 21rem; min-height: calc(100vh - 2.7rem); }
  #nav { background: var(--side); border-right: 1px solid var(--line); padding: .75rem; overflow-y: auto; }
  #nav input[type=search] { width: 100%; }
  #nav ul { list-style: none; padding: 0; margin: .25rem 0; } #nav li { padding: .1rem 0; }
  #nav .nav-runs { padding-left: .9rem; } #nav [aria-current=page] > a { font-weight: 600; color: var(--accent); }
  #nav .nav-toggle { background: none; border: 0; padding: 0; cursor: pointer; text-align: left; }
  .nav-head { font-size: .75rem; letter-spacing: .05em; text-transform: uppercase; color: var(--muted); margin: .9rem 0 .2rem; }
  #store-status { margin-top: 1.5rem; font-size: 12px; } #store-status > * { margin: .2rem 0; }
  #main { padding: 0 16px 4rem; min-width: 0; }
  #panel { background: var(--side); border-left: 1px solid var(--line); padding: .75rem; }
  #panel-inner { position: sticky; top: 3.3rem; }
  .dot-good { color: var(--good); } .dot-bad { color: var(--bad); }
  .crumbs { color: var(--muted); margin-top: .75rem; } .crumbs a { color: inherit; }
  .muted { color: var(--muted); } [data-error] { color: var(--bad); } .warn, #stale[data-notice] { color: var(--warn); }
  table { border-collapse: collapse; width: 100%; } td, th { text-align: left; padding: .3rem .5rem; border-bottom: 1px solid var(--line); vertical-align: top; }
  th { color: var(--muted); font-weight: 500; font-size: 12px; } .scroll { overflow-x: auto; }
  .chip { display: inline-block; background: var(--chip); border-radius: 4px; padding: 0 5px; margin: 0 2px 2px 0; font-size: 12px; }
  .ext { display: inline-block; background: var(--code); border-radius: 3px; padding: 0 4px; margin-right: 2px; font: 11px ui-monospace, monospace; }
  .constant { font-size: 12px; } .badge { display: inline-block; border: 1px solid var(--line); border-radius: 999px; padding: .05rem .6rem; font-size: 12px; margin-right: .35rem; }
  details.fold { border-top: 1px solid var(--line); padding: .4rem 0; } details.fold > summary { cursor: pointer; font-weight: 600; }
  form { display: grid; gap: .5rem; margin: 1rem 0; } fieldset { border: 1px solid var(--line); }
  .rows { list-style: none; padding: 0; margin: .5rem 0; } .row { padding: .4rem 0; border-bottom: 1px solid var(--line); }
  .row-head { display: flex; flex-wrap: wrap; gap: .4rem; align-items: baseline; } .row-action { margin-left: auto; }
  .row-pairs, .row-via, .row-cid { margin: .15rem 0 0 1.6rem; } .row-pairs:empty { display: none; }
  .row-actions { display: flex; flex-wrap: wrap; gap: .5rem; align-items: baseline; }
  .snippet { display: flex; gap: .5rem; align-items: flex-start; margin: .35rem 0; }
  .snippet pre { margin: 0; padding: .4rem .6rem; border: 1px solid var(--line); background: var(--bg); flex: 1; white-space: pre-wrap; overflow-wrap: anywhere; }
  .snippet-toggle button[aria-pressed=true], .run-switch button[aria-pressed=true] { font-weight: 600; }
  .picked, .picked-entries { list-style: none; padding: 0; } .picked-entries { margin-left: 1rem; font-size: 12px; }
  @media (max-width: 1200px) {
    #shell { grid-template-columns: minmax(0, 1fr) 21rem; }
    #nav { display: none; position: fixed; top: 2.7rem; bottom: 0; left: 0; width: 16rem; z-index: 3; }
    body[data-nav=open] #nav { display: block; } #nav-toggle { display: inline-block; }
  }
  @media (max-width: 900px) {
    #shell { grid-template-columns: minmax(0, 1fr); }
    #panel { display: none; position: fixed; left: 0; right: 0; bottom: 0; max-height: 70vh; overflow-y: auto; z-index: 3;
      border-left: 0; border-top: 1px solid var(--line); }
    #panel-inner { position: static; }
    body[data-panel=open] #panel { display: block; } #use-toggle { display: inline-block; }
  }
`
```

- [ ] **Step 3: Replace `web/src/index.html`**

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>nf-blocks explorer</title>
</head>
<body data-state="loading" data-render="0" data-route="open" data-nav="closed" data-panel="closed">
<header id="bar">
  <button type="button" id="nav-toggle" aria-label="pipelines and runs">☰</button>
  <a class="brand" href="#/">nf-blocks</a>
  <span class="spacer"></span>
  <a id="tray" href="#/compose" data-count="0">Picked (0)</a>
  <span id="tray-unsaved" class="warn" role="status" hidden></span>
  <button type="button" id="use-toggle">Use</button>
</header>
<div id="shell">
  <nav id="nav" aria-label="pipelines, runs and Selections">
    <div id="nav-body"></div>
    <section id="store-status" class="muted" aria-label="store">
      <nav id="members"></nav>
      <div id="where"></div>
      <div id="snapshot-mode"></div>
      <div id="stale"></div>
    </section>
  </nav>
  <main id="main"><p class="muted">Opening the snapshot...</p></main>
  <aside id="panel" aria-label="Use in a workflow"><div id="panel-inner"></div></aside>
</div>
<!--APP-->
</body>
</html>
```

- [ ] **Step 4: Wire `web/src/app.js`.** Make these changes:

  1. Imports: add `import { APP_CSS } from './styles.js'`, `import { createPanel } from './panel.js'`, `import { createNav } from './nav.js'`; keep `import * as views from './views/index.js'`.
  2. Route `compose` becomes (plan P5):
     ```js
     ['compose', /^#\/compose$/, (ex, m, ctx) => { document.body.dataset.panel = 'open'; return views.home(ex, ctx) }],
     ```
  3. Module state: add `let panel = null` and `let nav = null`.
  4. `render()` becomes:
     ```js
     async function render() {
       const mine = ++sequence
       const main = document.getElementById('main')
       const hash = location.hash || '#/'
       const route = ROUTES.find(([, pattern]) => pattern.test(hash))
       const name = route ? route[0] : 'unknown'
       document.body.dataset.state = 'loading'
       document.body.dataset.route = name
       const progress = h('p', { id: 'progress', class: 'muted' })
       main.replaceChildren(h('p', { class: 'muted' }, 'Loading...'), progress)
       let view = null
       let rendered = false
       const ctx = {
         progress: (done, total) => { if (mine === sequence) progress.textContent = `fetched ${done} of ${total} blocks`; updateStale() },
         tray, write, trayChanged: updateTray, rerender: render,
         // What the view shows, for the panel (layout B spec §6); redrawn when it changes after the render.
         use: (v) => { if (mine !== sequence) return; view = v; if (rendered) panel.draw({ route: name, view }) },
         // Where the reader is, for the left column (plan P1: `expand` only from the run and pipeline views).
         mark: (where) => { if (mine === sequence) nav.mark(where).catch(() => {}) },
       }
       try {
         if (!route) throw Object.assign(new Error(`there is no view for ${hash}`), { code: 'bad_route' })
         const node = await route[2](explorer, hash.match(route[1]), ctx)
         if (mine !== sequence) return
         if (pendingBanner) {
           if (hash === `#/selection/${pendingBanner.address}`) node.prepend(views.failureBanner(pendingBanner.address, pendingBanner.failures, ctx))
           pendingBanner = null
         }
         main.replaceChildren(node)
         rendered = true
         await panel.draw({ route: name, view })
         if (mine !== sequence) return
         updateStale()
         finish('ready')
       } catch (e) {
         if (mine !== sequence) return
         main.replaceChildren(views.errorNode(e))
         await panel.draw({ route: 'error', view: null })
         finish('error')
       }
     }
     ```
  5. `updateTray()` gains, at its end: `panel?.draw()`; its text becomes `` `Picked (${tray.size})` ``.
  6. In `runWrite`, after `await explorer.refreshTail(listLog)` add `await nav.reloadSelections().catch(() => {})`.
  7. In `start()`: change the first line to `document.head.append(h('style', {}, APP_CSS), h('style', {}, PAIRS_CSS))`. After `updateStale()` (still inside the `try`), add:
     ```js
     panel = createPanel({ el: document.getElementById('panel-inner'), ex: explorer, ctx: { tray, write, trayChanged: updateTray } })
     nav = createNav(explorer, { el: document.getElementById('nav-body') })
     // Before the first render, so its snapshot reads fall in the open phase (plan P1).
     await nav.load()
     ```
     Before `window.addEventListener('hashchange', render)`, add the narrow-screen toggles (registered first, so the compose route can reopen the panel after they close it):
     ```js
     const toggle = (key) => { document.body.dataset[key] = document.body.dataset[key] === 'open' ? 'closed' : 'open' }
     document.getElementById('nav-toggle').addEventListener('click', () => toggle('nav'))
     document.getElementById('use-toggle').addEventListener('click', () => toggle('panel'))
     window.addEventListener('hashchange', () => { document.body.dataset.nav = 'closed'; document.body.dataset.panel = 'closed' })
     ```
     Delete the `style: 'margin-right: .75rem'` attribute in `renderMembers` (the stylesheet spaces `#members a`); add `#members a { margin-right: .75rem; }` to `APP_CSS`.
  8. Delete `compose` and `deletedNote` from `web/src/views/selection.js` and drop `compose` from `web/src/views/index.js`; remove the now-unused imports (`saveChoice`, `saveSequence`) from `views/selection.js` if nothing else there uses them (`retryRestore` stays, `failureBanner` uses it).

- [ ] **Step 5: Run the web tests and the build**

Run: `cd web && npm test && node build.mjs`
Expected: PASS; `dist/index.html` written.

- [ ] **Step 6: Run the Gate**

Run: `make gate`
Expected: lineage, tier A and tier B as before (`gate/README.md` gives the current counts). If tier A fails on request counts, a nav or panel read has landed in the query phase: find it with the step's `requests` in `$GATE_ROOT/browser/observed.json`.

- [ ] **Step 7: Commit**

```bash
git add web/src/styles.js web/src/index.html web/src/app.js web/src/views web/test/views.test.mjs
git commit -s -m "feat(web): three-region shell with the panel and left column

The router renders the middle region only; the panel is drawn before
body[data-render] advances. #/compose renders home with the panel open.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: The run overview

**Files:**
- Modify: `web/src/views/run.js` (rewrite `run`)
- Test: `web/test/views.test.mjs` (update three tests), new `web/test/run-view.test.mjs`

**Interfaces:**
- Consumes: `Explorer.run`, `Explorer.collection(cid, { limit })` (gives `total`, `items`, `index`), `Explorer.latestSuccessfulRun`, `Explorer.runIdentity`, `Previews`; `itemTable`, `fileChips` (Task 2); `anomalyLines`, `healthSummary`, `statusText`, `statusTone`, `whenText` (Task 1); `fold` (Task 5); `crumbs`, `retentionPanel`, `enc` (`views/common.js`).
- Produces: the run view. Keeps `[data-collection][data-output]` on each output row, `[data-no-outputs]`, `[data-retention]`. New: `[data-anomaly="<key>"]` per Lineage check line; `[data-fold="storage"|"details"|"lineage"]`.

- [ ] **Step 1: Write the failing test**

```js
// web/test/run-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { resetFolds } from '../src/folds.js'
import { run } from '../src/views/run.js'

function fakeEx() {
  const completion = { status: 'succeeded', possibly_incomplete: false, finished_at: '2026-09-30T21:10:38.345Z', error: null,
    anomalies: { unresolvable: 0, unaddressed: 0, declined: 439, never_published: 5, unjoined: 5 } }
  const items = { quant: ['q1', 'q2'], markdup: ['m1', 'm2', 'm3'] }
  return {
    run: async () => ({ row: { run_name: 'high_jang', pipeline: 'nf-core/rnaseq', nf_run_hash: 'abc', manifest_cid: 'bafyman' }, completion,
      collections: [{ output: 'quant', cid: 'cq' }, { output: 'markdup', cid: 'cm' }], state: claimState([]) }),
    collection: async (cid) => { const list = cid === 'cq' ? items.quant : [...items.markdup, 'm4', 'm5'].slice(0, 3); return { total: cid === 'cq' ? 2 : 5, items: list, index: null } },
    item: async (c, i) => ({ view: { id: i, single_end: i === 'm1' }, leaves: [{ name: `${i}.bam` }, { name: `${i}.bam.bai` }] }),
    latestSuccessfulRun: async () => 'bafyrun',
    runIdentity: async () => ({ revision: '3.14.0', lid: 'lid://abc', config: 'process {}' }),
    blocks: { urlFor: (a) => `blocks/${a}` },
  }
}
const ctx = () => ({ tray: new Tray(null), trayChanged: () => {}, rerender: () => {},
  write: { available: false, reason: 'read only', hrefFor: (h) => h } })

test('the run overview: status in words, outputs A–Z with counts, keys and file chips', async () => {
  installDom()
  resetFolds()
  const marks = []
  const c = { ...ctx(), mark: (m) => marks.push(m) }
  const page = await run(fakeEx(), 'bafyrun', c)
  await frame(); await frame()
  assert.deepEqual(marks, [{ pipeline: 'nf-core/rnaseq', run: 'bafyrun', expand: true }])
  assert.match(page.querySelector('h1').textContent, /high_jang/)
  assert.match(page.textContent, /Succeeded/)
  assert.match(page.textContent, /latest good run of nf-core\/rnaseq/)
  const rows = page.querySelectorAll('[data-collection]')
  assert.deepEqual(rows.map(r => r.dataset.output), ['markdup', 'quant'])
  assert.equal(rows[0].querySelector('a').getAttribute('href'), '#/items/bafyrun/markdup')
  assert.match(rows[0].textContent, /5/)
  assert.match(rows[0].textContent, /id/)
  assert.match(rows[0].textContent, /\.bam/)
  assert.ok(!page.querySelector('[data-snippet]'), 'the panel shows the call, not the run page')
})

test('the lineage check speaks plainly and flags only what is worth a look', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx())
  const lineage = page.querySelector('[data-fold="lineage"]')
  assert.match(lineage.querySelector('summary').textContent, /worth a look/)
  assert.equal(lineage.querySelector('[data-anomaly="unjoined"]').textContent, '5 published files that are in no output')
  assert.ok(lineage.querySelector('[data-anomaly="unjoined"]').classList.contains('warn'))
  assert.ok(!lineage.querySelector('[data-anomaly="declined"]').classList.contains('warn'))
  assert.ok(page.querySelector('[data-fold="storage"]').querySelector('[data-retention]'))
  assert.match(page.querySelector('[data-fold="details"]').textContent, /lid:\/\/abc/)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/run-view.test.mjs`
Expected: FAIL (outputs not sorted A–Z by the old view only by accident; `[data-fold]` missing).

- [ ] **Step 3: Rewrite `run` in `web/src/views/run.js`**

```js
// web/src/views/run.js
// #/run/<completion> (explorer layout B spec §5.3): what the run made, A–Z,
// and its details, storage and lineage check folded away beneath.
import { h, link, cid } from '../html.js'
import { Previews } from '../previews.js'
import { fold } from '../folds.js'
import { fileChips, itemTable } from '../table.js'
import { anomalyLines, healthSummary, statusText, statusTone, whenText } from '../words.js'
import { crumbs, enc, retentionPanel, table } from './common.js'

const SAMPLE = 3

export async function run(ex, completionCid, ctx) {
  const { row, completion, collections, state } = await ex.run(completionCid)
  ctx.mark?.({ pipeline: row.pipeline, run: completionCid, expand: true })
  const lid = row.nf_run_hash ? `lid://${row.nf_run_hash}` : null
  const latestNote = h('span', { class: 'muted' })
  ex.latestSuccessfulRun?.(row.pipeline).then((best) => { if (best === completionCid) latestNote.textContent = ` · latest good run of ${row.pipeline}` }, () => {})
  const previews = new Previews(ex)
  const outputs = [...collections].sort((a, b) => (a.output < b.output ? -1 : a.output > b.output ? 1 : 0))
  const rows = outputs.map(c => ({ c, count: h('td', { class: 'muted' }, '…'), keys: h('td', { class: 'muted' }), files: h('td', {}), items: [] }))
  const redraw = () => {
    for (const r of rows) {
      const loaded = r.items.map(i => previews.get(i)).filter(p => p?.pairs)
      if (!loaded.length) continue
      const { columns } = itemTable(loaded)
      r.keys.textContent = columns.slice(0, 2).join(', ')
      const chips = fileChips(loaded[0].files).chips
      r.files.replaceChildren(...chips.slice(0, 2).map(t => h('span', { class: 'ext' }, t)),
        chips.length > 2 ? h('span', { class: 'ext' }, `+${chips.length - 2}`) : null, r.indexLink ?? null)
    }
  }
  previews.onChange(redraw)
  for (const r of rows) {
    ex.collection(r.c.cid, { limit: SAMPLE }).then((coll) => {
      r.count.textContent = String(coll.total)
      r.count.classList.remove('muted')
      r.items = coll.items
      if (coll.index?.leaf?.address) {
        r.indexLink = h('a', { href: ex.blocks.urlFor(String(coll.index.leaf.address)), download: coll.index.leaf.name, 'data-output-index': String(coll.index.leaf.address) }, ' index file')
        r.files.append(r.indexLink)
      }
      for (const i of coll.items) previews.ask(r.c.cid, i)
    }, () => { r.count.textContent = '?' })
  }
  const details = h('div', {},
    lid ? h('p', {}, 'Run reference: ', h('code', {}, lid)) : null,
    h('p', {}, 'RunCompletion: ', cid(completionCid)),
    row.manifest_cid ? h('p', {}, 'RunManifest: ', cid(row.manifest_cid)) : null)
  ex.runIdentity?.(completionCid).then((id) => {
    if (id.revision) details.prepend(h('p', {}, 'Revision: ', h('code', {}, id.revision)))
    if (id.config) details.append(fold(`config:${completionCid}`, 'Configuration', h('pre', { class: 'scroll' }, id.config)))
  }, () => {})
  const lines = anomalyLines(completion.anomalies)
  return h('section', {},
    crumbs({ text: row.pipeline, href: `#/pipeline/${enc(row.pipeline)}` }, { text: row.run_name ?? 'run' }),
    h('h1', {}, row.run_name ?? 'run'),
    h('p', {}, h('span', { class: `dot-${statusTone({ status: completion.status, possibly_incomplete: completion.possibly_incomplete })}` }, '● '),
      statusText(completion), ' · finished ', whenText(completion.finished_at), latestNote),
    completion.error ? h('p', { class: 'warn' }, completion.error) : null,
    h('h2', {}, 'Outputs'),
    collections.length === 0 ? h('p', { class: 'muted', 'data-no-outputs': '' }, 'This run published no outputs.')
      : table(['output', 'items', 'keyed by', 'files'], rows.map(r => h('tr', { 'data-collection': r.c.cid, 'data-output': r.c.output },
        h('td', {}, link(`#/items/${completionCid}/${enc(r.c.output)}`, h('strong', {}, r.c.output))), r.count, r.keys, r.files))),
    fold(`details:${completionCid}`, 'Run details', details),
    fold(`storage:${completionCid}`, 'Storage', retentionPanel(completionCid, state, ctx, { isRun: true, kind: 'run', href: ctx.write.hrefFor(`#/run/${completionCid}`) })),
    fold(`lineage:${completionCid}`, `Lineage check: ${healthSummary(completion.anomalies)}`,
      lines.length === 0 ? h('p', { class: 'muted' }, 'Every file is accounted for.')
        : h('ul', {}, lines.map(l => h('li', { 'data-anomaly': l.key, class: l.concern ? 'warn' : 'muted' }, l.text)))))
}
```

Note: the `table` helper in `common.js` puts the header row and data rows in one `<table>`; the `data-collection` rows are the data rows, as before.

- [ ] **Step 4: Update the three existing run tests in `web/test/views.test.mjs`**

  1. `'the run page counts unjoined publishes'`: replace its assertion with `assert.match(page.textContent, /2 published files that are in no output/)`.
  2. `fixture()` (retention tests): add to its `ex` `latestSuccessfulRun: async () => null, runIdentity: async () => ({})`, and `blocks: { urlFor: (a) => a }`.
  3. Retention tests that read `#pin`, `#release`, `#restore` or badges keep working: the fold is in the tree, only closed. If a test locates the retention section with `page.children[...]`, change it to `page.querySelector('[data-retention]')`.

- [ ] **Step 5: Run the web tests**

Run: `cd web && npm test`
Expected: PASS.

- [ ] **Step 6: Open the Storage fold in tier B (P7).** `#pin-note` is now inside a closed `<details>`, which Playwright will not fill. In `gate/browser_b_assert.py` `prepare`, put `{"click": "[data-fold=storage] > summary"}` first in the `B.retain` step's `actions`. The fold stays open across each write's re-render (Task 5), so the later `#release` and `#restore` clicks find their buttons.

Run: `make gate`
Expected: tier B check 20 PASS, everything else as before.

- [ ] **Step 7: Commit**

```bash
git add web/src/views/run.js web/test/run-view.test.mjs web/test/views.test.mjs gate/browser_b_assert.py
git commit -s -m "feat(web): run overview with outputs A-Z and folded details

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: The items view

**Files:**
- Modify: `web/src/views/items.js` (rewrite `items`; keep `TYPES` and `whereForm`)
- Test: new `web/test/items-view.test.mjs`

**Interfaces:**
- Consumes: `Explorer.items` (unchanged), `Explorer.item` through `Previews`, `Explorer.runIdentity` through `runCrumbs`; `itemTable`, `cellOf`, `fileChips` (Task 2); `filterHref`, `valueText`, `pairsOf` (`pairs.js`); `pickButton`, `pickAll`, `runCrumbs`, `nextFrame`, `errorNode`, `enc` (`views/common.js`); `fold` (Task 5).
- Produces: the items view. Each row is `<tr data-item-result="<item>" data-preview-for="<item>">` with a checkbox, one cell per column, a files cell, an "Open" link and a "Pick" button carrying `[data-pick]` (P6). The table carries `data-columns` (JSON). "Pick all N" keeps `[data-pick-all][data-via][data-count]`. The view calls `ctx.use({ completion, output, where, count, checked, pickChecked })` and `ctx.mark({ run })`.

- [ ] **Step 1: Write the failing test**

```js
// web/test/items-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { items } from '../src/views/items.js'

const view = (id, singleEnd) => ({ id, single_end: singleEnd, strandedness: 'reverse' })
const VIEWS = { i1: view('WT_REP1', false), i2: view('WT_REP2', false), i3: view('RAP1_REP1', true) }

// Every snapshot-backed method throws: the items view and its panel input may
// use only Explorer.items, block reads and runIdentity (Review Focus 1, plan P1/P2).
function fakeEx() {
  const forbidden = (name) => () => { throw new Error(`${name} reads the snapshot during query 3`) }
  return {
    items: async (completion, output, where) => ({ items: where.length ? ['i1', 'i2'] : ['i1', 'i2', 'i3'], collection: 'coll' }),
    item: async (c, i) => ({ view: VIEWS[i], leaves: [{ name: `${VIEWS[i].id}.markdup.sorted.bam` }, { name: `${VIEWS[i].id}.markdup.sorted.bam.bai` }] }),
    runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'high_jang', lid: 'lid://x' }),
    runLabel: forbidden('runLabel'), runRow: forbidden('runRow'), runPage: forbidden('runPage'), pipelines: forbidden('pipelines'),
    collection: forbidden('collection'), latestSuccessfulRun: forbidden('latestSuccessfulRun'), allItems: forbidden('allItems'),
    producersOf: forbidden('producersOf'), selectionPage: forbidden('selectionPage'),
  }
}
function ctx() {
  const c = { tray: new Tray(null), used: [], marked: [], rerender: () => {}, write: { available: false, hrefFor: (h) => h } }
  c.trayChanged = () => {}
  c.use = (v) => c.used.push({ ...v })
  c.mark = (m) => c.marked.push(m)
  return c
}

test('rows, columns from the loaded previews, the constant line, file chips; no snapshot read but the query (Review Focus 1)', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await frame(); await frame()
  const rows = page.querySelectorAll('[data-item-result]')
  assert.deepEqual(rows.map(r => r.dataset.itemResult), ['i1', 'i2', 'i3'])
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.columns), ['id', 'single_end'])
  assert.match(page.querySelector('.constant').textContent, /Same for every item: strandedness reverse/)
  assert.match(rows[0].textContent, /\.bam/)
  assert.ok(!rows[0].textContent.includes('WT_REP1.markdup.sorted'))
  assert.equal(rows[0].querySelector('[data-pick]').dataset.pick, 'i1')
  assert.equal(rows[0].querySelector('[data-pick]').dataset.via, 'coll')
  assert.equal(c.used.at(-1).count, 3)
  assert.deepEqual(c.marked, [{ run: 'bafyrun' }])
})

test('a value is a link that adds its filter; the chips bar removes one', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify([['single_end', 'bool', 'false']]), ctx())
  await frame(); await frame()
  const chip = page.querySelector('[data-chip="single_end"]')
  assert.match(chip.textContent, /single_end = false/)
  assert.equal(chip.querySelector('a').getAttribute('href'), `#/items/bafyrun/markdup?where=${encodeURIComponent('[]')}`)
  const value = page.querySelectorAll('[data-item-result]')[0].querySelectorAll('a').find(a => a.textContent === 'WT_REP1')
  assert.match(decodeURIComponent(value.getAttribute('href')), /\["id","string","WT_REP1"\]/)
})

test('checkboxes report to the panel, and "Pick these" puts the checked rows in the tray with the collection', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  const box = page.querySelectorAll('[data-item-result]')[1].querySelector('input')
  box.checked = false
  await box.fire('change')
  assert.equal(c.used.at(-1).checked, 2)
  c.used.at(-1).pickChecked()
  assert.deepEqual(c.tray.entries().map(e => [e.address, e.via]), [['i1', ['coll']], ['i3', ['coll']]])
})

test('no items says so, and still reports the empty output to the panel', async () => {
  installDom()
  const ex = fakeEx()
  ex.items = async () => ({ items: [], collection: null })
  const c = ctx()
  const page = await items(ex, 'bafyrun', 'markdup', JSON.stringify([['id', 'string', 'nope']]), c)
  assert.match(page.textContent, /No items match/)
  assert.equal(c.used.at(-1).count, 0)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/items-view.test.mjs`
Expected: FAIL (no `table`, no `ctx.use`).

- [ ] **Step 3: Rewrite `items` in `web/src/views/items.js`**

```js
// web/src/views/items.js
// #/items/<completion>/<output>?where=... (explorer layout B spec §5.4): query 3
// as a table whose columns are the Meta Map keys that tell items apart.
// Nothing here reads the snapshot but Explorer.items (Gate assertion 2).
import { h, link } from '../html.js'
import { Previews } from '../previews.js'
import { fold } from '../folds.js'
import { cellOf, fileChips, itemTable } from '../table.js'
import { filterHref, valueText } from '../pairs.js'
import { enc, nextFrame, pickAll, pickButton, runCrumbs } from './common.js'

export const TYPES = ['string', 'int', 'float', 'bool', 'null']

function parseWhere(whereText) {
  try {
    const where = JSON.parse(whereText)
    if (!Array.isArray(where) || !where.every(w => Array.isArray(w) && w.length === 3)) throw new Error('not a list of [path, type, value]')
    return where
  } catch (e) {
    throw Object.assign(new Error(`the where filter is not JSON [[path, type, value], ...]: ${e.message}`), { code: 'bad_route' })
  }
}

const routeOf = (target, where) => `#/items/${target.completion}/${enc(target.output)}?where=${enc(JSON.stringify(where))}`

function chipsBar(target) {
  if (!target.where.length) return h('p', { class: 'muted' }, 'Click any value to filter by it.')
  return h('p', { class: 'chips-bar' }, target.where.map(([p, t, v], i) => h('span', { class: 'chip', 'data-chip': p },
    `${p} = ${valueText(t, v)} `, h('a', { href: routeOf(target, target.where.filter((_, j) => j !== i)), 'aria-label': `remove ${p}` }, '✕'))))
}

function valueNode(entry, target) {
  if (!entry) return h('span', { class: 'muted' }, '—')
  if (entry.values.length !== 1) return h('span', {}, entry.values.map(v => v ?? 'null').join(', '))
  const type = entry.types[0]
  const value = entry.values[0]
  const href = filterHref(target, entry.path, type, value)
  const text = valueText(type, value)
  return href ? h('a', { href, title: `only items where ${entry.path} is ${text}` }, text) : h('span', {}, text)
}

export async function items(ex, completionCid, output, whereText, ctx) {
  const where = parseWhere(whereText)
  let found
  try {
    found = await ex.items(completionCid, output, where, ctx.progress)
  } catch (e) {
    if (e.constructor?.name === 'PredicateError') throw Object.assign(e, { code: 'bad_predicate' })
    throw e
  }
  const { items: results, collection: collectionCid } = found
  const via = collectionCid ? [collectionCid] : []
  const target = { completion: completionCid, output, where }
  const checked = new Set(results)
  const view = { completion: completionCid, output, where, count: results.length, checked: results.length,
    pickChecked: () => {
      ctx.tray.addMany([...checked].map(address => ({ address, via, kind: 'item' })))
      ctx.trayChanged()
    } }
  ctx.use?.(view)
  ctx.mark?.({ run: completionCid })
  const report = () => { view.checked = checked.size; ctx.use?.(view) }
  const status = h('span', { class: 'muted' })
  const pickAllButton = h('button', { type: 'button', 'data-pick-all': '', 'data-via': via.join(' '), 'data-count': results.length, onclick: async (event) => {
    const button = event.currentTarget
    button.disabled = true
    try {
      const n = await pickAll(ex, ctx.tray, { items: results, collectionCid })
      ctx.trayChanged()
      status.textContent = `Picked ${n}.`
    } finally {
      button.disabled = false
    }
  } }, `Pick all ${results.length}`)
  return h('section', {},
    runCrumbs(ex, completionCid, [{ text: output }]),
    h('h1', {}, output, h('span', { class: 'muted' }, ` · ${results.length} item${results.length === 1 ? '' : 's'}`)),
    chipsBar(target),
    fold(`filter:${completionCid}/${output}`, '+ filter', whereForm(completionCid, output, where)),
    results.length === 0 ? h('p', { class: 'muted' }, 'No items match.')
      : [h('p', { class: 'row-actions' }, pickAllButton, ' ', status),
        itemsTable(ex, ctx, { results, via, target, checked, onCheck: report })])
}

function itemsTable(ex, ctx, { results, via, target, checked, onCheck }) {
  const previews = new Previews(ex)
  const head = h('tr', {})
  const body = h('tbody', {})
  const constantLine = h('p', { class: 'constant muted' })
  const table = h('table', { class: 'items-table', 'data-columns': '[]' }, h('thead', {}, head), body)
  const all = h('input', { type: 'checkbox', checked: true, 'aria-label': 'check every item', onchange: (event) => {
    const on = event.currentTarget.checked
    for (const r of rows) { r.box.checked = on; if (on) checked.add(r.address); else checked.delete(r.address) }
    onCheck()
  } })
  const rows = results.map((address, index) => {
    const box = h('input', { type: 'checkbox', checked: true, 'aria-label': 'check this item', onchange: (event) => {
      if (event.currentTarget.checked) checked.add(address)
      else checked.delete(address)
      onCheck()
    } })
    return { address, index, box, tr: h('tr', { 'data-item-result': address, 'data-preview-for': address }), drawn: undefined }
  })
  body.append(...rows.map(r => r.tr))
  let shape = null
  let shapeKey = null
  const drawRow = (r) => {
    const p = previews.get(r.address)
    r.drawn = p
    const open = link(`#/item/${via[0] ?? '-'}/${r.address}`, 'Open')
    const pick = pickButton(ctx, { address: r.address, via })
    if (p === undefined) {
      const wait = r.index < previews.cap ? h('span', { class: 'muted' }, 'loading...')
        : h('button', { type: 'button', onclick: () => previews.ask(via[0] ?? null, r.address) }, 'show details')
      r.tr.replaceChildren(h('td', {}, r.box), h('td', { colspan: String(Math.max(1, shape.columns.length + 1)) }, wait), h('td', {}, open, ' ', pick))
      return
    }
    if (p.error) {
      r.tr.replaceChildren(h('td', {}, r.box), h('td', { class: 'muted', colspan: String(Math.max(1, shape.columns.length + 1)) },
        p.error === 'block_missing' ? 'not held in this member' : `no preview (${p.error})`), h('td', {}, open, ' ', pick))
      return
    }
    const { chips } = fileChips(p.files)
    r.tr.replaceChildren(h('td', {}, r.box),
      ...shape.columns.map(path => h('td', {}, valueNode(cellOf(p, path), target))),
      h('td', {}, chips.slice(0, 3).map(t => h('span', { class: 'ext' }, t)), chips.length > 3 ? h('span', { class: 'ext' }, `+${chips.length - 3}`) : null),
      h('td', {}, open, ' ', pick))
  }
  const redraw = nextFrame(() => {
    const next = itemTable(rows.map(r => previews.get(r.address)))
    const key = JSON.stringify([next.columns, next.constant.map(e => [e.path, e.values])])
    const reshaped = key !== shapeKey
    shape = next
    shapeKey = key
    if (reshaped) {
      table.dataset.columns = JSON.stringify(shape.columns)
      head.replaceChildren(h('th', {}, all), ...shape.columns.map(p => h('th', {}, p)), h('th', {}, 'files'), h('th', {}))
      constantLine.replaceChildren(...(shape.constant.length ? ['Same for every item: ',
        shape.constant.map((e, i) => [i ? ' · ' : null, `${e.path} `, h('strong', {}, valueNode(e, target))])] : []))
    }
    for (const r of rows) if (reshaped || previews.get(r.address) !== r.drawn) drawRow(r)
  })
  shape = itemTable([])
  head.replaceChildren(h('th', {}, all), h('th', {}, 'files'), h('th', {}))
  for (const r of rows) drawRow(r)
  previews.onChange(redraw)
  previews.askFirst(rows.map(r => ({ collection: via[0] ?? null, item: r.address })))
  return h('div', {}, constantLine, h('div', { class: 'scroll' }, table))
}
```

Keep `whereForm` as it is (it is already in this file from Task 6). Unlike `watchRows`, this table does not cancel its preview queue when the route changes; `Previews.cancel` is cheap to add later if long queues show up in use.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd web && node --test test/items-view.test.mjs && npm test`
Expected: PASS. In the fake DOM, `checked` passed to `h()` becomes an attribute, not the `checked` property; the test sets `box.checked` itself before firing `change`, so it holds.

- [ ] **Step 5: Gate tier A**

Run: `make gate`
Expected: tier A 5/5 (A1 items and A2 items read the same rows and stay within `QUERY3_LIMIT`). Tier B 17 (`B.picks`) passes: it clicks `[data-pick="<a>"]`, now a row's Pick button.

- [ ] **Step 6: Commit**

```bash
git add web/src/views/items.js web/test/items-view.test.mjs
git commit -s -m "feat(web): items as a table of distinguishing keys, filter by clicking

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Item, Selection, Collection, Pipeline, Home and the rest

**Files:**
- Modify: `web/src/views/item.js`, `views/selection.js`, `views/collection.js`, `views/pipeline.js`, `views/home.js`, `views/common.js` (`pickButton` and `itemRows` wording; label de-duplication in `watchRows` and `fillRow`)
- Test: new `web/test/pages.test.mjs`; update wording assertions in `web/test/views.test.mjs`

**Interfaces:**
- Consumes: Tasks 1, 2, 5, 6; `Explorer.latestSuccessfulRun`, `Explorer.runIdentity`.
- Produces: views in the middle column with breadcrumbs. The Selection view calls `ctx.use({ selection, members, names })` and renders no `[data-snippet]` or `[data-samplesheet]` of its own (the panel does, Review Focus 5). Collection calls `ctx.use({ completion, output, where: [], count: total })`; Item calls `ctx.use({ completion, output, where: [], count: null })` when its run is known. Pipeline calls `ctx.mark({ pipeline, expand: true })`. Wording: "Add to picked" / "Picked" (`pickButton`), "Pick all N" (`[data-pick-all]`), "Pick checked (k)", "Picked N." (status).

- [ ] **Step 1: Write the failing tests**

```js
// web/test/pages.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { item } from '../src/views/item.js'
import { selection } from '../src/views/selection.js'
import { pipeline } from '../src/views/pipeline.js'
import { home } from '../src/views/home.js'

const ctx = () => {
  const c = { tray: new Tray(null), used: [], marked: [], rerender: () => {}, trayChanged: () => {},
    write: { available: false, reason: 'read only', served: true, hrefFor: (h) => h, here: false } }
  c.use = (v) => c.used.push(v)
  c.mark = (m) => c.marked.push(m)
  return c
}

test('the item page: a de-duplicated title, sizes, downloads, produced-by, and the panel told its output', async () => {
  installDom()
  const ex = {
    item: async () => ({ view: { id: 'WT_REP1', meta: { id: 'WT_REP1' }, single_end: false }, state: claimState([]),
      leaves: [{ name: 'WT_REP1.bam', size: 412_000_000, address: 'bafybam' }, { name: 'in.fq', size: null, address: null, reason: 'never_published' }] }),
    producersOf: async () => [{ collection_cid: 'coll', completion_cid: 'bafyrun' }, { collection_cid: 'coll2', completion_cid: 'bafyold' }],
    selectionsHolding: async () => [],
    runLabel: async (c) => ({ run_name: c === 'coll' ? 'high_jang' : 'happy_rubens', output: 'markdup', completion: c === 'coll' ? 'bafyrun' : 'bafyold' }),
    runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'high_jang' }),
    blocks: { urlFor: (a) => `blocks/${a}` },
  }
  const c = ctx()
  const page = await item(ex, 'coll', 'bafyitem', c)
  await frame()
  assert.equal(page.querySelector('h1').textContent, 'WT_REP1')
  assert.match(page.textContent, /412 MB/)
  assert.equal(page.querySelector('a[download]').getAttribute('href'), 'blocks/bafybam')
  assert.match(page.textContent, /not stored here \(such as an input file\)/)
  assert.equal(page.querySelector('[data-pick]').textContent, 'Add to picked')
  assert.deepEqual(c.used.at(-1), { completion: 'bafyrun', output: 'markdup', where: [], count: null })
  assert.ok(page.querySelector('[data-fold="storage"]'))
})

test('the Selection view leaves the call and samplesheets to the panel (Review Focus 5)', async () => {
  installDom()
  const st = { names: ['treated BAMs'], nameClaims: [], current: [], nameConflicted: false, deletion: 'none', deletionClaims: [] }
  const ex = { selection: async () => ({ state: st, members: [{ kind: 'item', address: 'i1', via: ['coll'] }], firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async () => 'here', item: async () => ({ view: { id: 'A' }, leaves: [] }), runLabel: async () => null }
  const c = ctx()
  const page = await selection(ex, 'bafysel', c)
  assert.equal(page.querySelectorAll('[data-snippet]').length, 0)
  assert.equal(page.querySelectorAll('[data-samplesheet]').length, 0)
  assert.deepEqual(c.used.at(-1), { selection: 'bafysel', members: 1, names: ['treated BAMs'] })
  assert.ok(page.querySelector('[data-selection-view="bafysel"]'))
})

test('the pipeline page: health in words, rows keep [data-run], the left column expands it', async () => {
  installDom()
  const ex = {
    runPage: async () => ({ rows: [{ completion_cid: 'bafyrun', pipeline: 'p', run_name: 'high_jang', status: 'succeeded', possibly_incomplete: 0, finished_at: '2026-09-30T21:10:38.345Z', source: 'snapshot' }],
      hidden: 0, first: 1, last: 1, total: 1, prev: null, next: null }),
    completionOf: async () => ({ anomalies: { declined: 3, unjoined: 1 } }),
    stale: [],
  }
  const c = ctx()
  const page = await pipeline(ex, 'p', 0, c)
  await frame()
  assert.equal(page.querySelector('[data-run]').dataset.status, 'succeeded')
  assert.equal(page.querySelector('[data-anomalies-for="bafyrun"]').textContent, '1 published file that is in no output')
  assert.deepEqual(c.marked, [{ pipeline: 'p', expand: true }])
})

test('home links each pipeline to its latest good run', async () => {
  installDom()
  const ex = { pipelines: async () => [{ pipeline: 'p', runs: 2, latest: '2026-09-30T21:10:38.345Z' }], stale: [],
    latestSuccessfulRun: async () => 'bafyrun', runIdentity: async () => ({ run_name: 'high_jang' }) }
  const page = await home(ex, ctx())
  await frame()
  assert.ok(page.querySelectorAll('a').some(a => a.getAttribute('href') === '#/run/bafyrun' && a.textContent === 'high_jang'))
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd web && node --test test/pages.test.mjs`
Expected: FAIL (old item title "Item", Selection view has a snippet, `pipeline` takes no `ctx`).

- [ ] **Step 3: Wording and labels in `views/common.js`.**

  1. `pickButton`: label `inTray ? 'Picked' : kind === 'selection' ? 'Add this Selection to picked' : 'Add to picked'`; after a click, `textContent = 'Picked'`. `markInTray`: `'Picked'`.
  2. `added`: `` [`Picked ${n}.`, trayNote(tray)].filter(Boolean).join(' ') ``.
  3. `itemRows`: `'Add checked (0)'` and its update become `` `Pick checked (${checked.size})` ``; the "Add all" button text becomes `` `Pick all ${total}` ``.
  4. In `watchRows`, compute the label paths from de-duplicated pairs (fixes "multiqc_report · multiqc_report"): add `import { itemTable, withoutDuplicates } from '../table.js'` and replace the `paths` line with
     ```js
     const loaded = rows.map(r => previews.get(r.address)).filter(p => p?.pairs)
     const { duplicates } = itemTable(loaded)
     const paths = labelPaths(loaded.map(p => ({ ...p, pairs: withoutDuplicates(p.pairs, duplicates) })))
     ```
     and pass `duplicates` to `fillRow(r, p, paths, duplicates)`, which draws `pairsNode(withoutDuplicates(preview.pairs, duplicates), row.target, { size: 'row' })`.

  Update `web/test/views.test.mjs` to the new words: `'Add all 2 to the tray'` → `'Pick all 2'`; `'In the tray'` → `'Picked'`; `` `Added 3 to the tray. ${UNSAVED_NOTE}` `` → `` `Picked 3. ${UNSAVED_NOTE}` ``; `'Add checked (2)'` → `'Pick checked (2)'`.

- [ ] **Step 4: Rewrite `item` in `web/src/views/item.js`**

```js
// web/src/views/item.js
// #/item/<collection>/<item> (explorer layout B spec §5.5).
import { h, link, cid } from '../html.js'
import { fold } from '../folds.js'
import { labelPaths, labelText, pairsNode, pairsOf } from '../pairs.js'
import { itemTable, withoutDuplicates } from '../table.js'
import { humanBytes, leafReasonText } from '../words.js'
import { crumbs, pickButton, retentionPanel, runCrumbs, runLabelNode, table } from './common.js'

const SHOWN_PAIRS = 8

export async function item(ex, collectionCid, itemCid, ctx) {
  const it = await ex.item(collectionCid, itemCid)
  const producers = await Promise.all(it.leaves.filter(l => l.address).map(async l => [l, await ex.producersOf(l.address.toString(), ctx.progress)]))
  const holding = await ex.selectionsHolding(itemCid)
  const from = collectionCid === '-' ? null : await ex.runLabel(collectionCid).catch(() => null)
  if (from?.completion) {
    ctx.use?.({ completion: from.completion, output: from.output, where: [], count: null })
    ctx.mark?.({ run: from.completion })
  }
  const files = it.leaves.map(l => l.name).filter(Boolean)
  const all = pairsOf(it.view)
  const { duplicates } = itemTable([{ pairs: all, files }])
  const pairs = withoutDuplicates(all, duplicates)
  const title = labelText({ pairs, files }, labelPaths([{ pairs }], 1)) || 'Item'
  const target = from?.completion ? { completion: from.completion, output: from.output, where: [] } : null
  const runs = new Map()
  for (const [, rows] of producers) for (const p of rows) runs.set(p.completion_cid, p.collection_cid)
  const elsewhere = [...runs].filter(([completion]) => completion !== from?.completion)
  return h('section', {},
    from?.completion ? runCrumbs(ex, from.completion, [{ text: from.output, href: `#/items/${from.completion}/${encodeURIComponent(from.output)}?where=%5B%5D` }, { text: title }])
      : crumbs({ text: title }),
    h('h1', {}, title),
    pairs.length ? h('div', {}, pairsNode(pairs.slice(0, SHOWN_PAIRS), target, { size: 'page' }),
      pairs.length > SHOWN_PAIRS ? fold(`meta:${itemCid}`, `+${pairs.length - SHOWN_PAIRS} more`, pairsNode(pairs.slice(SHOWN_PAIRS), target, { size: 'page' })) : null)
      : h('p', { class: 'muted' }, 'No Meta Map.'),
    h('h2', {}, 'Files'),
    table(['file', 'size', ''], it.leaves.map(l => h('tr', {},
      h('td', {}, h('code', {}, l.name ?? '')),
      h('td', {}, humanBytes(l.size)),
      h('td', {}, l.address
        ? [h('a', { href: ex.blocks.urlFor(String(l.address)), download: l.name ?? '' }, 'Download'), ' ', link(`#/content/${l.address}`, 'where else')]
        : h('span', { class: 'muted' }, leafReasonText(l.reason)))))),
    h('p', {}, pickButton(ctx, { address: itemCid, via: collectionCid === '-' ? [] : [collectionCid] })),
    h('h2', {}, 'Produced by'),
    from ? h('p', {}, from.completion ? link(`#/run/${from.completion}`, `${from.run_name ?? 'this run'}`) : from.run_name ?? 'this run',
      elsewhere.length ? [h('span', { class: 'muted' }, ' · same files also in: '),
        elsewhere.map(([completion, coll], i) => [i ? ', ' : null, runLabelNode(ex, coll, completion)])] : null)
      : h('p', { class: 'muted' }, 'Picked by a query across runs.'),
    holding.length ? [h('h2', {}, 'In Selections'), h('ul', {}, holding.map(s => h('li', {}, link(`#/selection/${s}`, cid(s)))))] : null,
    fold(`storage:${itemCid}`, 'Storage', retentionPanel(itemCid, it.state, ctx, { kind: 'item', href: ctx.write.hrefFor(`#/item/${collectionCid}/${itemCid}`) })),
    fold(`details:${itemCid}`, 'Details',
      h('p', {}, 'Address: ', collectionCid !== '-' ? cid(`cas://${collectionCid}/${itemCid}`) : cid(itemCid)),
      h('ul', {}, it.leaves.filter(l => l.address).map(l => h('li', {}, l.name ?? '', ': ', cid(String(l.address))))),
      pairsNode(all, target, { size: 'row' })))
}
```

- [ ] **Step 5: Selection, Collection, Pipeline, Home.**

  1. `views/selection.js` `selection()`: delete the last three children (`h('h2', {}, 'Use it in a workflow')`, its `p`, `snippetBlock(...)`) and the samplesheet `p`; drop `snippetBlock, snippetToggle` from its imports. Before `return`, add `ctx.use?.({ selection: selectionCid, members: s.members.length, names: st.names })`. Make the first child `crumbs({ text: 'Selections', href: '#/selections' }, { text: st.names.length ? st.names.join(' / ') : 'Unnamed Selection' })` and import `crumbs` from `./common.js`. Move the `cid(selectionCid)` line and the "first seen / assembled by" `dl` into `fold(`details:${selectionCid}`, 'Details', ...)` (import `fold` from `'../folds.js'`).
  2. `views/selection.js` `selections()`: first child `crumbs({ text: 'Selections' })`.
  3. `views/collection.js` `collection()`: after loading, `if (c.completion) ctx.use?.({ completion: c.completion, output: c.output, where: [], count: c.total })`; first child `c.completion ? runCrumbs(ex, c.completion, [{ text: c.output }]) : crumbs({ text: c.output })`; wrap the `retentionPanel(...)` in `fold(`storage:${collectionCid}`, 'Storage', ...)` and move `cid(collectionCid)` into `fold(`details:${collectionCid}`, 'Details', ...)`. `content()`: wrap its `retentionPanel` in `fold(`storage:${contentCid}`, 'Storage', ...)`, first child `crumbs({ text: 'File' })`, title `'Every run that produced this file'`.
  4. `views/pipeline.js` `pipeline(ex, name, offset = 0, ctx)`: add `ctx?.mark?.({ pipeline: name, expand: true })`; first child `crumbs({ text: name })`; columns `['run', 'status', 'finished', 'health', 'from']`; status cell `statusText(r)`; finished cell `whenText(r.finished_at)`; the anomalies callback sets `anomalies.textContent = healthText(c.anomalies)` (import `healthText, statusText, whenText` from `'../words.js'`). Keep every `data-*` attribute. In `web/src/app.js`, pass `ctx`: `['pipeline', ..., (ex, m, ctx) => views.pipeline(ex, decodeURIComponent(m[1]), Number(m[2] ?? 0), ctx)]`. `latest()`: first child `crumbs({ text: pipelineName, href: `#/pipeline/${enc(pipelineName)}` }, { text: 'latest good run' })`, keep `[data-latest]`.
  5. `views/home.js` `home(ex, ctx)`: columns `['pipeline', 'runs', 'latest run', 'latest good run']`; latest-run cell `whenText(p.latest)`; the last cell starts as `'…'` and is filled with `link(`#/run/${best}`, name)` once `ex.latestSuccessfulRun(p.pipeline)` and `ex.runIdentity(best)` answer, or `'none'` when `best` is null; errors leave `'?'`. Keep `unreadableRuns(ex)` (Gate A4 reads its `[data-error]`).

- [ ] **Step 6: Run the web tests**

Run: `cd web && npm test`
Expected: PASS. The old `item` test in `views.test.mjs` (`'a query result row: ...'` uses `itemRows`, not `item`) is unaffected; retention tests that render `item(...)` find the controls inside the Storage fold.

- [ ] **Step 7: Commit**

```bash
git add web/src/views web/src/app.js web/test/pages.test.mjs web/test/views.test.mjs
git commit -s -m "feat(web): item, Selection, collection, pipeline and home in layout B

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: The Gate and DESIGN.md

**Files:**
- Modify: `gate/browser_b_assert.py` (`prepare` steps, `consumer`, `check` with new `b21`), `gate/browser/drive.mjs` (`EXTRACT_B`), `gate/browser/tier_b.sh` (run the `B.where` consumer), `gate/test_browser_b_assert.py` (world gains `B.where` and `selection-where`; two new tests), `gate/README.md` (tier B count), `DESIGN.md` §15–16, `docs/superpowers/specs/2026-09-30-explorer-layout-b-design.md` §10 (P12 erratum)
- Test: `python3 -m unittest discover -s gate`, `make gate`

**Interfaces:**
- Consumes: the page's `[data-panel-state]`, `[data-snippet][data-where]`, `[data-item-result]`, `[data-fold=storage] > summary`.
- Produces: tier B check 21, "the items view's filtered call, run verbatim, stages exactly the items the page listed".

- [ ] **Step 1: Extend `EXTRACT_B` in `gate/browser/drive.mjs`** (after `snippets:`):

```js
  // Layout B (DESIGN.md §16): the panel's state, the chips its call carries, and the rows the items view listed.
  panel: document.querySelector('[data-panel-state]')?.dataset.panelState ?? null,
  where: document.querySelector('[data-snippet][data-where]')?.dataset.where ?? null,
  results: [...document.querySelectorAll('[data-item-result]')].map((e) => e.dataset.itemResult),
```

- [ ] **Step 2: Scenario changes in `gate/browser_b_assert.py` `prepare`.** (`B.retain` already opens the Storage fold, Task 10.) After the `B.snippets` step add:

```python
        # B21 (layout B): the items view's filtered call, untyped, as the panel shows it.
        {"id": "B.where", "server": "explore", "path": "", "query": q,
         "hash": "#/items/%s/aligned%s" % (cold.completion_cid, _where("sample", "A")),
         "actions": [{"snippetMode": "untyped"}, {"extract": "untyped"}]},
```

Give `consumer` a step argument: `def consumer(root, mode, src, dest, step="B.snippets"):` and read `_observed(out).get(step)`. In the command-line dispatch at the bottom of the file, pass `sys.argv[6]` as `step` when present (read the existing `consumer` branch of the dispatch and add the optional argument there).

- [ ] **Step 3: `gate/browser/tier_b.sh`.** After the two `run_consumer` lines add:

```bash
# B21 (layout B): the items view's filtered call (step B.where), run verbatim in the same consumer.
where_launch="$GATE_ROOT/selection-where"
rm -rf "${where_launch:?}" && mkdir -p "$where_launch" "$B/store-where" "$B/cache-where"
where_status=0
echo "--- browser tier B: selection-where (the items view's filtered untyped call)"
if python3 "$REPO/gate/browser_b_assert.py" consumer "$GATE_ROOT" untyped "$REPO/gate/selection" "$where_launch" B.where; then
    ( cd "$where_launch" && GATE_B_STORE="$B/store" GATE_B_OUT="$B/store-where" XDG_CACHE_HOME="$B/cache-where" \
      "$NEXTFLOW" run . -name selection-where --selection unused --samplesheet "$B/samplesheet.csv" ) > "$B/selection-where.log" 2>&1 || where_status=$?
else
    where_status=no-snippet
fi
echo "$where_status" > "$B/selection-where.exit"
```

`--selection unused` satisfies the template's parameter check; its call line no longer reads `params.selection`. The samplesheet half of the consumer still runs and is ignored by B21.

- [ ] **Step 4: Write the failing unit tests** in `gate/test_browser_b_assert.py`. First extend the test world's `write()` (beside where it writes `B.snippets` and the `selection` launch): add a `B.where` step whose extract is `self.extract(snippets={"untyped": self.where_call, "typed": None}, panel="filtered", where='[["sample","string","A"]]', results=[self.items["A"]])` (use the names the world already has for item A's CID and for `extract`), write `selection-where.exit` with `self.where_exit`, hash `self.store_where` with `{"fromstore": {<A's file>.sha256: <A's sha256>}}` the way it hashes `store_out`, and write `selection-where/main.nf` from `UNTYPED_TEMPLATE` with `self.where_call`. Initialise `self.where_call = "channel.fromStore(run: 'lid://%s', output: 'aligned', where: [sample: 'A'])" % <cold's nf_run_hash in the world>`, `self.where_exit = "0"`, `self.store_where = os.path.join(self.out, "store-where")`. Then add:

```python
    def test_b21_the_whole_world_passes(self):
        self.assertPass(21)

    def test_b21_a_call_without_the_filter_fails(self):
        self.w.where_call = self.w.where_call.replace(", where: [sample: 'A']", "")
        self.assertFail(21, "where")

    def test_b21_a_consumer_staging_more_than_the_page_listed_fails(self):
        self.w.where_hashes_extra = True   # the world adds B's file to store-where's fromstore hashes
        self.assertFail(21, "staged")

    def test_b21_a_failed_pipeline_fails(self):
        self.w.where_exit = "1"
        self.assertFail(21, "exit status")
```

Use the helpers the file already has for pass and fail assertions (read `assertFail` and the test `test_the_whole_world_passes` near line 416 for their exact names and signatures, and add `assertPass` only if it does not exist, as `self.assertEqual(self.run_check()[21][0], "PASS")` in the file's own idiom).

- [ ] **Step 5: Run them to verify they fail**

Run: `python3 -m unittest gate.test_browser_b_assert -k b21` (from the repo root; if `-k` is not accepted by the Python on the machine, run `python3 -m unittest discover -s gate -p test_browser_b_assert.py`)
Expected: FAIL, check 21 does not exist.

- [ ] **Step 6: Implement `b21`** in `check`, beside `b20`, and register it after `run(20, ...)`:

```python
    def b21():
        problems = []
        got = extract("B.where", "untyped")
        shown = ((got.get("snippets") or {}).get("untyped") or "").strip()
        if got.get("panel") != "filtered":
            problems.append("the items view's panel is %r, expected filtered" % got.get("panel"))
        if got.get("results") != [expected["items"]["A"]]:
            problems.append("the items view listed %s, expected only A %s" % (got.get("results"), expected["items"]["A"]))
        if "where: [sample: 'A']" not in shown:
            problems.append("the panel's call %r does not carry where: [sample: 'A']" % shown)
        ran = snippet_call(os.path.join(root, "selection-where", "main.nf"))
        if ran != shown:
            problems.append("selection-where/main.nf ran %r, not the panel's call %r" % (ran, shown))
        staged = pipeline_hashes("selection-where", "store-where").get("fromstore") or {}
        want = {"%s.sha256" % files["A"]["name"]: files["A"]["sha256"]}
        if staged != want:
            problems.append("the filtered call staged %s, expected exactly %s" % (json.dumps(staged, sort_keys=True), json.dumps(want, sort_keys=True)))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, "the items view's filtered call (%s), run verbatim, staged exactly A, the one item the page listed" % shown

    run(21, "the items view's filtered call, run verbatim, stages exactly the items the page listed", b21)
```

`files` is the name the check already uses for `expected["files"]` (line ~499, `want_hashes`); if it is named differently there, use that name.

- [ ] **Step 7: Run the unit tests**

Run: `python3 -m unittest discover -s gate`
Expected: PASS, including the four new tests.

- [ ] **Step 8: DESIGN.md and the spec erratum.**

  1. DESIGN.md §15 page-contract table: add rows for `[data-panel-state]` (`none|whole|filtered|picked|saved`, the panel's state, layout B spec §6.1), `[data-where]` (on the panel's call `<code>`, the chips as JSON `[[path, type, value], ...]`), `[data-run-mode]` (`this|latest`, the panel's run switch), `[data-columns]` (on the items table, its column paths as JSON), `[data-fold]` (`storage|details|lineage|filter|meta|config`, a folded section; its open state survives a re-render), `[data-anomaly]` (one Lineage check line, the anomaly's key). Amend `[data-snippet]` ("on the panel; at most one per mode on a page"), `[data-tray-entry]` ("an entry of the panel's picked list"), `[data-preview-for]` ("the items table row, or an item row's pills container"), `[data-pick-all]` ("Pick all N"), and `#tray` ("Picked (N)").
  2. DESIGN.md §16: add a "Layout B (2026-09-30)" subsection listing plan decisions P1–P16 with one line each, and the routes' new homes (the panel replaces `#/compose`'s page; `#/compose` renders home with the panel open).
  3. Spec §10: replace "`[data-member]` move with the store status into the left column." with "`#members` (the store member links) moves with the store status into the left column; `[data-member]`, a Selection's member row, stays on the Selection view." and add a line under the date: "Amended 2026-09-30 by the implementation plan (P12)."
  4. `gate/README.md`: tier B count goes up by one (B21), with one line saying what it checks.

- [ ] **Step 9: Run the Gate**

Run: `make gate`
Expected: lineage unchanged, tier A 5/5, tier B all PASS including 21.

- [ ] **Step 10: Commit**

```bash
git add gate DESIGN.md docs/superpowers/specs/2026-09-30-explorer-layout-b-design.md
git commit -s -m "gate: tier B check 21 runs the items view's filtered call; layout B contract

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: Verification and the manual pass

**Files:**
- Create: `docs/plans/2026-09-30-explorer-layout-b-manual-test.md`
- Test: everything

**Interfaces:**
- Consumes: the whole branch.
- Produces: a green branch and written manual steps for Rob.

- [ ] **Step 1: Run everything from a clean tree**

Run: `cd web && npm ci && npm test && node build.mjs && cd .. && make check && make gate`
Expected: web tests PASS; Gradle check green (unchanged, no Groovy touched); Gate lineage unchanged, tier A 5/5, tier B with B21 all PASS.

- [ ] **Step 2: Write the manual test steps.** Create `docs/plans/2026-09-30-explorer-layout-b-manual-test.md` with these steps, naming controls by their visible label:

```markdown
# Explorer layout B: manual test

Open the explorer on a store holding an nf-core/rnaseq run (`make explore CONFIG=...`) in a desktop-width window.

1. The left column lists the pipelines. Click "nf-core/rnaseq": its runs appear with green and red dots.
2. Click the good run. The page shows its outputs A–Z with item counts, "keyed by" and file chips. Run details, Storage and Lineage check are folded at the bottom; Lineage check says "worth a look" if any published file is in no output.
3. Click "quant_salmon" (or any output). The right panel says "All N items of quant_salmon" and shows a one-line `fromStore` call. Press "Copy"; paste it somewhere and check it names the run's `lid://`.
4. Click "latest good run" in the panel. The call changes to `run: 'latest', pipeline: 'nf-core/rnaseq'`. Click "this run" to go back.
5. Open "markdup". Click a `single_end` value of `false`. The page filters to those items and shows a chip "single_end = false ✕"; the panel's call gains `where: [single_end: false]`.
6. Uncheck one row. The panel says "M of N checked" and offers "Pick these M". Press it. "Picked (M)" in the top bar counts them; the panel lists them under "markdup · <run>".
7. Open the failed run's "markdup" from the left column and press "Pick all N". The panel lists two groups.
8. Type a name and press "Save as Selection". The page opens the new Selection; the panel shows its `selection:` call and the samplesheet links.
9. Narrow the window below 900 px. The left column becomes a ☰ menu and the panel a "Use" button; both open and close, and nothing scrolls sideways.
10. In dark mode (system setting), every page is readable.
```

- [ ] **Step 3: Commit**

```bash
git add docs/plans/2026-09-30-explorer-layout-b-manual-test.md
git commit -s -m "docs: manual test steps for explorer layout B

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
