# Explorer Visual Language Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One grey icon per kind wherever the explorer names a thing, every address a quiet link to a new `#/block/<cid>` page that shows any block, its hash check and its links, and simpler item rows and Selection page.

**Architecture:** A new `web/src/icons.js` owns the icon set (CSS-mask spans, so the test DOM needs no SVG), `icon()`, `ref()` and `address()`. A pure `web/src/blockinfo.js` decodes a CID and names a block's kind; `Explorer.block()` fetches a block for the page (a range read for large raw blocks, never recorded as verified); `web/src/views/block.js` draws it. Existing views swap plain text and `cid()` for these helpers. Gate browser tier A gains A8.

**Tech Stack:** Plain ES modules bundled by esbuild (`web/build.mjs`), `node --test` with the fake DOM in `web/test/dom.mjs`, `multiformats` and `@ipld/dag-cbor` / `@ipld/dag-json`, Python 3 stdlib for the Gate (`gate/browser_assert.py`), Playwright in `gate/browser/drive.mjs`.

**Spec:** `docs/superpowers/specs/2026-10-14-explorer-visual-language-design.md`

## Global Constraints

- Every `data-*` selector in DESIGN.md §15 keeps its element and meaning (the Gate reads them).
- Icons are one colour, `--muted`; colour stays reserved for status.
- `BLOCK_PAGE_RAW_CAP = 1048576` (1 MiB) and `BLOCK_PAGE_PREVIEW = 4096` (`Range: bytes=0-4095`), in `web/src/config.js`.
- A range read (`BlockFetcher.partial`) is counted in `partialFetches` and never added to `verified`.
- `shortCid`: first 10 and last 6 characters joined by `...`.
- Words: "member" means a store only; "via" becomes "from"; times use `whenText(iso)` with the ISO text as `title`.
- No change to `web/src/queries.json` (it is mirrored in `Index.groovy` and pinned by `ExplorerQueriesTest`).
- Prose (commit messages, DESIGN.md) follows the user's writing rules in `~/.claude/CLAUDE.md`: no em-dash runs, no bold-first bullets, no unicode arrows.
- Commits: `git commit -s`, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Run web tests with `cd web && npm test` (all) or `cd web && node --test test/<file>.test.mjs` (one file). Use `/usr/bin/grep`, not the shell's `grep` (ugrep here misses matches).

## Review Focus

1. A large raw block on a server that ignores Range answers 200 with the whole body: the page must not download it; it reports "not checked" and never calls `raw()`. Test in Task 3.
2. `#/block/<not a cid>`: the page shows a `schema_invalid` error node, not a crash. Test in Task 3 (`Explorer.block` rejects with `schema_invalid`; the view awaits it, so `app.js` draws its usual error node).
3. A structured block with a very long array (an OutputCollection with thousands of items): the contents draw the first 200 entries and a "... N more" line. Test in Task 4.
4. A 4 KiB text preview that ends mid-way through a multi-byte UTF-8 character still reads as text, not "binary". Test in Task 4.
5. A Claim whose `subject` is missing or not a link: its kind line has no link and nothing throws. Test in Task 3.

---

### Task 1: Icons

**Files:**
- Create: `web/src/icons.js`
- Create: `web/test/icons.test.mjs`
- Modify: `web/src/app.js` (append `ICON_CSS` beside `APP_CSS`)

**Interfaces:**
- Produces: `KINDS` (array of kind strings), `icon(kind) -> HTMLSpanElement` (`class="icon icon-<kind>"`, `data-icon=<kind>`, `title=<name>`, `aria-hidden="true"`), `ref(kind, label, href = null) -> HTMLSpanElement` (`class="ref"`, icon then label, the label inside `<a href>` when `href`), `ICON_CSS` (string).

- [ ] **Step 1: Write the failing test**

```js
// web/test/icons.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { ICON_CSS, KINDS, icon, ref } from '../src/icons.js'

test('every kind has an icon span naming its kind, and a CSS rule drawing it', () => {
  installDom()
  assert.deepEqual(KINDS, ['pipeline', 'run', 'output', 'item', 'file', 'directory', 'selection', 'claim', 'address', 'store'])
  for (const kind of KINDS) {
    const el = icon(kind)
    assert.equal(el.tagName, 'SPAN')
    assert.ok(el.classList.contains('icon') && el.classList.contains(`icon-${kind}`))
    assert.equal(el.dataset.icon, kind)
    assert.equal(el.getAttribute('aria-hidden'), 'true')
    assert.ok(el.getAttribute('title').length > 0)
    assert.match(ICON_CSS, new RegExp(`\\.icon-${kind}\\s*\\{`))
  }
  assert.equal(icon('selection').getAttribute('title'), 'Selection')
  assert.equal(icon('claim').getAttribute('title'), 'name or pin')
})

test('an unknown kind throws', () => {
  installDom()
  assert.throws(() => icon('blob'), /no icon for 'blob'/)
})

test('ref: the icon, then the label, linked when given an href', () => {
  installDom()
  const linked = ref('run', 'boring_yalow', '#/run/bafyrun')
  assert.ok(linked.classList.contains('ref'))
  assert.equal(linked.querySelector('[data-icon]').dataset.icon, 'run')
  assert.equal(linked.querySelector('a').getAttribute('href'), '#/run/bafyrun')
  assert.equal(linked.textContent, 'boring_yalow')
  const plain = ref('file', 'Tarik_Benali.dat')
  assert.equal(plain.querySelector('a'), null)
  assert.equal(plain.textContent, 'Tarik_Benali.dat')
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/icons.test.mjs`
Expected: FAIL, `Cannot find module '../src/icons.js'`.

- [ ] **Step 3: Write the implementation**

```js
// web/src/icons.js
// One grey icon per kind the explorer names (visual language spec §2): the
// same shape wherever a run, output, item, file, directory, Selection,
// Claim, address or store appears; colour stays for status. Each icon is a
// span painted by a CSS mask, so the page and the test DOM need no SVG
// elements; ICON_CSS holds the shapes.
import { h, link } from './html.js'

const SHAPES = {
  pipeline: '<path d="M2 4h4v8H2zM10 4h4v8h-4zM6 8h4" fill="none" stroke="#000" stroke-width="1.5"/>',
  run: '<path d="M4 2.5v11l9-5.5z"/>',
  output: '<path d="M2 4h12M2 8h12M2 12h12" stroke="#000" stroke-width="1.6"/>',
  item: '<path d="M6 2.5C4 2.5 4.5 6 3 8c1.5 2 1 5.5 3 5.5M10 2.5c2 0 1.5 3.5 3 5.5-1.5 2-1 5.5-3 5.5" fill="none" stroke="#000" stroke-width="1.5"/>',
  file: '<path d="M4 1.5h5l3 3v10H4zM9 1.5v3h3" fill="none" stroke="#000" stroke-width="1.4"/>',
  directory: '<path d="M1.5 4h5l1.5 1.5h6.5v8h-13z" fill="none" stroke="#000" stroke-width="1.4"/>',
  selection: '<path d="M4 1.5h8v13l-4-3-4 3z" fill="none" stroke="#000" stroke-width="1.4"/>',
  claim: '<path d="M1.5 8.5l7-7h6v6l-7 7z" fill="none" stroke="#000" stroke-width="1.4"/><circle cx="11" cy="4.6" r="1"/>',
  address: '<path d="M6 2l-1.5 12M11.5 2 10 14M2.5 5.5h11M2 10.5h11" stroke="#000" stroke-width="1.3"/>',
  store: '<ellipse cx="8" cy="3.5" rx="5.5" ry="2" fill="none" stroke="#000" stroke-width="1.3"/><path d="M2.5 3.5v9c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2v-9M2.5 8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2" fill="none" stroke="#000" stroke-width="1.3"/>',
}

const NAMES = { pipeline: 'pipeline', run: 'run', output: 'output', item: 'item', file: 'file', directory: 'directory',
  selection: 'Selection', claim: 'name or pin', address: 'address', store: 'store' }

export const KINDS = Object.keys(SHAPES)

const dataUri = (shape) => `url("data:image/svg+xml,${encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16">${shape}</svg>`)}")`

export const ICON_CSS = [
  '.icon { display: inline-block; width: 14px; height: 14px; vertical-align: -2px; margin-right: 4px; flex: none;',
  '  background-color: var(--muted); -webkit-mask: var(--icon) center / contain no-repeat; mask: var(--icon) center / contain no-repeat; }',
  '.ref { white-space: nowrap; }',
  ...KINDS.map(k => `.icon-${k} { --icon: ${dataUri(SHAPES[k])}; }`),
].join('\n')

export function icon(kind) {
  if (!(kind in SHAPES)) throw new Error(`no icon for '${kind}'`)
  return h('span', { class: `icon icon-${kind}`, 'data-icon': kind, title: NAMES[kind], 'aria-hidden': 'true' })
}

export function ref(kind, label, href = null) {
  return h('span', { class: 'ref' }, icon(kind), href ? link(href, label) : label)
}
```

In `web/src/app.js`, import and append the stylesheet:

```js
import { ICON_CSS } from './icons.js'
```

and change the first line of `start()`:

```js
  document.head.append(h('style', {}, APP_CSS), h('style', {}, PAIRS_CSS), h('style', {}, ICON_CSS))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd web && node --test test/icons.test.mjs && npm test`
Expected: PASS; the full suite still passes.

- [ ] **Step 5: Commit**

```bash
git add web/src/icons.js web/test/icons.test.mjs web/src/app.js
git commit -s -m "feat(web): one grey icon per kind the explorer names" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Addresses

**Files:**
- Modify: `web/src/icons.js` (add `shortCid`, `address`)
- Modify: `web/src/html.js:16` (remove `cid`)
- Modify: `web/src/views/common.js:123` (`shortCid` re-exported from icons)
- Modify: `web/src/views/collection.js:43,53,59-60`, `web/src/views/item.js:53,56,57`, `web/src/views/pipeline.js:40`, `web/src/views/run.js:52-53`, `web/src/views/selection.js:73,107`
- Modify: `web/src/styles.js` (address style)
- Test: `web/test/icons.test.mjs`

**Interfaces:**
- Consumes: `icon(kind)`, `ref(kind, label, href)` from Task 1.
- Produces: `shortCid(text) -> string`, `address(cidText) -> HTMLAnchorElement` (`class="address"`, `href="#/block/<cid>"`, `title=<cid>`, `data-address=<cid>`, an address icon then `shortCid(cid)`). `address()` lives in `icons.js` (it draws an icon), not `html.js` as the spec's table says; Task 9 records this.

- [ ] **Step 1: Write the failing test** (append to `web/test/icons.test.mjs`, and add `address, shortCid` to its import)

```js
test('address: the address icon and the short CID, a quiet link to its block page', () => {
  installDom()
  const cid = 'bafyreian3sfv6mv6jh2en6g3ryqo2p2lde4nh3zxaysbhs5g574uac6uye'
  const a = address(cid)
  assert.equal(a.tagName, 'A')
  assert.ok(a.classList.contains('address'))
  assert.equal(a.getAttribute('href'), `#/block/${cid}`)
  assert.equal(a.getAttribute('title'), cid)
  assert.equal(a.dataset.address, cid)
  assert.equal(a.querySelector('[data-icon]').dataset.icon, 'address')
  assert.equal(a.textContent, 'bafyreian3s...ac6uye')
  assert.equal(shortCid('short'), 'short')
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/icons.test.mjs`
Expected: FAIL, `address` is not exported.

- [ ] **Step 3: Write the implementation**

Append to `web/src/icons.js`:

```js
export const shortCid = (text) => (text.length > 20 ? `${text.slice(0, 10)}...${text.slice(-6)}` : text)

/** A block's address (spec §3): quiet, always present, a link to the block page. */
export function address(cidText) {
  return h('a', { class: 'address', href: `#/block/${cidText}`, title: cidText, 'data-address': cidText }, icon('address'), shortCid(cidText))
}
```

In `web/src/html.js` delete the line `export const cid = (text) => h('code', { class: 'cid', title: text }, text)`.

In `web/src/views/common.js` replace line 123 (`export const shortCid = ...`) with:

```js
export { shortCid } from '../icons.js'
```

and add `import { shortCid } from '../icons.js'` beside the other imports (common.js uses `shortCid` itself).

Replace each `cid(...)` call (find them with `/usr/bin/grep -n "\bcid(" web/src/views/*.js`) and drop `cid` from each file's `../html.js` import, adding `import { address, ref, shortCid } from '../icons.js'` as each file needs:

`web/src/views/collection.js`:

```js
    fold(`details:${collectionCid}`, 'Details', h('p', {}, address(collectionCid)), indexAsWritten(ex, c.index)))
```
```js
    h('h1', {}, 'Every run that produced this file'), address(contentCid),
```
```js
      h('td', {}, p.filename ?? ''), h('td', {}, ref('item', shortCid(p.item_cid), `#/item/${p.collection_cid}/${p.item_cid}`)),
      h('td', {}, ref('run', shortCid(p.completion_cid), `#/run/${p.completion_cid}`))))))
```

`web/src/views/item.js`:

```js
    holding.length ? [h('h2', {}, 'In Selections'), h('ul', {}, holding.map(s => h('li', {}, ref('selection', shortCid(s), `#/selection/${s}`))))] : null,
```
```js
      h('p', {}, 'Address: ', address(itemCid),
        collectionCid !== '-' ? [' · as it appeared in its output: ', h('code', {}, `cas://${collectionCid}/${itemCid}`)] : null),
      h('ul', {}, it.leaves.filter(l => l.address).map(l => h('li', {}, l.name ?? '', ': ', address(String(l.address))))),
```

`web/src/views/pipeline.js`:

```js
    h('p', { 'data-latest': best ?? '' }, best ? ref('run', shortCid(best), `#/run/${best}`) : 'None: no run of this pipeline succeeded without being possibly incomplete.'))
```

`web/src/views/run.js`:

```js
    h('p', {}, 'RunCompletion: ', address(completionCid)),
    row.manifest_cid ? h('p', {}, 'RunManifest: ', address(row.manifest_cid)) : null)
```

`web/src/views/selection.js` (line 73, the nested Selection row; Task 6 redraws it):

```js
        h('div', { class: 'row-head' }, h('strong', {}, 'Selection'), address(m.address), h('span', { class: 'row-action' }, held)))
```
```js
    fold(`details:${selectionCid}`, 'Details', h('p', {}, address(selectionCid)),
```

In `web/src/styles.js`, after the `.ext` rule add:

```js
  .address { color: var(--muted); font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; text-decoration-color: var(--line); white-space: nowrap; }
  .address .icon { margin-right: 2px; }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd web && npm test`
Expected: PASS. If a test imported `cid` from `../src/html.js` (`/usr/bin/grep -rn "cid }" web/test` or `"cid,"`), change it to `address` from `../src/icons.js` and assert on `[data-address]` instead.

- [ ] **Step 5: Commit**

```bash
git add web/src web/test
git commit -s -m "feat(web): every address is a quiet link to its block page" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Reading a block for the block page

**Files:**
- Create: `web/src/blockinfo.js`
- Create: `web/test/blockinfo.test.mjs`
- Modify: `web/src/blocks.js` (add `partial`, `partialFetches`)
- Modify: `web/src/config.js` (two constants)
- Modify: `web/src/model.js` (add `Explorer.block`)
- Modify: `web/test/fixture.mjs` (add `rangedFetch`)
- Test: `web/test/blocks.test.mjs`, `web/test/model.test.mjs`

**Interfaces:**
- Produces: `cidParts(cidText) -> { version: number, codec: 'dag-cbor'|'raw'|string, hash: 'sha2-256'|string, digest: string (hex), standard: boolean }`, throwing `BlockError('schema_invalid')` for a non-CID.
- Produces: `friendlyOf(cidText, value, stale = []) -> { kind: string (an icon kind), text: string, label: string|null, href: string|null }`.
- Produces: `BlockFetcher#partial(cidText, n) -> Promise<{ bytes: Uint8Array|null, size: number|null }>`; `BlockFetcher#partialFetches` (number).
- Produces: `Explorer#block(cidText, { rawCap, previewBytes } = {}) -> Promise<BlockInfo>` where `BlockInfo = { cid, parts, check: 'verified'|'unchecked'|'refused'|'missing', size: number|null, value: any|null, schemaError: string|null, bytes: Uint8Array|null, preview: Uint8Array|null, friendly: object|null, message: string|null }`.
- Produces (tests): `rangedFetch(blocks)` in `web/test/fixture.mjs`.

- [ ] **Step 1: Write the failing tests**

```js
// web/test/blockinfo.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import * as Digest from 'multiformats/hashes/digest'
import { block, rawCid } from './fixture.mjs'
import { cidParts, friendlyOf } from '../src/blockinfo.js'

test('cidParts: a dag-cbor and a raw address decoded', () => {
  const item = block({ kind: 'OutputItem', schema: 2, value: {} }).cid
  const p = cidParts(item.toString())
  assert.deepEqual([p.version, p.codec, p.hash, p.standard], [1, 'dag-cbor', 'sha2-256', true])
  assert.equal(p.digest, [...item.multihash.digest].map(b => b.toString(16).padStart(2, '0')).join(''))
  assert.equal(cidParts(rawCid('x').toString()).codec, 'raw')
})

test('cidParts: an address this store would not write is decoded and marked; a non-CID is schema_invalid', () => {
  const sha512 = CID.create(1, 0x55, Digest.create(0x13, new Uint8Array(64)))
  const p = cidParts(sha512.toString())
  assert.equal(p.hash, '0x13')
  assert.equal(p.standard, false)
  assert.throws(() => cidParts('not-a-cid'), (e) => e.code === 'schema_invalid')
})

test('friendlyOf: each kind names itself and links to its page', () => {
  const c = 'bafyx'
  assert.deepEqual(friendlyOf(c, { kind: 'RunCompletion' }), { kind: 'run', text: 'a run', label: 'Open the run', href: '#/run/bafyx' })
  assert.equal(friendlyOf(c, { kind: 'OutputCollection' }).href, '#/collection/bafyx')
  assert.equal(friendlyOf(c, { kind: 'OutputItem' }).href, '#/item/-/bafyx')
  assert.equal(friendlyOf(c, { kind: 'Selection' }).href, '#/selection/bafyx')
  const subject = block({ kind: 'Selection' }).cid
  assert.equal(friendlyOf(c, { kind: 'Claim', subject, verb: 'set', attribute: 'name' }).href, `#/block/${subject}`)
  assert.equal(friendlyOf(c, { kind: 'DirectoryManifest' }).kind, 'directory')
  assert.equal(friendlyOf(c, { kind: 'Forget' }).href, null)
  assert.equal(friendlyOf(c, null).text, 'a block that does not decode')
})

test('friendlyOf: a Claim with no usable subject has no link (Review Focus 5)', () => {
  assert.equal(friendlyOf('bafyx', { kind: 'Claim', verb: 'set', attribute: 'name' }).href, null)
  assert.equal(friendlyOf('bafyx', { kind: 'Claim', subject: 'not a link', verb: 'set' }).href, null)
})

test('friendlyOf: a RunManifest links to its run when the Store Log tail names it', () => {
  const manifest = block({ kind: 'RunManifest' }).cid
  const stale = [{ cid: 'bafycompletion', completion: { run: manifest } }]
  assert.equal(friendlyOf(manifest.toString(), { kind: 'RunManifest' }, stale).href, '#/run/bafycompletion')
  assert.equal(friendlyOf(manifest.toString(), { kind: 'RunManifest' }, []).href, null)
})
```

Append to `web/test/fixture.mjs`:

```js
/** A fetch over the fixture's blocks that honours `Range: bytes=0-<n>` with a 206, as serve.py and explore do. */
export function rangedFetch(blocks) {
  const fn = async (url, init = {}) => {
    const cid = String(url).split('/').pop()
    const bytes = blocks.get(cid)
    if (!bytes) return new Response('no such block', { status: 404 })
    const m = /^bytes=0-(\d+)$/.exec(init.headers?.Range ?? '')
    if (!m) return new Response(bytes, { headers: { 'Content-Length': String(bytes.length) } })
    const part = bytes.slice(0, Number(m[1]) + 1)
    return new Response(part, { status: 206, headers: { 'Content-Range': `bytes 0-${part.length - 1}/${bytes.length}` } })
  }
  return fn
}
```

Append to `web/test/blocks.test.mjs` (add `rangedFetch` to its fixture import):

```js
test('partial: the first n bytes and the whole size by Range, never recorded as verified', async () => {
  const text = 'x'.repeat(10000)
  const cid = rawCid(text).toString()
  const fetcher = new BlockFetcher('http://h/', { fetchFn: rangedFetch(new Map([[cid, new TextEncoder().encode(text)]])) })
  const { bytes, size } = await fetcher.partial(cid, 4096)
  assert.equal(bytes.length, 4096)
  assert.equal(size, 10000)
  assert.equal(fetcher.partialFetches, 1)
  assert.deepEqual(fetcher.verified, [])
})

test('partial: a server that ignores Range gives no bytes, only the size it states (Review Focus 1)', async () => {
  const cid = rawCid('whole').toString()
  let cancelled = false
  const fetchFn = async () => ({ status: 200, ok: true, headers: new Headers({ 'Content-Length': '5' }), body: { cancel: () => { cancelled = true } } })
  const { bytes, size } = await new BlockFetcher('http://h/', { fetchFn }).partial(cid, 4096)
  assert.equal(bytes, null)
  assert.equal(size, 5)
  assert.ok(cancelled, 'the whole body is not read')
})
```

Append to `web/test/model.test.mjs` (add `rangedFetch` to its fixture import, and `snapshotDb` is already imported):

```js
async function openRanged(extraBlocks = []) {
  const now = Date.now()
  const member = await buildMember({ now })
  for (const [cid, bytes] of extraBlocks) member.blocks.set(cid, bytes)
  const blocks = new BlockFetcher('http://h/m/lab/', { fetchFn: rangedFetch(member.blocks) })
  const ex = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot), blocks,
    listFn: async () => ({ names: member.log, readable: true }), now: () => now })
  return { ex, member, blocks }
}

test('block: a structured block, verified, with its kind and friendly link', async () => {
  const { ex, member } = await openRanged()
  const b = await ex.block(member.runs.R1.completion)
  assert.equal(b.check, 'verified')
  assert.equal(b.parts.codec, 'dag-cbor')
  assert.equal(b.value.kind, 'RunCompletion')
  assert.equal(b.size, b.bytes.length)
  assert.equal(b.friendly.href, `#/run/${member.runs.R1.completion}`)
})

test('block: a small raw block is read whole and verified; a large one is previewed and unchecked', async () => {
  const small = new TextEncoder().encode('Tarik Benali from Casablanca, Africa\n')
  const large = new TextEncoder().encode('y'.repeat(6000))
  const smallCid = rawCid('Tarik Benali from Casablanca, Africa\n').toString()
  const largeCid = rawCid('y'.repeat(6000)).toString()
  const { ex, blocks } = await openRanged([[smallCid, small], [largeCid, large]])
  const s = await ex.block(smallCid)
  assert.equal(s.check, 'verified')
  assert.equal(new TextDecoder().decode(s.bytes), 'Tarik Benali from Casablanca, Africa\n')
  assert.equal(s.friendly.href, `#/content/${smallCid}`)
  const l = await ex.block(largeCid, { rawCap: 5000 })
  assert.equal(l.check, 'unchecked')
  assert.equal(l.size, 6000)
  assert.equal(l.preview.length, 4096)
  assert.equal(l.bytes, null)
  assert.ok(!blocks.verified.includes(largeCid))
})

test('block: missing, refused, and a block that fails the schema is still shown with its reason', async () => {
  const tampered = rawCid('real').toString()
  const odd = block({ kind: 'OutputItem' })   // no schema, no value: decodes, fails validBlock
  const { ex } = await openRanged([[tampered, new TextEncoder().encode('fake')], [odd.cid.toString(), odd.bytes]])
  assert.equal((await ex.block(rawCid('absent').toString())).check, 'missing')
  const refused = await ex.block(tampered)
  assert.equal(refused.check, 'refused')
  assert.equal(refused.bytes, null)
  const shown = await ex.block(odd.cid.toString())
  assert.equal(shown.check, 'verified')
  assert.equal(shown.value.kind, 'OutputItem')
  assert.match(shown.schemaError, /schema/)
})

test('block: a large raw block on a server without Range is never read whole (Review Focus 1)', async () => {
  const big = new TextEncoder().encode('z'.repeat(6000))
  const cid = rawCid('z'.repeat(6000)).toString()
  const { ex, blocks } = await openRanged([[cid, big]])
  blocks.fetchFn = async () => ({ status: 200, ok: true, headers: new Headers({ 'Content-Length': '6000' }), body: { cancel() {} } })
  const before = blocks.fetches   // opening the Explorer already fetched the tail's blocks
  const b = await ex.block(cid, { rawCap: 5000 })
  assert.equal(b.check, 'unchecked')
  assert.equal(b.preview, null)
  assert.equal(blocks.fetches, before, 'raw() was never called')
})

test('block: a URL that is not a CID is schema_invalid (Review Focus 2)', async () => {
  const { ex } = await openRanged()
  await assert.rejects(ex.block('not-a-cid'), (e) => e.code === 'schema_invalid')
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd web && node --test test/blockinfo.test.mjs test/blocks.test.mjs test/model.test.mjs`
Expected: FAIL; `blockinfo.js` is missing, `partial` and `block` are not functions.

- [ ] **Step 3: Write the implementation**

`web/src/config.js`, append:

```js
/** The largest raw block the block page reads whole and hash-checks (visual language spec §4.3). */
export const BLOCK_PAGE_RAW_CAP = 1048576
/** The bytes of a raw block the block page previews. */
export const BLOCK_PAGE_PREVIEW = 4096
```

`web/src/blocks.js`: in the constructor add `this.partialFetches = 0`, and add this method after `raw`:

```js
  /**
   * The first `n` bytes of a block by Range, and its whole size from
   * Content-Range: `{ bytes, size }`. Not verified, since checking needs
   * every byte, so never recorded in `verified`; counted in `partialFetches`.
   * A server that ignores Range answers 200 with the whole body, which is not
   * read: `bytes` is null and `size` is its Content-Length, or null.
   */
  async partial(cidText, n) {
    try {
      CID.parse(cidText)
    } catch (e) {
      throw new BlockError('schema_invalid', cidText, `${cidText} is not a CID: ${e.message}`)
    }
    let res
    try {
      res = await this.fetchFn(this.urlFor(cidText), { headers: { Range: `bytes=0-${n - 1}` } })
    } catch (e) {
      throw new BlockError('fetch_failed', cidText, `could not fetch block ${cidText}: ${e.message}`)
    }
    this.partialFetches++
    if (res.status === 404 || res.status === 403)
      throw new BlockError('block_missing', cidText, `block ${cidText} is not in this member; it may be held in another one`)
    if (res.status !== 206) {
      try { await res.body?.cancel?.() } catch { /* already closed */ }
      return { bytes: null, size: Number(res.headers.get('Content-Length')) || null }
    }
    const size = Number((res.headers.get('Content-Range') ?? '').split('/')[1]) || null
    return { bytes: new Uint8Array(await res.arrayBuffer()), size }
  }
```

`web/src/blockinfo.js`:

```js
// web/src/blockinfo.js
// What the block page says about an address (visual language spec §4.1,
// §4.2): its CID decoded, and the kind of block it names with a link to the
// block's own page. Pure.
import { CID } from 'multiformats/cid'
import { DAG_CBOR, RAW } from './cid.js'
import { BlockError } from './blocks.js'

const SHA2_256 = 0x12
const CODECS = { [DAG_CBOR]: 'dag-cbor', [RAW]: 'raw' }
const HASHES = { [SHA2_256]: 'sha2-256' }
const hex = (bytes) => [...bytes].map(b => b.toString(16).padStart(2, '0')).join('')

export function cidParts(cidText) {
  let c
  try {
    c = CID.parse(cidText)
  } catch (e) {
    throw new BlockError('schema_invalid', cidText, `${cidText} is not a CID: ${e.message}`)
  }
  return {
    version: c.version,
    codec: CODECS[c.code] ?? `0x${c.code.toString(16)}`,
    hash: HASHES[c.multihash.code] ?? `0x${c.multihash.code.toString(16)}`,
    digest: hex(c.multihash.digest),
    standard: c.version === 1 && c.code in CODECS && c.multihash.code === SHA2_256,
  }
}

const linkText = (v) => { const c = CID.asCID(v); return c ? c.toString() : null }

const FRIENDLY = {
  RunCompletion: (cid) => ({ kind: 'run', text: 'a run', label: 'Open the run', href: `#/run/${cid}` }),
  OutputCollection: (cid) => ({ kind: 'output', text: 'an output', label: 'Open the output', href: `#/collection/${cid}` }),
  OutputItem: (cid) => ({ kind: 'item', text: 'an item: one channel item as a run published it', label: 'Open as an item', href: `#/item/-/${cid}` }),
  Selection: (cid) => ({ kind: 'selection', text: 'a Selection', label: 'Open the Selection', href: `#/selection/${cid}` }),
  Claim: (cid, v) => {
    const subject = linkText(v.subject)
    return { kind: 'claim', text: `a Claim: ${[v.verb, v.attribute].filter(Boolean).join(' ')}`,
      label: subject ? 'What it is about' : null, href: subject ? `#/block/${subject}` : null }
  },
  DirectoryManifest: () => ({ kind: 'directory', text: 'a directory: its entries are below', label: null, href: null }),
  Forget: () => ({ kind: 'address', text: "a sweep's record of the blocks it deleted", label: null, href: null }),
  RunManifest: (cid, v, stale) => {
    const run = stale.find(s => s.completion && linkText(s.completion.run) === cid)
    return { kind: 'run', text: "a run's start record", label: run ? 'Open the run' : null, href: run ? `#/run/${run.cid}` : null }
  },
}

export function friendlyOf(cidText, value, stale = []) {
  const make = FRIENDLY[value?.kind]
  if (make) return make(cidText, value, stale)
  return { kind: 'address', text: value ? `a block of kind ${value.kind ?? 'unknown'}` : 'a block that does not decode', label: null, href: null }
}
```

`web/src/model.js`: add imports

```js
import * as dagCbor from '@ipld/dag-cbor'
import { cidParts, friendlyOf } from './blockinfo.js'
```

add `BLOCK_PAGE_PREVIEW, BLOCK_PAGE_RAW_CAP` to the `./config.js` import, and add this method after `stableIndex`:

```js
  /**
   * One block for the block page (visual language spec §4): its CID
   * decoded, the check the browser made, and its value or bytes. A raw block
   * over `rawCap` is previewed by Range and left unchecked; a structured
   * block that fails the schema is still decoded, with the reason. A
   * missing or refused block is reported, not thrown.
   */
  async block(cidText, { rawCap = BLOCK_PAGE_RAW_CAP, previewBytes = BLOCK_PAGE_PREVIEW } = {}) {
    const parts = cidParts(cidText)
    const out = { cid: cidText, parts, check: null, size: null, value: null, schemaError: null, bytes: null, preview: null, friendly: null, message: null }
    try {
      if (parts.codec === 'raw') {
        const head = await this.blocks.partial(cidText, previewBytes)
        out.size = head.size
        if (head.size !== null && head.size <= rawCap) {
          out.bytes = await this.blocks.raw(cidText)
          out.size = out.bytes.length
          out.check = 'verified'
        } else {
          out.preview = head.bytes
          out.check = 'unchecked'
        }
        out.friendly = { kind: 'file', text: "a file's bytes", label: 'Who produced it', href: `#/content/${cidText}` }
      } else {
        try {
          const b = await this.blocks.get(cidText)
          out.bytes = b.bytes
          out.value = b.value
        } catch (e) {
          if (!(e instanceof BlockError) || e.code !== 'schema_invalid') throw e
          out.bytes = await this.blocks.raw(cidText)
          out.schemaError = e.message
          try { out.value = dagCbor.decode(out.bytes) } catch { out.value = null }
        }
        out.size = out.bytes.length
        out.check = 'verified'
        out.friendly = friendlyOf(cidText, out.value, this.stale)
      }
    } catch (e) {
      if (!(e instanceof BlockError)) throw e
      if (e.code === 'hash_mismatch') Object.assign(out, { check: 'refused', bytes: null, value: null, preview: null, message: e.message })
      else if (e.code === 'block_missing') Object.assign(out, { check: 'missing', message: e.message })
      else throw e
    }
    return out
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd web && node --test test/blockinfo.test.mjs test/blocks.test.mjs test/model.test.mjs && npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/src/blockinfo.js web/src/blocks.js web/src/config.js web/src/model.js web/test
git commit -s -m "feat(web): read any block for the block page, a large raw one by Range" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: The block page

**Files:**
- Create: `web/src/views/block.js`
- Create: `web/test/block-view.test.mjs`
- Modify: `web/src/views/index.js` (export `block`)
- Modify: `web/src/app.js` (route, `ctx.store`)
- Modify: `web/src/styles.js` (block page styles)

**Interfaces:**
- Consumes: `Explorer#block(cid) -> BlockInfo` (Task 3), `icon`, `address` (Tasks 1-2), `copyOutcome`, `saveText` (`html.js`), `humanBytes` (`words.js`).
- Consumes: `ctx.store = { label: string, others: (hash) => Array<{ alias, href }> }`, set by `app.js` in this task.
- Produces: `block(ex, cidText, ctx) -> Promise<HTMLElement>`: a `section` with `data-block=<cid>` and `data-block-check=<check>`; `[data-block-kind]` on the kind line.

- [ ] **Step 1: Write the failing test**

```js
// web/test/block-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { block as blockView } from '../src/views/block.js'
import { block as blockOf, rawCid } from './fixture.mjs'

const leaf = rawCid('Tarik Benali from Casablanca, Africa\n')
const item = blockOf({ kind: 'OutputItem', schema: 2, value: { datafile: { kind: 'Leaf', name: 'Tarik_Benali.dat', address: leaf, size: 37, reason: null } } })
const parts = (codec) => ({ version: 1, codec, hash: 'sha2-256', digest: 'ab'.repeat(32), standard: true })
const ctx = (extra = {}) => ({ store: { label: 'roblab', others: (hash) => [{ alias: 'shared', href: `?member=shared${hash}` }] }, ...extra })
const exWith = (info) => ({ block: async () => info, blocks: { urlFor: (c) => `blocks/${c}` } })

function structured(extra = {}) {
  return { cid: item.cid.toString(), parts: parts('dag-cbor'), check: 'verified', size: item.bytes.length, value: { kind: 'OutputItem', schema: 2,
    value: { datafile: { kind: 'Leaf', name: 'Tarik_Benali.dat', address: leaf, size: 37, reason: null } } }, schemaError: null,
    bytes: item.bytes, preview: null, friendly: { kind: 'item', text: 'an item', label: 'Open as an item', href: `#/item/-/${item.cid}` }, message: null, ...extra }
}

test('a structured block: what its address says, the check, and its contents with each link an address', async () => {
  installDom()
  const page = await blockView(exWith(structured()), item.cid.toString(), ctx())
  assert.equal(page.dataset.block, item.cid.toString())
  assert.equal(page.dataset.blockCheck, 'verified')
  assert.equal(page.querySelector('[data-block-kind]').querySelector('a').getAttribute('href'), `#/item/-/${item.cid}`)
  assert.match(page.textContent, /dag-cbor/)
  assert.match(page.textContent, /Read \d+ bytes from roblab; their SHA-256 matches the address/)
  const pre = page.querySelector('pre')
  assert.equal(pre.querySelector('[data-address]').dataset.address, leaf.toString())
  assert.match(pre.textContent, /"name": "Tarik_Benali.dat"/)
})

test('a schema failure is drawn with its reason; a refused block shows nothing from its bytes', async () => {
  installDom()
  const odd = await blockView(exWith(structured({ schemaError: 'block x does not match the OutputItem schema' })), item.cid.toString(), ctx())
  assert.match(odd.querySelector('.warn').textContent, /does not match the OutputItem schema/)
  assert.ok(odd.querySelector('pre'))
  const refused = await blockView(exWith(structured({ check: 'refused', value: null, bytes: null, friendly: null })), item.cid.toString(), ctx())
  assert.equal(refused.dataset.blockCheck, 'refused')
  assert.equal(refused.querySelector('pre'), null)
  assert.match(refused.textContent, /Refused/)
})

test('a missing block says where else to look', async () => {
  installDom()
  const page = await blockView(exWith(structured({ check: 'missing', value: null, bytes: null, friendly: null })), item.cid.toString(), ctx())
  assert.equal(page.dataset.blockCheck, 'missing')
  assert.match(page.textContent, /Not in roblab/)
  assert.equal(page.querySelectorAll('a').find(a => a.textContent === 'Look in shared').getAttribute('href'), `?member=shared#/block/${item.cid}`)
})

test('a raw block: a text preview and Download; unchecked when large; binary named as such', async () => {
  installDom()
  const raw = { cid: leaf.toString(), parts: parts('raw'), check: 'verified', size: 37, value: null, schemaError: null,
    bytes: new TextEncoder().encode('Tarik Benali from Casablanca, Africa\n'), preview: null,
    friendly: { kind: 'file', text: "a file's bytes", label: 'Who produced it', href: `#/content/${leaf}` }, message: null }
  const small = await blockView(exWith(raw), leaf.toString(), ctx())
  assert.match(small.querySelector('pre').textContent, /Tarik Benali from Casablanca/)
  assert.equal(small.querySelector('a[download]').getAttribute('href'), `blocks/${leaf}`)
  const large = await blockView(exWith({ ...raw, check: 'unchecked', size: 6000000, bytes: null, preview: new TextEncoder().encode('head of a big file') }), leaf.toString(), ctx())
  assert.equal(large.dataset.blockCheck, 'unchecked')
  assert.match(large.textContent, /Not checked: checking needs all 6000000 bytes/)
  assert.match(large.querySelector('pre').textContent, /head of a big file/)
  const binary = await blockView(exWith({ ...raw, bytes: new Uint8Array([0, 159, 146, 150]) }), leaf.toString(), ctx())
  assert.match(binary.textContent, /binary, 4 bytes/)
})

test('a preview cut inside a multi-byte character is still text (Review Focus 4)', async () => {
  installDom()
  const cut = new TextEncoder().encode('café').slice(0, 4)   // 'caf' and the first byte of 'é'
  const page = await blockView(exWith({ cid: leaf.toString(), parts: parts('raw'), check: 'unchecked', size: 9000000, value: null, schemaError: null,
    bytes: null, preview: cut, friendly: { kind: 'file', text: "a file's bytes", label: null, href: null }, message: null }), leaf.toString(), ctx())
  assert.match(page.querySelector('pre').textContent, /^caf/)
  assert.doesNotMatch(page.textContent, /binary/)
})

test('a long array draws its first 200 entries and says how many more (Review Focus 3)', async () => {
  installDom()
  const items = Array.from({ length: 450 }, (_, i) => rawCid(`i${i}`))
  const page = await blockView(exWith(structured({ value: { kind: 'OutputCollection', items } })), item.cid.toString(), ctx())
  assert.equal(page.querySelector('pre').querySelectorAll('[data-address]').length, 200)
  assert.match(page.querySelector('pre').textContent, /\.\.\. 250 more/)
})

test('an address this store would not write is marked', async () => {
  installDom()
  const page = await blockView(exWith(structured({ parts: { ...parts('dag-cbor'), hash: '0x13', standard: false } })), item.cid.toString(), ctx())
  assert.match(page.textContent, /Not an address this store writes/)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/block-view.test.mjs`
Expected: FAIL, `Cannot find module '../src/views/block.js'`.

- [ ] **Step 3: Write the implementation**

```js
// web/src/views/block.js
// #/block/<cid> (visual language spec §4): any block by its address, with
// what the address says, the check the browser made, and its contents, each
// link inside them an address to its own block page.
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'
import { h, link, copyOutcome, saveText } from '../html.js'
import { address, icon } from '../icons.js'
import { humanBytes } from '../words.js'

const FORMAT = {
  'dag-cbor': 'structured data, which can link to other blocks',
  raw: 'bytes exactly as the file held them; no name, no links',
}
const ARRAY_SHOWN = 200
const shortHex = (hex) => (hex.length > 16 ? `${hex.slice(0, 8)}...${hex.slice(-8)}` : hex)

export async function block(ex, cidText, ctx) {
  const b = await ex.block(cidText)
  const copy = h('button', { type: 'button', onclick: async (e) => { e.currentTarget.textContent = await copyOutcome(globalThis.navigator?.clipboard, cidText) } }, 'Copy')
  const read = b.check === 'verified' || b.check === 'unchecked'
  return h('section', { 'data-block': cidText, 'data-block-check': b.check },
    h('h1', { class: 'block-cid' }, icon('address'), h('code', {}, cidText), ' ', copy),
    kindLine(b),
    h('h2', {}, 'What the address says'), partsList(b),
    h('h2', {}, 'Checked'), checkLine(b, ctx),
    read ? [h('h2', {}, 'Contents'), contents(ex, b, ctx)] : null)
}

function kindLine(b) {
  if (!b.friendly) return h('p', { class: 'muted' }, 'Not read, so what it holds is unknown.')
  const f = b.friendly
  return h('p', { 'data-block-kind': b.value?.kind ?? b.parts.codec }, icon(f.kind), `This is ${f.text}.`, f.href ? [' ', link(f.href, f.label)] : null)
}

function partsList(b) {
  const p = b.parts
  const of = b.size === null ? 'its bytes' : `its ${b.size} bytes`
  return h('dl', { class: 'block-parts' },
    h('dt', {}, 'format'), h('dd', {}, h('code', {}, p.codec), FORMAT[p.codec] ? ` ${FORMAT[p.codec]}` : ''),
    h('dt', {}, 'fingerprint'), h('dd', {}, h('code', {}, p.hash), ` of ${of}: `, h('code', { title: p.digest }, shortHex(p.digest))),
    h('dt', {}, 'so'), h('dd', {}, 'Any copy of these bytes, anywhere, has this address; change one byte and it changes.',
      p.codec === 'raw' ? ' Two runs that write the same bytes share this one block.' : ''),
    p.standard ? null : [h('dt', {}, 'note'), h('dd', { class: 'warn' }, 'Not an address this store writes (DESIGN.md §3).')])
}

function checkLine(b, ctx) {
  const from = ctx.store?.label ?? 'this store'
  if (b.check === 'verified') return h('p', { class: 'ok' }, `✓ Read ${b.size} bytes from ${from}; their SHA-256 matches the address. Checked in your browser.`)
  if (b.check === 'unchecked') {
    return h('p', { class: 'muted' }, b.size === null
      ? 'Not checked: this server did not say how large the block is, and checking needs every byte.'
      : `Not checked: checking needs all ${b.size} bytes (${humanBytes(b.size)}).`)
  }
  if (b.check === 'refused') return h('p', { class: 'warn' }, 'Refused: these bytes do not hash to the address, so nothing from them is shown.')
  const others = ctx.store?.others?.(`#/block/${b.cid}`) ?? []
  return h('p', { class: 'warn' }, `Not in ${from}; it may be held in another store.`, others.map(o => [' ', link(o.href, `Look in ${o.alias}`)]))
}

function contents(ex, b, ctx) {
  if (b.parts.codec === 'raw') {
    const bytes = b.bytes ?? b.preview
    return [
      bytes ? preview(bytes, b.bytes === null) : h('p', { class: 'muted' }, 'No preview: this server ignores Range.'),
      h('p', { class: 'muted' }, b.bytes ? '' : `The first ${bytes?.length ?? 0} bytes. `, h('a', { href: ex.blocks.urlFor(b.cid), download: '' }, 'Download')),
    ]
  }
  if (b.value === null) return h('p', { class: 'warn' }, b.schemaError ?? 'These bytes do not decode as DAG-CBOR.')
  const save = (ctx.download ?? saveText)
  return [
    b.schemaError ? h('p', { class: 'warn' }, b.schemaError) : null,
    h('pre', { class: 'dag-json' }, json(b.value, 0)),
    h('p', { class: 'muted' }, 'Each address in it is a link: a block names the blocks it depends on by their addresses. ',
      h('a', { href: ex.blocks.urlFor(b.cid), download: `${b.cid}.cbor` }, 'Raw bytes'), ' ',
      h('button', { type: 'button', class: 'link', onclick: () => save(`${b.cid}.json`, new TextDecoder().decode(dagJson.encode(b.value))) }, 'DAG-JSON')),
  ]
}

/** Text when the bytes decode as UTF-8 with no NUL, else "binary"; `truncated` lets the last character be cut off. */
function preview(bytes, truncated) {
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes, { stream: truncated })
    if (!text.includes('\0')) return h('pre', { class: 'scroll' }, text)
  } catch { /* not UTF-8 */ }
  return h('p', { class: 'muted' }, `binary, ${bytes.length} bytes`)
}

/** DAG-JSON drawn from the decoded value, each link an address; an array past ARRAY_SHOWN is cut with a count. */
function json(value, depth) {
  const pad = '  '.repeat(depth + 1)
  const end = '  '.repeat(depth)
  const cid = CID.asCID(value)
  if (cid) return ['{"/": ', address(cid.toString()), '}']
  if (value instanceof Uint8Array) return [`{"/": {"bytes": "${btoa(String.fromCharCode(...value))}"}}`]
  if (Array.isArray(value)) {
    if (!value.length) return ['[]']
    const shown = value.slice(0, ARRAY_SHOWN)
    const more = value.length - shown.length
    return ['[\n', shown.flatMap((v, i) => [pad, ...json(v, depth + 1), i < shown.length - 1 || more ? ',\n' : '\n']),
      more ? `${pad}... ${more} more\n` : '', end, ']']
  }
  if (value !== null && typeof value === 'object') {
    const keys = Object.keys(value)
    if (!keys.length) return ['{}']
    return ['{\n', keys.flatMap((k, i) => [pad, JSON.stringify(k), ': ', ...json(value[k], depth + 1), i < keys.length - 1 ? ',\n' : '\n']), end, '}']
  }
  return [typeof value === 'bigint' ? String(value) : JSON.stringify(value)]
}
```

`web/src/views/index.js`, add:

```js
export { block } from './block.js'
```

`web/src/app.js`: add the route after `content`:

```js
  ['block', /^#\/block\/([^/?]+)$/, (ex, m, ctx) => views.block(ex, m[1], ctx)],
```

and in `render()`'s `ctx` object add:

```js
    // Which store the page reads, and the same page in the others (the block page, visual language spec §4.5).
    store: { label: store.member ?? store.base,
      others: (hash) => (store.members ?? []).filter(m => m.alias !== store.member).map(m => ({ alias: m.alias, href: hrefIn(m.alias, hash) })) },
```

`web/src/styles.js`, add:

```js
  .block-cid { font-size: 1rem; display: flex; gap: .4rem; align-items: baseline; flex-wrap: wrap; } .block-cid code { overflow-wrap: anywhere; }
  .block-parts { display: grid; grid-template-columns: auto 1fr; gap: .1rem .8rem; } .block-parts dt { color: var(--muted); } .block-parts dd { margin: 0; }
  .ok { color: var(--good); } pre.dag-json { background: var(--code); border-radius: 5px; padding: .5rem .7rem; overflow-x: auto; }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd web && node --test test/block-view.test.mjs && npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add web/src/views/block.js web/src/views/index.js web/src/app.js web/src/styles.js web/test/block-view.test.mjs
git commit -s -m "feat(web): #/block/<cid>, one page for any block" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Item rows

**Files:**
- Modify: `web/src/views/common.js:191-220` (`itemRow`, `fillRow`, `watchRows`' call), `runLabelNode`
- Modify: `web/src/previews.js:63` (record directory leaves)
- Modify: `web/src/views/selection.js` (held text, `fromLabel`)
- Modify: `web/src/styles.js` (row styles)
- Test: `web/test/views.test.mjs`, `web/test/pages.test.mjs`

**Interfaces:**
- Consumes: `icon`, `ref`, `address` (Tasks 1-2).
- Produces: `itemRow(ex, ctx, { address, via, previews, target, index, attrs, lead, action, fromLabel = null, noVia, perVia })` (`viaLabel` renamed `fromLabel`); a row is `li.row` > `div.row-head` (lead, item icon, `strong.row-label`, `span.row-action`) and `div.row-sub` (`span.row-files`, the from line, `address(item)`). No `[data-preview-for]` in rows.
- Produces: previews gain `dirs: string[]`, the names of leaves whose address is a dag-cbor (directory) CID.
- Produces: `runLabelNode` draws `ref('run', run_name, '#/run/<c>')`, ` / `, `ref('output', output, '#/items/<c>/<output>?where=%5B%5D')`.

- [ ] **Step 1: Write the failing tests**

In `web/test/views.test.mjs`, replace the test `'a query result row: label, file chips, pills linking to query 3 ...'` with:

```js
test('an item row: the item label linking to its page, its files, the pick carrying its collection, its address; no pills (decision 12)', async () => {
  installDom()
  const ex = rowModel()
  const ctx = rowCtx()
  const node = itemRows(ex, ctx, { items: ['i1', 'i2'], collectionCid: 'coll', completion: 'run1', output: 'aligned', where: [],
    previews: new Previews(ex), results: true })
  await frame()
  assert.deepEqual(node.querySelectorAll('[data-item-result]').map(li => li.dataset.itemResult), ['i1', 'i2'])
  const row = node.querySelector('[data-item-result=i1]')
  assert.equal(row.querySelector('.row-label').textContent, 'S-i1 · 2 · true')
  assert.equal(row.querySelector('.row-label').querySelector('a').getAttribute('href'), '#/item/coll/i1')
  assert.equal(row.querySelector('.row-head').querySelector('[data-icon]').dataset.icon, 'item')
  assert.deepEqual(row.querySelector('.row-files').querySelectorAll('[data-icon]').map(i => i.dataset.icon), ['file'])
  assert.equal(row.querySelector('.row-files').textContent, 'i1.bam')
  assert.equal(row.querySelector('[data-preview-for]'), null, 'no pills in a row')
  assert.equal(row.querySelector('[data-address]').dataset.address, 'i1')
  assert.equal(row.querySelector('[data-pick]').dataset.via, 'coll')
  const all = node.querySelector('[data-pick-all]')
  assert.equal(all.dataset.via, 'coll')
  assert.equal(all.textContent, 'Pick all 2')
})
```

and replace `'an item with no Meta Map has an empty pills container and is labelled by its files'` with:

```js
test('an item with no Meta Map is labelled by its files', async () => {
  installDom()
  const ex = { ...rowModel(), item: async () => ({ view: null, leaves: [{ name: 'A.bam' }] }) }
  const node = itemRows(ex, rowCtx(), { items: ['i1'], collectionCid: 'coll', previews: new Previews(ex) })
  await frame()
  assert.equal(node.querySelector('.row-label').textContent, 'A.bam')
})
```

In `web/test/pages.test.mjs`, add (it uses `selection` already imported there):

```js
test('a Selection row says "from" its run and output, and where it is held only when that is not here', async () => {
  installDom()
  const st = { names: ['africans'], nameClaims: [], current: [], nameConflicted: false, deletion: 'none', deletionClaims: [] }
  const ex = { selection: async () => ({ state: st, members: [{ kind: 'item', address: 'i1', via: ['coll'] }, { kind: 'item', address: 'i2', via: ['coll'] }],
    firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async (kind, a) => (a === 'i1' ? 'here' : 'elsewhere'),
    item: async (c, i) => ({ view: { firstName: i }, leaves: [{ name: `${i}.dat` }] }),
    runLabel: async () => ({ run_name: 'boring_yalow', output: 'people', completion: 'bafyrun' }) }
  const page = await selection(ex, 'bafysel', ctx())
  await frame()
  const [here, away] = page.querySelectorAll('[data-member]')
  assert.equal(here.dataset.held, 'here')
  assert.equal(here.querySelector('.row-action').textContent, '')
  assert.equal(away.querySelector('.row-action').textContent, 'not in this store')
  assert.match(here.querySelector('.row-via').textContent, /^ ?from boring_yalow \/ people/)
  assert.deepEqual(here.querySelector('.row-via').querySelectorAll('[data-icon]').map(i => i.dataset.icon), ['run', 'output'])
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd web && node --test test/views.test.mjs test/pages.test.mjs`
Expected: FAIL (the label is not a link, pills present, "held in this member" text, "via").

- [ ] **Step 3: Write the implementation**

`web/src/previews.js` line 63, record directory leaves:

```js
        (it) => { this.done.set(itemCid, { pairs: pairsOf(it.view), files: it.leaves.map(l => l.name).filter(Boolean),
          dirs: it.leaves.filter(l => l.name && CID.asCID(l.address)?.code === DAG_CBOR).map(l => l.name) }); this.fetched++ },
```

with `import { CID } from 'multiformats/cid'` and `import { DAG_CBOR } from './cid.js'` at the top.

`web/src/views/common.js`: add `import { address, icon, ref } from '../icons.js'` (merging with the `shortCid` import from Task 2). Replace `runLabelNode`'s `.then` body:

```js
  ex.runLabel(collectionCid).then((label) => {
    if (!label) return
    const c = label.completion
    node.replaceChildren(
      ref('run', label.run_name ?? 'unnamed run', c ? `#/run/${c}` : null), ' / ',
      ref('output', label.output, c ? `#/items/${c}/${encodeURIComponent(label.output)}?where=%5B%5D` : null))
  }, () => {})
```

Replace `itemRow` and `fillRow` (and their doc comment) with:

```js
/**
 * One item row (visual language spec §5.1): `lead` (a checkbox), the item
 * icon and its label, which links to the item page once the preview
 * arrives, `action` at the right; beneath, its files, a from line when
 * `fromLabel` is set (each via named by its run and output, `perVia(via)`
 * after it), and the item's address. The preview is drawn by `watchRows`; a
 * row at `index` past `previews.cap` waits for "show details".
 */
export function itemRow(ex, ctx, { address: itemCid, via = [], previews, target = null, index = 0, attrs = {}, lead = null, action = null,
  fromLabel = null, noVia = 'a query', perVia = () => null }) {
  const collection = via[0] ?? null
  const row = { address: itemCid, collection, target, drawn: undefined, href: `#/item/${collection ?? '-'}/${itemCid}`,
    label: h('strong', { class: 'row-label' }), files: h('span', { class: 'row-files' }) }
  const loading = () => h('span', { class: 'muted' }, 'loading...')
  row.label.append(index < previews.cap ? loading()
    : h('button', { type: 'button', onclick: () => { row.label.replaceChildren(loading()); previews.ask(collection, itemCid) } }, 'show details'))
  const fromLine = fromLabel === null ? null : h('span', { class: 'row-via muted' }, ` ${fromLabel} `,
    via.length ? via.map((v, i) => [i ? '; ' : null, runLabelNode(ex, v), ' ', perVia(v)]) : [noVia, ' ', perVia('-')])
  const li = h('li', { class: 'row', ...attrs },
    h('div', { class: 'row-head' }, lead, icon('item'), row.label, h('span', { class: 'row-action' }, action)),
    h('div', { class: 'row-sub' }, row.files, fromLine, ' ', address(itemCid)))
  rowOf.set(li, row)
  return li
}

export function fillRow(row, preview, paths) {
  if (preview.error) {
    row.label.replaceChildren(h('span', { class: 'muted', title: preview.message ?? '' },
      preview.error === 'block_missing' ? 'not in this store' : `no preview (${preview.error})`))
    return
  }
  row.label.replaceChildren(link(row.href, labelText(preview, paths) || h('span', { class: 'muted' }, 'no Meta Map')))
  const dirs = new Set(preview.dirs ?? [])
  row.files.replaceChildren(...preview.files.flatMap((f, i) => [i ? ' ' : null, ref(dirs.has(f) ? 'directory' : 'file', h('code', {}, f))]))
}
```

In `watchRows`, change the `fillRow(...)` call to drop the fourth argument (`fillRow(row, preview, paths)`), and remove the now-unused `duplicates` computation and the `pairsNode`/`withoutDuplicates` imports if nothing else in `common.js` uses them (`/usr/bin/grep -n "pairsNode\|withoutDuplicates\|itemTable" web/src/views/common.js`).

`web/src/views/selection.js`: in the `ex.held(...)` success handler:

```js
      (state) => {
        node.dataset.held = state
        held.textContent = state === 'here' ? '' : 'not in this store'
        held.classList.remove('muted')
      },
```

change the placeholder `h('span', { 'data-held-for': m.address, class: 'muted' }, '...')` to `h('span', { 'data-held-for': m.address, class: 'muted' })` (no "..." text), and in the `itemRow(...)` call replace `viaLabel: 'via'` with `fromLabel: 'from'`. Give the per-via Copy button `class: 'quiet'`:

```js
  const copy = (text) => h('button', { type: 'button', class: 'quiet', title: `Copy ${text}`, onclick: async (event) => {
    event.currentTarget.textContent = await copyOutcome(navigator.clipboard, text)
  } }, 'Copy')
```

`web/src/styles.js`: replace the rule `.row-pairs, .row-via, .row-cid { margin: .15rem 0 0 1.6rem; } .row-pairs:empty { display: none; }` with:

```js
  .row-sub { margin: .15rem 0 0 1.6rem; color: var(--muted); } .row-sub code { color: var(--fg); }
  button.quiet { font-size: 12px; padding: 0 .35rem; }
```

(`--fg` is the page's text token; check `:root` in `styles.js` and use its name if it differs.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd web && npm test`
Expected: PASS. Fix any other test that read `.row-cid`, `.chip` in a row, or `[data-preview-for]` on a row (`/usr/bin/grep -n "row-cid\|data-preview-for" web/test/*.mjs`); the items table keeps its own `[data-preview-for]` on `tr`.

- [ ] **Step 5: Commit**

```bash
git add web/src web/test
git commit -s -m "feat(web): item rows in two lines, from their run, without pills" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The Selection page

**Files:**
- Modify: `web/src/views/selection.js` (heading, names line, Items count, nested Selection row, details)
- Modify: `web/src/app.js` (the read-only reason text)
- Test: `web/test/pages.test.mjs`

**Interfaces:**
- Consumes: `icon`, `ref`, `address` (Tasks 1-2), `whenText` (`words.js`), `ex.selection(cid) -> { state, members, firstSeen, block }`.
- Produces: the Selection view keeps `[data-selection-view]`, `[data-deletion]`, `[data-name]`/`[data-claim]`/`[data-conflicted]` per current name, `[data-member]`/`[data-kind]`/`[data-held]` per row, `[data-unavailable]`.

- [ ] **Step 1: Write the failing test** (append to `web/test/pages.test.mjs`)

```js
test('the Selection page: icon and name, one quiet line naming who named it and when, "Items", a nested Selection by its name', async () => {
  installDom()
  const claim = { cid: 'bafyname', verb: 'set', value: 'africans', asserted_by: 'anonymous', timestamp: '2026-10-14T20:07:44.417Z', conflicted: false }
  const st = { names: ['africans'], nameClaims: ['bafyname'], current: [claim], nameConflicted: false, deletion: 'none', deletionClaims: [] }
  const ex = {
    selection: async (cid) => (cid === 'bafysel'
      ? { state: st, members: [{ kind: 'item', address: 'i1', via: ['coll'] }, { kind: 'selection', address: 'bafyinner' }], firstSeen: '2026-10-14T20:07:44.412Z', block: { asserted_by: 'anonymous' } }
      : { state: { ...st, names: ['lagos'] }, members: [], firstSeen: null, block: { asserted_by: 'x' } }),
    held: async () => 'here', item: async () => ({ view: { id: 'A' }, leaves: [] }), runLabel: async () => null,
  }
  const page = await selection(ex, 'bafysel', ctx())
  await frame()
  const h1 = page.querySelector('h1')
  assert.equal(h1.querySelector('[data-icon]').dataset.icon, 'selection')
  assert.equal(h1.textContent, 'africans')
  const name = page.querySelector('[data-name]')
  assert.equal(name.dataset.claim, 'bafyname')
  assert.match(name.textContent, /named by anonymous · /)
  assert.equal(name.querySelectorAll('[title]').find(e => !e.dataset.icon).getAttribute('title'), '2026-10-14T20:07:44.417Z')
  assert.equal(page.querySelector('.selection-meta').querySelector('[data-address]').dataset.address, 'bafysel')
  assert.ok(page.querySelectorAll('h2').some(e => e.textContent === 'Items and Selections (2)'))
  const nested = page.querySelector('[data-member=bafyinner]')
  assert.equal(nested.querySelector('[data-icon]').dataset.icon, 'selection')
  assert.match(nested.textContent, /lagos/)
  assert.equal(nested.querySelector('[data-address]').dataset.address, 'bafyinner')
  assert.ok(page.querySelector('[data-unavailable]'))
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && node --test test/pages.test.mjs`
Expected: FAIL (no icon in the heading, "Members (2)").

- [ ] **Step 3: Write the implementation**

In `web/src/views/selection.js` add `import { address, icon, ref } from '../icons.js'` and `import { whenText } from '../words.js'`, and a helper:

```js
const when = (iso) => (iso ? h('span', { title: iso }, whenText(iso)) : 'an unknown time')
```

Replace the nested Selection row (the `m.kind === 'selection'` branch):

```js
    const node = m.kind === 'selection'
      ? nestedRow(ex, m, held)
      : itemRow(ex, ctx, { address: m.address, via: m.via, previews, index: itemIndex++, attrs: { 'data-member': m.address, 'data-kind': m.kind },
        action: held, fromLabel: 'from', noVia: 'no run (picked by a query across runs)', perVia: (v) => copy(copyText(m.address, v)) })
```

with, outside `selection()`:

```js
/** A nested Selection as a row: its icon and name (read lazily), where it is held, its address. */
function nestedRow(ex, m, held) {
  const name = h('span', {}, 'Selection')
  ex.selection(m.address).then((s) => { name.textContent = s.state.names.length ? s.state.names.join(' / ') : 'unnamed Selection' }, () => {})
  return h('li', { class: 'row', 'data-member': m.address, 'data-kind': m.kind },
    h('div', { class: 'row-head' }, ref('selection', name, `#/selection/${m.address}`), h('span', { class: 'row-action' }, held)),
    h('div', { class: 'row-sub' }, address(m.address)))
}
```

Replace the heading through the details fold and the members heading in the returned `section`:

```js
    h('h1', {}, icon('selection'), st.names.length ? st.names.join(' / ') : 'Unnamed Selection'),
    st.nameConflicted ? h('p', { class: 'warn' }, 'More than one name is current. Renaming supersedes them all.') : null,
    h('p', { class: 'selection-meta muted' },
      st.nameClaims.map(c => byCid.get(c)).filter(c => c.verb === 'set').map((c, i) => h('span', {
        'data-name': c.value, 'data-claim': c.cid, 'data-conflicted': c.conflicted ? '' : null },
      i ? ' · ' : null, icon('claim'), st.names.length > 1 ? `${c.value}: ` : '', `named by ${c.asserted_by ?? 'unknown'} · `, when(c.timestamp))),
      ' ', address(selectionCid)),
    st.deletion === 'none' ? null : h('div', { class: 'warn' },
      h('p', {}, st.deletion === 'deleted' ? 'Deleted: hidden from the list of Selections until undone.'
        : 'Deletion in conflict: these Claims are all current, so it stays visible.'),
      h('ul', {}, st.deletionClaims.map(c => byCid.get(c)).map(c => h('li', { 'data-deletion-claim': c.cid, 'data-verb': c.verb },
        `${c.verb} by ${c.asserted_by ?? 'unknown'}, `, when(c.timestamp))))),
    fold(`details:${selectionCid}`, 'Details',
      h('dl', {}, h('dt', {}, 'first seen in this store'), h('dd', {}, s.firstSeen ? when(s.firstSeen) : 'unknown'),
        h('dt', {}, 'assembled by'), h('dd', {}, s.block.asserted_by))),
    actions(selectionCid, st, ctx, status),
    status,
    h('h2', {}, `${s.members.some(m => m.kind === 'selection') ? 'Items and Selections' : 'Items'} (${s.members.length})`),
    memberList)
```

In `web/src/app.js`, change the first `reason` string:

```js
    const reason = !store.members ? 'Read only here: writing needs this page opened through `nextflow plugin nf-blocks:explore`.'
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd web && npm test`
Expected: PASS. A test that matched the old "Members (" heading or the `ul` of names changes with it (`/usr/bin/grep -n "Members (\|named by" web/test/*.mjs`).

- [ ] **Step 5: Commit**

```bash
git add web/src/views/selection.js web/src/app.js web/test/pages.test.mjs
git commit -s -m "feat(web): the Selection page leads with its name and lists Items" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Icons on the other pages

**Files:**
- Modify: `web/src/views/run.js`, `web/src/views/items.js`, `web/src/views/item.js`, `web/src/views/home.js`, `web/src/views/pipeline.js`, `web/src/views/collection.js`, `web/src/nav.js`
- Test: `web/test/run-view.test.mjs`, `web/test/pages.test.mjs`, `web/test/items-view.test.mjs`, `web/test/nav.test.mjs`

**Interfaces:**
- Consumes: `icon`, `ref`, `address` (Tasks 1-2), `runLabelNode` (Task 5).

- [ ] **Step 1: Write the failing tests**

Append to `web/test/run-view.test.mjs`:

```js
test('the run page names its kinds: the run icon in the heading, the output icon per output', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx())
  await frame()
  assert.equal(page.querySelector('h1').querySelector('[data-icon]').dataset.icon, 'run')
  for (const row of page.querySelectorAll('[data-collection]')) assert.equal(row.querySelector('[data-icon]').dataset.icon, 'output')
})
```

Append to `web/test/pages.test.mjs`:

```js
test('home, pipeline and item pages name their kinds with icons', async () => {
  installDom()
  const homeEx = { pipelines: async () => [{ pipeline: 'p', runs: 2, latest: '2026-09-30T21:10:38.345Z' }], stale: [],
    latestSuccessfulRun: async () => 'bafyrun', runIdentity: async () => ({ run_name: 'high_jang' }) }
  const homePage = await home(homeEx, ctx())
  await frame()
  assert.deepEqual(homePage.querySelectorAll('[data-icon]').map(i => i.dataset.icon), ['pipeline', 'run'])
  const pipeEx = { runPage: async () => ({ rows: [{ completion_cid: 'bafyrun', pipeline: 'p', run_name: 'high_jang', status: 'succeeded', possibly_incomplete: 0,
    finished_at: '2026-09-30T21:10:38.345Z', source: 'snapshot' }], hidden: 0, first: 1, last: 1, total: 1, prev: null, next: null }),
    completionOf: async () => ({ anomalies: {} }), stale: [] }
  const pipePage = await pipeline(pipeEx, 'p', 0, ctx())
  assert.equal(pipePage.querySelector('h1').querySelector('[data-icon]').dataset.icon, 'pipeline')
  assert.equal(pipePage.querySelector('[data-run]').querySelector('[data-icon]').dataset.icon, 'run')
})
```

and, in the existing item page test in `pages.test.mjs` (the one asserting `h1` is `'WT_REP1'`), add after its first assertion:

```js
  assert.equal(page.querySelector('h1').querySelector('[data-icon]').dataset.icon, 'item')
  assert.ok(page.querySelectorAll('td').some(td => td.querySelector('[data-icon=file]')))
```

and change `assert.equal(page.querySelector('h1').textContent, 'WT_REP1')` only if it fails because of the icon (an icon span adds no text, so it should not).

Append to `web/test/items-view.test.mjs` a check inside its first test (after rows are drawn): `assert.ok(rows[0].querySelector('[data-address]'))`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd web && node --test test/run-view.test.mjs test/pages.test.mjs test/items-view.test.mjs`
Expected: FAIL (no icons, no address in items rows).

- [ ] **Step 3: Write the implementation**

`web/src/views/run.js`: import `{ icon }` from `../icons.js`; heading `h('h1', {}, icon('run'), row.run_name ?? 'run')`; the output cell `h('td', {}, icon('output'), link(...))`.

`web/src/views/items.js`: import `{ address, icon }` from `../icons.js`; in `drawRow`, the files cell gets `icon('file')` before the chips when there are any, and each actions cell becomes `h('td', {}, open, ' ', address(r.address), ' ', pick)` (all three `replaceChildren` branches).

`web/src/views/item.js`: import `{ address, icon, ref, shortCid }` from `../icons.js` (Task 2 added some), `{ CID }` from `multiformats/cid` and `{ DAG_CBOR }` from `../cid.js`; heading `h('h1', {}, icon('item'), title)`; the files table name cell:

```js
      h('td', {}, ref(CID.asCID(l.address)?.code === DAG_CBOR ? 'directory' : 'file', h('code', {}, l.name ?? ''))),
```

and in "Produced by" the first run link becomes `from.completion ? ref('run', from.run_name ?? 'this run', `#/run/${from.completion}`) : from.run_name ?? 'this run'`.

`web/src/views/home.js`: import `{ ref }`; the pipeline cell `h('td', {}, ref('pipeline', p.pipeline, `#/pipeline/${enc(p.pipeline)}`))`; the latest good run `good.replaceChildren(ref('run', id?.run_name ?? 'unnamed run', `#/run/${best}`))`.

`web/src/views/pipeline.js`: import `{ icon, ref, shortCid }`; heading `h('h1', {}, icon('pipeline'), name)`; run cell `h('td', {}, ref('run', r.run_name ?? r.completion_cid, `#/run/${r.completion_cid}`))`.

`web/src/views/collection.js`: heading `h('h1', {}, icon('output'), c.output)`; the content page heading `h('h1', {}, icon('file'), 'Every run that produced this file')`.

`web/src/nav.js`: import `{ icon }` from `./icons.js`; in the pipeline toggle button, after the arrow: `open.has(p.pipeline) ? '▾ ' : '▸ ', icon('pipeline'), p.pipeline, ...`; each Selection link: `h('a', { href: ..., 'data-nav-selection': s.cid }, icon('selection'), s.state?.names?.length ? ... : 'unnamed')`. Runs keep their status dot, which already marks a run.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd web && npm test`
Expected: PASS. A test asserting exact text such as a nav label still passes, since an icon span has no text.

- [ ] **Step 5: Commit**

```bash
git add web/src web/test
git commit -s -m "feat(web): runs, outputs, items, files, pipelines and Selections carry their icons" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Gate tier A check A8

**Files:**
- Modify: `gate/browser/drive.mjs` (`EXTRACT` gains `block`, `blockCheck`, `addresses`)
- Modify: `gate/browser_assert.py` (`prepare` steps and `expected["walk"]`; `block_walk`; `a8`)
- Modify: `gate/test_browser_assert.py` (`BlockWalkTest`)
- Modify: `gate/README.md` (tier A list)
- Modify: `docs/superpowers/specs/2026-10-14-explorer-visual-language-design.md` (A6 becomes A8; `data-block-check` adds `missing`)

**Interfaces:**
- Consumes: `[data-block]`, `[data-block-check]`, `[data-address]` (Tasks 2, 4); the collection page's rows carrying each item's address (Task 5).
- Produces: `block_walk(walk, collection, item, leaf) -> list[str]` (problems; empty means pass); `BLOCK_PAGE_RAW_CAP = 1048576`.

- [ ] **Step 1: Write the failing test** (append to `gate/test_browser_assert.py`)

```python
class BlockWalkTest(unittest.TestCase):
    ITEM = "bafyreiitem" + "a" * 48
    LEAF = "bafkreileaf" + "b" * 48
    WALK = {"collection": "bafyreicoll", "items": [ITEM], "item": ITEM, "leaf": LEAF, "leaf_size": 37, "leaf_hashes": True}

    def pages(self, **kw):
        collection = step(addresses=[self.ITEM])
        item = step(block=self.ITEM, blockCheck="verified", addresses=[self.LEAF])
        leaf = step(block=self.LEAF, blockCheck="verified", addresses=[])
        for key, value in kw.items():
            {"collection": collection, "item": item, "leaf": leaf}[key.split("_")[0]][key.split("_", 1)[1]] = value
        return collection, item, leaf

    def test_a_walk_that_reaches_both_blocks_passes(self):
        self.assertEqual(B.block_walk(self.WALK, *self.pages()), [])

    def test_a_collection_page_without_the_item_address_fails(self):
        problems = B.block_walk(self.WALK, *self.pages(collection_addresses=[]))
        self.assertTrue(any("links no address for item" in p for p in problems))

    def test_an_unverified_item_block_fails(self):
        problems = B.block_walk(self.WALK, *self.pages(item_blockCheck="refused"))
        self.assertTrue(any("item's block page" in p for p in problems))

    def test_an_item_page_without_the_leaf_link_fails(self):
        problems = B.block_walk(self.WALK, *self.pages(item_addresses=[]))
        self.assertTrue(any("for its file" in p for p in problems))

    def test_a_large_leaf_is_expected_unchecked(self):
        walk = dict(self.WALK, leaf_size=B.BLOCK_PAGE_RAW_CAP + 1)
        self.assertTrue(B.block_walk(walk, *self.pages()))
        self.assertEqual(B.block_walk(walk, *self.pages(leaf_blockCheck="unchecked")), [])

    def test_bytes_that_do_not_hash_to_the_leaf_fail(self):
        problems = B.block_walk(dict(self.WALK, leaf_hashes=False), *self.pages())
        self.assertTrue(any("do not hash" in p for p in problems))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest gate.test_browser_assert -v 2>&1 | tail -5` (from the repo root; or `cd gate && python3 -m unittest test_browser_assert`)
Expected: FAIL, `module 'browser_assert' has no attribute 'block_walk'`.

- [ ] **Step 3: Write the implementation**

`gate/browser/drive.mjs`, in `EXTRACT` add:

```js
    block: document.querySelector('[data-block]')?.dataset.block ?? null,
    blockCheck: document.querySelector('[data-block]')?.dataset.blockCheck ?? null,
    addresses: [...document.querySelectorAll('[data-address]')].map((e) => e.dataset.address),
```

`gate/browser_assert.py`, near the other constants:

```python
# The block page reads a raw block whole, and hash-checks it, up to this size
# (web/src/config.js BLOCK_PAGE_RAW_CAP, visual language spec section 4.3).
BLOCK_PAGE_RAW_CAP = 1024 * 1024


def block_walk(walk, collection, item, leaf):
    """Problems with A8's walk: a collection page links each item's address,
    the item's block page is verified and links its file's address, and the
    file's block page shows the check its size calls for."""
    problems = []
    missing = sorted(set(walk["items"]) - set(collection.get("addresses") or []))
    if missing:
        problems.append("the collection page links no address for item(s) %s" % [m[:16] for m in missing])
    if item.get("block") != walk["item"] or item.get("blockCheck") != "verified":
        problems.append("the item's block page shows %s (check %s), expected %s, verified"
                        % (item.get("block"), item.get("blockCheck"), walk["item"][:16]))
    if walk["leaf"] not in (item.get("addresses") or []):
        problems.append("the item's block page links no address %s for its file" % walk["leaf"][:16])
    want = "verified" if walk["leaf_size"] <= BLOCK_PAGE_RAW_CAP else "unchecked"
    if leaf.get("block") != walk["leaf"] or leaf.get("blockCheck") != want:
        problems.append("the file's block page shows %s (check %s), expected %s, %s"
                        % (leaf.get("block"), leaf.get("blockCheck"), walk["leaf"][:16], want))
    if not walk["leaf_hashes"]:
        problems.append("the file block's bytes do not hash to %s by the Gate's own reckoning" % walk["leaf"][:16])
    return problems
```

In `prepare(root)`, after `content = cas.cid_of_file(bams[0])`:

```python
    coll_cid, coll_block = cold.collections(gate)["aligned"]
    item_cids = sorted(link.text for link in coll_block.get("items") or [] if link is not None)
    walk_item = item_cids[0]
    walk_leaf = next(A._address_text(l.get("address")) for l in A._leaves((gate.block(walk_item) or {}).get("value"))
                     if A._address_text(l.get("address")))
    leaf_path = os.path.join(store, "blocks", walk_leaf[-2:], walk_leaf)
```

add to `expected`:

```python
        "walk": {"collection": coll_cid, "items": item_cids, "item": walk_item, "leaf": walk_leaf,
                 "leaf_size": os.path.getsize(leaf_path), "leaf_hashes": cas.cid_of_file(leaf_path) == walk_leaf},
```

and to `steps`, after the A5 steps:

```python
        {"id": "A8.collection", "server": "range", "path": "stores/current/index.html", "hash": "#/collection/%s" % coll_cid},
        {"id": "A8.item", "server": "range", "path": "stores/current/index.html", "hash": "#/block/%s" % walk_item},
        {"id": "A8.file", "server": "range", "path": "stores/current/index.html", "hash": "#/block/%s" % walk_leaf},
```

In `check(root)`, add after `a5`:

```python
    def a8():
        problems = block_walk(expected["walk"], seen("A8.collection"), seen("A8.item"), seen("A8.file"))
        return (FAIL, "; ".join(problems)) if problems else (PASS, "the collection page links every item's address; the item's block "
                                                                   "page is hash-checked and links its file, whose block page is too")
```

and after `run(5, ...)`:

```python
    run(8, "an address walks to its block, and on to the blocks it names", a8)
```

`gate/README.md`: in the tier A list, add a line for A8 in the form the list uses, e.g. `A8 an item's address opens its block page, hash-checked, and its file's address opens the file's block page`.

The spec: change "Gate browser tier A gains A6" to "A8" (twice: §7 and §8), and in §4.3 and §7 list `data-block-check` values as `verified`, `unchecked`, `refused` or `missing`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd gate && python3 -m unittest test_browser_assert -v 2>&1 | tail -3 && python3 -m unittest discover -s . 2>&1 | tail -3`
Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add gate docs/superpowers/specs/2026-10-14-explorer-visual-language-design.md
git commit -s -m "gate: A8, an address walks to its block and on to the file it names" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: DESIGN.md and the whole check

**Files:**
- Modify: `DESIGN.md` (new section under §16's layout B, the route list near line 1683, the §15 selector table near line 1743)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the DESIGN.md text**

After the "Stable references (2026-10-14)" subsection, add:

```markdown
### Visual language and the block page (2026-10-14)

Spec `docs/superpowers/specs/2026-10-14-explorer-visual-language-design.md`,
plan `docs/superpowers/plans/2026-10-14-explorer-visual-language.md`.

- One grey icon per kind (`web/src/icons.js`): pipeline, run, output, item,
  file, directory, Selection, Claim, address, store. An icon is a span
  painted by a CSS mask (`ICON_CSS`), so the test DOM needs no SVG.
  `ref(kind, label, href)` names a thing with its icon.
- `address(cid)` (in `icons.js`, since it draws an icon; the spec placed it
  in `html.js`) replaces `cid()`: a quiet link to `#/block/<cid>`.
- `#/block/<cid>` shows any block: its CID decoded, the browser's hash
  check, its kind with a link to its friendly page, and its contents as
  DAG-JSON with each link an address. A raw block over 1 MiB is previewed by
  Range and left unchecked; `BlockFetcher.partial` never records it as
  verified, so A4 holds. A RunManifest links to its run only when the Store
  Log tail names the run: the snapshot has no query by manifest, and adding
  one would change `Index.groovy`'s mirror.
- Item rows are two lines: the item and its label, then its files, "from"
  its run and output, and its address. Pills are on the item page and the
  items table only. A row says where it is held only when that is not here
  ("not in this store"; the page knows only here or elsewhere, not which
  other store).
- The Selection page leads with its name, one quiet line of who named it
  and when and its address, and lists "Items".
- Gate tier A check A8 walks from a collection page to an item's block and
  on to its file's block.
```

In the route list (the bullet starting "Routes (location hash):"), add `` `#/block/<cid>` (any block by its address) `` after `` `#/content/<cid>` (query 1) ``.

In the §15 selector table, add rows:

```markdown
| `[data-block]` | the block page; its value the CID (2026-10-14) |
| `[data-block-check]` | on the block page: `verified`, `unchecked` (a raw block over 1 MiB), `refused` (hash mismatch) or `missing` |
| `[data-address]` | an address link to `#/block/<cid>`; its value the CID |
| `[data-icon]` | a kind's icon; its value the kind |
```

- [ ] **Step 2: Run every check**

```bash
cd web && npm test
cd .. && ./gradlew check
GATE_ROOT=$(mktemp -d) make gate
```

Expected: web tests pass; `./gradlew check` passes; `make gate` prints lineage 18 PASS, 0 FAIL, 5 SKIP, `browser tier A (local): 6 PASS, 0 FAIL` (A1 to A5 and A8), tier B 14/14. A1 has a known flake (the `resumed` producer); rerun `make gate` once if A1 alone fails.

- [ ] **Step 3: Look at it against a real store**

```bash
cd web && node build.mjs
python3 ../gate/browser/serve.py /tmp/cas-exploration/store 8711 &
python3 ../gate/browser/serve.py dist 8712 &
```

Open `http://127.0.0.1:8712/?store=http://127.0.0.1:8711/` and check: a Selection page (two-line rows, "Items"), a run page (icons), an item's address opening its block page with "Checked" and a walkable link to its file block.

- [ ] **Step 4: Commit**

```bash
git add DESIGN.md
git commit -s -m "docs(design): visual language and the block page" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
