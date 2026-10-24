// web/test/items-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { items } from '../src/views/items.js'
import { rawCid } from './fixture.mjs'

const view = (id, singleEnd) => ({ id, single_end: singleEnd, strandedness: 'reverse' })
const VIEWS = { i1: view('WT_REP1', false), i2: view('WT_REP2', false), i3: view('RAP1_REP1', true) }
const leaf = (name, address, size = 10, reason = null) => ({ kind: 'Leaf', name, address, size, reason })
const BAM = rawCid('bam')
// A markdup-shaped record; i3 has no bai, json_info is a declined optional output on every item, and
// i1's samtools.idxstats is declined while its stats and flagstat are stored.
const valueOf = (i) => {
  const id = VIEWS[i].id
  const value = { id, bam: leaf(`${id}.markdup.sorted.bam`, BAM, 2048), bai: leaf(`${id}.markdup.sorted.bam.bai`, rawCid(`bai-${i}`)),
    samtools: { stats: leaf(`${id}.stats`, rawCid(`stats-${i}`)), flagstat: leaf(`${id}.flagstat`, rawCid(`flagstat-${i}`)) },
    json_info: leaf(null, null, null, 'declined') }
  if (i === 'i3') delete value.bai
  if (i === 'i1') value.samtools.idxstats = leaf(null, null, null, 'declined')
  return value
}

// Every snapshot-backed method throws: the items view and its panel input may
// use only Explorer.items, block reads and runIdentity (Review Focus 1, plan P1/P2).
function fakeEx() {
  const forbidden = (name) => () => { throw new Error(`${name} reads the snapshot during query 3`) }
  return {
    items: async (completion, output, where) => ({ items: where.length ? ['i1', 'i2'] : ['i1', 'i2', 'i3'], collection: 'coll' }),
    item: async (c, i) => ({ view: VIEWS[i], leaves: [{ name: `${VIEWS[i].id}.markdup.sorted.bam` }, { name: `${VIEWS[i].id}.markdup.sorted.bam.bai` }], value: valueOf(i) }),
    runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'high_jang', lid: 'lid://x' }),
    runLabel: forbidden('runLabel'), runRow: forbidden('runRow'), runPage: forbidden('runPage'), pipelines: forbidden('pipelines'),
    collection: forbidden('collection'), latestSuccessfulRun: forbidden('latestSuccessfulRun'), allItems: forbidden('allItems'),
    producersOf: forbidden('producersOf'), selectionPage: forbidden('selectionPage'),
  }
}
function ctx() {
  const c = { tray: new Tray(null), sources: new Map(), used: [], marked: [], rerender: () => {}, write: { available: false, hrefFor: (h) => h } }
  c.trayChanged = () => {}
  c.use = (v) => c.used.push({ ...v })
  c.mark = (m) => c.marked.push(m)
  return c
}

test('rows, columns from the loaded previews, the constant line; no snapshot read but the query (Review Focus 1)', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await frame(); await frame()
  const rows = page.querySelectorAll('[data-item-result]')
  assert.deepEqual(rows.map(r => r.dataset.itemResult), ['i1', 'i2', 'i3'])
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.columns), ['id', 'single_end'])
  assert.match(page.querySelector('.constant').textContent, /Same for every item: strandedness reverse/)
  assert.equal(rows[0].querySelector('[data-pick]').dataset.pick, 'i1')
  assert.equal(rows[0].querySelector('[data-pick]').dataset.via, 'coll')
  assert.ok(rows[0].querySelector('[data-address]'))
  assert.equal(c.used.at(-1).count, 3)
  assert.deepEqual(c.marked, [{ run: 'bafyrun' }])
})

test('one column per file key; pills link to the block page with no download', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.deepEqual(JSON.parse(page.querySelector('table').dataset.fileColumns), ['bai', 'bam', 'samtools'])
  assert.deepEqual(page.querySelectorAll('th[data-file-column]').map(t => t.textContent), ['bai', 'bam', 'samtools'])
  const row = page.querySelectorAll('[data-item-result]')[0]
  const bam = row.querySelector('a[data-pill="bam"]')
  assert.equal(bam.getAttribute('href'), `#/block/${BAM.toString()}`)
  assert.equal(bam.getAttribute('download'), null)
  assert.equal(bam.getAttribute('title'), null)
  assert.equal(bam.getAttribute('aria-label'), 'bam\nWT_REP1.markdup.sorted.bam\n2.0 KB')
  assert.ok(bam.querySelector('[data-icon="file"]'))
  assert.deepEqual(row.querySelector('[data-file-cell="samtools"]').querySelectorAll('a[data-pill]').map(p => p.dataset.pill),
    ['samtools.stats', 'samtools.flagstat'])
})

test('a missing key is a dash; a cell\'s address-less slots are one dashed pill after the stored ones, not a link', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const rows = page.querySelectorAll('[data-item-result]')
  assert.equal(rows[2].querySelector('[data-file-cell="bai"]').textContent, '—')
  const cell = rows[0].querySelector('[data-file-cell="samtools"]')
  const empty = cell.querySelector('[data-pill-empty]')
  assert.equal(empty.tagName, 'SPAN')
  assert.equal(empty.dataset.pillEmpty, '1')
  assert.equal(empty.textContent, '1')
  assert.equal(empty.getAttribute('title'), null)
  assert.equal(empty.getAttribute('aria-label'), '1 without a stored file\nsamtools.idxstats: no file (an optional output was empty)')
  assert.equal(cell.querySelectorAll('[data-pill-empty]').length, 1)
  assert.equal(rows[1].querySelector('[data-file-cell="samtools"]').querySelector('[data-pill-empty]'), null)
})

test('keys with no stored file in any item get no column; one line above the table names them', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.equal(page.querySelector('th[data-file-column="json_info"]'), null)
  assert.equal(page.querySelector('[data-empty-columns]').textContent, 'No stored file in any item: json_info')
})

test('more than five slots shows five and +N to the item page, "-" when there is no collection (Review Focus 5)', async () => {
  installDom()
  const ex = fakeEx()
  ex.items = async () => ({ items: ['i1'], collection: null })
  ex.item = async () => ({ view: { id: 'S' }, leaves: [], value: { id: 'S', mqc: [leaf(null, null, null, 'declined'),
    ...[...Array(9)].map((_, i) => leaf(`f${i}`, rawCid(`f${i}`))), leaf(null, null, null, 'declined')] } })
  const page = await items(ex, 'bafyrun', 'bam_qc', '[]', ctx())
  await frame(); await frame()
  const cell = page.querySelector('[data-file-cell="mqc"]')
  assert.deepEqual(cell.querySelectorAll('a[data-pill]').map(p => p.dataset.pill), ['mqc[1]', 'mqc[2]', 'mqc[3]', 'mqc[4]', 'mqc[5]'])
  assert.equal(cell.querySelector('.file-pill-more').textContent, '+4')
  assert.equal(cell.querySelector('[data-pill-empty]').dataset.pillEmpty, '2')
  assert.equal(cell.querySelector('.file-pill-more').getAttribute('href'), '#/item/-/i1')
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
  assert.equal(failed.querySelectorAll('td')[2].getAttribute('colspan'), String(columns + files))
  assert.ok(failed.querySelectorAll('td')[1].querySelector('[data-pick]'))
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

test('the items view records which run and output its collection is, for the panel', async () => {
  installDom()
  const c = ctx()
  await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  assert.deepEqual(c.sources.get('coll'), { completion: 'bafyrun', output: 'markdup' })
})

test('the row actions, then the file columns, come first, after the checkbox and before the metadata columns', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.deepEqual(page.querySelectorAll('th').map(t => t.dataset.fileColumn ?? t.textContent), ['', '', 'bai', 'bam', 'samtools', 'id', 'single_end'])
  const cells = page.querySelectorAll('[data-item-result]')[0].querySelectorAll('td')
  assert.equal(cells.length, 7)
  assert.ok(cells[1].querySelector('[data-pick]') && cells[1].querySelector('[data-open-item]'))
  assert.deepEqual(cells.slice(2, 5).map(td => td.dataset.fileCell), ['bai', 'bam', 'samtools'])
  assert.equal(cells[5].textContent, 'WT_REP1')
})

test('a "+" pill picks the item with its collection and then shows it is picked; a braces pill opens the item page', async () => {
  installDom()
  const c = ctx()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await frame(); await frame()
  const tip = page.querySelector('[data-pill-tip]')
  const row = page.querySelectorAll('[data-item-result]')[0]
  const pick = row.querySelector('button[data-pick]')
  assert.equal(pick.dataset.pick, 'i1')
  assert.equal(pick.dataset.via, 'coll')
  assert.equal(pick.textContent, '+')
  assert.equal(pick.getAttribute('aria-label'), 'Add to picked')
  await pick.fire('click')
  assert.deepEqual(c.tray.entries().map(e => [e.address, e.via]), [['i1', ['coll']]])
  assert.equal(pick.disabled, true)
  assert.equal(pick.textContent, '✓')
  await pick.fire('mouseenter')
  assert.deepEqual(tip.querySelectorAll('div').map(d => d.textContent), ['Picked'])
  const open = row.querySelector('a[data-open-item]')
  assert.equal(open.getAttribute('href'), '#/item/coll/i1')
  assert.equal(open.dataset.address, 'i1')
  await open.fire('mouseenter')
  assert.deepEqual(tip.querySelectorAll('div').map(d => d.textContent), ['Open item', 'i1'])
})

test('an item already in the tray draws its "+" pill as picked', async () => {
  installDom()
  const c = ctx()
  c.tray.add({ address: 'i2', via: ['coll'], kind: 'item' })
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', c)
  await frame(); await frame()
  const pick = page.querySelectorAll('[data-item-result]')[1].querySelector('button[data-pick]')
  assert.equal(pick.disabled, true)
  assert.equal(pick.textContent, '✓')
  assert.equal(pick.getAttribute('aria-label'), 'Picked')
})

test('hovering or focusing a pill opens the page\'s own tooltip at once; leaving closes it', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const tip = page.querySelector('[data-pill-tip]')
  assert.equal(tip.dataset.open, 'false')
  const rows = page.querySelectorAll('[data-item-result]')
  const bam = rows[0].querySelector('a[data-pill="bam"]')
  await bam.fire('mouseenter')
  assert.equal(tip.dataset.open, 'true')
  assert.deepEqual(tip.querySelectorAll('div').map(d => d.textContent), ['bam', 'WT_REP1.markdup.sorted.bam', '2.0 KB'])
  await bam.fire('mouseleave')
  assert.equal(tip.dataset.open, 'false')
  const empty = rows[0].querySelector('[data-pill-empty]')
  await empty.fire('focus')
  assert.equal(tip.dataset.open, 'true')
  assert.deepEqual(tip.querySelectorAll('div').map(d => d.textContent),
    ['1 without a stored file', 'samtools.idxstats: no file (an optional output was empty)'])
  await empty.fire('blur')
  assert.equal(tip.dataset.open, 'false')
})

test('the filter bar holds the chips, Add filter, Clear all and Pick all on one row; the old form is gone', async () => {
  installDom()
  const where = [['single_end', 'bool', 'false']]
  const page = await items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify(where), ctx())
  await frame(); await frame()
  const bar = page.querySelector('[data-filter-bar]')
  assert.ok(bar.querySelector('[data-chip="single_end"]'))
  assert.equal(bar.querySelector('[data-chip="single_end"]').getAttribute('title'), 'single_end = false')
  assert.ok(bar.querySelector('button[data-add-filter]'))
  assert.equal(bar.querySelector('a[data-clear-filters]').getAttribute('href'), `#/items/bafyrun/markdup?where=${encodeURIComponent('[]')}`)
  assert.ok(bar.querySelector('[data-pick-all]'))
  assert.equal(page.querySelector('fieldset'), null)
  assert.ok(!page.textContent.includes('+ filter'))
})

test('with no conditions the bar has no Clear all', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  assert.equal(page.querySelector('[data-clear-filters]'), null)
  assert.equal(page.querySelectorAll('[data-chip]').length, 0)
})

test('Add filter lists the loaded fields; a field lists its values as filter links with item counts; back returns', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const picker = page.querySelector('[data-filter-picker]')
  assert.equal(picker.dataset.open, 'false')
  await page.querySelector('button[data-add-filter]').fire('click')
  assert.equal(picker.dataset.open, 'true')
  assert.deepEqual(picker.querySelectorAll('[data-field]').map(b => b.dataset.field), ['id', 'single_end', 'strandedness'])
  assert.match(picker.querySelector('[data-field="single_end"]').textContent, /2 values/)
  await picker.querySelector('[data-field="single_end"]').fire('click')
  const values = picker.querySelectorAll('a[data-value]')
  assert.deepEqual(values.map(a => [a.dataset.value, decodeURIComponent(a.getAttribute('href').split('where=')[1])]), [
    ['false', '[["single_end","bool","false"]]'], ['true', '[["single_end","bool","true"]]']])
  assert.match(values[0].textContent, /2 items/)
  assert.match(values[1].textContent, /1 item$/)
  assert.ok(picker.querySelector('input[data-filter-value]'))
  await picker.querySelector('button[data-filter-back]').fire('click')
  assert.equal(picker.querySelectorAll('[data-field]').length, 3)
})

test('the field search ignores case', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  const search = picker.querySelector('[data-filter-search]')
  const shown = (q) => { search.value = q; search.dispatchEvent(new Event('input')); return picker.querySelectorAll('[data-field]').map(b => b.dataset.field) }
  assert.deepEqual(shown('Single'), ['single_end'])
  assert.deepEqual(shown('STRAND'), ['strandedness'])
  assert.deepEqual(shown(''), ['id', 'single_end', 'strandedness'])
})

test('arrow keys move a highlight through the listed fields, wrapping at either end; Enter opens the highlighted one', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  const search = picker.querySelector('[data-filter-search]')
  const active = () => picker.querySelectorAll('[data-field]').filter(b => b.classList.contains('active')).map(b => b.dataset.field)
  assert.deepEqual(active(), [])
  assert.equal(search.press('Enter'), false, 'Enter with nothing highlighted does nothing')
  assert.ok(picker.querySelector('[data-filter-search]'))
  assert.equal(search.press('ArrowDown'), true, 'the caret stays put')
  assert.deepEqual(active(), ['id'])
  const id = picker.querySelector('[data-field="id"]')
  assert.equal(search.getAttribute('aria-activedescendant'), id.getAttribute('id'))
  assert.equal(id.getAttribute('aria-selected'), 'true')
  search.press('ArrowDown'); search.press('ArrowDown')
  assert.deepEqual(active(), ['strandedness'])
  search.press('ArrowDown')
  assert.deepEqual(active(), ['id'], 'down from the last wraps to the first')
  search.press('ArrowUp')
  assert.deepEqual(active(), ['strandedness'], 'up from the first wraps to the last')
  search.value = 's'; search.dispatchEvent(new Event('input'))
  assert.deepEqual(active(), ['single_end'], 'typing highlights the best match')
  assert.equal(search.getAttribute('aria-activedescendant'), picker.querySelector('[data-field="single_end"]').getAttribute('id'))
  search.press('ArrowUp')
  assert.deepEqual(active(), ['strandedness'])
  search.press('Enter')
  assert.equal(picker.querySelector('input[data-filter-value]').getAttribute('aria-label'), 'a value for strandedness')
})

test('typing ranks the best match first and Enter opens it before the name is finished; clearing the box clears the highlight', async () => {
  installDom()
  const page = await items(fakeEx(), 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  await page.querySelector('button[data-add-filter]').fire('click')
  const picker = page.querySelector('[data-filter-picker]')
  const search = picker.querySelector('[data-filter-search]')
  const type = (q) => { search.value = q; search.dispatchEvent(new Event('input')) }
  type('d')
  assert.deepEqual(picker.querySelectorAll('[data-field]').map(b => b.dataset.field), ['id', 'single_end', 'strandedness'], 'each has a d inside, so the given order holds')
  type('strd')
  assert.deepEqual(picker.querySelectorAll('[data-field]').map(b => b.dataset.field), ['strandedness'])
  type('')
  assert.equal(picker.querySelectorAll('[data-field]').filter(b => b.classList.contains('active')).length, 0)
  type('stra')
  assert.equal(search.press('Enter'), true)
  assert.equal(picker.querySelector('input[data-filter-value]').getAttribute('aria-label'), 'a value for strandedness')
})

test('a range condition in the route has a chip reading path >= value, and its remove link keeps the rest', async () => {
  installDom()
  const where = [['single_end', 'bool', 'false'], ['lane', 'int', '2', '>=']]
  const page = await items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify(where), ctx())
  const chip = page.querySelector('[data-chip="lane"]')
  assert.match(chip.textContent, /lane >= 2/)
  assert.equal(chip.getAttribute('title'), 'lane >= 2')
  assert.equal(decodeURIComponent(chip.querySelector('a').getAttribute('href').split('where=')[1]), '[["single_end","bool","false"]]')
  await assert.rejects(items(fakeEx(), 'bafyrun', 'markdup', JSON.stringify([['lane', 'int', '2', '!=']]), ctx()), (e) => e.code === 'bad_route')
})

test('a numeric field offers = > >= < <=; choosing one turns its values into range links; a text field offers none', async () => {
  installDom()
  const ex = fakeEx()
  ex.item = async (c, i) => ({ view: { id: i, lane: { i1: 1, i2: 2, i3: 2 }[i] }, leaves: [], value: { id: i } })
  const page = await items(ex, 'bafyrun', 'markdup', '[]', ctx())
  await frame(); await frame()
  const picker = page.querySelector('[data-filter-picker]')
  await page.querySelector('button[data-add-filter]').fire('click')
  await picker.querySelector('[data-field="id"]').fire('click')
  assert.equal(picker.querySelectorAll('[data-op]').length, 0)
  await picker.querySelector('button[data-filter-back]').fire('click')
  await picker.querySelector('[data-field="lane"]').fire('click')
  const ops = picker.querySelectorAll('button[data-op]')
  assert.deepEqual(ops.map(b => b.dataset.op), ['=', '>', '>=', '<', '<='])
  assert.equal(ops[0].getAttribute('aria-pressed'), 'true')
  const where = (a) => decodeURIComponent(a.getAttribute('href').split('where=')[1])
  assert.equal(where(picker.querySelector('a[data-value="2"]')), '[["lane","int","2"]]')
  await ops[2].fire('click')
  assert.equal(picker.querySelector('button[data-op=">="]').getAttribute('aria-pressed'), 'true')
  assert.equal(where(picker.querySelector('a[data-value="2"]')), '[["lane","int","2",">="]]')
})
