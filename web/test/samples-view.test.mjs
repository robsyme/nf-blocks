// web/test/samples-view.test.mjs
// The run page's Samples tab (P1a Task 2, mockups/07-run-samples.html) over a
// fixture member: a tail run of nf-core/rnaseq whose per-sample outputs join
// on meta.id, with one run-level output.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { buildMember, blockFetch, entryName, rawCid } from './fixture.mjs'
import { snapshotDb } from './helpers.mjs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { Tray } from '../src/tray.js'
import { resetFolds } from '../src/folds.js'
import { run } from '../src/views/run.js'
import { samplesTab } from '../src/views/samples.js'
import { SAMPLES_ITEM_CAP } from '../src/config.js'

const READS = { RAP1_IAA_30M_REP1: 49791, RAP1_UNINDUCED_REP1: 49969, RAP1_UNINDUCED_REP2: 99971, WT_REP1: 99566, WT_REP2: 49760 }
const MAPPED = { RAP1_IAA_30M_REP1: 90.4, RAP1_UNINDUCED_REP1: 96.1, RAP1_UNINDUCED_REP2: 95.8, WT_REP1: 89.5, WT_REP2: 89.25 }
const SAMPLES = Object.keys(READS).sort()
const THRESHOLD = JSON.stringify([['preprocessed:num_trimmed_reads', 'int', '50000', '>=']])

const leaf = (name, address) => ({ kind: 'Leaf', name, address, size: 10, provider: 'head-node', reason: null })
const manifest = (runName) => ({ kind: 'RunManifest', schema: 1, asserted_by: 'test', pipeline: 'nf-core/rnaseq', repository: null, revision: null,
  commit_id: null, run_name: runName, nf_run_hash: `hash-${runName}`, session_id: 's', resumed: false,
  nextflow_version: '26.04.6', params: {}, config: {}, script: null, started_at: '2026-09-01T00:00:00.000Z' })
const completion = (run, collections) => ({ kind: 'RunCompletion', schema: 1, asserted_by: 'test', run, collections, input_set: null,
  status: 'succeeded', exit_status: 0, possibly_incomplete: false, started_at: '2026-09-01T00:00:00.000Z',
  finished_at: '2026-09-04T10:00:00.000Z', anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }, error: null })

/**
 * A member whose one tail run publishes `outputs` ({ name: [item value] }):
 * `{ ex, ids: { completion, collections: { name: cid }, items: { name: [cid in value order] } } }`.
 */
async function memberWith(outputs) {
  const now = Date.now()
  let ids
  const member = await buildMember({
    now,
    extra: ({ put, at }) => {
      const m = put(manifest('R9'))
      const collections = {}
      const items = {}
      const colls = Object.entries(outputs).map(([name, values]) => {
        const cids = values.map(value => put({ kind: 'OutputItem', schema: 1, value }))
        items[name] = cids.map(String)
        const sorted = [...cids].sort((a, b) => (a.toString() < b.toString() ? -1 : 1))
        const coll = put({ kind: 'OutputCollection', schema: 1, asserted_by: 'test', run: m, name, items: sorted,
          paths: sorted.map((_, i) => [`${name}/${i}`]) })
        collections[name] = coll.toString()
        return coll
      })
      const comp = put(completion(m, colls))
      ids = { completion: comp.toString(), collections, items }
      return { log: [entryName(at.R3 + 2000, 'run', comp.toString())], rows: null }
    },
  })
  const ex = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }),
    now: () => now,
  })
  return { ex, ids }
}

const single = (id) => id.startsWith('RAP1_UNINDUCED')
/** distraught_murdock in small: three per-sample outputs keyed by meta.id and a run-level multiqc. */
const joined = () => memberWith({
  preprocessed: SAMPLES.map(id => [{ meta: { id, single_end: single(id) }, num_trimmed_reads: READS[id], fastqc: 'pass' }, leaf(`${id}.fastq.gz`, rawCid(`fq-${id}`))]),
  aligned: SAMPLES.map(id => [{ meta: { id, single_end: single(id) }, percent_mapped: MAPPED[id], aligner: 'star_salmon' }, leaf(`${id}.bam`, rawCid(`bam-${id}`))]),
  quant_salmon: SAMPLES.map(id => [{ meta: { id, single_end: single(id) } }, leaf(`${id}.sf`, rawCid(`sf-${id}`))]),
  multiqc: [[{ id: 'multiqc_report' }, leaf('multiqc_report.html', rawCid('mqc'))]],
})

function ctx() {
  const used = []
  return { used, tray: new Tray(null), trayChanged: () => {}, rerender: () => {}, use: (v) => used.push(v),
    store: { name: 'roblab', key: 'a' }, write: { available: false, reason: 'read only', hrefFor: (h) => h } }
}

/** The fake DOM with a history that records replaceState, and a body and #panel h3 for "Use N samples". */
function dom() {
  installDom()
  resetFolds()
  const replaced = []
  globalThis.history = { replaceState: (_state, _title, url) => replaced.push(url) }
  const focused = []
  globalThis.document.body = { dataset: {} }
  globalThis.document.querySelector = (sel) => (sel === '#panel h3' ? { focus: () => focused.push(sel) } : null)
  return { replaced, focused }
}

/** Waits, a frame at a time, until `fn()` is truthy. */
async function until(fn, what = 'the condition') {
  for (let i = 0; i < 100; i++) {
    if (fn()) return
    await frame()
  }
  assert.fail(`timed out waiting for ${what}`)
}

const rows = (page) => page.querySelectorAll('[data-sample]').filter(r => r.tagName === 'TR')
const ids = (page) => rows(page).map(r => r.dataset.sample)
const loaded = (page) => until(() => page.querySelector('[data-samples-table]') || page.querySelector('[data-samples-empty]'), 'the samples table')
const lastUse = (c) => c.used.at(-1)

test('the default tab joins the items as the Samples tab does: ids that never meet in a second output open on Outputs', async () => {
  dom()
  // Two keyed outputs of several items whose ids never meet (contrasts, lanes): each first item has a key, yet no sample joins.
  const apart = await memberWith({ a: [[{ meta: { id: 'x_vs_y' } }], [{ meta: { id: 'x_vs_z' } }]], b: [[{ meta: { id: 'S1' } }], [{ meta: { id: 'S2' } }]] })
  const page = await run(apart.ex, apart.ids.completion, ctx())
  assert.equal(page.querySelector('[data-run-tab-body]')?.dataset.runTabBody, 'outputs')
  assert.ok(page.querySelector('[data-tab=samples]').hasAttribute('hidden'), 'no Samples tab without samples')
  const keyless = await memberWith({ a: [[{ lane: 1 }], [{ lane: 2 }]] })
  const plain = await run(keyless.ex, keyless.ids.completion, ctx())
  assert.equal(plain.querySelector('[data-run-tab-body]')?.dataset.runTabBody, 'outputs')
})

test('a joinable run opens on Samples, and each item block is read once between the default-tab decision and the tab', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const reads = new Map()
  const itemBlock = ex.itemBlock.bind(ex)
  ex.itemBlock = (c, i) => { reads.set(i, (reads.get(i) ?? 0) + 1); return itemBlock(c, i) }
  const page = await run(ex, idsOf.completion, ctx())
  assert.equal(page.querySelector('[data-run-tab-body]')?.dataset.runTabBody, 'samples')
  await loaded(page)
  assert.deepEqual(ids(page), SAMPLES)
  const items = Object.values(idsOf.items).flat()
  assert.equal(reads.size, items.length, 'every item is read')
  assert.deepEqual([...reads.values()].filter(n => n !== 1), [], 'no item block is read twice')
})

test('while a bare run URL joins its items, the page reports how many it has read through ctx.progress', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const c = ctx()
  const seen = []
  c.progress = (done, total) => seen.push([done, total])
  const page = await run(ex, idsOf.completion, c)
  const total = Object.values(idsOf.items).flat().length
  assert.equal(page.querySelector('[data-run-tab-body]').dataset.runTabBody, 'samples')
  assert.ok(seen.length >= 2, `progress is reported as it goes: ${JSON.stringify(seen)}`)
  assert.deepEqual(seen.at(-1), [total, total])
  assert.ok(seen.every(([d, t], i) => t === total && d <= t && (i === 0 || d >= seen[i - 1][0])), JSON.stringify(seen))
})

test('on a named tab the Samples tab shows from each output\'s first item alone, with no full join', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const reads = []
  const itemBlock = ex.itemBlock.bind(ex)
  ex.itemBlock = (c, i) => { reads.push(c); return itemBlock(c, i) }
  let listed = 0
  const allItems = ex.allItems.bind(ex)
  ex.allItems = (c) => { listed++; return allItems(c) }
  // Provenance reads no item of its own, so every item read here is the visibility check's.
  const page = await run(ex, idsOf.completion, ctx(), { tab: 'provenance' })
  await until(() => !page.querySelector('[data-tab=samples]').hasAttribute('hidden'), 'the Samples tab')
  for (let i = 0; i < 5; i++) await frame()
  assert.equal(listed, 0, 'no collection is listed in full')
  const multi = Object.entries(idsOf.items).filter(([, items]) => items.length >= 2).map(([name]) => idsOf.collections[name])
  assert.ok(reads.filter(c => multi.includes(c)).length <= multi.length, `one first item per output of several items: ${reads.length}`)
  const keyless = await memberWith({ a: [[{ lane: 1 }], [{ lane: 2 }]], b: [[{ meta: { id: 'S1' } }]] })
  const plain = await run(keyless.ex, keyless.ids.completion, ctx(), { tab: 'provenance' })
  for (let i = 0; i < 10; i++) await frame()
  assert.ok(plain.querySelector('[data-tab=samples]').hasAttribute('hidden'), 'no keyed output of several items, no Samples tab')
})

test('a run over the item cap opens on Outputs without reading an item block', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const collection = ex.collection.bind(ex)
  ex.collection = async (c, o) => ({ ...(await collection(c, o)), total: 1500 })
  let reads = 0
  ex.itemBlock = () => { reads++; return new Promise(() => {}) }
  const page = await run(ex, idsOf.completion, ctx())
  assert.equal(page.querySelector('[data-run-tab-body]')?.dataset.runTabBody, 'outputs')
  assert.equal(reads, 0)
})

test('no tab opens on Samples, first in the tabs, with the sample count; Outputs is a tab of its own', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const page = await run(ex, idsOf.completion, ctx())
  assert.ok(page.querySelector('.page-head'), 'the header is drawn')
  await until(() => page.querySelector('[data-run-tab-body=samples]'), 'the Samples body')
  await loaded(page)
  const tabs = page.querySelectorAll('[data-tab]')
  assert.deepEqual(tabs.map(t => t.dataset.tab), ['samples', 'outputs', 'provenance', 'storage'])
  assert.equal(tabs[0].hasAttribute('hidden'), false)
  assert.ok(tabs[0].classList.contains('on'))
  await until(() => tabs[0].querySelector('small'), 'the sample count')
  assert.equal(tabs[0].querySelector('small').textContent, '5')
  const base = `#/run/${idsOf.completion}`
  assert.deepEqual(tabs.map(t => t.getAttribute('href')), [`${base}?tab=samples`, `${base}?tab=outputs`, `${base}?tab=provenance`, `${base}?tab=storage`])
  assert.deepEqual(ids(page), SAMPLES)
})

test('with no tab asked for, the tab body is drawn by the time the run view resolves, so the page is ready with it', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const page = await run(ex, idsOf.completion, ctx())
  assert.equal(page.querySelector('[data-run-tab-body]')?.dataset.runTabBody, 'samples')
  const plain = await memberWith({ a: [[{ meta: { id: 'S1' } }]], b: [[{ versions: 'v1' }]] })
  const outputs = await run(plain.ex, plain.ids.completion, ctx())
  assert.equal(outputs.querySelector('[data-run-tab-body]')?.dataset.runTabBody, 'outputs')
})

test('a run with only one-item outputs opens on Outputs; ?tab=samples says so plainly and links to Outputs', async () => {
  dom()
  const { ex, ids: idsOf } = await memberWith({ a: [[{ meta: { id: 'S1' } }]], b: [[{ versions: 'v1' }]] })
  const page = await run(ex, idsOf.completion, ctx())
  await until(() => page.querySelector('[data-run-tab-body]'), 'a tab body')
  assert.equal(page.querySelector('[data-run-tab-body]').dataset.runTabBody, 'outputs')
  assert.ok(page.querySelector('[data-tab=samples]').hasAttribute('hidden'), 'no Samples tab without samples')
  const samples = await run(ex, idsOf.completion, ctx(), { tab: 'samples' })
  await loaded(samples)
  const empty = samples.querySelector('[data-samples-empty]')
  assert.ok(empty)
  assert.equal(samples.querySelector('[data-samples-table]'), null, 'never an empty table')
  assert.equal(empty.querySelector('a').getAttribute('href'), `#/run/${idsOf.completion}?tab=outputs`)
  const says = 'No output holds two or more items, each with a sample id, or none of those ids appears in a second output.'
  assert.ok(empty.textContent.includes(says), empty.textContent)
  // Two keyed outputs of several items that share no id (contrasts, lanes): no samples, and the same words hold.
  const apart = await memberWith({ a: [[{ meta: { id: 'x_vs_y' } }], [{ meta: { id: 'x_vs_z' } }]], b: [[{ meta: { id: 'S1' } }], [{ meta: { id: 'S2' } }]] })
  const other = await run(apart.ex, apart.ids.completion, ctx(), { tab: 'samples' })
  await loaded(other)
  assert.ok(other.querySelector('[data-samples-empty]').textContent.includes(says))
})

test('the where threshold keeps two of five samples; the chip names the column, its output, the operator and the value', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const page = await run(ex, idsOf.completion, ctx(), { tab: 'samples', where: THRESHOLD })
  await loaded(page)
  assert.deepEqual(ids(page), ['RAP1_UNINDUCED_REP2', 'WT_REP1'])
  assert.equal(page.querySelector('[data-match]').textContent, '2 of 5 samples')
  const chip = page.querySelector('.fchip')
  assert.equal(chip.dataset.chip, 'preprocessed:num_trimmed_reads')
  assert.equal(chip.children[0].textContent, 'Reads (trimmed)')
  assert.equal(chip.querySelector('.src').textContent, 'preprocessed')
  assert.equal(chip.querySelector('.op').textContent, '≥')
  assert.equal(chip.querySelector('input').value, '50000')
  assert.equal(page.querySelector('[data-use-samples]').textContent, 'Use 2 samples')
})

test('"Show 3 excluded" reveals the three dimmed rows and keeps the choice in the URL', async () => {
  const { replaced } = dom()
  const { ex, ids: idsOf } = await joined()
  const page = await run(ex, idsOf.completion, ctx(), { tab: 'samples', where: THRESHOLD })
  await loaded(page)
  const show = page.querySelector('[data-show-excluded]')
  assert.equal(show.textContent, 'Show 3 excluded')
  await show.click()
  assert.equal(rows(page).length, 5)
  assert.deepEqual(rows(page).filter(r => r.classList.contains('dim')).map(r => r.dataset.sample), ['RAP1_IAA_30M_REP1', 'RAP1_UNINDUCED_REP1', 'WT_REP2'])
  assert.equal(show.textContent, 'Hide excluded')
  assert.match(replaced.at(-1), /[?&]show=excluded/)
  const again = await run(ex, idsOf.completion, ctx(), { tab: 'samples', where: THRESHOLD, show: 'excluded' })
  await loaded(again)
  assert.equal(rows(again).length, 5, 'a reload keeps the excluded rows shown')
})

test('unticking a kept row marks it manual and drops it from the selection; changing the filter clears the marks', async () => {
  const { replaced } = dom()
  const { ex, ids: idsOf } = await joined()
  const c = ctx()
  const page = await run(ex, idsOf.completion, c, { tab: 'samples', where: THRESHOLD })
  await loaded(page)
  assert.deepEqual(lastUse(c).samples.selected, ['RAP1_UNINDUCED_REP2', 'WT_REP1'])
  const box = page.querySelector('[data-sample=WT_REP1]').querySelector('input')
  box.checked = false
  await box.fire('change')
  assert.deepEqual(lastUse(c).samples.selected, ['RAP1_UNINDUCED_REP2'])
  assert.equal(lastUse(c).samples.manual.get('WT_REP1'), false)
  assert.equal(page.querySelector('[data-match]').textContent, '1 of 5 samples')
  await page.querySelector('[data-show-excluded]').click()
  const wt = page.querySelector('[data-sample=WT_REP1]')
  assert.ok(wt.classList.contains('dim'))
  assert.equal(wt.querySelector('[data-manual]').textContent, 'manual')
  // Typing a new threshold: the URL follows without a re-render, and the manual marks go.
  const input = page.querySelector('.fchip').querySelector('input')
  input.value = '40000'
  await input.fire('input')
  assert.equal(lastUse(c).samples.manual.size, 0)
  assert.deepEqual(lastUse(c).samples.selected, SAMPLES)
  assert.deepEqual(lastUse(c).samples.where, [['preprocessed:num_trimmed_reads', 'int', '40000', '>=']])
  assert.equal(page.querySelector('[data-manual]'), null)
  const url = replaced.at(-1)
  assert.ok(url.startsWith(`#/run/${idsOf.completion}?tab=samples&where=`))
  assert.deepEqual(JSON.parse(new URLSearchParams(url.split('?')[1]).get('where')), [['preprocessed:num_trimmed_reads', 'int', '40000', '>=']])
})

test('the rail gets { completion, output: null, samples: { table, selected, where, manual }, sizeOf }', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const c = ctx()
  const page = await run(ex, idsOf.completion, c, { tab: 'samples', where: THRESHOLD })
  await loaded(page)
  const view = lastUse(c)
  assert.deepEqual(Object.keys(view).sort(), ['completion', 'output', 'samples', 'sizeOf'])
  assert.deepEqual(Object.keys(view.samples).sort(), ['manual', 'selected', 'table', 'where'])
  assert.equal(view.completion, idsOf.completion)
  assert.equal(view.output, null)
  assert.deepEqual(view.samples.table.runLevel, ['multiqc'])
  assert.deepEqual(view.samples.where, JSON.parse(THRESHOLD))
  const size = await view.sizeOf('WT_REP1', 'aligned')
  assert.deepEqual([size.bytes, size.blocks], [10, 1])
  const all = await view.sizeOf('WT_REP1', null)
  assert.deepEqual([all.bytes, all.blocks], [30, 3])
})

test('the table: Sample links to its first per-sample item; promoted columns with their source; formatted values; Outputs; Size', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const page = await run(ex, idsOf.completion, ctx(), { tab: 'samples' })
  await loaded(page)
  const heads = page.querySelector('[data-samples-table]').querySelectorAll('th')
  assert.deepEqual(heads.map(t => t.textContent), ['', 'Sample', 'Reads (trimmed)preprocessed', '% mappedaligned', 'Layoutmeta.single_end', 'Outputs', 'Sizeall outputs'])
  assert.deepEqual(heads.filter(t => t.querySelector('small')).slice(0, 3).map(t => t.querySelector('small').textContent), ['preprocessed', 'aligned', 'meta.single_end'])
  assert.ok(heads[2].classList.contains('r') && heads[3].classList.contains('r') && !heads[4].classList.contains('r'))
  const wt = page.querySelector('[data-sample=WT_REP1]')
  const a = wt.querySelector('.namecell').querySelector('a')
  assert.equal(a.textContent, 'WT_REP1')
  const aligned = idsOf.items.aligned[SAMPLES.indexOf('WT_REP1')]
  assert.equal(a.getAttribute('href'), `#/item/${idsOf.collections.aligned}/${aligned}`)
  const cell = (key) => wt.querySelector(`[data-column="${key}"]`)
  assert.equal(cell('preprocessed:num_trimmed_reads').textContent, '99,566')
  assert.ok(cell('preprocessed:num_trimmed_reads').classList.contains('r'))
  assert.equal(cell('aligned:percent_mapped').textContent, '89.5%')
  assert.equal(cell('meta.single_end').textContent, 'paired')
  assert.equal(page.querySelector('[data-sample=RAP1_UNINDUCED_REP2]').querySelector('[data-column="meta.single_end"]').textContent, 'single')
  assert.deepEqual(wt.querySelector('[data-outputs]').querySelectorAll('.kind').map(k => k.textContent), ['quant'], 'has-chips from the config')
  await until(() => wt.querySelector('[data-size]').textContent === '30 B', 'the sample size')
  assert.doesNotMatch(page.textContent, /null|undefined|NaN/)
})

test('the column picker: the line under the table counts its fields and outputs; Columns adds one with its source', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const page = await run(ex, idsOf.completion, ctx(), { tab: 'samples' })
  await loaded(page)
  const line = page.querySelector('[data-more-fields]')
  assert.match(line.textContent, /^2 more fields from 2 outputs in the column picker/)
  await page.querySelector('[data-columns-button]').click()
  const picker = page.querySelector('[data-column-picker]')
  assert.equal(picker.dataset.open, 'true')
  assert.deepEqual(picker.querySelectorAll('[data-group]').map(g => g.dataset.group), ['aligned', 'preprocessed'])
  await picker.querySelector('[data-add-column="preprocessed:fastqc"]').click()
  const th = page.querySelector('[data-samples-table]').querySelector('th[data-column="preprocessed:fastqc"]')
  assert.equal(th.textContent, 'fastqcpreprocessed')
  assert.equal(page.querySelector('[data-sample=WT_REP1]').querySelector('[data-column="preprocessed:fastqc"]').textContent, 'pass')
  assert.match(line.textContent, /^1 more field from 1 output in the column picker/)
})

test('+ Add filter: pick a field, an operator and a typed value; the filter applies and the URL follows', async () => {
  const { replaced } = dom()
  const { ex, ids: idsOf } = await joined()
  const c = ctx()
  const page = await run(ex, idsOf.completion, c, { tab: 'samples' })
  await loaded(page)
  await page.querySelector('[data-add-filter]').click()
  const picker = page.querySelector('[data-filter-picker]')
  assert.equal(picker.dataset.open, 'true')
  const search = picker.querySelector('[data-filter-search]')
  search.value = 'percent'
  await search.fire('input')
  const fields = picker.querySelectorAll('[data-field]')
  assert.equal(fields[0].dataset.field, 'aligned:percent_mapped')
  await fields[0].click()
  await picker.querySelector('[data-op=">="]').click()
  const value = picker.querySelector('[data-filter-value]')
  value.value = '95'
  value.press('Enter')
  assert.deepEqual(lastUse(c).samples.where, [['aligned:percent_mapped', 'int', '95', '>=']])
  assert.deepEqual(ids(page), ['RAP1_UNINDUCED_REP1', 'RAP1_UNINDUCED_REP2'])
  assert.equal(page.querySelector('[data-match]').textContent, '2 of 5 samples')
  assert.match(decodeURIComponent(replaced.at(-1)), /where=\[\["aligned:percent_mapped","int","95",">="\]\]/)
  // The chip's × takes it away again.
  await page.querySelector('.fchip').querySelector('.x').click()
  assert.deepEqual(lastUse(c).samples.where, [])
  assert.equal(rows(page).length, 5)
})

test('a picker listens for clicks on the page only while it is open', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const page = await run(ex, idsOf.completion, ctx(), { tab: 'samples' })
  await loaded(page)
  const idle = document.listenerCount('click')
  await page.querySelector('[data-add-filter]').click()
  const picker = page.querySelector('[data-filter-picker]')
  assert.equal(document.listenerCount('click'), idle + 1)
  picker.press('Escape')
  assert.equal(picker.dataset.open, 'false')
  assert.equal(document.listenerCount('click'), idle)
  // Opened again, a click elsewhere closes it and takes the listener away.
  await page.querySelector('[data-add-filter]').click()
  document.dispatch('click', { target: page })
  assert.equal(picker.dataset.open, 'false')
  assert.equal(document.listenerCount('click'), idle)
  // The Columns button opens and closes its own picker the same way.
  await page.querySelector('[data-columns-button]').click()
  assert.equal(document.listenerCount('click'), idle + 1)
  await page.querySelector('[data-columns-button]').click()
  assert.equal(page.querySelector('[data-column-picker]').dataset.open, 'false')
  assert.equal(document.listenerCount('click'), idle)
})

test('"Use N samples" opens the rail and focuses its heading', async () => {
  const { focused } = dom()
  const { ex, ids: idsOf } = await joined()
  const page = await run(ex, idsOf.completion, ctx(), { tab: 'samples', where: THRESHOLD })
  await loaded(page)
  await page.querySelector('[data-use-samples]').click()
  assert.equal(globalThis.document.body.dataset.panel, 'open')
  assert.deepEqual(focused, ['#panel h3'])
})

test('a run over the item cap gets a note pointing to the Outputs tab, and no table', async () => {
  dom()
  const ex = { collection: async () => ({ total: 1500, items: [] }), allItems: async () => { throw new Error('not read') } }
  const node = samplesTab(ex, 'bafyrun', ctx(), { pipeline: 'nf-core/rnaseq', collections: [{ output: 'a', cid: 'ca' }, { output: 'b', cid: 'cb' }],
    where: [], showExcluded: false, counted: () => {} })
  await until(() => node.querySelector('.note'), 'the note')
  assert.equal(SAMPLES_ITEM_CAP, 2000)
  assert.equal(node.querySelector('.note').textContent, 'This run has 3,000 items. The Samples tab joins up to 2,000; use the Outputs tab.')
  assert.equal(node.querySelector('[data-samples-table]'), null)
})

test('leaving before the probe answers reads no item blocks and opens no tab', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  let leftAt = null
  const reads = []
  const itemBlock = ex.itemBlock.bind(ex)
  ex.itemBlock = (c, i) => { reads.push(leftAt === null ? 'before' : 'after'); return itemBlock(c, i) }
  const allItems = ex.allItems.bind(ex)
  let listed = 0
  ex.allItems = (c) => { listed++; return allItems(c) }
  // app.js render: the reader moves on to another page while the probe lists the outputs.
  const collection = ex.collection.bind(ex)
  ex.collection = (c, o) => {
    if (leftAt === null) { ex.pageChanged(); leftAt = reads.length }
    return collection(c, o)
  }
  const page = await run(ex, idsOf.completion, ctx())
  for (let i = 0; i < 10; i++) await frame()
  assert.equal(page.querySelector('[data-run-tab-body]'), null, 'no tab opens for a page that is gone')
  assert.equal(listed, 0, 'no collection is listed')
  assert.deepEqual(reads.filter(r => r === 'after'), [], 'no item block is read after the page changed')
})

test('a Samples tab that loses its page before listing reads no item blocks', async () => {
  dom()
  const { ex, ids: idsOf } = await joined()
  const { collections } = await ex.run(idsOf.completion)
  let reads = 0
  ex.itemBlock = () => { reads++; return new Promise(() => {}) }
  let gone = false
  const node = samplesTab(ex, idsOf.completion, ctx(), { pipeline: 'nf-core/rnaseq', collections, alive: () => !gone })
  gone = true
  for (let i = 0; i < 5; i++) await frame()
  assert.equal(reads, 0)
  assert.equal(node.querySelector('[data-samples-table]'), null)
})
