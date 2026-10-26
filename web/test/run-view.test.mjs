// web/test/run-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { resetFolds } from '../src/folds.js'
import { durationText, run } from '../src/views/run.js'
import { FAKE_STORAGE } from './helpers.mjs'
import { createPresence } from '../src/presence.js'

const KINDS = {
  cm: [['bam', 5, 9e6], ['bai', 5, 1e5], ['stats', 5, 9e4], ['flagstat', 5, 8e4], ['idxstats', 5, 7e4], ['txt', 5, 6e4]],
  cq: [['sf', 2, 2e5]],
  cx: [['html', 1, 5e6], ['dir', 2, 2e7]],
}
const SIZES = { cm: 19_784_930, cq: 394_197, cx: 25_145_688 }
const FILES = { cm: 30, cq: 2, cx: 371 }

function fakeEx() {
  const completion = { status: 'succeeded', possibly_incomplete: false, started_at: '2026-10-24T13:24:00.000Z', finished_at: '2026-10-24T13:29:15.000Z', error: null,
    anomalies: { unresolvable: 0, unaddressed: 0, declined: 439, never_published: 5, unjoined: 5 } }
  const items = { cq: ['q1', 'q2'], cm: ['m1', 'm2', 'm3'], cx: ['x1'] }
  const totals = { cq: 2, cm: 5, cx: 1, cl: 0, cb: 0 }
  // quant and markdup items share their sample ids (q1 and m1 are S1), so the run has samples.
  const sample = (i) => `S${i.slice(1)}`
  return { ...FAKE_STORAGE,
    run: async () => ({ row: { run_name: 'high_jang', pipeline: 'nf-core/rnaseq', nf_run_hash: 'abc', manifest_cid: 'bafyman' }, completion,
      collections: [{ output: 'quant', cid: 'cq' }, { output: 'markdup', cid: 'cm' }, { output: 'multiqc', cid: 'cx' },
        { output: 'lint_ribo', cid: 'cl' }, { output: 'bam_qc_rustqc', cid: 'cb' }], state: claimState([]) }),
    collection: async (cid) => ({ total: totals[cid], items: items[cid] ?? [], index: null }),
    allItems: async (cid) => items[cid] ?? [],
    itemBlock: async (c, i) => ({ view: c === 'cm' ? { id: sample(i), meta: { single_end: i === 'm1', has_genome_bam: true, strandedness: 'auto' } } : c === 'cq' ? { id: sample(i), reads: i === 'q1' ? 1 : 2 } : { id: i },
      leaves: [{ name: `${i}.bam` }] }),
    collectionSize: async (cid) => (cid in SIZES ? { bytes: SIZES[cid], blocks: FILES[cid], items: totals[cid], kinds: KINDS[cid], partial: false }
      : { bytes: 0, blocks: 0, items: 0, kinds: [], partial: false }),
    runSize: async () => ({ bytes: 78_400_000, blocks: 894, partial: false }),
    latestSuccessfulRun: async () => 'bafyrun',
    runIdentity: async () => ({ manifest: 'bafyman', revision: '3.14.0', lid: 'lid://abc', config: 'process {}' }),
    blocks: { urlFor: (a) => `blocks/${a}`, ofKind: async () => ({ value: { nextflow_version: '26.09.2-edge', session_id: 'sess-1' } }) },
  }
}
const ctx = () => ({ tray: new Tray(null), trayChanged: () => {}, rerender: () => {},
  store: { name: 'roblab', key: 'a' },
  write: { available: false, reason: 'read only', hrefFor: (h) => h } })

const bodyRows = (page) => page.querySelector('tbody').children.filter(r => r.hasAttribute('data-collection'))
const settled = async () => { await frame(); await frame() }
// The Outputs tab by name: with no tab, a run whose items share a join key opens on Samples.
const OUTPUTS = { tab: 'outputs' }

/** Every element under `el` (the fake DOM has no descendant selectors). */
const within = (el, sel) => el.querySelectorAll(sel)

// Item 8. In Chromium the folds were never inside the table (the DOM, the
// accessibility tree and a re-parse of the HTML all agree): each collapsed
// fold was a full-width strip with the table rows' own 1px rule, drawn flush
// under the last row, so Run details, Storage and Lineage check read as three
// more rows of the outputs table, beneath seven rows of blank cells. The fix
// moves them off the Outputs tab: nothing fold-like follows the table.
test('item 8: the Outputs tab draws no fold beneath the outputs table, and no tab puts a <details> inside a table', async () => {
  for (const tab of ['outputs', 'provenance', 'storage']) {
    installDom()
    resetFolds()
    const page = await run(fakeEx(), 'bafyrun', ctx(), { tab })
    await settled()
    for (const t of within(page, 'table')) assert.equal(within(t, 'details').length, 0, `no details inside a table (${tab})`)
    if (tab === 'outputs') assert.equal(within(page, 'details').length, 0, 'the Outputs tab has no folds at all')
  }
})

test('tabs: Samples, Outputs, Provenance and Storage, each in the URL; no ?tab opens on Samples when the items share a key', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx())
  await settled()
  const tabs = within(page, '[data-tab]')
  assert.deepEqual(tabs.map(t => t.dataset.tab), ['samples', 'outputs', 'provenance', 'storage'])
  assert.deepEqual(tabs.map(t => t.getAttribute('href')),
    ['#/run/bafyrun?tab=samples', '#/run/bafyrun?tab=outputs', '#/run/bafyrun?tab=provenance', '#/run/bafyrun?tab=storage'])
  assert.deepEqual(tabs.map(t => t.classList.contains('on')), [true, false, false, false])
  assert.equal(page.querySelector('[data-run-tab-body]').dataset.runTabBody, 'samples')
  assert.equal(tabs[1].textContent, 'Outputs3', 'the count is the outputs that hold items')
  for (const [tab, on] of [['outputs', 1], ['provenance', 2], ['storage', 3], ['nonsense', 0]]) {
    const other = await run(fakeEx(), 'bafyrun', ctx(), { tab })
    await settled()
    const all = within(other, '[data-tab]')
    assert.equal(all.findIndex(t => t.classList.contains('on')), on)
    assert.equal(all[0].hasAttribute('hidden'), false, `the Samples tab shows on the ${tab} tab`)
    assert.equal(all[1].textContent, 'Outputs3', `the Outputs count shows on the ${tab} tab too`)
  }
})

test('no ?tab opens on Outputs when no output of two or more items has a sample key; the Samples tab is hidden', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  ex.itemBlock = async (c, i) => ({ view: { reads: i.length }, leaves: [] })
  const page = await run(ex, 'bafyrun', ctx())
  await settled()
  const tabs = within(page, '[data-tab]')
  assert.equal(page.querySelector('[data-run-tab-body]').dataset.runTabBody, 'outputs')
  assert.deepEqual(tabs.map(t => t.classList.contains('on')), [false, true, false, false])
  assert.ok(tabs[0].hasAttribute('hidden'))
  assert.equal(within(page, 'tr').filter(r => r.dataset.collection).length, 3, 'the outputs table')
})

test('the header: seal, name, status, durability, one presence dot, Pin… opening the Storage tab and Release… its confirmation, and the subline', async () => {
  installDom()
  resetFolds()
  const marks = []
  const writable = { available: true, here: true, writable: 'roblab', hrefFor: (h) => h, writer: {}, run: async () => {} }
  const page = await run(fakeEx(), 'bafyrun', { ...ctx(), write: writable, mark: (m) => marks.push(m) }, OUTPUTS)
  await settled()
  assert.deepEqual(marks, [{ pipeline: 'nf-core/rnaseq', run: 'bafyrun', expand: true }])
  const head = page.querySelector('.page-head')
  assert.equal(head.querySelector('h1').textContent, 'high_jang')
  assert.equal(head.querySelector('.title').querySelector('[data-seal]').dataset.seal, 'bafyrun', 'the run seal')
  assert.equal(head.querySelector('.status').textContent, 'Succeeded')
  assert.equal(head.querySelector('.state').textContent, 'Held by run')
  assert.ok(head.querySelector('.state').classList.contains('s-held'))
  assert.equal(within(head.querySelector('.presence'), '.dot').length, 1)
  assert.ok(head.querySelector('.presence').querySelector('.dot').classList.contains('a'))
  const actions = within(head, '.btn')
  assert.deepEqual(actions.map(a => a.textContent), ['Pin…', 'Release…', 'Add to project…', 'Copy to…'])
  assert.deepEqual(actions.map(a => a.getAttribute('href')), ['#/run/bafyrun?tab=storage', '#/run/bafyrun?tab=storage&release=1', null, null])
  assert.equal(head.querySelector('[data-copy-to]').dataset.copyTo, 'unavailable', 'no copy in this ctx')
  const subline = within(head.querySelector('.subline'), 'span').filter(s => !s.hasAttribute('hidden') && s.dataset.slot)
  const text = subline.map(s => s.textContent)
  assert.equal(text[0], 'Latest good run of nf-core/rnaseq')
  assert.match(text[1], /^Finished Oct 24, \d{1,2}:\d{2}\s[AP]M · 5 min 15 s$/)
  assert.equal(text[2], 'Nextflow 26.09.2-edge')
  assert.equal(text[3], '78.4 MB stored · 894 file blocks')
  const lid = head.querySelector('.subline').querySelector('[data-ref]')
  assert.equal(lid.dataset.ref, 'lid://abc')
  assert.doesNotMatch(page.textContent, /null|undefined/)
})

test('durability follows the retention state: Pinned, Releasing, else Held by run', async () => {
  for (const [claims, text, cls] of [
    [[{ cid: 'p1', verb: 'add', attribute: 'pin', value: 'paper', supersedes: [] }], 'Pinned', 's-pinned'],
    [[{ cid: 'r1', verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [] }], 'Releasing', 's-releasing']]) {
    installDom()
    resetFolds()
    const ex = fakeEx()
    const run0 = ex.run
    ex.run = async () => ({ ...(await run0()), state: claimState(claims) })
    const page = await run(ex, 'bafyrun', ctx(), OUTPUTS)
    assert.equal(page.querySelector('.state').textContent, text)
    assert.ok(page.querySelector('.state').classList.contains(cls))
  }
})

test('a run that is not the latest good one says nothing about it', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  ex.latestSuccessfulRun = async () => 'bafyother'
  const page = await run(ex, 'bafyrun', ctx(), OUTPUTS)
  await settled()
  assert.doesNotMatch(page.textContent, /Latest good run/)
})

test('the outputs table: its headers, sizes descending by default, the empty outputs as one row that lists their names', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx(), OUTPUTS)
  await settled()
  assert.deepEqual(within(page, 'th').map(t => t.textContent), ['Output', 'Items', 'Key fields', 'Files', 'Size'])
  const rows = bodyRows(page)
  assert.deepEqual(rows.map(r => r.dataset.output), ['multiqc', 'markdup', 'quant'])
  assert.equal(rows[0].querySelector('a').getAttribute('href'), '#/items/bafyrun/multiqc')
  assert.deepEqual(rows.map(r => r.children[1].textContent), ['1', '5', '2'])
  const group = page.querySelector('[data-empty-outputs]')
  assert.equal(page.querySelector('tbody').children.at(-1), group, 'the empty outputs row comes last')
  assert.equal(group.querySelector('td').getAttribute('colspan'), '5')
  const toggle = group.querySelector('button')
  assert.equal(toggle.textContent, '2 empty outputs')
  assert.equal(toggle.getAttribute('aria-expanded'), 'false')
  const names = group.querySelector('[data-empty-names]')
  assert.ok(names.hasAttribute('hidden'))
  await toggle.click()
  assert.equal(toggle.getAttribute('aria-expanded'), 'true')
  assert.ok(!names.hasAttribute('hidden'))
  assert.deepEqual(within(names, '[data-output]').map(n => n.dataset.output), ['bam_qc_rustqc', 'lint_ribo'])
  assert.equal(names.querySelector('[data-output]').getAttribute('href'), '#/items/bafyrun/bam_qc_rustqc')
  await toggle.click()
  assert.ok(names.hasAttribute('hidden'))
})

test('clicking Items or Size re-sorts; Size breaks ties under Items', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx(), OUTPUTS)
  await settled()
  const order = () => bodyRows(page).map(r => r.dataset.output)
  const sizeTh = within(page, 'th')[4]
  assert.equal(sizeTh.getAttribute('aria-sort'), 'descending')
  await page.querySelector('[data-sort=items]').click()
  assert.deepEqual(order(), ['markdup', 'quant', 'multiqc'])
  assert.equal(within(page, 'th')[1].getAttribute('aria-sort'), 'descending')
  assert.equal(sizeTh.hasAttribute('aria-sort'), false)
  await page.querySelector('[data-sort=items]').click()
  assert.deepEqual(order(), ['multiqc', 'quant', 'markdup'])
  await page.querySelector('[data-sort=size]').click()
  assert.deepEqual(order(), ['multiqc', 'markdup', 'quant'])
  await page.querySelector('[data-sort=size]').click()
  assert.deepEqual(order(), ['quant', 'markdup', 'multiqc'])
  assert.equal(page.querySelector('[data-empty-outputs]'), page.querySelector('tbody').children.at(-1))
})

test('files: ext×count chips by bytes, at most four then +N, then the file count', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx(), OUTPUTS)
  await settled()
  const [multiqc, markdup, quant] = bodyRows(page)
  const files = (r) => r.querySelector('[data-files]')
  assert.deepEqual(within(files(markdup), '.kind').map(k => k.textContent), ['bam×5', 'bai×5', 'stats×5', 'flagstat×5'])
  assert.deepEqual(within(files(markdup), '.more').map(k => k.textContent), ['+2', '30 files'])
  assert.equal(files(markdup).querySelector('[data-more]').getAttribute('aria-label'), 'idxstats×5\ntxt×5')
  assert.deepEqual(within(files(multiqc), '.kind').map(k => k.textContent), ['html×1', 'dir×2'])
  assert.deepEqual(within(files(multiqc), '.more').map(k => k.textContent), ['371 files'])
  assert.deepEqual(within(files(quant), '.more').map(k => k.textContent), ['2 files'])
})

test('size: bytes, right-aligned, with a bar scaled to the largest output; a partial size says at least', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  const size0 = ex.collectionSize
  ex.collectionSize = async (cid) => ({ ...(await size0(cid)), partial: cid === 'cq' })
  const page = await run(ex, 'bafyrun', ctx(), OUTPUTS)
  await settled()
  const [multiqc, markdup, quant] = bodyRows(page)
  const cell = (r) => r.children[4]
  assert.ok(cell(multiqc).classList.contains('r'))
  assert.equal(cell(multiqc).querySelector('.num').textContent, '25.1 MB')
  assert.equal(cell(markdup).querySelector('.num').textContent, '19.8 MB')
  assert.equal(cell(multiqc).querySelector('.minibar').children[0].getAttribute('style'), 'width:100.0%')
  assert.equal(cell(markdup).querySelector('.minibar').children[0].getAttribute('style'), `width:${(SIZES.cm / SIZES.cx * 100).toFixed(1)}%`)
  assert.equal(cell(quant).textContent.replace(/\s+/g, ' ').trim(), 'at least 394 KB')
})

test('key fields: up to three promoted labels, then +N fields; else fields that vary, never a constant one', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx(), OUTPUTS)
  await settled()
  const [multiqc, markdup, quant] = bodyRows(page)
  const fields = (r) => r.querySelector('[data-fields]')
  assert.equal(fields(markdup).querySelector('.sub').textContent, 'Layout · Strand')
  assert.equal(fields(markdup).querySelector('[data-more]').textContent, '+2 fields')
  assert.equal(fields(markdup).querySelector('[data-more]').getAttribute('aria-label'), 'id\nmeta.has_genome_bam')
  // quant has no promoted field: the fields that vary, by short label.
  assert.equal(fields(quant).querySelector('.sub').textContent, 'id · reads')
  assert.equal(fields(quant).querySelector('[data-more]'), null)
  // multiqc has one item and no promoted field, so nothing varies: a dash.
  assert.equal(fields(multiqc).querySelector('.sub'), null)
  assert.equal(fields(multiqc).querySelector('[data-more]'), null)
  assert.equal(fields(multiqc).textContent, '—')
})

test('a constant meta.has_* is never a key field', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  ex.run = async () => ({ row: { run_name: 'r', pipeline: 'custom.nf', nf_run_hash: null }, completion: (await fakeEx().run()).completion,
    collections: [{ output: 'markdup', cid: 'cm' }], state: claimState([]) })
  const page = await run(ex, 'bafyrun', ctx(), OUTPUTS)
  await settled()
  const shown = bodyRows(page)[0].querySelector('[data-fields]').querySelector('.sub').textContent
  assert.equal(shown, 'id · single_end')
  assert.doesNotMatch(shown, /has_genome_bam/)
})

test('fields +N names the rest in the page tooltip', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx(), OUTPUTS)
  await settled()
  const more = bodyRows(page)[1].querySelector('[data-fields]').querySelector('[data-more]')
  const tip = page.querySelector('[data-pill-tip]')
  await more.fire('mouseenter')
  assert.equal(tip.dataset.open, 'true')
  assert.deepEqual(within(tip, 'div').map(d => d.textContent), ['id', 'meta.has_genome_bam'])
  await more.fire('mouseleave')
  assert.equal(tip.dataset.open, 'false')
})

test('index file: the link sits after the kinds and downloads the index with addresses in place of Publish Coordinates', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  const collection = ex.collection
  ex.collection = async (cid) => ({ ...(await collection(cid)), index: cid === 'cq' ? { leaf: { address: 'bafyindex', name: 'quant.json' } } : null })
  const asked = []
  ex.stableIndex = async (cid) => { asked.push(cid); return { name: 'quant.json', text: '[cas://bafkfile/q1.bam]', replaced: 2, unmatched: 1 } }
  const saved = []
  const page = await run(ex, 'bafyrun', { ...ctx(), download: (name, text) => saved.push([name, text]) }, OUTPUTS)
  await settled()
  const cell = bodyRows(page).find(r => r.dataset.output === 'quant').querySelector('[data-files]')
  const index = cell.querySelector('[data-index-stable]')
  assert.equal(index.textContent, 'index file')
  assert.ok(index.classList.contains('index-link'))
  await index.click()
  await frame()
  assert.deepEqual(asked, ['cq'])
  assert.deepEqual(saved, [['quant.json', '[cas://bafkfile/q1.bam]']])
  assert.match(cell.querySelector('[data-index-status]').textContent, /1 path left as Nextflow wrote it/)
  ex.stableIndex = async () => { throw new Error('block bafyindex is not in this member') }
  await index.click()
  await frame()
  assert.match(cell.querySelector('[data-index-status]').textContent, /Could not prepare the index file: block bafyindex/)
})

test('Provenance: the lid reference to copy, the RunCompletion and RunManifest chips, revision, configuration folded, and the lineage check', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx(), { tab: 'provenance' })
  assert.equal(page.querySelector('table'), null, 'no outputs table here')
  const body = page.querySelector('[data-run-tab-body]')
  assert.equal(body.dataset.runTabBody, 'provenance')
  const refs = within(body, '[data-ref]')
  assert.deepEqual(refs.map(r => r.dataset.ref), ['lid://abc', 'bafyrun', 'bafyman'])
  assert.deepEqual(refs.map(r => r.getAttribute('data-address')), [null, 'bafyrun', 'bafyman'])
  assert.match(body.textContent, /Revision3\.14\.0/)
  assert.match(body.textContent, /Nextflow26\.09\.2-edge/)
  const config = body.querySelector('[data-fold="config"]')
  assert.equal(config.querySelector('summary').textContent, 'Configuration')
  assert.equal(config.hasAttribute('open'), false)
  const lineage = body.querySelector('[data-lineage]')
  const health = lineage.querySelector('[data-health]')
  assert.equal(health.dataset.health, 'act')
  assert.equal(health.textContent, '5 published files are outside any workflow output. Declare them in the output { } block to keep their lineage. 5 inputs are referenced, not stored.')
  assert.ok(health.querySelector('.note').classList.contains('warn'))
  assert.deepEqual(within(lineage, '[data-anomaly]').map(d => [d.dataset.anomaly, d.textContent]),
    [['unjoined', 'Outside any output'], ['declined', 'Empty optional slots'], ['never_published', 'Referenced, not stored']])
  assert.ok(lineage.querySelector('[data-anomaly="unjoined"]').classList.contains('warn'))
  assert.ok(!lineage.querySelector('[data-anomaly="declined"]').classList.contains('warn'))
  assert.deepEqual(within(lineage, 'dd').map(d => d.textContent), ['5', '439', '5'])
  assert.doesNotMatch(page.textContent, /null|undefined/)
})

test('Storage: the retention panel, with the run size before any write', async () => {
  installDom()
  resetFolds()
  const page = await run(fakeEx(), 'bafyrun', ctx(), { tab: 'storage' })
  await settled()
  const body = page.querySelector('[data-run-tab-body]')
  assert.equal(body.dataset.runTabBody, 'storage')
  assert.ok(body.querySelector('[data-retention]'))
  assert.equal(body.querySelector('[data-run-size]').textContent,
    'This run holds 78.4 MB (894 file blocks) in roblab. Files that other runs or pins also hold stay after a release.')
  assert.doesNotMatch(body.textContent, /frees/)
})

test('Storage: Pin says the bytes it keeps, in this store, before the click', async () => {
  installDom()
  resetFolds()
  const writable = { ...ctx(), write: { available: true, here: true, writable: 'roblab', hrefFor: (h) => h, writer: {}, run: async () => {} } }
  const page = await run(fakeEx(), 'bafyrun', writable, { tab: 'storage' })
  assert.equal(page.querySelector('#pin').textContent, 'Pin 78.4 MB in roblab')
  assert.ok(page.querySelector('#release'))
  const ex = fakeEx()
  ex.runSize = async () => ({ bytes: 78_400_000, blocks: 894, partial: true })
  const partial = await run(ex, 'bafyrun', writable, { tab: 'storage' })
  assert.equal(partial.querySelector('#pin').textContent, 'Pin at least 78.4 MB in roblab')
})

test('a calm lineage says inputs are expected, without a warning', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  const run0 = ex.run
  ex.run = async () => { const r = await run0(); return { ...r, completion: { ...r.completion, anomalies: { never_published: 5 } } } }
  const page = await run(ex, 'bafyrun', ctx(), { tab: 'provenance' })
  const health = page.querySelector('[data-health]')
  assert.equal(health.dataset.health, 'ok')
  assert.equal(health.textContent, 'Inputs are referenced, not stored. Expected for local inputs.')
  assert.ok(!health.querySelector('.note').classList.contains('warn'))
})

test('the table reorders once: rows stay A–Z with every count in until the sizes are in too', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  let release
  const gate = new Promise((resolve) => { release = resolve })
  const size0 = ex.collectionSize
  ex.collectionSize = async (cid) => { await gate; return size0(cid) }
  const page = await run(ex, 'bafyrun', ctx(), OUTPUTS)
  await settled()
  const order = () => page.querySelector('tbody').children.map(r => r.dataset.output ?? 'group')
  assert.deepEqual(order(), ['bam_qc_rustqc', 'lint_ribo', 'markdup', 'multiqc', 'quant'], 'counts alone do not reorder')
  release()
  await settled()
  assert.deepEqual(order(), ['multiqc', 'markdup', 'quant', 'group'])
})

test('durations read in plain units', () => {
  assert.equal(durationText(42_000), '42 s')
  assert.equal(durationText(315_000), '5 min 15 s')
  assert.equal(durationText(300_000), '5 min')
  assert.equal(durationText(3_900_000), '1 h 5 min')
  assert.equal(durationText(90_000_000), '1 d 1 h')
})

test('a released run says who released it and when, as a claim tag', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  const run0 = ex.run
  const rel = { cid: 'r1', verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [], asserted_by: 'ada.lovelace@example.org', timestamp: '2026-10-24T11:00:00Z' }
  ex.run = async () => ({ ...(await run0()), state: claimState([rel]) })
  const page = await run(ex, 'bafyrun', ctx())
  const tag = page.querySelector('.claim')
  assert.ok(tag, 'a claim tag')
  assert.match(tag.textContent, /released/)
  assert.equal(tag.querySelector('.av').getAttribute('title'), 'ada.lovelace@example.org')
  assert.ok(tag.querySelector('.age'))
})

test('the header with a second store: the run is held here at once, and the other store is asked once for the RunCompletion', async () => {
  installDom()
  resetFolds()
  const asked = []
  const fetchFn = async (url, init) => { asked.push([init?.method, String(url)]); return new Response(null, { status: 404 }) }
  const presence = createPresence({ primary: 'roblab', list: [{ alias: 'roblab', key: 'a', base: 'm/roblab/' }, { alias: 'org-s3', key: 'b', base: 'm/org-s3/' }] },
    { fetchFn, pageUrl: 'http://h/index.html' })
  const page = await run(fakeEx(), 'bafyrun', { ...ctx(), presence }, OUTPUTS)
  await settled()
  const dots = page.querySelector('.page-head').querySelector('.presence')
  assert.deepEqual(dots.querySelectorAll('.dot').map(d => d.getAttribute('class')), ['dot a', 'dot ring b'])
  assert.equal(dots.getAttribute('title'), 'Held in roblab; not in org-s3')
  assert.deepEqual(asked, [['HEAD', 'http://h/m/org-s3/blocks/un/bafyrun']])
})
