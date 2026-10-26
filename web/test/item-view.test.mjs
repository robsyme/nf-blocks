// The item page and the file page over the small fake DOM (dom.mjs).
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { item, orderRows } from '../src/views/item.js'
import { content } from '../src/views/collection.js'
import { resetFolds } from '../src/folds.js'
import { FAKE_STORAGE } from './helpers.mjs'
import { createPresence } from '../src/presence.js'

const ITEM = 'bafyreicooyl2pyo3ux3ieqj6kyhacb2zsxhijhfpiixexmu4sn3qxuzrty'
const COLL = 'bafyreiabfmoro4h5zrbnzltdc5u4bguxoptvjq2pkn32fkczz3woqtcyju'
const RUN = 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq'
const OLD = 'bafyreidordqagvbzb63ew3h4xwykbkufpxufq7gyf6l567dbl5bhz7ac3q'
const FAILED = 'bafyreiaedzibppjsllztdb2wqg322harsiohnkmccapz2uemjervj5kcgu'
const ZIP = 'bafkreibr5nkdlnukzyfhxj2rcgtoxsfztlli2fr6lfmyxekn2iow22ol2u'
const LOG = 'bafkreieyzp4oif6e74pxxgcthwxreclhjfde56wgiyh66qb4yvzkz3h7fm'
const TSV = 'bafkreibxyh4xa4v5od634co6m4wcvs3yrh7s4vhsz2hgiybzwkahgz3oym'

const leaf = (name, size, address) => ({ kind: 'Leaf', name, size, address })
const LABELS = { [COLL]: { run_name: 'distraught_murdock', output: 'preprocessed', completion: RUN, status: 'succeeded', possibly_incomplete: false },
  c2: { run_name: 'sad_yonath', output: 'preprocessed', completion: OLD, status: 'succeeded', possibly_incomplete: false },
  c3: { run_name: 'sharp_magritte', output: 'preprocessed', completion: FAILED, status: 'failed', possibly_incomplete: false } }

/** The file page's block reads: runs by RunCompletion (runIdentity), outputs by collection (collectionLabel); the snapshot-backed lookups throw. */
function byBlocks() {
  const byCompletion = Object.fromEntries(Object.values(LABELS).map(l => [l.completion, l]))
  const forbidden = (name) => () => { throw new Error(`${name} reads the snapshot on the file page`) }
  return {
    runIdentity: async (c) => { const l = byCompletion[c]; if (!l) throw new Error('no blocks'); return { completion: c, pipeline: 'nf-core/rnaseq', finished_at: '2026-10-24T13:29:00Z', ...l } },
    collectionLabel: async (c) => { const l = LABELS[c]; if (!l) throw new Error('no blocks'); return { output: l.output, run_name: l.run_name } },
    runLabel: forbidden('runLabel'), runRow: forbidden('runRow'),
  }
}

function ctxFor({ writable = true, calls = [] } = {}) {
  return { tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, progress: () => {}, use: () => {}, mark: () => {},
    store: { name: 'roblab', key: 'a' },
    write: { available: true, here: writable, writable: 'roblab', hrefFor: (h) => h,
      writer: { pin: async (s, n) => { calls.push(['pin', s, n]) }, unpin: async (s, c) => { calls.push(['unpin', s, c]) } },
      run: async (status, fn) => { await fn() } } }
}

function itemEx(over = {}) {
  const value = {
    meta: { id: 'WT_REP1', single_end: false }, num_trimmed_reads: 99566,
    fastqc: { raw_zip: [leaf('WT_REP1_raw_1_fastqc.zip', 368509, ZIP)] },
    trim: { log: [leaf('WT_REP1_trimmed_1.fastq.gz_trimming_report.txt', 2663, LOG)] },
    input: leaf('WT_REP1_1.fastq.gz', null, null),
  }
  value.input.reason = 'never_published'
  return {
    item: async () => ({ cid: ITEM, value, view: { id: 'WT_REP1', meta: { id: 'WT_REP1', single_end: false }, num_trimmed_reads: 99566 },
      state: claimState([]), leaves: [value.fastqc.raw_zip[0], value.trim.log[0], value.input] }),
    itemSize: async () => ({ bytes: 5_290_000, blocks: 15, files: 15, kinds: [], partial: false }),
    producersOf: async () => [{ collection_cid: COLL, completion_cid: RUN }, { collection_cid: 'c2', completion_cid: OLD }, { collection_cid: 'c3', completion_cid: FAILED }],
    selectionsHolding: async () => [],
    runLabel: async (c) => LABELS[c] ?? null,
    runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'distraught_murdock' }),
    runRow: async () => ({ finished_at: '2026-10-24T13:29:00Z' }),
    claimStates: async (cids) => new Map(cids.map(c => [c, claimState([])])),
    blocks: { urlFor: (a) => `blocks/${a}`, partial: async () => { throw Object.assign(new Error('missing'), { code: 'block_missing' }) } },
    ...FAKE_STORAGE, ...over }
}

test('item page: the id with its Seal, promoted chips, the files by record key, and every leaf as an address', async () => {
  installDom(); resetFolds()
  const page = await item(itemEx(), COLL, ITEM, ctxFor())
  await frame()
  assert.equal(page.querySelector('h1').textContent, 'WT_REP1')
  assert.ok(page.querySelector('.title').children[0].innerHTML.includes('<svg'), 'a Seal beside the title')
  const chips = page.querySelectorAll('.field')
  const chip = (label) => chips.find(c => c.querySelector('.k').textContent === label)?.querySelector('.v').textContent
  assert.equal(chip('Reads (trimmed)'), '99,566')
  assert.equal(chip('Layout'), 'paired')
  const groups = page.querySelectorAll('tr.group').map(g => g.textContent)
  assert.deepEqual(groups, ['fastqc', 'trim', 'input'], 'largest group first')
  const subkeys = page.querySelector('tbody').querySelectorAll('tr').filter(r => !r.classList.contains('group')).map(r => r.children[1].textContent)
  assert.deepEqual(subkeys, ['raw_zip[0]', 'log[0]', ''])
  for (const cid of [ZIP, LOG]) assert.ok(page.querySelectorAll('[data-address]').some(a => a.dataset.address === cid), `${cid} is an address on the page`)
  assert.match(page.querySelectorAll('.ph').find(ph => ph.textContent.startsWith('Files')).textContent, /15 files · 5\.29 MB/)
  assert.equal(page.querySelector('a[download]').getAttribute('href'), `blocks/${ZIP}`)
  assert.equal(page.querySelectorAll('[data-preview-button]').length, 1, 'only the log is text-like')
  assert.match(page.textContent, /not stored here/)
})

test('item page: Produced by names the runs; the pin says its bytes and waits for a reason', async () => {
  installDom(); resetFolds()
  const calls = []
  const page = await item(itemEx(), COLL, ITEM, ctxFor({ calls }))
  await frame()
  const facts = page.querySelectorAll('.fact').map(f => f.textContent)
  assert.equal(facts.length, 3, 'the run it came from is a chip like the other two')
  assert.ok(facts[0].includes('distraught_murdock'))
  assert.ok(facts.some(f => f.includes('sad_yonath')))
  assert.ok(facts.some(f => /sharp_magritte.*failed/.test(f)))
  const pin = page.querySelector('#pin')
  assert.equal(pin.textContent, 'Pin 5.29 MB in roblab')
  assert.equal(pin.disabled, true)
  const note = page.querySelector('#pin-note')
  note.value = '  '; note.dispatchEvent(new Event('input'))
  assert.equal(pin.disabled, true, 'a blank reason is no reason')
  note.value = 'figure 3'; note.dispatchEvent(new Event('input'))
  assert.equal(pin.disabled, false)
  pin.click()
  await frame()
  assert.deepEqual(calls, [['pin', ITEM, 'figure 3']])
})

test('item page of a swept item: the Files header and Pin give what is stored here, against what the record names', async () => {
  installDom(); resetFolds()
  // The Leaves name 371,172 B in 2 files; a sweep left one file of 5,550 B.
  const stored = { bytes: 5550, blocks: 1, partial: false }
  let page = await item(itemEx({ itemStoredSize: async () => stored }), COLL, ITEM, ctxFor())
  await frame()
  assert.equal(page.querySelector('#pin').textContent, 'Pin 5.55 KB in roblab')
  assert.match(page.textContent, /1 files · 5\.55 KB stored here of 371 KB/)

  installDom(); resetFolds()
  page = await item(itemEx({ itemStoredSize: async () => ({ ...stored, partial: true }) }), COLL, ITEM, ctxFor())
  await frame()
  assert.match(page.textContent, /1 files · at least 5\.55 KB stored here of 371 KB/, 'a partial store gives at least')

  installDom(); resetFolds()
  page = await item(itemEx({ itemStoredSize: async () => ({ bytes: 371172, blocks: 2, partial: false }) }), COLL, ITEM, ctxFor())
  await frame()
  assert.match(page.textContent, /2 files · 371 KB/)
  assert.doesNotMatch(page.textContent, /stored here of/, 'nothing taken, nothing said')
})

test('item page of a swept item with a directory Leaf: the count is the files stored here, never "N of M" across units', async () => {
  installDom(); resetFolds()
  // Three Leaf rows (a zip, a log, a directory); item_size counts the directory's files, so 4 files are stored here.
  const DIR = 'bafyreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
  const base = itemEx()
  const it = await base.item()
  const reads = leaf('reads', 5000, DIR)
  const value = { ...it.value, dir: { reads: [reads] } }
  const page = await item(itemEx({ item: async () => ({ ...it, value, leaves: [...it.leaves, reads] }),
    itemStoredSize: async () => ({ bytes: 5550, blocks: 4, partial: false }) }), COLL, ITEM, ctxFor())
  await frame()
  assert.ok(page.querySelectorAll('tbody tr').some(r => r.textContent.includes('Directory')), 'the directory is one row')
  assert.match(page.textContent, /4 files · 5\.55 KB stored here of 376 KB/)
  assert.doesNotMatch(page.textContent, /\d+ of \d+ files/)
})

test('item page: Preview opens the file in a sheet; a table shows its true size', async () => {
  installDom(); resetFolds()
  const log = new TextEncoder().encode('line one\nline two\n')
  const ex = itemEx({ blocks: { urlFor: (a) => a, partial: async () => ({ bytes: null, size: log.length }), raw: async () => log } })
  const page = await item(ex, COLL, ITEM, ctxFor())
  assert.equal(page.querySelector('[data-preview-sheet]').children.length, 0)
  page.querySelector('[data-preview-button]').click()
  await frame()
  const sheet = page.querySelector('[data-preview-sheet]')
  assert.equal(sheet.querySelectorAll('code').length, 2)
  assert.match(sheet.textContent, /2 lines/)
})

test('file page: the most common filename, size and kind, a copyable cas:// address, the producers by name', async () => {
  installDom(); resetFolds()
  const body = new TextEncoder().encode(['gene_id\tgene_name\tA\tB', ...Array.from({ length: 126 }, (_, i) => `g${i}\tn${i}\t${i * 1000}\t${i}`)].join('\n') + '\n')
  const rows = [
    { content_cid: TSV, item_cid: ITEM, collection_cid: COLL, completion_cid: RUN, filename: 'salmon.merged.gene_counts.tsv' },
    { content_cid: TSV, item_cid: ITEM, collection_cid: 'c2', completion_cid: OLD, filename: 'salmon.merged.gene_counts.tsv' },
    { content_cid: TSV, item_cid: ITEM, collection_cid: 'c3', completion_cid: FAILED, filename: 'other.tsv' }]
  const ex = { producersOf: async () => rows, claimStates: async (c) => new Map(c.map(x => [x, claimState([])])),
    ...byBlocks(),
    itemBlock: async () => ({ view: { id: 'all_samples' }, leaves: [] }),
    blocks: { urlFor: (a) => `blocks/${a}`, partial: async () => ({ bytes: null, size: body.length }), raw: async () => body } }
  const page = await content(ex, TSV, ctxFor())
  await frame()
  assert.equal(page.querySelector('h1').textContent, 'salmon.merged.gene_counts.tsv')
  const sub = page.querySelector('.subline').textContent
  assert.match(sub, /^[\d.]+ [KM]?B · TSV/)
  assert.match(sub, /126 rows × 4 columns/)
  const chip = page.querySelector('.addr')
  assert.equal(chip.dataset.ref, `cas://${TSV}/salmon.merged.gene_counts.tsv`)
  assert.equal(page.querySelector('[data-preview=table]').querySelectorAll('tr').slice(1).length, 50)
  assert.equal(page.querySelector('[data-preview=table]').querySelectorAll('tr')[1].children[2].textContent, '0')
  assert.equal(page.querySelector('[data-preview=table]').querySelectorAll('tr').slice(1)[49].children[2].textContent, '49,000')
  assert.deepEqual(page.querySelectorAll('thead').at(-1).querySelectorAll('th').map(t => t.textContent), ['Run', 'Output · item', 'Known as', 'Finished'])
  const producers = page.querySelectorAll('[data-producer]')
  assert.equal(producers.length, 3)
  assert.equal(producers[0].dataset.completion, RUN)
  assert.equal(producers[0].dataset.filename, 'salmon.merged.gene_counts.tsv')
  assert.equal(producers[0].dataset.content, TSV)
  assert.equal(producers[0].dataset.item, ITEM)
  assert.equal(producers[0].dataset.collection, COLL)
  assert.match(producers[0].children[0].textContent, /distraught_murdock/)
  assert.match(producers[2].children[0].textContent, /sharp_magritte/)
  assert.match(producers[0].children[1].textContent, /^preprocessed · /)
  assert.match(page.querySelector('#pin').textContent, /^Pin 2\.\d+ KB in roblab$/)
})

test('file page: an HTML file loads nothing until the click, then a frame without scripts', async () => {
  installDom(); resetFolds()
  let reads = 0
  const html = new TextEncoder().encode('<h1>hi</h1><script>alert(1)</script>')
  const created = []
  const revoked = []
  globalThis.URL.createObjectURL = (b) => { created.push(b); return 'blob:x' }
  globalThis.URL.revokeObjectURL = (u) => { revoked.push(u) }
  const ex = { producersOf: async () => [{ content_cid: TSV, item_cid: ITEM, collection_cid: COLL, completion_cid: RUN, filename: 'multiqc_report.html' }],
    claimStates: async (c) => new Map(c.map(x => [x, claimState([])])), ...byBlocks(),
    blocks: { urlFor: (a) => a, partial: async () => ({ bytes: null, size: html.length }), raw: async () => { reads++; return html } } }
  const page = await content(ex, TSV, ctxFor())
  assert.equal(page.querySelector('iframe'), null)
  const readsBefore = reads
  const open = page.querySelector('[data-preview=html]').querySelector('button')
  await open.click()
  await frame()
  const frameEl = page.querySelector('iframe')
  assert.ok(frameEl)
  assert.equal(frameEl.getAttribute('sandbox'), '', 'an empty sandbox: no scripts')
  assert.equal(frameEl.getAttribute('src'), 'blob:x')
  assert.ok(reads > readsBefore)
  assert.equal(created.length, 1)
  assert.deepEqual(revoked, [], 'kept until the frame has loaded')
  await frameEl.fire('load')
  assert.deepEqual(revoked, ['blob:x'])
})

test('file page: a binary file shows its kind and size from the item Leaf, and reads no bytes', async () => {
  installDom(); resetFolds()
  let asked = 0
  const ex = { producersOf: async () => [{ content_cid: TSV, item_cid: ITEM, collection_cid: COLL, completion_cid: RUN, filename: 'WT_REP1.bam' }],
    claimStates: async (c) => new Map(c.map(x => [x, claimState([])])), ...byBlocks(),
    itemBlock: async () => ({ view: null, leaves: [{ kind: 'Leaf', name: 'WT_REP1.bam', size: 412_000_000, address: TSV }] }),
    blocks: { urlFor: (a) => a, partial: async () => { asked++; return { bytes: null, size: 1 } }, raw: async () => { asked++ } } }
  const page = await content(ex, TSV, ctxFor())
  assert.match(page.querySelector('.subline').textContent, /^412 MB · BAM/)
  assert.match(page.querySelector('[data-preview=binary]').textContent, /No preview for BAM files/)
  assert.equal(asked, 0)
})

test('file page: a previewable file missing here is asked for once and says so', async () => {
  installDom(); resetFolds()
  let asked = 0
  const ex = { producersOf: async () => [{ content_cid: TSV, item_cid: ITEM, collection_cid: COLL, completion_cid: RUN, filename: 'x.tsv' }],
    claimStates: async (c) => new Map(c.map(x => [x, claimState([])])), ...byBlocks(),
    itemBlock: async () => ({ view: null, leaves: [{ kind: 'Leaf', name: 'x.tsv', size: 10, address: TSV }] }),
    blocks: { urlFor: (a) => a, partial: async () => { asked++; throw Object.assign(new Error('gone'), { code: 'block_missing' }) } } }
  const page = await content(ex, TSV, ctxFor())
  assert.equal(asked, 1)
  assert.match(page.textContent, /This file is not in this store/)
  assert.equal(page.querySelector('a[download]'), null)
})

test('item page: groups run largest first, ties A-Z, files keep slot order', () => {
  const r = (group, size) => ({ group, size })
  const out = orderRows([r('b', 5), r('a', 5), r('c', 100), r('b', 1), r('a', 1), r('d', null)])
  assert.deepEqual(out.map(x => x.group), ['c', 'a', 'a', 'b', 'b', 'd'])
  assert.deepEqual(out.filter(x => x.group === 'b').map(x => x.size), [5, 1])
})

test('file page: runs named and dated from their blocks, succeeded first then newest; the crumbs follow the first', async () => {
  installDom(); resetFolds()
  const forbidden = (name) => () => { throw new Error(`${name} reads the snapshot on the file page`) }
  const rows = [
    { content_cid: TSV, item_cid: ITEM, collection_cid: 'c3', completion_cid: FAILED, filename: 'x.tsv' },
    { content_cid: TSV, item_cid: ITEM, collection_cid: 'c2', completion_cid: OLD, filename: 'x.tsv' },
    { content_cid: TSV, item_cid: ITEM, collection_cid: COLL, completion_cid: RUN, filename: 'x.tsv' }]
  const ids = {
    [FAILED]: { pipeline: 'nf-core/rnaseq', run_name: 'sharp_magritte', status: 'failed', possibly_incomplete: true, finished_at: '2026-10-25T09:00:00Z' },
    [OLD]: { pipeline: 'nf-core/rnaseq', run_name: 'sad_yonath', status: 'succeeded', possibly_incomplete: false, finished_at: '2026-10-20T09:00:00Z' },
    [RUN]: { pipeline: 'nf-core/rnaseq', run_name: 'distraught_murdock', status: 'succeeded', possibly_incomplete: false, finished_at: '2026-10-24T13:29:00Z' } }
  const ex = { producersOf: async () => rows, claimStates: async (c) => new Map(c.map(x => [x, claimState([])])),
    runIdentity: async (c) => ({ completion: c, ...ids[c] }),
    collectionLabel: async () => ({ output: 'preprocessed', run_name: null }),
    itemBlock: async () => ({ view: { id: 'WT_REP1' }, leaves: [{ kind: 'Leaf', name: 'x.tsv', size: 10, address: TSV }] }),
    runLabel: forbidden('runLabel'), runRow: forbidden('runRow'), item: forbidden('item'),
    blocks: { urlFor: (a) => a, partial: async () => { throw Object.assign(new Error('gone'), { code: 'block_missing' }) } } }
  const page = await content(ex, TSV, ctxFor())
  await frame()
  const producers = page.querySelectorAll('[data-producer]')
  assert.deepEqual(producers.map(p => p.dataset.completion), [RUN, OLD, FAILED])
  assert.match(producers[0].children[0].textContent, /distraught_murdock/)
  assert.match(producers[2].children[0].textContent, /sharp_magritte/)
  assert.match(producers[0].children[1].textContent, /^preprocessed · WT_REP1$/)
  assert.notEqual(producers[0].children[3].textContent, '', 'finished, from the RunCompletion')
  assert.equal(page.querySelector('.crumbs').textContent, 'nf-core/rnaseq/distraught_murdock/preprocessed/x.tsv')
})

test('item page: other runs are counted per run; In Selections names each Selection', async () => {
  installDom(); resetFolds()
  const ex = itemEx({
    // sad_yonath wrote both stored files, sharp_magritte only the zip.
    producersOf: async (cid) => [{ collection_cid: COLL, completion_cid: RUN }, { collection_cid: 'c2', completion_cid: OLD },
      ...(cid === ZIP ? [{ collection_cid: 'c3', completion_cid: FAILED }] : [])],
    selectionsHolding: async () => ['bafyreisel1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'],
    claimStates: async (cids) => new Map(cids.map(c => [c, claimState(c.startsWith('bafyreisel1')
      ? [{ cid: 'k1', subject: c, verb: 'set', attribute: 'name', value: 'wigshop', supersedes: [], asserted_by: 'rob', timestamp: '2026-10-24T10:00:00Z' }] : [])])),
  })
  const page = await item(ex, COLL, ITEM, ctxFor())
  await frame()
  assert.match(page.textContent, /Some of these files were also produced by/)
  assert.doesNotMatch(page.textContent, /The same 2 files/)
  const facts = page.querySelectorAll('.fact').map(f => f.textContent)
  assert.ok(facts.some(f => /sad_yonath/.test(f) && /2 of 2/.test(f)), facts.join('|'))
  assert.ok(facts.some(f => /sharp_magritte/.test(f) && /1 of 2/.test(f)), facts.join('|'))
  const sel = page.querySelectorAll('a').find(a => a.getAttribute('href') === '#/selection/bafyreisel1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
  assert.equal(sel.textContent, 'wigshop')
  assert.match(sel.parentNode?.textContent ?? page.textContent, /bafy…aaaaaa/)
})

test('item page of a released run shows who released it and when beside Run released', async () => {
  installDom(); resetFolds()
  const rel = { cid: 'r1', verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [], asserted_by: 'ada.lovelace@example.org', timestamp: '2026-10-24T11:00:00Z' }
  const page = await item(itemEx({ claimStates: async (cids) => new Map(cids.map(c => [c, claimState(c === RUN ? [rel] : [])])) }), COLL, ITEM, ctxFor())
  await frame()
  assert.ok(page.querySelector('.state.s-releasing'))
  const tag = page.querySelector('.claim')
  assert.ok(tag, 'the release claim tag')
  assert.match(tag.textContent, /released/)
  assert.equal(tag.querySelector('.av').getAttribute('title'), 'ada.lovelace@example.org')
})

// ---- presence (P3 Task 3) --------------------------------------------------

/** Two members, lab open; `status(alias, cid)` answers each HEAD. */
function twoStores(status) {
  const asked = []
  const fetchFn = async (url, init) => {
    const [, alias, cid] = String(url).match(/\/m\/([^/]+)\/blocks\/[^/]{2}\/([^/]+)$/)
    asked.push([init?.method, alias, cid])
    return new Response(null, { status: status(alias, cid) })
  }
  const members = { primary: 'lab', list: [{ alias: 'lab', key: 'a', base: 'm/lab/' }, { alias: 'org', key: 'b', base: 'm/org/' }] }
  return { presence: createPresence(members, { fetchFn, pageUrl: 'http://h/index.html' }), asked }
}

test('item page: with a second store, the header and each file have its dot, and the files are not asked of the open store when all are here', async () => {
  installDom(); resetFolds()
  const { presence, asked } = twoStores(() => 200)
  const page = await item(itemEx(), COLL, ITEM, { ...ctxFor(), store: { name: 'lab', key: 'a' }, presence })
  await frame()
  const head = page.querySelector('.page-head').querySelector('.presence')
  assert.equal(head.getAttribute('title'), 'Held in lab, org')
  const files = page.querySelectorAll('tr').filter(r => r.dataset.file)
  assert.deepEqual(files.map(r => r.querySelector('.presence').getAttribute('title')), ['Held in lab, org', 'Held in lab, org'])
  assert.deepEqual(asked.map(a => a[1]), ['org', 'org', 'org'])
  assert.ok(asked.every(a => a[0] === 'HEAD'))
})

test('item page: a file missing here shows a ring and Missing here, and its Download is disabled naming the store that holds it', async () => {
  installDom(); resetFolds()
  // The index stores the log only: the zip went, so the page asks the open store which files it still has.
  const { presence, asked } = twoStores((alias, cid) => (alias === 'lab' && cid === ZIP ? 404 : 200))
  const ex = itemEx({ itemStoredSize: async () => ({ bytes: 2663, blocks: 1, partial: false }) })
  const page = await item(ex, COLL, ITEM, { ...ctxFor(), store: { name: 'lab', key: 'a' }, presence })
  await frame()
  const row = (cid) => page.querySelectorAll('tr').find(r => r.dataset.file === cid)
  const zip = row(ZIP)
  assert.ok(zip.hasAttribute('data-missing'))
  assert.deepEqual(zip.querySelector('.presence').querySelectorAll('.dot').map(d => d.getAttribute('class')), ['dot ring a', 'dot b'])
  assert.equal(zip.querySelector('.state.s-missing').textContent, 'Missing here')
  const download = zip.querySelector('button')
  assert.equal(download.textContent, 'Download')
  assert.ok(download.disabled)
  assert.equal(download.getAttribute('title'), 'Not in lab. Held in org.')
  assert.equal(zip.querySelector('a[download]'), null)
  const log = row(LOG)
  assert.equal(log.hasAttribute('data-missing'), false)
  assert.ok(log.querySelector('a[download]'), 'a file still here keeps its Download')
  assert.ok(asked.some(([, alias, cid]) => alias === 'lab' && cid === ZIP))
})

test('item page: one store and a swept item ask nothing, and every file keeps its dot', async () => {
  installDom(); resetFolds()
  const asked = []
  const presence = createPresence({ primary: 'lab', list: [{ alias: 'lab', key: 'a', base: 'm/lab/' }] },
    { fetchFn: async (u) => { asked.push(u); return new Response(null) }, pageUrl: 'http://h/index.html' })
  const ex = itemEx({ itemStoredSize: async () => ({ bytes: 2663, blocks: 1, partial: false }) })
  const page = await item(ex, COLL, ITEM, { ...ctxFor(), presence })
  await frame()
  assert.deepEqual(asked, [])
  assert.equal(page.querySelectorAll('[data-missing]').length, 0)
})

test('file page: a file missing here with a second store says Missing here and names the store that holds it on Download', async () => {
  installDom(); resetFolds()
  const { presence, asked } = twoStores(() => 200)
  const ex = { producersOf: async () => [{ content_cid: TSV, item_cid: ITEM, collection_cid: COLL, completion_cid: RUN, filename: 'x.tsv' }],
    claimStates: async (c) => new Map(c.map(x => [x, claimState([])])), ...byBlocks(),
    itemBlock: async () => ({ view: null, leaves: [{ kind: 'Leaf', name: 'x.tsv', size: 10, address: TSV }] }),
    blocks: { urlFor: (a) => a, partial: async () => { throw Object.assign(new Error('gone'), { code: 'block_missing' }) } } }
  const page = await content(ex, TSV, { ...ctxFor(), store: { name: 'lab', key: 'a' }, presence })
  const head = page.querySelector('.page-head')
  assert.equal(head.querySelector('.state.s-missing').textContent, 'Missing here')
  const download = head.querySelector('[data-download-elsewhere]')
  assert.ok(download.disabled)
  assert.equal(download.getAttribute('title'), 'Not in lab. Checking the other stores.')
  await frame()
  assert.equal(download.getAttribute('title'), 'Not in lab. Held in org.')
  assert.equal(head.querySelector('.presence').getAttribute('title'), 'Held in org; not in lab')
  assert.deepEqual(asked.map(a => a[1]), ['org'], 'the open store is not asked: the page already read that it is missing')
})

test('file page: a file that could not be read (not missing) keeps its Download and says it could not be read, never Missing here', async () => {
  installDom(); resetFolds()
  const { presence } = twoStores(() => 200)
  let reads = 0
  const ex = { producersOf: async () => [{ content_cid: TSV, item_cid: ITEM, collection_cid: COLL, completion_cid: RUN, filename: 'x.tsv' }],
    claimStates: async (c) => new Map(c.map(x => [x, claimState([])])), ...byBlocks(),
    itemBlock: async () => ({ view: null, leaves: [{ kind: 'Leaf', name: 'x.tsv', size: 10, address: TSV }] }),
    blocks: { urlFor: (a) => `blocks/${a}`, partial: async () => { reads++; throw Object.assign(new Error('block x: HTTP 502'), { code: 'fetch_failed' }) } } }
  const page = await content(ex, TSV, { ...ctxFor(), store: { name: 'lab', key: 'a' }, presence })
  const head = page.querySelector('.page-head')
  assert.equal(head.querySelector('.state.s-missing'), null, 'not claimed missing')
  assert.equal(head.querySelector('[data-download-elsewhere]'), null)
  const download = head.querySelector('a[download]')
  assert.ok(download, 'Download stays a link')
  assert.equal(download.getAttribute('href'), `blocks/${TSV}`)
  const unread = page.querySelector('[data-read-error]')
  assert.equal(unread.textContent, 'Could not read the file: block x: HTTP 502')
  assert.equal(reads, 1, 'read once')
  assert.doesNotMatch(page.textContent, /not in this store|Missing here/)
})

test('file page: a file whose block is missing (block_missing) still says Missing here', async () => {
  installDom(); resetFolds()
  const { presence } = twoStores(() => 200)
  const ex = { producersOf: async () => [{ content_cid: TSV, item_cid: ITEM, collection_cid: COLL, completion_cid: RUN, filename: 'x.tsv' }],
    claimStates: async (c) => new Map(c.map(x => [x, claimState([])])), ...byBlocks(),
    itemBlock: async () => ({ view: null, leaves: [{ kind: 'Leaf', name: 'x.tsv', size: 10, address: TSV }] }),
    blocks: { urlFor: (a) => a, partial: async () => { throw Object.assign(new Error('gone'), { code: 'block_missing' }) } } }
  const page = await content(ex, TSV, { ...ctxFor(), store: { name: 'lab', key: 'a' }, presence })
  const head = page.querySelector('.page-head')
  assert.equal(head.querySelector('.state.s-missing').textContent, 'Missing here')
  assert.ok(head.querySelector('[data-download-elsewhere]'))
  assert.equal(page.querySelector('[data-read-error]'), null)
})
