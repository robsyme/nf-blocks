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

test('the run overview: status in words, outputs A–Z with counts, fields and file suffixes', async () => {
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
  assert.deepEqual([...page.querySelectorAll('th')].map(t => t.textContent), ['output', 'items', 'fields', 'files'])
  assert.match(rows[0].textContent, /5/)
  assert.equal(rows[0].querySelector('[data-fields]').textContent, 'id, single_end')
  assert.deepEqual(rows[0].querySelector('[data-files]').querySelectorAll('.ext').map(c => c.textContent), ['.bai', '.bam'])
  assert.doesNotMatch(page.textContent, /null|undefined/)
  assert.ok(!page.querySelector('[data-snippet]'), 'the panel shows the call, not the run page')
})

test('fields: at most three, then +N naming the rest; files: at most two suffixes, then +N, and the index link after them; nothing when there is nothing', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  ex.item = async (c, i) => c === 'cq'
    ? { view: null, leaves: [{ name: `${i}.txt` }] }
    : { view: { a: 1, b: 2, c: 3, d: i, e: 5 }, leaves: [{ name: `${i}.bam` }, { name: `${i}.bai` }, { name: `${i}.csv` }] }
  const collection = ex.collection
  ex.collection = async (cid) => ({ ...(await collection(cid)), index: cid === 'cq' ? { leaf: { address: 'bafyindex', name: 'quant.json' } } : null })
  const page = await run(ex, 'bafyrun', ctx())
  await frame(); await frame()
  const [markdup, quant] = page.querySelectorAll('[data-collection]')
  const fields = markdup.querySelector('[data-fields]')
  assert.equal(fields.textContent, 'a, b, c +2')
  assert.equal(fields.querySelector('[title]').getAttribute('title'), 'd, e')
  assert.deepEqual(markdup.querySelector('[data-files]').querySelectorAll('.ext').map(c => c.textContent), ['.bai', '.bam', '+1'])
  assert.equal(quant.querySelector('[data-fields]').textContent, '')
  const index = quant.querySelector('[data-files]').querySelector('[data-index-stable]')
  assert.equal(index.textContent, 'index file', 'no space inside the link to underline')
  assert.ok(index.classList.contains('index-link'))
  assert.equal(quant.querySelector('[data-files]').textContent, '.txtindex file')
  assert.doesNotMatch(page.textContent, /null|undefined/)
})

test('index file: downloads the index with addresses in place of Publish Coordinates', async () => {
  installDom()
  resetFolds()
  const ex = fakeEx()
  const collection = ex.collection
  ex.collection = async (cid) => ({ ...(await collection(cid)), index: cid === 'cq' ? { leaf: { address: 'bafyindex', name: 'quant.json' } } : null })
  const asked = []
  ex.stableIndex = async (cid) => { asked.push(cid); return { name: 'quant.json', text: '[cas://bafkfile/q1.bam]', replaced: 2, unmatched: 1 } }
  const saved = []
  const page = await run(ex, 'bafyrun', { ...ctx(), download: (name, text) => saved.push([name, text]) })
  await frame(); await frame()
  const cell = page.querySelectorAll('[data-collection]')[1].querySelector('[data-files]')
  await cell.querySelector('[data-index-stable]').click()
  await frame()
  assert.deepEqual(asked, ['cq'])
  assert.deepEqual(saved, [['quant.json', '[cas://bafkfile/q1.bam]']])
  assert.match(cell.querySelector('[data-index-status]').textContent, /1 path left as Nextflow wrote it/)
  ex.stableIndex = async () => { throw new Error('block bafyindex is not in this member') }
  await cell.querySelector('[data-index-stable]').click()
  await frame()
  assert.match(cell.querySelector('[data-index-status]').textContent, /Could not prepare the index file: block bafyindex/)
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
