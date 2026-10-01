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
