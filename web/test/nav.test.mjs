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
