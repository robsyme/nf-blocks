// The one retention panel (P0 final fix wave, findings 4 and 5): pins as claim
// tags with who and when, Pin naming its bytes and waiting for a reason and
// those bytes, and a released run that references its bytes rather than holding them.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { pinsList, retentionPanel } from '../src/views/common.js'
import { run } from '../src/views/run.js'
import { resetFolds } from '../src/folds.js'
import { FAKE_STORAGE } from './helpers.mjs'

const PIN = { cid: 'bafyreipin', subject: 'bafysubject', verb: 'add', attribute: 'pin', value: 'figure 3', supersedes: [],
  asserted_by: 'ada.lovelace@example.org', timestamp: '2026-10-24T10:00:00Z' }
const RELEASE = { cid: 'bafyreirel', subject: 'bafyrun', verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [],
  asserted_by: 'ada.lovelace@example.org', timestamp: '2026-10-24T11:00:00Z' }

function ctxFor(calls = []) {
  return { tray: new Tray(null), trayChanged: () => {}, rerender: () => {}, progress: () => {}, use: () => {}, mark: () => {},
    store: { name: 'roblab', key: 'a' },
    write: { available: true, here: true, writable: 'roblab', hrefFor: (h) => h,
      writer: { pin: async (s, n) => { calls.push(['pin', s, n]) }, unpin: async () => {}, release: async () => {}, restore: async () => {} },
      run: async (status, fn) => { await fn() } } }
}

test('a pin is a claim tag: who pinned it, the note and its age, with its own Unpin', () => {
  installDom()
  const node = retentionPanel('bafysubject', claimState([PIN]), ctxFor(), { kind: 'item', size: { bytes: 5_290_000, partial: false } })
  const pin = node.querySelector('[data-pin]')
  assert.equal(pin.dataset.claim, PIN.cid)
  const tag = pin.querySelector('.claim')
  assert.ok(tag, 'a claim tag')
  assert.equal(tag.querySelector('.av').textContent, 'AL')
  assert.match(tag.textContent, /pinned/)
  assert.match(tag.textContent, /figure 3/)
  assert.ok(tag.querySelector('.age'))
  assert.ok(pin.querySelector(`[data-unpin="${PIN.cid}"]`))
  assert.ok(node.querySelector('[data-badge="pinned"]'))
  assert.equal(node.querySelector('#pin').textContent, 'Pin 5.29 MB in roblab')
})

test('Pin waits for both a reason and the bytes it keeps', async () => {
  installDom()
  const calls = []
  let answer
  const size = new Promise((resolve) => { answer = resolve })
  const node = retentionPanel('bafysubject', claimState([]), ctxFor(calls), { kind: 'Selection', size })
  const pin = node.querySelector('#pin')
  const note = node.querySelector('#pin-note')
  assert.equal(pin.textContent, 'Pin in roblab')
  note.value = 'figure 3'; note.dispatchEvent(new Event('input'))
  assert.equal(pin.disabled, true, 'a reason, but no bytes yet')
  answer({ bytes: 1_135_852, partial: true })
  await frame()
  assert.equal(pin.textContent, 'Pin at least 1.14 MB in roblab')
  assert.equal(pin.disabled, false)
  note.value = ' '; note.dispatchEvent(new Event('input'))
  assert.equal(pin.disabled, true, 'bytes, but no reason')
})

function runEx(claims) {
  return { ...FAKE_STORAGE,
    run: async () => ({ row: { run_name: 'distraught_murdock', pipeline: 'nf-core/rnaseq', nf_run_hash: null }, collections: [],
      completion: { status: 'succeeded', possibly_incomplete: false, finished_at: '2026-10-24T13:29:00Z', anomalies: {}, error: null },
      state: { ...claimState(claims), claims } }),
    runSize: async () => ({ bytes: 78_400_000, blocks: 894, partial: false }),
    latestSuccessfulRun: async () => null, runIdentity: async () => ({}), collection: async () => ({ total: 0 }),
    blocks: { urlFor: (a) => a },
  }
}

test('a released run references its bytes, and Pin names none, since a sweep may have taken them', async () => {
  installDom(); resetFolds()
  const page = await run(runEx([RELEASE]), 'bafyrun', ctxFor(), { tab: 'storage' })
  await frame()
  const said = page.querySelector('[data-run-size]').textContent
  assert.match(said, /^This run references 78\.4 MB \(894 file blocks\) in roblab\./)
  assert.doesNotMatch(said, /This run holds/)
  assert.equal(page.querySelector('#pin').textContent, 'Pin in roblab')
  assert.ok(page.querySelector('#restore'))
  assert.match(page.querySelector('[data-slot=size]').textContent, /78\.4 MB referenced/)
  const held = await run(runEx([]), 'bafyrun', ctxFor(), { tab: 'storage' })
  await frame()
  assert.match(held.querySelector('[data-run-size]').textContent, /^This run holds 78\.4 MB/)
  assert.equal(held.querySelector('#pin').textContent, 'Pin 78.4 MB in roblab')
  assert.match(held.querySelector('[data-slot=size]').textContent, /78\.4 MB stored/)
})

test('the content released badge is a claim tag with who and when, and keeps its hooks', () => {
  installDom()
  const node = retentionPanel('bafyrun', claimState([RELEASE]), ctxFor(), { isRun: true, kind: 'run' })
  const badge = node.querySelector('[data-badge="content-released"]')
  assert.ok(badge)
  const tag = badge.querySelector('.claim')
  assert.ok(tag, 'a claim tag inside the badge')
  assert.match(tag.textContent, /released/)
  assert.equal(tag.querySelector('.av').getAttribute('title'), RELEASE.asserted_by)
  assert.ok(tag.querySelector('.age'))
})

test('pinsList is shared: read-only gives claim tags and no Unpin', () => {
  installDom()
  const st = claimState([PIN])
  const ro = pinsList(st, { can: false })
  assert.equal(ro.querySelector('[data-pin]').dataset.claim, PIN.cid)
  assert.ok(ro.querySelector('[data-pin]').querySelector('.claim'))
  assert.equal(ro.querySelector('[data-unpin]'), null)
  const rw = pinsList(st, { can: true, onUnpin: () => {} })
  assert.ok(rw.querySelector(`[data-unpin="${PIN.cid}"]`))
  assert.equal(pinsList(claimState([]), { can: false }), null)
})
