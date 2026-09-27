import { test } from 'node:test'
import assert from 'node:assert/strict'
import { saveSequence, retryRestore, bannerText, bannerFrom, refreshFailureLines } from '../src/save-flow.js'

const fail = (code) => Object.assign(new Error(code), { code })
function fakeWriter({ renameFails, undoFails } = {}) {
  const calls = []
  return {
    calls,
    selection: async (members) => { calls.push(['selection']); return { address: 'bafyS' } },
    rename: async (s, name, sup) => { calls.push(['rename', s, name, sup]); if (renameFails) throw fail(renameFails) },
    undo: async (s, sup) => { calls.push(['undo', s, sup]); if (undoFails) throw fail(undoFails) },
  }
}
const restoring = { state: 'elsewhere', names: ['n'], prefill: 'n', supersedes: ['bafyN'], deletion: 'deleted', restore: ['bafyD'] }

test('selection, then name, then del, in that order; no failures', async () => {
  const w = fakeWriter()
  let saved = false
  const r = await saveSequence(w, [], restoring, 'n', { onSaved: () => { saved = true } })
  assert.deepEqual(w.calls.map(c => c[0]), ['selection', 'rename', 'undo'])
  assert.deepEqual(r, { address: 'bafyS', failures: [] })
  assert.equal(saved, true)
})

test('a naming failure still tries the del, and reports naming', async () => {
  const w = fakeWriter({ renameFails: 'clock_skew' })
  const r = await saveSequence(w, [], restoring, 'n')
  assert.deepEqual(w.calls.map(c => c[0]), ['selection', 'rename', 'undo'])
  assert.deepEqual(r.failures.map(f => [f.step, f.code]), [['naming', 'clock_skew']])
})

test('both fail: both reported, and the restoring failure carries its retry', async () => {
  const r = await saveSequence(fakeWriter({ renameFails: 'clock_skew', undoFails: 'write_failed' }), [], restoring, 'n')
  assert.deepEqual(r.failures.map(f => f.step), ['naming', 'restoring'])
  assert.deepEqual(r.failures[1].retry, ['bafyD'])
})

test('no name typed and nothing to restore: only the selection write', async () => {
  const w = fakeWriter()
  await saveSequence(w, [], { state: 'new', names: [], prefill: '', supersedes: [], deletion: 'none', restore: [] }, '')
  assert.deepEqual(w.calls.map(c => c[0]), ['selection'])
})

test('the selection write failing throws, and nothing else is tried', async () => {
  const w = fakeWriter()
  w.selection = async () => { throw fail('too_large') }
  await assert.rejects(saveSequence(w, [], restoring, 'n'), { code: 'too_large' })
})

test('retryRestore sends the same del again, and rethrows a second failure', async () => {
  const w = fakeWriter()
  await retryRestore(w, 'bafyS', ['bafyD'])
  assert.deepEqual(w.calls, [['undo', 'bafyS', ['bafyD']]])
  await assert.rejects(retryRestore(fakeWriter({ undoFails: 'write_failed' }), 'bafyS', ['bafyD']), { code: 'write_failed' })
})

test('a carried banner round-trips through JSON with code, message and retry intact', async () => {
  const r = await saveSequence(fakeWriter({ renameFails: 'clock_skew', undoFails: 'write_failed' }), [], restoring, 'n')
  const banner = { address: r.address, failures: r.failures }
  assert.deepEqual(bannerFrom(bannerText(banner)), banner)
  assert.deepEqual(bannerFrom(bannerText(banner)).failures[1], { step: 'restoring', code: 'write_failed', message: 'write_failed', retry: ['bafyD'] })
})

test('refreshFailureLines: one line per failure, naming and restoring worded the same way as the banner, each linking to the saved Selection', () => {
  const banner = { address: 'bafyS', failures: [{ step: 'naming', code: 'clock_skew', message: 'clock skew' },
    { step: 'restoring', code: 'write_failed', message: 'write_failed', retry: ['bafyD'] }] }
  const hrefFor = (hash) => `http://h/m/lab/${hash}`
  assert.deepEqual(refreshFailureLines(banner, hrefFor), [
    { text: 'Saved, but naming it failed (clock_skew).', href: 'http://h/m/lab/#/selection/bafyS' },
    { text: 'Saved, but restoring it failed (write_failed).', href: 'http://h/m/lab/#/selection/bafyS' },
  ])
})

test('an unreadable or empty carried banner reads as none', () => {
  assert.equal(bannerFrom(null), null)
  assert.equal(bannerFrom('not json'), null)
  assert.equal(bannerFrom(JSON.stringify({ address: 'bafyS', failures: [] })), null)
  assert.equal(bannerFrom(JSON.stringify({ failures: [{ step: 'naming', code: 'x', message: 'x' }] })), null)
})
