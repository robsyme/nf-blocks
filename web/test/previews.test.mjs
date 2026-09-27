import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Previews } from '../src/previews.js'
import { BlockError } from '../src/blocks.js'

const tick = () => new Promise((resolve) => setImmediate(resolve))

/** An `ex` whose item() answers only when the test says so, recording the most it had open at once. */
function fakeEx() {
  const open = []
  const asked = []
  let most = 0
  const ex = {
    item(collectionCid, itemCid) {
      asked.push([collectionCid, itemCid])
      return new Promise((resolve, reject) => {
        open.push({ itemCid, resolve: () => resolve({ view: { sample: itemCid, lane: 1 }, leaves: [{ name: `${itemCid}.bam` }] }), reject })
        most = Math.max(most, open.length)
      })
    },
  }
  const settle = async (n = open.length, how = 'resolve') => {
    for (const o of open.splice(0, n)) {
      if (how === 'resolve') o.resolve()
      else o.reject(new BlockError('block_missing', o.itemCid, `block ${o.itemCid} is not in this member`))
    }
    await tick()
  }
  return { ex, asked, open, most: () => most, settle }
}

test('at most six previews are fetched at once; the rest wait their turn (ticket 03 Q3)', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  for (let i = 0; i < 10; i++) p.ask('coll', `i${i}`)
  assert.deepEqual(p.stats, { fetched: 0, failed: 0, inFlight: 6, queued: 4 })
  assert.equal(f.asked.length, 6)
  await f.settle(1)
  assert.deepEqual(p.stats, { fetched: 1, failed: 0, inFlight: 6, queued: 3 })
  while (f.open.length) await f.settle()
  assert.deepEqual(p.stats, { fetched: 10, failed: 0, inFlight: 0, queued: 0 })
  assert.equal(f.most(), 6)
  assert.deepEqual(p.get('i3'), {
    pairs: [{ path: 'lane', type: 'int', values: ['1'], types: ['int'] }, { path: 'sample', type: 'string', values: ['i3'], types: ['string'] }],
    files: ['i3.bam'] })
})

test('askFirst asks for the first hundred rows only; the rest wait for a click (ticket 03 Q3)', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  const rows = Array.from({ length: 150 }, (_, i) => ({ collection: 'coll', item: `i${i}` }))
  assert.equal(p.cap, 100)
  assert.equal(p.askFirst(rows), 100)
  while (f.open.length) await f.settle()
  assert.equal(f.asked.length, 100)
  assert.equal(p.get('i99')?.files[0], 'i99.bam')
  assert.equal(p.get('i100'), undefined)
  p.ask('coll', 'i100')
  await f.settle()
  assert.equal(p.get('i100')?.files[0], 'i100.bam')
  assert.equal(new Previews(f.ex, { cap: 2 }).askFirst(rows), 2)
})

test('an item asked for twice, or again once it has arrived, is fetched once', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  p.ask('coll', 'i0')
  p.ask('coll', 'i0')
  p.ask(null, 'i0')
  await f.settle()
  p.ask('coll', 'i0')
  assert.deepEqual(f.asked, [['coll', 'i0']])
})

test('a preview that fails is recorded with its code and counted, and listeners hear of it', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  const told = []
  p.onChange((itemCid) => told.push(itemCid))
  p.ask(null, 'gone')
  await f.settle(1, 'reject')
  assert.equal(p.get('gone').error, 'block_missing')
  assert.deepEqual(p.stats, { fetched: 0, failed: 1, inFlight: 0, queued: 0 })
  assert.deepEqual(told, ['gone'])
  assert.deepEqual(f.asked, [[null, 'gone']])
  const throwing = new Previews({ item: () => { throw new Error('boom') } })
  throwing.ask(null, 'x')
  await tick()
  assert.deepEqual(throwing.get('x'), { error: 'query_failed', message: 'boom' })
})

test('cancel drops the queue and keeps what is in flight; an unsubscribed listener hears nothing', async () => {
  const f = fakeEx()
  const p = new Previews(f.ex)
  const told = []
  const off = p.onChange((itemCid) => told.push(itemCid))
  for (let i = 0; i < 8; i++) p.ask('coll', `i${i}`)
  p.cancel()
  assert.deepEqual(p.stats, { fetched: 0, failed: 0, inFlight: 6, queued: 0 })
  off()
  while (f.open.length) await f.settle()
  assert.equal(p.stats.fetched, 6)
  assert.deepEqual(told, [])
  assert.equal(p.get('i7'), undefined)
  p.ask('coll', 'i7')
  assert.equal(p.stats.inFlight, 1)
})
