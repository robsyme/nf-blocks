import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { Tray, UNSAVED_NOTE, trayNote } from '../src/tray.js'
import { block } from './fixture.mjs'

const I1 = block({ n: 1 }).cid.toString()
const I2 = block({ n: 2 }).cid.toString()
const C1 = block({ c: 1 }).cid.toString()
const C2 = block({ c: 2 }).cid.toString()

function memoryStorage() {
  const m = new Map()
  return { getItem: k => m.get(k) ?? null, setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k) }
}

test('an item picked twice is one entry with both vias (Review Focus 2)', () => {
  const t = new Tray(null)
  t.add({ address: I1, via: [C2], kind: 'item' })
  t.add({ address: I1, via: [C1], kind: 'item' })
  assert.equal(t.size, 1)
  assert.deepEqual(t.entries()[0].via, [C1, C2].sort())
})

test('toMembers is DAG-JSON-ready: CID objects, sorted by address', () => {
  const t = new Tray(null)
  t.add({ address: I2, via: [], kind: 'item' })
  t.add({ address: I1, via: [C1], kind: 'selection' })
  const addresses = t.toMembers().map(m => m.item?.address ?? m.selection)
  assert.ok(addresses.every(a => CID.asCID(a) !== null))
  assert.deepEqual(addresses.map(String), [I1, I2].sort())
  assert.deepEqual(t.toMembers().map(m => (m.item ? 'item' : 'selection')), [I1, I2].sort().map(a => (a === I1 ? 'selection' : 'item')))
})

test('a tray holding only one Selection is flagged, since the explorer does not offer that Selection', () => {
  const t = new Tray(null)
  t.add({ address: I1, via: [], kind: 'selection' })
  assert.equal(t.onlyOneSelection(), true)
  t.add({ address: I2, via: [], kind: 'item' })
  assert.equal(t.onlyOneSelection(), false)
})

test('the tray survives a reload through its storage, and works without one', () => {
  const storage = memoryStorage()
  new Tray(storage).add({ address: I1, via: [C1], kind: 'item' })
  assert.deepEqual(new Tray(storage).entries().map(e => e.address), [I1])
  const broken = { getItem: () => { throw new Error('denied') }, setItem: () => { throw new Error('denied') } }
  const t = new Tray(broken)
  t.add({ address: I1, via: [], kind: 'item' })
  assert.equal(t.size, 1)
})

test('addMany merges vias as add does and writes storage once', () => {
  const writes = []
  const storage = { getItem: () => null, setItem: (k, v) => writes.push([k, v]) }
  const t = new Tray(storage)
  t.addMany([{ address: I1, via: [C1] }, { address: I2 }, { address: I1, via: [C2] }])
  assert.equal(t.size, 2)
  assert.deepEqual(t.entries().find(e => e.address === I1).via, [C1, C2].sort())
  assert.equal(writes.length, 1)
})

test('a storage write the browser refuses is reported, so the page can say the tray will not survive a reload', () => {
  const full = { getItem: () => null, setItem: () => { throw new Error('QuotaExceededError') } }
  const t = new Tray(full)
  assert.equal(t.saved, true, 'nothing to lose yet')
  assert.equal(trayNote(t), '')
  t.addMany([{ address: I1, via: [C1] }, { address: I2 }])
  assert.equal(t.size, 2, 'the tray still works for this page')
  assert.equal(t.saved, false)
  assert.equal(t.persist(), false)
  assert.equal(trayNote(t), UNSAVED_NOTE)
  assert.match(UNSAVED_NOTE, /could not be saved in this browser/)
  assert.match(UNSAVED_NOTE, /lost on reload/)
})

test('a write that succeeds again clears the note', () => {
  let refuse = true
  const m = new Map()
  const storage = { getItem: k => m.get(k) ?? null, setItem: (k, v) => { if (refuse) throw new Error('full'); m.set(k, v) } }
  const t = new Tray(storage)
  t.add({ address: I1, via: [], kind: 'item' })
  assert.equal(t.saved, false)
  refuse = false
  t.remove(I1)
  assert.equal(t.saved, true)
  assert.equal(trayNote(t), '')
})
