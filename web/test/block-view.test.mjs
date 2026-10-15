// web/test/block-view.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { block as blockView } from '../src/views/block.js'
import { block as blockOf, rawCid } from './fixture.mjs'

const leaf = rawCid('Tarik Benali from Casablanca, Africa\n')
const item = blockOf({ kind: 'OutputItem', schema: 2, value: { datafile: { kind: 'Leaf', name: 'Tarik_Benali.dat', address: leaf, size: 37, reason: null } } })
const parts = (codec) => ({ version: 1, codec, hash: 'sha2-256', digest: 'ab'.repeat(32), standard: true })
const ctx = (extra = {}) => ({ store: { label: 'roblab', others: (hash) => [{ alias: 'shared', href: `?member=shared${hash}` }] }, ...extra })
const exWith = (info) => ({ block: async () => info, blocks: { urlFor: (c) => `blocks/${c}` } })

function structured(extra = {}) {
  return { cid: item.cid.toString(), parts: parts('dag-cbor'), check: 'verified', size: item.bytes.length, value: { kind: 'OutputItem', schema: 2,
    value: { datafile: { kind: 'Leaf', name: 'Tarik_Benali.dat', address: leaf, size: 37, reason: null } } }, schemaError: null,
    bytes: item.bytes, preview: null, friendly: { kind: 'item', text: 'an item', label: 'Open as an item', href: `#/item/-/${item.cid}` }, message: null, ...extra }
}

test('a structured block: what its address says, the check, and its contents with each link an address', async () => {
  installDom()
  const page = await blockView(exWith(structured()), item.cid.toString(), ctx())
  assert.equal(page.dataset.block, item.cid.toString())
  assert.equal(page.dataset.blockCheck, 'verified')
  assert.equal(page.querySelector('[data-block-kind]').querySelector('a').getAttribute('href'), `#/item/-/${item.cid}`)
  assert.match(page.textContent, /dag-cbor/)
  assert.match(page.textContent, /Read \d+ bytes from roblab; their SHA-256 matches the address/)
  const pre = page.querySelector('pre')
  assert.equal(pre.querySelector('[data-address]').dataset.address, leaf.toString())
  assert.match(pre.textContent, /"name": "Tarik_Benali.dat"/)
})

test('a schema failure is drawn with its reason; a refused block shows nothing from its bytes', async () => {
  installDom()
  const odd = await blockView(exWith(structured({ schemaError: 'block x does not match the OutputItem schema' })), item.cid.toString(), ctx())
  assert.match(odd.querySelector('.warn').textContent, /does not match the OutputItem schema/)
  assert.ok(odd.querySelector('pre'))
  const refused = await blockView(exWith(structured({ check: 'refused', value: null, bytes: null, friendly: null })), item.cid.toString(), ctx())
  assert.equal(refused.dataset.blockCheck, 'refused')
  assert.equal(refused.querySelector('pre'), null)
  assert.match(refused.textContent, /Refused/)
})

test('a missing block says where else to look', async () => {
  installDom()
  const page = await blockView(exWith(structured({ check: 'missing', value: null, bytes: null, friendly: null })), item.cid.toString(), ctx())
  assert.equal(page.dataset.blockCheck, 'missing')
  assert.match(page.textContent, /Not in roblab/)
  assert.equal(page.querySelectorAll('a').find(a => a.textContent === 'Look in shared').getAttribute('href'), `?member=shared#/block/${item.cid}`)
})

test('a raw block: a text preview and Download; unchecked when large; binary named as such', async () => {
  installDom()
  const raw = { cid: leaf.toString(), parts: parts('raw'), check: 'verified', size: 37, value: null, schemaError: null,
    bytes: new TextEncoder().encode('Tarik Benali from Casablanca, Africa\n'), preview: null,
    friendly: { kind: 'file', text: "a file's bytes", label: 'Who produced it', href: `#/content/${leaf}` }, message: null }
  const small = await blockView(exWith(raw), leaf.toString(), ctx())
  assert.match(small.querySelector('pre').textContent, /Tarik Benali from Casablanca/)
  assert.equal(small.querySelector('a[download]').getAttribute('href'), `blocks/${leaf}`)
  const large = await blockView(exWith({ ...raw, check: 'unchecked', size: 6000000, bytes: null, preview: new TextEncoder().encode('head of a big file') }), leaf.toString(), ctx())
  assert.equal(large.dataset.blockCheck, 'unchecked')
  assert.match(large.textContent, /Not checked: checking needs all 6000000 bytes/)
  assert.match(large.querySelector('pre').textContent, /head of a big file/)
  const binary = await blockView(exWith({ ...raw, bytes: new Uint8Array([0, 159, 146, 150]) }), leaf.toString(), ctx())
  assert.match(binary.textContent, /binary, 4 bytes/)
})

test('a preview cut inside a multi-byte character is still text (Review Focus 4)', async () => {
  installDom()
  const cut = new TextEncoder().encode('café').slice(0, 4)   // 'caf' and the first byte of 'é'
  const page = await blockView(exWith({ cid: leaf.toString(), parts: parts('raw'), check: 'unchecked', size: 9000000, value: null, schemaError: null,
    bytes: null, preview: cut, friendly: { kind: 'file', text: "a file's bytes", label: null, href: null }, message: null }), leaf.toString(), ctx())
  assert.match(page.querySelector('pre').textContent, /^caf/)
  assert.doesNotMatch(page.textContent, /binary/)
})

test('a long array draws its first 200 entries and says how many more (Review Focus 3)', async () => {
  installDom()
  const items = Array.from({ length: 450 }, (_, i) => rawCid(`i${i}`))
  const page = await blockView(exWith(structured({ value: { kind: 'OutputCollection', items } })), item.cid.toString(), ctx())
  assert.equal(page.querySelector('pre').querySelectorAll('[data-address]').length, 200)
  assert.match(page.querySelector('pre').textContent, /\.\.\. 250 more/)
})

test('an address this store would not write is marked', async () => {
  installDom()
  const page = await blockView(exWith(structured({ parts: { ...parts('dag-cbor'), hash: '0x13', standard: false } })), item.cid.toString(), ctx())
  assert.match(page.textContent, /Not an address this store writes/)
})

test('a large bytes field is drawn without overflowing the call stack', async () => {
  installDom()
  const page = await blockView(exWith(structured({ value: { kind: 'OutputItem', blob: new Uint8Array(300000).fill(7) } })), item.cid.toString(), ctx())
  assert.ok(page.querySelector('pre').textContent.includes('{"/": {"bytes": "'))
})
