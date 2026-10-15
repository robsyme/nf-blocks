import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import * as Digest from 'multiformats/hashes/digest'
import { block, rawCid } from './fixture.mjs'
import { cidParts, friendlyOf } from '../src/blockinfo.js'

test('cidParts: a dag-cbor and a raw address decoded', () => {
  const item = block({ kind: 'OutputItem', schema: 2, value: {} }).cid
  const p = cidParts(item.toString())
  assert.deepEqual([p.version, p.codec, p.hash, p.standard], [1, 'dag-cbor', 'sha2-256', true])
  assert.equal(p.digest, [...item.multihash.digest].map(b => b.toString(16).padStart(2, '0')).join(''))
  assert.equal(cidParts(rawCid('x').toString()).codec, 'raw')
})

test('cidParts: an address this store would not write is decoded and marked; a non-CID is schema_invalid', () => {
  const sha512 = CID.create(1, 0x55, Digest.create(0x13, new Uint8Array(64)))
  const p = cidParts(sha512.toString())
  assert.equal(p.hash, '0x13')
  assert.equal(p.standard, false)
  assert.throws(() => cidParts('not-a-cid'), (e) => e.code === 'schema_invalid')
})

test('friendlyOf: each kind names itself and links to its page', () => {
  const c = 'bafyx'
  assert.deepEqual(friendlyOf(c, { kind: 'RunCompletion' }), { kind: 'run', text: 'a run', label: 'Open the run', href: '#/run/bafyx' })
  assert.equal(friendlyOf(c, { kind: 'OutputCollection' }).href, '#/collection/bafyx')
  assert.equal(friendlyOf(c, { kind: 'OutputItem' }).href, '#/item/-/bafyx')
  assert.equal(friendlyOf(c, { kind: 'Selection' }).href, '#/selection/bafyx')
  const subject = block({ kind: 'Selection' }).cid
  assert.equal(friendlyOf(c, { kind: 'Claim', subject, verb: 'set', attribute: 'name' }).href, `#/block/${subject}`)
  assert.equal(friendlyOf(c, { kind: 'DirectoryManifest' }).kind, 'directory')
  assert.equal(friendlyOf(c, { kind: 'Forget' }).href, null)
  assert.equal(friendlyOf(c, null).text, 'a block that does not decode')
})

test('friendlyOf: a Claim with no usable subject has no link (Review Focus 5)', () => {
  assert.equal(friendlyOf('bafyx', { kind: 'Claim', verb: 'set', attribute: 'name' }).href, null)
  assert.equal(friendlyOf('bafyx', { kind: 'Claim', subject: 'not a link', verb: 'set' }).href, null)
})

test('friendlyOf: a RunManifest links to its run when the Store Log tail names it', () => {
  const manifest = block({ kind: 'RunManifest' }).cid
  const stale = [{ cid: 'bafycompletion', completion: { run: manifest } }]
  assert.equal(friendlyOf(manifest.toString(), { kind: 'RunManifest' }, stale).href, '#/run/bafycompletion')
  assert.equal(friendlyOf(manifest.toString(), { kind: 'RunManifest' }, []).href, null)
})
