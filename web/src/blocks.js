// Blocks as the page may use them: fetched from <base>blocks/<xx>/<cid>, hashed
// against the address they were asked for, then decoded and schema-checked,
// or refused (spec section 5.3, DESIGN.md §15).
import * as dagCbor from '@ipld/dag-cbor'
import { CID } from 'multiformats/cid'
import { blockPath, verifies } from './cid.js'
import { validBlock, validLeaf, validLeafV1 } from './schema.js'
import { leavesOf } from './metadata.js'

export class BlockError extends Error {
  constructor(code, cid, message) { super(message); this.code = code; this.cid = cid }
}

const PROVIDERS = new Set(['head-node', 'fusion-node', 's3-copy'])

// What the IPLD Schema cannot say (DESIGN.md §6, ticket 16): an OutputItem's
// leaves are Leaf at schema 2 and LeafV1 at schema 1; a RunCompletion carries
// providers exactly when it is at schema 2, with known provider names; a
// DirectoryManifest at schema 2 carries no executable entry.
function validKind(value) {
  if (value.kind === 'OutputItem') {
    if (value.schema === 2) return leavesOf(value.value).every(validLeaf)
    if (value.schema === 1) return leavesOf(value.value).every(validLeafV1)
    return false
  }
  if (value.kind === 'RunCompletion') {
    if (value.schema === 1) return value.providers === undefined
    return value.schema === 2 && value.providers !== undefined && Object.keys(value.providers).every(k => PROVIDERS.has(k))
  }
  if (value.kind === 'DirectoryManifest') {
    if (value.schema === 1) return true
    return value.schema === 2 && value.entries.every(e => e.mode !== 'executable')
  }
  return true
}

export class BlockFetcher {
  constructor(base, { fetchFn = (...a) => fetch(...a) } = {}) {
    this.base = base
    this.fetchFn = fetchFn
    this.cache = new Map()
    this.verified = []
    this.fetches = 0
  }

  get(cidText) {
    if (!this.cache.has(cidText))
      this.cache.set(cidText, this.load(cidText).catch((e) => { this.cache.delete(cidText); throw e }))
    return this.cache.get(cidText)
  }

  async ofKind(cidText, kind) {
    const block = await this.get(cidText)
    if (block.value?.kind !== kind)
      throw new BlockError('schema_invalid', cidText, `block ${cidText} is a ${block.value?.kind ?? 'value with no kind'}, not a ${kind}`)
    return block
  }

  async load(cidText) {
    // A Store Log name can match the entry pattern and still not be a CID.
    try {
      CID.parse(cidText)
    } catch (e) {
      throw new BlockError('schema_invalid', cidText, `${cidText} is not a CID: ${e.message}`)
    }
    let res
    try {
      res = await this.fetchFn(new URL(blockPath(cidText), this.base).href)
    } catch (e) {
      throw new BlockError('fetch_failed', cidText, `could not fetch block ${cidText}: ${e.message}`)
    }
    this.fetches++
    if (res.status === 404 || res.status === 403)
      throw new BlockError('block_missing', cidText, `block ${cidText} is not in this member; it may be held in another one`)
    if (!res.ok) throw new BlockError('fetch_failed', cidText, `block ${cidText}: HTTP ${res.status}`)
    const bytes = new Uint8Array(await res.arrayBuffer())
    if (!verifies(cidText, bytes))
      throw new BlockError('hash_mismatch', cidText, `block ${cidText} does not hash to its address; it was refused`)
    this.verified.push(cidText)
    let value
    try {
      value = dagCbor.decode(bytes)
    } catch (e) {
      throw new BlockError('schema_invalid', cidText, `block ${cidText} is not DAG-CBOR: ${e.message}`)
    }
    if (!validBlock(value) || !validKind(value))
      throw new BlockError('schema_invalid', cidText, `block ${cidText} does not match the ${value?.kind ?? 'Block'} schema (DESIGN.md §6)`)
    return { cid: cidText, bytes, value }
  }
}
