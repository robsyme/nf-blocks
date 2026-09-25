// Blocks as the page may use them: fetched from <base>blocks/<xx>/<cid>, hashed
// against the address they were asked for, then decoded and schema-checked,
// or refused (spec section 5.3, DESIGN.md §15).
import * as dagCbor from '@ipld/dag-cbor'
import { blockPath, verifies } from './cid.js'
import { validBlock, validLeaf } from './schema.js'
import { leavesOf } from './metadata.js'

export class BlockError extends Error {
  constructor(code, cid, message) { super(message); this.code = code; this.cid = cid }
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
    if (!validBlock(value) || (value.kind === 'OutputItem' && !leavesOf(value.value).every(validLeaf)))
      throw new BlockError('schema_invalid', cidText, `block ${cidText} does not match the ${value?.kind ?? 'Block'} schema (DESIGN.md §6)`)
    return { cid: cidText, bytes, value }
  }
}
