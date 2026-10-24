// web/src/slots.js
// An OutputItem value's file slots (items file columns spec §6.1): every Leaf,
// stored or not, with the column it belongs to (a record's top-level key, or
// a tuple's position as `[i]`) and its field path inside the item. Pure.
import { CID } from 'multiformats/cid'
import { DAG_CBOR } from './cid.js'
import { isLeaf } from './metadata.js'

/** The codec of a leaf address given as a CID or as its text; null when it is neither. */
export const codeOf = (a) => { try { return (CID.asCID(a) ?? CID.parse(String(a))).code } catch { return null } }

const slotOf = (leaf, column, path) => ({
  column, path, name: leaf.name ?? null, size: leaf.size ?? null,
  address: leaf.address ? String(leaf.address) : null,
  dir: leaf.address ? codeOf(leaf.address) === DAG_CBOR : false,
  reason: leaf.reason ?? null,
})

function walk(value, column, path, out) {
  if (isLeaf(value)) out.push(slotOf(value, column, path))
  else if (Array.isArray(value)) value.forEach((v, i) => walk(v, column, `${path}[${i}]`, out))
  else if (value !== null && typeof value === 'object' && !(value instanceof Uint8Array) && !CID.asCID(value)) {
    for (const [k, v] of Object.entries(value)) walk(v, column, `${path}.${k}`, out)
  }
}

/** Every Leaf in `value`, in the value's own order, as `{column, path, name, size, address, dir, reason}`. */
export function fileSlots(value) {
  const out = []
  if (isLeaf(value)) out.push(slotOf(value, '[0]', '[0]'))
  else if (Array.isArray(value)) value.forEach((v, i) => walk(v, `[${i}]`, `[${i}]`, out))
  else if (value !== null && typeof value === 'object') for (const [k, v] of Object.entries(value)) walk(v, k, k, out)
  return out
}
