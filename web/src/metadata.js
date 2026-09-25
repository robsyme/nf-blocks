// The metadata view and its item_attr rows, a port of MetadataView.groovy
// (DESIGN.md §12), so query 3 over a stale run matches what the index would.
import { sha256 } from '@noble/hashes/sha2.js'
import { Float } from './typed.js'

export const VALUE_CAP_BYTES = 1024

export class PredicateError extends Error {}

const isCid = (v) => v !== null && typeof v === 'object' && v['/'] === v.bytes && typeof v.toV1 === 'function'
const isMap = (v) => v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Uint8Array) && !(v instanceof Float) && !isCid(v)

export const isLeaf = (v) => isMap(v) && v.kind === 'Leaf'

export function metadataView(value) {
  if (isLeaf(value)) return null
  if (isMap(value)) return value
  if (Array.isArray(value)) for (const e of value) if (isMap(e) && !isLeaf(e)) return e
  return null
}

export function leavesOf(value, out = []) {
  if (isLeaf(value)) out.push(value)
  else if (Array.isArray(value)) value.forEach(v => leavesOf(v, out))
  else if (isMap(value)) Object.values(value).forEach(v => leavesOf(v, out))
  return out
}

/** Java's Double.toString, which is what Groovy's toString of a Double gives. */
export function javaDouble(x) {
  if (Object.is(x, -0)) return '-0.0'
  if (x === 0) return '0.0'
  const abs = Math.abs(x)
  if (abs >= 1e-3 && abs < 1e7) {
    const s = String(x)
    return s.includes('.') ? s : `${s}.0`
  }
  let [mantissa, exponent] = x.toExponential().split('e')
  // Java keeps at least two significant digits, rounding to the nearer one.
  if (!mantissa.includes('.')) [mantissa, exponent] = x.toExponential(1).split('e')
  return `${mantissa}E${Number(exponent)}`
}

function textRow(path, text) {
  const bytes = new TextEncoder().encode(text)
  if (bytes.length <= VALUE_CAP_BYTES) return { path, type: 'string', value: text, truncated: 0 }
  const hex = [...sha256(bytes)].map(b => b.toString(16).padStart(2, '0')).join('')
  return { path, type: 'string', value: `sha256:${hex}`, truncated: 1 }
}

function scalarRow(path, v) {
  if (v === null || v === undefined) return { path, type: 'null', value: null, truncated: 0 }
  if (typeof v === 'boolean') return { path, type: 'bool', value: String(v), truncated: 0 }
  if (v instanceof Float) return { path, type: 'float', value: javaDouble(v.value), truncated: 0 }
  if (typeof v === 'number' || typeof v === 'bigint') return { path, type: 'int', value: String(v), truncated: 0 }
  return textRow(path, String(v))
}

export function attrRows(view) {
  const rows = []
  const walk = (value, path) => {
    if (isLeaf(value)) return
    if (Array.isArray(value)) return value.forEach(v => walk(v, path))
    if (isMap(value)) return Object.entries(value).forEach(([k, v]) => walk(v, path ? `${path}.${k}` : k))
    rows.push(scalarRow(path, value))
  }
  if (isMap(view) && !isLeaf(view)) Object.entries(view).forEach(([k, v]) => walk(v, k))
  return rows
}

/** The row a typed predicate from the page must equal, as MetadataView.scalar would type the value. */
export function predicateRow(path, type, text) {
  switch (type) {
    case 'string': return textRow(path, text)
    case 'int':
      if (!/^-?\d+$/.test(text)) throw new PredicateError(`'${text}' is not a whole number`)
      return { path, type, value: BigInt(text).toString(), truncated: 0 }
    case 'float': {
      const x = Number(text)
      if (text.trim() === '' || !Number.isFinite(x)) throw new PredicateError(`'${text}' is not a finite number`)
      return { path, type, value: javaDouble(x), truncated: 0 }
    }
    case 'bool':
      if (text !== 'true' && text !== 'false') throw new PredicateError(`'${text}' is not true or false`)
      return { path, type, value: text, truncated: 0 }
    case 'null': return { path, type, value: null, truncated: 0 }
    default: throw new PredicateError(`unknown type '${type}'; use string, int, float, bool or null`)
  }
}

export function matches(rows, predicates) {
  return predicates.every(p => rows.some(r => r.truncated === 0 && r.path === p.path && r.type === p.type &&
    (p.value === null ? r.value === null : r.value === p.value)))
}
