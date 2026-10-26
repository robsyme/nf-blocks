// web/src/views/block.js
// #/block/<cid> (visual language spec §4): any block by its address, with
// what the address says, the check the browser made, and its contents, each
// link inside them an address to its own block page.
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'
import { h, link, copyOutcome, saveText } from '../html.js'
import { address, icon } from '../icons.js'
import { humanBytes } from '../words.js'
import { presenceOf } from '../presence.js'

const FORMAT = {
  'dag-cbor': 'structured data, which can link to other blocks',
  raw: 'bytes exactly as the file held them; no name, no links',
}
const ARRAY_SHOWN = 200

/**
 * The first `limit` bytes of a raw block for a preview, and its whole size:
 * `{ bytes, size, truncated, verified }`. A block of `limit` bytes or fewer is
 * read whole and hash-checked; a larger one comes back by Range, one byte past
 * `limit` so a reader can tell it was cut, and unchecked. `bytes` is null when
 * the server ignores Range on a large block.
 */
export async function readHead(ex, cid, limit = 65536) {
  const head = await ex.blocks.partial(cid, limit + 1)
  if (head.size !== null && head.size <= limit) {
    const bytes = await ex.blocks.raw(cid)
    return { bytes, size: bytes.length, truncated: false, verified: true }
  }
  if (head.size === null && head.bytes && head.bytes.length <= limit) return { bytes: head.bytes, size: head.bytes.length, truncated: false, verified: false }
  return { bytes: head.bytes, size: head.size, truncated: true, verified: false }
}
const shortHex = (hex) => (hex.length > 16 ? `${hex.slice(0, 8)}...${hex.slice(-8)}` : hex)

export async function block(ex, cidText, ctx) {
  const b = await ex.block(cidText)
  const copy = h('button', { type: 'button', onclick: async (e) => { e.currentTarget.textContent = await copyOutcome(globalThis.navigator?.clipboard, cidText) } }, 'Copy')
  const read = b.check === 'verified' || b.check === 'unchecked'
  return h('section', { 'data-block': cidText, 'data-block-check': b.check },
    h('h1', { class: 'block-cid' }, icon('address'), h('code', {}, cidText), ' ', copy, ' ', presenceOf(ctx).dots(cidText, { hereHeld: b.check !== 'missing' })),
    kindLine(b),
    h('h2', {}, 'What the address says'), partsList(b),
    h('h2', {}, 'Checked'), checkLine(b, ctx),
    read ? [h('h2', {}, 'Contents'), contents(ex, b, ctx)] : null)
}

function kindLine(b) {
  if (!b.friendly) return h('p', { class: 'muted' }, 'Not read, so what it holds is unknown.')
  const f = b.friendly
  return h('p', { 'data-block-kind': typeof b.value?.kind === 'string' ? b.value.kind : b.parts.codec }, icon(f.kind), `This is ${f.text}.`, f.href ? [' ', link(f.href, f.label)] : null)
}

function partsList(b) {
  const p = b.parts
  const of = b.size === null ? 'its bytes' : `its ${b.size} bytes`
  return h('dl', { class: 'block-parts' },
    h('dt', {}, 'format'), h('dd', {}, h('code', {}, p.codec), FORMAT[p.codec] ? ` ${FORMAT[p.codec]}` : ''),
    h('dt', {}, 'fingerprint'), h('dd', {}, h('code', {}, p.hash), ` of ${of}: `, h('code', { title: p.digest }, shortHex(p.digest))),
    h('dt', {}, 'so'), h('dd', {}, 'Any copy of these bytes, anywhere, has this address; change one byte and it changes.',
      p.codec === 'raw' ? ' Two runs that write the same bytes share this one block.' : ''),
    p.standard ? null : [h('dt', {}, 'note'), h('dd', { class: 'warn' }, 'Not an address this store writes (DESIGN.md §3).')])
}

function checkLine(b, ctx) {
  const from = ctx.store?.label ?? 'this store'
  if (b.check === 'verified') return h('p', { class: 'ok' }, `✓ Read ${b.size} bytes from ${from}; their SHA-256 matches the address. Checked in your browser.`)
  if (b.check === 'unchecked') {
    return h('p', { class: 'muted' }, b.size === null
      ? 'Not checked: this server did not say how large the block is, and checking needs every byte.'
      : `Not checked: checking needs all ${b.size} bytes (${humanBytes(b.size)}).`)
  }
  if (b.check === 'refused') return h('p', { class: 'warn' }, 'Refused: these bytes do not hash to the address, so nothing from them is shown.')
  const others = ctx.store?.others?.(`#/block/${b.cid}`) ?? []
  return h('p', { class: 'warn' }, `Not in ${from}; it may be held in another store.`, others.map(o => [' ', link(o.href, `Look in ${o.alias}`)]))
}

function contents(ex, b, ctx) {
  if (b.parts.codec === 'raw') {
    const bytes = b.bytes ?? b.preview
    return [
      bytes ? preview(bytes, b.bytes === null) : h('p', { class: 'muted' }, 'No preview: this server ignores Range.'),
      h('p', { class: 'muted' }, b.bytes || !b.preview ? '' : `The first ${b.preview.length} bytes. `, h('a', { href: ex.blocks.urlFor(b.cid), download: '' }, 'Download')),
    ]
  }
  if (b.value === null) return h('p', { class: 'warn' }, b.schemaError ?? 'These bytes do not decode as DAG-CBOR.')
  const save = (ctx.download ?? saveText)
  return [
    b.schemaError ? h('p', { class: 'warn' }, b.schemaError) : null,
    h('pre', { class: 'dag-json' }, json(b.value, 0)),
    h('p', { class: 'muted' }, 'Each address in it is a link: a block names the blocks it depends on by their addresses. ',
      h('a', { href: ex.blocks.urlFor(b.cid), download: `${b.cid}.cbor` }, 'Raw bytes'), ' ',
      h('button', { type: 'button', class: 'link', onclick: () => save(`${b.cid}.json`, new TextDecoder().decode(dagJson.encode(b.value))) }, 'DAG-JSON')),
  ]
}

/** Text when the bytes decode as UTF-8 with no NUL, else "binary"; `truncated` lets the last character be cut off. */
function preview(bytes, truncated) {
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes, { stream: truncated })
    if (!text.includes('\0')) return h('pre', { class: 'scroll' }, text)
  } catch { /* not UTF-8 */ }
  return h('p', { class: 'muted' }, `binary, ${bytes.length} bytes`)
}

/** Base64 in chunks: spreading a large array as call arguments overflows the stack. */
function base64(bytes) {
  let binary = ''
  for (let i = 0; i < bytes.length; i += 0x8000) binary += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000))
  return btoa(binary)
}

/** DAG-JSON drawn from the decoded value, each link an address; an array past ARRAY_SHOWN is cut with a count. */
function json(value, depth) {
  const pad = '  '.repeat(depth + 1)
  const end = '  '.repeat(depth)
  const cid = CID.asCID(value)
  if (cid) return ['{"/": ', address(cid.toString()), '}']
  if (value instanceof Uint8Array) return [`{"/": {"bytes": "${base64(value)}"}}`]
  if (Array.isArray(value)) {
    if (!value.length) return ['[]']
    const shown = value.slice(0, ARRAY_SHOWN)
    const more = value.length - shown.length
    return ['[\n', shown.flatMap((v, i) => [pad, ...json(v, depth + 1), i < shown.length - 1 || more ? ',\n' : '\n']),
      more ? `${pad}... ${more} more\n` : '', end, ']']
  }
  if (value !== null && typeof value === 'object') {
    const keys = Object.keys(value)
    if (!keys.length) return ['{}']
    return ['{\n', keys.flatMap((k, i) => [pad, JSON.stringify(k), ': ', ...json(value[k], depth + 1), i < keys.length - 1 ? ',\n' : '\n']), end, '}']
  }
  return [typeof value === 'bigint' ? String(value) : JSON.stringify(value)]
}
