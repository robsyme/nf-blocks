// web/src/seal.js
// The Seal: a deterministic glyph drawn from a CID. This is an exact port of
// seal() in docs/design/mockups/src/kit.js, which is normative: the same CID
// must give the same SVG string here, in exports and in the mockups
// (test/seal.test.mjs compares the two byte for byte). Change it there first.
//   1. Raw file block if the CID starts with "bafk", else a record.
//   2. seed = FNV-1a 32-bit over cid.slice(4); PRNG = mulberry32(seed).
//   3. 5x5 grid mirrored left to right: rows 0..4, columns 0..2, a cell when rng() < 0.5.
//   4. Raw blocks draw rounded squares, records draw circles.
//   5. One more rng() picks the corner of a small accent dot.
import { h } from './html.js'

function fnv1a(str) {
  let hash = 2166136261 >>> 0
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i)
    hash = Math.imul(hash, 16777619) >>> 0
  }
  return hash
}

function mulberry32(a) {
  return function () {
    a |= 0
    a = a + 0x6D2B79F5 | 0
    let t = Math.imul(a ^ a >>> 15, 1 | a)
    t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t
    return ((t ^ t >>> 14) >>> 0) / 4294967296
  }
}

const fixed = (n) => n.toFixed(2)

// The label's cid slices go into an attribute of an innerHTML string, and a
// route like #/project/<anything> hands this any text. A real cid has none of
// these characters, so its SVG stays byte for byte the kit's.
const ESCAPES = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }
const escaped = (text) => text.replace(/[&<>"']/g, c => ESCAPES[c])

/** The Seal of `cid` as an SVG string, `size` pixels square. */
export function sealSvg(cid, size = 20) {
  size = size || 20
  const raw = /^bafk/.test(cid)
  const r = mulberry32(fnv1a(cid.slice(4)))
  const n = 5
  const pad = size * 0.14
  const cell = (size - pad * 2) / n
  let cells = ''
  for (let y = 0; y < n; y++) {
    for (let x = 0; x < 3; x++) {
      if (r() < 0.5) {
        const cols = x === 2 ? [2] : [x, n - 1 - x]
        for (const cx of cols) {
          const px = pad + cx * cell
          const py = pad + y * cell
          cells += raw
            ? `<rect x="${fixed(px + cell * 0.08)}" y="${fixed(py + cell * 0.08)}" width="${fixed(cell * 0.84)}" height="${fixed(cell * 0.84)}" rx="${fixed(cell * 0.14)}"/>`
            : `<circle cx="${fixed(px + cell / 2)}" cy="${fixed(py + cell / 2)}" r="${fixed(cell * 0.42)}"/>`
        }
      }
    }
  }
  const tick = Math.floor(r() * 4)
  const tx = [pad * 0.5, size - pad * 0.5][tick & 1]
  const ty = [pad * 0.5, size - pad * 0.5][tick >> 1]
  return `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" role="img" aria-label="seal ${escaped(cid.slice(0, 4))}…${escaped(cid.slice(-6))}">` +
    `<rect x=".5" y=".5" width="${size - 1}" height="${size - 1}" rx="${fixed(size * 0.2)}" style="fill:var(--surface);stroke:var(--line-2)"/>` +
    `<g style="fill:var(--ink)">${cells}</g>` +
    `<circle cx="${tx}" cy="${ty}" r="${fixed(Math.max(1.2, size * 0.035))}" style="fill:var(--accent)"/></svg>`
}

/** The Seal as a node: span.seal[data-seal] holding the SVG. */
export function seal(cid, { size = 20 } = {}) {
  const el = h('span', { class: 'seal', 'data-seal': cid })
  el.innerHTML = sealSvg(cid, size)
  return el
}
