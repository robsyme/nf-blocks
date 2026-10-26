// web/test/seal.test.mjs
// The Seal is normative (mockups/src/kit.js): the same CID draws the same SVG
// string in the explorer and in the mockups, byte for byte.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import vm from 'node:vm'
import { installDom } from './dom.mjs'
import { seal, sealSvg } from '../src/seal.js'

function kit() {
  const source = readFileSync(new URL('../../docs/design/mockups/src/kit.js', import.meta.url), 'utf8')
  const window = {}
  vm.runInNewContext(source, { window, document: { addEventListener() {} } })
  return window.NFB
}

const CIDS = [
  'bafkreibxyh4xa4v5od634co6m4wcvs3yrh7s4vhsz2hgiybzwkahgz3oym',
  'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq',
  'bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce',
  'bafkreibr5nkdlnukzyfhxj2rcgtoxsfztlli2fr6lfmyxekn2iow22ol2u',
]

test('sealSvg is byte-identical to the mockup kit for every CID and size', () => {
  const NFB = kit()
  for (const cid of CIDS) for (const size of [14, 20, 36]) assert.equal(sealSvg(cid, size), NFB.seal(cid, size), `${cid} at ${size}`)
  assert.equal(sealSvg(CIDS[1]), NFB.seal(CIDS[1]), 'size defaults to 20')
})

test('raw file blocks draw squares, records draw circles', () => {
  const raw = sealSvg(CIDS[0], 20)
  const record = sealSvg(CIDS[1], 20)
  assert.match(raw.match(/<g[^>]*>(.*)<\/g>/)[1], /^<rect/)
  assert.doesNotMatch(raw.match(/<g[^>]*>(.*)<\/g>/)[1], /<circle/)
  assert.match(record.match(/<g[^>]*>(.*)<\/g>/)[1], /^<circle/)
  assert.doesNotMatch(record.match(/<g[^>]*>(.*)<\/g>/)[1], /<rect/)
})

test('seal: a span.seal carrying the CID, holding the SVG', () => {
  installDom()
  const el = seal(CIDS[2], { size: 36 })
  assert.equal(el.tagName, 'SPAN')
  assert.ok(el.classList.contains('seal'))
  assert.equal(el.dataset.seal, CIDS[2])
  assert.equal(el.innerHTML, sealSvg(CIDS[2], 36))
  assert.equal(seal(CIDS[2]).innerHTML, sealSvg(CIDS[2], 20))
})

test('a hostile cid (an address like #/project/<anything>) draws no markup of its own in the label', () => {
  installDom()
  const hostile = `"><img src=x onerror=alert(1)>'&<b>"`
  const svg = sealSvg(hostile, 20)
  const label = svg.match(/aria-label="([^"]*)"/)[1]
  assert.doesNotMatch(label, /[<>"']/, 'the slices are escaped')
  assert.equal(label, 'seal &quot;&gt;&lt;i…&#39;&amp;&lt;b&gt;&quot;')
  assert.doesNotMatch(svg, /<img|<b>/)
  assert.equal(seal(hostile).innerHTML, svg)
})
