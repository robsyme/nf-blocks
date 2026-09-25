// web/test/dag-json-vectors.mjs
// Writes fixtures/dag-json-vectors.json with @ipld/dag-json, the codec the page
// uses, so DagJsonTest pins the Groovy codec to it (spec section 9.1).
//   node test/dag-json-vectors.mjs
import { writeFileSync } from 'node:fs'
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'

const link = CID.parse('bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua')
const raw = CID.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')
const text = new TextDecoder()
// No floats: JavaScript writes 1e+21 where Groovy writes 1.0E21, both valid; floats are decode-only vectors.
const values = {
  empty: {},
  scalars: { t: true, f: false, n: null, i: 7, neg: -12, big: 9007199254740991, s: 'plain' },
  order: { bb: 1, a: 2, aaa: 3, B: 4, 'é': 5, z: 6 },
  escapes: { s: 'q"\\\n\r\t\b\f\u0001\u001fé€𝄞' },
  link: { l: link, r: raw, list: [link, raw] },
  bytes: { b: new Uint8Array([1, 2, 3, 250]), none: new Uint8Array([]) },
  selection: { kind: 'Selection', members: [{ item: { address: link, via: [link] } }, { selection: link }], derived_from: [link.bytes] },
  claim: { kind: 'Claim', subject: link, verb: 'set', attribute: 'name', value: 'tumour, "batch 2"', supersedes: [], timestamp: '2026-09-25T10:00:00.000Z' },
}
const vectors = Object.entries(values).map(([name, value]) => ({ name, json: text.decode(dagJson.encode(value)) }))
writeFileSync(new URL('./fixtures/dag-json-vectors.json', import.meta.url), JSON.stringify(vectors, null, 1) + '\n')
console.log(`wrote ${vectors.length} vectors`)
