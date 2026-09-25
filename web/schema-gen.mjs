// web/schema-gen.mjs
// Extracts the IPLD Schema of DESIGN.md §6 (the one ```ipldsch block) into
// src/generated/schema.json for the page (block explorer spec section 12).
// @ipld/schema's typed builder names the prelude Any "$$Any", so value
// references are rewritten to it, as the prototype's validate.mjs did.
import { fromDSL } from '@ipld/schema/from-dsl.js'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'

export function extractSchema(design) {
  const block = /```ipldsch\n([\s\S]*?)```/.exec(design)
  if (!block) throw new Error('DESIGN.md has no ```ipldsch block')
  const parsed = fromDSL(block[1])
  return JSON.parse(JSON.stringify(parsed), (key, value) =>
    (value === 'Any' && (key === 'valueType' || key === 'type')) ? '$$Any' : value)
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const here = new URL('.', import.meta.url)
  const schema = extractSchema(readFileSync(new URL('../DESIGN.md', here), 'utf8'))
  mkdirSync(new URL('src/generated/', here), { recursive: true })
  writeFileSync(new URL('src/generated/schema.json', here), JSON.stringify(schema))
  console.log(`src/generated/schema.json: ${Object.keys(schema.types).length} types`)
}
