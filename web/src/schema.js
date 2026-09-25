// Every fetched block is checked against the IPLD Schema of DESIGN.md §6,
// generated into generated/schema.json by schema-gen.mjs (spec section 12).
import { create } from '@ipld/schema/typed.js'
import schema from './generated/schema.json' with { type: 'json' }

const block = create(schema, 'Block')
const leaf = create(schema, 'Leaf')

export const validBlock = (value) => block.toTyped(value) !== undefined
export const validLeaf = (value) => leaf.toTyped(value) !== undefined
