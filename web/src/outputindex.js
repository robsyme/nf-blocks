// web/src/outputindex.js
// Nextflow's Output Index File names each file by its Publish Coordinate,
// cas://<alias>/<publish path>, which the next run to that path overwrites
// and which another reader's config may map elsewhere (DESIGN.md §7). The
// page offers the same file with each coordinate replaced by the address
// fromStore would emit for that leaf (§13). Pure: model.js fetches the blocks.
import { CID } from 'multiformats/cid'
import { leavesOf } from './metadata.js'

const RAW = 0x55
const basename = (path) => path.slice(path.lastIndexOf('/') + 1)

/** The Store URI fromStore emits for a leaf: a file by its address and name; a directory as the Item Leaf, or its manifest when another leaf of the item shares its name. */
function leafUri(itemCid, leaf, leaves) {
  const address = CID.asCID(leaf.address) ?? CID.parse(String(leaf.address))
  if (address.code === RAW) return `cas://${address}/${leaf.name}`
  return leaves.filter(l => l.name === leaf.name).length === 1 ? `cas://${itemCid}/${leaf.name}` : `cas://${address}`
}

/**
 * Publish path -> Store URI over an OutputCollection's items (`values`: item
 * CID text -> OutputItem value). `paths[i]` is in the order the run built the
 * item, and a stored map comes back in key order, so the two are paired by
 * file name within the item; an item with one path and one addressed leaf
 * pairs them whatever their names. A path two items claim pairs with neither.
 */
export function addressesByPath(collection, values) {
  const found = new Map()
  const claimed = new Set()
  collection.items.forEach((cid, i) => {
    if (!cid) return
    const itemCid = cid.toString()
    const value = values.get(itemCid)
    if (value === undefined) return
    const leaves = leavesOf(value)
    const addressed = leaves.filter(l => l.address)
    const paths = (collection.paths?.[i] ?? []).filter(Boolean)
    for (const path of paths) {
      const named = addressed.filter(l => l.name === basename(path))
      const leaf = paths.length === 1 && addressed.length === 1 ? addressed[0] : named.length === 1 ? named[0] : null
      if (!leaf) continue
      if (claimed.has(path)) { found.delete(path); continue }
      claimed.add(path)
      found.set(path, leafUri(itemCid, leaf, leaves))
    }
  })
  return found
}

const isCid = (text) => { try { CID.parse(text); return true } catch { return false } }

/**
 * `text` with each cas://<alias>/<...>/<publish path> that `byPath` knows
 * replaced by its address, and everything else as it was. A coordinate it
 * does not know is left and counted in `unmatched`; a Store URI (its
 * authority a CID) is already an address and is not counted.
 */
export function rewriteIndex(text, byPath) {
  const paths = [...byPath.keys()].sort((a, b) => b.length - a.length)
  let replaced = 0, unmatched = 0
  const out = text.replace(/cas:\/\/[^\s"',<>\\]+/g, (uri) => {
    const m = /^cas:\/\/([^/]+)\/(.+)$/.exec(uri)
    if (!m || isCid(m[1])) return uri
    const path = paths.find(p => m[2] === p || m[2].endsWith(`/${p}`))
    if (!path) { unmatched++; return uri }
    replaced++
    return byPath.get(path)
  })
  return { text: out, replaced, unmatched }
}
