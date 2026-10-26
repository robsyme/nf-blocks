// Client-side size accounting over OutputItem leaves and DirectoryManifest
// entries (P2 replaces this with an index table). Pure over
// `fetch(cid, kind) -> { value }`, the shape BlockFetcher.ofKind answers.
import { codeOf, fileSlots } from './slots.js'
import { DAG_CBOR } from './cid.js'
import { BlockError } from './blocks.js'

const MIXED_CASE = new Map([['bigwig', 'bigWig'], ['rdata', 'RData']])

/**
 * A file name's extension, lower case: what follows its last '.', or '' when
 * there is none (no dot, a leading dot only, or a trailing dot). The one rule
 * for size kinds, the items grid's file chips and the file preview.
 */
export function extOf(name) {
  const text = String(name ?? '')
  const dot = text.lastIndexOf('.')
  return dot <= 0 || dot === text.length - 1 ? '' : text.slice(dot + 1).toLowerCase()
}

/** An extension as a chip writes it: `bigWig` and `RData` keep their usual case. */
export const extLabel = (ext) => MIXED_CASE.get(ext) ?? ext

/**
 * Every stored file of an OutputItem value: `{ files: Map<rawCid, { size,
 * name, path, column }>, partial }`. A directory leaf is expanded through the
 * `regular` entries of its DirectoryManifest, recursively. Symlinks,
 * unresolvable entries and leaves with no address add nothing. A manifest the
 * member does not hold, or refuses (any BlockError), sets `partial`; any other
 * failure is thrown.
 */
export async function itemFiles(value, fetch) {
  const files = new Map()
  const dirs = []
  let partial = false
  const add = (cid, f) => { if (!files.has(cid)) files.set(cid, f) }
  const walkDir = async (cid, path, column, seen, inDir = true) => {
    if (seen.has(cid)) return
    if (codeOf(cid) !== DAG_CBOR) { partial = true; return }
    let manifest
    try {
      manifest = (await fetch(cid, 'DirectoryManifest')).value
    } catch (e) {
      if (e instanceof BlockError) { partial = true; return }
      throw e
    }
    for (const entry of manifest.entries) {
      const here = `${path}/${entry.name}`
      if (entry.mode === 'directory' && entry.address) await walkDir(String(entry.address), here, column, new Set(seen).add(cid))
      else if ((entry.mode === 'regular' || entry.mode === 'executable') && entry.address)
        add(String(entry.address), { size: entry.size ?? 0, name: entry.name, path: here, column, inDir })
    }
  }
  for (const slot of fileSlots(value)) {
    if (!slot.address) continue
    if (slot.dir) {
      dirs.push({ address: slot.address, name: slot.name, path: slot.path, column: slot.column })
      await walkDir(slot.address, slot.path, slot.column, new Set())
    } else add(slot.address, { size: slot.size ?? 0, name: slot.name ?? slot.address, path: slot.path, column: slot.column })
  }
  return { files, dirs, partial }
}

/** Bytes and block count over the union of file maps, each raw CID once. */
export function sumUnique(fileMaps) {
  const seen = new Map()
  let partial = false
  for (const m of fileMaps) {
    if (m.partial) partial = true
    for (const [cid, f] of m.files) seen.set(cid, f)
  }
  let bytes = 0
  for (const f of seen.values()) bytes += f.size
  return { bytes, blocks: seen.size, partial }
}


/**
 * `[[ext, count, bytes]]`, most bytes first. Each directory leaf in `dirs`
 * counts once as `dir`, carrying the bytes of the files inside; files found
 * inside a directory (`inDir`), and names with no extension, add no chip.
 */
export function kindsOf(files, dirs = []) {
  const by = new Map()
  if (dirs.length) by.set('dir', ['dir', dirs.length, 0])
  for (const f of files.values()) {
    if (f.inDir) { if (by.has('dir')) by.get('dir')[2] += f.size; continue }
    const k = extLabel(extOf(f.name))
    if (!k) continue
    const e = by.get(k) ?? [k, 0, 0]
    e[1] += 1
    e[2] += f.size
    by.set(k, e)
  }
  return [...by.values()].sort((a, b) => b[2] - a[2] || (a[0] < b[0] ? -1 : 1))
}
