// web/src/blockinfo.js
// What the block page says about an address (visual language spec §4.1,
// §4.2): its CID decoded, and the kind of block it names with a link to the
// block's own page. Pure.
import { CID } from 'multiformats/cid'
import { DAG_CBOR, RAW } from './cid.js'
import { BlockError } from './blocks.js'

const SHA2_256 = 0x12
const CODECS = { [DAG_CBOR]: 'dag-cbor', [RAW]: 'raw' }
const HASHES = { [SHA2_256]: 'sha2-256' }
const hex = (bytes) => [...bytes].map(b => b.toString(16).padStart(2, '0')).join('')

export function cidParts(cidText) {
  let c
  try {
    c = CID.parse(cidText)
  } catch (e) {
    throw new BlockError('schema_invalid', cidText, `${cidText} is not a CID: ${e.message}`)
  }
  return {
    version: c.version,
    codec: CODECS[c.code] ?? `0x${c.code.toString(16)}`,
    hash: HASHES[c.multihash.code] ?? `0x${c.multihash.code.toString(16)}`,
    digest: hex(c.multihash.digest),
    standard: c.version === 1 && c.code in CODECS && c.multihash.code === SHA2_256,
  }
}

const linkText = (v) => { const c = CID.asCID(v); return c ? c.toString() : null }

const FRIENDLY = {
  RunCompletion: (cid) => ({ kind: 'run', text: 'a run', label: 'Open the run', href: `#/run/${cid}` }),
  OutputCollection: (cid) => ({ kind: 'output', text: 'an output', label: 'Open the output', href: `#/collection/${cid}` }),
  OutputItem: (cid) => ({ kind: 'item', text: 'an item: one channel item as a run published it', label: 'Open as an item', href: `#/item/-/${cid}` }),
  Selection: (cid) => ({ kind: 'selection', text: 'a Selection', label: 'Open the Selection', href: `#/selection/${cid}` }),
  Claim: (cid, v) => {
    const subject = linkText(v.subject)
    return { kind: 'claim', text: `a Claim: ${[v.verb, v.attribute].filter(Boolean).join(' ')}`,
      label: subject ? 'What it is about' : null, href: subject ? `#/block/${subject}` : null }
  },
  DirectoryManifest: () => ({ kind: 'directory', text: 'a directory: its entries are below', label: null, href: null }),
  Forget: () => ({ kind: 'address', text: "a sweep's record of the blocks it deleted", label: null, href: null }),
  RunManifest: (cid, v, stale) => {
    const run = stale.find(s => s.completion && linkText(s.completion.run) === cid)
    return { kind: 'run', text: "a run's start record", label: run ? 'Open the run' : null, href: run ? `#/run/${run.cid}` : null }
  },
}

export function friendlyOf(cidText, value, stale = []) {
  const make = FRIENDLY[value?.kind]
  if (make) return make(cidText, value, stale)
  return { kind: 'address', text: value ? `a block of kind ${value.kind ?? 'unknown'}` : 'a block that does not decode', label: null, href: null }
}
