// Content identity as DESIGN.md §3 has it, with a pure-JavaScript SHA-256:
// crypto.subtle exists only in a secure context, and a page on plain http from
// a LAN host or an S3 website endpoint is not one.
import { CID } from 'multiformats/cid'
import * as Digest from 'multiformats/hashes/digest'
import { sha256 } from '@noble/hashes/sha2.js'

export const DAG_CBOR = 0x71
export const RAW = 0x55
const SHA2_256 = 0x12

export const cidFor = (bytes, code) => CID.create(1, code, Digest.create(SHA2_256, sha256(bytes)))

/** True when the bytes hash to the CID they were fetched as, codec included. */
export function verifies(cidText, bytes) {
  const want = CID.parse(cidText)
  return want.multihash.code === SHA2_256 && cidFor(bytes, want.code).equals(want)
}

export const blockPath = (cidText) => `blocks/${cidText.slice(-2)}/${cidText}`
