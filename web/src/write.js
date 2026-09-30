// The page's only write path: DAG-JSON requests to nf-blocks:explore's
// POST /api/put (spec sections 9.1 and 9.5). The page builds request
// content, never a block: the server encodes (spec section 1.4).
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'

export class WriteError extends Error {
  constructor(code, message, at, status) { super(message); this.code = code; this.at = at; this.status = status }
}

const cid = (text) => CID.parse(text)
const TRANSPORT = { 403: 'forbidden', 413: 'too_large', 415: 'unsupported_media_type' }

export function writer({ endpoint, token, fetchFn = (...a) => fetch(...a), now = () => new Date() }) {
  async function post(request, dryRun = false) {
    let res
    try {
      res = await fetchFn(dryRun ? `${endpoint}?dry_run=true` : endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/vnd.ipld.dag-json', 'X-NF-Blocks-Token': token },
        body: dagJson.encode(request),
      })
    } catch (e) {
      throw new WriteError('write_failed', `could not reach the explore server: ${e.message}`, '', 0)
    }
    const type = res.headers.get('Content-Type') ?? ''
    if (type.includes('application/vnd.ipld.dag-json')) {
      const body = dagJson.decode(new Uint8Array(await res.arrayBuffer()))
      if (!res.ok) throw new WriteError(body.error, body.message, body.at, res.status)
      return { ...body, address: body.address.toString(),
        ...(body.name_claims ? { name_claims: body.name_claims.map(String) } : {}),
        ...(body.deletion_claims ? { deletion_claims: body.deletion_claims.map(String) } : {}),
        ...(body.retain_claims ? { retain_claims: body.retain_claims.map(String) } : {}),
        ...(body.pin_claims ? { pin_claims: body.pin_claims.map(String) } : {}) }
    }
    throw new WriteError(TRANSPORT[res.status] ?? 'write_failed', (await res.text()).trim() || `HTTP ${res.status}`, '', res.status)
  }
  const claim = (subject, verb, attribute, value, supersedes) => post({
    kind: 'Claim', subject: cid(subject), verb, attribute, value, supersedes: supersedes.map(cid), timestamp: now().toISOString() })
  return {
    selection: (members, { dryRun = false } = {}) => post({ kind: 'Selection', members, derived_from: [] }, dryRun),
    rename: (subject, name, supersedes) => claim(subject, 'set', 'name', name, supersedes),
    remove: (subject, supersedes) => claim(subject, 'delete', null, null, supersedes),
    undo: (subject, supersedes) => claim(subject, 'del', null, null, supersedes),
    release: (subject, retainClaims) => claim(subject, 'set', 'retain', 'lineage', retainClaims),
    restore: (subject, retainClaims) => claim(subject, 'del', 'retain', null, retainClaims),
    pin: (subject, note) => claim(subject, 'add', 'pin', note, []),
    unpin: (subject, pinClaim) => claim(subject, 'del', 'pin', null, [pinClaim]),
  }
}
