// The page's only write path: DAG-JSON requests to nf-blocks:explore's
// POST /api/put (spec sections 9.1 and 9.5), and plain JSON to
// POST /api/handoff beside it (P4 plan 1.5). The page builds request
// content, never a block: the server encodes (spec section 1.4).
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'

export class WriteError extends Error {
  constructor(code, message, at, status) { super(message); this.code = code; this.at = at; this.status = status }
}

const cid = (text) => CID.parse(text)
const TRANSPORT = { 403: 'forbidden', 413: 'too_large', 415: 'unsupported_media_type' }
/** The policies a `set` Claim holds (P4 plan 1.2); people are a set, written with addPerson. */
const SET_POLICIES = new Set(['default_store', 'retention'])
const ref = (r) => ({ pipeline: r.pipeline, id: r.id })
const occurrence = (o) => ({ pipeline: o.pipeline, id: o.id, run: cid(o.run) })

/** One POST to the explore server; an unreachable server is a WriteError `write_failed`. */
async function send(fetchFn, url, dryRun, type, token, body) {
  try {
    return await fetchFn(dryRun ? `${url}?dry_run=true` : url, { method: 'POST', headers: { 'Content-Type': type, 'X-NF-Blocks-Token': token }, body })
  } catch (e) {
    throw new WriteError('write_failed', `could not reach the explore server: ${e.message}`, '', 0)
  }
}

/**
 * `endpoint` is api/put's URL; api/handoff is its sibling. Claim methods take
 * cids as strings and `supersedes` as the Claim cids the page showed.
 */
export function writer({ endpoint, token, fetchFn = (...a) => fetch(...a), now = () => new Date() }) {
  const handoffEndpoint = new URL('handoff', endpoint).href
  async function post(request, dryRun = false) {
    const res = await send(fetchFn, endpoint, dryRun, 'application/vnd.ipld.dag-json', token, dagJson.encode(request))
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
  /** POST /api/handoff (plan 1.5): plain JSON both ways, errors as `{ error, message }`. */
  async function handoff({ template, selection, project = null, files, params }, { dryRun = false } = {}) {
    const res = await send(fetchFn, handoffEndpoint, dryRun, 'application/json', token,
      JSON.stringify({ template, selection, project: project ?? null, files, params, timestamp: now().toISOString() }))
    if ((res.headers.get('Content-Type') ?? '').includes('application/json')) {
      let body
      try {
        body = await res.json()
      } catch (e) {
        throw new WriteError('write_failed', `the explore server's answer is not JSON: ${e.message}`, '', res.status)
      }
      if (!res.ok) throw new WriteError(body.error ?? 'write_failed', body.message ?? `HTTP ${res.status}`, '', res.status)
      return body
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
    // P4 projects (plan 1.1 and 1.2). A Project's rename, delete and undo are rename, remove and undo above.
    project: ({ name, description = '' }, { dryRun = false } = {}) =>
      post({ kind: 'Project', name, description, created_at: now().toISOString() }, dryRun),
    addToProject: (subject, project, supersedes = []) => claim(subject, 'add', 'project', cid(project), supersedes),
    removeFromProject: (subject, membershipClaims) => claim(subject, 'del', 'project', null, membershipClaims),
    addSample: (project, sample) => claim(project, 'add', 'sample', ref(sample), []),
    removeSample: (project, sampleClaims) => claim(project, 'del', 'sample', null, sampleClaims),
    annotate: (project, sample, name, value, supersedes = []) =>
      claim(project, 'add', 'annotation', { sample: ref(sample), name, value }, supersedes),
    clearAnnotation: (project, annotationClaims) => claim(project, 'del', 'annotation', null, annotationClaims),
    sameSample: (project, a, b, same, supersedes = []) =>
      claim(project, 'add', 'same_sample', { a: occurrence(a), b: occurrence(b), same }, supersedes),
    undoSameSample: (project, sameClaims) => claim(project, 'del', 'same_sample', null, sameClaims),
    setPolicy: (project, policy, value, supersedes) => {
      if (!SET_POLICIES.has(policy)) throw new Error(`${policy} is not a set policy (default_store or retention)`)
      return claim(project, 'set', `policy.${policy}`, value, supersedes)
    },
    clearPolicy: (project, policy, policyClaims) => claim(project, 'del', `policy.${policy}`, null, policyClaims),
    addPerson: (project, principal) => claim(project, 'add', 'policy.people', principal, []),
    removePerson: (project, personClaims) => claim(project, 'del', 'policy.people', null, personClaims),
    handoff,
  }
}
