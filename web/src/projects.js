// A project's state from Claim rows (P4 plan 1.2, decision D1): which runs,
// Selections and items are members, the samples added to it, annotations per
// (sample, name), same_sample merges and splits, and its policies. Rows are
// Explorer.claimsFor's (the same shape claims.js reads); current means not
// superseded, through claimState. Pure.
import { claimState } from './claims.js'

const byText = (a, b) => (a < b ? -1 : a > b ? 1 : 0)

/** A link as DAG-JSON writes it, `{"/": "<cid>"}`, read as the cid string. */
const linkReviver = (key, v) => (v && typeof v === 'object' && !Array.isArray(v) && typeof v['/'] === 'string'
  && Object.keys(v).length === 1 ? v['/'] : v)

/**
 * A Claim value whose shape is a map or a link: the index keeps it as DAG-JSON
 * text (DESIGN.md §16 decision 4), which is parsed, never matched as a string.
 * Links come back as cid strings. A value already decoded is returned as it
 * is (links in it are not rewritten); text that does not parse is undefined.
 */
export function decodeValue(value) {
  if (typeof value !== 'string') return value ?? null
  try {
    return JSON.parse(value, linkReviver)
  } catch {
    return undefined
  }
}

/** Who and when, for a claim tag, and the cid a removal supersedes. */
const tagOf = (c) => ({ cid: c.cid, asserted_by: c.asserted_by ?? null, timestamp: c.timestamp ?? null })
const nonEmpty = (s) => typeof s === 'string' && s.length > 0
const asRef = (v) => (v && nonEmpty(v.pipeline) && nonEmpty(v.id) ? { pipeline: v.pipeline, id: v.id } : null)
const asOccurrence = (v) => (asRef(v) && nonEmpty(v.run) ? { pipeline: v.pipeline, id: v.id, run: v.run } : null)
export const refKey = (r) => `${r.pipeline}\u0000${r.id}`

/** The current `add` Claims of one attribute, each with its decoded value. */
function currentAdds(claims, attribute) {
  return claimState(claims).current.filter(c => c.attribute === attribute && c.verb === 'add')
    .map(c => ({ claim: c, value: decodeValue(c.value) }))
}

/**
 * Every current membership of the subjects in `claimsBySubject` (a Map of
 * subject cid to its Claim rows): `[{ subject, project, claim: { cid,
 * asserted_by, timestamp }, hidden }]`, in subject then project order.
 * `hidden`: the subject itself has a current delete.
 */
export function membershipsOf(claimsBySubject) {
  const out = []
  for (const [subject, claims] of claimsBySubject) {
    const state = claimState(claims)
    for (const c of state.current.filter(c => c.attribute === 'project' && c.verb === 'add')) {
      const project = decodeValue(c.value)
      if (nonEmpty(project)) out.push({ subject, project, claim: tagOf(c), hidden: state.hidden })
    }
  }
  return out.sort((a, b) => byText(a.subject, b.subject) || byText(a.project, b.project) || byText(a.claim.cid, b.claim.cid))
}

/** The samples added to a project: `[{ ref: { pipeline, id }, claims }]`, one per SampleRef, sorted. Removing one supersedes every Claim in `claims`. */
export function sampleRefs(projectClaims) {
  const out = new Map()
  for (const { claim, value } of currentAdds(projectClaims, 'sample')) {
    const ref = asRef(value)
    if (!ref) continue
    if (!out.has(refKey(ref))) out.set(refKey(ref), { ref, claims: [] })
    out.get(refKey(ref)).claims.push(tagOf(claim))
  }
  return [...out.values()].sort((a, b) => byText(refKey(a.ref), refKey(b.ref)))
}

/** One cell's state from its current annotation Claims: the value when they agree, else null and conflicted. */
function cell(entries) {
  const values = [...new Set(entries.map(e => e.value))].sort(byText)
  return { value: values.length === 1 ? values[0] : null, values, conflicted: values.length > 1, claims: entries.map(e => e.claim) }
}

/**
 * Current annotations per (sample, name): `[{ sample, name, value, values,
 * conflicted, claims, entries }]`, sorted by sample then name (`entries`: each
 * current Claim with its value). Two current Claims with
 * different values conflict (`value` null, `values` both); the same value twice
 * does not. An edit or clear supersedes every Claim in `claims`.
 */
export function annotations(projectClaims) {
  const groups = new Map()
  for (const { claim, value } of currentAdds(projectClaims, 'annotation')) {
    const sample = asRef(value?.sample)
    if (!sample || !nonEmpty(value.name) || typeof value.value !== 'string') continue
    const k = `${refKey(sample)}\u0000${value.name}`
    if (!groups.has(k)) groups.set(k, { sample, name: value.name, entries: [] })
    groups.get(k).entries.push({ value: value.value, claim: tagOf(claim) })
  }
  return [...groups.entries()].sort(([a], [b]) => byText(a, b))
    .map(([, g]) => ({ sample: g.sample, name: g.name, ...cell(g.entries), entries: g.entries }))
}

/**
 * One sample's annotations through any of its SampleRefs (plan 1.6), from
 * `annotations()`: a Map of name to `{ value, values, conflicted, claims,
 * entries }` (`entries`: each current Claim with its value, which a
 * conflicted cell shows), names sorted. Different values under two refs of
 * one sample conflict too.
 */
export function sampleAnnotations(refs, list) {
  const keys = new Set(refs.map(refKey))
  const byName = new Map()
  for (const a of list.filter(a => keys.has(refKey(a.sample)))) {
    if (!byName.has(a.name)) byName.set(a.name, [])
    byName.get(a.name).push(...a.entries)
  }
  return new Map([...byName.keys()].sort(byText).map(name => [name, { ...cell(byName.get(name)), entries: byName.get(name) }]))
}

/** The current same_sample Claims: `[{ a, b, same, claim }]`, a and b Occurrences with `run` as a cid string; the input sampleGroups takes. */
export function sameSampleClaims(projectClaims) {
  return currentAdds(projectClaims, 'same_sample').map(({ claim, value }) => ({
    a: asOccurrence(value?.a), b: asOccurrence(value?.b), same: value?.same, claim: tagOf(claim) }))
    .filter(c => c.a && c.b && typeof c.same === 'boolean')
}

/**
 * A `set` policy's state: its value when the current `set`s give one, and
 * whether the group conflicts. The same value set twice is one value, as an
 * annotation's cell has it; two values, or a value beside a clearing `del`,
 * conflict (`value` null). `claims` is every current Claim in the group, a
 * clearing `del` included, as claims.js's nameClaims and retainClaims are,
 * so the next `set` supersedes them all.
 */
function setPolicy(current, attribute, fallback) {
  const group = current.filter(c => c.attribute === attribute)
  const values = [...new Set(group.filter(c => c.verb === 'set' && typeof c.value === 'string').map(c => c.value))].sort(byText)
  const cleared = group.some(c => c.verb === 'del')
  const conflicted = values.length + (cleared ? 1 : 0) > 1
  return { value: conflicted ? null : values.length === 1 ? values[0] : fallback, values, conflicted, claims: group.map(tagOf) }
}

/**
 * A project's policies (plan 1.7): `defaultStore` and `retention` (`{ value,
 * values, conflicted, claims }`; retention is "none" with no Claim), and
 * `people` (`[{ principal, claims }]`, a set). A change supersedes `claims`.
 */
export function policyState(projectClaims) {
  const current = claimState(projectClaims).current
  const people = new Map()
  for (const c of current.filter(c => c.attribute === 'policy.people' && c.verb === 'add' && nonEmpty(c.value))) {
    if (!people.has(c.value)) people.set(c.value, { principal: c.value, claims: [] })
    people.get(c.value).claims.push(tagOf(c))
  }
  return {
    defaultStore: setPolicy(current, 'policy.default_store', null),
    retention: setPolicy(current, 'policy.retention', 'none'),
    people: [...people.values()].sort((a, b) => byText(a.principal, b.principal)),
  }
}

/**
 * Everything the page shows of one project from its block (or index row:
 * `{ name, description, asserted_by, created_at }`) and the Claims on it. The
 * name is the current `set name` Claims' (the first when they conflict),
 * falling back to the block's. `state` is claimState's.
 */
export function projectState(block, projectClaims) {
  const state = claimState(projectClaims)
  return {
    name: state.names[0] ?? block?.name ?? null,
    names: state.names,
    nameClaims: state.nameClaims,
    nameConflicted: state.nameConflicted,
    deletion: state.deletion,
    deletionClaims: state.deletionClaims,
    hidden: state.hidden,
    samples: sampleRefs(projectClaims),
    annotations: annotations(projectClaims),
    sameSample: sameSampleClaims(projectClaims),
    policy: policyState(projectClaims),
    state,
  }
}
