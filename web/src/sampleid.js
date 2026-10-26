// Sample identity in a project (P4 plan 1.6): which occurrences, one run's
// meta.id each, are the same sample. Grouping runs in the page so a merge or
// split shows at once from the page's own writes (decision D2). Pure;
// test/fixtures/sample-vectors.json pins it.
//
// An occurrence is `{ pipeline, id, run }` (run: the RunCompletion's cid as a
// string). Input rows are run_input rows: `{ completion_cid, role, address, sample }`.

const byText = (a, b) => (a < b ? -1 : a > b ? 1 : 0)

/** One occurrence's key: its run and meta.id (the run fixes the pipeline). */
export const occurrenceKey = (o) => `${o.run}\u0000${o.id}`
const refKey = (r) => `${r.pipeline}\u0000${r.id}`

/**
 * The run_input rows that can tell samples apart: role `task`, a sample and
 * an address, and not an address that carries two or more samples within one
 * run (a shared reference such as a salmon index staged per sample). Such an
 * address is left out everywhere, since it names no one sample.
 */
function sampleRows(rows) {
  const task = (rows ?? []).filter(r => r.role === 'task' && r.address && r.sample !== null && r.sample !== undefined)
  const samples = new Map()
  for (const r of task) {
    const k = `${r.completion_cid}\u0000${r.address}`
    if (!samples.has(k)) samples.set(k, new Set())
    samples.get(k).add(r.sample)
  }
  const shared = new Set(task.filter(r => samples.get(`${r.completion_cid}\u0000${r.address}`).size > 1).map(r => r.address))
  return task.filter(r => !shared.has(r.address))
}

/** Per occurrence key, the input addresses that tell its sample apart (sampleRows). An occurrence with none has no entry. */
export function inputAddresses(rows) {
  const out = new Map()
  for (const r of sampleRows(rows)) {
    const k = occurrenceKey({ run: r.completion_cid, id: r.sample })
    if (!out.has(k)) out.set(k, new Set())
    out.get(k).add(r.address)
  }
  return out
}

/**
 * The occurrences of `sample` Claims' SampleRefs (plan 1.6 step 1): each ref
 * in every run of its pipeline that holds its meta.id. `runs`: `[{ run,
 * pipeline, ids }]`. `unmatched`: the refs no run holds.
 */
export function matchRefs(refs, runs) {
  const occurrences = []
  const unmatched = []
  for (const ref of refs) {
    const found = runs.filter(r => r.pipeline === ref.pipeline && r.ids.includes(ref.id))
    if (!found.length) unmatched.push({ pipeline: ref.pipeline, id: ref.id })
    for (const r of found) occurrences.push({ pipeline: ref.pipeline, id: ref.id, run: r.run })
  }
  return { occurrences, unmatched }
}

class Components {
  constructor(keys) { this.parent = new Map(keys.map(k => [k, k])) }
  find(k) {
    let root = k
    while (this.parent.get(root) !== root) root = this.parent.get(root)
    while (this.parent.get(k) !== root) { const next = this.parent.get(k); this.parent.set(k, root); k = next }
    return root
  }
  join(a, b) { this.parent.set(this.find(a), this.find(b)) }
  groups() {
    const out = new Map()
    for (const k of this.parent.keys()) {
      const root = this.find(k)
      if (!out.has(root)) out.set(root, [])
      out.get(root).push(k)
    }
    return [...out.values()]
  }
}

const pairKey = (a, b) => (a < b ? `${a}\u0001${b}` : `${b}\u0001${a}`)
const shares = (a, b) => { for (const x of a) if (b.has(x)) return true; return false }

/**
 * The samples of a project (plan 1.6): `[{ label, basis, occurrences, refs }]`,
 * sorted by label, then by first occurrence. `inputs`: run_input rows.
 * `claims`: the current `same_sample` Claims' values, `{ a, b, same }` with
 * Occurrences for a and b. `basis` is "input reads" when every occurrence has
 * input addresses (inputAddresses), otherwise "sample ID". `refs`: the
 * SampleRefs `{ pipeline, id }` of the occurrences, once each, which
 * annotations attach through.
 */
export function sampleGroups(occurrences, { inputs = [], claims = [] } = {}) {
  const byKey = new Map()
  for (const o of occurrences) if (!byKey.has(occurrenceKey(o))) byKey.set(occurrenceKey(o), { pipeline: o.pipeline, id: o.id, run: o.run })
  const keys = [...byKey.keys()].sort(byText)
  const addresses = inputAddresses(inputs)
  const none = new Set()
  const addressesOf = (k) => addresses.get(k) ?? none

  // Step 2: automatic edges.
  const edges = new Map()
  for (let i = 0; i < keys.length; i++) {
    for (let j = i + 1; j < keys.length; j++) {
      const a = byKey.get(keys[i])
      const b = byKey.get(keys[j])
      const xa = addressesOf(keys[i])
      const xb = addressesOf(keys[j])
      const share = shares(xa, xb)
      const sameName = a.pipeline === b.pipeline && a.id === b.id && !(xa.size && xb.size && !share)
      if (share || sameName) edges.set(pairKey(keys[i], keys[j]), [keys[i], keys[j]])
    }
  }
  const automatic = new Components(keys)
  for (const [a, b] of edges.values()) automatic.join(a, b)

  // Step 3: a split takes b's edges to every occurrence of a's automatic group away.
  const known = (o) => o && byKey.has(occurrenceKey(o))
  for (const c of claims.filter(c => c.same === false && known(c.a) && known(c.b))) {
    const b = occurrenceKey(c.b)
    const group = automatic.find(occurrenceKey(c.a))
    for (const k of keys) if (k !== b && automatic.find(k) === group) edges.delete(pairKey(k, b))
  }

  // Steps 4 and 5: merges, then connected components.
  const samples = new Components(keys)
  for (const [a, b] of edges.values()) samples.join(a, b)
  for (const c of claims.filter(c => c.same === true && known(c.a) && known(c.b))) samples.join(occurrenceKey(c.a), occurrenceKey(c.b))

  const groups = samples.groups().map(members => {
    const list = members.sort(byText).map(k => byKey.get(k))
    const counts = new Map()
    for (const o of list) counts.set(o.id, (counts.get(o.id) ?? 0) + 1)
    const label = [...counts].sort((x, y) => y[1] - x[1] || byText(x[0], y[0]))[0][0]
    const refs = [...new Map(list.map(o => [refKey(o), { pipeline: o.pipeline, id: o.id }])).values()]
    return { label, basis: members.every(k => addressesOf(k).size > 0) ? 'input reads' : 'sample ID', occurrences: list, refs, first: members[0] }
  })
  return groups.sort((x, y) => byText(x.label, y.label) || byText(x.first, y.first)).map(({ first, ...g }) => g)
}

/**
 * Runs that share input files, for the suggestions (P4 Task 2): the runs
 * linked by addresses two or more of them read (the same rows as sampleGroups,
 * sampleRows), as `[{ runs, files }]`: `files` is how many addresses two or
 * more of those runs share. Largest first.
 */
export function runsSharingInputs(rows) {
  const runsOf = new Map()
  for (const r of sampleRows(rows)) {
    if (!runsOf.has(r.address)) runsOf.set(r.address, new Set())
    runsOf.get(r.address).add(r.completion_cid)
  }
  const shared = [...runsOf].filter(([, runs]) => runs.size > 1)
  const runs = new Components([...new Set(shared.flatMap(([, rs]) => [...rs]))])
  for (const [, rs] of shared) { const [first, ...rest] = [...rs]; for (const r of rest) runs.join(first, r) }
  return runs.groups().map(group => {
    const root = runs.find(group[0])
    return { runs: group.sort(byText), files: shared.filter(([, rs]) => runs.find([...rs][0]) === root).length }
  }).sort((a, b) => b.runs.length - a.runs.length || b.files - a.files || byText(a.runs[0], b.runs[0]))
}
