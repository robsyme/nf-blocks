// A subject's current state from its Claims (block explorer spec section 8;
// decision 5 of the milestone 2 plan). The same rules as ClaimState.groovy;
// test/fixtures/claim-vectors.json pins both.
export const NONE = 'none'
export const DELETED = 'deleted'
export const CONFLICTED = 'conflicted'
export const RELEASED = 'released'

const DELETION = '\u0000deletion'
const groupOf = (c) => (c.attribute === null || c.attribute === undefined ? DELETION : c.attribute)
const byCid = (a, b) => (a.cid < b.cid ? -1 : a.cid > b.cid ? 1 : 0)

export function claimState(claims) {
  const superseded = new Set(claims.flatMap(c => c.supersedes ?? []))
  // Ticket 21 answer 2: a group that holds an `add` is a set; its current Claims never conflict.
  const addGroups = new Set(claims.filter(c => c.verb === 'add').map(groupOf))
  const current = claims.filter(c => !superseded.has(c.cid)).sort(byCid)
  const sizes = new Map()
  for (const c of current) sizes.set(groupOf(c), (sizes.get(groupOf(c)) ?? 0) + 1)
  const conflicted = (c) => sizes.get(groupOf(c)) > 1 && !addGroups.has(groupOf(c))
  const nameGroup = current.filter(c => c.attribute === 'name')
  const deletionGroup = current.filter(c => groupOf(c) === DELETION && (c.verb === 'delete' || c.verb === 'del'))
  const deletion = deletionGroup.length > 1 ? CONFLICTED
    : deletionGroup.length === 1 && deletionGroup[0].verb === 'delete' ? DELETED : NONE
  const retainGroup = current.filter(c => c.attribute === 'retain')
  const retain = retainGroup.length > 1 ? CONFLICTED
    : retainGroup.length === 1 && retainGroup[0].verb === 'set' && retainGroup[0].value === 'lineage' ? RELEASED : NONE
  const pins = current.filter(c => c.attribute === 'pin' && c.verb === 'add').map(c => ({ cid: c.cid, note: String(c.value) }))
  return {
    current: current.map(c => ({ ...c, conflicted: conflicted(c) })),
    names: nameGroup.filter(c => c.verb === 'set').map(c => String(c.value)),
    nameClaims: nameGroup.map(c => c.cid),
    nameConflicted: nameGroup.length > 1,
    deletion,
    deletionClaims: deletionGroup.map(c => c.cid),
    hidden: deletion === DELETED,
    retain,
    retainClaims: retainGroup.map(c => c.cid),
    released: retain === RELEASED,
    pins,
    pinClaims: pins.map(p => p.cid),
    pinned: pins.length > 0,
  }
}
