// A subject's current state from its Claims (block explorer spec section 8;
// decision 5 of the milestone 2 plan). The same rules as ClaimState.groovy;
// test/fixtures/claim-vectors.json pins both.
export const NONE = 'none'
export const DELETED = 'deleted'
export const CONFLICTED = 'conflicted'

const DELETION = '\u0000deletion'
const groupOf = (c) => (c.attribute === null || c.attribute === undefined ? DELETION : c.attribute)
const byCid = (a, b) => (a.cid < b.cid ? -1 : a.cid > b.cid ? 1 : 0)

export function claimState(claims) {
  const superseded = new Set(claims.flatMap(c => c.supersedes ?? []))
  const current = claims.filter(c => !superseded.has(c.cid)).sort(byCid)
  const sizes = new Map()
  for (const c of current) sizes.set(groupOf(c), (sizes.get(groupOf(c)) ?? 0) + 1)
  const nameGroup = current.filter(c => c.attribute === 'name')
  const deletionGroup = current.filter(c => groupOf(c) === DELETION && (c.verb === 'delete' || c.verb === 'del'))
  const deletion = deletionGroup.length > 1 ? CONFLICTED
    : deletionGroup.length === 1 && deletionGroup[0].verb === 'delete' ? DELETED : NONE
  return {
    current: current.map(c => ({ ...c, conflicted: sizes.get(groupOf(c)) > 1 })),
    names: nameGroup.filter(c => c.verb === 'set').map(c => String(c.value)),
    nameClaims: nameGroup.map(c => c.cid),
    nameConflicted: nameGroup.length > 1,
    deletion,
    deletionClaims: deletionGroup.map(c => c.cid),
    hidden: deletion === DELETED,
  }
}
