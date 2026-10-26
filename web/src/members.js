// The members of one explore server, each opened only when something asks for
// it (P3 Task 1). The page opens its primary member as before; the others
// cost nothing until a view, the switcher or a copy needs their snapshot.
import { BlockFetcher } from './blocks.js'
import { Explorer } from './model.js'
import { listLog } from './storelog.js'
import { storeKey } from './storename.js'

/**
 * The `openExplorer` the page gives createMembers: one Explorer per member,
 * each with its own BlockFetcher over its own base. The primary (the alias the
 * page opened, or the lone store when there is no members.json) reads
 * `store.base` through `primaryBlocks`, so the page's verified list is that
 * fetcher's. `openDb(url, { primary })` opens one snapshot URL; `hasFile(url)` says whether a file exists.
 */
export function memberOpener(store, { pageUrl, primaryBlocks, openDb, hasFile, listFn = listLog, fetchFn }) {
  return (member) => {
    const primary = member.alias === store.member
    const base = primary ? store.base : new URL(member.base, pageUrl).href
    const blocks = primary ? primaryBlocks : new BlockFetcher(base, fetchFn ? { fetchFn } : undefined)
    return Explorer.open({ base, blocks, listFn: (b, o) => listFn(b, { ...o, ...(fetchFn ? { fetchFn } : {}) }),
      openDb: (url) => openDb(url, { primary }), hasFile })
  }
}

/**
 * `openExplorer(member)` returns a Promise<Explorer> for one members.json
 * entry; the caller decides how (its own BlockFetcher and openSnapshot), and
 * may hand back the Explorer it already opened for the primary.
 */
export function createMembers(store, { openExplorer }) {
  const list = (store.members ?? []).map(m => ({ ...m, key: storeKey(store, m.alias) }))
  const cache = new Map() // alias -> { promise, state, error }

  const find = (alias) => {
    const m = list.find(x => x.alias === alias)
    if (!m) throw new Error(`no member named ${alias}`)
    return m
  }

  function explorer(alias) {
    const m = find(alias)
    let entry = cache.get(alias)
    if (!entry) {
      entry = { state: 'opening', error: null }
      entry.promise = new Promise(resolve => resolve(openExplorer(m))).then(
        (ex) => { entry.state = 'open'; return ex },
        (e) => { entry.state = e?.code === 'no_snapshot' ? 'no_snapshot' : 'failed'; entry.error = e; throw e })
      // A member that fails never breaks the page: whoever awaits sees the error, nobody else is told.
      entry.promise.catch(() => {})
      cache.set(alias, entry)
    }
    return entry.promise
  }

  /** `{ state, error }`: 'open' | 'opening' | 'no_snapshot' | 'failed'. A member nobody has asked for reads as 'opening'. */
  function status(alias) {
    find(alias)
    const entry = cache.get(alias)
    return entry ? { state: entry.state, error: entry.error } : { state: 'opening', error: null }
  }

  /** Forget an opened Explorer, so the next explorer(alias) reads the member again (after a copy). */
  function invalidate(alias) {
    cache.delete(alias)
  }

  return { primary: store.member, list, explorer, status, invalidate }
}

/**
 * Whether a list page shows every member's rows (P3 rule D5): `?member=*`
 * with more than one member. With one member, or none, the page lists its
 * one store exactly as before and asks nothing more.
 */
export const listsAll = (ctx) => !!ctx?.store?.all && (ctx?.members?.list?.length ?? 0) > 1

/**
 * Asks every member the same question, in members.json order:
 * `[{ member, ex, value }]` for a member that answered and
 * `[{ member, state, error }]` for one whose snapshot could not be opened or
 * whose answer failed. One member failing never hides the others.
 */
export async function fromEach(members, ask) {
  return Promise.all(members.list.map(async (member) => {
    let ex
    try {
      ex = await members.explorer(member.alias)
    } catch (error) {
      return { member, state: members.status(member.alias).state === 'no_snapshot' ? 'no_snapshot' : 'failed', error }
    }
    try {
      return { member, ex, value: await ask(ex, member) }
    } catch (error) {
      return { member, state: 'failed', error }
    }
  }))
}

/**
 * The rows of every answer, one per key, in the order first seen (members.json
 * order, then each member's own order): `{ key, row, member, ex, heldIn }`,
 * where `row`, `member` and `ex` are the first member's that lists it and
 * `heldIn` is every alias that lists it. `rowsOf(value)` gives an answer's rows.
 */
export function mergeRows(answers, rowsOf, keyOf) {
  const merged = new Map()
  for (const a of answers.filter(x => 'value' in x)) {
    for (const row of rowsOf(a.value)) {
      const key = keyOf(row)
      const seen = merged.get(key)
      if (seen) seen.heldIn.push(a.member.alias)
      else merged.set(key, { key, row, member: a.member, ex: a.ex, heldIn: [a.member.alias] })
    }
  }
  return [...merged.values()]
}

/**
 * Each member's presence for a row the members' own lists name: held where
 * `heldIn` lists it, not where an answering member does not, and unknown
 * (no dot; "couldn't check") where the member could not answer. The shape
 * presence.js draws: `[{ alias, key, state }]`.
 */
export function listedStates(answers, heldIn) {
  return answers.map(a => ({ alias: a.member.alias, key: a.member.key,
    state: !('value' in a) ? 'unknown' : heldIn.includes(a.member.alias) ? 'held' : 'not' }))
}

/** The muted line for a member that could not answer: what it is, and that the others are still listed. */
export function unreadableText({ member, state, error }, what) {
  return state === 'no_snapshot'
    ? error?.older ? `${member.alias}'s snapshot is older than this explorer reads, so its ${what} are not listed; nextflow plugin nf-blocks:snapshot rewrites it.`
      : `${member.alias} has no snapshot yet, so its ${what} are not listed.`
    : `${member.alias} could not be read (${error?.message ?? error}), so its ${what} are not listed.`
}

/** `hash` opened in member `alias`: the page itself for the open member, else the page with ?member=alias (ctx.store.hrefIn). */
export const hrefIn = (ctx, alias, hash) => ctx?.store?.hrefIn?.(alias, hash) ?? hash
