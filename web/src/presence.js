// web/src/presence.js
// Which stores hold a block, wherever the page shows its address (P3 Task 3,
// rule D4; design doc: location is a property, not an identity). The open
// member is held unless the page already knows the block is missing here;
// every other member answers a HEAD for the block's URL under its own base,
// the URL BlockFetcher reads: 200 held, 404 not held, anything else unknown.
// With one member, or none (?store=), nothing is asked unless the page cannot
// tell (hereHeld null): one dot, as before.
import { h } from './html.js'
import { blockHref } from './blocks.js'
import { button } from './components.js'

/**
 * At most this many HEADs in flight for the whole page: under the browser's
 * six connections per host, so the page's own block and snapshot reads keep
 * a connection while the dots are asked.
 */
export const IN_FLIGHT = 4

/** Runs at most `n` of the functions given to it at once, the rest in the order they came. */
function limiter(n) {
  let running = 0
  const queue = []
  const pump = () => {
    while (running < n && queue.length) {
      const { fn, resolve } = queue.shift()
      running++
      Promise.resolve().then(fn).then(resolve).finally(() => { running--; pump() })
    }
  }
  return (fn) => new Promise((resolve) => { queue.push({ fn, resolve }); pump() })
}

/** A dot's state as one word: 'held' | 'not' | 'unknown' | 'pending'. */
async function headState(fetchFn, url) {
  try {
    const res = await fetchFn(url, { method: 'HEAD', cache: 'no-store' })
    return res.status === 200 ? 'held' : res.status === 404 ? 'not' : 'unknown'
  } catch {
    return 'unknown'
  }
}

/**
 * The tooltip for a list of `{ alias, state }`, in member order:
 * "Held in roblab, org-s3; not in ana-hpc", then "couldn't check" and
 * "checking" parts for the stores that have not said.
 */
export function presenceTitle(states) {
  const of = (state) => states.filter(s => s.state === state).map(s => s.alias)
  const parts = [['Held in', of('held')], ['not in', of('not')], ["couldn't check", of('unknown')], ['checking', of('pending')]]
    .filter(([, names]) => names.length).map(([words, names]) => `${words} ${names.join(', ')}`)
  const text = parts.join('; ')
  return text.charAt(0).toUpperCase() + text.slice(1)
}

/**
 * Why a block that is not here cannot be downloaded, naming who holds it:
 * "Not in roblab. Held in org-s3." Null while any other store is still
 * being checked and none holds it yet.
 */
export function elsewhereText(states, open) {
  const here = `Not in ${open}.`
  const others = states.filter(s => !s.here)
  const held = others.filter(s => s.state === 'held').map(s => s.alias)
  const unknown = others.filter(s => s.state === 'unknown').map(s => s.alias)
  if (held.length) return `${here} Held in ${held.join(', ')}.`
  if (others.some(s => s.state === 'pending')) return `${here} Checking the other stores.`
  if (unknown.length) return `${here} Couldn't check ${unknown.join(', ')}.`
  return others.length ? `${here} No other store holds it.` : here
}

/** Draws `list` (`[{ alias, key, state }]`) into a `.presence`; `title`, when given, replaces the tooltip. */
function draw(node, list, title = presenceTitle(list)) {
  node.setAttribute('title', title)
  node.setAttribute('aria-label', title)
  node.dataset.state = list.some(s => s.state === 'pending') ? 'pending' : 'done'
  // An unknown store gets no dot: the tooltip says it could not be checked.
  node.replaceChildren(...list.filter(s => s.state !== 'unknown').map(s =>
    h('span', { class: `dot${s.state === 'held' ? '' : ' ring'}${s.state === 'pending' ? ' pending' : ''} ${s.key}`, 'data-store': s.alias, 'data-held': s.state })))
}

/**
 * Dots for what the page already knows, asking nothing: a list page knows
 * which stores hold a row from which members' indexes list it (rule D5).
 * `states` is `[{ alias, key, state }]` in members.json order; `title`
 * replaces the tooltip when the row has more to say (per-store counts).
 */
export function knownDots(states, { title = null } = {}) {
  const node = h('span', { class: 'presence', role: 'img' })
  draw(node, states, title ?? presenceTitle(states))
  return node
}

/**
 * `members`: createMembers' `{ primary, list }` (or null with no
 * members.json). `here`: `{ name, key, base }` for the store the page reads when
 * the list does not name it. `pageUrl` resolves each member's base, as
 * memberOpener does.
 *
 * Returns:
 * - `dots(cid, { hereHeld, held })`: a `.presence` that renders at once and fills
 *   in as the HEADs answer. `hereHeld`: true (held, the default), false (the
 *   page knows it is missing here: a ring), or null (the page cannot tell, so
 *   the open member is asked too, however many members there are; a store
 *   with no base to ask reads as held).
 *   `held`: the aliases the page already knows hold it (a list row their
 *   indexes list, rule D5); those are drawn held and not asked.
 * - `states(cid, { hereHeld })`: a Promise of `[{ alias, key, here, state }]`
 *   in members.json order, `state` 'held' | 'not' | 'unknown'.
 * - `now(cid, { hereHeld })`: the same list as it stands, 'pending' where a
 *   HEAD has not answered.
 * - `invalidate(cid?)`: forget every answer (or one block's), after a copy.
 * - `open`: the open member's name; `several`: more than one member.
 */
export function createPresence(members, { fetchFn = (...a) => fetch(...a), pageUrl = globalThis.location?.href, here = null } = {}) {
  const list = members?.list ?? []
  const several = list.length > 1
  const openAlias = members?.primary ?? null
  const fallback = { alias: here?.name ?? here?.label ?? openAlias ?? 'this store', key: here?.key ?? 'a', base: here?.base ?? null }
  const stores = list.length && list.some(m => m.alias === openAlias) ? list : [fallback]
  const open = stores.find(m => m.alias === openAlias) ?? stores[0]
  const answers = new Map() // `${alias} ${cid}` -> { state, promise }
  const slot = limiter(IN_FLIGHT)

  const ask = (m, cid) => {
    const key = `${m.alias} ${cid}`
    let entry = answers.get(key)
    if (!entry) {
      entry = { state: 'pending' }
      const url = blockHref(cid, new URL(m.base, pageUrl).href)
      entry.promise = slot(() => headState(fetchFn, url)).then((state) => {
        // An invalidate while this HEAD was out leaves the old answer unrecorded.
        if (answers.get(key) === entry) entry.state = state
        return state
      })
      answers.set(key, entry)
    }
    return entry
  }

  /** Each store's entry for `cid`: a fixed state for what the page knows, else a HEAD's. */
  const entries = (cid, hereHeld, held = []) => stores.map((m) => {
    const isHere = m === open
    const fixed = held.includes(m.alias) ? 'held'
      : isHere && (hereHeld !== null || !m.base) ? (hereHeld === false ? 'not' : 'held') : null
    const entry = fixed ? { state: fixed, promise: Promise.resolve(fixed) } : ask(m, cid)
    return { m, isHere, entry }
  })
  const shape = ({ m, isHere }, state) => ({ alias: m.alias, key: m.key, here: isHere, state })

  function now(cid, { hereHeld = true, held = [] } = {}) {
    return entries(cid, hereHeld, held).map(e => shape(e, e.entry.state))
  }

  function states(cid, { hereHeld = true, held = [] } = {}) {
    const es = entries(cid, hereHeld, held)
    return Promise.all(es.map(e => e.entry.promise.then(state => shape(e, state))))
  }

  function dots(cid, { hereHeld = true, held = [] } = {}) {
    const node = h('span', { class: 'presence', role: 'img', 'data-presence': cid })
    const es = entries(cid, hereHeld, held)
    const current = es.map(e => shape(e, e.entry.state))
    draw(node, current)
    es.forEach((e, i) => {
      if (e.entry.state === 'pending') e.entry.promise.then((state) => { current[i] = shape(e, state); draw(node, current) })
    })
    return node
  }

  function invalidate(cid = null) {
    if (cid === null) answers.clear()
    else for (const key of [...answers.keys()]) if (key.endsWith(` ${cid}`)) answers.delete(key)
  }

  return { dots, states, now, invalidate, open: open.alias, several }
}

/**
 * The Download for a block that is not here: disabled, its tooltip naming the
 * stores that hold it once they answer (mockup 05: "Missing file"). `attrs`
 * go to button() (size, kind).
 */
export function downloadElsewhere(pres, cid, attrs = {}) {
  const say = (states) => elsewhereText(states, pres.open)
  const btn = button('Download', { ...attrs, disabled: true, title: say(pres.now(cid, { hereHeld: false })), 'data-download-elsewhere': cid })
  pres.states(cid, { hereHeld: false }).then((states) => btn.setAttribute('title', say(states)))
  return btn
}

/** The page's presence (ctx.presence), or for a caller that gives none the one store ctx names, which asks nothing. */
export const presenceOf = (ctx) => ctx?.presence ?? createPresence(null, { here: ctx?.store ?? null })
