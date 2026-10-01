// web/src/app.js
// Opens the member's snapshot and routes the location hash to a view
// (DESIGN.md §15). Sets body[data-state], [data-route] and [data-render] for
// every render, so a driver can wait for one to finish.
import { DEFAULT_CAP_BYTES } from './config.js'
import { openSnapshot } from './db.js'
import { createInlineWorker, inlineWasm } from './inline.js'
import { BlockFetcher } from './blocks.js'
import { listLog } from './storelog.js'
import { resolveStore } from './store.js'
import { Explorer } from './model.js'
import { Tray, safeSessionStorage, trayNote } from './tray.js'
import { writer } from './write.js'
import { bannerText, bannerFrom, refreshFailureLines } from './save-flow.js'
import { h, link } from './html.js'
import * as views from './views/index.js'
import { PAIRS_CSS } from './pairs.js'
import { APP_CSS } from './styles.js'
import { createPanel, useLabel } from './panel.js'
import { createNav } from './nav.js'

const ROUTES = [
  ['home', /^#?\/?$/, (ex) => views.home(ex)],
  ['idle', /^#\/idle$/, () => views.idle()],
  ['pipeline', /^#\/pipeline\/([^/?]+)(?:\?offset=(\d+))?$/, (ex, m) => views.pipeline(ex, decodeURIComponent(m[1]), Number(m[2] ?? 0))],
  ['run', /^#\/run\/([^/]+)$/, (ex, m, ctx) => views.run(ex, m[1], ctx)],
  ['collection', /^#\/collection\/([^/?]+)(?:\?offset=(\d+))?$/, (ex, m, ctx) => views.collection(ex, m[1], Number(m[2] ?? 0), ctx)],
  ['item', /^#\/item\/([^/]+)\/([^/]+)$/, (ex, m, ctx) => views.item(ex, m[1], m[2], ctx)],
  ['content', /^#\/content\/([^/]+)$/, (ex, m, ctx) => views.content(ex, m[1], ctx)],
  ['latest', /^#\/latest\/([^/]+)$/, (ex, m) => views.latest(ex, decodeURIComponent(m[1]))],
  ['items', /^#\/items\/([^/]+)\/([^/?]+)(?:\?where=(.*))?$/, (ex, m, ctx) =>
    views.items(ex, m[1], decodeURIComponent(m[2]), m[3] ? decodeURIComponent(m[3]) : '[]', ctx)],
  ['selections', /^#\/selections(?:\?(.*))?$/, (ex, m, ctx) => {
    const q = new URLSearchParams(m[1] ?? '')
    return views.selections(ex, { offset: Number(q.get('offset') ?? 0), deleted: q.get('deleted') === '1' }, ctx)
  }],
  ['selection', /^#\/selection\/([^/?]+)$/, (ex, m, ctx) => views.selection(ex, m[1], ctx)],
  ['compose', /^#\/compose$/, (ex, m, ctx) => { document.body.dataset.panel = 'open'; return views.home(ex, ctx) }],
]

let explorer = null
let sequence = 0
let tray = null
let write = null
let store = null
let busy = false
let panel = null
let nav = null
// A partly failed save's banner, waiting for the saved Selection's page to render.
let pendingBanner = null
const OUTCOME_KEY = 'nf-blocks-write-outcome'

function finish(state) {
  document.body.dataset.state = state
  document.body.dataset.render = String(Number(document.body.dataset.render) + 1)
}

function updateStale() {
  const el = document.getElementById('stale')
  el.dataset.staleCount = String(explorer.staleCount)
  // "0 runs newer" would read as current when the listing never answered.
  el.dataset.log = explorer.logReadable ? 'read' : 'unreadable'
  const count = `${explorer.staleCount} run${explorer.staleCount === 1 ? '' : 's'} newer than the snapshot`
  const parts = [explorer.logReadable ? count
    : `Store Log not readable here, so runs newer than the snapshot are unknown${explorer.staleCount ? ` (${count} found)` : ''}`]
  if (explorer.notice) {
    el.dataset.notice = ''
    parts.push('; to rewrite it run ', h('code', { 'data-command': '' }, 'nextflow plugin nf-blocks:snapshot'),
      ' or open the member through ', h('code', {}, 'nextflow plugin nf-blocks:explore'))
  } else {
    delete el.dataset.notice
  }
  el.replaceChildren(...parts)
}

async function render() {
  const mine = ++sequence
  const main = document.getElementById('main')
  const hash = location.hash || '#/'
  const route = ROUTES.find(([, pattern]) => pattern.test(hash))
  const name = route ? route[0] : 'unknown'
  document.body.dataset.state = 'loading'
  document.body.dataset.route = name
  const progress = h('p', { id: 'progress', class: 'muted' })
  main.replaceChildren(h('p', { class: 'muted' }, 'Loading...'), progress)
  let view = null
  let rendered = false
  const ctx = {
    progress: (done, total) => { if (mine === sequence) progress.textContent = `fetched ${done} of ${total} blocks`; updateStale() },
    tray, write, trayChanged: updateTray, rerender: render,
    // What the view shows, for the panel (layout B spec §6); redrawn when it changes after the render.
    use: (v) => { if (mine !== sequence) return; view = v; if (rendered) panel.draw({ route: name, view }) },
    // Where the reader is, for the left column (plan P1: `expand` only from the run and pipeline views).
    mark: (where) => { if (mine === sequence) nav.mark(where).catch(() => {}) },
  }
  try {
    if (!route) throw Object.assign(new Error(`there is no view for ${hash}`), { code: 'bad_route' })
    const node = await route[2](explorer, hash.match(route[1]), ctx)
    if (mine !== sequence) return
    if (pendingBanner) {
      if (hash === `#/selection/${pendingBanner.address}`) node.prepend(views.failureBanner(pendingBanner.address, pendingBanner.failures, ctx))
      pendingBanner = null
    }
    main.replaceChildren(node)
    rendered = true
    await panel.draw({ route: name, view }).catch(() => {})
    if (mine !== sequence) return
    updateStale()
    finish('ready')
  } catch (e) {
    if (mine !== sequence) return
    main.replaceChildren(views.errorNode(e))
    await panel.draw({ route: 'error', view: null }).catch(() => {})
    finish('error')
  }
}

/** The page URL for another member, keeping every other query parameter (the token among them). */
function hrefIn(alias, hash) {
  const url = new URL(location.href)
  url.searchParams.set('member', alias)
  url.hash = hash
  return url.href
}

function updateTray() {
  const el = document.getElementById('tray')
  el.dataset.count = String(tray.size)
  el.textContent = `Picked (${tray.size})`
  const note = document.getElementById('tray-unsaved')
  note.textContent = trayNote(tray)
  note.hidden = !note.textContent
  panel?.draw()
}

function outcome(value) {
  document.body.dataset.writeOutcome = value
  document.body.dataset.writeSeq = String(Number(document.body.dataset.writeSeq ?? 0) + 1)
}

/** A write that ends by opening another member's page carries its outcome, and any failure banner, there (one sessionStorage key). */
function carryOutcome() {
  try {
    const { writeOutcome, writeSeq, written } = document.body.dataset
    safeSessionStorage()?.setItem(OUTCOME_KEY, JSON.stringify({ writeOutcome, writeSeq, written: written ?? null,
      banner: pendingBanner ? bannerText(pendingBanner) : null }))
  } catch {
    // Storage refused; the next page starts without the outcome.
  }
}

function restoreOutcome() {
  try {
    const storage = safeSessionStorage()
    const carried = storage?.getItem(OUTCOME_KEY)
    if (!carried) return
    storage.removeItem(OUTCOME_KEY)
    const { writeOutcome, writeSeq, written, banner } = JSON.parse(carried)
    if (writeOutcome) document.body.dataset.writeOutcome = writeOutcome
    if (writeSeq) document.body.dataset.writeSeq = writeSeq
    if (written) document.body.dataset.written = written
    pendingBanner = bannerFrom(banner)
  } catch {
    // Nothing readable carried over.
  }
}

/**
 * One write attempt from a view: shows progress in `status`, refreshes the
 * tail so the page sees its own write (decision 14), opens `href`, then
 * records the outcome for a driver on body[data-write-*]. One attempt at a
 * time: a second while one runs is ignored, and `button` stays disabled
 * until the attempt ends. The outcome is recorded however the attempt ends.
 */
async function runWrite(status, attempt, button = null) {
  if (busy) return
  busy = true
  if (button) button.disabled = true
  let recorded = 'write_failed'
  try {
    status.replaceChildren(h('p', { class: 'muted' }, 'Saving...'))
    let done
    try {
      done = await attempt()
    } catch (e) {
      recorded = e.code ?? 'write_failed'
      const node = views.errorNode(e)
      if (e.code === 'stale_supersedes') node.append(' Someone changed this since the page loaded; reload to see the current state.')
      status.replaceChildren(node)
      return
    }
    if (done?.outcome === 'exists' || done?.outcome === 'elsewhere') {
      recorded = done.outcome
      return
    }
    if (done?.address) document.body.dataset.written = done.address
    // A save whose naming or restoring failed still wrote the Selection: its
    // page opens with a banner naming each failure (decision 23). Kept local
    // until the render (or navigation) meant to show it, and only then
    // assigned to `pendingBanner`, so a render already in flight cannot clear
    // it first (final review finding 2).
    const banner = done?.failures?.length ? { address: done.address, failures: done.failures } : null
    recorded = 'written'
    if (store.member !== write.writable) {
      outcome(recorded)
      recorded = null
      pendingBanner = banner
      carryOutcome()
      location.assign(hrefIn(write.writable, done.href))
      return
    }
    try {
      await explorer.refreshTail(listLog)
      await nav.reloadSelections().catch(() => {})
      pendingBanner = banner
      history.pushState(null, '', done.href)
      await render()
    } catch (e) {
      const node = h('p', { class: 'warn', 'data-refresh-failed': '' },
        `Saved, but the page could not refresh (${e?.message ?? e}); reload to see it.`)
      // The render that would have shown `failureBanner` never happened: say
      // the same thing here instead, with a link to the saved Selection
      // (final review finding 1).
      if (banner) for (const { text, href } of refreshFailureLines(banner, write.hrefFor)) node.append(' ', text, ' ', link(href, 'Open it'))
      status.replaceChildren(node)
    }
  } finally {
    busy = false
    if (button?.isConnected) button.disabled = false
    if (recorded) outcome(recorded)
  }
}

function renderMembers(store) {
  const nav = document.getElementById('members')
  nav.replaceChildren(...store.members.map(m => {
    const url = new URL(location.href)
    url.searchParams.set('member', m.alias)
    return h('a', { href: url.href, 'aria-current': m.alias === store.member ? 'page' : null },
      m.alias, m.writable ? ' (writable)' : '')
  }))
}

async function start() {
  document.head.append(h('style', {}, APP_CSS), h('style', {}, PAIRS_CSS))
  window.__nfBlocks = { verified: [] }
  try {
    store = await resolveStore(location.href)
    const blocks = new BlockFetcher(store.base)
    window.__nfBlocks.verified = blocks.verified
    document.getElementById('where').textContent = store.base
    if (store.members) renderMembers(store)
    tray = new Tray()
    const token = new URL(location.href).searchParams.get('token')
    const writable = store.members?.find(m => m.writable)?.alias ?? null
    const reason = !store.members ? 'Composing, rename and delete need this page opened through `nextflow plugin nf-blocks:explore`, which holds the write endpoint.'
      : !store.write || !writable ? 'This explore server has no writable member.'
      : !token ? 'Open this page with the URL nf-blocks:explore printed; it carries the write token.'
      : null
    write = {
      available: reason === null, reason, served: !!store.members, writable,
      // Rename, delete and undo write Claims beside the Selection's own member: only offered there.
      here: store.member !== null && store.member === writable,
      writer: reason === null ? writer({ endpoint: new URL('api/put', location.href).href.split('?')[0], token }) : null,
      hrefFor: (hash) => (store.member === writable ? hash : hrefIn(writable, hash)),
      run: runWrite,
    }
    document.body.dataset.write = write.available ? 'available' : 'unavailable'
    restoreOutcome()
    updateTray()
    const cap = Number(new URL(location.href).searchParams.get('cap')) || DEFAULT_CAP_BYTES
    let mode = null
    explorer = await Explorer.open({
      base: store.base, blocks, listFn: listLog,
      openDb: async (url) => {
        const db = await openSnapshot(url, { cap, wasm: inlineWasm(), createWorker: createInlineWorker })
        mode = db.mode
        return db
      },
    })
    const modeEl = document.getElementById('snapshot-mode')
    modeEl.dataset.mode = mode
    modeEl.textContent = mode === 'whole' ? 'snapshot downloaded whole: this server ignores Range' : 'snapshot read by range'
    updateStale()
    panel = createPanel({ el: document.getElementById('panel-inner'), ex: explorer, ctx: { tray, write, trayChanged: updateTray },
      onState: (s) => { document.getElementById('use-toggle').textContent = useLabel(s, tray) } })
    nav = createNav(explorer, { el: document.getElementById('nav-body') })
    // Before the first render, so its snapshot reads fall in the open phase (plan P1).
    await nav.load()
  } catch (e) {
    document.getElementById('main').replaceChildren(views.errorNode(e))
    finish('error')
    return
  }
  const toggle = (key) => { document.body.dataset[key] = document.body.dataset[key] === 'open' ? 'closed' : 'open' }
  document.getElementById('nav-toggle').addEventListener('click', () => toggle('nav'))
  document.getElementById('use-toggle').addEventListener('click', () => toggle('panel'))
  window.addEventListener('hashchange', () => { document.body.dataset.nav = 'closed'; document.body.dataset.panel = 'closed' })
  window.addEventListener('hashchange', render)
  await render()
}

start()
