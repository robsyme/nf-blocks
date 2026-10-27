// web/src/app.js
// Opens the member's snapshot and routes the location hash to a view
// (DESIGN.md §15). Sets body[data-state], [data-route] and [data-render] for
// every render, so a driver can wait for one to finish.
import { DEFAULT_CAP_BYTES } from './config.js'
import { openSnapshot } from './db.js'
import { createInlineWorker, inlineWasm } from './inline.js'
import { BlockFetcher } from './blocks.js'
import { listLog } from './storelog.js'
import { createMembers, memberOpener } from './members.js'
import { createPresence } from './presence.js'
import { explainFailure, resolveStore } from './store.js'
import { Explorer } from './model.js'
import { Tray, safeSessionStorage, trayNote } from './tray.js'
import { writer } from './write.js'
import { copyAvailability, copyClient } from './copy.js'
import { bannerText, bannerFrom, refreshFailureLines } from './save-flow.js'
import { h, link } from './html.js'
import * as views from './views/index.js'
import { PAIRS_CSS } from './pairs.js'
import { KIT_CSS } from './kit-css.js'
import { APP_CSS } from './styles.js'
import { storeKey, storeName } from './storename.js'
import { identityChip } from './views/identity.js'
import { viewerOf } from './identity.js'
import { ICON_CSS } from './icons.js'
import { createPanel, showsRail, useLabel } from './panel.js'
import { argsOf, routeOf } from './routes.js'
import { createNav, drawStores } from './nav.js'
import { createIndexing, indexChangeHandler, indexClient } from './indexing.js'
import { createSizes, memberHref, mountSwitcher } from './views/switcher.js'
import { agoText, fullWhenText, snapshotModeText, staleText } from './words.js'

let explorer = null
let sequence = 0
let tray = null
// Which run and output each collection is, recorded by the views that know both; the picked panel reads it instead of the snapshot.
const sources = new Map()
let write = null
// Whether this page can copy to another member, and the client that does (P3 interface 1.7).
let copy = null
let store = null
let members = null
// Which stores hold each block the page shows, asked once per page load (P3 rule D4).
let presence = null
let sizes = null
let storesLoader = null
let redrawStores = null
let indexing = null
let busy = false
let panel = null
let nav = null
// A partly failed save's banner, waiting for the saved Selection's page to render.
let pendingBanner = null
const OUTCOME_KEY = 'nf-blocks-write-outcome'

/**
 * Whether a file exists, from its status alone: a GET for the first byte (S3
 * CORS rules may not allow HEAD), the body dropped unread so a server without
 * Range support does not send the whole file. 200 and 206 mean present;
 * 403, 404 and anything else, including a failed request, mean absent.
 */
async function urlExists(url) {
  const controller = new AbortController()
  try {
    const res = await fetch(url, { headers: { Range: 'bytes=0-0' }, signal: controller.signal })
    return res.status === 200 || res.status === 206
  } catch { return false } finally { controller.abort() }
}

function finish(state) {
  document.body.dataset.state = state
  document.body.dataset.render = String(Number(document.body.dataset.render) + 1)
}

function updateStale() {
  const el = document.getElementById('stale')
  el.dataset.staleCount = String(explorer.staleCount)
  // "0 newer runs" would read as current when the listing never answered.
  el.dataset.log = explorer.logReadable ? 'read' : 'unreadable'
  const parts = [staleText(explorer)]
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
  // Size walks the last page queued stop here (Explorer.pageChanged); what they finished stays cached.
  explorer?.pageChanged()
  const main = document.getElementById('main')
  const hash = location.hash || '#/'
  const route = routeOf(hash)
  const name = route?.name ?? 'unknown'
  document.body.dataset.state = 'loading'
  document.body.dataset.route = name
  document.querySelector('.app')?.classList.toggle('has-rail', showsRail(name))
  const progress = h('p', { id: 'progress', class: 'muted' })
  main.replaceChildren(h('p', { class: 'muted' }, 'Loading...'), progress)
  // The left column forgets the last run until this view marks where the reader is (it reads nothing).
  nav?.mark({}).catch(() => {})
  let view = null
  let rendered = false
  const ctx = {
    progress: (done, total) => { if (mine === sequence) progress.textContent = `fetched ${done} of ${total} blocks`; updateStale() },
    tray, write, copy, sizes, sources, trayChanged: updateTray, rerender: render, members, presence, indexing,
    // Who is signing: the server's report, and the principal alone (null when unknown or anonymous).
    identity: store.identity ?? null, viewer: viewerOf(store.identity),
    // What the view shows, for the panel (layout B spec §6); redrawn when it changes after the render.
    use: (v) => { if (mine !== sequence) return; view = v; if (rendered) panel.draw({ route: name, view }) },
    // Where the reader is, for the left column (plan P1: `expand` only from the run and pipeline views).
    mark: (where) => { if (mine === sequence) nav.mark(where).catch(() => {}) },
    // Which store the page reads, and the same page in the others (the block page, visual language spec §4.5).
    // In all-stores mode (?member=*) the page reads the primary; a list row opens its own member through hrefIn.
    store: { name: storeName(store), key: storeKey(store), label: storeName(store), all: !!store.all,
      hrefIn: (alias, hash) => (alias === store.member && !store.all ? hash : hrefIn(alias, hash)),
      others: (hash) => (store.members ?? []).filter(m => m.alias !== store.member).map(m => ({ alias: m.alias, href: hrefIn(m.alias, hash) })) },
  }
  try {
    if (!route) throw Object.assign(new Error(`there is no view for ${hash}`), { code: 'bad_route' })
    const node = await route.view(explorer, argsOf(route, hash), ctx)
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
  } catch (failure) {
    const e = await explainFailure(failure, location.href)
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
  el.querySelector('.count').textContent = String(tray.size)
  el.setAttribute('aria-label', `Picked (${tray.size})`)
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
 * An attempt that wrote something but partly failed returns `stay` (a node)
 * instead of `href`: the tail and the left column are refreshed, the page
 * stays, and `status` shows `stay`. It records `written`, as a save whose
 * naming failed does (decision 23).
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
    } catch (failure) {
      const e = await explainFailure(failure, location.href)
      recorded = e.code ?? 'write_failed'
      status.replaceChildren(views.writeFailureNode(e))
      return
    }
    if (done?.outcome === 'exists' || done?.outcome === 'elsewhere') {
      recorded = done.outcome
      return
    }
    if (done?.address) document.body.dataset.written = done.address
    if (done?.stay) {
      recorded = 'written'
      // Another member's write is not in this member's tail; the node links to it there.
      if (store.member === write.writable) {
        try {
          await explorer.refreshTail(listLog)
          await nav.reloadAfterWrite()
        } catch {
          // The node still says what was written and links to it.
        }
      }
      status.replaceChildren(done.stay)
      return
    }
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
      await nav.reloadAfterWrite().catch(() => {})
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

/** The top bar's store switcher and the left column's Stores section, one entry per member (or the one store read). */
function renderStores(store) {
  const name = storeName(store)
  const wrap = document.getElementById('store-wrap')
  if (!store.members) {
    mountSwitcher(wrap, { store })
    const entry = [h('span', { class: `dot ${storeKey(store)}` }), h('span', { class: 'lbl' }, name)]
    document.getElementById('members').replaceChildren(h('a', { href: '#/', class: 'on', title: store.base }, entry))
    return
  }
  sizes = createSizes(members)
  mountSwitcher(wrap, { store, members, sizes })
  const el = document.getElementById('members')
  const draw = () => drawStores(el, { store, members: members.list, figure: sizes.get, href: (alias) => memberHref(location.href, alias) })
  draw()
  redrawStores = draw
  // Other members' sizes load after the first render, and only when there are others.
  sizes.onChange(draw)
  storesLoader = () => (members.list.length > 1 ? sizes.loadAll() : null)
}

/** The "you" at the right of the top bar, after Picked (mockup 08); none when the page cannot tell who is signing. */
function renderIdentity(identity) {
  const chip = identityChip(identity)
  if (chip) document.getElementById('tray')?.after(chip)
}

async function start() {
  document.head.append(h('style', {}, KIT_CSS), h('style', {}, APP_CSS), h('style', {}, PAIRS_CSS), h('style', {}, ICON_CSS))
  window.__nfBlocks = { verified: [] }
  try {
    store = await resolveStore(location.href)
    const blocks = new BlockFetcher(store.base)
    window.__nfBlocks.verified = blocks.verified
    // The base URL is the store's address for a reader who wants it, not a line of its own (mockups 03/04/08).
    document.getElementById('where').setAttribute('title', store.base)
    renderIdentity(store.identity)
    tray = new Tray()
    const token = new URL(location.href).searchParams.get('token')
    const writable = store.members?.find(m => m.writable)?.alias ?? null
    const reason = !store.members ? 'Read only here: writing needs this page opened through `nextflow plugin nf-blocks:explore`.'
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
    // The same token and origin as api/put; the server checks both.
    const copyable = copyAvailability(store, { token })
    copy = { ...copyable, client: copyable.available ? copyClient({ endpoint: new URL('api/copy', location.href).href.split('?')[0], token }) : null }
    restoreOutcome()
    updateTray()
    const cap = Number(new URL(location.href).searchParams.get('cap')) || DEFAULT_CAP_BYTES
    let mode = null
    const open = memberOpener(store, {
      pageUrl: location.href, primaryBlocks: blocks,
      hasFile: urlExists,
      openDb: async (url, { primary }) => {
        const db = await openSnapshot(url, { cap, wasm: inlineWasm(), createWorker: createInlineWorker })
        if (primary) mode = db.mode
        return db
      },
    })
    members = createMembers(store, { openExplorer: open })
    presence = createPresence(members, { pageUrl: location.href, here: { name: storeName(store), key: storeKey(store), base: store.base } })
    renderStores(store)
    // Index jobs run on the server (POST /api/index); the page starts them and polls while one runs.
    if (store.members && token) {
      indexing = createIndexing(members, {
        client: indexClient({ endpoint: new URL('api/index', location.href).href.split('?')[0], token }),
        fetchStatus: () => fetch('api/index', { cache: 'no-store' }).then(r => r.json()),
      })
      // The primary's snapshot is the page's own open file: a job that rewrites it reloads the page (final review Important 1).
      indexing.onChange(indexChangeHandler({ primary: store.member, sizes, redrawStores, presence, render, reload: () => location.reload(),
        storageShowing: () => (location.hash || '').startsWith('#/storage') }))
      indexing.watch()
    }
    // The primary is opened here, once; the other members only when something asks members.explorer(alias).
    explorer = await (store.members ? members.explorer(store.member) : open({ alias: null, base: store.base }))
    const modeEl = document.getElementById('snapshot-mode')
    modeEl.dataset.mode = mode
    modeEl.textContent = snapshotModeText(mode)
    const written = explorer.snapshotWrittenAt
    const ageEl = document.getElementById('snapshot-age')
    ageEl.textContent = written ? `Snapshot ${agoText(written)}` : 'Snapshot'
    if (written) ageEl.setAttribute('title', `Snapshot written ${fullWhenText(written)}`)
    updateStale()
    panel = createPanel({ el: document.getElementById('panel-inner'), ex: explorer, ctx: { tray, write, sources, trayChanged: updateTray },
      onState: (s) => { document.getElementById('use-toggle').textContent = useLabel(s, tray) } })
    nav = createNav(explorer, { el: document.getElementById('nav-body'), countEl: document.getElementById('nav-pipeline-count'),
      projectCountEl: document.getElementById('nav-project-count'), storesEl: document.getElementById('members') })
    // Before the first render, so its snapshot reads fall in the open phase (plan P1).
    await nav.load()
  } catch (failure) {
    // The box ships enabled in index.html; with no snapshot it has nothing to search.
    document.getElementById('search').disabled = true
    document.getElementById('main').replaceChildren(views.errorNode(await explainFailure(failure, location.href)))
    finish('error')
    return
  }
  // The top bar's search (P1a Task 4): a typeahead under the box, `/` to focus it from anywhere but a field.
  const searchInput = document.getElementById('search')
  const searchMenu = views.searchBox(searchInput, explorer, { menu: document.getElementById('search-menu') })
  searchMenu.sync(location.hash)
  window.addEventListener('hashchange', () => { searchMenu.close(); searchMenu.sync(location.hash) })
  document.addEventListener('keydown', (e) => {
    if (!views.slashFocuses(e)) return
    e.preventDefault()
    searchInput.focus()
    searchInput.select()
  })
  const toggle = (key) => { document.body.dataset[key] = document.body.dataset[key] === 'open' ? 'closed' : 'open' }
  document.getElementById('nav-toggle').addEventListener('click', () => toggle('nav'))
  document.getElementById('use-toggle').addEventListener('click', () => toggle('panel'))
  window.addEventListener('hashchange', () => { document.body.dataset.nav = 'closed'; document.body.dataset.panel = 'closed' })
  window.addEventListener('hashchange', render)
  await render()
  storesLoader?.()
}

start()
