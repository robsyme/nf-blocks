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
import { Tray } from './tray.js'
import { writer } from './write.js'
import { h } from './html.js'
import * as views from './views.js'

const ROUTES = [
  ['home', /^#?\/?$/, (ex) => views.home(ex)],
  ['idle', /^#\/idle$/, () => views.idle()],
  ['pipeline', /^#\/pipeline\/([^/?]+)(?:\?offset=(\d+))?$/, (ex, m) => views.pipeline(ex, decodeURIComponent(m[1]), Number(m[2] ?? 0))],
  ['run', /^#\/run\/([^/]+)$/, (ex, m) => views.run(ex, m[1])],
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
  ['compose', /^#\/compose$/, (ex, m, ctx) => views.compose(ex, ctx)],
]

let explorer = null
let sequence = 0
let tray = null
let write = null
let store = null

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
  document.body.dataset.state = 'loading'
  document.body.dataset.route = route ? route[0] : 'unknown'
  const progress = h('p', { id: 'progress', class: 'muted' })
  main.replaceChildren(h('p', { class: 'muted' }, 'Loading...'), progress)
  const ctx = {
    progress: (done, total) => { if (mine === sequence) progress.textContent = `fetched ${done} of ${total} blocks`; updateStale() },
    tray, write, trayChanged: updateTray, rerender: render,
  }
  try {
    if (!route) throw Object.assign(new Error(`there is no view for ${hash}`), { code: 'bad_route' })
    const node = await route[2](explorer, hash.match(route[1]), ctx)
    if (mine !== sequence) return
    main.replaceChildren(node)
    updateStale()
    finish('ready')
  } catch (e) {
    if (mine !== sequence) return
    main.replaceChildren(views.errorNode(e))
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
  el.textContent = `Tray (${tray.size})`
}

function outcome(value) {
  document.body.dataset.writeOutcome = value
  document.body.dataset.writeSeq = String(Number(document.body.dataset.writeSeq ?? 0) + 1)
}

/**
 * One write attempt from a view: shows progress in `status`, refreshes the
 * tail so the page sees its own write (decision 14), opens `href`, then
 * records the outcome for a driver on body[data-write-*].
 */
async function runWrite(status, attempt) {
  status.replaceChildren(h('p', { class: 'muted' }, 'Saving...'))
  let done
  try {
    done = await attempt()
  } catch (e) {
    const node = views.errorNode(e)
    if (e.code === 'stale_supersedes') node.append(' Someone changed this since the page loaded; reload to see the current state.')
    status.replaceChildren(node)
    outcome(e.code ?? 'write_failed')
    return
  }
  if (done?.outcome === 'exists') {
    outcome('exists')
    return
  }
  if (done?.address) document.body.dataset.written = done.address
  if (store.member !== write.writable) {
    outcome('written')
    location.assign(hrefIn(write.writable, done.href))
    return
  }
  await explorer.refreshTail(listLog)
  history.pushState(null, '', done.href)
  await render()
  outcome('written')
}

function renderMembers(store) {
  const nav = document.getElementById('members')
  nav.replaceChildren(...store.members.map(m => {
    const url = new URL(location.href)
    url.searchParams.set('member', m.alias)
    return h('a', { href: url.href, 'aria-current': m.alias === store.member ? 'page' : null, style: 'margin-right: .75rem' },
      m.alias, m.writable ? ' (writable)' : '')
  }))
}

async function start() {
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
      writer: reason === null ? writer({ endpoint: new URL('api/put', location.href).href.split('?')[0], token }) : null,
      hrefFor: (hash) => (store.member === writable ? hash : hrefIn(writable, hash)),
      run: runWrite,
    }
    document.body.dataset.write = write.available ? 'available' : 'unavailable'
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
  } catch (e) {
    document.getElementById('main').replaceChildren(views.errorNode(e))
    finish('error')
    return
  }
  window.addEventListener('hashchange', render)
  await render()
}

start()
