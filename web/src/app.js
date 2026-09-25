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
import { h } from './html.js'
import * as views from './views.js'

const ROUTES = [
  ['home', /^#?\/?$/, (ex) => views.home(ex)],
  ['idle', /^#\/idle$/, () => views.idle()],
  ['pipeline', /^#\/pipeline\/([^/?]+)(?:\?offset=(\d+))?$/, (ex, m) => views.pipeline(ex, decodeURIComponent(m[1]), Number(m[2] ?? 0))],
  ['run', /^#\/run\/([^/]+)$/, (ex, m) => views.run(ex, m[1])],
  ['collection', /^#\/collection\/([^/?]+)(?:\?offset=(\d+))?$/, (ex, m) => views.collection(ex, m[1], Number(m[2] ?? 0))],
  ['item', /^#\/item\/([^/]+)\/([^/]+)$/, (ex, m) => views.item(ex, m[1], m[2])],
  ['content', /^#\/content\/([^/]+)$/, (ex, m, ctx) => views.content(ex, m[1], ctx)],
  ['latest', /^#\/latest\/([^/]+)$/, (ex, m) => views.latest(ex, decodeURIComponent(m[1]))],
  ['items', /^#\/items\/([^/]+)\/([^/?]+)(?:\?where=(.*))?$/, (ex, m, ctx) =>
    views.items(ex, m[1], decodeURIComponent(m[2]), m[3] ? decodeURIComponent(m[3]) : '[]', ctx)],
]

let explorer = null
let sequence = 0

function finish(state) {
  document.body.dataset.state = state
  document.body.dataset.render = String(Number(document.body.dataset.render) + 1)
}

function updateStale() {
  const el = document.getElementById('stale')
  el.dataset.staleCount = String(explorer.staleCount)
  const parts = [`${explorer.staleCount} run${explorer.staleCount === 1 ? '' : 's'} newer than the snapshot`]
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
  const ctx = { progress: (done, total) => { if (mine === sequence) progress.textContent = `fetched ${done} of ${total} blocks`; updateStale() } }
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
    const store = await resolveStore(location.href)
    const blocks = new BlockFetcher(store.base)
    window.__nfBlocks.verified = blocks.verified
    document.getElementById('where').textContent = store.base
    if (store.members) renderMembers(store)
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
