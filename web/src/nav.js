// web/src/nav.js
// The left column's tree (explorer layout B spec §4, drawn as the kit's nav):
// the newest projects and the "Not in a project" link (P4, mockup 13), then
// pipelines A–Z, each opening onto its newest runs with a status dot and a
// date, then the newest Selections. index.html holds the Browse links, the
// Stores section and the store status around it. It reads the snapshot at
// load, on a reader's click, and when the run or pipeline view asks (mark with
// expand), so query 3's measured cost stays the query's own (Gate assertion 2,
// plan P1). Its attributes are data-nav-*, never the Gate's data-run or
// data-selection. The tree opens with the Storage link, so it sits under
// Browse; the open store's entry in Stores shows what the store holds
// (Explorer.storeStats, read once at load) when the snapshot has figures.
import { h } from './html.js'
import { icon, sealed } from './icons.js'
import { bytes } from './format.js'
import { dayText, runTone, statusText, whenText } from './words.js'

const NAV_RUNS = 10
const NAV_SELECTIONS = 5
const NAV_PROJECTS = 5
const enc = encodeURIComponent
const warn = (what, e) => h('p', { class: 'warn' }, `${what}: ${e?.message ?? e}`)
const DOT = { good: 'ok', bad: 'fail' }

const STORAGE_ICON = '<svg class="nav-ico" viewBox="0 0 24 24" aria-hidden="true"><ellipse cx="12" cy="5.5" rx="7.5" ry="2.5"/><path d="M4.5 5.5v13c0 1.4 3.4 2.5 7.5 2.5s7.5-1.1 7.5-2.5v-13"/><path d="M4.5 12c0 1.4 3.4 2.5 7.5 2.5s7.5-1.1 7.5-2.5"/></svg>'

/** The Browse link to #/storage, with the kit's database icon. */
function storageLink() {
  const ico = h('span', { class: 'nav-ico-wrap', 'aria-hidden': 'true' })
  ico.innerHTML = STORAGE_ICON
  return h('a', { href: '#/storage', 'data-nav-link': 'storage' }, ico, h('span', { class: 'lbl' }, 'Storage'))
}

/**
 * Puts `stored` bytes on the open store's entry (`a.on`) in `storesEl`,
 * ahead of what its <small> already says (`writable`).
 */
function showStored(storesEl, stored) {
  const entry = storesEl?.querySelector('a.on')
  if (!entry) return
  // A drawn Stores section already says it (drawStores).
  if (entry.querySelector('[data-store-stored]')) return
  let small = entry.querySelector('small')
  const said = small && !small.hasAttribute('data-store-stored') ? small.textContent : null
  if (!small) entry.append(small = h('small', {}))
  small.textContent = [bytes(stored), said].filter(Boolean).join(' · ')
  small.setAttribute('data-store-stored', String(stored))
  small.setAttribute('title', 'Stored in this store, as of the snapshot')
}

/**
 * The Stores section for several members: each one's dot, alias and what its
 * store holds (its own figure, never a sum), "no snapshot" for a member with
 * none. `figure(alias)` is `{ state, stored }` (createSizes.get); `href(alias)`
 * opens the page on that member. The open member is marked; in all-stores
 * mode none is.
 */
export function drawStores(el, { store, members, figure, href }) {
  el.replaceChildren(...members.map((m) => {
    // All stores (?member=*) opens none of them in particular.
    const here = !store.all && m.alias === store.member
    const f = figure(m.alias)
    const said = f.state !== 'open' && m.index?.state === 'indexing' ? 'indexing…'
      : f.state === 'no_snapshot' ? 'no snapshot' : f.state === 'failed' ? 'unavailable'
      : [f.state === 'open' && f.stored !== null ? bytes(f.stored) : null, m.writable ? 'writable' : null].filter(Boolean).join(' · ')
    const small = h('small', f.state === 'open' && f.stored !== null ? { 'data-store-stored': String(f.stored), title: 'Stored in this store, as of the snapshot' } : {}, said)
    return h('a', { href: href(m.alias), class: here ? 'on' : null, 'aria-current': here ? 'page' : null, 'data-store-entry': m.alias },
      h('span', { class: `dot ${m.key}` }), h('span', { class: 'lbl' }, m.alias), said ? small : null)
  }))
}

/**
 * `el` takes the tree; `countEl`, when given, the number of pipelines (the
 * Browse link's count); `projectCountEl` the number of projects;
 * `storesEl`, when given, the Stores list, whose open entry gets the
 * store's stored bytes.
 */
export function createNav(ex, { el, countEl = null, projectCountEl = null, storesEl = null }) {
  let pipelines = []
  let selections = []
  let projects = []
  let pipelinesError = null
  let selectionsError = null
  let projectsError = null
  const open = new Set()
  const runs = new Map()       // pipeline -> { rows, total } once read
  const runErrors = new Map()
  const asking = new Map()
  let here = { pipeline: null, run: null, project: null }

  function readRuns(name) {
    if (runs.has(name)) return Promise.resolve()
    if (!asking.has(name)) {
      asking.set(name, ex.runPage(name, { limit: NAV_RUNS }).then(
        (page) => { runs.set(name, page); runErrors.delete(name) },
        (e) => { runErrors.set(name, e) }).finally(() => asking.delete(name)))
    }
    return asking.get(name)
  }

  function runList(name) {
    if (runErrors.has(name)) return warn('Could not list its runs', runErrors.get(name))
    const page = runs.get(name)
    if (!page) return h('p', { class: 'muted' }, 'loading...')
    return h('ul', { class: 'nav-runs' },
      page.rows.map((r) => {
        const current = r.completion_cid === here.run
        const tone = runTone(r)
        return h('li', { 'data-nav-run': r.completion_cid, 'aria-current': current ? 'page' : null },
          h('a', { class: `sub nav-run${current ? ' on' : ''}`, href: `#/run/${r.completion_cid}`, title: [r.run_name ?? 'unnamed run', statusText(r), whenText(r.finished_at)].join(' · ') },
            sealed('run', r.completion_cid, { size: 16, tone }),
            h('span', { class: 'lbl nav-run-name' }, r.run_name ?? 'unnamed run'),
            h('small', { class: 'nav-run-when' }, dayText(r.finished_at, Date.now()))))
      }),
      page.total > page.rows.length ? h('li', {}, h('a', { class: 'sub more-link', href: `#/pipeline/${enc(name)}` }, 'all runs…')) : null)
  }

  function pipelineRow(p) {
    const isOpen = open.has(p.pipeline)
    return h('li', { 'data-nav-pipeline': p.pipeline, class: p.pipeline === here.pipeline ? 'here' : null },
      h('button', { type: 'button', class: 'nav-toggle', 'aria-expanded': String(isOpen), onclick: () => {
        if (open.has(p.pipeline)) open.delete(p.pipeline)
        else { open.add(p.pipeline); readRuns(p.pipeline).then(draw) }
        draw()
      } }, h('span', { class: 'chev', 'aria-hidden': 'true' }, isOpen ? '▾' : '▸'), h('span', { class: 'lbl' }, p.pipeline), h('small', {}, String(p.runs))),
      isOpen ? runList(p.pipeline) : null)
  }

  /** The Projects section's nodes (replaceChildren takes no arrays). */
  function projectList() {
    const unassigned = h('li', {}, h('a', { class: 'sub', href: '#/projects?unassigned=1', 'data-nav-link': 'unassigned' }, h('span', { class: 'lbl muted' }, 'Not in a project')))
    // A failed read keeps the way to the runs and Selections in no project.
    if (projectsError) return [warn('Could not list projects', projectsError), h('ul', { class: 'nav-projects' }, unassigned)]
    return [h('ul', { class: 'nav-projects' },
      projects.slice(0, NAV_PROJECTS).map((p) => {
        const current = p.cid === here.project
        return h('li', { 'aria-current': current ? 'page' : null },
          h('a', { class: `sub${current ? ' on' : ''}`, href: `#/project/${p.cid}`, 'data-nav-project': p.cid }, h('span', { class: 'lbl' }, p.name)))
      }),
      projects.length > NAV_PROJECTS ? h('li', {}, h('a', { class: 'sub more-link', href: '#/projects' }, 'all projects…')) : null,
      unassigned)]
  }

  function draw() {
    if (countEl) countEl.textContent = pipelinesError ? '' : String(pipelines.length)
    if (projectCountEl) projectCountEl.textContent = projectsError || projects.length === 0 ? '' : String(projects.length)
    el.replaceChildren(
      storageLink(),
      h('h2', { class: 'h eyebrow' }, 'Projects'),
      ...projectList(),
      h('h2', { class: 'h eyebrow' }, 'Runs by pipeline'),
      pipelinesError ? warn('Could not list pipelines', pipelinesError)
        : pipelines.length === 0 ? h('p', { class: 'muted' }, 'No runs yet.')
          : h('ul', { class: 'nav-pipelines' }, pipelines.map(pipelineRow)),
      h('h2', { class: 'h eyebrow' }, 'Recent Selections'),
      selectionsError ? warn('Could not list Selections', selectionsError)
        : selections.length === 0 ? h('p', { class: 'muted' }, 'None yet.')
          : h('ul', { class: 'nav-selections' },
            selections.map(s => h('li', {}, h('a', { class: 'sub', href: `#/selection/${s.cid}`, 'data-nav-selection': s.cid }, sealed('selection', s.cid, { size: 16 }),
              h('span', { class: 'lbl' }, s.state?.names?.length ? s.state.names.join(' / ') : 'unnamed'))))))
  }

  async function readProjects() {
    try {
      projects = (await ex.projects()).rows
      projectsError = null
    } catch (e) {
      projectsError = e
    }
  }

  async function readSelections() {
    try {
      selections = (await ex.selectionPage({ limit: NAV_SELECTIONS })).rows
      selectionsError = null
    } catch (e) {
      selectionsError = e
    }
  }

  return {
    async load() {
      await Promise.all([
        ex.pipelines().then((p) => { pipelines = [...p].sort((a, b) => (a.pipeline < b.pipeline ? -1 : a.pipeline > b.pipeline ? 1 : 0)); pipelinesError = null },
          (e) => { pipelinesError = e }),
        readSelections(),
        readProjects(),
        // A lean snapshot has no figures (null); a failed read leaves the entry as it was.
        storesEl && ex.storeStats ? ex.storeStats().then((s) => { if (s) showStored(storesEl, s.stored) }, () => {}) : null,
      ])
      draw()
    },
    /** After a write: the Selections and projects again, and the open pipelines' runs. */
    async reloadAfterWrite() {
      runs.clear()
      await Promise.all([readSelections(), readProjects()])
      for (const name of open) await readRuns(name)
      draw()
    },
    async mark({ pipeline = null, run = null, project = null, expand = false } = {}) {
      here = { pipeline, run, project }
      if (expand && pipeline) {
        open.add(pipeline)
        await readRuns(pipeline)
      }
      draw()
    },
  }
}
