// web/src/nav.js
// The left column (explorer layout B spec §4): a reserved search box,
// pipelines A–Z with their newest runs, and recent Selections; app.js keeps
// the store status beneath it. It reads the snapshot at load, on a reader's
// click, and when the run or pipeline view asks (mark with expand), so query
// 3's measured cost stays the query's own (Gate assertion 2, plan P1). Its
// attributes are data-nav-*, never the Gate's data-run or data-selection.
import { h, link } from './html.js'
import { statusTone, whenText } from './words.js'

const NAV_RUNS = 10
const NAV_SELECTIONS = 5
const enc = encodeURIComponent
const warn = (what, e) => h('p', { class: 'warn' }, `${what}: ${e?.message ?? e}`)

export function createNav(ex, { el }) {
  let pipelines = []
  let selections = []
  let pipelinesError = null
  let selectionsError = null
  const open = new Set()
  const runs = new Map()       // pipeline -> { rows, total } once read
  const runErrors = new Map()
  const asking = new Map()
  let here = { pipeline: null, run: null }

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
      page.rows.map(r => h('li', { 'data-nav-run': r.completion_cid, 'aria-current': r.completion_cid === here.run ? 'page' : null },
        link(`#/run/${r.completion_cid}`, h('span', { class: `dot dot-${statusTone(r)}`, 'aria-hidden': 'true' }, '●'), ' ',
          r.run_name ?? 'unnamed run', ' ', h('span', { class: 'muted' }, whenText(r.finished_at).split(',')[0])))),
      page.total > page.rows.length ? h('li', {}, link(`#/pipeline/${enc(name)}`, 'all runs…')) : null)
  }

  function draw() {
    el.replaceChildren(
      h('input', { type: 'search', disabled: true, placeholder: 'Search samples, runs… (later)', 'aria-label': 'search (not yet available)' }),
      h('h2', { class: 'nav-head' }, 'Pipelines'),
      pipelinesError ? warn('Could not list pipelines', pipelinesError)
        : pipelines.length === 0 ? h('p', { class: 'muted' }, 'No runs yet.')
          : h('ul', { class: 'nav-pipelines' }, pipelines.map(p => h('li', { 'data-nav-pipeline': p.pipeline },
            h('button', { type: 'button', class: 'nav-toggle', 'aria-expanded': String(open.has(p.pipeline)), onclick: () => {
              if (open.has(p.pipeline)) open.delete(p.pipeline)
              else { open.add(p.pipeline); readRuns(p.pipeline).then(draw) }
              draw()
            } }, open.has(p.pipeline) ? '▾ ' : '▸ ', p.pipeline, ' ', h('span', { class: 'muted' }, String(p.runs))),
            open.has(p.pipeline) ? runList(p.pipeline) : null))),
      h('h2', { class: 'nav-head' }, 'Selections'),
      selectionsError ? warn('Could not list Selections', selectionsError)
        : h('ul', { class: 'nav-selections' },
          selections.map(s => h('li', {}, h('a', { href: `#/selection/${s.cid}`, 'data-nav-selection': s.cid },
            s.state?.names?.length ? s.state.names.join(' / ') : 'unnamed'))),
          h('li', {}, link('#/selections', 'all selections…'))))
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
      ])
      draw()
    },
    async reloadSelections() {
      runs.clear()
      await readSelections()
      for (const name of open) await readRuns(name)
      draw()
    },
    async mark({ pipeline = null, run = null, expand = false } = {}) {
      here = { pipeline, run }
      if (expand && pipeline) {
        open.add(pipeline)
        await readRuns(pipeline)
      }
      draw()
    },
  }
}
