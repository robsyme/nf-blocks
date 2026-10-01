// web/src/views/home.js
// #/ and the idle page (DESIGN.md §15).
import { h, link } from '../html.js'
import { whenText } from '../words.js'
import { enc, table, unreadableRuns } from './common.js'

export function idle() {
  return h('p', { class: 'muted' }, 'Snapshot open.')
}

export async function home(ex, ctx) {
  const pipelines = await ex.pipelines()
  const cells = pipelines.map(p => ({ p, good: h('td', { class: 'muted' }, '…') }))
  for (const { p, good } of cells) {
    ex.latestSuccessfulRun(p.pipeline).then(async (best) => {
      if (!best) { good.textContent = 'none'; return }
      const id = await ex.runIdentity(best)
      good.classList.remove('muted')
      good.replaceChildren(link(`#/run/${best}`, id.run_name ?? best))
    }, () => { good.textContent = '?' })
  }
  return h('section', {},
    h('h1', {}, 'Pipelines'),
    pipelines.length === 0 ? h('p', { class: 'muted' }, 'No runs in this member yet.')
      : table(['pipeline', 'runs', 'latest run', 'latest good run'], cells.map(({ p, good }) => h('tr', {},
        h('td', {}, link(`#/pipeline/${enc(p.pipeline)}`, p.pipeline)), h('td', {}, String(p.runs)), h('td', {}, whenText(p.latest)), good))),
    unreadableRuns(ex))
}
