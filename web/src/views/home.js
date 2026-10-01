// web/src/views/home.js
// #/ and the idle page (DESIGN.md §15).
import { h, link } from '../html.js'
import { enc, table, unreadableRuns } from './common.js'

export function idle() {
  return h('p', { class: 'muted' }, 'Snapshot open.')
}

export async function home(ex) {
  const pipelines = await ex.pipelines()
  return h('section', {},
    h('h1', {}, 'Pipelines'),
    pipelines.length === 0 ? h('p', { class: 'muted' }, 'No runs in this member yet.')
      : table(['pipeline', 'runs', 'latest finish'], pipelines.map(p => h('tr', {},
        h('td', {}, link(`#/pipeline/${enc(p.pipeline)}`, p.pipeline)), h('td', {}, String(p.runs)), h('td', {}, p.latest ?? '')))),
    unreadableRuns(ex))
}
