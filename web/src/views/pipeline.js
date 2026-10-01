// web/src/views/pipeline.js
// #/pipeline/<name> and the latest-run redirect (DESIGN.md §15).
import { h, link, cid } from '../html.js'
import { enc, errorNode, pager, table, unreadableRuns } from './common.js'

export async function pipeline(ex, name, offset = 0) {
  const page = await ex.runPage(name, { offset })
  // Anomaly counts are only in each RunCompletion; each row's cell is filled
  // in when its block arrives, without holding up the view.
  const rows = page.rows.map(r => ({ r, anomalies: h('td', { 'data-anomalies-for': r.completion_cid, class: 'muted' }, '...') }))
  const node = h('section', {},
    h('h1', {}, name),
    h('p', {}, link(`#/latest/${enc(name)}`, 'Latest successful run')),
    pager(`#/pipeline/${enc(name)}`, page, 'runs'),
    page.hidden ? h('p', { 'data-hidden-runs': page.hidden, class: 'muted' }, `${page.hidden} run${page.hidden === 1 ? '' : 's'} on this page hidden by a delete Claim`) : null,
    table(['run', 'status', 'finished', 'anomalies', 'from'], rows.map(({ r, anomalies }) => h('tr', {
      'data-run': r.completion_cid, 'data-pipeline': r.pipeline, 'data-status': r.status, 'data-source': r.source },
    h('td', {}, link(`#/run/${r.completion_cid}`, r.run_name ?? r.completion_cid)),
    h('td', {}, r.possibly_incomplete ? `${r.status}, possibly incomplete` : r.status),
    h('td', {}, r.finished_at ?? ''),
    anomalies,
    h('td', { class: 'muted' }, r.source === 'tail' ? 'newer than the snapshot' : 'snapshot')))),
    unreadableRuns(ex))
  for (const { r, anomalies } of rows) {
    ex.completionOf(r.completion_cid).then(
      c => { anomalies.textContent = Object.entries(c.anomalies).filter(([, n]) => n).map(([k, n]) => `${k.replace('_', ' ')} ${n}`).join(', ') || 'none' },
      e => { anomalies.replaceChildren(errorNode(e)) })
  }
  return node
}

export async function latest(ex, pipelineName) {
  const best = await ex.latestSuccessfulRun(pipelineName)
  return h('section', {},
    h('h1', {}, `Latest successful run of ${pipelineName}`),
    h('p', { 'data-latest': best ?? '' }, best ? link(`#/run/${best}`, cid(best)) : 'None: no run of this pipeline succeeded without being possibly incomplete.'))
}
