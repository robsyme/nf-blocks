// web/src/views/pipeline.js
// #/pipeline/<name> and the latest-run redirect (DESIGN.md §15).
import { h, link } from '../html.js'
import { icon, ref, sealed, shortCid } from '../icons.js'
import { avatar, empty, note, pageHead, status } from '../components.js'
import { sizeText } from '../format.js'
import { healthLine, runTone, sharedHealth, whenText } from '../words.js'
import { crumbs, enc, errorNode, mineToggle, pager, unreadableRuns } from './common.js'
import { ask, storedCell, storedTitle } from './home.js'

const sub = (text) => h('span', { class: 'sub' }, text)

export async function pipeline(ex, name, offset = 0, ctx, { mine = false } = {}) {
  ctx?.mark?.({ pipeline: name, expand: true })
  // Filter by ctx.viewer, the one seam for "who is looking"; an unavailable Mine (anonymous, no identity) shows everything.
  const by = mine && ctx?.viewer ? ctx.viewer : null
  const page = await ex.runPage(name, by ? { offset, by } : { offset })
  const base = `#/pipeline/${enc(name)}`
  const route = by ? `${base}?mine=1` : base
  // Anomaly counts are only in each RunCompletion, and sizes come from the
  // run's blocks; each cell is filled in when its answer arrives, without
  // holding up the view.
  const rows = page.rows.map(r => ({ r,
    stored: h('td', { class: 'r num nowrap', 'data-stored-for': r.completion_cid }, sub('…')),
    anomalies: h('td', { 'data-anomalies-for': r.completion_cid }) }))
  const shared = h('div', { 'data-shared-health': '' })
  const total = h('span', {})
  const storeName = ctx?.store?.name ?? 'this store'
  const node = h('section', { style: 'min-width:0' },
    crumbs({ text: name }),
    pageHead({ title: name, subline: [
      [h('span', { class: `dot ${ctx?.store?.key ?? 'a'}` }), ' ', h('b', {}, storeName)],
      `${page.total} run${page.total === 1 ? '' : 's'}`, total] }),
    h('p', {}, link(`#/latest/${enc(name)}`, 'Latest successful run')),
    shared,
    pager(route, page, 'runs'),
    page.hidden ? h('p', { 'data-hidden-runs': page.hidden, class: 'muted' }, `${page.hidden} run${page.hidden === 1 ? '' : 's'} on this page hidden by a delete Claim`) : null,
    h('section', { class: 'panel flush' },
      h('div', { class: 'ph' }, h('h2', {}, 'Runs'), h('span', { class: 'spacer' }),
        mineToggle(ctx?.identity, { allHref: base, mineHref: `${base}?mine=1`, on: !!by })),
      by && page.rows.length === 0 ? empty(`None of these runs were signed by ${by}.`, '') : null,
      // An empty page draws no table header under it, as the Selections list does.
      page.rows.length === 0 ? null : h('div', { class: 'pb tbox' }, h('table', { class: 't' },
      h('thead', {}, h('tr', {}, ['Run', 'Status', 'Finished'].map(t => h('th', {}, t)),
        h('th', { 'aria-label': 'By', title: 'Signed by', style: 'width:1%' }, 'By'),
        h('th', { class: 'r' }, 'Stored'), h('th', {}, 'Health'), h('th', {}, 'From'))),
      h('tbody', {}, rows.map(({ r, stored, anomalies }) => h('tr', {
        'data-run': r.completion_cid, 'data-pipeline': r.pipeline, 'data-status': r.status, 'data-source': r.source },
      h('td', {}, h('div', { class: 'namecell' }, sealed('run', r.completion_cid, { size: 20, tone: runTone(r) }), link(`#/run/${r.completion_cid}`, r.run_name ?? shortCid(r.completion_cid)))),
      h('td', {}, status(r)),
      h('td', { class: 'nowrap' }, sub(whenText(r.finished_at))),
      h('td', { 'data-by': r.asserted_by ?? '' }, avatar(r.asserted_by)),
      stored,
      anomalies,
      h('td', {}, sub(r.source === 'tail' ? 'newer than the snapshot' : 'snapshot')))))))),
    unreadableRuns(ex))
  ask(() => ex.pipelineSize(name)).then((size) => {
    const empty = size.blocks === 0 && size.bytes === 0 && !size.partial
    total.replaceChildren(h('b', {}, empty ? 'No outputs published' : sizeText(size)), empty ? '' : ' stored')
    total.setAttribute('title', storedTitle(size))
  }, () => {})
  for (const { r, stored } of rows)
    ask(() => ex.runSize(r.completion_cid)).then(size => stored.replaceChildren(storedCell(size)), () => stored.replaceChildren(sub('?')))
  const completions = rows.map(({ r, anomalies }) => ask(() => ex.completionOf(r.completion_cid)).then(
    c => c, (e) => { anomalies.replaceChildren(errorNode(e)); return null }))
  Promise.all(completions).then((all) => {
    const line = all.length && all.every(Boolean) ? sharedHealth(all) : null
    if (line) { shared.replaceChildren(note(line.text, line.act ? 'warn' : 'info')); return }
    rows.forEach(({ anomalies }, i) => {
      const text = all[i] ? healthLine(all[i].anomalies)?.text : null
      if (text) anomalies.replaceChildren(sub(text))
    })
  }).catch(() => {})
  return node
}

/** #/latest/<pipeline>: query 2's answer, named from the run's blocks (runIdentity, no further snapshot read), its short CID beside the name. */
export async function latest(ex, pipelineName) {
  const best = await ex.latestSuccessfulRun(pipelineName)
  const id = best ? await Promise.resolve().then(() => ex.runIdentity(best)).catch(() => null) : null
  return h('section', {},
    crumbs({ text: pipelineName, href: `#/pipeline/${enc(pipelineName)}` }, { text: 'latest good run' }),
    h('h1', {}, `Latest successful run of ${pipelineName}`),
    h('p', { 'data-latest': best ?? '' }, best
      ? [ref('run', id?.run_name ?? shortCid(best), `#/run/${best}`, { tone: 'good', cid: best }),
        id?.run_name ? [' ', h('code', { class: 'cid', title: best }, shortCid(best))] : null]
      : 'None: no run of this pipeline succeeded without being possibly incomplete.'))
}
