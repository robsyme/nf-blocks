// web/src/views/run.js
// #/run/<completion> (explorer layout B spec §5.3): what the run made, A–Z
// with the empty outputs greyed below the rest, and its details, storage and
// lineage check folded away beneath.
import { h, link } from '../html.js'
import { address, icon } from '../icons.js'
import { Previews } from '../previews.js'
import { fold } from '../folds.js'
import { fieldPaths, outputSuffixes } from '../table.js'
import { anomalyLines, healthSummary, runTone, statusText, whenText } from '../words.js'
import { crumbs, enc, pillTip, retentionPanel, stableIndexControl, table } from './common.js'

const SAMPLE = 3
const FIELDS = 3
const SUFFIXES = 2

export async function run(ex, completionCid, ctx) {
  const { row, completion, collections, state } = await ex.run(completionCid)
  ctx.mark?.({ pipeline: row.pipeline, run: completionCid, expand: true })
  const lid = row.nf_run_hash ? `lid://${row.nf_run_hash}` : null
  const latestNote = h('span', { class: 'muted' })
  ex.latestSuccessfulRun?.(row.pipeline).then((best) => { if (best === completionCid) latestNote.textContent = ` · latest good run of ${row.pipeline}` }, () => {})
  const previews = new Previews(ex)
  const tip = pillTip()
  const more = (cls, rest) => h('span', { class: cls, tabindex: '0', 'data-more': '', ...tip.on(rest.map(t => ['tip-path', t])) }, `+${rest.length}`)
  const outputs = [...collections].sort((a, b) => (a.output < b.output ? -1 : a.output > b.output ? 1 : 0))
  const rows = outputs.map(c => ({ c, count: h('td', { class: 'muted' }, '…'), fields: h('td', { class: 'muted', 'data-fields': '' }), files: h('td', { 'data-files': '' }), items: [] }))
  const redraw = () => {
    for (const r of rows) {
      const loaded = r.items.map(i => previews.get(i)).filter(p => p?.pairs)
      if (!loaded.length) continue
      const fields = fieldPaths(loaded)
      r.fields.replaceChildren(fields.slice(0, FIELDS).join(', '),
        ...(fields.length > FIELDS ? [' ', more('muted', fields.slice(FIELDS))] : []))
      const suffixes = outputSuffixes(loaded)
      r.files.replaceChildren(...suffixes.slice(0, SUFFIXES).map(t => h('span', { class: 'ext' }, t)),
        ...(suffixes.length > SUFFIXES ? [more('ext', suffixes.slice(SUFFIXES))] : []),
        ...(r.indexLink ? [r.indexLink] : []))
    }
  }
  previews.onChange(redraw)
  for (const r of rows) {
    r.tr = h('tr', { 'data-collection': r.c.cid, 'data-output': r.c.output },
      h('td', { class: 'output-name' }, icon('output'), link(`#/items/${completionCid}/${enc(r.c.output)}`, h('strong', {}, r.c.output))), r.count, r.fields, r.files)
  }
  const outputsTable = rows.length ? table(['output', 'items', 'fields', 'files'], rows.map(r => r.tr)) : null
  const counted = rows.map(r => ex.collection(r.c.cid, { limit: SAMPLE }).then((coll) => {
    r.count.textContent = String(coll.total)
    r.count.classList.remove('muted')
    if (coll.total === 0) { r.empty = true; r.tr.classList.add('empty-output') }
    r.items = coll.items
    if (coll.index?.leaf?.address) {
      r.indexLink = h('span', {}, ...stableIndexControl(ex, ctx, r.c.cid, 'index file', 'link index-link'))
      r.files.append(r.indexLink)
    }
    for (const i of coll.items) previews.ask(r.c.cid, i)
  }, () => { r.count.textContent = '?' }))
  // Rows draw A–Z at once; the empty ones move down together when every count
  // is in, so the table reorders once rather than row by row.
  Promise.all(counted).then(() => {
    if (!rows.some(r => r.empty)) return
    const [t] = outputsTable.children
    t.replaceChildren(t.children[0], ...rows.filter(r => !r.empty).map(r => r.tr), ...rows.filter(r => r.empty).map(r => r.tr))
  })
  const details = h('div', {},
    lid ? h('p', {}, 'Run reference: ', h('code', {}, lid)) : null,
    h('p', {}, 'RunCompletion: ', address(completionCid)),
    row.manifest_cid ? h('p', {}, 'RunManifest: ', address(row.manifest_cid)) : null)
  ex.runIdentity?.(completionCid).then((id) => {
    if (id.revision) details.prepend(h('p', {}, 'Revision: ', h('code', {}, id.revision)))
    if (id.config) details.append(fold(`config:${completionCid}`, 'Configuration', h('pre', { class: 'scroll' }, id.config)))
  }, () => {})
  const lines = anomalyLines(completion.anomalies)
  return h('section', {},
    crumbs({ text: row.pipeline, href: `#/pipeline/${enc(row.pipeline)}` }, { text: row.run_name ?? 'run' }),
    h('h1', {}, icon('run', { tone: runTone(completion) }), row.run_name ?? 'run'),
    h('p', {}, statusText(completion), ' · finished ', whenText(completion.finished_at), latestNote),
    completion.error ? h('p', { class: 'warn' }, completion.error) : null,
    h('h2', {}, 'Outputs'),
    collections.length === 0 ? h('p', { class: 'muted', 'data-no-outputs': '' }, 'This run published no outputs.')
      : outputsTable,
    tip.el,
    fold(`details:${completionCid}`, 'Run details', details),
    fold(`storage:${completionCid}`, 'Storage', retentionPanel(completionCid, state, ctx, { isRun: true, kind: 'run', href: ctx.write.hrefFor(`#/run/${completionCid}`) })),
    fold(`lineage:${completionCid}`, `Lineage check: ${healthSummary(completion.anomalies)}`,
      lines.length === 0 ? h('p', { class: 'muted' }, 'Every file is accounted for.')
        : h('ul', {}, lines.map(l => h('li', { 'data-anomaly': l.key, class: l.concern ? 'warn' : 'muted' }, l.text)))))
}
