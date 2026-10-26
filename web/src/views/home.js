// web/src/views/home.js
// #/ and the idle page (DESIGN.md §15; mockups/02-pipelines.html). In
// all-stores mode (?member=*, P3 rule D5) the table has one row per pipeline
// across the members, with dots for the stores that hold its runs; the row's
// figures are those of the first member that has it, where its link opens.
import { h, link } from '../html.js'
import { icon } from '../icons.js'
import { ago, avatar, claimTag, factTag, pageHead, panel, status } from '../components.js'
import { seal } from '../seal.js'
import { bytes, ratioText, sizeText } from '../format.js'
import { dayText, fullWhenText, healthLine, timeText, whenText } from '../words.js'
import { claimValueOf, claimVerb, enc, nameProjectLinks, unreadableRuns } from './common.js'
import { projectNames, projectsCache } from './addproject.js'
import { fromEach, hrefIn, listedStates, listsAll, mergeRows, unreadableText } from '../members.js'
import { knownDots, presenceOf } from '../presence.js'

export function idle() {
  return h('p', { class: 'muted' }, 'Snapshot open.')
}

export const ask = (fn) => Promise.resolve().then(fn)
const sub = (text) => h('span', { class: 'sub' }, text)
/** A date that wraps as one piece. */
const when = (iso) => h('span', { class: 'sub', style: 'white-space:nowrap' }, whenText(iso))

/** `[count, ['2 ok', '1 failed']]` from a runCounts entry: the run count, then each outcome that occurs. */
export function runsSummary({ runs, ok, failed }) {
  const other = runs - ok - failed
  return [`${runs}`, [ok ? `${ok} ok` : null, failed ? `${failed} failed` : null, other ? `${other} other` : null].filter(Boolean)]
}

/** A stored size, or the empty state when the runs published nothing. */
export function storedCell(size) {
  if (!size) return sub('…')
  if (size.blocks === 0 && size.bytes === 0 && !size.partial) return sub('No outputs published')
  return h('span', {}, sizeText(size))
}

/**
 * What a pipeline's Stored figure counts (lead ruling: runs hidden by a delete
 * Claim are still stored here). `store` names the member the figure is from
 * when the page lists several (all stores).
 */
export function storedTitle(size, { store = null } = {}) {
  const what = countedText(size)
  return store ? `Stored in ${store}. ${what}` : what
}

function countedText(size) {
  if (size?.source === 'snapshot') {
    const n = size.newer ?? 0
    const counts = 'Counts the runs that hold files here: held runs, and released or deleted runs that are pinned'
    if (!n) return `${counts}.`
    const runs = n === 1 ? '1 newer run may' : `${n} newer runs may`
    return `Up to ${bytes(size.bytes)}; ${runs} share files with older ones. ${counts}, plus the newer ${n === 1 ? 'run' : 'runs'} sized from their blocks.`
  }
  const hidden = 'Counts runs hidden by a delete Claim too: their files are still stored in this store.'
  return size?.more ? `Sums the newest ${size.runs} runs only, so the total is at least this. ${hidden}` : hidden
}

/**
 * `href(hash)` opens a link in the row's member (all-stores mode); `dots`
 * goes after the name.
 */
function pipelineRow(ex, p, counts, { href = (hash) => hash, dots = null, opensIn = null } = {}) {
  const runs = h('td', { class: 'num nowrap' }, sub('…'))
  // The counts, the latest good run and the size keep to one line; the other cells wrap, so the Health column fits inside the table at 1440 (lead's visual finding 9).
  const good = h('td', { class: 'nowrap' }, sub('…'))
  const last = h('td', {}, when(p.latest))
  const stored = h('td', { class: 'r num nowrap', 'data-stored': '' }, sub('…'))
  const health = h('td', { 'data-health': '', class: 'health-cell' })
  const row = h('tr', { 'data-pipeline': p.pipeline },
    h('td', {}, h('div', { class: 'namecell' }, icon('pipeline'), opensInLink(href(`#/pipeline/${enc(p.pipeline)}`), p.pipeline, opensIn), dots)),
    runs, good, last, stored, health)
  counts.then(async (all) => {
    const c = all.get(p.pipeline) ?? { runs: 0, ok: 0, failed: 0 }
    const [count, parts] = runsSummary(c)
    runs.replaceChildren(...(parts.length ? [count, ' ', sub(`· ${parts.join(' · ')}`)] : [count]))
  }).catch(() => { runs.textContent = '?' })
  // The health line has its own failure: a RunCompletion that cannot be read leaves the cell empty, and the Runs cell as it is.
  counts.then(async () => {
    const [newest] = await ex.runsOfPipeline(p.pipeline, { limit: 1 })
    if (!newest) return
    const line = healthLine((await ex.completionOf(newest.completion_cid)).anomalies)
    if (line) health.replaceChildren(sub(line.text))
  }).catch(() => {})
  ask(() => ex.latestSuccessfulRun(p.pipeline)).then(async (best) => {
    if (!best) { good.replaceChildren(sub('None')); return }
    const r = await ex.runRow(best)
    // One line (mockup 02): the header says the run succeeded, so the run's name takes the status word's place.
    good.replaceChildren(h('span', { class: 'row', style: 'gap:6px;flex-wrap:nowrap' },
      status({ status: 'succeeded' }, opensInLink(href(`#/run/${best}`), r?.run_name ?? 'unnamed run', opensIn)), when(r?.finished_at)))
  }).catch(() => { good.textContent = '?' })
  ask(() => ex.pipelineSize(p.pipeline)).then((size) => {
    stored.replaceChildren(storedCell(size))
    stored.setAttribute('title', storedTitle(size, { store: opensIn }))
  }, () => stored.replaceChildren(sub('?')))
  return row
}

/** One activity row's tag: runs and Selections are facts, claims are claims. */
export function activityTag(e) {
  if (e.kind === 'run') return factTag('run', e.name ?? 'unnamed run', e.cid, `#/run/${e.cid}`)
  if (e.kind === 'selection') return factTag('selection', e.name ?? 'unnamed', e.cid, `#/selection/${e.cid}`)
  if (e.kind === 'transfer') return transferTag(e)
  const c = e.claim ?? {}
  return claimTag({ assertedBy: c.assertedBy, verb: claimVerb(c), value: claimValueOf(c), at: c.timestamp })
}

/**
 * A copy into this store, as the run and Selection entries are: the
 * Transfer's seal and "Copied from <from> · <depth>", linking its block; then
 * who copied, and when.
 */
function transferTag(e) {
  const t = e.transfer ?? {}
  const fact = factTag('Copied from', [t.from ?? 'another store', t.depth ? ` · ${t.depth}` : null].filter(Boolean).join(''), e.cid, `#/block/${e.cid}`)
  fact.setAttribute('data-transfer-block', e.cid)
  return h('span', { class: 'row', style: 'gap:6px', 'data-activity-transfer': e.cid }, fact, avatar(t.assertedBy ?? null),
    ago(t.at, Date.now(), { class: 'muted small' }))
}

function activityWhen(iso, now = new Date()) {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return ''
  return d.toDateString() === now.toDateString() ? timeText(d) : dayText(d, now)
}

/**
 * The newest three Selections. `rows()` resolves to `{ shown, unreadable }`,
 * `shown` as `[{ row, ex, alias, heldIn }]`: one member's page as it is, or
 * every member's merged by cid (all-stores mode), where each row opens in its
 * first member and carries dots for the stores holding it.
 */
function selectionsPanel(rows, ctx, all) {
  const body = h('div', { style: 'display:grid;gap:12px' }, h('span', { class: 'muted' }, '…'))
  ask(rows).then(({ shown, unreadable }) => {
    const notes = unreadable.map(a => h('span', { class: 'muted', 'data-member-unreadable': a.member.alias }, unreadableText(a, 'Selections')))
    if (shown.length === 0) { body.replaceChildren(h('span', { class: 'muted' }, 'No Selections yet.'), ...notes); return }
    body.replaceChildren(...shown.map(({ row: r, ex, alias, heldIn }) => {
      const name = r.state.names.join(' / ')
      const meta = h('span', { class: 'sub muted', style: 'font-size:12px' }, '…')
      ask(() => Promise.all([ex.selection(r.cid), ex.selectionSize(r.cid)])).then(([s, size]) => {
        const n = s.members.length
        meta.textContent = `${n} item${n === 1 ? '' : 's'} · ${sizeText(size)}`
        if (all) meta.setAttribute('title', `Stored in ${alias}`)
      }, () => { meta.textContent = '' })
      const named = r.state.current.find(c => c.attribute === 'name' && c.verb === 'set')
      const title = all
        ? h('span', { class: 'row', style: 'gap:6px' }, opensInLink(hrefIn(ctx, alias, `#/selection/${r.cid}`), name || 'Unnamed Selection', alias),
          presenceOf(ctx).dots(r.cid, { hereHeld: null, held: heldIn }))
        : link(`#/selection/${r.cid}`, name || 'Unnamed Selection')
      return h('div', { class: 'row', 'data-home-selection': r.cid, style: 'justify-content:space-between' },
        h('div', { class: 'row' }, seal(r.cid, { size: 28 }), h('div', { style: 'display:grid' }, title, meta)),
        named ? claimTag({ assertedBy: named.asserted_by, verb: 'named', at: named.timestamp }) : null)
    }), ...notes)
  }).catch(() => { body.replaceChildren(h('span', { class: 'muted' }, 'Selections could not be read.')) })
  return panel({ title: 'Selections', actions: [h('a', { href: '#/selections', class: 'btn ghost sm' }, 'All Selections')] }, body)
}

const HOME_SELECTIONS = 3
const newestSeen = (a, b) => ((a.row.firstSeen ?? '') < (b.row.firstSeen ?? '') ? 1 : (a.row.firstSeen ?? '') > (b.row.firstSeen ?? '') ? -1 : 0)

/** One member's newest Selections, as selectionsPanel takes them. */
const selectionsOf = (ex) => async () => ({ unreadable: [],
  shown: (await ex.selectionPage({ limit: HOME_SELECTIONS })).rows.map(row => ({ row, ex, alias: null, heldIn: [] })) })

/** Every member's newest Selections, one row per cid, newest first seen first. */
const selectionsOfAll = (members) => async () => {
  const answers = await fromEach(members, ex => ex.selectionPage({ limit: HOME_SELECTIONS }))
  const shown = mergeRows(answers, page => page.rows, r => r.cid).sort(newestSeen).slice(0, HOME_SELECTIONS)
    .map(m => ({ row: m.row, ex: m.ex, alias: m.member.alias, heldIn: m.heldIn }))
  return { shown, unreadable: answers.filter(a => !('value' in a)) }
}

function activityPanel(ex, storeName) {
  const body = h('div', { style: 'display:grid;gap:10px' }, h('span', { class: 'muted' }, '…'))
  ask(() => ex.recentActivity(8)).then((entries) => {
    body.replaceChildren(...(entries.length === 0 ? [h('span', { class: 'muted' }, 'Nothing in the log yet.')]
      : entries.map(e => h('div', { class: 'row', 'data-activity': e.kind },
        h('span', { class: 'mono muted', style: 'width:52px', title: fullWhenText(e.written_at) }, activityWhen(e.written_at)), activityTag(e)))))
    // A membership's project reads as its short cid until the projects read names it.
    nameProjectLinks(body.querySelectorAll('[data-project-name]'), (cids) => projectNames(ex, projectsCache(ex), cids))
  }).catch(() => { body.replaceChildren(h('span', { class: 'muted' }, 'The store log could not be read.')) })
  return panel({ title: `Activity in ${storeName}` }, body)
}

const newestFirst = (a, b) => ((a.latest ?? '') < (b.latest ?? '') ? 1 : (a.latest ?? '') > (b.latest ?? '') ? -1 : 0)

/** A link that, in all-stores mode, says which member it opens (rule D5: a detail page opens in the first member that holds it). */
function opensInLink(href, text, alias) {
  const a = link(href, text)
  if (alias) a.setAttribute('title', `Opens in ${alias}`)
  return a
}

const TABLE_HEAD = () => h('thead', {}, h('tr', {}, ['Pipeline', 'Runs', 'Latest good run', 'Last run'].map(t => h('th', {}, t)),
  h('th', { class: 'r' }, 'Stored'), h('th', { class: 'health-cell' }, 'Health')))

const runWords = (c) => {
  const [count, parts] = runsSummary(c)
  return `${count} run${c.runs === 1 ? '' : 's'}${parts.length ? ` (${parts.join(', ')})` : ''}`
}

/** The dots' tooltip for a pipeline in all-stores mode: each store's runs of it, never added up, and whose figures the row shows. */
export function pipelineStoresTitle(answers, heldIn, pipeline, shownIn) {
  const parts = answers.map((a) => {
    const alias = a.member.alias
    if (!('value' in a)) return `${alias} couldn't be checked`
    if (!heldIn.includes(alias)) return `${alias} none`
    const c = a.value.counts?.get(pipeline)
    return `${alias} ${c ? runWords(c) : 'runs not counted'}`
  })
  return `Runs per store: ${parts.join('; ')}. This row shows ${shownIn}, where its links open.`
}

/** Pipelines in every member (?member=*): one row per pipeline, its figures from the first member that has it. */
async function homeAll(ex, ctx) {
  const members = ctx.members
  const answers = await fromEach(members, async mex => ({ pipelines: await mex.pipelines(), counts: await mex.runCounts().catch(() => null) }))
  const rows = mergeRows(answers, v => v.pipelines, p => p.pipeline).sort((a, b) => newestFirst(a.row, b.row))
  const unreadable = answers.filter(a => !('value' in a))
  const counted = new Map(answers.filter(a => 'value' in a).map(a => [a.member.alias, a.value.counts]))
  const everyStore = knownDots(answers.map(a => ({ alias: a.member.alias, key: a.member.key, state: 'value' in a ? 'held' : 'unknown' })))
  const n = rows.length
  return h('section', { style: 'min-width:0', 'data-all-stores': '' },
    pageHead({ title: 'Pipelines', subline: [
      [everyStore, ' ', h('b', {}, 'All stores')], `${n} pipeline${n === 1 ? '' : 's'}`,
      h('span', { class: 'muted' }, "Each row's figures are from the store it opens in")] }),
    n === 0 && unreadable.length === 0 ? h('p', { class: 'muted' }, 'No runs in any store yet.')
      : h('section', { class: 'panel flush' }, h('div', { class: 'pb tbox' }, h('table', { class: 't' }, TABLE_HEAD(),
        h('tbody', {}, rows.map((m) => {
          const alias = m.member.alias
          const counts = counted.get(alias)
          const dots = knownDots(listedStates(answers, m.heldIn), { title: pipelineStoresTitle(answers, m.heldIn, m.key, alias) })
          return pipelineRow(m.ex, m.row, counts ? Promise.resolve(counts) : Promise.reject(new Error('runs not counted')),
            { href: (hash) => hrefIn(ctx, alias, hash), dots, opensIn: alias })
        }), unreadable.map(a => h('tr', { 'data-member-unreadable': a.member.alias },
          h('td', { colspan: '6', class: 'muted' }, unreadableText(a, 'pipelines')))))))),
    h('div', { class: 'grid-2' }, selectionsPanel(selectionsOfAll(members), ctx, true), activityPanel(ex, ctx?.store?.name ?? 'this store')),
    unreadableRuns(ex))
}

export async function home(ex, ctx) {
  if (listsAll(ctx)) return homeAll(ex, ctx)
  const pipelines = (await ex.pipelines()).sort(newestFirst)
  const storeName = ctx?.store?.name ?? 'this store'
  const counts = ask(() => ex.runCounts())
  const total = h('span', {}, '…')
  counts.then((all) => { const n = [...all.values()].reduce((m, c) => m + c.runs, 0); total.textContent = `${n} run${n === 1 ? '' : 's'}` }, () => { total.textContent = '?' })
  // The store-wide line (mockup 02), from the snapshot's figures (read once, at load, by the left column). A lean snapshot has none.
  const stats = await Promise.resolve(ex.storeStats?.()).catch(() => null)
  const figures = stats ? h('span', { 'data-store-figures': '' }, h('b', {}, bytes(stats.stored)), ' stored · ', `${bytes(stats.referenced)} referenced`,
    stats.dedup == null ? null : ` · ${ratioText(stats.dedup)} dedup`) : null
  return h('section', { style: 'min-width:0' },
    pageHead({ title: 'Pipelines', subline: [
      [h('span', { class: `dot ${ctx?.store?.key ?? 'a'}` }), ' ', h('b', {}, storeName)],
      total, ...(figures ? [figures] : [])] }),
    pipelines.length === 0 ? h('p', { class: 'muted' }, 'No runs in this member yet.')
      : h('section', { class: 'panel flush' }, h('div', { class: 'pb tbox' }, h('table', { class: 't' }, TABLE_HEAD(),
        h('tbody', {}, pipelines.map(p => pipelineRow(ex, p, counts)))))),
    h('div', { class: 'grid-2' }, selectionsPanel(selectionsOf(ex), ctx, false), activityPanel(ex, storeName)),
    unreadableRuns(ex))
}
