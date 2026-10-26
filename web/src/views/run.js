// web/src/views/run.js
// #/run/<completion>[?tab=samples|outputs|provenance|storage[&release=1]] (mockups/03-run-outputs.html,
// 07-run-samples.html, 10-run-storage.html): the run's header, then one of four tabs. Samples joins
// the outputs' items on the sample key (views/samples.js) and is the default
// when the run has samples; otherwise Outputs is. Outputs lists what the run
// published, largest first, with the outputs that hold no items folded into
// one row at the foot; Provenance holds the run's records, its configuration
// and the Lineage check; Storage says what the run holds in this store, what
// releasing it would free, and holds the retention panel, with Release's
// inline confirmation (open from the start with release=1).
import { h, link } from '../html.js'
import { icon } from '../icons.js'
import { Previews } from '../previews.js'
import { fold } from '../folds.js'
import { addrChip, button, claimTag, durability, kindChip, kv, minibar, note, pageHead, panel, status, storageBar, tabs } from '../components.js'
import { presenceOf } from '../presence.js'
import { formatNumber, sizeText } from '../format.js'
import { matchPromoted, pipelineConfig, promotedFor, shortLabel } from '../promoted.js'
import { fieldPaths, itemTable } from '../table.js'
import { parseSamplesWhere } from '../samples.js'
import { firstItemKeyed, joinRun, samplesTab } from './samples.js'
import { anomalyLines, dayText, healthLine, whenText } from '../words.js'
import { enc, holds, namesText, omittedNote, pillTip, retentionPanel, stableIndexControl, staleFiguresText, transfersNode } from './common.js'
import { samePrincipal, whoText } from '../identity.js'
import { copyToButton } from './copy.js'
import { aimCopyTo, projectControl } from './addproject.js'

const TABS = [['samples', 'Samples'], ['outputs', 'Outputs'], ['provenance', 'Provenance'], ['storage', 'Storage']]
const DAY = 86_400_000
const COMPARE = 10
const SHARED_CAP = 5
const SAMPLE = 3
const FIELDS = 3
const KINDS = 4
const COLUMNS = ['Output', 'Items', 'Key fields', 'Files', 'Size']

const plural = (n, one, many = `${one}s`) => `${n} ${n === 1 ? one : many}`
// A sharer with no name of its own, by its kind.
const UNNAMED = { run: 'an unnamed run', selection: 'an unnamed Selection', handoff: 'an unnamed hand-off' }
// The Lineage check's counts, in words.js's order (concerns first).
const COUNT_LABELS = { unjoined: 'Outside any output', unresolvable: 'Links not found', unaddressed: 'No stored content',
  declined: 'Empty optional slots', never_published: 'Referenced, not stored' }
const byOutput = (a, b) => (a.c.output < b.c.output ? -1 : a.c.output > b.c.output ? 1 : 0)

/** How long a run took, in plain units: `42 s`, `5 min 15 s`, `1 h 5 min`, `1 d 1 h`. */
export function durationText(ms) {
  const s = Math.max(0, Math.round(ms / 1000))
  const pair = (big, bigUnit, small, smallUnit) => (small ? `${big} ${bigUnit} ${small} ${smallUnit}` : `${big} ${bigUnit}`)
  if (s < 60) return `${s} s`
  const m = Math.floor(s / 60)
  if (m < 60) return pair(m, 'min', s % 60, 's')
  const hours = Math.floor(m / 60)
  if (hours < 24) return pair(hours, 'h', m % 60, 'min')
  return pair(Math.floor(hours / 24), 'd', hours % 24, 'h')
}

/** replaceChildren that skips null, undefined and false, as h() does. */
const put = (el, ...parts) => el.replaceChildren(...parts.flat(Infinity).filter(p => p !== null && p !== undefined && p !== false))

/**
 * Will the run's files survive the next sweep here, from its retention state.
 * A released run reads `Releasing · <date>` once `earliest` (see
 * earliestSweep) answers with a date, and `Released` once that date has
 * passed: a sweep may already have run.
 */
function runDurability(state, earliest) {
  if (state?.pinned) return durability('pinned', 'Pinned')
  if (state?.released) {
    // Who released it and when (a Claim, P1b), and from when a sweep may reclaim it (R6, R7).
    const badge = durability('releasing', 'Releasing')
    earliest.then((sweep) => { if (sweep) badge.textContent = sweepPassed(sweep) ? 'Released' : `Releasing · ${dayText(sweep.at, sweep.now)}` }, () => {})
    return [badge, ' ', claimTag({ assertedBy: state.releaseClaim?.asserted_by, verb: 'released', at: state.releaseClaim?.timestamp })]
  }
  return durability('held', 'Held by run')
}

/**
 * When a sweep may first reclaim what a release lets go (R7): `{ days, at,
 * passed, now }`, the grace in whole days, the release's time plus the grace
 * (now, before a release; the retain Claim's timestamp after one), whether
 * that time is past, and the Explorer's clock (`ex.now`) both were read by,
 * which dayText writes the date by. Null when the
 * snapshot gives no grace (meta sweep_grace_ms, which a lean snapshot keeps
 * too). We never promise a sweep date: sweeps are run by hand, so the copy
 * says "earliest".
 */
async function earliestSweep(ex, state) {
  const grace = await ex.sweepGraceMs().catch(() => null)
  if (grace === null) return null
  const days = Math.round(grace / DAY)
  const now = ex.now()
  if (!state?.released) return { days, at: now + grace, passed: false, now }
  const claim = [...(state.current ?? []), ...(state.claims ?? [])].find(c => c.attribute === 'retain' && c.verb === 'set')
  const at = Date.parse(claim?.timestamp ?? '')
  return Number.isFinite(at) ? { days, at: at + grace, passed: at + grace <= now, now } : null
}

/** Whether the earliest sweep date (earliestSweep) has passed, so a sweep may already have reclaimed what was released. */
const sweepPassed = (sweep) => !!sweep?.passed

/** Whether this run's figures come from the snapshot's storage tables (a snapshot not written lean, and not a run newer than it). */
const indexed = (ex, row) => !!ex.storage && row?.source === 'snapshot'

/** The RunManifest's own fields (Nextflow version, session), read from its block; null when it cannot be read. */
async function manifestOf(ex, completionCid, row) {
  try {
    const cid = row.manifest_cid ?? (await ex.runIdentity?.(completionCid))?.manifest
    if (!cid || !ex.blocks?.ofKind) return null
    return (await ex.blocks.ofKind(cid, 'RunManifest')).value ?? null
  } catch {
    return null
  }
}

/**
 * `tab` is the one asked for; with none (or one not known), the page joins
 * the run's items (joinRun) and opens on Samples when that finds samples,
 * else on Outputs, before it resolves. Samples draws from that same join.
 * `where` and `show` are the Samples tab's URL filter and `show=excluded`;
 * `release` opens the Storage tab's Release confirmation from the start.
 */
export async function run(ex, completionCid, ctx, { tab = null, where = null, show = null, release = false } = {}) {
  const asked = TABS.some(([key]) => key === tab) ? tab : null
  const samplesWhere = parseSamplesWhere(where)
  const { row, completion, collections, state } = await ex.run(completionCid)
  ctx.mark?.({ pipeline: row.pipeline, run: completionCid, expand: true })
  const base = `#/run/${completionCid}`
  const hrefOf = (key) => `${base}?tab=${key}`
  const lid = row.nf_run_hash ? `lid://${row.nf_run_hash}` : null
  const manifest = manifestOf(ex, completionCid, row)
  const earliest = earliestSweep(ex, state)
  const nav = tabs(TABS.map(([key, label]) => ({ key, label, href: hrefOf(key), on: key === asked })), { label: 'Run views' })
  const tabOf = (key) => nav.querySelector(`[data-tab=${key}]`)
  const tip = pillTip()
  const countOn = (key) => (n) => tabOf(key).append(h('small', {}, String(n)))
  const counted = countOn('outputs')
  // This render's page (Explorer.pageChanged marks it gone): work started after the reader leaves is skipped.
  const page = ex.page
  const alive = () => !page?.gone
  // With no tab named, the page joins the run's items as the Samples tab does (joinRun), its count of items read
  // on the page's progress line (ctx.progress), and opens on Samples when that finds samples; over
  // SAMPLES_ITEM_CAP, or a read that fails, is no. On a named tab only the Samples tab's visibility is in
  // question, so each output's first item decides it (firstItemKeyed), and the tab, once opened, runs the join
  // itself.
  const config = pipelineConfig(row.pipeline)
  const joined = asked || collections.length === 0 ? Promise.resolve(null)
    : joinRun(ex, collections, config, { alive, onProgress: ctx.progress }).catch(() => null)
  const found = (j) => (j?.table?.samples.length ?? 0) > 0
  if (asked !== 'samples') {
    tabOf('samples').setAttribute('hidden', '')
    const shows = asked && collections.length ? firstItemKeyed(ex, collections, { joinKey: config.joinKey, alive }).catch(() => false) : joined.then(found)
    shows.then((yes) => { if (yes) tabOf('samples').removeAttribute('hidden') })
  }
  const holder = h('div', { class: 'run-tab' })
  // Add to project (P4): its memberships from the run's own Claims (state), so the subline's tags cost no query.
  // A write draws the tab that is open again.
  const projects = projectControl(ex, ctx, { subject: completionCid, kind: 'run', claims: state?.claims ?? null,
    href: () => (holder.dataset.runTabBody ? hrefOf(holder.dataset.runTabBody) : base) })
  const open = async (on, read = null) => {
    for (const [key] of TABS) {
      const t = tabOf(key)
      if (key === on) { t.classList.add('on'); t.setAttribute('aria-current', 'page') } else { t.classList.remove('on'); t.removeAttribute('aria-current') }
    }
    holder.dataset.runTabBody = on
    const body = on === 'provenance' ? await provenanceTab(ex, completionCid, ctx, { row, completion, lid, manifest })
      : on === 'storage' ? await storageTab(ex, completionCid, ctx, { state, row, earliest, release, href: ctx.write.hrefFor(hrefOf('storage')) })
      : on === 'samples' ? samplesTab(ex, completionCid, ctx, { pipeline: row.pipeline, collections, where: samplesWhere,
        showExcluded: show === 'excluded', counted: countOn('samples'), alive, joined: read })
      : outputsTab(ex, completionCid, ctx, { pipeline: row.pipeline, collections, tip, counted })
    put(holder, body)
    if (on !== 'outputs') countOutputs(ex, collections).then(counted, () => {})
  }
  // A run with no outputs has no samples: its tab is known without a read. Otherwise the join decides, and the
  // page waits for it, so the tab body is drawn before app.js marks the page ready.
  const known = asked ?? (collections.length === 0 ? 'outputs' : null)
  if (known) await open(known)
  else await joined.then(j => (alive() ? open(found(j) ? 'samples' : 'outputs', j) : null)).catch((e) => put(holder, note(`Could not draw this tab: ${e?.message ?? e}`, 'bad')))
  return h('section', { class: 'run-page' },
    header(ex, completionCid, ctx, { row, completion, state, lid, manifest, earliest, storageHref: hrefOf('storage'), projects }),
    projects.node,
    completion.error ? note(completion.error, 'bad') : null,
    nav,
    holder,
    tip.el)
}

/** How many of the run's outputs hold items: each collection's count, no items read. */
async function countOutputs(ex, collections) {
  const totals = await Promise.all(collections.map(c => ex.collection(c.cid, { limit: 0 }).then(coll => coll.total, () => 0)))
  return totals.filter(n => n > 0).length
}

/**
 * Seal, name, status, durability and presence, the Pin and Release buttons
 * where this page can write (they open the Storage tab, Release with its
 * confirmation open), Add to project… and Copy to…, then the subline, which
 * ends with the run's project tags. The subline's parts that need a read wait hidden in their place,
 * so it never reorders.
 */
function header(ex, completionCid, ctx, { row, completion, state, lid, manifest, earliest, storageHref, projects }) {
  const slot = (name, ...content) => h('span', { 'data-slot': name, hidden: content.length === 0 }, ...content)
  const fill = (el, ...content) => { put(el, content); el.removeAttribute('hidden') }
  const latest = slot('latest')
  ex.latestSuccessfulRun?.(row.pipeline).then((best) => {
    if (best === completionCid) fill(latest, 'Latest good run of ', h('b', {}, row.pipeline))
  }, () => {})
  const started = Date.parse(completion.started_at ?? '')
  const finished = Date.parse(completion.finished_at ?? '')
  const took = Number.isFinite(started) && Number.isFinite(finished) && finished >= started ? ` · ${durationText(finished - started)}` : ''
  const when = completion.finished_at ? slot('finished', 'Finished ', h('b', {}, whenText(completion.finished_at, ex.now())), took) : slot('finished')
  const nextflow = slot('nextflow')
  manifest.then((m) => { if (m?.nextflow_version) fill(nextflow, 'Nextflow ', h('b', {}, m.nextflow_version)) })
  const size = slot('size')
  // From the index the figure is the run root's reachable bytes, which leave out what a sweep took (F1): for a released run, what is
  // still stored here. A walk (a lean snapshot or a tail run) reads the records, so a released run's walked figure is what it references (R6).
  const word = !state?.released ? (indexed(ex, row) ? 'reachable' : 'stored') : indexed(ex, row) ? 'still stored here' : 'referenced'
  ex.runSize?.(completionCid).then((s) => fill(size, h('b', {}, sizeText(s)), ` ${word} · ${plural(s.blocks, 'file block')}`), () => {})
  // Copy to… points at the run's project's default store once its projects are read (P4 plan 1.7).
  const copyTo = copyToButton(ctx, 'run', completionCid)
  aimCopyTo(copyTo, projects, (choice) => copyToButton(ctx, 'run', completionCid, choice))
  const head = pageHead({
    crumbs: [{ text: 'Pipelines', href: '#/' }, { text: row.pipeline, href: `#/pipeline/${enc(row.pipeline)}` }, { text: row.run_name ?? 'run' }],
    seal: completionCid,
    title: row.run_name ?? 'run',
    after: [status(completion), runDurability(state, earliest),
      presenceOf(ctx).dots(completionCid)],
    // A store this page cannot write to offers no Pin or Release (the Storage tab says why). Copy to… shows either way, disabled with its reason.
    actions: [...(ctx.write?.available && ctx.write?.here ? [h('a', { class: 'btn', href: storageHref }, 'Pin…'),
      state?.released ? h('a', { class: 'btn', href: storageHref }, 'Restore…') : h('a', { class: 'btn', href: `${storageHref}&release=1` }, 'Release…')] : []),
    projects.button, copyTo],
  })
  head.append(h('div', { class: 'subline' }, latest, when, nextflow, size,
    lid ? slot('lid', addrChip(lid, { seal: false })) : null, projects.tags))
  return head
}

/**
 * The outputs table. Rows draw A–Z at once; each fills as its count, size
 * and sampled items arrive. When every count is in, the outputs with no
 * items leave the body for the one row at its foot; when every size is in,
 * the rows take the chosen order (Size, largest first, unless the reader
 * picked another), so the table reorders once rather than row by row.
 */
function outputsTab(ex, completionCid, ctx, { pipeline, collections, tip, counted }) {
  if (collections.length === 0) return h('p', { class: 'muted', 'data-no-outputs': '' }, 'This run published no outputs.')
  const previews = new Previews(ex)
  const more = (text, rest) => h('span', { class: 'more', tabindex: '0', 'data-more': '', ...tip.on(rest.map(t => ['tip-path', t])) }, text)
  const hrefOf = (output) => `#/items/${completionCid}/${enc(output)}`
  const rows = collections.map(c => ({ c, count: null, size: null, items: [], indexLink: null })).sort(byOutput)
  let sort = { key: 'size', dir: -1 }
  const order = {
    size: (a, b) => (a.size?.bytes ?? -1) - (b.size?.bytes ?? -1),
    items: (a, b) => ((a.count ?? -1) - (b.count ?? -1)) || ((a.size?.bytes ?? -1) - (b.size?.bytes ?? -1)),
  }
  const heads = COLUMNS.map((label) => {
    const key = label === 'Items' ? 'items' : label === 'Size' ? 'size' : null
    const th = h('th', { class: key ? 'r' : null }, key
      ? h('button', { type: 'button', class: 'sort', 'data-sort': key, onclick: () => {
        sort = { key, dir: sort.key === key ? -sort.dir : -1 }
        place()
      } }, label)
      : label)
    return { key, th }
  })

  for (const r of rows) {
    r.countCell = h('td', { class: 'r num muted' }, '…')
    r.fields = h('td', { 'data-fields': '' })
    r.kinds = h('div', { class: 'kinds' })
    r.files = h('td', { 'data-files': '' }, r.kinds)
    r.sizeNum = h('span', { class: 'num muted' }, '…')
    r.barCell = h('div', { class: 'bar-cell' }, r.sizeNum)
    r.tr = h('tr', { 'data-collection': r.c.cid, 'data-output': r.c.output, class: 'output-row',
      onclick: (e) => {
        if (e.target?.closest?.('a, button, [data-more]')) return
        if (globalThis.location) globalThis.location.hash = hrefOf(r.c.output)
      } },
    h('td', { class: 'nowrap' }, h('div', { class: 'namecell' }, icon('output'), link(hrefOf(r.c.output), r.c.output))),
    r.countCell, r.fields, r.files,
    h('td', { class: 'r nowrap' }, r.barCell))
  }

  const toggle = h('button', { type: 'button', class: 'group-toggle', 'aria-expanded': 'false' })
  const names = h('span', { class: 'muted', 'data-empty-names': '', hidden: true })
  toggle.addEventListener('click', () => {
    const open = toggle.getAttribute('aria-expanded') !== 'true'
    toggle.setAttribute('aria-expanded', String(open))
    if (open) names.removeAttribute('hidden')
    else names.setAttribute('hidden', '')
  })
  const group = h('tr', { class: 'group', 'data-empty-outputs': '' }, h('td', { colspan: '5' }, toggle, names))
  const tbody = h('tbody', {}, rows.map(r => r.tr))

  function place() {
    for (const { key, th } of heads) {
      if (key === sort.key) th.setAttribute('aria-sort', sort.dir < 0 ? 'descending' : 'ascending')
      else th.removeAttribute('aria-sort')
    }
    const empty = rows.filter(r => r.count === 0)
    const shown = rows.filter(r => r.count !== 0).sort((a, b) => sort.dir * order[sort.key](a, b) || byOutput(a, b))
    toggle.textContent = plural(empty.length, 'empty output')
    put(names, ' · ', empty.map((r, i) => [i ? ', ' : null,
      h('a', { href: hrefOf(r.c.output), 'data-collection': r.c.cid, 'data-output': r.c.output }, r.c.output)]))
    tbody.replaceChildren(...shown.map(r => r.tr), ...(empty.length ? [group] : []))
  }

  const drawFields = () => {
    for (const r of rows) {
      const loaded = r.items.map(i => previews.get(i)).filter(p => p?.pairs)
      if (!loaded.length) continue
      const paths = fieldPaths(loaded)
      let shown = matchPromoted(promotedFor(pipeline, { output: r.c.output }), paths).slice(0, FIELDS).map(m => ({ path: m.path, label: m.field.label }))
      // Without a promoted field, the fields that vary over the loaded items; one item varies in nothing.
      if (!shown.length && loaded.length >= 2) {
        shown = itemTable(loaded, { maxColumns: Infinity }).columns.slice(0, FIELDS).map(p => ({ path: p, label: shortLabel(p) }))
      }
      const taken = new Set(shown.map(s => s.path))
      // One item with no promoted field has no key fields to show: a dash, not a count of its fields.
      const rest = !shown.length && loaded.length < 2 ? [] : paths.filter(p => !taken.has(p))
      put(r.fields,
        shown.length ? h('span', { class: 'sub' }, shown.map(s => s.label).join(' · ')) : null,
        shown.length && rest.length ? ' ' : null,
        rest.length ? more(`+${plural(rest.length, 'field')}`, rest) : null,
        !shown.length && !rest.length ? h('span', { class: 'muted' }, '—') : null)
    }
  }
  previews.onChange(drawFields)

  const drawBars = () => {
    const max = Math.max(0, ...rows.map(r => r.size?.bytes ?? 0))
    // The bar is the cell's own flex item: .minibar is a span, sized only as one.
    for (const r of rows) if (r.size) put(r.barCell, r.sizeNum, minibar(max ? r.size.bytes / max : 0))
  }
  const drawKinds = (r) => {
    const kinds = r.size?.kinds ?? []
    put(r.kinds, ...kinds.slice(0, KINDS).map(([ext, n]) => kindChip(ext, n)),
      kinds.length > KINDS ? more(`+${kinds.length - KINDS}`, kinds.slice(KINDS).map(([ext, n]) => `${ext}×${n}`)) : null,
      r.size ? h('span', { class: 'more' }, plural(r.size.blocks, 'file')) : null,
      r.indexLink)
  }

  const counts = rows.map(r => ex.collection(r.c.cid, { limit: SAMPLE }).then((coll) => {
    r.count = coll.total
    r.countCell.textContent = String(coll.total)
    r.countCell.classList.remove('muted')
    r.items = coll.items
    if (coll.index?.leaf?.address) {
      r.indexLink = h('span', { class: 'index' }, ...stableIndexControl(ex, ctx, r.c.cid, 'index file', 'link index-link'))
      drawKinds(r)
    }
    for (const i of coll.items) previews.ask(r.c.cid, i)
  }, () => { r.countCell.textContent = '?' }))
  Promise.all(counts).then(() => counted(rows.filter(r => r.count).length))
  const sizes = rows.map(r => (ex.collectionSize ? ex.collectionSize(r.c.cid) : Promise.reject(new Error('no sizes'))).then((size) => {
    r.size = size
    r.sizeNum.replaceChildren(sizeText(size))
    r.sizeNum.classList.remove('muted')
    drawKinds(r)
    drawBars()
    // An index-backed size comes without its chips; they follow from the walk.
    if (!size.kinds && ex.collectionKinds) ex.collectionKinds(r.c.cid).then((kinds) => { r.size = { ...r.size, kinds }; drawKinds(r) }, () => {})
  }, () => { r.sizeNum.textContent = '?' }))
  // The first draw is A–Z with every row; the one reorder waits for every count and size.
  Promise.all([...counts, ...sizes]).then(place)
  place()

  return panel({ flush: true },
    h('div', { class: 'tbox' }, h('table', { class: 't outputs' }, h('thead', {}, h('tr', {}, heads.map(x => x.th))), tbody)))
}

/** The run's records: its lid reference, block addresses, revision and Nextflow, the configuration folded, then the Lineage check. */
async function provenanceTab(ex, completionCid, ctx, { row, completion, lid, manifest }) {
  const [id, m] = await Promise.all([ex.runIdentity ? ex.runIdentity(completionCid).catch(() => null) : null, manifest])
  const manifestCid = row.manifest_cid ?? id?.manifest ?? null
  const chip = (cid) => [addrChip(cid), ' ', link(`#/block/${cid}`, 'open block')]
  const transfers = await transfersNode(ex, completionCid, ctx)
  const line = healthLine(completion.anomalies)
  const counts = anomalyLines(completion.anomalies)
  return [
    transfers ? panel({ title: 'How it got here' }, transfers) : null,
    panel({ title: 'Run details' },
      kv([
        lid ? ['Run reference', addrChip(lid, { seal: false })] : null,
        ['RunCompletion', chip(completionCid)],
        manifestCid ? ['RunManifest', chip(manifestCid)] : null,
        id?.revision ? ['Revision', h('code', {}, id.revision)] : null,
        m?.nextflow_version ? ['Nextflow', m.nextflow_version] : null,
        m?.session_id ? ['Session', h('code', {}, m.session_id)] : null,
      ].filter(Boolean)),
      id?.config ? fold(`config:${completionCid}`, 'Configuration', h('pre', { class: 'scroll' }, id.config)) : null),
    h('div', { 'data-lineage': '' }, panel({ title: 'Lineage check' },
      line ? h('div', { 'data-health': line.act ? 'act' : 'ok' }, note(line.text, line.act ? 'warn' : 'info'))
        : h('p', { class: 'muted' }, 'Every file is accounted for.'),
      counts.length ? h('dl', { class: 'kv lineage-counts' }, counts.map(l => [
        h('dt', { 'data-anomaly': l.key, class: l.concern ? 'warn' : null }, COUNT_LABELS[l.key] ?? l.key),
        h('dd', { class: 'num' }, formatNumber(l.count))])) : null)),
  ]
}

/**
 * The Storage tab. For a run the snapshot holds, everything comes from
 * its run root (Explorer.rootStorage) with no block walk: "This run in
 * <store>" (reachable, only this run, shared and with whom, pinned and by
 * whom), then what each of the pipeline's held runs would free (only
 * where this page can release), then the
 * retention panel. A released run says what it references and what of that
 * is releasing, and from when (R6). On a lean snapshot, or for a run newer than the
 * snapshot, P0's figures: the walked size, and a confirmation with no freed
 * figure (Review focus 4). The same confirmation, a plain Unpin and no
 * "already pinned" line when a Claim since the snapshot has made its
 * figures stale; the kv then says they are as of the snapshot. Release
 * opens the confirmation above the panels.
 */
async function storageTab(ex, completionCid, ctx, { state, row, earliest, release, href }) {
  const store = ctx.store?.name ?? 'this store'
  const released = !!state?.released
  const can = ctx.write.available && ctx.write.here
  const storage = indexed(ex, row) ? await Promise.resolve(ex.rootStorage?.(completionCid)).catch(() => null) : null
  const root = storage?.roots.find(r => r.kind === 'run') ?? null
  if (!root) {
    const size = ex.runSize ? await ex.runSize(completionCid).catch(() => null) : null
    const where = ctx.store?.name ? ` in ${ctx.store.name}` : ''
    const held = h('p', { class: 'muted', 'data-run-size': '' },
      size ? `This run ${released ? 'references' : 'holds'} ${sizeText(size)} (${plural(size.blocks, 'file block')})${where}. ` : null,
      released ? 'Its content is released, so a sweep may already have removed files that no other run or pin holds.'
        : 'Files that other runs or pins also hold stay after a release.')
    const confirm = can && !released ? releaseConfirm(ctx, completionCid, state, { name: row.run_name, store, open: release, sweep: await earliest }) : null
    return [confirm?.el,
      panel({ title: 'Storage' }, omittedNote(ex), held,
        retentionPanel(completionCid, state, ctx, { isRun: true, kind: 'run', href, size: released ? null : size, footer: false,
          ...holds(ex, completionCid), onRelease: confirm?.open }))]
  }
  // A pin, release or deletion Claim, or a run or Selection, since the snapshot leaves its freed and kept figures out of date
  // (Explorer.storageStale); who pins what moves only with a Claim (storageChanges).
  const changes = ex.storageChanges()
  const stale = changes.claims || changes.logged
  const pinsMoved = changes.claims
  const [pinners, runs, sweep] = await Promise.all([
    Promise.resolve(ex.pinnedBy?.(completionCid)).then(r => r ?? [], () => []),
    Promise.resolve(ex.storageRuns?.()).then(r => r ?? [], () => []),
    earliest])
  const size = { bytes: root.reachable, blocks: root.blocks, partial: root.partial }
  const amount = (n) => sizeText({ bytes: n, partial: root.partial })
  // Shared is everything another content root also reaches; pins are content roots, so it holds the pinned bytes too.
  const sharedBytes = Math.max(0, root.reachable - root.exclusive)
  // Runs share first, as the mockup names them; when no run does, the Selections and hand-offs that do. Each links to
  // its page (a hand-off to the Selection it handed over, as rootStorage gives it). A hand-off has no page of its own
  // (#/handoff/ is the template route), so one with no Selection to link to is named unlinked.
  const sharerOf = (s) => ({ name: s.name ?? UNNAMED[s.kind], href: s.href ?? (s.kind === 'handoff' ? null : `#/${s.kind}/${s.cid}`), kind: s.kind })
  const runSharers = storage.shared.filter(s => s.kind === 'run').map(sharerOf)
  const sharerLinks = runSharers.length ? runSharers : storage.shared.filter(s => s.kind === 'selection' || s.kind === 'handoff').map(sharerOf)
  const sharers = sharerLinks.map(s => s.name)
  // root_shared keeps the five largest rows per root: at the cap there may be sharers it does not name.
  const capped = storage.shared.length >= SHARED_CAP
  // The confirmation splits Shared into what pins keep and the rest, so Frees + Stays (shared) + Stays (pinned) = Reachable.
  const figures = stale ? null : { reachable: root.reachable, blocks: root.blocks, exclusive: root.exclusive, shared: Math.max(0, sharedBytes - root.pinned),
    pinned: root.pinned, partial: root.partial, sharers: sharerLinks, capped, runs: runSharers.length > 0, pinners: pinners.map(p => whoText(p.assertedBy, ctx.viewer)) }
  const confirm = can && !released ? releaseConfirm(ctx, completionCid, state, { name: row.run_name, store, open: release, sweep, figures }) : null
  const yours = samePrincipal(root.assertedBy, ctx.viewer) ? root.reachable : null
  // Stale figures are drawn as the snapshot saw the run: a run released since then was still held in it.
  const snapReleased = stale ? root.state === 'released' : released
  const here = panel({ title: `This run in ${store}`, meta: stale ? 'as of the snapshot' : null },
    storageBar({ total: root.reachable, pinned: root.pinned, yours,
      held: Math.max(0, root.reachable - root.pinned - (snapReleased ? root.exclusive : 0)), releasing: snapReleased ? root.exclusive : 0 }),
    kv([
      ['Reachable', `${sizeText(size)} · ${plural(root.blocks, 'file block')}`],
      // Only this run is what a release would free: like Compare, only where this page can release.
      snapReleased ? ['Releasing', amount(root.exclusive)] : can ? ['Only this run', amount(root.exclusive)] : null,
      ['Shared', sharedBytes > 0 ? `${amount(sharedBytes)}${sharers.length ? ` with ${namesText(sharers, 3, capped)}` : ''}` : 'nothing'],
      ['Pinned here', pinners.length ? pinners.map(p => `${whoText(p.assertedBy, ctx.viewer, { capital: true })} ${amount(p.bytes)}`).join(', ') : 'nothing'],
    ].filter(Boolean)),
    stale ? h('p', { class: 'muted small', 'data-run-storage-asof': '' },
      staleFiguresText(snapReleased ? 'Releasing and Pinned here are' : can ? 'Only this run and Pinned here are' : 'Pinned here is', changes)) : null)
  here.setAttribute('data-run-storage', completionCid)
  return [confirm?.el,
    released ? h('p', { class: 'muted', 'data-run-size': '' }, releasedText(size, root, sweep, stale)) : null,
    // What each release would free shows only where this page can release (mockup 09's rule for freed bytes).
    can ? h('div', { class: 'grid-2' }, here, compare(runs, row, completionCid, store, stale)) : here,
    panel({ title: released ? 'Pin or restore' : 'Pin or release' },
      retentionPanel(completionCid, state, ctx, { isRun: true, kind: 'run', href, size: released ? null : size, footer: false,
        pinnedBy: pinsMoved ? null : pinners, roots: stale ? null : storage.roots, onRelease: confirm?.open }))]
}

/**
 * A released run's line on its Storage tab (R6). The run root's figures are
 * what is still stored here: a sweep's absent blocks leave them (F1), so
 * they never say what the run references. The line gives what is still
 * stored, what of it is releasing (its exclusive bytes) and from when a
 * sweep can reclaim it. With nothing of its own past the earliest date, or
 * with no grace known, a sweep may already have run: the line says so with
 * no figure. Stale figures leave the releasing amount to the next snapshot.
 */
function releasedText(size, root, sweep, stale) {
  // A sweep's deletions do not come back with a Restore, as retentionPanel says.
  const restore = 'Restore content holds whatever of its files are still here.'
  const until = !sweep ? 'after the grace period' : sweepPassed(sweep) ? 'now' : `from ${dayText(sweep.at, sweep.now)}`
  if (stale) {
    return `This run's files still stored here, as of the snapshot: ${sizeText(size)}. The next snapshot says how much of it is releasing; `
      + `a sweep can reclaim that ${until}. ${restore}`
  }
  if (root.exclusive === 0 && (!sweep || sweepPassed(sweep))) {
    return 'A sweep may already have reclaimed any files only this run held; its records and lineage stay here.'
  }
  const stored = `This run's files still stored here: ${sizeText(size)}`
  // Exclusive 0 before the earliest date: shared throughout, or a sweep has already run (a sweep may run early, by hand).
  if (root.exclusive === 0) {
    return `${stored}; none of it is releasing, since other runs, Selections or pins here hold every file, or a sweep has already reclaimed them. ${restore}`
  }
  return `${stored}; ${sizeText({ bytes: root.exclusive, partial: root.partial })} of it is releasing and a sweep can reclaim it ${until}. ${restore}`
}

/** What each of the pipeline's held runs would free on release here, newest first: this run marked. */
function compare(runs, row, completionCid, store, stale = false) {
  const shown = runs.filter(r => r.pipeline === row.pipeline && r.state === 'held').slice(0, COMPARE)
  const body = shown.map((r) => {
    const me = r.cid === completionCid
    const name = r.name ?? 'unnamed run'
    const freed = sizeText({ bytes: r.exclusive, partial: r.partial })
    return h('tr', { class: me ? 'sel' : null, 'data-compare-run': r.cid },
      h('td', {}, me ? h('b', {}, name) : h('a', { href: `#/run/${r.cid}?tab=storage` }, name)),
      h('td', { class: 'r num' }, sizeText({ bytes: r.reachable, partial: r.partial })),
      h('td', { class: 'r num' }, me ? h('b', {}, freed) : freed))
  })
  const node = panel({ title: 'Compare', meta: `what each release would free in ${store}${stale ? ', as of the snapshot' : ''}`, flush: true },
    h('table', { class: 't' }, h('thead', {}, h('tr', {}, h('th', {}, 'Run'), h('th', { class: 'r' }, 'Reachable'), h('th', { class: 'r' }, 'Frees'))),
      h('tbody', {}, body)))
  node.querySelector('.pb').classList.add('tbox')
  node.setAttribute('data-release-compare', '')
  return node
}

/** A sharer's name, linked to its page when it has one. */
const sharerLink = (s) => (s.href ? h('a', { href: s.href }, s.name) : h('span', {}, s.name))

/** namesText's list with each sharer linked to its page (mockup 10): `a, b, c and 2 more`, or `a, b and others` past root_shared's cap. */
function nameLinks(sharers, max = 3, capped = false) {
  const rest = capped ? ' and others' : sharers.length > max ? ` and ${sharers.length - max} more` : null
  return [...sharers.slice(0, max).flatMap((s, i) => [i ? ', ' : null, sharerLink(s)]), rest]
}

/** Drops `release=1` from the address without a render, so a reload after Cancel does not open the confirmation again. */
function forgetRelease() {
  const loc = globalThis.location
  if (!loc?.hash?.includes('release=1') || !globalThis.history?.replaceState) return
  globalThis.history.replaceState(globalThis.history.state, '', loc.hash.replace(/[?&]release=1/, (m) => (m[0] === '?' ? '?' : '')).replace('?&', '?'))
}

/**
 * Release's inline confirmation (mockups/10-run-storage.html), under the page
 * head so the numbers stay next to the run: what releasing frees in this
 * store, what stays (shared, pinned) and what is always kept, then Cancel and
 * a button that repeats the outcome, which writes the release Claim.
 * `figures` is null when the snapshot has none for this run (a lean snapshot, or a run
 * newer than it), or when a pin, release or deletion Claim written since has
 * made them stale: the confirmation says when the figure will be known.
 * `figures.sharers` are `{ name, href, kind }`, linked in the note when `href` is not null.
 * `sweep` is earliestSweep's answer. Returns `{ el, open }`.
 */
function releaseConfirm(ctx, completionCid, state, { name, store, open, sweep, figures = null }) {
  const status = h('p', { class: 'muted', 'data-release-status': '' })
  const amount = (n) => sizeText({ bytes: n, partial: figures?.partial })
  const sub = (text) => h('div', { class: 'muted sub' }, text)
  const cell = (attr, eyebrow, ...body) => h('div', { [attr]: '' }, h('div', { class: 'eyebrow' }, eyebrow), ...body)
  // "earliest <date>" keeps to one line, so in four columns the line breaks before it, never inside it.
  const grace = () => sub(sweep ? [`after ${sweep.days}-day grace · `, h('span', { class: 'nowrap' }, `earliest ${dayText(sweep.at, sweep.now)}`)] : "after the sweep's grace period")
  const big = (n) => h('div', { class: 'bignum', 'data-bytes': n }, amount(n))
  const cells = [figures
    ? cell('data-release-frees', `Frees in ${store}`, big(figures.exclusive), grace())
    : cell('data-release-frees', `Frees in ${store}`, h('div', { class: 'release-unknown' }, 'Freed bytes are known once the next snapshot is written.'), grace())]
  let explain = null
  if (figures) {
    const links = figures.sharers
    const names = links.map(s => s.name)
    const several = names.length > 1 || figures.capped
    // Every file is held by another run, Selection or hand-off: nothing is exclusive and no pin accounts for any of it.
    const all = figures.reachable > 0 && figures.shared === figures.reachable
    // What holds the files when no run does: Selections, hand-offs or both, by the kinds named.
    const kinds = new Set(links.map(s => s.kind))
    const what = kinds.size > 1 ? 'Selections and hand-offs' : kinds.has('handoff') ? (several ? 'hand-offs' : 'a hand-off') : several ? 'Selections' : 'a Selection'
    const holder = figures.runs ? (all ? 'another run' : 'other runs') : names.length ? what : 'a pin'
    const none = figures.pinned > 0 ? 'every file that stays is pinned' : 'no other run, Selection, hand-off or pin holds these files'
    cells.push(cell('data-release-shared', 'Stays (shared)', big(figures.shared),
      sub(figures.shared === 0 ? none : `${all ? 'every file is also' : 'also'} held by ${holder}`)))
    if (figures.pinned > 0) {
      cells.push(cell('data-release-pinned', 'Stays (pinned)', big(figures.pinned),
        sub(figures.pinners.length ? `by ${figures.pinners.join(', ')}` : 'by a pin here')))
    }
    const named = sharerLink
    const either = () => (names.length === 2 && !figures.capped ? [named(links[0]), ', ', named(links[1]), ', or both']
      : several ? ['one or more of ', nameLinks(links, 3, figures.capped)] : [named(links[0])])
    const until = figures.runs ? 'those runs are released' : several ? `those ${what} are deleted` : `that ${what.replace(/^an? /, '')} is deleted`
    if (figures.reachable === 0) explain = 'This run holds no files here.'
    else if (all && names.length) {
      explain = [`Each of the ${formatNumber(figures.blocks)} files is also held by `, either(), `${figures.runs ? ', because they produced identical bytes' : ''}. `
        + `Releasing this run tidies the run list but won't free disk space until ${until} too.`]
    } else if (figures.shared > 0 && names.length) {
      explain = [`${amount(figures.shared)} of it is also held by `, nameLinks(links, 3, figures.capped), ', so it stays here.']
    }
  }
  cells.push(cell('data-release-kept', 'Always kept', h('div', { class: 'kept' }, 'Run records & lineage'), sub('completion, manifest, config')))
  const confirm = button(figures ? `Release (frees ${amount(figures.exclusive)})` : 'Release', { id: 'release-confirm', kind: 'danger',
    // The address loses release=1 first, so a later re-render (after Restore, say) does not open this again.
    onclick: (e) => { forgetRelease(); return ctx.write.run(status, () => ctx.write.writer.release(completionCid, state.retainClaims), e.currentTarget) } })
  const heading = h('h2', { tabindex: '-1' }, `Release ${name ?? 'this run'} in ${store}?`)
  const el = h('section', { class: 'inline-confirm release-confirm', 'data-release-confirm': completionCid, hidden: !open,
    'aria-label': `Release ${name ?? 'this run'} in ${store}` },
  h('div', { class: 'row' }, heading, h('span', { class: 'spacer' }),
    h('span', { class: `dot ${ctx.store?.key ?? 'a'}` }), h('span', { class: 'muted' }, store)),
  h('div', { class: 'grid-3' }, cells),
  explain ? note(explain) : null,
  h('div', { class: 'row' },
    button('Cancel', { id: 'release-cancel', onclick: () => { el.setAttribute('hidden', ''); forgetRelease() } }),
    h('span', { class: 'spacer' }), confirm),
  status)
  // Focus lands on the question, never on the destructive button.
  return { el, open: () => { el.removeAttribute('hidden'); heading.focus?.() } }
}
