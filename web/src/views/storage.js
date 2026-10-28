// web/src/views/storage.js
// #/storage (P2 storage accounting; mockups/09-storage.html): the open store's
// stored, referenced and dedup figures, its pinned / held / releasing bar and
// the viewer's share, then the store broken down by run (grouped by
// pipeline), by file kind, by pin and by person. Every figure belongs to this
// one store and comes from the snapshot's aggregates (Explorer.storeStats and
// friends), so the page reads no blocks. With one member there is no card
// row (ruling R9): the summary heads the breakdown. With more than one, in
// either mode, mockup 09's card row heads the page instead (P3 rule D5): one
// card per store with its own figures, never a total, and the breakdown below
// is the open member's (the primary in all-stores mode). Freed on release shows
// only where you can act on it (mockup 09): a read-only store has no such
// column. Once the Store Log tail holds a pin, release or deletion, or a run
// or Selection, the snapshot's figures are stale (Explorer.storageChanges):
// the page says so and withholds Freed and Release. Durability is the Claim
// state, as the run page draws it: a run that a tail Claim names reads its
// current state (Explorer.tailClaimStates). The Pinned panel keeps the
// snapshot's pin roots and says they are as of it. A lean snapshot
// (Explorer.storageOmitted) has no figures and says why. Its attributes are
// data-storage-*, never the Gate's data-run or data-address.
import { h, link } from '../html.js'
import { icon, sealed } from '../icons.js'
import { avatar, durability, empty, minibar, note, pageHead, panel, status, storageBar } from '../components.js'
import { bytes, formatNumber, ratioText, sizeText } from '../format.js'
import { agoText, dayText, fullWhenText, runTone, statusText } from '../words.js'
import { asserterKey, isAnon, samePrincipal, whoText } from '../identity.js'
import { extLabel } from '../sizes.js'
import { ask, storedTitle } from './home.js'
import { enc, omittedNote, shortCid, staleFiguresText, unreadableTransfersNote } from './common.js'
import { hrefIn } from '../members.js'
import { progressText } from '../indexing.js'

const TOP_KINDS = 7
const KIND_LABELS = { bam: 'BAM', html: 'HTML reports', zip: 'Zip', txt: 'Text', json: 'JSON', pdf: 'PDF', bigwig: 'bigWig' }
const LOCK = '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="5" y="11" width="14" height="9" rx="2"/><path d="M8 11V8a4 4 0 0 1 8 0v3"/></svg>'

const sub = (text, attrs = {}) => h('span', { class: 'sub', ...attrs }, text)
const pct = (part, total) => (total > 0 ? Math.round(part / total * 100) : 0)

/** A table in a flush panel, as the kit draws one (`.pb.tbox`). */
function tablePanel({ title, meta = null, attrs = {} }, head, rows) {
  const node = panel({ title, meta, flush: true }, h('table', { class: 't' }, h('thead', {}, h('tr', {}, head)), h('tbody', {}, rows)))
  node.querySelector('.pb').classList.add('tbox')
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v)
  return node
}

const th = (label, small = null, right = false) => h('th', { class: right ? 'r' : null }, label, small ? h('small', {}, small) : null)

/** Who wrote: "You" for the viewer, else the principal as the snapshot holds it (identity.js). */
const who = (assertedBy, viewer) => whoText(assertedBy, viewer, { capital: true })

/**
 * The lock a store this page can't write to shows instead of Release and Pin:
 * "View only", since "read-only" is the switcher's word for a store copy can't write to.
 */
function lock(reason, name) {
  const ico = h('span', { class: 'lock-ico' })
  ico.innerHTML = LOCK
  return h('span', { class: 'lock muted', 'data-read-only': '', title: [`This page can't write to ${name ?? 'this store'}.`, reason].filter(Boolean).join(' ') }, ico, 'View only')
}

/** A store's name with its dot, and on the right what it is (`where`) and whether this page can write to it. */
function storeHead(store, writable, lockReason, where = null) {
  const said = (text) => h('span', { class: 'mono muted', style: 'font-size:11px' }, text)
  const right = writable ? said([where, 'writable'].filter(Boolean).join(' · '))
    : where ? h('span', { class: 'row', style: 'gap:6px' }, said(where), lock(lockReason, store.name)) : lock(lockReason, store.name)
  return h('div', { class: 'row', style: 'justify-content:space-between' },
    h('b', {}, h('span', { class: `dot ${store.key ?? 'a'}` }), ' ', store.name), right)
}

/** The figures under a store's head: stored, referenced and dedup, the stacked bar and the Yours bar, and only the notes that apply. */
function figures(stats, yours, stale) {
  const dedup = stats.dedup == null ? null : ratioText(stats.dedup)
  return [
    h('div', { 'data-storage-summary': '' },
      h('span', { class: 'bignum' }, bytes(stats.stored)), ' ',
      h('span', { class: 'muted', style: 'font-size:12px' },
        ['stored', `${bytes(stats.referenced)} referenced`, dedup].filter(Boolean).join(' · '))),
    storageBar({ total: stats.stored, pinned: stats.pinned, held: stats.held, releasing: stats.releasing, yours }),
    stale ? h('p', { class: 'muted storage-bar-asof', 'data-storage-bar-asof': '' }, 'Pinned, Held and Releasing are as of the snapshot.') : null,
    stats.partial ? note('Some sizes are missing, so these figures are at least what is shown.', 'warn') : null].filter(Boolean)
}

function summary(stats, yours, store, writable, write, stale) {
  const node = panel({}, storeHead(store, writable, write?.reason), figures(stats, yours, stale))
  node.classList.add('storage-summary')
  node.setAttribute('data-storage-store', store.name)
  return node
}

/**
 * A run's durability here: pinned when a pin root names it, else its root's
 * state (R3). A run a Claim since the snapshot names carries its current
 * Claim state instead (`claimPinned`, and `state` and `since` from
 * currentRuns). A released run reads `Releasing · <earliest sweep date>`,
 * and `Released` once that date has passed, as the run page's badge does.
 */
function runState(run, pinned, graceMs, now) {
  if (run.claimPinned ?? pinned.has(run.cid)) return durability('pinned', 'Pinned')
  if (run.state === 'released') {
    const at = Date.parse(run.since ?? '')
    const earliest = graceMs !== null && Number.isFinite(at) ? at + graceMs : null
    if (earliest !== null && earliest <= now) return durability('releasing', 'Released')
    return durability('releasing', `Releasing${earliest === null ? '' : ` · ${dayText(earliest, now)}`}`)
  }
  return durability('held', 'Held')
}

/** What a stale figure shows in its place. */
const withheld = () => h('td', { class: 'r num nowrap muted', title: 'Known once the next snapshot is written.' }, '—')

function freedCell(run, stale) {
  if (stale) return withheld()
  const freed = sizeText({ bytes: run.exclusive, partial: run.partial })
  if (run.exclusive === 0 && run.reachable > 0) {
    return h('td', { class: 'r num nowrap' }, freed, ' ', sub('· all shared', { title: 'Every file is also held by another run, Selection or pin here.' }))
  }
  return h('td', { class: 'r num nowrap' }, freed)
}

function runRow(run, { pinned, graceMs, writable, stale, now }) {
  const settled = run.status === 'succeeded' || run.status === 'failed'
  const line = [run.pipeline, run.status === 'failed' ? 'failed' : settled ? null : statusText({ status: run.status }).toLowerCase(), dayText(run.finishedAt, now)]
  const release = writable && !stale && run.state === 'held' && run.reachable > 0
    ? h('a', { class: 'btn sm', href: `#/run/${run.cid}?tab=storage&release=1`, 'data-release': run.cid }, 'Release…') : null
  return h('tr', { 'data-storage-run': run.cid },
    h('td', {}, h('div', { class: 'namecell' }, sealed('run', run.cid, { size: 20, tone: runTone({ status: run.status }) }), link(`#/run/${run.cid}`, run.name ?? 'unnamed run')),
      sub(line.filter(Boolean).join(' · '))),
    h('td', {}, runState(run, pinned, graceMs, now)),
    h('td', { class: 'r num nowrap' }, sizeText({ bytes: run.reachable, partial: run.partial })),
    writable ? [freedCell(run, stale), h('td', { class: 'r' }, release)] : null)
}

/** The pipeline's header row; its bytes come from pipeline_stats when the snapshot has them (Explorer.pipelineSize). */
function pipelineRow(ex, pipeline, writable) {
  const cell = h('td', { class: 'r num nowrap' }, sub('…'))
  ask(() => ex.pipelineSize(pipeline)).then((size) => {
    cell.replaceChildren(sizeText(size))
    cell.setAttribute('title', storedTitle(size))
  }, () => cell.replaceChildren(sub('?')))
  return h('tr', { class: 'group', 'data-storage-pipeline': pipeline },
    h('td', { colspan: '2' }, h('div', { class: 'namecell' }, icon('pipeline'), link(`#/pipeline/${enc(pipeline)}`, pipeline))),
    cell, writable ? h('td', { colspan: '2' }) : null)
}

function byRun(ex, runs, stats, options) {
  // Runs hidden by a delete Claim are left out, as the run list leaves them out (R5).
  const shown = runs.filter(r => r.state !== 'deleted')
  const groups = new Map()
  for (const r of shown) groups.set(r.pipeline, [...(groups.get(r.pipeline) ?? []), r])
  const rows = [...groups].flatMap(([pipeline, rs]) => [pipelineRow(ex, pipeline, options.writable), ...rs.map(r => runRow(r, options))])
  const meta = stats.shared > 0 ? `${bytes(stats.shared)} is shared by more than one run` : null
  // Freed on release, and the Release links beside it, only where this page can release (mockup 09).
  const head = [th('Run'), th('Durability'), th('Reachable', 'unique bytes', true),
    options.writable ? [th('Freed on release', 'this store', true), h('th', {})] : null]
  const body = rows.length ? rows : [h('tr', {}, h('td', { colspan: options.writable ? '5' : '3' }, empty('No runs yet', 'Runs appear here once they are logged in this store and a snapshot is written.')))]
  return tablePanel({ title: 'By run', meta, attrs: { 'data-storage-by-run': '' } }, head, body)
}

/** The kinds table's rows: the top seven by bytes, then Other for the rest. */
export function kindRows(kinds) {
  const top = kinds.slice(0, TOP_KINDS).map(k => ({ key: k.kind, label: KIND_LABELS[k.kind] ?? (k.kind ? k.kind.toUpperCase() : 'No extension'), ext: k.kind, files: k.files, bytes: k.bytes }))
  const rest = kinds.slice(TOP_KINDS)
  if (rest.length) top.push({ key: 'other', label: 'Other', ext: null, files: rest.reduce((n, k) => n + k.files, 0), bytes: rest.reduce((n, k) => n + k.bytes, 0) })
  return top
}

function byKind(kinds) {
  const rows = kindRows(kinds)
  const max = Math.max(0, ...rows.map(r => r.bytes))
  const body = rows.map(r => h('tr', { 'data-storage-kind': r.key },
    h('td', {}, r.label, r.ext ? [' ', h('span', { class: 'kind' }, extLabel(r.ext))] : null),
    h('td', { class: 'r num' }, formatNumber(r.files)),
    h('td', { class: 'r nowrap' }, h('div', { class: 'bar-cell' }, h('span', { class: 'num' }, bytes(r.bytes)), minibar(max > 0 ? r.bytes / max : 0)))))
  if (!body.length) return panel({ title: 'By kind' }, empty('Nothing stored yet', 'File kinds appear here once a run publishes outputs to this store.'))
  return tablePanel({ title: 'By kind' }, [th('Kind'), th('Files', null, true), th('Stored', null, true)], body)
}

/** A pinned subject's page: a run or Selection by name, anything else by its short CID. An item's page needs a collection holding it. */
function pinSubject(p) {
  const route = { run: 'run', selection: 'selection', collection: 'collection' }[p.subjectKind]
  const href = route ? `#/${route}/${p.cid}` : p.subjectKind === 'item' && p.collection ? `#/item/${p.collection}/${p.cid}` : `#/block/${p.cid}`
  return p.name ? h('a', { href, title: p.cid }, p.name) : h('a', { href, class: 'mono', title: p.cid }, shortCid(p.cid))
}

/**
 * The Pinned panel, from the snapshot's pin roots. `stale` hides what only
 * each pin keeps; `asOf` (a pin, release or deletion Claim since the
 * snapshot) marks the panel as the snapshot's, since its rows may have moved.
 */
function pins(roots, store, viewer, { stale, asOf, now }) {
  const title = `Pinned in ${store.name}`
  const meta = asOf ? 'as of the snapshot' : null
  if (!roots.length) {
    const node = panel({ title, meta }, empty('Nothing pinned yet', 'Pinned items stay in this store even after their runs are released. Pin from any run, item, file or Selection.'))
    node.setAttribute('data-storage-pins', '')
    return node
  }
  // The pinner in the form By person shows: the key the snapshot's per-person table uses.
  const body = roots.map(p => h('tr', { 'data-storage-pin': p.cid },
    h('td', {}, pinSubject(p)),
    h('td', {}, h('span', { class: 'row' }, avatar(asserterKey(p.assertedBy)), who(asserterKey(p.assertedBy), viewer))),
    h('td', { class: 'nowrap' }, sub(dayText(p.since, now), { title: p.since ?? '' })),
    h('td', { class: 'r num nowrap', title: stale ? null : `${bytes(p.exclusive)} is kept only by this pin` }, bytes(p.reachable))))
  return tablePanel({ title, meta, attrs: { 'data-storage-pins': '' } }, [th('Pinned'), th('By'), th('When'), th('Keeps', 'this store', true)], body)
}

function onlyWriter(person, store, viewer) {
  if (samePrincipal(person.assertedBy, viewer)) return `You're the only person who has written to ${store.name}.`
  if (isAnon({ principal: person.assertedBy })) return `Everything in ${store.name} was written without an identity.`
  return `Only ${person.assertedBy} has written to ${store.name}.`
}

function byPerson(people, stats, store, viewer, { writable, stale }) {
  const freed = (p) => (!writable ? null : stale ? withheld() : h('td', { class: 'r num nowrap' }, bytes(p.exclusive)))
  const body = people.map(p => h('tr', { 'data-storage-person': p.assertedBy },
    h('td', {}, h('span', { class: 'row' }, avatar(p.assertedBy), who(p.assertedBy, viewer))),
    h('td', { class: 'r num nowrap' }, `${bytes(p.reachable)} · ${pct(p.reachable, stats.stored)}%`),
    freed(p),
    h('td', { class: 'r num nowrap' }, bytes(p.pinned))))
  const node = body.length
    ? tablePanel({ title: 'By person', attrs: { 'data-storage-people': '' } },
      [th('Person'), th('Reachable', 'from their roots', true), writable ? th('Freed on release', 'this store', true) : null, th('Pinned', null, true)], body)
    : panel({ title: 'By person' }, h('p', { class: 'muted' }, `Nobody has written to ${store.name} yet.`))
  node.setAttribute('data-storage-people', '')
  if (people.length === 1) node.append(h('p', { class: 'muted storage-foot' }, onlyWriter(people[0], store, viewer)))
  return node
}

/** What Yours shows: unknown or anonymous viewer hides it; a known viewer with no roots in this store holds 0 B of it (mockup 09's ana-hpc card). */
const yoursOf = (people, viewer) => (viewer === null ? null : people.find(p => samePrincipal(p.assertedBy, viewer))?.reachable ?? 0)

export const CARDS_CAPTION = 'Each store keeps its own pins and runs its own cleanup.'

/**
 * Mockup 09's card row: one card per member, in members.json order, each with
 * its own store's figures and never a total. The open member's card (`open`:
 * its explorer, stats, people, stale and writable) is the one broken down
 * below; every other card opens that member's Storage page, and fills in when
 * its snapshot answers. A member whose snapshot cannot be read says so in its
 * card and leaves the others as they are.
 */
function storeCards(ctx, viewer, open) {
  const write = ctx.write ?? null
  const indexing = ctx.indexing ?? null
  const cards = ctx.members.list.map((m) => {
    const shown = m.alias === ctx.members.primary
    const writable = shown ? open.writable : !!write?.available && write.writable === m.alias
    const reason = shown || !write?.available ? write?.reason : `This explore server writes only to ${write.writable}.`
    const where = (m.kind === 's3' ? m.location : m.kind) ?? null
    const head = storeHead({ name: m.alias, key: m.key }, writable, reason, where)
    const body = h('div', { class: 'storage-card-body' })
    const actions = h('div', { class: 'storage-card-actions', 'data-index-actions': '' })
    const main = shown ? h('div', { class: 'storage-card-main' }, head, body)
      : h('a', { class: 'storage-card-main', href: hrefIn(ctx, m.alias, '#/storage'), 'data-storage-card-open': '', title: `Open the Storage page in ${m.alias}` }, head, body)
    const card = h('div', shown ? { class: 'panel storage-card on', 'aria-current': 'true', 'data-storage-card': m.alias, title: `${m.alias} is broken down below` }
      : { class: 'panel storage-card', 'data-storage-card': m.alias }, h('div', { class: 'pb' }, main, actions))
    const said = (text, attrs = {}) => h('p', { class: 'muted', 'data-storage-card-note': '', ...attrs }, text)
    const noFigures = () => said('Its snapshot leaves out storage figures.')
    // A member served from its local snapshot has one: "No snapshot yet" would be wrong there.
    const loadFigures = (idx) => ctx.members.explorer(m.alias).then(async (mex) => {
      const stats = await mex.storeStats()
      if (!stats) return [noFigures()]
      const people = await mex.asserterStats()
      const changes = mex.storageChanges()
      return figures(stats, yoursOf(people, viewer), changes.claims || changes.logged)
    }).catch((e) => [said(ctx.members.status(m.alias).state !== 'no_snapshot' || servesLocal(idx) ? `Could not be read (${e?.message ?? e}).`
      : e?.older ? 'Its snapshot is older than this explorer reads; nextflow plugin nf-blocks:snapshot rewrites it.' : 'No snapshot yet.')])

    let refused = null
    let notes = []
    let figs = []
    // The figures are read again only when the state, time or local flag moves, not on a progress tick.
    let figsFor = null
    const draw = () => body.replaceChildren(...notes, ...figs)
    // A phase without counters reports the same index on every poll, so the
    // elapsed time is repainted on a clock of its own, while the card shows.
    let ticking = null
    const clock = (idx) => {
      if (idx?.state === 'indexing') ticking ??= setInterval(() => { if (card.isConnected) paintBody(); else clock(null) }, 1000)
      else if (ticking !== null) { clearInterval(ticking); ticking = null }
    }
    const paintBody = () => {
      const idx = indexing?.stateOf(m.alias) ?? null
      clock(idx)
      notes = [...indexNotes(idx, m.alias), refused ? said(refused, { 'data-index-error': '' }) : null].filter(Boolean)
      if (shown) figs = open.stats ? figures(open.stats, yoursOf(open.people, viewer), open.stale) : [noFigures()]
      else if (!opensSnapshot(idx)) { figs = []; figsFor = null }
      else {
        const key = JSON.stringify([idx?.state, idx?.at, idx?.local])
        if (key !== figsFor) {
          figsFor = key
          figs = [said('…')]
          loadFigures(idx).then((nodes) => { if (figsFor === key) { figs = nodes; draw() } })
        }
      }
      draw()
    }
    // A refused click's message holds until the card's state or the running job changes.
    const seen = () => JSON.stringify([indexing?.stateOf(m.alias)?.state ?? null, indexing?.running() ?? null])
    let refusedAt = null
    const paintActions = () => {
      const idx = indexing?.stateOf(m.alias) ?? null
      const buttons = (idx ? indexButtons(idx) : []).map(([label, mode]) => {
        const cannotPush = mode === 'push' && m.copy_target === false
        const why = cannotPush ? (m.copy_reason || "this store can't be written") : indexing.running() ? 'another store is being indexed' : null
        return h('button', { type: 'button', class: 'btn sm', 'data-index-mode': mode, disabled: why !== null, title: why, onclick: why === null ? () => start(mode) : null }, label)
      })
      const blocked = buttons.some(b => b.dataset.indexMode === 'push') && m.copy_target === false
      actions.replaceChildren(...buttons, ...(blocked ? [h('p', { class: 'muted', 'data-index-why': '' }, `Can't push here: ${m.copy_reason || "this store can't be written"}`)] : []))
    }
    async function start(mode) {
      refused = null
      try { await indexing.start(m.alias, mode) } catch (e) { refused = e?.message ?? String(e); refusedAt = seen() }
      paintBody()
      paintActions()
    }

    paintBody()
    paintActions()
    if (indexing) {
      // Alive until the card has been on the page and then left it (copy.js's rule); the fake DOM of tests is never connected.
      let connected = false
      const off = indexing.onChange((alias) => {
        if (card.isConnected) connected = true
        else if (connected) { off(); return }
        const cleared = refused !== null && seen() !== refusedAt
        if (cleared) refused = null
        if (alias === m.alias || cleared) paintBody()
        paintActions()
      })
    }
    return card
  })
  return [h('div', { class: 'grid-3 storage-cards', 'data-storage-cards': '' }, cards),
    h('p', { class: 'muted storage-caption', 'data-storage-caption': '' }, CARDS_CAPTION)]
}

/** Whether the member is served from the snapshot indexed on this machine: local, or a job on it running or failed with that one kept. */
const servesLocal = (idx) => idx?.state === 'local' || (!!idx?.local && (idx.state === 'indexing' || idx.state === 'failed'))

/** Whether a member's card reads its snapshot: no index object (an older server) means it always tries, as before. */
const opensSnapshot = (idx) => !idx || idx.state === 'snapshot' || servesLocal(idx)

/** What each index state says in its card; `snapshot` and no index object say nothing beyond the figures. */
function indexNotes(idx, alias) {
  if (!idx) return []
  const say = (text, attrs = {}) => h('p', { class: 'muted', 'data-storage-card-note': '', ...attrs }, text)
  const onThisMachine = say(['Indexed on this machine', idx.at ? agoText(idx.at) : null].filter(Boolean).join(' · '))
  switch (idx.state) {
    case 'local': return [onThisMachine]
    case 'none': return [say(['Not indexed: its runs and Selections are not listed.', idx.reason && idx.reason !== 'absent' ? `Its own snapshot can't be used (${idx.reason}).` : null].filter(Boolean).join(' '))]
    case 'indexing': return [say(progressText(idx, alias), { 'data-index-progress': '' })]
    case 'failed': return [say(`Indexing failed: ${idx.reason}`), idx.local ? onThisMachine : null].filter(Boolean)
    case 'outside': return [say('Not in cas.resolve, so the explorer does not index it')]
    default: return []
  }
}

/** The buttons a state offers, as [label, mode]. */
function indexButtons(idx) {
  switch (idx.state) {
    case 'local': return [['Push to store', 'push']]
    case 'none': return [['Index locally', 'local'], ['Index and push', 'push']]
    case 'failed': return [['Retry', idx.mode ?? 'local']]
    default: return []
  }
}

export async function storage(ex, ctx) {
  const store = ctx?.store ?? { name: 'this store', key: 'a' }
  const viewer = ctx?.viewer ?? null
  const written = ex.snapshotWrittenAt
  const now = ex.now()
  // More than one member, in either mode: the card row replaces R9's single summary, and its caption the lead.
  const several = (ctx?.members?.list?.length ?? 0) > 1
  const head = pageHead({ title: 'Storage', after: [h('span', { class: 'muted storage-asof', title: written ? fullWhenText(written) : null }, `as of snapshot${written ? ` · ${agoText(written, now)}` : ''}`)] })
  if (!several) head.append(h('p', { class: 'muted storage-lead' }, 'Each store keeps its own pins and runs its own cleanup, so every number here belongs to one store.'))
  // A read-only store shows a lock and no Release: Release writes a Claim beside the run, in this member.
  const writable = !!ctx?.write?.available && ctx.write.here !== false
  const unreadable = unreadableTransfersNote(ex)
  if (unreadable) head.append(unreadable)
  const stats = await ex.storeStats()
  if (!stats) {
    return h('section', { class: 'storage', style: 'min-width:0' }, head,
      several ? storeCards(ctx, viewer, { ex, stats, people: [], stale: false, writable }) : null,
      omittedNote(ex))
  }
  const changes = ex.storageChanges()
  const stale = changes.claims || changes.logged
  if (stale) head.append(h('p', { class: 'muted storage-stale', 'data-storage-stale': '' }, staleFiguresText('These figures are', changes)))
  const [people, snapshotRuns, kinds, roots] = await Promise.all([ex.asserterStats(), ex.storageRuns(), ex.kindStats(), ex.pinRoots()])
  const runs = changes.claims ? await currentRuns(ex, snapshotRuns) : snapshotRuns
  const pinned = new Set(roots.map(p => p.cid))
  return h('section', { class: 'storage', style: 'min-width:0' }, head,
    several ? storeCards(ctx, viewer, { ex, stats, people, stale, writable })
      : summary(stats, yoursOf(people, viewer), store, writable, ctx?.write, stale),
    h('h2', { class: 'storage-detail' }, h('span', { class: `dot ${store.key ?? 'a'}` }), ` ${store.name} in detail`),
    h('div', { class: 'grid-2 storage-grid' },
      byRun(ex, runs, stats, { pinned, graceMs: stats.graceMs, writable, stale, now }),
      h('div', { class: 'stack' }, byKind(kinds), pins(roots, store, viewer, { stale, asOf: changes.claims, now }),
        byPerson(people, stats, store, viewer, { writable, stale }))))
}

/**
 * The snapshot's runs with each one a Claim since the snapshot names given
 * its current Claim state, as the run page reads it (Explorer.tailClaimStates):
 * `state` deleted, released or held, `since` the release Claim's time, and
 * `claimPinned`. Their byte figures stay the snapshot's.
 */
async function currentRuns(ex, runs) {
  const states = await ex.tailClaimStates(runs.map(r => r.cid))
  return runs.map((r) => {
    const s = states.get(r.cid)
    if (!s) return r
    const state = s.hidden ? 'deleted' : s.released ? 'released' : 'held'
    return { ...r, state, since: s.released ? s.releaseClaim?.timestamp ?? null : r.since, claimPinned: s.pinned }
  })
}
