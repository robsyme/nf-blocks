// web/src/views/project.js
// #/project/<cid>?tab= (P4 Task 3, mockups/13-project.html, design doc §4):
// one project. The head holds its name, who created it, its people, its
// default store and retention, and "Add to project…"; Edit… opens its
// policies (plan 1.7) and Rename. Tabs: Overview (people, storage per
// store, recent activity), Samples (project-samples.js), Runs and
// Selections (the members, each with the Claim that added it, and Remove),
// Activity (the Claims on it and its members, hand-offs and copies, newest
// first). Every storage figure is one store's, never added up; every pin
// it writes names its bytes first.
import { h, link } from '../html.js'
import { ago, avatar, button, claimTag, pageHead, panel, shortCid, storageBar, tabs } from '../components.js'
import { icon, sealed } from '../icons.js'
import { formatNumber, plural, sizeText } from '../format.js'
import { samePrincipal } from '../identity.js'
import { fromEach, hrefIn, unreadableText } from '../members.js'
import { isAddressQuery } from '../model.js'
import { runTone, whenText } from '../words.js'
import { cids, errorNode, flag, holds, omittedNote, pinsList, projectClaimWords, show, unavailableNote } from './common.js'
import { samplesTab } from './project-samples.js'
import { pinFigures, pinnedByWriter, policyAdd, policyNote } from './addproject.js'

// The tab keys are listquery.js's PROJECT_TABS, which the route parses.
const TABS = [['overview', 'Overview'], ['samples', 'Samples'], ['runs', 'Runs'], ['selections', 'Selections'], ['activity', 'Activity']]
const RECENT = 8
const CAPTION = "Each store keeps its own pins and runs its own cleanup, so they're shown side by side and never added up."
const newestFirst = (a, b) => ((a.at ?? '') < (b.at ?? '') ? 1 : (a.at ?? '') > (b.at ?? '') ? -1 : a.key < b.key ? -1 : 1)

// The policy pin's note, and whether the writer pins a Selection already, are addproject.js's (shared with the run,
// Selection and item pages' Add to project).
export { policyNote }

/** Every note a policy pin of this project may carry: one per name it has ever had, its block's and every `set name` Claim's, superseded ones included. */
const policyNotes = (p) => new Set([p.blockName, ...(p.claims ?? []).filter(c => c.attribute === 'name' && c.verb === 'set').map(c => c.value), ...p.names]
  .filter(Boolean).map(policyNote))

export async function project(ex, cid, ctx, { tab = 'overview' } = {}) {
  const p = await ex.project(cid)
  ctx.mark?.({ project: cid })
  const on = TABS.some(([k]) => k === tab) ? tab : 'overview'
  const members = groupMembers((await ex.projectMembers(cid)).filter(m => !m.hidden))
  const of = (kind) => members.filter(m => m.kind === kind)
  const selStates = await ex.claimStates(of('selection').map(m => m.subject))
  const names = await memberNames(ex, members, selStates)
  const can = !!(ctx.write?.available && ctx.write?.here)
  const status = h('div', { id: 'write-status', 'data-project-status': '' })
  const href = (t = on) => `#/project/${cid}?tab=${t}`
  // One write from this page: through ctx.write.run, then this tab again from the page's own tail.
  const write = (attempt, el = null) => ctx.write.run(status, async () => { await attempt(ctx.write.writer); return { href: href() } }, el)
  const retention = p.policy.retention.value === 'pin_selections'
  const policyNames = policyNotes(p)
  const sels = of('selection').map(m => ({ ...m, name: names.get(m.subject), state: selStates.get(m.subject) }))
  const pins = { ex, p, ctx, write, status, policyNames }
  // D5: the Selections the policy has not pinned here, which the head, the Edit panel and the Selections tab each offer to pin.
  const unpinned = retention ? notPinned(sels, policyNames, ctx) : []
  const edit = can ? editPanel(ex, p, ctx, { write, sels, pins, unpinned }) : null
  const add = can ? addPanel(ex, p, ctx, { write, members, retention }) : null
  const counts = { runs: of('run').length, selections: sels.length }
  const nav = tabs(TABS.map(([key, label]) => ({ key, label, href: href(key), on: key === on, count: counts[key] })), { label: 'Project views' })
  const counted = (n) => nav.querySelector('[data-tab=samples]').append(h('small', {}, String(n)))
  const activity = () => activityEvents(ex, p, ctx, { members, names, sels })
  const scope = memberScope(ctx, cid)
  // This render's page (Explorer.pageChanged marks it gone): the samples read stops when the reader leaves.
  const page = ex.page
  // On Samples the runs' items are read first; storage and activity fill the aside while they load.
  const aside = h('div', { class: 'stack' })
  const body = on === 'samples' ? samplesTab(ex, p, ctx, { members, can, write, counted, alive: () => !page?.gone, aside })
    : on === 'runs' ? runsTab(ex, p, ctx, { runs: of('run'), items: of('item'), names, can, write, scope })
      : on === 'selections' ? selectionsTab(ex, p, ctx, { sels, can, retention, pins, unpinned, scope })
        : on === 'activity' ? activityPanel(await activity(), ex, { all: true, scope })
          : h('div', { class: 'grid-2' }, h('div', { class: 'stack' }, peoplePanel(p), activityPanel(await activity(), ex, { limit: RECENT, more: href('activity'), scope })),
            await storagePanel(ex, cid, ctx))
  if (on === 'samples') {
    const [storage, events] = await Promise.all([storagePanel(ex, cid, ctx), activity()])
    aside.replaceChildren(storage, activityPanel(events, ex, { limit: 4, scope }))
  }
  return h('section', { class: 'project-page', 'data-project': cid, 'data-tab': on },
    head(p, ctx, { edit, add, retention, unpinned, selectionsHref: href('selections') }),
    can ? null : unavailableNote(ctx, 'Editing this project', 'Open it there', ctx.write.hrefFor(href())),
    p.nameConflicted ? h('p', { class: 'warn' }, 'More than one name is current. Renaming supersedes them all.') : null,
    edit?.node, add?.node, status, nav, body)
}

/** projectMembers' rows, one per subject: `[{ subject, kind, claims: [{ cid, asserted_by, timestamp }] }]`. */
function groupMembers(rows) {
  const out = new Map()
  for (const m of rows) {
    if (!out.has(m.subject)) out.set(m.subject, { subject: m.subject, kind: m.kind, claims: [] })
    out.get(m.subject).claims.push(m.claim)
  }
  return [...out.values()]
}

/**
 * Which store the member lists and the Activity read (P3's R9: the open
 * member only). With one member, nothing. With several, `meta(text)` adds
 * "in <alias>" to a panel's meta, and `line(tab)` is one muted line linking
 * the same tab of this project in each other member.
 */
function memberScope(ctx, cid) {
  const list = ctx.members?.list ?? []
  if (list.length < 2) return { meta: (text) => text, line: () => null }
  const here = ctx.members.primary ?? ctx.store?.name
  const others = list.filter(m => m.alias !== here)
  return {
    meta: (text) => [text, `in ${here}`].filter(Boolean).join(' · '),
    line: (tab) => h('p', { class: 'muted small', 'data-other-members': '' }, `Read from ${here} only. The same in `,
      others.map((m, i) => [i ? ', ' : null, link(hrefIn(ctx, m.alias, `#/project/${cid}?tab=${tab}`), m.alias)]), '.'),
  }
}

/** What each member is called: a run's name, a Selection's current names, an item's short cid. */
async function memberNames(ex, members, selStates) {
  const out = new Map()
  await Promise.all(members.map(async (m) => {
    if (m.kind === 'run') out.set(m.subject, (await ex.runRow(m.subject).catch(() => null))?.run_name ?? 'unnamed run')
    else if (m.kind === 'selection') out.set(m.subject, selStates.get(m.subject)?.names.join(' / ') || 'unnamed Selection')
    else out.set(m.subject, shortCid(m.subject))
  }))
  return out
}

// ---- the head ----------------------------------------------------------------

/** The creator, then the people its policy names (advisory), each once. */
const peopleOf = (p) => [p.createdBy, ...p.policy.people.map(x => x.principal)]
  .filter((who, i, all) => who && all.findIndex(o => samePrincipal(o, who)) === i)

/**
 * The default store as the subline shows it: its dot and name, and a note
 * when this explorer cannot copy there (2.Q1): unknown here, or a member
 * that is read-only. The note never makes a read-only member a target.
 */
export function defaultStoreNode(alias, ctx) {
  const list = ctx.members?.list ?? []
  const member = list.find(m => m.alias === alias) ?? (!list.length && alias === ctx.store?.name ? { alias, key: ctx.store.key, copy_target: true } : null)
  const why = !member ? 'not configured here' : member.copy_target === false ? 'read-only here' : null
  return h('span', { 'data-default-store': alias, 'data-default-store-note': why },
    'default store ', member ? h('span', { class: `dot ${member.key ?? 'a'}` }) : null, member ? ' ' : null, h('b', {}, alias),
    why ? h('span', { class: 'muted' }, ` (${why})`) : null)
}

function head(p, ctx, { edit, add, retention, unpinned, selectionsHref }) {
  const store = p.policy.defaultStore
  return pageHead({
    crumbs: [{ text: 'Projects', href: '#/projects' }, { text: p.name ?? 'Project' }],
    title: p.name ?? 'Unnamed project',
    after: [p.nameConflicted ? flag('names in conflict') : null,
      p.createdBy ? claimTag({ assertedBy: p.createdBy, verb: 'created', at: p.createdAt }) : null,
      h('span', { class: 'row project-people', 'data-people': '' }, peopleOf(p).map(who => avatar(who, { size: 'md' }))),
      p.state.deletion === 'deleted' ? flag('deleted') : null],
    actions: [add?.button, edit?.button].filter(Boolean),
    subline: [p.description || null,
      store.conflicted ? h('span', { class: 'warn', 'data-default-store': '' }, 'default store in conflict') : store.value ? defaultStoreNode(store.value, ctx) : null,
      p.policy.retention.conflicted ? h('span', { class: 'warn', 'data-retention': 'conflicted' }, 'retention in conflict') : null,
      retention ? h('span', { 'data-retention': 'pin_selections' }, 'pins every Selection',
        unpinned.length ? [' · ', link(selectionsHref, `${formatNumber(unpinned.length)} not pinned here`)] : null) : null].filter(Boolean),
  })
}

// ---- Edit: rename and policies -------------------------------------------------

/**
 * Rename, the default store, retention and people, in one panel the head's
 * Edit… opens. Turning retention on first shows what each pin keeps
 * (retentionConfirm); turning it off writes no unpin.
 */
function editPanel(ex, p, ctx, { write, sels, pins, unpinned }) {
  const name = h('input', { type: 'text', id: 'project-rename', maxlength: '256', placeholder: 'New name', 'aria-label': 'New name' })
  const rename = button('Rename', { id: 'project-rename-save', onclick: (e) => write((w) => {
    const typed = name.value?.trim() ?? ''
    if (!typed) throw Object.assign(new Error('type a name first'), { code: 'invalid' })
    return w.rename(p.cid, typed, p.nameClaims)
  }, e.currentTarget) })
  const ds = p.policy.defaultStore
  const known = ctx.members?.list?.length ? ctx.members.list.map(m => m.alias) : [ctx.store?.name].filter(Boolean)
  const aliases = [...new Set([...known, ...(ds.value ? [ds.value] : [])])]
  const store = h('select', { id: 'project-default-store', 'aria-label': 'Default store' },
    h('option', { value: '' }, 'No default store'), aliases.map(a => h('option', { value: a }, a)))
  store.value = ds.value ?? ''
  const saveStore = button('Save', { id: 'project-default-store-save', disabled: !ds.conflicted, onclick: (e) => write((w) => (store.value
    ? w.setPolicy(p.cid, 'default_store', store.value, cids(ds.claims)) : w.clearPolicy(p.cid, 'default_store', cids(ds.claims))), e.currentTarget) })
  // Save writes only a change: the same value again, or "No default store" with none set (a del superseding nothing), is no change.
  store.addEventListener('change', () => { saveStore.disabled = !ds.conflicted && (store.value ?? '') === (ds.value ?? '') })
  const r = p.policy.retention
  const on = r.value === 'pin_selections'
  const confirm = h('div', { 'data-retention-confirm': '', hidden: true })
  const pinConfirm = h('div', { 'data-edit-pin-confirm': '', hidden: true })
  const retention = on
    ? [h('p', { class: 'small' }, 'This project pins every Selection in it. Turning it off writes no unpin: the pins stay until someone unpins them on the Selections tab.'),
      unpinned.length ? h('p', { class: 'small', 'data-edit-not-pinned': String(unpinned.length) }, `${plural(unpinned.length, 'Selection')} not pinned by policy · `,
        button('Pin', { size: 'sm', kind: 'ghost', 'data-edit-pin-unpinned': '',
          onclick: () => { pinConfirm.replaceChildren(retentionConfirm(unpinned, pins)); show(pinConfirm, true) } })) : null,
      button('Turn off', { id: 'project-retention-off', onclick: (e) => write(w => w.setPolicy(p.cid, 'retention', 'none', cids(r.claims)), e.currentTarget) }),
      pinConfirm]
    : [h('p', { class: 'small' }, 'Pin every Selection in this project, in this store. Each pin is your Claim with the note "', policyNote(p.name), '".'),
      button('Pin every Selection…', { id: 'project-retention-on', onclick: () => {
        confirm.replaceChildren(retentionConfirm(sels, { ...pins, turnOn: cids(r.claims) }))
        show(confirm, true)
      } }), confirm]
  const person = h('input', { type: 'text', id: 'project-person', placeholder: 'name@example.org', 'aria-label': 'Add a person' })
  const people = p.policy.people.map(x => h('li', { 'data-person': x.principal }, avatar(x.principal), ' ', x.principal, ' ',
    button('Remove', { size: 'sm', kind: 'ghost', 'data-remove-person': x.principal, onclick: (e) => write(w => w.removePerson(p.cid, cids(x.claims)), e.currentTarget) })))
  const node = h('div', { class: 'project-edit', 'data-project-edit': '', hidden: true },
    panel({ title: 'Edit project', meta: 'each change is a Claim on the project' },
      h('div', { class: 'project-fields' },
        h('div', { class: 'row' }, h('b', {}, 'Name'), name, rename),
        h('div', {}, h('div', { class: 'row' }, h('b', {}, 'Default store'), store, saveStore),
          h('p', { class: 'muted small' }, 'Where Copy to… points from this project\'s runs and Selections, unless one is also in a project with another default store. A store that is read-only here, or not configured here, is never preselected.')),
        h('div', { 'data-retention-policy': on ? 'pin_selections' : 'none' }, h('b', {}, 'Retention'), retention),
        h('div', {}, h('b', {}, 'People'), h('p', { class: 'muted small' }, 'Advisory: who works on it. Nothing is enforced.'),
          people.length ? h('ul', { class: 'plain' }, people) : null,
          h('div', { class: 'row' }, person, button('Add person', { id: 'project-person-add', onclick: (e) => write((w) => {
            const who = person.value?.trim() ?? ''
            if (!who) throw Object.assign(new Error('type who to add first'), { code: 'invalid' })
            return w.addPerson(p.cid, who)
          }, e.currentTarget) }))))))
  return { node, button: button('Edit…', { id: 'project-edit-open', onclick: () => show(node, node.hasAttribute('hidden')) }) }
}

/**
 * What pinning `sels` writes and keeps, before it is written (P2's pin
 * confirmation, plan 1.7): each Selection the writer has not pinned yet,
 * with what a pin keeps here and who already pins it here; one Selection's
 * figure never added to another's, since they overlap. With `turnOn` (the
 * retention Claims it supersedes) it sets the policy first. The button
 * waits for every figure.
 */
export function retentionConfirm(sels, { ex, p, ctx, write, turnOn = null }) {
  const store = ctx.store?.name ?? 'this store'
  const targets = sels.filter(s => !pinnedByWriter(s.state, ctx))
  const skipped = sels.length - targets.length
  // What this confirmation has written: a retry after a refused pin sets no policy again, and pins only what is left.
  const written = { policy: false, pinned: new Set() }
  const go = button(turnOn && !targets.length ? 'Turn on' : turnOn ? `Turn on and pin ${plural(targets.length, 'Selection')}` : `Pin ${plural(targets.length, 'Selection')}`,
    { kind: 'primary', id: 'project-pin-go', disabled: true, onclick: (e) => write(async (w) => {
      if (turnOn && !written.policy) {
        await w.setPolicy(p.cid, 'retention', 'pin_selections', turnOn)
        written.policy = true
      }
      for (const s of targets.filter(t => !written.pinned.has(t.subject))) {
        await w.pin(s.subject, policyNote(p.name))
        written.pinned.add(s.subject)
      }
    }, e.currentTarget) })
  const rows = targets.map((s) => {
    const kept = h('span', { 'data-pin-bytes': '' }, 'measuring…')
    const others = h('div', { class: 'muted small', 'data-pinned-by': '', hidden: true })
    const figures = pinFigures(ex, ctx, s.subject).then(({ size, note }) => {
      kept.textContent = size ? `Pin ${sizeText(size)} in ${store}` : `Pin in ${store}`
      if (note) { others.textContent = note; show(others, true) }
    })
    return { figures, node: h('li', { 'data-pin-target': s.subject }, h('b', {}, s.name), ' ', kept, others) }
  })
  Promise.all(rows.map(r => r.figures)).then(() => { go.disabled = false })
  return h('div', { class: 'project-confirm' },
    rows.length ? h('ul', { class: 'plain' }, rows.map(r => r.node)) : h('p', { class: 'muted small' }, sels.length
      ? 'Every Selection here is already pinned by you.' : 'There are no Selections in this project yet. One added from this page is pinned as it is added.'),
    skipped ? h('p', { class: 'muted small', 'data-pin-skipped': '' }, `${plural(skipped, 'Selection')} you already pin ${skipped === 1 ? 'is' : 'are'} left as ${skipped === 1 ? 'it is' : 'they are'}.`) : null,
    h('p', { class: 'muted small' }, `Pins apply to ${store} only. The note on each is "${policyNote(p.name)}".`),
    go)
}

// ---- Add to project… -------------------------------------------------------------

/**
 * Add to project…: an address (a run's, a Selection's or an item's, or the
 * start of one) or a sample ID, then Add on a result. A Selection added
 * while the project pins every Selection is pinned too, unless the writer
 * pins it already: its bytes are named on the button first, with who
 * already pins it here (P2's line).
 */
function addPanel(ex, p, ctx, { write, members, retention }) {
  const already = new Set(members.map(m => m.subject))
  const results = h('ul', { class: 'plain', 'data-add-results': '' })
  const query = h('input', { type: 'search', id: 'project-add-query', placeholder: 'Address, or a sample ID', 'aria-label': 'Find a run, Selection or item' })
  const row = (r) => {
    // A Selection added while the policy is on is pinned too, unless the writer pins it already; decided before the click.
    const policy = retention && r.kind === 'selection' && !already.has(r.cid)
    let pinIt = false
    // A retry after a refused pin does not add it to the project again.
    let added = false
    const add = button(already.has(r.cid) ? 'In this project' : 'Add', { size: 'sm', 'data-add': r.cid, disabled: already.has(r.cid) || policy,
      onclick: (e) => write(async (w) => {
        if (!added) {
          await w.addToProject(r.cid, p.cid)
          added = true
        }
        if (pinIt) await w.pin(r.cid, policyNote(p.name))
      }, e.currentTarget) })
    const said = h('div', { class: 'muted small', 'data-add-pin-note': '', hidden: true })
    if (policy) {
      policyAdd(ex, ctx, r.cid).then(({ pin, label, note }) => {
        pinIt = pin
        add.textContent = label
        if (note) { said.textContent = note; show(said, true) }
        add.disabled = false
      })
    }
    return h('li', { 'data-add-result': r.cid, 'data-kind': r.kind },
      h('div', { class: 'row' }, icon(r.kind === 'selection' ? 'selection' : r.kind === 'run' ? 'run' : 'item'),
        link(r.href, r.label), h('span', { class: 'muted' }, r.sub ?? r.kind), h('span', { class: 'spacer' }), add), said)
  }
  const find = async () => {
    const q = query.value?.trim() ?? ''
    if (!q) return
    results.replaceChildren(h('li', { class: 'muted' }, 'Looking…'))
    try {
      const found = isAddressQuery(q) ? (await ex.searchAddresses(q)).filter(r => ['run', 'selection', 'item'].includes(r.kind))
        : (await ex.searchSamples(q)).map(r => ({ cid: r.item_cid, kind: 'item', label: r.sample, href: `#/item/${r.collection_cid}/${r.item_cid}`,
          sub: `${r.run_name ?? 'unnamed run'} · ${r.output_name}` }))
      results.replaceChildren(...(found.length ? found.map(row) : [h('li', { class: 'muted' }, 'Nothing here matches.')]))
    } catch (e) {
      results.replaceChildren(h('li', {}, errorNode(e)))
    }
  }
  query.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault?.(); find() } })
  const node = h('div', { class: 'project-add', 'data-project-add': '', hidden: true },
    panel({ title: 'Add to project', meta: 'a run, a Selection or an item; membership is your Claim' },
      h('div', { class: 'row' }, query, button('Find', { id: 'project-add-find', onclick: find })), results))
  return { node, find, button: button('Add to project…', { id: 'project-add-open', onclick: () => show(node, node.hasAttribute('hidden')) }) }
}

// ---- Overview: people and storage ------------------------------------------------

function peoplePanel(p) {
  const rows = [p.createdBy ? h('li', { class: 'row', 'data-person': p.createdBy }, claimTag({ assertedBy: p.createdBy, verb: 'created', value: h('span', {}, p.createdBy), at: p.createdAt })) : null,
    ...p.policy.people.filter(x => !samePrincipal(x.principal, p.createdBy)).map(x => h('li', { class: 'row', 'data-person': x.principal },
      ...x.claims.map(c => claimTag({ assertedBy: c.asserted_by, verb: 'added', value: h('span', {}, x.principal), at: c.timestamp }))))]
  return panel({ title: 'People', meta: 'advisory' }, h('ul', { class: 'plain' }, rows))
}

/** One store's figures for the project: `{ stats }`, or `{ why }` ('lean', 'none'). */
async function storeFigures(mex, cid) {
  if (mex.storageOmitted) return { why: 'lean' }
  const stats = (await mex.projectStatsOf([cid])).get(cid)
  return stats ? { stats } : { why: 'none' }
}

/**
 * Storage, per store: one bar for each store whose snapshot holds part of
 * the project, side by side and never added up, from its project_stats.
 * Every member is asked (P3's members; one, normally). A store with no
 * figure says why: a lean snapshot as P2 says it (omittedNote), or none
 * for this project.
 */
export async function storagePanel(ex, cid, ctx) {
  const several = (ctx.members?.list?.length ?? 0) > 1
  const answers = several ? await fromEach(ctx.members, mex => storeFigures(mex, cid))
    : [{ member: { alias: ctx.store?.name ?? 'this store', key: ctx.store?.key ?? 'a' }, ex, value: await storeFigures(ex, cid) }]
  const parts = answers.map(({ member, ex: mex, value, ...failed }) => {
    if (!value) return h('p', { class: 'muted', 'data-store-unread': member.alias }, unreadableText({ member, ...failed }, 'figures'))
    if (value.stats) {
      const s = value.stats
      return h('div', { 'data-store-bar': member.alias, 'data-bytes': String(s.reachable) },
        storageBar({ total: s.reachable, pinned: s.pinned, held: s.held, releasing: s.releasing, store: member.key ?? 'a', storeName: member.alias }))
    }
    if (value.why === 'lean') return h('div', { 'data-store-lean': member.alias }, omittedNote(mex))
    return h('p', { class: 'muted', 'data-store-none': member.alias }, `${member.alias}'s snapshot has no figure for this project.`)
  })
  return panel({ title: 'Storage', meta: 'per store' }, h('div', { class: 'project-storage' }, parts, h('p', { class: 'muted small', 'data-storage-caption': '' }, CAPTION)))
}

// ---- Runs and Selections -----------------------------------------------------------

const addedTags = (m) => m.claims.map(c => claimTag({ assertedBy: c.asserted_by, verb: 'added', at: c.timestamp }))
const removeButton = (p, m, write) => button('Remove', { size: 'sm', kind: 'ghost', 'data-remove': m.subject,
  title: 'Supersedes the Claim that added it. Nothing is deleted or released.', onclick: (e) => write(w => w.removeFromProject(m.subject, cids(m.claims)), e.currentTarget) })

function runsTab(ex, p, ctx, { runs, items, names, can, write, scope }) {
  const runRow = (m) => {
    const cells = { pipeline: h('td', {}), finished: h('td', {}), name: h('span', {}, link(`#/run/${m.subject}`, names.get(m.subject))) }
    ex.runRow(m.subject).then((r) => {
      if (!r) return
      cells.name.replaceChildren(h('span', { class: 'namecell' }, sealed('run', m.subject, { size: 20, tone: runTone(r) }), link(`#/run/${m.subject}`, r.run_name ?? 'unnamed run')))
      cells.pipeline.replaceChildren(r.pipeline ?? '')
      cells.finished.replaceChildren(whenText(r.finished_at))
    }, () => {})
    return h('tr', { 'data-member': m.subject, 'data-kind': 'run' }, h('td', {}, cells.name), cells.pipeline, cells.finished,
      h('td', {}, addedTags(m)), h('td', { class: 'r' }, can ? removeButton(p, m, write) : null))
  }
  const itemRow = (m) => {
    const from = h('td', { class: 'muted' })
    const name = h('span', {}, shortCid(m.subject))
    ex.itemCollections([m.subject]).then(async (found) => {
      const collection = found.get(m.subject)
      if (!collection) return
      name.replaceChildren(link(`#/item/${collection}/${m.subject}`, shortCid(m.subject)))
      const label = await ex.runLabel(collection)
      if (label) from.replaceChildren(`${label.run_name ?? 'unnamed run'} · ${label.output}`)
    }).catch(() => {})
    return h('tr', { 'data-member': m.subject, 'data-kind': 'item' }, h('td', {}, name), from, h('td', {}, addedTags(m)), h('td', { class: 'r' }, can ? removeButton(p, m, write) : null))
  }
  return h('div', { class: 'stack' },
    panel({ title: 'Runs', meta: scope.meta('whole runs in this project'), flush: true }, runs.length
      ? h('div', { class: 'tbox' }, h('table', { class: 't' }, h('thead', {}, h('tr', {}, ['Run', 'Pipeline', 'Finished', 'Added', ''].map(t => h('th', {}, t)))),
        h('tbody', {}, runs.map(runRow))))
      : h('p', { class: 'muted', style: 'padding:12px 16px' }, 'No runs yet.')),
    items.length ? panel({ title: 'Items', meta: scope.meta('single items of other runs'), flush: true }, h('div', { class: 'tbox' }, h('table', { class: 't' },
      h('thead', {}, h('tr', {}, ['Item', 'From', 'Added', ''].map(t => h('th', {}, t)))), h('tbody', {}, items.map(itemRow))))) : null,
    scope.line('runs'))
}

/**
 * A Selection's pins that the retention policy wrote (their note is
 * policyNote of a name the project has had), through pinsList: each a claim
 * tag with its own Unpin, which names what only that pin keeps here once the
 * snapshot says, counted against every pin on the Selection.
 */
function policyPins(s, { ex, ctx, write, policyNames }, can) {
  const only = new Set(cids((s.state?.pins ?? []).filter(x => policyNames.has(x.note))))
  if (!only.size) return null
  return pinsList(s.state, { can, only, store: ctx.store?.name ?? 'this store', roots: can ? holds(ex, s.subject).roots : null,
    onUnpin: (cid, e) => write(w => w.unpin(s.subject, cid), e.currentTarget) })
}

/** D5: the Selections in `sels` with no current pin here that the policy wrote (under any name the project has had) or the viewer did. */
const notPinned = (sels, policyNames, ctx) => sels.filter(s => !(s.state?.current ?? []).some(c => c.attribute === 'pin' && c.verb === 'add'
  && (policyNames.has(String(c.value)) || samePrincipal(c.asserted_by, ctx.viewer))))

function selectionsTab(ex, p, ctx, { sels, can, retention, pins, unpinned, scope }) {
  const confirm = h('div', { 'data-retention-confirm': '', hidden: true })
  const d5 = retention && unpinned.length ? h('div', { class: 'note warn', 'data-not-pinned': String(unpinned.length) },
    h('span', {}, `${plural(unpinned.length, 'Selection')} not pinned by policy`, can ? [' · ', button('Pin', { size: 'sm', kind: 'ghost', 'data-pin-unpinned': '',
      onclick: () => { confirm.replaceChildren(retentionConfirm(unpinned, pins)); show(confirm, true) } })] : null)) : null
  const row = (s) => h('tr', { 'data-member': s.subject, 'data-kind': 'selection' },
    h('td', {}, h('span', { class: 'namecell' }, sealed('selection', s.subject, { size: 20 }), link(`#/selection/${s.subject}`, s.name))),
    h('td', {}, policyPins(s, pins, can) ?? h('span', { class: 'muted' }, s.state?.pinned ? 'pinned' : '—')),
    h('td', {}, addedTags(s)), h('td', { class: 'r' }, can ? removeButton(p, s, pins.write) : null))
  return h('div', { class: 'stack' }, d5, confirm,
    panel({ title: 'Selections', meta: scope.meta(retention ? 'this project pins every Selection' : null), flush: true }, sels.length
      ? h('div', { class: 'tbox' }, h('table', { class: 't' }, h('thead', {}, h('tr', {}, ['Selection', 'Policy pins', 'Added', ''].map(t => h('th', {}, t)))),
        h('tbody', {}, sels.map(row))))
      : h('p', { class: 'muted', style: 'padding:12px 16px' }, 'No Selections yet.')),
    scope.line('selections'))
}

// ---- Activity ------------------------------------------------------------------------

/**
 * Every event of the project, newest first: `[{ key, at, who, verb, text }]`,
 * a Claim on the project with `value` (projectClaimWords') for `text`.
 * Its creation and the Claims on it; its members' membership Claims, added
 * and removed (projectHistory); the pins its policy wrote; its hand-offs
 * (and how many could not be read); and the Transfers that carried a member
 * run or Selection.
 */
export async function activityEvents(ex, p, ctx, { members, names, sels }) {
  const events = []
  if (p.createdBy || p.createdAt) events.push({ key: `created ${p.cid}`, at: p.createdAt, who: p.createdBy, verb: 'created', text: p.blockName ?? '' })
  const byCid = new Map((p.claims ?? []).map(c => [c.cid, c]))
  for (const c of p.claims ?? []) {
    const old = (c.supersedes ?? []).map(s => byCid.get(s)).find(Boolean)
    const { verb, value } = projectClaimWords(c, old?.value ?? null)
    events.push({ key: c.cid, at: c.timestamp, who: c.asserted_by, verb, value })
  }
  const history = await ex.projectHistory(p.cid)
  const unnamed = history.filter(r => !names.has(r.subject))
  const more = await memberNames(ex, unnamed.map(r => ({ subject: r.subject, kind: r.kind })), await ex.claimStates(unnamed.filter(r => r.kind === 'selection').map(r => r.subject)))
  const nameOf = (s) => names.get(s) ?? more.get(s) ?? shortCid(s)
  for (const { subject, claim: c } of history)
    events.push({ key: c.cid, at: c.timestamp, who: c.asserted_by, verb: c.verb === 'add' ? 'added' : 'removed', text: nameOf(subject) })
  const policyNames = policyNotes(p)
  for (const s of sels) {
    for (const c of (s.state?.claims ?? []).filter(c => c.attribute === 'pin' && c.verb === 'add' && policyNames.has(String(c.value))))
      events.push({ key: c.cid, at: c.timestamp, who: c.asserted_by, verb: 'pinned', text: `${s.name} in ${ctx.store?.name ?? 'this store'} · policy` })
  }
  for (const x of await ex.handoffs({ project: p.cid }))
    events.push({ key: x.cid, at: x.at, who: x.assertedBy, verb: 'handed off', text: `${nameOf(x.selection)} to ${x.template}` })
  const roots = members.filter(m => m.kind === 'run' || m.kind === 'selection')
  const transfers = await Promise.all(roots.map(m => ex.transfersOf(m.subject).then(ts => ts.map(t => ({ t, m })), () => [])))
  const seen = new Set()
  for (const { t, m } of transfers.flat()) {
    if (seen.has(t.cid)) continue
    seen.add(t.cid)
    events.push({ key: t.cid, at: t.at, who: t.assertedBy, verb: 'copied', text: `${nameOf(m.subject)} from ${t.from} to ${t.to}` })
  }
  const sorted = events.sort(newestFirst)
  return Object.assign(sorted, { unreadable: ex.unreadableHandoffs().length })
}

function activityPanel(events, ex, { limit = Infinity, more = null, all = false, scope }) {
  const now = ex.now?.() ?? Date.now()
  const shown = events.slice(0, limit)
  const rows = shown.map(e => h('div', { class: 'row activity-row', 'data-activity': e.key, 'data-at': e.at ?? '' },
    ago(e.at, now, { class: 'mono muted activity-age' }) ?? h('span', { class: 'mono muted activity-age' }),
    claimTag({ assertedBy: e.who, verb: e.verb, value: e.value ?? (e.text ? h('span', {}, e.text) : null) })))
  return panel({ title: 'Activity', meta: scope.meta(all ? 'newest first' : null) },
    rows.length ? rows : h('p', { class: 'muted' }, 'Nothing yet.'),
    events.length > shown.length && more ? h('p', { class: 'small' }, link(more, `All ${events.length} events`)) : null,
    events.unreadable ? h('p', { class: 'muted small', 'data-handoffs-unread': String(events.unreadable) },
      `${plural(events.unreadable, 'hand-off')} in this store could not be read, so ${events.unreadable === 1 ? 'it is' : 'they are'} not listed here; ${events.unreadable === 1 ? 'it' : 'they'} may belong to this project.`) : null,
    scope.line('activity'))
}

