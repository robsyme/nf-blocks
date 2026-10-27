// web/src/views/copy.js
// #/selection/<cid>/copy and #/run/<cid>/copy (mockups/11-copy.html, P3
// interface 1.3 and 1.7): copy a Selection or a run from the open member to
// another, keeping provenance. The page shows the plugin's dry run (what
// travels at each depth, what is already at the destination, the claims that
// go with it, the same command for the CLI), re-asked once the choices settle,
// and copies only on a button that names the blocks it moves. Run records
// always travel. A copy whose root or run the destination released or
// deleted is held back (D9). After a copy it offers to release the root here,
// with the bytes that frees in this store, when the destination keeps it.
import { h, link } from '../html.js'
import { seal } from '../seal.js'
import { addrChip, button, claimTag, initials, kv, note, pageHead, panel, shortCid } from '../components.js'
import { presenceOf } from '../presence.js'
import { bytes, formatNumber, sizeText } from '../format.js'
import { isAnon, whoText } from '../identity.js'
import { dayText } from '../words.js'
import { hrefIn } from '../members.js'
import { CATEGORIES, DEFAULT_CLAIMS, DEPTHS, cantCopyText, copyAvailability, copyHref } from '../copy.js'
import { claimVerb, enc, errorNode, isProjectClaim, projectClaimWords } from './common.js'
import { defaultStoreNote } from './addproject.js'

const plural = (n, one, many = `${one}s`) => `${formatNumber(n)} ${n === 1 ? one : many}`
// "Projects": the category carries memberships, project policies and the Project blocks themselves.
const LABELS = { names: 'Names', annotations: 'Annotations', projects: 'Projects', pins: 'Pins' }
// The claims panel's order (mockup 11): names, annotations, projects, then pins.
const SHOWN = ['names', 'annotations', 'projects', 'pins']
const NO_GROUP = { blocks: 0, bytes: 0, present: 0 }
const RUNS_SHOWN = 3

/** How long the page waits for the choices to settle before it asks for a dry run: each one costs the destination a `has` per block. */
export const REPLAN_DELAY_MS = 300
// A dry run another page's newer one replaced (D10) is asked again, this many times at most.
const SUPERSEDED_RETRIES = 5

/**
 * "Copy to…" for a Selection's or a run's head: a link to the copy page when
 * this page can copy (ctx.copy, copyAvailability), else a disabled button
 * whose title says why. Needs no request either way. `to` preselects a
 * destination; `project` is the project whose default store the copy page
 * explains, or a list of projects whose default stores disagree (P4 plan
 * 1.7), each encoded and the list comma-separated.
 */
export function copyToButton(ctx, kind, cid, { primary = false, to = null, project = null } = {}) {
  const c = ctx?.copy ?? copyAvailability(null)
  const projects = project ? [].concat(project).map(encodeURIComponent).join(',') : null
  const q = [to ? `to=${encodeURIComponent(to)}` : null, projects ? `project=${projects}` : null].filter(Boolean).join('&')
  if (c.available) return h('a', { class: primary ? 'btn primary' : 'btn', href: `#/${kind}/${cid}/copy${q ? `?${q}` : ''}`, 'data-copy-to': cid }, 'Copy to…')
  return h('span', { title: c.reason, 'data-copy-to': 'unavailable' },
    button('Copy to…', { disabled: true, 'aria-label': `Copy to… is unavailable: ${c.reason}` }))
}

/** A fact in the chain row: optional Seal, kind label and value; `dashed` for what stays a reference. */
function fact(kind, value, { cid = null, dashed = false, title = null, attr = null, ring = null } = {}) {
  return h('span', { class: dashed ? 'fact dashed' : 'fact', title, ...(attr ? { [`data-chain-${attr}`]: '' } : {}) },
    cid ? seal(cid, { size: 16 }) : null,
    ring ? h('span', { class: 'presence' }, h('span', { class: `dot ring ${ring}` })) : null,
    h('span', { class: 'k' }, kind), value)
}

/** The lineage rule each run's lineage depth used (interface 1.3 runs[].lineage), in words; null below lineage depth. */
export function lineageRuleText(runs, source) {
  const rules = new Set((runs ?? []).map(r => r.lineage).filter(Boolean))
  if (rules.size === 0) return null
  if (rules.size > 1) return `from task lineage where ${source} has it, else same samples`
  return rules.has('tasks') ? 'from task lineage' : `same samples (no task lineage in ${source})`
}

/**
 * The claims a category shows as tags: those 1.3 marks `current`, else those
 * no other listed claim supersedes (`supersedes`), else all of them.
 */
export function currentClaims(claims) {
  if (claims.some(c => typeof c.current === 'boolean')) return claims.filter(c => c.current !== false)
  const replaced = new Set(claims.flatMap(c => c.supersedes ?? []))
  return claims.filter(c => !replaced.has(c.cid))
}

/** A failed request's note: the server stopping is plain muted text, anything else its error with the server's words. */
const failureNode = (e) => e?.code === 'stopping'
  ? h('p', { class: 'muted', 'data-stopping': '' }, "The explore server is stopping; open the page again once it's back.")
  : errorNode(e)

/** The roots and runs the destination released or deleted (1.3 destination_state, D9). */
export const notKeptOf = (plan) => (plan?.destination_state ?? []).filter(d => d.state === 'released' || d.state === 'deleted')

/**
 * A claim as its tag says it (verb and value): a name or pin as every page
 * says it, a P4 Claim (a membership, an annotation, a policy) as the project
 * page says it, another attribute as `attribute = "value"`.
 */
export function claimWords(c) {
  if (isProjectClaim(c)) return projectClaimWords(c)
  if (c.category === 'annotations' || (c.attribute && !['name', 'pin', 'retain'].includes(c.attribute))) {
    return c.verb === 'del' ? { verb: `unset ${c.attribute}`, value: null } : { verb: `${c.attribute} =`, value: c.value ?? null }
  }
  return { verb: claimVerb(c), value: c.value ?? null }
}

/** The plan panel's note on provenance (mockup 11): ok when every run's records are at the destination or travel with this copy. */
function provenanceNote(plan, to, source) {
  const runs = plan.runs ?? []
  const records = runs.length > 1 ? 'the completions and manifests' : 'the completion and manifest'
  if (runs.length && runs.every(r => r.present)) {
    return note(`Provenance is complete at the destination: ${records} are already there, so lineage resolves in ${to} on its own.`, 'ok')
  }
  if (runs.length && plan.missing_at_source === 0) {
    return note(`Provenance is complete at the destination: ${records} travel with these results, so lineage resolves in ${to} on its own.`, 'ok')
  }
  if (!plan.missing_at_source) return note(`This plan names no run records, so ${to} has no lineage for it.`, 'warn')
  return note(`${plural(plan.missing_at_source, 'block')} in this plan ${plan.missing_at_source === 1 ? 'is' : 'are'} not in ${source}, so ${plan.missing_at_source === 1 ? 'it stays a reference' : 'they stay references'}. `
    + `If the run records are among them, lineage won't resolve in ${to} on its own.`, 'warn')
}

/**
 * The copy page. `kind` is 'selection' or 'run'; `to`, `depth`, `claims` and
 * `project` come from the address (parseCopyQuery). With no `to` that can
 * take a copy, the first store that can is chosen, except when a `project`
 * is named (ruling Q1): then no store is, and no dry run starts until the
 * reader picks one. The dry run is asked for at once
 * and again once a change has settled (REPLAN_DELAY_MS); an answer to an
 * older choice, or one the server superseded (D10), is dropped. The section
 * carries data-plan-state (loading | ready | error) and data-copy-state
 * (idle | running | copied | failed | refused).
 *
 * A copy is synchronous and can take minutes, so it belongs to the app, not
 * the page: `ctx.copy.running` is the one in flight (`{ root, to, section,
 * done }`) and `ctx.copy.last` the last that ended (`{ root, to, result,
 * failure }`). Another copy page meanwhile says a copy is running and holds
 * its buttons; when it ends that page redraws, and a page for the same root
 * shows the result. Only the page that started it, while still shown, moves
 * on to the release confirmation.
 */
export async function copyView(ex, kind, cid, ctx, { to = null, depth = 'results', claims = DEFAULT_CLAIMS, project = null } = {}) {
  const copy = ctx.copy ?? copyAvailability(null)
  // The app's copy state (ctx.copy outlives every page); a page given none has its own.
  const jobs = copy
  const rootRef = `${kind}:${cid}`
  const client = copy.available ? copy.client ?? null : null
  const list = ctx.members?.list ?? []
  const source = ctx.members?.primary ?? ctx.store?.name ?? null
  const sourceName = ctx.store?.name ?? source ?? 'this store'
  const others = list.filter(m => m.alias !== source)
  const targets = others.filter(m => m.copy_target)
  const memberOf = (alias) => list.find(m => m.alias === alias) ?? null
  // Ruling Q1: a Copy to… from a project whose default store can't take a copy here names the project and no store,
  // and one from projects whose default stores disagree names them all. Then nothing is preselected and no dry run
  // starts until the reader picks a store.
  const named = targets.find(m => m.alias === to)?.alias ?? null
  const state = {
    to: named ?? (project ? null : targets[0]?.alias ?? null),
    depth: DEPTHS.includes(depth) ? depth : 'results',
    claims: new Set(claims.filter(c => CATEGORIES.includes(c))),
    accept: false, plan: null, error: null,
    // The last dry run's command (a refused copy's body names the copy, not the dry run).
    command: null,
    // Right after a copy that failed part way: "Copy again" even when the plan has nothing left to transfer.
    retry: false,
  }
  // The lineage rule the last lineage-depth plan reported: it depends on the source alone, so it stays on the card after going back to results.
  let lineageRule = null
  const backHref = `#/${kind}/${cid}`
  const head = await headOf(ex, kind, cid, backHref)

  const section = h('section', { class: 'copy-page', 'data-copy-view': cid, 'data-copy-kind': kind, 'data-plan-state': 'idle', 'data-copy-state': 'idle' })
  // Whether this page is still the one shown: not once the app rendered another (Explorer.pageChanged), nor once it was shown and then removed.
  const pageToken = ex.page ?? null
  let shown = false
  const here = () => {
    if (section.isConnected) shown = true
    return !pageToken?.gone && (!shown || section.isConnected)
  }
  // This page's own copy is running: its choices stay as they were sent.
  const own = () => !!jobs.running && jobs.running.section === section
  const dest = destSwitch()
  const status = h('div', { 'data-copy-status': '' })
  const plan = () => state.plan?.plan ?? null
  // No store chosen yet while some could take the copy (ruling Q1): the page waits for the reader.
  const choosing = () => !state.to && targets.length > 0 && !!client
  const group = (name) => plan()?.groups?.find(g => g.group === name) ?? NO_GROUP
  const runName = () => {
    const runs = plan()?.runs ?? []
    return runs.length === 1 ? runs[0].run_name ?? 'the run' : runs.length ? 'the producing runs' : head.runName ?? 'the run'
  }

  // ---- What travels -------------------------------------------------------
  const cards = DEPTHS.map((d) => {
    const radio = h('input', { type: 'radio', name: 'copy-depth', value: d, 'data-depth': d,
      onchange: (e) => { if (e.currentTarget.checked && state.depth !== d) choose(() => { state.depth = d }) } })
    const title = h('b', {})
    const desc = h('span', { class: 'muted small' })
    const rule = h('span', { class: 'muted small', 'data-lineage-rule': '', hidden: true })
    const figures = h('span', { 'data-depth-figures': d }, '…')
    // What this depth would transfer: about the destination, so it names it (depths[].transfer, transfer_bytes).
    const transfer = h('span', { class: 'muted depth-transfer', 'data-depth-transfer': d })
    const card = h('label', { class: 'panel depth-card', 'data-depth-card': d }, radio, h('span', { class: 'depth-text' }, title, desc, rule),
      h('span', { class: 'mono figures' }, figures, transfer))
    return { d, radio, title, desc, rule, figures, transfer, card }
  })
  const chain = h('div', { class: 'row chain', 'data-copy-chain': '' })
  const travels = panel({ title: 'What travels' },
    h('div', { class: 'depths', role: 'radiogroup', 'aria-label': 'Depth' }, cards.map(c => c.card)),
    chain,
    h('p', { class: 'muted small', 'data-run-records-note': '' },
      "Run records always travel at every depth: the completion, the manifest with its params and config, and the script. They can't be left out, or the copy would lose its provenance."))

  // ---- Plan -----------------------------------------------------------------
  const cell = (key, eyebrow) => {
    const num = h('div', { class: 'bignum' }, '…')
    const sub = h('div', { class: 'muted small' })
    const eye = h('div', { class: 'eyebrow' }, eyebrow)
    return { key, num, sub, eye, el: h('div', { [`data-plan-${key}`]: '' }, eye, num, sub) }
  }
  const cells = { blocks: cell('blocks', 'In plan'), present: cell('present', 'Already there'), transfer: cell('transfer', 'To transfer'), upload: cell('upload', 'To upload') }
  const planNotes = h('div', { class: 'plan-notes' })
  const planPanel = panel({ title: 'Plan', meta: 'dry run · nothing has moved yet' },
    h('div', { class: 'grid-3 plan-cells' }, Object.values(cells).map(c => c.el)), planNotes)

  // ---- Claims, CLI, buttons -----------------------------------------------
  const claimsBody = h('div', { class: 'claims-carry' })
  const claimsPanel = panel({ title: 'Claims to carry' }, claimsBody)
  const command = h('pre', { class: 'code', 'data-copy-command': '' }, '…')
  const cliPanel = panel({ title: 'Same thing from the CLI' }, command)
  const accept = h('input', { type: 'checkbox', 'data-accept-audit': '', onchange: (e) => { state.accept = !!e.currentTarget.checked; drawButtons() } })
  // Drawn into acceptSlot only while the plan has findings: `hidden` alone loses to the kit's .check { display: grid }.
  const acceptSlot = h('div', { 'data-accept-slot': '' })
  const acceptRow = h('label', { class: 'check', 'data-accept-row': '' }, accept,
    h('span', {}, 'Copy anyway: I have read the audit findings, and these run records may travel as they are.'))
  // Releasing here writes a Claim beside the source: only when this page writes to it, and it is not read-only.
  const sourceMember = memberOf(source)
  const sourceReadOnly = sourceMember?.copy_target === false
  const canRelease = !!(ctx.write?.available && ctx.write?.here) && !sourceReadOnly
  const readOnlyWhy = sourceReadOnly && sourceMember.copy_reason ? ` (${sourceMember.copy_reason})` : ''
  const releaseReason = canRelease ? null : sourceReadOnly ? `This page can't write to ${sourceName}${readOnlyWhy}.`
    : ctx.write?.available ? `This page can't write to ${sourceName}.` : (ctx.write?.reason ?? `This page can't write to ${sourceName}.`)
  // Why "Copy, then release" is not on the page, in one muted line (the button is left out, not disabled).
  const releaseUnavailable = canRelease ? null : h('p', { class: 'muted small', 'data-release-unavailable': '', title: ctx.write?.available ? null : ctx.write?.reason ?? null },
    "Copy, then release isn't offered: ", sourceReadOnly ? `this page can't write to ${sourceName}${readOnlyWhy}.`
      : ctx.write?.available && ctx.write?.writable ? `this explore server writes to ${ctx.write.writable}, not ${sourceName}.`
        : `this page can't write to ${sourceName}.`)
  const goRelease = button(`Copy, then release in ${sourceName}…`, { 'data-copy-release': '', disabled: true, onclick: (e) => go(true, e.currentTarget) })
  // Holds goRelease only while the plan does not say the destination would not keep the copy (D9).
  const releaseSlot = h('span', { 'data-release-slot': '', style: 'display:contents' }, goRelease)
  const goCopy = button('Copy', { kind: 'primary', 'data-copy-go': '', disabled: true, onclick: (e) => go(false, e.currentTarget) })
  const actions = h('div', { class: 'row copy-actions' }, link(backHref, 'Cancel'), h('span', { class: 'spacer' }), releaseSlot, goCopy)
  actions.querySelector('a').setAttribute('class', 'btn')
  actions.querySelector('a').setAttribute('data-copy-cancel', '')

  // Element.append writes null as the text "null": only nodes go in.
  section.append(...[
    pageHead({ crumbs: [...head.crumbs, { text: 'Copy' }], seal: cid, kind: kind === 'run' ? 'run' : 'selection', title: `Copy ${head.name}`,
      after: [h('span', { class: 'muted' }, 'from'),
        h('span', { class: 'fact', 'data-copy-from': source ?? '' }, h('span', { class: `dot ${ctx.store?.key ?? 'a'}` }), sourceName),
        h('span', { class: 'muted' }, 'to'), dest.el] }),
    copy.available ? null : h('div', { class: 'note warn', 'data-copy-unavailable': '' }, h('span', {}, copy.reason)),
    project ? defaultStoreNote(ex, ctx, project) : null,
    h('div', { class: 'grid-2' },
      h('div', { class: 'stack' }, travels, planPanel),
      h('div', { class: 'stack' }, ...[claimsPanel, cliPanel, acceptSlot, actions, releaseUnavailable, status].filter(Boolean)))].filter(Boolean))

  // ---- drawing --------------------------------------------------------------
  function drawDepths() {
    const p = state.plan
    const items = group('items').blocks
    const files = group('files').blocks
    for (const c of cards) {
      c.radio.checked = c.d === state.depth
      if (c.radio.checked) c.radio.setAttribute('checked', ''); else c.radio.removeAttribute('checked')
      c.radio.disabled = own() || !client
      c.card.classList[c.d === state.depth ? 'add' : 'remove']('on')
      const totals = p?.depths?.[c.d]
      c.figures.replaceChildren(...(totals ? [plural(totals.blocks, 'block'), h('br'), bytes(totals.bytes)] : ['…']))
      const n = totals?.transfer
      c.transfer.textContent = typeof n !== 'number' || !state.to ? ''
        : n === 0 ? `Nothing to transfer to ${state.to}` : `${formatNumber(n)} to transfer to ${state.to} · ${bytes(totals.transfer_bytes ?? 0)}`
    }
    const [results, lineage, all] = cards
    results.title.textContent = 'Results + run records'
    results.desc.textContent = p ? `${plural(items, 'item')}, ${plural(files, 'file')}, and the completion + manifest (params, config, script) that produced them`
      : 'The items, their files, and the completion + manifest (params, config, script) that produced them'
    lineage.title.textContent = '+ upstream lineage'
    lineage.desc.textContent = 'Also the upstream outputs these results were made from'
    lineageRule = lineageRuleText(plan()?.runs, sourceName) ?? lineageRule
    const rule = lineageRule
    lineage.rule.textContent = rule ?? ''
    if (rule) lineage.rule.removeAttribute('hidden'); else lineage.rule.setAttribute('hidden', '')
    all.title.textContent = `Everything ${runName()} stored`
    all.desc.textContent = 'Every output, item and file, and the inputs when this store holds them'
  }

  function drawChain() {
    const p = plan()
    if (!p) { chain.replaceChildren(h('span', { class: 'muted' }, state.error ? 'No plan.' : choosing() ? 'Choose a store to copy to.' : 'Asking for the plan...')); return }
    const sep = (t) => h('span', { class: 'muted' }, t)
    const runs = p.runs ?? []
    const rootRun = kind === 'run' ? runs.find(r => r.completion === cid) : null
    const parts = [fact(kind === 'run' ? 'run' : 'selection', kind === 'run' ? (rootRun?.run_name ?? head.name) : '1', { cid, attr: 'root' }),
      sep('→'), fact('items', formatNumber(group('items').blocks), { attr: 'items' }),
      sep('→'), fact('files', formatNumber(group('files').blocks), { attr: 'files' }), sep('+')]
    for (const r of runs.slice(0, RUNS_SHOWN)) {
      if (r.completion !== cid) parts.push(fact('completion', r.run_name ?? 'unnamed run', { cid: r.completion, attr: 'completion' }))
      parts.push(fact('manifest', '+ config', { cid: r.manifest, attr: 'manifest' }))
    }
    if (runs.length > RUNS_SHOWN) parts.push(h('span', { class: 'muted' }, `and ${plural(runs.length - RUNS_SHOWN, 'more run')}`))
    const records = group('run_records').blocks
    parts.push(fact('run records', plural(records, 'record'), { attr: 'run-records',
      title: 'The completion, manifest and script, and every output collection and item record of each run: records only, no files.' }))
    if (group('lineage').blocks) parts.push(fact('upstream', plural(group('lineage').blocks, 'block'), { attr: 'lineage' }))
    if (group('run_outputs').blocks) parts.push(fact('run outputs', plural(group('run_outputs').blocks, 'block'), { attr: 'run-outputs' }))
    if (group('inputs').blocks) parts.push(fact('inputs', plural(group('inputs').blocks, 'block'), { attr: 'inputs' }))
    // The Handoffs citing a carried Selection travel with their files, so a hand-off command's cas:// addresses resolve there too.
    const handoffs = group('handoffs')
    if (handoffs.blocks) {
      parts.push(fact('hand-offs', plural(handoffs.blocks, 'block'), { attr: 'handoffs',
        title: `The hand-offs that cite a Selection this copy carries, with their files: ${bytes(handoffs.bytes ?? 0)}.` }))
    }
    const references = (p.unaddressed ?? 0) + (p.missing_at_source ?? 0)
    if (references) {
      parts.push(fact('inputs', `${formatNumber(references)} · stay as references`, { dashed: true, ring: ctx.store?.key ?? 'a', attr: 'references',
        title: `${plural(p.unaddressed ?? 0, 'file')} referenced with no address (never published, such as input FASTQs), `
          + `and ${plural(p.missing_at_source ?? 0, 'block')} not in ${sourceName}. They keep their references and are not copied.` }))
    }
    chain.replaceChildren(...parts)
  }

  function drawPlan() {
    const p = plan()
    const toName = state.to ?? 'the destination'
    cells.present.eye.textContent = `Already in ${toName}`
    if (!p) {
      for (const c of Object.values(cells)) { c.num.textContent = '…'; c.sub.textContent = '' }
      // A dry run the server kept superseding (D10) is not an error: it says so plainly.
      planNotes.replaceChildren(state.error?.code === 'superseded' ? h('p', { class: 'muted', 'data-plan-superseded': '' }, state.error.message)
        : state.error ? failureNode(state.error) : h('p', { class: 'muted' }, choosing() ? 'Choose a store to copy to.' : client ? 'Asking for the plan...' : ''))
      return
    }
    const presentGroups = (p.groups ?? []).filter(g => g.present > 0)
    cells.blocks.num.textContent = formatNumber(p.blocks)
    cells.blocks.sub.textContent = p.blocks === 1 ? 'block' : 'blocks'
    cells.present.num.textContent = formatNumber(p.present)
    cells.present.sub.textContent = presentGroups.length && presentGroups.every(g => g.group === 'run_records') ? 'run records' : p.present === 1 ? 'block' : 'blocks'
    cells.transfer.num.textContent = formatNumber(p.transfer)
    cells.transfer.sub.textContent = p.transfer === 1 ? 'block' : 'blocks'
    cells.upload.num.textContent = sizeText({ bytes: p.transfer_bytes, partial: p.unsized > 0 })
    cells.upload.num.dataset.bytes = String(p.transfer_bytes)
    cells.upload.sub.textContent = 'verified on write'
    const findings = p.audit?.findings ?? []
    const unsized = p.unsized > 0 ? note(`${plural(p.unsized, 'block')} could not be sized, so the bytes are at least the figure shown.`, 'warn') : null
    unsized?.setAttribute('data-unsized', String(p.unsized))
    // A copy the destination would not keep (D9) is held back: no green "provenance is complete" beside that.
    const provenance = provenanceNote(p, toName, sourceName)
    const shownProvenance = p.kept === false && provenance.classList.contains('ok') ? null : provenance
    shownProvenance?.setAttribute('data-provenance', '')
    planNotes.replaceChildren(...[p.kept === false ? keptNote(p, toName) : null, shownProvenance, unsized,
      findings.length ? auditNote(p.audit, findings) : null].filter(Boolean))
  }

  /** A root's or run's name for the not-kept note: its own, else this page's root's, else its short address. */
  const stateName = (d) => d.name ?? (d.cid === cid ? head.name : `${d.role === 'run' ? 'run' : 'selection'} ${shortCid(d.cid)}`)
  const namesOf = (entries) => entries.map(stateName).join(', ')

  /**
   * D9: the destination released or deleted a root or run of this copy, so its
   * next sweep would take what the copy adds. One line per root or run, each
   * linking its page in the destination. Restoring writes a Claim to this
   * explore server's writable member: when that is the destination, its page
   * can restore; otherwise the note names the way that can (wayOut).
   */
  function keptNote(p, toName) {
    const entries = notKeptOf(p)
    // Its page can restore only when the destination is the writable member and this page can write (it has the token).
    const writesTo = ctx.write?.writable != null && ctx.write.writable === toName
    const there = writesTo && !!ctx.write?.available
    const restoreThere = there ? ' Restore it there.' : ''
    const lines = entries.length
      ? entries.map(d => h('p', { 'data-not-kept-entry': d.cid },
        link(hrefIn(ctx, toName, d.role === 'run' ? `#/run/${d.cid}` : `#/${d.cid === cid ? kind : 'selection'}/${d.cid}`), stateName(d)),
        ` is ${d.state} in ${toName}, so its next sweep would take what this copy adds.${restoreThere}`))
      : [h('p', {}, `${toName} has released or deleted something this copy adds to, so its next sweep would take it.${restoreThere}`)]
    return h('div', { class: 'note warn', 'data-not-kept': String(entries.length) },
      h('div', {}, lines, there ? null : writesTo ? tokenWayOut(entries, toName) : wayOut(entries, toName)))
  }

  /** Whether a destination_state entry is a run: a run of a Selection, or this page's own run root. */
  const isRun = (d) => d.role === 'run' || (d.cid === cid && kind === 'run')

  /** What restores one entry on the destination's own page: a released run's Storage tab, a deleted Selection's Undo delete. */
  function restoreStep(d) {
    if (d.state === 'released' && isRun(d)) return 'Restore the run on its Storage tab'
    if (d.state === 'deleted' && !isRun(d)) return "Undo delete on the Selection's page"
    return `restore the ${isRun(d) ? 'run' : 'Selection'} there`
  }

  /** What restores the entries, in one clause: each step on the destination's page, else "restore it there". */
  function stepsText(entries) {
    const steps = [...new Set(entries.map(restoreStep))]
    return steps.length ? steps.join(steps.length === 2 ? ' and ' : ', ') : `restore ${entries.length > 1 ? 'them' : 'it'} there`
  }

  /** The way out when the destination is the writable member but this page can't write (no token): open the page with its token. */
  function tokenWayOut(entries, toName) {
    return h('div', { 'data-not-kept-way-out': 'token' },
      h('p', {}, `This page can't write to ${toName}: ${ctx.write?.reason ?? 'it needs the write token.'} `
        + `Then ${stepsText(entries)}, and copy again.`))
  }

  /**
   * The way out when this server writes elsewhere: open the destination as the
   * writable store (nf-blocks:explore with lineage.store.location naming it, in
   * a config file of its own named by NXF_CONFIG_FILE; -c does not reach plugin
   * verbs on Nextflow 26.08+), then restore on its page. NXF_CONFIG_FILE
   * replaces ./nextflow.config, so that file also includes nextflow.config (or
   * holds the cas.stores block), or the explorer has no stores; includeConfig
   * resolves beside the including file, so it goes beside nextflow.config. The lineage
   * line must stay out of the pipelines' config, or they would write into the
   * destination. CopyCommand's not_kept message names the same route. A
   * restore Claim from the CLI (nf-blocks:put) would need the release or
   * deletion Claims it supersedes, which the plan does not carry, so the note
   * does not offer it.
   */
  function wayOut(entries, toName) {
    const writes = ctx.write?.writable
    const lead = writes ? `This page writes to ${writes}, so it can't restore anything in ${toName}. ` : `This page can't write to ${toName}. `
    const them = entries.length > 1 ? 'them' : 'it'
    return h('div', { 'data-not-kept-way-out': 'explore' },
      h('p', {}, lead, `To restore ${them}, open ${toName} as the writable store: make a config file beside your nextflow.config holding these two lines `
        + '(or your cas.stores block in place of the includeConfig line), and start ',
        h('code', {}, 'nextflow plugin nf-blocks:explore'), ' with ', h('code', {}, 'NXF_CONFIG_FILE=<that file>'), '.'),
      h('pre', { class: 'code', 'data-not-kept-config': '' }, `includeConfig 'nextflow.config'\nlineage.store.location = 'cas://${toName}'`),
      h('p', { 'data-not-kept-pipelines': '' }, `Don't put the lineage line in the config your pipelines use: they would start writing into ${toName}.`),
      h('p', {}, `Then ${stepsText(entries)}, and copy again.`))
  }

  /** Why Copy is held back when the destination would not keep the copy. */
  function keptReason(p, toName) {
    const entries = notKeptOf(p)
    return entries.length
      ? `${toName} has released or deleted ${namesOf(entries)}: restore ${entries.length === 1 ? 'it' : 'them'} in ${toName} first.`
      : `${toName} would not keep this copy: restore what it released or deleted first.`
  }

  function auditNote(audit, findings) {
    return h('div', { class: 'note warn', 'data-audit-findings': String(findings.length) },
      h('div', {},
        h('p', {}, `The audit read ${plural(audit.checked ?? 0, 'run manifest')} and found ${plural(findings.length, 'thing')} that may not belong in a shared store:`),
        h('ul', { class: 'findings' }, findings.map(f => h('li', { 'data-finding': f.cid },
          addrChip(f.cid), ' ', h('code', {}, f.where), ` ${f.what}`, f.kind ? h('span', { class: 'muted' }, ` (${f.kind})`) : null))),
        h('p', {}, 'Run records always travel, so these would go too. Tick "Copy anyway" to copy them as they are.')))
  }

  function drawClaims() {
    const p = plan()
    const toName = state.to ?? 'the destination'
    const listed = p?.claims ?? []
    const rows = SHOWN.map((cat) => {
      const ticked = state.claims.has(cat)
      const all = listed.filter(c => c.category === cat)
      const current = currentClaims(all)
      const earlier = all.length - current.length
      const box = h('input', { type: 'checkbox', 'data-claim-category': cat,
        onchange: (e) => { const on = e.currentTarget.checked; choose(() => { if (on) state.claims.add(cat); else state.claims.delete(cat) }) } })
      box.checked = ticked
      if (ticked) box.setAttribute('checked', '')
      const pinNote = cat === 'pins' ? `Pins are per store; someone with write access confirms them in ${toName}.` : null
      let body
      if (!p) {
        body = h('span', { class: 'muted' }, `${LABELS[cat]} · ${ticked ? '…' : 'not carried'}`)
      } else if (all.length === 0) {
        // Nothing of this kind on these items: nothing to choose.
        box.disabled = true
        body = h('span', { class: 'muted' }, cat === 'pins' ? `Pins · none in ${sourceName}. ${pinNote}` : `${LABELS[cat]} · none on these items`)
      } else {
        body = h('span', { class: 'carry' }, current.map(c => claimRow(c, toName)),
          earlier ? h('span', { class: 'muted small', 'data-claims-history': String(earlier) },
            `+ ${plural(earlier, 'earlier claim')} ${earlier === 1 ? 'it supersedes' : 'they supersede'}`) : null,
          all.some(c => !c.carried) ? h('span', { class: 'muted small', 'data-not-carried': '' }, `${LABELS[cat]} · not carried`) : null,
          pinNote && !ticked ? h('span', { class: 'muted small' }, pinNote) : null)
      }
      if (own() || !client) box.disabled = true
      return h('label', { class: 'check', 'data-claims-row': cat }, box, body)
    })
    // The example is the name a copy carries now: the current name claim, when names travel.
    const named = state.claims.has('names') ? currentClaims(listed.filter(c => c.category === 'names'))
      .filter(c => c.attribute === 'name' && c.verb === 'set' && c.carried !== false)
      .sort((a, b) => (a.timestamp < b.timestamp ? 1 : -1))[0] : null
    const by = named ? (isAnon({ principal: named.asserted_by }) ? 'anonymously' : `by ${initials(named.asserted_by)}`) : null
    const example = named ? `"named ${by} on ${(dayText(named.timestamp, Date.now()) || named.timestamp)}"` : 'who made each one, and when,'
    const skipped = p?.skipped_claims ?? {}
    claimsBody.replaceChildren(...rows,
      h('p', { class: 'muted small', 'data-claims-verbatim': '' }, `Claims are copied as-is, so ${example} stays true in ${toName}.`),
      ...((skipped.retain || skipped.deletion)
        ? [h('p', { class: 'muted small', 'data-claims-kept-here': '' }, `Releases and deletions never travel: they are about what ${sourceName} keeps.`)] : []))
  }

  function claimRow(c, toName) {
    const { verb, value } = claimWords(c)
    const tag = claimTag({ assertedBy: c.asserted_by, verb, value, at: c.timestamp })
    return h('span', { class: c.carried === false ? 'carry-claim muted' : 'carry-claim', 'data-carry-claim': c.cid, 'data-category': c.category,
      'data-carried': String(c.carried !== false) },
      tag, ' ', h('span', { class: 'muted small' }, whoText(c.asserted_by, ctx.viewer)),
      c.present ? h('span', { class: 'muted small' }, ` · already in ${toName}`) : null)
  }

  function drawButtons() {
    const p = plan()
    const toName = state.to ?? 'the destination'
    const findings = p?.audit?.findings ?? []
    if (findings.length) { if (!acceptSlot.contains(acceptRow)) acceptSlot.replaceChildren(acceptRow) } else { acceptSlot.replaceChildren(); state.accept = false; accept.checked = false }
    accept.disabled = own()
    const n = p?.transfer ?? 0
    // A copy that failed part way can leave nothing to transfer and its entries unwritten: copying again finishes it.
    const again = n === 0 && state.retry
    const unkept = p?.kept === false
    goCopy.textContent = !p ? 'Copy' : again ? 'Copy again' : n === 0 ? 'Nothing to copy' : `Copy ${plural(n, 'block')}`
    const blocked = !client || !p || !!jobs.running || (n === 0 && !again) || unkept || (findings.length > 0 && !state.accept)
      || section.dataset.planState !== 'ready'
    goCopy.disabled = blocked
    const why = jobs.running && !own() ? `A copy to ${jobs.running.to} is running; this page can copy when it ends.`
      : unkept ? keptReason(p, toName) : null
    if (why) goCopy.setAttribute('title', why); else goCopy.removeAttribute('title')
    // D9: no "Copy, then release" while the plan says the destination would not keep it; enabled only once it says it would.
    // None at all where the source can't take a release from this page (releaseUnavailable says why).
    if (unkept || !canRelease) releaseSlot.replaceChildren(); else if (!releaseSlot.contains(goRelease)) releaseSlot.replaceChildren(goRelease)
    goRelease.disabled = blocked || !canRelease || p?.kept !== true
    const releaseWhy = releaseReason ?? why ?? (p && p.kept !== true ? `${toName} has not said it keeps this copy.` : null)
    if (releaseWhy) goRelease.setAttribute('title', releaseWhy); else goRelease.removeAttribute('title')
    command.textContent = state.command ? commandText(state.command, findings.length > 0 && state.accept) : (state.error ? '' : '…')
    command.classList[section.dataset.planState === 'loading' ? 'add' : 'remove']('stale')
  }

  /** The dry run's command, with --accept-audit true when "Copy anyway" is ticked, as the copy then sends it. */
  function commandText(text, accepted) {
    if (!accepted || /--accept-audit\b/.test(text)) return text
    return / --dry-run(\s|$)/.test(text) ? text.replace(/ --dry-run(\s|$)/, ' --accept-audit true --dry-run$1') : `${text} --accept-audit true`
  }

  function draw() {
    dest.paint()
    drawDepths()
    drawChain()
    drawPlan()
    drawClaims()
    drawButtons()
  }

  // ---- the dry run --------------------------------------------------------
  // No category ticked is `none`: the server refuses an empty list (bad_claims).
  const chosenClaims = () => {
    const ticked = CATEGORIES.filter(c => state.claims.has(c))
    return ticked.length ? ticked : ['none']
  }
  const request = (real) => ({ root: rootRef, from: source, to: state.to, depth: state.depth,
    claims: chosenClaims(), accept_audit: real ? state.accept : false })

  /** A choice changed: apply it, then ask again once the choices settle. Not while this page's own copy runs. */
  function choose(apply) {
    if (own()) return
    apply()
    state.retry = false
    replanSoon()
  }

  let asked = 0
  let timer = null
  let superseded = 0
  /** Marks the plan as being asked again now, and asks after REPLAN_DELAY_MS; a change meanwhile starts the wait again. */
  function replanSoon() {
    syncHref()
    if (!client || !state.to) { draw(); return }
    // Any answer still in flight is for choices no longer on the page.
    asked++
    section.dataset.planState = 'loading'
    draw()
    clearTimeout(timer)
    timer = setTimeout(() => { timer = null; replan() }, REPLAN_DELAY_MS)
  }

  async function replan() {
    clearTimeout(timer)
    timer = null
    syncHref()
    if (!client || !state.to) { draw(); return }
    if (!here()) return
    const mine = ++asked
    section.dataset.planState = 'loading'
    draw()
    let answer = null
    let failure = null
    try {
      answer = await client.plan(request(false))
    } catch (e) {
      failure = e
    }
    // A later choice asked again: this answer is for choices no longer on the page.
    if (mine !== asked) return
    if (failure?.code === 'superseded') {
      // D10: another page's newer dry run replaced this one while it waited. Not an error: ask again shortly, a few times.
      if (here() && superseded++ < SUPERSEDED_RETRIES) { timer = setTimeout(() => { timer = null; replan() }, REPLAN_DELAY_MS); return }
    }
    superseded = 0
    if (answer) {
      state.plan = answer
      state.command = answer.command ?? null
      state.error = null
    } else {
      state.plan = null
      state.command = null
      state.error = failure?.code === 'superseded'
        ? Object.assign(new Error('Other pages kept asking this server for plans, so this one was not answered. Change a choice to ask again.'), { code: 'superseded' })
        : failure
    }
    section.dataset.planState = answer ? 'ready' : 'error'
    draw()
  }

  function syncHref() {
    const loc = globalThis.location
    const hist = globalThis.history
    // A page no longer shown never rewrites the address of the one that is.
    if (!loc || !hist?.replaceState || !here()) return
    const href = copyHref(kind, cid, { to: state.to, depth: state.depth, claims: [...state.claims], project })
    if (loc.hash !== href) hist.replaceState(hist.state, '', href)
  }

  // ---- the copy -------------------------------------------------------------
  async function go(thenRelease, el) {
    if (jobs.running || !client || !plan()) return
    if (el) el.disabled = true
    const to = state.to
    const n = plan().transfer
    const job = { root: rootRef, to, section, done: null }
    jobs.running = job
    section.dataset.copyState = 'running'
    draw()
    status.replaceChildren(h('p', { class: 'muted', 'data-copy-running': '' },
      n === 0 ? `Copying again to ${to}: it writes what the stopped copy left, then logs a transfer entry.`
        : `Copying ${plural(n, 'block')} (${sizeText({ bytes: plan().transfer_bytes, partial: plan().unsized > 0 })}) to ${to}. `
          + `${to} checks each block against its address as it writes it, then logs a transfer entry.`))
    // The job settles the app's state whatever page is shown by then; done never rejects.
    job.done = client.copy(request(true)).then(result => ({ result, failure: null }), failure => ({ result: null, failure }))
      .then(({ result, failure }) => {
        const last = { root: rootRef, to, result, failure }
        // Settle the page's state first: a listener that throws while forgetting must not leave `running` set or reject `done`.
        jobs.running = null
        jobs.last = last
        if (wrote(last)) {
          try { forget(to, result?.index) } catch (e) { console.error('copy: could not refresh the app after the copy', e) }
        }
        return last
      })
    const last = await job.done
    const offer = showOutcome(last)
    // Only the page that asked, while it is still the one shown, moves on to the release confirmation.
    if (offer && thenRelease && canRelease && here()) {
      if (kind === 'run' && globalThis.location) globalThis.location.hash = offer.href
      else offer.el.querySelector('a')?.focus?.()
    }
  }

  /** Whether a copy may have written to its destination: a result, a 1.3 body with a failure, a 500, or a lost connection. Not a refusal. */
  function wrote({ result, failure }) {
    if (result) return !result.refused
    return !!failure?.body || failure?.status === 500 || failure?.status === 0
  }

  /**
   * A copy that ended, on this page: the result, what failed, or why nothing
   * was copied. Asks for the plan again when the copy wrote anything, unless
   * `ask` is false (the page is opening and asks anyway). Returns the release
   * offer when there is one.
   */
  function showOutcome(last, { ask = true } = {}) {
    const { to, result, failure } = last
    if (result?.refused) {
      // The refused body's plan: its audit findings, or what the destination does not keep (D9). The command stays the dry run's.
      state.plan = result
      state.retry = false
      section.dataset.copyState = 'refused'
      status.replaceChildren(refusedNote(result, to))
      draw()
      return null
    }
    const body = result ?? failure?.body ?? null
    const failures = body?.failures ?? []
    const failed = !result || failures.length > 0
    section.dataset.copyState = failed ? 'failed' : 'copied'
    if (failed) {
      state.retry = wrote(last)
      // The result panel lists the failed blocks; the error says it again only when no panel lists them.
      const lead = failures.length ? null
        : failure?.status === 0 ? lostNote(to, failure)
          : failure?.status === 500 && !body ? stoppedNote(to, failure)
            : failure ? failureNode(failure) : null
      status.replaceChildren(...[lead, body ? resultPanel(body, to, true) : null].filter(Boolean))
      if (state.retry && ask) replan(); else draw()
      return null
    }
    state.retry = false
    // D9: release here only when the copy's own plan says the destination keeps it.
    const kept = result.plan?.kept === true
    const offer = kept ? releaseOffer(to) : null
    status.replaceChildren(resultPanel(result, to, false), offer ? offer.el
      : h('p', { class: 'muted small', 'data-release-not-offered': '' }, `${to} did not say it keeps this copy, so releasing it in ${sourceName} is not offered.`))
    if (ask) replan(); else draw()
    return offer
  }

  function refusedNote(result, to) {
    if (result.refused === 'not_kept') {
      const entries = notKeptOf(result.plan)
      const what = entries.length ? namesOf(entries) : 'what this copy adds to'
      return note(`Nothing was copied: ${to} has released or deleted ${what}. Restore ${entries.length > 1 ? 'them' : 'it'} in ${to} first, then copy.`, 'warn')
    }
    return note('Nothing was copied: the audit found things in the run records. Read them in the plan, then tick "Copy anyway" to copy.', 'warn')
  }

  function stoppedNote(to, failure) {
    const n = note(`The copy to ${to} stopped: ${failure.message || 'the server gave no reason'}. Nothing may have been copied, or only part of it. `
      + `Copying again skips any blocks already in ${to}.`, 'bad')
    n.setAttribute('data-copy-stopped', '')
    return n
  }

  function lostNote(to, failure) {
    const n = note(`The page lost touch with the explore server while copying to ${to} (${failure.message || 'no reason given'}). `
      + `The copy may still be running on the server: reload this page later to see what ${to} holds. Copying again skips any blocks already there.`, 'bad')
    n.setAttribute('data-copy-lost', '')
    return n
  }

  /**
   * Each listener on its own: one that throws does not stop the others. The
   * destination's `index` from the copy's answer goes to the watch last, so a
   * refresh the copy queued is followed to its end (final review Important 2).
   */
  function forget(alias, index = null) {
    const errors = []
    for (const f of [() => ctx.presence?.invalidate?.(), () => ctx.members?.invalidate?.(alias), () => ctx.sizes?.forget?.(alias),
      () => { if (index) ctx.indexing?.track?.(alias, index) }]) {
      try { f() } catch (e) { errors.push(e) }
    }
    if (errors.length) throw errors[0]
  }

  function resultPanel(r, to, failed) {
    const failures = r.failures ?? []
    const snapshot = r.snapshot === 'written' ? `${to}'s snapshot rewritten`
      : r.snapshot === 'skipped' ? 'not rewritten; the next snapshot of ' + to + ' picks this up'
        : r.snapshot === 'not_indexed' ? `not written: ${to} is not indexed yet; index it from the Storage page`
        : r.snapshot ? `not rewritten (${String(r.snapshot).replace(/^failed:\s*/, '')}); the copy still stands` : null
    const pres = presenceOf(ctx)
    const node = panel({ title: failed ? `Copy to ${to} stopped` : `Copied to ${to}`, meta: failed ? null : 'each block verified against its address' },
      kv([
        ['Copied', `${plural(r.copied ?? 0, 'block')} · ${bytes(r.copied_bytes ?? 0)}`],
        ['Already there', plural(r.plan?.present ?? 0, 'block')],
        ['Transfer record', r.transfer_entry ? h('span', { 'data-transfer-entry': r.transfer_entry }, transferChip(r.transfer_entry, to)) : 'not written'],
        snapshot ? ['Snapshot', snapshot] : null,
        pres.several ? ['Held in', pres.dots(cid)] : null,
      ].filter(Boolean)),
      failures.length ? [
        h('p', {}, `${plural(failures.length, 'block')} failed, so the copy stopped before its transfer entry was written. `
          + `The blocks already written stay in ${to}; copying again skips them.`),
        h('ul', { class: 'failures' }, failures.map(f => h('li', { 'data-failure': f.cid }, addrChip(f.cid), ' ', f.error)))] : null)
    node.setAttribute('data-copy-result', failed ? 'failed' : 'copied')
    return node
  }

  /** The Transfer block as the kit's address chip (seal and short cid), linking its block page in the destination. */
  const transferChip = (transferCid, to) => h('a', { class: 'addr', href: hrefIn(ctx, to, `#/block/${transferCid}`), title: transferCid, 'data-address': transferCid },
    seal(transferCid, { size: 14 }), shortCid(transferCid))

  /**
   * "Release here (kept in <to>)": the run's Release confirmation, or the
   * Selection's page (a Selection is released by deleting it), with what that
   * frees in this store from its root's exclusive bytes.
   */
  function releaseOffer(to) {
    const href = kind === 'run' ? `#/run/${cid}?tab=storage&release=1` : `#/selection/${cid}`
    const frees = h('span', { 'data-release-frees': '' }, '…')
    Promise.resolve().then(() => ex.rootStorage?.(cid)).then((storage) => {
      const root = storage?.roots?.find(r => r.kind === kind) ?? null
      if (!root) { frees.textContent = `What this frees in ${sourceName} is known once its next snapshot is written.`; return }
      const asOf = ex.storageStale?.() ? ', as of the snapshot' : ''
      frees.dataset.bytes = String(root.exclusive)
      frees.textContent = `Frees ${sizeText({ bytes: root.exclusive, partial: root.partial })} in ${sourceName}${asOf}`
        + (root.exclusive === 0 ? ': other runs, Selections, hand-offs or pins here hold the same files.' : ', once a sweep runs after the grace period.')
    }).catch(() => { frees.textContent = `What this frees in ${sourceName} could not be read.` })
    const label = `Release here (kept in ${to})`
    const action = canRelease ? h('a', { class: 'btn', href, 'data-release-offer': kind }, label)
      : h('span', { title: releaseReason }, button(label, { disabled: true, 'data-release-offer': 'unavailable' }))
    const el = h('div', { class: 'note ok release-offer', 'data-release-here': cid },
      h('div', {}, h('div', { class: 'row' }, action, frees),
        kind === 'selection' ? h('p', { class: 'muted small' }, 'A Selection is released by deleting it here: Delete on its page lets a sweep free what only it holds.') : null,
        canRelease ? null : h('p', { class: 'muted small' }, releaseReason)))
    return { el, href }
  }

  // ---- the destination switch ---------------------------------------------
  function destSwitch() {
    const btn = h('button', { type: 'button', class: 'store-switch', 'data-copy-dest': '', 'aria-haspopup': 'listbox', 'aria-expanded': 'false',
      title: 'The store this copy writes to' })
    const menu = h('div', { class: 'store-menu', role: 'listbox', 'aria-label': 'Copy to', hidden: true })
    const el = h('span', { class: 'store-wrap copy-dest' }, btn, menu)
    let open = false
    const stops = []
    const close = () => {
      if (!open) return
      open = false
      menu.setAttribute('hidden', '')
      btn.setAttribute('aria-expanded', 'false')
      for (const stop of stops.splice(0)) stop()
    }
    const options = () => others.map(m => h('button', { type: 'button', role: 'option', 'data-dest-option': m.alias,
      'aria-selected': String(m.alias === state.to), disabled: !m.copy_target, 'aria-disabled': m.copy_target ? null : 'true',
      onclick: () => {
        if (!m.copy_target || own()) return
        close()
        if (state.to !== m.alias) choose(() => { state.to = m.alias })
      } },
    h('span', { class: `dot ${m.key ?? ''}` }), h('span', { class: 'lbl' }, m.alias),
    m.location ? h('small', { class: 'where' }, m.location) : null,
    m.copy_target ? null : h('small', { class: 'role', 'data-read-only': '' }, cantCopyText(m))))
    btn.addEventListener('click', () => {
      if (open) { close(); return }
      open = true
      menu.replaceChildren(...options())
      menu.removeAttribute('hidden')
      btn.setAttribute('aria-expanded', 'true')
      const onClick = (e) => { if (!el.contains(e.target)) close() }
      const onKey = (e) => { if (e.key === 'Escape') { close(); btn.focus?.() } }
      globalThis.document?.addEventListener('click', onClick)
      globalThis.document?.addEventListener('keydown', onKey)
      stops.push(() => globalThis.document?.removeEventListener('click', onClick), () => globalThis.document?.removeEventListener('keydown', onKey))
    })
    const paint = () => {
      const m = memberOf(state.to)
      btn.replaceChildren(h('span', { class: `dot ${m?.key ?? ''}` }),
        m ? `${m.alias}${m.location ? ` · ${m.location}` : ''}` : targets.length ? 'Choose a store' : 'no store to copy to',
        h('span', { class: 'chev', 'aria-hidden': 'true' }, '▾'))
      btn.dataset.copyDest = state.to ?? ''
      btn.disabled = own() || others.length === 0
    }
    return { el, paint, close }
  }

  /** A copy another page started is running: say so, and when it ends redraw (with its result, for this root) and ask again. */
  function watch(job) {
    const same = job.root === rootRef
    const what = kind === 'run' ? 'This run' : 'This Selection'
    status.replaceChildren(h('div', { class: 'note', 'data-copy-elsewhere': job.root },
      h('span', {}, same ? `${what} is being copied to ${job.to}. Its result shows here when the copy ends.`
        : `A copy to ${job.to} is running. This page can copy when it ends: one copy runs at a time.`)))
    job.done.then((last) => {
      if (!here()) return
      if (same && jobs.last === last) { showOutcome(last); return }
      status.replaceChildren()
      replan()
    })
  }

  draw()
  if (jobs.running) watch(jobs.running)
  else if (jobs.last?.root === rootRef && jobs.last.to === state.to && !jobs.last.result?.refused) showOutcome(jobs.last, { ask: false })
  replan()
  return section
}


/** The head's name and crumbs: a Selection by its current names, a run by its name under its pipeline. Reads the snapshot only. */
async function headOf(ex, kind, cid, backHref) {
  if (kind === 'run') {
    const row = await Promise.resolve().then(() => ex.runRow?.(cid)).catch(() => null)
    const name = row?.run_name ?? 'run'
    return { name, runName: row?.run_name ?? null,
      crumbs: [{ text: 'Pipelines', href: '#/' }, ...(row?.pipeline ? [{ text: row.pipeline, href: `#/pipeline/${enc(row.pipeline)}` }] : []), { text: name, href: backHref }] }
  }
  const states = await Promise.resolve().then(() => ex.claimStates?.([cid])).catch(() => null)
  const names = states?.get?.(cid)?.names ?? []
  const name = names.length ? names.join(' / ') : 'Unnamed Selection'
  return { name, runName: null, crumbs: [{ text: 'Selections', href: '#/selections' }, { text: name, href: backHref }] }
}
