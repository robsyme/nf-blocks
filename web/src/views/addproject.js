// web/src/views/addproject.js
// "Add to project" (P4 Task 4, design doc §4, plan 1.2 and 1.7): one control
// for the run, Selection and item pages, and for a sample on a run's Samples
// tab. A run, Selection or item joins a project by a membership Claim on
// itself; a sample has no cid, so it joins by an `add sample` Claim on the
// project. The control lists the current memberships as claim tags, with
// Remove per project (it supersedes that project's Claims), a chooser of the
// projects that are not deleted, and New project…. A Selection added to a
// project that pins every Selection is pinned too, unless the writer pins it
// already, and its bytes are named on the button first (policyAdd, which the
// project page's Add to project… uses as well). The run, Selection and item
// pages show the memberships as tags in their head's subline, and the run
// and Selection pages' Copy to… points at their project's default store
// (copyChoice, plan 1.7).
//
// Reads: a subject's memberships come from the Claims its page has already
// read (its claim state), so the tags cost no query of their own; the project
// names are one Explorer.projects read, made only when there is a membership
// to name or when the control opens, and shared by every control on a page
// (projectsCache).
import { h, link } from '../html.js'
import { button, claimTag, panel } from '../components.js'
import { sizeText } from '../format.js'
import { samePrincipal } from '../identity.js'
import { membershipsOf, refKey } from '../projects.js'
import { cids, errorNode, flag, holds, pinnedByText, projectLink, show } from './common.js'

const KIND_WORD = { run: 'run', selection: 'Selection', item: 'item' }
const invalid = (message) => Object.assign(new Error(message), { code: 'invalid' })

/** The note a pin written by a project's retention policy carries (plan 1.7). */
export const policyNote = (name) => `project policy: ${name}`

/** Whether the viewer, who writes, already pins a subject with this claim state: the policy then writes no pin of its own (plan 1.7). */
export const pinnedByWriter = (state, ctx) => (state?.current ?? []).some(c => c.attribute === 'pin' && c.verb === 'add' && samePrincipal(c.asserted_by, ctx.viewer))

/**
 * What pinning Selection `subject` keeps here, and who already pins it here,
 * before it is written (P2's figure and line): `{ size, note }`. `size` is
 * selectionSize's, or null when it could not be measured; `note` is
 * pinnedByText of the others' pins, or null when no one else pins it here.
 */
export async function pinFigures(ex, ctx, subject) {
  const [size, rows] = await Promise.all([Promise.resolve(ex.selectionSize(subject)).catch(() => null),
    Promise.resolve(holds(ex, subject).pinnedBy).catch(() => null)])
  const theirs = (rows ?? []).filter(x => !samePrincipal(x.assertedBy, ctx.viewer))
  return { size, note: theirs.length ? pinnedByText(theirs, ctx.viewer, !!size?.partial) : null }
}

/**
 * What adding Selection `subject` to a project that pins every Selection
 * writes, decided before the click (plan 1.7, D5): `{ pin, label, note }`.
 * When the writer pins it already, `pin` is false, the label is "Add" and
 * the note says why. Otherwise the label names what the pin keeps here
 * (P2's figure, "Add and pin 2.50 MB in lab") and the note is who already
 * pins it here (P2's line), or null. `state` is the Selection's claim state
 * when the page has read it; without it this reads it.
 */
export async function policyAdd(ex, ctx, subject, { state = undefined } = {}) {
  const store = ctx.store?.name ?? 'this store'
  const st = state !== undefined ? state : await Promise.resolve(ex.claimStates([subject])).then(m => m.get(subject), () => null)
  if (pinnedByWriter(st, ctx)) return { pin: false, label: 'Add', note: 'Already pinned by you, so adding it writes no pin.' }
  const { size, note } = await pinFigures(ex, ctx, subject)
  return { pin: true, label: size ? `Add and pin ${sizeText(size)} in ${store}` : `Add and pin in ${store}`, note }
}

/**
 * Every project this member knows, deleted ones too (Explorer.projects with
 * includeDeleted), read at most once per page and shared by the controls on
 * it. `reset()` drops it after a write, so the next read sees the write.
 */
export function projectsCache(ex) {
  let rows = null
  return {
    get: () => (rows ??= Promise.resolve(ex.projects({ includeDeleted: true })).then(r => r.rows, (e) => { rows = null; throw e })),
    reset: () => { rows = null },
  }
}

/** Memberships `[{ project, claim }]` grouped by project: `[{ project, claims: [{ cid, asserted_by, timestamp }] }]`. */
function byProject(list) {
  const out = new Map()
  for (const m of list) {
    if (!out.has(m.project)) out.set(m.project, { project: m.project, claims: [] })
    out.get(m.project).claims.push(m.claim)
  }
  return [...out.values()]
}

/** The projects whose current `add sample` Claims name `sample`, from the projects' own state: `[{ project, claims }]`. */
const sampleMemberships = (rows, sample) => rows.flatMap(r => (r.state?.samples ?? [])
  .filter(s => refKey(s.ref) === refKey(sample)).map(s => ({ project: r.cid, claims: s.claims })))

/**
 * Each project's name, whether it is deleted and its default store, a Map of
 * cid to `{ name, deleted, defaultStore }`: from the projects read, else the
 * project's own block (one this member's log does not announce). A project
 * that cannot be read has no entry, and shows as its short cid.
 */
async function namesOf(ex, rows, projects) {
  const out = new Map(rows.map(r => [r.cid, { name: r.name, deleted: !!r.state?.hidden, defaultStore: r.state?.policy?.defaultStore?.value ?? null }]))
  await Promise.all(projects.filter(p => !out.has(p)).map(p => Promise.resolve(ex.project?.(p))
    .then((x) => { if (x) out.set(p, { name: x.name, deleted: !!x.hidden, defaultStore: x.policy?.defaultStore?.value ?? null }) }, () => {})))
  return out
}

/** Project names for nameProjectLinks: namesOf over the page's projects read (`cache`, a projectsCache). */
export const projectNames = (ex, cache, projects) => cache.get().catch(() => []).then(rows => namesOf(ex, rows, projects))

/**
 * Where Copy to… points from a subject in projects `list` (plan 1.7, ruling
 * Q1), given their `info` (namesOf), from the projects that are not deleted
 * and set a default store: null when there are none. When they all name one
 * store, that store if it is a copy target here (`{ to }`), else `{ project }`,
 * one project naming it, so the copy page can say why it preselects no store.
 * When they name different stores, `{ project: [...] }`, each of them, so the
 * copy page preselects none and names them. A read-only member is never made
 * a target.
 */
function copyChoice(list, info, ctx) {
  const set = list.map(m => ({ project: m.project, ...info.get(m.project) })).filter(p => p.defaultStore && !p.deleted)
  if (!set.length) return null
  if (new Set(set.map(p => p.defaultStore)).size > 1) return { project: set.map(p => p.project) }
  const { project, defaultStore: alias } = set[0]
  const source = ctx.members?.primary ?? ctx.store?.name ?? null
  return alias !== source && (ctx.members?.list ?? []).some(m => m.alias === alias && m.copy_target) ? { to: alias } : { project }
}

/**
 * Points a Copy to… link (copyToButton's) at the default store of the
 * subject's projects once they are read (copyChoice): `make(choice)` is the
 * link copyToButton draws for that choice. A Copy to… that cannot copy is a
 * disabled button, and stays as it is. A plain click before the projects are
 * read would open the copy page at P3's first target, so it waits for them
 * and then follows the aimed link, unless the page was left meanwhile.
 */
export function aimCopyTo(link, control, make) {
  if (link?.tagName !== 'A') return
  let aimed = false
  // The unaimed href is the copy page at P3's first target, which Q1 forbids for a project whose default store is unusable. Until
  // the projects are read the link has no href, so a modifier-click or a middle click opens nothing.
  const unaimed = link.getAttribute('href')
  link.removeAttribute('href')
  link.setAttribute('tabindex', '0')
  link.setAttribute('role', 'link')
  const aiming = control.copyChoice()
    .then((choice) => { link.setAttribute('href', choice ? make(choice).getAttribute('href') : unaimed) }, () => { link.setAttribute('href', unaimed) })
    .then(() => { aimed = true; for (const a of ['aria-busy', 'tabindex', 'role']) link.removeAttribute(a) })
  const wait = () => {
    link.setAttribute('aria-busy', 'true')
    const from = globalThis.location.hash
    aiming.then(() => { if (globalThis.location.hash === from) globalThis.location.hash = link.getAttribute('href') })
  }
  link.addEventListener('click', (e) => {
    if (aimed || !globalThis.location) return
    e.preventDefault()
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button) return
    wait()
  })
  // An href-less link takes no Enter from the browser, so it is given here, as an early plain click.
  link.addEventListener('keydown', (e) => {
    if (aimed || e.key !== 'Enter' || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || !globalThis.location) return
    e.preventDefault()
    wait()
  })
}

/**
 * The copy page's line when the project its Copy to… came from has a
 * default store this page does not preselect (plan 1.7, ruling Q1): one not
 * configured here, read-only here, or the store it copies from. `project`
 * may list several projects, comma-separated, whose default stores
 * disagree: the line then names each project's store. Hidden until the
 * projects are read, and when there is nothing to say.
 */
export function defaultStoreNote(ex, ctx, project) {
  const node = h('p', { class: 'muted', 'data-copy-default-store': '', hidden: true })
  const cids = String(project).split(',').filter(Boolean)
  if (cids.length > 1) {
    Promise.all(cids.map(c => Promise.resolve(ex.project(c)).catch(() => null))).then((ps) => {
      const named = ps.filter(p => p && !p.hidden && p.policy?.defaultStore?.value)
      const stores = [...new Set(named.map(p => p.policy.defaultStore.value))]
      if (stores.length < 2) return
      node.setAttribute('data-copy-default-store', stores.join(','))
      node.textContent = `The projects name different default stores: ${named.map(p => `${p.name ?? 'an unnamed project'}'s is ${p.policy.defaultStore.value}`).join(', ')}.`
      show(node, true)
    }, () => {})
    return node
  }
  Promise.resolve(ex.project(project)).then((p) => {
    const alias = p?.policy?.defaultStore?.value
    const member = (ctx.members?.list ?? []).find(m => m.alias === alias)
    const why = !alias ? null : alias === (ctx.members?.primary ?? ctx.store?.name) ? 'is the store this copies from'
      : !member ? 'is not configured here' : !member.copy_target ? 'is read-only here' : null
    if (!why) return
    node.setAttribute('data-copy-default-store', alias)
    node.textContent = `${p.name ?? 'This project'}'s default store ${alias} ${why}.`
    show(node, true)
  }, () => {})
  return node
}


/**
 * The "Add to project" control. For a run, Selection or item, `subject` is
 * its cid, `kind` 'run', 'selection' or 'item', `claims` the Claims its page
 * read about it (claim state's `claims`; without them one projectsOf read),
 * `state` its claim state (a Selection's, for policyAdd) and `href` the
 * page to draw again after a write (a string, or a function giving one).
 * For a sample, `sample` is its SampleRef `{ pipeline, id }`; its writes
 * keep the page and draw the control again. `cache` is the page's
 * projectsCache.
 *
 * Returns `{ tags, button, node, open, copyChoice }`:
 * - `tags`: a subject's memberships, one claim tag per membership Claim
 *   (who, when, the project linked), or null when it is in none;
 * - `button`: "Add to project…", which shows or hides `node`; null where
 *   this page cannot write (ctx.write.available and here, as for Rename
 *   and Pin);
 * - `node`: the panel, hidden until opened, drawn on its first opening.
 *   For a subject it is null where the page cannot write (its tags say
 *   it all); for a sample it lists the memberships read only.
 * - `open()`: shows or hides `node`, as `button` does.
 * - `copyChoice()`: where the subject's Copy to… points (copyChoice), or
 *   null; it shares the tags' reads.
 */
export function projectControl(ex, ctx, { subject = null, kind = null, claims = null, state = undefined, sample = null, href = null,
  cache = projectsCache(ex) } = {}) {
  const can = !!(ctx.write?.available && ctx.write?.here)
  // A subject's memberships: from its page's Claims at once, else read.
  const known = !sample && claims ? byProject(membershipsOf(new Map([[subject, claims]]))) : null
  const memberships = () => (sample ? cache.get().then(rows => sampleMemberships(rows, sample))
    : known ? Promise.resolve(known)
      : Promise.resolve(ex.projectsOf?.([subject])).then(m => byProject(m?.get(subject) ?? []), () => []))
  // A subject's memberships as its page opened, and their projects' names and default stores: read once, for the tags and Copy to….
  const first = sample || (known && !known.length) || (!known && !ex.projectsOf) ? null : memberships()
  let firstInfo = null
  const infoOf = (list) => (firstInfo ??= cache.get().catch(() => []).then(rows => namesOf(ex, rows, list.map(m => m.project))))
  const tags = first ? tagsNode(first, infoOf) : null
  const copyTo = () => (first ? first.then(async list => (list.length ? copyChoice(list, await infoOf(list), ctx) : null)) : Promise.resolve(null))
  if (!sample && !can) return { tags, button: null, node: null, open: () => {}, copyChoice: copyTo }

  const status = h('div', { 'data-project-status': '' })
  const body = h('div', { class: 'stack' })
  const what = sample ? sample.id : `this ${KIND_WORD[kind] ?? 'subject'}`
  const node = h('div', { class: 'project-control', 'data-project-control': sample ? refKey(sample) : subject, hidden: true },
    panel({ title: sample ? `Projects with ${sample.id}` : 'Projects', meta: sample ? `${sample.pipeline} · a sample joins by a Claim on the project` : 'membership is a Claim' },
      body, status))
  const pageHref = () => (typeof href === 'function' ? href() : href)
  // One write through ctx.write.run. A subject's page is drawn again from its own tail. A write that keeps the
  // page (a sample's, or a new project whose membership failed) draws the control again once runWrite has
  // refreshed the tail. `attempt` may return a done of its own.
  const write = async (attempt, el, said) => {
    let done = null
    await ctx.write.run(status, async () => {
      done = (await attempt(ctx.write.writer)) ?? (sample || !pageHref() ? { stay: h('p', { class: 'muted', 'data-project-written': '' }, said) } : { href: pageHref() })
      return done
    }, el)
    if (done?.stay) { cache.reset(); await draw() }
  }

  const list = (current, names) => (current.length
    ? h('ul', { class: 'plain', 'data-project-memberships': String(current.length) }, current.map(m => h('li', { class: 'row', 'data-membership': m.project },
      projectLink(m.project, names.get(m.project)?.name), names.get(m.project)?.deleted ? flag('deleted') : null,
      m.claims.map(c => claimTag({ assertedBy: c.asserted_by, verb: 'added', at: c.timestamp })),
      can ? button('Remove', { size: 'sm', kind: 'ghost', 'data-project-remove': m.project,
        title: sample ? 'Supersedes the Claims that added it to this project.' : 'Supersedes the Claim that added it. Nothing is deleted or released.',
        onclick: (e) => write(w => (sample ? w.removeSample(m.project, cids(m.claims)) : w.removeFromProject(subject, cids(m.claims))), e.currentTarget,
          `Removed ${what} from ${names.get(m.project)?.name ?? 'the project'}.`) }) : null)))
    : h('p', { class: 'muted', 'data-project-memberships': '0' }, `${sample ? sample.id : `This ${KIND_WORD[kind] ?? 'subject'}`} is in no project yet.`))

  const chooser = (current, rows) => {
    const member = new Set(current.map(m => m.project))
    const offered = rows.filter(r => !r.state?.hidden && !member.has(r.cid))
    if (!offered.length) {
      return h('p', { class: 'muted small', 'data-project-none': '' }, rows.some(r => !r.state?.hidden)
        ? `${sample ? sample.id : `This ${KIND_WORD[kind] ?? 'subject'}`} is in every project here. New project… makes another.` : 'There are no projects here yet.')
    }
    const select = h('select', { 'data-project-choose': '', 'aria-label': 'Project' }, offered.map(r => h('option', { value: r.cid }, r.name ?? 'Unnamed project')))
    select.value = offered[0].cid
    const chosen = () => offered.find(r => r.cid === select.value) ?? null
    const note = h('div', { class: 'muted small', 'data-add-pin-note': '', hidden: true })
    const policy = h('div', { class: 'muted small', 'data-add-policy': '', hidden: true })
    let plan = { pin: false }
    // The projects this chooser has added the subject to: a retry after a refused pin does not add it again.
    const added = new Set()
    const add = button('Add', { kind: 'primary', size: 'sm', 'data-project-add-go': '', onclick: (e) => {
      const r = chosen()
      return write(async (w) => {
        if (!r) throw invalid('choose a project first')
        if (sample) {
          await w.addSample(r.cid, sample)
          return null
        }
        if (!added.has(r.cid)) {
          await w.addToProject(subject, r.cid)
          added.add(r.cid)
        }
        if (plan.pin) await w.pin(subject, policyNote(r.name))
        return null
      }, e.currentTarget, `Added ${what} to ${r?.name ?? 'the project'}.`)
    } })
    // A Selection going into a project that pins every Selection is pinned too, unless the writer pins it already:
    // the button waits for what the pin keeps here, and names it.
    const decide = () => {
      const r = chosen()
      plan = { pin: false }
      add.textContent = 'Add'
      add.disabled = false
      show(note, false)
      const pins = kind === 'selection' && r?.state?.policy?.retention?.value === 'pin_selections'
      show(policy, pins)
      if (!pins) return
      policy.textContent = `${r.name ?? 'This project'} pins every Selection in it, in ${ctx.store?.name ?? 'this store'}. The pin's note is "${policyNote(r.name)}".`
      add.disabled = true
      add.textContent = 'Add and pin…'
      const asked = r.cid
      policyAdd(ex, ctx, subject, { state }).then((p) => {
        if (chosen()?.cid !== asked) return
        plan = p
        add.textContent = p.label
        if (p.note) { note.textContent = p.note; show(note, true) }
        add.disabled = false
      })
    }
    select.addEventListener('change', decide)
    decide()
    return h('div', { class: 'stack', 'data-project-chooser': '' }, h('div', { class: 'row' }, h('span', {}, 'Add to'), select, add), policy, note)
  }

  const newProject = () => {
    const name = h('input', { type: 'text', maxlength: '256', placeholder: 'Name', 'aria-label': 'Project name', 'data-project-new-name': '' })
    const description = h('textarea', { maxlength: '4096', rows: '2', placeholder: 'What it is for (optional)', 'aria-label': 'Description',
      'data-project-new-description': '' })
    const form = h('div', { class: 'project-control-form', 'data-project-new': '', hidden: true },
      h('label', {}, h('span', { class: 'muted' }, 'Name'), name),
      h('label', {}, h('span', { class: 'muted' }, 'Description'), description),
      h('div', { class: 'row' },
        button('Create and add', { kind: 'primary', size: 'sm', 'data-project-new-create': '', onclick: (e) => {
          const typed = name.value?.trim() ?? ''
          return write(async (w) => {
            if (!typed) throw invalid('type a name first')
            const made = await w.project({ name: typed, description: description.value?.trim() ?? '' })
            try {
              if (sample) await w.addSample(made.address, sample)
              else await w.addToProject(subject, made.address)
            } catch (err) {
              // The project is written: the form empties and closes, so Create cannot write it twice.
              name.value = ''
              description.value = ''
              show(form, false)
              return { address: made.address, stay: h('p', { class: 'warn', 'data-project-partial': made.address },
                'Created ', link(ctx.write.hrefFor(`#/project/${made.address}`), typed), `, but could not add ${what} to it (${err?.message ?? err}). `,
                'Choose it above to try again.') }
            }
            return null
          }, e.currentTarget, `Created ${typed} and added ${what} to it.`)
        } }),
        button('Cancel', { size: 'sm', 'data-project-new-cancel': '', onclick: () => show(form, false) })))
    return [button('New project…', { size: 'sm', kind: 'ghost', 'data-project-new-open': '', onclick: () => { show(form, form.hasAttribute('hidden')); name.focus?.() } }), form]
  }

  let drawn = false
  async function draw() {
    drawn = true
    body.replaceChildren(h('p', { class: 'muted' }, 'Reading projects…'))
    try {
      const [rows, current] = await Promise.all([cache.get(), memberships()])
      const names = await namesOf(ex, rows, current.map(m => m.project))
      body.replaceChildren(...[list(current, names), can ? chooser(current, rows) : null, can ? newProject() : null].flat().filter(Boolean))
    } catch (e) {
      body.replaceChildren(errorNode(e))
    }
  }
  const open = () => {
    const opening = node.hasAttribute('hidden')
    show(node, opening)
    return opening && !drawn ? draw() : Promise.resolve()
  }
  return { tags, node, open, copyChoice: copyTo, button: can ? button('Add to project…', { 'data-project-control-open': '', onclick: open }) : null }
}

/**
 * A subject's memberships for its head's subline: one claim tag per
 * membership Claim (who, "added to", the project linked, when). Names arrive
 * with the projects read; until then a project reads as its short cid.
 */
function tagsNode(memberships, infoOf) {
  const node = h('span', { class: 'project-tags', 'data-project-tags': '' })
  const tag = (project, c, names) => h('span', { class: 'project-tag', 'data-project-tag': project, 'data-claim': c.cid },
    claimTag({ assertedBy: c.asserted_by, verb: 'added to', value: projectLink(project, names.get(project)?.name), at: c.timestamp }),
    names.get(project)?.deleted ? flag('deleted') : null)
  const draw = (list, names) => node.replaceChildren(...list.flatMap(m => m.claims.map(c => tag(m.project, c, names))))
  memberships.then(async (list) => {
    if (!list.length) return
    draw(list, new Map())
    draw(list, await infoOf(list))
  }, () => {})
  return node
}
