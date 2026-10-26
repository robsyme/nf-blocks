// web/src/views/projects.js
// #/projects (P4 Task 2, design doc §4): every project this member knows,
// newest created first, with who created it, its people, how many runs and
// Selections it holds and what it holds in each store (one figure per
// store, never added up). "New project" writes a Project; runs that read
// the same input files are offered as a project. #/projects?unassigned=1
// lists the runs and Selections no project holds, so the old Pipelines way
// in stays a click away. A project's samples are counted on its own page,
// which groups them (sampleid.js).
import { h, link } from '../html.js'
import { avatar, button, claimTag, empty, note, pageHead, panel } from '../components.js'
import { icon, sealed } from '../icons.js'
import { bytes, plural } from '../format.js'
import { SELECTIONS_PAGE } from '../config.js'
import { RUNS_PAGE } from '../model.js'
import { runsSharingInputs } from '../sampleid.js'
import { samePrincipal } from '../identity.js'
import { fromEach, hrefIn, listsAll, mergeRows, unreadableText } from '../members.js'
import { runTone, whenText } from '../words.js'
import { enc, errorNode, flag, namesText, show, unavailableNote } from './common.js'


export async function projects(ex, ctx, { unassigned = false } = {}) {
  return unassigned ? notInAProject(ex, ctx) : projectList(ex, ctx)
}

// ---- what each project holds -------------------------------------------------

/**
 * One member's projects and what each holds: `{ rows, counts, memberships }`.
 * `rows` leaves deleted projects out; `memberships` are those in a project
 * that is not deleted, so a run whose only project was deleted is in no
 * project; `counts` is a Map of project cid to `{ run, selection, item }`,
 * without members that have a current deletion themselves.
 */
async function holdings(mex) {
  const [{ rows: every }, memberships] = await Promise.all([mex.projects({ includeDeleted: true }), mex.projectMemberships()])
  const deleted = new Set(every.filter(r => r.state.hidden).map(r => r.cid))
  const live = memberships.filter(m => !deleted.has(m.project))
  const kinds = await mex.subjectKinds(live.map(m => m.subject))
  const counts = new Map()
  for (const m of live.filter(m => !m.hidden)) {
    const c = counts.get(m.project) ?? { run: 0, selection: 0, item: 0 }
    const kind = kinds.get(m.subject)
    if (kind in c) c[kind]++
    counts.set(m.project, c)
  }
  return { rows: every.filter(r => !r.state.hidden), counts, memberships: live }
}

/** The creator, then the people its policy names (advisory), each once. */
function people(r) {
  const out = []
  for (const who of [r.createdBy, ...r.state.policy.people.map(p => p.principal)])
    if (who && !out.some(o => samePrincipal(o, who))) out.push(who)
  return out
}

/**
 * One store's figure for a project: the bytes its members reach in that
 * store (project_stats), or a dash saying why there is none.
 */
function storeFigure(member, stats, lean) {
  const why = stats ? `What this project's runs, Selections and items hold in ${member.alias}, as of its snapshot`
    : lean ? `${member.alias}'s snapshot leaves storage figures out`
      : `${member.alias}'s snapshot has no figure for this project`
  return h('div', { class: 'store-figure', 'data-store-figure': member.alias, 'data-bytes': stats ? String(stats.reachable) : null, title: why },
    h('span', { class: `dot ${member.key ?? 'a'}` }), ' ', h('span', { class: 'muted' }, member.alias), ' ',
    stats ? h('b', {}, bytes(stats.reachable)) : h('span', { class: 'muted' }, '—'))
}

/**
 * Fills each project's figure cell (`cells`, a Map of cid to its cell) once
 * every store has answered: one projectStatsOf read per store.
 */
function fillFigures(cells, asks) {
  const cids = [...cells.keys()]
  Promise.all(asks.map(({ member, mex }) => mex.projectStatsOf(cids).then(
    (stats) => ({ member, stats, lean: !!mex.storageOmitted }), (error) => ({ member, error }))))
    .then((answers) => {
      for (const [cid, cell] of cells) {
        cell.removeAttribute('data-loading')
        cell.replaceChildren(...answers.map(a => (a.error
          ? h('div', { class: 'store-figure muted', 'data-store-figure': a.member.alias }, `${a.member.alias} could not be read`)
          : storeFigure(a.member, a.stats.get(cid) ?? null, a.lean))))
      }
    })
}

function projectRow(r, { counts, cell, href = `#/project/${r.cid}`, countedIn = null }) {
  const c = counts.get(r.cid) ?? { run: 0, selection: 0, item: 0 }
  const name = link(href, r.name ?? h('span', { class: 'muted' }, 'unnamed project'))
  if (countedIn) name.setAttribute('title', `Opens in ${countedIn}`)
  const counted = countedIn ? `Counted in ${countedIn}` : null
  return h('tr', { 'data-project': r.cid, 'data-source': r.source },
    h('td', {}, h('b', {}, name), r.state.nameConflicted ? flag('names in conflict') : null,
      r.description ? h('div', { class: 'muted project-desc' }, r.description) : null, r.error ? errorNode(r.error) : null),
    h('td', {}, r.createdBy ? claimTag({ assertedBy: r.createdBy, verb: 'created', at: r.createdAt }) : null),
    h('td', {}, h('span', { class: 'row project-people', 'data-people': '' }, people(r).map(p => avatar(p)))),
    h('td', { class: 'r num', 'data-count': 'runs', title: counted }, String(c.run)),
    h('td', { class: 'r num', 'data-count': 'selections', title: counted }, String(c.selection)),
    cell)
}

const LIST_HEAD = () => h('thead', {}, h('tr', {}, h('th', {}, 'Project'), h('th', {}, 'Created'), h('th', {}, 'People'),
  h('th', { class: 'r' }, 'Runs'), h('th', { class: 'r' }, 'Selections'), h('th', { class: 'r' }, 'Stored', h('small', {}, 'per store'))))

// ---- New project ---------------------------------------------------------------

/**
 * The New project form: a name, a description, and the runs a suggestion
 * adds. Create writes the Project, then one `add project` Claim per run, and
 * opens the new project. When a run could not be added the project is
 * already written: the form closes and empties (so Create cannot write it
 * twice), the page stays, and `status` links to the project and names the
 * runs left out. `status` sits outside the form, so it shows either way.
 */
function newProjectForm(ctx) {
  const status = h('div', { id: 'project-status' })
  const name = h('input', { type: 'text', id: 'project-name', maxlength: '256', placeholder: 'Name', 'aria-label': 'Project name' })
  const description = h('textarea', { id: 'project-description', maxlength: '4096', rows: '3', placeholder: 'What it is for (optional)', 'aria-label': 'Description' })
  const adds = h('p', { class: 'muted', 'data-project-adds': '', hidden: true })
  let runs = []
  const fill = ({ name: prefill = '', description: about = '', runs: chosen = [] } = {}) => {
    name.value = prefill
    description.value = about
    runs = chosen
    adds.replaceChildren(...(chosen.length ? [`Adds ${plural(chosen.length, 'run')}: ${namesText(chosen.map(r => r.name))}.`] : []))
    show(adds, chosen.length > 0)
  }
  const create = button('Create project', { kind: 'primary', id: 'project-create', onclick: (event) => ctx.write.run(status, async () => {
    const typed = name.value?.trim() ?? ''
    if (!typed) throw Object.assign(new Error('type a name first'), { code: 'invalid' })
    const written = await ctx.write.writer.project({ name: typed, description: description.value?.trim() ?? '' })
    const failed = []
    for (const run of runs) {
      try {
        await ctx.write.writer.addToProject(run.cid, written.address)
      } catch (e) {
        failed.push(`${run.name} (${e?.message ?? e})`)
      }
    }
    const href = `#/project/${written.address}`
    if (!failed.length) return { address: written.address, href }
    fill()
    show(form, false)
    return { address: written.address, stay: h('p', { class: 'warn', 'data-project-partial': written.address },
      'Created ', link(ctx.write.hrefFor(href), typed), `, but could not add ${namesText(failed, failed.length)}. `,
      `Add ${failed.length === 1 ? 'it' : 'them'} from the project's page.`) }
  }, event.currentTarget) })
  const form = h('form', { class: 'project-form', id: 'project-form', 'data-project-form': '', hidden: true, onsubmit: (e) => e.preventDefault() },
    h('label', {}, h('span', { class: 'muted' }, 'Name'), name),
    h('label', {}, h('span', { class: 'muted' }, 'Description'), description),
    adds,
    h('div', { class: 'row' }, create, button('Cancel', { id: 'project-cancel', onclick: () => show(form, false) })))
  return {
    form,
    status,
    /** Opens the form, filled in for a suggestion (`{ name, description, runs: [{ cid, name }] }`) or empty. */
    open(prefill = {}) {
      fill(prefill)
      status.replaceChildren()
      show(form, true)
      name.focus?.()
    },
  }
}

// ---- suggestions -----------------------------------------------------------------

/**
 * Runs that read the same input files and share no project yet
 * (sampleid.js runsSharingInputs over the snapshot's shared task inputs):
 * `[{ runs: [{ cid, name, pipeline }], files }]`. Asked once per render.
 * `memberships` are the current
 * ones; a group whose runs are all in one project together is left out.
 */
async function suggestionsOf(ex, memberships) {
  const groups = runsSharingInputs(await ex.sharedInputRows())
  const projectsOf = new Map()
  for (const m of memberships) {
    if (!projectsOf.has(m.subject)) projectsOf.set(m.subject, new Set())
    projectsOf.get(m.subject).add(m.project)
  }
  const together = (runs) => {
    const [first, ...rest] = runs.map(r => projectsOf.get(r) ?? new Set())
    return [...first].some(p => rest.every(s => s.has(p)))
  }
  const shown = groups.filter(g => !together(g.runs))
  const cids = [...new Set(shown.flatMap(g => g.runs))]
  const rows = new Map(await Promise.all(cids.map(async c => [c, await ex.runRow(c).catch(() => null)])))
  return shown.map(g => ({ files: g.files,
    runs: g.runs.map(cid => ({ cid, name: rows.get(cid)?.run_name ?? 'unnamed run', pipeline: rows.get(cid)?.pipeline ?? null })) }))
}

/** A suggested project's name: its pipeline's when every run is of one pipeline. */
function suggestedName(runs) {
  const pipelines = new Set(runs.map(r => r.pipeline))
  const [only] = pipelines
  return pipelines.size === 1 && only ? only : 'Runs sharing inputs'
}

const SUGGESTION_NAMES = 3

function suggestionsPanel(groups, form, canWrite, where) {
  if (!groups.length) return null
  return panel({ title: 'Suggested projects', meta: `runs ${where ? `in ${where} ` : ''}that read the same input files` },
    groups.map((g) => {
      const said = `These ${plural(g.runs.length, 'run')} share ${plural(g.files, 'input file')}`
      const more = g.runs.length - SUGGESTION_NAMES
      return h('div', { class: 'row suggestion', 'data-suggestion': g.runs.map(r => r.cid).join(' ') },
        h('span', {}, h('b', {}, said), ': ', g.runs.slice(0, SUGGESTION_NAMES).map((r, i) => [i ? ', ' : null, link(`#/run/${r.cid}`, r.name)]),
          more > 0 ? ` and ${more} more` : null),
        h('span', { class: 'spacer' }),
        canWrite ? button('Create project', { size: 'sm', 'data-suggest-create': '', onclick: () => form.open({
          name: suggestedName(g.runs), description: `${said}.`, runs: g.runs }) }) : null)
    }))
}

// ---- the list --------------------------------------------------------------------

const newestCreated = (a, b) => ((a.row.createdAt ?? '') < (b.row.createdAt ?? '') ? 1 : (a.row.createdAt ?? '') > (b.row.createdAt ?? '') ? -1 : 0)

async function projectList(ex, ctx) {
  const all = listsAll(ctx)
  const canWrite = !!ctx?.write?.available
  const form = canWrite ? newProjectForm(ctx) : null
  const here = { alias: ctx?.store?.name ?? 'this store', key: ctx?.store?.key ?? 'a' }
  const cells = new Map()
  const cellFor = (cid) => {
    const cell = h('td', { class: 'r figures', 'data-loading': '' }, h('span', { class: 'muted' }, 'loading…'))
    cells.set(cid, cell)
    return cell
  }
  let rows
  let unreadable = []
  let memberships
  let asks
  if (all) {
    // Every member's projects, one row per cid, its counts from the first member that lists it; a figure from each member.
    const answers = await fromEach(ctx.members, mex => holdings(mex))
    const read = answers.filter(a => 'value' in a)
    unreadable = answers.filter(a => !('value' in a))
    asks = read.map(a => ({ member: a.member, mex: a.ex }))
    rows = mergeRows(answers, v => v.rows, r => r.cid).sort(newestCreated).map(m => projectRow(m.row, {
      counts: read.find(a => a.member.alias === m.member.alias).value.counts, cell: cellFor(m.row.cid),
      href: hrefIn(ctx, m.member.alias, `#/project/${m.row.cid}`), countedIn: m.member.alias }))
    memberships = read.find(a => a.ex === ex)?.value.memberships ?? (await holdings(ex)).memberships
  } else {
    const held = await holdings(ex)
    asks = [{ member: here, mex: ex }]
    rows = held.rows.map(r => projectRow(r, { counts: held.counts, cell: cellFor(r.cid) }))
    memberships = held.memberships
  }
  if (cells.size) fillFigures(cells, asks)
  const suggestions = await suggestionsOf(ex, memberships)
  const newButton = canWrite ? button('New project', { kind: 'primary', id: 'project-new', onclick: () => form.open() }) : null
  const intro = 'A project groups runs, Selections and samples for one study. Membership is a Claim, so it can change and keeps its history.'
  return h('section', { class: 'projects-page', ...(all ? { 'data-all-stores': '' } : {}) },
    pageHead({ title: 'Projects', actions: newButton ? [newButton] : [],
      subline: [all ? h('b', {}, 'All stores') : [h('span', { class: `dot ${here.key}` }), ' ', h('b', {}, here.alias)],
        plural(rows.length, 'project'), link('#/projects?unassigned=1', 'Not in a project')] }),
    canWrite ? null : unavailableNote(ctx),
    form?.form,
    form?.status,
    suggestionsPanel(suggestions, form, canWrite, all ? here.alias : null),
    rows.length === 0 && unreadable.length === 0
      ? h('div', { 'data-projects-empty': '' }, empty('No projects yet', intro))
      : h('section', { class: 'panel flush' }, h('div', { class: 'pb tbox' }, h('table', { class: 't' }, LIST_HEAD(), h('tbody', {}, rows,
        unreadable.map(a => h('tr', { 'data-member-unreadable': a.member.alias }, h('td', { colspan: '6', class: 'muted' }, unreadableText(a, 'projects')))))))))
}

// ---- Not in a project ----------------------------------------------------------

/**
 * The runs and Selections with no current membership in a project that is
 * not deleted, from the open member: each pipeline's newest RUNS_PAGE runs
 * and the newest SELECTIONS_PAGE Selections, so a big store is read a page
 * at a time; the pipeline's own page lists the rest.
 */
async function notInAProject(ex, ctx) {
  const [{ memberships }, pipelines, selectionPage] = await Promise.all([
    holdings(ex), ex.pipelines(), ex.selectionPage({ limit: SELECTIONS_PAGE })])
  const held = new Set(memberships.map(m => m.subject))
  const pages = await Promise.all(pipelines.map(p => ex.runPage(p.pipeline, { limit: RUNS_PAGE }).then(page => ({ pipeline: p.pipeline, page }))))
  const runs = pages.flatMap(({ pipeline, page }) => page.rows.filter(r => !held.has(r.completion_cid)).map(r => ({ ...r, pipeline })))
  const cut = pages.filter(({ page }) => page.next !== null)
  // A Selection entry whose block could not be read is the Selections page's to report.
  const selections = selectionPage.rows.filter(s => !s.error && !held.has(s.cid))
  const selectionsCut = selectionPage.next !== null
  const storeName = ctx?.store?.name ?? 'this store'
  const where = listsAll(ctx) ? `in ${storeName}` : null
  const meta = (cutText) => [where, cutText].filter(Boolean).join(', ') || null
  const runRow = (r) => h('tr', { 'data-unassigned-run': r.completion_cid },
    h('td', {}, h('div', { class: 'namecell' }, sealed('run', r.completion_cid, { size: 20, tone: runTone(r) }), link(`#/run/${r.completion_cid}`, r.run_name ?? 'unnamed run'))),
    h('td', {}, link(`#/pipeline/${enc(r.pipeline)}`, r.pipeline)),
    h('td', {}, whenText(r.finished_at)))
  const selectionRow = (s) => h('tr', { 'data-unassigned-selection': s.cid },
    h('td', {}, link(`#/selection/${s.cid}`, s.state.names.length ? s.state.names.join(' / ') : h('span', { class: 'muted' }, 'unnamed'))),
    h('td', {}, s.assertedBy ? h('span', { class: 'namecell' }, avatar(s.assertedBy), ' ', h('span', { class: 'muted' }, s.assertedBy)) : null),
    h('td', {}, s.firstSeen ? whenText(s.firstSeen) : ''))
  const none = (text) => h('p', { class: 'muted', style: 'padding:12px 16px', 'data-unassigned-none': '' }, text)
  return h('section', { class: 'projects-page', 'data-unassigned': '' },
    pageHead({ crumbs: [{ text: 'Projects', href: '#/projects' }, { text: 'Not in a project' }], title: 'Not in a project',
      subline: [[h('span', { class: `dot ${ctx?.store?.key ?? 'a'}` }), ' ', h('b', {}, storeName)],
        `${plural(runs.length, 'run')} and ${plural(selections.length, 'Selection')} in no project`, link('#/', 'All pipelines')] }),
    panel({ title: 'Runs', meta: meta(cut.length ? `among each pipeline's newest ${RUNS_PAGE}` : null), flush: true },
      runs.length ? h('div', { class: 'tbox' }, h('table', { class: 't' },
        h('thead', {}, h('tr', {}, h('th', {}, 'Run'), h('th', {}, 'Pipeline'), h('th', {}, 'Finished'))), h('tbody', {}, runs.map(runRow))))
        : none(cut.length ? 'All the runs checked are in a project.' : 'Every run is in a project.')),
    cut.length ? note(['Only the newest runs of ', cut.map((c, i) => [i ? ', ' : null, link(`#/pipeline/${enc(c.pipeline)}`, c.pipeline)]),
      ' were checked; open the pipeline for the rest.']) : null,
    panel({ title: 'Selections', meta: meta(selectionsCut ? `among the newest ${SELECTIONS_PAGE}` : null), flush: true },
      selections.length ? h('div', { class: 'tbox' }, h('table', { class: 't' },
        h('thead', {}, h('tr', {}, h('th', {}, 'Selection'), h('th', {}, 'By'), h('th', {}, 'First seen'))), h('tbody', {}, selections.map(selectionRow))))
        : none(selectionsCut ? 'All the Selections checked are in a project.' : 'Every Selection is in a project.')))
}
