// #/projects (P4 Task 2): the list with counts, people and one storage figure
// per store, the empty state, New project, the suggestions from shared input
// files, and #/projects?unassigned=1. A real Explorer over the fixture member
// on the plugin's schema.sql; the writer is a fake that records its calls.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { frame, installDom, strayTexts } from './dom.mjs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { createMembers } from '../src/members.js'
import { bytes } from '../src/format.js'
import { parseProjectsQuery } from '../src/listquery.js'
import { projects } from '../src/views/projects.js'
import { snapshotDb } from './helpers.mjs'
import { blockFetch, buildMember, entryName, rawCid } from './fixture.mjs'

const NOW = Date.parse('2026-10-25T12:00:00.000Z')
const AT = '2026-10-25T10:00:00.000Z'
const claim = (subject, verb, attribute, value, supersedes = [], by = 'ana@x') =>
  ({ kind: 'Claim', schema: 1, asserted_by: by, subject, verb, attribute, value, supersedes, timestamp: AT })
const project = (name, createdAt, by = 'ana@x') =>
  ({ kind: 'Project', schema: 1, asserted_by: by, name, description: `about ${name}`, created_at: createdAt })

/**
 * P1 "Liver study" (snapshot, people ben@x) holds R1 (snapshot Claim), R2
 * and Selection S (tail Claims), and names one sample. P2 "Kidney" is a tail
 * Project with nothing in it. P3 is deleted and holds R3, so R3 counts as in
 * no project. S2 is a tail Selection in no project. R2 and R3 read the same
 * two input files; `together` puts R3 in P1 beside R2, and `many` adds two
 * runs the snapshot does not know reading them too. `noProjects` builds
 * none of it; `noInputs` writes no run_input rows. `labOnly` adds P5, a
 * project only this member holds, with a figure. `noStats` writes no
 * project_stats row for P1.
 */
async function open({ together = false, noProjects = false, noInputs = false, many = false, labOnly = false, noStats = false } = {}) {
  const ids = {}
  const member = await buildMember({
    now: NOW,
    extra: noProjects ? null : ({ put, runs }) => {
      const R = (k) => CID.parse(runs[k].completion)
      ids.P1 = put(project('Liver study', '2026-10-20T10:00:00.000Z'))
      ids.P2 = put(project('Kidney', '2026-10-24T10:00:00.000Z', 'ben@x'))
      ids.P3 = put(project('Old', '2026-10-01T10:00:00.000Z'))
      ids.MR1 = put(claim(R('R1'), 'add', 'project', ids.P1))
      ids.PB = put(claim(ids.P1, 'add', 'policy.people', 'ben@x'))
      ids.SA = put(claim(ids.P1, 'add', 'sample', { pipeline: 'demo', id: 'A' }))
      ids.MR2 = put(claim(R('R2'), 'add', 'project', ids.P1))
      ids.S = put({ kind: 'Selection', schema: 1, asserted_by: 'ana@x', derived_from: [], members: [] })
      ids.S2 = put({ kind: 'Selection', schema: 1, asserted_by: 'ben@x', derived_from: [], members: [{ item: { address: CID.parse(runs.R1.collection), via: [] } }] })
      ids.N2 = put(claim(ids.S2, 'set', 'name', 'loose BAMs', [], 'ben@x'))
      ids.MS = put(claim(ids.S, 'add', 'project', ids.P1))
      ids.MR3 = put(claim(R('R3'), 'add', 'project', together ? ids.P1 : ids.P3))
      ids.D3 = put(claim(ids.P3, 'delete', null, null))
      if (labOnly) ids.P5 = put(project('Lab only', '2026-10-21T10:00:00.000Z'))
      const t = (n) => NOW - 100_000 + n * 1000
      return {
        log: [entryName(t(1), 'project', ids.P2), entryName(t(2), 'claim', ids.MR2), entryName(t(3), 'selection', ids.S),
          entryName(t(4), 'claim', ids.MS), entryName(t(5), 'claim', ids.MR3), entryName(t(6), 'claim', ids.D3),
          entryName(t(7), 'selection', ids.S2), entryName(t(8), 'claim', ids.N2), entryName(t(9), 'project', ids.P3)],
        rows: (db) => {
          const row = (sql, bind) => db.exec({ sql, bind })
          const p1 = project('Liver study', '2026-10-20T10:00:00.000Z')
          row('INSERT INTO project VALUES (?, ?, ?, ?, ?)', [ids.P1.toString(), p1.name, p1.description, p1.asserted_by, p1.created_at])
          if (!noStats) row('INSERT INTO project_stats VALUES (?, 1000000, 0, 1000000, 0, 1)', [ids.P1.toString()])
          if (labOnly) {
            row('INSERT INTO project VALUES (?, ?, ?, ?, ?)', [ids.P5.toString(), 'Lab only', '', 'ana@x', '2026-10-21T10:00:00.000Z'])
            row('INSERT INTO project_stats VALUES (?, 5000, 0, 5000, 0, 1)', [ids.P5.toString()])
          }
          for (const [c, subject, attribute, value] of [['MR1', R('R1'), 'project', `{"/":"${ids.P1}"}`], ['PB', ids.P1, 'policy.people', 'ben@x'],
            ['SA', ids.P1, 'sample', '{"id":"A","pipeline":"demo"}']]) {
            row('INSERT INTO claim VALUES (?, ?, ?, ?, ?, ?, ?)', [ids[c].toString(), subject.toString(), 'add', attribute, value, AT, 'ana@x'])
            row('INSERT INTO claim_current VALUES (?, ?, ?, ?, 0)', [subject.toString(), attribute, value, ids[c].toString()])
          }
          if (noInputs) return
          row("INSERT INTO run_input VALUES (?, 'task', 'A_R1.fastq.gz', ?, 100, 'A')", [runs.R1.completion, rawCid('reads-A').toString()])
          for (const run of [runs.R2.completion, runs.R3.completion, ...(many ? ['bafyx1', 'bafyx2'] : [])]) {
            for (const s of ['B', 'C'])
              row("INSERT INTO run_input VALUES (?, 'task', ?, ?, 100, ?)", [run, `${s}_R1.fastq.gz`, rawCid(`reads-${s}`).toString(), s])
          }
        },
      }
    },
  })
  const explorer = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }), now: () => NOW })
  return { explorer, member, ids: Object.fromEntries(Object.entries(ids).map(([k, v]) => [k, v.toString()])) }
}

/** A ctx whose writer records its calls and answers with `address`; `ran` holds what each write attempt returned. */
function ctxFor({ available = true, address = 'bafyreinewproject', failAdd = null } = {}) {
  const calls = []
  const ran = []
  const writer = {
    project: async (fields) => { calls.push(['project', fields]); return { address } },
    addToProject: async (subject, p) => {
      calls.push(['addToProject', subject, p])
      if (subject === failAdd) throw Object.assign(new Error('not_found'), { code: 'not_found' })
      return { address: `claim-${subject}` }
    },
  }
  const ctx = { mark: () => {}, use: () => {}, store: { name: 'lab', key: 'a', all: false },
    write: { available, here: true, writable: 'lab', reason: available ? null : 'Read only here.', hrefFor: (h) => h, writer,
      run: async (status, fn) => {
        try {
          const done = await fn()
          ran.push({ done })
          // As app.js's runWrite: a partly failed write stays, and its status says so.
          if (done?.stay) status.replaceChildren(done.stay)
        } catch (e) {
          ran.push({ error: e })
          status.replaceChildren(e.message)
        }
      } } }
  return { ctx, calls, ran }
}

const text = (el) => el.textContent.replace(/\s+/g, ' ')
const settle = async () => { await frame(); await frame() }

test('#/projects?unassigned=1 parses to the Not in a project listing', () => {
  assert.deepEqual(parseProjectsQuery(undefined), { unassigned: false })
  assert.deepEqual(parseProjectsQuery('unassigned=1'), { unassigned: true })
})

test('the list: name, the created tag, people, runs and Selections, and this store\'s figure read once; a deleted project is left out', async () => {
  installDom()
  const { explorer, ids } = await open()
  let reads = 0
  let answer
  const answered = new Promise((resolve) => { answer = resolve })
  const statsOf = explorer.projectStatsOf.bind(explorer)
  explorer.projectStatsOf = async (cids) => { reads++; await answered; return statsOf(cids) }
  const page = await projects(explorer, ctxFor().ctx)
  const loading = page.querySelectorAll('td[data-loading]')
  assert.equal(loading.length, 2, 'every figure cell says loading until the store answers')
  assert.match(loading[0].textContent, /loading/)
  answer()
  await settle()
  assert.equal(reads, 1, 'one figures read for the store, not one per row')
  assert.equal(page.querySelectorAll('td[data-loading]').length, 0)
  const rows = page.querySelectorAll('tr[data-project]')
  assert.deepEqual(rows.map(r => r.dataset.project), [ids.P2, ids.P1], 'newest created first; P3 is deleted')
  const p1 = rows[1]
  assert.equal(p1.querySelector('a').getAttribute('href'), `#/project/${ids.P1}`)
  assert.equal(p1.querySelector('a').textContent, 'Liver study')
  const created = p1.querySelector('.claim')
  assert.match(created.textContent, /created/)
  assert.equal(created.querySelector('.av').getAttribute('title'), 'ana@x')
  assert.deepEqual(p1.querySelector('[data-people]').querySelectorAll('.av').map(a => a.getAttribute('title')), ['ana@x', 'ben@x'])
  const count = (row, what) => row.querySelector(`[data-count=${what}]`).textContent
  assert.deepEqual([count(p1, 'runs'), count(p1, 'selections')], ['2', '1'])
  assert.deepEqual([count(rows[0], 'runs'), count(rows[0], 'selections')], ['0', '0'])
  assert.equal(p1.querySelector('[data-count=samples]'), null, 'samples are counted on the project page')
  assert.equal(p1.querySelector('[data-count=runs]').getAttribute('title'), null, 'one store: no "counted in"')
  const figures = p1.querySelectorAll('[data-store-figure]')
  assert.equal(figures.length, 1, 'one figure: this store')
  assert.equal(figures[0].dataset.storeFigure, 'lab')
  assert.equal(figures[0].dataset.bytes, '1000000')
  assert.match(text(figures[0]), new RegExp(`lab ${bytes(1_000_000)}`))
  const none = rows[0].querySelector('[data-store-figure]')
  assert.equal(none.dataset.bytes, undefined)
  assert.match(none.getAttribute('title'), /no figure for this project/)
  assert.ok(page.querySelector('a[href="#/projects?unassigned=1"]'), 'Not in a project is a click away')
  assert.deepEqual(strayTexts(page), [])
})

test('all stores: one row per project, and one figure per store side by side, never added up; a store without the project shows a dash', async () => {
  installDom()
  const lab = await open({ labOnly: true })
  const org = await open()
  const list = [{ alias: 'lab', key: 'a', writable: true, base: 'm/lab/' }, { alias: 'org', key: 'b', writable: false, base: 'm/org/' }]
  const exes = { lab: lab.explorer, org: org.explorer }
  const members = createMembers({ member: 'lab', all: true, members: list }, { openExplorer: async (m) => exes[m.alias] })
  const { ctx } = ctxFor()
  Object.assign(ctx, { members, store: { name: 'lab', key: 'a', all: true, hrefIn: (alias, hash) => `?member=${alias}${hash}` } })
  const page = await projects(lab.explorer, ctx)
  await settle()
  assert.ok(page.hasAttribute('data-all-stores'))
  const rows = page.querySelectorAll('tr[data-project]')
  assert.deepEqual(rows.map(r => r.dataset.project), [lab.ids.P2, lab.ids.P5, lab.ids.P1])
  const figures = rows[2].querySelectorAll('[data-store-figure]')
  assert.deepEqual(figures.map(f => [f.dataset.storeFigure, f.dataset.bytes]), [['lab', '1000000'], ['org', '1000000']])
  assert.doesNotMatch(text(rows[2]), new RegExp(bytes(2_000_000)))
  assert.equal(rows[2].querySelector('a').getAttribute('href'), `?member=lab#/project/${lab.ids.P1}`)
  assert.equal(rows[2].querySelector('[data-count=runs]').getAttribute('title'), 'Counted in lab')
  const only = rows[1].querySelectorAll('[data-store-figure]')
  assert.deepEqual(only.map(f => [f.dataset.storeFigure, f.dataset.bytes ?? null]), [['lab', '5000'], ['org', null]])
  assert.match(text(only[1]), /org —/)
  assert.match(text(page), /runs in lab that read the same input files/)
})

test('no projects: one sentence on what a project is, and New project', async () => {
  installDom()
  const { explorer } = await open({ noProjects: true })
  const page = await projects(explorer, ctxFor().ctx)
  const none = page.querySelector('[data-projects-empty]')
  assert.ok(none)
  assert.match(text(none), /No projects yet/)
  assert.match(text(none), /A project groups runs, Selections and samples for one study\./)
  assert.equal(page.querySelectorAll('button').filter(b => b.textContent === 'New project').length, 1, 'one New project, in the head')
  assert.equal(page.querySelector('table'), null)
})

test('New project writes the Project with its name and description, then opens it', async () => {
  installDom()
  const { explorer } = await open()
  const { ctx, calls, ran } = ctxFor({ address: 'bafyreinew' })
  const page = await projects(explorer, ctx)
  const form = page.querySelector('#project-form')
  assert.ok(form.hasAttribute('hidden'))
  await page.querySelector('#project-new').click()
  assert.ok(!form.hasAttribute('hidden'))
  await form.querySelector('#project-create').click()
  assert.deepEqual(calls, [], 'no name, no write')
  assert.match(text(page.querySelector('#project-status')), /type a name first/)
  form.querySelector('#project-name').value = '  RAP1 depletion '
  form.querySelector('#project-description').value = 'Yeast RAP1 auxin-depletion time course'
  await form.querySelector('#project-create').click()
  assert.deepEqual(calls, [['project', { name: 'RAP1 depletion', description: 'Yeast RAP1 auxin-depletion time course' }]])
  assert.deepEqual(ran.at(-1).done, { address: 'bafyreinew', href: '#/project/bafyreinew' })
})

test('without a write endpoint there is no New project, and the page says why', async () => {
  installDom()
  const { explorer } = await open()
  const page = await projects(explorer, ctxFor({ available: false }).ctx)
  assert.equal(page.querySelector('#project-new'), null)
  assert.equal(page.querySelector('#project-form'), null)
  assert.match(text(page.querySelector('[data-unavailable]')), /Read only here\./)
})

test('a suggestion: these runs share input files, by name, with Create project', async () => {
  installDom()
  const { explorer, member } = await open()
  const page = await projects(explorer, ctxFor().ctx)
  const s = page.querySelectorAll('[data-suggestion]')
  assert.equal(s.length, 1)
  assert.equal(s[0].dataset.suggestion, [member.runs.R2.completion, member.runs.R3.completion].sort().join(' '))
  assert.match(text(s[0]), /These 2 runs share 2 input files: R[23], R[23]/)
  assert.deepEqual(s[0].querySelectorAll('a').map(a => a.getAttribute('href')).sort(),
    [`#/run/${member.runs.R2.completion}`, `#/run/${member.runs.R3.completion}`].sort())
  assert.ok(s[0].querySelector('[data-suggest-create]'))
})

test('Create project from a suggestion opens the form filled in, and adds the runs to the new project', async () => {
  installDom()
  const { explorer, member } = await open()
  const { ctx, calls, ran } = ctxFor({ address: 'bafyreinew' })
  const page = await projects(explorer, ctx)
  await page.querySelector('[data-suggest-create]').click()
  const form = page.querySelector('#project-form')
  assert.ok(!form.hasAttribute('hidden'))
  assert.equal(form.querySelector('#project-name').value, 'demo', 'the runs\' pipeline names it')
  assert.equal(form.querySelector('#project-description').value, 'These 2 runs share 2 input files.')
  assert.match(text(form.querySelector('[data-project-adds]')), /^Adds 2 runs: R[23], R[23]\.$/)
  await form.querySelector('#project-create').click()
  const runs = [member.runs.R2.completion, member.runs.R3.completion].sort()
  assert.deepEqual(calls[0], ['project', { name: form.querySelector('#project-name').value.trim(), description: 'These 2 runs share 2 input files.' }])
  assert.deepEqual(calls.slice(1).map(c => [c[0], c[1], c[2]]).sort(), runs.map(r => ['addToProject', r, 'bafyreinew']))
  assert.equal(ran.at(-1).done.href, '#/project/bafyreinew')
  // New project afterwards starts empty and adds nothing.
  await page.querySelector('#project-new').click()
  assert.equal(form.querySelector('#project-name').value, '')
  assert.ok(form.querySelector('[data-project-adds]').hasAttribute('hidden'))
})

test('a run that could not be added: the page stays, links to the new project, names the run, and a second Create writes nothing', async () => {
  installDom()
  const { explorer, member } = await open()
  const { ctx, ran, calls } = ctxFor({ address: 'bafyreinew', failAdd: member.runs.R3.completion })
  const page = await projects(explorer, ctx)
  await page.querySelector('[data-suggest-create]').click()
  await page.querySelector('#project-create').click()
  const done = ran.at(-1).done
  assert.equal(done.address, 'bafyreinew')
  assert.equal(done.href, undefined, 'the page stays')
  const partial = page.querySelector('[data-project-partial]')
  assert.ok(partial, 'shown in the status, outside the form')
  assert.equal(partial.querySelector('a').getAttribute('href'), '#/project/bafyreinew')
  assert.equal(partial.querySelector('a').textContent, 'demo')
  assert.equal(text(partial), "Created demo, but could not add R3 (not_found). Add it from the project's page.")
  const form = page.querySelector('#project-form')
  assert.ok(form.hasAttribute('hidden'), 'the form closes')
  assert.equal(form.querySelector('#project-name').value, '')
  const projectsWritten = () => calls.filter(c => c[0] === 'project').length
  assert.equal(projectsWritten(), 1)
  await form.querySelector('#project-create').click()
  assert.equal(projectsWritten(), 1, 'no second Project')
})

test('a suggestion of many runs names three and counts the rest; runs of no one pipeline get a plain name', async () => {
  installDom()
  const { explorer } = await open({ many: true })
  const page = await projects(explorer, ctxFor().ctx)
  const s = page.querySelector('[data-suggestion]')
  assert.match(text(s), /These 4 runs share 2 input files: [^:]+ and 1 more/)
  assert.equal(s.querySelectorAll('a').length, 3)
  await s.querySelector('[data-suggest-create]').click()
  assert.equal(page.querySelector('#project-name').value, 'Runs sharing inputs')
  assert.match(text(page.querySelector('[data-project-adds]')), /^Adds 4 runs: .+ and 1 more\.$/)
})

test('no suggestion when the runs are already in one project together', async () => {
  installDom()
  const { explorer } = await open({ together: true })
  const page = await projects(explorer, ctxFor().ctx)
  assert.equal(page.querySelector('[data-suggestion]'), null)
})

test('no suggestion on a snapshot whose runs share no inputs', async () => {
  installDom()
  const { explorer } = await open({ noInputs: true })
  const page = await projects(explorer, ctxFor().ctx)
  assert.equal(page.querySelector('[data-suggestion]'), null)
  assert.equal(page.querySelectorAll('tr[data-project]').length, 2, 'the list itself is unchanged')
})

test('the shared-input query runs once per render', async () => {
  installDom()
  const { explorer } = await open()
  let asked = 0
  const shared = explorer.sharedInputRows.bind(explorer)
  explorer.sharedInputRows = () => { asked++; return shared() }
  await projects(explorer, ctxFor().ctx)
  assert.equal(asked, 1)
})

test('Not in a project: runs and Selections with no current membership, a deleted project\'s included; Pipelines stays a link away', async () => {
  installDom()
  const { explorer, member, ids } = await open()
  const page = await projects(explorer, ctxFor().ctx, { unassigned: true })
  assert.ok(page.hasAttribute('data-unassigned'))
  assert.equal(page.querySelector('h1').textContent, 'Not in a project')
  assert.deepEqual(page.querySelectorAll('[data-unassigned-run]').map(r => r.dataset.unassignedRun), [member.runs.R3.completion],
    'R1 and R2 are in P1; R3 only in the deleted P3')
  const r3 = page.querySelector('[data-unassigned-run]')
  assert.equal(r3.querySelector('a').getAttribute('href'), `#/run/${member.runs.R3.completion}`)
  assert.ok(r3.querySelector('a[href="#/pipeline/demo"]'))
  assert.deepEqual(page.querySelectorAll('[data-unassigned-selection]').map(r => r.dataset.unassignedSelection), [ids.S2])
  assert.match(text(page.querySelector('[data-unassigned-selection]')), /loose BAMs/)
  assert.ok(page.querySelector('a[href="#/"]'), 'All pipelines')
  assert.match(text(page), /1 run and 1 Selection in no project/)
  assert.deepEqual(strayTexts(page), [])
})

test('Not in a project when the check was cut: "All the runs checked", never "Every run"', async () => {
  installDom()
  const { explorer, member } = await open()
  const page1 = explorer.runPage.bind(explorer)
  explorer.runPage = async (p, o) => {
    const page = await page1(p, o)
    return { ...page, rows: page.rows.filter(r => r.completion_cid !== member.runs.R3.completion), next: 50 }
  }
  const page = await projects(explorer, ctxFor().ctx, { unassigned: true })
  const none = page.querySelector('[data-unassigned-none]')
  assert.equal(none.textContent, 'All the runs checked are in a project.')
  assert.doesNotMatch(text(page), /Every run/)
  assert.match(text(page), /among each pipeline's newest 50/)
  assert.ok(page.querySelector('a[href="#/pipeline/demo"]'))
})

test('Not in a project in all-stores mode says which store it lists', async () => {
  installDom()
  const { explorer } = await open()
  const list = [{ alias: 'lab', key: 'a', writable: true, base: 'm/lab/' }, { alias: 'org', key: 'b', writable: false, base: 'm/org/' }]
  const members = createMembers({ member: 'lab', all: true, members: list }, { openExplorer: async () => explorer })
  const { ctx } = ctxFor()
  Object.assign(ctx, { members, store: { name: 'lab', key: 'a', all: true, hrefIn: (alias, hash) => hash } })
  const page = await projects(explorer, ctx, { unassigned: true })
  assert.deepEqual(page.querySelectorAll('.ph').map(m => m.querySelector('.muted').textContent), ['in lab', 'in lab'])
})

test('a project the snapshot has no project_stats row for: each figure is a dash that says so', async () => {
  installDom()
  const { explorer } = await open({ noStats: true })
  const page = await projects(explorer, ctxFor().ctx)
  await settle()
  const figures = page.querySelectorAll('[data-store-figure]')
  assert.equal(figures.length, 2)
  for (const f of figures) {
    assert.equal(f.dataset.bytes, undefined)
    assert.equal(f.getAttribute('title'), "lab's snapshot has no figure for this project")
  }
})
