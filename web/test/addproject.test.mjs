// "Add to project" (P4 Task 4, views/addproject.js) on the run, Selection
// and item pages and on a sample's row of the run's Samples tab: the
// project tags in the head (who and when), Add (a membership Claim whose
// value is a link to the project), Remove (it supersedes), New project then
// Add, a Selection going into a project that pins every Selection (its bytes
// first; no pin when the writer pins it already), read-only pages, deleted
// projects left out of the chooser, and what the tags cost in snapshot
// queries. A real Explorer over the fixture member on the plugin's
// schema.sql; the writer is a fake that records its calls.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as dagJson from '@ipld/dag-json'
import { CID } from 'multiformats/cid'
import { frame, installDom, strayTexts } from './dom.mjs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { Tray } from '../src/tray.js'
import { resetFolds } from '../src/folds.js'
import { writer } from '../src/write.js'
import { bytes, sizeText } from '../src/format.js'
import { run } from '../src/views/run.js'
import { selection } from '../src/views/selection.js'
import { item } from '../src/views/item.js'
import { samplesTab } from '../src/views/samples.js'
import { snapshotDb } from './helpers.mjs'
import { blockFetch, buildMember, entryName, rawCid } from './fixture.mjs'
import SQL from '../src/queries.json' with { type: 'json' }

const NOW = Date.parse('2026-10-25T12:00:00.000Z')
const PIPE = 'nf-core/rnaseq'
const VIEWER = 'rob@x'
const at = (minutes) => new Date(NOW - minutes * 60_000).toISOString()
const claim = (subject, verb, attribute, value, { supersedes = [], by = 'ana@x', minutes = 60 } = {}) =>
  ({ kind: 'Claim', schema: 1, asserted_by: by, subject, verb, attribute, value, supersedes, timestamp: at(minutes) })
const leaf = (name, address) => ({ kind: 'Leaf', name, address, size: 10, provider: 'head-node', reason: null })

/**
 * Projects P ("RAP1 depletion", in the tail; with `retention` it pins every
 * Selection), N ("Atlas", in the tail) and Q ("Old study", in the snapshot,
 * deleted there). R1 (the fixture's snapshot run) is in P (ana@x) and in Q;
 * item A of R1 is in P (ben@x); Selection S (WT_REP1 of the tail run Ra) is
 * in N (ben@x), and ben@x pins 5,000 bytes of it here. P names sample
 * WT_REP1 of nf-core/rnaseq. With `mine` the viewer pins S too (in the
 * snapshot, so the storage figures stay current). `record` collects every
 * snapshot query as `{ sql, params }`. `defaults` sets a project's default
 * store (`{ P: 'org' }`); with `alsoInP` S is in P too (ana@x), and with
 * `leftP` ben@x then removed it.
 */
async function open({ retention = false, mine = false, record = null, defaults = {}, alsoInP = false, leftP = false } = {}) {
  const ids = {}
  const ra = {}
  const member = await buildMember({
    now: NOW,
    extra: ({ put, runs: demo, item: demoItem }) => {
      let tick = 0
      const log = []
      const entry = (kind, cid) => { log.push(entryName(NOW - 200_000 + (tick++) * 1000, kind, cid)); return cid }
      const m = put({ kind: 'RunManifest', schema: 1, asserted_by: 'ana@x', pipeline: PIPE, repository: null, revision: null, commit_id: null,
        run_name: 'Ra', nf_run_hash: 'hash-Ra', session_id: 's', resumed: false, nextflow_version: '26.04.6', params: {}, config: {},
        script: null, started_at: '2026-10-01T10:00:00.000Z' })
      const items = Object.fromEntries(['WT_REP1', 'KO_REP1'].map((id, i) =>
        [id, put({ kind: 'OutputItem', schema: 1, value: [{ meta: { id }, num_trimmed_reads: 1000 * (i + 1) }, leaf(`${id}.fastq.gz`, rawCid(`fq-${id}`))] })]))
      const sorted = Object.values(items).sort((a, b) => (a.toString() < b.toString() ? -1 : 1))
      const coll = put({ kind: 'OutputCollection', schema: 1, asserted_by: 'ana@x', run: m, name: 'preprocessed', items: sorted, paths: sorted.map((_, i) => [`p/${i}`]) })
      const comp = put({ kind: 'RunCompletion', schema: 1, asserted_by: 'ana@x', run: m, collections: [coll], input_set: null, status: 'succeeded',
        exit_status: 0, possibly_incomplete: false, started_at: '2026-10-01T10:00:00.000Z', finished_at: '2026-10-01T11:00:00.000Z',
        anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }, error: null })
      entry('run', comp)
      Object.assign(ra, { completion: comp.toString(), collection: coll.toString() })
      ids.P = entry('project', put({ kind: 'Project', schema: 1, asserted_by: 'ana@x', name: 'RAP1 depletion', description: '', created_at: at(600) }))
      ids.N = entry('project', put({ kind: 'Project', schema: 1, asserted_by: 'ben@x', name: 'Atlas', description: '', created_at: at(500) }))
      ids.Q = put({ kind: 'Project', schema: 1, asserted_by: 'ana@x', name: 'Old study', description: '', created_at: at(900) })
      ids.S = entry('selection', put({ kind: 'Selection', schema: 1, asserted_by: 'ben@x', derived_from: [],
        members: [{ item: { address: items.WT_REP1, via: [coll] } }] }))
      const c = (key, value) => { ids[key] = entry('claim', put(value)); return ids[key] }
      const R1 = CID.parse(demo.R1.completion)
      c('MR1', claim(R1, 'add', 'project', ids.P, { minutes: 200 }))
      c('MRQ', claim(R1, 'add', 'project', ids.Q, { minutes: 300 }))
      c('MA', claim(demoItem.A, 'add', 'project', ids.P, { by: 'ben@x', minutes: 190 }))
      c('MS', claim(ids.S, 'add', 'project', ids.N, { by: 'ben@x', minutes: 180 }))
      c('SA', claim(ids.P, 'add', 'sample', { pipeline: PIPE, id: 'WT_REP1' }, { minutes: 170 }))
      if (retention) c('RET', claim(ids.P, 'set', 'policy.retention', 'pin_selections', { minutes: 90 }))
      if (alsoInP || leftP) c('MSP', claim(ids.S, 'add', 'project', ids.P, { minutes: 175 }))
      if (leftP) c('DSP', claim(ids.S, 'del', 'project', null, { supersedes: [ids.MSP], by: 'ben@x', minutes: 170 }))
      for (const [key, alias] of Object.entries(defaults)) c(`DS${key}`, claim(ids[key], 'set', 'policy.default_store', alias, { minutes: 80 }))
      return {
        log,
        rows: (db) => {
          const row = (sql, bind) => db.exec({ sql, bind })
          row('INSERT INTO project VALUES (?, ?, ?, ?, ?)', [ids.Q.toString(), 'Old study', '', 'ana@x', at(900)])
          ids.DQ = rawCid('delete-Q')
          row('INSERT INTO claim VALUES (?, ?, ?, ?, ?, ?, ?)', [ids.DQ.toString(), ids.Q.toString(), 'delete', null, null, at(400), 'ana@x'])
          row('INSERT INTO claim_current VALUES (?, ?, ?, ?, 0)', [ids.Q.toString(), null, null, ids.DQ.toString()])
          row('INSERT INTO pinned_overlap VALUES (?, ?, ?)', [ids.S.toString(), 'ben@x', 5000])
          if (mine) {
            ids.PIN = rawCid('pin-S-mine')
            row('INSERT INTO claim VALUES (?, ?, ?, ?, ?, ?, ?)', [ids.PIN.toString(), ids.S.toString(), 'add', 'pin', 'mine', at(500), VIEWER])
            row('INSERT INTO claim_current VALUES (?, ?, ?, ?, 0)', [ids.S.toString(), 'pin', 'mine', ids.PIN.toString()])
          }
        },
      }
    },
  })
  const explorer = await Explorer.open({ base: 'http://h/m/lab/', blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    openDb: async () => {
      const db = await snapshotDb(member.snapshot)
      return record ? { ...db, query: (sql, params) => { record.push({ sql, params }); return db.query(sql, params) } } : db
    },
    listFn: async () => ({ names: member.log, readable: true }), now: () => NOW })
  return { explorer, member, ra, ids: Object.fromEntries(Object.entries(ids).map(([k, v]) => [k, v.toString()])) }
}

/**
 * A ctx whose writer records each call (or is `real`), writing where the page
 * is unless `can` is false; `fail` refuses every call of that name, `failOnce`
 * the first of each name it lists. With `members` (P3's members), Copy to… is a link.
 * `used` collects what the page gives the rail (ctx.use).
 */
function ctxFor({ can = true, real = null, fail = null, failOnce = [], members = null } = {}) {
  const calls = []
  const failing = new Set(failOnce)
  const record = (name) => async (...args) => {
    calls.push([name, ...args])
    if (fail === name || failing.delete(name)) throw Object.assign(new Error('not_found'), { code: 'not_found' })
    return { address: `bafy-${name}` }
  }
  const fake = Object.fromEntries(['addToProject', 'removeFromProject', 'addSample', 'removeSample', 'project', 'pin'].map(n => [n, record(n)]))
  const ran = []
  const used = []
  const ctx = { mark: () => {}, use: (v) => used.push(v), trayChanged: () => {}, rerender: () => {}, tray: new Tray(null), viewer: VIEWER,
    store: { name: 'lab', key: 'a', all: false }, ...(members ? { members, copy: { available: true, reason: null } } : {}),
    write: { available: can, here: can, writable: 'lab', reason: can ? null : 'Read only here.', hrefFor: (x) => x, writer: real ?? fake,
      run: async (status, fn) => {
        try { ran.push({ done: await fn() }) } catch (e) { ran.push({ error: e }); status.replaceChildren(e.message) }
      } } }
  return { ctx, calls, ran, used }
}

const text = (el) => el.textContent.replace(/\s+/g, ' ').trim()
async function until(fn, what) {
  for (let i = 0; i < 100; i++) { if (fn()) return; await frame() }
  assert.fail(`timed out waiting for ${what}`)
}
const fresh = () => { installDom(); resetFolds() }
/** The control's panel once its projects are read. */
async function opened(page) {
  await page.querySelector('[data-project-control-open]').click()
  const node = page.querySelector('[data-project-control]')
  await until(() => node.querySelector('[data-project-memberships]'), 'the memberships')
  return node
}
const options = (node) => node.querySelector('[data-project-choose]').querySelectorAll('option').map(o => o.textContent)
const tagsOf = (page) => page.querySelector('.subline').querySelectorAll('[data-project-tag]')

test('run page: a tag per membership in the subline, with who and when and the project linked; a deleted project says so', async () => {
  fresh()
  const { explorer, member, ids } = await open()
  const page = await run(explorer, member.runs.R1.completion, ctxFor().ctx, { tab: 'outputs' })
  await until(() => tagsOf(page).length === 2 && tagsOf(page).every(t => !/bafy/.test(t.querySelector('[data-project-name]').textContent)), 'the named tags')
  const [inP, inQ] = [ids.P, ids.Q].map(p => tagsOf(page).find(t => t.dataset.projectTag === p))
  assert.equal(inP.dataset.claim, ids.MR1)
  assert.equal(inP.querySelector('.verb').textContent, 'added to')
  assert.equal(inP.querySelector('a').textContent, 'RAP1 depletion')
  assert.equal(inP.querySelector('.av').getAttribute('title'), 'ana@x')
  assert.equal(inP.querySelector('a').getAttribute('href'), `#/project/${ids.P}`)
  assert.equal(inP.querySelector('.age').getAttribute('title'), at(200))
  assert.match(text(inQ), /Old study.*\(deleted\)/)
  // Add to project… sits beside Copy to….
  const labels = page.querySelector('.page-head').querySelectorAll('.btn').map(b => b.textContent)
  assert.deepEqual(labels.slice(-2), ['Add to project…', 'Copy to…'])
  assert.deepEqual(strayTexts(page), [])
})

test('run page: Add writes the membership Claim for the chosen project, Remove supersedes, and the tab is drawn again', async () => {
  fresh()
  const { explorer, member, ids } = await open()
  const R1 = member.runs.R1.completion
  const { ctx, calls, ran } = ctxFor()
  const page = await run(explorer, R1, ctx, { tab: 'outputs' })
  const node = await opened(page)
  assert.deepEqual(node.querySelectorAll('[data-membership]').map(li => li.dataset.membership).sort(), [ids.P, ids.Q].sort())
  const p = node.querySelector(`[data-membership="${ids.P}"]`)
  assert.match(text(p), /RAP1 depletion/)
  assert.equal(p.querySelector('.claim').querySelector('.av').getAttribute('title'), 'ana@x')
  // Not in it yet and not deleted: only Atlas.
  assert.deepEqual(options(node), ['Atlas'])
  await node.querySelector('[data-project-add-go]').click()
  assert.deepEqual(calls, [['addToProject', R1, ids.N]])
  assert.deepEqual(ran.at(-1).done, { href: `#/run/${R1}?tab=outputs` })
  await p.querySelector('[data-project-remove]').click()
  assert.deepEqual(calls[1], ['removeFromProject', R1, [ids.MR1]])
})

test('Add sends the project as a CID link, through the real writer', async () => {
  fresh()
  const { explorer, member, ids } = await open()
  const posted = []
  const fetchFn = async (url, init) => {
    posted.push(dagJson.decode(init.body))
    return new Response(dagJson.encode({ address: rawCid('written'), written: true }), { headers: { 'Content-Type': 'application/vnd.ipld.dag-json' } })
  }
  const { ctx } = ctxFor({ real: writer({ endpoint: 'http://h/api/put', token: 't', fetchFn, now: () => new Date(NOW) }) })
  const page = await run(explorer, member.runs.R1.completion, ctx, { tab: 'outputs' })
  const node = await opened(page)
  await node.querySelector('[data-project-add-go]').click()
  const [body] = posted
  assert.equal(body.attribute, 'project')
  assert.equal(body.verb, 'add')
  assert.ok(body.value instanceof CID, 'the value is a link')
  assert.equal(body.value.toString(), ids.N)
  assert.equal(body.subject.toString(), member.runs.R1.completion)
})

test('Selection page: tags, and a deleted project is never offered', async () => {
  fresh()
  const { explorer, ids } = await open()
  const page = await selection(explorer, ids.S, ctxFor().ctx)
  await until(() => tagsOf(page).length === 1 && /Atlas/.test(text(tagsOf(page)[0])), 'the tag')
  assert.equal(tagsOf(page)[0].querySelector('.av').getAttribute('title'), 'ben@x')
  const node = await opened(page)
  assert.deepEqual(options(node), ['RAP1 depletion'], 'Old study is deleted, Atlas holds it already')
  assert.equal(node.querySelector('[data-add-policy]').hasAttribute('hidden'), true, 'P does not pin Selections')
})

test('Selection page, project pins every Selection: the bytes and who pins it already show first, then add and pin', async () => {
  fresh()
  const { explorer, ids } = await open({ retention: true })
  const { ctx, calls } = ctxFor()
  const page = await selection(explorer, ids.S, ctx)
  const node = await opened(page)
  const add = node.querySelector('[data-project-add-go]')
  await until(() => !add.disabled, 'the bytes')
  assert.equal(add.textContent, `Add and pin ${sizeText(await explorer.selectionSize(ids.S))} in lab`)
  assert.equal(text(node.querySelector('[data-add-pin-note]')), `${bytes(5000)} already pinned here by ben@x`)
  assert.match(text(node.querySelector('[data-add-policy]')), /pins every Selection in it, in lab\. The pin's note is "project policy: RAP1 depletion"/)
  await add.click()
  assert.deepEqual(calls, [['addToProject', ids.S, ids.P], ['pin', ids.S, 'project policy: RAP1 depletion']])
})

test('Selection page, project pins every Selection, the writer pins it already: Add, and no pin', async () => {
  fresh()
  const { explorer, ids } = await open({ retention: true, mine: true })
  const { ctx, calls } = ctxFor()
  const page = await selection(explorer, ids.S, ctx)
  const node = await opened(page)
  const add = node.querySelector('[data-project-add-go]')
  await until(() => !add.disabled, 'the decision')
  assert.equal(add.textContent, 'Add')
  assert.match(text(node.querySelector('[data-add-pin-note]')), /Already pinned by you, so adding it writes no pin/)
  await add.click()
  assert.deepEqual(calls, [['addToProject', ids.S, ids.P]])
})

test('item page: its tag, and New project… writes the Project, then adds the item to it', async () => {
  fresh()
  const { explorer, member, ids } = await open()
  const A = member.item.A
  const { ctx, calls, ran } = ctxFor()
  const page = await item(explorer, member.runs.R1.collection, A, ctx)
  await until(() => tagsOf(page).length === 1 && /RAP1 depletion/.test(text(tagsOf(page)[0])), 'the tag')
  assert.equal(tagsOf(page)[0].dataset.claim, ids.MA)
  assert.equal(tagsOf(page)[0].querySelector('.av').getAttribute('title'), 'ben@x')
  assert.ok(page.querySelector('.page-head').querySelector('[data-project-control-open]'))
  const node = await opened(page)
  assert.deepEqual(options(node), ['Atlas'])
  assert.ok(node.querySelector('[data-project-new]').hasAttribute('hidden'), 'New project… stays closed until pressed')
  await node.querySelector('[data-project-new-open]').click()
  const form = node.querySelector('[data-project-new]')
  assert.equal(form.hasAttribute('hidden'), false)
  await form.querySelector('[data-project-new-create]').click()
  assert.deepEqual(calls, [], 'no name, nothing written')
  assert.match(text(node.querySelector('[data-project-status]')), /type a name first/)
  form.querySelector('[data-project-new-name]').value = '  Yeast atlas '
  form.querySelector('[data-project-new-description]').value = 'every yeast run'
  await form.querySelector('[data-project-new-create]').click()
  assert.deepEqual(calls, [['project', { name: 'Yeast atlas', description: 'every yeast run' }], ['addToProject', A, 'bafy-project']])
  assert.deepEqual(ran.at(-1).done, { href: `#/item/${member.runs.R1.collection}/${A}` })
})

test('New project… whose membership fails: the project is named, the form empties, and the control is drawn again', async () => {
  fresh()
  const { explorer, member } = await open()
  const { ctx, calls, ran } = ctxFor({ fail: 'addToProject' })
  const page = await item(explorer, member.runs.R1.collection, member.item.B, ctx)
  const node = await opened(page)
  assert.equal(text(node.querySelector('[data-project-memberships]')), 'This item is in no project yet.')
  await node.querySelector('[data-project-new-open]').click()
  node.querySelector('[data-project-new-name]').value = 'Atlas 2'
  await node.querySelector('[data-project-new-create]').click()
  assert.deepEqual(calls.map(c => c[0]), ['project', 'addToProject'])
  const stay = ran.at(-1).done.stay
  assert.equal(stay.dataset.projectPartial, 'bafy-project')
  assert.match(text(stay), /^Created Atlas 2, but could not add this item to it \(not_found\)\. Choose it above to try again\.$/)
  assert.equal(stay.querySelector('a').getAttribute('href'), '#/project/bafy-project')
  assert.equal(node.querySelector('[data-project-new]').hasAttribute('hidden'), true, 'drawn again, closed')
  assert.equal(node.querySelector('[data-project-new-name]').value ?? '', '', 'and empty')
})

test('read only: the run, Selection and item pages show their tags, with no Add or Remove', async () => {
  fresh()
  const { explorer, member, ids } = await open()
  const { ctx } = ctxFor({ can: false })
  for (const page of [await run(explorer, member.runs.R1.completion, ctx, { tab: 'outputs' }), await selection(explorer, ids.S, ctx),
    await item(explorer, member.runs.R1.collection, member.item.A, ctx)]) {
    await until(() => tagsOf(page).length > 0, 'the tags')
    assert.equal(page.querySelector('[data-project-control-open]'), null)
    assert.equal(page.querySelector('[data-project-control]'), null)
    assert.equal(page.querySelector('[data-project-remove]'), null)
  }
})

/** The run Ra's Samples tab, drawn. */
async function samplesOf(explorer, ra, ctx) {
  globalThis.history = { replaceState: () => {} }
  const { collections } = await explorer.run(ra.completion)
  const node = samplesTab(explorer, ra.completion, ctx, { pipeline: PIPE, collections })
  await until(() => node.querySelector('[data-samples-table]'), 'the samples table')
  return node
}

test('a sample row: a small link beside its name opens its projects; Add writes add sample, Remove supersedes', async () => {
  fresh()
  const { explorer, ra, ids } = await open()
  let reads = 0
  const projects = explorer.projects.bind(explorer)
  explorer.projects = (o) => { reads++; return projects(o) }
  const { ctx, calls, ran } = ctxFor()
  const tab = await samplesOf(explorer, ra, ctx)
  const row = (s) => tab.querySelectorAll('tr').find(r => r.dataset.sample === s)
  const action = row('WT_REP1').querySelector('[data-sample-project]')
  assert.ok(action.classList.contains('cell-filter'), "drawn as the table's other small row links")
  assert.equal(action.getAttribute('title'), 'Add WT_REP1 to a project')
  assert.equal(reads, 0, 'nothing is read until a row asks')
  await action.click()
  const area = tab.querySelector('[data-sample-projects]')
  assert.equal(area.hasAttribute('hidden'), false)
  await until(() => area.querySelector('[data-project-memberships]'), 'the memberships')
  assert.match(text(area.querySelector('.ph')), /Projects with WT_REP1/)
  const inP = area.querySelector(`[data-membership="${ids.P}"]`)
  assert.match(text(inP), /RAP1 depletion/)
  assert.equal(inP.querySelector('.claim').querySelector('.av').getAttribute('title'), 'ana@x')
  assert.deepEqual(options(area), ['Atlas'], 'not P (it has the sample), not Old study (deleted)')
  await area.querySelector('[data-project-add-go]').click()
  assert.deepEqual(calls, [['addSample', ids.N, { pipeline: PIPE, id: 'WT_REP1' }]])
  assert.match(text(ran.at(-1).done.stay), /Added WT_REP1 to Atlas\./)
  assert.equal(reads, 2, 'read once on opening, and again after the write')
  await until(() => area.querySelector('[data-project-remove]'), 'drawn again')
  await area.querySelector(`[data-membership="${ids.P}"]`).querySelector('[data-project-remove]').click()
  assert.deepEqual(calls[1], ['removeSample', ids.P, [ids.SA]])
  // Another row: in no project, both live projects offered, the projects not read again.
  const before = reads
  await row('KO_REP1').querySelector('[data-sample-project]').click()
  await until(() => area.querySelector('[data-project-memberships]')?.textContent === 'KO_REP1 is in no project yet.', 'KO_REP1')
  assert.deepEqual(options(area).sort(), ['Atlas', 'RAP1 depletion'])
  assert.equal(reads, before, 'one read per page')
})

test('a sample row, read only: its projects with who and when, and no Add or Remove', async () => {
  fresh()
  const { explorer, ra, ids } = await open()
  const tab = await samplesOf(explorer, ra, ctxFor({ can: false }).ctx)
  const action = tab.querySelectorAll('tr').find(r => r.dataset.sample === 'WT_REP1').querySelector('[data-sample-project]')
  assert.equal(action.getAttribute('title'), 'Projects with WT_REP1')
  await action.click()
  const area = tab.querySelector('[data-sample-projects]')
  await until(() => area.querySelector('[data-project-memberships]'), 'the memberships')
  assert.ok(area.querySelector(`[data-membership="${ids.P}"]`).querySelector('.claim'))
  assert.equal(area.querySelector('[data-project-remove]'), null)
  assert.equal(area.querySelector('[data-project-choose]'), null)
  assert.equal(area.querySelector('[data-project-new-open]'), null)
})

// ---- what the tags cost ------------------------------------------------------------

/** The snapshot queries one page asks after the Explorer is open (and its tail read). */
async function queriesOf(draw, options = {}) {
  const record = []
  const opened = await open({ ...options, record })
  record.length = 0
  const page = await draw(opened)
  await frame(); await frame()
  return { page, record }
}
const aboutProjects = (record) => record.filter(q => /\bproject\b|attribute = 'project'/.test(q.sql))

test('the tags read the subject\'s Claims no more than the page did before, and the project names in one query', async () => {
  fresh()
  const claimsReads = (record, cid) => record.filter(q => q.sql === SQL.claimsOf && JSON.parse(q.params[0]).includes(cid)).length
  const inP = await queriesOf(({ explorer, member }) => item(explorer, member.runs.R1.collection, member.item.A, ctxFor().ctx))
  await until(() => tagsOf(inP.page).length === 1, 'the tag')
  assert.deepEqual(aboutProjects(inP.record).map(q => q.sql), [SQL.projects], 'one read of the projects, no membership query')
  // The same page for an item in no project: the item's Claims are read as often, so the tags take them from the page's read.
  fresh()
  const none = await queriesOf(({ explorer, member }) => item(explorer, member.runs.R1.collection, member.item.B, ctxFor().ctx))
  const { member } = await open()
  assert.equal(claimsReads(inP.record, member.item.A), claimsReads(none.record, member.item.B))
  assert.equal(claimsReads(inP.record, member.item.A), 1, "one read of the item's Claims: the page's own")
})

test('a subject in no project: the tags ask nothing', async () => {
  fresh()
  const { page, record } = await queriesOf(({ explorer, member }) => item(explorer, member.runs.R1.collection, member.item.B, ctxFor().ctx))
  assert.equal(page.querySelector('[data-project-tags]'), null)
  assert.deepEqual(aboutProjects(record), [])
})

// ---- Copy to… and the default store (plan 1.7, ruling Q1) ------------------------------------

/** lab is open; org takes copies; ro is read-only here. */
const MEMBERS = { primary: 'lab', list: [{ alias: 'lab', key: 'a', copy_target: false }, { alias: 'org', key: 'b', copy_target: true },
  { alias: 'ro', key: 'c', copy_target: false, copy_reason: 'readOnly is set for this store' }] }
const copyHrefOf = (page) => page.querySelector('[data-copy-to]').getAttribute('href')
/** The page once its project tags are named, and a few frames more, so Copy to… has had every chance to change. */
async function settled(page) {
  await until(() => tagsOf(page).length && tagsOf(page).every(t => !/bafy/.test(t.querySelector('[data-project-name]').textContent)), 'the named tags')
  for (let i = 0; i < 5; i++) await frame()
}

test('Copy to… on a run in one project (and a deleted one) preselects its default store, a copy target here', async () => {
  fresh()
  const { explorer, member } = await open({ defaults: { P: 'org', Q: 'ro' } })
  const R1 = member.runs.R1.completion
  const page = await run(explorer, R1, ctxFor({ members: MEMBERS }).ctx, { tab: 'outputs' })
  assert.equal(copyHrefOf(page), null, 'nothing until the projects are read')
  await until(() => copyHrefOf(page) === `#/run/${R1}/copy?to=org`, 'the default store')
})

test('Copy to… on a Selection preselects its project\'s default store', async () => {
  fresh()
  const { explorer, ids } = await open({ defaults: { N: 'org' } })
  const page = await selection(explorer, ids.S, ctxFor({ members: MEMBERS }).ctx)
  await until(() => copyHrefOf(page) === `#/selection/${ids.S}/copy?to=org`, 'the default store')
})

test('a default store not configured here, or read-only here: Copy to… names the project for the copy page\'s note, and preselects no store', async () => {
  for (const alias of ['elsewhere', 'ro', 'lab']) {
    fresh()
    const { explorer, member, ids } = await open({ defaults: { P: alias } })
    const R1 = member.runs.R1.completion
    const page = await run(explorer, R1, ctxFor({ members: MEMBERS }).ctx, { tab: 'outputs' })
    await until(() => copyHrefOf(page) === `#/run/${R1}/copy?project=${ids.P}`, `the project, for ${alias}`)
  }
})

test('a Selection in two projects: different default stores preselect nothing, the same one is preselected', async () => {
  fresh()
  const two = await open({ defaults: { P: 'org', N: 'ro' }, alsoInP: true })
  const page = await selection(two.explorer, two.ids.S, ctxFor({ members: MEMBERS }).ctx)
  await settled(page)
  assert.equal(tagsOf(page).length, 2)
  // Both projects go to the copy page, so it preselects no store (P3's first target would be one neither names) and says why.
  const href = copyHrefOf(page)
  assert.match(href, new RegExp(`^#/selection/${two.ids.S}/copy\\?project=[^&]+$`))
  assert.deepEqual(href.slice(href.indexOf('=') + 1).split(',').sort(), [`${two.ids.P}`, `${two.ids.N}`].sort())
  fresh()
  const same = await open({ defaults: { P: 'org', N: 'org' }, alsoInP: true })
  const again = await selection(same.explorer, same.ids.S, ctxFor({ members: MEMBERS }).ctx)
  await until(() => copyHrefOf(again) === `#/selection/${same.ids.S}/copy?to=org`, 'the shared default store')
})

test('a click on Copy to… before the projects are read waits for them, so it opens the copy page at the default store', async () => {
  fresh()
  const { explorer, member } = await open({ defaults: { P: 'org', Q: 'ro' } })
  const R1 = member.runs.R1.completion
  const saved = globalThis.location
  globalThis.location = { hash: `#/run/${R1}` }
  try {
    const page = await run(explorer, R1, ctxFor({ members: MEMBERS }).ctx, { tab: 'outputs' })
    const a = page.querySelector('[data-copy-to]')
    assert.equal(copyHrefOf(page), null, 'not aimed yet: no href, so no modifier-click reaches an unaimed copy page')
    assert.equal(a.getAttribute('aria-busy'), null)
    assert.equal(a.getAttribute('tabindex'), '0', 'keyboard-reachable while waiting')
    assert.equal(a.getAttribute('role'), 'link')
    let prevented = false
    const click = (extra = {}) => Promise.all(a.listeners.get('click').map(fn => fn({ currentTarget: a, target: a, preventDefault() { prevented = true }, ...extra })))
    const waiting = click()
    assert.equal(prevented, true, 'the early click does not follow the unaimed link')
    assert.equal(a.getAttribute('aria-busy'), 'true', 'the early click shows it is waiting')
    await waiting
    await until(() => globalThis.location.hash === `#/run/${R1}/copy?to=org`, 'the copy page at the default store')
    assert.equal(a.getAttribute('aria-busy'), null, 'cleared once aimed')
    assert.equal(a.getAttribute('tabindex'), null)
    assert.equal(a.getAttribute('role'), null)
    // Once aimed, a click is an ordinary link's.
    prevented = false
    await click()
    assert.equal(prevented, false)
  } finally {
    globalThis.location = saved
  }
})

test('a modifier-click before the link is aimed cannot reach a preselected target', async () => {
  fresh()
  const { explorer, member } = await open({ defaults: { P: 'org', Q: 'ro' } })
  const R1 = member.runs.R1.completion
  const saved = globalThis.location
  globalThis.location = { hash: `#/run/${R1}` }
  try {
    const page = await run(explorer, R1, ctxFor({ members: MEMBERS }).ctx, { tab: 'outputs' })
    const a = page.querySelector('[data-copy-to]')
    assert.equal(a.getAttribute('href'), null, 'a new tab has nowhere to go')
    let prevented = 0
    for (const extra of [{ metaKey: true }, { ctrlKey: true }, { shiftKey: true }, { button: 1 }]) {
      await Promise.all(a.listeners.get('click').map(fn => fn({ currentTarget: a, target: a, preventDefault() { prevented++ }, ...extra })))
    }
    assert.equal(prevented, 4)
    assert.equal(a.getAttribute('aria-busy'), null, 'only a plain click waits')
    await until(() => copyHrefOf(page) === `#/run/${R1}/copy?to=org`, 'the aim')
    assert.equal(globalThis.location.hash, `#/run/${R1}`, 'no modifier-click navigated')
  } finally {
    globalThis.location = saved
  }
})

test('Enter on Copy to… before it is aimed acts as an early click', async () => {
  fresh()
  const { explorer, member } = await open({ defaults: { P: 'org', Q: 'ro' } })
  const R1 = member.runs.R1.completion
  const saved = globalThis.location
  globalThis.location = { hash: `#/run/${R1}` }
  try {
    const page = await run(explorer, R1, ctxFor({ members: MEMBERS }).ctx, { tab: 'outputs' })
    const a = page.querySelector('[data-copy-to]')
    let prevented = false
    a.listeners.get('keydown').forEach(fn => fn({ key: 'Enter', preventDefault() { prevented = true } }))
    assert.equal(prevented, true)
    assert.equal(a.getAttribute('aria-busy'), 'true')
    await until(() => globalThis.location.hash === `#/run/${R1}/copy?to=org`, 'the copy page at the default store')
  } finally {
    globalThis.location = saved
  }
})

test('an early click whose page was left meanwhile goes nowhere', async () => {
  fresh()
  const { explorer, member } = await open({ defaults: { P: 'org' } })
  const R1 = member.runs.R1.completion
  const saved = globalThis.location
  globalThis.location = { hash: `#/run/${R1}` }
  try {
    const page = await run(explorer, R1, ctxFor({ members: MEMBERS }).ctx, { tab: 'outputs' })
    const a = page.querySelector('[data-copy-to]')
    await Promise.all(a.listeners.get('click').map(fn => fn({ currentTarget: a, target: a, preventDefault() {} })))
    globalThis.location.hash = '#/selections'
    await until(() => copyHrefOf(page) === `#/run/${R1}/copy?to=org`, 'the aim')
    for (let i = 0; i < 5; i++) await frame()
    assert.equal(globalThis.location.hash, '#/selections')
  } finally {
    globalThis.location = saved
  }
})

test('no default store: Copy to… stays as it is', async () => {
  fresh()
  const { explorer, member } = await open()
  const R1 = member.runs.R1.completion
  const page = await run(explorer, R1, ctxFor({ members: MEMBERS }).ctx, { tab: 'outputs' })
  await settled(page)
  assert.equal(copyHrefOf(page), `#/run/${R1}/copy`)
})

test('the Selection\'s Claims rail: a membership reads "added to" or "removed from" its project, named and linked', async () => {
  fresh()
  const { explorer, ids } = await open({ leftP: true })
  const { ctx, used } = ctxFor()
  await selection(explorer, ids.S, ctx)
  const rows = used.at(-1).claims
  const row = (cid) => rows.find(r => r.cid === cid)
  assert.equal(row(ids.MS).verb, 'added to')
  assert.equal(row(ids.DSP).verb, 'removed from')
  assert.equal(row(ids.MSP).verb, 'added to')
  assert.equal(row(ids.MSP).superseded, true)
  await until(() => row(ids.MS).value.textContent === 'Atlas' && row(ids.DSP).value.textContent === 'RAP1 depletion', 'the project names')
  assert.equal(row(ids.MS).value.getAttribute('href'), `#/project/${ids.N}`)
  assert.equal(row(ids.DSP).value.getAttribute('href'), `#/project/${ids.P}`)
})

test('Selection page, project pins every Selection, the pin refused: the retry pins, and writes no second membership', async () => {
  fresh()
  const { explorer, ids } = await open({ retention: true })
  const { ctx, calls, ran } = ctxFor({ failOnce: ['pin'] })
  const page = await selection(explorer, ids.S, ctx)
  const node = await opened(page)
  const add = node.querySelector('[data-project-add-go]')
  await until(() => !add.disabled, 'the bytes')
  await add.click()
  assert.ok(ran.at(-1).error, 'the pin was refused')
  await add.click()
  const pin = ['pin', ids.S, 'project policy: RAP1 depletion']
  assert.deepEqual(calls, [['addToProject', ids.S, ids.P], pin, pin])
})
