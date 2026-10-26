// #/project/<cid> (P4 Task 3, mockups/13-project.html): the head and tabs,
// storage per store, the Samples tab's grouping across runs, annotations,
// merge and split, the input-reads basis, the retention policy's pins, the
// default store's notes, Remove, and the Activity tab. A real Explorer over
// the fixture member on the plugin's schema.sql, with three tail runs of
// nf-core/rnaseq; the writer is a fake that records its calls.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { frame, installDom, strayTexts } from './dom.mjs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { createMembers } from '../src/members.js'
import { bytes, sizeText } from '../src/format.js'
import { occurrenceKey } from '../src/sampleid.js'
import { project, defaultStoreNode } from '../src/views/project.js'
import { projectSamples, samplesTab } from '../src/views/project-samples.js'
import { whenText } from '../src/words.js'
import { samplesHref } from '../src/views/search.js'
import { parseHandoffQuery } from '../src/listquery.js'
import { snapshotDb } from './helpers.mjs'
import { APP_CSS } from '../src/styles.js'
import { blockFetch, buildMember, entryName, rawCid } from './fixture.mjs'

const NOW = Date.parse('2026-10-25T12:00:00.000Z')
const PIPE = 'nf-core/rnaseq'
const NAME = 'RAP1 depletion'
const VIEWER = 'rob@x'
const at = (minutes) => new Date(NOW - minutes * 60_000).toISOString()
const claim = (subject, verb, attribute, value, { supersedes = [], by = 'ana@x', minutes = 60 } = {}) =>
  ({ kind: 'Claim', schema: 1, asserted_by: by, subject, verb, attribute, value, supersedes, timestamp: at(minutes) })
const leaf = (name, address) => ({ kind: 'Leaf', name, address, size: 10, provider: 'head-node', reason: null })

// Three runs of one pipeline: WT_REP1 is in all three, KO_REP1 in all three.
const RUNS = {
  Ra: { finished: '2026-10-01T10:00:00.000Z', reads: { WT_REP1: 1000, WT_REP2: 2000, KO_REP1: 3000 } },
  Rb: { finished: '2026-10-02T10:00:00.000Z', reads: { WT_REP1: 1100, KO_REP1: 3100 } },
  Rc: { finished: '2026-10-03T10:00:00.000Z', reads: { WT_REP1: 1200, KO_REP1: 3200, KO_REP2: 4200 } },
}
// With `inputs`, each run's InputSet: KO_REP1 in Rc read other files, so it is another sample there.
const READS = (run, id) => (id === 'KO_REP1' && run === 'Rc' ? 'reads-KO1-again' : `reads-${id}`)

/**
 * Project P ("RAP1 depletion", ana@x, people ben@x) holds runs Ra and Rb
 * whole, item KO_REP2 of Rc, Selection S1 (WT_REP1 of Rc) and S2 (KO_REP1 of
 * Ra), and names samples KO_REP1 and GHOST. WT_REP1's condition is
 * "control" (ben@x); KO_REP1's is in conflict. R1 (a demo run) was added
 * and removed. A hand-off H names P. Options: `defaultStore`, `retention`
 * (the policy on), `merged` (WT_REP2 of Ra and KO_REP2 of Rc are one
 * sample), `split` (and then split again), `inputs` (InputSets),
 * `policyPin` (ben@x's policy pin on S2), `manualPin` (and another pin of
 * his on S2), `renamed` (renamed twice, with a policy pin on S2 under
 * the superseded name), `noRefs` (no samples added by name), `noStats` (no project_stats
 * row for P), `stats` (P's reachable bytes in this store), `retention2` (a second
 * current retention Claim, ben@x's, with this value).
 */
async function open({ retention2 = null, defaultStore = 'lab', retention = false, merged = false, split = false, inputs = false, noStats = false, stats = 5000, policyPin = false,
  manualPin = false, noRefs = false, renamed = false } = {}) {
  const ids = {}
  const runs = {}
  const member = await buildMember({
    now: NOW,
    extra: ({ put, runs: demo }) => {
      let tick = 0
      const log = []
      const entry = (kind, cid) => { log.push(entryName(NOW - 200_000 + (tick++) * 1000, kind, cid)); return cid }
      for (const [name, r] of Object.entries(RUNS)) {
        const m = put({ kind: 'RunManifest', schema: 1, asserted_by: 'ana@x', pipeline: PIPE, repository: null, revision: null, commit_id: null,
          run_name: name, nf_run_hash: `hash-${name}`, session_id: 's', resumed: false, nextflow_version: '26.04.6', params: {}, config: {},
          script: null, started_at: r.finished })
        const items = {}
        for (const [id, n] of Object.entries(r.reads))
          items[id] = put({ kind: 'OutputItem', schema: 1, value: [{ meta: { id }, num_trimmed_reads: n }, leaf(`${id}.fastq.gz`, rawCid(`fq-${name}-${id}`))] })
        const sorted = Object.values(items).sort((a, b) => (a.toString() < b.toString() ? -1 : 1))
        const coll = put({ kind: 'OutputCollection', schema: 1, asserted_by: 'ana@x', run: m, name: 'preprocessed', items: sorted, paths: sorted.map((_, i) => [`p/${i}`]) })
        const inputSet = inputs ? put({ kind: 'InputSet', schema: 1, asserted_by: 'ana@x', inputs: Object.keys(r.reads).sort().map(id =>
          ({ role: 'task', name: `${id}_R1.fastq.gz`, address: rawCid(READS(name, id)), size: 100, sample: id, reason: null })) }) : null
        const comp = put({ kind: 'RunCompletion', schema: 1, asserted_by: 'ana@x', run: m, collections: [coll], input_set: inputSet, status: 'succeeded',
          exit_status: 0, possibly_incomplete: false, started_at: r.finished, finished_at: r.finished,
          anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }, error: null })
        entry('run', comp)
        runs[name] = { completion: comp.toString(), collection: coll.toString(), items: Object.fromEntries(Object.entries(items).map(([k, v]) => [k, v.toString()])) }
      }
      const item = (run, id) => CID.parse(runs[run].items[id])
      const occ = (run, id) => ({ pipeline: PIPE, id, run: CID.parse(runs[run].completion) })
      ids.P = entry('project', put({ kind: 'Project', schema: 1, asserted_by: 'ana@x', name: NAME, description: 'Yeast RAP1 auxin-depletion time course',
        created_at: '2026-10-20T10:00:00.000Z' }))
      ids.S1 = entry('selection', put({ kind: 'Selection', schema: 1, asserted_by: 'ana@x', derived_from: [],
        members: [{ item: { address: item('Rc', 'WT_REP1'), via: [CID.parse(runs.Rc.collection)] } }] }))
      ids.S2 = entry('selection', put({ kind: 'Selection', schema: 1, asserted_by: 'ben@x', derived_from: [],
        members: [{ item: { address: item('Ra', 'KO_REP1'), via: [CID.parse(runs.Ra.collection)] } }] }))
      const c = (key, value) => { ids[key] = entry('claim', put(value)); return ids[key] }
      const R1 = CID.parse(demo.R1.completion)
      c('MR1', claim(R1, 'add', 'project', ids.P, { minutes: 300 }))
      c('DR1', claim(R1, 'del', 'project', null, { supersedes: [ids.MR1], by: 'ben@x', minutes: 250 }))
      c('MRa', claim(CID.parse(runs.Ra.completion), 'add', 'project', ids.P, { minutes: 200 }))
      c('MRb', claim(CID.parse(runs.Rb.completion), 'add', 'project', ids.P, { minutes: 190 }))
      c('MI', claim(item('Rc', 'KO_REP2'), 'add', 'project', ids.P, { minutes: 180 }))
      c('MS1', claim(ids.S1, 'add', 'project', ids.P, { minutes: 170 }))
      c('MS2', claim(ids.S2, 'add', 'project', ids.P, { by: 'ben@x', minutes: 160 }))
      c('PB', claim(ids.P, 'add', 'policy.people', 'ben@x', { minutes: 150 }))
      if (!noRefs) c('SA', claim(ids.P, 'add', 'sample', { pipeline: PIPE, id: 'KO_REP1' }, { minutes: 140 }))
      if (!noRefs) c('SG', claim(ids.P, 'add', 'sample', { pipeline: PIPE, id: 'GHOST' }, { minutes: 139 }))
      c('A1', claim(ids.P, 'add', 'annotation', { sample: { pipeline: PIPE, id: 'WT_REP1' }, name: 'condition', value: 'control' }, { by: 'ben@x', minutes: 130 }))
      c('K1', claim(ids.P, 'add', 'annotation', { sample: { pipeline: PIPE, id: 'KO_REP1' }, name: 'condition', value: 'mutant' }, { minutes: 120 }))
      c('K2', claim(ids.P, 'add', 'annotation', { sample: { pipeline: PIPE, id: 'KO_REP1' }, name: 'condition', value: 'knockout' }, { by: 'ben@x', minutes: 110 }))
      if (defaultStore) c('DS', claim(ids.P, 'set', 'policy.default_store', defaultStore, { minutes: 100 }))
      if (retention) c('RET', claim(ids.P, 'set', 'policy.retention', 'pin_selections', { minutes: 90 }))
      if (retention2) c('RET2', claim(ids.P, 'set', 'policy.retention', retention2, { by: 'ben@x', minutes: 89 }))
      if (merged || split) c('M', claim(ids.P, 'add', 'same_sample', { a: occ('Ra', 'WT_REP2'), b: occ('Rc', 'KO_REP2'), same: true }, { minutes: 80 }))
      if (policyPin) c('PP', claim(ids.S2, 'add', 'pin', `project policy: ${NAME}`, { by: 'ben@x', minutes: 60 }))
      if (renamed) {
        c('N1', claim(ids.P, 'set', 'name', 'RAP1 first', { minutes: 58 }))
        c('N2', claim(ids.P, 'set', 'name', 'RAP1 second', { supersedes: [ids.N1], minutes: 57 }))
        c('PO', claim(ids.S2, 'add', 'pin', 'project policy: RAP1 first', { by: 'ben@x', minutes: 56 }))
      }
      if (manualPin) c('MP', claim(ids.S2, 'add', 'pin', 'figure 2', { by: 'ben@x', minutes: 59 }))
      if (split) c('X', claim(ids.P, 'add', 'same_sample', { a: occ('Ra', 'WT_REP2'), b: occ('Rc', 'KO_REP2'), same: false }, { supersedes: [ids.M], minutes: 70 }))
      ids.H = entry('handoff', put({ kind: 'Handoff', schema: 1, asserted_by: 'ben@x', template: 'nf-core/differentialabundance', selection: ids.S1,
        project: ids.P, files: [], params: {}, timestamp: at(30) }))
      return {
        log,
        rows: (db) => {
          const row = (sql, bind) => db.exec({ sql, bind })
          if (!noStats) row('INSERT INTO project_stats VALUES (?, ?, ?, ?, ?, 3)', [ids.P.toString(), stats, 1000, stats - 1500, 500])
          // S1 is pinned by the viewer, in the snapshot (so the tail holds no pin Claim and the figures stay current).
          ids.PIN1 = rawCid('pin-claim-S1')
          row('INSERT INTO claim VALUES (?, ?, ?, ?, ?, ?, ?)', [ids.PIN1.toString(), ids.S1.toString(), 'add', 'pin', 'mine', at(500), VIEWER])
          row('INSERT INTO claim_current VALUES (?, ?, ?, ?, 0)', [ids.S1.toString(), 'pin', 'mine', ids.PIN1.toString()])
          row('INSERT INTO pinned_overlap VALUES (?, ?, ?)', [ids.S2.toString(), 'ben@x', 5000])
        },
      }
    },
  })
  const explorer = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }), now: () => NOW })
  return { explorer, member, runs, ids: Object.fromEntries(Object.entries(ids).map(([k, v]) => [k, v.toString()])) }
}

/** A ctx whose writer records each call; the first call of each name in `failOnce` is refused. */
function ctxFor({ can = true, members = null, failOnce = [] } = {}) {
  const calls = []
  const failing = new Set(failOnce)
  const record = (name) => async (...args) => {
    calls.push([name, ...args])
    if (failing.delete(name)) throw Object.assign(new Error('write_failed'), { code: 'write_failed' })
    return { address: `bafy-${name}` }
  }
  const writer = Object.fromEntries(['rename', 'addToProject', 'removeFromProject', 'annotate', 'clearAnnotation', 'sameSample', 'setPolicy',
    'clearPolicy', 'addPerson', 'removePerson', 'pin', 'unpin'].map(n => [n, record(n)]))
  const ran = []
  const ctx = { mark: () => {}, use: () => {}, viewer: VIEWER, members, store: { name: 'lab', key: 'a', all: false },
    write: { available: can, here: can, writable: 'lab', reason: can ? null : 'Read only here.', hrefFor: (h) => h, writer,
      run: async (status, fn) => {
        try { ran.push({ done: await fn() }) } catch (e) { ran.push({ error: e }); status.replaceChildren(e.message) }
      } } }
  return { ctx, calls, ran }
}

const text = (el) => el.textContent.replace(/\s+/g, ' ')
async function until(fn, what) {
  for (let i = 0; i < 100; i++) { if (fn()) return; await frame() }
  assert.fail(`timed out waiting for ${what}`)
}
const samplesOf = async (explorer, ids, ctx) => {
  const page = await project(explorer, ids.P, ctx, { tab: 'samples' })
  await until(() => page.querySelector('[data-samples-panel]') || page.querySelector('[data-error]'), 'the samples table')
  return page
}
const groupRows = (page) => page.querySelectorAll('tr[data-sample-group]')

test('the head: crumbs, name, the created tag, people, Add to project and Edit, the subline; five tabs with counts', async () => {
  installDom()
  const { explorer, ids } = await open({ retention: true })
  const page = await project(explorer, ids.P, ctxFor().ctx, { tab: 'overview' })
  assert.equal(page.querySelector('h1').textContent, NAME)
  assert.deepEqual(page.querySelector('.crumbs').querySelectorAll('a').map(a => a.getAttribute('href')), ['#/projects'])
  const created = page.querySelector('.title').querySelector('.claim')
  assert.match(text(created), /created/)
  assert.equal(created.querySelector('.av').getAttribute('title'), 'ana@x')
  assert.deepEqual(page.querySelector('[data-people]').querySelectorAll('.av').map(a => a.getAttribute('title')), ['ana@x', 'ben@x'])
  assert.ok(page.querySelector('#project-add-open'))
  assert.ok(page.querySelector('#project-edit-open'))
  const sub = text(page.querySelector('.subline'))
  assert.match(sub, /Yeast RAP1 auxin-depletion time course/)
  assert.match(sub, /default store lab/)
  assert.match(sub, /pins every Selection/)
  const tabs = page.querySelector('nav.tabs').querySelectorAll('a')
  assert.deepEqual(tabs.map(t => [t.dataset.tab, t.getAttribute('href')]), ['overview', 'samples', 'runs', 'selections', 'activity']
    .map(k => [k, `#/project/${ids.P}?tab=${k}`]))
  assert.equal(page.querySelector('[data-tab=runs]').querySelector('small').textContent, '2')
  assert.equal(page.querySelector('[data-tab=selections]').querySelector('small').textContent, '2')
  assert.equal(page.querySelector('nav.tabs').querySelector('.on').dataset.tab, 'overview')
  assert.ok(page.querySelector('[data-person="ben@x"]'), 'the Overview lists people')
  assert.deepEqual(strayTexts(page), [])
})

test('Samples: Hand off… opens the differentialabundance hand-off from the newest run, this project active, its samples there ticked', async () => {
  installDom()
  const { explorer, ids, runs } = await open()
  const page = await samplesOf(explorer, ids, ctxFor().ctx)
  const href = page.querySelector('[data-handoff-link]').getAttribute('href')
  assert.match(href, /^#\/handoff\/nf-core%2Fdifferentialabundance\?/)
  assert.deepEqual(parseHandoffQuery(href.slice(href.indexOf('?') + 1)),
    { from: runs.Rc.completion, where: null, project: ids.P, samples: ['KO_REP1', 'KO_REP2', 'WT_REP1'], outputs: [] })
})

test('read only: no edit controls, and the reason', async () => {
  installDom()
  const { explorer, ids } = await open()
  const page = await project(explorer, ids.P, ctxFor({ can: false }).ctx, { tab: 'runs' })
  assert.equal(page.querySelector('#project-edit-open'), null)
  assert.equal(page.querySelector('[data-remove]'), null)
  assert.match(text(page.querySelector('[data-unavailable]')), /Read only here/)
})

test('storage with two members: one bar per store side by side, never a total, with the caption', async () => {
  installDom()
  const lab = await open()
  const org = await open({ stats: 3000 })
  assert.equal(lab.ids.P, org.ids.P)
  const list = [{ alias: 'lab', key: 'a', writable: true, base: 'm/lab/' }, { alias: 'org', key: 'b', writable: false, base: 'm/org/' }]
  const exes = { lab: lab.explorer, org: org.explorer }
  const members = createMembers({ member: 'lab', members: list }, { openExplorer: async (m) => exes[m.alias] })
  const page = await project(lab.explorer, lab.ids.P, ctxFor({ members }).ctx, { tab: 'overview' })
  const bars = page.querySelectorAll('[data-store-bar]')
  assert.deepEqual(bars.map(b => [b.dataset.storeBar, b.dataset.bytes]), [['lab', '5000'], ['org', '3000']])
  assert.equal(page.querySelectorAll('.sbar').length, 2)
  assert.doesNotMatch(text(page), new RegExp(bytes(8000)))
  assert.equal(page.querySelector('[data-storage-caption]').textContent,
    "Each store keeps its own pins and runs its own cleanup, so they're shown side by side and never added up.")
})

test('storage on a lean snapshot says so as P2 does; a snapshot with no project_stats row for the project says it has no figure', async () => {
  installDom()
  const { explorer, ids } = await open()
  explorer.storageOmitted = 'over_cap'
  const page = await project(explorer, ids.P, ctxFor().ctx, { tab: 'overview' })
  assert.equal(page.querySelector('[data-store-bar]'), null)
  assert.ok(page.querySelector('[data-store-lean=lab]').querySelector('[data-storage-omitted=over_cap]'))
  const old = await open({ noStats: true })
  const page2 = await project(old.explorer, old.ids.P, ctxFor().ctx, { tab: 'overview' })
  assert.equal(text(page2.querySelector('[data-store-none=lab]')), "lab's snapshot has no figure for this project.")
})

test('samples: occurrences from whole runs, an item, a Selection and a sample by name, grouped by meta.id across three runs', async () => {
  installDom()
  const { explorer, ids, runs } = await open()
  const page = await samplesOf(explorer, ids, ctxFor().ctx)
  const rows = groupRows(page)
  assert.deepEqual(rows.map(r => r.dataset.sampleGroup), ['KO_REP1', 'KO_REP2', 'WT_REP1', 'WT_REP2'])
  const row = (label) => rows.find(r => r.dataset.sampleGroup === label)
  assert.deepEqual(rows.map(r => r.querySelector('[data-runs]').textContent), ['3', '1', '3', '1'], 'the same meta.id joins across runs')
  assert.equal(row('WT_REP1').querySelector('[data-field]').textContent, '1,200', 'Reads (trimmed) from the latest run, Rc')
  assert.equal(row('KO_REP1').querySelector('[data-field]').textContent, '3,200', 'KO_REP1 reaches Rc through the sample added by name')
  assert.equal(row('WT_REP2').querySelector('[data-field]').textContent, '2,000')
  assert.equal(row('WT_REP1').querySelector('a').getAttribute('href'), samplesHref(runs.Rc.completion, 'meta.id', 'WT_REP1'))
  const head = page.querySelector('[data-project-samples]').querySelectorAll('th').map(th => text(th))
  assert.deepEqual(head, ['Sample', 'Runs', 'Reads (trimmed)latest run', 'conditionproject claim', 'Where'])
  assert.ok(row('WT_REP1').querySelector('.presence'))
  assert.equal(text(page.querySelector('[data-basis-note]')),
    'Grouped by meta.id. Grouping by input reads needs the plugin to record input addresses (input_set is empty in these runs).')
  assert.equal(page.querySelector('[data-basis-meta]').textContent, 'grouped by sample ID')
  assert.match(text(page.querySelector('[data-unmatched]')), /1 sample added by name is in none of these runs: GHOST/)
  assert.equal(page.querySelector('[data-tab=samples]').querySelector('small').textContent, '4')
  assert.ok(page.querySelector('[data-store-bar=lab]'), 'storage beside the table, as in the mockup')
  assert.deepEqual(strayTexts(page), [])
})

test('an annotation edit writes add annotation superseding the cell\'s Claims, on the first occurrence\'s SampleRef; typing shows your avatar', async () => {
  installDom()
  const { explorer, ids } = await open()
  const { ctx, calls } = ctxFor()
  const page = await samplesOf(explorer, ids, ctx)
  const cell = groupRows(page).find(r => r.dataset.sampleGroup === 'WT_REP1').querySelector('td[data-annotation=condition]')
  assert.equal(cell.dataset.value, 'control')
  assert.equal(cell.querySelector('.av').getAttribute('title'), 'ben@x')
  const input = cell.querySelector('input')
  assert.equal(input.value, 'control')
  input.value = 'wild_type'
  await input.fire('input')
  assert.equal(cell.querySelector('.av').getAttribute('title'), VIEWER)
  await input.fire('change')
  assert.deepEqual(calls, [['annotate', ids.P, { pipeline: PIPE, id: 'WT_REP1' }, 'condition', 'wild_type', [ids.A1]]])
  // An empty cell writes a new Claim, superseding nothing; clearing a value writes a del.
  const empty = groupRows(page).find(r => r.dataset.sampleGroup === 'WT_REP2').querySelector('td[data-annotation=condition]').querySelector('input')
  empty.value = 'wild_type'
  await empty.fire('change')
  assert.deepEqual(calls[1], ['annotate', ids.P, { pipeline: PIPE, id: 'WT_REP2' }, 'condition', 'wild_type', []])
  input.value = ''
  await input.fire('change')
  assert.deepEqual(calls[2], ['clearAnnotation', ids.P, [ids.A1]])
})

test('a conflicted cell shows both values; picking one writes it, superseding both', async () => {
  installDom()
  const { explorer, ids } = await open()
  const { ctx, calls } = ctxFor()
  const page = await samplesOf(explorer, ids, ctx)
  const cell = groupRows(page).find(r => r.dataset.sampleGroup === 'KO_REP1').querySelector('td[data-annotation=condition]')
  assert.ok(cell.hasAttribute('data-conflicted'))
  const picks = cell.querySelectorAll('[data-pick-value]')
  assert.deepEqual(picks.map(b => b.dataset.pickValue), ['mutant', 'knockout'])
  await picks[1].click()
  assert.deepEqual(calls.length, 1)
  const [name, p, ref, attr, value, supersedes] = calls[0]
  assert.deepEqual([name, p, ref, attr, value], ['annotate', ids.P, { pipeline: PIPE, id: 'KO_REP1' }, 'condition', 'knockout'])
  assert.deepEqual([...supersedes].sort(), [ids.K1, ids.K2].sort())
  // Read only, the values show as claim tags and nothing can be picked.
  const ro = await samplesOf(explorer, ids, ctxFor({ can: false }).ctx)
  const roCell = groupRows(ro).find(r => r.dataset.sampleGroup === 'KO_REP1').querySelector('td[data-annotation=condition]')
  assert.equal(roCell.querySelectorAll('.claim').length, 2)
  assert.equal(roCell.querySelector('input'), null)
})

test('Merge writes one same:true Claim between an occurrence of each sample', async () => {
  installDom()
  const { explorer, ids, runs } = await open()
  const { ctx, calls } = ctxFor()
  const page = await samplesOf(explorer, ids, ctx)
  const form = page.querySelector('[data-merge]')
  assert.ok(form.hasAttribute('hidden'))
  await page.querySelector('[data-merge-open]').click()
  assert.equal(form.hasAttribute('hidden'), false)
  form.querySelector('[data-merge-a]').value = '1'
  form.querySelector('[data-merge-b]').value = '3'
  await form.querySelector('[data-merge-go]').click()
  assert.deepEqual(calls, [['sameSample', ids.P, { pipeline: PIPE, id: 'KO_REP2', run: runs.Rc.completion },
    { pipeline: PIPE, id: 'WT_REP2', run: runs.Ra.completion }, true, []]])
})

// Task 6 found both forms drawn open: .project-page .project-form { display: grid } beat .project-page [hidden].
test('the Merge and Split forms start closed, and the stylesheet lets [hidden] beat a view\'s display rule', async () => {
  installDom()
  const { explorer, ids } = await open({ merged: true })
  const page = await samplesOf(explorer, ids, ctxFor().ctx)
  for (const key of ['merge', 'split']) {
    assert.ok(page.querySelector(`[data-${key}-open]`), `${key}: its button`)
    assert.ok(page.querySelector(`[data-${key}]`).hasAttribute('hidden'), `${key}: closed until its button is pressed`)
  }
  // The test DOM applies no CSS, so the rule itself is checked: one bare [hidden] rule, !important, so no class rule outranks it.
  assert.match(APP_CSS, /(^|\n)\s*\[hidden\] \{ display: none !important; \}/)
})

test('Split supersedes the merge that joined the occurrence, and the split then shows it as its own sample', async () => {
  installDom()
  const { explorer, ids, runs } = await open({ merged: true })
  const { ctx, calls } = ctxFor()
  const page = await samplesOf(explorer, ids, ctx)
  assert.deepEqual(groupRows(page).map(r => r.dataset.sampleGroup), ['KO_REP1', 'KO_REP2', 'WT_REP1'], 'the merge joins WT_REP2 into KO_REP2')
  const form = page.querySelector('[data-split]')
  const pick = form.querySelector('[data-split-pick]')
  const b = { pipeline: PIPE, id: 'KO_REP2', run: runs.Rc.completion }
  pick.value = pick.querySelectorAll('option').find(o => o.dataset.occurrence === occurrenceKey(b)).getAttribute('value')
  await form.querySelector('[data-split-go]').click()
  assert.deepEqual(calls, [['sameSample', ids.P, { pipeline: PIPE, id: 'WT_REP2', run: runs.Ra.completion }, b, false, [ids.M]]])
  // A split of an automatic group: its a is another occurrence of the same meta.id, and there is no merge to supersede.
  const wt = { pipeline: PIPE, id: 'WT_REP1', run: runs.Rb.completion }
  pick.value = pick.querySelectorAll('option').find(o => o.dataset.occurrence === occurrenceKey(wt)).getAttribute('value')
  await form.querySelector('[data-split-go]').click()
  const [, , a, b2, same, supersedes] = calls[1]
  assert.deepEqual([b2, same, supersedes], [wt, false, []])
  assert.equal(a.id, 'WT_REP1')
  assert.ok([runs.Ra.completion, runs.Rc.completion].includes(a.run))
  const after = await samplesOf((await open({ split: true })).explorer, ids, ctxFor().ctx)
  assert.deepEqual(groupRows(after).map(r => r.dataset.sampleGroup), ['KO_REP1', 'KO_REP2', 'WT_REP1', 'WT_REP2'])
})

test('with input rows the samples are grouped by input reads, with an Input reads column of seals', async () => {
  installDom()
  const { explorer, ids } = await open({ inputs: true })
  const page = await samplesOf(explorer, ids, ctxFor().ctx)
  const rows = groupRows(page)
  // KO_REP1 in Rc read other files: same name, other reads, another sample.
  assert.deepEqual(rows.map(r => [r.dataset.sampleGroup, r.querySelector('[data-runs]').textContent]).sort(),
    [['KO_REP1', '1'], ['KO_REP1', '2'], ['KO_REP2', '1'], ['WT_REP1', '3'], ['WT_REP2', '1']])
  assert.ok(rows.every(r => r.dataset.basis === 'input reads'))
  assert.equal(page.querySelector('[data-basis-meta]').textContent, 'grouped by input reads')
  assert.match(text(page.querySelector('[data-basis-note]')), /^Grouped by input reads/)
  assert.ok(page.querySelector('[data-project-samples]').querySelectorAll('th').some(th => th.textContent === 'Input reads'))
  const wt = rows.find(r => r.dataset.sampleGroup === 'WT_REP1')
  assert.deepEqual(wt.querySelectorAll('[data-input]').map(s => s.dataset.input), [rawCid('reads-WT_REP1').toString()])
  const sealEl = wt.querySelector('[data-input]').querySelector('.seal')
  assert.equal(sealEl.dataset.seal, rawCid('reads-WT_REP1').toString())
  assert.match(sealEl.innerHTML, /^<svg /)
})

test('turning retention on shows each pin\'s bytes and who already pins it, then pins only the Selections you have not pinned', async () => {
  installDom()
  const { explorer, ids } = await open()
  const { ctx, calls } = ctxFor()
  const page = await project(explorer, ids.P, ctx, { tab: 'overview' })
  await page.querySelector('#project-edit-open').click()
  await page.querySelector('#project-retention-on').click()
  const confirm = page.querySelector('[data-retention-confirm]')
  assert.deepEqual(confirm.querySelectorAll('[data-pin-target]').map(li => li.dataset.pinTarget), [ids.S2], 'S1 is pinned by you already')
  const go = confirm.querySelector('#project-pin-go')
  assert.ok(go.disabled, 'Pin waits for the bytes')
  await until(() => !go.disabled, 'the pin figures')
  const kept = sizeText(await explorer.selectionSize(ids.S2))
  assert.equal(confirm.querySelector('[data-pin-bytes]').textContent, `Pin ${kept} in lab`)
  await until(() => !confirm.querySelector('[data-pinned-by]').hasAttribute('hidden'), 'the pinned-by line')
  assert.match(text(confirm.querySelector('[data-pinned-by]')), new RegExp(`${bytes(5000)} already pinned here by ben@x`))
  assert.match(text(confirm.querySelector('[data-pin-skipped]')), /1 Selection you already pin is left as it is/)
  assert.equal(go.textContent, 'Turn on and pin 1 Selection')
  await go.click()
  assert.deepEqual(calls, [['setPolicy', ids.P, 'retention', 'pin_selections', []], ['pin', ids.S2, `project policy: ${NAME}`]])
})

test('with retention on: the Selections tab says how many are not pinned by policy and offers Pin; turning it off writes no unpin', async () => {
  installDom()
  const { explorer, ids } = await open({ retention: true })
  const { ctx, calls } = ctxFor()
  const page = await project(explorer, ids.P, ctx, { tab: 'selections' })
  const line = page.querySelector('[data-not-pinned]')
  assert.equal(line.dataset.notPinned, '1')
  assert.match(text(line), /1 Selection not pinned by policy · Pin/)
  await page.querySelector('[data-pin-unpinned]').click()
  assert.deepEqual(page.querySelector('[data-retention-confirm]').querySelectorAll('[data-pin-target]').map(li => li.dataset.pinTarget), [ids.S2])
  await page.querySelector('#project-edit-open').click()
  await page.querySelector('#project-retention-off').click()
  assert.deepEqual(calls, [['setPolicy', ids.P, 'retention', 'none', [ids.RET]]])
})

test('D5 with retention on: the head says how many are not pinned here and links the Selections tab; the Edit panel offers Pin, which pins only those', async () => {
  installDom()
  const { explorer, ids } = await open({ retention: true })
  const { ctx, calls } = ctxFor()
  const page = await project(explorer, ids.P, ctx, { tab: 'overview' })
  const said = page.querySelector('.subline').querySelector('[data-retention]')
  assert.equal(text(said), 'pins every Selection · 1 not pinned here')
  assert.equal(said.querySelector('a').getAttribute('href'), `#/project/${ids.P}?tab=selections`)
  await page.querySelector('#project-edit-open').click()
  const edit = page.querySelector('[data-project-edit]')
  assert.match(text(edit.querySelector('[data-edit-not-pinned]')), /1 Selection not pinned by policy · Pin/)
  await edit.querySelector('[data-edit-pin-unpinned]').click()
  const confirm = edit.querySelector('[data-edit-pin-confirm]')
  assert.equal(confirm.hasAttribute('hidden'), false)
  assert.deepEqual(confirm.querySelectorAll('[data-pin-target]').map(li => li.dataset.pinTarget), [ids.S2])
  const go = confirm.querySelector('#project-pin-go')
  await until(() => !go.disabled, 'the pin figures')
  assert.equal(go.textContent, 'Pin 1 Selection')
  await go.click()
  assert.deepEqual(calls, [['pin', ids.S2, `project policy: ${NAME}`]])
  // Every Selection pinned: the head says only that the project pins them, and the Edit panel offers no Pin.
  const all = await open({ retention: true, policyPin: true })
  const done = await project(all.explorer, all.ids.P, ctxFor().ctx, { tab: 'overview' })
  assert.equal(text(done.querySelector('.subline').querySelector('[data-retention]')), 'pins every Selection')
  assert.equal(done.querySelector('[data-edit-not-pinned]'), null)
})

test('the default store: a read-only member and an unknown one each get a note, and neither is made a target', async () => {
  installDom()
  const list = [{ alias: 'lab', key: 'a', copy_target: false }, { alias: 'org', key: 'b', copy_target: false }, { alias: 'ana-hpc', key: 'c', copy_target: true }]
  const ctx = { members: { list }, store: { name: 'lab', key: 'a' } }
  const ro = defaultStoreNode('org', ctx)
  assert.equal(ro.dataset.defaultStoreNote, 'read-only here')
  assert.equal(text(ro), 'default store org (read-only here)')
  assert.ok(ro.querySelector('.dot.b'))
  const unknown = defaultStoreNode('elsewhere', ctx)
  assert.equal(text(unknown), 'default store elsewhere (not configured here)')
  assert.equal(unknown.querySelector('.dot'), null)
  assert.equal(defaultStoreNode('ana-hpc', ctx).dataset.defaultStoreNote, undefined)
  assert.equal(list[1].copy_target, false, 'the note never changes the member')
  installDom()
  const { explorer, ids } = await open({ defaultStore: 'elsewhere' })
  const page = await project(explorer, ids.P, ctxFor().ctx, { tab: 'runs' })
  assert.match(text(page.querySelector('.subline')), /default store elsewhere \(not configured here\)/)
})

test('Remove supersedes the membership Claim; runs and items are listed with who added them', async () => {
  installDom()
  const { explorer, ids, runs } = await open()
  const { ctx, calls } = ctxFor()
  const page = await project(explorer, ids.P, ctx, { tab: 'runs' })
  const rows = page.querySelectorAll('tr[data-member]')
  assert.deepEqual(rows.map(r => r.dataset.kind).sort(), ['item', 'run', 'run'])
  const ra = rows.find(r => r.dataset.member === runs.Ra.completion)
  assert.match(text(ra.querySelector('.claim')), /added/)
  assert.equal(ra.querySelector('.claim').querySelector('.av').getAttribute('title'), 'ana@x')
  await ra.querySelector('[data-remove]').click()
  assert.deepEqual(calls, [['removeFromProject', runs.Ra.completion, [ids.MRa]]])
  const sels = await project(explorer, ids.P, ctx, { tab: 'selections' })
  await sels.querySelector(`[data-remove="${ids.S2}"]`).click()
  assert.deepEqual(calls[1], ['removeFromProject', ids.S2, [ids.MS2]])
})

test('Activity: newest first, each row with who and when; Claims on the project, memberships added and removed, and the hand-off', async () => {
  installDom()
  const { explorer, ids } = await open()
  const page = await project(explorer, ids.P, ctxFor().ctx, { tab: 'activity' })
  const rows = page.querySelectorAll('[data-activity]')
  const ats = rows.map(r => r.dataset.at)
  assert.deepEqual(ats, [...ats].sort().reverse(), 'newest first')
  for (const r of rows) {
    assert.ok(r.querySelector('.av').getAttribute('title'), 'who')
    assert.ok(r.querySelector('.activity-age').textContent, 'when')
  }
  const said = rows.map(r => text(r.querySelector('.claim')))
  assert.match(said[0], /handed off.*nf-core\/differentialabundance/)
  assert.ok(said.some(s => /condition\s*KO_REP1 = knockout/.test(s)))
  assert.ok(said.some(s => /removed\s*R1/.test(s)), 'a removed member')
  assert.ok(said.some(s => /added\s*R1/.test(s)))
  assert.ok(said.some(s => /added person\s*ben@x/.test(s)))
  assert.match(said.at(-1), /created/)
  const removed = rows.find(r => r.dataset.activity === ids.DR1)
  assert.equal(removed.querySelector('.av').getAttribute('title'), 'ben@x')
  assert.equal(removed.querySelector('.activity-age').textContent, '4h')
  assert.deepEqual(strayTexts(page), [])
})

// Task 6: "handed off <a long Selection name> to <template>" ran out of the Activity panel, with its age pushed onto a line of its own.
test('Activity: a long event stays inside its panel, cut short with the whole text in its title, its age beside it', async () => {
  installDom()
  const { explorer, ids } = await open()
  const page = await project(explorer, ids.P, ctxFor().ctx, { tab: 'activity' })
  const row = page.querySelectorAll('[data-activity]').find(r => /handed off/.test(text(r)))
  const val = row.querySelector('.claim').querySelector('.val')
  assert.ok(val, 'the value has its own span, so it alone is cut')
  assert.match(val.getAttribute('title'), /nf-core\/differentialabundance$/)
  assert.equal(val.getAttribute('title'), text(val))
  // nowrap keeps the age beside the tag; min-width: 0 lets the row (a grid item of .pb) be narrower than the tag's whole text.
  assert.match(APP_CSS, /\.project-page \.activity-row \{[^}]*min-width: 0;[^}]*flex-wrap: nowrap/)
})

test('Add to project…: an address finds a run, a sample ID finds an item; Add writes the membership Claim', async () => {
  installDom()
  const { explorer, ids, runs, member } = await open()
  const { ctx, calls } = ctxFor()
  const page = await project(explorer, ids.P, ctx, { tab: 'overview' })
  const panelEl = page.querySelector('[data-project-add]')
  assert.ok(panelEl.hasAttribute('hidden'))
  await page.querySelector('#project-add-open').click()
  assert.equal(panelEl.hasAttribute('hidden'), false)
  const query = panelEl.querySelector('#project-add-query')
  const find = async (q, what) => {
    query.value = q
    await panelEl.querySelector('#project-add-find').click()
    await until(() => panelEl.querySelector(what), what)
    return panelEl.querySelector(what)
  }
  const rc = await find(runs.Rc.completion, `[data-add-result="${runs.Rc.completion}"]`)
  assert.equal(rc.dataset.kind, 'run')
  await rc.querySelector('[data-add]').click()
  assert.deepEqual(calls, [['addToProject', runs.Rc.completion, ids.P]])
  const ra = (await find(runs.Ra.completion, `[data-add-result="${runs.Ra.completion}"]`)).querySelector('[data-add]')
  assert.ok(ra.disabled, 'a run already in the project')
  assert.equal(ra.textContent, 'In this project')
  const item = await find('B', `[data-add-result="${member.item.B}"]`)
  assert.equal(item.dataset.kind, 'item')
  await item.querySelector('[data-add]').click()
  assert.deepEqual(calls[1], ['addToProject', member.item.B, ids.P])
})

test('Add to project… with retention on: a Selection is added and pinned, its bytes on the button first', async () => {
  installDom()
  const { explorer, ids } = await open({ retention: true })
  const { ctx, calls } = ctxFor()
  // A Selection in no project yet: S1's block, as a search answer would give it.
  const page = await project(explorer, ids.P, ctx, { tab: 'overview' })
  explorer.searchAddresses = async () => [{ cid: 'bafyreinewselection', kind: 'selection', label: 'new picks', href: '#/selection/bafyreinewselection' }]
  explorer.selectionSize = async () => ({ bytes: 2_500_000, partial: false })
  const panelEl = page.querySelector('[data-project-add]')
  panelEl.querySelector('#project-add-query').value = 'bafyreinewselection'
  await panelEl.querySelector('#project-add-find').click()
  await until(() => panelEl.querySelector('[data-add-result]'), 'the result')
  const add = panelEl.querySelector('[data-add]')
  await until(() => !add.disabled, 'the bytes')
  assert.equal(add.textContent, `Add and pin ${bytes(2_500_000)} in lab`)
  await add.click()
  assert.deepEqual(calls, [['addToProject', 'bafyreinewselection', ids.P], ['pin', 'bafyreinewselection', `project policy: ${NAME}`]])
})

test('the Selections tab lists the policy\'s pins, each with its own Unpin', async () => {
  installDom()
  const { explorer, ids } = await open({ retention: true, policyPin: true })
  const { ctx, calls } = ctxFor()
  const page = await project(explorer, ids.P, ctx, { tab: 'selections' })
  const row = page.querySelector(`tr[data-member="${ids.S2}"]`)
  const pin = row.querySelector('li[data-pin]')
  assert.equal(pin.dataset.claim, ids.PP)
  assert.match(text(pin.querySelector('.claim')), /pinned/)
  assert.equal(pin.querySelector('.av').getAttribute('title'), 'ben@x')
  await until(() => !pin.querySelector('[data-unpin]').disabled, 'Unpin')
  await pin.querySelector('[data-unpin]').click()
  assert.deepEqual(calls, [['unpin', ids.S2, ids.PP]])
  assert.equal(page.querySelector('[data-not-pinned]'), null, 'every Selection is pinned by policy or by you')
})

/** projectMembers' rows, one per subject, as the page groups them. */
async function membersOf(explorer, P) {
  const out = new Map()
  for (const m of await explorer.projectMembers(P)) {
    if (!out.has(m.subject)) out.set(m.subject, { subject: m.subject, kind: m.kind, claims: [] })
    out.get(m.subject).claims.push(m.claim)
  }
  return [...out.values()].filter(m => m.kind)
}

test('a policy pin\'s Unpin counts against every pin on the Selection: one person\'s two pins each keep 0 B', async () => {
  installDom()
  const stub = (explorer) => {
    explorer.storageChanges = () => ({ claims: false, logged: false })
    explorer.rootStorage = async () => ({ roots: [{ kind: 'pin', assertedBy: 'ben@x', exclusive: 4000, partial: false }], shared: [] })
  }
  const one = await open({ retention: true, policyPin: true })
  stub(one.explorer)
  const alone = (await project(one.explorer, one.ids.P, ctxFor().ctx, { tab: 'selections' })).querySelector('[data-unpin]')
  await until(() => !alone.disabled, 'Unpin')
  assert.equal(alone.textContent, `Unpin (kept only by this pin in lab: ${bytes(4000)})`)
  const two = await open({ retention: true, policyPin: true, manualPin: true })
  stub(two.explorer)
  const page = await project(two.explorer, two.ids.P, ctxFor().ctx, { tab: 'selections' })
  const pins = page.querySelector(`tr[data-member="${two.ids.S2}"]`).querySelectorAll('li[data-pin]')
  assert.deepEqual(pins.map(p => p.dataset.claim), [two.ids.PP], 'only the policy pin is listed')
  const unpin = pins[0].querySelector('[data-unpin]')
  await until(() => !unpin.disabled, 'Unpin')
  assert.equal(unpin.textContent, 'Unpin (kept only by this pin in lab: 0 B)')
})

test('the item budget: whole runs are read in order until the budget, the rest are left out and named; their member items still count', async () => {
  installDom()
  const { explorer, ids, runs } = await open()
  const p = await explorer.project(ids.P)
  const members = await membersOf(explorer, ids.P)
  const d = await projectSamples(explorer, p, members, { budget: 4 })
  // Rb (2 items, the newest whole-run member) fits; Ra (3) and Rc (3, named by a sample added by name) do not.
  assert.ok(d.runs.get(runs.Rb.completion).table)
  assert.equal(d.runs.get(runs.Ra.completion).budget, 3)
  assert.equal(d.runs.get(runs.Rc.completion).budget, 3)
  assert.deepEqual(d.groups.map(g => [g.label, new Set(g.occurrences.map(o => o.run)).size]),
    [['KO_REP1', 2], ['KO_REP2', 1], ['WT_REP1', 2]], 'KO_REP1 of Ra through S2, WT_REP1 and KO_REP2 of Rc through S1 and the item')
  const tab = samplesTab(explorer, p, ctxFor().ctx, { members, can: true, write: () => {}, budget: 4 })
  await until(() => tab.querySelector('[data-samples-panel]'), 'the panel')
  assert.match(text(tab.querySelector('[data-runs-unread]')), /^Left out: Rc \(3 items\), Ra \(3 items\) would pass the 4 items this page reads for a project\. Their member items still count\.$/)
  // Whole runs alone: a run left out has no member items to count, so the line says nothing of them.
  const wholeOnly = members.filter(m => m.kind === 'run')
  const tab2 = samplesTab(explorer, p, ctxFor().ctx, { members: wholeOnly, can: true, write: () => {}, budget: 2 })
  await until(() => tab2.querySelector('[data-samples-panel]'), 'the panel')
  assert.equal(text(tab2.querySelector('[data-runs-unread]')), 'Left out: Ra (3 items) would pass the 2 items this page reads for a project.')
  // A page gone before the counts: nothing more is read.
  let asked = 0
  const run = explorer.run.bind(explorer)
  explorer.run = async (c) => { asked++; return run(c) }
  assert.equal(await projectSamples(explorer, p, members, { alive: () => false }), null)
  assert.equal(asked, 0)
})

test('a run that only member items reach is not read whole: its items come from their own blocks', async () => {
  installDom()
  const { explorer, ids, runs } = await open({ noRefs: true })
  const asked = []
  const allItems = explorer.allItems.bind(explorer)
  explorer.allItems = async (c) => { asked.push(c); return allItems(c) }
  const p = await explorer.project(ids.P)
  const d = await projectSamples(explorer, p, await membersOf(explorer, ids.P))
  assert.equal(asked.includes(runs.Rc.collection), false, 'Rc is not read whole')
  assert.deepEqual(asked.sort(), [runs.Ra.collection, runs.Rb.collection].sort())
  const rc = d.runs.get(runs.Rc.completion)
  assert.equal(rc.table, undefined)
  assert.deepEqual([...rc.items.keys()].sort(), ['KO_REP2', 'WT_REP1'])
  const page = await samplesOf(explorer, ids, ctxFor().ctx)
  const row = (label) => groupRows(page).find(r => r.dataset.sampleGroup === label)
  assert.equal(row('WT_REP1').querySelector('[data-runs]').textContent, '3')
  assert.equal(row('WT_REP1').querySelector('[data-field]').textContent, '1,200', 'Reads (trimmed) from the item\'s own block')
  assert.equal(row('KO_REP2').querySelector('[data-field]').textContent, '4,200')
  assert.ok(row('KO_REP2').querySelector('.presence'))
  assert.equal(row('KO_REP1').querySelector('[data-runs]').textContent, '2')
  assert.equal(page.querySelector('[data-runs-unread]'), null)
})

test('Add to project… with retention on: who already pins it here, and a Selection you pin already is added with no pin', async () => {
  installDom()
  const { explorer, ids } = await open({ retention: true })
  const members = explorer.projectMembers.bind(explorer)
  explorer.projectMembers = async (cid) => (await members(cid)).filter(m => m.kind !== 'selection')
  explorer.searchAddresses = async () => [{ cid: ids.S2, kind: 'selection', label: 'S2', href: '#' }, { cid: ids.S1, kind: 'selection', label: 'S1', href: '#' }]
  const { ctx, calls } = ctxFor()
  const page = await project(explorer, ids.P, ctx, { tab: 'overview' })
  const panelEl = page.querySelector('[data-project-add]')
  panelEl.querySelector('#project-add-query').value = ids.S2
  await panelEl.querySelector('#project-add-find').click()
  await until(() => panelEl.querySelectorAll('[data-add]').length === 2 && panelEl.querySelectorAll('[data-add]').every(b => !b.disabled), 'both results')
  const [s2, s1] = [ids.S2, ids.S1].map(c => panelEl.querySelector(`[data-add-result="${c}"]`))
  assert.equal(s2.querySelector('[data-add]').textContent, `Add and pin ${sizeText(await explorer.selectionSize(ids.S2))} in lab`)
  assert.equal(text(s2.querySelector('[data-add-pin-note]')), `${bytes(5000)} already pinned here by ben@x`)
  assert.equal(s1.querySelector('[data-add]').textContent, 'Add')
  assert.match(text(s1.querySelector('[data-add-pin-note]')), /Already pinned by you/)
  await s1.querySelector('[data-add]').click()
  assert.deepEqual(calls, [['addToProject', ids.S1, ids.P]])
})

test('the default store\'s Save writes only a change', async () => {
  installDom()
  const { explorer, ids } = await open()
  const { ctx, calls } = ctxFor()
  const page = await project(explorer, ids.P, ctx, { tab: 'overview' })
  const select = page.querySelector('#project-default-store')
  const save = page.querySelector('#project-default-store-save')
  assert.ok(save.disabled)
  select.value = ''
  await select.fire('change')
  assert.equal(save.disabled, false)
  await save.click()
  assert.deepEqual(calls, [['clearPolicy', ids.P, 'default_store', [ids.DS]]])
  const none = await open({ defaultStore: null })
  const page2 = await project(none.explorer, none.ids.P, ctxFor().ctx, { tab: 'overview' })
  const select2 = page2.querySelector('#project-default-store')
  select2.value = ''
  await select2.fire('change')
  assert.ok(page2.querySelector('#project-default-store-save').disabled, 'no default store, and none chosen: nothing to write')
  select2.value = 'lab'
  await select2.fire('change')
  assert.equal(page2.querySelector('#project-default-store-save').disabled, false)
})

test('annotation cells carry who and when; emptying a conflicted cell clears nothing, its Clear clears every value', async () => {
  installDom()
  const { explorer, ids } = await open()
  const { ctx, calls } = ctxFor()
  const page = await samplesOf(explorer, ids, ctx)
  const row = (label) => groupRows(page).find(r => r.dataset.sampleGroup === label)
  const wt = row('WT_REP1').querySelector('td[data-annotation=condition]').querySelector('.cell-claim')
  assert.equal(wt.getAttribute('title'), `ben@x, ${whenText(at(130))}`)
  const ko = row('KO_REP1').querySelector('td[data-annotation=condition]')
  assert.deepEqual(ko.querySelectorAll('[data-pick-value]').map(b => b.getAttribute('title')), [`ana@x, ${whenText(at(120))}`, `ben@x, ${whenText(at(110))}`])
  const input = ko.querySelector('input')
  input.value = ''
  await input.fire('change')
  assert.deepEqual(calls, [])
  await ko.querySelector('[data-clear-annotation]').click()
  assert.equal(calls.length, 1)
  assert.deepEqual([calls[0][0], calls[0][1], [...calls[0][2]].sort()], ['clearAnnotation', ids.P, [ids.K1, ids.K2].sort()])
})

test('a policy pin under any name the project has had counts, a superseded name included', async () => {
  installDom()
  const { explorer, ids } = await open({ retention: true, renamed: true })
  const page = await project(explorer, ids.P, ctxFor().ctx, { tab: 'selections' })
  assert.equal(page.querySelector('h1').textContent, 'RAP1 second')
  assert.deepEqual(page.querySelector(`tr[data-member="${ids.S2}"]`).querySelectorAll('li[data-pin]').map(li => li.dataset.claim), [ids.PO])
  assert.equal(page.querySelector('[data-not-pinned]'), null)
})

test('retention set twice to one value reads as on; two values say "retention in conflict" in the head', async () => {
  installDom()
  const same = await open({ retention: true, retention2: 'pin_selections' })
  const on = await project(same.explorer, same.ids.P, ctxFor().ctx, { tab: 'selections' })
  assert.match(text(on.querySelector('.subline').querySelector('[data-retention]')), /^pins every Selection/)
  assert.equal(on.querySelector('[data-not-pinned]').dataset.notPinned, '1', 'D5 still counts')
  const two = await open({ retention: true, retention2: 'none' })
  const page = await project(two.explorer, two.ids.P, ctxFor().ctx, { tab: 'overview' })
  const said = page.querySelector('.subline').querySelector('[data-retention]')
  assert.equal(said.dataset.retention, 'conflicted')
  assert.equal(text(said), 'retention in conflict')
  assert.ok(said.classList.contains('warn'))
})

test('turning retention on, a pin refused: the retry pins what is left and writes no second set', async () => {
  installDom()
  const { explorer, ids } = await open()
  const { ctx, calls, ran } = ctxFor({ failOnce: ['pin'] })
  const page = await project(explorer, ids.P, ctx, { tab: 'overview' })
  await page.querySelector('#project-edit-open').click()
  await page.querySelector('#project-retention-on').click()
  const go = page.querySelector('#project-pin-go')
  await until(() => !go.disabled, 'the pin figures')
  await go.click()
  assert.ok(ran.at(-1).error, 'the pin was refused')
  await go.click()
  assert.deepEqual(calls, [['setPolicy', ids.P, 'retention', 'pin_selections', []], ['pin', ids.S2, `project policy: ${NAME}`],
    ['pin', ids.S2, `project policy: ${NAME}`]])
  assert.ok(ran.at(-1).done)
})

test('Add to project… with retention on, the pin refused: the retry pins, and writes no second membership', async () => {
  installDom()
  const { explorer, ids } = await open({ retention: true })
  const { ctx, calls } = ctxFor({ failOnce: ['pin'] })
  const page = await project(explorer, ids.P, ctx, { tab: 'overview' })
  explorer.searchAddresses = async () => [{ cid: 'bafyreinewselection', kind: 'selection', label: 'new picks', href: '#/selection/bafyreinewselection' }]
  explorer.selectionSize = async () => ({ bytes: 2_500_000, partial: false })
  const panelEl = page.querySelector('[data-project-add]')
  panelEl.querySelector('#project-add-query').value = 'bafyreinewselection'
  await panelEl.querySelector('#project-add-find').click()
  await until(() => panelEl.querySelector('[data-add-result]'), 'the result')
  const add = panelEl.querySelector('[data-add]')
  await until(() => !add.disabled, 'the bytes')
  await add.click()
  await add.click()
  const pin = ['pin', 'bafyreinewselection', `project policy: ${NAME}`]
  assert.deepEqual(calls, [['addToProject', 'bafyreinewselection', ids.P], pin, pin])
})

test('two members: Activity, Runs and Selections say which store they read, and link the same tab in the other', async () => {
  installDom()
  const lab = await open()
  const org = await open({ stats: 3000 })
  const list = [{ alias: 'lab', key: 'a', writable: true, base: 'm/lab/' }, { alias: 'org', key: 'b', writable: false, base: 'm/org/' }]
  const members = createMembers({ member: 'lab', members: list }, { openExplorer: async (m) => ({ lab: lab.explorer, org: org.explorer })[m.alias] })
  const { ctx } = ctxFor({ members })
  ctx.store = { ...ctx.store, hrefIn: (alias, hash) => `?member=${alias}${hash}` }
  const metaOf = (page, title) => page.querySelectorAll('.panel').find(p => p.querySelector('h2')?.textContent === title).querySelector('.ph').querySelector('.muted').textContent
  for (const [tab, title, meta] of [['activity', 'Activity', 'newest first · in lab'], ['runs', 'Runs', 'whole runs in this project · in lab'],
    ['selections', 'Selections', 'in lab'], ['overview', 'Activity', 'in lab']]) {
    const page = await project(lab.explorer, lab.ids.P, ctx, { tab })
    assert.equal(metaOf(page, title), meta, tab)
    const line = page.querySelector('[data-other-members]')
    const there = tab === 'overview' ? 'activity' : tab
    assert.deepEqual(line.querySelectorAll('a').map(a => [a.textContent, a.getAttribute('href')]), [['org', `?member=org#/project/${lab.ids.P}?tab=${there}`]], tab)
    assert.match(text(line), /^Read from lab only\. The same in org\.$/)
    assert.ok(line.classList.contains('muted'))
  }
  // One member: no store named, no line.
  const one = await project(lab.explorer, lab.ids.P, ctxFor().ctx, { tab: 'activity' })
  assert.equal(metaOf(one, 'Activity'), 'newest first')
  assert.equal(one.querySelector('[data-other-members]'), null)
})
