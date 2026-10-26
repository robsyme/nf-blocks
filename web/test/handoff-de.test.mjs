// #/handoff/nf-core%2Fdifferentialabundance (P4 Task 5, mockups/14-handoff-de.html):
// the ticks from the URL, the conditions from the active project, the live
// checks (the mockup's acceptance at 50,000 reads, include all 5, the matrix
// columns, the GTF warning), the files and their cids, the command, Save's
// order and arguments, a failure part-way, the read-only preview, Add to
// project after a save with none, and the unknown template. A real Explorer
// over a small distraught_murdock; the writer is a fake that records calls.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { installDom, frame, strayTexts } from './dom.mjs'
import { buildMember, entryName, rangedFetch, rawCid } from './fixture.mjs'
import { snapshotDb } from './helpers.mjs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { panelState } from '../src/panel.js'
import { sizeText } from '../src/format.js'
import { WriteError } from '../src/write.js'
import { handoffTemplate } from '../src/views/handoff-de.js'
import { rawCidOf } from '../src/templates/differentialabundance.js'
import { whenText } from '../src/words.js'
import { APP_CSS } from '../src/styles.js'

const NOW = Date.parse('2026-10-25T12:00:00.000Z')
const PIPE = 'nf-core/rnaseq'
const NAME = 'RAP1 depletion'
const VIEWER = 'rob@x'
const READS = { RAP1_IAA_30M_REP1: 49791, RAP1_UNINDUCED_REP1: 49969, RAP1_UNINDUCED_REP2: 99971, WT_REP1: 99566, WT_REP2: 49760 }
const SAMPLES = Object.keys(READS).sort()
const SF = { RAP1_IAA_30M_REP1: 1100, RAP1_UNINDUCED_REP1: 1200, RAP1_UNINDUCED_REP2: 1300, WT_REP1: 1400, WT_REP2: 1500 }
const CONDITION = { RAP1_IAA_30M_REP1: 'auxin_30m', RAP1_UNINDUCED_REP1: 'uninduced', RAP1_UNINDUCED_REP2: 'uninduced', WT_REP1: 'wild_type', WT_REP2: 'wild_type' }
const THRESHOLD = JSON.stringify([['preprocessed:num_trimmed_reads', 'int', '50000', '>=']])
const MATRIX = (ids = SAMPLES) => `gene_id\tgene_name\t${ids.join('\t')}\nENSG1\tg1\t${ids.map((_, i) => i).join('\t')}\n`
const TEMPLATE = 'nf-core/differentialabundance'

const at = (minutes) => new Date(NOW - minutes * 60_000).toISOString()
const leaf = (name, address, size) => ({ kind: 'Leaf', name, address, size, provider: 'head-node', reason: null })
const claim = (subject, verb, attribute, value, { supersedes = [], by = 'ana@x', minutes = 60 } = {}) =>
  ({ kind: 'Claim', schema: 1, asserted_by: by, subject, verb, attribute, value, supersedes, timestamp: at(minutes) })

/**
 * distraught_murdock in small, a tail run: per-sample preprocessed and
 * quant_salmon, run-level quant_merged. Options: `matrix` (its header's
 * samples), `gtf` ('none': no InputSet row; 'unheld': a param:gtf row whose
 * bytes no store holds; 'held': the GTF stored here), `project` (P with
 * `conditions`, by ana@x), `retention` (P pins every Selection).
 */
async function open({ matrix = SAMPLES, gtf = 'none', project = false, conditions = CONDITION, retention = false, conflict = null } = {}) {
  const ids = {}
  const member = await buildMember({
    now: NOW,
    extra: ({ put, putRaw, at: times }) => {
      let tick = 0
      const log = []
      const entry = (kind, cid) => { log.push(entryName(times.R3 + 2000 + (tick++) * 1000, kind, cid)); return cid }
      const m = put({ kind: 'RunManifest', schema: 1, asserted_by: 'ana@x', pipeline: PIPE, repository: null, revision: null, commit_id: null,
        run_name: 'distraught_murdock', nf_run_hash: 'hash-dm', session_id: 's', resumed: false, nextflow_version: '26.04.6', params: {}, config: {},
        script: null, started_at: '2026-09-01T00:00:00.000Z' })
      const matrixText = MATRIX(matrix)
      const matrixCid = putRaw(matrixText)
      const outputs = {
        preprocessed: SAMPLES.map(id => [{ meta: { id }, num_trimmed_reads: READS[id] }, leaf(`${id}.fastq.gz`, rawCid(`fq-${id}`), 100)]),
        quant_salmon: SAMPLES.map(id => [{ meta: { id } }, leaf(`${id}.sf`, rawCid(`sf-${id}`), SF[id])]),
        quant_merged: [[{ id: 'all_samples' }, { counts: leaf('salmon.merged.gene_counts.tsv', matrixCid, matrixText.length),
          tpm: leaf('salmon.merged.gene_tpm.tsv', rawCid('tpm'), 4000) }]],
      }
      ids.items = {}
      ids.collections = {}
      const colls = Object.entries(outputs).map(([name, values]) => {
        const cids = values.map(value => put({ kind: 'OutputItem', schema: 1, value }))
        ids.items[name] = Object.fromEntries(cids.map((c, i) => [values[i][0].meta?.id ?? 'run', c.toString()]))
        const sorted = [...cids].sort((a, b) => (a.toString() < b.toString() ? -1 : 1))
        const coll = put({ kind: 'OutputCollection', schema: 1, asserted_by: 'ana@x', run: m, name, items: sorted, paths: sorted.map((_, i) => [`${name}/${i}`]) })
        ids.collections[name] = coll.toString()
        return coll
      })
      ids.gtf = gtf === 'held' ? putRaw('##gtf\n').toString() : rawCid('gtf-not-stored').toString()
      const inputSet = gtf === 'none' ? null : put({ kind: 'InputSet', schema: 1, asserted_by: 'ana@x', inputs: [
        { role: 'param:gtf', name: 'genes.gtf', address: CID.parse(ids.gtf), size: 6, sample: null, reason: null }] })
      const comp = put({ kind: 'RunCompletion', schema: 1, asserted_by: 'ana@x', run: m, collections: colls, input_set: inputSet, status: 'succeeded',
        exit_status: 0, possibly_incomplete: false, started_at: '2026-09-01T00:00:00.000Z', finished_at: '2026-09-04T10:00:00.000Z',
        anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }, error: null })
      ids.completion = entry('run', comp).toString()
      ids.matrix = matrixCid.toString()
      if (project) {
        ids.P = entry('project', put({ kind: 'Project', schema: 1, asserted_by: 'ana@x', name: NAME, description: '', created_at: at(500) }))
        ids.claims = {}
        for (const [id, value] of Object.entries(conditions)) {
          ids.claims[id] = entry('claim', put(claim(ids.P, 'add', 'annotation', { sample: { pipeline: PIPE, id }, name: 'condition', value }, { minutes: 100 }))).toString()
        }
        // `conflict`: [id, value, by], a second current value for one sample.
        if (conflict) ids.conflict = entry('claim', put(claim(ids.P, 'add', 'annotation', { sample: { pipeline: PIPE, id: conflict[0] }, name: 'condition', value: conflict[1] },
          { by: conflict[2], minutes: 80 }))).toString()
        if (retention) entry('claim', put(claim(ids.P, 'set', 'policy.retention', 'pin_selections', { minutes: 90 })))
        ids.P = ids.P.toString()
      }
      return { log, rows: null }
    },
  })
  const explorer = await Explorer.open({ base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: rangedFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }), now: () => NOW })
  return { explorer, ids }
}

const SEL = rawCid('the-selection').toString()
const HANDOFF = rawCid('the-handoff').toString()

/** A ctx whose writer records each call; `fail` names a call that refuses (a WriteError), `failAt` which time it refuses. */
function ctxFor({ can = true, fail = null, failAt = 1 } = {}) {
  const calls = []
  const counts = new Map()
  const record = (name, answer) => async (...args) => {
    calls.push([name, ...args])
    counts.set(name, (counts.get(name) ?? 0) + 1)
    if (name === fail && counts.get(name) === failAt) throw new WriteError('stale_supersedes', `${name} refused`, '', 400)
    return answer(...args)
  }
  const writer = {
    selection: record('selection', () => ({ address: SEL })),
    rename: record('rename', () => ({ address: rawCid(`n-${calls.length}`).toString() })),
    handoff: record('handoff', (body) => ({ address: HANDOFF, written: true, entry: 'log/x',
      files: body.files.map(f => ({ name: f.name, address: rawCidOf(f.text), size: f.text.length })),
      params: Object.fromEntries(Object.entries(body.params).map(([k, v]) => [k, typeof v === 'object'
        ? `cas://${rawCidOf(body.files.find(f => f.name === v.file).text)}/${v.file}` : v])) })),
    annotate: record('annotate', () => ({ address: rawCid(`a-${calls.length}`).toString() })),
    pin: record('pin', () => ({ address: rawCid('pin').toString() })),
    addToProject: record('addToProject', () => ({ address: rawCid('member').toString() })),
  }
  const ran = []
  const used = []
  const ctx = { mark: () => {}, use: (v) => used.push(v), viewer: VIEWER, members: null, store: { name: 'lab', key: 'a', all: false },
    write: { available: can, here: can, writable: 'lab', reason: can ? null : 'Read only here: writing needs this page opened through `nextflow plugin nf-blocks:explore`.',
      hrefFor: (h) => h, writer,
      // runWrite's shape: a `stay` goes in the status, a throw shows its message.
      run: async (status, fn) => {
        try {
          const done = await fn()
          ran.push({ done })
          if (done?.stay) status.replaceChildren(done.stay)
        } catch (e) {
          ran.push({ error: e })
          status.replaceChildren(e.message)
        }
      } } }
  return { ctx, calls, ran, used }
}

const text = (el) => el.textContent.replace(/\s+/g, ' ').trim()
/** A check as it reads: its mark, a space, its message. */
const said = (el) => el.querySelectorAll('span').map(s => s.textContent).join(' ')
async function until(fn, what) {
  for (let i = 0; i < 100; i++) { if (fn()) return; await frame() }
  assert.fail(`timed out waiting for ${what}`)
}

/** The page and its rail (one element holding the sections the view gave ctx.use), with the matrix header read. */
async function page(explorer, ctx, used, query) {
  const node = await handoffTemplate(explorer, TEMPLATE, ctx, { samples: [], outputs: [], where: null, project: null, ...query })
  const rail = document.createElement('div')
  rail.append(...used.at(-1).rail)
  await until(() => !/Reading the matrix/.test(node.querySelector('[data-checks]').textContent), 'the matrix header')
  return { node, rail }
}
const check = (node, key) => node.querySelector(`[data-check=${key}]`)
const ticked = (node) => node.querySelectorAll('input[data-id]').filter(b => b.checked).map(b => b.dataset.id)
const condition = (node, id) => node.querySelector(`input[data-c=${id}]`)
async function type(input, value) {
  input.value = value
  await input.fire('input')
}
async function tick(node, id, on) {
  const box = node.querySelector(`input[data-id=${id}]`)
  box.checked = on
  await box.fire('change')
}

test('mockup acceptance: at 50,000 reads two samples are ticked, one per condition, and the replicate check fails with the mockup message', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true })
  const { ctx, used } = ctxFor()
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion, where: THRESHOLD, project: ids.P })
  assert.equal(node.querySelector('h1').textContent, 'Hand off to nf-core/differentialabundance')
  assert.deepEqual(node.querySelector('.crumbs').querySelectorAll('a').map(a => a.textContent), ['distraught_murdock', 'Samples'])
  assert.deepEqual(node.querySelector('.subline').children.map(s => text(s)),
    ['From distraught_murdock', 'filter: reads (trimmed) ≥ 50,000', 'matrix: salmon.merged.gene_counts.tsv', 'project: RAP1 depletion'])
  assert.equal(node.querySelector('.subline').querySelector('.addr').dataset.ref, `cas://${ids.matrix}/salmon.merged.gene_counts.tsv`)
  // Rows: every sample, Reads (trimmed), the condition from the project with its writer's avatar.
  assert.deepEqual(node.querySelectorAll('tr[data-sample]').map(r => r.dataset.sample), SAMPLES)
  assert.deepEqual(node.querySelector('thead').querySelectorAll('th').map(t => t.textContent), ['', 'sample', 'Reads (trimmed)', 'condition'])
  assert.equal(node.querySelector('tr[data-sample=WT_REP1]').querySelectorAll('td')[2].textContent, '99,566')
  assert.equal(condition(node, 'WT_REP1').value, 'wild_type')
  assert.equal(node.querySelector('[data-condition=WT_REP1]').querySelector('.av').getAttribute('title'), 'ana@x')
  assert.equal(node.querySelector('[data-condition=WT_REP1]').getAttribute('title'), `ana@x, ${whenText(at(100))}`)
  assert.deepEqual(ticked(node), ['RAP1_UNINDUCED_REP2', 'WT_REP1'])
  assert.equal(node.querySelector('tr[data-sample=WT_REP2]').getAttribute('class'), 'dim')
  assert.equal(node.querySelector('#all').textContent, 'Include all 5')
  // The contrast picks uninduced against wild_type.
  assert.deepEqual([node.querySelector('#target').value, node.querySelector('#ref').value], ['uninduced', 'wild_type'])
  assert.deepEqual(node.querySelector('#target').querySelectorAll('option').map(o => o.textContent), ['uninduced', 'wild_type'])
  assert.equal(said(check(node, 'contrast')), '✓ Contrast uninduced vs wild_type')
  assert.equal(check(node, 'replicates').dataset.ok, 'false')
  assert.equal(said(check(node, 'replicates')),
    '! Each side needs at least 2 replicates: uninduced has 1, wild_type has 1. The reads (trimmed) ≥ 50,000 filter leaves one sample per condition.')
  assert.equal(said(check(node, 'matrix')), '✓ All 2 samples are columns in the matrix')
  assert.equal(node.querySelector('[data-check=gtf]').getAttribute('class'), 'note warn')
  assert.equal(text(node.querySelector('[data-matrix-note]')), "differentialabundance reads only the samplesheet's samples from the matrix, so the whole matrix is passed.")
  // The files and their cids, live.
  const sheet = 'sample,condition\nRAP1_UNINDUCED_REP2,uninduced\nWT_REP1,wild_type\n'
  assert.equal(node.querySelector('#ss').textContent, sheet)
  assert.equal(node.querySelector('[data-samplesheet-cid]').querySelector('.addr').dataset.ref, `cas://${rawCidOf(sheet)}/samplesheet.csv`)
  const contrasts = 'id,variable,reference,target\nuninduced_vs_wild_type,condition,wild_type,uninduced\n'
  assert.equal(node.querySelector('[data-contrasts]').textContent, `contrasts.csv\n${contrasts}`)
  // The rail: what Save writes, Save off, the command with the computed cids.
  assert.deepEqual(rail.querySelectorAll('h3').map(x => x.textContent), ['Saves', 'Run command'])
  assert.deepEqual(rail.querySelector('[data-saves]').children.map(f => [f.children[0].textContent, f.childNodes.at(-1).textContent]),
    [['selection', 'samples + matrix'], ['name', '“uninduced vs wild_type (differentialabundance)”'], ['added to', NAME],
      ['block', 'samplesheet.csv'], ['block', 'contrasts.csv'], ['block', 'hand-off'], ['condition', '× 0 samples']])
  assert.equal(rail.querySelector('#save').disabled, true)
  const cmd = rail.querySelector('#cmd').textContent
  assert.match(cmd, new RegExp(`--input {5}cas://${rawCidOf(sheet)}/samplesheet\\.csv \\\\`))
  assert.match(cmd, new RegExp(`--contrasts cas://${rawCidOf(contrasts)}/contrasts\\.csv \\\\`))
  assert.match(cmd, new RegExp(`--matrix {4}cas://${ids.matrix}/salmon\\.merged\\.gene_counts\\.tsv \\\\`))
  assert.doesNotMatch(cmd, /--gtf/)
  assert.match(cmd, /--study_type rnaseq \\\n {2}--outdir results$/)
  assert.equal(text(rail.querySelector('[data-cas-note]')), 'cas:// addresses resolve when the run loads the nf-blocks plugin with a store that holds them.')
  assert.deepEqual(strayTexts(node), [])
  assert.deepEqual(strayTexts(rail), [])
})

// Task 6: the long name tag made the rail's content wider than the rail, clipping Save hand-off and the command.
test('the rail fits its width: claim values are cut with their whole text in a title, Save spans the rail, the command scrolls', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true })
  const { ctx, used } = ctxFor()
  const { rail } = await page(explorer, ctx, used, { from: ids.completion, where: THRESHOLD, project: ids.P })
  const name = rail.querySelector('[data-name-claim]').querySelector('.val')
  assert.equal(name.textContent, '“uninduced vs wild_type (differentialabundance)”')
  assert.equal(name.getAttribute('title'), name.textContent)
  assert.equal(rail.querySelector('[data-member-claim]').querySelector('.val').getAttribute('title'), NAME)
  // A grid track that may shrink below its content, so a tag or the command can't widen the rail.
  assert.match(APP_CSS, /\.rail #panel-inner \{ min-width: 0; \}/)
  assert.match(APP_CSS, /\.rail \.sec \{ grid-template-columns: minmax\(0, 1fr\); \}/)
  assert.equal(rail.querySelector('#cmd').getAttribute('class'), 'code', 'pre.code scrolls sideways (kit: overflow-x: auto)')
})

test('include all 5 with uninduced ×2 and wild_type ×2: every blocking check passes and Save goes', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true })
  const { ctx, used } = ctxFor()
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion, where: THRESHOLD, project: ids.P })
  await node.querySelector('#all').click()
  assert.deepEqual(ticked(node), SAMPLES)
  assert.deepEqual([node.querySelector('#target').value, node.querySelector('#ref').value], ['uninduced', 'wild_type'])
  assert.deepEqual(node.querySelector('#ref').querySelectorAll('option').map(o => o.textContent), ['auxin_30m', 'uninduced', 'wild_type'])
  assert.equal(said(check(node, 'replicates')), '✓ Replicates: uninduced 2, wild_type 2')
  assert.equal(said(check(node, 'matrix')), '✓ All 5 samples are columns in the matrix')
  assert.equal(rail.querySelector('#save').disabled, false)
  assert.equal(node.querySelector('#ss').textContent.split('\n').length, 7)
  // Changing the contrast in its select redraws the files.
  const target = node.querySelector('#target')
  target.value = 'auxin_30m'
  await target.fire('change')
  assert.equal(said(check(node, 'replicates')), '! Each side needs at least 2 replicates: auxin_30m has 1, wild_type has 2.')
  assert.equal(rail.querySelector('#save').disabled, true)
  assert.match(node.querySelector('[data-contrasts]').textContent, /auxin_30m_vs_wild_type,condition,wild_type,auxin_30m/)
})

test('conditions edit in place: the avatar becomes the viewer\'s, a bad value blocks with the reason, and without a project they start empty', async () => {
  installDom()
  const { explorer, ids } = await open()
  const { ctx, used } = ctxFor()
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion })
  assert.deepEqual(ticked(node), SAMPLES, 'no filter and no names: all ticked')
  assert.ok(SAMPLES.every(s => condition(node, s).value === ''))
  assert.equal(node.querySelector('[data-filter]'), null)
  assert.match(said(check(node, 'values')), /RAP1_IAA_30M_REP1, RAP1_UNINDUCED_REP1, RAP1_UNINDUCED_REP2, WT_REP1 and WT_REP2 have no condition\./)
  for (const s of SAMPLES) await type(condition(node, s), CONDITION[s])
  assert.equal(node.querySelector('[data-condition=WT_REP2]').querySelector('.av').getAttribute('title'), VIEWER)
  assert.equal(check(node, 'values'), null)
  assert.equal(rail.querySelector('#save').disabled, false)
  await type(condition(node, 'RAP1_IAA_30M_REP1'), 'auxin 30m')
  assert.equal(said(check(node, 'values')), '! Conditions may use only letters, digits, _ . and -: "auxin 30m" (RAP1_IAA_30M_REP1).')
  assert.equal(rail.querySelector('#save').disabled, true)
  // No project: the rail says the conditions stay in the file, and no condition Claims are listed.
  assert.ok(rail.querySelector('[data-no-project]'))
  assert.equal(rail.querySelector('[data-condition-claims]'), null)
})

test('samples= ticks those names over the filter', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true })
  const { ctx, used } = ctxFor()
  const { node } = await page(explorer, ctx, used, { from: ids.completion, where: THRESHOLD, project: ids.P, samples: ['WT_REP2', 'WT_REP1', 'NOT_HERE'] })
  assert.deepEqual(ticked(node), ['WT_REP1', 'WT_REP2'])
  // Ticked by name, not by the filter: no sentence about the filter.
  assert.equal(said(check(node, 'replicates')), '! Each side needs at least 2 replicates: wild_type has 2, — has 0.')
})

test('the matrix-column check: a sample the matrix header lacks fails and blocks Save', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true, matrix: SAMPLES.filter(s => s !== 'WT_REP2') })
  const { ctx, used } = ctxFor()
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion, project: ids.P })
  assert.equal(said(check(node, 'matrix')), '! WT_REP2 is not a column in the matrix, and differentialabundance stops on a samplesheet sample the matrix lacks.')
  assert.equal(rail.querySelector('#save').disabled, true)
  await tick(node, 'WT_REP2', false)
  assert.equal(said(check(node, 'matrix')), '✓ All 4 samples are columns in the matrix')
  // WT_REP2 out leaves wild_type with one.
  assert.equal(check(node, 'replicates').dataset.ok, 'false')
})

test('the GTF: a warning with a field when the run\'s GTF is not stored; typed in, it passes and joins the command', async () => {
  for (const gtf of ['none', 'unheld']) {
    installDom()
    const { explorer, ids } = await open({ project: true, gtf })
    const { ctx, used } = ctxFor()
    const { node, rail } = await page(explorer, ctx, used, { from: ids.completion, project: ids.P })
    await node.querySelector('#all').click()
    const warn = check(node, 'gtf')
    assert.equal(warn.getAttribute('class'), 'note warn', gtf)
    assert.equal(said(warn), '! GTF not in lab: rnaseq read it by path. Without one, differentialabundance uses the matrix as its feature table, '
      + 'and the annotated results may carry counts for samples left out. Give a path or a cas:// address.')
    assert.equal(rail.querySelector('#save').disabled, false, 'a missing GTF does not block')
    const field = node.querySelector('#gtf')
    await type(field, '/refs/genes.gtf')
    assert.equal(said(check(node, 'gtf')), '✓ GTF: /refs/genes.gtf')
    assert.match(rail.querySelector('#cmd').textContent, /\n {2}--gtf {7}\/refs\/genes\.gtf \\\n/)
  }
})

test('the GTF: a param:gtf input the store holds is used as cas://, with no field', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true, gtf: 'held' })
  const { ctx, used } = ctxFor()
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion, project: ids.P })
  assert.equal(said(check(node, 'gtf')), "✓ GTF: genes.gtf from the run's inputs, held in lab")
  assert.equal(node.querySelector('#gtf'), null)
  assert.match(rail.querySelector('#cmd').textContent, new RegExp(`--gtf {7}cas://${ids.gtf}/genes\\.gtf \\\\`))
})

test('Save, in order: the Selection (samples\' items and the matrix item), its name, the hand-off, its membership, the changed conditions superseding, the policy pin', async () => {
  installDom()
  const conditions = { ...CONDITION, RAP1_IAA_30M_REP1: 'auxin', WT_REP2: undefined }
  delete conditions.WT_REP2
  const { explorer, ids } = await open({ project: true, retention: true, conditions })
  const { ctx, calls, used, ran } = ctxFor()
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion, where: THRESHOLD, project: ids.P, outputs: ['quant_salmon', 'quant_merged'] })
  await node.querySelector('#all').click()
  await type(condition(node, 'WT_REP2'), 'wild_type')
  await type(condition(node, 'RAP1_IAA_30M_REP1'), 'auxin_30m')
  // Before Save: two condition Claims and the pin's bytes, from the items' file maps.
  assert.equal(rail.querySelector('[data-condition-claims]').dataset.conditionClaims, '2')
  await until(() => !/…/.test(rail.querySelector('[data-pin-claim]').textContent), 'the pin bytes')
  const bytes = Object.values(SF).reduce((a, b) => a + b, 0) + MATRIX().length + 4000
  assert.equal(text(rail.querySelector('[data-pin-claim]')), `pinkeeps about ${sizeText({ bytes, partial: false })} in lab`)
  assert.equal(rail.querySelector('[data-name-claim]').dataset.nameClaim, 'uninduced vs wild_type (differentialabundance)')
  assert.equal(rail.querySelector('[data-member-claim]').dataset.memberClaim, ids.P)
  assert.equal(text(rail.querySelector('[data-member-claim]')), `added to${NAME}`)
  assert.equal(text(rail.querySelector('[data-pin-note]')), `${NAME} pins every Selection in it, in lab. The pin's note is "project policy: ${NAME}".`)
  const save = rail.querySelector('#save')
  assert.equal(save.disabled, false)
  await save.click()
  assert.deepEqual(calls.map(c => c[0]), ['selection', 'rename', 'handoff', 'addToProject', 'annotate', 'annotate', 'pin'])
  // 1. The Selection: the five samples' quant_salmon items and the one quant_merged item, each with its collection.
  const members = calls[0][1].map(m => [String(m.item.address), m.item.via.map(String)])
  assert.deepEqual(members.sort(), [...SAMPLES.map(s => [ids.items.quant_salmon[s], [ids.collections.quant_salmon]]),
    [ids.items.quant_merged.run, [ids.collections.quant_merged]]].sort())
  // 2. Its name, a new Selection's first.
  assert.deepEqual(calls[1], ['rename', SEL, 'uninduced vs wild_type (differentialabundance)', []])
  // 3. The hand-off.
  const sheet = `sample,condition\n${SAMPLES.map(s => `${s},${CONDITION[s]}`).join('\n')}\n`
  const contrasts = 'id,variable,reference,target\nuninduced_vs_wild_type,condition,wild_type,uninduced\n'
  assert.deepEqual(calls[2][1], { template: TEMPLATE, selection: SEL, project: ids.P,
    files: [{ name: 'samplesheet.csv', text: sheet }, { name: 'contrasts.csv', text: contrasts }],
    params: { input: { file: 'samplesheet.csv' }, contrasts: { file: 'contrasts.csv' }, matrix: `cas://${ids.matrix}/salmon.merged.gene_counts.tsv`, study_type: 'rnaseq' } })
  // 4. The Selection joins the project.
  assert.deepEqual(calls[3], ['addToProject', SEL, ids.P])
  // 5. The conditions that differ from the project's, each superseding its cell's current Claims.
  assert.deepEqual(calls[4], ['annotate', ids.P, { pipeline: PIPE, id: 'RAP1_IAA_30M_REP1' }, 'condition', 'auxin_30m', [ids.claims.RAP1_IAA_30M_REP1]])
  assert.deepEqual(calls[5], ['annotate', ids.P, { pipeline: PIPE, id: 'WT_REP2' }, 'condition', 'wild_type', []])
  // Each written cell now says who wrote it and when.
  const written = node.querySelector('[data-condition=WT_REP2]')
  assert.match(written.getAttribute('title'), new RegExp(`^${VIEWER}, \\S`))
  assert.equal(written.querySelector('.av').getAttribute('title'), VIEWER)
  // 6. The policy pin, after the membership.
  assert.deepEqual(calls[6], ['pin', SEL, `project policy: ${NAME}`])
  // After Save: the Handoff's seal and address, the samplesheet's reference, the Selection and the project.
  assert.equal(ran.at(-1).done.address, HANDOFF)
  const done = rail.querySelector('[data-handoff-saved]')
  assert.equal(done.dataset.handoffSaved, HANDOFF)
  assert.equal(done.querySelector('.seal').dataset.seal, HANDOFF)
  assert.equal(done.querySelector('.addr').dataset.address, HANDOFF)
  assert.equal(done.querySelector('[data-saved-samplesheet]').querySelector('.addr').dataset.ref, `cas://${rawCidOf(sheet)}/samplesheet.csv`)
  assert.deepEqual(done.querySelectorAll('a').map(a => a.getAttribute('href')), [`#/selection/${SEL}`, `#/project/${ids.P}`])
  assert.equal(save.disabled, true, 'saved: nothing new to save')
  // An edit after Save drops the saved state.
  await type(condition(node, 'WT_REP2'), 'wt')
  await type(condition(node, 'WT_REP1'), 'wt')
  assert.equal(rail.querySelector('[data-handoff-saved]'), null)
  // The rail counts only what is still to write: after Save, nothing; after these edits, two.
  assert.equal(rail.querySelector('[data-condition-claims]').dataset.conditionClaims, '2')
  calls.length = 0
  await rail.querySelector('#save').click()
  // Saved again: the same Selection, already a member and pinned, is not written again; its new name supersedes the first,
  // and each condition supersedes the Claim the first Save wrote (or the project's).
  assert.deepEqual(calls.map(c => c[0]), ['rename', 'handoff', 'annotate', 'annotate'])
  assert.deepEqual(calls[0], ['rename', SEL, 'uninduced vs wt (differentialabundance)', [rawCid('n-2').toString()]])
  assert.deepEqual(calls[2].slice(2), [{ pipeline: PIPE, id: 'WT_REP1' }, 'condition', 'wt', [ids.claims.WT_REP1]])
  assert.deepEqual(calls[3].slice(2), [{ pipeline: PIPE, id: 'WT_REP2' }, 'condition', 'wt', [rawCid('a-6').toString()]])
  assert.equal(rail.querySelector('[data-condition-claims]').dataset.conditionClaims, '0')
})

test('after Save the command comes from the server\'s unscrubbed params', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true })
  const { ctx, used } = ctxFor()
  ctx.write.writer.handoff = async (body) => ({ address: HANDOFF, written: true, files: [],
    params: { input: 'cas://bafkreiserver/samplesheet.csv', contrasts: 'cas://bafkreiserver2/contrasts.csv', matrix: body.params.matrix,
      gtf: '/home/ana/refs/genes.gtf', study_type: 'rnaseq' } })
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion, project: ids.P })
  await type(node.querySelector('#gtf'), '/home/ana/refs/genes.gtf')
  await rail.querySelector('#save').click()
  const cmd = rail.querySelector('#cmd').textContent
  assert.match(cmd, /--input {5}cas:\/\/bafkreiserver\/samplesheet\.csv/)
  assert.match(cmd, /--gtf {7}\/home\/ana\/refs\/genes\.gtf/)
})

test('a failure part-way says what was written: the hand-off refused after the Selection; a condition refused after the first', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true, conditions: { ...CONDITION, WT_REP1: 'wt', WT_REP2: 'wt' } })
  const one = ctxFor({ fail: 'handoff' })
  const a = await page(explorer, one.ctx, one.used, { from: ids.completion, project: ids.P })
  await a.rail.querySelector('#save').click()
  assert.deepEqual(one.calls.map(c => c[0]), ['selection', 'rename', 'handoff'])
  const partial = a.rail.querySelector('[data-handoff-partial]')
  assert.equal(text(partial.querySelector('p')), `Saved the Selection ${SEL.slice(0, 4)}…${SEL.slice(-6)} and its name, but the hand-off failed. `
    + 'Nothing after it was written; Save again to go on from there.')
  assert.equal(partial.querySelector('a').getAttribute('href'), `#/selection/${SEL}`)
  // The refusal as runWrite words it, with the stale supersedes hint.
  const why = partial.querySelector('[data-error]')
  assert.equal(why.dataset.error, 'stale_supersedes')
  assert.equal(text(why), 'handoff refused Someone changed this since the page loaded; reload to see the current state.')

  installDom()
  const two = ctxFor({ fail: 'annotate', failAt: 2 })
  const b = await page(explorer, two.ctx, two.used, { from: ids.completion, project: ids.P })
  await type(condition(b.node, 'WT_REP1'), 'wild_type')
  await type(condition(b.node, 'WT_REP2'), 'wild_type')
  await b.rail.querySelector('#save').click()
  assert.deepEqual(two.calls.map(c => c[0]), ['selection', 'rename', 'handoff', 'addToProject', 'annotate', 'annotate'])
  assert.match(text(b.rail.querySelector('[data-handoff-partial]').querySelector('p')),
    /^Saved the Selection \S+, its name, the hand-off \S+, its membership of RAP1 depletion and 1 of 2 conditions, but the condition for WT_REP2 failed\. /)
  // The rail now counts only the condition still to write.
  assert.equal(b.rail.querySelector('[data-condition-claims]').dataset.conditionClaims, '1')
  // Save again goes on from the step that failed: no second Selection, name, Handoff or membership, and WT_REP1 is not written again.
  two.calls.length = 0
  await b.rail.querySelector('#save').click()
  assert.deepEqual(two.calls.map(c => c[0]), ['annotate'])
  assert.deepEqual(two.calls[0].slice(2), [{ pipeline: PIPE, id: 'WT_REP2' }, 'condition', 'wild_type', [ids.claims.WT_REP2]])
  assert.equal(b.rail.querySelector('[data-handoff-saved]').dataset.handoffSaved, HANDOFF)
  assert.equal(b.rail.querySelector('[data-handoff-partial]'), null)

  installDom()
  const three = ctxFor({ fail: 'selection' })
  const c = await page(explorer, three.ctx, three.used, { from: ids.completion, project: ids.P })
  await c.rail.querySelector('#save').click()
  assert.deepEqual(three.calls.map(x => x[0]), ['selection'])
  assert.equal(text(c.rail.querySelector('[data-handoff-status]')), 'selection refused')
})

test('read only: a preview with checks, files, cids and command, and Save off with the reason', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true })
  const { ctx, used, calls } = ctxFor({ can: false })
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion, project: ids.P })
  assert.equal(rail.querySelector('#save').disabled, true)
  assert.match(text(rail.querySelector('[data-unavailable]')), /Read only here/)
  assert.equal(said(check(node, 'replicates')), '✓ Replicates: uninduced 2, wild_type 2')
  const sheet = node.querySelector('#ss').textContent
  assert.match(rail.querySelector('#cmd').textContent, new RegExp(`cas://${rawCidOf(sheet)}/samplesheet\\.csv`))
  await type(condition(node, 'WT_REP2'), 'other')
  assert.equal(check(node, 'replicates').dataset.ok, 'false')
  assert.deepEqual(calls, [])
})

test('without a project: no condition Claims; Add to project then adds the Selection and writes the conditions there', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true, conditions: { WT_REP1: 'wild_type' } })
  const { ctx, calls, used } = ctxFor()
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion })
  for (const s of SAMPLES) await type(condition(node, s), CONDITION[s])
  await rail.querySelector('#save').click()
  assert.deepEqual(calls.map(c => c[0]), ['selection', 'rename', 'handoff'])
  assert.equal(calls[2][1].project, null)
  const keep = rail.querySelector('[data-add-project]')
  assert.match(text(keep), /No project was active, so no condition Claims were written\./)
  await keep.querySelector('[data-add-project-open]').click()
  const choose = keep.querySelector('[data-add-project-choose]')
  assert.deepEqual(choose.querySelectorAll('option').map(o => o.textContent), ['Choose a project', NAME])
  choose.value = ids.P
  await choose.fire('change')
  assert.equal(text(keep.querySelector('[data-add-project-note]')), `Writes 4 conditions to ${NAME}.`)
  await keep.querySelector('[data-add-project-go]').click()
  assert.deepEqual(calls.slice(3).map(c => c[0]), ['addToProject', 'annotate', 'annotate', 'annotate', 'annotate'])
  assert.deepEqual(calls[3], ['addToProject', SEL, ids.P])
  assert.deepEqual(calls.slice(4).map(c => [c[1], c[2].id, c[3], c[4]]), ['RAP1_IAA_30M_REP1', 'RAP1_UNINDUCED_REP1', 'RAP1_UNINDUCED_REP2', 'WT_REP2']
    .map(id => [ids.P, id, 'condition', CONDITION[id]]))
  assert.match(text(keep.querySelector('[data-added-to-project]')), new RegExp(`Added the Selection and 4 conditions to ${NAME}\\.`))
})

test('Add to project after Save: a condition refused after the membership says what was written', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true, conditions: {} })
  const { ctx, calls, used } = ctxFor({ fail: 'annotate', failAt: 2 })
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion })
  for (const s of SAMPLES) await type(condition(node, s), CONDITION[s])
  await rail.querySelector('#save').click()
  const keep = rail.querySelector('[data-add-project]')
  await keep.querySelector('[data-add-project-open]').click()
  const choose = keep.querySelector('[data-add-project-choose]')
  choose.value = ids.P
  await choose.fire('change')
  await keep.querySelector('[data-add-project-go]').click()
  assert.deepEqual(calls.slice(3).map(c => c[0]), ['addToProject', 'annotate', 'annotate'])
  const partial = keep.querySelector('[data-add-project-partial]')
  assert.equal(text(partial.querySelector('p')),
    `Added the Selection to ${NAME} and 1 of 5 conditions, but the condition for RAP1_UNINDUCED_REP1 failed. Nothing after it was written.`)
  assert.equal(partial.querySelector('[data-error]').dataset.error, 'stale_supersedes')
  // Add again: no second membership, the project read again, and only the conditions still pending written.
  let reads = 0
  const read = explorer.project.bind(explorer)
  explorer.project = (cid) => { reads++; return read(cid) }
  calls.length = 0
  await keep.querySelector('[data-add-project-go]').click()
  assert.equal(reads, 1)
  assert.deepEqual(calls.map(c => [c[0], c[2].id]), ['RAP1_UNINDUCED_REP1', 'RAP1_UNINDUCED_REP2', 'WT_REP1', 'WT_REP2'].map(id => ['annotate', id]))
  assert.match(text(keep.querySelector('[data-added-to-project]')), new RegExp(`Added the Selection and 5 conditions to ${NAME}\\.`))
})

test('a first Save refused at the name or the hand-off shows what was written, and the next Save goes on', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true })
  const named = ctxFor({ fail: 'rename' })
  const a = await page(explorer, named.ctx, named.used, { from: ids.completion, project: ids.P })
  await a.rail.querySelector('#save').click()
  assert.deepEqual(named.calls.map(c => c[0]), ['selection', 'rename'])
  assert.equal(named.ran.at(-1).done.address, SEL)
  const note = a.rail.querySelector('[data-handoff-partial]')
  assert.equal(note.dataset.handoffPartial, 'naming the Selection')
  assert.equal(text(note.querySelector('p')), `Saved the Selection ${SEL.slice(0, 4)}…${SEL.slice(-6)}, but naming the Selection failed. `
    + 'Nothing after it was written; Save again to go on from there.')
  named.calls.length = 0
  await a.rail.querySelector('#save').click()
  assert.deepEqual(named.calls.map(c => c[0]), ['rename', 'handoff', 'addToProject'])

  installDom()
  const handed = ctxFor({ fail: 'handoff' })
  const b = await page(explorer, handed.ctx, handed.used, { from: ids.completion, project: ids.P })
  await b.rail.querySelector('#save').click()
  assert.deepEqual(handed.calls.map(c => c[0]), ['selection', 'rename', 'handoff'])
  assert.equal(handed.ran.at(-1).done.address, SEL)
  assert.equal(text(b.rail.querySelector('[data-handoff-partial]').querySelector('p')),
    `Saved the Selection ${SEL.slice(0, 4)}…${SEL.slice(-6)} and its name, but the hand-off failed. Nothing after it was written; Save again to go on from there.`)
  handed.calls.length = 0
  await b.rail.querySelector('#save').click()
  assert.deepEqual(handed.calls.map(c => c[0]), ['handoff', 'addToProject'])
  assert.equal(b.rail.querySelector('[data-handoff-saved]').dataset.handoffSaved, HANDOFF)
})

test('a hand-off refused for edited files keeps the last one: back to the saved files, Save writes no second Handoff', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true })
  const { ctx, calls, used } = ctxFor({ fail: 'handoff', failAt: 2 })
  const { node, rail } = await page(explorer, ctx, used, { from: ids.completion, project: ids.P })
  const save = rail.querySelector('#save')
  await save.click()
  assert.deepEqual(calls.map(c => c[0]), ['selection', 'rename', 'handoff', 'addToProject'])
  const first = rail.querySelector('#cmd').textContent
  // Edited (a GTF), the hand-off is refused: the note does not count the first hand-off as this one.
  await type(node.querySelector('#gtf'), '/refs/genes.gtf')
  calls.length = 0
  await save.click()
  assert.deepEqual(calls.map(c => c[0]), ['handoff'])
  assert.equal(text(rail.querySelector('[data-handoff-partial]').querySelector('p')), `Saved the Selection ${SEL.slice(0, 4)}…${SEL.slice(-6)}, its name and its membership of ${NAME}, `
    + 'but the hand-off failed. Nothing after it was written; Save again to go on from there.')
  // Back to the first files: the first hand-off is the answer, and the command is built from it.
  await type(node.querySelector('#gtf'), '')
  calls.length = 0
  await save.click()
  assert.deepEqual(calls, [])
  assert.equal(rail.querySelector('[data-handoff-saved]').dataset.handoffSaved, HANDOFF)
  assert.equal(rail.querySelector('#cmd').textContent, first)
  assert.equal(save.disabled, true)
})

test('a conflicted condition starts empty, its title naming each value with who and when', async () => {
  installDom()
  const { explorer, ids } = await open({ project: true, conflict: ['WT_REP2', 'wt', 'ben@x'] })
  const { ctx, used } = ctxFor()
  const { node } = await page(explorer, ctx, used, { from: ids.completion, project: ids.P })
  assert.equal(condition(node, 'WT_REP2').value, '')
  assert.equal(node.querySelector('[data-condition=WT_REP2]').getAttribute('title'),
    `In conflict in the project: wild_type (ana@x, ${whenText(at(100))}); wt (ben@x, ${whenText(at(80))})`)
  // Typing makes it the viewer's value: the project's who and when no longer describe it.
  await type(condition(node, 'WT_REP2'), 'wild_type')
  assert.equal(node.querySelector('[data-condition=WT_REP2]').getAttribute('title'), null)
})

test('an unknown template gets its own page; the mockup\'s slug is differentialabundance; no run says where a hand-off starts', async () => {
  installDom()
  const { ctx } = ctxFor()
  const unknown = await handoffTemplate(null, 'nf-core/rnaseq', ctx, { samples: [], outputs: [] })
  assert.equal(unknown.querySelector('h1').textContent, 'No template for nf-core/rnaseq')
  assert.equal(unknown.dataset.templateUnknown, '')
  assert.match(text(unknown), /This explorer has a hand-off template for nf-core\/differentialabundance only\./)
  // #/handoff/nf-core-differentialabundance (mockup 14's route) is the same page as the encoded nf-core%2Fdifferentialabundance.
  const slug = await handoffTemplate(null, 'nf-core-differentialabundance', ctx, { samples: [], outputs: [] })
  assert.equal(slug.dataset.templateUnknown, undefined)
  assert.equal(slug.dataset.template, TEMPLATE)
  assert.match(text(slug), /A hand-off starts from an nf-core\/rnaseq run/)
  const none = await handoffTemplate(null, TEMPLATE, ctx, { samples: [], outputs: [] })
  assert.match(text(none), /A hand-off starts from an nf-core\/rnaseq run/)
})

test('the panel shows a template page\'s own rail, even with items picked', () => {
  const rail = ['a', 'b']
  assert.deepEqual(panelState({ route: 'handoff', view: { rail }, picked: 3 }), { state: 'template', rail })
  assert.equal(panelState({ route: 'handoff', view: null, picked: 0 }).state, 'none')
  assert.equal(panelState({ route: 'run', view: { rail }, picked: 0 }).state, 'none')
})
