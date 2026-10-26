// web/test/handoff.test.mjs
// The run page's hand-off rail (P1a Task 3, mockups/07-run-samples.html):
// the Samples tab over a fixture member reports its selection through
// ctx.use, and the panel draws the rail from it: outputs with counts and
// bytes, Pick, Save as Selection and the proposed snippet.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { buildMember, entryName, rangedFetch, rawCid } from './fixture.mjs'
import { snapshotDb } from './helpers.mjs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { Tray } from '../src/tray.js'
import { resetFolds } from '../src/folds.js'
import { bytes } from '../src/format.js'
import { createPanel } from '../src/panel.js'
import { parseSamplesWhere } from '../src/samples.js'
import { samplesTab } from '../src/views/samples.js'
import { mergedText } from '../src/handoff.js'
import { parseHandoffQuery } from '../src/listquery.js'

const READS = { RAP1_IAA_30M_REP1: 49791, RAP1_UNINDUCED_REP1: 49969, RAP1_UNINDUCED_REP2: 99971, WT_REP1: 99566, WT_REP2: 49760 }
const SAMPLES = Object.keys(READS).sort()
const SF = { RAP1_IAA_30M_REP1: 1100, RAP1_UNINDUCED_REP1: 1200, RAP1_UNINDUCED_REP2: 1300, WT_REP1: 1400, WT_REP2: 1500 }
const THRESHOLD = JSON.stringify([['preprocessed:num_trimmed_reads', 'int', '50000', '>=']])
const MATRIX = `gene_id\tgene_name\t${SAMPLES.join('\t')}\nENSG1\tg1\t1\t2\t3\t4\t5\n`
const TPM = 4000

const leaf = (name, address, size) => ({ kind: 'Leaf', name, address, size, provider: 'head-node', reason: null })
const manifest = (runName, pipeline) => ({ kind: 'RunManifest', schema: 1, asserted_by: 'test', pipeline, repository: null, revision: null,
  commit_id: null, run_name: runName, nf_run_hash: `hash-${runName}`, session_id: 's', resumed: false,
  nextflow_version: '26.04.6', params: {}, config: {}, script: null, started_at: '2026-09-01T00:00:00.000Z' })
const completion = (run, collections) => ({ kind: 'RunCompletion', schema: 1, asserted_by: 'test', run, collections, input_set: null,
  status: 'succeeded', exit_status: 0, possibly_incomplete: false, started_at: '2026-09-01T00:00:00.000Z',
  finished_at: '2026-09-04T10:00:00.000Z', anomalies: { unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }, error: null })

/**
 * distraught_murdock in small: per-sample preprocessed and quant_salmon,
 * run-level quant_merged (the stored count matrix and a tpm file) and
 * multiqc. `{ ex, ids: { completion, collections, items, matrix } }`.
 */
async function member({ pipeline = 'nf-core/rnaseq', matrix = MATRIX } = {}) {
  const now = Date.now()
  let ids
  const built = await buildMember({
    now,
    extra: ({ put, putRaw, at }) => {
      const m = put(manifest('R9', pipeline))
      const matrixCid = matrix === null ? rawCid('no-matrix') : putRaw(matrix)
      const outputs = {
        preprocessed: SAMPLES.map(id => [{ meta: { id }, num_trimmed_reads: READS[id] }, leaf(`${id}.fastq.gz`, rawCid(`fq-${id}`), 100)]),
        quant_salmon: SAMPLES.map(id => [{ meta: { id } }, leaf(`${id}.sf`, rawCid(`sf-${id}`), SF[id])]),
        quant_merged: [[{ id: 'all_samples' }, { counts: leaf('salmon.merged.gene_counts.tsv', matrixCid, (matrix ?? '').length),
          tpm: leaf('salmon.merged.gene_tpm.tsv', rawCid('tpm'), TPM) }]],
        multiqc: [[{ id: 'multiqc_report' }, leaf('multiqc_report.html', rawCid('mqc'), 25_000)]],
      }
      const collections = {}
      const items = {}
      const colls = Object.entries(outputs).map(([name, values]) => {
        const cids = values.map(value => put({ kind: 'OutputItem', schema: 1, value }))
        items[name] = cids.map(String)
        const sorted = [...cids].sort((a, b) => (a.toString() < b.toString() ? -1 : 1))
        const coll = put({ kind: 'OutputCollection', schema: 1, asserted_by: 'test', run: m, name, items: sorted,
          paths: sorted.map((_, i) => [`${name}/${i}`]) })
        collections[name] = coll.toString()
        return coll
      })
      const comp = put(completion(m, colls))
      ids = { completion: comp.toString(), collections, items, matrix: matrixCid.toString() }
      return { log: [entryName(at.R3 + 2000, 'run', comp.toString())], rows: null }
    },
  })
  const ex = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(built.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: rangedFetch(built.blocks) }),
    listFn: async () => ({ names: built.log, readable: true }),
    now: () => now,
  })
  return { ex, ids }
}

async function until(fn, what = 'the condition') {
  for (let i = 0; i < 100; i++) {
    if (fn()) return
    await frame()
  }
  assert.fail(`timed out waiting for ${what}`)
}

/** A writer that records its calls; the dry run says the Selection is new. */
function recordingWriter() {
  const calls = []
  return {
    calls,
    selection: async (members, opts = {}) => {
      calls.push(['selection', members.map(m => [String(m.item.address), m.item.via.map(String)]), !!opts.dryRun])
      return opts.dryRun ? { exists: false } : { address: 'bafysaved' }
    },
    rename: async (address, name, supersedes) => { calls.push(['rename', address, name, supersedes]) },
  }
}

/**
 * The Samples tab for `where` and a panel fed by its ctx.use, the way app.js
 * wires them. Resolves once the rail's sizes and notes have filled in.
 */
async function rail({ where = THRESHOLD, pipeline, matrix, write = {} } = {}) {
  installDom()
  resetFolds()
  globalThis.history = { replaceState: () => {} }
  globalThis.document.body = { dataset: {} }
  globalThis.document.querySelector = () => null
  const { ex, ids } = await member({ pipeline, matrix })
  const writer = recordingWriter()
  const views = []
  let panel = null
  const ctx = {
    tray: new Tray(null), sources: new Map(), rerender: () => {},
    trayChanged: () => panel.draw(),
    write: { available: true, served: true, writer, hrefFor: (h) => h, run: async (status, fn) => fn(), ...write },
    use: (v) => { views.push(v); panel?.draw({ route: 'run', view: v }) },
  }
  const { collections } = await ex.run(ids.completion)
  samplesTab(ex, ids.completion, ctx, { pipeline: pipeline ?? 'nf-core/rnaseq', collections, where: parseSamplesWhere(where) })
  await until(() => views.length, 'the Samples tab to report')
  const el = document.createElement('div')
  panel = createPanel({ el, ex, ctx })
  await panel.draw({ route: 'run', view: views.at(-1) })
  await settled(el)
  return { ex, ids, ctx, el, panel, writer, views }
}

/** Waits until no size or note in the rail is still being read. */
const settled = (el) => until(() => el.querySelector('[data-total]') && !/…/.test(el.textContent), 'the rail to fill in')

const row = (el, output) => el.querySelectorAll('label.opt').find(l => l.querySelector(`[data-out=${output}]`))
const box = (el, output) => el.querySelector(`[data-out=${output}]`)

test('the rail for two filtered samples: heading, per-sample and run-level outputs, counts and unique bytes', async () => {
  const { el } = await rail()
  assert.equal(el.querySelector('[data-panel-state]').dataset.panelState, 'samples')
  const headings = el.querySelectorAll('h3').map(h => h.textContent)
  assert.deepEqual(headings, ['Hand off 2 samples', 'Save as Selection', 'Use in a workflow', 'Hand off to a pipeline'])
  assert.deepEqual(el.querySelectorAll('.eyebrow').map(e => e.textContent), ['Per sample', 'Run-level'])
  // Config order for the outputs it describes (quant_salmon before preprocessed), each with its label.
  assert.deepEqual(el.querySelectorAll('[data-out]').map(b => b.dataset.out), ['quant_salmon', 'preprocessed', 'quant_merged', 'multiqc'])
  assert.match(row(el, 'quant_salmon').textContent, /quant_salmon\s*Salmon quant dir/)
  assert.equal(row(el, 'quant_salmon').querySelector('.sz').textContent, `2 · ${bytes(SF.RAP1_UNINDUCED_REP2 + SF.WT_REP1)}`)
  assert.equal(row(el, 'preprocessed').querySelector('.sz').textContent, `2 · ${bytes(200)}`)
  assert.equal(row(el, 'quant_merged').querySelector('.sz').textContent, `1 · ${bytes(MATRIX.length + TPM)}`)
  // The config's handoff list is ticked to start with.
  assert.deepEqual(el.querySelectorAll('[data-out]').filter(b => b.checked).map(b => b.dataset.out), ['quant_salmon', 'quant_merged'])
  assert.equal(el.querySelector('[data-total]').textContent, `3 items · ${bytes(SF.RAP1_UNINDUCED_REP2 + SF.WT_REP1 + MATRIX.length + TPM)}`)
  assert.match(el.textContent, /Total\s*3 items/)
})

test('the merged note counts the matrix header columns and links to the preview with the selected samples highlighted', async () => {
  const { el, ids } = await rail()
  const note = row(el, 'quant_merged').querySelector('.note2')
  assert.equal(note.textContent, 'Holds 5 samples as columns; your 2 are among them. Preview')
  assert.equal(note.querySelector('a').getAttribute('href'), `#/content/${ids.matrix}?highlight=["RAP1_UNINDUCED_REP2","WT_REP1"]`)
  assert.equal(row(el, 'multiqc').querySelector('.note2').textContent, 'One item for the whole run.')
})

test('the merged note falls back when the pipeline names no matrix, or the matrix cannot be read', async () => {
  const unknown = await rail({ pipeline: 'demo' })
  assert.equal(row(unknown.el, 'quant_merged').querySelector('.note2').textContent, 'One item for the whole run.')
  const missing = await rail({ matrix: null })
  assert.equal(row(missing.el, 'quant_merged').querySelector('.note2').textContent, 'One item for the whole run.')
})

test('mergedText: plain counts for one, some or none of yours', () => {
  assert.equal(mergedText(5, 2, 2), 'Holds 5 samples as columns; your 2 are among them.')
  assert.equal(mergedText(5, 1, 1), 'Holds 5 samples as columns; your 1 is among them.')
  assert.equal(mergedText(4, 1, 2), 'Holds 4 samples as columns; 1 of your 2 is among them.')
  assert.equal(mergedText(4, 0, 2), 'Holds 4 samples as columns; none of your 2 is among them.')
  assert.equal(mergedText(1, 0, 0), 'Holds 1 sample as a column.')
})

test('ticking an output redraws the total and the snippet', async () => {
  const { el } = await rail()
  const pre = () => el.querySelector('[data-proposed-snippet]').textContent
  assert.match(pre(), /^quant_salmon = samples\.output\('quant_salmon'\)$/m)
  assert.doesNotMatch(pre(), /preprocessed =/)
  const tick = box(el, 'preprocessed')
  tick.checked = true
  await tick.fire('change')
  await settled(el)
  assert.equal(el.querySelector('[data-total]').textContent, `5 items · ${bytes(SF.RAP1_UNINDUCED_REP2 + SF.WT_REP1 + MATRIX.length + TPM + 200)}`)
  assert.match(pre(), /^preprocessed = samples\.output\('preprocessed'\)$/m)
  assert.ok(box(el, 'preprocessed').checked, 'the tick holds across the redraw')
})

test('the proposed snippet: where: from the filter, never in a [data-snippet] the Gate would run, and Copy copies it', async () => {
  const { el, ids } = await rail()
  const text = el.querySelector('[data-proposed-snippet]').textContent
  assert.match(text, /^\/\/ Proposed API, not in the plugin yet: docs\/design\/adr\/0001-fromstore-samples\.md$/m)
  assert.match(text, new RegExp(`run: 'cas://${ids.completion}',\\n {2}samples: \\[where: \\['preprocessed:num_trimmed_reads >=': 50000\\]\\]\\)`))
  assert.match(text, new RegExp(`^quant_merged = channel\\.fromStore\\(run: 'cas://${ids.completion}', output: 'quant_merged'\\)$`, 'm'))
  assert.equal(el.querySelector('[data-snippet]'), null)
  let copied = null
  const saved = globalThis.navigator
  Object.defineProperty(globalThis, 'navigator', { value: { clipboard: { writeText: async (t) => { copied = t } } }, configurable: true })
  try {
    const copy = el.querySelectorAll('button').find(b => b.textContent === 'Copy snippet')
    await copy.click()
    assert.equal(copied, text)
    assert.equal(copy.textContent, 'Copied')
  } finally {
    Object.defineProperty(globalThis, 'navigator', { value: saved, configurable: true })
  }
})

test('a tick by hand names the samples by id in the snippet', async () => {
  const w = await rail()
  const last = w.views.at(-1)
  const manual = new Map([['WT_REP2', true]])
  const selected = ['RAP1_UNINDUCED_REP2', 'WT_REP1', 'WT_REP2']
  await w.panel.draw({ route: 'run', view: { ...last, samples: { ...last.samples, selected, manual } } })
  await settled(w.el)
  assert.match(w.el.querySelector('[data-proposed-snippet]').textContent, /samples: \[ids: \['RAP1_UNINDUCED_REP2', 'WT_REP1', 'WT_REP2'\]\]/)
  assert.equal(w.el.querySelector('h3').textContent, 'Hand off 3 samples')
})

test('P4: the drawer links to the differentialabundance hand-off with this run, the filter and the ticked outputs', async () => {
  const { el, ids } = await rail()
  const sec = el.querySelector('[data-handoff-template]')
  assert.equal(sec.dataset.handoffTemplate, 'nf-core/differentialabundance')
  const a = sec.querySelector('a')
  assert.equal(a.textContent, 'Hand off to nf-core/differentialabundance')
  const href = a.getAttribute('href')
  assert.match(href, /^#\/handoff\/nf-core%2Fdifferentialabundance\?/)
  assert.deepEqual(parseHandoffQuery(href.slice(href.indexOf('?') + 1)),
    { from: ids.completion, where: THRESHOLD, project: null, samples: [], outputs: ['quant_salmon', 'quant_merged'] })
  // Ticking another output carries it too.
  const tick = box(el, 'preprocessed')
  tick.checked = true
  await tick.fire('change')
  await settled(el)
  const next = el.querySelector('[data-handoff-template]').querySelector('a').getAttribute('href')
  assert.deepEqual(parseHandoffQuery(next.slice(next.indexOf('?') + 1)).outputs, ['quant_salmon', 'preprocessed', 'quant_merged'])
})

test('P4: ticks by hand go to the hand-off by name; no link for another pipeline or a run without quant_merged', async () => {
  const w = await rail()
  const last = w.views.at(-1)
  const manual = new Map([['WT_REP2', true]])
  const selected = ['RAP1_UNINDUCED_REP2', 'WT_REP1', 'WT_REP2']
  await w.panel.draw({ route: 'run', view: { ...last, samples: { ...last.samples, selected, manual } } })
  await settled(w.el)
  const href = w.el.querySelector('[data-handoff-template]').querySelector('a').getAttribute('href')
  assert.deepEqual(parseHandoffQuery(href.slice(href.indexOf('?') + 1)).samples, selected)
  const other = await rail({ pipeline: 'demo' })
  assert.equal(other.el.querySelector('[data-handoff-template]'), null)
  const noMerged = await rail()
  const view = noMerged.views.at(-1)
  const runLevelItems = new Map([...view.samples.table.runLevelItems].filter(([o]) => o !== 'quant_merged'))
  await noMerged.panel.draw({ route: 'run', view: { ...view, samples: { ...view.samples, table: { ...view.samples.table, runLevelItems, runLevel: ['multiqc'] } } } })
  await settled(noMerged.el)
  assert.equal(noMerged.el.querySelector('[data-handoff-template]'), null)
})

test('"Pick 3 items" puts exactly those items in the picked list, each with its collection', async () => {
  const { el, ctx, ids } = await rail()
  const pick = el.querySelector('[data-handoff-pick]')
  assert.equal(pick.textContent, 'Pick 3 items')
  await pick.click()
  const sf = (id) => ids.items.quant_salmon[SAMPLES.indexOf(id)]
  const want = [
    { address: sf('RAP1_UNINDUCED_REP2'), kind: 'item', via: [ids.collections.quant_salmon] },
    { address: sf('WT_REP1'), kind: 'item', via: [ids.collections.quant_salmon] },
    { address: ids.items.quant_merged[0], kind: 'item', via: [ids.collections.quant_merged] },
  ].sort((a, b) => (a.address < b.address ? -1 : 1))
  assert.deepEqual(ctx.tray.entries(), want)
  await frame()
  // The rail stays the hand-off and links the picked list.
  assert.equal(el.querySelector('[data-panel-state]').dataset.panelState, 'samples')
  const line = el.querySelector('[data-handoff-picked]')
  assert.equal(line.textContent, '3 picked · Open the picked list')
  assert.equal(line.querySelector('a').getAttribute('href'), '#/compose')
})

test('with items already picked, the Samples tab still shows the hand-off, with a line to the picked list', async () => {
  const w = await rail()
  assert.equal(w.el.querySelector('[data-handoff-picked]'), null, 'no line while nothing is picked')
  w.ctx.tray.addMany(Array.from({ length: 1200 }, (_, i) => ({ address: `bafyitem${i}`, via: [], kind: 'item' })))
  await w.panel.draw()
  await settled(w.el)
  assert.equal(w.el.querySelector('[data-panel-state]').dataset.panelState, 'samples')
  assert.ok(w.el.querySelector('[data-handoff-pick]'))
  assert.equal(w.el.querySelector('[data-handoff-picked]').textContent, '1,200 picked · Open the picked list')
})

// In the app, runWrite opens #/selection/<cid> once the save is written, as the
// picked and compose saves do; the rail's saved-call branch below is what the
// reader sees on Back, or when the page could not refresh and stays put.
test('Save writes the same three items as a Selection, names it, and the rail then offers the saved Selection\'s call', async () => {
  const { el, ids, writer, ctx } = await rail()
  const name = el.querySelector('[data-handoff-name]')
  name.value = 'rap1-wt-50k'
  await el.querySelector('[data-handoff-save]').click()
  await frame()
  const sf = (id) => ids.items.quant_salmon[SAMPLES.indexOf(id)]
  const members = [[sf('RAP1_UNINDUCED_REP2'), [ids.collections.quant_salmon]], [sf('WT_REP1'), [ids.collections.quant_salmon]],
    [ids.items.quant_merged[0], [ids.collections.quant_merged]]].sort((a, b) => (a[0] < b[0] ? -1 : 1))
  assert.deepEqual(writer.calls, [['selection', members, true], ['selection', members, false], ['rename', 'bafysaved', 'rap1-wt-50k', []]])
  assert.equal(ctx.tray.size, 0, 'the picked list is left alone')
  assert.equal(el.querySelector('[data-snippet="untyped"]').textContent, "channel.fromStore(selection: 'bafysaved')")
  assert.equal(el.querySelector('[data-proposed-snippet]'), null)
})

test('Save is offered only where the page can write, and says why not', async () => {
  const { el } = await rail({ write: { available: false, reason: 'read only', writer: null } })
  assert.ok(el.querySelector('[data-handoff-save]').disabled)
  assert.equal(el.querySelector('[data-unavailable]').textContent, 'read only')
})

test('a save started on the Samples tab keeps its outcome when a filter edit rewrites the URL meanwhile', async () => {
  const saved = globalThis.location
  try {
    const w = await rail()
    globalThis.location = { hash: `#/run/${w.ids.completion}?tab=samples&where=${encodeURIComponent(THRESHOLD)}` }
    w.writer.selection = async () => ({ exists: true, here: true, address: 'bafyold', names: ['earlier'] })
    w.ctx.write.run = async (status, fn) => {
      status.textContent = 'Saving...'
      // The Samples tab rewrites the hash with replaceState as a value is typed, and reports again.
      globalThis.location.hash = `#/run/${w.ids.completion}?tab=samples&where=${encodeURIComponent('[["preprocessed:num_trimmed_reads","int","5000",">="]]')}`
      await w.panel.draw({ route: 'run', view: w.views.at(-1) })
      return fn()
    }
    await w.el.querySelector('[data-handoff-save]').click()
    await frame()
    assert.ok(w.el.querySelector('[data-exists]'), 'the outcome is shown')
    assert.match(w.el.textContent, /already exists as earlier/)
    await w.panel.draw({ route: 'run', view: w.views.at(-1) })
    assert.ok(w.el.querySelector('[data-exists]'), 'and still there on the next report from the tab')
  } finally { globalThis.location = saved }
})

/** The last report with `selected` in place of the tab's own selection. */
const reportWith = (w, selected) => {
  const last = w.views.at(-1)
  return { ...last, samples: { ...last.samples, selected, manual: new Map(selected.map(s => [s, true])) } }
}

test('a save outcome for other items goes when the tab reported new samples during the save', async () => {
  const w = await rail()
  // Held in another member: the prompt's "Save a copy here" would write the members the save began with.
  w.writer.selection = async () => ({ exists: true, here: false, address: 'bafyother', names: ['earlier'], name_claims: ['c1'] })
  w.ctx.write.run = async (status, fn) => {
    const done = await fn()
    // The reader ticks another sample while the dry run is out; the panel holds the draw until the save settles.
    await w.panel.draw({ route: 'run', view: reportWith(w, ['WT_REP2']) })
    return done
  }
  await w.el.querySelector('[data-handoff-save]').click()
  await settled(w.el)
  assert.equal(w.el.querySelector('h3').textContent, 'Hand off 1 sample')
  assert.equal(w.el.querySelector('[data-held-elsewhere]'), null)
  assert.equal(w.el.querySelector('#compose-copy'), null)
  assert.equal(w.el.querySelector('#write-status').childNodes.length, 0)
})

test('Pick and Save wait for the total\'s bytes; a size that cannot be read still lets them go, marked unknown', async () => {
  const w = await rail()
  const read = w.ex.itemFileMap.bind(w.ex)
  let release
  const gate = new Promise(r => { release = r })
  w.ex.itemFileMap = async (cid) => { await gate; return read(cid) }
  await w.panel.draw({ route: 'run', view: w.views.at(-1) })
  await frame()
  assert.match(w.el.querySelector('[data-total]').textContent, /…/)
  assert.ok(w.el.querySelector('[data-handoff-pick]').disabled, 'Pick waits for the bytes')
  assert.ok(w.el.querySelector('[data-handoff-save]').disabled, 'Save waits for the bytes')
  release()
  await settled(w.el)
  assert.ok(!w.el.querySelector('[data-handoff-pick]').disabled)
  assert.ok(!w.el.querySelector('[data-handoff-save]').disabled)

  w.ex.itemFileMap = async () => { throw new Error('gone') }
  await w.panel.draw({ route: 'run', view: w.views.at(-1) })
  await settled(w.el)
  assert.equal(w.el.querySelector('[data-total]').textContent, '3 items · size unknown')
  assert.ok(!w.el.querySelector('[data-handoff-pick]').disabled)
  assert.ok(!w.el.querySelector('[data-handoff-save]').disabled)
})

test('a size walk stopped because the page went writes nothing and leaves Pick and Save off', async () => {
  const w = await rail()
  w.ex.itemFileMap = async () => { throw Object.assign(new Error('the page that asked for this size is gone'), { code: 'aborted' }) }
  await w.panel.draw({ route: 'run', view: w.views.at(-1) })
  await frame()
  await frame()
  assert.equal(w.el.querySelector('[data-total]').textContent, '3 items · …')
  assert.equal(row(w.el, 'quant_salmon').querySelector('.sz').textContent, '2 · …')
  assert.doesNotMatch(w.el.textContent, /size unknown|—/)
  assert.ok(w.el.querySelector('[data-handoff-pick]').disabled)
  assert.ok(w.el.querySelector('[data-handoff-save]').disabled)
})

test('with no sample selected, Pick and Save are off even with a run-level output ticked', async () => {
  const w = await rail()
  await w.panel.draw({ route: 'run', view: reportWith(w, []) })
  await settled(w.el)
  assert.equal(w.el.querySelector('h3').textContent, 'Hand off 0 samples')
  assert.ok(box(w.el, 'quant_merged').checked)
  assert.equal(w.el.querySelector('[data-total]').textContent.split(' · ')[0], '1 item')
  assert.ok(w.el.querySelector('[data-handoff-pick]').disabled)
  assert.ok(w.el.querySelector('[data-handoff-save]').disabled)
})
