import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { INCLUDE_LINE, SNIPPET_KEY, groovyKey, groovyValue, samplesSnippet, setSnippetMode, snippetLines, snippetMode, snippetBlock, snippetToggle, liveCount, whereLiteral, whereRepeatsPath } from '../src/snippets.js'

const SELECTION = 'bafyreib7lhx4cekrw4appaxcfb5r4yxk5amw2p6yrnnv2evci3niqplcbu'
const RUN = 'cas://bafyreibzvyocjhlwf7uo6tltbuvyqmtybeq7p54ndnnh52rspsebr7aegy'

function memoryStorage() {
  const m = new Map()
  return { getItem: k => m.get(k) ?? null, setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k) }
}

test('the Selection snippet, untyped and typed (decision 13)', () => {
  assert.equal(INCLUDE_LINE, "include { fromStore } from 'plugin/nf-blocks'")
  assert.deepEqual(snippetLines({ kind: 'selection', cid: SELECTION }, 'untyped'),
    { include: INCLUDE_LINE, call: `channel.fromStore(selection: '${SELECTION}')` })
  assert.deepEqual(snippetLines({ kind: 'selection', cid: SELECTION }, 'typed'),
    { include: INCLUDE_LINE, call: `nextflow.Channel.fromStore(selection: '${SELECTION}', records: true)` })
})

test('the run snippet names the run by its RunCompletion address and the output by name (decision 13, amended)', () => {
  assert.equal(snippetLines({ kind: 'run', run: RUN, output: 'aligned' }, 'untyped').call,
    `channel.fromStore(run: '${RUN}', output: 'aligned')`)
  assert.equal(snippetLines({ kind: 'run', run: RUN, output: 'aligned' }, 'typed').call,
    `nextflow.Channel.fromStore(run: '${RUN}', output: 'aligned', records: true)`)
})

test('a quote or a backslash in an output name stays inside its Groovy string', () => {
  assert.equal(snippetLines({ kind: 'run', run: RUN, output: "it's\\here" }, 'untyped').call,
    `channel.fromStore(run: '${RUN}', output: 'it\\'s\\\\here')`)
})

test('the mode is untyped unless the viewer chose typed', () => {
  assert.equal(SNIPPET_KEY, 'nf-blocks.snippets')
  assert.equal(snippetMode(memoryStorage()), 'untyped')
  assert.equal(snippetMode(null), 'untyped')
  const odd = memoryStorage()
  odd.setItem(SNIPPET_KEY, 'strict')
  assert.equal(snippetMode(odd), 'untyped')
})

test('choosing a mode remembers it under nf-blocks.snippets', () => {
  const storage = memoryStorage()
  setSnippetMode('typed', storage)
  assert.equal(storage.getItem(SNIPPET_KEY), 'typed')
  assert.equal(snippetMode(storage), 'typed')
  setSnippetMode('untyped', storage)
  assert.equal(snippetMode(storage), 'untyped')
})

test('storage that refuses every call costs only the memory, and another mode is refused', () => {
  const broken = { getItem: () => { throw new Error('denied') }, setItem: () => { throw new Error('denied') } }
  assert.equal(snippetMode(broken), 'untyped')
  assert.doesNotThrow(() => setSnippetMode('typed', broken))
  assert.throws(() => setSnippetMode('strict', memoryStorage()), /untyped or typed/)
})

test('where: one condition per chip, in chip order, as a Groovy map literal', () => {
  assert.equal(snippetLines({ kind: 'run', run: RUN, output: 'markdup', where: [['single_end', 'bool', 'false'], ['id', 'string', 'WT_REP1']] }, 'untyped').call,
    `channel.fromStore(run: '${RUN}', output: 'markdup', where: [single_end: false, id: 'WT_REP1'])`)
  assert.equal(snippetLines({ kind: 'run', run: RUN, output: 'markdup', where: [] }, 'untyped').call,
    `channel.fromStore(run: '${RUN}', output: 'markdup')`)
})

test('latest good run names the pipeline instead of the run', () => {
  assert.equal(snippetLines({ kind: 'latest', pipeline: 'nf-core/rnaseq', output: 'markdup', where: [['single_end', 'bool', 'false']] }, 'typed').call,
    "nextflow.Channel.fromStore(run: 'latest', pipeline: 'nf-core/rnaseq', output: 'markdup', where: [single_end: false], records: true)")
})

test('keys: identifiers bare; dotted, odd and Groovy keywords quoted (Review Focus 3)', () => {
  assert.equal(groovyKey('sample'), 'sample')
  assert.equal(groovyKey('_x1'), '_x1')
  assert.equal(groovyKey('meta.id'), "'meta.id'")
  assert.equal(groovyKey('read-group'), "'read-group'")
  assert.equal(groovyKey('1st'), "'1st'")
  assert.equal(groovyKey('in'), "'in'")
  assert.equal(groovyKey('class'), "'class'")
  assert.equal(groovyKey("it's"), "'it\\'s'")
})

test('values by type: a numeric-looking string stays a string, a float stays a Double (P8)', () => {
  assert.equal(groovyValue('string', '10'), "'10'")
  assert.equal(groovyValue('string', "o'brien"), "'o\\'brien'")
  assert.equal(groovyValue('int', '42'), '42')
  assert.equal(groovyValue('int', '-7'), '-7')
  assert.equal(groovyValue('int', '123456789012345678901234567890'), '123456789012345678901234567890')
  assert.equal(groovyValue('float', '10.371008628978885'), '10.371008628978885d')
  assert.equal(groovyValue('float', '1.0E-5'), '1.0E-5d')
  assert.equal(groovyValue('float', '-0.0'), '-0.0d')
  assert.equal(groovyValue('bool', 'true'), 'true')
  assert.equal(groovyValue('null', null), 'null')
  assert.throws(() => groovyValue('date', 'x'), /unknown type/)
})

test('whereLiteral', () => {
  assert.equal(whereLiteral([]), null)
  assert.equal(whereLiteral([['strandedness', 'string', 'reverse', '!='], ['lane', 'int', '2', '!=']]), "['strandedness !=': 'reverse', 'lane !=': 2]")
  assert.equal(whereLiteral([['meta.id', 'string', 'A'], ['depth', 'float', '1.5'], ['x', 'null', null]]), "['meta.id': 'A', depth: 1.5d, x: null]")
})

test('every call is one line, whatever the filter holds (the Gate substitutes it into one line)', () => {
  const where = [['a\nb', 'string', 'line\nbreak'], ['c', 'string', 'tab\there']]
  for (const mode of ['untyped', 'typed']) {
    const { call } = snippetLines({ kind: 'run', run: RUN, output: 'o', where }, mode)
    assert.ok(!call.includes('\n'), call)
  }
  assert.equal(groovyValue('string', 'line\nbreak'), "'line\\nbreak'")
})

test('whereRepeatsPath: true only when a path appears twice', () => {
  assert.equal(whereRepeatsPath([]), false)
  assert.equal(whereRepeatsPath(undefined), false)
  assert.equal(whereRepeatsPath([['a', 'string', 'x'], ['b', 'string', 'x']]), false)
  assert.equal(whereRepeatsPath([['a', 'string', 'x'], ['a', 'string', 'y']]), true)
  assert.equal(whereRepeatsPath([['a', 'string', 'x'], ['a', 'int', '1']]), true)
})

test('groovyValue writes the normalised number, not the typed text', () => {
  assert.equal(groovyValue('int', '08'), '8')
  assert.equal(groovyValue('int', '-0'), '0')
  assert.equal(groovyValue('float', ' 1.5'), '1.5d')
  assert.equal(groovyValue('float', '0x10'), '16.0d')
})

test('the set of drawn snippets stays bounded across redraws', () => {
  installDom()
  const before = liveCount()
  for (let i = 0; i < 500; i++) { snippetBlock({ kind: 'selection', cid: 'x' }); snippetToggle() }
  assert.ok(liveCount() < before + 200, `grew to ${liveCount()}`)
  // The newest draw is still there.
  assert.ok(snippetBlock({ kind: 'selection', cid: 'x' }).querySelector('[data-snippet]'))
})

test('where: a range is the path and its operator as one quoted key; two bounds on one path are two keys', () => {
  assert.equal(whereLiteral([['meta.depth', 'float', '30.0', '>='], ['meta.depth', 'float', '100.0', '<'], ['lane', 'int', '2', '>']]),
    "['meta.depth >=': 30.0d, 'meta.depth <': 100.0d, 'lane >': 2]")
  assert.equal(whereRepeatsPath([['depth', 'float', '1.0', '>='], ['depth', 'float', '2.0', '<']]), false)
  assert.equal(whereRepeatsPath([['depth', 'float', '1.0', '>='], ['depth', 'float', '2.0', '>=']]), true)
})

test('the snippet is the kit\'s: a pre.code block, a full-width Copy snippet, and an Untyped | Typed seg-toggle', () => {
  installDom()
  setSnippetMode('untyped', memoryStorage())
  const block = snippetBlock({ kind: 'selection', cid: SELECTION })
  const pre = block.querySelector('pre')
  assert.ok(pre.classList.contains('code'))
  assert.equal(block.querySelector('[data-snippet]').textContent, `channel.fromStore(selection: '${SELECTION}')`, 'the call line alone, as the Gate runs it')
  const copy = block.querySelectorAll('button').find(b => b.textContent === 'Copy snippet')
  assert.ok(copy && copy.classList.contains('btn') && copy.classList.contains('snippet-copy'))
  const toggle = snippetToggle()
  assert.ok(toggle.classList.contains('seg-toggle'))
  const [untyped, typed] = toggle.querySelectorAll('button')
  assert.deepEqual([untyped.textContent, typed.textContent], ['Untyped', 'Typed'])
  assert.deepEqual([untyped.dataset.snippetMode, typed.dataset.snippetMode], ['untyped', 'typed'])
  assert.ok(untyped.classList.contains('on'))
  typed.click()
  assert.ok(typed.classList.contains('on'))
  assert.ok(!untyped.classList.contains('on'))
  setSnippetMode('untyped', memoryStorage())
})

// ---- the Samples tab's hand-off (P1a Task 3): a proposed call, see docs/design/adr/0001-fromstore-samples.md ----

const COMPLETION = 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq'

test('samplesSnippet: the threshold as a samples where:, a per-sample output from the handle, a run-level one by run and output', () => {
  assert.equal(samplesSnippet({ completion: COMPLETION, where: [['preprocessed:num_trimmed_reads', 'int', '50000', '>=']], ids: null,
    outputs: ['quant_salmon'], runLevel: ['quant_merged'] }),
  `include { fromStore } from 'plugin/nf-blocks'

// Proposed API, not in the plugin yet: docs/design/adr/0001-fromstore-samples.md
samples = channel.fromStore(
  run: 'cas://${COMPLETION}',
  samples: [where: ['preprocessed:num_trimmed_reads >=': 50000]])

quant_salmon = samples.output('quant_salmon')
quant_merged = channel.fromStore(run: 'cas://${COMPLETION}', output: 'quant_merged')`)
})

test('samplesSnippet: ticks by hand name the samples by id; no filter is every sample; no output ends after the handle', () => {
  const ids = samplesSnippet({ completion: COMPLETION, where: [['preprocessed:num_trimmed_reads', 'int', '50000', '>=']],
    ids: ['RAP1_UNINDUCED_REP2', 'WT_REP1'], outputs: ['quant_salmon'], runLevel: [] })
  assert.match(ids, /\n {2}samples: \[ids: \['RAP1_UNINDUCED_REP2', 'WT_REP1'\]\]\)\n/)
  assert.doesNotMatch(ids, /where:/)
  const all = samplesSnippet({ completion: COMPLETION, where: [], ids: null, outputs: [], runLevel: [] })
  assert.ok(all.endsWith('  samples: [where: [:]])'), all)
})

test('samplesSnippet: an output name that is no Groovy name gets a variable that is one, and keeps its own name in the call', () => {
  const text = samplesSnippet({ completion: COMPLETION, where: [], ids: null, outputs: ['bam-qc', '2pass', 'in'], runLevel: [] })
  assert.match(text, /^bam_qc = samples\.output\('bam-qc'\)$/m)
  assert.match(text, /^_2pass = samples\.output\('2pass'\)$/m)
  assert.match(text, /^in_ = samples\.output\('in'\)$/m)
})

test('samplesSnippet: outputs called samples or channel leave the handle and channel.fromStore alone, and names that clean up alike get _2', () => {
  const text = samplesSnippet({ completion: COMPLETION, where: [], ids: null, outputs: ['samples', 'channel', 'bam-qc', 'bam_qc'], runLevel: ['bam.qc', 'channel'] })
  assert.match(text, /^samples_2 = samples\.output\('samples'\)$/m)
  assert.match(text, /^channel_2 = samples\.output\('channel'\)$/m)
  assert.match(text, /^channel_3 = channel\.fromStore\(run: '[^']+', output: 'channel'\)$/m)
  assert.equal(text.match(/^channel = /gm), null, 'nothing shadows channel')
  assert.match(text, /^bam_qc = samples\.output\('bam-qc'\)$/m)
  assert.match(text, /^bam_qc_2 = samples\.output\('bam_qc'\)$/m)
  assert.match(text, /^bam_qc_3 = channel\.fromStore\(run: '[^']+', output: 'bam\.qc'\)$/m)
  assert.equal(text.match(/^samples = /gm).length, 1, 'the handle is assigned once')
})
