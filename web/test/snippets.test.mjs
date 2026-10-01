import { test } from 'node:test'
import assert from 'node:assert/strict'
import { INCLUDE_LINE, SNIPPET_KEY, groovyKey, groovyValue, setSnippetMode, snippetLines, snippetMode, whereLiteral } from '../src/snippets.js'

const SELECTION = 'bafyreib7lhx4cekrw4appaxcfb5r4yxk5amw2p6yrnnv2evci3niqplcbu'
const LID = 'lid://4f1b2c3d4e5f60718293a4b5c6d7e8f9'

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

test('the run snippet names the run by its lid:// and the output by name (decision 13)', () => {
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: 'aligned' }, 'untyped').call,
    `channel.fromStore(run: '${LID}', output: 'aligned')`)
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: 'aligned' }, 'typed').call,
    `nextflow.Channel.fromStore(run: '${LID}', output: 'aligned', records: true)`)
})

test('a quote or a backslash in an output name stays inside its Groovy string', () => {
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: "it's\\here" }, 'untyped').call,
    `channel.fromStore(run: '${LID}', output: 'it\\'s\\\\here')`)
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
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: 'markdup', where: [['single_end', 'bool', 'false'], ['id', 'string', 'WT_REP1']] }, 'untyped').call,
    `channel.fromStore(run: '${LID}', output: 'markdup', where: [single_end: false, id: 'WT_REP1'])`)
  assert.equal(snippetLines({ kind: 'run', lid: LID, output: 'markdup', where: [] }, 'untyped').call,
    `channel.fromStore(run: '${LID}', output: 'markdup')`)
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
  assert.equal(whereLiteral([['meta.id', 'string', 'A'], ['depth', 'float', '1.5'], ['x', 'null', null]]), "['meta.id': 'A', depth: 1.5d, x: null]")
})

test('every call is one line, whatever the filter holds (the Gate substitutes it into one line)', () => {
  const where = [['a\nb', 'string', 'line\nbreak'], ['c', 'string', 'tab\there']]
  for (const mode of ['untyped', 'typed']) {
    const { call } = snippetLines({ kind: 'run', lid: LID, output: 'o', where }, mode)
    assert.ok(!call.includes('\n'), call)
  }
  assert.equal(groovyValue('string', 'line\nbreak'), "'line\\nbreak'")
})
