import { test } from 'node:test'
import assert from 'node:assert/strict'
import { INCLUDE_LINE, SNIPPET_KEY, setSnippetMode, snippetLines, snippetMode } from '../src/snippets.js'

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
