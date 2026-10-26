// web/test/routequery.test.mjs
// The run, content and search routes read their query with queryOf, which
// decodes each part with decodeURIComponent, so a '+' a reader types in the
// URL stays a '+' (URLSearchParams would read it as a space).
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { queryOf } from '../src/routequery.js'
import { parseSamplesWhere, samplesWhereText } from '../src/samples.js'

test('an output-qualified where list survives encodeURIComponent, queryOf and parseSamplesWhere', () => {
  const where = [['preprocessed:num_trimmed_reads', 'int', '50000', '>='], ['meta.id', 'string', 'a+b&c=d e'],
    ['aligned:percent_mapped', 'float', '89.5', '!=']]
  const q = queryOf(`tab=samples&where=${encodeURIComponent(samplesWhereText(where))}&show=excluded`)
  assert.deepEqual(parseSamplesWhere(q.get('where')), where)
  assert.equal(q.get('tab'), 'samples')
  assert.equal(q.get('show'), 'excluded')
})

test("a raw '+' stays '+'", () => {
  assert.equal(queryOf('q=WT+REP1').get('q'), 'WT+REP1')
  assert.equal(queryOf('where=[["meta.id","string","a+b"]]').get('where'), '[["meta.id","string","a+b"]]')
})

test('a malformed escape does not throw; the raw text is kept', () => {
  assert.doesNotThrow(() => queryOf('q=%E0%A4%A&tab=items'))
  const q = queryOf('q=%E0%A4%A&tab=items')
  assert.equal(q.get('q'), '%E0%A4%A')
  assert.equal(q.get('tab'), 'items')
})

test('an empty query gives an empty map', () => {
  for (const text of ['', null, undefined]) assert.equal(queryOf(text).size, 0)
})

test('a key without = is an empty value; the first of a repeated key wins, as URLSearchParams.get does', () => {
  const q = queryOf('release&tab=storage&tab=outputs&&x=1=2')
  assert.equal(q.get('release'), '')
  assert.equal(q.get('tab'), 'storage')
  assert.equal(q.get('x'), '1=2', 'only the first = splits')
  assert.equal(q.has(''), false)
})
