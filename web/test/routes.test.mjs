// web/test/routes.test.mjs
// The route table app.js dispatches through (routes.js), run as app.js runs
// it: routeOf picks the route and argsOf parses the hash. A '+' typed into
// the URL by hand stays a '+' in every route's query.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { ROUTES, argsOf, routeOf } from '../src/routes.js'
import { parseSamplesWhere } from '../src/samples.js'

const at = (hash) => {
  const r = routeOf(hash)
  return r ? [r.name, argsOf(r, hash)] : null
}

test('every route has a name, a pattern, args and a view, and the names are distinct', () => {
  for (const r of ROUTES) {
    assert.equal(typeof r.name, 'string')
    assert.ok(r.pattern instanceof RegExp, r.name)
    assert.equal(typeof r.args, 'function', r.name)
    assert.equal(typeof r.view, 'function', r.name)
  }
  assert.equal(new Set(ROUTES.map(r => r.name)).size, ROUTES.length)
  assert.equal(routeOf('#/nowhere'), null)
})

test("the run route: a hand-typed '+' in where stays '+', and the tab, show and release come through", () => {
  const [name, args] = at('#/run/bafyrun?tab=samples&where=[["meta.id","string","WT+REP1"]]&show=excluded&release=1')
  assert.equal(name, 'run')
  assert.deepEqual(args, { cid: 'bafyrun', tab: 'samples', where: '[["meta.id","string","WT+REP1"]]', show: 'excluded', release: true })
  assert.deepEqual(parseSamplesWhere(args.where), [['meta.id', 'string', 'WT+REP1']])
  assert.deepEqual(at('#/run/bafyrun'), ['run', { cid: 'bafyrun', tab: null, where: null, show: null, release: false }])
})

test("the run route: an encoded where decodes once, and %2B is '+' too", () => {
  const where = JSON.stringify([['meta.id', 'string', 'a+b c']])
  assert.equal(at(`#/run/bafyrun?where=${encodeURIComponent(where)}`)[1].where, where)
  assert.equal(at('#/run/bafyrun?where=a%2Bb')[1].where, 'a+b')
})

test("the content route: a hand-typed '+' in highlight stays '+'", () => {
  assert.deepEqual(at('#/content/bafyfile?highlight=["WT+REP1","x"]'), ['content', { cid: 'bafyfile', highlight: ['WT+REP1', 'x'] }])
  assert.deepEqual(at('#/content/bafyfile'), ['content', { cid: 'bafyfile', highlight: [] }])
})

test("the search route: a hand-typed '+' in q stays '+'", () => {
  assert.deepEqual(at('#/search?q=WT+REP1&tab=samples'), ['search', { q: 'WT+REP1', tab: 'samples' }])
  assert.deepEqual(at('#/search'), ['search', { q: '', tab: null }])
})

test("the handoff route: its template decodes, and a '+' in from, where and samples stays '+'", () => {
  const [name, args] = at('#/handoff/nf-core%2Fdifferentialabundance?from=bafy+run&where=[["meta.id","string","a+b"]]&samples=WT+1,KO%2C1&outputs=quant+salmon')
  assert.equal(name, 'handoff')
  assert.deepEqual(args, { template: 'nf-core/differentialabundance',
    query: { from: 'bafy+run', where: '[["meta.id","string","a+b"]]', project: null, samples: ['WT+1', 'KO,1'], outputs: ['quant+salmon'] } })
})

test('the copy route comes before the run route, and a pipeline name decodes', () => {
  assert.equal(routeOf('#/run/bafyrun/copy?to=b').name, 'copy')
  assert.deepEqual(at('#/pipeline/nf-core%2Frnaseq?offset=20&mine=1'), ['pipeline', { pipeline: 'nf-core/rnaseq', offset: 20, mine: true }])
})
