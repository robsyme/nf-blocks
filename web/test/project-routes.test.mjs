// P4 routes: #/projects, #/project/<cid>?tab=, #/handoff/<template>?from=&where=&project=&samples=&outputs=, their query parsing and views.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { PROJECT_TABS, parseHandoffQuery, parseProjectQuery } from '../src/listquery.js'
import { installDom } from './dom.mjs'
import { showsRail } from '../src/panel.js'
import { argsOf, routeOf } from '../src/routes.js'

test('a project page\'s tab is one of its five, else Overview', () => {
  assert.deepEqual(PROJECT_TABS, ['overview', 'samples', 'runs', 'selections', 'activity'])
  assert.deepEqual(parseProjectQuery(undefined), { tab: 'overview' })
  assert.deepEqual(parseProjectQuery('tab=samples'), { tab: 'samples' })
  assert.deepEqual(parseProjectQuery('tab=nonsense'), { tab: 'overview' })
})

test('a hand-off\'s query: the run it starts from, the where filter as text, the project, and the samples as a list', () => {
  assert.deepEqual(parseHandoffQuery(undefined), { from: null, where: null, project: null, samples: [], outputs: [] })
  const where = encodeURIComponent('[{"path":"condition","op":"=","value":"treated"}]')
  assert.deepEqual(parseHandoffQuery(`from=bafyrun&where=${where}&project=bafyproj&samples=WT_1,WT_2,,KO%2C1&outputs=quant_salmon,markdup`),
    { from: 'bafyrun', where: '[{"path":"condition","op":"=","value":"treated"}]', project: 'bafyproj', samples: ['WT_1', 'WT_2', 'KO,1'],
      outputs: ['quant_salmon', 'markdup'] })
})

test("a hand-off's query keeps a '+' as '+', in its fields and its lists, as the run route does", () => {
  assert.deepEqual(parseHandoffQuery('from=bafy+run&where=[["meta.id","string","a+b"]]&project=p+1&samples=WT+1,KO%2B2&outputs=quant+salmon'),
    { from: 'bafy+run', where: '[["meta.id","string","a+b"]]', project: 'p+1', samples: ['WT+1', 'KO+2'], outputs: ['quant+salmon'] })
  assert.deepEqual(parseHandoffQuery('from=a%E0%A4%A'), { from: 'a%E0%A4%A', where: null, project: null, samples: [], outputs: [] }, 'a malformed escape stays raw')
})

test('the routes table sends the three P4 pages to their views with their parsed queries', async () => {
  const at = (hash) => { const r = routeOf(hash); return [r.name, argsOf(r, hash)] }
  assert.deepEqual(at('#/projects?unassigned=1'), ['projects', { unassigned: true }])
  assert.deepEqual(at('#/project/bafyproj?tab=samples'), ['project', { cid: 'bafyproj', query: { tab: 'samples' } }])
  assert.deepEqual(at('#/handoff/nf-core%2Fdifferentialabundance?from=bafyrun&samples=WT_1'),
    ['handoff', { template: 'nf-core/differentialabundance', query: { from: 'bafyrun', where: null, project: null, samples: ['WT_1'], outputs: [] } }])
  // The route's own view, so a swapped argument fails here too.
  installDom()
  const hash = '#/handoff/nf-core-rnaseq?from=bafyrun'
  const route = routeOf(hash)
  const page = await route.view(null, argsOf(route, hash), {})
  assert.equal(page.querySelector('h1').textContent, 'No template for nf-core-rnaseq')
})

test('the hand-off route has its own view (Task 5), and so does the project route', async () => {
  installDom()
  const views = await import('../src/views/index.js')
  const { project } = await import('../src/views/project.js')
  const { handoffTemplate } = await import('../src/views/handoff-de.js')
  assert.equal(views.project, project)
  assert.equal(views.handoffTemplate, handoffTemplate)
  const unknown = await views.handoffTemplate(null, 'nf-core-rnaseq', {}, parseHandoffQuery('from=bafyrun'))
  assert.equal(unknown.querySelector('h1').textContent, 'No template for nf-core-rnaseq')
})

// Mockup 13 has no rail, and the rail's 320 px pushed the Samples table's Where column out of view; mockup 14 has one.
test('the Projects list and a project page have no rail; the hand-off page keeps its own', () => {
  assert.equal(showsRail('projects'), false)
  assert.equal(showsRail('project'), false)
  assert.equal(showsRail('handoff'), true)
})
