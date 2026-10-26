// web/test/nav.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { installDom } from './dom.mjs'
import { createNav } from '../src/nav.js'

const PROJECTS = ['Liver', 'Kidney', 'Lung', 'Heart', 'Brain', 'Skin'].map((name, i) => ({ cid: `bafyproj${i}`, name }))

function fakeEx() {
  const asked = []
  const ex = {
    asked,
    pipelines: async () => { asked.push('pipelines'); return [{ pipeline: 'nf-core/sarek', runs: 3 }, { pipeline: 'nf-core/rnaseq', runs: 12 }] },
    projects: async () => { asked.push('projects'); return { rows: PROJECTS, hiddenCount: 0 } },
    selectionPage: async ({ limit }) => { asked.push(`selections ${limit}`); return { rows: [{ cid: 'bafysel', state: { names: ['treated BAMs'] } }, { cid: 'bafyunnamed', state: { names: [] } }] } },
    runPage: async (name, { limit }) => {
      asked.push(`runs ${name} ${limit}`)
      return { rows: [{ completion_cid: 'bafygood', run_name: 'high_jang', status: 'succeeded', possibly_incomplete: 0, finished_at: '2026-09-30T21:10:38.345Z' },
        { completion_cid: 'bafybad', run_name: 'happy_rubens', status: 'failed', possibly_incomplete: 1, finished_at: '2026-09-30T16:55:59.157Z' }], total: 12 }
    },
  }
  return ex
}

test('load lists pipelines A–Z and five Selections, and reads no runs (P1)', async () => {
  installDom()
  const el = document.createElement('div')
  const ex = fakeEx()
  const countEl = document.createElement('small')
  const nav = createNav(ex, { el, countEl })
  await nav.load()
  assert.deepEqual(ex.asked.sort(), ['pipelines', 'projects', 'selections 5'])
  assert.equal(countEl.textContent, '2', 'the Browse link counts the pipelines')
  const names = el.querySelectorAll('[data-nav-pipeline]').map(n => n.dataset.navPipeline)
  assert.deepEqual(names, ['nf-core/rnaseq', 'nf-core/sarek'])
  assert.deepEqual(el.querySelectorAll('[data-nav-selection]').map(n => n.textContent), ['treated BAMs', 'unnamed'])
  assert.equal(el.querySelector('input'), null, 'the reserved search box lives in the top bar')
})

test('mark with expand reads ten runs of that pipeline; without expand it reads nothing', async () => {
  installDom()
  const el = document.createElement('div')
  const ex = fakeEx()
  const nav = createNav(ex, { el })
  await nav.load()
  await nav.mark({ pipeline: 'nf-core/rnaseq', run: 'bafygood' })
  assert.ok(!ex.asked.some(a => a.startsWith('runs')))
  await nav.mark({ pipeline: 'nf-core/rnaseq', run: 'bafygood', expand: true })
  assert.ok(ex.asked.includes('runs nf-core/rnaseq 10'))
  const runs = el.querySelectorAll('[data-nav-run]')
  assert.deepEqual(runs.map(r => r.dataset.navRun), ['bafygood', 'bafybad'])
  assert.equal(runs[0].getAttribute('aria-current'), 'page')
  assert.equal(runs[0].querySelector('[data-sealed]').querySelector('[data-seal]').dataset.seal, 'bafygood', 'the run by its Seal')
  assert.ok(runs[0].querySelector('[data-icon]').classList.contains('tone-good'), 'its badge green for a good run')
  assert.ok(runs[1].querySelector('[data-icon]').classList.contains('tone-bad'))
  assert.ok(runs[0].querySelector('a').classList.contains('on'), 'the current run is marked')
  assert.equal(runs[0].querySelector('small').textContent, new Date('2026-09-30T21:10:38.345Z').toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }).split(',')[0])
  assert.equal(runs[0].querySelector('.nav-run-name').textContent, 'high_jang')
  assert.match(runs[0].querySelector('a').getAttribute('title'), /^high_jang · Succeeded · /)
  assert.match(runs[1].querySelector('a').getAttribute('title'), /^happy_rubens · Failed, may be incomplete · /)
  assert.doesNotMatch(el.textContent, /●/)
  assert.ok(el.querySelectorAll('a').some(a => a.textContent === 'all runs…' && a.getAttribute('href') === '#/pipeline/nf-core%2Frnaseq'))
  await nav.mark({ pipeline: 'nf-core/rnaseq', run: 'bafybad', expand: true })
  assert.equal(ex.asked.filter(a => a.startsWith('runs')).length, 1, 'one read per pipeline')
})

test('clicking a pipeline expands it', async () => {
  installDom()
  const el = document.createElement('div')
  const ex = fakeEx()
  const nav = createNav(ex, { el })
  await nav.load()
  await el.querySelector('[data-nav-pipeline="nf-core/sarek"]').querySelector('button').click()
  await new Promise(r => setTimeout(r, 0))
  assert.ok(ex.asked.includes('runs nf-core/sarek 10'))
})

test('failures show as warnings, never [data-error]; no [data-run] or [data-selection] anywhere (P14)', async () => {
  installDom()
  const el = document.createElement('div')
  const ex = fakeEx()
  ex.pipelines = async () => { throw Object.assign(new Error('range read failed'), { code: 'query_failed' }) }
  const nav = createNav(ex, { el })
  await nav.load()
  assert.match(el.textContent, /range read failed/)
  assert.ok(!el.querySelector('[data-error]'))
  const okEl = document.createElement('div')
  const ok = createNav(fakeEx(), { el: okEl })
  await ok.load()
  await ok.mark({ pipeline: 'nf-core/rnaseq', expand: true })
  assert.ok(okEl.querySelector('[data-nav-run]'), 'runs are listed')
  assert.ok(!okEl.querySelector('[data-run]') && !okEl.querySelector('[data-selection]') && !okEl.querySelector('[data-error]'))
})

test('Browse lists Projects first, then Pipelines, Selections and Storage (design doc §2)', async () => {
  const page = readFileSync(new URL('../src/index.html', import.meta.url), 'utf8')
  const browse = page.slice(page.indexOf('Browse'), page.indexOf('id="nav-body"'))
  const links = [...browse.matchAll(/<a href="([^"]+)" data-nav-link="([^"]+)"/g)].map(m => [m[2], m[1]])
  assert.deepEqual(links, [['projects', '#/projects'], ['pipelines', '#/'], ['selections', '#/selections']])
  assert.match(browse, /data-nav-link="projects">.*<span class="lbl">Projects<\/span><small id="nav-project-count"><\/small><\/a>/)
  installDom()
  const el = document.createElement('div')
  await createNav(fakeEx(), { el }).load()
  // Storage opens the tree, so it sits right under the Browse links.
  assert.equal(el.children[0].dataset.navLink, 'storage')
})

test('the Projects section: the five newest projects, all projects…, and Not in a project, ahead of the pipelines', async () => {
  installDom()
  const el = document.createElement('div')
  const projectCountEl = document.createElement('small')
  const nav = createNav(fakeEx(), { el, projectCountEl })
  await nav.load()
  assert.equal(projectCountEl.textContent, '6')
  const heads = el.querySelectorAll('h2').map(h => h.textContent)
  assert.deepEqual(heads, ['Projects', 'Runs by pipeline', 'Recent Selections'])
  const links = el.querySelectorAll('[data-nav-project]')
  assert.deepEqual(links.map(a => [a.textContent, a.getAttribute('href')]), PROJECTS.slice(0, 5).map(p => [p.name, `#/project/${p.cid}`]))
  assert.ok(el.querySelectorAll('a').some(a => a.textContent === 'all projects…' && a.getAttribute('href') === '#/projects'))
  const loose = el.querySelector('[data-nav-link=unassigned]')
  assert.equal(loose.getAttribute('href'), '#/projects?unassigned=1')
  assert.equal(loose.textContent, 'Not in a project')
  await nav.mark({ project: 'bafyproj1' })
  const on = el.querySelectorAll('[data-nav-project]').filter(a => a.classList.contains('on'))
  assert.deepEqual(on.map(a => a.dataset.navProject), ['bafyproj1'])
})

test('no projects: no count, and Not in a project still shows; a failed read is a warning', async () => {
  installDom()
  const el = document.createElement('div')
  const projectCountEl = document.createElement('small')
  const ex = fakeEx()
  ex.projects = async () => ({ rows: [], hiddenCount: 0 })
  await createNav(ex, { el, projectCountEl }).load()
  assert.equal(projectCountEl.textContent, '')
  assert.equal(el.querySelectorAll('[data-nav-project]').length, 0)
  assert.ok(el.querySelector('[data-nav-link=unassigned]'))
  const bad = document.createElement('div')
  ex.projects = async () => { throw new Error('range read failed') }
  await createNav(ex, { el: bad }).load()
  assert.match(bad.textContent, /Could not list projects: range read failed/)
  assert.ok(bad.querySelector('[data-nav-link=unassigned]'), 'Not in a project stays below the warning')
  assert.ok(bad.querySelector('[data-nav-pipeline]'), 'the pipelines still list')
})

test('reloadAfterWrite reads the projects again, so a new project shows after its write', async () => {
  installDom()
  const el = document.createElement('div')
  const ex = fakeEx()
  const nav = createNav(ex, { el })
  await nav.load()
  ex.projects = async () => ({ rows: [{ cid: 'bafynew', name: 'Fresh' }, ...PROJECTS], hiddenCount: 0 })
  await nav.reloadAfterWrite()
  assert.equal(el.querySelector('[data-nav-project]').textContent, 'Fresh')
})
