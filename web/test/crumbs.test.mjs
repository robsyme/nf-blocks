// web/test/crumbs.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { crumbs, runCrumbs } from '../src/views/common.js'

test('crumbs: linked segments joined by " / ", the last one plain', () => {
  installDom()
  const node = crumbs({ text: 'nf-core/rnaseq', href: '#/pipeline/nf-core%2Frnaseq' }, { text: 'high_jang', href: '#/run/c' }, { text: 'markdup' })
  assert.equal(node.textContent, 'nf-core/rnaseq / high_jang / markdup')
  assert.equal(node.querySelectorAll('a').length, 2)
  assert.equal(node.getAttribute('aria-label'), 'breadcrumb')
})

test('runCrumbs fills pipeline and run name from runIdentity, and shows the tail meanwhile', async () => {
  installDom()
  const ex = { runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'high_jang' }) }
  const node = runCrumbs(ex, 'bafyrun', [{ text: 'markdup' }])
  assert.equal(node.textContent, 'run / markdup')
  await new Promise(r => setTimeout(r, 0))
  assert.equal(node.textContent, 'nf-core/rnaseq / high_jang / markdup')
  const quiet = runCrumbs({}, 'bafyrun', [])
  assert.equal(quiet.textContent, 'run')
})
