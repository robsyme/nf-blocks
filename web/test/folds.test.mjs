// web/test/folds.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { fold, resetFolds } from '../src/folds.js'

test('a fold starts closed, names its kind, and remembers being opened across a redraw (P7)', async () => {
  installDom()
  resetFolds()
  const first = fold('storage:bafyrun', 'Storage', 'body')
  assert.equal(first.tagName, 'DETAILS')
  assert.equal(first.dataset.fold, 'storage')
  assert.equal(first.hasAttribute('open'), false)
  assert.equal(first.querySelector('summary').textContent, 'Storage')
  first.open = true
  await first.fire('toggle')
  assert.equal(fold('storage:bafyrun', 'Storage').hasAttribute('open'), true)
  assert.equal(fold('storage:other', 'Storage').hasAttribute('open'), false)
  const again = fold('storage:bafyrun', 'Storage')
  again.open = false
  await again.fire('toggle')
  assert.equal(fold('storage:bafyrun', 'Storage').hasAttribute('open'), false)
})

test('a fold can start open', () => {
  installDom()
  resetFolds()
  assert.equal(fold('details:x', 'Details', { open: true }).hasAttribute('open'), true)
})
