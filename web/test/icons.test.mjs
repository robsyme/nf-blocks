import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { ICON_CSS, KINDS, icon, ref } from '../src/icons.js'

test('every kind has an icon span naming its kind, and a CSS rule drawing it', () => {
  installDom()
  assert.deepEqual(KINDS, ['pipeline', 'run', 'output', 'item', 'file', 'directory', 'selection', 'claim', 'address', 'store'])
  for (const kind of KINDS) {
    const el = icon(kind)
    assert.equal(el.tagName, 'SPAN')
    assert.ok(el.classList.contains('icon') && el.classList.contains(`icon-${kind}`))
    assert.equal(el.dataset.icon, kind)
    assert.equal(el.getAttribute('aria-hidden'), 'true')
    assert.ok(el.getAttribute('title').length > 0)
    assert.match(ICON_CSS, new RegExp(`\\.icon-${kind}\\s*\\{`))
  }
  assert.equal(icon('selection').getAttribute('title'), 'Selection')
  assert.equal(icon('claim').getAttribute('title'), 'name or pin')
})

test('an unknown kind throws', () => {
  installDom()
  assert.throws(() => icon('blob'), /no icon for 'blob'/)
})

test('ref: the icon, then the label, linked when given an href', () => {
  installDom()
  const linked = ref('run', 'boring_yalow', '#/run/bafyrun')
  assert.ok(linked.classList.contains('ref'))
  assert.equal(linked.querySelector('[data-icon]').dataset.icon, 'run')
  assert.equal(linked.querySelector('a').getAttribute('href'), '#/run/bafyrun')
  assert.equal(linked.textContent, 'boring_yalow')
  const plain = ref('file', 'Tarik_Benali.dat')
  assert.equal(plain.querySelector('a'), null)
  assert.equal(plain.textContent, 'Tarik_Benali.dat')
})
