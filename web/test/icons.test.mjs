import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { ICON_CSS, KINDS, address, icon, ref, shortCid } from '../src/icons.js'

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

test('a tone colours an icon by status, and ref passes it on; no tone stays grey', () => {
  installDom()
  assert.ok(icon('run', { tone: 'good' }).classList.contains('tone-good'))
  assert.ok(!icon('run').getAttribute('class').includes('tone-'))
  assert.ok(ref('run', 'x', '#/run/x', { tone: 'bad' }).querySelector('[data-icon]').classList.contains('tone-bad'))
  assert.match(ICON_CSS, /\.icon\.tone-good\s*\{[^}]*var\(--good\)/)
  assert.match(ICON_CSS, /\.icon\.tone-bad\s*\{[^}]*var\(--bad\)/)
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

test('address: the address icon and the short CID, a quiet link to its block page', () => {
  installDom()
  const cid = 'bafyreian3sfv6mv6jh2en6g3ryqo2p2lde4nh3zxaysbhs5g574uac6uye'
  const a = address(cid)
  assert.equal(a.tagName, 'A')
  assert.ok(a.classList.contains('address'))
  assert.equal(a.getAttribute('href'), `#/block/${cid}`)
  assert.equal(a.getAttribute('title'), cid)
  assert.equal(a.dataset.address, cid)
  assert.equal(a.querySelector('[data-icon]').dataset.icon, 'address')
  assert.equal(a.textContent, 'bafyreian3...ac6uye')
  assert.equal(shortCid('short'), 'short')
})
