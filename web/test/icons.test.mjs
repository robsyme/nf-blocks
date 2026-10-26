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
  assert.match(ICON_CSS, /\.icon\.tone-good\s*\{[^}]*var\(--ok\)/)
  assert.match(ICON_CSS, /\.icon\.tone-bad\s*\{[^}]*var\(--fail\)/)
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
  assert.equal(a.textContent, 'bafy…ac6uye', 'the kit short form, as addrChip shows it')
  assert.equal(shortCid('short'), 'short')
})

test('sealed: the Seal with the kind on a badge, the plain icon without a CID', async () => {
  installDom()
  const { sealSvg } = await import('../src/seal.js')
  const cid = 'bafyreian3sfv6mv6jh2en6g3ryqo2p2lde4nh3zxaysbhs5g574uac6uye'
  const { sealed } = await import('../src/icons.js')
  const s = sealed('run', cid, { size: 20, tone: 'bad' })
  assert.equal(s.dataset.sealed, 'run')
  assert.equal(s.querySelector('[data-seal]').innerHTML, sealSvg(cid, 20))
  assert.equal(s.querySelector('.badge').querySelector('[data-icon]').dataset.icon, 'run')
  assert.ok(s.querySelector('[data-icon]').classList.contains('tone-bad'))
  assert.equal(sealed('pipeline', null).dataset.icon, 'pipeline', 'no CID, no Seal')
  assert.equal(ref('output', 'people', '#/x', { cid }).querySelector('[data-sealed]').dataset.sealed, 'output')
  assert.match(ICON_CSS, /\.sealed \.badge\s*\{/)
})
