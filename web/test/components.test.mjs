// web/test/components.test.mjs
// The DOM contract of the kit's builders: classes, data-* and text.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { addrChip, age, avatar, shortCid, toneFor, claimTag, factTag, fileChip, kindChip, note, pageHead, panel, presence, status, storageBar, tabs } from '../src/components.js'
import { sealSvg } from '../src/seal.js'
import { samePrincipal } from '../src/identity.js'
import { APP_CSS } from '../src/styles.js'

const CID = 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq'

test('addrChip: data-ref and data-address, the short CID, its Seal and a copy affordance', () => {
  installDom()
  const chip = addrChip(CID)
  assert.equal(chip.tagName, 'SPAN')
  assert.ok(chip.classList.contains('addr'))
  assert.equal(chip.dataset.ref, CID)
  assert.equal(chip.dataset.address, CID)
  assert.match(chip.textContent, /^bafy…26ocxq/)
  assert.ok(chip.querySelector('.copy'))
  assert.equal(chip.querySelector('[data-seal]').innerHTML, sealSvg(CID, 14))
  const ref = addrChip(CID, { ref: `cas://${CID}/x.tsv`, seal: false })
  assert.equal(ref.dataset.ref, `cas://${CID}/x.tsv`)
  assert.equal(ref.dataset.address, CID)
  assert.equal(ref.querySelector('[data-seal]'), null)
})

test('addrChip: a click copies the full reference and says so', async () => {
  installDom()
  const copied = []
  globalThis.navigator ??= {}
  const before = Object.getOwnPropertyDescriptor(globalThis, 'navigator')
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: { clipboard: { writeText: async (t) => { copied.push(t) } } } })
  try {
    const chip = addrChip(CID, { ref: `cas://${CID}` })
    await chip.click()
    assert.deepEqual(copied, [`cas://${CID}`])
    assert.equal(chip.getAttribute('title'), 'Copied')
  } finally {
    Object.defineProperty(globalThis, 'navigator', before)
  }
})

test('avatar: initials of the email or name; anonymous is a dashed question mark', () => {
  installDom()
  assert.equal(avatar('ada.lovelace@example.org').textContent, 'AL')
  assert.equal(avatar('Ana Kim').textContent, 'AK')
  assert.ok(avatar('ada.lovelace@example.org').classList.contains('sm'))
  assert.ok(avatar('ada.lovelace@example.org', { size: 'lg' }).classList.contains('lg'))
  const anon = avatar('anonymous')
  assert.ok(anon.classList.contains('anon'))
  assert.equal(anon.textContent, '?')
  assert.equal(anon.getAttribute('title'), 'Signed anonymously. Set up your identity')
  assert.ok(avatar('').classList.contains('anon'))
})

test('status: icon and word, ok or fail', () => {
  installDom()
  const failed = status({ status: 'failed' })
  assert.match(failed.textContent, /Failed/)
  assert.ok(failed.classList.contains('fail'))
  const ok = status({ status: 'succeeded' })
  assert.equal(ok.textContent, 'Succeeded')
  assert.ok(ok.classList.contains('ok'))
  assert.match(ok.querySelector('.ico').innerHTML, /^<svg/)
  const partial = status({ status: 'succeeded', possibly_incomplete: 1 })
  assert.equal(partial.textContent, 'Succeeded, may be incomplete')
  assert.ok(partial.classList.contains('fail'))
})

test('claimTag: avatar, verb, value and age', () => {
  installDom()
  const now = Date.parse('2026-10-24T15:00:00Z')
  const claim = claimTag({ assertedBy: 'ada.lovelace@example.org', verb: 'named', value: 'wigshop', at: '2026-10-24T12:00:00Z', now })
  assert.ok(claim.classList.contains('claim'))
  assert.equal(claim.querySelector('.verb').textContent, 'named')
  assert.equal(claim.querySelector('.av').textContent, 'AL')
  assert.match(claim.textContent, /wigshop/)
  assert.equal(claim.querySelector('.age').textContent, '3h')
  const conflict = claimTag({ assertedBy: 'anonymous', verb: 'named', value: 'wig-final', conflict: true })
  assert.ok(conflict.classList.contains('conflict'))
  assert.match(conflict.textContent, /“wig-final” · conflicts$/, 'a conflict is said in words, not colour alone')
  assert.equal(claimTag({ assertedBy: 'anonymous', verb: 'named' }).querySelector('.age'), null)
})

// Task 6: a long value ran a tag out of its panel. The value alone is cut, with the whole of it in its title.
test('claimTag: the value sits in its own span, titled with its whole text, and a tag never grows wider than its box', () => {
  installDom()
  const long = 'uninduced vs wild_type (differentialabundance) to nf-core/differentialabundance'
  const tag = claimTag({ assertedBy: 'ada.lovelace@example.org', verb: 'handed off', value: long, at: '2026-10-24T12:00:00Z', now: Date.parse('2026-10-24T15:00:00Z') })
  const val = tag.querySelector('.val')
  assert.equal(val.textContent, `“${long}”`)
  assert.equal(val.getAttribute('title'), `“${long}”`)
  const node = document.createElement('span')
  node.append('sad_yonath')
  assert.equal(claimTag({ assertedBy: 'anonymous', verb: 'added', value: node }).querySelector('.val').getAttribute('title'), 'sad_yonath')
  assert.equal(claimTag({ assertedBy: 'anonymous', verb: 'created' }).querySelector('.val'), null)
  assert.match(APP_CSS, /\.claim \{ max-width: 100%; min-width: 0; \}/)
  assert.match(APP_CSS, /\.claim \.val \{[^}]*min-width: 0;[^}]*overflow: hidden;[^}]*text-overflow: ellipsis;/)
})

test('claimTag: a value that is not a string reads as its JSON text, cut short, never [object Object]', () => {
  installDom()
  const verb = (value) => claimTag({ assertedBy: 'anonymous', verb: 'dose =', value }).textContent.replace(/^AN|^\?/, '').replace(/^.*?dose =/, '')
  assert.equal(verb({ mg: 5, unit: 'per day' }), '{"mg":5,"unit":"per day"}')
  assert.equal(verb(3), '3')
  assert.equal(verb(false), 'false')
  const long = verb({ note: 'x'.repeat(200) })
  assert.equal(long.length, 60)
  assert.ok(long.endsWith('…'))
  assert.doesNotMatch(claimTag({ assertedBy: 'anonymous', verb: 'set', value: { a: 1 } }).textContent, /object Object/)
})

test('age: hours, days, then a short date', () => {
  const now = Date.parse('2026-10-24T15:00:00Z')
  assert.equal(age('2026-10-24T14:59:40Z', now), 'now')
  assert.equal(age('2026-10-24T14:20:00Z', now), '40m')
  assert.equal(age('2026-10-24T12:00:00Z', now), '3h')
  assert.equal(age('2026-10-22T12:00:00Z', now), '2d')
  assert.equal(age('2026-09-01T12:00:00Z', now), 'Sep 1')
  assert.equal(age('2025-09-01T12:00:00Z', now), 'Sep 1, 2025')
})

test('factTag, presence, chips, notes and the page head', () => {
  installDom()
  const fact = factTag('run', 'distraught_murdock', CID, `#/run/${CID}`)
  assert.equal(fact.tagName, 'A')
  assert.equal(fact.querySelector('.k'), null, 'a known kind rides on the badge, not a word')
  assert.equal(fact.querySelector('[data-sealed]').dataset.sealed, 'run')
  assert.ok(factTag('manifest', 'nf-core/rnaseq', CID).querySelector('.k'), 'a kind with no icon keeps its word')
  assert.equal(fact.querySelector('[data-seal]').dataset.seal, CID)
  assert.equal(fact.getAttribute('data-address'), null, 'facts are not address chips')
  const p = presence([{ key: 'a', name: 'roblab', held: true }, { key: 'c', name: 'ana-hpc', held: false }])
  assert.deepEqual(p.querySelectorAll('.dot').map(d => d.getAttribute('class')), ['dot a', 'dot ring c'])
  assert.equal(p.getAttribute('title'), 'Held in roblab; referenced in ana-hpc')
  assert.equal(kindChip('bam', 5).textContent, 'bam×5')
  const file = fileChip('fastqc', [['zip', 6], ['html', 6]], { href: '#/x' })
  assert.equal(file.tagName, 'A')
  assert.equal(file.querySelector('.ext').textContent, 'zip×6 html×6')
  assert.ok(note('Careful', 'warn').classList.contains('warn'))
  const head = pageHead({ crumbs: [{ text: 'Pipelines', href: '#/' }, { text: 'here' }], seal: CID, title: 'distraught_murdock', subline: ['4 runs'] })
  assert.equal(head.querySelector('h1').textContent, 'distraught_murdock')
  assert.equal(head.querySelector('.crumbs').querySelector('a').getAttribute('href'), '#/')
  assert.equal(head.querySelector('.crumbs').querySelector('b').textContent, 'here')
  assert.equal(head.querySelector('.title').querySelector('[data-seal]').dataset.seal, CID)
  const t = tabs([{ key: 'outputs', label: 'Outputs', href: '#/o', count: 17, on: true }, { key: 'storage', label: 'Storage', href: '#/s' }])
  assert.deepEqual(t.querySelectorAll('a').map(a => a.getAttribute('class')), ['on', null])
  assert.equal(t.querySelector('small').textContent, '17')
  const box = panel({ title: 'Selections', flush: true }, 'body')
  assert.ok(box.classList.contains('flush'))
  assert.equal(box.querySelector('h2').textContent, 'Selections')
  assert.equal(box.querySelector('.pb').textContent, 'body')
})

test('addrChip: a lid:// reference shortens its id and carries no data-address', () => {
  installDom()
  const chip = addrChip('lid://e25b50f2755ea17dffe98a907420b06f', { seal: false })
  assert.match(chip.textContent, /^lid:\/\/e25b…b06f/)
  assert.equal(chip.getAttribute('data-address'), null)
})

test('storageBar: segments by share of the total, the legend in kit bytes, and the reader\'s share', () => {
  installDom()
  const bar = storageBar({ total: 142134100, held: 142134100, yours: 142134100, store: 'a', storeName: 'roblab' })
  assert.equal(bar.querySelector('.seg.held').getAttribute('style'), 'width:100.00%')
  assert.match(bar.querySelector('.legend').textContent, /Held 142 MB/)
  assert.equal(bar.querySelector('.mine').querySelector('.num').textContent, '142 MB · 100%')
  assert.equal(storageBar({ total: 10, pinned: 5 }).querySelector('.mine'), null)
})

test('one short form: icons.js shortCid is the kit form addrChip shows', async () => {
  const icons = await import('../src/icons.js')
  assert.equal(icons.shortCid, shortCid)
  assert.equal(shortCid(CID), 'bafy…26ocxq')
  assert.equal(shortCid('short'), 'short')
})

test('avatar: a tone from the principal when none is given; anonymous in any case', () => {
  installDom()
  assert.ok(['rs', 'ak', 'jt'].includes(toneFor('ada.lovelace@example.org')))
  assert.equal(toneFor('ada.lovelace@example.org'), toneFor('ada.lovelace@example.org'), 'deterministic')
  assert.notEqual(new Set(['ada.lovelace@example.org', 'ana.kim@example.org', 'jo.tan@example.org', 'paolo@example.org'].map(toneFor)).size, 1)
  assert.ok(avatar('ada.lovelace@example.org').classList.contains(toneFor('ada.lovelace@example.org')))
  assert.ok(avatar('ada.lovelace@example.org', { tone: 'jt' }).classList.contains('jt'))
  // The colour follows samePrincipal: ASCII whitespace and ASCII case only.
  assert.equal(toneFor(' Ada@Example.ORG\t\v'), toneFor('ada@example.org'))
  assert.ok(!samePrincipal('\u00a0rob', 'rob'))
  assert.notEqual(toneFor('\u00a0rob'), toneFor('rob'), 'a no-break space is not trimmed')
  assert.ok(!samePrincipal('\u00c9mile@example.org', '\u00e9mile@example.org'))
  assert.notEqual(toneFor('\u00c9mile@example.org'), toneFor('\u00e9mile@example.org'), 'a non-ASCII capital is not lowercased')
  for (const who of ['Anonymous', ' ANONYMOUS ', null, undefined]) {
    const a = avatar(who)
    assert.ok(a.classList.contains('anon'), String(who))
    assert.equal(a.textContent, '?')
  }
})

test('status: a run neither succeeded nor failed is neutral, with its own word', () => {
  installDom()
  for (const [run, word] of [[{ status: 'running' }, 'Running'], [{ status: null }, 'Unknown'], [{}, 'Unknown']]) {
    const el = status(run)
    assert.equal(el.textContent, word)
    assert.ok(el.classList.contains('neutral'), word)
    assert.ok(!el.classList.contains('fail') && !el.classList.contains('ok'), word)
    assert.doesNotMatch(el.querySelector('.ico').innerHTML, /m9 9 6 6/, 'no X')
  }
  assert.ok(status({ status: 'failed', possibly_incomplete: 1 }).classList.contains('fail'))
})
