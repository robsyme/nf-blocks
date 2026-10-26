// P1b: who is signing. identity.js, the #/identity page, the top bar chip, and the local-time fixes.
process.env.TZ = 'America/Los_Angeles'
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { installDom } from './dom.mjs'

const identityMod = await import('../src/identity.js')
const { samePrincipal, mineState, viewerOf, isAnon, SOURCE_TEXT } = await import('../src/identity.js')
const { identity: identityView, identityChip } = await import('../src/views/identity.js')
const { avatar, age, claimTag } = await import('../src/components.js')
const { resolveStore } = await import('../src/store.js')
const { APP_CSS } = await import('../src/styles.js')

const ROB = { principal: 'ada.lovelace@example.org', source: 'git' }
const ANON = { principal: 'anonymous', source: 'anonymous' }
// A typed `anonymous` from an old server or a config that predates the fall-through.
const TYPED_ANON = { principal: 'anonymous', source: 'config' }

test('samePrincipal trims and ignores case, and never matches empty', () => {
  assert.equal(samePrincipal(' Ada.Lovelace@Example.org ', 'ada.lovelace@example.org'), true)
  assert.equal(samePrincipal('', ''), false)
  assert.equal(samePrincipal('a', ''), false)
  assert.equal(samePrincipal(null, undefined), false)
})

test('samePrincipal trims ASCII whitespace and lowercases ASCII only, as SQLite does', () => {
  assert.equal(samePrincipal('\tAda@Example.org\n', 'ada@example.org'), true)
  assert.equal(samePrincipal('ada@example.org', 'ADA@EXAMPLE.ORG'), true)
  assert.equal(samePrincipal('\v\f\r ada@example.org', 'ada@example.org'), true)
  assert.equal(samePrincipal('Émile@example.org', 'émile@example.org'), false)
  assert.equal(samePrincipal('\u00a0ada@example.org', 'ada@example.org'), false, 'a no-break space is not ASCII whitespace')
})

test('isMine is gone', async () => {
  assert.equal('isMine' in identityMod, false)
  const { execFileSync } = await import('node:child_process')
  let hits = []
  try { hits = execFileSync('grep', ['-rl', 'isMine', new URL('../src', import.meta.url).pathname]).toString().trim().split('\n').filter(Boolean) } catch { /* grep exits 1 on no match */ }
  assert.deepEqual(hits, [])
})

test('viewerOf: the principal, or null for no identity and anonymous', () => {
  assert.equal(viewerOf(ROB), 'ada.lovelace@example.org')
  assert.equal(viewerOf(ANON), null)
  assert.equal(viewerOf(null), null)
  assert.equal(viewerOf({ principal: '', source: 'git' }), null)
})

test('mineState says why Mine is unavailable', () => {
  assert.deepEqual(mineState(ROB), { available: true, reason: null })
  assert.deepEqual(mineState(null), { available: false, reason: 'Mine needs this page opened through nextflow plugin nf-blocks:explore, which knows who you are.' })
  assert.deepEqual(mineState(ANON), { available: false, reason: 'No identity is configured, so none of these can be marked as yours.' })
  assert.deepEqual(mineState(TYPED_ANON), mineState(ANON))
})

test('isAnon is the one anonymous rule: no identity, a blank name, the anonymous source, or a name that reads anonymous', () => {
  installDom()
  assert.equal(isAnon(null), true)
  assert.equal(isAnon(ANON), true)
  assert.equal(isAnon(TYPED_ANON), true)
  assert.equal(isAnon({ principal: ' Anonymous ', source: 'env' }), true)
  assert.equal(isAnon({ principal: '', source: 'git' }), true)
  assert.equal(isAnon(ROB), false)
  // components.js's avatar uses the same rule for a claim's asserted_by.
  assert.ok(avatar(' ANONYMOUS ').classList.contains('anon'))
})

test('SOURCE_TEXT names every source', () => {
  assert.deepEqual(Object.keys(SOURCE_TEXT).sort(), ['anonymous', 'config', 'env', 'git', 'platform'])
  assert.equal(SOURCE_TEXT.config, 'cas.asserted_by in your config')
})

test('resolveStore keeps identity from members.json, and gives null without one', async () => {
  const body = (extra) => async () => new Response(JSON.stringify({ members: [{ alias: 'lab', base: 'm/lab/' }], ...extra }), { headers: { 'Content-Type': 'application/json' } })
  assert.deepEqual((await resolveStore('http://127.0.0.1:9/', { fetchFn: body({ identity: ROB }) })).identity, ROB)
  assert.equal((await resolveStore('http://127.0.0.1:9/', { fetchFn: body({}) })).identity, null)
  assert.equal((await resolveStore('http://127.0.0.1:9/?store=http://x/y', { fetchFn: body({ identity: ROB }) })).identity, null)
  assert.equal((await resolveStore('http://h/stores/current/index.html', { fetchFn: async () => new Response('no', { status: 404 }) })).identity, null)
})

test('the anonymous avatar links to #/identity; link:false gives a span; a named one has an aria-label', () => {
  installDom()
  const a = avatar('anonymous')
  assert.equal(a.tagName, 'A')
  assert.equal(a.attributes.get('href'), '#/identity')
  assert.ok(a.classList.contains('anon'))
  assert.equal(a.textContent, '?')
  assert.equal(a.attributes.get('aria-label'), 'Signed anonymously. Set up your identity')
  assert.equal(a.attributes.get('title'), 'Signed anonymously. Set up your identity')
  assert.equal(avatar('anonymous', { link: false }).tagName, 'SPAN')
  assert.equal(avatar('ada.lovelace@example.org').attributes.get('aria-label'), 'ada.lovelace@example.org')
})

test('age formats short dates in the viewer\'s local zone', () => {
  const now = Date.parse('2026-02-10T12:00:00Z')
  assert.equal(age('2026-01-02T23:30:00Z', now), 'Jan 2')
  assert.equal(age('2026-01-03T03:30:00Z', now), 'Jan 2')
})

test('claimTag absolute reads local "Oct 24, 3:40 PM" and keeps the ISO title', () => {
  installDom()
  const local = new Date(2026, 9, 24, 15, 40)
  const tag = claimTag({ assertedBy: 'ada.lovelace@example.org', verb: 'named', at: local.toISOString(), absolute: true })
  const el = tag.querySelector('.age')
  assert.equal(el.textContent, 'Oct 24, 3:40 PM')
  assert.equal(el.attributes.get('title'), local.toISOString())
})

test('the identity view shows the principal, the source and the five steps', () => {
  installDom()
  const text = identityView({ identity: ROB }).textContent
  assert.match(text, /Identity/)
  assert.match(text, /Signing as/)
  assert.match(text, /ada\.lovelace@example\.org/)
  assert.match(text, /from git config user\.email/)
  assert.match(text, /How nf-blocks decides/)
  for (const s of ['cas.asserted_by', 'tower.accessToken', 'TOWER_ACCESS_TOKEN', 'NXF_BLOCKS_USER', 'git config user.email', 'anonymous']) assert.ok(text.includes(s), s)
  assert.match(text, /NXF_BLOCKS_USER=/)
  assert.doesNotMatch(text, /No identity is configured/)
})

test('the closing note says cas.asserted_by overrides everything and when NXF_BLOCKS_USER applies', () => {
  installDom()
  const view = identityView({ identity: ROB })
  const closing = view.querySelector('.note')
  assert.ok(closing.querySelectorAll('code').some(c => c.textContent === 'cas.asserted_by'), 'cas.asserted_by is in <code>')
  assert.match(closing.textContent, /cas\.asserted_by.* overrides every other source/)
  assert.match(closing.textContent, /NXF_BLOCKS_USER=you@example\.org/)
  assert.match(closing.textContent, /applies when neither cas\.asserted_by nor Seqera Platform gives a name: no Platform token is set, or the Platform read failed/)
  assert.doesNotMatch(closing.textContent, /only when/)
})

test('the identity view for an anonymous identity says what claims will say, in a sentence', () => {
  installDom()
  for (const id of [ANON, TYPED_ANON]) {
    const view = identityView({ identity: id })
    const text = view.textContent
    assert.equal(view.querySelector('.principal').textContent, 'anonymous')
    assert.match(text, /No identity is configured, so claims you write will say anonymous\./)
    assert.doesNotMatch(text, /from nothing configured/)
    assert.doesNotMatch(text, /from cas\.asserted_by/)
  }
})

test('the identity view without an identity says it cannot tell', () => {
  installDom()
  const text = identityView({ identity: null }).textContent
  assert.match(text, /This page isn't served by nf-blocks:explore, so it can't tell who you are\./)
  assert.match(text, /How nf-blocks decides/)
})

test('the top bar chip: named, anonymous, and none', () => {
  installDom()
  const named = identityChip(ROB)
  assert.equal(named.attributes.get('href'), '#/identity')
  assert.equal(named.attributes.get('data-identity'), 'git')
  assert.match(named.textContent, /ada\.lovelace@example\.org/)
  assert.match(named.attributes.get('title'), /git config user\.email/)
  assert.ok(named.querySelector('.av'))
  // Mockup 08: plain avatar and email, the kit's .me, no bordered chip.
  assert.ok(named.classList.contains('me'))
  assert.ok(!named.classList.contains('you'))
  assert.ok(named.querySelector('.av').classList.contains('sm'))
  for (const id of [ANON, TYPED_ANON]) {
    const anon = identityChip(id)
    assert.equal(anon.attributes.get('data-identity'), 'anonymous')
    assert.equal(anon.attributes.get('href'), '#/identity')
    assert.ok(anon.classList.contains('warn'))
    assert.ok(anon.classList.contains('me'))
    assert.match(anon.textContent, /No identity: claims will say anonymous/)
  }
  assert.equal(identityChip(null), null)
})

test('the anonymous chip stays on one line at the top bar height', () => {
  const rule = APP_CSS.match(/\.top a\.me\.warn\s*\{([^}]*)\}/)
  assert.ok(rule, 'a rule for the anonymous chip in the top bar')
  assert.match(rule[1], /white-space:\s*nowrap/)
  assert.doesNotMatch(APP_CSS, /\.you\b/)
})

test('the you chip goes after Picked, as in mockup 08', () => {
  const app = readFileSync(new URL('../src/app.js', import.meta.url), 'utf8')
  const body = app.slice(app.indexOf('function renderIdentity'), app.indexOf('async function start'))
  assert.match(body, /getElementById\('tray'\)\??\.after\(chip\)/)
  assert.doesNotMatch(body, /prepend/)
})
