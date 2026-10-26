// The app shell's brand mark and the left column's footer (P0 final fix wave,
// findings 8 and 12): a mark that is no CID's Seal, and the footer's two lines
// from mockups 03/04/08 with the base URL only as a title.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { agoText, snapshotModeText, staleText } from '../src/words.js'

const page = readFileSync(new URL('../src/index.html', import.meta.url), 'utf8')

test('the brand mark is a fixed glyph, not a Seal of some CID', () => {
  assert.doesNotMatch(page, /brand-seal/)
  assert.match(page, /<span class="brand-mark"[^>]*><svg/)
  const app = readFileSync(new URL('../src/app.js', import.meta.url), 'utf8')
  assert.doesNotMatch(app, /BRAND_MARK|bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce/)
})

test('the footer keeps #where, #snapshot-mode and #stale, #stale inside the snapshot line', () => {
  const foot = page.slice(page.indexOf('class="foot"'), page.indexOf('</nav>'))
  for (const id of ['where', 'snapshot-mode', 'stale', 'snapshot-age']) assert.match(foot, new RegExp(`id="${id}"`))
  assert.ok(foot.indexOf('id="stale"') > foot.indexOf('id="where"') && foot.indexOf('id="stale"') < foot.indexOf('id="snapshot-mode"'))
})

test('the footer lines: "Snapshot 2 min ago · 0 newer runs", "Index v6 · read by range"', () => {
  const now = Date.parse('2026-10-24T12:00:00Z')
  assert.equal(agoText('2026-10-24T11:59:40Z', now), 'just now')
  assert.equal(agoText('2026-10-24T11:58:00Z', now), '2 min ago')
  assert.equal(agoText('2026-10-24T09:00:00Z', now), '3 h ago')
  assert.equal(agoText('2026-10-22T12:00:00Z', now), '2 days ago')
  assert.equal(agoText(null, now), '')
  assert.equal(staleText({ staleCount: 0, logReadable: true }), '0 newer runs')
  assert.equal(staleText({ staleCount: 1, logReadable: true }), '1 newer run')
  assert.equal(staleText({ staleCount: 2, logReadable: false }), 'newer runs unknown: the Store Log is not readable here (2 found)')
  assert.equal(snapshotModeText('range'), 'Index v6 · read by range')
  assert.equal(snapshotModeText('whole'), 'Index v6 · downloaded whole (this server ignores Range)')
})

// app.js runs on import, so this reads its text: the search box ships enabled in
// index.html, and a page that failed to start has nothing for it to search.
test('a startup error disables the top bar\'s search box', () => {
  assert.match(page, /<input type="search" id="search"(?![^>]*disabled)[^>]*>/)
  const app = readFileSync(new URL('../src/app.js', import.meta.url), 'utf8')
  const start = app.slice(app.indexOf('async function start()'))
  const failed = start.slice(start.indexOf('} catch (failure) {'), start.indexOf("finish('error')"))
  assert.match(failed, /document\.getElementById\('search'\)\.disabled = true/)
})

test('the top bar ships the store label inside #store-wrap, where the switcher mounts, with #store-name inside it', () => {
  const wrap = page.slice(page.indexOf('id="store-wrap"'), page.indexOf('<label class="search">'))
  assert.match(wrap, /id="store-label"/)
  assert.match(wrap, /id="store-name"/)
})
