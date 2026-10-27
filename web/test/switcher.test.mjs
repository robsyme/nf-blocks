// web/test/switcher.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom } from './dom.mjs'
import { createSizes, mountSwitcher, memberHref, sizeText } from '../src/views/switcher.js'
import { drawStores } from '../src/nav.js'
import { createMembers } from '../src/members.js'

const MEMBERS = [
  { alias: 'roblab', writable: true, base: 'm/roblab/', kind: 'local', location: null, copy_target: true, copy_reason: null },
  { alias: 'org-s3', writable: false, base: 'm/org-s3/', kind: 's3', location: 's3://org-bucket/blocks', copy_target: true, copy_reason: null },
  { alias: 'ana-hpc', writable: false, base: 'm/ana/', kind: 'local', location: null, copy_target: false, copy_reason: 'the server cannot write there' },
]
const store = (over = {}) => ({ base: 'http://h/m/roblab/', members: MEMBERS, member: 'roblab', all: false, copy: true, write: true, identity: null, ...over })

/** createMembers over fake Explorers; `asked` lists the aliases opened, `broken` those that fail. */
function world({ broken = {}, stored = { roblab: 5_290_000, 'org-s3': 12_000_000 } } = {}) {
  const asked = []
  const s = store()
  const members = createMembers(s, { openExplorer: async (m) => {
    asked.push(m.alias)
    if (broken[m.alias]) throw Object.assign(new Error(broken[m.alias]), { code: broken[m.alias] })
    return { storeStats: async () => (m.alias in stored ? { stored: stored[m.alias] } : null) }
  } })
  return { s, members, asked }
}

const flush = () => new Promise(resolve => setTimeout(resolve, 5))
const PAGE = 'http://h/index.html?token=t&member=roblab#/run/bafy'

function mount(over = {}, opts = {}) {
  installDom()
  const wrap = document.createElement('span')
  const w = world(opts.world)
  const s = over.store ?? w.s
  const sizes = createSizes(w.members)
  const gone = []
  const sw = mountSwitcher(wrap, { store: s, members: w.members, sizes, pageHref: () => PAGE, go: (href) => gone.push(href) })
  return { wrap, sw, sizes, gone, ...w }
}

test('memberHref sets ?member= and keeps the hash and the other parameters', () => {
  assert.equal(memberHref(PAGE, 'org-s3'), 'http://h/index.html?token=t&member=org-s3#/run/bafy')
  assert.equal(memberHref('http://h/?x=1#/storage', '*'), 'http://h/?x=1&member=*#/storage')
})

test('with members the label is a listbox button showing the open store; the popover is closed and nothing else is opened', () => {
  const { wrap, sw, asked } = mount()
  const b = wrap.querySelector('button')
  assert.equal(b, sw.button)
  assert.ok(b.classList.contains('store-switch'))
  assert.equal(b.id, undefined)
  assert.equal(b.getAttribute('id'), 'store-label')
  assert.equal(b.getAttribute('aria-haspopup'), 'listbox')
  assert.equal(b.getAttribute('aria-expanded'), 'false')
  assert.equal(b.querySelector('#store-name').textContent, 'roblab')
  assert.ok(b.querySelector('.dot').classList.contains('a'))
  assert.equal(sw.popover.hidden, true)
  assert.deepEqual(asked, [], 'no member opened before the popover')
})

test('opening lists one option per member, then All stores, and opens each member once for its size', async () => {
  const { wrap, sw, asked } = mount()
  await sw.button.click()
  assert.equal(sw.button.getAttribute('aria-expanded'), 'true')
  assert.equal(sw.popover.hidden, false)
  const opts = wrap.querySelectorAll('[role=option]')
  assert.deepEqual(opts.map(o => o.getAttribute('data-store-option')), ['roblab', 'org-s3', 'ana-hpc', 'All stores'])
  assert.deepEqual(opts.map(o => o.getAttribute('aria-selected')), ['true', 'false', 'false', 'false'])
  assert.deepEqual(opts.slice(0, 3).map(o => o.querySelector('.dot').classList.contains('abc'[opts.indexOf(o)])), [true, true, true])
  const [lab, s3, hpc, all] = opts
  assert.match(lab.textContent, /local/)
  assert.match(lab.textContent, /writable/)
  assert.match(s3.textContent, /s3 · s3:\/\/org-bucket\/blocks/)
  assert.doesNotMatch(s3.textContent, /writable|read-only|Can't copy/)
  // The switcher's word is about copying: "View only" is the storage cards' word for what this page can't write.
  assert.match(hpc.textContent, /Can't copy here: the server cannot write there/)
  assert.doesNotMatch(hpc.textContent, /read-only|Read only/)
  assert.equal(all.querySelector('.presence').querySelectorAll('.dot').length, 3)
  await flush()
  assert.deepEqual(asked.sort(), ['ana-hpc', 'org-s3', 'roblab'])
  assert.match(lab.querySelector('.stored').textContent, /5\.29 MB|5\.3 MB|MB/)
  assert.match(s3.querySelector('.stored').textContent, /MB/)
  assert.notEqual(lab.querySelector('.stored').textContent, s3.querySelector('.stored').textContent, 'each store its own figure')
  assert.equal(hpc.querySelector('.stored').textContent, '', 'no storage figures on that snapshot: nothing claimed')
  await sw.button.click()
  await sw.button.click()
  await flush()
  assert.equal(asked.length, 3, 'reopening asks nothing again')
})

test('a member with no snapshot says so in the option', async () => {
  const { wrap, sw } = mount({}, { world: { broken: { 'org-s3': 'no_snapshot', 'ana-hpc': 'boom' } } })
  await sw.button.click()
  await flush()
  const text = (alias) => wrap.querySelector(`[data-store-option=${alias}]`).querySelector('.stored').textContent
  assert.equal(text('org-s3'), 'no snapshot')
  assert.equal(text('ana-hpc'), 'unavailable')
})

test('choosing an option goes to ?member=<alias> keeping the hash and the token; All stores goes to ?member=*', async () => {
  const { wrap, sw, gone } = mount()
  await sw.button.click()
  await wrap.querySelector('[data-store-option=org-s3]').click()
  assert.deepEqual(gone, ['http://h/index.html?token=t&member=org-s3#/run/bafy'])
  assert.equal(sw.popover.hidden, true)
  await sw.button.click()
  await wrap.querySelector('[data-store-option="All stores"]').click()
  assert.equal(gone[1], 'http://h/index.html?token=t&member=*#/run/bafy')
})

test('Escape closes from the button, the popover and the document; arrow keys move between options', async () => {
  const { wrap, sw } = mount()
  assert.equal(sw.button.press('ArrowDown'), true, 'ArrowDown on the button opens it')
  assert.equal(sw.popover.hidden, false)
  const on = () => wrap.querySelectorAll('[role=option]').findIndex(o => o.classList.contains('on'))
  assert.equal(on(), 0)
  sw.popover.press('ArrowDown')
  sw.popover.press('ArrowDown')
  assert.equal(on(), 2)
  sw.popover.press('ArrowDown')
  assert.equal(on(), 3)
  sw.popover.press('ArrowDown')
  assert.equal(on(), 0, 'wraps')
  sw.popover.press('ArrowUp')
  assert.equal(on(), 3)
  sw.popover.press('Home')
  assert.equal(on(), 0)
  sw.popover.press('End')
  assert.equal(on(), 3)
  sw.popover.press('Escape')
  assert.equal(sw.popover.hidden, true)
  assert.equal(sw.button.getAttribute('aria-expanded'), 'false')
  await sw.button.click()
  document.dispatch('keydown', { key: 'Escape' })
  assert.equal(sw.popover.hidden, true)
  assert.equal(document.listenerCount('keydown'), 0, 'listeners leave with the popover')
})

test('a click outside closes it; a click inside does not', async () => {
  const { wrap, sw } = mount()
  await sw.button.click()
  document.dispatch('click', { target: wrap.querySelector('.store-menu') })
  assert.equal(sw.popover.hidden, false)
  document.dispatch('click', { target: document.createElement('div') })
  assert.equal(sw.popover.hidden, true)
  assert.equal(document.listenerCount('click'), 0)
})

test('?member=* shows every member\'s dot and "All stores" on the button, with All stores selected', async () => {
  const { wrap, sw } = mount({ store: store({ all: true }) })
  const b = sw.button
  assert.equal(b.querySelector('#store-name').textContent, 'All stores')
  assert.equal(b.querySelector('.presence').querySelectorAll('.dot').length, 3)
  await b.click()
  assert.deepEqual(wrap.querySelectorAll('[role=option]').map(o => o.getAttribute('aria-selected')), ['false', 'false', 'false', 'true'])
})

test('with no members.json the switcher is a plain label, not a button', () => {
  installDom()
  const wrap = document.createElement('span')
  const s = { base: 'http://h/stores/lab/', members: null, member: null, all: false }
  assert.equal(mountSwitcher(wrap, { store: s }), null)
  assert.equal(wrap.querySelector('button'), null)
  const label = wrap.querySelector('#store-label')
  assert.ok(label.classList.contains('store-switch'))
  assert.equal(label.querySelector('#store-name').textContent, 'lab')
})

test('sizes: nothing opens until asked; a size is the member\'s own; sizeText', async () => {
  const { members, asked } = world()
  const sizes = createSizes(members)
  assert.deepEqual(sizes.get('org-s3'), { state: 'loading', stored: null })
  assert.deepEqual(asked, [])
  await sizes.load('org-s3')
  assert.deepEqual(asked, ['org-s3'])
  assert.equal(sizes.get('org-s3').stored, 12_000_000)
  assert.equal(sizeText({ state: 'loading', stored: null }), '')
  assert.equal(sizeText({ state: 'no_snapshot', stored: null }), 'no snapshot')
})

test('the Stores section: dot, alias and bytes per member, the open one marked, no snapshot named, never a total', async () => {
  installDom()
  const { s, members } = world({ broken: { 'ana-hpc': 'no_snapshot' } })
  const sizes = createSizes(members)
  await sizes.loadAll()
  const el = document.createElement('div')
  drawStores(el, { store: s, members: members.list, figure: sizes.get, href: (a) => memberHref(PAGE, a) })
  const rows = el.querySelectorAll('a')
  assert.deepEqual(rows.map(r => r.querySelector('.lbl').textContent), ['roblab', 'org-s3', 'ana-hpc'])
  assert.deepEqual(rows.map(r => r.classList.contains('on')), [true, false, false])
  assert.equal(rows[0].getAttribute('aria-current'), 'page')
  assert.match(rows[0].querySelector('small').textContent, /MB · writable$/)
  assert.match(rows[1].querySelector('small').textContent, /^\d[\d.]* MB$/)
  assert.equal(rows[2].querySelector('small').textContent, 'no snapshot')
  assert.equal(rows[1].getAttribute('href'), 'http://h/index.html?token=t&member=org-s3#/run/bafy')
  assert.ok(rows.every((r, i) => r.querySelector('.dot').classList.contains('abc'[i])))
})

test('sizes.forget after a copy: the Stores section hears the member again, then its new size, read from a fresh open', async () => {
  installDom()
  const stored = { roblab: 5_290_000, 'org-s3': 12_000_000 }
  const { s, members, asked } = world({ stored })
  const sizes = createSizes(members)
  await sizes.loadAll()
  const el = document.createElement('div')
  const draw = () => drawStores(el, { store: s, members: members.list, figure: sizes.get, href: (a) => memberHref(PAGE, a) })
  const heard = []
  sizes.onChange((alias, figure) => { heard.push([alias, figure.state]); draw() })
  draw()
  const small = () => el.querySelectorAll('a')[1].querySelector('small').textContent
  assert.equal(small(), '12 MB')
  // The copy wrote 3 MB into org-s3; the page forgets the member (members.invalidate) and its size.
  stored['org-s3'] = 15_000_000
  members.invalidate('org-s3')
  sizes.forget('org-s3')
  assert.deepEqual(heard[0], ['org-s3', 'loading'], 'redrawn at once, as loading')
  await flush()
  assert.deepEqual(heard.at(-1), ['org-s3', 'open'])
  assert.equal(small(), '15 MB')
  assert.equal(asked.filter(a => a === 'org-s3').length, 2, 'opened again')
  // A member nobody asked for stays unasked.
  const fresh = createSizes(world().members)
  const quiet = []
  fresh.onChange((a) => quiet.push(a))
  fresh.forget('org-s3')
  await flush()
  assert.deepEqual(quiet, [])
})

test('a member being indexed reads indexing… instead of no snapshot', () => {
  assert.equal(sizeText({ state: 'no_snapshot', stored: null }, { index: { state: 'indexing' } }), 'indexing…')
  assert.equal(sizeText({ state: 'no_snapshot', stored: null }, { index: { state: 'none' } }), 'no snapshot')
  assert.equal(sizeText({ state: 'open', stored: 2048 }, { index: { state: 'indexing' } }), '2.05 KB')
})
