// web/src/views/switcher.js
// The top bar's store switcher (P3 Task 2, mockup build.py top()): a button
// showing the open store, or every member under "All stores", that opens a
// listbox of the members. Choosing one reloads the page with ?member=. With
// no members.json the page reads one store and the switcher is a plain label.
// Stored bytes are per member and never added together; a member's snapshot
// is opened only when the popover or the Stores section asks for its size.
import { h } from '../html.js'
import { bytes } from '../format.js'
import { presence } from '../components.js'
import { storeKey, storeName } from '../storename.js'
import { cantCopyText } from '../copy.js'

/** This page's URL with ?member= set to `alias` ('*' for every member); the hash and other parameters stay. */
export function memberHref(pageHref, alias) {
  const url = new URL(pageHref)
  url.searchParams.set('member', alias)
  return url.href
}

/**
 * What each member's store holds, read once and only when asked.
 * `get(alias)` is `{ state: 'loading'|'open'|'no_snapshot'|'failed', stored }`
 * (`stored` null on a snapshot without storage figures); `onChange(fn)` hears
 * each member as it settles. `load(alias)` opens that member's Explorer.
 */
export function createSizes(members) {
  const known = new Map()
  const asked = new Map()
  const listeners = new Set()
  const settle = (alias, figure) => { known.set(alias, figure); for (const fn of [...listeners]) fn(alias, figure) }
  function load(alias) {
    if (!asked.has(alias)) {
      asked.set(alias, (async () => {
        let ex
        try {
          ex = await members.explorer(alias)
        } catch {
          // Asked for above, so the status is the member's own now, not "opening".
          settle(alias, { state: members.status(alias).state === 'no_snapshot' ? 'no_snapshot' : 'failed', stored: null })
          return
        }
        const stats = await Promise.resolve().then(() => ex.storeStats?.()).catch(() => null)
        settle(alias, { state: 'open', stored: stats ? stats.stored : null })
      })())
    }
    return asked.get(alias)
  }
  return {
    get: (alias) => known.get(alias) ?? { state: 'loading', stored: null },
    load,
    loadAll: () => Promise.all(members.list.map(m => load(m.alias))),
    /**
     * Forget a member's figure, after a copy changed what it holds. A member
     * already asked for is told to its listeners as loading at once (the Stores
     * section redraws) and read again; one never asked for stays unasked.
     */
    forget: (alias) => {
      const was = asked.has(alias)
      known.delete(alias)
      asked.delete(alias)
      if (!was) return
      for (const fn of [...listeners]) fn(alias, { state: 'loading', stored: null })
      load(alias)
    },
    onChange: (fn) => { listeners.add(fn); return () => listeners.delete(fn) },
  }
}

/** The words for a member's size, or '' while it is still loading. */
export function sizeText(figure) {
  if (figure.state === 'no_snapshot') return 'no snapshot'
  if (figure.state === 'failed') return 'unavailable'
  return figure.state === 'open' && figure.stored !== null ? bytes(figure.stored) : ''
}

/**
 * Fills `wrap` with the switcher. With `store.members` it is a button plus a
 * popover; without, a label. Returns `{ button, popover, open, close }`, or
 * null for the label.
 */
export function mountSwitcher(wrap, { store, members = null, sizes = null, pageHref = () => globalThis.location.href,
  go = (href) => globalThis.location.assign(href) }) {
  const name = storeName(store)
  if (!store.members || !members || !sizes) {
    wrap.replaceChildren(h('span', { class: 'store-switch', id: 'store-label', title: 'The store this page reads' },
      h('span', { class: `dot ${storeKey(store)}` }), h('span', { id: 'store-name' }, name)))
    return null
  }
  const list = members.list
  const shown = store.all
    ? [presence(list.map(m => ({ key: m.key, name: m.alias, held: true }))), h('span', { id: 'store-name' }, 'All stores')]
    : [h('span', { class: `dot ${storeKey(store)}` }), h('span', { id: 'store-name' }, name)]
  const button = h('button', { type: 'button', class: 'store-switch', id: 'store-label', 'aria-haspopup': 'listbox', 'aria-expanded': 'false',
    'aria-controls': 'store-menu', title: 'The store this page reads. Choose another, or all of them.' },
    ...shown, h('span', { class: 'chev', 'aria-hidden': 'true' }, '▾'))
  const popover = h('div', { class: 'store-menu', id: 'store-menu', role: 'listbox', 'aria-label': 'Stores' })
  popover.hidden = true
  wrap.replaceChildren(button, popover)

  let options = []
  let active = -1
  let isOpen = false
  const stops = []

  const sized = new Map() // alias -> the option's size element
  const paint = (alias) => {
    const el = sized.get(alias)
    if (el) el.textContent = sizeText(sizes.get(alias))
  }

  const option = (key, label, href, selected, ...details) => h('button', { type: 'button', role: 'option', tabindex: '-1', 'aria-selected': String(selected),
    'data-store-option': label, onclick: () => { close(); go(href) } }, key, h('span', { class: 'lbl' }, label), ...details)

  function build() {
    sized.clear()
    options = list.map((m) => {
      const size = h('small', { class: 'stored', 'data-store-size': m.alias }, '')
      sized.set(m.alias, size)
      const where = [m.kind, m.kind === 's3' ? m.location : null].filter(Boolean).join(' · ')
      const role = m.writable ? h('small', { class: 'role' }, 'writable')
        : !m.copy_target ? h('small', { class: 'role', 'data-read-only': '' }, cantCopyText(m)) : null
      return option(h('span', { class: `dot ${m.key}` }), m.alias, memberHref(pageHref(), m.alias), !store.all && m.alias === store.member,
        size, where ? h('small', { class: 'where' }, where) : null, role)
    })
    options.push(option(presence(list.map(m => ({ key: m.key, name: m.alias, held: true }))), 'All stores', memberHref(pageHref(), '*'), store.all))
    popover.replaceChildren(...options)
    for (const m of list) paint(m.alias)
  }

  function highlight(i) {
    options.forEach((o, j) => (j === i ? o.classList.add('on') : o.classList.remove('on')))
    active = i
    if (i >= 0) options[i].focus?.()
  }

  function close({ refocus = false } = {}) {
    if (!isOpen) return
    isOpen = false
    popover.hidden = true
    popover.replaceChildren()
    options = []
    active = -1
    button.setAttribute('aria-expanded', 'false')
    for (const stop of stops.splice(0)) stop()
    if (refocus) button.focus?.()
  }

  function open() {
    if (isOpen) return
    isOpen = true
    build()
    popover.hidden = false
    button.setAttribute('aria-expanded', 'true')
    // Each member's snapshot opens now, once; its size lands in its option.
    const stop = sizes.onChange(paint)
    stops.push(stop)
    for (const m of list) sizes.load(m.alias)
    const onClick = (e) => { if (!wrap.contains(e.target)) close() }
    const onKey = (e) => { if (e.key === 'Escape') close({ refocus: true }) }
    document.addEventListener('click', onClick)
    document.addEventListener('keydown', onKey)
    stops.push(() => document.removeEventListener('click', onClick), () => document.removeEventListener('keydown', onKey))
    const selected = options.findIndex(o => o.getAttribute('aria-selected') === 'true')
    highlight(selected >= 0 ? selected : 0)
  }

  const onKeys = (e) => {
    const stop = () => e.preventDefault()
    if (e.key === 'Escape') { close({ refocus: true }); return }
    if (e.key === 'Tab') { close(); return }
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      stop()
      if (!isOpen) { open(); return }
      highlight((active + (e.key === 'ArrowDown' ? 1 : -1) + options.length) % options.length)
    } else if (isOpen && e.key === 'Home') { stop(); highlight(0) }
    else if (isOpen && e.key === 'End') { stop(); highlight(options.length - 1) }
  }
  button.addEventListener('click', () => (isOpen ? close() : open()))
  button.addEventListener('keydown', onKeys)
  popover.addEventListener('keydown', onKeys)
  return { button, popover, open, close }
}
