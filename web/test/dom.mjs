// A DOM just large enough for html.js's `h` and the row code in views.js to
// run under node:test: elements with attributes, dataset, classList,
// children, listeners and querySelectorAll over simple compound selectors
// (`tag`, `.class`, `[attr]`, `[attr=value]`), and listeners on `document`
// that a test fires with `document.dispatch`. Not a browser: no layout (no
// getBoundingClientRect), no connected tree (isConnected is always false),
// no event bubbling.

class FakeNode {}

class FakeText extends FakeNode {
  constructor(text) { super(); this.data = String(text) }
  get textContent() { return this.data }
}

const camel = (name) => name.replace(/-([a-z])/g, (_, c) => c.toUpperCase())
const kebab = (name) => name.replace(/[A-Z]/g, c => `-${c.toLowerCase()}`)

class FakeElement extends FakeNode {
  constructor(tag) {
    super()
    this.tagName = tag.toUpperCase()
    this.attributes = new Map()
    this.childNodes = []
    this.listeners = new Map()
    this.checked = false
    this.style = {}
    const el = this
    this.dataset = new Proxy({}, {
      get: (_, k) => el.attributes.get(`data-${kebab(String(k))}`),
      set: (_, k, v) => { el.attributes.set(`data-${kebab(String(k))}`, String(v)); return true },
      deleteProperty: (_, k) => { el.attributes.delete(`data-${kebab(String(k))}`); return true },
    })
    this.classList = {
      add: (c) => { const s = new Set(el.classes()); s.add(c); el.setAttribute('class', [...s].join(' ')) },
      remove: (c) => { el.setAttribute('class', el.classes().filter(x => x !== c).join(' ')) },
      contains: (c) => el.classes().includes(c),
    }
  }

  get isConnected() { return false }
  classes() { return (this.attributes.get('class') ?? '').split(/\s+/).filter(Boolean) }
  setAttribute(k, v) { this.attributes.set(k, String(v)) }
  removeAttribute(k) { this.attributes.delete(k) }
  getAttribute(k) { return this.attributes.get(k) ?? null }
  hasAttribute(k) { return this.attributes.has(k) }
  get disabled() { return this.attributes.has('disabled') }
  set disabled(v) { if (v) this.attributes.set('disabled', ''); else this.attributes.delete('disabled') }
  get children() { return this.childNodes.filter(n => n instanceof FakeElement) }

  append(...nodes) {
    for (const n of nodes) this.childNodes.push(n instanceof FakeNode ? n : new FakeText(n))
  }
  prepend(...nodes) { this.childNodes.unshift(...nodes.map(n => (n instanceof FakeNode ? n : new FakeText(n)))) }
  replaceChildren(...nodes) { this.childNodes = []; this.append(...nodes) }

  get textContent() { return this.childNodes.map(n => n.textContent).join('') }
  set textContent(v) { this.childNodes = [new FakeText(v)] }
  /** Markup is not parsed: the setter keeps the string (the Seal's SVG) and empties the children, as a browser would replace them. */
  get innerHTML() { return this.html ?? '' }
  set innerHTML(v) { this.html = String(v); this.childNodes = [] }

  addEventListener(type, fn) { this.listeners.set(type, [...(this.listeners.get(type) ?? []), fn]) }
  removeEventListener(type, fn) { this.listeners.set(type, (this.listeners.get(type) ?? []).filter(f => f !== fn)) }
  /** Whether `node` is this element or inside it. */
  contains(node) { return node === this || this.childNodes.some(c => c instanceof FakeElement && c.contains(node)) }
  /** Runs this element's own listeners for `type` and returns what they return (a Promise for an async handler). */
  fire(type) { return Promise.all((this.listeners.get(type) ?? []).map(fn => fn({ currentTarget: this, target: this, preventDefault() {} }))) }
  click() { return this.fire('click') }
  /** A keydown for `key` (e.g. 'ArrowDown') to this element's own listeners; true when a listener called preventDefault. */
  press(key) {
    let prevented = false
    for (const fn of this.listeners.get('keydown') ?? []) fn({ key, currentTarget: this, target: this, type: 'keydown', preventDefault() { prevented = true } })
    return prevented
  }
  /** A real Event (e.g. `new Event('input')`) dispatched synchronously to this element's own listeners only. */
  dispatchEvent(event) {
    for (const fn of this.listeners.get(event.type) ?? []) fn({ currentTarget: this, target: this, type: event.type, preventDefault() {} })
    return true
  }

  matches(selector) {
    const parts = selector.match(/^[a-z][a-z0-9]*|#[\w-]+|\.[\w-]+|\[[^\]]+\]/gi) ?? []
    return parts.every((p) => {
      if (p.startsWith('#')) return this.attributes.get('id') === p.slice(1)
      if (p.startsWith('.')) return this.classes().includes(p.slice(1))
      if (p.startsWith('[')) {
        const [, k, v] = p.match(/^\[([\w-]+)(?:=["']?([^"'\]]*)["']?)?\]$/)
        return v === undefined ? this.attributes.has(k) : this.attributes.get(k) === v
      }
      return this.tagName === p.toUpperCase()
    })
  }

  querySelectorAll(selector) {
    const out = []
    const walk = (el) => {
      for (const c of el.children) {
        if (c.matches(selector)) out.push(c)
        walk(c)
      }
    }
    walk(this)
    return out
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null }
}

/**
 * Installs the fake `document` and `Node` on globalThis. `document.dispatch(type,
 * fields)` runs the document's listeners for `type` with an event carrying
 * `fields` (e.g. `{ key: 'Escape' }` or `{ target: el }`);
 * `document.listenerCount(type)` says how many there are.
 */
export function installDom() {
  globalThis.Node = FakeNode
  const listeners = new Map()
  globalThis.document = {
    createElement: (tag) => new FakeElement(tag),
    addEventListener(type, fn) { listeners.set(type, [...(listeners.get(type) ?? []), fn]) },
    removeEventListener(type, fn) { listeners.set(type, (listeners.get(type) ?? []).filter(f => f !== fn)) },
    dispatch(type, fields = {}) {
      for (const fn of [...(listeners.get(type) ?? [])]) fn({ type, target: globalThis.document, preventDefault() {}, ...fields })
    },
    listenerCount: (type) => (listeners.get(type) ?? []).length,
  }
}

/**
 * The text nodes under `el` that read "null" or "undefined". A real DOM's
 * append, prepend and replaceChildren write a null child as the text "null",
 * and so does this shim; this finds them, since a page shows them as words.
 */
export function strayTexts(el) {
  const out = []
  const walk = (n) => {
    for (const c of n.childNodes ?? []) {
      if (c instanceof FakeText) { if (c.data === 'null' || c.data === 'undefined') out.push(c.data) } else walk(c)
    }
  }
  walk(el)
  return out
}

/** Resolves after watchRows's frame (setTimeout 16 ms without requestAnimationFrame) and any fetch before it. */
export const frame = () => new Promise(resolve => setTimeout(resolve, 40))
