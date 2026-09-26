// The items picked for a Selection (spec section 5.6), kept per tab so picks
// survive switching member (decision 15). Storage may be absent or refuse
// every call; the tray then lives only as long as the page.
import { CID } from 'multiformats/cid'

const KEY = 'nf-blocks-tray'

export function safeSessionStorage() {
  try { return globalThis.sessionStorage ?? null } catch { return null }
}

export class Tray {
  constructor(storage = safeSessionStorage()) {
    this.storage = storage
    this.items = new Map()
    try {
      for (const e of JSON.parse(storage?.getItem(KEY) ?? '[]')) this.items.set(e.address, { ...e, via: new Set(e.via) })
    } catch {
      // No storage, or nothing readable in it.
    }
  }

  add({ address, via = [], kind = 'item' }) {
    const seen = this.items.get(address) ?? { address, kind, via: new Set() }
    for (const v of via) seen.via.add(v)
    this.items.set(address, seen)
    this.persist()
  }

  remove(address) { this.items.delete(address); this.persist() }

  clear() { this.items.clear(); this.persist() }

  has(address) { return this.items.has(address) }

  get size() { return this.items.size }

  entries() {
    return [...this.items.values()].map(e => ({ address: e.address, kind: e.kind, via: [...e.via].sort() }))
      .sort((a, b) => (a.address < b.address ? -1 : a.address > b.address ? 1 : 0))
  }

  /** Spec section 7.2: legal, but the explorer does not offer a Selection whose only member is a Selection. */
  onlyOneSelection() { return this.items.size === 1 && [...this.items.values()][0].kind === 'selection' }

  /** Members for a DAG-JSON Selection request; the server normalises again. */
  toMembers() {
    return this.entries().map(e => (e.kind === 'selection'
      ? { selection: CID.parse(e.address) }
      : { item: { address: CID.parse(e.address), via: e.via.map(v => CID.parse(v)) } }))
  }

  persist() {
    try {
      this.storage?.setItem(KEY, JSON.stringify(this.entries()))
    } catch {
      // Storage refused; the tray still works for this page.
    }
  }
}
