// The items picked for a Selection (spec section 5.6), kept per tab so picks
// survive switching member (decision 15). Storage may be absent or refuse
// every call; the tray then lives only as long as the page, and the page says
// so (Tray.saved, trayNote).
import { CID } from 'multiformats/cid'

const KEY = 'nf-blocks-tray'

export const UNSAVED_NOTE = 'The tray could not be saved in this browser, so it will be lost on reload.'

/** The note the page shows while the tray's last write to storage failed, else ''. */
export const trayNote = (tray) => (tray.saved ? '' : UNSAVED_NOTE)

export function safeSessionStorage() {
  try { return globalThis.sessionStorage ?? null } catch { return null }
}

export class Tray {
  constructor(storage = safeSessionStorage()) {
    this.storage = storage
    this.items = new Map()
    /** False while the last write to storage failed (a full quota, or no storage at all). */
    this.saved = true
    try {
      for (const e of JSON.parse(storage?.getItem(KEY) ?? '[]')) this.items.set(e.address, { ...e, via: new Set(e.via) })
    } catch {
      // No storage, or nothing readable in it.
    }
  }

  add(pick) { this.addMany([pick]) }

  /** Many picks with one write to storage: "Add all" may add a whole collection (DESIGN.md §16 milestone 3 decision 12). */
  addMany(picks) {
    for (const { address, via = [], kind = 'item' } of picks) {
      const seen = this.items.get(address) ?? { address, kind, via: new Set() }
      for (const v of via) seen.via.add(v)
      this.items.set(address, seen)
    }
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

  /** Writes the tray to storage; false, and `saved` false, when the browser refused (the tray still works for this page). */
  persist() {
    try {
      if (!this.storage) throw new Error('no storage')
      this.storage.setItem(KEY, JSON.stringify(this.entries()))
      this.saved = true
    } catch {
      this.saved = false
    }
    return this.saved
  }
}
