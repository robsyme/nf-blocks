// web/src/storename.js
// Which store the page reads, in the words and colour the page shows it by:
// the top bar, the Stores section, and every action that names a store
// ("Pin 5.29 MB in roblab", via ctx.store.name). P3 extends storeKey to the
// several stores a block may be held in.
const KEYS = ['a', 'b', 'c']

/** The store's human name: its member alias, else the last part of its URL, else its host; never the whole URL. */
export function storeName(store) {
  if (store.member) return store.member
  try {
    const url = new URL(store.base)
    const last = url.pathname.split('/').filter(Boolean).pop()
    return last ? decodeURIComponent(last) : url.host || 'this store'
  } catch {
    return 'this store'
  }
}

/** A store's colour key (the kit's .dot.a/.b/.c) by `alias`'s place in members.json; the open member by default. */
export function storeKey(store, alias = store.member) {
  const i = store.members?.findIndex(m => m.alias === alias) ?? -1
  return KEYS[Math.max(0, i) % KEYS.length]
}
