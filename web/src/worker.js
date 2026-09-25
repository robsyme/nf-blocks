// SQLite in a Worker, reading the snapshot through the read-only VFS.
import sqlite3InitModule from '@sqlite.org/sqlite-wasm'
import { installReadOnlyVfs } from './vfs/httpvfs.js'
import { ChunkedSource, Counter, MemorySource, xhrRange } from './vfs/sources.js'

const VFS = 'nfb-http'
const counter = new Counter()
let sqlite3
let vfs
let db

self.onmessage = async ({ data: message }) => {
  try {
    self.postMessage({ id: message.id, ok: true, result: await handle(message) })
  } catch (e) {
    const cause = vfs?.lastError ? ` (${vfs.lastError.message})` : ''
    self.postMessage({ id: message.id, ok: false, error: `${e?.message ?? e}${cause}` })
  }
}

async function handle(message) {
  switch (message.op) {
    case 'open': {
      if (!sqlite3) {
        sqlite3 = await sqlite3InitModule({ wasmBinary: message.wasm, print: () => {}, printErr: () => {} })
        vfs = installReadOnlyVfs(sqlite3, VFS)
      }
      const source = message.mode === 'whole'
        ? new MemorySource(message.bytes)
        : new ChunkedSource(message.size, xhrRange(message.url, counter), { head: message.head })
      vfs.register('snapshot', source)
      db = new sqlite3.oo1.DB({ filename: 'file:snapshot?immutable=1', flags: 'r', vfs: VFS })
      return { sqlite: sqlite3.version.libVersion }
    }
    case 'query':
      return db.selectObjects(message.sql, message.params).map(row => ({ ...row }))
    case 'stats':
      return counter.reset()
    case 'close':
      db?.close()
      db = null
      return null
    default:
      throw new Error(`unknown worker op ${message.op}`)
  }
}
