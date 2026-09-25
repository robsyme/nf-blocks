import { readFileSync } from 'node:fs'
import sqlite3InitModule from '@sqlite.org/sqlite-wasm'

let loaded
/** sqlite-wasm in Node, given its wasm bytes the way the page gives them. */
export async function loadSqlite() {
  loaded ??= sqlite3InitModule({
    wasmBinary: readFileSync(new URL('../node_modules/@sqlite.org/sqlite-wasm/dist/sqlite3.wasm', import.meta.url)),
    print: () => {},
    printErr: () => {},
  })
  return loaded
}

/** A database built in memory from SQL statements, exported as file bytes. */
export function makeDb(sqlite3, statements) {
  const db = new sqlite3.oo1.DB(':memory:')
  try {
    db.exec('PRAGMA page_size=4096')
    for (const sql of statements) db.exec(sql)
    return sqlite3.capi.sqlite3_js_db_export(db)
  } finally {
    db.close()
  }
}

/** A fetchRange over bytes in memory that counts what it is asked for. */
export function countingRange(bytes) {
  const calls = []
  const fn = (start, endInclusive) => {
    calls.push([start, endInclusive])
    return bytes.subarray(start, endInclusive + 1)
  }
  fn.calls = calls
  return fn
}
