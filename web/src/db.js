// The snapshot as the page sees it (spec section 5.2): probe with one ranged
// GET, then read pages through the VFS in a Worker, or, when the server
// ignores Range, download the whole file if it is under the cap.
import { CHUNK_BYTES, DEFAULT_CAP_BYTES } from './config.js'

export class SnapshotError extends Error {
  constructor(code, message) { super(message); this.code = code }
}

export async function probeSnapshot(url, { cap = DEFAULT_CAP_BYTES, fetchFn = fetch } = {}) {
  const abort = new AbortController()
  let res
  try {
    res = await fetchFn(url, { headers: { Range: `bytes=0-${CHUNK_BYTES - 1}` }, signal: abort.signal, cache: 'no-store' })
  } catch (e) {
    throw new SnapshotError('fetch_failed', `could not fetch ${url}: ${e.message}`)
  }
  if (res.status === 206) {
    const range = res.headers.get('Content-Range')
    const total = range && /\/(\d+)$/.exec(range)
    if (!total)
      throw new SnapshotError('cors_headers', `${url} answered 206 but Content-Range is not readable; a bucket's CORS rule must expose Content-Range (DESIGN.md §15)`)
    return { mode: 'range', size: Number(total[1]), head: new Uint8Array(await res.arrayBuffer()) }
  }
  if (res.status === 200) return { mode: 'whole', ...(await readCapped(url, res, cap, abort)) }
  if (res.status === 403 || res.status === 404)
    throw new SnapshotError('no_snapshot', `no Index Snapshot at ${url} (HTTP ${res.status})`)
  throw new SnapshotError('fetch_failed', `${url} answered HTTP ${res.status}`)
}

async function readCapped(url, res, cap, abort) {
  const tooBig = (n) => new SnapshotError('no_range_over_cap',
    `${url} does not support Range requests, and the snapshot (${n}) is larger than the ${cap}-byte cap for downloading it whole. Serve it with Range (nf-blocks:explore does) or rewrite it smaller.`)
  const declared = Number(res.headers.get('Content-Length'))
  if (declared > cap) { abort.abort(); throw tooBig(`${declared} bytes`) }
  const reader = res.body.getReader()
  const parts = []
  let total = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    total += value.length
    if (total > cap) { abort.abort(); await reader.cancel().catch(() => {}); throw tooBig(`more than ${cap} bytes`) }
    parts.push(value)
  }
  const bytes = new Uint8Array(total)
  let at = 0
  for (const p of parts) { bytes.set(p, at); at += p.length }
  return { size: total, bytes }
}

export async function openSnapshot(url, { cap = DEFAULT_CAP_BYTES, wasm, createWorker, fetchFn = fetch } = {}) {
  const probe = await probeSnapshot(url, { cap, fetchFn })
  const worker = createWorker()
  const pending = new Map()
  let next = 0
  worker.onmessage = ({ data }) => {
    const waiting = pending.get(data.id)
    if (!waiting) return
    pending.delete(data.id)
    if (data.ok) waiting.resolve(data.result)
    else waiting.reject(new SnapshotError('query_failed', data.error))
  }
  worker.onerror = (event) => {
    for (const waiting of pending.values()) waiting.reject(new SnapshotError('worker_failed', event.message || 'the snapshot worker failed'))
    pending.clear()
  }
  const call = (message, transfer = []) => new Promise((resolve, reject) => {
    const id = ++next
    pending.set(id, { resolve, reject })
    worker.postMessage({ ...message, id }, transfer)
  })
  const wasmCopy = wasm.slice()
  const transfer = [wasmCopy.buffer]
  if (probe.bytes) transfer.push(probe.bytes.buffer)
  try {
    await call({ op: 'open', url: new URL(url, globalThis.location?.href).href, mode: probe.mode, size: probe.size, head: probe.head, bytes: probe.bytes, wasm: wasmCopy }, transfer)
  } catch (e) {
    worker.terminate()
    throw e
  }
  return {
    mode: probe.mode,
    size: probe.size,
    query: (sql, params = []) => call({ op: 'query', sql, params }),
    stats: () => call({ op: 'stats' }),
    close: async () => { await call({ op: 'close' }); worker.terminate() },
  }
}
