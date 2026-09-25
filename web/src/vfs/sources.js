// Byte sources the read-only VFS reads through (spec section 5.2). Every read
// is synchronous: the VFS runs inside SQLite's C call stack and cannot await,
// which is why the page reads the snapshot from a Worker with synchronous XHR.
import { CHUNK_BYTES } from '../config.js'
import { SnapshotError } from '../db.js'

/** Requests and bytes a source has fetched since the last reset. */
export class Counter {
  constructor() { this.requests = 0; this.bytes = 0 }
  reset() {
    const seen = { requests: this.requests, bytes: this.bytes }
    this.requests = 0
    this.bytes = 0
    return seen
  }
}

/** A whole snapshot already in memory: the fallback for a server without Range. */
export class MemorySource {
  constructor(bytes) { this.bytes = bytes; this.size = bytes.length }
  read(offset, length) { return this.bytes.subarray(offset, Math.min(offset + length, this.size)) }
}

/**
 * A remote file read in fixed chunks, each fetched once and kept (least
 * recently used first out). Contiguous missing chunks go in one request.
 * `fetchRange(start, endInclusive)` must return exactly those bytes.
 */
export class ChunkedSource {
  constructor(size, fetchRange, { chunk = CHUNK_BYTES, maxChunks = 16384, head } = {}) {
    this.size = size
    this.fetchRange = fetchRange
    this.chunk = chunk
    this.maxChunks = maxChunks
    this.chunks = new Map()
    if (head && (head.length >= Math.min(chunk, size))) this.put(0, head.subarray(0, Math.min(chunk, size)))
  }

  cachedChunks() { return this.chunks.size }

  read(offset, length) {
    const end = Math.min(offset + length, this.size)
    if (offset >= end) return new Uint8Array(0)
    const first = Math.floor(offset / this.chunk)
    const last = Math.floor((end - 1) / this.chunk)
    const parts = this.fill(first, last)
    const out = new Uint8Array(end - offset)
    for (let c = first; c <= last; c++) {
      const bytes = parts.get(c)
      const chunkStart = c * this.chunk
      const from = Math.max(offset, chunkStart)
      const to = Math.min(end, chunkStart + bytes.length)
      out.set(bytes.subarray(from - chunkStart, to - chunkStart), from - offset)
    }
    return out
  }

  /** The chunks first..last, fetching the missing ones in contiguous runs. */
  fill(first, last) {
    const parts = new Map()
    let c = first
    while (c <= last) {
      const cached = this.chunks.get(c)
      if (cached) {
        this.chunks.delete(c)
        this.chunks.set(c, cached)
        parts.set(c, cached)
        c++
        continue
      }
      let run = c
      while (run + 1 <= last && !this.chunks.has(run + 1)) run++
      const start = c * this.chunk
      const endInclusive = Math.min((run + 1) * this.chunk, this.size) - 1
      const bytes = this.fetchRange(start, endInclusive)
      if (bytes.length !== endInclusive - start + 1)
        throw new Error(`short range read: asked for bytes ${start}-${endInclusive}, got ${bytes.length}`)
      for (let k = c; k <= run; k++) {
        const piece = bytes.subarray((k - c) * this.chunk, Math.min((k - c + 1) * this.chunk, bytes.length))
        this.put(k, piece)
        parts.set(k, piece)
      }
      c = run + 1
    }
    return parts
  }

  put(index, bytes) {
    this.chunks.set(index, bytes)
    while (this.chunks.size > this.maxChunks) this.chunks.delete(this.chunks.keys().next().value)
  }
}

/**
 * A synchronous ranged GET. Only a Worker may do this; a window cannot set
 * responseType on sync XHR. Every 206 is checked against the version the
 * probe saw (its ETag, else Last-Modified, and its total size): a snapshot is
 * rewritten in place by every run and by explore, and pages of two files
 * would answer wrongly without any error (spec section 4). The check reads
 * the headers the range already carries, so it costs no request.
 */
export function xhrRange(url, counter, { tag = null, size = null, newXhr = () => new XMLHttpRequest() } = {}) {
  let changed = null
  return (start, endInclusive) => {
    if (changed) throw changed
    const xhr = newXhr()
    xhr.open('GET', url, false)
    xhr.responseType = 'arraybuffer'
    xhr.setRequestHeader('Range', `bytes=${start}-${endInclusive}`)
    xhr.send()
    if (xhr.status !== 206) throw new Error(`ranged read of ${url} answered ${xhr.status}, not 206`)
    const seenTag = xhr.getResponseHeader('ETag') ?? xhr.getResponseHeader('Last-Modified')
    const total = /\/(\d+)$/.exec(xhr.getResponseHeader('Content-Range') ?? '')
    if ((tag !== null && seenTag !== tag) || (size !== null && total && Number(total[1]) !== size)) {
      changed = new SnapshotError('snapshot_changed',
        `the snapshot was rewritten since this page opened it (${tag ?? size} is now ${seenTag ?? total?.[1]}); reload the page`)
      throw changed
    }
    const bytes = new Uint8Array(xhr.response)
    counter.requests++
    counter.bytes += bytes.length
    return bytes
  }
}
