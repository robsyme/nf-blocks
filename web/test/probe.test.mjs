import { test } from 'node:test'
import assert from 'node:assert/strict'
import { probeSnapshot, SnapshotError } from '../src/db.js'

const URL_ = 'http://store.test/index/v2.sqlite'

function response(status, body, headers = {}) {
  return new Response(body, { status, headers })
}

/** A body that yields `total` bytes in 64 KiB pieces and records how much was read. */
function streamed(total) {
  let sent = 0
  const stream = new ReadableStream({
    pull(controller) {
      if (sent >= total) return controller.close()
      const n = Math.min(65536, total - sent)
      sent += n
      controller.enqueue(new Uint8Array(n))
    },
  })
  stream.sent = () => sent
  return stream
}

test('206 means range mode, with the size from Content-Range', async () => {
  const head = new Uint8Array(4096).fill(1)
  const fetchFn = async (url, init) => {
    assert.equal(init.headers.Range, 'bytes=0-4095')
    return response(206, head, { 'Content-Range': 'bytes 0-4095/580000000' })
  }
  const probe = await probeSnapshot(URL_, { cap: 1000, fetchFn })
  assert.equal(probe.mode, 'range')
  assert.equal(probe.size, 580000000)
  assert.equal(probe.head.length, 4096)
})

test('a 206 whose Content-Range is hidden by CORS is a configuration error', async () => {
  const fetchFn = async () => response(206, new Uint8Array(4096))
  await assert.rejects(probeSnapshot(URL_, { cap: 1000, fetchFn }), e => e instanceof SnapshotError && e.code === 'cors_headers')
})

test('200 under the cap downloads the whole file', async () => {
  const fetchFn = async () => response(200, new Uint8Array(5000), { 'Content-Length': '5000' })
  const probe = await probeSnapshot(URL_, { cap: 10000, fetchFn })
  assert.equal(probe.mode, 'whole')
  assert.equal(probe.bytes.length, 5000)
})

test('200 over the cap by Content-Length stops before reading the body', async () => {
  const body = streamed(20_000_000)
  const fetchFn = async () => response(200, body, { 'Content-Length': '20000000' })
  await assert.rejects(probeSnapshot(URL_, { cap: 1_000_000, fetchFn }),
    e => e.code === 'no_range_over_cap' && /does not support Range/.test(e.message))
  assert.ok(body.sent() <= 65536 * 2, `read ${body.sent()} bytes`)
})

test('200 with no Content-Length streams and stops at the cap (Review Focus 1)', async () => {
  const body = streamed(20_000_000)
  const fetchFn = async () => response(200, body)
  await assert.rejects(probeSnapshot(URL_, { cap: 1_000_000, fetchFn }), e => e.code === 'no_range_over_cap')
  assert.ok(body.sent() < 2_000_000, `read ${body.sent()} bytes`)
})

test('404 and 403 mean there is no snapshot here', async () => {
  for (const status of [403, 404]) {
    const fetchFn = async () => response(status, 'nope')
    await assert.rejects(probeSnapshot(URL_, { cap: 1, fetchFn }), e => e.code === 'no_snapshot')
  }
})

test('a network failure is fetch_failed, not a crash', async () => {
  const fetchFn = async () => { throw new TypeError('Failed to fetch') }
  await assert.rejects(probeSnapshot(URL_, { cap: 1, fetchFn }), e => e.code === 'fetch_failed')
})
