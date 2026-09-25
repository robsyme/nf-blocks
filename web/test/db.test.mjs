// openSnapshot's Worker lifecycle: a Worker it creates but never opens a
// snapshot on must not leak (Task 11's controller ruling).
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { openSnapshot, SnapshotError } from '../src/db.js'

const URL_ = 'http://store.test/index/v2.sqlite'

function response(status, body, headers = {}) {
  return new Response(body, { status, headers })
}

/** A fake Worker whose 'open' call always rejects. */
function rejectingWorker() {
  const worker = {
    terminated: false,
    postMessage(msg) {
      queueMicrotask(() => worker.onmessage({ data: { id: msg.id, ok: false, error: 'open failed' } }))
    },
    onmessage: null,
    onerror: null,
    terminate() { worker.terminated = true },
  }
  return worker
}

test('a rejected open terminates the Worker instead of leaking it', async () => {
  const fetchFn = async () => response(200, new Uint8Array(100), { 'Content-Length': '100' })
  const worker = rejectingWorker()
  const createWorker = () => worker
  await assert.rejects(
    openSnapshot(URL_, { cap: 1000, wasm: new Uint8Array(4), createWorker, fetchFn }),
    e => e instanceof SnapshotError && e.code === 'query_failed',
  )
  assert.equal(worker.terminated, true)
})
