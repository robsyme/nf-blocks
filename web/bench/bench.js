// The spike's harness: runs the three load-bearing queries through
// openSnapshot, calling window.__mark(step) after each so the driver can read
// its own network counters. SQL copied from Index.groovy on 2026-09-25; Task 4
// moves the page's SQL into src/queries.json.
import { openSnapshot } from '../src/db.js'
import { createInlineWorker, inlineWasm } from '../src/inline.js'

const SQL = {
  producersOf: 'SELECT content_cid, item_cid, collection_cid, completion_cid, filename FROM producer WHERE content_cid = ?',
  latestSuccessfulRun: "SELECT completion_cid FROM run WHERE pipeline = ? AND status = 'succeeded' AND possibly_incomplete = 0 ORDER BY finished_at DESC, completion_cid ASC LIMIT 1",
  itemsWhere: 'SELECT ci.item_cid FROM collection_item ci JOIN collection c ON c.collection_cid = ci.collection_cid WHERE c.completion_cid = ? AND c.output_name = ? AND EXISTS (SELECT 1 FROM item_attr a WHERE a.item_cid = ci.item_cid AND a.truncated = 0 AND a.path = ? AND a.type = ? AND a.value = ?) ORDER BY ci.item_cid',
  watermark: "SELECT value FROM meta WHERE key = 'store_log_watermark'",
}

const mark = (step, extra) => (window.__mark ? window.__mark(step, extra ?? null) : Promise.resolve())

window.bench = async ({ url, params, params2, cap }) => {
  const out = { steps: [] }
  const db = await openSnapshot(url, { cap, wasm: inlineWasm(), createWorker: createInlineWorker })
  out.mode = db.mode
  out.sqlite = (await db.query('SELECT sqlite_version() AS v'))[0].v
  await db.query(SQL.watermark)
  await mark('open')
  for (const [phase, set] of [['cold', params], ['warm', params], ['other', params2]]) {
    for (const name of ['producersOf', 'latestSuccessfulRun', 'itemsWhere']) {
      const t = performance.now()
      const rows = await db.query(SQL[name], set[name])
      const step = { step: `${phase}:${name}`, ms: Math.round(performance.now() - t), rows: rows.length, first: rows[0] ? Object.values(rows[0])[0] : null }
      out.steps.push(step)
      await mark(step.step, step)
    }
  }
  await db.close()
  document.getElementById('out').textContent = JSON.stringify(out, null, 2)
  return out
}
document.getElementById('out').textContent = 'ready'
