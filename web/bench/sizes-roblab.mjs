// Dev script (not shipped): prints client-side sizes for the real roblab store, read-only.
//   node bench/sizes-roblab.mjs [storeDir]
import { readFileSync, readdirSync, existsSync } from 'node:fs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { snapshotDb } from '../test/helpers.mjs'

const store = process.argv[2] ?? '/tmp/cas-exploration/store'
const SELECTION = 'bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce'
const MURDOCK = 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq'

const fetchFn = async (url) => {
  const cid = String(url).split('/').pop()
  const path = `${store}/blocks/${cid.slice(-2)}/${cid}`
  return existsSync(path) ? new Response(readFileSync(path)) : new Response('missing', { status: 404 })
}
const explorer = await Explorer.open({
  base: 'http://h/',
  openDb: async () => snapshotDb(new Uint8Array(readFileSync(`${store}/index/v3.sqlite`))),
  blocks: new BlockFetcher('http://h/', { fetchFn }),
  listFn: async () => ({ names: readdirSync(`${store}/log`), readable: true }),
})

// 3 significant figures, SI (what format.bytes does).
const si = (n) => {
  if (n < 1000) return `${n} B`
  const u = ['KB', 'MB', 'GB', 'TB']
  let v = n / 1000, i = 0
  while (v >= 1000 && i < u.length - 1) { v /= 1000; i++ }
  return `${v < 10 ? v.toFixed(2) : v < 100 ? v.toFixed(1) : Math.round(v)} ${u[i]}`
}
const show = (label, r) => console.log(label.padEnd(44), `${si(r.bytes)} (${r.bytes} B)`, `blocks=${r.blocks}`,
  ...Object.entries(r).filter(([k]) => !['bytes', 'blocks', 'kinds'].includes(k)).map(([k, v]) => `${k}=${v}`))

for (const p of await explorer.pipelines()) {
  const runs = await explorer.runsOfPipeline(p.pipeline, { limit: 1000 })
  for (const r of runs) {
    const id = await explorer.runIdentity(r.completion_cid)
    show(`run ${id.run_name} (${p.pipeline}) ${r.completion_cid.slice(0, 12)}`, await explorer.runSize(r.completion_cid))
  }
  show(`pipeline ${p.pipeline}`, await explorer.pipelineSize(p.pipeline))
}

const run = await explorer.run(MURDOCK)
const pre = run.collections.find(c => c.output === 'preprocessed')
if (pre) {
  show('murdock preprocessed collection', await explorer.collectionSize(pre.cid))
  for (const item of await explorer.allItems(pre.cid)) {
    const v = (await explorer.item(pre.cid, item)).view
    if (JSON.stringify(v).includes('WT_REP1')) { const s = await explorer.itemSize(item); show(`item WT_REP1 ${item.slice(0, 12)}`, s); console.log('  kinds', JSON.stringify(s.kinds)); break }
  }
}
show('wigshop selection', await explorer.selectionSize(SELECTION))
