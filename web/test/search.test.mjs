// web/test/search.test.mjs
// Search v1 (P1a Task 4): sample IDs through item_attr's (path, type, value)
// index, addresses through each table's primary key, both as ranges, so no
// character in the query is a wildcard. Run against a snapshot built from the
// index schema, with real OutputItem blocks for the labels.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { CID } from 'multiformats/cid'
import { Explorer, isAddressQuery, prefixRange } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { promotedText, sampleGroups, search, searchBox, slashFocuses } from '../src/views/search.js'
import { block, blockFetch, rawCid } from './fixture.mjs'
import { loadSqlite, snapshotDb } from './helpers.mjs'
import SQL from '../src/queries.json' with { type: 'json' }
import { frame, installDom } from './dom.mjs'

const fake = (tag) => block({ tag }).cid.toString()
const leaf = (name, address) => ({ kind: 'Leaf', name, address, size: 10, provider: 'head-node', reason: null })

/**
 * Four rnaseq-shaped runs. R1 has WT_REP1 in two outputs (in `aligned` at
 * both `id` and `meta.id`) and WT_REP2; R2 and R3 have WT_REP1 once; RD, the
 * newest, has WT_REP1 and a current delete Claim. R2 also holds sample IDs
 * that start with `%`, `_` and an emoji. Selection S1 holds R1's aligned item.
 */
async function open() {
  const blocks = new Map()
  const putItem = (value) => { const b = block({ kind: 'OutputItem', schema: 1, value }); blocks.set(b.cid.toString(), b.bytes); return b.cid.toString() }
  const ids = {
    R1: fake('R1'), R2: fake('R2'), R3: fake('R3'), RD: fake('RD'),
    S1: fake('S1'), name: fake('name'), del: fake('del'),
    file: rawCid('WT_REP1.bam in R1').toString(),
  }
  const runs = [
    ['R1', 'distraught_murdock', 'succeeded', '2026-10-24T13:29:00.000Z'],
    ['R2', 'sad_yonath', 'succeeded', '2026-10-23T12:00:00.000Z'],
    ['R3', 'sharp_magritte', 'failed', '2026-10-23T08:00:00.000Z'],
    ['RD', 'gone_run', 'succeeded', '2026-10-25T09:00:00.000Z'],
  ]
  const items = []
  const item = (run, output, attrs, value) => {
    const cid = putItem(value)
    items.push({ run, output, cid, attrs })
    return cid
  }
  ids.r1pre1 = item('R1', 'preprocessed', [['id', 'string', 'WT_REP1'], ['genome', 'string', 'WT_genome']],
    [{ id: 'WT_REP1', run: 'R1', num_trimmed_reads: 99566 }, leaf('WT_REP1.fq.gz', rawCid('fq-1'))])
  ids.r1pre2 = item('R1', 'preprocessed', [['id', 'string', 'WT_REP2']], [{ id: 'WT_REP2', run: 'R1' }, leaf('WT_REP2.fq.gz', rawCid('fq-2'))])
  ids.r1aln = item('R1', 'aligned', [['id', 'string', 'WT_REP1'], ['meta.id', 'string', 'WT_REP1']],
    { id: 'WT_REP1', meta: { id: 'WT_REP1' }, percent_mapped: 89.04, bam: leaf('WT_REP1.bam', CID.parse(ids.file)) })
  ids.r2pre = item('R2', 'preprocessed', [['id', 'string', 'WT_REP1']], [{ id: 'WT_REP1', run: 'R2', num_trimmed_reads: 1 }])
  blocks.delete(ids.r2pre)     // this member lacks R2's item block: its promoted values cannot be read
  ids.pct = item('R2', 'preprocessed', [['id', 'string', '%weird']], [{ id: '%weird' }])
  ids.under = item('R2', 'preprocessed', [['sample', 'string', '_under']], [{ sample: '_under' }])
  ids.emoji = item('R2', 'preprocessed', [['id', 'string', 'WT_\u{1F600}']], [{ id: 'WT_\u{1F600}' }])
  ids.long = item('R2', 'preprocessed', [['id', 'string', 'WT_digest', 1]], [{ id: 'WT_long' }])
  ids.r3pre = item('R3', 'preprocessed', [['id', 'string', 'WT_REP1']], [{ id: 'WT_REP1', run: 'R3', num_trimmed_reads: 50000 }])
  ids.rdpre = item('RD', 'preprocessed', [['id', 'string', 'WT_REP1']], [{ id: 'WT_REP1', run: 'RD' }])

  const sqlite3 = await loadSqlite()
  const db = new sqlite3.oo1.DB(':memory:')
  let bytes
  try {
    db.exec('PRAGMA page_size=4096')
    db.exec(readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'))
    const run = (sql, bind) => db.exec({ sql, bind })
    for (const [key, name, status, finished] of runs)
      run('INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES (?, ?, ?, ?, ?, ?)',
        [ids[key], 'nf-core/rnaseq', name, status, status === 'succeeded' ? 0 : 1, finished])
    const colls = new Map()
    for (const it of items) {
      const k = `${it.run}:${it.output}`
      if (!colls.has(k)) {
        colls.set(k, fake(k))
        run("INSERT INTO collection VALUES (?, 'output', ?, ?, 'test')", [colls.get(k), ids[it.run], it.output])
      }
      run('INSERT OR IGNORE INTO item VALUES (?)', [it.cid])
      run('INSERT INTO collection_item (collection_cid, item_cid) VALUES (?, ?)', [colls.get(k), it.cid])
      for (const [path, type, value, truncated = 0] of it.attrs) run('INSERT INTO item_attr VALUES (?, ?, ?, ?, ?)', [it.cid, path, type, value, truncated])
    }
    ids.r1aligned = colls.get('R1:aligned')
    ids.r1preprocessed = colls.get('R1:preprocessed')
    run('INSERT INTO producer VALUES (?, ?, ?, ?, ?)', [ids.file, ids.r1aln, ids.r1aligned, ids.R1, 'WT_REP1.bam'])
    run("INSERT INTO collection (collection_cid, kind, asserted_by) VALUES (?, 'selection', 'test')", [ids.S1])
    run('INSERT INTO collection_item VALUES (?, ?, ?)', [ids.S1, ids.r1aln, ids.r1aligned])
    run("INSERT INTO claim VALUES (?, ?, 'set', 'name', 'my picks', '2026-10-24T14:00:00.000Z', 'test')", [ids.name, ids.S1])
    run("INSERT INTO claim VALUES (?, ?, 'delete', NULL, NULL, '2026-10-25T10:00:00.000Z', 'test')", [ids.del, ids.RD])
    bytes = sqlite3.capi.sqlite3_js_db_export(db)
  } finally {
    db.close()
  }
  const ex = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(blocks) }),
    listFn: async () => ({ names: [], readable: true }),
  })
  return { ex, ids }
}

const ctx = (extra = {}) => ({ store: { name: 'roblab', key: 'a' }, ...extra })
const textOf = (el) => el.textContent.replace(/\s+/g, ' ').trim()
/** A table row's cells, ` | ` between them. */
const cells = (tr) => tr.children.map(td => textOf(td)).join(' | ')

test('a prefix range runs from the prefix to the prefix and U+10FFFF', () => {
  assert.deepEqual(prefixRange('WT_'), ['WT_', 'WT_\u{10FFFF}'])
})

test('prefix WT_ finds WT_REP1 and WT_REP2, one row per item, whatever follows the prefix', async () => {
  const { ex, ids } = await open()
  const rows = await ex.searchSamples('WT_')
  const got = rows.map(r => [r.item_cid, r.sample]).sort()
  const want = [[ids.r1pre1, 'WT_REP1'], [ids.r1pre2, 'WT_REP2'], [ids.r1aln, 'WT_REP1'], [ids.r2pre, 'WT_REP1'],
    [ids.emoji, 'WT_\u{1F600}'], [ids.r3pre, 'WT_REP1']].sort()
  assert.deepEqual(got, want, 'WT_genome is at a path that is no sample ID, and a truncated value is a digest')
  const r1 = rows.find(r => r.item_cid === ids.r1aln)
  assert.equal(r1.path, 'id', 'the item with id and meta.id is one result, under the path SAMPLE_PATHS names first')
  assert.deepEqual(r1.paths, ['id', 'meta.id'], 'every matched path is kept')
  assert.deepEqual(Object.keys(r1).sort(),
    ['collection_cid', 'completion_cid', 'finished_at', 'item_cid', 'output_name', 'path', 'paths', 'pipeline', 'possibly_incomplete', 'run_name', 'sample', 'status'])
  assert.equal(rows[0].run_name, 'distraught_murdock', 'latest visible run first')
})

test('the two sample queries walk item_attr through its index: distinct values in the range, then the rows of one value', async () => {
  const sqlite3 = await loadSqlite()
  const db = new sqlite3.oo1.DB(':memory:')
  try {
    db.exec(readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'))
    const values = db.selectObjects(`EXPLAIN QUERY PLAN ${SQL.searchSampleValues}`, ['["id"]', 'a', 'b', 20]).map(r => r.detail)
    assert.ok(values[0].startsWith('SEARCH item_attr USING COVERING INDEX item_attr_path_type_value (path=? AND type=? AND value>? AND value<?)'),
      `the index alone answers, with no table seek per row: ${values.join(' | ')}`)
    assert.ok(!values.some(d => d.includes('TEMP B-TREE FOR ORDER BY')), `DISTINCT streams, so LIMIT stops the walk: ${values.join(' | ')}`)
    const rows = db.selectObjects(`EXPLAIN QUERY PLAN ${SQL.searchSampleRows}`, ['["id"]', 'a', 30]).map(r => r.detail)
    assert.ok(rows[0].startsWith('SEARCH a USING INDEX item_attr_path_type_value (path=? AND type=? AND value=?)'), rows.join(' | '))
  } finally {
    db.close()
  }
  const { ex } = await open()
  const rows = await ex.searchSamples('WT_')
  const finished = rows.map(r => r.finished_at)
  assert.deepEqual(finished, [...finished].sort().reverse())
  const few = await ex.searchSamples('WT_', { limit: 2 })
  assert.equal(few.more, true)
  assert.ok(few.length <= 2)
  const one = await ex.searchSamples('WT_', { values: 1 })
  assert.equal(one.more, true, 'more distinct values than asked for')
  assert.deepEqual([...new Set(one.map(r => r.sample))], ['WT_REP1'])
})

/**
 * Three runs of one sample, 20 items each, the oldest run's rows written to
 * the index first, so an index walk meets them first. More samples, one item
 * each: `newest` in the newest run (WT_REP1x and WT_REP0 by default), `oldest`
 * in the oldest.
 */
async function crowded({ newest = ['WT_REP1x', 'WT_REP0'], oldest = [] } = {}) {
  const sqlite3 = await loadSqlite()
  const db = new sqlite3.oo1.DB(':memory:')
  const runs = [['old_run', '2026-10-01T00:00:00.000Z'], ['mid_run', '2026-10-10T00:00:00.000Z'], ['new_run', '2026-10-20T00:00:00.000Z']]
  let bytes
  try {
    db.exec('PRAGMA page_size=4096')
    db.exec(readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'))
    const run = (sql, bind) => db.exec({ sql, bind })
    const item = (coll, sample) => {
      const cid = fake(`${coll} ${sample} ${Math.random()}`)
      run('INSERT INTO item VALUES (?)', [cid])
      run('INSERT INTO collection_item (collection_cid, item_cid) VALUES (?, ?)', [coll, cid])
      run("INSERT INTO item_attr VALUES (?, 'id', 'string', ?, 0)", [cid, sample])
    }
    for (const [name, finished] of runs) {
      const completion = fake(name)
      run("INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES (?, 'nf-core/rnaseq', ?, 'succeeded', 0, ?)",
        [completion, name, finished])
      const coll = fake(`${name}:out`)
      run("INSERT INTO collection VALUES (?, 'output', ?, 'out', 'test')", [coll, completion])
      for (let i = 0; i < 20; i++) item(coll, 'WT_REP1')
      for (const sample of name === 'new_run' ? newest : name === 'old_run' ? oldest : []) item(coll, sample)
    }
    bytes = sqlite3.capi.sqlite3_js_db_export(db)
  } finally {
    db.close()
  }
  return Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(new Map()) }),
    listFn: async () => ({ names: [], readable: true }),
  })
}

test('with more rows than the limit, the newest run still comes first, whatever order the index holds them in', async () => {
  const ex = await crowded()
  const rows = await ex.searchSamples('WT_REP1', { limit: 5 })
  assert.equal(rows.more, true)
  assert.deepEqual(rows.map(r => [r.run_name, r.sample]), Array(5).fill(['new_run', 'WT_REP1']), 'the exact match has the whole budget')
  const all = await ex.searchSamples('WT_REP1')
  assert.equal(all.more, false)
  assert.equal(all[0].sample, 'WT_REP1', 'the exact match comes first')
  assert.deepEqual([...new Set(all.map(r => r.run_name))], ['new_run', 'mid_run', 'old_run'])
  assert.equal(all.length, 61)
})

test('the typeahead shares its rows among its sample IDs, so one common ID does not crowd out the others', async () => {
  installDom()
  const b = box(await crowded({ newest: [], oldest: ['WT_REP2'] }))
  b.type('WT_')
  await settle()
  const labels = b.menu.querySelector('[data-typeahead-group="samples"]').querySelectorAll('a[data-typeahead-entry]')
    .map(a => textOf(a.querySelector('.lbl')))
  assert.ok(labels.includes('WT_REP1') && labels.includes('WT_REP2'), labels.join(', '))
})

/**
 * Makes `ex` answer searchSampleRows as SQL is free to: in the query's ORDER
 * BY, with rows that tie on every ORDER BY column in a different order on each
 * call for the same sample ID (reversed on every other one), then LIMIT.
 */
function loosenTies(ex) {
  const query = ex.db.query.bind(ex.db)
  const keys = /ORDER BY (.*) LIMIT \?$/.exec(SQL.searchSampleRows)[1].split(',').map(k => k.trim())
    .map(k => ({ column: k.replace(/ DESC$/, '').replace(/^\w+\./, ''), desc: k.endsWith(' DESC') }))
  const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0)
  const calls = new Map()
  ex.db.query = async (sql, params) => {
    if (sql !== SQL.searchSampleRows) return query(sql, params)
    const all = await query(sql, [...params.slice(0, -1), 1_000_000])
    const n = calls.get(params[1]) ?? 0
    calls.set(params[1], n + 1)
    const flip = n % 2 === 1
    const order = all.map((r, i) => ({ r, i: flip ? -i : i }))
    order.sort((x, y) => keys.reduce((n, k) => n || (k.desc ? -1 : 1) * cmp(x.r[k.column] ?? '', y.r[k.column] ?? ''), 0) || x.i - y.i)
    return order.slice(0, params.at(-1)).map(o => o.r)
  }
  return ex
}

test('rows that tie on run read the same way on every call, so the overflow read skips and repeats nothing', async () => {
  const ex = loosenTies(await crowded({ newest: [], oldest: ['WT_REP2'] }))
  const rows = await ex.searchSamples('WT_', { limit: 30 })
  const items = rows.map(r => r.item_cid)
  assert.equal(new Set(items).size, items.length, 'no item twice')
  assert.equal(rows.length, 30, 'nothing skipped: the budget is spent')
  for (const r of rows) assert.deepEqual(r.paths, [...new Set(r.paths)], 'no path twice')
  assert.equal(rows.filter(r => r.run_name === 'new_run').length, 20, 'every row of the newest run, each once')
})

test('a shared budget is filled newest first once each ID has had its share', async () => {
  const ex = await crowded({ newest: [], oldest: ['WT_REP2'] })
  const rows = await ex.searchSamples('WT_', { limit: 30 })
  assert.equal(rows.length, 30)
  assert.equal(rows.more, true)
  assert.equal(rows.filter(r => r.sample === 'WT_REP2').length, 1)
  assert.deepEqual([...new Set(rows.filter(r => r.sample === 'WT_REP1').map(r => r.run_name))], ['new_run', 'mid_run'])
})

test('an ID equal to the query reads first with the whole budget; the other IDs share what it leaves', async () => {
  const ex = await crowded()
  const rows = await ex.searchSamples('WT_REP1', { limit: 30 })
  assert.deepEqual([...new Set(rows.map(r => r.sample))], ['WT_REP1'])
  assert.equal(rows.length, 30)
  assert.equal(rows.more, true)
  assert.deepEqual(rows.map(r => r.run_name), [...Array(20).fill('new_run'), ...Array(10).fill('mid_run')])
  const roomy = await ex.searchSamples('WT_REP1', { limit: 70 })
  assert.equal(roomy.filter(r => r.sample === 'WT_REP1').length, 60, 'every WT_REP1 row when they fit')
  assert.equal(roomy.filter(r => r.sample === 'WT_REP1x').length, 1)
  assert.equal(roomy.more, false)
  // No ID equals WT_: each has its share, so WT_REP0 and WT_REP1x are listed beside WT_REP1.
  const shared = await ex.searchSamples('WT_', { limit: 6 })
  assert.deepEqual([...new Set(shared.map(r => r.sample))].sort(), ['WT_REP0', 'WT_REP1', 'WT_REP1x'])
})

test('the typeahead lists the newest run among its top samples', async () => {
  installDom()
  const b = box(await crowded())
  b.type('WT_REP1')
  await settle()
  const notes = b.menu.querySelector('[data-typeahead-group="samples"]').querySelectorAll('a[data-typeahead-entry]')
    .map(a => textOf(a.querySelector('.sub')))
  assert.equal(notes[0], 'new_run')
  assert.ok(notes.includes('new_run'))
})

test('more says something was cut, never that a limit was merely reached', async () => {
  const { ex } = await open()
  const one = await ex.searchSamples('WT_REP2', { limit: 1 })
  assert.equal(one.length, 1)
  assert.equal(one.more, false, 'exactly one row, and one row asked for')
  // WT_REP1, WT_REP2 and the emoji have rows; the digest of a truncated value has none and does not count.
  const three = await ex.searchSamples('WT_', { values: 3 })
  assert.equal(three.more, false)
  assert.equal((await ex.searchSamples('WT_', { values: 2 })).more, true)
})

test('a run with a current delete Claim is left out, as the pipeline page leaves it out', async () => {
  const { ex, ids } = await open()
  const rows = await ex.searchSamples('WT_REP1')
  assert.ok(!rows.some(r => r.completion_cid === ids.RD))
  assert.deepEqual([...new Set(rows.map(r => r.run_name))], ['distraught_murdock', 'sad_yonath', 'sharp_magritte'])
})

/**
 * IDs that sort before the real ones and have no rows search can show:
 * WT_0digest is a truncated value's digest, WT_0gone is only in a run a
 * delete Claim hides, WT_0sel is only in a Selection. WT_A and WT_B are in
 * new_run, one item each.
 */
async function sparse() {
  const sqlite3 = await loadSqlite()
  const db = new sqlite3.oo1.DB(':memory:')
  let bytes
  try {
    db.exec('PRAGMA page_size=4096')
    db.exec(readFileSync(new URL('./fixtures/schema.sql', import.meta.url), 'utf8'))
    const run = (sql, bind) => db.exec({ sql, bind })
    const item = (coll, sample, truncated = 0) => {
      const cid = fake(`${coll} ${sample}`)
      run('INSERT INTO item VALUES (?)', [cid])
      run('INSERT INTO collection_item (collection_cid, item_cid) VALUES (?, ?)', [coll, cid])
      run("INSERT INTO item_attr VALUES (?, 'id', 'string', ?, ?)", [cid, sample, truncated])
    }
    for (const [name, finished] of [['new_run', '2026-10-20T00:00:00.000Z'], ['gone_run', '2026-10-21T00:00:00.000Z']]) {
      run("INSERT INTO run (completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at) VALUES (?, 'nf-core/rnaseq', ?, 'succeeded', 0, ?)",
        [fake(name), name, finished])
      run("INSERT INTO collection VALUES (?, 'output', ?, 'out', 'test')", [fake(`${name}:out`), fake(name)])
    }
    item(fake('new_run:out'), 'WT_A')
    item(fake('new_run:out'), 'WT_B')
    item(fake('new_run:out'), 'WT_0digest', 1)
    item(fake('gone_run:out'), 'WT_0gone')
    run("INSERT INTO claim VALUES (?, ?, 'delete', NULL, NULL, '2026-10-25T10:00:00.000Z', 'test')", [fake('del'), fake('gone_run')])
    run("INSERT INTO collection (collection_cid, kind, asserted_by) VALUES (?, 'selection', 'test')", [fake('sel')])
    item(fake('sel'), 'WT_0sel')
    bytes = sqlite3.capi.sqlite3_js_db_export(db)
  } finally {
    db.close()
  }
  return Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(bytes),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(new Map()) }),
    listFn: async () => ({ names: [], readable: true }),
  })
}

test('an ID with no rows to show (a digest, a hidden run, a Selection only) takes no share and does not count toward values', async () => {
  const ex = await sparse()
  const two = await ex.searchSamples('WT_', { values: 2 })
  assert.deepEqual(two.map(r => r.sample).sort(), ['WT_A', 'WT_B'], 'the empty IDs sort first, and the values are refilled past them')
  assert.equal(two.more, false, 'two IDs have rows, and two were asked for')
  const one = await ex.searchSamples('WT_', { values: 1 })
  assert.deepEqual(one.map(r => r.sample), ['WT_A'])
  assert.equal(one.more, true)
  assert.deepEqual(await ex.searchSamples('WT_0'), [])
  assert.equal((await ex.searchSamples('WT_0')).more, false)
})

/** Records each claimStates call `ex` makes, with the subjects it asked about. */
function countClaims(ex) {
  const calls = []
  const claimStates = ex.claimStates.bind(ex)
  ex.claimStates = (subjects) => { calls.push([...subjects]); return claimStates(subjects) }
  return calls
}

test('a sample search looks up claims once per walk, over the distinct runs of its IDs\' first rows, not once per ID; each run once', async () => {
  const ex = await crowded()
  const calls = countClaims(ex)
  const rows = await ex.searchSamples('WT_')
  assert.deepEqual([...new Set(rows.map(r => r.sample))].sort(), ['WT_REP0', 'WT_REP1', 'WT_REP1x'])
  // One lookup for the walk's three IDs, whose first rows share one run; then one for the older runs the rows read
  // came from, which the walk never saw.
  assert.equal(calls.length, 2, JSON.stringify(calls))
  assert.equal(calls[0].length, 1, 'their first rows share one run, asked about once')
  assert.equal(new Set(calls.flat()).size, calls.flat().length, 'no run is asked about twice')
  // Two walks (the first finds only IDs with nothing to show): two lookups, and the hidden run is not asked about again.
  const sparseEx = await sparse()
  const sparseCalls = countClaims(sparseEx)
  assert.deepEqual((await sparseEx.searchSamples('WT_', { values: 2 })).map(r => r.sample).sort(), ['WT_A', 'WT_B'])
  assert.equal(sparseCalls.length, 2, JSON.stringify(sparseCalls))
  assert.equal(new Set(sparseCalls.flat()).size, sparseCalls.flat().length, 'no run is asked about twice')
})

test("the IDs' first reads run together, after the exact match's read", async () => {
  const ex = await crowded()
  const query = ex.db.query.bind(ex.db)
  let open = 0
  let most = 0
  const log = []
  ex.db.query = async (sql, params) => {
    if (sql !== SQL.searchSampleRows || params.at(-1) === 1) return query(sql, params)
    open++
    most = Math.max(most, open)
    log.push(`start ${params[1]}`)
    await new Promise(r => setTimeout(r, 5))
    const rows = await query(sql, params)
    log.push(`end ${params[1]}`)
    open--
    return rows
  }
  await ex.searchSamples('WT_', { limit: 30 })
  assert.equal(most, 3, 'WT_REP0, WT_REP1 and WT_REP1x are read at once')
  log.length = 0
  most = 0
  await ex.searchSamples('WT_REP1', { limit: 30 })
  assert.deepEqual(log.slice(0, 2), ['start WT_REP1', 'end WT_REP1'], 'the exact match reads first, alone')
})

test('% and _ are characters, not wildcards: they find only values that start with them', async () => {
  const { ex, ids } = await open()
  assert.deepEqual((await ex.searchSamples('%')).map(r => r.item_cid), [ids.pct])
  assert.deepEqual((await ex.searchSamples('_')).map(r => r.item_cid), [ids.under])
  assert.deepEqual(await ex.searchSamples('W%'), [])
  assert.deepEqual(await ex.searchSamples('W_REP1'), [])
  assert.deepEqual(await ex.searchSamples("WT_REP1'"), [])
  assert.deepEqual(await ex.searchSamples(''), [])
})

test('an address needs six characters past its fixed head (bafyrei, bafkrei) before it is looked up', async () => {
  const { ex } = await open()
  assert.equal(isAddressQuery('bafyrei'), false)
  assert.equal(isAddressQuery('bafyreiabcde'), false)
  assert.equal(isAddressQuery('bafyreiabcdef'), true)
  assert.equal(isAddressQuery('WT_REP1abcdef'), false)
  assert.deepEqual(await ex.searchAddresses('bafyrei'), [])
  assert.deepEqual(await ex.searchAddresses('bafkrei'), [])
  assert.deepEqual(await ex.searchAddresses('WT_REP1'), [])
})

test('searchAddresses names what each address is, with the page it opens', async () => {
  const { ex, ids } = await open()
  assert.deepEqual(await ex.searchAddresses(ids.R1.slice(0, 13)), [{ cid: ids.R1, kind: 'run', label: 'distraught_murdock', href: `#/run/${ids.R1}` }])
  assert.deepEqual(await ex.searchAddresses(ids.r1aligned),
    [{ cid: ids.r1aligned, kind: 'output', label: 'distraught_murdock / aligned', href: `#/collection/${ids.r1aligned}` }])
  assert.deepEqual(await ex.searchAddresses(ids.S1), [{ cid: ids.S1, kind: 'selection', label: 'my picks', href: `#/selection/${ids.S1}`,
    named: [{ asserted_by: 'test', timestamp: '2026-10-24T14:00:00.000Z', conflicted: false }] }])
  assert.deepEqual(await ex.searchAddresses(ids.file), [{ cid: ids.file, kind: 'file', label: 'WT_REP1.bam', href: `#/content/${ids.file}` }])
  // In an output and a Selection: one result, opened through its output.
  assert.deepEqual(await ex.searchAddresses(ids.r1aln),
    [{ cid: ids.r1aln, kind: 'item', label: 'aligned · WT_REP1', href: `#/item/${ids.r1aligned}/${ids.r1aln}` }])
})

test('the view opens an address that matches exactly one CID instead of listing it', async () => {
  installDom()
  const { ex, ids } = await open()
  const went = []
  await search(ex, ids.R1, ctx({ navigate: (href) => went.push(href) }))
  assert.deepEqual(went, [`#/run/${ids.R1}`])
  await search(ex, ids.r1aln.slice(0, 16), ctx({ navigate: (href) => went.push(href) }))
  assert.deepEqual(went.at(-1), `#/item/${ids.r1aligned}/${ids.r1aln}`)
  const before = went.length
  const page = await search(ex, 'WT_REP1', ctx({ navigate: (href) => went.push(href) }))
  assert.equal(went.length, before, 'a sample search never navigates')
  assert.ok(page.querySelector('[data-search-group="samples"]'))
})

test('the results page: one Samples row per run, linking to its Samples tab filtered to the sample', async () => {
  installDom()
  const { ex, ids } = await open()
  const page = await search(ex, 'WT_REP1', ctx())
  const group = page.querySelector('[data-search-group="samples"]')
  const rows = group.querySelectorAll('tr[data-sample]')
  assert.deepEqual(rows.map(r => r.dataset.run), [ids.R1, ids.R2, ids.R3])
  // R1's aligned item matched at meta.id too, the first path of rnaseq's join key; R2's items only at id.
  const where = (path) => encodeURIComponent(JSON.stringify([[path, 'string', 'WT_REP1']]))
  assert.equal(rows[0].querySelector('a').getAttribute('href'), `#/run/${ids.R1}?tab=samples&where=${where('meta.id')}`)
  assert.equal(rows[1].querySelector('a').getAttribute('href'), `#/run/${ids.R2}?tab=samples&where=${where('id')}`)
  await frame()
  assert.equal(cells(rows[0]), 'WT_REP1 | distraught_murdock | 99,566 reads (trimmed) · 89% mapped | 2 outputs | Oct 24')
  assert.equal(cells(rows[1]), 'WT_REP1 | sad_yonath |  | 1 output | Oct 23', 'an item that cannot be read leaves the row as it is')
  assert.equal(cells(rows[2]), 'WT_REP1 | sharp_magritte · Failed, may be incomplete | 50,000 reads (trimmed) | 1 output | Oct 23')
  assert.match(textOf(group), /same sample ID in 3 runs/)
})

test('sample links fall back to the matched path when no join-key path matched', () => {
  const row = (path, pipeline = 'nf-core/rnaseq') => ({ sample: 'S1', path, paths: [path], completion_cid: 'r', output_name: 'o', item_cid: 'i', pipeline })
  assert.equal(sampleGroups([row('sample')])[0].path, 'sample')
  assert.equal(sampleGroups([row('id', 'main.nf'), { ...row('meta.id', 'main.nf'), item_cid: 'j' }])[0].path, 'meta.id', 'an unknown pipeline joins on meta.id, then id')
})

test('promotedText: a count and its label, a percent with the % once', () => {
  assert.equal(promotedText({ label: 'Reads (trimmed)', format: 'int' }, { type: 'int', values: ['99566'] }), '99,566 reads (trimmed)')
  assert.equal(promotedText({ label: '% mapped', format: 'percent' }, { type: 'float', values: ['89.04'] }), '89% mapped', 'three significant figures at most, trailing zeros dropped')
})

test('the results page: Items as output · sample with the run name and a size, then Selections that hold them', async () => {
  installDom()
  const { ex, ids } = await open()
  const page = await search(ex, 'WT_REP1', ctx())
  const items = page.querySelector('[data-search-group="items"]').querySelectorAll('tr[data-item]')
  assert.deepEqual(items.map(r => r.dataset.item), [ids.r1aln, ids.r1pre1, ids.r2pre, ids.r3pre])
  assert.match(cells(items[0]), /^aligned · WT_REP1 \| distraught_murdock \| /)
  assert.equal(items[0].querySelector('a').getAttribute('href'), `#/item/${ids.r1aligned}/${ids.r1aln}`)
  assert.ok(items[0].querySelector('[data-size-for]'))
  await frame(); await frame()
  const selections = page.querySelector('[data-search-group="selections"]')
  assert.match(textOf(selections), /my picks/)
})

test('the results page says plainly what is searchable, and how to jump to an address', async () => {
  installDom()
  const { ex } = await open()
  const page = await search(ex, 'nothing_like_this', ctx())
  const text = textOf(page)
  assert.match(text, /File, output, run and Selection names are not searchable yet/)
  assert.equal(textOf(page.querySelector('[data-search-scope]')),
    'Searches sample IDs and addresses. Sample IDs match from the start and are case-sensitive. File, output, run and Selection names are not searchable yet.')
  const empty = await search(ex, '', ctx())
  assert.match(textOf(empty.querySelector('[data-search-scope]')), /Sample IDs match from the start and are case-sensitive\./)
  assert.match(text, /paste an address \(starts with baf\) to jump straight to it/)
  assert.match(text, /No sample ID starts with "nothing_like_this"/)
})

test('a short address opens nothing and says how many characters an address search needs', async () => {
  installDom()
  const { ex } = await open()
  const went = []
  const page = await search(ex, 'bafyrei', ctx({ navigate: (href) => went.push(href) }))
  assert.deepEqual(went, [])
  assert.match(textOf(page.querySelector('[data-search-short-address]')), /same 7 characters, so an address search needs 6 more/)
})

test('an address prefix with several matches lists them under Addresses, and opens none', async () => {
  installDom()
  const went = []
  const found = [{ cid: 'bafyreiabcdefg1', kind: 'run', label: 'distraught_murdock', href: '#/run/bafyreiabcdefg1' },
    { cid: 'bafyreiabcdefg2', kind: 'item', label: 'aligned · WT_REP1', href: '#/item/c/bafyreiabcdefg2' }]
  const ex = { searchAddresses: async () => found, searchSamples: async () => [], claimStates: async () => new Map(),
    selectionsHolding: async () => [], itemSize: async () => ({ bytes: 1, partial: false }) }
  const page = await search(ex, 'bafyreiabcdefg', ctx({ navigate: (href) => went.push(href) }))
  assert.deepEqual(went, [])
  const rows = page.querySelector('[data-search-group="addresses"]').querySelectorAll('tr[data-address-result]')
  assert.deepEqual(rows.map(r => r.dataset.addressResult), ['bafyreiabcdefg1', 'bafyreiabcdefg2'])
  assert.equal(cells(rows[0]), 'distraught_murdock | run | bafy…cdefg1')
})

test('slash focuses the box unless the reader is typing somewhere', () => {
  const key = (target, extra = {}) => ({ key: '/', target, ...extra })
  assert.equal(slashFocuses(key({ tagName: 'BODY' })), true)
  assert.equal(slashFocuses(key({ tagName: 'A' })), true)
  assert.equal(slashFocuses(key({ tagName: 'INPUT' })), false)
  assert.equal(slashFocuses(key({ tagName: 'TEXTAREA' })), false)
  assert.equal(slashFocuses(key({ tagName: 'SELECT' })), false)
  assert.equal(slashFocuses(key({ tagName: 'DIV', isContentEditable: true })), false)
  assert.equal(slashFocuses(key({ tagName: 'BODY' }, { metaKey: true })), false)
  assert.equal(slashFocuses({ key: 'a', target: { tagName: 'BODY' } }), false)
})

function box(ex) {
  const input = document.createElement('input')
  const menu = document.createElement('div')
  const went = []
  const asked = []
  const limits = []
  const counting = { searchSamples: (q, o) => { asked.push(q); limits.push([o?.limit, o?.values]); return ex.searchSamples(q, o) }, searchAddresses: (q, o) => ex.searchAddresses(q, o) }
  const api = searchBox(input, counting, { menu, go: (href) => went.push(href), wait: 5 })
  const type = (q) => { input.value = q; input.dispatchEvent(new Event('input')) }
  return { input, menu, went, asked, limits, api, type }
}
const settle = () => new Promise(resolve => setTimeout(resolve, 60))

test('the typeahead waits for two characters and a pause, then lists at most three per group', async () => {
  installDom()
  const { ex } = await open()
  const b = box(ex)
  b.type('W')
  await settle()
  assert.deepEqual(b.asked, [])
  assert.equal(b.menu.hidden, true)
  b.type('WT'); b.type('WT_'); b.type('WT_R')
  await settle()
  assert.deepEqual(b.asked, ['WT_R'], 'one search after the pause, for the last text')
  assert.deepEqual(b.limits, [[30, 3]], 'few rows and three sample IDs, so both index walks stop early')
  assert.equal(b.menu.hidden, false)
  for (const g of b.menu.querySelectorAll('[data-typeahead-group]'))
    assert.ok(g.querySelectorAll('a[data-typeahead-entry]').length <= 3, g.dataset.typeaheadGroup)
  const samples = b.menu.querySelector('[data-typeahead-group="samples"]').querySelectorAll('a[data-typeahead-entry]')
  assert.equal(samples.length, 3)
  assert.ok(b.menu.querySelector('[data-typeahead-group="items"]'))
})

test("sync puts a #/search query back in the box, a hand-typed '+' kept", async () => {
  installDom()
  const { ex } = await open()
  const b = box(ex)
  b.api.sync('#/search?q=WT+REP1&tab=items')
  assert.equal(b.input.value, 'WT+REP1')
  b.api.sync('#/search?q=a%20b%26c')
  assert.equal(b.input.value, 'a b&c')
  b.api.sync('#/search')
  assert.equal(b.input.value, '')
})

test('a new list of entries drops the highlight the box pointed at', async () => {
  installDom()
  const { ex } = await open()
  const b = box(ex)
  b.type('WT_REP1')
  await settle()
  b.input.press('ArrowDown')
  assert.equal(b.input.getAttribute('aria-activedescendant'), 'search-opt-0')
  b.type('WT_REP')
  await settle()
  assert.equal(b.menu.hidden, false)
  assert.equal(b.input.hasAttribute('aria-activedescendant'), false)
  assert.equal(b.menu.querySelectorAll('a.on').length, 0)
})

test('Enter opens the results page; ArrowDown then Enter opens the highlighted entry', async () => {
  installDom()
  const { ex } = await open()
  const b = box(ex)
  b.type('WT_REP1')
  await settle()
  assert.equal(b.input.press('Enter'), true)
  assert.deepEqual(b.went, ['#/search?q=WT_REP1'])
  b.type('WT_REP1')
  await settle()
  b.input.press('ArrowDown')
  b.input.press('Enter')
  assert.match(b.went[1], /^#\/run\/[^?]+\?tab=samples&where=/)
  b.type('a b&c')
  b.input.press('Enter')
  assert.equal(b.went[2], '#/search?q=a%20b%26c')
})

const named = (by, at = '2026-10-24T14:00:00.000Z', conflicted = false) => ({ asserted_by: by, timestamp: at, conflicted })
const selectionEx = (states, found) => ({ searchAddresses: async () => found, searchSamples: async () => [{ item_cid: 'bafyitem', output_name: 'o', sample: 's', collection_cid: 'c', completion_cid: 'r', pipeline: 'p', path: 'id' }],
  claimStates: async () => states, selectionsHolding: async () => [...states.keys()], itemSize: async () => ({ bytes: 1, partial: false }) })
const claim = (cid, value, by, extra = {}) => ({ cid, attribute: 'name', verb: 'set', value, asserted_by: by, timestamp: '2026-10-24T14:00:00.000Z', conflicted: false, ...extra })
const state = (...current) => ({ names: current.map(c => c.value), current, hidden: false })

test('the Selections panel tags each name with who named it and when; conflicting names each say so; an unnamed one has no tag', async () => {
  installDom()
  const states = new Map([
    ['bafyreiS1', state(claim('c1', 'my picks', 'alice'))],
    ['bafyreiS2', state(claim('c2', 'a', 'bob', { conflicted: true }), claim('c3', 'b', 'carol', { conflicted: true }))],
    ['bafyreiS3', state()]])
  const page = await search(selectionEx(states, []), 'WT', ctx(), { tab: 'selections' })
  await frame(); await frame()
  const row = (cid) => page.querySelector(`tr[data-selection="${cid}"]`)
  const one = row('bafyreiS1').querySelectorAll('.claim')
  assert.equal(one.length, 1)
  assert.equal(one[0].querySelector('.av').getAttribute('title'), 'alice')
  assert.ok(one[0].querySelector('.age'))
  assert.equal(one[0].classList.contains('conflict'), false)
  const two = row('bafyreiS2').querySelectorAll('.claim.conflict')
  assert.equal(two.length, 2)
  assert.equal(row('bafyreiS3').querySelectorAll('.claim').length, 0)
  assert.match(textOf(row('bafyreiS3')), /unnamed Selection/)
})

test('the Addresses panel and the typeahead tag a named Selection; conflicts are marked; unnamed has none', async () => {
  installDom()
  const found = [
    { cid: 'bafyreiS1', kind: 'selection', label: 'my picks', href: '#/selection/bafyreiS1', named: [named('alice')] },
    { cid: 'bafyreiS2', kind: 'selection', label: 'a / b', href: '#/selection/bafyreiS2', named: [named('bob', undefined, true), named('carol', undefined, true)] },
    { cid: 'bafyreiS3', kind: 'selection', label: 'unnamed Selection', href: '#/selection/bafyreiS3', named: [] },
    { cid: 'bafyreiR1', kind: 'run', label: 'a run', href: '#/run/bafyreiR1' }]
  const ex = selectionEx(new Map(), found)
  const page = await search(ex, 'bafyreiabcdefg', ctx({ navigate: () => {} }))
  const row = (cid) => page.querySelector(`tr[data-address-result="${cid}"]`)
  const one = row('bafyreiS1').querySelectorAll('.claim')
  assert.equal(one.length, 1)
  assert.ok(one[0].querySelector('.age'))
  assert.equal(one[0].querySelector('.av').getAttribute('title'), 'alice')
  assert.equal(row('bafyreiS2').querySelectorAll('.claim.conflict').length, 2)
  assert.equal(row('bafyreiS3').querySelectorAll('.claim').length, 0)
  assert.equal(row('bafyreiR1').querySelectorAll('.claim').length, 0)

  const b = box(ex)
  b.type('bafyreiabcdefg')
  await settle()
  const entries = b.menu.querySelector('[data-typeahead-group="addresses"]').querySelectorAll('a[data-typeahead-entry]')
  assert.equal(entries[0].querySelectorAll('.claim').length, 1)
  assert.equal(entries[1].querySelectorAll('.claim.conflict').length, 2)
  assert.equal(entries[2].querySelectorAll('.claim').length, 0)
})
