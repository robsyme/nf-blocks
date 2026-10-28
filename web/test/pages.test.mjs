// web/test/pages.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Tray } from '../src/tray.js'
import { claimState } from '../src/claims.js'
import { item } from '../src/views/item.js'
import { selection } from '../src/views/selection.js'
import { pipeline } from '../src/views/pipeline.js'
import { home } from '../src/views/home.js'
import { FAKE_STORAGE } from './helpers.mjs'
import { createPresence } from '../src/presence.js'
import { fullWhenText } from '../src/words.js'

const ctx = () => {
  const c = { tray: new Tray(null), used: [], marked: [], rerender: () => {}, trayChanged: () => {},
    write: { available: false, reason: 'read only', served: true, hrefFor: (h) => h, here: false }, store: { name: 'roblab', key: 'a' } }
  c.use = (v) => c.used.push(v)
  c.mark = (m) => c.marked.push(m)
  return c
}

const sizes = { ...FAKE_STORAGE, selectionSize: async () => ({ bytes: 1_135_000, blocks: 9, files: 9, partial: false }),
  itemSize: async () => ({ bytes: 417_000, blocks: 3, files: 3, partial: false }),
  runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'boring_yalow' }) }
const stateOf = (extra = {}) => ({ names: ['africans'], nameClaims: [], current: [], nameConflicted: false, deletion: 'none', deletionClaims: [], pins: [], pinned: false, ...extra })

test('the item page: a de-duplicated title, sizes, downloads, produced-by, and the panel told its output', async () => {
  installDom()
  const ex = { ...FAKE_STORAGE,
    item: async () => ({ view: { id: 'WT_REP1', meta: { id: 'WT_REP1' }, single_end: false }, state: claimState([]),
      leaves: [{ name: 'WT_REP1.bam', size: 412_000_000, address: 'bafybam' }, { name: 'in.fq', size: null, address: null, reason: 'never_published' }] }),
    producersOf: async () => [{ collection_cid: 'coll', completion_cid: 'bafyrun' }, { collection_cid: 'coll2', completion_cid: 'bafyold' }],
    selectionsHolding: async () => [],
    runLabel: async (c) => ({ run_name: c === 'coll' ? 'high_jang' : 'happy_rubens', output: 'markdup', completion: c === 'coll' ? 'bafyrun' : 'bafyold',
      status: c === 'coll' ? 'succeeded' : 'failed', possibly_incomplete: c !== 'coll' }),
    runIdentity: async () => ({ pipeline: 'nf-core/rnaseq', run_name: 'high_jang' }),
    blocks: { urlFor: (a) => `blocks/${a}` },
  }
  const c = ctx()
  const page = await item(ex, 'coll', 'bafyitem', c)
  await frame()
  assert.equal(page.querySelector('h1').textContent, 'WT_REP1')
  assert.match(page.textContent, /412 MB/)
  assert.equal(page.querySelector('a[download]').getAttribute('href'), 'blocks/bafybam')
  assert.match(page.textContent, /not stored here \(such as an input file\)/)
  assert.equal(page.querySelector('[data-pick]').textContent, 'Pick')
  assert.equal(page.querySelector('[data-pick]').getAttribute('aria-label'), 'Pick this item')
  assert.deepEqual(c.used.at(-1), { completion: 'bafyrun', output: 'markdup', where: [], count: null })
  assert.ok(page.querySelector('[data-retention]'))
  const facts = page.querySelectorAll('section.panel').find(p => p.querySelector('h2')?.textContent === 'Produced by').querySelectorAll('.fact').map(f => f.textContent)
  assert.match(facts[0], /high_jang$/, 'the run it came from, a chip like the others, no status word as it succeeded')
  assert.match(facts[1], /happy_rubens failed/, 'the other run, by name')
})

test('the item page folds its metadata only when the fold would hide five pills or more', async () => {
  installDom()
  const view = (n) => Object.fromEntries(Array.from({ length: n }, (_, i) => [`k${String(i).padStart(2, '0')}`, i]))
  const page = async (n) => item({ ...FAKE_STORAGE, item: async () => ({ view: view(n), state: claimState([]), leaves: [] }), producersOf: async () => [],
    selectionsHolding: async () => [], runLabel: async () => null, blocks: { urlFor: (a) => a } }, '-', 'bafyitem', ctx())
  const header = (p) => p.querySelectorAll('.pills-page').flatMap(g => g.querySelectorAll('.pill'))
  const twelve = await page(12)
  assert.equal(twelve.querySelector('[data-fold="meta"]'), null)
  assert.equal(header(twelve).length, 12)
  const thirteen = await page(13)
  assert.equal(thirteen.querySelector('[data-fold="meta"]').querySelector('summary').textContent, '+5 more')
  assert.equal(header(thirteen).length, 13)
})

test('the Selection view leaves the call to the panel and keeps the samplesheet export on the page', async () => {
  installDom()
  const ex = { ...sizes, selection: async () => ({ state: stateOf({ names: ['treated BAMs'] }), members: [{ kind: 'item', address: 'i1', via: ['coll'] }], firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async () => 'here', itemBlock: async () => ({ view: { id: 'A' }, leaves: [] }), runLabel: async () => null }
  const c = ctx()
  const page = await selection(ex, 'bafysel', c)
  assert.equal(page.querySelectorAll('[data-snippet]').length, 0)
  assert.deepEqual(page.querySelectorAll('[data-samplesheet]').map(a => a.dataset.samplesheet), ['csv', 'json'])
  assert.equal(page.querySelector('[data-samplesheet=csv]').getAttribute('href'), 'api/samplesheet/bafysel.csv')
  const sheets = page.querySelectorAll('[data-samplesheet]')
  assert.deepEqual(sheets.map(a => a.textContent), ['Download CSV', 'Download JSON'], 'the two formats read alike')
  assert.deepEqual(sheets.map(a => a.getAttribute('class')), ['btn sm', 'btn sm'], 'and look alike')
  assert.deepEqual(c.used.at(-1), { selection: 'bafysel', members: 1, names: ['treated BAMs'], claims: [], pinned: false, store: 'roblab' })
  assert.equal(page.getAttribute('data-selection-view'), 'bafysel')
})

test('the pipeline page: health in words, rows keep [data-run], the left column expands it', async () => {
  installDom()
  const ex = {
    runPage: async () => ({ rows: [{ completion_cid: 'bafyrun', pipeline: 'p', run_name: 'high_jang', status: 'succeeded', possibly_incomplete: 0, finished_at: '2026-09-30T21:10:38.345Z', source: 'snapshot' }],
      hidden: 0, first: 1, last: 1, total: 1, prev: null, next: null }),
    completionOf: async () => ({ anomalies: { declined: 3, unjoined: 1 } }),
    stale: [],
  }
  const c = ctx()
  const page = await pipeline(ex, 'p', 0, c)
  await frame()
  assert.equal(page.querySelector('[data-run]').dataset.status, 'succeeded')
  // One run: its health is shared, so it shows once above the table and the row's cell stays blank.
  assert.match(page.querySelector('[data-shared-health]').textContent, /1 published file is outside any workflow output/)
  assert.equal(page.querySelector('[data-anomalies-for="bafyrun"]').textContent, '')
  assert.deepEqual(c.marked, [{ pipeline: 'p', expand: true }])
})

test('home links each pipeline to its latest good run', async () => {
  installDom()
  const ex = { pipelines: async () => [{ pipeline: 'p', runs: 2, latest: '2026-09-30T21:10:38.345Z' }], stale: [],
    latestSuccessfulRun: async () => 'bafyrun', runRow: async () => ({ run_name: 'high_jang', finished_at: '2026-09-30T21:10:38.345Z' }) }
  const page = await home(ex, ctx())
  await frame()
  assert.ok(page.querySelectorAll('a').some(a => a.getAttribute('href') === '#/run/bafyrun' && a.textContent === 'high_jang'))
})

test('home: a failing runRow shows ? and nothing is left unhandled', async () => {
  installDom()
  const seen = []
  const on = (e) => seen.push(e)
  process.on('unhandledRejection', on)
  const ex = { pipelines: async () => [{ pipeline: 'p', runs: 1, latest: null }], stale: [],
    latestSuccessfulRun: async () => 'bafyrun', runRow: async () => { throw new Error('x') } }
  const page = await home(ex, ctx())
  await frame(); await new Promise(r => setTimeout(r, 20))
  process.off('unhandledRejection', on)
  assert.equal(seen.length, 0)
  assert.ok(page.textContent.includes('?'))
  assert.ok(!page.textContent.includes('…'))
})

const itemEx = (over) => ({ ...FAKE_STORAGE,
  item: async () => ({ view: { id: 'A', meta: { id: 'A' } }, state: claimState([]), leaves: [{ name: 'a.bam', size: 1, address: 'bafybam' }] }),
  producersOf: async () => [{ collection_cid: 'c1', completion_cid: 'r1' }, { collection_cid: 'c2', completion_cid: 'r2' }],
  selectionsHolding: async () => [], runLabel: async (c) => ({ run_name: c === 'c1' ? 'one' : 'two', output: 'o', completion: c === 'c1' ? 'r1' : 'r2' }),
  runIdentity: async () => ({}), blocks: { urlFor: (a) => a }, ...over })

test('item with no collection lists every producing run and the query note', async () => {
  installDom()
  const page = await item(itemEx({}), '-', 'bafyitem', ctx())
  await frame()
  assert.match(page.textContent, /one/)
  assert.match(page.textContent, /two/)
  assert.match(page.textContent, /Picked by a query across runs\./)
})

test('item whose runLabel rejects lists its producers without the query note', async () => {
  installDom()
  const page = await item(itemEx({ runLabel: async () => { throw new Error('no') } }), 'c1', 'bafyitem', ctx())
  await frame()
  assert.ok(page.querySelectorAll('a').some(a => a.getAttribute('href') === '#/run/r1'))
  assert.ok(!page.textContent.includes('Picked by a query'))
})

test('a Selection row says which run and output it is from, and where it is held only when that is not here', async () => {
  installDom()
  const ex = { ...sizes, selection: async () => ({ state: stateOf(), members: [{ kind: 'item', address: 'i1', via: ['coll'] }, { kind: 'item', address: 'i2', via: ['coll'] }],
    firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async (kind, a) => (a === 'i1' ? 'here' : 'elsewhere'),
    itemBlock: async (c, i) => ({ view: { id: i }, leaves: [{ name: `${i}.dat` }], value: null }),
    runLabel: async () => ({ run_name: 'boring_yalow', output: 'people', completion: 'bafyrun', status: 'succeeded', possibly_incomplete: false }) }
  const page = await selection(ex, 'bafysel', ctx())
  await frame()
  await frame()
  const [here, away] = page.querySelectorAll('[data-member]')
  assert.equal(here.dataset.held, 'here')
  assert.equal(here.querySelector('[data-held-for]').textContent, '')
  assert.equal(away.querySelector('[data-held-for]').textContent, 'not in this store')
  assert.match(here.textContent, /boring_yalow · people/)
  const chip = here.querySelector('a[href="#/run/bafyrun"]')
  assert.ok(chip.classList.contains('fact'), 'the run as a chip, as Produced by shows it')
  assert.equal(chip.querySelector('[data-sealed]').dataset.sealed, 'run', 'the kind rides on the Seal\'s badge')
  assert.equal(chip.querySelector('.k'), null)
  assert.match(chip.textContent, /boring_yalow$/)
  assert.deepEqual(page.querySelectorAll('th').map(t => t.textContent).slice(0, 1), ['Sample'])
})

test('the Selection page: seal and name, the name as a claim tag with its author, durability, presence, a sized subline, a nested Selection by its name', async () => {
  installDom()
  const claim = { cid: 'bafyname', verb: 'set', value: 'africans', asserted_by: 'anonymous', timestamp: '2026-10-14T20:07:44.417Z', conflicted: false }
  const st = stateOf({ nameClaims: ['bafyname'], current: [claim] })
  const ex = { ...sizes,
    selection: async (cid) => (cid === 'bafysel'
      ? { state: st, members: [{ kind: 'item', address: 'i1', via: ['coll'] }, { kind: 'selection', address: 'bafyinner' }], firstSeen: '2026-10-14T20:07:44.412Z', block: { asserted_by: 'anonymous' } }
      : { state: stateOf({ names: ['lagos'] }), members: [], firstSeen: null, block: { asserted_by: 'x' } }),
    held: async () => 'here', itemBlock: async () => ({ view: { id: 'A' }, leaves: [] }), runLabel: async () => null,
  }
  const c = ctx()
  c.store = { name: 'roblab', key: 'a' }
  const page = await selection(ex, 'bafysel', c)
  await frame()
  const h1 = page.querySelector('h1')
  assert.equal(h1.textContent, 'africans')
  const name = page.querySelector('[data-name]')
  assert.equal(name.dataset.claim, 'bafyname')
  assert.ok(name.classList.contains('claim'))
  assert.ok(name.querySelector('.av').classList.contains('anon'), 'anonymous renders the kit anonymous avatar')
  assert.match(name.textContent, /named/)
  assert.equal(name.querySelector('.age').getAttribute('title'), fullWhenText('2026-10-14T20:07:44.417Z'))
  assert.ok(page.querySelector('.state.s-held'))
  assert.equal(page.querySelectorAll('.presence').length, 1)
  assert.equal(page.querySelector('.subline').querySelector('.addr').dataset.address, 'bafysel')
  assert.match(page.querySelector('.subline').textContent, /1 item and 1 Selection · 9 files · 1\.14 MB/)
  assert.ok(page.querySelectorAll('h2').some(e => e.textContent === 'Items and Selections'))
  const nested = page.querySelector('[data-member=bafyinner]')
  assert.equal(nested.dataset.kind, 'selection')
  assert.equal(nested.querySelector('[data-icon]').dataset.icon, 'selection')
  assert.match(nested.textContent, /lagos/)
  assert.ok(page.querySelector('[data-unavailable]'))
  // Copy to… needs explore's copy (ctx.copy); without it the button is disabled and says why.
  assert.equal(page.querySelector('[data-copy-to]').dataset.copyTo, 'unavailable')
  assert.equal(page.querySelector('[data-copy-to]').querySelector('button').disabled, true)
})

test('the Selection page offers Rename, Pin… (with the bytes it keeps) and Delete where the writer is here', async () => {
  installDom()
  const ex = { ...sizes, selection: async () => ({ state: stateOf(), members: [{ kind: 'item', address: 'i1', via: [] }], firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async () => 'here', itemBlock: async () => ({ view: { id: 'A' }, leaves: [] }), runLabel: async () => null }
  const c = ctx()
  c.store = { name: 'roblab', key: 'a' }
  c.write = { available: true, here: true, served: true, writer: {}, hrefFor: (h) => h, run: async () => {} }
  const page = await selection(ex, 'bafysel', c)
  await frame()
  for (const id of ['#rename-name', '#rename-save', '#pin-open', '#delete']) assert.ok(page.querySelector(id), id)
  assert.equal(page.querySelector('#pin').textContent, 'Pin 1.14 MB in roblab')
  assert.equal(page.querySelector('[data-copy-to]').dataset.copyTo, 'unavailable')
  c.copy = { available: true, reason: null, targets: ['org-s3'] }
  const copyable = await selection(ex, 'bafysel', c)
  const copyTo = copyable.querySelector('[data-copy-to]')
  assert.equal(copyTo.getAttribute('href'), '#/selection/bafysel/copy')
  assert.equal(copyTo.getAttribute('class'), 'btn primary')
})

test('promoted columns appear when the items carry their fields, and the table reads Sample, fields, Files, From, Size', async () => {
  installDom()
  const ex = { ...sizes, selection: async () => ({ state: stateOf(), members: [{ kind: 'item', address: 'i1', via: ['coll'] }], firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async () => 'here',
    itemBlock: async () => ({ view: { id: 'WT_REP2', meta: { single_end: false, strandedness: 'reverse' } }, value: { forward: { name: 'WT_REP2.forward.bigWig', address: 'bafkx' } },
      leaves: [{ name: 'WT_REP2.forward.bigWig' }] }),
    runLabel: async () => ({ run_name: 'distraught_murdock', output: 'bigwig', completion: 'bafyrun', status: 'succeeded', possibly_incomplete: false }) }
  const page = await selection(ex, 'bafysel', ctx())
  for (let i = 0; i < 4; i++) await frame()
  assert.deepEqual(page.querySelectorAll('th').map(t => t.textContent), ['Sample', 'Layout', 'Strand', 'Files', 'From', 'Size'])
  const row = page.querySelector('[data-member=i1]')
  assert.match(row.textContent, /WT_REP2/)
  assert.match(row.textContent, /paired/)
  assert.match(row.textContent, /reverse/)
  assert.match(row.textContent, /417 KB/)
})

test('home, pipeline and item pages name their kinds with icons', async () => {
  installDom()
  const homeEx = { pipelines: async () => [{ pipeline: 'p', runs: 2, latest: '2026-09-30T21:10:38.345Z' }], stale: [],
    latestSuccessfulRun: async () => 'bafyrun', runRow: async () => ({ run_name: 'high_jang' }) }
  const homePage = await home(homeEx, ctx())
  await frame()
  assert.deepEqual(homePage.querySelectorAll('[data-icon]').map(i => i.dataset.icon), ['pipeline', 'run'])
  assert.ok(homePage.querySelector('[data-sealed]').querySelector('.tone-good'), 'the latest good run: its Seal, the badge green')
  const pipeEx = { runPage: async () => ({ rows: [{ completion_cid: 'bafyrun', pipeline: 'p', run_name: 'high_jang', status: 'succeeded', possibly_incomplete: 0,
    finished_at: '2026-09-30T21:10:38.345Z', source: 'snapshot' }], hidden: 0, first: 1, last: 1, total: 1, prev: null, next: null }),
    completionOf: async () => ({ anomalies: {} }), stale: [] }
  const pipePage = await pipeline(pipeEx, 'p', 0, ctx())
  assert.equal(pipePage.querySelector('[data-run]').querySelector('[data-icon]').dataset.icon, 'run')
  assert.equal(pipePage.querySelector('[data-run]').querySelector('[data-seal]').dataset.seal, 'bafyrun', 'a run row leads with its Seal')
  assert.ok(pipePage.querySelector('[data-run]').querySelector('[data-icon]').classList.contains('tone-good'))
})

test('a Selection row whose collection this store does not know shows the collection as an address', async () => {
  installDom()
  const ex = { ...sizes, selection: async () => ({ state: stateOf(), members: [{ kind: 'item', address: 'i1', via: ['bafyunknowncoll'] }], firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async () => 'here', itemBlock: async () => ({ view: { firstName: 'x' }, leaves: [] }), runLabel: async () => null }
  const page = await selection(ex, 'bafysel', ctx())
  await frame()
  assert.equal(page.querySelector('[data-member]').querySelector('[data-address=bafyunknowncoll]').dataset.address, 'bafyunknowncoll')
  assert.equal(page.querySelector('[data-member]').querySelectorAll('[data-address]').find(a => a.dataset.address === 'i1').dataset.address, 'i1')
})

test('an item this store does not hold says "not in this store" once, and its label is the short address', async () => {
  installDom()
  const gone = Object.assign(new Error('block i1 is not in this store'), { code: 'block_missing' })
  const ex = { ...sizes, selection: async () => ({ state: stateOf(), members: [{ kind: 'item', address: 'bafyitemmissing1234567890', via: ['coll'] }], firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async () => 'elsewhere', itemBlock: async () => { throw gone }, runLabel: async () => null }
  const page = await selection(ex, 'bafysel', ctx())
  await frame()
  const row = page.querySelector('[data-member]')
  assert.equal(row.textContent.split('not in this store').length - 1, 1)
  assert.match(row.textContent, /bafy…567890/)
})

test('each From Copy copies the member as an Item Occurrence: cas://<via>/<item>, or the bare address with no via', async () => {
  installDom()
  const ex = { ...sizes, selection: async () => ({ state: stateOf(), members: [{ kind: 'item', address: 'i1', via: ['coll'] }, { kind: 'item', address: 'i2', via: [] }], firstSeen: null, block: { asserted_by: 'rob' } }),
    held: async () => 'here', itemBlock: async () => ({ view: { id: 'A' }, leaves: [] }), runLabel: async () => null }
  const page = await selection(ex, 'bafysel', ctx())
  const copies = (addr) => page.querySelector(`[data-member=${addr}]`).querySelectorAll('button').filter(b => /^Copy /.test(b.getAttribute('title') ?? '')).map(b => b.getAttribute('title'))
  assert.deepEqual(copies('i1'), ['Copy cas://coll/i1'])
  assert.deepEqual(copies('i2'), ['Copy i2'])
})

test('Pin… learns the bytes in place: the same pin form and note, not a rebuilt one', async () => {
  installDom()
  const ex = { ...sizes, selection: async () => ({ state: stateOf(), members: [], firstSeen: null, block: { asserted_by: 'rob' } }), held: async () => 'here', runLabel: async () => null }
  const c = ctx()
  c.write = { available: true, here: true, served: true, writer: {}, hrefFor: (h) => h, run: async () => {} }
  const page = await selection(ex, 'bafysel', c)
  const before = page.querySelector('#pin-note')
  await frame()
  assert.equal(page.querySelector('#pin-note'), before)
  assert.equal(page.querySelector('#pin').textContent, 'Pin 1.14 MB in roblab')
})

test('pickButton says "Picked" in its aria-label once picked', async () => {
  installDom()
  const { pickButton } = await import('../src/views/common.js')
  const c = ctx()
  const b = pickButton(c, { address: 'x' })
  await b.click()
  assert.equal(b.textContent, 'Picked')
  assert.equal(b.getAttribute('aria-label'), 'Pick this item (already picked)')
})

const PIN_CLAIM = { cid: 'bafypin', verb: 'add', attribute: 'pin', value: 'figure 3', asserted_by: 'ada.lovelace@example.org', timestamp: '2026-10-14T20:07:44.417Z', conflicted: false }
const NAME_CLAIM = { cid: 'bafyname', verb: 'set', attribute: 'name', value: 'africans', asserted_by: 'ada.lovelace@example.org', timestamp: '2026-10-14T20:07:44.417Z', conflicted: false }
const selEx = (state) => ({ ...sizes, selection: async () => ({ state, members: [{ kind: 'item', address: 'i1', via: [] }], firstSeen: null, block: { asserted_by: 'ada.lovelace@example.org' } }),
  held: async () => 'here', itemBlock: async () => ({ view: { id: 'A' }, leaves: [] }), runLabel: async () => null })
const pinnedState = () => stateOf({ nameClaims: ['bafyname'], current: [NAME_CLAIM, PIN_CLAIM], pins: [{ cid: 'bafypin', note: 'figure 3' }], pinned: true })

test('a read-only viewer sees the pins list as claim tags, with no Unpin or note form', async () => {
  installDom()
  const page = await selection(selEx(pinnedState()), 'bafysel', ctx())
  await frame()
  const pin = page.querySelector('[data-pin]')
  assert.ok(pin, 'pins show read-only')
  assert.equal(pin.dataset.claim, 'bafypin')
  assert.equal(pin.querySelector('.claim').querySelector('.av').getAttribute('title'), 'ada.lovelace@example.org')
  assert.ok(pin.querySelector('.claim').querySelector('.age'))
  assert.match(pin.textContent, /figure 3/)
  assert.equal(page.querySelector('[data-unpin]'), null)
  assert.equal(page.querySelector('#pin'), null)
  assert.equal(page.querySelector('#pin-note'), null)
  assert.ok(page.querySelectorAll('h2').some(e => e.textContent === 'Pins'))
})

test('no pins and read-only: no Pins panel', async () => {
  installDom()
  const page = await selection(selEx(stateOf()), 'bafysel', ctx())
  await frame()
  assert.ok(!page.querySelectorAll('h2').some(e => e.textContent === 'Pins'))
})

const DELETE_CLAIM = { cid: 'bafydel', verb: 'delete', attribute: null, value: null, asserted_by: 'gate', timestamp: '2026-10-14T20:07:44.417Z', conflicted: false }
const RETAIN_CLAIM = { cid: 'bafyretain', verb: 'set', attribute: 'retain', value: 'lineage', asserted_by: 'gate', timestamp: '2026-10-14T20:07:44.417Z', conflicted: false }

const nameBy = (cid, value, at, supersedes = [], by = 'ada.lovelace@example.org') => ({ cid, verb: 'set', attribute: 'name', value, asserted_by: by, timestamp: at, supersedes, conflicted: false })

test('a Selection renamed twice gives the rail C current, then B and A muted, newest first', async () => {
  installDom()
  const A = nameBy('bafyA', 'one', '2026-10-01T00:00:00.000Z')
  const B = nameBy('bafyB', 'two', '2026-10-02T00:00:00.000Z', ['bafyA'])
  const C = nameBy('bafyC', 'three', '2026-10-03T00:00:00.000Z', ['bafyB'], 'gate')
  const c = ctx()
  const page = await selection(selEx(stateOf({ names: ['three'], nameClaims: ['bafyC'], current: [C], claims: [A, C, B] })), 'bafysel', c)
  await frame()
  const used = c.used.at(-1)
  assert.deepEqual(used.claims.map(x => x.cid), ['bafyC', 'bafyB', 'bafyA'])
  assert.deepEqual(used.claims.map(x => !!x.superseded), [false, true, true])
  assert.deepEqual(used.claims.map(x => x.value), ['three', 'two', 'one'])
  assert.equal(page.querySelector('[data-rail-claim]'), null)
})

test('a delete then undo shows the superseded delete in the rail', async () => {
  installDom()
  const del = { cid: 'bafyd', verb: 'delete', attribute: null, value: null, asserted_by: 'gate', timestamp: '2026-10-02T00:00:00.000Z', supersedes: [] }
  const undo = { cid: 'bafyu', verb: 'del', attribute: null, value: null, asserted_by: 'gate', timestamp: '2026-10-03T00:00:00.000Z', supersedes: ['bafyd'] }
  const c = ctx()
  await selection(selEx(stateOf({ current: [undo], claims: [del, undo] })), 'bafysel', c)
  await frame()
  const used = c.used.at(-1)
  assert.deepEqual(used.claims.map(x => [x.cid, !!x.superseded, x.verb]), [['bafyu', false, 'restored'], ['bafyd', true, 'deleted']])
  assert.equal(used.claims[0].assertedBy, 'gate')
  assert.equal(used.claims[0].at, undo.timestamp)
})

test('a delete, undo, delete shows the current delete nowhere in the rail, and the earlier two superseded in order', async () => {
  installDom()
  const d1 = { cid: 'bafyd', verb: 'delete', attribute: null, value: null, asserted_by: 'gate', timestamp: '2026-10-02T00:00:00.000Z', supersedes: [] }
  const undo = { cid: 'bafyu', verb: 'del', attribute: null, value: null, asserted_by: 'gate', timestamp: '2026-10-03T00:00:00.000Z', supersedes: ['bafyd'] }
  const d2 = { cid: 'bafyd2', verb: 'delete', attribute: null, value: null, asserted_by: 'gate', timestamp: '2026-10-04T00:00:00.000Z', supersedes: ['bafyu'] }
  const c = ctx()
  await selection(selEx(stateOf({ current: [d2], claims: [d1, undo, d2] })), 'bafysel', c)
  await frame()
  const used = c.used.at(-1)
  assert.deepEqual(used.claims.map(x => [x.cid, !!x.superseded, x.verb]), [['bafyu', true, 'restored'], ['bafyd', true, 'deleted']])
})

test('a pin then unpin shows the superseded pin in the rail as pinned, with its note', async () => {
  installDom()
  const pin = { ...PIN_CLAIM, timestamp: '2026-10-02T00:00:00.000Z', supersedes: [] }
  const unpin = { cid: 'bafyunpin', verb: 'del', attribute: 'pin', value: null, asserted_by: 'gate', timestamp: '2026-10-03T00:00:00.000Z', supersedes: ['bafypin'] }
  const c = ctx()
  await selection(selEx(stateOf({ current: [unpin], claims: [pin, unpin] })), 'bafysel', c)
  await frame()
  const used = c.used.at(-1)
  assert.deepEqual(used.claims.map(x => [x.cid, !!x.superseded, x.verb, x.value]), [['bafyunpin', false, 'unpinned', null], ['bafypin', true, 'pinned', 'figure 3']])
})

test('a release then restore, and a superseded undo, read as released and restored', async () => {
  installDom()
  const release = { ...RETAIN_CLAIM, timestamp: '2026-10-02T00:00:00.000Z', supersedes: [] }
  const restore = { cid: 'bafyrestore', verb: 'del', attribute: 'retain', value: null, asserted_by: 'gate', timestamp: '2026-10-03T00:00:00.000Z', supersedes: ['bafyretain'] }
  const undo = { cid: 'bafyu', verb: 'del', attribute: null, value: null, asserted_by: 'gate', timestamp: '2026-10-04T00:00:00.000Z', supersedes: [] }
  const again = { ...DELETE_CLAIM, cid: 'bafyd2', timestamp: '2026-10-05T00:00:00.000Z', supersedes: ['bafyu'] }
  const c = ctx()
  await selection(selEx(stateOf({ current: [restore, again], claims: [release, restore, undo, again] })), 'bafysel', c)
  await frame()
  const used = c.used.at(-1)
  assert.deepEqual(used.claims.map(x => [x.cid, !!x.superseded, x.verb]),
    [['bafyrestore', false, 'restored'], ['bafyu', true, 'restored'], ['bafyretain', true, 'released']])
})

test('the Selection page hands its claims to the rail, without pins or deletions, and says who assembled it', async () => {
  installDom()
  const c = ctx()
  const st = pinnedState()
  st.current = [NAME_CLAIM, PIN_CLAIM, DELETE_CLAIM, RETAIN_CLAIM]
  const page = await selection(selEx(st), 'bafysel', c)
  await frame()
  assert.equal(page.querySelector('[data-rail-claim]'), null, 'the Claims list is in the rail, not the main column')
  assert.equal(page.querySelector('[data-rail-claims]'), null)
  const used = c.used.at(-1)
  assert.deepEqual(used.claims.map(x => x.cid), ['bafyname', 'bafyretain'])
  assert.equal(used.claims[0].verb, 'named')
  assert.equal(used.claims[0].value, 'africans')
  assert.equal(used.claims[1].verb, 'released')
  assert.equal(used.claims[0].absolute, undefined, 'rail tags show relative ages')
  assert.equal(used.pinned, true)
  assert.equal(used.store, 'roblab')
  // Only the header name tag carries [data-claim] for a name.
  assert.deepEqual(page.querySelectorAll('[data-claim]').map(e => e.dataset.claim).filter(x => x === 'bafyname'), ['bafyname'])
  const by = page.querySelector('[data-assembled-by]')
  assert.equal(by.querySelector('.av').getAttribute('title'), 'ada.lovelace@example.org')
  assert.match(by.textContent, /ada\.lovelace@example\.org/)
})

test('the Selection page head: with a second store, its dots name where the Selection block is held', async () => {
  installDom()
  const asked = []
  const presence = createPresence({ primary: 'roblab', list: [{ alias: 'roblab', key: 'a', base: 'm/roblab/' }, { alias: 'org-s3', key: 'b', base: 'm/org-s3/' }] },
    { fetchFn: async (url) => { asked.push(String(url)); return new Response(null, { status: 200 }) }, pageUrl: 'http://h/' })
  const ex = { ...sizes, selection: async () => ({ state: stateOf({ names: ['lagos'] }), members: [], firstSeen: null, block: { asserted_by: 'x' } }),
    held: async () => 'here', runLabel: async () => null }
  const page = await selection(ex, 'bafysel', { ...ctx(), store: { name: 'roblab', key: 'a' }, presence })
  await frame()
  const dots = page.querySelector('.page-head').querySelector('.presence')
  assert.equal(dots.getAttribute('title'), 'Held in roblab, org-s3')
  assert.deepEqual(asked, ['http://h/m/org-s3/blocks/el/bafysel'])
})
