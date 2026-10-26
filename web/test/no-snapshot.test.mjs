// The Picked panel, #/compose and the items grid's previews over a real
// Explorer whose snapshot throws on any query once the page has opened (P0
// final fix wave, finding 2): naming picked items, their groups and the grid's
// rows is block reads only. The items grid's own query 3 is the one snapshot
// read it may make.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { installDom, frame } from './dom.mjs'
import { Explorer } from '../src/model.js'
import { BlockFetcher } from '../src/blocks.js'
import { Tray } from '../src/tray.js'
import { createPanel } from '../src/panel.js'
import { compose } from '../src/views/compose.js'
import { items } from '../src/views/items.js'
import { content } from '../src/views/collection.js'
import { snapshotDb } from './helpers.mjs'
import { blockFetch, buildMember } from './fixture.mjs'

async function world() {
  installDom()
  const now = Date.now()
  const member = await buildMember({ now })
  const gate = { open: true, asked: [] }
  const ex = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => {
      const db = await snapshotDb(member.snapshot)
      return { ...db, query: (sql, params) => {
        if (!gate.open) { gate.asked.push(sql); throw new Error(`snapshot query after open: ${sql.slice(0, 60)}`) }
        return db.query(sql, params)
      } }
    },
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }), now: () => now,
  })
  gate.open = false
  const tray = new Tray(null)
  const ctx = { tray, sources: new Map(), trayChanged: () => {}, use: () => {}, mark: () => {}, progress: () => {}, rerender: () => {},
    store: { name: 'lab', key: 'a' },
    write: { available: false, reason: 'read only', here: false, served: false, hrefFor: (h) => h, run: async () => {} } }
  return { ex, member, tray, ctx, gate }
}

const settle = async () => { for (let i = 0; i < 6; i++) { await frame(); await new Promise(r => setTimeout(r, 2)) } }

test('the Picked panel names items and their group without a snapshot query', async () => {
  const { ex, member, tray, ctx, gate } = await world()
  tray.addMany([{ address: member.item.A, via: [member.runs.R1.collection] }, { address: member.item.C, via: [member.runs.R2.collection] }])
  const el = document.createElement('div')
  const panel = createPanel({ el, ex, ctx })
  await panel.draw({ route: 'items', view: null })
  await settle()
  const names = el.querySelectorAll('[data-tray-entry]').map(e => e.querySelector('.pname').textContent).sort()
  assert.deepEqual(names, ['A', 'C'])
  assert.match(el.textContent, /R1 \/ aligned/)
  assert.match(el.textContent, /R2 \/ aligned/)
  assert.deepEqual(gate.asked, [])
})

test('#/compose names items, groups and the picked size without a snapshot query', async () => {
  const { ex, member, tray, ctx, gate } = await world()
  tray.addMany([{ address: member.item.A, via: [member.runs.R1.collection] }, { address: member.item.B, via: [member.runs.R1.collection] }])
  const page = await compose(ex, ctx)
  await settle()
  assert.deepEqual(page.querySelectorAll('[data-tray-entry]').map(e => e.querySelector('.pname').textContent).sort(), ['A', 'B'])
  assert.ok(page.querySelectorAll('h2').some(e => e.textContent === 'R1 / aligned'), page.querySelectorAll('h2').map(e => e.textContent).join('|'))
  assert.match(page.querySelector('[data-picked-size]').textContent, /20 B/)
  assert.deepEqual(gate.asked, [])
})

test('the items grid fills its rows and sizes from blocks; query 3 is its only snapshot read', async () => {
  const { ex, member, ctx, gate } = await world()
  const query = ex.items.bind(ex)
  let queried = 0
  ex.items = async (...args) => {
    gate.open = true
    try { queried++; return await query(...args) } finally { gate.open = false }
  }
  const page = await items(ex, member.runs.R1.completion, 'aligned', '[]', ctx)
  await settle()
  assert.equal(queried, 1)
  const rows = page.querySelectorAll('[data-item-result]')
  assert.equal(rows.length, 2)
  assert.deepEqual(rows.map(r => r.querySelector('[data-column=sample]').textContent).sort(), ['A', 'B'], 'named by sample, the Gate fixtures\' key')
  assert.ok(rows.every(r => /10 B/.test(r.querySelector('[data-size]').textContent)))
  assert.deepEqual(gate.asked, [])
})

test('the file page reads the snapshot for its producers and its own claims only (Gate assertion 2)', async () => {
  const { ex, member, ctx, gate } = await world()
  gate.open = true
  const asked = []
  const query = ex.db.query
  ex.db.query = (sql, params) => { asked.push(sql.trim().split(/\s+/).slice(0, 6).join(' ')); return query(sql, params) }
  const page = await content(ex, member.content.A, ctx)
  await settle()
  assert.equal(page.querySelectorAll('[data-producer]').length, 1)
  assert.match(page.querySelector('[data-producer]').textContent, /R1/)
  assert.equal(asked.length, 2, asked.join(' | '))
})
