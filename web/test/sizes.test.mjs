import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CID } from 'multiformats/cid'
import { block, rawCid } from './fixture.mjs'
import { itemFiles, kindsOf, sumUnique } from '../src/sizes.js'
import { BlockError } from '../src/blocks.js'

const leaf = (name, address, size, reason = null) => ({ kind: 'Leaf', name, address, size, provider: 'p', reason })
const dir = (entries) => block({ kind: 'DirectoryManifest', schema: 2, entries })
const file = (name, text) => ({ name, mode: 'regular', size: text.length, address: rawCid(text), target: null })

function store(blocks) {
  const fetch = async (cid, kind) => {
    const b = blocks.find(x => x.cid.toString() === String(cid))
    if (!b) throw new BlockError('block_missing', String(cid), 'missing')
    return { value: (await import('@ipld/dag-cbor')).decode(b.bytes) }
  }
  return { fetch }
}

test('two leaves sharing one raw CID count once', async () => {
  const c = rawCid('same')
  const { files, partial } = await itemFiles([{ id: 1 }, leaf('a.bam', c, 4), leaf('b.bam', c, 4)], store([]).fetch)
  assert.equal(files.size, 1)
  assert.equal(files.get(c.toString()).size, 4)
  assert.equal(partial, false)
})

test('a directory leaf expands through regular entries, recursively; symlinks add nothing', async () => {
  const inner = dir([file('z.txt', 'zzz')])
  const top = dir([file('a.txt', 'aa'), file('b.log', 'bbbb'),
    { name: 'link', mode: 'symlink', size: 3, address: null, target: 'a.txt' },
    { name: 'sub', mode: 'directory', size: 0, address: inner.cid, target: null }])
  const { fetch } = store([top, inner])
  const { files, partial } = await itemFiles({ out: leaf('d', top.cid, 0) }, fetch)
  assert.equal(files.size, 3)
  assert.equal([...files.values()].reduce((n, f) => n + f.size, 0), 9)
  assert.ok([...files.values()].some(f => f.path === 'out/sub/z.txt'))
  assert.equal(partial, false)
})

test('a never_published leaf adds nothing and is not partial', async () => {
  const { files, partial } = await itemFiles([leaf('x', null, null, 'never_published')], store([]).fetch)
  assert.equal(files.size, 0)
  assert.equal(partial, false)
})

test('a missing manifest sets partial and keeps the other files', async () => {
  const gone = block({ kind: 'DirectoryManifest', schema: 2, entries: [] })
  const { files, partial } = await itemFiles([leaf('d', gone.cid, 0), leaf('a.bam', rawCid('q'), 7)], store([]).fetch)
  assert.equal(partial, true)
  assert.equal(files.size, 1)
})

test('sumUnique counts a shared file once and carries partial', () => {
  const a = new Map([['x', { size: 5 }], ['y', { size: 1 }]])
  const b = new Map([['x', { size: 5 }], ['z', { size: 10 }]])
  assert.deepEqual(sumUnique([{ files: a, partial: false }, { files: b, partial: true }]), { bytes: 16, blocks: 3, partial: true })
})

test('kindsOf orders by bytes desc, lower-casing but for bigWig and RData', () => {
  const files = new Map([
    ['1', { name: 'a.BAM', size: 100 }], ['2', { name: 'b.bam', size: 50 }],
    ['3', { name: 'c.bigwig', size: 400 }], ['4', { name: 'd.rdata', size: 5 }], ['5', { name: 'e.TSV', size: 7 }]])
  assert.deepEqual(kindsOf(files), [['bigWig', 1, 400], ['bam', 2, 150], ['tsv', 1, 7], ['RData', 1, 5]])
})

test('Explorer size methods are unique per run and pipeline, and cached across concurrent calls (P0\'s walks, on a lean snapshot)', async () => {
  const { Explorer } = await import('../src/model.js')
  const { BlockFetcher } = await import('../src/blocks.js')
  const { blockFetch, buildMember } = await import('./fixture.mjs')
  const { snapshotDb } = await import('./helpers.mjs')
  const now = Date.now()
  const member = await buildMember({ now, lean: true })
  const fetchFn = blockFetch(member.blocks)
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/',
    openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn }),
    listFn: async () => ({ names: member.log, readable: true }), now: () => now })
  const [a, b] = await Promise.all([explorer.runSize(member.runs.R2.completion), explorer.runSize(member.runs.R2.completion)])
  assert.equal(a, b)
  assert.deepEqual(a, { bytes: 20, blocks: 2, partial: false })
  assert.deepEqual(await explorer.pipelineSize('demo'), { bytes: 30, blocks: 3, partial: false, runs: 3, more: false })
  const col = await explorer.collectionSize(member.runs.R1.collection)
  assert.deepEqual([col.bytes, col.blocks, col.items], [20, 2, 2])
  assert.deepEqual(col.kinds, [['bam', 2, 20]])
  const it = await explorer.itemSize(member.item.A)
  assert.deepEqual([it.bytes, it.blocks, it.files, it.partial], [10, 1, 1, false])
})

test('a directory leaf is one dir kind; its files add bytes and no extension chips', async () => {
  const top = dir([file('a.txt', 'aa'), file('b.log', 'bbbb')])
  const { files, dirs } = await itemFiles([leaf('d', top.cid, 0), leaf('x.bam', rawCid('bam'), 3)], store([top]).fetch)
  assert.equal(dirs.length, 1)
  assert.equal(files.size, 3)
  assert.deepEqual(kindsOf(files, dirs), [['dir', 1, 6], ['bam', 1, 3]])
})

test('a directory entry with a non-dag-cbor address is skipped and partial', async () => {
  const top = dir([file('a.txt', 'aa'), { name: 'odd', mode: 'directory', size: 0, address: rawCid('odd'), target: null }])
  const { files, partial } = await itemFiles([leaf('d', top.cid, 0)], store([top]).fetch)
  assert.equal(files.size, 1)
  assert.equal(partial, true)
})

test('a missing item block gives a partial collection total from the other items', async () => {
  const { Explorer } = await import('../src/model.js')
  const { BlockFetcher } = await import('../src/blocks.js')
  const { blockFetch, buildMember } = await import('./fixture.mjs')
  const { snapshotDb } = await import('./helpers.mjs')
  const now = Date.now()
  const member = await buildMember({ now })
  member.blocks.delete(member.item.A)
  const explorer = await Explorer.open({
    base: 'http://h/m/lab/', openDb: async () => snapshotDb(member.snapshot),
    blocks: new BlockFetcher('http://h/m/lab/', { fetchFn: blockFetch(member.blocks) }),
    listFn: async () => ({ names: member.log, readable: true }), now: () => now })
  const col = await explorer.collectionSize(member.runs.R1.collection)
  assert.deepEqual([col.bytes, col.blocks, col.partial], [10, 1, true])
  assert.equal((await explorer.runSize(member.runs.R1.completion)).partial, true)
})

test('extOf: one rule for every page; a name with no dot has no extension, so no chip', async () => {
  const { extOf, extLabel } = await import('../src/sizes.js')
  assert.equal(extOf('a.BAM'), 'bam')
  assert.equal(extOf('x.fastq.gz'), 'gz')
  assert.equal(extOf('README'), '')
  assert.equal(extOf('.bashrc'), '')
  assert.equal(extOf('trailing.'), '')
  assert.equal(extOf(null), '')
  assert.equal(extLabel('bigwig'), 'bigWig')
  const files = new Map([['1', { name: 'README', size: 5 }], ['2', { name: 'LICENSE', size: 5 }], ['3', { name: 'a.txt', size: 1 }]])
  assert.deepEqual(kindsOf(files), [['txt', 1, 1]], 'no empty "×2" chip for the names without a dot')
})
