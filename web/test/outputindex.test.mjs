// web/test/outputindex.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { block, rawCid } from './fixture.mjs'
import { addressesByPath, rewriteIndex } from '../src/outputindex.js'

const leaf = (name, address) => ({ kind: 'Leaf', name, address, size: 10, reason: null })
const declined = { kind: 'Leaf', name: null, address: null, size: null, reason: 'declined' }

test('addressesByPath: a file leaf is cas://<cid>/<name>, as fromStore emits it', () => {
  const tarik = rawCid('Tarik'), greta = rawCid('Greta')
  const items = [block({ x: 1 }).cid, block({ x: 2 }).cid]
  const values = new Map([
    [items[0].toString(), { person: { firstName: 'Tarik' }, datafile: leaf('Tarik_Benali.dat', tarik) }],
    [items[1].toString(), { person: { firstName: 'Greta' }, datafile: leaf('Greta_Larsen.dat', greta) }],
  ])
  const byPath = addressesByPath({ items, paths: [['people/Africa/Tarik_Benali.dat'], ['people/Europe/Greta_Larsen.dat']] }, values)
  assert.deepEqual([...byPath], [
    ['people/Africa/Tarik_Benali.dat', `cas://${tarik}/Tarik_Benali.dat`],
    ['people/Europe/Greta_Larsen.dat', `cas://${greta}/Greta_Larsen.dat`],
  ])
})

test('addressesByPath: several leaves pair by file name, not position, since a stored map is in key order', () => {
  const bam = rawCid('bam'), reads = rawCid('reads')
  const item = block({ y: 1 }).cid
  // Join recorded paths in the channel's order (reads, bam); the decoded map is in canonical key order (bam, reads).
  const values = new Map([[item.toString(), { bam: leaf('A.bam', bam), reads: leaf('A.fq.gz', reads), skipped: declined }]])
  const byPath = addressesByPath({ items: [item], paths: [['out/A.fq.gz', 'out/A.bam', null]] }, values)
  assert.equal(byPath.get('out/A.fq.gz'), `cas://${reads}/A.fq.gz`)
  assert.equal(byPath.get('out/A.bam'), `cas://${bam}/A.bam`)
  assert.equal(byPath.size, 2)
})

test('addressesByPath: a directory leaf is the Item Leaf, or its manifest when another leaf shares its name', () => {
  const dir = block({ kind: 'DirectoryManifest' }).cid, other = block({ kind: 'DirectoryManifest', n: 2 }).cid
  const one = block({ z: 1 }).cid, two = block({ z: 2 }).cid
  const values = new Map([
    [one.toString(), [leaf('A_qc', dir)]],
    [two.toString(), [leaf('qc', dir), leaf('qc', other)]],
  ])
  const byPath = addressesByPath({ items: [one, two], paths: [['qc/A_qc'], ['x/qc', 'y/qc']] }, values)
  assert.equal(byPath.get('qc/A_qc'), `cas://${one}/A_qc`)
  assert.equal(byPath.has('x/qc'), false, 'two leaves named qc: neither name pairs, and two paths with two leaves cannot pair by position')
})

test('addressesByPath: one path and one addressed leaf pair even when the names differ; a path two items claim pairs with neither', () => {
  const a = rawCid('a'), b = rawCid('b')
  const one = block({ w: 1 }).cid, two = block({ w: 2 }).cid
  const values = new Map([[one.toString(), [leaf('renamed.txt', a)]], [two.toString(), [leaf('same.txt', b)]]])
  const byPath = addressesByPath({ items: [one, two], paths: [['out/original.txt'], ['out/original.txt']] }, values)
  assert.equal(byPath.has('out/original.txt'), false)
  const alone = addressesByPath({ items: [one], paths: [['out/original.txt']] }, values)
  assert.equal(alone.get('out/original.txt'), `cas://${a}/renamed.txt`)
})

test('rewriteIndex: each coordinate becomes its address; the rest of the text is kept byte for byte', () => {
  const byPath = new Map([['people/Europe/Carmen_Ruiz.dat', 'cas://bafkreiaaa/Carmen_Ruiz.dat']])
  const json = '[\n    {\n        "person": {"firstName": "Carmen"},\n        "datafile": "cas://roblab/people/Europe/Carmen_Ruiz.dat"\n    }\n]\n'
  assert.deepEqual(rewriteIndex(json, byPath), {
    text: '[\n    {\n        "person": {"firstName": "Carmen"},\n        "datafile": "cas://bafkreiaaa/Carmen_Ruiz.dat"\n    }\n]\n',
    replaced: 1, unmatched: 0,
  })
  const csv = 'name,file\nCarmen,cas://roblab/people/Europe/Carmen_Ruiz.dat\nnobody,cas://roblab/people/gone.dat\n'
  assert.deepEqual(rewriteIndex(csv, byPath), {
    text: 'name,file\nCarmen,cas://bafkreiaaa/Carmen_Ruiz.dat\nnobody,cas://roblab/people/gone.dat\n', replaced: 1, unmatched: 1 })
})

test('rewriteIndex: an outputDir below the alias still matches; a Store URI and plain text are left alone and not counted', () => {
  const byPath = new Map([['a/x.txt', 'cas://bafkreixxx/x.txt']])
  const cid = rawCid('kept').toString()
  const text = `cas://lab/results/a/x.txt\tcas://${cid}/kept.txt\tnot a uri\tcas://lab\n`
  assert.deepEqual(rewriteIndex(text, byPath),
    { text: `cas://bafkreixxx/x.txt\tcas://${cid}/kept.txt\tnot a uri\tcas://lab\n`, replaced: 1, unmatched: 0 })
  assert.deepEqual(rewriteIndex('cas://lab/aa/x.txt', byPath), { text: 'cas://lab/aa/x.txt', replaced: 0, unmatched: 1 },
    'a path matches only at a segment boundary')
})
