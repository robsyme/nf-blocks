import { test } from 'node:test'
import assert from 'node:assert/strict'
import { entriesSince, listLog, parseEntry } from '../src/storelog.js'
import { entryName } from './fixture.mjs'

const CID = 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'
const T = 1_758_000_000_000
const MIN = 60_000

test('entry names parse as StoreLog.parse does', () => {
  const e = parseEntry(entryName(T, 'run', CID))
  assert.deepEqual([e.kind, e.cid, e.writtenAtMillis], ['run', CID, T])
  for (const bad of ['.DS_Store', `123-run-${CID}`, `${'1'.repeat(13)}-pin-${CID}`, `${'1'.repeat(13)}-run-nope`])
    assert.equal(parseEntry(bad), null, bad)
})

test('since the watermark: newer, the watermark, and 10 minutes behind it; newest first', () => {
  const names = [entryName(T + MIN, 'run', CID), entryName(T, 'run', CID), entryName(T - 5 * MIN, 'run', CID),
    entryName(T - 11 * MIN, 'run', CID), 'junk']
  const since = entriesSince(names, entryName(T, 'run', CID), T + 60 * MIN)
  assert.deepEqual(since.map(e => e.writtenAtMillis), [T + MIN, T, T - 5 * MIN])
})

test('the floor is clamped to the local clock, as a watermark from a fast clock cannot hide entries', () => {
  const ahead = entryName(T + 60 * MIN, 'run', CID)
  const since = entriesSince([entryName(T - 5 * MIN, 'run', CID), entryName(T - 20 * MIN, 'run', CID)], ahead, T)
  assert.deepEqual(since.map(e => e.writtenAtMillis), [T - 5 * MIN])
})

test('no watermark means every entry', () => {
  assert.equal(entriesSince([entryName(T, 'run', CID), entryName(T - 99 * MIN, 'run', CID)], null, T).length, 2)
})

const json = (body) => new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } })

test('explore answers log/ with JSON', async () => {
  const names = [entryName(T, 'run', CID)]
  const fetchFn = async (url) => { assert.equal(url, 'http://h/m/lab/log/'); return json({ entries: names }) }
  assert.deepEqual(await listLog('http://h/m/lab/', { fetchFn }), { names, readable: true })
})

test('a static server answers log/ with an HTML index', async () => {
  const name = entryName(T, 'run', CID)
  const html = `<html><body><ul><li><a href="${name}">${name}</a></li><li><a href="../">..</a></li></ul></body></html>`
  const fetchFn = async () => new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8' } })
  assert.deepEqual(await listLog('http://h/store/', { fetchFn }), { names: [name], readable: true })
})

test('S3 is listed with ListObjectsV2, paged, stopping once a page reaches past the floor', async () => {
  const pages = [
    [entryName(T + 2 * MIN, 'run', CID), entryName(T + MIN, 'run', CID)],
    [entryName(T, 'run', CID), entryName(T - 30 * MIN, 'run', CID)],
    [entryName(T - 60 * MIN, 'run', CID)],
  ]
  const asked = []
  const fetchFn = async (url) => {
    asked.push(url)
    if (url.endsWith('/log/')) return new Response('<Error><Code>AccessDenied</Code></Error>', { status: 403 })
    const u = new URL(url)
    assert.equal(u.searchParams.get('prefix'), 'member/log/')
    const i = Number(u.searchParams.get('continuation-token') ?? 0)
    const keys = pages[i].map(n => `<Contents><Key>member/log/${n}</Key></Contents>`).join('')
    const next = i + 1 < pages.length ? `<NextContinuationToken>${i + 1}</NextContinuationToken>` : ''
    return new Response(`<?xml version="1.0"?><ListBucketResult>${keys}${next}</ListBucketResult>`, { headers: { 'Content-Type': 'application/xml' } })
  }
  const { names, readable } = await listLog('https://b.s3.ca-central-1.amazonaws.com/member/', { fetchFn, watermark: entryName(T, 'run', CID), nowMillis: T })
  assert.equal(readable, true)
  assert.equal(names.length, 4)
  assert.equal(asked.filter(u => u.includes('list-type=2')).length, 2)
})

test('no listing at all is an unreadable log, not an empty one (final review finding 6)', async () => {
  const fetchFn = async () => new Response('nope', { status: 404 })
  assert.deepEqual(await listLog('http://h/store/', { fetchFn }), { names: [], readable: false })
  const down = async () => { throw new TypeError('Failed to fetch') }
  assert.deepEqual(await listLog('http://h/store/', { fetchFn: down }), { names: [], readable: false })
})

test('an empty listing that answered is a readable, empty log', async () => {
  const fetchFn = async () => json({ entries: [] })
  assert.deepEqual(await listLog('http://h/m/lab/', { fetchFn }), { names: [], readable: true })
})

test('an S3 listing that fails part way is unreadable, with what it had', async () => {
  const fetchFn = async (url) => {
    if (url.endsWith('/log/')) return new Response('', { status: 403 })
    if (new URL(url).searchParams.get('continuation-token')) return new Response('', { status: 500 })
    return new Response(`<ListBucketResult><Contents><Key>m/log/${entryName(T, 'run', CID)}</Key></Contents><NextContinuationToken>1</NextContinuationToken></ListBucketResult>`)
  }
  const got = await listLog('https://b.s3.amazonaws.com/m/', { fetchFn })
  assert.deepEqual(got, { names: [entryName(T, 'run', CID)], readable: false })
})

test('a static server answering ListObjectsV2 with its own index.html is not a listing, even one quoting <ListBucketResult', async () => {
  const page = '<!doctype html><script>if (!xml.includes("<ListBucketResult")) {}</script>'
  const fetchFn = async (url) => (url.endsWith('/log/') ? new Response('', { status: 404 })
    : new Response(page, { headers: { 'Content-Type': 'text/html' } }))
  assert.deepEqual(await listLog('http://h/nolog/', { fetchFn }), { names: [], readable: false })
})
