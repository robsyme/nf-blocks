// The Store Log as the page reads it (spec section 3, DESIGN.md §15): names
// parse exactly as StoreLog.parse, and the tail re-reads a 10-minute overlap
// before the watermark, its floor clamped to the local clock.
import { OVERLAP_MILLIS } from './config.js'

const HORIZON = 9999999999999
const NAME = /^(\d{13})-(run|selection|claim)-(b[a-z2-7]{58})$/

export function parseEntry(name) {
  const m = NAME.exec(name)
  return m ? { name, rts: m[1], writtenAtMillis: HORIZON - Number(m[1]), kind: m[2], cid: m[3] } : null
}

export function floorOf(watermark, nowMillis) {
  const mark = watermark ? parseEntry(watermark) : null
  return mark ? Math.min(mark.writtenAtMillis, nowMillis) - OVERLAP_MILLIS : null
}

export function entriesSince(names, watermark, nowMillis = Date.now()) {
  const entries = names.map(parseEntry).filter(Boolean).sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
  const floor = floorOf(watermark, nowMillis)
  return floor === null ? entries : entries.filter(e => e.writtenAtMillis >= floor)
}

/** Every entry name the member's log/ listing gives, in the first form that answers (DESIGN.md §15). */
export async function listLog(base, { fetchFn = fetch, watermark = null, nowMillis = Date.now() } = {}) {
  let res = null
  try {
    res = await fetchFn(new URL('log/', base).href, { headers: { Accept: 'application/json' }, cache: 'no-store' })
  } catch {
    res = null
  }
  if (res?.ok) {
    const type = res.headers.get('Content-Type') ?? ''
    if (type.includes('application/json')) return (await res.json()).entries ?? []
    if (type.includes('text/html')) return hrefs(await res.text())
  }
  return listS3(base, fetchFn, floorOf(watermark, nowMillis))
}

function hrefs(html) {
  return [...html.matchAll(/href="([^"?#]+)"/g)]
    .map(m => decodeURIComponent(m[1]).replace(/\/$/, ''))
    .filter(name => parseEntry(name))
}

const XML = { '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'" }
const unxml = (text) => text.replace(/&(amp|lt|gt|quot|apos);/g, m => XML[m])

/** ListObjectsV2, virtual-hosted style. New entries sort first, so paging stops at the floor. */
async function listS3(base, fetchFn, floor) {
  const url = new URL(base)
  const prefix = `${url.pathname.replace(/^\//, '')}log/`
  const names = []
  let token = null
  for (;;) {
    const query = new URLSearchParams({ 'list-type': '2', prefix })
    if (token) query.set('continuation-token', token)
    let res
    try {
      res = await fetchFn(`${url.origin}/?${query}`, { cache: 'no-store' })
    } catch {
      return names
    }
    if (!res.ok) return names
    const xml = await res.text()
    if (!xml.includes('<ListBucketResult')) return names
    const keys = [...xml.matchAll(/<Key>([^<]+)<\/Key>/g)].map(m => unxml(m[1]).slice(prefix.length))
    names.push(...keys)
    const next = /<NextContinuationToken>([^<]+)<\/NextContinuationToken>/.exec(xml)
    const last = parseEntry(keys[keys.length - 1] ?? '')
    if (!next || (floor !== null && last && last.writtenAtMillis < floor)) return names
    token = unxml(next[1])
  }
}
