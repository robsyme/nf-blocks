// Which member the page reads (spec section 5.1, DESIGN.md §15).

const withSlash = (href) => (href.endsWith('/') ? href : `${href}/`)

/** A browser will not fetch or range-read file:// URLs, so such a store fails every read (DESIGN.md §15). */
function refuseFile(base) {
  if (new URL(base).protocol === 'file:')
    throw Object.assign(new Error(`${base} is on the local disk, and a browser will not read files from there. ` +
      'Serve the member\'s directory through any static file server, or open it through `nextflow plugin nf-blocks:explore`.'), { code: 'file_protocol' })
  return base
}

export async function resolveStore(href, { fetchFn = (...a) => fetch(...a) } = {}) {
  const url = new URL(href)
  const store = url.searchParams.get('store')
  if (store) return { base: refuseFile(withSlash(new URL(store, url).href)), members: null, member: null }
  if (url.protocol === 'file:') refuseFile(url.href)
  try {
    const res = await fetchFn(new URL('members.json', url).href, { cache: 'no-store' })
    if (res.ok && (res.headers.get('Content-Type') ?? '').includes('application/json')) {
      const { members } = await res.json()
      const wanted = url.searchParams.get('member')
      const member = members.find(m => m.alias === wanted) ?? members[0]
      return { base: new URL(member.base, url).href, members, member: member.alias }
    }
  } catch {
    // Not served by explore.
  }
  return { base: new URL('.', url).href, members: null, member: null }
}
