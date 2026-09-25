// Which member the page reads (spec section 5.1, DESIGN.md §15).

const withSlash = (href) => (href.endsWith('/') ? href : `${href}/`)

export async function resolveStore(href, { fetchFn = (...a) => fetch(...a) } = {}) {
  const url = new URL(href)
  const store = url.searchParams.get('store')
  if (store) return { base: withSlash(new URL(store, url).href), members: null, member: null }
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
