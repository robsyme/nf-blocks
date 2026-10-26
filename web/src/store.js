// Which member the page reads (spec section 5.1, DESIGN.md §15).

const withSlash = (href) => (href.endsWith('/') ? href : `${href}/`)

/** A browser will not fetch or range-read file:// URLs, so such a store fails every read (DESIGN.md §15). */
function refuseFile(base) {
  if (new URL(base).protocol === 'file:')
    throw Object.assign(new Error(`${base} is on the local disk, and a browser will not read files from there. ` +
      'Serve the member\'s directory through any static file server, or open it through `nextflow plugin nf-blocks:explore`.'), { code: 'file_protocol' })
  return base
}

/** A members.json entry with the fields an older server leaves out read as null/false. */
const normalizeMember = (m) => ({ ...m, kind: m.kind ?? null, location: m.location ?? null,
  copy_target: m.copy_target === true, copy_reason: m.copy_reason ?? null })

export async function resolveStore(href, { fetchFn = (...a) => fetch(...a) } = {}) {
  const url = new URL(href)
  const store = url.searchParams.get('store')
  if (store) return { base: refuseFile(withSlash(new URL(store, url).href)), members: null, member: null, all: false, copy: false, write: false, identity: null }
  if (url.protocol === 'file:') refuseFile(url.href)
  try {
    const res = await fetchFn(new URL('members.json', url).href, { cache: 'no-store' })
    if (res.ok && (res.headers.get('Content-Type') ?? '').includes('application/json')) {
      const body = await res.json()
      const members = body.members.map(normalizeMember)
      const wanted = url.searchParams.get('member')
      // ?member=* shows every member; the primary (the writable one, else the first) is still the one the views read.
      const all = wanted === '*'
      const member = all ? (members.find(m => m.writable) ?? members[0]) : (members.find(m => m.alias === wanted) ?? members[0])
      return { base: new URL(member.base, url).href, members, member: member.alias, all, copy: body.copy === true,
        write: body.write === true, identity: body.identity ?? null }
    }
  } catch {
    // Not served by explore.
  }
  return { base: new URL('.', url).href, members: null, member: null, all: false, copy: false, write: false, identity: null }
}

/**
 * `e` as the page should show it. A stopped server fails every read with the
 * browser's own words ("Load failed", "Failed to fetch", a NetworkError), so
 * ask the server that served this page whether it still answers: when it
 * does not, say that instead. Any answer, even a 404, means it is up.
 */
export async function explainFailure(e, href, { fetchFn = (...a) => fetch(...a) } = {}) {
  const url = new URL(href)
  if (url.protocol === 'file:') return e
  try {
    await fetchFn(new URL('/', url).href, { method: 'HEAD', cache: 'no-store' })
    return e
  } catch {
    return Object.assign(new Error(`The server at ${url.origin} is not answering, so this page cannot read the store. ` +
      'If it was started by `nextflow plugin nf-blocks:explore`, that command has stopped: run it again and open the URL it prints.'),
    { code: 'server_gone', cause: e })
  }
}
