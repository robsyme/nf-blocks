// web/src/routequery.js
// A route's query string as a Map. Each part is split on '&', then on its
// first '=', and decoded with decodeURIComponent, so a '+' typed into a URL
// by hand stays a '+'. URLSearchParams reads it as a space, which breaks a
// sample ID or a where value holding one.

/** One part of a query, decoded with decodeURIComponent ('+' stays '+'), or the raw text when its escapes are malformed (`%E0%A4%A`). */
export function decoded(text) {
  try {
    return decodeURIComponent(text)
  } catch {
    return text
  }
}

/** `a=1&b=x+y` as `Map { a => '1', b => 'x+y' }`. A key without '=' maps to ''; the first of a repeated key wins. */
export function queryOf(text) {
  const out = new Map()
  for (const part of String(text ?? '').split('&')) {
    if (!part) continue
    const at = part.indexOf('=')
    const key = decoded(at < 0 ? part : part.slice(0, at))
    if (!out.has(key)) out.set(key, at < 0 ? '' : decoded(part.slice(at + 1)))
  }
  return out
}
