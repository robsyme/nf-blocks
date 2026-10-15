// A DOM builder small enough to read in one sitting.

export function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag)
  for (const [key, value] of Object.entries(attrs ?? {})) {
    if (value === null || value === undefined || value === false) continue
    if (key.startsWith('on')) el.addEventListener(key.slice(2), value)
    else el.setAttribute(key, value === true ? '' : String(value))
  }
  for (const child of children.flat(Infinity))
    if (child !== null && child !== undefined && child !== false) el.append(child instanceof Node ? child : String(child))
  return el
}

export const link = (href, ...children) => h('a', { href }, ...children)

/** What the Copy button should say after `clipboard.writeText(text)`; a missing clipboard counts as a rejection (page minors, ticket 11). */
export async function copyOutcome(clipboard, text) {
  if (!clipboard) return 'Copy failed'
  try {
    await clipboard.writeText(text)
    return 'Copied'
  } catch {
    return 'Copy failed'
  }
}

/** Hands the viewer `text` as a file named `name`, built in the page rather than fetched. */
export function saveText(name, text) {
  const url = URL.createObjectURL(new Blob([text], { type: 'text/plain' }))
  const a = h('a', { href: url, download: name })
  document.body.append(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 0)
}
