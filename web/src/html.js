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
export const cid = (text) => h('code', { class: 'cid', title: text }, text)
