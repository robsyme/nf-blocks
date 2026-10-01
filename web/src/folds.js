// web/src/folds.js
// A <details> section whose open state survives a re-render for the life of
// the page (layout B plan P7): a write re-renders its view, and the reader's
// open Storage section, with its Pin and Release buttons, must stay open.
import { h } from './html.js'

const remembered = new Map()

/** Forgets every fold's state (tests). */
export function resetFolds() { remembered.clear() }

/**
 * `key` is `<kind>:<subject>`; `data-fold` carries the kind. A first argument
 * after `summary` that is `{ open: true }` opens it the first time it is drawn.
 */
export function fold(key, summary, ...body) {
  const options = body.length && body[0] && typeof body[0] === 'object' && !(body[0] instanceof Node) && 'open' in body[0] ? body.shift() : {}
  const open = remembered.has(key) ? remembered.get(key) : !!options.open
  const el = h('details', { class: 'fold', 'data-fold': key.split(':')[0], open }, h('summary', {}, summary), ...body)
  el.addEventListener('toggle', () => { remembered.set(key, !!el.open) })
  return el
}
