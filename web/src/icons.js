// One grey icon per kind the explorer names (visual language spec §2): the
// same shape wherever a run, output, item, file, directory, Selection,
// Claim, address or store appears; colour stays for status, so a run's icon
// takes its status colour (`tone`) where the status is known. Each icon is a
// span painted by a CSS mask, so the page and the test DOM need no SVG
// elements; ICON_CSS holds the shapes.
import { h, link } from './html.js'
import { shortCid } from './components.js'

const SHAPES = {
  pipeline: '<path d="M2 4h4v8H2zM10 4h4v8h-4zM6 8h4" fill="none" stroke="#000" stroke-width="1.5"/>',
  run: '<circle cx="8" cy="9" r="5.3" fill="none" stroke="#000" stroke-width="1.4"/><path d="M6.3 1.5h3.4M8 1.5v2.2M8 9V6.2M12 4.4l1.2-1.2" fill="none" stroke="#000" stroke-width="1.4" stroke-linecap="round"/>',
  output: '<path d="M2 4h12M2 8h12M2 12h12" stroke="#000" stroke-width="1.6"/>',
  item: '<path d="M6 2.5C4 2.5 4.5 6 3 8c1.5 2 1 5.5 3 5.5M10 2.5c2 0 1.5 3.5 3 5.5-1.5 2-1 5.5-3 5.5" fill="none" stroke="#000" stroke-width="1.5"/>',
  file: '<path d="M4 1.5h5l3 3v10H4zM9 1.5v3h3" fill="none" stroke="#000" stroke-width="1.4"/>',
  directory: '<path d="M1.5 4h5l1.5 1.5h6.5v8h-13z" fill="none" stroke="#000" stroke-width="1.4"/>',
  selection: '<path d="M4 1.5h8v13l-4-3-4 3z" fill="none" stroke="#000" stroke-width="1.4"/>',
  claim: '<path d="M1.5 8.5l7-7h6v6l-7 7z" fill="none" stroke="#000" stroke-width="1.4"/><circle cx="11" cy="4.6" r="1"/>',
  address: '<path d="M6 2l-1.5 12M11.5 2 10 14M2.5 5.5h11M2 10.5h11" stroke="#000" stroke-width="1.3"/>',
  store: '<ellipse cx="8" cy="3.5" rx="5.5" ry="2" fill="none" stroke="#000" stroke-width="1.3"/><path d="M2.5 3.5v9c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2v-9M2.5 8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2" fill="none" stroke="#000" stroke-width="1.3"/>',
}

const NAMES = { pipeline: 'pipeline', run: 'run', output: 'output', item: 'item', file: 'file', directory: 'directory',
  selection: 'Selection', claim: 'name or pin', address: 'address', store: 'store' }

export const KINDS = Object.keys(SHAPES)

const dataUri = (shape) => `url("data:image/svg+xml,${encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16">${shape}</svg>`)}")`

export const ICON_CSS = [
  '.icon { display: inline-block; width: 14px; height: 14px; vertical-align: -2px; margin-right: 4px; flex: none;',
  '  background-color: var(--ink-3); -webkit-mask: var(--icon) center / contain no-repeat; mask: var(--icon) center / contain no-repeat; }',
  '.ref { white-space: nowrap; }',
  '.icon.tone-good { background-color: var(--ok); } .icon.tone-bad { background-color: var(--fail); }',
  ...KINDS.map(k => `.icon-${k} { --icon: ${dataUri(SHAPES[k])}; }`),
].join('\n')

/** `tone` ('good' or 'bad', from runTone) paints the icon in that status colour; without it the icon is grey. */
export function icon(kind, { tone = null } = {}) {
  if (!(kind in SHAPES)) throw new Error(`no icon for '${kind}'`)
  return h('span', { class: `icon icon-${kind}${tone ? ` tone-${tone}` : ''}`, 'data-icon': kind, title: NAMES[kind], 'aria-hidden': 'true' })
}

export function ref(kind, label, href = null, { tone = null } = {}) {
  return h('span', { class: 'ref' }, icon(kind, { tone }), href ? link(href, label) : label)
}

// One short form everywhere: the kit's (`bafy…26ocxq`), as addrChip shows it.
export { shortCid }

/** A block's address (spec §3): quiet, always present, a link to the block page. */
export function address(cidText) {
  return h('a', { class: 'address', href: `#/block/${cidText}`, title: cidText, 'data-address': cidText }, icon('address'), shortCid(cidText))
}
