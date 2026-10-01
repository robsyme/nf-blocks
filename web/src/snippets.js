// web/src/snippets.js
// The consumer code the Selection and run pages offer (DESIGN.md §16
// decision 13, ticket 07 Q4): the include line and one fromStore call,
// untyped or typed. The mode is one choice for every snippet on the page,
// remembered per viewer in localStorage where the browser allows it. The
// Gate runs these call lines verbatim (decision 14).
import { h, copyOutcome } from './html.js'

export const SNIPPET_KEY = 'nf-blocks.snippets'
export const INCLUDE_LINE = "include { fromStore } from 'plugin/nf-blocks'"
const MODES = ['untyped', 'typed']

function safeLocalStorage() {
  try { return globalThis.localStorage ?? null } catch { return null }
}

/** The remembered mode: 'typed' only when storage says so; anything else, or no storage, is 'untyped'. */
export function snippetMode(storage = safeLocalStorage()) {
  try {
    return storage?.getItem(SNIPPET_KEY) === 'typed' ? 'typed' : 'untyped'
  } catch {
    return 'untyped'
  }
}

// The mode chosen on this page, which holds even when storage refused it,
// and every snippet and toggle drawn, redrawn when the mode changes.
let chosen = null
const live = new Set()
const current = () => chosen ?? snippetMode()

/** Sets the mode for every snippet on the page and remembers it; storage that refuses loses only the memory. */
export function setSnippetMode(mode, storage = safeLocalStorage()) {
  if (!MODES.includes(mode)) throw new Error(`a snippet is untyped or typed, not ${mode}`)
  chosen = mode
  try {
    storage?.setItem(SNIPPET_KEY, mode)
  } catch {
    // Not remembered past this page.
  }
  for (const shown of [...live]) {
    if (shown.node.isConnected) shown.draw(mode)
    else live.delete(shown)
  }
}

// A Groovy single-quoted string: backslash, quote and line breaks escaped, so a call stays one line.
const quote = (text) => `'${String(text).replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')}'`

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/
// Groovy's reserved words, quoted as map keys to be safe.
const KEYWORDS = new Set(['abstract', 'as', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char', 'class', 'const', 'continue',
  'def', 'default', 'do', 'double', 'else', 'enum', 'extends', 'false', 'final', 'finally', 'float', 'for', 'goto', 'if', 'implements',
  'import', 'in', 'instanceof', 'int', 'interface', 'long', 'native', 'new', 'null', 'package', 'private', 'protected', 'public',
  'return', 'short', 'static', 'strictfp', 'super', 'switch', 'synchronized', 'this', 'threadsafe', 'throw', 'throws', 'trait',
  'transient', 'true', 'try', 'var', 'void', 'volatile', 'while', 'yields', 'record', 'sealed', 'permits', 'non-sealed'])

/** A Meta Map path as a `where:` key: bare when it is a plain identifier, else quoted. */
export const groovyKey = (path) => (IDENTIFIER.test(path) && !KEYWORDS.has(path) ? path : quote(path))

/**
 * A condition's value as the Groovy literal fromStore(where:) types back to the
 * same item_attr row (MetadataView.scalar): a float gets `d`, since a BigDecimal
 * literal's toString differs from Double's (P8).
 */
export function groovyValue(type, value) {
  switch (type) {
    case 'string': return quote(value)
    case 'int': return String(value)
    case 'float': return `${value}d`
    case 'bool': return String(value)
    case 'null': return 'null'
    default: throw new Error(`unknown type '${type}'`)
  }
}

/** `[k: v, ...]` for the chips, or null when there are none. */
export const whereLiteral = (where) => (where?.length ? `[${where.map(([p, t, v]) => `${groovyKey(p)}: ${groovyValue(t, v)}`).join(', ')}]` : null)

/** The two lines of a snippet: the include, and the call for `mode`. */
export function snippetLines(spec, mode) {
  const where = whereLiteral(spec.where)
  const args = spec.kind === 'selection' ? `selection: ${quote(spec.cid)}`
    : spec.kind === 'latest' ? `run: 'latest', pipeline: ${quote(spec.pipeline)}, output: ${quote(spec.output)}`
      : `run: ${quote(spec.lid)}, output: ${quote(spec.output)}`
  const full = where ? `${args}, where: ${where}` : args
  return { include: INCLUDE_LINE, call: mode === 'typed' ? `nextflow.Channel.fromStore(${full}, records: true)` : `channel.fromStore(${full})` }
}

/** A snippet with a Copy button; its call line is `<code data-snippet="untyped"|"typed">`. */
export function snippetBlock(spec) {
  const call = h('code', spec.where?.length ? { 'data-where': JSON.stringify(spec.where) } : {})
  const copy = h('button', { type: 'button', onclick: async (event) => {
    const button = event.currentTarget
    button.textContent = await copyOutcome(navigator.clipboard, `${INCLUDE_LINE}\n${call.textContent}`)
  } }, 'Copy')
  const node = h('div', { class: 'snippet' }, h('pre', {}, h('code', {}, INCLUDE_LINE), '\n', call), copy)
  const draw = (mode) => {
    call.dataset.snippet = mode
    call.textContent = snippetLines(spec, mode).call
    copy.textContent = 'Copy'
  }
  draw(current())
  live.add({ node, draw })
  return node
}

/** The one "untyped | typed" choice for every snippet on the page. */
export function snippetToggle() {
  const buttons = MODES.map(mode => h('button', { type: 'button', 'data-snippet-mode': mode, 'aria-pressed': String(current() === mode),
    title: mode === 'typed' ? 'For scripts with nextflow.enable.types: processes get records.' : 'For scripts without nextflow.enable.types.',
    onclick: () => setSnippetMode(mode) }, mode))
  const node = h('span', { class: 'snippet-toggle', role: 'group', 'aria-label': 'script kind' }, buttons[0], ' | ', buttons[1])
  live.add({ node, draw: (mode) => buttons.forEach(b => b.setAttribute('aria-pressed', String(b.dataset.snippetMode === mode))) })
  return node
}
