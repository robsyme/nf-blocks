// web/src/snippets.js
// The consumer code the Selection and run pages offer (DESIGN.md §16
// decision 13, ticket 07 Q4): the include line and one fromStore call,
// untyped or typed. The mode is one choice for every snippet on the page,
// remembered per viewer in localStorage where the browser allows it. The
// Gate runs these call lines verbatim (decision 14).
import { h, copyOutcome } from './html.js'
import { predicateRow } from './metadata.js'

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
const LIVE_LIMIT = 64
/** How many snippets and toggles are tracked for a mode change (tests). */
export const liveCount = () => live.size

// A panel redraw builds new snippets and drops the old nodes; without this the
// set would keep every one. Only past a limit, and only nodes the browser says
// left the page, so the snippets of the draw in progress are never lost.
function track(entry) {
  if (live.size >= LIVE_LIMIT) for (const shown of live) if (shown.node.isConnected === false) live.delete(shown)
  live.add(entry)
}
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
    case 'int': return predicateRow('', 'int', String(value)).value
    case 'float': return `${predicateRow('', 'float', String(value)).value}d`
    case 'bool': return String(value)
    case 'null': return 'null'
    default: throw new Error(`unknown type '${type}'`)
  }
}

/** A condition's `where:` key: the path, or for a range the path and its operator (`'depth >='`, Conditions.splitKey). */
const whereKey = ([p, , , op]) => (op ? `${p} ${op}` : p)

/** True when two conditions share a key: a Groovy map keeps one value per key, so `where:` cannot say it. */
export const whereRepeatsPath = (where) => new Set((where ?? []).map(whereKey)).size < (where?.length ?? 0)

/** `[k: v, ...]` for the chips, or null when there are none. */
export const whereLiteral = (where) => (where?.length ? `[${where.map(([p, t, v, op]) => `${op ? quote(whereKey([p, t, v, op])) : groovyKey(p)}: ${groovyValue(t, v)}`).join(', ')}]` : null)

/** The two lines of a snippet: the include, and the call for `mode`. */
export function snippetLines(spec, mode) {
  const where = whereLiteral(spec.where)
  const args = spec.kind === 'selection' ? `selection: ${quote(spec.cid)}`
    : spec.kind === 'latest' ? `run: 'latest', pipeline: ${quote(spec.pipeline)}, output: ${quote(spec.output)}`
      : `run: ${quote(spec.run)}, output: ${quote(spec.output)}`
  const full = where ? `${args}, where: ${where}` : args
  return { include: INCLUDE_LINE, call: mode === 'typed' ? `nextflow.Channel.fromStore(${full}, records: true)` : `channel.fromStore(${full})` }
}

/** An output name as a Groovy variable: other characters become `_`, a leading digit or a reserved word gets one too. */
const variableOf = (name) => {
  const v = String(name).replace(/[^A-Za-z0-9_]/g, '_')
  return /^[0-9]/.test(v) ? `_${v}` : KEYWORDS.has(v) ? `${v}_` : v || '_'
}

/**
 * The Samples tab's hand-off as a script (P1a): the samples by `where`, or by
 * `ids` when given (ticks by hand, or a filter a Groovy map cannot hold),
 * each per-sample output in `outputs` from that handle, each run-level one
 * in `runLevel` as today's run and output call. `samples:` is a proposal
 * (docs/design/adr/0001-fromstore-samples.md), so the script says so and
 * the page never puts it where the Gate runs snippets.
 */
export function samplesSnippet({ completion, where = [], ids = null, outputs = [], runLevel = [] }) {
  const run = quote(`cas://${completion}`)
  const samples = ids ? `[ids: [${ids.map(quote).join(', ')}]]` : `[where: ${whereLiteral(where) ?? '[:]'}]`
  const lines = [INCLUDE_LINE, '',
    '// Proposed API, not in the plugin yet: docs/design/adr/0001-fromstore-samples.md',
    'samples = channel.fromStore(', `  run: ${run},`, `  samples: ${samples})`]
  // One variable per output: `samples` is the handle, `channel` reads the run-level outputs, and names that clean up alike get _2, _3, ...
  const taken = new Set(['samples', 'channel'])
  const variable = (o) => {
    const base = variableOf(o)
    let v = base
    for (let n = 2; taken.has(v); n++) v = `${base}_${n}`
    taken.add(v)
    return v
  }
  const calls = [...outputs.map(o => `${variable(o)} = samples.output(${quote(o)})`),
    ...runLevel.map(o => `${variable(o)} = channel.fromStore(run: ${run}, output: ${quote(o)})`)]
  return (calls.length ? [...lines, '', ...calls] : lines).join('\n')
}

// INCLUDE_LINE with the kit's keyword and string colours; its text is the line itself.
const includeNode = () => h('code', {}, h('span', { class: 'k' }, 'include'), ' { fromStore } ', h('span', { class: 'k' }, 'from'), ' ',
  h('span', { class: 's' }, "'plugin/nf-blocks'"))

/**
 * A snippet as the kit draws it (mockups 03/04/08): a `pre.code` block, the
 * include line and the call a blank line apart, then a full-width "Copy
 * snippet". The call line alone is `<code data-snippet="untyped"|"typed">`,
 * which the Gate runs verbatim.
 */
export function snippetBlock(spec) {
  const call = h('code', spec.where?.length ? { 'data-where': JSON.stringify(spec.where) } : {})
  const copy = h('button', { type: 'button', class: 'btn sm snippet-copy', onclick: async (event) => {
    const button = event.currentTarget
    button.textContent = await copyOutcome(navigator.clipboard, `${INCLUDE_LINE}\n\n${call.textContent}`)
  } }, 'Copy snippet')
  const node = h('div', { class: 'snippet' }, h('pre', { class: 'code' }, includeNode(), '\n\n', call), copy)
  const draw = (mode) => {
    call.dataset.snippet = mode
    call.textContent = snippetLines(spec, mode).call
    copy.textContent = 'Copy snippet'
  }
  draw(current())
  track({ node, draw })
  return node
}

/** Marks the pressed button of a seg-toggle: `on` for the kit's look, aria-pressed for the reader. */
export function pressSeg(buttons, isOn) {
  for (const b of buttons) {
    const on = isOn(b)
    b.setAttribute('aria-pressed', String(on))
    if (on) b.classList.add('on')
    else b.classList.remove('on')
  }
}

/** The one Untyped | Typed choice for every snippet on the page, as the kit's seg-toggle. */
export function snippetToggle() {
  const buttons = MODES.map(mode => h('button', { type: 'button', class: 'seg', 'data-snippet-mode': mode,
    title: mode === 'typed' ? 'For scripts with nextflow.enable.types: processes get records.' : 'For scripts without nextflow.enable.types.',
    onclick: () => { setSnippetMode(mode); draw(mode) } }, mode === 'typed' ? 'Typed' : 'Untyped'))
  function draw(mode) { pressSeg(buttons, b => b.dataset.snippetMode === mode) }
  draw(current())
  const node = h('div', { class: 'seg-toggle snippet-toggle', role: 'group', 'aria-label': 'script kind' }, buttons)
  track({ node, draw })
  return node
}
