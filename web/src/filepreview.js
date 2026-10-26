// web/src/filepreview.js
// What a file page and an item page show of a file's bytes: a table for
// TSV and CSV, numbered lines for text, a click-to-open frame for HTML, and
// nothing for anything else. Pure over the bytes; the views draw the result.
import { extOf } from './sizes.js'

export const PREVIEW_BYTES = 65536
const TABLE = new Map([['tsv', '\t'], ['csv', ',']])
const TEXT = new Set(['txt', 'log', 'yml', 'yaml', 'json', 'out', 'tab'])


/** True for a name whose bytes can be previewed (tables, text, html). */
export const isPreviewable = (name) => {
  const ext = extOf(name)
  return TABLE.has(ext) || TEXT.has(ext) || ext === 'html'
}

/** The file kind as the page names it: the last extension in capitals, or FILE. */
export const kindOf = (name) => extOf(name).toUpperCase() || 'FILE'

/** Fields of delimited text; a quoted field may hold the delimiter and "" for a quote. */
function parseDelimited(text, delimiter) {
  const rows = []
  let row = []
  let field = ''
  let quoted = false
  for (let i = 0; i < text.length; i++) {
    const c = text[i]
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++ } else if (c === '"') quoted = false
      else field += c
    } else if (c === '"' && field === '') quoted = true
    else if (c === delimiter) { row.push(field); field = '' }
    else if (c === '\n') { row.push(field.replace(/\r$/, '')); rows.push(row); row = []; field = '' }
    else field += c
  }
  if (field !== '' || row.length) { row.push(field.replace(/\r$/, '')); rows.push(row) }
  return rows
}

/**
 * Which of `sampleIds` a count matrix's header row holds as columns, in
 * header order, each once. `text` is the file's first bytes (PREVIEW_BYTES
 * is plenty for a header); tab-separated when the header has a tab, else
 * comma-separated. A header with no newline was cut, so its last cell is
 * not counted.
 */
export function matrixSamples(text, sampleIds) {
  const end = text.indexOf('\n')
  const line = (end < 0 ? text : text.slice(0, end)).replace(/\r$/, '')
  const cells = parseDelimited(line, line.includes('\t') ? '\t' : ',')[0] ?? []
  if (end < 0) cells.pop()
  const wanted = new Set(sampleIds)
  return [...new Set(cells.filter(c => wanted.has(c)))]
}

/** The `highlight` of a file page URL: a JSON list of sample ids; anything else highlights nothing. */
export function highlightOf(text) {
  try {
    const list = JSON.parse(text ?? '')
    return Array.isArray(list) ? list.filter(x => typeof x === 'string') : []
  } catch {
    return []
  }
}

/**
 * `{ kind: 'table', head, rows, totalRows, totalCols, truncated }` for
 * TSV/CSV (the first 64 KB parsed; `totalRows` counts data rows in those
 * bytes, so it is a lower bound when `truncated`), `{ kind: 'text', lines,
 * truncated }`, `{ kind: 'html' }` or `{ kind: 'binary' }`.
 */
export function previewOf(name, bytes, { maxRows = 50 } = {}) {
  const ext = extOf(name)
  if (ext === 'html') return { kind: 'html' }
  const delimiter = TABLE.get(ext)
  if (!delimiter && !TEXT.has(ext)) return { kind: 'binary' }
  const truncated = bytes.length > PREVIEW_BYTES
  let text
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, PREVIEW_BYTES), { stream: truncated })
  } catch { return { kind: 'binary' } }
  if (text.includes('\0')) return { kind: 'binary' }
  // A cut file ends mid-line: leave that line out rather than show half a row.
  // With no newline in the whole 64 KB there is no whole line to keep: show the cut text as it is.
  if (truncated && text.includes('\n')) text = text.slice(0, text.lastIndexOf('\n') + 1)
  else if (truncated) return { kind: 'text', lines: [text], truncated }
  if (!delimiter) {
    const lines = text.split(/\r?\n/)
    if (lines.at(-1) === '') lines.pop()
    return { kind: 'text', lines, truncated }
  }
  const all = parseDelimited(text, delimiter).filter(r => !(r.length === 1 && r[0] === ''))
  const [head = [], ...data] = all
  return { kind: 'table', head, rows: data.slice(0, maxRows), totalRows: data.length, totalCols: head.length, truncated }
}
