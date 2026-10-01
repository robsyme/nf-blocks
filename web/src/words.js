// web/src/words.js
// The page's plain words for store terms (explorer layout B spec §8). DESIGN.md
// §6 defines each anomaly and Leaf reason: change a meaning there first.

const counted = (n, one, many) => `${n} ${n === 1 ? one : many}`

// Concerns first, then the expected ones; each line's text is the spec §8 table's.
const ANOMALIES = [
  { key: 'unjoined', concern: true, text: (n) => `${counted(n, 'published file', 'published files')} that ${n === 1 ? 'is' : 'are'} in no output` },
  { key: 'unresolvable', concern: true, text: (n) => `${counted(n, 'link', 'links')} whose target could not be found` },
  { key: 'unaddressed', concern: true, text: (n) => `${counted(n, 'file', 'files')} with no stored content` },
  { key: 'declined', concern: false, text: (n) => `${counted(n, 'empty optional slot', 'empty optional slots')} (the pipeline passed no file)` },
  { key: 'never_published', concern: false, text: (n) => `${counted(n, 'referenced file', 'referenced files')} not stored here, such as input files` },
]

/** One line per non-zero anomaly, concerns first. A missing count (unjoined on an older block) reads as 0. */
export function anomalyLines(anomalies) {
  return ANOMALIES.map(a => ({ key: a.key, count: Number(anomalies?.[a.key] ?? 0), concern: a.concern, text: null, a }))
    .filter(l => l.count > 0)
    .map(({ a, ...l }) => ({ ...l, text: a.text(l.count) }))
}

/** The Lineage check summary line. */
export const healthSummary = (anomalies) => (anomalyLines(anomalies).some(l => l.concern) ? 'worth a look' : 'nothing to check')

/** The pipeline page's health cell: the concerns, or "nothing to check". */
export function healthText(anomalies) {
  const concerns = anomalyLines(anomalies).filter(l => l.concern)
  return concerns.length ? concerns.map(l => l.text).join('; ') : 'nothing to check'
}

const capital = (s) => (s ? s[0].toUpperCase() + s.slice(1) : '')

export function statusText({ status, possibly_incomplete }) {
  const base = capital(String(status ?? 'unknown'))
  return possibly_incomplete ? `${base}, may be incomplete` : base
}

export const statusTone = ({ status, possibly_incomplete }) => (status === 'succeeded' && !possibly_incomplete ? 'good' : 'bad')

const UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

/** Decimal units, as file browsers show them. */
export function humanBytes(n) {
  if (n === null || n === undefined || n === '') return ''
  let v = Number(n)
  let i = 0
  while (v >= 1000 && i < UNITS.length - 1) { v /= 1000; i++ }
  return i === 0 ? `${v} B` : `${v < 10 ? v.toFixed(1) : Math.round(v)} ${UNITS[i]}`
}

export function whenText(iso) {
  if (!iso) return ''
  const d = new Date(iso)
  return Number.isNaN(d.getTime()) ? String(iso) : d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
}

const REASONS = {
  declined: 'no file (an optional output was empty)',
  never_published: 'not stored here (such as an input file)',
  unresolvable: 'a link whose target could not be found',
  unaddressed: 'no stored content',
}

export const leafReasonText = (reason) => REASONS[reason] ?? String(reason ?? '')
