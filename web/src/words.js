// web/src/words.js
// The page's plain words for store terms (explorer layout B spec §8). DESIGN.md
// §6 defines each anomaly and Leaf reason: change a meaning there first.
import { bytes } from './format.js'
import { SCHEMA_VERSION } from './config.js'

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

const count = (anomalies, key) => Number(anomalies?.[key] ?? 0)

/**
 * One line of run health: a fact, then what to do about it. `act` is true when
 * the reader can fix something. Null when there is nothing to say. Declined
 * optional slots are routine and stay out of it.
 */
export function healthLine(anomalies) {
  const unjoined = count(anomalies, 'unjoined')
  const unresolvable = count(anomalies, 'unresolvable')
  const unaddressed = count(anomalies, 'unaddressed')
  const never = count(anomalies, 'never_published')
  const parts = []
  if (unjoined > 0) {
    parts.push(`${counted(unjoined, 'published file is', 'published files are')} outside any workflow output. Declare ${unjoined === 1 ? 'it' : 'them'} in the output { } block to keep ${unjoined === 1 ? 'its' : 'their'} lineage.`)
  }
  if (unresolvable > 0) parts.push(`${counted(unresolvable, 'link points', 'links point')} at content that could not be found.`)
  if (unaddressed > 0) parts.push(`${counted(unaddressed, 'file has', 'files have')} no stored content.`)
  const act = parts.length > 0
  if (never > 0) {
    parts.push(act ? `${counted(never, 'input is', 'inputs are')} referenced, not stored.`
      : 'Inputs are referenced, not stored. Expected for local inputs.')
  }
  return parts.length ? { text: parts.join(' '), act } : null
}

/** The health line every run shares, or null when the runs differ or none has anything to say. `completions` are RunCompletion values (or their anomalies). */
export function sharedHealth(completions) {
  if (!completions?.length) return null
  // Runs share a line when they would say the same thing: an optional-slot count that differs (declined) is not part of it.
  const signature = (c) => healthLine(c?.anomalies ?? c)?.text ?? ''
  const first = signature(completions[0])
  if (!completions.every(c => signature(c) === first)) return null
  return healthLine(completions[0]?.anomalies ?? completions[0])
}

const capital = (s) => (s ? s[0].toUpperCase() + s.slice(1) : '')

export function statusText({ status, possibly_incomplete }) {
  const base = capital(String(status ?? 'unknown'))
  return possibly_incomplete ? `${base}, may be incomplete` : base
}

export const statusTone = ({ status, possibly_incomplete }) => (status === 'succeeded' && !possibly_incomplete ? 'good' : 'bad')

/** A run icon's tone: statusTone when the run's status is known, else null (the icon stays grey). */
export const runTone = (run) => (run?.status ? statusTone(run) : null)

/** Decimal units, as file browsers show them (format.js `bytes`); nothing for an unknown size. */
export function humanBytes(n) {
  if (n === null || n === undefined || n === '') return ''
  return bytes(Number(n))
}

/**
 * The page's one date-and-time: `Oct 24, 1:29 PM` in the reader's zone, with
 * the year as dayText adds it. Day and time are formatted apart because some
 * browsers write the one-call form as `Oct 24 at 1:29 PM`. `now` is the
 * caller's clock, as dayText's; views without an Explorer at hand leave it out.
 */
export function whenText(at, now = Date.now()) {
  if (at === null || at === undefined || at === '') return ''
  const day = dayText(at, now)
  return day ? `${day}, ${timeText(at)}` : String(at)
}

/** whenText with the year always: `Oct 24, 2026, 1:29 PM`, the title of every relative time (`11m`, `2 days ago`). */
export function fullWhenText(at) {
  if (at === null || at === undefined || at === '') return ''
  const d = new Date(at)
  if (Number.isNaN(d.getTime())) return String(at)
  return `${d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}, ${timeText(d)}`
}

/** `1:29 PM` in the reader's zone, with a plain space before AM/PM. */
export const timeText = (at) => new Date(at).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' }).replace(/\s/g, ' ')

const REASONS = {
  declined: 'no file (an optional output was empty)',
  never_published: 'not stored here (such as an input file)',
  unresolvable: 'a link whose target could not be found',
  unaddressed: 'no stored content',
}

export const leafReasonText = (reason) => REASONS[reason] ?? String(reason ?? '')

/** How long ago `iso` was, for the left column's footer: `just now`, `2 min ago`, `3 h ago`, `2 days ago`, then the date. */
export function agoText(iso, now = Date.now()) {
  const t = Date.parse(iso ?? '')
  if (!Number.isFinite(t)) return ''
  const min = Math.floor((now - t) / 60_000)
  if (min < 1) return 'just now'
  if (min < 60) return `${min} min ago`
  if (min < 24 * 60) return `${Math.floor(min / 60)} h ago`
  const days = Math.floor(min / (24 * 60))
  if (days < 30) return `${days} day${days === 1 ? '' : 's'} ago`
  return dayText(t, now)
}

/** The footer's count of runs newer than the snapshot; an unreadable Store Log means the count is unknown, not 0. */
export function staleText({ staleCount, logReadable }) {
  const count = `${staleCount} newer run${staleCount === 1 ? '' : 's'}`
  return logReadable ? count : `newer runs unknown: the Store Log is not readable here${staleCount ? ` (${staleCount} found)` : ''}`
}

/** The footer's second line: the index version the page reads and how it reads it. */
export const snapshotModeText = (mode) => `Index v${SCHEMA_VERSION} · ${mode === 'whole' ? 'downloaded whole (this server ignores Range)' : 'read by range'}`

/**
 * A day as the storage figures write it: `Oct 31`, with the year when it is
 * not the year of `now`. `at` is an ISO string or ms; an unreadable one gives
 * ''. `now` is the caller's clock (ms or a Date), the Explorer's (`ex.now()`)
 * on every page, so a page and its tests agree on what "this year" is.
 */
export function dayText(at, now) {
  const d = new Date(at ?? '')
  if (Number.isNaN(d.getTime())) return ''
  const opts = d.getFullYear() === new Date(now).getFullYear() ? { month: 'short', day: 'numeric' } : { month: 'short', day: 'numeric', year: 'numeric' }
  return d.toLocaleDateString('en-US', opts)
}
