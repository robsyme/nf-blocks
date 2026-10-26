// web/src/components.js
// The Blockprint kit's components as DOM builders (mockups/01-components.html;
// classes in kit-css.js). Each returns one element built with h(). Facts
// (immutable records) use .fact, .addr and Seals; claims (signed, mutable
// statements) use .claim: never style one like the other.
import { copyOutcome, h } from './html.js'
import { bytes } from './format.js'
import { seal } from './seal.js'
import { dayText, fullWhenText, statusText, statusTone, whenText } from './words.js'
import { isAnon, principalKey } from './identity.js'

/** The kit's short form: `bafy…26ocxq` for a CID, `lid://e25b…b06f` for a lineage id. */
export function shortCid(text) {
  const lid = text.match(/^lid:\/\/(.+)$/)
  if (lid) return lid[1].length > 10 ? `lid://${lid[1].slice(0, 4)}…${lid[1].slice(-4)}` : text
  return text.length > 12 ? `${text.slice(0, 4)}…${text.slice(-6)}` : text
}

/**
 * An address chip: the short CID with its Seal, and a click that copies the
 * full reference (`ref`: a cas:// or lid:// reference, or the CID itself).
 * `data-address` carries the CID (a lid:// text has none).
 */
export function addrChip(text, { ref = text, seal: withSeal = true } = {}) {
  const cid = text.startsWith('lid://') ? null : text
  const label = h('span', { class: 'copy', 'aria-live': 'polite' }, 'copy')
  const chip = h('span', { class: 'addr', 'data-ref': ref, 'data-address': cid, role: 'button', tabindex: '0', title: ref },
    withSeal ? seal(text, { size: 14 }) : null, shortCid(text), ' ', label)
  // The label says what happened, not only the title: a click gives no hover tooltip to read.
  const copy = async () => {
    const said = await copyOutcome(globalThis.navigator?.clipboard, ref)
    chip.setAttribute('title', said)
    label.textContent = said.toLowerCase()
    chip.classList.add('flash')
    setTimeout(() => { chip.setAttribute('title', ref); label.textContent = 'copy'; chip.classList.remove('flash') }, 1200)
  }
  chip.addEventListener('click', copy)
  chip.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); copy() } })
  return chip
}

/** An immutable record: its Seal, a kind label and a name; a link when given `href`. */
export function factTag(kind, label, cid, href = null) {
  return h(href ? 'a' : 'span', { class: 'fact', href }, seal(cid, { size: 16 }), h('span', { class: 'k' }, kind), label)
}

/**
 * A run as a fact: its seal, RUN, its name (a run label: run_name, status,
 * possibly_incomplete), the status in words unless it succeeded whole, then `extra`.
 */
export function runFact(label, completion, extra = null) {
  const word = label?.status && statusTone(label) !== 'good' ? h('span', { class: 'muted' }, ` ${statusText(label).toLowerCase()}`) : null
  return factTag('run', [label?.run_name ?? 'unnamed run', word, extra], completion, completion ? `#/run/${completion}` : null)
}

const isAnonymous = (who) => isAnon({ principal: who })
// The kit's avatar colours.
const TONES = ['rs', 'ak', 'jt']

/** Initials of an email's local part or a name: `ada.lovelace@example.org` gives AL. */
export function initials(who) {
  const parts = String(who).split('@')[0].split(/[\s._-]+/).filter(Boolean)
  if (parts.length === 0) return '?'
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase()
  return (parts[0][0] + parts[1][0]).toUpperCase()
}

/** A principal's avatar colour, the same every time: an FNV-1a hash of `assertedBy`'s comparison form (identity.js principalKey) into the kit's tones, so two spellings samePrincipal matches share a colour. */
export function toneFor(assertedBy) {
  const text = principalKey(assertedBy)
  let hash = 2166136261 >>> 0
  for (let i = 0; i < text.length; i++) hash = Math.imul(hash ^ text.charCodeAt(i), 16777619) >>> 0
  return TONES[hash % TONES.length]
}

/** Who asserted a claim. Anonymous (any case, or nobody) is a dashed question mark. `tone` (rs, ak, jt) overrides toneFor. */
export function avatar(assertedBy, { size = 'sm', tone = null, link = true } = {}) {
  const sized = size === 'md' ? '' : ` ${size}`
  if (isAnonymous(assertedBy)) {
    return link
      ? h('a', { class: `av${sized} anon`, href: '#/identity', title: 'Signed anonymously. Set up your identity', 'aria-label': 'Signed anonymously. Set up your identity' }, '?')
      : h('span', { class: `av${sized} anon`, title: 'No identity configured' }, '?')
  }
  return h('span', { class: `av${sized} ${tone ?? toneFor(assertedBy)}`, title: assertedBy, 'aria-label': assertedBy }, initials(assertedBy))
}

const DAY = 86_400_000

/** How long ago `at` was, briefly: `now`, `40m`, `3h`, `2d`, else a short date (`Sep 1`, with the year when it differs). */
export function age(at, now = Date.now()) {
  const t = typeof at === 'number' ? at : Date.parse(at)
  if (!Number.isFinite(t)) return ''
  const ms = now - t
  if (ms < 60_000) return 'now'
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m`
  if (ms < DAY) return `${Math.floor(ms / 3_600_000)}h`
  if (ms < 7 * DAY) return `${Math.floor(ms / DAY)}d`
  return dayText(t, now)
}

/** `at` as age writes it, the whole date and time on hover; null when `at` is unreadable. */
export function ago(at, now = Date.now(), attrs = {}) {
  const text = at === null || at === undefined ? '' : age(at, now)
  return text ? h('span', { ...attrs, title: fullWhenText(at) }, text) : null
}

/** At most this many characters of a claim value that is not a string (its JSON text). */
const VALUE_TEXT = 60

/** A claim value as its tag shows it: a string quoted, a node as is, anything else as its JSON text, cut to VALUE_TEXT. */
function claimValue(value) {
  if (value === null || value === undefined) return null
  if (typeof Node !== 'undefined' && value instanceof Node) return value
  if (typeof value === 'string') return `“${value}”`
  let text
  try { text = JSON.stringify(value) ?? String(value) } catch { text = String(value) }
  return text.length > VALUE_TEXT ? `${text.slice(0, VALUE_TEXT - 1)}…` : text
}

/**
 * A signed statement: avatar, verb, value (a quoted string, a node as is, else JSON text) and age; a conflict also says so in words. `tone` passes to the avatar.
 * The value has its own span (.val), cut with an ellipsis when the tag meets its box's edge, and titled with its whole text.
 */
export function claimTag({ assertedBy, verb, value = null, at = null, conflict = false, tone = null, now = Date.now(), absolute = false, superseded = false }) {
  const shown = claimValue(value)
  const when = at === null || at === undefined ? '' : absolute ? whenText(at, now) : age(at, now)
  const title = typeof at === 'number' || typeof at === 'string' ? fullWhenText(at) : null
  const full = shown === null ? null : typeof shown === 'string' ? shown : shown.textContent
  return h('span', { class: `claim${conflict ? ' conflict' : ''}${superseded ? ' old' : ''}`, title: superseded ? 'Superseded' : null },
    avatar(assertedBy, { size: 'sm', tone }), h('span', { class: 'verb' }, verb), shown === null ? null : h('span', { class: 'val', title: full }, shown),
    conflict ? ' · conflicts' : null,
    when ? h('span', { class: 'age', title }, when) : null)
}

/**
 * Which stores hold a block: a filled dot where it is held, a ring where it is
 * only referenced. `stores`: [{ key: 'a'|'b'|'c', name, held }].
 */
export function presence(stores) {
  const held = stores.filter(s => s.held).map(s => s.name)
  const referenced = stores.filter(s => !s.held).map(s => s.name)
  const title = [held.length ? `Held in ${held.join(', ')}` : null, referenced.length ? `referenced in ${referenced.join(', ')}` : null]
    .filter(Boolean).join('; ')
  return h('span', { class: 'presence', title, 'aria-label': title, role: 'img' },
    stores.map(s => h('span', { class: `dot${s.held ? '' : ' ring'} ${s.key}` })))
}

/** Will it survive the next sweep in this store: 'pinned' | 'held' | 'releasing' | 'missing'. */
export const durability = (state, text) => h('span', { class: `state s-${state}` }, text)

const STATUS_ICONS = {
  ok: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="m8 12 3 3 5-6"/></svg>',
  fail: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="m9 9 6 6m0-6-6 6"/></svg>',
  neutral: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="12" cy="12" r="9" stroke-dasharray="3 3"/></svg>',
  partial: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 7.5v5.5M12 16.5v.01"/></svg>',
}

/**
 * A run's status: icon and word, never colour alone. Succeeded and failed
 * take statusTone's ok or fail (a possibly incomplete success is fail, with
 * its own icon); any other status (running, unknown, none) is neutral.
 * `label` replaces the word where the column header already says it.
 */
export function status(run, label = statusText(run ?? {})) {
  const settled = run?.status === 'succeeded' || run?.status === 'failed'
  const tone = !settled ? 'neutral' : statusTone(run) === 'good' ? 'ok' : 'fail'
  const ico = h('span', { class: 'ico' })
  ico.innerHTML = STATUS_ICONS[tone === 'fail' && run.status === 'succeeded' ? 'partial' : tone]
  return h('span', { class: `status ${tone}` }, ico, label)
}

/** A kind of output file and how many: `bam×5`. */
export const kindChip = (ext, count) => h('span', { class: 'kind' }, ext, h('b', {}, `×${count}`))

/** A promoted field and its formatted value. */
export const fieldChip = (label, valueText, { title = null } = {}) =>
  h('span', { class: 'field', title }, h('span', { class: 'k' }, label), h('span', { class: 'v' }, valueText))

/** Files from one tool, with their extensions and counts: `fastqc zip×6 html×6`. */
export function fileChip(label, exts, { href = null, title = null } = {}) {
  return h(href ? 'a' : 'span', { class: 'fileb', href, title }, label, ' ',
    h('span', { class: 'ext' }, exts.map(([ext, count]) => `${ext}×${count}`).join(' ')))
}

/** A button. `kind`: 'default' | 'primary' | 'ghost' | 'danger'; `size`: 'md' | 'sm'; other attributes pass to h(). */
export function button(label, { kind = 'default', size = 'md', class: extra = null, ...attrs } = {}) {
  const cls = ['btn', kind === 'default' ? null : kind, size === 'sm' ? 'sm' : null, extra].filter(Boolean).join(' ')
  return h('button', { type: 'button', ...attrs, class: cls }, label)
}

/** An explanation, then what to do. `tone`: 'info' | 'ok' | 'warn' | 'bad'. */
export const note = (text, tone = 'info') => h('div', { class: tone === 'info' ? 'note' : `note ${tone}` }, h('span', {}, text))

/** An empty state: what is missing, then why or what to do. */
export const empty = (title, text) => h('div', { class: 'empty' }, h('b', {}, title), text)

/** Crumbs, the title row (Seal, h1, extras, actions on the right) and the subline. */
export function pageHead({ crumbs = [], seal: cid = null, title, after = [], actions = [], subline = [] }) {
  const trail = crumbs.flatMap((c, i) => [i ? '/' : null, c.href ? h('a', { href: c.href }, c.text) : h('b', {}, c.text)])
  return h('div', { class: 'page-head' },
    crumbs.length ? h('nav', { class: 'crumbs', 'aria-label': 'Breadcrumb' }, trail) : null,
    h('div', { class: 'title' }, cid ? seal(cid, { size: 30 }) : null, h('h1', {}, title), after,
      actions.length ? [h('span', { class: 'spacer' }), actions] : null),
    subline.length ? h('div', { class: 'subline' }, subline.map(s => h('span', {}, s))) : null)
}

/** Tabs: [{ key, label, href, count?, on }]. */
export function tabs(items, { label = null } = {}) {
  return h('nav', { class: 'tabs', 'aria-label': label },
    items.map(t => h('a', { href: t.href, class: t.on ? 'on' : null, 'data-tab': t.key, 'aria-current': t.on ? 'page' : null },
      t.label, t.count === undefined || t.count === null ? null : h('small', {}, String(t.count)))))
}

/** A panel: a head (title, meta, actions) when any is given, then the body. `flush` drops the body's padding, for tables. */
export function panel({ title = null, meta = null, actions = [], flush = false } = {}, ...body) {
  const head = title || meta || actions.length
    ? h('div', { class: 'ph' }, title ? h('h2', {}, title) : null, meta ? h('span', { class: 'muted' }, meta) : null,
      actions.length ? [h('span', { class: 'spacer' }), actions] : null)
    : null
  return h('section', { class: flush ? 'panel flush' : 'panel' }, head, h('div', { class: 'pb' }, body))
}

/** A definition list: [[term, value]]. */
export const kv = (pairs) => h('dl', { class: 'kv' }, pairs.map(([dt, dd]) => [h('dt', {}, dt), h('dd', {}, dd)]))

/** A small bar for a fraction between 0 and 1. */
export function minibar(fraction) {
  const pct = Math.max(0, Math.min(1, Number(fraction) || 0)) * 100
  return h('span', { class: 'minibar', role: 'img', 'aria-label': `${Math.round(pct)}%` }, h('span', { style: `width:${pct.toFixed(1)}%` }))
}

/** A lineage thread: inputs at the top, `here` highlighted, `ext` (not stored) dashed. */
export function thread(nodes) {
  return h('div', { class: 'thread' },
    nodes.map(n => h('div', { class: ['node', n.here ? 'here' : null, n.ext ? 'ext' : null].filter(Boolean).join(' ') },
      n.href ? h('a', { href: n.href }, n.text) : n.here ? h('b', {}, n.text) : n.text)))
}

const share = (part, total) => `${(part / total * 100).toFixed(2)}%`

/**
 * One store's bytes, stacked pinned / held / releasing, then an optional thin
 * bar for the reader's own share. Sizes in bytes; `store` is its .dot key.
 */
export function storageBar({ total, pinned = 0, held = 0, releasing = 0, yours = null, store = null, storeName = null }) {
  const t = total || 1
  return h('div', { class: 'sbar' },
    store ? h('div', { class: 'storehd' }, h('span', { class: `dot ${store}` }), storeName, ' ', h('span', { class: 'muted', style: 'font-weight:400' }, `· ${bytes(total)}`)) : null,
    h('div', { class: 'track', role: 'img', 'aria-label': `${bytes(pinned)} pinned, ${bytes(held)} held, ${bytes(releasing)} releasing, of ${bytes(t)}` },
      h('div', { class: 'seg pinned', style: `width:${share(pinned, t)}` }), h('div', { class: 'seg held', style: `width:${share(held, t)}` }),
      h('div', { class: 'seg releasing', style: `width:${share(releasing, t)}` })),
    h('div', { class: 'legend' },
      h('span', {}, h('i', { style: 'background:var(--pinned)' }), `Pinned ${bytes(pinned)}`),
      h('span', {}, h('i', { style: 'background:var(--held);opacity:.55' }), `Held ${bytes(held)}`),
      h('span', {}, h('i', { style: 'background:repeating-linear-gradient(135deg,var(--releasing) 0 3px,transparent 3px 5px)' }), `Releasing ${bytes(releasing)}`)),
    yours === null || yours === undefined ? null : h('div', { class: 'mine' }, h('b', {}, 'Yours'),
      h('span', { class: 'mt', role: 'img', 'aria-label': `yours ${bytes(yours)}` }, h('span', { style: `width:${share(yours, t)}` })),
      h('span', { class: 'num' }, `${bytes(yours)} · ${Math.round(yours / t * 100)}%`)))
}
