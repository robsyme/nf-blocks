// web/src/views/search.js
// #/search?q= and the top bar's search box (P1a Task 4; mockups/src/pages/
// 12-search.html). Search v1 reads the snapshot only: sample IDs
// (Explorer.searchSamples) and addresses (searchAddresses). An address that
// matches exactly one CID opens its page. Names of files, outputs, runs and
// Selections need P2's index and are not searched yet; the page says so.
// In all-stores mode (?member=*, P3 rule D5) every member that opens is
// searched; a result found in several is one row, opening in the first, with
// dots for the stores whose indexes list it.
import { h, link } from '../html.js'
import { icon } from '../icons.js'
import { claimTag, pageHead, panel, shortCid, tabs } from '../components.js'
import { formatValue, sizeText } from '../format.js'
import { ADDRESS_HEAD, ADDRESS_MIN, isAddressQuery, namedBy } from '../model.js'
import { SAMPLE_PATHS } from '../table.js'
import { pairsOf } from '../pairs.js'
import { matchPromoted, pipelineConfig, promotedFor } from '../promoted.js'
import { runTone, statusText, statusTone } from '../words.js'
import { enc } from './common.js'
import { fromEach, hrefIn, listsAll, mergeRows, unreadableText } from '../members.js'
import { presenceOf } from '../presence.js'
import { queryOf } from '../routequery.js'

/** How long the box waits after the last keystroke, and how short a query it ignores. */
export const TYPEAHEAD_WAIT = 150
export const TYPEAHEAD_MIN = 2
const TYPEAHEAD_EACH = 3
/** Rows one typeahead lookup reads: few, so the index walk stops early. */
const TYPEAHEAD_ROWS = 30
/** Rows per group on the All tab before "N more"; each group's own tab shows every row. */
const ALL_TAB = { samples: 10, items: 5 }
/** Items whose Selections are looked up (one selectionsHolding query each). */
const SELECTION_LOOKUPS = 50
/** Sample rows whose promoted values are read (one item block per output-qualified field each). */
const PROMOTED_ROWS = 50

export const searchHref = (q, tab = null) => `#/search?q=${enc(q)}${tab ? `&tab=${tab}` : ''}`
/** A run's Samples tab filtered to one sample (the filter codec of P1a Task 2: `[[key, type, value]]`). */
export const samplesHref = (completion, path, sample) =>
  `#/run/${completion}?tab=samples&where=${enc(JSON.stringify([[path, 'string', sample]]))}`
const itemHref = (r) => `#/item/${r.collection_cid}/${r.item_cid}`
const rank = (path) => SAMPLE_PATHS.indexOf(path)
const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`
const sub = (text) => h('span', { class: 'sub' }, text)

/** `Oct 24`, with the year when it is not this year's. */
function dayText(iso, now = Date.now()) {
  const d = new Date(iso)
  if (!iso || Number.isNaN(d.getTime())) return ''
  const year = d.getUTCFullYear() === new Date(now).getUTCFullYear() ? {} : { year: 'numeric' }
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC', ...year })
}

/**
 * searchSamples rows as one entry per run and sample, latest run first:
 * `{ sample, path, run, rows, outputs, href }`. `path`, which the Samples tab
 * link filters on, is the first path of the pipeline's join key that the
 * sample's rows matched, so the filter uses the path the tab joins on; with
 * none, the matched path SAMPLE_PATHS names first. `outputs` counts the
 * outputs holding the sample.
 */
export function sampleGroups(rows) {
  const groups = new Map()
  for (const r of rows) {
    const k = `${r.completion_cid}\u0000${r.sample}`
    const g = groups.get(k) ?? { sample: r.sample, run: r, rows: [], matched: new Set(), outputs: new Set() }
    g.rows.push(r)
    for (const p of r.paths ?? [r.path]) g.matched.add(p)
    g.outputs.add(r.output_name)
    groups.set(k, g)
  }
  return [...groups.values()].map(({ matched, ...g }) => {
    const path = pipelineConfig(g.run.pipeline).joinKey.find(p => matched.has(p)) ?? [...matched].sort((a, b) => rank(a) - rank(b))[0]
    return { ...g, path, outputs: g.outputs.size, href: samplesHref(g.run.completion_cid, path, g.sample) }
  })
}

/** A promoted value as short text: `99,566 reads (trimmed)`, `89% mapped`. `entry` is a pairsOf entry with one value. */
export function promotedText(field, entry) {
  const value = formatValue(entry.type, entry.values[0], field)
  const label = field.label.toLowerCase()
  return field.format === 'percent' && label.startsWith('% ') ? `${value} ${label.slice(2)}` : `${value} ${label}`
}

/**
 * The sample's output-qualified promoted values in this run, joined with
 * ` · `: each read from the sample's item in that field's output (the
 * searchSamples rows name it). A field whose item cannot be read, or has no
 * single value there, is left out.
 */
async function promotedLine(ex, g) {
  const fields = promotedFor(g.run.pipeline).filter(f => f.output)
  const parts = await Promise.all(fields.map(async (field) => {
    const r = g.rows.find(x => x.output_name === field.output)
    if (!r) return null
    try {
      const pairs = pairsOf((await ex.itemBlock(r.collection_cid, r.item_cid)).view)
      const [m] = matchPromoted([field], pairs.map(e => e.path))
      const e = m ? pairs.find(p => p.path === m.path) : null
      return e && e.values.length === 1 && e.type !== 'mixed' ? promotedText(field, e) : null
    } catch {
      return null
    }
  }))
  return parts.filter(Boolean).join(' · ')
}

const ICONS = { run: 'run', output: 'output', selection: 'selection', item: 'item', file: 'file' }
const KIND_WORDS = { run: 'run', output: 'output', selection: 'Selection', item: 'item', file: 'file' }

function runCell(run, at) {
  const good = run.status && statusTone(run) === 'good'
  return h('td', {}, h('span', { class: 'runname', title: statusText(run) },
    icon('run', { tone: runTone(run) }), at.link(run, `#/run/${run.completion_cid}`, run.run_name ?? shortCid(run.completion_cid)),
    at.dots(run, run.completion_cid)),
  good ? null : sub(` · ${statusText(run)}`))
}

function promotedCell(at, g, read) {
  const cell = h('td', { class: 'num', 'data-promoted': '' })
  if (!read || !promotedFor(g.run.pipeline).some(f => f.output)) return cell
  cell.append(sub('…'))
  promotedLine(at.ex(g.run), g).then(text => cell.replaceChildren(text), () => cell.replaceChildren())
  return cell
}

function samplesPanel(at, groups, query, { cap = null, href }) {
  const runs = new Set(groups.map(g => g.run.completion_cid)).size
  const ids = new Set(groups.map(g => g.sample)).size
  const meta = groups.length === 0 ? null
    : ids === 1 ? (runs === 1 ? 'in 1 run' : `same sample ID in ${runs} runs`) : `${ids} sample IDs in ${plural(runs, 'run')}`
  const shown = cap ? groups.slice(0, cap) : groups
  const body = groups.length === 0
    ? h('p', { class: 'muted pad' }, `No sample ID starts with "${query}".`)
    : h('div', { class: 'tbox' }, h('table', { class: 't' }, h('tbody', {},
      shown.map((g, i) => h('tr', { 'data-sample': g.sample, 'data-run': g.run.completion_cid },
        h('td', {}, h('div', { class: 'namecell' }, at.link(g.run, g.href, g.sample))),
        runCell(g.run, at),
        promotedCell(at, g, i < PROMOTED_ROWS),
        h('td', { class: 'muted' }, plural(g.outputs, 'output')),
        h('td', { class: 'muted r nowrap' }, dayText(g.run.finished_at)))),
      groups.length > shown.length
        ? h('tr', { class: 'group' }, h('td', { colspan: '5' }, link(href, `${groups.length - shown.length} more`)))
        : null)))
  return h('div', { 'data-search-group': 'samples' }, panel({ title: 'Samples', meta, flush: true }, body))
}

function itemsPanel(at, rows, { cap = null, href }) {
  const shown = cap ? rows.slice(0, cap) : rows
  const body = h('div', { class: 'tbox' }, h('table', { class: 't' }, h('tbody', {},
    shown.map((r) => {
      const size = h('td', { class: 'r num nowrap', 'data-size-for': r.item_cid }, sub('…'))
      Promise.resolve().then(() => at.ex(r).itemSize(r.item_cid))
        .then(s => size.replaceChildren(sizeText(s)), () => size.replaceChildren(sub('?')))
      return h('tr', { 'data-item': r.item_cid, 'data-collection': r.collection_cid },
        h('td', {}, at.link(r, itemHref(r), r.output_name), ` · ${r.sample}`, at.dots(r, r.item_cid)),
        h('td', { class: 'muted' }, r.run_name ?? shortCid(r.completion_cid)),
        size)
    }),
    rows.length > shown.length
      ? h('tr', { class: 'group' }, h('td', { colspan: '3' }, link(href, `${rows.length - shown.length} more items`)))
      : null)))
  return h('div', { 'data-search-group': 'items' }, panel({ title: 'Items', meta: 'latest run first', flush: true }, body))
}

/** One claim tag per name claim (`named` as Explorer.searchAddresses gives it); several current names each say they conflict. */
const nameTags = (named) => (named ?? []).map(n =>
  claimTag({ assertedBy: n.asserted_by, verb: 'named', at: n.timestamp, conflict: n.conflicted }))

/**
 * The Selections in one member that hold any of `rows`' items, as
 * `[{ cid, state }]`, named by that member's Claims; a deleted one is left out.
 */
async function holdingIn(ex, rows) {
  const items = [...new Set(rows.map(r => r.item_cid))].slice(0, SELECTION_LOOKUPS)
  const holding = [...new Set((await Promise.all(items.map(i => ex.selectionsHolding(i)))).flat())]
  const states = holding.length ? await ex.claimStates(holding) : new Map()
  return holding.filter(cid => !states.get(cid)?.hidden).map(cid => ({ cid, state: states.get(cid) }))
}

/** The Selections that hold any of the items (`lookup()` resolves to `[{ cid, state, at? }]`). Filled when the lookups answer. */
function selectionsPanel(at, lookup, query, counted) {
  const body = h('div', {}, h('p', { class: 'muted pad' }, '…'))
  const node = h('div', { 'data-search-group': 'selections' }, panel({ title: 'Selections', flush: true }, body))
  Promise.resolve().then(lookup).then((shown) => {
    counted(shown.length)
    // All stores: a member whose lookup failed is named, and "none" covers only the stores that answered.
    const unreadable = shown.unreadable ?? []
    const gone = unreadable.map(a => h('p', { class: 'muted pad', 'data-selections-unreadable': a.member.alias }, unreadableText(a, 'Selections')))
    // With every member unreadable there is no "none" to say: the lines above say it all.
    const none = unreadable.length === 0 ? `No Selections contain ${query}.`
      : shown.answered ? `No Selections in the other stores contain ${query}.` : null
    if (shown.length === 0) {
      body.replaceChildren(...gone, ...(none ? [h('p', { class: 'muted pad', 'data-selections-none': '' }, none)] : []))
      return
    }
    body.replaceChildren(...gone, h('div', { class: 'tbox' }, h('table', { class: 't' }, h('tbody', {}, shown.map(s =>
        h('tr', { 'data-selection': s.cid },
          h('td', {}, h('div', { class: 'namecell' }, icon('selection'), at.link(s, `#/selection/${s.cid}`, s.state?.names.join(' / ') || 'unnamed Selection'),
            at.dots(s, s.cid))),
          h('td', {}, nameTags(namedBy(s.state?.current ?? []))),
          h('td', { class: 'r' }, h('code', { class: 'cid', title: s.cid }, shortCid(s.cid)))))))))
  }).catch(() => body.replaceChildren(h('p', { class: 'muted pad' }, 'The Selections holding these items could not be read.')))
  return node
}

function addressesPanel(at, addresses) {
  return h('div', { 'data-search-group': 'addresses' }, panel({ title: 'Addresses', meta: `${addresses.length} matches`, flush: true },
    h('div', { class: 'tbox' }, h('table', { class: 't' }, h('tbody', {}, addresses.map(a =>
      h('tr', { 'data-address-result': a.cid, 'data-kind': a.kind },
        h('td', {}, h('div', { class: 'namecell' }, icon(ICONS[a.kind]), at.link(a, a.href, a.label), at.dots(a, a.cid)), nameTags(a.named)),
        h('td', { class: 'muted' }, KIND_WORDS[a.kind]),
        h('td', { class: 'r' }, h('code', { class: 'cid', title: a.cid }, shortCid(a.cid))))))))))
}

const scopeNote = () => h('p', { class: 'muted search-scope', 'data-search-scope': '' },
  'Searches sample IDs and addresses. Sample IDs match from the start and are case-sensitive. File, output, run and Selection names are not searchable yet.')
const tip = () => h('p', { class: 'muted search-tip' },
  'Tip: paste an address (starts with ', h('span', { class: 'mono' }, 'baf'), ') to jump straight to it.')

/**
 * #/search?q=<text>[&tab=samples|items|selections|addresses]. An address
 * matching exactly one CID opens that page (`ctx.navigate`, else
 * location.replace, so Back skips the search), whatever the tab.
 */
export async function search(ex, q, ctx, { tab = null } = {}) {
  const query = String(q ?? '').trim()
  const on = ['samples', 'items', 'selections', 'addresses'].includes(tab) ? tab : 'all'
  const every = listsAll(ctx)
  const where = every ? 'all stores' : ctx?.store?.name ?? 'this store'
  if (!query) {
    return h('section', { class: 'search-page' },
      pageHead({ title: 'Search', after: [h('span', { class: 'muted' }, `in ${where}`)] }),
      h('p', {}, 'Type a sample ID, or the start of one, in the box above.'), scopeNote(), tip())
  }
  const at = every ? placed(ctx) : here(ex)
  const answers = every ? await fromEach(ctx.members, async mex => ({ addresses: await mex.searchAddresses(query) })) : null
  const addresses = every ? mergeRows(answers, v => v.addresses, a => a.cid).map(m => tag(m.row, m)) : await ex.searchAddresses(query)
  if (addresses.length === 1) {
    const [only] = addresses
    const href = at.href(only, only.href)
    const navigate = ctx?.navigate ?? ((target) => globalThis.location?.replace(target))
    navigate(href)
    return h('p', { class: 'muted', 'data-search-jump': href }, `Opening ${KIND_WORDS[only.kind]} ${only.label}...`)
  }
  // Samples are asked of the members whose addresses answered, after the address search, as with one store.
  const samples = every ? await samplesOfAll(answers, query) : await ex.searchSamples(query)
  const unreadable = every ? answers.filter(a => !('value' in a)) : []
  const groups = sampleGroups(samples)
  // No matching items, so no Selection holds one: the Selections tab says 0 at once; otherwise its count arrives with the lookups.
  const counts = { samples: groups.length, items: samples.length, selections: samples.length === 0 ? 0 : null, addresses: addresses.length }
  const tabList = tabs([
    { key: 'all', label: 'All', href: searchHref(query), on: on === 'all' },
    { key: 'samples', label: 'Samples', count: counts.samples, href: searchHref(query, 'samples'), on: on === 'samples' },
    { key: 'items', label: 'Items', count: counts.items, href: searchHref(query, 'items'), on: on === 'items' },
    { key: 'selections', label: 'Selections', count: counts.selections, href: searchHref(query, 'selections'), on: on === 'selections' },
    { key: 'addresses', label: 'Addresses', count: counts.addresses, href: searchHref(query, 'addresses'), on: on === 'addresses' },
  ], { label: 'Result kinds' })
  const counted = (n) => { if (counts.selections === null) tabList.querySelector('[data-tab="selections"]')?.append(h('small', {}, String(n))) }
  const all = on === 'all'
  const showSamples = all || on === 'samples'
  const showItems = (all && samples.length > 0) || on === 'items'
  const showSelections = (all && samples.length > 0) || on === 'selections'
  const address = isAddressQuery(query)
  const fullCid = /^b[a-z2-7]{58}$/.test(query)
  return h('section', { class: 'search-page', 'data-search': query },
    pageHead({ title: `Results for "${query}"`, after: [h('span', { class: 'muted' }, `in ${where}`)] }),
    tabList,
    scopeNote(),
    unreadable.map(a => h('p', { class: 'muted', 'data-member-unreadable': a.member.alias }, unreadableText(a, 'results'))),
    samples.more ? h('p', { class: 'muted' }, `Showing the first ${samples.length} matching items; type more of the ID to narrow it.`) : null,
    (all || on === 'addresses') && addresses.length ? addressesPanel(at, addresses) : null,
    (all || on === 'addresses') && /^baf/.test(query) && !address
      ? h('p', { class: 'muted', 'data-search-short-address': '' },
        `Every address starts with the same ${ADDRESS_HEAD} characters, so an address search needs ${ADDRESS_MIN} more.`)
      : null,
    (all || on === 'addresses') && address && addresses.length === 0
      ? h('p', { class: 'muted', 'data-search-no-address': '' }, `No run, output, Selection, item or file in ${every ? 'any store' : where}'s index starts with ${query}.`,
        fullCid ? [' ', link(`#/block/${query}`, 'Look for the block itself')] : null)
      : null,
    showSamples && !(address && groups.length === 0) ? samplesPanel(at, groups, query, { cap: all ? ALL_TAB.samples : null, href: searchHref(query, 'samples') }) : null,
    showItems || showSelections
      ? h('div', { class: all ? 'grid-2' : null },
        showItems ? itemsPanel(at, samples, { cap: all ? ALL_TAB.items : null, href: searchHref(query, 'items') }) : null,
        showSelections ? selectionsPanel(at, every ? () => holdingInAll(answers, samples) : () => holdingIn(ex, samples), query, counted) : null)
      : null,
    tip())
}

/** Where a result lives, with one store: the page's own explorer and plain links. */
const here = (ex) => ({ ex: () => ex, href: (r, hash) => hash, link: (r, hash, text) => link(hash, text), dots: () => null })

/** A row found in all-stores mode, carrying the first member that lists it and every member that does. */
const tag = (row, m) => ({ ...row, found: { ex: m.ex, alias: m.member.alias, heldIn: m.heldIn } })

/** Where a result lives in all-stores mode: its first member's explorer, links that open there, and dots for every member listing it. */
function placed(ctx) {
  const href = (r, hash) => hrefIn(ctx, r.found.alias, hash)
  return {
    ex: (r) => r.found.ex,
    href,
    link: (r, hash, text) => {
      const a = link(href(r, hash), text)
      a.setAttribute('title', `Opens in ${r.found.alias}`)
      return a
    },
    dots: (r, cid) => presenceOf(ctx).dots(cid, { hereHeld: null, held: r.found.heldIn }),
  }
}

const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0)

/**
 * Every answering member's searchSamples rows, one per item, ordered as one
 * member orders them (latest run first). A member whose sample search fails
 * takes the place of its answer in `answers`, so the page names it.
 */
async function samplesOfAll(answers, query) {
  const each = await Promise.all(answers.filter(a => 'value' in a).map(async (a) => {
    try {
      return { ...a, value: await a.ex.searchSamples(query) }
    } catch (error) {
      const failed = { member: a.member, state: 'failed', error }
      answers[answers.indexOf(a)] = failed
      return failed
    }
  }))
  const rows = mergeRows(each, v => v, r => `${r.collection_cid} ${r.item_cid}`).map(m => tag(m.row, m))
    .sort((x, y) => cmp(y.finished_at ?? '', x.finished_at ?? '') || cmp(x.completion_cid, y.completion_cid)
      || cmp(x.sample, y.sample) || cmp(x.output_name, y.output_name) || cmp(x.item_cid, y.item_cid))
  return Object.defineProperty(rows, 'more', { value: each.some(a => a.value?.more) })
}

/**
 * The Selections holding the items, asked of each member for the items its
 * own index lists; one row per Selection. A member whose lookup fails is in
 * the rows' `unreadable`, as samplesOfAll names it, so the panel never
 * claims "no Selections" for a store it could not read.
 */
async function holdingInAll(answers, rows) {
  const each = await Promise.all(answers.filter(a => 'value' in a).map(async (a) => {
    const mine = rows.filter(r => r.found.heldIn.includes(a.member.alias))
    try {
      return { ...a, value: mine.length ? await holdingIn(a.ex, mine) : [] }
    } catch (error) {
      return { member: a.member, state: 'failed', error }
    }
  }))
  const found = mergeRows(each, v => v, s => s.cid).map(m => tag(m.row, m))
  return Object.defineProperties(found, {
    unreadable: { value: each.filter(a => !('value' in a)) },
    answered: { value: each.filter(a => 'value' in a).length },
  })
}

/** Whether a keydown of `/` should focus the search box: not while typing in a field, nor with a modifier. */
export function slashFocuses(e) {
  if (e.key !== '/' || e.ctrlKey || e.metaKey || e.altKey) return false
  const t = e.target
  return !(t?.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(t?.tagName))
}

/**
 * The top bar's box: after a pause of `wait` ms with at least two
 * characters, `menu` lists up to three samples, items and addresses, then a
 * link to every result. ArrowUp and ArrowDown move through the entries; Enter
 * opens the highlighted one, or the results page; Escape closes the menu.
 * Returns `{ close, sync(hash) }`: `sync` puts a #/search query back in the box.
 */
export function searchBox(input, ex, { menu, go = (href) => { globalThis.location.hash = href }, wait = TYPEAHEAD_WAIT } = {}) {
  let timer = null
  let seq = 0
  let entries = []
  let active = -1

  const close = () => {
    clearTimeout(timer)
    seq++
    entries = []
    active = -1
    menu.hidden = true
    menu.replaceChildren()
    input.setAttribute('aria-expanded', 'false')
    input.removeAttribute('aria-activedescendant')
  }

  const highlight = (i) => {
    entries.forEach((a, j) => (j === i ? a.classList.add('on') : a.classList.remove('on')))
    active = i
    if (i >= 0) input.setAttribute('aria-activedescendant', entries[i].getAttribute('id'))
  }

  const entry = (href, label, note, attrs = {}) => {
    const a = h('a', { href, id: `search-opt-${entries.length}`, role: 'option', 'data-typeahead-entry': '', ...attrs },
      h('span', { class: 'lbl' }, label), note ? sub(note) : null)
    entries.push(a)
    return a
  }

  const group = (key, title, list) => (list.length
    ? h('span', { class: 'grp', 'data-typeahead-group': key, role: 'group', 'aria-label': title }, h('span', { class: 'eyebrow' }, title), list)
    : null)

  const show = (q, samples, addresses) => {
    entries = []
    active = -1
    input.removeAttribute('aria-activedescendant')
    const sampleList = sampleGroups(samples).slice(0, TYPEAHEAD_EACH)
      .map(g => entry(g.href, g.sample, g.run.run_name ?? shortCid(g.run.completion_cid)))
    const itemList = samples.slice(0, TYPEAHEAD_EACH)
      .map(r => entry(itemHref(r), `${r.output_name} · ${r.sample}`, r.run_name ?? shortCid(r.completion_cid)))
    const addressList = addresses.slice(0, TYPEAHEAD_EACH).map((a) => {
      const e = entry(a.href, a.label, `${KIND_WORDS[a.kind]} ${shortCid(a.cid)}`)
      e.append(...nameTags(a.named))
      return e
    })
    const none = entries.length === 0 ? h('span', { class: 'none' }, `No sample ID or address starts with "${q}".`) : null
    const more = h('span', { class: 'grp' }, entry(searchHref(q), `All results for "${q}"`, null, { 'data-typeahead-all': '' }))
    menu.replaceChildren(...[group('samples', 'Samples', sampleList), group('items', 'Items', itemList),
      group('addresses', 'Addresses', addressList), none, more].filter(Boolean))
    menu.hidden = false
    input.setAttribute('aria-expanded', 'true')
  }

  const lookup = async (q) => {
    const mine = ++seq
    let samples
    let addresses
    try {
      [samples, addresses] = await Promise.all([ex.searchSamples(q, { limit: TYPEAHEAD_ROWS, values: TYPEAHEAD_EACH }), ex.searchAddresses(q, { limit: TYPEAHEAD_EACH })])
    } catch {
      if (mine === seq) close()
      return
    }
    if (mine === seq) show(q, samples, addresses)
  }

  menu.hidden = true
  input.addEventListener('input', () => {
    clearTimeout(timer)
    const q = input.value.trim()
    if (q.length < TYPEAHEAD_MIN) { close(); return }
    timer = setTimeout(() => lookup(q), wait)
  })
  input.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      if (!entries.length) return
      e.preventDefault()
      const step = e.key === 'ArrowDown' ? 1 : -1
      highlight(active < 0 ? (step > 0 ? 0 : entries.length - 1) : (active + step + entries.length) % entries.length)
    } else if (e.key === 'Enter') {
      const q = input.value.trim()
      if (!q) return
      e.preventDefault()
      const href = active >= 0 ? entries[active].getAttribute('href') : searchHref(q)
      close()
      go(href)
    } else if (e.key === 'Escape') {
      close()
    }
  })
  // A click on an entry keeps the focus in the box, so blur does not close the menu under the pointer.
  menu.addEventListener('mousedown', (e) => e.preventDefault())
  input.addEventListener('blur', close)

  return {
    close,
    sync(hash) {
      const m = /^#\/search(?:\?(.*))?$/.exec(hash ?? '')
      if (m) input.value = queryOf(m[1]).get('q') ?? ''
    },
  }
}
