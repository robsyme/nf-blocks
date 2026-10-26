// web/src/views/handoff-de.js
// #/handoff/<template>?from=&where=&project=&samples=&outputs= (P4 Task 5,
// mockups/14-handoff-de.html): the hand-off to nf-core/differentialabundance.
// It starts from one nf-core/rnaseq run's sample table (P1a's, read through
// runSampleTable), ticks the samples the URL names (else those its where
// filter passes, else all), and asks for what the pipeline needs that the
// run does not have: a condition per sample and a contrast. The checks run
// live (templates/differentialabundance.js), the samplesheet and contrasts
// show with the raw cids they will have, and the rail holds what Save writes
// and the command that runs the pipeline on them.
//
// Save writes, in order: a Selection of the included samples' items and the
// matrix item, and its name; the Handoff (POST /api/handoff), which stores
// the two files; then, with a project active, the Selection's membership, its
// condition Claims (D3: none without one) and, when that project pins every
// Selection, the policy pin, its bytes shown first. Each step's failure says
// what was written before it, and Save again goes on from there. Without a
// writable member the page is a preview: checks, files, cids and command.
import { copyOutcome, h, link } from '../html.js'
import { addrChip, avatar, button, note, pageHead, panel, shortCid } from '../components.js'
import { seal } from '../seal.js'
import { formatNumber, sizeText } from '../format.js'
import { valueText } from '../pairs.js'
import { membershipsOf, sampleAnnotations } from '../projects.js'
import { handoffSet, parseSamplesWhere, sampleMatches, samplesWhereText, splitKey, valueOf } from '../samples.js'
import { shortLabel } from '../promoted.js'
import { sumUnique } from '../sizes.js'
import { Tray } from '../tray.js'
import { matrixColumns, matrixFile } from '../handoff.js'
import { presenceOf } from '../presence.js'
import { CONTRACT, TEMPLATE, checksFor, commandText, filesFor, handoffHref, levelsOf, paramsFor, pickContrast, ready, resolveParams }
  from '../templates/differentialabundance.js'
import { explainFailure } from '../store.js'
import { cids, errorNode, show, unavailableNote, writeFailureNode } from './common.js'
import { OP_TEXT, runSampleTable } from './samples.js'
import { whoWhen } from './project-samples.js'
import { policyAdd, policyNote, projectsCache } from './addproject.js'

const GRAIN = CONTRACT.samplesheet.grouping
// A rail tag's value, as claimTag draws one: cut with an ellipsis at the rail's edge, the whole text in its title.
const val = (text) => h('span', { class: 'val', title: text }, text)
const put = (el, ...parts) => el.replaceChildren(...parts.flat(Infinity).filter(p => p !== null && p !== undefined && p !== false))
const lower = (text) => text.charAt(0).toLowerCase() + text.slice(1)
const MATRIX_NOTE = "differentialabundance reads only the samplesheet's samples from the matrix, so the whole matrix is passed."
const CAS_NOTE = 'cas:// addresses resolve when the run loads the nf-blocks plugin with a store that holds them.'

/** A file's address chip that reads as its name (mockup 14's matrix): its Seal, the name, and a click that copies `cas://<cid>/<name>`. */
function fileChip(cid, name) {
  const ref = `cas://${cid}/${name}`
  const chip = h('span', { class: 'addr', 'data-ref': ref, 'data-address': cid, role: 'button', tabindex: '0', title: ref }, seal(cid, { size: 14 }), name)
  chip.addEventListener('click', async () => chip.setAttribute('title', await copyOutcome(globalThis.navigator?.clipboard, ref)))
  return chip
}

/** "No template for X": the one template this explorer has, and where hand-offs start. */
function unknownTemplate(template) {
  return h('section', { class: 'handoff-page', 'data-template': template, 'data-template-unknown': '' },
    pageHead({ title: `No template for ${template}` }),
    note(`This explorer has a hand-off template for ${TEMPLATE} only. Open a run's Samples tab and use its hand-off rail to start one.`))
}

/** The where filter in words, as the mockup's subline has it: "reads (trimmed) ≥ 50,000". */
function filterText(where, table) {
  const outputs = [...table.perSample, ...table.runLevel]
  return where.map(([key, type, value, op = '=']) => {
    const column = table.columns.find(c => c.key === key)
    const label = column ? lower(column.label) : shortLabel(splitKey(key, outputs).path)
    const shown = (type === 'int' || type === 'float') && value !== '' && Number.isFinite(Number(value)) ? formatNumber(Number(value)) : value
    return `${label} ${OP_TEXT[op] ?? op} ${shown}`
  }).join(', ')
}

/** The name of a store that holds the block: this member when a one-byte read answers, else a member presence finds holding it; null when none does. */
async function heldWhere(ex, ctx, cid) {
  try {
    await ex.blocks.partial(cid, 1)
    return ctx.store?.name ?? 'this store'
  } catch {
    // Not here: ask the other members.
  }
  try {
    const states = await presenceOf(ctx).states(cid, { hereHeld: false })
    return states.find(s => !s.here && s.state === 'held')?.alias ?? null
  } catch {
    return null
  }
}

// Mockup 14's route names the template with a dash for its slash: #/handoff/nf-core-differentialabundance.
const SLUGS = { [TEMPLATE.replace('/', '-')]: TEMPLATE }

/**
 * The page. `template` is the route's decoded name (TEMPLATE, or its slug in
 * SLUGS); `query` is parseHandoffQuery's `{ from, where, project, samples,
 * outputs }`.
 */
export async function handoffTemplate(ex, route, ctx, query = {}) {
  const template = SLUGS[route] ?? route
  if (template !== TEMPLATE) return unknownTemplate(template)
  if (!query.from) {
    return h('section', { class: 'handoff-page', 'data-template': template },
      pageHead({ title: `Hand off to ${TEMPLATE}` }),
      note(`A hand-off starts from an ${CONTRACT.from} run: open the run's Samples tab and use its hand-off rail.`))
  }
  const from = query.from
  const where = parseSamplesWhere(query.where)
  const read = await runSampleTable(ex, from)
  const { row } = read
  const runName = row.run_name ?? shortCid(from)
  const samplesHrefOf = (w) => `#/run/${from}?tab=samples${w.length ? `&where=${encodeURIComponent(samplesWhereText(w))}` : ''}`
  const crumbs = [{ text: runName, href: `#/run/${from}` }, { text: 'Samples', href: samplesHrefOf(where) }, { text: 'Hand off' }]
  if (!read.table || !read.table.samples.length) {
    return h('section', { class: 'handoff-page', 'data-template': template },
      pageHead({ crumbs, title: `Hand off to ${TEMPLATE}` }),
      note(read.over !== undefined ? `This run has ${formatNumber(read.over)} items, more than a sample table joins.` : 'This run has no samples to hand off.', 'warn'))
  }
  const { table, config } = read
  const samples = table.samples
  const pipeline = row.pipeline

  // The filter: the conditions that read, as the Samples tab keeps them.
  const inForce = where.filter((c) => {
    if (c[2] === '') return false
    try { sampleMatches(table, samples[0], [c]); return true } catch { return false }
  })
  const passing = new Set(samples.filter(s => sampleMatches(table, s, inForce)))
  const named = new Set(query.samples ?? [])
  const initial = named.size ? new Set(samples.filter(s => named.has(s))) : inForce.length ? passing : new Set(samples)
  const filter = inForce.length ? filterText(inForce, table) : null

  // The active project and its condition per sample.
  let p = null
  let projectError = null
  if (query.project) {
    try { p = await ex.project(query.project) } catch (e) { projectError = e }
  }
  const cellOf = (s) => (p ? sampleAnnotations([{ pipeline, id: s }], p.annotations).get(GRAIN) ?? null : null)
  const cells = new Map(samples.map(s => [s, cellOf(s)]))

  // The matrix: its item and address now, its header's columns when read.
  const m = CONTRACT.matrix
  let at = null
  let matrix = { state: 'reading' }
  try {
    at = await matrixFile(ex, table.runLevelItems.get(m.output) ?? [], m.file)
    if (!at) matrix = { state: 'missing' }
  } catch (e) {
    matrix = { state: 'failed', message: e?.message ?? String(e) }
  }
  const matrixRef = at ? `cas://${at.cid}/${m.file}` : null

  // The GTF: the run's own when its InputSet names one a member holds; otherwise the reader gives one.
  const store = ctx.store?.name ?? 'this store'
  const gtfInput = (await ex.runInputs([from]).catch(() => []))
    .find(r => r.role === CONTRACT.gtf.role && r.address) ?? null
  const gtfHolder = gtfInput ? await heldWhere(ex, ctx, gtfInput.address) : null
  const gtfStored = gtfHolder ? { value: `cas://${gtfInput.address}/${gtfInput.name ?? 'genes.gtf'}`,
    label: `${gtfInput.name ?? 'the GTF'} from the run's inputs, held in ${gtfHolder}` } : null

  // What the Selection takes: the drawer's outputs (else the contract's), and the matrix item.
  const outputs = (query.outputs?.length ? query.outputs : CONTRACT.outputs)
    .filter(o => table.perSample.includes(o) || table.runLevelItems.has(o))
  const reads = config.fields[0] ? table.columns.find(c => c.field?.label === config.fields[0].label) ?? null : null
  const canWrite = !!(ctx.write?.available && ctx.write?.here)
  const pins = !!p && p.policy?.retention?.value === 'pin_selections'

  // ---- state ----
  const included = new Map(samples.map(s => [s, initial.has(s)]))
  const conditions = new Map(samples.map(s => [s, cells.get(s)?.conflicted ? '' : (cells.get(s)?.value ?? '')]))
  let contrast = { target: '', reference: '' }
  // Until the reader picks in a select, the contrast follows the samples; a pick keeps both sides where they stand.
  let picked = {}
  let gtfTyped = ''
  let saved = null // { key, selection, response } while what Save wrote is what the page shows
  const sizes = new Map() // the Selection's item addresses -> { text, promise }: what a pin of it keeps here

  const rowsNow = () => samples.filter(s => included.get(s)).map(s => ({ sample: s, condition: (conditions.get(s) ?? '').trim() }))
  const itemsNow = (included) => {
    const items = handoffSet(table, included, outputs).items
    if (at && !items.some(i => i.address === at.item)) items.push({ address: at.item, via: at.collection ? [at.collection] : [], output: m.output, sample: null })
    return items
  }
  /** Conditions whose value differs from the project's current one (or whose cell is in conflict): the Claims Save writes. */
  const changedNow = (rows, project = p, cellFor = (s) => cells.get(s)) => (project ? rows.filter((r) => {
    const cell = cellFor(r.sample)
    return r.condition && (!cell || cell.conflicted || cell.value !== r.condition)
  }) : [])

  // ---- the table ----
  const body = h('tbody', {})
  const rowEls = new Map()
  for (const s of samples) {
    const box = h('input', { type: 'checkbox', 'data-id': s, 'aria-label': `Include ${s}`, onchange: (e) => { included.set(s, !!e.currentTarget.checked); update() } })
    box.checked = included.get(s)
    if (box.checked) box.setAttribute('checked', '')
    const cell = cells.get(s)
    const face = h('span', { class: 'cell-face' })
    let tag = null
    const input = h('input', { type: 'text', class: 'cell-input', 'data-c': s, maxlength: '128', placeholder: cell?.conflicted ? 'in conflict' : '—',
      'aria-label': `condition for ${s}`,
      oninput: (e) => {
        conditions.set(s, String(e.currentTarget.value ?? ''))
        face.replaceChildren(avatar(ctx.viewer ?? 'anonymous'))
        tag.removeAttribute('title')
        update()
      } })
    input.value = conditions.get(s)
    const values = reads ? valueOf(table, s, reads).map(v => valueText(v.type, v.value, reads.field)) : []
    const tr = h('tr', { 'data-sample': s, class: included.get(s) ? null : 'dim' },
      h('td', {}, box),
      h('td', { class: 'mono nowrap' }, s),
      reads ? h('td', { class: values.length ? 'r num' : 'r num muted' }, values.length ? values.join(', ') : '—') : null,
      h('td', {}, tag = h('span', { class: 'claim cell-claim', 'data-condition': s }, face, input)))
    rowEls.set(s, { tr, box, tag, face })
    drawCell(s)
    body.append(tr)
  }
  /** A condition cell's avatar and title from the project's current Claims: who wrote it and when, as the project's Samples tab says it; each value of a conflicted cell. */
  function drawCell(s) {
    const cell = cells.get(s)
    const { tag, face } = rowEls.get(s)
    const last = cell && !cell.conflicted ? cell.claims.at(-1) ?? null : null
    put(face, last ? avatar(last.asserted_by ?? null) : null)
    const title = cell?.conflicted ? `In conflict in the project: ${cell.entries.map(e => `${e.value} (${whoWhen(e.claim)})`).join('; ')}` : whoWhen(last)
    if (title) tag.setAttribute('title', title); else tag.removeAttribute('title')
  }
  const all = button(`Include all ${samples.length}`, { size: 'sm', id: 'all', onclick: () => {
    for (const s of samples) included.set(s, true)
    update()
  } })

  // ---- the contrast, checks and files ----
  const target = h('select', { id: 'target', class: 'handoff-select', 'aria-label': 'target condition', onchange: (e) => { picked = { ...contrast, target: e.currentTarget.value }; update() } })
  const reference = h('select', { id: 'ref', class: 'handoff-select', 'aria-label': 'reference condition', onchange: (e) => { picked = { ...contrast, reference: e.currentTarget.value }; update() } })
  const contrastFile = h('pre', { class: 'muted mono handoff-contrasts', 'data-contrasts': '' })
  const contrastCid = h('p', { class: 'muted small', 'data-contrasts-cid': '' })
  const checks = h('div', { class: 'handoff-checks', 'data-checks': '' })
  const gtfField = h('input', { type: 'text', id: 'gtf', 'data-gtf': '', placeholder: '/path/to/genes.gtf or cas://…', 'aria-label': 'GTF path or address',
    oninput: (e) => { gtfTyped = String(e.currentTarget.value ?? ''); update() } })
  gtfField.value = ''
  const gtfRow = gtfStored ? null : h('label', { class: 'row handoff-gtf' }, h('span', { class: 'muted' }, 'GTF'), gtfField)
  const sheet = h('pre', { class: 'code', id: 'ss', 'data-samplesheet': '' })
  const sheetCid = h('span', { 'data-samplesheet-cid': '' })

  // ---- the rail ----
  const status = h('div', { id: 'write-status', 'data-handoff-status': '' })
  const facts = h('div', { class: 'row handoff-facts', 'data-saves': '' })
  const pinLine = h('p', { class: 'muted small', 'data-pin-note': '', hidden: true })
  const save = button('Save hand-off', { kind: 'primary', id: 'save', 'data-handoff-save': '', disabled: true })
  const command = h('pre', { class: 'code', id: 'cmd', 'data-command': '' })
  const copy = button('Copy command', { size: 'sm', 'data-copy-command': '', onclick: async (e) => {
    const b = e.currentTarget
    b.textContent = await copyOutcome(globalThis.navigator?.clipboard, command.textContent)
  } })
  const rail = [
    h('section', { class: 'sec', 'data-rail-saves': '' }, h('h3', {}, 'Saves'), facts, pinLine,
      p ? null : h('p', { class: 'muted small', 'data-no-project': '' }, 'No project is active, so the conditions are kept only in samplesheet.csv. After Save, Add to project keeps them as Claims.'),
      save,
      canWrite ? null : unavailableNote(ctx, 'The hand-off and its Claims', 'Open this hand-off there', ctx.write?.hrefFor?.(handoffHref(query)) ?? '#/'),
      status),
    h('section', { class: 'sec', 'data-rail-command': '' }, h('h3', {}, 'Run command'), command,
      h('p', { class: 'muted small', 'data-cas-note': '' }, CAS_NOTE), h('div', { class: 'row' }, copy)),
  ]
  ctx.use?.({ rail })

  let saving = false
  let current = null // the last update's { rows, files, params, items, changed, list, key }
  function update() {
    const rows = rowsNow()
    for (const s of samples) {
      const { tr, box } = rowEls.get(s)
      const on = included.get(s)
      box.checked = on
      if (on) box.setAttribute('checked', ''); else box.removeAttribute('checked')
      if (on) tr.removeAttribute('class'); else tr.setAttribute('class', 'dim')
    }
    const levels = levelsOf(rows)
    contrast = pickContrast(levels, picked, (l) => rows.filter(r => r.condition === l).length)
    for (const [sel, value] of [[target, contrast.target], [reference, contrast.reference]]) {
      put(sel, levels.map(l => h('option', { value: l, selected: l === value ? '' : null }, l)))
      sel.value = value
    }
    const ids = rows.map(r => r.sample)
    const only = ids.length === passing.size && ids.every(s => passing.has(s))
    const gtf = gtfStored ?? { value: gtfTyped.trim() || null, store }
    const list = checksFor({ rows, contrast, matrix, gtf, filter: filter ? { text: filter, only } : null })
    put(checks, list.map(c => h('div', { class: `note ${c.tone}`, 'data-check': c.key, 'data-ok': String(c.ok) },
      h('span', {}, c.ok ? '✓' : '!'), h('span', {}, c.text))),
      h('p', { class: 'muted small', 'data-matrix-note': '' }, MATRIX_NOTE))
    const files = filesFor(rows, contrast)
    const [ss, cs] = [files.find(f => f.name === CONTRACT.samplesheet.file), files.find(f => f.name === CONTRACT.contrasts.file)]
    sheet.textContent = ss.text
    put(sheetCid, addrChip(ss.address, { ref: `cas://${ss.address}/${ss.name}` }))
    contrastFile.textContent = cs ? `${cs.name}\n${cs.text}` : ''
    put(contrastCid, cs ? addrChip(cs.address, { ref: `cas://${cs.address}/${cs.name}` }) : null)
    const params = paramsFor({ matrix: matrixRef, gtf: gtf.value })
    const items = itemsNow(ids)
    const changed = changedNow(rows)
    const key = JSON.stringify([files.map(f => f.address), params, items.map(i => i.address)])
    if (saved && saved.key !== key) { saved = null; status.replaceChildren() }
    const name = contrast.target && contrast.reference && contrast.target !== contrast.reference
      ? `${contrast.target} vs ${contrast.reference} (${TEMPLATE.split('/').pop()})` : null
    current = { rows, files, params, items, changed, list, key, name }
    command.textContent = commandText(saved ? saved.response.params : resolveParams(params, files))
    drawFacts()
  }

  /** The rail's Saves facts (mockup 14), the pin's bytes when the project pins every Selection, and whether Save may go. */
  function drawFacts() {
    const { changed, items, list, key, name } = current
    const pin = pins ? sizeOf(items) : null
    put(facts,
      h('span', { class: 'fact' }, h('span', { class: 'k' }, 'selection'), 'samples + matrix'),
      h('span', { class: 'claim', 'data-name-claim': name ?? '' }, h('span', { class: 'verb' }, 'name'), val(name ? `“${name}”` : '…')),
      p ? h('span', { class: 'claim', 'data-member-claim': p.cid }, h('span', { class: 'verb' }, 'added to'), val(p.name)) : null,
      h('span', { class: 'fact' }, h('span', { class: 'k' }, 'block'), CONTRACT.samplesheet.file),
      h('span', { class: 'fact' }, h('span', { class: 'k' }, 'block'), CONTRACT.contrasts.file),
      h('span', { class: 'fact' }, h('span', { class: 'k' }, 'block'), 'hand-off'),
      p ? h('span', { class: 'claim', 'data-condition-claims': String(changed.length) }, h('span', { class: 'verb' }, GRAIN), val(`× ${changed.length} samples`)) : null,
      pin ? h('span', { class: 'claim', 'data-pin-claim': '' }, h('span', { class: 'verb' }, 'pin'), val(pin.text ?? '…')) : null)
    show(pinLine, !!pin)
    if (pin) pinLine.textContent = `${p.name} pins every Selection in it, in ${store}. The pin's note is "${policyNote(p.name)}".`
    const known = !pin || pin.text !== null
    save.disabled = !(canWrite && ready(list) && known && !saving && saved?.key !== key)
    if (pin && !known) pin.promise.then(() => { if (current?.key === key) drawFacts() })
  }

  /** About what a pin of the Selection keeps here: unique bytes over its items' file sizes, as selectionSize counts a Selection the snapshot does not know. */
  function sizeOf(items) {
    const k = JSON.stringify(items.map(i => i.address).sort())
    if (!sizes.has(k)) {
      const entry = { text: null, promise: null }
      entry.promise = Promise.all(items.map(i => ex.itemFileMap(i.address))).then(maps => sumUnique(maps))
        .then(z => { entry.text = `keeps about ${sizeText(z)} in ${store}` }, () => { entry.text = `size unknown, in ${store}` })
      sizes.set(k, entry)
    }
    return sizes.get(k)
  }

  // ---- Save ----
  // Each step's result, kept across attempts for the same items: a Save after a part-way failure goes on from the
  // step that failed, and never writes a second Handoff for the same files or a condition already written.
  let progress = null // { itemsKey, selection, state, named, nameClaims, handoffKey, response, member, pinned }
  save.addEventListener('click', (e) => {
    const snapshot = current
    saving = true
    return Promise.resolve(ctx.write.run(status, () => saveAll(snapshot), e.currentTarget)).finally(() => { saving = false; update() })
  })

  /** What the steps so far wrote, as the part-way note lists it. */
  function wroteSoFar(pr, conditions, key) {
    const out = [h('span', {}, 'the Selection ', link(ctx.write.hrefFor(`#/selection/${pr.selection}`), shortCid(pr.selection)))]
    if (pr.named) out.push('its name')
    if (pr.response && pr.handoffKey === key) out.push(h('span', {}, 'the hand-off ', shortCid(pr.response.address)))
    if (pr.member) out.push(`its membership of ${p.name}`)
    if (conditions) out.push(conditions)
    if (pr.pinned) out.push('the policy pin')
    return out
  }
  /**
   * A step that failed after others were written: what was, what failed (as
   * runWrite words a refusal), and that Save goes on from there. `key` is the
   * attempt's files and params: a hand-off counts as written only for them.
   */
  async function partway(pr, key, step, e, conditions = null) {
    const explained = await explainFailure(e, globalThis.location?.href ?? 'file:///')
    const wrote = wroteSoFar(pr, conditions, key)
    return { address: pr.response && pr.handoffKey === key ? pr.response.address : pr.selection, stay: h('div', { class: 'warn', 'data-handoff-partial': step },
      h('p', {}, 'Saved ', wrote.flatMap((x, i) => (i ? [i === wrote.length - 1 ? ' and ' : ', ', x] : [x])), `, but ${step} failed. `,
        'Nothing after it was written; Save again to go on from there.'),
      writeFailureNode(explained)) }
  }

  async function saveAll({ rows, files, params, items, changed, key, name }) {
    const w = ctx.write.writer
    const itemsKey = JSON.stringify(items.map(i => i.address).sort())
    if (progress?.itemsKey !== itemsKey) progress = { itemsKey }
    const pr = progress
    // 1. The Selection: nothing is written if it fails, and the attempt reports the error.
    if (!pr.selection) {
      const members = new Tray(null)
      members.addMany(items.map(i => ({ address: i.address, via: i.via, kind: 'item' })))
      pr.selection = (await w.selection(members.toMembers())).address
    }
    const selection = pr.selection
    // 2. Its name, unless it is already the one current name.
    if (pr.named !== name) {
      try {
        pr.state ??= (await ex.claimStates([selection])).get(selection) ?? null
        pr.nameClaims ??= pr.state?.nameClaims ?? []
        const names = pr.named ? [pr.named] : (pr.state?.names ?? [])
        if (!(names.length === 1 && names[0] === name)) pr.nameClaims = [(await w.rename(selection, name, pr.nameClaims)).address]
        pr.named = name
      } catch (e) {
        return partway(pr, key, 'naming the Selection', e)
      }
    }
    // 3. The Handoff, with the two files: once for these files and params.
    if (pr.handoffKey !== key) {
      try {
        pr.response = await w.handoff({ template: TEMPLATE, selection, project: p?.cid ?? null, files: files.map(f => ({ name: f.name, text: f.text })), params })
        pr.handoffKey = key
      } catch (e) {
        // The last hand-off written (for other files) stays known, so going back to those files does not write it again.
        return partway(pr, key, 'the hand-off', e)
      }
    }
    const response = pr.response
    if (p) {
      // 4. The Selection joins the project, unless it is in it already.
      if (!pr.member) {
        try {
          const inIt = membershipsOf(new Map([[selection, pr.state?.claims ?? []]])).some(m => m.project === p.cid && !m.hidden)
          if (!inIt) await w.addToProject(selection, p.cid)
          pr.member = true
        } catch (e) {
          return partway(pr, key, `adding the Selection to ${p.name}`, e)
        }
      }
      // 5. The conditions, as the project's Claims (D3), each superseding its cell's current Claims; only those still pending.
      const result = await keepConditions(w, p, pipeline, changed, (s) => cells.get(s), ctx.viewer)
      // What was written is now each cell's current Claim, so a later Save supersedes it and does not write it again.
      for (const [sample, cell] of result.written) { cells.set(sample, cell); drawCell(sample) }
      if (result.error) {
        return partway(pr, key, `the ${GRAIN} for ${result.sample}`, result.error, result.done ? `${result.done} of ${changed.length} conditions` : null)
      }
      // 6. The project's policy pin, after the membership, unless the writer pins the Selection already.
      if (pins && !pr.pinned) {
        try {
          const decision = await policyAdd(ex, ctx, selection)
          if (decision.pin) await w.pin(selection, policyNote(p.name))
          pr.pinned = true
        } catch (e) {
          return partway(pr, key, 'the policy pin', e)
        }
      }
    }
    saved = { key, selection, response }
    command.textContent = commandText(response.params)
    return { address: response.address, stay: savedNode({ selection, response, rows }) }
  }

  /** What Save wrote: the Handoff's seal and address, the samplesheet's reference, the Selection and the project; Add to project without one. */
  function savedNode({ selection, response, rows }) {
    const sheetFile = (response.files ?? []).find(f => f.name === CONTRACT.samplesheet.file)
    return h('div', { class: 'handoff-saved', 'data-handoff-saved': response.address },
      h('p', { class: 'row' }, seal(response.address, { size: 20 }), h('b', {}, response.written === false ? 'Hand-off already saved' : 'Hand-off saved'),
        addrChip(response.address, { seal: false })),
      sheetFile ? h('p', { class: 'row', 'data-saved-samplesheet': '' }, h('span', { class: 'muted' }, '--input'),
        addrChip(sheetFile.address, { ref: response.params?.input ?? `cas://${sheetFile.address}/${sheetFile.name}` })) : null,
      h('p', { class: 'row' }, link(ctx.write.hrefFor(`#/selection/${selection}`), 'Open the Selection'),
        p ? [' · ', link(ctx.write.hrefFor(`#/project/${p.cid}`), `Open ${p.name}`)] : null),
      p ? null : addToProject(selection, rows))
  }

  /**
   * D3: a hand-off saved without a project wrote no Claims; this adds its
   * Selection to a project and writes the conditions there, superseding
   * that project's current values. A project that pins every Selection pins
   * it too, its bytes named on the button first (policyAdd). A refusal after
   * the membership says what was written.
   */
  function addToProject(selection, rows) {
    const cache = projectsCache(ex)
    const choose = h('select', { 'data-add-project-choose': '', 'aria-label': 'project' })
    const go = button('Add', { size: 'sm', 'data-add-project-go': '', disabled: true })
    const said = h('p', { class: 'muted small', 'data-add-project-note': '', hidden: true })
    const where = h('div', { 'data-add-project-status': '' })
    const form = h('div', { class: 'row handoff-add-project', hidden: true }, choose, go)
    let chosen = null
    const decide = async () => {
      const cid = choose.value
      go.disabled = true
      show(said, false)
      chosen = null
      if (!cid) return
      let target
      let decision
      try {
        target = await ex.project(cid)
        decision = target.policy?.retention?.value === 'pin_selections' ? await policyAdd(ex, ctx, selection) : null
      } catch (e) {
        if (choose.value !== cid) return
        said.textContent = `Could not read that project: ${e?.message ?? e}`
        show(said, true)
        return
      }
      if (choose.value !== cid) return
      chosen = { project: target, decision }
      const n = changedNow(rows, target, (s) => sampleAnnotations([{ pipeline, id: s }], target.annotations).get(GRAIN) ?? null).length
      go.textContent = decision ? decision.label : 'Add'
      said.textContent = [`Writes ${n} ${n === 1 ? 'condition' : 'conditions'} to ${target.name}.`, decision?.note].filter(Boolean).join(' ')
      show(said, true)
      go.disabled = false
    }
    choose.addEventListener('change', decide)
    // Per project, what an earlier attempt wrote: its membership, each condition's Claim, its pin. Add again goes on from there.
    const kept = new Map()
    go.addEventListener('click', (e) => ctx.write.run(where, async () => {
      const { decision } = chosen
      const w = ctx.write.writer
      if (!kept.has(chosen.project.cid)) kept.set(chosen.project.cid, { member: false, cells: new Map(), pinned: false })
      const done = kept.get(chosen.project.cid)
      if (!done.member) {
        await w.addToProject(selection, chosen.project.cid)
        done.member = true
        cache.reset()
      }
      // The project's annotations as they stand now (the tail has its own writes), and what this page wrote over them.
      const target = await ex.project(chosen.project.cid).catch(() => chosen.project)
      const cellFor = (s) => done.cells.get(s) ?? sampleAnnotations([{ pipeline, id: s }], target.annotations).get(GRAIN) ?? null
      const changed = changedNow(rows, target, cellFor)
      const stopped = async (step, error, done) => ({ stay: h('div', { class: 'warn', 'data-add-project-partial': step },
        h('p', {}, `Added the Selection to ${target.name}${done ? ` and ${done} of ${changed.length} conditions` : ''}, but ${step} failed. Nothing after it was written.`),
        writeFailureNode(await explainFailure(error, globalThis.location?.href ?? 'file:///'))) })
      const result = await keepConditions(w, target, pipeline, changed, cellFor, ctx.viewer)
      for (const [sample, cell] of result.written) done.cells.set(sample, cell)
      if (result.error) return stopped(`the ${GRAIN} for ${result.sample}`, result.error, result.done)
      if (decision?.pin && !done.pinned) {
        try {
          await w.pin(selection, policyNote(target.name))
          done.pinned = true
        } catch (error) {
          return stopped('the policy pin', error, result.done)
        }
      }
      show(form, false)
      show(said, false)
      show(open, false)
      const n = done.cells.size
      return { stay: h('p', { 'data-added-to-project': target.cid }, `Added the Selection and ${n} ${n === 1 ? 'condition' : 'conditions'} to `,
        link(ctx.write.hrefFor(`#/project/${target.cid}`), target.name), '.') }
    }, e.currentTarget))
    const open = button('Add to project…', { size: 'sm', 'data-add-project-open': '', onclick: async () => {
      show(form, true)
      const projects = (await cache.get().catch(() => [])).filter(r => !r.state?.hidden)
      put(choose, h('option', { value: '' }, 'Choose a project'), projects.map(r => h('option', { value: r.cid }, r.name ?? shortCid(r.cid))))
      choose.value = ''
    } })
    return canWrite ? h('div', { class: 'handoff-keep', 'data-add-project': '' },
      h('p', { class: 'muted small' }, 'No project was active, so no condition Claims were written.'), open, form, said, where) : null
  }

  // The matrix's header, read once.
  if (at) {
    matrixColumns(ex, at.cid, samples).then(
      (found) => { matrix = found === null ? { state: 'failed', message: 'the store sent no bytes for a range read' } : { state: 'ready', columns: found }; update() },
      (e) => { matrix = { state: 'failed', message: e?.message ?? String(e) }; update() })
  }

  update()
  return h('section', { class: 'handoff-page', 'data-template': template, 'data-from': from },
    pageHead({ crumbs, title: `Hand off to ${TEMPLATE}`,
      subline: [
        h('span', {}, 'From ', h('b', {}, runName)),
        filter ? h('span', { 'data-filter': '' }, `filter: ${filter}`) : null,
        at ? h('span', {}, 'matrix: ', fileChip(at.cid, m.file)) : null,
        p ? h('span', { 'data-project': p.cid }, 'project: ', link(`#/project/${p.cid}`, p.name)) : null,
      ].filter(Boolean) }),
    projectError ? h('div', {}, h('p', { class: 'warn' }, 'Could not read the project named in the link, so this hand-off has none:'), errorNode(projectError)) : null,
    pipeline !== CONTRACT.from ? note(`This template starts from ${CONTRACT.from} runs; this run is ${pipeline ?? 'of an unknown pipeline'}.`, 'warn') : null,
    h('div', { class: 'grid-2' },
      h('section', { class: 'panel flush' },
        h('div', { class: 'ph' }, h('h2', {}, '1 · Samples and conditions'), h('span', { class: 'spacer' }), all),
        h('div', { class: 'pb tbox' }, h('table', { class: 't', 'data-handoff-samples': '' },
          h('thead', {}, h('tr', {}, h('th', {}), h('th', {}, 'sample'), reads ? h('th', { class: 'r' }, reads.label) : null, h('th', {}, GRAIN))),
          body))),
      h('div', { class: 'stack' },
        panel({ title: '2 · Contrast' },
          h('div', { class: 'row handoff-contrast' }, 'Compare ', target, ' against ', reference),
          contrastFile, contrastCid),
        panel({ title: '3 · Checks' }, checks, gtfRow))),
    h('section', { class: 'panel flush' },
      h('div', { class: 'ph' }, h('h2', {}, CONTRACT.samplesheet.file), sheetCid, h('span', { class: 'spacer' }), h('span', { class: 'muted' }, 'saved as a block on Save')),
      h('div', { class: 'pb' }, sheet)))
}

/**
 * Writes `changed` (`[{ sample, condition }]`) as the project's annotation
 * Claims on `{ pipeline, id: sample }`, one per sample, each superseding its
 * cell's current Claims (`cellFor(sample)`), as `viewer`. Stops at the first refusal.
 * `{ done, written, sample, error }`: `written` is a Map of sample to the
 * cell as it now stands (the new Claim alone), `sample` and `error` only on
 * a refusal.
 */
async function keepConditions(w, project, pipeline, changed, cellFor, viewer) {
  const written = new Map()
  for (const r of changed) {
    try {
      const { address } = await w.annotate(project.cid, { pipeline, id: r.sample }, GRAIN, r.condition, cids(cellFor(r.sample)?.claims ?? []))
      const claim = { cid: address, asserted_by: viewer ?? null, timestamp: new Date().toISOString() }
      written.set(r.sample, { value: r.condition, values: [r.condition], conflicted: false, claims: [claim], entries: [{ value: r.condition, claim }] })
    } catch (error) {
      return { done: written.size, written, sample: r.sample, error }
    }
  }
  return { done: written.size, written }
}
