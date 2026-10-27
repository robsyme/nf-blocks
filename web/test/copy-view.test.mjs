// The copy page (P3 Task 4, mockups/11-copy.html): the dry run's numbers,
// chain, claims and command; a new choice asks again and an older answer is
// dropped; audit findings gate the copy; the result and the release offer.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { installDom, strayTexts } from './dom.mjs'
import { resetFolds } from '../src/folds.js'
import { bytes } from '../src/format.js'
import { copyView, copyToButton, lineageRuleText, REPLAN_DELAY_MS } from '../src/views/copy.js'
import { CopyError, copyAvailability, copyClient } from '../src/copy.js'
import { openRoblab, ROBLAB } from './storage-fixture.mjs'
import { fullWhenText } from '../src/words.js'
import { APP_CSS } from '../src/styles.js'

const PLAN = JSON.parse(readFileSync(new URL('./fixtures/copy-plan-wigshop.json', import.meta.url), 'utf8'))
const WIGSHOP = PLAN.root.cid
const DM = ROBLAB.runs.distraught_murdock.cid
const clone = (x) => JSON.parse(JSON.stringify(x))

const tick = () => new Promise(r => setTimeout(r, 2))
// Re-plans wait for the choices to settle (REPLAN_DELAY_MS), so a wait allows a few of them.
async function until(fn, what = 'the condition') {
  const end = Date.now() + 3000
  while (Date.now() < end) {
    if (fn()) return
    await tick()
  }
  assert.fail(`timed out waiting for ${what}`)
}

const member = (alias, key, extra = {}) => ({ alias, key, writable: false, base: `m/${alias}/`, kind: 'local', location: null, copy_target: true, copy_reason: null, ...extra })
const LIST = [member('roblab', 'a', { writable: true, copy_target: true }), member('org-s3', 'b', { kind: 's3', location: 's3://lab-blocks' }),
  member('ana-hpc', 'c', { copy_target: false, copy_reason: 'readOnly is set for this store' })]

/**
 * A plan answer for `request`, from the canned wigshop plan: the depth,
 * destination and claim categories asked for, and the command they make.
 */
function answer(request, edit = (p) => p) {
  const p = clone(PLAN)
  p.to = request.to
  p.depth = request.depth
  // ['none'] carries no category: the body's claims are then empty, and the command says none.
  const categories = request.claims.filter(c => c !== 'none')
  p.claims = categories
  const totals = p.depths[request.depth]
  Object.assign(p.plan, { blocks: totals.blocks, bytes: totals.bytes, transfer: totals.transfer, transfer_bytes: totals.transfer_bytes })
  if (request.depth !== 'results') p.plan.runs[0].lineage = 'join_key'
  // 1.3 as amended: every candidate claim is listed, carried or not.
  for (const c of p.plan.claims) c.carried = request.claims.includes(c.category)
  p.command = `nextflow plugin nf-blocks:copy ${request.root} --from ${request.from} --to ${request.to} --depth ${request.depth} `
    + `--claims ${categories.length ? categories.join(',') : 'none'} --dry-run`
  return edit(p)
}

/** What CopyService answers a request whose claims list is empty: 400 bad_claims (the copy client throws it as a CopyError). */
function refuseEmptyClaims(r) {
  if (Array.isArray(r.claims) && r.claims.length === 0) {
    throw new CopyError('bad_claims', '--claims names at least one of names, pins, projects, annotations, or none', 400)
  }
}

/** A copy client that records each request and answers from `plan` / `copy` (each may return a promise), refusing what the server refuses. */
function fakeClient({ plan = (r) => answer(r), copy = null } = {}) {
  const asked = []
  return {
    asked,
    plan: async (r) => { asked.push(['plan', r]); refuseEmptyClaims(r); return plan(r) },
    copy: async (r) => { asked.push(['copy', r]); refuseEmptyClaims(r); return copy(r) },
  }
}

function ctxFor({ client = fakeClient(), list = LIST, write = { available: true, here: true, reason: null, writable: 'roblab' }, copy = null } = {}) {
  const forgot = []
  const store = { members: list, member: 'roblab', copy: true }
  return {
    forgot,
    store: { name: 'roblab', key: 'a', hrefIn: (alias, hash) => `?member=${alias}${hash}` },
    members: { primary: 'roblab', list, invalidate: (a) => forgot.push(['members', a]) },
    presence: { several: false, invalidate: () => forgot.push(['presence']), dots: () => null },
    sizes: { forget: (a) => forgot.push(['sizes', a]) },
    copy: copy ?? { ...copyAvailability(store, { token: 't' }), client },
    // The explore server's writable member is the open one (roblab) unless a test says otherwise.
    write: { hrefFor: (x) => x, writable: 'roblab', ...write },
    viewer: 'ada.lovelace@example.org',
  }
}

async function page({ kind = 'selection', cid = WIGSHOP, query = {}, project = null, ...options } = {}) {
  installDom(); resetFolds()
  const { explorer } = await openRoblab()
  // A project the page reads by its cid (Explorer.project), for the default store's note.
  if (project) explorer.project = async (c) => [].concat(project).find(p => p.cid === c) ?? null
  const ctx = ctxFor(options)
  const el = await copyView(explorer, kind, cid, ctx, query)
  return { el, ctx, ex: explorer }
}
const ready = (el) => until(() => el.dataset.planState === 'ready', 'the plan')
const big = (el, key) => el.querySelector(`[data-plan-${key}]`).querySelector('.bignum').textContent
const sub = (el, key) => el.querySelector(`[data-plan-${key}]`).querySelector('.small').textContent
const eyebrow = (el, key) => el.querySelector(`[data-plan-${key}]`).querySelector('.eyebrow').textContent

test('the head, depth cards, chain, plan, claims and command from the canned wigshop plan', async () => {
  const client = fakeClient()
  const { el } = await page({ client })
  await ready(el)
  // One dry run, from the open member to the first store that can take a copy, with the default claims.
  assert.deepEqual(client.asked, [['plan', { root: `selection:${WIGSHOP}`, from: 'roblab', to: 'org-s3', depth: 'results',
    claims: ['names', 'projects', 'annotations'], accept_audit: false }]])
  assert.equal(el.querySelector('h1').textContent, 'Copy wigshop')
  assert.deepEqual(el.querySelector('.crumbs').querySelectorAll('a').map(a => [a.textContent, a.getAttribute('href')]),
    [['Selections', '#/selections'], ['wigshop', `#/selection/${WIGSHOP}`]])
  assert.match(el.querySelector('[data-copy-from]').textContent, /roblab/)
  assert.equal(el.querySelector('[data-copy-dest]').textContent, 'org-s3 · s3://lab-blocks▾')

  // What travels: three depths with their totals; results is chosen.
  const figures = (d) => el.querySelector(`[data-depth-figures=${d}]`).textContent
  assert.equal(figures('results'), '116 blocks1.29 MB')
  assert.equal(figures('lineage'), '238 blocks21.4 MB')
  assert.equal(figures('all'), '1,001 blocks78.5 MB')
  // What each depth would transfer: a figure about the destination, so it names it.
  const toTransfer = (d) => el.querySelector(`[data-depth-transfer=${d}]`).textContent
  assert.equal(toTransfer('results'), '114 to transfer to org-s3 · 1.28 MB')
  assert.equal(toTransfer('lineage'), '236 to transfer to org-s3 · 21.4 MB')
  assert.equal(toTransfer('all'), '999 to transfer to org-s3 · 78.5 MB')
  assert.ok(el.querySelector('[data-depth-card=results]').classList.contains('on'))
  assert.equal(el.querySelector('[data-depth=results]').checked, true)
  assert.match(el.querySelector('[data-depth-card=results]').textContent, /Results \+ run records3 items, 9 files, and the completion \+ manifest \(params, config, script\) that produced them/)
  assert.match(el.querySelector('[data-depth-card=all]').textContent, /Everything distraught_murdock stored/)
  assert.ok(el.querySelector('[data-depth-card=lineage]').querySelector('[data-lineage-rule]').hasAttribute('hidden'), 'no lineage rule below lineage depth')
  assert.match(el.querySelector('[data-run-records-note]').textContent, /can't be left out/)

  // The chain: root, items, files, completion, manifest, run records (as records) and the inputs that stay references.
  const chain = el.querySelector('[data-copy-chain]')
  const at = (k) => chain.querySelector(`[data-chain-${k}]`).textContent
  assert.equal(at('root'), 'selection1')
  assert.equal(at('items'), 'items3')
  assert.equal(at('files'), 'files9')
  assert.equal(at('completion'), 'completiondistraught_murdock')
  assert.equal(at('manifest'), 'manifest+ config')
  assert.equal(at('run-records'), 'run records101 records')
  assert.equal(at('references'), 'inputs4 · stay as references')
  assert.ok(chain.querySelector('[data-chain-references]').classList.contains('dashed'))
  assert.equal(chain.querySelector('[data-chain-lineage]'), null)
  assert.equal(chain.querySelector('[data-chain-handoffs]'), null, 'an empty handoffs group has no fact')

  // The plan: four numbers, and provenance complete since the run's records are already there.
  assert.equal(big(el, 'blocks'), '116')
  assert.equal(eyebrow(el, 'present'), 'Already in org-s3')
  assert.equal(big(el, 'present'), '2')
  assert.equal(sub(el, 'present'), 'run records')
  assert.equal(big(el, 'transfer'), '114')
  assert.equal(big(el, 'upload'), '1.28 MB')
  assert.equal(sub(el, 'upload'), 'verified on write')
  assert.equal(el.querySelector('[data-provenance]').textContent,
    'Provenance is complete at the destination: the completion and manifest are already there, so lineage resolves in org-s3 on its own.')
  assert.ok(el.querySelector('[data-provenance]').classList.contains('ok'))
  assert.equal(el.querySelector('[data-unsized]'), null)
  assert.equal(el.querySelector('[data-audit-findings]'), null)

  // Claims: the name as a claim tag with who; annotations and projects have none; pins off with the mockup's note.
  const row = (cat) => el.querySelector(`[data-claims-row=${cat}]`)
  const name = row('names').querySelector('[data-carry-claim]')
  assert.equal(name.dataset.carryClaim, PLAN.plan.claims[0].cid)
  assert.equal(name.querySelector('.av').textContent, 'AL')
  assert.match(name.textContent, /^ALnamed“wigshop”.+ you$/)
  assert.equal(row('names').querySelector('input').checked, true)
  for (const [cat, text] of [['annotations', 'Annotations · none on these items'], ['projects', 'Projects · none on these items']]) {
    assert.equal(row(cat).textContent, text)
    assert.equal(row(cat).querySelector('input').disabled, true)
    assert.equal(row(cat).querySelector('input').checked, true)
  }
  assert.equal(row('pins').querySelector('input').checked, false)
  assert.equal(row('pins').querySelector('input').disabled, true)
  assert.equal(row('pins').textContent, 'Pins · none in roblab. Pins are per store; someone with write access confirms them in org-s3.')
  assert.match(el.querySelector('[data-claims-verbatim]').textContent, /^Claims are copied as-is, so "named by AL on Oct 24(, 2026)?" stays true in org-s3\.$/)

  // The command, verbatim, and the buttons.
  assert.equal(el.querySelector('[data-copy-command]').textContent, PLAN.command)
  assert.equal(el.querySelector('[data-copy-go]').textContent, 'Copy 114 blocks')
  assert.equal(el.querySelector('[data-copy-go]').disabled, false)
  assert.equal(el.querySelector('[data-copy-release]').textContent, 'Copy, then release in roblab…')
  assert.equal(el.querySelector('[data-copy-cancel]').getAttribute('href'), `#/selection/${WIGSHOP}`)
  assert.equal(el.querySelector('[data-accept-row]'), null, 'no "Copy anyway" without findings (audit checked 1, found 0)')
  assert.deepEqual(strayTexts(el), [], 'no null child written as text')
  assert.doesNotMatch(el.textContent, /null/)
})

test('a destination option is not styled as a bare page button, so its location and reason wrap inside the menu', () => {
  // The options are unclassed buttons inside .main; the bare-button look (one line, 28px high) made a long
  // "Can't copy here" reason run past the menu's right edge and below its bottom.
  assert.match(APP_CSS, /\.main button:not\(\[class\]\):not\(\[role=option\]\), \.rail button:not\(\[class\]\):not\(\[role=option\]\)/)
  assert.match(APP_CSS, /\.main button:not\(\[class\]\):not\(\[role=option\]\):hover/)
})

test('the destination switch: only stores that can take a copy are choosable, the others say why; choosing one asks again', async () => {
  const list = [...LIST, member('lab2', 'c')]
  const client = fakeClient()
  const { el } = await page({ client, list })
  await ready(el)
  await el.querySelector('[data-copy-dest]').click()
  const options = el.querySelectorAll('[data-dest-option]')
  assert.deepEqual(options.map(o => o.dataset.destOption), ['org-s3', 'ana-hpc', 'lab2'], 'every store but the source')
  const ana = options[1]
  assert.equal(ana.disabled, true)
  assert.equal(ana.querySelector('[data-read-only]').textContent, "Can't copy here: readOnly is set for this store")
  assert.equal(options[0].getAttribute('aria-selected'), 'true')
  await ana.click()
  assert.equal(client.asked.length, 1, 'a read-only store is not asked for')
  await el.querySelector('[data-copy-dest]').click()
  await el.querySelectorAll('[data-dest-option]')[2].click()
  await ready(el)
  assert.equal(client.asked[1][1].to, 'lab2')
  assert.equal(eyebrow(el, 'present'), 'Already in lab2')
  assert.match(el.querySelector('[data-copy-command]').textContent, /--to lab2 /)
})

test('changing depth or claims asks again and the command follows; an older answer arriving last is dropped', async () => {
  const held = []
  const client = fakeClient({ plan: (r) => (r.depth === 'lineage' ? new Promise(resolve => held.push(() => resolve(answer(r)))) : answer(r)) })
  const { el } = await page({ client })
  await ready(el)
  const lineage = el.querySelector('[data-depth=lineage]')
  lineage.checked = true
  await lineage.fire('change')
  assert.equal(el.dataset.planState, 'loading')
  await until(() => client.asked.length === 2, 'the lineage dry run to be asked')
  // Before lineage answers, the reader picks all.
  const all = el.querySelector('[data-depth=all]')
  all.checked = true
  await all.fire('change')
  await ready(el)
  assert.match(el.querySelector('[data-copy-command]').textContent, /--depth all /)
  assert.equal(big(el, 'transfer'), '999')
  held[0]()
  await tick(); await tick()
  assert.match(el.querySelector('[data-copy-command]').textContent, /--depth all /, 'the lineage answer came last and was dropped')
  assert.equal(big(el, 'transfer'), '999')
  assert.ok(el.querySelector('[data-depth-card=all]').classList.contains('on'))
  assert.deepEqual(client.asked.map(([, r]) => r.depth), ['results', 'lineage', 'all'])
  // At lineage depth and above, the lineage card says which rule applied.
  const rule = el.querySelector('[data-depth-card=lineage]').querySelector('[data-lineage-rule]')
  assert.equal(rule.hasAttribute('hidden'), false)
  assert.equal(rule.textContent, 'same samples (no task lineage in roblab)')
  const results = el.querySelector('[data-depth=results]')
  results.checked = true
  await results.fire('change')
  await ready(el)
  assert.equal(rule.textContent, 'same samples (no task lineage in roblab)', 'the rule stays known back at results')

  // Unticking names asks without it; the name stays listed as not carried.
  const names = el.querySelector('[data-claim-category=names]')
  names.checked = false
  await names.fire('change')
  await ready(el)
  assert.deepEqual(client.asked.at(-1)[1].claims, ['projects', 'annotations'])
  assert.match(el.querySelector('[data-copy-command]').textContent, /--claims projects,annotations /)
  assert.match(el.querySelector('[data-claims-row=names]').textContent, /“wigshop”.*Names · not carried/)
})

test('lineageRuleText: task lineage, the join-key fallback, a mix, or nothing below lineage depth', () => {
  assert.equal(lineageRuleText([{ lineage: 'tasks' }], 'roblab'), 'from task lineage')
  assert.equal(lineageRuleText([{ lineage: 'join_key' }], 'roblab'), 'same samples (no task lineage in roblab)')
  assert.equal(lineageRuleText([{ lineage: 'tasks' }, { lineage: 'join_key' }], 'roblab'), 'from task lineage where roblab has it, else same samples')
  assert.equal(lineageRuleText([{ lineage: null }], 'roblab'), null)
})

test('pins are off by default; an unticked pin still shows who pinned it and when, and ticking carries it', async () => {
  const pin = { cid: 'bafyreipin', category: 'pins', subject: WIGSHOP, verb: 'add', attribute: 'pin', value: 'figure 3', asserted_by: 'ana@example.org',
    timestamp: '2026-10-24T20:00:00.000Z', present: false, current: true, supersedes: [] }
  const client = fakeClient({ plan: (r) => answer(r, (p) => {
    p.plan.claims.push({ ...pin, carried: r.claims.includes('pins') })
    if (!r.claims.includes('pins')) p.plan.skipped_claims.pins = 1
    return p
  }) })
  const { el } = await page({ client })
  await ready(el)
  const box = el.querySelector('[data-claim-category=pins]')
  assert.equal(box.checked, false)
  assert.equal(box.disabled, false)
  const row = el.querySelector('[data-claims-row=pins]')
  const tag = row.querySelector('[data-carry-claim=bafyreipin]')
  assert.equal(tag.dataset.carried, 'false')
  assert.ok(tag.classList.contains('muted'), 'not carried reads dimmed')
  assert.equal(tag.querySelector('.av').textContent, 'AN')
  assert.equal(tag.querySelector('.age').getAttribute('title'), fullWhenText(pin.timestamp), 'when, to the reader')
  assert.match(tag.textContent, /pinned“figure 3”.*ana@example\.org$/)
  assert.match(row.textContent, /Pins · not carried.*Pins are per store; someone with write access confirms them in org-s3\./)
  box.checked = true
  await box.fire('change')
  await ready(el)
  assert.deepEqual(client.asked.at(-1)[1].claims, ['names', 'pins', 'projects', 'annotations'])
  assert.equal(el.querySelector('[data-carry-claim=bafyreipin]').dataset.carried, 'true')
  assert.equal(el.querySelector('[data-claims-row=pins]').querySelector('[data-not-carried]'), null)
})

test('opened with claims=none, the name claim still shows, marked not carried, and ticking a category keeps its claims while it asks', async () => {
  let release
  const client = fakeClient({ plan: (r) => (r.claims[0] !== 'none' ? new Promise(resolve => { release = () => resolve(answer(r)) }) : answer(r)) })
  const { el } = await page({ client, query: { claims: [] } })
  await ready(el)
  assert.deepEqual(client.asked[0][1].claims, ['none'], 'no category ticked asks for none, never an empty list')
  assert.match(el.querySelector('[data-copy-command]').textContent, /--claims none /)
  const row = el.querySelector('[data-claims-row=names]')
  assert.equal(row.querySelector('input').checked, false)
  assert.equal(row.querySelector('input').disabled, false)
  const tag = row.querySelector('[data-carry-claim]')
  assert.equal(tag.dataset.carried, 'false')
  assert.match(tag.textContent, /^ALnamed“wigshop”/)
  assert.match(row.textContent, /Names · not carried/)
  assert.match(el.querySelector('[data-claims-verbatim]').textContent, /who made each one, and when,/)
  const box = row.querySelector('input')
  box.checked = true
  await box.fire('change')
  assert.equal(el.dataset.planState, 'loading')
  const loading = el.querySelector('[data-claims-row=names]')
  assert.ok(loading.querySelector('[data-carry-claim]'), 'the claim stays listed while the plan is asked again')
  assert.equal(loading.querySelector('input').disabled, false)
  await until(() => client.asked.length === 2, 'the dry run with names')
  release()
  await ready(el)
  assert.equal(el.querySelector('[data-carry-claim]').dataset.carried, 'true')
})

test('a renamed Selection: only the current name is a tag, the earlier one is counted, and the example uses the current name', async () => {
  const old = { cid: 'bafyreioldname', category: 'names', subject: WIGSHOP, verb: 'set', attribute: 'name', value: 'bigwigs', asserted_by: 'ana@example.org',
    timestamp: '2026-10-20T10:00:00.000Z', present: false, current: false, supersedes: [] }
  const client = fakeClient({ plan: (r) => answer(r, (p) => {
    p.plan.claims[0].supersedes = [old.cid]
    p.plan.claims.unshift({ ...old, carried: r.claims.includes('names') })
    return p
  }) })
  const { el } = await page({ client })
  await ready(el)
  const row = el.querySelector('[data-claims-row=names]')
  assert.deepEqual(row.querySelectorAll('[data-carry-claim]').map(t => t.dataset.carryClaim), [PLAN.plan.claims[0].cid])
  assert.equal(row.querySelector('[data-claims-history]').textContent, '+ 1 earlier claim it supersedes')
  assert.match(el.querySelector('[data-claims-verbatim]').textContent, /"named by AL on Oct 24(, 2026)?"/)
})

test('currentClaims: the current flag, else what nothing supersedes, else all', async () => {
  const { currentClaims } = await import('../src/views/copy.js')
  assert.deepEqual(currentClaims([{ cid: 'a', current: false }, { cid: 'b', current: true }]).map(c => c.cid), ['b'])
  assert.deepEqual(currentClaims([{ cid: 'a' }, { cid: 'b', supersedes: ['a'] }]).map(c => c.cid), ['b'])
  assert.deepEqual(currentClaims([{ cid: 'a' }, { cid: 'b' }]).map(c => c.cid), ['a', 'b'])
})

test('no runs in the plan: no claim that run records travel', async () => {
  const client = fakeClient({ plan: (r) => answer(r, (p) => { p.plan.runs = []; return p }) })
  const { el } = await page({ client })
  await ready(el)
  const n = el.querySelector('[data-provenance]')
  assert.ok(n.classList.contains('warn'))
  assert.doesNotMatch(n.textContent, /travel with these results/)
})

test('audit findings and unsized blocks: warned in the plan, and copying waits for "Copy anyway", which sends accept_audit', async () => {
  const finding = { cid: PLAN.plan.runs[0].manifest, kind: 'RunManifest', where: 'params.outdir', what: 'absolute path' }
  const copied = []
  const client = fakeClient({
    plan: (r) => answer(r, (p) => { p.plan.audit.findings = [finding]; p.plan.unsized = 3; return p }),
    copy: (r) => { copied.push(r); return { ...answer(r), dry_run: false, copied: 114, copied_bytes: 1279431, transfer_entry: 'bafyreitransfer', failures: [], snapshot: 'written' } },
  })
  const { el } = await page({ client })
  await ready(el)
  const audit = el.querySelector('[data-audit-findings]')
  assert.equal(audit.dataset.auditFindings, '1')
  const li = audit.querySelector('[data-finding]')
  assert.equal(li.querySelector('.addr').dataset.address, finding.cid)
  assert.match(li.textContent, /params\.outdir absolute path \(RunManifest\)$/)
  assert.equal(el.querySelector('[data-unsized]').textContent, '3 blocks could not be sized, so the bytes are at least the figure shown.')
  assert.equal(big(el, 'upload'), 'at least 1.28 MB')
  const go = el.querySelector('[data-copy-go]')
  assert.equal(go.disabled, true, 'findings hold the copy back')
  assert.ok(el.querySelector('[data-accept-row]'))
  const accept = el.querySelector('[data-accept-audit]')
  const dry = el.querySelector('[data-copy-command]').textContent
  assert.doesNotMatch(dry, /--accept-audit/)
  accept.checked = true
  await accept.fire('change')
  assert.equal(go.disabled, false)
  assert.equal(el.querySelector('[data-copy-command]').textContent, dry.replace(' --dry-run', ' --accept-audit true --dry-run'),
    'the CLI command copies anyway too')
  await go.click()
  assert.equal(copied.length, 1)
  assert.equal(copied[0].accept_audit, true)
})

test('a copy the server refuses for its audit shows the findings and copies nothing', async () => {
  const finding = { cid: PLAN.plan.runs[0].manifest, kind: 'RunManifest', where: 'config', what: 'credential-like value' }
  const client = fakeClient({ copy: (r) => ({ ...answer(r, (p) => { p.plan.audit.findings = [finding]; return p }), dry_run: false, refused: 'audit',
    command: answer(r).command.replace(' --dry-run', '') }) })
  const { el, ctx } = await page({ client })
  await ready(el)
  const command = el.querySelector('[data-copy-command]').textContent
  await el.querySelector('[data-copy-go]').click()
  assert.equal(el.querySelector('[data-copy-command]').textContent, command, 'the dry run\'s command, not the refused copy\'s')
  assert.equal(el.dataset.copyState, 'refused')
  assert.match(el.querySelector('[data-copy-status]').textContent, /Nothing was copied/)
  assert.ok(el.querySelector('[data-audit-findings]'))
  assert.equal(el.querySelector('[data-copy-go]').disabled, true)
  assert.deepEqual(ctx.forgot, [], 'nothing written, nothing to forget')
})

test('the copy: one attempt at a time, then the result, the destination forgotten, and the release offer with the bytes it frees', async () => {
  let finish
  const client = fakeClient({ copy: (r) => new Promise(resolve => { finish = () => resolve({ ...answer(r), dry_run: false, copied: 114, copied_bytes: 1279431,
    transfer_entry: 'bafyreigtransferentryaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', failures: [], snapshot: 'written' }) }) })
  const { el, ctx } = await page({ client })
  await ready(el)
  const go = el.querySelector('[data-copy-go]')
  const pending = go.click()
  await tick()
  assert.equal(el.dataset.copyState, 'running')
  assert.match(el.querySelector('[data-copy-running]').textContent, /^Copying 114 blocks \(1\.28 MB\) to org-s3\. org-s3 checks each block against its address/)
  assert.equal(go.disabled, true)
  assert.equal(el.querySelector('[data-copy-release]').disabled, true)
  await el.querySelector('[data-copy-release]').click()
  assert.equal(client.asked.filter(([k]) => k === 'copy').length, 1, 'a second click while one runs is ignored')
  finish()
  await pending
  await until(() => el.querySelector('[data-release-frees]')?.dataset.bytes !== undefined, 'the freed bytes')
  assert.equal(el.dataset.copyState, 'copied')
  const result = el.querySelector('[data-copy-result]')
  assert.equal(result.querySelector('h2').textContent, 'Copied to org-s3')
  assert.match(result.textContent, /Copied114 blocks · 1\.28 MB/)
  // The Transfer record: the kit's address chip, linking the block in the destination.
  const record = result.querySelector('[data-transfer-entry]')
  assert.match(result.textContent, /Transfer record/)
  assert.doesNotMatch(result.textContent, /Transfer entry/)
  const chip = record.querySelector('a')
  assert.ok(chip.classList.contains('addr'))
  assert.ok(chip.querySelector('.seal'), 'with its seal')
  assert.equal(chip.textContent, 'bafy…aaaaaa')
  assert.equal(chip.getAttribute('href'), '?member=org-s3#/block/bafyreigtransferentryaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
  assert.match(result.textContent, /Snapshotorg-s3's snapshot rewritten/)
  assert.deepEqual(ctx.forgot, [['presence'], ['members', 'org-s3'], ['sizes', 'org-s3']])
  assert.deepEqual(strayTexts(el), [])
  // A Selection has no release confirmation: the offer opens its page, where Delete is, and says what that frees here.
  const offer = el.querySelector('[data-release-offer]')
  assert.equal(offer.textContent, 'Release here (kept in org-s3)')
  assert.equal(offer.getAttribute('href'), `#/selection/${WIGSHOP}`)
  assert.equal(el.querySelector('[data-release-frees]').textContent, 'Frees 0 B in roblab: other runs, Selections, hand-offs or pins here hold the same files.')
  // The plan is asked again after the copy.
  await ready(el)
  assert.deepEqual(client.asked.map(([k]) => k), ['plan', 'copy', 'plan'])
})

test('a run: Copy, then release opens its Release confirmation, offered with the bytes it frees in roblab', async () => {
  // A run: root is listed once in destination_state, as role "root" (CopyService.destinationState).
  const runPlan = (r) => answer(r, (p) => { p.root = { kind: 'run', cid: DM }; p.plan.destination_state = [{ cid: DM, role: 'root', name: 'distraught_murdock', state: 'held' }]; return p })
  const client = fakeClient({ plan: runPlan, copy: (r) => ({ ...runPlan(r), dry_run: false, copied: 114, copied_bytes: 1279431, transfer_entry: 'bafyreitransfer', failures: [], snapshot: 'written' }) })
  const saved = globalThis.location
  globalThis.location = { hash: `#/run/${DM}/copy` }
  try {
    const { el } = await page({ client, kind: 'run', cid: DM })
    await ready(el)
    assert.equal(el.querySelector('h1').textContent, 'Copy distraught_murdock')
    assert.deepEqual(el.querySelector('.crumbs').querySelectorAll('a').map(a => a.textContent), ['Pipelines', 'nf-core/rnaseq', 'distraught_murdock'])
    assert.equal(client.asked[0][1].root, `run:${DM}`)
    assert.equal(el.querySelector('[data-copy-chain]').querySelector('[data-chain-root]').textContent, 'rundistraught_murdock')
    assert.equal(el.querySelector('[data-chain-completion]'), null, 'the root is the completion')
    await el.querySelector('[data-copy-release]').click()
    assert.equal(globalThis.location.hash, `#/run/${DM}?tab=storage&release=1`)
    const offer = el.querySelector('[data-release-offer]')
    assert.equal(offer.getAttribute('href'), `#/run/${DM}?tab=storage&release=1`)
    await until(() => el.querySelector('[data-release-frees]').dataset.bytes, 'the freed bytes')
    assert.equal(el.querySelector('[data-release-frees]').textContent, 'Frees 63.7 MB in roblab, once a sweep runs after the grace period.')
  } finally {
    globalThis.location = saved
  }
})

/** The copy client over a fake fetch: dry runs answer `answer(request)`, copies answer `respond(request)` as a Response. */
function realClient(respond) {
  return copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async (url, init) => {
    const r = JSON.parse(init.body)
    return String(url).endsWith('?dry_run=true') ? json(200, answer(r)) : respond(r)
  } })
}
const json = (status, value) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })

test('a failed copy (the server\'s real 500 body) lists each failure once, with no error beside it, and no release offer', async () => {
  // CopyService's partial-copy 500: the 1.3 body with copied, failures, null transfer entry and snapshot, and no error or message.
  const client = realClient((r) => json(500, { ...answer(r), dry_run: false, copied: 40, copied_bytes: 300000, transfer_entry: null,
    failures: [{ cid: 'bafkreifailed', error: 'hash mismatch' }], snapshot: null }))
  const { el, ctx } = await page({ client })
  await ready(el)
  await el.querySelector('[data-copy-go]').click()
  assert.equal(el.dataset.copyState, 'failed')
  assert.equal(el.querySelector('[data-error]'), null, 'the result panel says what failed; no message beside it')
  assert.doesNotMatch(el.querySelector('[data-copy-status]').textContent, /"plan"|\{/, 'never the JSON body as text')
  const result = el.querySelector('[data-copy-result]')
  assert.equal(result.querySelector('h2').textContent, 'Copy to org-s3 stopped')
  assert.match(result.textContent, /1 block failed, so the copy stopped before its transfer entry was written/)
  assert.match(result.querySelector('[data-failure]').textContent, /hash mismatch/)
  assert.match(result.textContent, /Transfer recordnot written/)
  assert.doesNotMatch(result.textContent, /Snapshot/, 'a null snapshot is not a line')
  assert.equal(el.querySelector('[data-release-offer]'), null)
  assert.deepEqual(ctx.forgot.map(f => f[0]), ['presence', 'members', 'sizes'], 'what was written is asked again')
  await ready(el)
  assert.equal(el.querySelector('[data-copy-go]').disabled, false, 'it can be tried again')
  assert.deepEqual(strayTexts(el), [])
})

test('a 500 with no plan body: the server\'s message, and that nothing or only part may have been copied', async () => {
  const { CopyError } = await import('../src/copy.js')
  const client = fakeClient({ copy: () => { throw new CopyError('copy_failed', 'org-s3: Access Denied (Service: S3, Status Code: 403)', 500, null) } })
  const { el, ctx } = await page({ client })
  await ready(el)
  await el.querySelector('[data-copy-go]').click()
  assert.equal(el.dataset.copyState, 'failed')
  const stopped = el.querySelector('[data-copy-stopped]')
  assert.equal(stopped.textContent, 'The copy to org-s3 stopped: org-s3: Access Denied (Service: S3, Status Code: 403). '
    + 'Nothing may have been copied, or only part of it. Copying again skips any blocks already in org-s3.')
  assert.equal(el.querySelector('[data-copy-result]'), null)
  assert.equal(el.querySelector('[data-release-offer]'), null)
  assert.deepEqual(ctx.forgot.map(f => f[0]), ['presence', 'members', 'sizes'], 'a partial copy may have written blocks')
  assert.deepEqual(strayTexts(el), [])
})

test('no Copy, then release when the page has no write, or when the source is read-only', async () => {
  let { el } = await page({ write: { available: false, here: false, reason: 'Open this page with the URL nf-blocks:explore printed; it carries the write token.', writable: 'roblab' } })
  await ready(el)
  assert.equal(el.querySelector('[data-copy-release]'), null)
  const line = el.querySelector('[data-release-unavailable]')
  assert.equal(line.textContent, "Copy, then release isn't offered: this page can't write to roblab.")
  assert.equal(line.getAttribute('title'), 'Open this page with the URL nf-blocks:explore printed; it carries the write token.')
  assert.equal(el.querySelector('[data-copy-go]').disabled, false, 'Copy itself still works')

  const list = [member('roblab', 'a', { writable: true, copy_target: false, copy_reason: 'readOnly is set for this store' }), ...LIST.slice(1)]
  ;({ el } = await page({ list }))
  await ready(el)
  assert.equal(el.querySelector('[data-copy-release]'), null)
  assert.equal(el.querySelector('[data-release-unavailable]').textContent, "Copy, then release isn't offered: this page can't write to roblab (readOnly is set for this store).")

  ;({ el } = await page())
  await ready(el)
  assert.ok(el.querySelector('[data-copy-release]'))
  assert.equal(el.querySelector('[data-release-unavailable]'), null)
})

test('an anonymous name claim reads "named anonymously" in the verbatim line', async () => {
  const client = fakeClient({ plan: (r) => answer(r, (p) => { p.plan.claims[0].asserted_by = 'anonymous'; return p }) })
  const { el } = await page({ client })
  await ready(el)
  assert.match(el.querySelector('[data-claims-verbatim]').textContent, /^Claims are copied as-is, so "named anonymously on Oct 24(, 2026)?" stays true in org-s3\.$/)
})

test('disabled states: nothing to copy, no write here for the release, no copy at all, a plan that fails', async () => {
  const none = fakeClient({ plan: (r) => answer(r, (p) => { p.plan.transfer = 0; p.plan.transfer_bytes = 0; Object.assign(p.depths.results, { transfer: 0, transfer_bytes: 0 }); return p }) })
  let { el } = await page({ client: none, write: { available: true, here: false, writable: 'org-s3' } })
  await ready(el)
  assert.equal(el.querySelector('[data-copy-go]').textContent, 'Nothing to copy')
  assert.equal(el.querySelector('[data-depth-transfer=results]').textContent, 'Nothing to transfer to org-s3')
  assert.equal(el.querySelector('[data-copy-go]').disabled, true)
  // The source can't take a release from this page: no disabled "Copy, then release", one muted line saying why.
  assert.equal(el.querySelector('[data-copy-release]'), null)
  assert.equal(el.querySelector('[data-release-unavailable]').textContent, "Copy, then release isn't offered: this explore server writes to org-s3, not roblab.")
  assert.ok(el.querySelector('[data-release-unavailable]').classList.contains('muted'))

  const reason = 'Open this page with the URL nf-blocks:explore printed; it carries the token copying needs.'
  const unavailable = fakeClient()
  ;({ el } = await page({ client: unavailable, copy: { available: false, reason, targets: [] } }))
  await tick()
  assert.equal(unavailable.asked.length, 0, 'no dry run without copy')
  assert.equal(el.querySelector('[data-copy-unavailable]').textContent, reason)
  assert.deepEqual(strayTexts(el), [])
  assert.equal(el.querySelector('[data-copy-go]').disabled, true)
  assert.equal(el.querySelector('[data-depth=results]').disabled, true)

  const failing = fakeClient({ plan: async () => { throw Object.assign(new Error('no such member: org-s3'), { code: 'bad_request' }) } })
  ;({ el } = await page({ client: failing }))
  await until(() => el.dataset.planState === 'error', 'the error')
  assert.equal(el.querySelector('[data-error]').textContent, 'no such member: org-s3')
  assert.equal(el.querySelector('[data-copy-go]').disabled, true)
})

test('Copy to…: a link to the copy page when this page can copy, else a disabled button saying why', () => {
  installDom()
  const on = copyToButton({ copy: { available: true, reason: null, targets: ['org-s3'] } }, 'selection', WIGSHOP, { primary: true })
  assert.equal(on.tagName, 'A')
  assert.equal(on.getAttribute('href'), `#/selection/${WIGSHOP}/copy`)
  assert.equal(on.getAttribute('class'), 'btn primary')
  const off = copyToButton({ copy: { available: false, reason: 'No other store can take a copy: org-s3 (readOnly is set for this store).' } }, 'run', DM)
  assert.equal(off.dataset.copyTo, 'unavailable')
  assert.equal(off.getAttribute('title'), 'No other store can take a copy: org-s3 (readOnly is set for this store).')
  assert.equal(off.querySelector('button').disabled, true)
  const plain = copyToButton({}, 'run', DM)
  assert.match(plain.getAttribute('title'), /opened through `nextflow plugin nf-blocks:explore`/)
})

// ---- the one-member budget ----------------------------------------------------

test('with one member the run page asks nothing more for Copy to…, and its button says why it is off', async () => {
  const { buildMember, blockFetch } = await import('./fixture.mjs')
  const { snapshotDb } = await import('./helpers.mjs')
  const { resolveStore } = await import('../src/store.js')
  const { BlockFetcher } = await import('../src/blocks.js')
  const { createMembers, memberOpener } = await import('../src/members.js')
  const { copyClient } = await import('../src/copy.js')
  const { Tray } = await import('../src/tray.js')
  const { run } = await import('../src/views/run.js')
  const PAGE = 'http://h/index.html?token=t'
  const built = await buildMember({ now: Date.now() })
  async function oneMember() {
    const asked = []
    const listing = { members: [{ alias: 'lab', writable: true, base: 'm/lab/', copy_target: true }], copy: true, write: true }
    const blocks = blockFetch(built.blocks)
    const fetchFn = async (url, init = {}) => {
      const u = String(url)
      asked.push(`${init.method ?? 'GET'} ${u}`)
      if (u.endsWith('/members.json')) return new Response(JSON.stringify(listing), { headers: { 'Content-Type': 'application/json' } })
      if (u.endsWith('/log/')) return new Response(JSON.stringify({ entries: built.log }), { headers: { 'Content-Type': 'application/json' } })
      return blocks(url, init)
    }
    const store = await resolveStore(PAGE, { fetchFn })
    const members = createMembers(store, { openExplorer: memberOpener(store, { pageUrl: PAGE, primaryBlocks: new BlockFetcher(store.base, { fetchFn }),
      openDb: async (url) => { await fetchFn(url); return snapshotDb(built.snapshot) }, fetchFn }) })
    const ex = await members.explorer(store.member)
    return { ex, store, asked, fetchFn }
  }
  const ctxOf = (copy) => ({ tray: new Tray(null), rerender: () => {}, trayChanged: () => {}, progress: () => {}, use: () => {}, mark: () => {},
    store: { name: 'lab', key: 'a', label: 'lab', others: () => [] }, ...(copy ? { copy } : {}),
    write: { available: false, here: false, reason: 'read only', hrefFor: (x) => x } })
  const settle = async () => { for (let i = 0; i < 6; i++) await new Promise(r => setTimeout(r, 20)) }

  installDom(); resetFolds()
  const withCopy = await oneMember()
  const copy = { ...copyAvailability(withCopy.store, { token: 't' }), client: copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: withCopy.fetchFn }) }
  assert.equal(copy.available, false)
  const start = withCopy.asked.length
  const page = await run(withCopy.ex, built.runs.R1.completion, ctxOf(copy), { tab: 'outputs' })
  await settle()
  const drawn = withCopy.asked.slice(start)
  assert.equal(page.querySelector('[data-copy-to]').getAttribute('title'), 'There is no other store in this configuration to copy to.')
  assert.deepEqual(drawn.filter(a => a.includes('api/copy')), [])

  installDom(); resetFolds()
  const before = await oneMember()
  const from = before.asked.length
  await run(before.ex, built.runs.R1.completion, ctxOf(null), { tab: 'outputs' })
  await settle()
  assert.ok(drawn.length > 0, 'the page reads something, so the comparison is not empty')
  assert.deepEqual(drawn, before.asked.slice(from), 'the same requests as the page without Copy to…')
})

// ---- claims none (C1) -----------------------------------------------------------

test('unticking every category asks for none, never an empty list, and the plan still answers', async () => {
  const extra = [
    { cid: 'bafyreiproject', category: 'projects', subject: WIGSHOP, verb: 'add', attribute: 'project', value: 'atlas', asserted_by: 'ana@example.org',
      timestamp: '2026-10-24T20:00:00.000Z', present: false, current: true, supersedes: [] },
    { cid: 'bafyreicondition', category: 'annotations', subject: WIGSHOP, verb: 'set', attribute: 'condition', value: 'treated', asserted_by: 'ana@example.org',
      timestamp: '2026-10-24T20:00:00.000Z', present: false, current: true, supersedes: [] },
  ]
  const client = fakeClient({ plan: (r) => answer(r, (p) => {
    for (const c of extra) p.plan.claims.push({ ...c, carried: r.claims.includes(c.category) })
    return p
  }) })
  const { el } = await page({ client })
  await ready(el)
  for (const cat of ['names', 'projects', 'annotations']) {
    const box = el.querySelector(`[data-claim-category=${cat}]`)
    assert.equal(box.disabled, false, `${cat} can be unticked`)
    box.checked = false
    await box.fire('change')
  }
  await ready(el)
  assert.deepEqual(client.asked.at(-1)[1].claims, ['none'])
  assert.equal(el.querySelector('[data-error]'), null)
  assert.match(el.querySelector('[data-copy-command]').textContent, /--claims none /)
  assert.equal(el.querySelector('[data-copy-go]').disabled, false)
})

test('a plan that carries hand-offs: the chain row names them, with their blocks, and their bytes in the title', async () => {
  const client = fakeClient({ plan: (r) => answer(r, (p) => {
    Object.assign(p.plan.groups.find(g => g.group === 'handoffs'), { blocks: 3, bytes: 1234, transfer: 3, transfer_bytes: 1234 })
    return p
  }) })
  const { el } = await page({ client })
  await ready(el)
  const fact = el.querySelector('[data-copy-chain]').querySelector('[data-chain-handoffs]')
  assert.equal(fact.textContent, 'hand-offs3 blocks')
  assert.ok(fact.classList.contains('fact'))
  assert.equal(fact.getAttribute('title'), `The hand-offs that cite a Selection this copy carries, with their files: ${bytes(1234)}.`)
})

// ---- kept at the destination (D9) --------------------------------------------------

/** The plan when distraught_murdock is released in the destination: not kept. */
const released = (p) => { p.plan.kept = false; p.plan.destination_state[1].state = 'released'; return p }

test('a dry run whose run is released at the destination: a note naming it and linking it there, Copy held back, no Copy-then-release', async () => {
  const client = fakeClient({ plan: (r) => answer(r, released) })
  const { el } = await page({ client })
  await ready(el)
  const note = el.querySelector('[data-not-kept]')
  assert.ok(note, 'the not-kept note')
  assert.ok(note.classList.contains('warn'))
  const line = note.querySelector(`[data-not-kept-entry="${DM}"]`)
  assert.equal(line.textContent, 'distraught_murdock is released in org-s3, so its next sweep would take what this copy adds.')
  assert.equal(line.querySelector('a').getAttribute('href'), `?member=org-s3#/run/${DM}`)
  // This server writes to roblab, so a Restore on org-s3's page would write beside roblab: the way out is to open org-s3 as the writable store.
  const way = note.querySelector('[data-not-kept-way-out]')
  assert.equal(way.dataset.notKeptWayOut, 'explore')
  // NXF_CONFIG_FILE replaces ./nextflow.config, so the explorer's file must bring the stores along (includeConfig, or the cas.stores block).
  assert.equal(way.querySelector('[data-not-kept-config]').textContent, "includeConfig 'nextflow.config'\nlineage.store.location = 'cas://org-s3'")
  assert.equal(way.textContent, "This page writes to roblab, so it can't restore anything in org-s3. To restore it, open org-s3 as the writable store: "
    + 'make a config file beside your nextflow.config holding these two lines (or your cas.stores block in place of the includeConfig line), '
    + 'and start nextflow plugin nf-blocks:explore with NXF_CONFIG_FILE=<that file>.'
    + "includeConfig 'nextflow.config'\nlineage.store.location = 'cas://org-s3'"
    + "Don't put the lineage line in the config your pipelines use: they would start writing into org-s3."
    + 'Then Restore the run on its Storage tab, and copy again.')
  // The same route and warning as the CLI's not_kept message (CopyCommand): NXF_CONFIG_FILE, since -c doesn't reach plugin verbs.
  assert.equal(way.querySelector('[data-not-kept-pipelines]').textContent,
    "Don't put the lineage line in the config your pipelines use: they would start writing into org-s3.")
  assert.doesNotMatch(note.textContent, /Restore it in org-s3 first/)
  // The plan's provenance is complete, but nothing will be copied: no green note beside the refusal.
  assert.equal(el.querySelector('[data-provenance]'), null)
  assert.ok(!el.querySelector('.plan-notes').querySelectorAll('.note').some(n => n.classList.contains('ok')))
  const go = el.querySelector('[data-copy-go]')
  assert.equal(go.disabled, true)
  assert.equal(go.getAttribute('title'), 'org-s3 has released or deleted distraught_murdock: restore it in org-s3 first.')
  assert.equal(el.querySelector('[data-copy-release]'), null, 'no Copy, then release')
  assert.deepEqual(strayTexts(el), [])
})

test('a deleted root at the destination links its own page there', async () => {
  const client = fakeClient({ plan: (r) => answer(r, (p) => { p.plan.kept = false; p.plan.destination_state[0].state = 'deleted'; return p }) })
  const { el } = await page({ client })
  await ready(el)
  const line = el.querySelector(`[data-not-kept-entry="${WIGSHOP}"]`)
  assert.equal(line.textContent, 'wigshop is deleted in org-s3, so its next sweep would take what this copy adds.')
  assert.equal(line.querySelector('a').getAttribute('href'), `?member=org-s3#/selection/${WIGSHOP}`)
  assert.match(el.querySelector('[data-not-kept-way-out]').textContent, /Then Undo delete on the Selection's page, and copy again\.$/)
})

test('a dry run held back by a destination this server writes to: the line links its page there and says to restore it there', async () => {
  const client = fakeClient({ plan: (r) => answer(r, released) })
  const { el } = await page({ client, write: { available: true, here: false, reason: null, writable: 'org-s3' } })
  await ready(el)
  const line = el.querySelector(`[data-not-kept-entry="${DM}"]`)
  assert.equal(line.textContent, 'distraught_murdock is released in org-s3, so its next sweep would take what this copy adds. Restore it there.')
  assert.equal(line.querySelector('a').getAttribute('href'), `?member=org-s3#/run/${DM}`)
  assert.equal(el.querySelector('[data-not-kept-way-out]'), null, 'no other way out to name')
  assert.equal(el.querySelector('[data-not-kept-config]'), null)
})

test('a destination this server writes to, on a page without the write token: no "Restore it there", open the page with its token instead', async () => {
  const client = fakeClient({ plan: (r) => answer(r, released) })
  const reason = 'Open this page with the URL nf-blocks:explore printed; it carries the write token.'
  const { el } = await page({ client, write: { available: false, here: false, reason, writable: 'org-s3' } })
  await ready(el)
  const line = el.querySelector(`[data-not-kept-entry="${DM}"]`)
  assert.equal(line.textContent, 'distraught_murdock is released in org-s3, so its next sweep would take what this copy adds.')
  const way = el.querySelector('[data-not-kept-way-out]')
  assert.equal(way.dataset.notKeptWayOut, 'token')
  assert.equal(way.textContent, `This page can't write to org-s3: ${reason} Then Restore the run on its Storage tab, and copy again.`)
  assert.equal(el.querySelector('[data-not-kept-config]'), null, 'org-s3 is already the writable store: no config line')
})

test('a released run and a deleted Selection at a destination this server does not write to: one way out naming both steps', async () => {
  const client = fakeClient({ plan: (r) => answer(r, (p) => { released(p); p.plan.destination_state[0].state = 'deleted'; return p }) })
  const { el } = await page({ client })
  await ready(el)
  assert.equal(el.querySelector('[data-not-kept]').dataset.notKept, '2')
  const way = el.querySelector('[data-not-kept-way-out]')
  assert.match(way.textContent, /To restore them, open org-s3 as the writable store/)
  assert.match(way.textContent, /Then Undo delete on the Selection's page and Restore the run on its Storage tab, and copy again\.$/)
})

test('a copy the server refuses as not kept (409 not_kept) shows the same note, copies nothing and keeps the dry-run command', async () => {
  const client = realClient((r) => json(409, { ...answer(r, released), dry_run: false, refused: 'not_kept',
    command: answer(r).command.replace(' --dry-run', '') }))
  const { el, ctx } = await page({ client })
  await ready(el)
  const command = el.querySelector('[data-copy-command]').textContent
  await el.querySelector('[data-copy-go]').click()
  assert.equal(el.dataset.copyState, 'refused')
  assert.match(el.querySelector('[data-copy-status]').textContent, /^Nothing was copied: org-s3 has released or deleted distraught_murdock\./)
  assert.equal(el.querySelector(`[data-not-kept-entry="${DM}"]`).textContent,
    'distraught_murdock is released in org-s3, so its next sweep would take what this copy adds.')
  assert.ok(el.querySelector('[data-not-kept-way-out]'))
  assert.equal(el.querySelector('[data-copy-go]').disabled, true)
  assert.equal(el.querySelector('[data-copy-release]'), null)
  assert.equal(el.querySelector('[data-copy-command]').textContent, command, 'the dry run\'s command, not the refused copy\'s')
  assert.deepEqual(ctx.forgot, [], 'nothing written, nothing to forget')
})

test('a copy whose result does not say kept true offers no release here', async () => {
  const client = fakeClient({ copy: (r) => answer(r, (p) => {
    delete p.plan.kept
    return Object.assign(p, { dry_run: false, copied: 114, copied_bytes: 1279431, transfer_entry: 'bafyreitransfer', failures: [], snapshot: 'written' })
  }) })
  const { el } = await page({ client })
  await ready(el)
  await el.querySelector('[data-copy-release]').click()
  assert.equal(el.dataset.copyState, 'copied')
  assert.ok(el.querySelector('[data-copy-result]'))
  assert.equal(el.querySelector('[data-release-offer]'), null)
  assert.equal(el.querySelector('[data-release-not-offered]').textContent,
    "org-s3 did not say it keeps this copy, so releasing it in roblab is not offered.")
})

// ---- a copy outlives its page (I3) ------------------------------------------------

test('a copy outlives its page: another copy page says one is running and redraws when it ends; the page that asked is gone, so no release confirmation opens', async () => {
  let finish
  // A run: root is listed once in destination_state, as role "root" (CopyService.destinationState).
  const runPlan = (r) => answer(r, (p) => { p.root = { kind: 'run', cid: DM }; p.plan.destination_state = [{ cid: DM, role: 'root', name: 'distraught_murdock', state: 'held' }]; return p })
  const client = fakeClient({ plan: runPlan, copy: (r) => new Promise(resolve => { finish = () => resolve({ ...runPlan(r), dry_run: false, copied: 114,
    copied_bytes: 1279431, transfer_entry: 'bafyreitransfer', failures: [], snapshot: 'written' }) }) })
  const saved = globalThis.location
  globalThis.location = { hash: `#/run/${DM}/copy` }
  try {
    installDom(); resetFolds()
    const { explorer } = await openRoblab()
    const ctx = ctxFor({ client })
    const first = await copyView(explorer, 'run', DM, ctx, {})
    await ready(first)
    const pending = first.querySelector('[data-copy-release]').click()
    await tick()
    assert.equal(first.dataset.copyState, 'running')
    // The reader moves on: the app renders another page (Explorer.pageChanged), then a copy page for a Selection.
    explorer.pageChanged()
    globalThis.location.hash = `#/selection/${WIGSHOP}/copy`
    const other = await copyView(explorer, 'selection', WIGSHOP, ctx, {})
    await ready(other)
    assert.equal(other.querySelector('[data-copy-elsewhere]').textContent,
      'A copy to org-s3 is running. This page can copy when it ends: one copy runs at a time.')
    const go = other.querySelector('[data-copy-go]')
    assert.equal(go.disabled, true)
    assert.equal(go.getAttribute('title'), 'A copy to org-s3 is running; this page can copy when it ends.')
    assert.equal(other.querySelector('[data-depth=lineage]').disabled, false, 'its own choices stay open')
    finish()
    await pending
    await until(() => !other.querySelector('[data-copy-elsewhere]') && other.dataset.planState === 'ready', 'the other page to redraw')
    assert.equal(go.disabled, false)
    assert.equal(go.hasAttribute('title'), false)
    assert.equal(globalThis.location.hash, `#/selection/${WIGSHOP}/copy`, 'the reader stays where they are')
    assert.equal(client.asked.at(-1)[1].root, `selection:${WIGSHOP}`, 'the Selection page asked again once the copy ended')
    // Back on the run's copy page, the last copy's result and its release offer are there.
    const back = await copyView(explorer, 'run', DM, ctx, {})
    assert.equal(back.dataset.copyState, 'copied')
    assert.equal(back.querySelector('[data-copy-result]').querySelector('h2').textContent, 'Copied to org-s3')
    assert.equal(back.querySelector('[data-release-offer]').getAttribute('href'), `#/run/${DM}?tab=storage&release=1`)
  } finally {
    globalThis.location = saved
  }
})

test('a copy page for the root being copied says so, and shows the result when the copy ends', async () => {
  let finish
  const client = fakeClient({ copy: (r) => new Promise(resolve => { finish = () => resolve({ ...answer(r), dry_run: false, copied: 114, copied_bytes: 1279431,
    transfer_entry: 'bafyreitransfer', failures: [], snapshot: 'written' }) }) })
  installDom(); resetFolds()
  const { explorer } = await openRoblab()
  const ctx = ctxFor({ client })
  const first = await copyView(explorer, 'selection', WIGSHOP, ctx, {})
  await ready(first)
  const pending = first.querySelector('[data-copy-go]').click()
  await tick()
  explorer.pageChanged()
  const second = await copyView(explorer, 'selection', WIGSHOP, ctx, {})
  await ready(second)
  assert.equal(second.querySelector('[data-copy-elsewhere]').textContent, 'This Selection is being copied to org-s3. Its result shows here when the copy ends.')
  finish()
  await pending
  await until(() => second.dataset.copyState === 'copied', 'the result on the new page')
  assert.equal(second.querySelector('[data-copy-result]').querySelector('h2').textContent, 'Copied to org-s3')
  assert.ok(second.querySelector('[data-release-offer]'))
  assert.deepEqual(ctx.forgot, [['presence'], ['members', 'org-s3'], ['sizes', 'org-s3']], 'the destination is forgotten once')
})

// ---- re-plans settle first; superseded is not an error (I4, D10) ----------------------

test('changes made in quick succession ask for one dry run once they settle', async () => {
  const client = fakeClient()
  const { el } = await page({ client })
  await ready(el)
  for (const d of ['lineage', 'all', 'lineage']) {
    const radio = el.querySelector(`[data-depth=${d}]`)
    radio.checked = true
    await radio.fire('change')
  }
  const names = el.querySelector('[data-claim-category=names]')
  names.checked = false
  await names.fire('change')
  assert.equal(el.dataset.planState, 'loading')
  assert.equal(el.querySelector('[data-copy-go]').disabled, true, 'no copy from a plan for other choices')
  assert.equal(client.asked.length, 1, 'nothing asked while the choices change')
  await ready(el)
  assert.equal(client.asked.length, 2)
  assert.equal(client.asked[1][1].depth, 'lineage')
  assert.deepEqual(client.asked[1][1].claims, ['projects', 'annotations'])
})

test('a dry run the server superseded (409 superseded) is not shown as an error; the page asks again', async () => {
  let n = 0
  const client = fakeClient({ plan: (r) => { if (++n === 2) throw new CopyError('superseded', 'HTTP 409', 409); return answer(r) } })
  const { el } = await page({ client })
  await ready(el)
  const all = el.querySelector('[data-depth=all]')
  all.checked = true
  await all.fire('change')
  await until(() => client.asked.length === 2, 'the superseded dry run')
  await tick()
  assert.equal(el.querySelector('[data-error]'), null)
  assert.equal(el.dataset.planState, 'loading')
  await ready(el)
  assert.equal(client.asked.length, 3)
  assert.equal(el.querySelector('[data-error]'), null)
  assert.match(el.querySelector('[data-copy-command]').textContent, /--depth all /)
})

// ---- after a failed copy (M6, M7) --------------------------------------------------

test('right after a copy that failed part way, Copy again is offered even with nothing left to transfer; a new choice ends that', async () => {
  let copies = 0
  const client = fakeClient({
    plan: (r) => answer(r, (p) => { if (copies > 0) { p.plan.transfer = 0; p.plan.transfer_bytes = 0 } return p }),
    copy: () => { copies++; throw new CopyError('copy_failed', 'org-s3: the log entry could not be written', 500, null) },
  })
  const { el } = await page({ client })
  await ready(el)
  await el.querySelector('[data-copy-go]').click()
  assert.equal(el.dataset.copyState, 'failed')
  await ready(el)
  const go = el.querySelector('[data-copy-go]')
  assert.equal(go.textContent, 'Copy again')
  assert.equal(go.disabled, false)
  await go.click()
  assert.equal(copies, 2, 'the server is asked to finish it')
  await ready(el)
  const names = el.querySelector('[data-claim-category=names]')
  names.checked = false
  await names.fire('change')
  await ready(el)
  assert.equal(go.textContent, 'Nothing to copy')
  assert.equal(go.disabled, true)
})

test('a copy that lost the server: the destination is forgotten and the page says the copy may still be running', async () => {
  const client = fakeClient({ copy: () => { throw new CopyError('copy_failed', 'could not reach the explore server: Failed to fetch', 0) } })
  const { el, ctx } = await page({ client })
  await ready(el)
  await el.querySelector('[data-copy-go]').click()
  assert.equal(el.dataset.copyState, 'failed')
  assert.equal(el.querySelector('[data-copy-lost]').textContent,
    'The page lost touch with the explore server while copying to org-s3 (could not reach the explore server: Failed to fetch). '
    + 'The copy may still be running on the server: reload this page later to see what org-s3 holds. Copying again skips any blocks already there.')
  assert.equal(el.querySelector('[data-error]'), null)
  assert.deepEqual(ctx.forgot.map(f => f[0]), ['presence', 'members', 'sizes'])
})

test('a 500 with an empty message says the server gave no reason', async () => {
  const client = fakeClient({ copy: () => { throw new CopyError('copy_failed', '', 500, null) } })
  const { el } = await page({ client })
  await ready(el)
  await el.querySelector('[data-copy-go]').click()
  assert.match(el.querySelector('[data-copy-stopped]').textContent, /^The copy to org-s3 stopped: the server gave no reason\. /)
})

// ---- claim tags (M9) ---------------------------------------------------------------

test('project and annotation claims read in the project page\'s words; another attribute reads with its name, a value that is not a string as JSON', async () => {
  const base = { subject: WIGSHOP, asserted_by: 'ana@example.org', timestamp: '2026-10-24T20:00:00.000Z', present: false, current: true, supersedes: [] }
  const P = 'bafyreiatlasprojectxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
  const extra = [
    { ...base, cid: 'bafyreiproject', category: 'projects', verb: 'add', attribute: 'project', value: { '/': P } },
    { ...base, cid: 'bafyreiannotation', category: 'annotations', subject: P, verb: 'add', attribute: 'annotation',
      value: { sample: { pipeline: 'nf-core/rnaseq', id: 'WT_REP1' }, name: 'condition', value: 'treated' } },
    { ...base, cid: 'bafyreiretention', category: 'projects', subject: P, verb: 'set', attribute: 'policy.retention', value: 'pin_selections' },
    { ...base, cid: 'bafyreicondition', category: 'annotations', verb: 'set', attribute: 'condition', value: 'treated' },
    { ...base, cid: 'bafyreidose', category: 'annotations', verb: 'set', attribute: 'dose', value: { mg: 5 } },
  ]
  const client = fakeClient({ plan: (r) => answer(r, (p) => { for (const c of extra) p.plan.claims.push({ ...c, carried: r.claims.includes(c.category) }); return p }) })
  const { el } = await page({ client })
  await ready(el)
  const tag = (cid) => el.querySelector(`[data-carry-claim=${cid}]`).querySelector('.claim')
  assert.equal(tag('bafyreiproject').querySelector('.verb').textContent, 'added to')
  assert.equal(tag('bafyreiproject').querySelector('[data-project-name]').getAttribute('href'), `#/project/${P}`)
  assert.equal(tag('bafyreiannotation').querySelector('.verb').textContent, 'condition')
  assert.equal(tag('bafyreiannotation').querySelector('.val').textContent, 'WT_REP1 = treated')
  assert.equal(tag('bafyreiretention').querySelector('.verb').textContent, 'retention')
  assert.equal(tag('bafyreiretention').querySelector('.val').textContent, 'pins every Selection')
  assert.equal(tag('bafyreicondition').querySelector('.verb').textContent, 'condition =')
  assert.match(tag('bafyreicondition').textContent, /condition =“treated”/)
  assert.match(tag('bafyreidose').textContent, /dose =\{"mg":5\}/)
  assert.doesNotMatch(el.textContent, /object Object|set“/)
})

// ---- the server stopping (D10) and a throwing refresh ---------------------------------

test('a 503 stopping on the dry run says the server is stopping, muted, not the raw error', async () => {
  const stopping = copyClient({ endpoint: 'http://h/api/copy', token: 't',
    fetchFn: async () => json(503, { error: 'stopping', message: 'the server is stopping' }) })
  const { el } = await page({ client: stopping })
  await until(() => el.dataset.planState === 'error', 'the error state')
  const n = el.querySelector('[data-stopping]')
  assert.equal(n.textContent, "The explore server is stopping; open the page again once it's back.")
  assert.ok(n.classList.contains('muted'))
  assert.equal(el.querySelector('[data-error]'), null)
})

test('a forget that throws still clears the running copy: done resolves and a new copy page can copy', async () => {
  const client = fakeClient({ copy: (r) => answer(r, (p) => Object.assign(p, { dry_run: false, copied: 114, copied_bytes: 1279431,
    transfer_entry: 'bafyreitransfer', failures: [], snapshot: 'written' })) })
  installDom(); resetFolds()
  const { explorer } = await openRoblab()
  const ctx = ctxFor({ client })
  ctx.sizes.forget = () => { throw new Error('a listener broke') }
  const saved = console.error
  console.error = () => {}
  try {
    const el = await copyView(explorer, 'selection', WIGSHOP, ctx, {})
    await ready(el)
    await el.querySelector('[data-copy-go]').click()
    assert.equal(el.dataset.copyState, 'copied')
    assert.equal(ctx.copy.running ?? null, null)
    const next = await copyView(explorer, 'selection', WIGSHOP, ctx, {})
    await ready(next)
    assert.equal(next.querySelector('[data-copy-elsewhere]'), null)
    assert.equal(next.querySelector('[data-copy-go]').disabled, false)
  } finally {
    console.error = saved
  }
})

// ---- a project's default store (P4 plan 1.7, ruling Q1) -----------------------------------

test('Copy to… from a project member with a default store: a copy target is preselected; one read-only or not configured here gets one muted line', async () => {
  const P = (alias) => ({ cid: 'bafyproject', name: 'RAP1 depletion', policy: { defaultStore: { value: alias } } })
  assert.equal(copyToButton(ctxFor(), 'selection', WIGSHOP, { to: 'org-s3' }).getAttribute('href'), `#/selection/${WIGSHOP}/copy?to=org-s3`)
  assert.equal(copyToButton(ctxFor(), 'run', DM, { project: 'bafyproject' }).getAttribute('href'), `#/run/${DM}/copy?project=bafyproject`)
  for (const [alias, said] of [['elsewhere', "RAP1 depletion's default store elsewhere is not configured here."],
    ['ana-hpc', "RAP1 depletion's default store ana-hpc is read-only here."]]) {
    const { el } = await page({ query: { project: 'bafyproject' }, project: P(alias) })
    const line = el.querySelector('[data-copy-default-store]')
    await until(() => !line.hasAttribute('hidden'), 'the note')
    assert.equal(line.textContent, said)
    assert.ok(line.classList.contains('muted'))
    assert.equal(line.dataset.copyDefaultStore, alias)
    assert.notEqual(el.querySelector('[data-copy-dest]').textContent.startsWith(alias), true, 'never the destination')
  }
  // With no project named, or a default store that is a copy target, there is nothing to say.
  const plain = await page({})
  assert.equal(plain.el.querySelector('[data-copy-default-store]'), null)
  const target = await page({ query: { to: 'org-s3', project: 'bafyproject' }, project: P('org-s3') })
  await ready(target.el)
  assert.equal(target.el.querySelector('[data-copy-default-store]').hasAttribute('hidden'), true)
})

test('a project whose default store is read-only or not configured here: no destination is preselected and no dry run starts until the reader picks one', async () => {
  const P = (alias) => ({ cid: 'bafyproject', name: 'RAP1 depletion', policy: { defaultStore: { value: alias } } })
  const saved = { location: globalThis.location, history: globalThis.history }
  try {
    for (const alias of ['ana-hpc', 'elsewhere']) {
      globalThis.location = { hash: `#/run/${DM}/copy?project=bafyproject` }
      globalThis.history = { state: null, replaceState: (s, t, href) => { globalThis.location.hash = href } }
      const client = fakeClient()
      const { el } = await page({ client, kind: 'run', cid: DM, query: { project: 'bafyproject' }, project: P(alias) })
      await until(() => !el.querySelector('[data-copy-default-store]').hasAttribute('hidden'), 'the note')
      await new Promise(r => setTimeout(r, REPLAN_DELAY_MS + 100))
      assert.equal(client.asked.length, 0, `${alias}: no dry run before a destination is chosen`)
      const dest = el.querySelector('[data-copy-dest]')
      assert.equal(dest.dataset.copyDest, '')
      assert.match(dest.textContent, /^Choose a store/)
      assert.equal(el.querySelector('[data-copy-chain]').textContent, 'Choose a store to copy to.')
      assert.equal(el.querySelector('[data-copy-go]').disabled, true)
      assert.equal(globalThis.location.hash, `#/run/${DM}/copy?depth=results&claims=names,projects,annotations&project=bafyproject`)
      await dest.click()
      assert.equal(el.querySelectorAll('[data-dest-option]').filter(o => o.getAttribute('aria-selected') === 'true').length, 0)
      await el.querySelector('[data-dest-option="org-s3"]').click()
      await ready(el)
      assert.equal(client.asked.length, 1)
      assert.equal(client.asked[0][1].to, 'org-s3')
      assert.equal(globalThis.location.hash, `#/run/${DM}/copy?to=org-s3&depth=results&claims=names,projects,annotations&project=bafyproject`,
        'the project survives the choice, so a reload still shows the note')
    }
  } finally {
    globalThis.location = saved.location
    globalThis.history = saved.history
  }
})

test('Copy to… encodes the projects it names, several of them comma-separated', () => {
  assert.equal(copyToButton(ctxFor(), 'run', DM, { project: 'a&to=evil#x' }).getAttribute('href'), `#/run/${DM}/copy?project=a%26to%3Devil%23x`)
  assert.equal(copyToButton(ctxFor(), 'run', DM, { project: ['bafyp1', 'bafyp2'] }).getAttribute('href'), `#/run/${DM}/copy?project=bafyp1,bafyp2`)
})

test('projects whose default stores disagree: nothing is preselected, no dry run starts, and one muted line names each store', async () => {
  const projects = [{ cid: 'bafyp1', name: 'RAP1 depletion', policy: { defaultStore: { value: 'org-s3' } } },
    { cid: 'bafyp2', name: 'Atlas', policy: { defaultStore: { value: 'ana-hpc' } } }]
  const saved = { location: globalThis.location, history: globalThis.history }
  try {
    globalThis.location = { hash: `#/run/${DM}/copy?project=bafyp1,bafyp2` }
    globalThis.history = { state: null, replaceState: (s, t, href) => { globalThis.location.hash = href } }
    const client = fakeClient()
    const { el } = await page({ client, kind: 'run', cid: DM, query: { project: 'bafyp1,bafyp2' }, project: projects })
    const line = el.querySelector('[data-copy-default-store]')
    await until(() => !line.hasAttribute('hidden'), 'the note')
    assert.equal(line.textContent, "The projects name different default stores: RAP1 depletion's is org-s3, Atlas's is ana-hpc.")
    assert.equal(line.dataset.copyDefaultStore, 'org-s3,ana-hpc')
    await new Promise(r => setTimeout(r, REPLAN_DELAY_MS + 100))
    assert.equal(client.asked.length, 0, 'no dry run before a destination is chosen')
    assert.equal(el.querySelector('[data-copy-dest]').dataset.copyDest, '')
    assert.equal(globalThis.location.hash, `#/run/${DM}/copy?depth=results&claims=names,projects,annotations&project=bafyp1,bafyp2`)
  } finally {
    globalThis.location = saved.location
    globalThis.history = saved.history
  }
})

test('with no project in the address, the first store that can take a copy is still preselected', async () => {
  const client = fakeClient()
  const { el } = await page({ client })
  await ready(el)
  assert.equal(el.querySelector('[data-copy-dest]').dataset.copyDest, 'org-s3')
  assert.equal(client.asked[0][1].to, 'org-s3')
})

test('a copy into a member that is not indexed says so in its result', async () => {
  const client = fakeClient({ copy: (r) => ({ ...answer(r), dry_run: false, copied: 114, copied_bytes: 1279431, transfer_entry: 'bafyreitransfer', failures: [], snapshot: 'not_indexed' }) })
  const { el } = await page({ client })
  await ready(el)
  await el.querySelector('[data-copy-go]').click()
  const result = el.querySelector('[data-copy-result]')
  assert.match(result.textContent, /not written: org-s3 is not indexed yet; index it from the Storage page/)
})
