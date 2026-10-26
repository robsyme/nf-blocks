// node shoot.mjs <outdir> <explore url> <phase>
// Drives the P4 acceptance through the real page (gate/p4/web-accept.sh serves
// it) and screenshots the build at 1440x900 in light and dark. STATE names a
// directory where each phase leaves what it saw (cids, counts, texts) as JSON
// for the next phase and for the report. With `mock`, the url is the mockups
// folder (file://.../docs/design/mockups/) and only 13 and 14 are shot.
import { readFileSync, writeFileSync, existsSync } from 'node:fs'

const PW = process.env.PLAYWRIGHT ?? new URL('../../../../gate/browser/node_modules/playwright/index.mjs', import.meta.url).pathname
const { chromium } = await import(PW)
const [out, base, phase] = process.argv.slice(2)
const STATE = process.env.STATE
const WIGSHOP = 'bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce'

const load = (name) => existsSync(`${STATE}/${name}.json`) ? JSON.parse(readFileSync(`${STATE}/${name}.json`, 'utf8')) : {}
const save = (name, value) => { writeFileSync(`${STATE}/${name}.json`, JSON.stringify(value, null, 1)); console.log(`${name}:`, JSON.stringify(value)) }

const at = (member, hash) => {
  const u = new URL(base)
  if (member) u.searchParams.set('member', member)
  u.hash = hash
  return u.href
}

// Waits for the page's render counter to pass `after` and its state to leave loading, then a little more
// for the lazy reads (storage bars, samples) the counter doesn't cover.
const ready = async (page, ms = 2500) => {
  if (await page.evaluate(() => !!document.body.dataset.render))
    await page.waitForFunction(() => document.body.dataset.state !== 'loading', null, { timeout: 120000 }).catch(() => {})
  await page.waitForTimeout(ms)
}
const go = async (page, url, ms) => {
  await page.goto(url, { waitUntil: 'load' }).catch(() => {})
  await page.evaluate(() => { document.querySelector('.spec')?.remove(); document.querySelector('.theme-fab')?.remove() })
  await ready(page, ms)
}
// A write through the page: waits for body[data-write-seq] to move and reports its outcome.
const written = async (page, click) => {
  const before = await page.evaluate(() => document.body.dataset.writeSeq ?? '0')
  await click()
  await page.waitForFunction((b) => (document.body.dataset.writeSeq ?? '0') !== b, before, { timeout: 120000 })
  const r = await page.evaluate(() => ({ outcome: document.body.dataset.writeOutcome, written: document.body.dataset.written ?? null }))
  if (r.outcome !== 'written') throw new Error(`write outcome ${r.outcome}: ${await page.evaluate(() => document.querySelector('[data-error]')?.textContent ?? document.body.innerText.slice(0, 400))}`)
  await ready(page, 1500)
  return r
}
const text = (page, sel) => page.evaluate((s) => document.querySelector(s)?.textContent?.replace(/\s+/g, ' ').trim() ?? null, sel)

const PHASES = {}

const RUNS = {
  sharp_magritte: 'bafyreiaedzibppjsllztdb2wqg322harsiohnkmccapz2uemjervj5kcgu',
  sad_yonath: 'bafyreidordqagvbzb63ew3h4xwykbkufpxufq7gyf6l567dbl5bhz7ac3q',
  distraught_murdock: 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq',
}
const CONDITIONS = { RAP1_IAA_30M_REP1: 'auxin_30m', RAP1_UNINDUCED_REP1: 'uninduced', RAP1_UNINDUCED_REP2: 'uninduced',
  WT_REP1: 'wild_type', WT_REP2: 'wild_type' }
const ANA = 'RAP1_IAA_30M_REP1'

// The project's Samples tab, once its rows are drawn.
const samplesTab = async (page, project) => {
  await go(page, at(null, `#/project/${project}?tab=samples`), 500)
  await page.waitForSelector('tr[data-sample-group]', { timeout: 180000 })
  await page.waitForTimeout(1000)
}
const sampleRows = (page) => page.evaluate(() => [...document.querySelectorAll('tr[data-sample-group]')].map(tr => ({
  sample: tr.dataset.sampleGroup, basis: tr.dataset.basis, occurrences: tr.dataset.occurrences.split(' ').length,
  cells: [...tr.cells].map(td => td.textContent.replace(/\s+/g, ' ').trim()),
  condition: tr.querySelector('td[data-annotation=condition]')?.dataset.value ?? null,
  conditionBy: tr.querySelector('td[data-annotation=condition] .claim')?.title ?? null,
})))
const setCondition = async (page, project, sample, value) => {
  await samplesTab(page, project)
  const input = page.locator(`tr[data-sample-group="${sample}"] td[data-annotation=condition] input`)
  await written(page, async () => { await input.fill(value); await input.press('Tab') })
}

// A: New project, Add to project from each run page, the retention policy, four conditions.
PHASES.a = async (page) => {
  const seen = {}
  await go(page, at(null, '#/projects'))
  seen.emptyList = await text(page, '[data-projects-empty]')
  await page.click('#project-new')
  await page.fill('#project-name', 'RAP1 depletion')
  await page.fill('#project-description', 'Yeast RAP1 auxin-depletion time course')
  const created = await written(page, () => page.click('#project-create'))
  seen.project = created.written
  seen.route = await page.evaluate(() => location.hash)
  if (seen.route !== `#/project/${seen.project}`) throw new Error(`after Create the page is at ${seen.route}`)
  seen.adds = {}
  for (const [name, cid] of Object.entries(RUNS)) {
    await go(page, at(null, `#/run/${cid}`))
    await page.click('[data-project-control-open]')
    await page.waitForSelector('select[data-project-choose]')
    await page.selectOption('select[data-project-choose]', seen.project)
    const r = await written(page, () => page.click('[data-project-add-go]'))
    seen.adds[name] = { claim: r.written, tags: await text(page, '[data-project-tags]') }
  }
  await go(page, at(null, `#/project/${seen.project}`))
  await page.click('#project-edit-open')
  await page.click('#project-retention-on')
  await page.waitForSelector('#project-pin-go:not([disabled])', { timeout: 60000 })
  seen.retentionConfirm = await text(page, '[data-retention-confirm]')
  seen.retentionButton = await text(page, '#project-pin-go')
  await written(page, () => page.click('#project-pin-go'))
  seen.subline = await text(page, '.page-head .subline')
  seen.defaultStore = await page.evaluate(() => document.querySelector('[data-default-store]')?.dataset.defaultStore ?? null)
  for (const [sample, value] of Object.entries(CONDITIONS)) if (sample !== ANA) await setCondition(page, seen.project, sample, value)
  await samplesTab(page, seen.project)
  seen.samples = await sampleRows(page)
  seen.basis = await text(page, '[data-basis-note]')
  save('a', seen)
}

// One shot in light and in dark, of the page as it stands (the color scheme is switched in place).
const shoot = async (page, name) => {
  for (const scheme of ['light', 'dark']) {
    await page.emulateMedia({ colorScheme: scheme })
    await page.waitForTimeout(400)
    await page.screenshot({ path: `${out}/${name}-${scheme}.png` })
    console.log(`${out}/${name}-${scheme}.png`)
  }
  await page.emulateMedia({ colorScheme: 'light' })
}

const handoffState = (page) => page.evaluate(() => ({
  ticked: [...document.querySelectorAll('input[data-id]')].filter(i => i.checked).map(i => i.dataset.id),
  rows: [...document.querySelectorAll('input[data-id]')].map(i => i.dataset.id),
  conditions: Object.fromEntries([...document.querySelectorAll('input[data-c]')].map(i => [i.dataset.c, i.value])),
  target: document.getElementById('target')?.value ?? null,
  reference: document.getElementById('ref')?.value ?? null,
  checks: [...document.querySelectorAll('[data-check]')].map(c => ({ key: c.dataset.check, ok: c.dataset.ok, text: c.textContent.replace(/\s+/g, ' ').trim() })),
  samplesheet: document.getElementById('ss')?.textContent ?? null,
  samplesheetCid: document.querySelector('[data-samplesheet-cid] [data-address]')?.dataset.address ?? null,
  contrasts: document.querySelector('[data-contrasts]')?.textContent ?? null,
  contrastsCid: document.querySelector('[data-contrasts-cid] [data-address]')?.dataset.address ?? null,
  command: document.getElementById('cmd')?.textContent ?? null,
  saves: document.querySelector('[data-saves]')?.textContent.replace(/\s+/g, ' ').trim() ?? null,
  save: document.getElementById('save') ? { disabled: document.getElementById('save').disabled, label: document.getElementById('save').textContent } : null,
}))

// Include all 5, with the contrast uninduced against wild_type.
const includeAllOf = async (page) => {
  await page.click('#all')
  await page.waitForTimeout(800)
  if (await page.inputValue('#target') !== 'uninduced') await page.selectOption('#target', 'uninduced')
  if (await page.inputValue('#ref') !== 'wild_type') await page.selectOption('#ref', 'wild_type')
  await page.waitForTimeout(800)
  return handoffState(page)
}
const savedNodeOf = (page) => page.evaluate(() => {
  const n = document.querySelector('[data-handoff-saved]')
  return n ? { address: n.dataset.handoffSaved, text: n.textContent.replace(/\s+/g, ' ').trim(),
    input: document.querySelector('[data-saved-samplesheet]')?.textContent.replace(/\s+/g, ' ').trim() ?? null,
    links: [...n.querySelectorAll('a')].map(a => [a.textContent.trim(), a.getAttribute('href')]) } : null
})

// The hand-off from distraught_murdock's Samples tab at >= 50,000 trimmed reads, with the project active:
// shot while the replicate check fails, then Include all 5, then Save.
PHASES.handoff = async (page) => {
  const { project } = load('a')
  const seen = {}
  const where = JSON.stringify([['preprocessed:num_trimmed_reads', 'int', '50000', '>=']])
  await go(page, at(null, `#/run/${RUNS.distraught_murdock}?tab=samples&where=${encodeURIComponent(where)}`))
  await page.waitForSelector('[data-handoff-template] a', { timeout: 120000 })
  seen.filterRows = await page.evaluate(() => document.querySelectorAll('tr[data-sample]').length)
  seen.drawerHref = await page.getAttribute('[data-handoff-template] a', 'href')
  await page.click('[data-handoff-template] a')
  await ready(page, 1000)
  seen.drawerLanded = await page.evaluate(() => location.hash)
  // The drawer's link carries no project; the project is made active by adding project= to it.
  await go(page, at(null, `${seen.drawerLanded}&project=${project}`))
  await page.waitForSelector('[data-check=matrix][data-ok=true]', { timeout: 120000 }).catch(() => {})
  await page.waitForTimeout(1500)
  seen.failing = await handoffState(page)
  seen.subline = await text(page, '.page-head .subline')
  await shoot(page, 'build-14-handoff-de')
  const includeAll = () => includeAllOf(page)
  const savedNode = () => savedNodeOf(page)
  const status = async () => ({ partial: await page.evaluate(() => document.querySelector('[data-handoff-partial]')?.dataset.handoffPartial ?? null),
    text: await text(page, '[data-handoff-status]') })
  seen.all = await includeAll()
  seen.firstSave = { outcome: await written(page, () => page.click('#save')) }
  seen.firstSave.saved = await savedNode()
  seen.firstSave.status = await status()
  if (!seen.firstSave.saved) {
    // The project pins every Selection, and the server refuses a pin whose subject is a Selection
    // (Put PINNABLE): recorded as a bug. The shot shows the page's part-way note.
    await page.locator('[data-handoff-partial]').scrollIntoViewIfNeeded().catch(() => {})
    await shoot(page, 'build-14-handoff-de-pin-refused')
    // Go on with the policy off, so the rest of the scenario can run: Turn off on the project's Edit panel,
    // then the same hand-off again from a fresh page (Save reuses what exists and writes the rest).
    await go(page, at(null, `#/project/${project}`))
    await page.click('#project-edit-open')
    await written(page, () => page.click('#project-retention-off'))
    seen.retentionOff = await text(page, '.page-head .subline')
    await go(page, at(null, `${seen.drawerLanded}&project=${project}`))
    await page.waitForSelector('[data-check=matrix][data-ok=true]', { timeout: 120000 }).catch(() => {})
    await page.waitForTimeout(1500)
    seen.all2 = await includeAll()
    seen.secondSave = { outcome: await written(page, () => page.click('#save')) }
    seen.secondSave.status = await status()
  }
  seen.saved = await savedNode()
  if (!seen.saved) throw new Error(`no saved hand-off: ${JSON.stringify(await status())}`)
  seen.after = await handoffState(page)
  await shoot(page, 'build-14-handoff-de-saved')
  save('handoff-page', seen)
  save('handoff', { handoff_address: seen.saved.address, samplesheet_cid: seen.all.samplesheetCid, samplesheet_text: seen.all.samplesheet,
    contrasts_cid: seen.all.contrastsCid, contrasts_text: seen.all.contrasts.replace(/^contrasts\.csv\n/, '') })
}

// C, before the copy: the wigshop Selection added to the project from its own page.
PHASES['c-add'] = async (page) => {
  const { project } = load('a')
  const seen = {}
  await go(page, at(null, `#/selection/${WIGSHOP}`))
  await page.click('[data-project-control-open]')
  await page.waitForSelector('select[data-project-choose]')
  await page.selectOption('select[data-project-choose]', project)
  await page.waitForTimeout(1500)
  // With the policy on, the button names the pin's bytes and the panel says who already pins it here, before the click.
  await page.waitForFunction(() => /pin|Add/.test(document.querySelector('[data-project-add-go]')?.textContent ?? '') &&
    !document.querySelector('[data-project-add-go]')?.disabled, null, { timeout: 60000 }).catch(() => {})
  await page.waitForTimeout(1000)
  seen.addLabel = await text(page, '[data-project-add-go]')
  seen.policyLine = await text(page, '[data-add-policy]')
  seen.pinNote = await page.evaluate(() => { const n = document.querySelector('[data-add-pin-note]'); return n && !n.hidden ? n.textContent : null })
  await page.locator('[data-project-control]').scrollIntoViewIfNeeded().catch(() => {})
  await shoot(page, 'build-selection-add-and-pin')
  await written(page, () => page.click('[data-project-add-go]'))
  seen.tags = await text(page, '[data-project-tags]')
  save('c-add', seen)
}

// Before the hand-off and Add to project: ana.k@example.org pins wigshop with the Selection page's own Pin… (P2),
// through the real server. Served with ana.config.
PHASES.pin = async (page) => {
  const seen = {}
  await go(page, at(null, `#/selection/${WIGSHOP}`))
  seen.before = { durability: await text(page, '.page-head .state'), badge: await text(page, '[data-badge=pinned]') }
  await page.click('#pin-open')
  await page.fill('#pin-note', 'paper figure 2')
  await page.waitForSelector('#pin:not([disabled])', { timeout: 120000 })
  seen.pinLabel = await text(page, '#pin')
  seen.pinnedBy = await page.evaluate(() => { const n = document.querySelector('[data-pinned-by]'); return n && !n.hidden ? n.textContent : null })
  seen.write = await written(page, () => page.click('#pin'))
  seen.pins = await page.evaluate(() => [...document.querySelectorAll('[data-pin][data-claim]')].map(n => ({ claim: n.dataset.claim, text: n.textContent.replace(/\s+/g, ' ').trim() })))
  seen.after = { durability: await text(page, '.page-head .state'), badge: await text(page, '[data-badge=pinned]') }
  save('pin', seen)
}

// C, after the copy, over roblab and org-local: default store org-local from the project's Edit panel.
PHASES['c-policy'] = async (page) => {
  const { project } = load('a')
  const seen = {}
  await go(page, at(null, `#/project/${project}`))
  await page.click('#project-edit-open')
  seen.options = await page.evaluate(() => [...document.querySelectorAll('#project-default-store option')].map(o => o.value))
  await page.selectOption('#project-default-store', 'org-local')
  await written(page, () => page.click('#project-default-store-save'))
  seen.subline = await text(page, '.page-head .subline')
  seen.defaultStore = await page.evaluate(() => document.querySelector('[data-default-store]')?.dataset.defaultStore ?? null)
  save('c-policy', seen)
}

// C, over org-local alone (org.config, so the page writes there): the copied wigshop gets the project's policy pin in
// org-local. Pins are per store. The page offers the project's "Pin every Selection…" when the policy Claim did not
// travel with the copy; otherwise D5's "N Selections not pinned by policy · Pin" on the project's Selections tab.
PHASES['c-org-pin'] = async (page) => {
  const { project } = load('a')
  const seen = {}
  await go(page, at(null, `#/project/${project}`), 4000)
  seen.subline = await text(page, '.page-head .subline')
  await page.click('#project-edit-open')
  seen.policy = await page.evaluate(() => document.querySelector('[data-retention-policy]')?.dataset.retentionPolicy ?? null)
  if (await page.$('#project-retention-on')) {
    seen.how = 'project Edit…, Pin every Selection…'
    await page.click('#project-retention-on')
    await page.waitForSelector('#project-pin-go:not([disabled])', { timeout: 120000 })
    seen.confirm = await text(page, '[data-retention-confirm]')
    seen.button = await text(page, '#project-pin-go')
    await page.locator('[data-retention-confirm]').scrollIntoViewIfNeeded().catch(() => {})
    await shoot(page, 'build-13-project-pin-org-local')
    seen.write = await written(page, () => page.click('#project-pin-go'))
  } else {
    seen.how = "project Selections tab, D5's Pin"
    await go(page, at(null, `#/project/${project}?tab=selections`), 4000)
    seen.notPinned = await text(page, '[data-not-pinned]')
    if (!seen.notPinned) throw new Error(`no Selection here is left to pin by policy: ${await text(page, '.page-head .subline')}`)
    await page.click('[data-pin-unpinned]')
    await page.waitForSelector('#project-pin-go:not([disabled])', { timeout: 120000 })
    seen.confirm = await text(page, '[data-retention-confirm]')
    seen.button = await text(page, '#project-pin-go')
    await page.locator('[data-retention-confirm]').scrollIntoViewIfNeeded().catch(() => {})
    await shoot(page, 'build-13-project-pin-org-local')
    seen.write = await written(page, () => page.click('#project-pin-go'))
  }
  seen.after = await text(page, '.page-head .subline')
  save('c-org-pin', seen)
}

// B: the sandbox's Projects list suggests the runs that share read inputs; Create project from it.
PHASES.b = async (page) => {
  const seen = {}
  await go(page, at(null, '#/projects'))
  await page.waitForSelector('[data-suggestion]', { timeout: 120000 })
  seen.suggestions = await page.evaluate(() => [...document.querySelectorAll('[data-suggestion]')].map(n => ({
    runs: n.dataset.suggestion.split(' '), text: n.textContent.replace(/\s+/g, ' ').trim() })))
  await shoot(page, 'build-projects-list-suggestion')
  await page.click('[data-suggestion] [data-suggest-create]')
  await page.waitForTimeout(500)
  seen.prefill = { name: await page.inputValue('#project-name'), description: await page.inputValue('#project-description'),
    adds: await text(page, '[data-project-adds]') }
  const r = await written(page, () => page.click('#project-create'))
  seen.project = r.written
  seen.route = await page.evaluate(() => location.hash)
  seen.partial = await text(page, '[data-project-partial]')
  await go(page, at(null, `#/project/${seen.project}?tab=runs`))
  seen.runsTab = await page.evaluate(() => [...document.querySelectorAll('[data-member]')].map(n => n.dataset.member))
  await samplesTab(page, seen.project)
  seen.samples = await sampleRows(page)
  seen.basisMeta = await text(page, '[data-basis-meta]')
  seen.basis = await text(page, '[data-basis-note]')
  seen.inputReadsHeader = await page.evaluate(() => !!document.querySelector('[data-input-reads]'))
  save('b', seen)
}

// The shots, over two members (roblab and org-local): Overview with two bars, Samples, Activity.
PHASES['shots-two'] = async (page) => {
  const { project } = load('a')
  const seen = {}
  await go(page, at(null, `#/project/${project}`), 5000)
  seen.bars = await page.evaluate(() => [...document.querySelectorAll('[data-store-bar]')].map(n => ({ store: n.dataset.storeBar,
    bytes: n.dataset.bytes ?? null, text: n.textContent.replace(/\s+/g, ' ').trim() })))
  seen.caption = await text(page, '[data-storage-caption]')
  seen.head = await text(page, '.page-head')
  seen.tabs = await text(page, 'nav.tabs')
  await shoot(page, 'build-13-project-overview-two-stores')
  await samplesTab(page, project)
  await page.waitForTimeout(3000)
  seen.samples = await sampleRows(page)
  await shoot(page, 'build-13-project-samples-two-stores')
  await go(page, at(null, `#/project/${project}?tab=activity`), 4000)
  seen.activity = await page.evaluate(() => [...document.querySelectorAll('[data-activity]')].map(n => n.textContent.replace(/\s+/g, ' ').trim()))
  await shoot(page, 'build-13-project-activity')
  await go(page, at(null, `#/project/${project}?tab=selections`), 3000)
  seen.selections = await text(page, 'main')
  // Copy to… from a project member (the hand-off's Selection, not copied yet): it points at the project's default
  // store, org-local, once the Selection's projects are read. The copy page opens with it chosen; nothing is copied.
  const handed = load('handoff-page')
  const selection = (handed.saved ?? handed.firstSave?.saved)?.links?.find(([t]) => t === 'Open the Selection')?.[1]?.split('/').pop()
  if (selection) {
    await go(page, at(null, `#/selection/${selection}`), 3000)
    await page.waitForFunction(() => /to=/.test(document.querySelector('a[data-copy-to]')?.getAttribute('href') ?? ''), null, { timeout: 60000 }).catch(() => {})
    seen.copyHref = await page.getAttribute('a[data-copy-to]', 'href')
    await page.click('a[data-copy-to]')
    await ready(page, 4000)
    seen.copyRoute = await page.evaluate(() => location.hash)
    seen.copyDest = await page.evaluate(() => document.querySelector('[data-copy-dest]')?.dataset.copyDest ?? null)
    seen.copyDefaultNote = await page.evaluate(() => { const n = document.querySelector('[data-copy-default-store]'); return n && !n.hidden ? n.textContent : null })
    await shoot(page, 'build-copy-default-store')
  }
  save('shots-two', seen)
}

// The shots over roblab alone: the Samples tab (scenario A's state), and a run page head with its project tag.
PHASES['shots-roblab'] = async (page) => {
  const { project } = load('a')
  const seen = {}
  await samplesTab(page, project)
  await page.waitForTimeout(3000)
  seen.samples = await sampleRows(page)
  seen.aside = await page.evaluate(() => [...document.querySelectorAll('[data-store-bar]')].map(n => n.textContent.replace(/\s+/g, ' ').trim()))
  await shoot(page, 'build-13-project-samples')
  await go(page, at(null, `#/run/${RUNS.distraught_murdock}`), 3000)
  seen.runTags = await text(page, '[data-project-tags]')
  await page.click('[data-project-control-open]')
  await page.waitForTimeout(1500)
  seen.runPanel = await text(page, '[data-project-control]')
  await shoot(page, 'build-run-add-to-project')
  // The Selection page after the policy pin (and ana's own): durability Pinned, and every pin with who and when.
  await go(page, at(null, `#/selection/${WIGSHOP}`), 4000)
  seen.selection = { durability: await text(page, '.page-head .state'), badge: await text(page, '[data-badge=pinned]'),
    pins: await page.evaluate(() => [...document.querySelectorAll('[data-pin][data-claim]')].map(n => n.textContent.replace(/\s+/g, ' ').trim())) }
  await shoot(page, 'build-selection-pinned')
  await go(page, at(null, '#/projects'), 4000)
  seen.list = await page.evaluate(() => [...document.querySelectorAll('tr[data-project]')].map(n => n.textContent.replace(/\s+/g, ' ').trim()))
  await shoot(page, 'build-projects-list')
  save('shots-roblab', seen)
}

// The sandbox project's Samples tab after its snapshot: grouped by input reads, with the Input reads seals.
PHASES['shots-sandbox'] = async (page) => {
  const { project } = load('b')
  await samplesTab(page, project)
  await page.waitForTimeout(3000)
  const seen = { samples: await sampleRows(page), basisMeta: await text(page, '[data-basis-meta]'), basis: await text(page, '[data-basis-note]') }
  await shoot(page, 'build-project-samples-input-reads')
  save('shots-sandbox', seen)
}

// SHOOT_ONLY (web-accept.sh): the hand-off shots again, from the URL the handoff phase landed on, over a
// throwaway copy of the roblab copy (Save writes a Handoff, which the other shots must not see).
PHASES['reshoot-handoff'] = async (page) => {
  const { project } = load('a')
  const { drawerLanded } = load('handoff-page')
  const seen = {}
  await go(page, at(null, `${drawerLanded}&project=${project}`))
  await page.waitForSelector('[data-check=matrix][data-ok=true]', { timeout: 120000 }).catch(() => {})
  await page.waitForTimeout(1500)
  seen.failing = await handoffState(page)
  await shoot(page, 'build-14-handoff-de')
  seen.all = await includeAllOf(page)
  seen.save = await written(page, () => page.click('#save'))
  seen.saved = await savedNodeOf(page)
  if (!seen.saved) throw new Error(`no saved hand-off: ${await text(page, '[data-handoff-status]')}`)
  seen.after = await handoffState(page)
  await shoot(page, 'build-14-handoff-de-saved')
  save('reshoot-handoff', seen)
}

// SHOOT_ONLY: the Projects list's suggestion, over a fresh copy of the sandbox store (scenario B's Create took its runs).
PHASES['reshoot-suggestion'] = async (page) => {
  await go(page, at(null, '#/projects'))
  await page.waitForSelector('[data-suggestion]', { timeout: 120000 })
  const suggestions = await page.evaluate(() => [...document.querySelectorAll('[data-suggestion]')].map(n => n.textContent.replace(/\s+/g, ' ').trim()))
  await shoot(page, 'build-projects-list-suggestion')
  save('reshoot-suggestion', { suggestions })
}

// The Gate's A2 way of counting (gate/browser/drive.mjs `idle` steps), over the roblab copy with the project:
// a cold context opens #/idle, waits until the page is ready, then moves to the route; every request after
// the move is the route's cost. Snapshot reads are the index/ requests.
PHASES.a2 = async (page, ctx) => {
  const { project } = load('a')
  const browser = ctx.browser()
  const routes = [
    ['run, in no project (sleepy_heyrovsky)', '#/run/bafyreiackkkvjk3njmwi4chqgmwse4q6tgburwz3xzxy6hd4in3rykhs54'],
    ['run, in the project (distraught_murdock)', `#/run/${RUNS.distraught_murdock}`],
    ['run, in the project (sad_yonath)', `#/run/${RUNS.sad_yonath}`],
    ['Selection, in the project (wigshop)', `#/selection/${WIGSHOP}`],
    ['Projects list', '#/projects'],
    ['project Overview', `#/project/${project}`],
    ['project Runs', `#/project/${project}?tab=runs`],
    ['project Activity', `#/project/${project}?tab=activity`],
    ['project Samples', `#/project/${project}?tab=samples`],
  ]
  const base = !!process.env.A2_BASE
  const results = []
  for (const [name, hash] of base ? routes.slice(0, 5) : routes) {
    const c = await browser.newContext()
    const p = await c.newPage()
    const reqs = []
    let phase = 'open'
    c.on('requestfinished', async (r) => {
      const entry = { url: r.url().replace(/token=[a-z2-7]+/, 'token=…'), phase, bytes: 0 }
      reqs.push(entry)
      const resp = await r.response().catch(() => null)
      if (resp) entry.bytes = Number((await resp.allHeaders())['content-length'] || 0)
    })
    await p.goto(at(null, '#/idle'))
    await p.waitForFunction(() => Number(document.body?.dataset.render ?? 0) > 0 && document.body.dataset.state !== 'loading', null, { timeout: 120000 })
    await p.waitForTimeout(1500)
    phase = 'query'
    const before = Number(await p.evaluate(() => document.body.dataset.render))
    await p.evaluate((h) => { location.hash = h }, hash)
    await p.waitForFunction((n) => Number(document.body.dataset.render) > n && document.body.dataset.state !== 'loading', before, { timeout: 180000 })
    await p.waitForTimeout(hash.includes('samples') ? 8000 : 4000)
    const q = reqs.filter(r => r.phase === 'query')
    const index = q.filter(r => new URL(r.url).pathname.includes('/index/'))
    const blocks = q.filter(r => new URL(r.url).pathname.includes('/blocks/'))
    results.push({ name, hash, state: await p.evaluate(() => document.body.dataset.state),
      index: { requests: index.length, bytes: index.reduce((a, r) => a + r.bytes, 0) },
      blocks: { requests: blocks.length, bytes: blocks.reduce((a, r) => a + r.bytes, 0) },
      other: q.filter(r => !index.includes(r) && !blocks.includes(r)).map(r => new URL(r.url).pathname) })
    console.log(JSON.stringify(results.at(-1)))
    await c.close()
  }
  save(base ? 'a2-base' : 'a2', results)
}

// The mockups, from their files, with the spec panel and theme button removed.
PHASES.mock = async (page) => {
  for (const [name, file] of [['mock-13-project', '13-project.html'], ['mock-14-handoff-de', '14-handoff-de.html']]) {
    await go(page, new URL(file, base).href, 1500)
    await shoot(page, name)
  }
}

// The fifth condition as ana.k@example.org (this server's cas.asserted_by).
PHASES.ana = async (page) => {
  const { project } = load('a')
  await setCondition(page, project, ANA, CONDITIONS[ANA])
  await samplesTab(page, project)
  save('ana', { samples: await sampleRows(page) })
}

// Free-form exploration: PROBE names a module exporting default async (page, ctx).
PHASES.probe = async (page) => {
  const mod = await import(process.env.PROBE)
  await mod.default(page, { at, go, ready, written, text, load, save, base, out })
}

// Every POST the page makes in a phase, with its answer: STATE/posts-<phase>.json.
const posts = []
const browser = await chromium.launch()
try {
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: 'light' })
  const page = await ctx.newPage()
  page.on('pageerror', (e) => console.log('[pageerror]', String(e)))
  page.on('console', (m) => { if (m.type() === 'error') console.log('[console]', m.text()) })
  page.on('response', async (r) => {
    if (r.request().method() === 'POST') posts.push({ path: new URL(r.url()).pathname, status: r.status(),
      request: r.request().postData()?.slice(0, 2000) ?? null, response: (await r.text().catch(() => '')).slice(0, 2000) })
    if (r.status() >= 400) console.log('[http]', r.status(), r.request().method(), r.url().replace(/token=[a-z2-7]+/, 'token=…'),
      r.request().method() === 'POST' ? `request ${r.request().postData()?.slice(0, 600)} response ${(await r.text().catch(() => '')).slice(0, 600)}` : '')
  })
  try {
    await PHASES[phase](page, ctx)
  } finally {
    if (STATE && posts.length) writeFileSync(`${STATE}/posts-${phase}.json`, JSON.stringify(posts, null, 1))
  }
} finally {
  await browser.close()
}
