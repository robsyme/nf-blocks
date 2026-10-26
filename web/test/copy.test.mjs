// The copy endpoint's client (P3 interface 1.7) and the page's choices for it.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { CopyError, copyAvailability, copyClient, copyHref, parseCopyQuery } from '../src/copy.js'

const PLAN = JSON.parse(readFileSync(new URL('./fixtures/copy-plan-wigshop.json', import.meta.url), 'utf8'))
const WIGSHOP = PLAN.root.cid
const REQUEST = { root: `selection:${WIGSHOP}`, from: 'roblab', to: 'org-s3', depth: 'results', claims: ['names', 'projects', 'annotations'], accept_audit: false }

const json = (status, value) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })

function fakeFetch(respond) {
  const calls = []
  const fn = async (url, init) => {
    calls.push({ url, init, body: JSON.parse(init.body) })
    return respond(url, init)
  }
  fn.calls = calls
  return fn
}

test('plan posts the JSON request with the token to api/copy?dry_run=true; copy posts it without', async () => {
  const fetchFn = fakeFetch(() => json(200, PLAN))
  const client = copyClient({ endpoint: 'http://h/api/copy', token: 't0k', fetchFn })
  const plan = await client.plan(REQUEST)
  assert.deepEqual(plan, PLAN)
  await client.copy({ ...REQUEST, accept_audit: true })
  const [dry, real] = fetchFn.calls
  assert.equal(dry.url, 'http://h/api/copy?dry_run=true')
  assert.equal(dry.init.method, 'POST')
  assert.equal(dry.init.headers['Content-Type'], 'application/json')
  assert.equal(dry.init.headers['X-NF-Blocks-Token'], 't0k')
  assert.deepEqual(dry.body, REQUEST)
  assert.equal(real.url, 'http://h/api/copy')
  assert.equal(real.body.accept_audit, true)
})

test('400, 403, 409 and 500 become coded CopyErrors', async () => {
  const cases = [
    [json(400, { error: 'bad_depth', message: 'depth must be results, lineage or all' }), 'bad_depth', 'depth must be results, lineage or all'],
    [json(400, { message: 'no error code' }), 'bad_request', 'no error code'],
    [new Response('bad token', { status: 403, headers: { 'Content-Type': 'text/plain' } }), 'forbidden', 'bad token'],
    [json(409, { error: 'copy_running', message: 'a copy is running' }), 'copy_running', 'a copy is running'],
    [new Response('', { status: 409 }), 'conflict', 'HTTP 409'],
    [new Response('boom', { status: 500 }), 'copy_failed', 'boom'],
    // What CopyService throws reaches the page as JSON with no 1.3 body.
    [json(500, { error: 'copy_failed', message: 'org-s3: Access Denied' }), 'copy_failed', 'org-s3: Access Denied'],
    [json(400, { error: 'not_a_target', message: 'org-s3 is read-only' }), 'not_a_target', 'org-s3 is read-only'],
  ]
  for (const [res, code, message] of cases) {
    const client = copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async () => res })
    await assert.rejects(client.copy(REQUEST), (e) => {
      assert.ok(e instanceof CopyError)
      assert.equal(e.code, code)
      assert.equal(e.message, message)
      assert.equal(e.body, null)
      return true
    })
  }
})

// The real partial-copy 500 (CopyService): the 1.3 body with copied, failures, a null transfer entry and snapshot, and no error or message.
const partial = (failures) => ({ ...PLAN, dry_run: false, copied: 40, copied_bytes: 300000, transfer_entry: null, failures, snapshot: null })

test('a 500 that carries the 1.3 body keeps it, and its message counts the failed blocks', async () => {
  const one = copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async () => json(500, partial([{ cid: WIGSHOP, error: 'hash mismatch' }])) })
  await assert.rejects(one.copy(REQUEST), (e) => {
    assert.equal(e.code, 'copy_failed')
    assert.equal(e.status, 500)
    assert.equal(e.message, '1 block failed')
    assert.equal(e.body.failures[0].error, 'hash mismatch')
    return true
  })
  const two = copyClient({ endpoint: 'http://h/api/copy', token: 't',
    fetchFn: async () => json(500, partial([{ cid: WIGSHOP, error: 'a' }, { cid: 'bafkreiother', error: 'b' }])) })
  await assert.rejects(two.copy(REQUEST), (e) => e.message === '2 blocks failed')
})

test('a JSON error with no message is not shown as its JSON text', async () => {
  const client = copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async () => json(500, { error: 'copy_failed' }) })
  await assert.rejects(client.copy(REQUEST), (e) => e.code === 'copy_failed' && e.message === 'HTTP 500')
})

test('a 409 with a plan body (audit findings, no accept_audit) answers with the plan and says it was refused', async () => {
  const findings = [{ cid: PLAN.plan.runs[0].manifest, kind: 'RunManifest', where: 'params.outdir', what: 'absolute path' }]
  const body = { ...PLAN, dry_run: false, plan: { ...PLAN.plan, audit: { checked: 1, findings } } }
  const client = copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async () => json(409, body) })
  const answer = await client.copy(REQUEST)
  assert.equal(answer.refused, 'audit')
  assert.deepEqual(answer.plan.audit.findings, findings)
  // The amended contract names the reason in the body.
  const said = copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async () => json(409, { ...body, refused: 'audit' }) })
  assert.equal((await said.copy(REQUEST)).refused, 'audit')
})

test('a 409 refused not_kept (D9) answers with the plan and says so', async () => {
  const plan = { ...PLAN.plan, kept: false,
    destination_state: [{ cid: PLAN.plan.runs[0].completion, role: 'run', name: 'distraught_murdock', state: 'released' }] }
  const client = copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async () => json(409, { ...PLAN, dry_run: false, plan, refused: 'not_kept' }) })
  const answer = await client.copy(REQUEST)
  assert.equal(answer.refused, 'not_kept')
  assert.equal(answer.plan.kept, false)
})

test('a dry run superseded by a newer one (D10) is a superseded CopyError', async () => {
  const client = copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async () => json(409, { error: 'superseded' }) })
  await assert.rejects(client.plan(REQUEST), (e) => e instanceof CopyError && e.code === 'superseded' && e.status === 409)
})

test('a server that cannot be reached is a copy_failed CopyError', async () => {
  const client = copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async () => { throw new TypeError('Failed to fetch') } })
  await assert.rejects(client.plan(REQUEST), (e) => e.code === 'copy_failed' && /could not reach the explore server: Failed to fetch/.test(e.message))
})

const member = (alias, extra = {}) => ({ alias, writable: false, base: `m/${alias}/`, kind: 'local', location: null, copy_target: true, copy_reason: null, ...extra })

test('copyAvailability: copy needs explore, the token, a server that copies, and another store that can be written', () => {
  const store = { members: [member('roblab', { writable: true }), member('org-s3', { kind: 's3', location: 's3://lab-blocks' })], member: 'roblab', copy: true }
  assert.deepEqual(copyAvailability(store, { token: 't' }), { available: true, reason: null, targets: ['org-s3'] })
  assert.match(copyAvailability({ members: null, member: null }, { token: 't' }).reason, /opened through `nextflow plugin nf-blocks:explore`/)
  assert.match(copyAvailability(store, { token: null }).reason, /URL nf-blocks:explore printed; it carries the token/)
  assert.equal(copyAvailability({ ...store, copy: false }, { token: 't' }).reason, "This explore server can't run copies.")
  assert.equal(copyAvailability({ ...store, members: [store.members[0]] }, { token: 't' }).reason, 'There is no other store in this configuration to copy to.')
  const readOnly = { ...store, members: [store.members[0], member('org-s3', { copy_target: false, copy_reason: 'readOnly is set for this store' })] }
  assert.deepEqual(copyAvailability(readOnly, { token: 't' }),
    { available: false, reason: 'No other store can take a copy: org-s3 (readOnly is set for this store).', targets: [] })
  // The open member is never its own destination, even when it could be written.
  assert.deepEqual(copyAvailability({ ...store, member: 'org-s3' }, { token: 't' }).targets, ['roblab'])
})

test('the copy page query: defaults, none, unknown values dropped; copyHref writes it back', () => {
  assert.deepEqual(parseCopyQuery(''), { to: null, depth: 'results', claims: ['names', 'projects', 'annotations'], project: null })
  assert.deepEqual(parseCopyQuery('to=org-s3&depth=lineage&claims=pins,names,bogus'), { to: 'org-s3', depth: 'lineage', claims: ['names', 'pins'], project: null })
  assert.deepEqual(parseCopyQuery('depth=deep&claims=none&project=bafyproject'), { to: null, depth: 'results', claims: [], project: 'bafyproject' })
  assert.equal(copyHref('selection', WIGSHOP, { to: 'org-s3', depth: 'results', claims: ['annotations', 'names', 'projects'] }),
    `#/selection/${WIGSHOP}/copy?to=org-s3&depth=results&claims=names,projects,annotations`)
  assert.equal(copyHref('run', 'bafyrun', { to: null, depth: 'all', claims: [] }), '#/run/bafyrun/copy?depth=all&claims=none')
  // P4: the project a Copy to… came from stays in the address, so a reload still explains its default store.
  const withProject = copyHref('run', 'bafyrun', { to: 'org-s3', depth: 'results', claims: ['names'], project: 'bafyproject' })
  assert.equal(withProject, '#/run/bafyrun/copy?to=org-s3&depth=results&claims=names&project=bafyproject')
  assert.equal(parseCopyQuery(withProject.split('?')[1]).project, 'bafyproject')
})

test("the copy page query keeps a '+' as '+', as the run route does", () => {
  assert.deepEqual(parseCopyQuery('to=lab+s3&project=bafy+proj&claims=names'), { to: 'lab+s3', depth: 'results', claims: ['names'], project: 'bafy+proj' })
  assert.equal(parseCopyQuery('to=lab%2Bs3').to, 'lab+s3')
})

test('a 503 stopping is a stopping CopyError', async () => {
  const client = copyClient({ endpoint: 'http://h/api/copy', token: 't', fetchFn: async () => json(503, { error: 'stopping', message: 'the server is stopping' }) })
  await assert.rejects(client.plan(REQUEST), (e) => e instanceof CopyError && e.code === 'stopping' && e.status === 503)
})
