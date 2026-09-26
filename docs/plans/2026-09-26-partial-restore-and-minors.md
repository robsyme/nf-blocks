# Partial Restore and Milestone 2 Minors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a restore that fails partway recoverable from the page. The same branch clears the twelve deferred minors from milestone 2's reviews.

**Architecture:** The compose save sequence moves into a pure module (`web/src/save-flow.js`) that never throws after the copy saved. It returns the failures, and `app.js` turns them into a banner on the saved Selection's page, carried across navigation like the write outcome. The "already exists" path gains a Restore button. The Groovy and page minors are small, independent fixes, each with a unit test.

**Tech Stack:** Groovy 4 / Spock (plugin), plain ES modules + `node --test` (`web/`), Python 3 stdlib (Gate), Playwright via `gate/browser/drive.mjs`.

**Spec:** `../.scratch/block-explorer/issues/11-partial-restore-and-m2-minors.md` (Answer, Q1-Q8), with DESIGN.md §15 and §16 decisions 21-23.

## Global Constraints

- DESIGN.md is the binding contract; when it and the spec disagree, DESIGN.md wins and gets fixed in the same task.
- Groovy main classes are `@CompileStatic`.
- The page never builds a block: the server encodes (no JS DAG-CBOR encoder).
- The Selection view keeps reading only the member's own Claims (decision 22); Claim actions stay offered only in the writable member (decision 14).
- Act on `deletion`, never on whether `deletion_claims` is empty (a current `del` stays in it while `deletion` is `none`).
- Save order: the Selection; then the name Claim if a name is set; then the `del` if restoring. The `del` is tried even when naming failed.
- Wording: "deleted in this composition" / "its deletion is in conflict in this composition" in both the here and elsewhere messages.
- After a save whose naming or restoring failed, the write outcome is `written`; the banner on the saved Selection's page carries `data-write-failed` (`naming`, `restoring`, or both space-separated) and each error's code in a `[data-error]`.
- DAG-JSON float literals longer than 64 characters are refused.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Work on branch `feat/partial-restore-and-minors` from `main`.

## Review Focus

1. Naming and the `del` both fail: the banner names both, and "Retry restore" still appears (Task 2 `saveSequence` test).
2. The copy was saved from a read-only member's view, so the page navigates to the writable member: the banner must survive the navigation (Task 2, `carryOutcome` carries it; test the serialise/restore helpers).
3. "Retry restore" fails again: the banner stays with the new error and the button stays usable (Task 2 test).
4. Latest run where the newest run is deleted in the snapshot but undone in the tail: it must be found (Task 3 test).
5. Every tail Claim value that isn't a string (map, list, number, null) renders the same as the snapshot's text form (Task 3 test against `Index.valueText`'s convention).

---

### Task 1: Groovy minors

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Put.groovy` (`entryFor`, around 338-341)
- Modify: `src/main/groovy/robsyme/cas/cli/CasCommands.groovy:91,108` (`readCapped`)
- Modify: `src/main/groovy/robsyme/cas/core/DagJson.groovy` (floats, around 170-173; integer cap at 31, 177-178)
- Test: `src/test/groovy/robsyme/cas/core/PutTest.groovy`, the DagJson test (`src/test/groovy/robsyme/cas/core/DagJsonTest.groovy` or wherever DagJson is tested)
- Modify: `DESIGN.md` (samplesheet route rows around 941 and 1166; §16 new decision 24)

**Interfaces:**
- Produces: `DagJson.MAX_FLOAT_CHARS = 64`; the idempotent path in `Put` ingests any Store Log entry it appends.

- [ ] **Step 1: Failing tests**

PutTest:

```groovy
    def 'the idempotent path ingests a Store Log entry it has to append (ticket 11)'() {
        given: 'a Selection block the writable member holds, with its Store Log entry gone and a fresh index'
        final String json = selection(item(itemA, [collA]))
        final Cid s = send(json).address
        Files.list(tempDir.resolve('store/log')).withCloseable { it.toList() }.each { Files.delete(it) }
        index.close()
        index = Index.open(tempDir.resolve('cache/fresh.sqlite'))
        put = new Put(store, store, index, 'ada', { now }, { })   // no catch-up: only Put's own ingest can index the entry

        when:
        final PutResult again = send(json)

        then:
        !again.written
        StoreLog.read(store).any { it.cid == s }
        index.firstLogEntry(s, 'lab') != null
    }
```

This test must FAIL before the fix. If `Put`'s own `ingest()` does not index without the catch-up closure (read `Put.ingest`), use whatever lets the test tell "appended and ingested" apart from "appended only". The assertion that matters is that the index knows the entry after the retry, with no separate catch-up.

DagJson test:

```groovy
    def 'a float literal over 64 characters is refused (ticket 11)'() {
        when:
        DagJson.decode(('{"x":1.' + '1' * 70 + '}').getBytes('UTF-8'))

        then:
        thrown(IllegalArgumentException)   // or the exception type the integer cap throws; match it
    }

    def 'the longest round-trip double still decodes'() {
        expect:
        DagJson.decode('{"x":-1.7976931348623157E308}'.getBytes('UTF-8')) != null
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `./gradlew test --tests robsyme.cas.core.PutTest --tests 'robsyme.cas.core.DagJson*'`
Expected: the idempotent-ingest test and the over-long float test FAIL.

- [ ] **Step 3: Implement**

- `Put.entryFor`: when it has to `append`, call `ingest()` after appending, then return the name.
- `CasCommands.readCapped`: drop the unused `String source` parameter and its argument at the call site.
- `DagJson`: add `static final int MAX_FLOAT_CHARS = 64` beside `MAX_INTEGER_DIGITS`. Before `Double.parseDouble(text)`, fail when `text.length() > MAX_FLOAT_CHARS`, the same way the integer cap fails, with a message in the same style.

- [ ] **Step 4: Run to see them pass**

Run: `./gradlew test`
Expected: PASS.

- [ ] **Step 5: DESIGN.md**

Change the samplesheet route rows (around 941 and 1166) from `GET` to "`GET` or `HEAD`". In §16, after decision 23, add:

```
24. Deferred minors from milestone 2's reviews (ticket 11). `Put`'s
    idempotent path ingests any Store Log entry it appends; DAG-JSON float
    literals over 64 characters are refused, as over-long integers are.
```

- [ ] **Step 6: Commit**

```bash
git add src DESIGN.md
git commit -m "fix(put): idempotent path ingests; cap DAG-JSON floats; readCapped (ticket 11)"
```

---

### Task 2: The save flow, a failure banner, Retry restore, and Restore on the "already exists" path

**Files:**
- Create: `web/src/save-flow.js`
- Create: `web/test/save-flow.test.mjs`
- Modify: `web/src/save-choice.js` (the here state carries `restore`)
- Modify: `web/test/save-choice.test.mjs`
- Modify: `web/src/views.js` (`compose`; new `failureBanner`)
- Modify: `web/src/app.js` (`runWrite`, `carryOutcome`/`restoreOutcome`, rendering the banner)
- Modify: `DESIGN.md` §15 DOM contract; §16 decision 23 amended

**Interfaces:**
- Consumes: `writer.selection(members)`, `writer.rename(subject, name, supersedes)`, `writer.undo(subject, supersedes)`; `saveChoice`, `namingRequest`, `restoreRequest`.
- Produces:
  - `saveSequence(writer, members, choice, typedName, { onSaved } = {}) -> Promise<{ address, failures }>`. It throws only when the Selection write itself fails. `failures` is an array of `{ step: 'naming' | 'restoring', code, message, retry? }`, where `retry` is the `del`'s `supersedes`, present only for a `restoring` failure.
  - `retryRestore(writer, address, supersedes) -> Promise<void>`.
  - DOM: `[data-write-failed]` banner, `#retry-restore`, `#exists-restore`.
  - `body[data-write-outcome="written"]` after a partly failed save.

  Task 4's driver reads the DOM.

- [ ] **Step 1: Failing tests**

`web/test/save-flow.test.mjs`:

```js
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { saveSequence, retryRestore } from '../src/save-flow.js'

const fail = (code) => Object.assign(new Error(code), { code })
function fakeWriter({ renameFails, undoFails } = {}) {
  const calls = []
  return {
    calls,
    selection: async (members) => { calls.push(['selection']); return { address: 'bafyS' } },
    rename: async (s, name, sup) => { calls.push(['rename', s, name, sup]); if (renameFails) throw fail(renameFails) },
    undo: async (s, sup) => { calls.push(['undo', s, sup]); if (undoFails) throw fail(undoFails) },
  }
}
const restoring = { state: 'elsewhere', names: ['n'], prefill: 'n', supersedes: ['bafyN'], deletion: 'deleted', restore: ['bafyD'] }

test('selection, then name, then del, in that order; no failures', async () => {
  const w = fakeWriter()
  let saved = false
  const r = await saveSequence(w, [], restoring, 'n', { onSaved: () => { saved = true } })
  assert.deepEqual(w.calls.map(c => c[0]), ['selection', 'rename', 'undo'])
  assert.deepEqual(r, { address: 'bafyS', failures: [] })
  assert.equal(saved, true)
})

test('a naming failure still tries the del, and reports naming', async () => {
  const w = fakeWriter({ renameFails: 'clock_skew' })
  const r = await saveSequence(w, [], restoring, 'n')
  assert.deepEqual(w.calls.map(c => c[0]), ['selection', 'rename', 'undo'])
  assert.deepEqual(r.failures.map(f => [f.step, f.code]), [['naming', 'clock_skew']])
})

test('both fail: both reported, and the restoring failure carries its retry', async () => {
  const r = await saveSequence(fakeWriter({ renameFails: 'clock_skew', undoFails: 'write_failed' }), [], restoring, 'n')
  assert.deepEqual(r.failures.map(f => f.step), ['naming', 'restoring'])
  assert.deepEqual(r.failures[1].retry, ['bafyD'])
})

test('no name typed and nothing to restore: only the selection write', async () => {
  const w = fakeWriter()
  await saveSequence(w, [], { state: 'new', names: [], prefill: '', supersedes: [], deletion: 'none', restore: [] }, '')
  assert.deepEqual(w.calls.map(c => c[0]), ['selection'])
})

test('the selection write failing throws, and nothing else is tried', async () => {
  const w = fakeWriter()
  w.selection = async () => { throw fail('too_large') }
  await assert.rejects(saveSequence(w, [], restoring, 'n'), { code: 'too_large' })
})

test('retryRestore sends the same del again, and rethrows a second failure', async () => {
  const w = fakeWriter()
  await retryRestore(w, 'bafyS', ['bafyD'])
  assert.deepEqual(w.calls, [['undo', 'bafyS', ['bafyD']]])
  await assert.rejects(retryRestore(fakeWriter({ undoFails: 'write_failed' }), 'bafyS', ['bafyD']), { code: 'write_failed' })
})
```

In `web/test/save-choice.test.mjs`, change the ticket 10 test "held here and deleted elsewhere … never restores". The here state now carries the restore list, used by the here path's Restore button (not by Save):

```js
test('held here and deleted in the composition: the here path carries the restore list for its Restore button', () => {
  const c = saveChoice(dry({ exists: true, here: true, deletion: 'deleted', deletion_claims: ['bafyD'] }))
  assert.equal(c.state, 'here')
  assert.deepEqual(c.restore, ['bafyD'])
  assert.deepEqual(restoreRequest(c), { supersedes: ['bafyD'] })
})

test('held here and live: nothing to restore, even with a current del in deletion_claims', () => {
  const c = saveChoice(dry({ exists: true, here: true, deletion: 'none', deletion_claims: ['bafyU'] }))
  assert.deepEqual(c.restore, [])
})
```

Add a test for the carried banner's serialisation, if Step 3 extracts it as a pure function. It should cover `failures` round-tripping through JSON, with `code`, `message` and `retry` intact.

- [ ] **Step 2: Run to see them fail**

Run: `cd web && npm test`
Expected: FAIL (no `save-flow.js`; the here state has `restore: []`).

- [ ] **Step 3: Implement**

`web/src/save-flow.js`:

```js
// The compose save sequence (DESIGN.md §16 decisions 21, 23 and 24): the
// Selection, then the name Claim if a name is set, then the del if restoring.
// Once the Selection is saved nothing throws: failures come back to the caller,
// and the del is tried even when naming failed.
import { namingRequest, restoreRequest } from './save-choice.js'

const failure = (step, e, extra = {}) => ({ step, code: e?.code ?? 'write_failed', message: e?.message ?? String(e), ...extra })

export async function saveSequence(writer, members, choice, typedName, { onSaved } = {}) {
  const written = await writer.selection(members)
  onSaved?.()
  const failures = []
  const naming = namingRequest(choice, typedName)
  if (naming) {
    try {
      await writer.rename(written.address, naming.name, naming.supersedes)
    } catch (e) {
      failures.push(failure('naming', e))
    }
  }
  const restoring = restoreRequest(choice)
  if (restoring) {
    try {
      await writer.undo(written.address, restoring.supersedes)
    } catch (e) {
      failures.push(failure('restoring', e, { retry: restoring.supersedes }))
    }
  }
  return { address: written.address, failures }
}

export async function retryRestore(writer, address, supersedes) {
  await writer.undo(address, supersedes)
}
```

`web/src/save-choice.js`: in the `here` branch, return `restore: deletion === 'none' ? [] : (dry.deletion_claims ?? [])` instead of `[]`, and update the header comment.

`web/src/views.js`:

- In `compose`, replace `saveAndName` with a call to `saveSequence(ctx.write.writer, members, choice, name.value, { onSaved: () => { ctx.tray.clear(); ctx.trayChanged() } })`. Return `{ address, href: '#/selection/<address>', failures }`.
- Change `deletedNote(choice.deletion, 'there')` to `deletedNote(choice.deletion, 'in this composition')`.
- `here` branch: when `choice.restore.length`, add a button `#exists-restore` with the label **Restore** after the "Open it to rename it" link:
  `h('button', { type: 'button', id: 'exists-restore', onclick: (e) => ctx.write.run(status, async () => { await ctx.write.writer.undo(dry.address, choice.restore); return { address: dry.address, href: \`#/selection/${dry.address}\` } }, e.currentTarget) }, 'Restore')`.
- Export `failureBanner(address, failures, ctx)`. It returns a `div` with `class: 'warn'`, `'data-write-failed': failures.map(f => f.step).join(' ')` and `'data-banner-for': address`, holding:
  - for each failure, a paragraph "Saved, but naming it failed:" or "Saved, but restoring it failed:" followed by `errorNode({ code: f.code, message: f.message })`;
  - for a `restoring` failure, a button `#retry-restore` labelled **Retry restore**. It runs `ctx.write.run(status, async () => { await retryRestore(ctx.write.writer, address, f.retry); return { address, href: \`#/selection/${address}\` } }, e.currentTarget)`, with its own `status` span inside the banner.

  A naming failure needs no button, because the Selection page's Rename is right below.

`web/src/app.js`:

- In `runWrite`, delete the whole `e.saved` branch, since nothing throws with `saved` any more. On success, when `done.failures?.length`, set a module-level `pendingBanner = { address: done.address, failures: done.failures }` before navigating, and keep `recorded = 'written'`.
- `carryOutcome()` also stores `pendingBanner`, and `restoreOutcome()` restores it. A save from a read-only member's view navigates to the writable member, and the banner must survive that.
- After each `render()`, if `pendingBanner` is set and the current route is `#/selection/<pendingBanner.address>`, insert `views.failureBanner(...)` at the top of the rendered section, then clear `pendingBanner`. A Retry that succeeds re-renders the page without the banner. A Retry that fails reports inside the banner's own status span, as `runWrite` does, and the banner stays.
- `busy` handling is unchanged: `#retry-restore` and `#exists-restore` go through `runWrite` like every other write.

- [ ] **Step 4: Run to see them pass**

Run: `cd web && npm test`, then `./gradlew assemble`.
Expected: PASS; BUILD SUCCESSFUL.

- [ ] **Step 5: DESIGN.md**

§15 DOM contract:
- add `[data-write-failed]` (the banner on the saved Selection's page after a partly failed save: the failed steps, each error in a `[data-error]`),
- add `#retry-restore` (resends the `del` with the same `supersedes`),
- add `#exists-restore` (on `[data-exists]` when the composition's `deletion` is not `none`: one `del` superseding `deletion_claims`),
- note that a partly failed save records the outcome `written`.

§16 decision 23: replace "Held here but deleted elsewhere, the 'already exists' message names the deletion" with "Held here but deleted in the composition, the 'already exists' message names the deletion and offers Restore (one `del` superseding `deletion_claims`; ticket 11)". Add: "The `del` is tried even when naming fails. A save whose naming or restoring failed opens the saved Selection with a banner naming each failure and, for the `del`, Retry restore. Both messages say 'in this composition', since the dry run cannot tell which member holds a deletion." Remove the "restoring it failed … Open it" wording it replaces.

- [ ] **Step 6: Commit**

```bash
git add web DESIGN.md
git commit -m "feat(explorer): a partly failed restore is finished from the page; Restore on the exists path (ticket 11)"
```

---

### Task 3: Page minors

**Files:**
- Modify: `web/src/queries.json:26` and `web/src/model.js` (`latestSuccessfulRun`, around 315-335; `claimsFor`, around 87-107)
- Modify: `web/src/views.js` (`copy` in `selection`, around 249-252; the Selections list, around 205-229)
- Delete: `gate/browser/page-smoke.mjs`
- Test: `web/test/model.test.mjs` (and `selections.test.mjs` if that is where the Selection view is tested)
- Modify: `DESIGN.md` §16 decision 24 (append the page minors)

**Interfaces:**
- Produces: `SQL.successfulRunsPage` (`... ORDER BY finished_at DESC, completion_cid ASC LIMIT ? OFFSET ?`), replacing `successfulRunsOfPipeline`.

- [ ] **Step 1: Failing tests**

Using the fixture the model tests already use (`web/test/fixture.mjs` builds a snapshot and tail), add:

1. **Latest run, the newest deleted in the tail.** The snapshot has runs R1 (newest) and R2, and the tail has a `delete` Claim on R1. `latestSuccessfulRun` returns R2, and reads the snapshot's run rows in pages, the first of size 1.
2. **Latest run, the newest deleted in the snapshot but undone in the tail.** The snapshot has R1 deleted (a snapshot `delete` Claim) and R2, and the tail has a `del` superseding it. The call returns R1.
3. **Latest run, more than 51 deleted.** 60 successful runs are in the snapshot and the tail deletes the newest 55. The call returns the 56th; the old code returned null.
4. **Tail values normalised.** A tail Claim with a map value `{a: 1}` and a snapshot Claim with the same value give the same `value` string from `claimsFor`, matching `Index.valueText`'s convention: strings as they are, anything else as DAG-JSON text. Also cover a number and null.
5. **`runPage` issues one query when there are no Claims.** For a subject with no Claims, `claimsFor` runs `SQL.claimsOf` and not `SQL.supersedesOf`. Count queries through the fixture's db wrapper.

For the view (if the view test harness can render `selection` and the Selections list; otherwise pure helpers):

6. A via-less member's Copy copies the bare item address, not `cas://<item>`.
7. Copy reports failure. With `navigator.clipboard` missing, or `writeText` rejecting, the button text becomes "Copy failed". On success it becomes "Copied".
8. The deleted list, viewed where writing is unavailable (or outside the writable member), shows one `[data-unavailable]` note: `ctx.write.reason`, or the link to open the list in the writable member. This mirrors the Selection view's `actions()`.

If a view item cannot be tested in the existing harness, extract the smallest pure helper (for example, `copyText(member, via)` returning the string to copy) and test that.

- [ ] **Step 2: Run to see them fail**

Run: `cd web && npm test`
Expected: FAIL on the new cases.

- [ ] **Step 3: Implement**

- `queries.json`: replace `successfulRunsOfPipeline` with `successfulRunsPage` (the same `SELECT` and order, `LIMIT ? OFFSET ?`).
- `latestSuccessfulRun` with tail Claims: read pages of the unfiltered successful runs, newest first, with page sizes 1, then 50, then 50, and so on. For each page, compute claim states and take the first run whose current deletion group holds no `delete`. Compare it with the visible stale candidates, as the code does now, and return the newest. Stop at the first page that yields a visible run, or when a page returns fewer rows than asked for. Keep the no-tail-Claims branch as it is.
- `claimsFor`:
  - skip `SQL.supersedesOf` when `claimsOf` returned no rows;
  - normalise tail values: a string stays as it is, anything else is DAG-JSON text. Use `@ipld/dag-json` `encode` plus `TextDecoder`, as `write.js` imports it, so the text matches `Index.valueText` byte for byte for the same value.
- `views.js` `copy`:
  - the via-less branch copies `m.address`;
  - the click handler sets the button's text to "Copied" on success and "Copy failed" on rejection, or when `navigator.clipboard` is missing.
- The Selections list: when `deleted` and writing is unavailable here (`!ctx.write.available || !ctx.write.here`), render one `data-unavailable` paragraph above the table. Its content matches what `actions()` shows in the Selection view: `ctx.write.reason` when unavailable, else "Undo writes to the writable member, <alias>." with a link to this list there (`ctx.write.hrefFor('#/selections?deleted=1')`).
- Delete `gate/browser/page-smoke.mjs`, and remove it from any live doc that mentions it (README, DESIGN, `gate/README.md`). Leave dated plans in `docs/plans/` as they are, since they are records.

- [ ] **Step 4: Run to see them pass**

Run: `cd web && npm test`, then `./gradlew assemble`.
Expected: PASS.

- [ ] **Step 5: DESIGN.md**

Append to decision 24:

```
    On the page: the latest successful run reads the snapshot's runs in
    pages (1, then 50 at a time) until a visible one appears, so a tail
    deletion of more than 50 runs cannot hide an older live run; tail Claim
    values are normalised to the snapshot's text form; `runPage` skips the
    supersedes query when there are no Claims; Copy says when it fails; a
    Selection member picked by query copies as its bare address; the deleted
    list says why Undo is unavailable; `page-smoke.mjs` is retired in favour
    of tier B.
```

Also update the §15 (or §12) text wherever it describes the latest-run query's 50-row read.

- [ ] **Step 6: Commit**

```bash
git add web gate/browser DESIGN.md
git commit -m "fix(explorer): latest run pages until visible; tail Claim values as text; Copy feedback; minors (ticket 11)"
```

---

### Task 4: Gate B16 step 2 clicks Restore

**Files:**
- Modify: `gate/browser_b_assert.py` (`prepare` step `B.deleted`; `b16`)
- Modify: `gate/test_browser_b_assert.py`
- Modify: `gate/README.md` (B16 bullet)

**Interfaces:**
- Consumes: Task 2's `#exists-restore`; the existing `expected["shared"]["s6"]`, `["d6"]`, `shared_store`, `_posts`, `verify_post`, `_claims_about`, `dagjson.claim_state`.

- [ ] **Step 1: Scenario**

Append to `B.deleted`'s actions, after `{"extract": "offer"}`:

```python
{"click": "#exists-restore"}, {"waitWrite": True}, {"extract": "after"}
```

- [ ] **Step 2: Failing unit tests**

Update the `World` fixture's `B.deleted` step. It now records one Claim POST (`del` on S6, superseding `d6`, answered 200), and an `after` extract with `view` S6 and `deletion` `none`. Then:
- the passing world passes;
- `B.deleted` with no Claim POST fails B16;
- a `del` superseding something other than `d6` fails B16;
- `after.deletion == "deleted"` fails B16.

Remove the old check "B.deleted wrote nothing" and its test: the step now writes exactly one `del`.

Run: `python3 -m unittest discover -s gate`
Expected: FAIL.

- [ ] **Step 3: b16**

Replace the "B.deleted wrote nothing" check with the following:
- exactly one writing POST in `B.deleted`, a Claim with verb `del`;
- `verify_post` passes;
- its subject is S6 and its `supersedes` is `[d6]`;
- across both members (`_claims_about(store, s6) + _claims_about(shared_store, s6)`), `claim_state(...)["deletion"] == "none"`;
- `extract("B.deleted", "after")` has `view == s6` and `deletion == "none"`.

Keep the existing offer checks (`exists == s6`, `existsDeletion == "deleted"`). Update the PASS message to say the "already exists" path restored it.

Run: `python3 -m unittest discover -s gate`
Expected: PASS.

- [ ] **Step 4: Docs**

In `gate/README.md`'s B16 bullet, replace "names the deletion and writes nothing" with "names the deletion, and its Restore writes one `del` superseding `shared`'s, so S6 is live across both members".

- [ ] **Step 5: Run the whole Gate**

Run: `GATE_ROOT=<a scratch dir> make gate`
Expected: lineage 11 PASS / 0 FAIL / 6 SKIP, tier A 5/5 (the year-scale limits in A2 must still hold with the paged latest-run query), tier B 9/9. Lineage assertion 4 can fail intermittently by design: rerun once. On a tier B failure read `browser-b/drive.log`, `explore.log` and `observed.json` first. Keep `gate/browser/tier_b.sh` at 100755.

- [ ] **Step 6: Commit**

```bash
git add gate
git commit -m "test(gate): B16 restores a Selection held here and deleted in shared (ticket 11)"
```

---

## After the last task

Run a whole-branch review, then `make gate` once more on the branch tip. Merge to `main` only when Rob says so. Then mark ticket 11 "built" with the merge commit.
