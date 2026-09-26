# Deleted-elsewhere Copy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When "Save a copy here" copies a Selection another member has deleted, restore it, and tell the user when a Selection they already hold has been deleted elsewhere.

**Architecture:** `Put`'s dry run adds the composition's deletion state (`deletion`, `deletion_claims`), read from the same `ClaimState` it already uses for names. The page's pure `saveChoice` gains a restore list. The compose view labels the copy "Restore a copy here" and, after the copy and its name Claim, writes a `del` Claim superseding every current deletion Claim. The `here` message names a deletion held elsewhere. Gate tier B gains B16.

**Tech Stack:** Groovy 4 / Spock (plugin), plain ES modules + `node --test` (`web/`), Python 3 stdlib (Gate), Playwright via `gate/browser/drive.mjs`.

**Spec:** `../.scratch/block-explorer/issues/10-deleted-elsewhere-copy.md` (Answer, Q1-Q4), building on `../.scratch/block-explorer/issues/09-cross-member-put-choices.md` and DESIGN.md §15 and §16 decisions 21-22.

## Global Constraints

- DESIGN.md is the binding contract; when it and the spec disagree, DESIGN.md wins and gets fixed in the same task.
- Groovy main classes are `@CompileStatic`.
- The page never builds a block: the server encodes (no JS DAG-CBOR encoder, spec section 1.4).
- Page Claim actions stay offered only in the writable member (DESIGN §16 decision 14); the Selection view keeps reading only the member's own Claims (decision 22).
- Dry-run body after this plan: `{"address", "exists", "here", "names", "name_claims", "deletion", "deletion_claims"}`; `deletion` is `none`, `deleted` or `conflicted`; `deletion_claims` are DAG-JSON links in claim-address order.
- Write order on "Restore a copy here": the Selection, then the name Claim (only if a name is set), then the `del` Claim (always).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Work on branch `feat/deleted-elsewhere-copy` from `main`.

## Review Focus

1. The name field is cleared before "Restore a copy here": no name Claim, but the `del` Claim is still written (Task 2 `saveChoice`/`restoreRequest` test).
2. The deletion is `conflicted` elsewhere: the `del` supersedes every current deletion Claim, `del` ones included (Task 2 test, Task 1 test for the body).
3. The `del` write fails after the copy saved: the page says the Selection was saved but restoring it failed, and links to it; it must not say "naming it failed" (Task 2, `app.js` message keyed by the failing phase).
4. A Selection deleted elsewhere that is also held here: `here` path, message names the deletion ("in this composition", since the deletion may be this member's own), no Restore button (Task 2 test via `saveChoice` state `here` carrying `deletion`).
5. A Selection held only elsewhere and not deleted: the button still reads "Save a copy here" and no `del` is written (Task 2 test).

---

### Task 1: The dry run reports `deletion` and `deletion_claims`

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/PutResult.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/Put.groovy:116-120`
- Test: `src/test/groovy/robsyme/cas/core/PutTest.groovy`
- Modify: `DESIGN.md` §15 dry-run paragraph and `nf-blocks:put` section; §16 new decision 23

**Interfaces:**
- Consumes: `twoMembers()`, `seedShared()`, `sendTo(...)` already in `PutTest`; `ClaimState.deletion` (String: `none`, `deleted`, `conflicted`) and `ClaimState.deletionClaims` (`List<String>`).
- Produces: `PutResult.dryRun(Cid address, boolean exists, boolean here, List<String> names, List<Cid> nameClaims, String deletion, List<Cid> deletionClaims)`; fields `deletion`, `deletionClaims`; body keys `deletion`, `deletion_claims` after `name_claims`.

- [ ] **Step 1: Write the failing test**

```groovy
    def 'a dry run reports the composition\'s deletion state, a deletion held elsewhere included (ticket 10)'() {
        given:
        final Put seed = seedShared()
        final String json = selection(item(itemA, [collA]))
        final Cid s = sendTo(seed, json).address
        final Cid named = sendTo(seed, claim(s, 'set', 'name', '"from-shared"', [])).address
        final Cid deleted = sendTo(seed, claim(s, 'delete', null, 'null', [])).address
        final Put both = twoMembers()

        when:
        final PutResult dry = sendTo(both, json, true)

        then:
        dry.deletion == 'deleted'
        dry.deletionClaims == [deleted]
        ((Map) DagJson.decode(dry.body())) == [address: s, exists: true, here: false, names: ['from-shared'],
                                               name_claims: [named], deletion: 'deleted', deletion_claims: [deleted]]

        when: 'restored from lab: the copy, then a del superseding the deletion'
        sendTo(both, json)
        final Cid undone = sendTo(both, claim(s, 'del', null, 'null', [deleted], '2025-09-16T05:20:00.001Z')).address

        then:
        sendTo(both, json, true).deletion == 'none'
        sendTo(both, json, true).deletionClaims == []
        !index.claimState(s).hidden

        when: 'a second deletion in shared, alongside lab\'s del, is a conflict'
        final Cid again = sendTo(seed, claim(s, 'delete', null, 'null', [], '2025-09-16T05:20:00.002Z')).address

        then:
        sendTo(both, json, true).deletion == 'conflicted'
        sendTo(both, json, true).deletionClaims.toSet() == [undone, again].toSet()
    }
```

If the conflicted case yields a different current set under `ClaimState`'s rules (read `ClaimState.groovy` and the shared vectors in `web/test/fixtures/claim-vectors.json`), assert what those rules give and say so in the report. Do not change the rules.

In the existing dry-run tests (`'a dry run writes nothing and reports existence and current names'` and `'a dry run says whether the writable member holds the block, and which Claims name it (ticket 09)'`), add `deletion: 'none', deletion_claims: []` to each full body map they compare.

- [ ] **Step 2: Run to see it fail**

Run: `./gradlew test --tests robsyme.cas.core.PutTest`
Expected: FAIL (no `deletion` property, or body maps missing keys).

- [ ] **Step 3: Implement**

`PutResult`: add `final String deletion` and `final List<Cid> deletionClaims`, threaded through the private constructor (`written(...)` passes `null, null`):

```groovy
    static PutResult dryRun(Cid address, boolean exists, boolean here, List<String> names, List<Cid> nameClaims,
                            String deletion, List<Cid> deletionClaims) {
        return new PutResult(address, true, exists, here, names, nameClaims, deletion, deletionClaims, null, null, false)
    }
```

In `body()`'s dry-run branch, after `name_claims`:

```groovy
            out.put('deletion', deletion)
            out.put('deletion_claims', deletionClaims)
```

`Put.groovy` dry-run branch:

```groovy
        if( dryRun ) {
            final ClaimState state = index.claimState(address)
            return PutResult.dryRun(address, store.has(address), writable.has(address), state.names,
                state.nameClaims.collect { String c -> Cid.parse(c) },
                state.deletion, state.deletionClaims.collect { String c -> Cid.parse(c) })
        }
```

- [ ] **Step 4: Run to see it pass**

Run: `./gradlew test`
Expected: PASS. Fix any endpoint or CLI test that pins the full dry-run body by adding the two keys.

- [ ] **Step 5: DESIGN.md**

§15 dry-run sentence: list `"deletion", "deletion_claims"` after `"name_claims"` and add: "`deletion` and `deletion_claims` are the composition's deletion state (`none`, `deleted` or `conflicted`) and its current deletion Claims' addresses, in claim-address order (decision 23)." In the `nf-blocks:put` section, extend the sentence added for decision 22 with "and `deletion`, `deletion_claims`".

§16, append after decision 22:

```
23. A Selection deleted in another member (ticket 10). The dry run reports
    the composition's `deletion` and `deletion_claims`. Held only elsewhere
    and `deleted` or `conflicted` there, the page labels the copy "Restore a
    copy here": after the copy and its name Claim (when a name is set) it
    writes a `del` Claim superseding every Claim in `deletion_claims`,
    whether or not a name is set, so the Selection is live across the
    composition. Held here but deleted elsewhere, the "already exists"
    message names the deletion; the Selection view still reads only this
    member's Claims (decision 22).
```

- [ ] **Step 6: Commit**

```bash
git add src DESIGN.md
git commit -m "feat(put): the dry run reports deletion and deletion_claims (ticket 10)"
```

---

### Task 2: The page restores a copy and names a deletion held elsewhere

**Files:**
- Modify: `web/src/save-choice.js`
- Modify: `web/test/save-choice.test.mjs`
- Modify: `web/src/write.js` (decode `deletion_claims`, as `name_claims` is)
- Test: `web/test/write.test.mjs`
- Modify: `web/src/views.js` (`compose`)
- Modify: `web/src/app.js` (the `e.saved` message)
- Modify: `DESIGN.md` §15 DOM contract rows for `[data-exists]`, `[data-held-elsewhere]`, `#compose-copy`

**Interfaces:**
- Consumes: the dry-run body from Task 1.
- Produces: `saveChoice(dry)` now also returns `deletion` (`none`/`deleted`/`conflicted`) and `restore` (array of Claim addresses; empty unless state is `elsewhere` and deletion is not `none`); `restoreRequest(choice) -> {supersedes} | null`. DOM: `data-deletion` on `[data-exists]` and `[data-held-elsewhere]`; `#compose-copy` text "Restore a copy here" when restoring, else "Save a copy here". An error thrown after the copy saved carries `saved` and `failed` (`'naming'` or `'restoring'`). Task 3's driver reads these.

- [ ] **Step 1: Write the failing tests**

Add to `web/test/save-choice.test.mjs` (keep existing tests; update any `deepEqual` on a whole choice object to include `deletion: 'none', restore: []`):

```js
import { restoreRequest } from '../src/save-choice.js'

test('held only elsewhere and deleted there: restore supersedes the deletion', () => {
  const c = saveChoice(dry({ exists: true, names: ['n'], name_claims: ['bafyN'], deletion: 'deleted', deletion_claims: ['bafyD'] }))
  assert.equal(c.state, 'elsewhere')
  assert.equal(c.deletion, 'deleted')
  assert.deepEqual(c.restore, ['bafyD'])
  assert.deepEqual(restoreRequest(c), { supersedes: ['bafyD'] })
})

test('a conflicted deletion elsewhere restores over every current deletion Claim', () => {
  const c = saveChoice(dry({ exists: true, deletion: 'conflicted', deletion_claims: ['bafy1', 'bafy2'] }))
  assert.deepEqual(restoreRequest(c), { supersedes: ['bafy1', 'bafy2'] })
})

test('restoring does not depend on the name: a cleared name still restores', () => {
  const c = saveChoice(dry({ exists: true, names: ['n'], name_claims: ['bafyN'], deletion: 'deleted', deletion_claims: ['bafyD'] }))
  assert.equal(namingRequest(c, ''), null)
  assert.deepEqual(restoreRequest(c), { supersedes: ['bafyD'] })
})

test('held only elsewhere and live: a plain copy, nothing to restore', () => {
  const c = saveChoice(dry({ exists: true, names: ['n'], name_claims: ['bafyN'] }))
  assert.equal(c.deletion, 'none')
  assert.equal(restoreRequest(c), null)
})

test('held here and deleted elsewhere: the here path carries the deletion, and never restores', () => {
  const c = saveChoice(dry({ exists: true, here: true, deletion: 'deleted', deletion_claims: ['bafyD'] }))
  assert.equal(c.state, 'here')
  assert.equal(c.deletion, 'deleted')
  assert.equal(restoreRequest(c), null)
})
```

In `web/test/write.test.mjs`, extend the dry-run `name_claims` test (or add one beside it, same fake-fetch pattern) so a body with `deletion_claims: [CID]` comes back with `deletion_claims` as strings.

- [ ] **Step 2: Run to see them fail**

Run: `cd web && npm test`
Expected: FAIL (`restoreRequest` not exported; missing `deletion`/`restore`).

- [ ] **Step 3: Implement**

`web/src/save-choice.js`:

```js
// What Save does after the dry run (DESIGN.md §16 decisions 21 and 23). Pure,
// so the outcomes are testable without a DOM.
export function saveChoice(dry) {
  const deletion = dry.deletion ?? 'none'
  if (!dry.exists) return { state: 'new', names: [], prefill: '', supersedes: [], deletion: 'none', restore: [] }
  const names = dry.names ?? []
  if (dry.here ?? true) return { state: 'here', names, prefill: '', supersedes: [], deletion, restore: [] }
  const nameClaims = dry.name_claims ?? []
  const prefill = names.length === 1 && nameClaims.length === 1 ? names[0] : ''
  const restore = deletion === 'none' ? [] : (dry.deletion_claims ?? [])
  return { state: 'elsewhere', names, prefill, supersedes: nameClaims, deletion, restore }
}

// The name Claim to write after a save, or null when no name was typed.
export function namingRequest(choice, typed) {
  const name = (typed ?? '').trim()
  return name ? { name, supersedes: choice.supersedes } : null
}

// The del Claim that restores a copy deleted elsewhere, or null when there is nothing to restore.
export function restoreRequest(choice) {
  return choice.restore.length ? { supersedes: choice.restore } : null
}
```

Existing tests whose expected object lacks `deletion`/`restore` must be updated in Step 1, as stated there.

`web/src/write.js`: next to the `name_claims` mapping, map `body.deletion_claims` to strings the same way.

`web/src/views.js` `compose`: import `restoreRequest`. In `saveAndName`, after the naming block:

```js
      const restoring = restoreRequest(choice)
      if (restoring) {
        try {
          await ctx.write.writer.undo(written.address, restoring.supersedes)
        } catch (e) {
          throw Object.assign(e, { saved: written.address, failed: 'restoring' })
        }
      }
```

and change the naming catch to `throw Object.assign(e, { saved: written.address, failed: 'naming' })`.

Add a helper beside `compose` and use it in both branches:

```js
const deletedNote = (deletion, where) => deletion === 'deleted' ? ` It is deleted ${where}.`
  : deletion === 'conflicted' ? ` Its deletion is in conflict ${where}.` : ''
```

- `here` branch: add `'data-deletion': choice.deletion` to the `[data-exists]` paragraph, and append `deletedNote(choice.deletion, 'in this composition')` after `This Selection already exists${named}.`. The dry run's deletion is composition-wide and may be this member's own, so "in this composition" is the wording that is always true.
- `elsewhere` branch: add `'data-deletion': choice.deletion` to the `[data-held-elsewhere]` paragraph, append `deletedNote(choice.deletion, 'there')` after `This Selection is already held in another member${named}.`, and set the button text to `choice.restore.length ? 'Restore a copy here' : 'Save a copy here'`.

`web/src/app.js`, the `e.saved` block:

```js
        const what = e.failed === 'restoring' ? 'restoring it' : 'naming it'
        node.prepend(`The Selection was saved, but ${what} failed: `)
        node.append(' ', link(write.hrefFor(`#/selection/${e.saved}`), e.failed === 'restoring' ? 'Open it' : 'Open it to name it'))
```

- [ ] **Step 4: Run to see them pass**

Run: `cd web && npm test`, then `./gradlew assemble` (the plugin bundles the page).
Expected: PASS; BUILD SUCCESSFUL.

- [ ] **Step 5: DESIGN.md §15**

In the DOM contract table: `[data-exists]` and `[data-held-elsewhere]` rows gain "`data-deletion` the composition's deletion state (decision 23)"; the `#compose-copy` row becomes "'Save a copy here', or 'Restore a copy here' when the dry run's deletion is not `none`: the write, the name Claim superseding `name_claims`, then a `del` superseding `deletion_claims` (decision 23)". Where DESIGN describes the "saved, but naming it failed" message, add the restoring variant.

- [ ] **Step 6: Commit**

```bash
git add web DESIGN.md
git commit -m "feat(explorer): restore a copy deleted in another member; name a deletion held elsewhere (ticket 10)"
```

---

### Task 3: Gate tier B16

**Files:**
- Modify: `gate/browser_b_assert.py` (`prepare`, `evaluate`)
- Modify: `gate/browser/drive.mjs` (`EXTRACT_B`)
- Test: `gate/test_browser_b_assert.py`
- Modify: `gate/browser/tier_b.sh` header (assertions 8-16), `gate/README.md` (B16 bullet, tier B count), `DESIGN.md` §16 tier B paragraph

**Interfaces:**
- Consumes: Task 1's body; Task 2's DOM (`data-deletion`, `#compose-copy` text, `[data-exists]`); the existing tier B helpers `_put_block(root, block)`, `_log(root, kind, cid, millis)`, `SHARED_TS`, `SHARED_MILLIS`, `_item`, `_posts`, `_kind`, `verify_post`, `_claims_about`, `dagjson.claim_state`, `dagjson.expected_claim`, `dagjson.expected_selection`, and `shared_store` in `evaluate`.
- Produces: `expected["shared"]` gains `s5`, `n5`, `d5`, `s6`, `d6`; observed steps `B.restore` and `B.deleted`.

- [ ] **Step 1: Prepare the blocks**

In `prepare`, after the S3/S4 block, reusing the existing `claim(...)`/`selection(...)` helpers there and adding one for a deletion:

```python
    def deletion(subject):
        return dagjson.expected_claim({"subject": cas.Cid(subject), "verb": "delete", "attribute": None, "value": None,
                                       "supersedes": [], "timestamp": SHARED_TS}, ASSERTED_BY)

    # S5: B from cold's aligned, held only in shared, named and deleted there (B.restore restores it).
    s5 = _put_block(shared, selection([_item(b, [coll["aligned"]])]))
    _log(shared, "selection", s5, SHARED_MILLIS)
    n5 = _put_block(shared, claim(s5, "restored-name", []))
    _log(shared, "claim", n5, SHARED_MILLIS)
    d5 = _put_block(shared, deletion(s5))
    _log(shared, "claim", d5, SHARED_MILLIS)
    # S6: B from again's aligned, held in lab, deleted in shared (B.deleted names the deletion).
    s6 = _put_block(store, selection([_item(b, [again_aligned])]))
    _log(store, "selection", s6, SHARED_MILLIS)
    d6 = _put_block(shared, deletion(s6))
    _log(shared, "claim", d6, SHARED_MILLIS)
```

Before writing, check that neither S5 ({B via aligned}) nor S6 ({B via again}) equals any Selection another step composes or B12's refusal Selection. If one does, pick a different single member and say so in the report. Add `s5, n5, d5, s6, d6` to `expected["shared"]`.

Append two steps after `B.foreign`:

```python
        {"id": "B.restore", "server": "explore", "path": "", "query": q, "hash": "#/item/%s/%s" % (coll["aligned"], b),
         "actions": [{"click": "[data-pick]"}, {"hash": "#/compose"}, {"click": "#compose-save"}, {"waitWrite": True},
                     {"extract": "offer"}, {"click": "#compose-copy"}, {"waitWrite": True}, {"extract": "after"}]},
        {"id": "B.deleted", "server": "explore", "path": "", "query": q, "hash": "#/item/%s/%s" % (again_aligned, b),
         "actions": [{"click": "[data-pick]"}, {"hash": "#/compose"}, {"click": "#compose-save"}, {"waitWrite": True},
                     {"extract": "offer"}]},
```

- [ ] **Step 2: The driver reads the new DOM**

In `EXTRACT_B`, add:

```js
  heldElsewhereDeletion: document.querySelector('[data-held-elsewhere]')?.dataset.deletion ?? null,
  exists: document.querySelector('[data-exists]')?.dataset.exists ?? null,
  existsDeletion: document.querySelector('[data-exists]')?.dataset.deletion ?? null,
  copyLabel: document.getElementById('compose-copy')?.textContent ?? null,
```

- [ ] **Step 3: Failing unit tests for B16**

In `gate/test_browser_b_assert.py`, extend the `World` fixture the file uses for B14/B15 with the new blocks and steps, and add:
- one test where the whole world passes B16;
- one where the `del` Claim POST is missing from `B.restore` (B16 fails);
- one where the `del` supersedes nothing (B16 fails);
- one where `B.deleted`'s `existsDeletion` is `none` (B16 fails).

Run: `python3 -m unittest discover -s gate`
Expected: FAIL, because B16 doesn't exist yet.

- [ ] **Step 4: B16 in `evaluate`**

```python
    def b16():
        problems, sh = [], expected["shared"]
        offer = extract("B.restore", "offer")
        if offer.get("heldElsewhere") != sh["s5"] or offer.get("heldElsewhereDeletion") != "deleted":
            problems.append("the dry run offered %r (deletion %r), expected S5 %s deleted in shared"
                            % (offer.get("heldElsewhere"), offer.get("heldElsewhereDeletion"), sh["s5"]))
        if offer.get("copyLabel") != "Restore a copy here":
            problems.append("the copy button read %r, expected 'Restore a copy here'" % offer.get("copyLabel"))
        if offer.get("composeName") != "restored-name":
            problems.append("the name field held %r, expected shared's name restored-name" % offer.get("composeName"))
        posts = _posts(seen("B.restore"))
        sels = [p for p in posts if _kind(p) == "Selection"]
        claims = [p for p in posts if _kind(p) == "Claim"]
        verbs = [json.loads(p["body"]).get("verb") for p in claims]
        if len(sels) != 1 or verbs != ["set", "del"]:
            problems.append("B.restore: %d Selection POST(s) and Claim verbs %r, expected 1 and ['set', 'del']" % (len(sels), verbs))
        else:
            address, _b, found = verify_post("B.restore", sels[0], dagjson.expected_selection)
            problems += found
            if address != sh["s5"]:
                problems.append("B.restore wrote %s, the Selection shared holds is %s" % (address, sh["s5"]))
            for post in claims:
                _c, block, found = verify_post("B.restore", post, dagjson.expected_claim)
                problems += found
                if _text(block["subject"]) != sh["s5"]:
                    problems.append("a %s Claim is about %s, not S5" % (block["verb"], _text(block["subject"])))
                want = [sh["n5"]] if block["verb"] == "set" else [sh["d5"]]
                if [_text(x) for x in block["supersedes"]] != want:
                    problems.append("the %s Claim supersedes %r, expected %r" % (block["verb"], [_text(x) for x in block["supersedes"]], want))
        state = dagjson.claim_state(_claims_about(store, sh["s5"]) + _claims_about(shared_store, sh["s5"]))
        if state["names"] != ["restored-name"] or state["deletion"] != "none":
            problems.append("across both members S5 is named %r with deletion %r, expected restored-name and none"
                            % (state["names"], state["deletion"]))
        held = extract("B.deleted", "offer")
        if held.get("exists") != sh["s6"] or held.get("existsDeletion") != "deleted":
            problems.append("composing S6 showed exists %r with deletion %r, expected S6 %s deleted"
                            % (held.get("exists"), held.get("existsDeletion"), sh["s6"]))
        if _posts(seen("B.deleted")):
            problems.append("B.deleted wrote %d request(s); the here path must write nothing" % len(_posts(seen("B.deleted"))))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("a Selection shared named and deleted was offered as 'Restore a copy here' with its name prefilled; one "
                      "click wrote it into lab with a name Claim and a del superseding shared's, live across both members; "
                      "composing a Selection lab holds and shared deleted named the deletion and wrote nothing")
```

Use the real key names of `dagjson.claim_state` (as B14/B15 do; e.g. `deletion`). Register B16 after B15, the same way B14 and B15 are registered:

```python
    run("B16", "a copy deleted in another member is restored; a deletion held elsewhere is named", b16)
```

Run: `python3 -m unittest discover -s gate`
Expected: PASS.

- [ ] **Step 5: Docs**

In `gate/README.md`, after B15:

```
- B16: `shared` names and deletes a Selection only it holds; composing it in the
  page offers "Restore a copy here" with the name prefilled, and one click
  writes it into `lab` with a name Claim and a `del` superseding `shared`'s
  deletion, so both members together show it live and named. Composing a
  Selection `lab` holds and `shared` deleted names the deletion and writes
  nothing.
```

Update the tier B count. In DESIGN §16's tier B paragraph add "a copy deleted in another member is restored (16)", reflowed to the paragraph's width. `tier_b.sh` header: assertions 8-16.

- [ ] **Step 6: Run the whole Gate**

Run: `GATE_ROOT=<a scratch dir> make gate`
Expected: lineage 11 PASS / 0 FAIL / 6 SKIP, tier A 5/5, tier B 9/9. On a tier B failure read `browser-b/drive.log`, `explore.log` and `observed.json` first. Lineage assertion 4 can fail intermittently by design: rerun once before debugging it.

- [ ] **Step 7: Commit**

```bash
git add gate DESIGN.md
git commit -m "test(gate): B16, a copy deleted in another member is restored (ticket 10)"
```

Check `git diff --cached --summary` before committing: `tier_b.sh` must stay mode 100755.

---

## After the last task

Run a whole-branch review, then `make gate` once more on the branch tip. Merge to `main` only when Rob says so. Then mark ticket 10 "built" with the merge commit.
