# Cross-member Put Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the explorer's write path behave sensibly when the composition has a read-only member: a Selection held only elsewhere can be copied here and named, and a Claim in another member no longer locks renames.

**Architecture:** `Put` (the one builder behind `nf-blocks:put` and `POST /api/put`) gains two things: its dry run reports `here` (the writable member holds the block) and `name_claims`, and its "already superseded" check counts only Claims logged in the writable member. The page turns a dry run of `exists && !here` into a "Save a copy here" offer whose name Claim supersedes the composition's current name Claims. Gate tier B gains a read-only member built by the Gate itself, and two assertions.

**Tech Stack:** Groovy 4 / Spock (plugin), SQLite cache index, plain ES modules + `node --test` (page, `web/`), Python 3 stdlib (Gate), Playwright via `gate/browser/drive.mjs`.

**Spec:** `../.scratch/block-explorer/issues/09-cross-member-put-choices.md` (the decisions, Q1-Q8), with `DESIGN.md` §15 (write endpoint, DOM contract) and §16 (milestone 2 decisions). Read the ticket's Answer first.

## Global Constraints

- DESIGN.md is the binding contract; when it and the spec disagree, DESIGN.md wins and gets fixed in the same task.
- Groovy main classes are `@CompileStatic`.
- The page never builds a block: the server encodes (no JS DAG-CBOR encoder, spec section 1.4).
- Page Claim actions stay offered only in the writable member (DESIGN §16 decision 14).
- `exists` keeps its meaning: true when any member of the composition holds the block.
- The Gate hashes its own bytes and never asks the plugin what it stored; `gate/*.py` is Python stdlib only.
- Nextflow 26.04.6 (the Gate refuses any other).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Work on branch `feat/cross-member-put` from `main`.

## Review Focus

1. The pre-filled name is cleared before "Save a copy here": the copy is saved unnamed and no Claim is written (Task 3, `namingRequest` test).
2. The other member's names are in conflict: the field stays empty, every name is listed, and a typed name supersedes all of their Claims (Task 3, `saveChoice` test).
3. Both the writable and a read-only member hold the block: `here` is true and the page keeps today's "Open it to rename it" path (Task 1 test).
4. A superseder logged in the writable member still refuses with `stale_supersedes`, as B11 needs, while one only in a read-only member does not (Task 2 test).
5. A Claim that supersedes Claims held only in a read-only member (the CLI's way to settle a cross-member conflict) passes the presence check and settles it (Task 2 test).

---

### Task 1: Dry run reports `here` and `name_claims`

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/PutResult.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/Put.groovy:116-117`
- Test: `src/test/groovy/robsyme/cas/core/PutTest.groovy`
- Modify: `DESIGN.md` §15 dry-run paragraph (around line 1119), §16 (new decision 21, the "fourth" sentence at 1281)

**Interfaces:**
- Produces: `PutResult.dryRun(Cid address, boolean exists, boolean here, List<String> names, List<Cid> nameClaims)`; fields `here`, `nameClaims`; dry-run body `{"address", "exists", "here", "names", "name_claims"}` with `name_claims` as DAG-JSON links in claim-address order.
- Produces (tests): `PutTest.twoMembers()` returns a `Put` over `CompositeStore([store, shared])` writing to `store` (`lab`); `PutTest.seedShared()` returns a `Put` that writes into `shared`. Task 2 reuses both.

- [ ] **Step 1: Add the two-member fixtures and the failing test**

In `PutTest`, add beside `newPut`:

```groovy
    LocalBlockStore shared

    /** lab writable, shared read-only: the composition the page sees with a Bundle mounted. */
    private Put twoMembers() {
        shared = new LocalBlockStore(tempDir.resolve('shared'), 'shared', false)
        return new Put(new CompositeStore([store, shared]), store, index, 'ada', { now }, catchUpBoth())
    }

    /** Writes into shared's directory, as the host where it was writable once did. */
    private Put seedShared() {
        final LocalBlockStore seed = new LocalBlockStore(tempDir.resolve('shared'), 'shared', true)
        return new Put(new CompositeStore([seed, store]), seed, index, 'ada', { now }, catchUpBoth())
    }

    private Closure catchUpBoth() {
        return {
            index.catchUp(store, StoreLog.of(store), 'lab')
            final LocalBlockStore s = new LocalBlockStore(tempDir.resolve('shared'), 'shared', false)
            index.catchUp(s, StoreLog.of(s), 'shared')
        }
    }

    private static PutResult sendTo(Put p, String json, boolean dry = false) { p.put(json.getBytes('UTF-8'), dry) }
```

Then the test:

```groovy
    def 'a dry run says whether the writable member holds the block, and which Claims name it (ticket 09)'() {
        given:
        final Put seed = seedShared()
        final String json = selection(item(itemA, [collA]))
        final Cid s = sendTo(seed, json).address
        final Cid named = sendTo(seed, claim(s, 'set', 'name', '"from-shared"', [])).address
        final Put both = twoMembers()

        when:
        final PutResult dry = sendTo(both, json, true)

        then:
        dry.exists
        !dry.here
        dry.names == ['from-shared']
        dry.nameClaims == [named]
        ((Map) DagJson.decode(dry.body())) == [address: s, exists: true, here: false, names: ['from-shared'], name_claims: [named]]

        when: 'the real write copies the block into the writable member'
        final PutResult copied = sendTo(both, json)

        then:
        copied.written
        store.has(s)
        StoreLog.read(store).any { it.cid == s }

        and: 'held in both members now, so here'
        sendTo(both, json, true).here
    }
```

If `StoreLogEntry` names its address field differently than `cid`, use its actual name (see `src/main/groovy/robsyme/cas/core/StoreLogEntry.groovy`).

Update the existing `'a dry run writes nothing and reports existence and current names'`: capture the name Claim (`final Cid named = send(claim(written, 'set', 'name', '"first"', [])).address`), assert `!dry.here` and `dry.nameClaims == []` on the first dry run, and change the final map to `[address: written, exists: true, here: true, names: ['first'], name_claims: [named]]`.

- [ ] **Step 2: Run to see it fail**

Run: `./gradlew test --tests robsyme.cas.core.PutTest`
Expected: compilation failure or FAIL on `dry.here` / `dry.nameClaims`.

- [ ] **Step 3: Implement**

`PutResult`: add `final boolean here` and `final List<Cid> nameClaims`, thread them through the private constructor (`written(...)` passes `true, null`), and change the factory:

```groovy
    static PutResult dryRun(Cid address, boolean exists, boolean here, List<String> names, List<Cid> nameClaims) {
        return new PutResult(address, true, exists, here, names, nameClaims, null, null, false)
    }
```

In `body()`'s dry-run branch:

```groovy
            out.put('exists', exists)
            out.put('here', here)
            out.put('names', names)
            out.put('name_claims', nameClaims)
```

`Put.groovy:116-117`:

```groovy
        if( dryRun ) {
            final ClaimState state = index.claimState(address)
            return PutResult.dryRun(address, store.has(address), writable.has(address), state.names,
                state.nameClaims.collect { String c -> Cid.parse(c) })
        }
```

Check every other caller of `PutResult.dryRun` (`grep -rn "PutResult.dryRun" src`) and update it.

- [ ] **Step 4: Run to see it pass**

Run: `./gradlew test --tests robsyme.cas.core.PutTest --tests 'robsyme.cas.explore.*' --tests 'robsyme.cas.cli.*'`
Expected: PASS. Fix any endpoint or CLI test that pins the old dry-run body by adding `here` and `name_claims`.

- [ ] **Step 5: DESIGN.md**

§15, replace the dry-run sentence with:

```
Dry run
(`?dry_run=true`, decision 10): `{"address", "exists", "here", "names",
"name_claims"}`, and writes nothing; `exists` is true when any member of the
composition holds the block, `here` when the writable member does (decision
21). `names` and `name_claims` are the composition's current name Claims'
values and addresses, in claim-address order.
```

§16, line 1281: replace "The fourth is open, below." with "The fourth was already fixed; see Resolved, below." Append after decision 20:

```
21. A Selection held only in a read-only member (ticket 09). The dry run's
    `here` separates "held here" from "held somewhere". With `exists` and
    not `here`, the page offers "Save a copy here": the ordinary write,
    which copies the block into the writable member, logs and ingests it.
    The name field is prefilled when the composition has exactly one
    current name, and the name Claim written with the copy supersedes every
    current name Claim in `name_claims`, so the composition ends with one
    name rather than a same-value conflict.
```

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/PutResult.groovy src/main/groovy/robsyme/cas/core/Put.groovy \
        src/test DESIGN.md
git commit -m "feat(put): the dry run reports here and name_claims (ticket 09)"
```

---

### Task 2: Staleness counts only the writable member's Claims

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy:549-556`
- Modify: `src/main/groovy/robsyme/cas/core/Put.groovy:303-305`
- Test: `src/test/groovy/robsyme/cas/core/PutTest.groovy`
- Modify: `DESIGN.md` §16 (decision 6's staleness clause; new decision 22)

**Interfaces:**
- Consumes: `twoMembers()`, `seedShared()`, `sendTo(...)` from Task 1.
- Produces: `Index.supersedersOf(Cid claim, String member)`.

- [ ] **Step 1: Write the failing test**

```groovy
    private static PutError refusedBy(Put p, String json) {
        try {
            p.put(json.getBytes('UTF-8'), false)
        }
        catch( PutError e ) {
            return e
        }
        throw new AssertionError('expected a PutError')
    }

    def 'staleness counts the writable member\'s Claims; presence counts every member (ticket 09)'() {
        given: 'a Selection named in lab, renamed by a Claim only shared holds'
        final Put both = twoMembers()
        final Cid s = sendTo(both, selection(item(itemA, [collA]))).address
        final Cid labName = sendTo(both, claim(s, 'set', 'name', '"lab-name"', [])).address
        final Cid sharedName = sendTo(seedShared(), claim(s, 'set', 'name', '"shared-name"', [labName])).address

        when: 'renaming from what lab shows'
        final PutResult renamed = sendTo(both, claim(s, 'set', 'name', '"lab-renamed"', [labName], '2025-09-16T05:20:00.001Z'))

        then: 'written, and the composition reports the disagreement as a conflict'
        renamed.written
        index.claimState(s).names.toSorted() == ['lab-renamed', 'shared-name']
        index.claimState(s).nameClaims.size() == 2

        when: 'a superseder lab holds still refuses'
        final PutError stale = refusedBy(both, claim(s, 'set', 'name', '"again"', [labName], '2025-09-16T05:20:00.002Z'))

        then:
        stale.code == 'stale_supersedes'
        stale.message.contains(renamed.address.toString())

        when: 'one Claim superseding both current names, one of them held only in shared'
        sendTo(both, claim(s, 'set', 'name', '"settled"', [renamed.address, sharedName], '2025-09-16T05:20:00.003Z'))

        then:
        index.claimState(s).names == ['settled']
    }
```

- [ ] **Step 2: Run to see it fail**

Run: `./gradlew test --tests robsyme.cas.core.PutTest`
Expected: FAIL at the first `when:` with `stale_supersedes ... already superseded by <sharedName>`.

- [ ] **Step 3: Implement**

`Index.groovy`, replace `supersedersOf`:

```groovy
    /**
     * The Claims logged in {@code member} that supersede {@code claim}. Put
     * checks staleness against the writable member only, the Claims the page
     * can see there (DESIGN.md §16 decision 22).
     */
    List<Cid> supersedersOf(Cid claim, String member) {
        final List<Cid> out = new ArrayList<Cid>()
        query('''SELECT s.claim_cid FROM claim_supersedes s
                 JOIN log_entry e ON e.cid = s.claim_cid AND e.member = ?
                 WHERE s.superseded_cid = ? ORDER BY s.claim_cid''', [member, claim.toString()]) { ResultSet rs ->
            out.add(Cid.parse(rs.getString(1)))
        }
        return out
    }
```

Run `grep -rn supersedersOf src` first. If anything besides `Put` calls the one-argument form, keep it beside the new one rather than replacing it.

`Put.groovy:303`: `final List<Cid> by = index.supersedersOf(s, writable.alias())`. Leave the `store.has(s)` presence check above it as it is.

- [ ] **Step 4: Run to see it pass**

Run: `./gradlew test`
Expected: PASS, including the existing single-member rename test (its superseder is logged in `lab`).

- [ ] **Step 5: DESIGN.md §16**

In decision 6, after "present and not already superseded (`stale_supersedes` otherwise)", add "(superseded by a Claim logged in the writable member; decision 22)". Append:

```
22. Staleness across members (ticket 09). The "already superseded" check
    counts only Claims logged in the writable member, the ones the page can
    see there. A superseder held only in a read-only member no longer blocks
    a rename; the composition then has two current names, reported as a
    conflict in claim-address order. The presence check stays
    composition-wide, so `nf-blocks:put` can supersede a Claim another
    member holds: that is how a person settles a conflict between members.
```

- [ ] **Step 6: Commit**

```bash
git add src DESIGN.md
git commit -m "fix(put): stale_supersedes counts only the writable member's Claims (ticket 09)"
```

---

### Task 3: The page offers "Save a copy here"

**Files:**
- Create: `web/src/save-choice.js`
- Create: `web/test/save-choice.test.mjs`
- Modify: `web/src/write.js:30` (decode `name_claims`)
- Modify: `web/src/views.js` (`compose`, around lines 320-345)
- Modify: `web/src/app.js:169-171` (the `exists` outcome)
- Test: `web/test/write.test.mjs`
- Modify: `DESIGN.md` §15 DOM contract table (around line 1046) and the `data-write-outcome` values

**Interfaces:**
- Consumes: the dry-run body from Task 1.
- Produces: `saveChoice(dry) -> {state: 'new'|'here'|'elsewhere', names, prefill, supersedes}`; `namingRequest(choice, typed) -> {name, supersedes} | null`; DOM `[data-held-elsewhere=<cid>][data-names=<json>]`, button `#compose-copy`, `body[data-write-outcome="elsewhere"]`. Task 4's driver reads these.

- [ ] **Step 1: Write the failing tests**

`web/test/save-choice.test.mjs`:

```js
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { saveChoice, namingRequest } from '../src/save-choice.js'

const dry = (o) => ({ address: 'bafyS', exists: false, here: false, names: [], name_claims: [], ...o })

test('a Selection nobody holds is a new save that supersedes nothing', () => {
  assert.deepEqual(saveChoice(dry({})), { state: 'new', names: [], prefill: '', supersedes: [] })
})

test('held here keeps the open-it-to-rename path', () => {
  assert.equal(saveChoice(dry({ exists: true, here: true, names: ['a'] })).state, 'here')
})

test('held only elsewhere with one name: prefilled, superseding that Claim', () => {
  assert.deepEqual(saveChoice(dry({ exists: true, names: ['from-shared'], name_claims: ['bafyN'] })),
    { state: 'elsewhere', names: ['from-shared'], prefill: 'from-shared', supersedes: ['bafyN'] })
})

test('held only elsewhere in conflict: no prefill, every current name Claim superseded', () => {
  const c = saveChoice(dry({ exists: true, names: ['a', 'b'], name_claims: ['bafy1', 'bafy2'] }))
  assert.equal(c.prefill, '')
  assert.deepEqual(c.names, ['a', 'b'])
  assert.deepEqual(c.supersedes, ['bafy1', 'bafy2'])
})

test('a cleared name writes no Claim; an edited one still supersedes', () => {
  const c = saveChoice(dry({ exists: true, names: ['x'], name_claims: ['bafyN'] }))
  assert.equal(namingRequest(c, '   '), null)
  assert.deepEqual(namingRequest(c, ' mine '), { name: 'mine', supersedes: ['bafyN'] })
  assert.deepEqual(namingRequest(saveChoice(dry({})), 'first'), { name: 'first', supersedes: [] })
})
```

In `web/test/write.test.mjs`, add a test beside the existing ones, using the same fake `fetchFn` pattern those tests already use: a dry-run response whose DAG-JSON body carries `name_claims: [CID]` comes back from `writer(...).selection(members, { dryRun: true })` with `name_claims` as an array of strings.

- [ ] **Step 2: Run to see them fail**

Run: `cd web && npm test`
Expected: FAIL, `Cannot find module '../src/save-choice.js'` and the `name_claims` assertion.

- [ ] **Step 3: Implement**

`web/src/save-choice.js`:

```js
// What Save does after the dry run (DESIGN.md §16 decision 21). Pure, so the
// three outcomes are testable without a DOM.
export function saveChoice(dry) {
  if (!dry.exists) return { state: 'new', names: [], prefill: '', supersedes: [] }
  const names = dry.names ?? []
  if (dry.here) return { state: 'here', names, prefill: '', supersedes: [] }
  return { state: 'elsewhere', names, prefill: names.length === 1 ? names[0] : '', supersedes: dry.name_claims ?? [] }
}

// The name Claim to write after a save, or null when no name was typed.
export function namingRequest(choice, typed) {
  const name = (typed ?? '').trim()
  return name ? { name, supersedes: choice.supersedes } : null
}
```

`web/src/write.js:30`:

```js
      return { ...body, address: body.address.toString(),
        ...(body.name_claims ? { name_claims: body.name_claims.map(String) } : {}) }
```

`web/src/views.js`: import `{ saveChoice, namingRequest }` from `./save-choice.js`. In `compose`, replace the `onclick` body with:

```js
    const members = ctx.tray.toMembers()
    const saveAndName = async (choice) => {
      const written = await ctx.write.writer.selection(members)
      ctx.tray.clear()
      ctx.trayChanged()
      const naming = namingRequest(choice, name.value)
      if (naming) {
        try {
          await ctx.write.writer.rename(written.address, naming.name, naming.supersedes)
        } catch (e) {
          throw Object.assign(e, { saved: written.address })
        }
      }
      return { address: written.address, href: `#/selection/${written.address}` }
    }
    const dry = await ctx.write.writer.selection(members, { dryRun: true })
    const choice = saveChoice(dry)
    const cancel = h('button', { type: 'button', onclick: () => status.replaceChildren() }, 'cancel')
    if (choice.state === 'here') {
      status.replaceChildren(h('p', { 'data-exists': dry.address, 'data-names': JSON.stringify(dry.names) },
        `This Selection already exists${dry.names.length ? ` as ${dry.names.join(', ')}` : ', unnamed'}. `,
        link(ctx.write.hrefFor(`#/selection/${dry.address}`), 'Open it to rename it'), ' or ', cancel, '.'))
      return { outcome: 'exists' }
    }
    if (choice.state === 'elsewhere') {
      if (!name.value.trim() && choice.prefill) name.value = choice.prefill
      const named = choice.names.length === 0 ? ', unnamed'
        : choice.names.length === 1 ? ` as ${choice.names[0]}` : ` as ${choice.names.join(', ')} (in conflict)`
      status.replaceChildren(h('p', { 'data-held-elsewhere': dry.address, 'data-names': JSON.stringify(choice.names) },
        `This Selection is already held in another member${named}. `,
        h('button', { type: 'button', id: 'compose-copy', onclick: (e) => ctx.write.run(status, () => saveAndName(choice), e.currentTarget) },
          'Save a copy here'), ' or ', cancel, '.'))
      return { outcome: 'elsewhere' }
    }
    return saveAndName(choice)
```

`web/src/app.js:169-171`:

```js
    if (done?.outcome === 'exists' || done?.outcome === 'elsewhere') {
      recorded = done.outcome
      return
    }
```

- [ ] **Step 4: Run to see them pass**

Run: `cd web && npm test`
Expected: PASS.

- [ ] **Step 5: DESIGN.md §15**

Add rows to the DOM contract table next to `[data-exists]`:

```
| `[data-held-elsewhere]` | the dry run found the Selection only in another member: `data-held-elsewhere` its address, `data-names` the composition's current names as JSON (decision 21) |
| `#compose-copy` | "Save a copy here": the write, then the name Claim superseding `name_claims` |
```

Wherever DESIGN lists the values of `data-write-outcome`, add `elsewhere`.

- [ ] **Step 6: Commit**

```bash
git add web/src web/test DESIGN.md
git commit -m "feat(explorer): save a copy of a Selection another member holds (ticket 09)"
```

---

### Task 4: Gate tier B, a read-only member, B14 and B15

**Files:**
- Modify: `gate/browser_b_assert.py` (`prepare`, `probe`, `evaluate`)
- Modify: `gate/browser/drive.mjs` (`EXTRACT_B`)
- Modify: `gate/browser/tier_b.sh` (header comment: assertions 8-15)
- Test: `gate/test_browser_b_assert.py`
- Modify: `gate/README.md` (tier B list after B13), `DESIGN.md` §16 tier B paragraph (around line 1283)

**Interfaces:**
- Consumes: the DOM contract from Task 3, the dry-run body from Task 1.
- Produces: `expected.json["shared"] = {"s3", "n_s", "s4", "n1", "n2"}` (addresses as strings); observed steps `B.copy` and `B.foreign`; `probed["foreign_dry"]`.

- [ ] **Step 1: Failing unit tests for the member writer**

In `gate/test_browser_b_assert.py`, add:

```python
class MemberWriteTest(unittest.TestCase):
    def test_a_block_and_its_log_entry_land_where_the_plugin_reads_them(self):
        root = tempfile.mkdtemp()
        try:
            block = {"kind": "Claim", "schema": 1, "asserted_by": "gate", "subject": cas.Cid(CID), "verb": "set",
                     "attribute": "name", "value": "x", "supersedes": [], "timestamp": "2026-01-01T00:00:00.000Z"}
            cid = BB._put_block(root, block)
            BB._log(root, "claim", cid, 1767225600000)
            store = cas.Store(root)
            self.assertEqual(store.read_block(cid), block)
            self.assertEqual(store.store_log(), [("%013d" % (9999999999999 - 1767225600000), "claim", cid)])
        finally:
            shutil.rmtree(root)
```

Use the module alias and the `CID` constant the file already has, and whatever class `browser_b_assert` already uses to open a store (the `store` object in `evaluate`); `cas.Store` above stands for that class.

Run: `python3 -m unittest gate.test_browser_b_assert` (or `cd gate && python3 -m unittest test_browser_b_assert`)
Expected: FAIL, `_put_block` not defined.

- [ ] **Step 2: Implement the writers**

In `browser_b_assert.py`, above `prepare`:

```python
SHARED_TS = "2026-01-01T00:00:00.000Z"
SHARED_MILLIS = 1767225600000
HORIZON_MILLIS = 9999999999999   # StoreLog.HORIZON_MILLIS


def _put_block(root, block):
    """Write the Gate's own encoding of `block` into a member directory, as a member stores it."""
    data = cas.encode(block)
    cid = cas.cid_dagcbor(data)
    path = os.path.join(root, "blocks", cid[-2:], cid)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)
    return cid


def _log(root, kind, cid, millis):
    """A Store Log entry: an empty file log/<13-digit reverse ts>-<kind>-<cid> (StoreLog.entryName)."""
    os.makedirs(os.path.join(root, "log"), exist_ok=True)
    open(os.path.join(root, "log", "%013d-%s-%s" % (HORIZON_MILLIS - millis, kind, cid)), "a").close()
```

If `cas.cid_dagcbor` returns a `cas.Cid` rather than text, take `.text`.

Run the unit test again. Expected: PASS.

- [ ] **Step 3: Build the read-only member in `prepare`**

After `a, b, c = ...`:

```python
    shared = os.path.join(out, "shared")
    os.makedirs(shared)

    def claim(subject, value, supersedes):
        return dagjson.expected_claim({"subject": cas.Cid(subject), "verb": "set", "attribute": "name", "value": value,
                                       "supersedes": [cas.Cid(s) for s in supersedes], "timestamp": SHARED_TS}, ASSERTED_BY)

    def selection(members):
        return dagjson.expected_selection({"kind": "Selection", "members": members, "derived_from": []}, ASSERTED_BY)

    # S3: what B.copy composes (A from cold's aligned), held only in shared, named there.
    s3 = _put_block(shared, selection([_item(a, [coll["aligned"]])]))
    _log(shared, "selection", s3, SHARED_MILLIS)
    n_s = _put_block(shared, claim(s3, "from-shared", []))
    _log(shared, "claim", n_s, SHARED_MILLIS)
    # S4: held and named in lab; shared renamed it, superseding lab's Claim.
    s4 = _put_block(store, selection([_item(c, [coll["stats"]])]))
    _log(store, "selection", s4, SHARED_MILLIS)
    n1 = _put_block(store, claim(s4, "lab-name", []))
    _log(store, "claim", n1, SHARED_MILLIS)
    n2 = _put_block(shared, claim(s4, "shared-name", [n1]))
    _log(shared, "claim", n2, SHARED_MILLIS)
```

Add `"shared": {"s3": s3, "n_s": n_s, "s4": s4, "n1": n1, "n2": n2}` to `expected`. In the explore config, change the `stores` line to:

```python
                 "cas {\n  stores {\n    lab { location = '%s' }\n    shared { location = '%s' }\n  }\n"
```

and pass `shared` after `store` in the format tuple. `lineage.store.location = 'cas://lab'` already makes `lab` the writable member.

Append two steps after `B.conflict`:

```python
        {"id": "B.copy", "server": "explore", "path": "", "query": q, "hash": "#/item/%s/%s" % (coll["aligned"], a),
         "actions": [{"click": "[data-pick]"}, {"hash": "#/compose"}, {"click": "#compose-save"}, {"waitWrite": True},
                     {"extract": "offer"}, {"click": "#compose-copy"}, {"waitWrite": True}, {"extract": "after"}]},
        {"id": "B.foreign", "server": "explore", "path": "", "query": q, "hash": "#/selection/%s" % s4,
         "actions": [{"extract": "before"}, {"fill": ["#rename-name", "lab-renamed"]}, {"click": "#rename-save"},
                     {"waitWrite": True}, {"extract": "after"}]},
```

- [ ] **Step 4: The driver reads the new DOM**

In `drive.mjs`'s `EXTRACT_B`, add:

```js
  composeName: document.getElementById('compose-name')?.value ?? null,
  heldElsewhere: document.querySelector('[data-held-elsewhere]')?.dataset.heldElsewhere ?? null,
  heldElsewhereNames: JSON.parse(document.querySelector('[data-held-elsewhere]')?.dataset.names ?? 'null'),
```

- [ ] **Step 5: The probe asks the endpoint for S4's composition-wide state**

In `probe`, beside B12's never-written Selection POST, send a dry run of S4's request, `{"kind": "Selection", "members": [item C via stats], "derived_from": []}`. Encode it the way that probe encodes its own request, POST it to `/api/put?dry_run=true` with the same token headers, and record `{"status", "body"}` under `"foreign_dry"` in the probe's output.

- [ ] **Step 6: B14 and B15 in `evaluate`**

Open the shared member with the same store class as `store` (call it `shared_store`), rooted at `browser-b/shared`. Then:

```python
    def b14():
        problems, sh = [], expected["shared"]
        offer = extract("B.copy", "offer")
        if offer.get("writeOutcome") != "elsewhere" or offer.get("heldElsewhere") != sh["s3"]:
            problems.append("the dry run offered %r for %r, expected elsewhere for S3 %s"
                            % (offer.get("writeOutcome"), offer.get("heldElsewhere"), sh["s3"]))
        if offer.get("composeName") != "from-shared":
            problems.append("the name field held %r, expected shared's name from-shared" % offer.get("composeName"))
        posts = _posts(seen("B.copy"))
        sels = [p for p in posts if _kind(p) == "Selection"]
        claims = [p for p in posts if _kind(p) == "Claim"]
        if len(sels) != 1 or len(claims) != 1:
            problems.append("B.copy: %d Selection and %d Claim POSTs that write, expected 1 and 1" % (len(sels), len(claims)))
        else:
            address, _block, found = verify_post("B.copy", sels[0], dagjson.expected_selection)
            problems += found
            if address != sh["s3"]:
                problems.append("B.copy wrote %s, the Selection shared holds is %s" % (address, sh["s3"]))
            _c, block, found = verify_post("B.copy", claims[0], dagjson.expected_claim)
            problems += found
            if block["value"] != "from-shared" or [_text(x) for x in block["supersedes"]] != [sh["n_s"]]:
                problems.append("the copy's name Claim is %r superseding %r, expected from-shared superseding %s"
                                % (block["value"], [_text(x) for x in block["supersedes"]], sh["n_s"]))
        if len([e for e in store.store_log() if e[1] == "selection" and e[2] == sh["s3"]]) != 1:
            problems.append("lab's log/ does not hold exactly one selection entry for S3")
        state = dagjson.claim_state(_claims_about(store, sh["s3"]) + _claims_about(shared_store, sh["s3"]))
        if state["names"] != ["from-shared"] or state["nameConflicted"]:
            problems.append("across both members S3 is named %r (conflicted %r), expected from-shared alone"
                            % (state["names"], state["nameConflicted"]))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("a Selection only shared held was offered as a copy with shared's name prefilled; one click wrote it "
                      "into lab at the Gate's address with a name Claim superseding shared's, one current name across both")

    def b15():
        problems, sh = [], expected["shared"]
        before = [(n["name"], n["claim"]) for n in extract("B.foreign", "before").get("names") or []]
        if before != [("lab-name", sh["n1"])]:
            problems.append("lab's view of S4 showed %r, expected lab-name from %s" % (before, sh["n1"]))
        claims = [p for p in _posts(seen("B.foreign")) if _kind(p) == "Claim"]
        if len(claims) != 1:
            problems.append("B.foreign: %d Claim POSTs that write, expected 1" % len(claims))
        else:
            _c, block, found = verify_post("B.foreign", claims[0], dagjson.expected_claim)
            problems += found
            if [_text(x) for x in block["supersedes"]] != [sh["n1"]]:
                problems.append("the rename supersedes %r, expected lab's %s" % ([_text(x) for x in block["supersedes"]], sh["n1"]))
        state = dagjson.claim_state(_claims_about(store, sh["s4"]) + _claims_about(shared_store, sh["s4"]))
        if sorted(state["names"]) != ["lab-renamed", "shared-name"] or not state["nameConflicted"]:
            problems.append("across both members S4 is named %r (conflicted %r), expected a conflict of lab-renamed and shared-name"
                            % (state["names"], state["nameConflicted"]))
        after = [(n["name"], n["conflicted"]) for n in extract("B.foreign", "after").get("names") or []]
        if after != [("lab-renamed", False)]:
            problems.append("lab's view after the rename showed %r, expected lab-renamed alone" % after)
        dry = probed().get("foreign_dry") or {}
        body = dagjson.loads(dry["body"]) if dry.get("status") == 200 else {}
        if body.get("here") is not True or len(body.get("names") or []) != 2:
            problems.append("the endpoint's dry run of S4 answered %s %r, expected here and two names" % (dry.get("status"), body))
        if problems:
            return FAIL, "; ".join(problems)
        return PASS, ("a rename in lab went through although shared holds a Claim superseding lab's; the composition "
                      "reports the two names as a conflict and lab's view shows its own")
```

Register them after B13: `run("B14", "a Selection held only in a read-only member is copied and named", b14)` and `run("B15", "a Claim in another member does not lock a rename; the disagreement is a conflict", b15)`, matching how B8-B13 are registered. `dagjson.claim_state`'s keys must match `claims.js`'s (`names`, `nameConflicted`). If they differ, use `dagjson`'s own names.

- [ ] **Step 7: Docs**

`gate/README.md`, after the B13 bullet:

```
- B14: a Selection that only the read-only member `shared` holds (built by the
  Gate, with a name Claim there) is offered as "Save a copy here" with its name
  prefilled; one click writes it into `lab` at the Gate's address, with one
  Store Log entry, and a name Claim superseding `shared`'s, so both members
  together show one current name.
- B15: `shared` holds a Claim superseding `lab`'s name for a Selection in `lab`;
  renaming it from the page still succeeds, the composition reports the two
  names as a conflict, and the endpoint's dry run answers `here` with both.
```

Update the tier B count wherever the README gives it. In DESIGN §16's tier B paragraph, add: "a read-only member's Selection is copied and named (14); a Claim in another member does not lock a rename (15)". In `tier_b.sh`'s header, change "assertions 8-13" to "assertions 8-15".

- [ ] **Step 8: Run the whole Gate**

Run: `python3 -m unittest discover -s gate` then `GATE_ROOT=<a scratch dir> make gate`
Expected: unit tests OK; lineage 11 PASS / 0 FAIL / 6 SKIP, tier A 5/5, tier B 8/8. If tier B fails, read `browser-b/drive.log`, `explore.log` and `observed.json` before changing anything.

- [ ] **Step 9: Commit**

```bash
git add gate DESIGN.md
git commit -m "test(gate): tier B with a read-only member: copy-and-name (B14), a foreign Claim does not lock a rename (B15)"
```

---

## After the last task

Run a whole-branch review, then `make gate` once more on the branch tip. Merge to `main` only when Rob says so. Then mark ticket 09's Answer "built" with the merge commit.
