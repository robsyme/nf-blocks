# Milestone 6 (retention) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a person get space back from a store without losing lineage: release a run's content, pin what must stay, prune by policy, and sweep dead blocks through a Trash ledger with a grace period, on local and S3 members, never beside a running pipeline.

**Architecture:** Claims gain two groups: `retain` (a run's content released by `set retain "lineage"`) and `pin` (a set of `add pin "<note>"`). A new `RetentionStorage` seam (local directory and S3, one contract test) holds `sweep.lock`, `live/<session>`, the Trash ledgers `trash/<deadline>-<sweep id>`, block listings with ages, and scratch. `SweepLock` and `LiveRegistry` build mutual exclusion on it; a run registers as a Live Writer and waits while a sweep holds the lock. `Mark` reads blocks (never the Index) from every member's Store Log roots; `Sweep` turns a mark into a dry-run report or an applied sweep (delete what is past its deadline and still dead, ledger what is newly dead). Three verbs (`sweep`, `prune`, `untrash`), `put` and the explorer gain the Claims, and Gate assertion 9 plus tier two T7 prove it from outside.

**Tech Stack:** Groovy 4 (`@CompileStatic`), Spock, Nextflow 26.04.6 plugin API, AWS SDK v2 through nf-amazon 3.9.2, the explorer page (plain JS, `node --test`), Python 3 Gate harness (unittest, stdlib only in tier one), bash.

**Spec:** the decisions this plan implements are the `## Answer` sections of
`../.scratch/post-gate/issues/20-retention-on-s3.md` (Retention on an S3 member),
`../.scratch/post-gate/issues/21-pins-as-content-roots.md` (Pins as content roots) and
`../.scratch/post-gate/issues/07-post-gate-cli-verbs.md` (verb rule 5, answers 2, 3 and 6),
summarised in `../.scratch/post-gate/roadmap.md` ("Milestone 6: retention"). Glossary:
`../CONTEXT.md` (Root, Pin, Prune, Sweep, Trash, Live Writer, Claim). Background:
`../.scratch/content-addressed-lineage/spec.md` §10, several of whose sentences tickets 20
and 21 supersede (the tickets win). Contract: `DESIGN.md`. Handoff with code pointers and
lessons: `../.scratch/post-gate/m6-handoff.md`.

**Branch:** create `feat/m6-retention` from `main` at `63dd384` (release 0.3.0-beta.1) before Task 1 (`git switch -c feat/m6-retention`). The version stays `0.3.0-beta.1` until the release after acceptance.

## Decisions this plan makes where the tickets are silent

Each is recorded in DESIGN.md §19 by Task 14. A reviewer may reject any of them.

1. **What a run's closure is, for the mark.** Metadata: the RunCompletion, its RunManifest, the RunManifest's `script` block, every OutputCollection and every OutputItem. Content: every Leaf address of every item and each collection's `index` Leaf, and for a Leaf that is a DirectoryManifest, the manifest and everything under it. A Directory Manifest is content, not metadata: it goes when the run's content is released.
2. **A pinned run keeps its whole closure**, metadata included, even when it is hidden by `delete` (ticket 21 answer 3 names "the subject's own metadata block"; keeping the collections and items too is what lets the page explain the pinned content). A pinned collection keeps itself, its RunManifest, its items and their content; a pinned item keeps itself and its content.
3. **A Selection with a clean `delete` is not a root**, the same rule as a run. A conflicted deletion keeps it. Its via collections are kept with their RunManifest (metadata only, block explorer spec section 7.3).
4. **Claims are live exactly when their subject is live** (glossary Store Log: "a Claim is listed but kept only while its subject is"), superseded ones included; they are small and are the history.
5. **Only `set retain "lineage"` releases.** A `set retain` with any other value keeps content, so a later value cannot be misread as consent by this build.
6. **A mark that cannot read a metadata block it needs refuses `--apply`** (exit 1, naming up to 20), because the blocks under it are unknown. A missing content block (raw or Directory Manifest) is reported, not refused: it is already gone, and what was under a gone manifest is protected only by other roots. A Store Log entry whose own block is in no member is dangling: not a root, reported, and deleted from the writable member's log by `--apply`.
7. **After deleting a block the sweep deletes its Store Log entries** in the writable member (a run, Selection or Claim entry naming it), then removes the index rows keyed by it (`Index.forget`) and rewrites the writable member's Index Snapshot, so a cold reader is not seeded with runs whose blocks are gone. Producer rows for released content stay: the lineage says who produced the bytes that were released.
8. **Trash timing.** One sweep writes at most one ledger, after its mark; nothing moves, so an interrupted sweep before that write changed nothing. The age floor and the grace period default to 14 days each (`cas.sweep.ageFloor`, `cas.sweep.grace`). The spec's "relationship validated in code" is: the age floor may not be under the Live Writer stale threshold (10 minutes), because a run whose heartbeat stopped counts as dead after that and may still reference what it wrote; the grace may be 0. Either under a day warns.
9. **`--budget <size>`** caps the bytes a sweep newly trashes (in address order). What it deletes was capped when it was trashed.
10. **Before each delete batch and before writing the ledger** the sweep re-checks: its lock (a failed heartbeat stops it), live registrations (a fresh one stops it: the lock is released and the run proceeds), and the Store Log of every member (entries written since the mark are marked into the live set, never out of it).
11. **A run that cannot write its registration warns and continues** (DESIGN §0 rule 3 applies to provenance; the age floor and the grace period still stand between a sweep and a block the run dedups onto). `put` and the explorer's write endpoint do not register (ticket 20 answer 7's readers, widened): a Claim or Selection written during a sweep is caught by the Store Log re-check unless it lands between the last re-check and a delete batch. Documented.
12. **`prune` reads the Index** (it only writes Claims; the sweep re-derives everything from blocks). With both `--keep-last` and `--keep-newer`, a run is kept when either keeps it. Without `--pipeline`, every Pipeline Identity is pruned separately. A run whose `retain` group is in conflict is skipped and reported; a pinned run is released as usual (its pins hold).
13. **`untrash`** takes the sweep lock, removes the named addresses (or a whole sweep with `--sweep <id>`) from the ledgers, and says that a block still unreachable is trashed again by the next sweep, so the way to keep it is a pin or `del retain`.
14. **Ledger format.** Not a block. JSON: `{"sweep": <id>, "trashed_at": <iso>, "deadline": <iso>, "blocks": [{"cid": <text>, "size": <int>}, ...]}`, blocks sorted by cid. Name `trash/<13-digit deadline epoch millis>-<sweep id>`, so a listing sorts by deadline. Sweep id `<yyyyMMdd'T'HHmmss'Z'>-<8 base32 chars>`.
15. **Lock body.** JSON `{"sweep": <id>, "state": "held"|"released", "started_at": <iso>, "beat": <int>}`. A registration body is JSON `{"session", "run_name", "pipeline", "started_at"}`.

## Global Constraints

- DESIGN §0 rule 1: released Nextflow 26.04.6 only; no `includeBuild`, no absolute paths in `build.gradle`.
- DESIGN §0 rule 2: memory bound; nothing here reads file content into memory. The mark reads metadata blocks (at most 1 MiB each, `Put.MAX_BLOCK_BYTES`; RunCompletions and manifests can be larger, so read with `readAllBytes` only for dag-cbor blocks, never raw ones).
- DESIGN §0 rule 3: a failure that would lose provenance throws `AbortRunException`; derived structures log and continue. A sweep failure never aborts a run; a run's registration failure warns.
- DESIGN §0 rule 5 as amended by ticket 07: the three verbs `sweep`, `prune`, `untrash` land here with Gate assertion 9; `sweep` and `prune` are dry runs unless `--apply`; `untrash` acts. `put` stays the only way to write a pin or a `retain` Claim from the command line.
- Only the writable member is ever swept (deleted from, ledgered, locked, registered in). Read-only members are read for roots and blocks.
- Nothing is swept that is not a block: `coords/`, `nf/`, `index/`, `index.html`, `log/` (except entries of deleted blocks, decision 7), `live/`, `trash/`, `sweep.lock`.
- A dry run takes no lock and writes nothing anywhere (no ledger, no deletion, no registration cleanup, no index change).
- Timestamps: the store's clock judges staleness and deadlines (S3: `LastModified` against the `Date` of a response from the same `S3Ops`; local: mtime against `System.currentTimeMillis()`).
- Constants, verbatim from ticket 20: heartbeat every 60 s, stale after 10 min, a waiting run polls every 30 s, `DeleteObjects` in batches of 1,000.
- The Claim rules have two implementations, `ClaimState.groovy` and `web/src/claims.js`, pinned by `web/test/fixtures/claim-vectors.json` (DESIGN §12 decision 5). Change all three together.
- Every new block field or Claim value that the page validates is in DESIGN.md §6's IPLD Schema (`web/schema-gen.mjs`). This plan adds no block kind and no field: Claims already allow any `attribute` and `value`, and `add` is already a `ClaimVerb`.
- All main classes `@CompileStatic`. Only `robsyme.cas.s3` imports `software.amazon.awssdk.*`. `robsyme.cas.core` imports nothing from Nextflow.
- Console output from a run goes through `robsyme.cas.trace.ConsoleLog.LOG` (logger `nextflow.cas`).
- The shell's `grep` is an ugrep alias that misbehaves, and `core/Records.groovy` holds a binary byte: use `/usr/bin/grep -a`.
- Subagents never search the whole filesystem (`find /`, `bfs /`, anything rooted at `/` or `~`) and stop every background process they start before reporting.
- Do not edit `../.scratch/content-addressed-lineage/test-pipeline/` or `gate/tier2/small/`.
- Every commit is `git commit -s` and its message ends with the trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (the steps' `-m` lines show the subject only; add the trailer).

## Review Focus

1. A block that two runs share, one released and one not, is never trashed; nor is a file inside a released run's directory that another run's directory also holds. (Task 6 tests `content shared with a kept run stays live` and `a file shared inside two directories stays live when one run is released`; Gate assertion 9 checks the shared `two.txt`.)
2. A Claim written after the mark began (a pin, a `del retain`, a new run) rescues the blocks it roots before the next delete batch. (Task 7 test `a pin written during the sweep rescues its content before the delete batch`.)
3. A `del retain` superseding the release, and nothing else, brings trashed content back: the next sweep drops it from the ledger and never deletes it, with no `untrash`. (Task 7 test `restored content is dropped from the ledger, not deleted`; Gate assertion 9.)
4. A sweep started while a run is registered refuses (or waits with `--wait`), and a run started while a sweep holds the lock waits and then proceeds; a stale registration or lock (over 10 minutes old by the store's clock, not the client's) is ignored. (Task 5 tests; Task 7 test `a registration that appears mid-sweep stops it`; Task 9 test; Gate assertion 9's live-registration check.)
5. Two sweepers on one S3 member: the one whose heartbeat meets a 412 stops before its next batch, and a released lock is taken over rather than refused. (Task 5 tests `a heartbeat that meets a takeover reports the lock lost` and `a released lock is taken, not refused`.)

---

### Task 1: Claim state learns `retain` and `pin`

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Claim.groovy` (constants)
- Modify: `src/main/groovy/robsyme/cas/core/ClaimState.groovy`
- Modify: `web/src/claims.js`
- Modify: `web/test/fixtures/claim-vectors.json` (append cases)
- Test: `src/test/groovy/robsyme/cas/core/ClaimStateTest.groovy`, `web/test/claims.test.mjs`

**Interfaces:**
- Produces: `Claim.RETAIN = 'retain'`, `Claim.PIN = 'pin'`, `Claim.LINEAGE = 'lineage'`.
- Produces on `ClaimState`: `static final String RELEASED = 'released'`; fields `final String retain` (`NONE`, `RELEASED` or `CONFLICTED`), `final List<String> retainClaims` (the current `retain` group, cid order), `final List<String> pinClaims` (current `add pin` claims, cid order), `final List<String> pinNotes` (their values as text, same order); methods `boolean isReleased()`, `boolean isPinned()`. A group that holds any `add` Claim (current or superseded) is never conflicted.
- Produces on `claimState(claims)` (JS): the same as `retain`, `retainClaims`, `pinClaims`, `pins` (array of `{cid, note}`), `pinned`, `released`.
- The vector file's `expect` may carry `retain`, `pins` (list of notes) and `pinned`; both test runners check them only when present, so the 14 existing cases are untouched.

- [ ] **Step 1: Append the vectors** (keep the file a JSON array; add these objects before its closing `]`)

```json
 {"name": "set retain lineage releases a run's content", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "retain", "value": "lineage", "supersedes": []}],
  "expect": {"current": ["c1"], "names": [], "nameConflicted": false, "deletion": "none", "retain": "released", "pins": [], "pinned": false}},
 {"name": "del retain superseding the release restores it", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "retain", "value": "lineage", "supersedes": []},
   {"cid": "c2", "verb": "del", "attribute": "retain", "value": null, "supersedes": ["c1"]}],
  "expect": {"current": ["c2"], "names": [], "nameConflicted": false, "deletion": "none", "retain": "none", "pins": [], "pinned": false}},
 {"name": "a release and a restore nothing orders conflict, and keep content", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "retain", "value": "lineage", "supersedes": []},
   {"cid": "c2", "verb": "set", "attribute": "retain", "value": "lineage", "supersedes": []},
   {"cid": "c3", "verb": "del", "attribute": "retain", "value": null, "supersedes": ["c1"]}],
  "expect": {"current": ["c2", "c3"], "names": [], "nameConflicted": false, "deletion": "none", "retain": "conflicted", "pins": [], "pinned": false}},
 {"name": "set retain with another value does not release", "claims": [
   {"cid": "c1", "verb": "set", "attribute": "retain", "value": "everything", "supersedes": []}],
  "expect": {"current": ["c1"], "names": [], "nameConflicted": false, "deletion": "none", "retain": "none", "pins": [], "pinned": false}},
 {"name": "two pins are a set, not a conflict", "claims": [
   {"cid": "c2", "verb": "add", "attribute": "pin", "value": "figure 3", "supersedes": []},
   {"cid": "c1", "verb": "add", "attribute": "pin", "value": "paper", "supersedes": []}],
  "expect": {"current": ["c1", "c2"], "names": [], "nameConflicted": false, "deletion": "none", "retain": "none", "pins": ["paper", "figure 3"], "pinned": true}},
 {"name": "del pin removes that one pin and the other stands", "claims": [
   {"cid": "c1", "verb": "add", "attribute": "pin", "value": "paper", "supersedes": []},
   {"cid": "c2", "verb": "add", "attribute": "pin", "value": "figure 3", "supersedes": []},
   {"cid": "c3", "verb": "del", "attribute": "pin", "value": null, "supersedes": ["c1"]}],
  "expect": {"current": ["c2", "c3"], "names": [], "nameConflicted": false, "deletion": "none", "retain": "none", "pins": ["figure 3"], "pinned": true}},
 {"name": "two del pins leave nothing pinned and no conflict", "claims": [
   {"cid": "c1", "verb": "add", "attribute": "pin", "value": "paper", "supersedes": []},
   {"cid": "c2", "verb": "add", "attribute": "pin", "value": "figure 3", "supersedes": []},
   {"cid": "c3", "verb": "del", "attribute": "pin", "value": null, "supersedes": ["c1"]},
   {"cid": "c4", "verb": "del", "attribute": "pin", "value": null, "supersedes": ["c2"]}],
  "expect": {"current": ["c3", "c4"], "names": [], "nameConflicted": false, "deletion": "none", "retain": "none", "pins": [], "pinned": false}},
 {"name": "a pin, a release and a delete are three separate groups", "claims": [
   {"cid": "c1", "verb": "add", "attribute": "pin", "value": "keep", "supersedes": []},
   {"cid": "c2", "verb": "set", "attribute": "retain", "value": "lineage", "supersedes": []},
   {"cid": "c3", "verb": "delete", "attribute": null, "value": null, "supersedes": []}],
  "expect": {"current": ["c1", "c2", "c3"], "names": [], "nameConflicted": false, "deletion": "deleted", "retain": "released", "pins": ["keep"], "pinned": true}}
```

- [ ] **Step 2: Check the new keys in both runners**

In `ClaimStateTest.groovy`, inside the `'#v.name'` feature's `then:` block, after `state.hidden == ...`:

```groovy
        !expect.containsKey('retain') || state.retain == expect.retain
        !expect.containsKey('pins') || state.pinNotes == expect.pins
        !expect.containsKey('pinned') || state.pinned == expect.pinned
        !expect.containsKey('retain') || state.released == (expect.retain == 'released')
```

and add a feature:

```groovy
    def 'a pin group is never flagged conflicted in claim_current'() {
        given:
        final ClaimState state = ClaimState.of([
            new ClaimState.Row('c1', 'add', 'pin', 'a', []),
            new ClaimState.Row('c2', 'add', 'pin', 'b', []),
            new ClaimState.Row('c3', 'set', 'retain', 'lineage', []),
            new ClaimState.Row('c4', 'set', 'retain', 'lineage', []),
        ])

        expect:
        state.currentRows()*.conflicted == [false, false, true, true]
        state.pinClaims == ['c1', 'c2']
        state.retainClaims == ['c3', 'c4']
        state.retain == ClaimState.CONFLICTED
    }
```

In `web/test/claims.test.mjs`, inside the loop's test after the `hidden` line:

```js
    if ('retain' in v.expect) {
      assert.equal(s.retain, v.expect.retain)
      assert.equal(s.released, v.expect.retain === 'released')
    }
    if ('pins' in v.expect) assert.deepEqual(s.pins.map(p => p.note), v.expect.pins)
    if ('pinned' in v.expect) assert.equal(s.pinned, v.expect.pinned)
```

- [ ] **Step 3: Run both to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.ClaimStateTest'` and `cd web && node --test test/claims.test.mjs`
Expected: FAIL (no `retain` property; the pin vectors show a conflict).

- [ ] **Step 4: Implement**

`Claim.groovy`, beside `NAME`:

```groovy
    static final String RETAIN = 'retain'
    static final String PIN = 'pin'
    /** The one `set retain` value that releases a run's content (ticket 21 answer 1). */
    static final String LINEAGE = 'lineage'
```

`ClaimState.groovy`: add `static final String RELEASED = 'released'` beside the other states, the fields, and replace the constructor and `of`:

```groovy
    final String retain
    final List<String> retainClaims
    final List<String> pinClaims
    final List<String> pinNotes

    private ClaimState(List<Row> current, Set<String> conflictedGroups) {
        // ... the existing name and deletion lines stay as they are ...
        final List<Row> retainGroup = current.findAll { Row r -> r.attribute == Claim.RETAIN }
        this.retainClaims = Collections.unmodifiableList(retainGroup*.cid)
        // Ticket 21 answers 1 and 4: only one clean `set retain "lineage"` releases; a conflict keeps content.
        this.retain = retainGroup.size() > 1 ? CONFLICTED
            : retainGroup.size() == 1 && retainGroup[0].verb == Claim.SET && retainGroup[0].value == Claim.LINEAGE ? RELEASED
            : NONE
        final List<Row> pins = current.findAll { Row r -> r.attribute == Claim.PIN && r.verb == Claim.ADD }
        this.pinClaims = Collections.unmodifiableList(pins*.cid)
        this.pinNotes = Collections.unmodifiableList(pins.collect { Row r -> String.valueOf(r.value) })
    }

    static ClaimState of(Collection<Row> claims) {
        final Set<String> superseded = new HashSet<String>()
        for( Row row : claims )
            superseded.addAll(row.supersedes ?: Collections.<String> emptyList())
        // Ticket 21 answer 2: a group that holds an `add` is a set; its current Claims never conflict.
        final Set<String> addGroups = new HashSet<String>()
        for( Row row : claims )
            if( row.verb == Claim.ADD )
                addGroups.add(groupOf(row))
        final List<Row> current = new ArrayList<Row>(claims.findAll { Row r -> !superseded.contains(r.cid) })
        current.sort { Row a, Row b -> a.cid <=> b.cid }
        final Map<String, Integer> sizes = new HashMap<String, Integer>()
        for( Row row : current )
            sizes.merge(groupOf(row), 1, { Integer a, Integer b -> a + b })
        final Set<String> conflicted = sizes.findAll { String k, Integer n -> n > 1 && !addGroups.contains(k) }.keySet()
        return new ClaimState(current, new HashSet<String>(conflicted))
    }

    boolean isReleased() { retain == RELEASED }

    boolean isPinned() { !pinClaims.isEmpty() }
```

Update the class comment's first sentence to name the two new groups.

`web/src/claims.js`: add `export const RELEASED = 'released'` and replace `claimState`:

```js
export function claimState(claims) {
  const superseded = new Set(claims.flatMap(c => c.supersedes ?? []))
  // Ticket 21 answer 2: a group that holds an `add` is a set; its current Claims never conflict.
  const addGroups = new Set(claims.filter(c => c.verb === 'add').map(groupOf))
  const current = claims.filter(c => !superseded.has(c.cid)).sort(byCid)
  const sizes = new Map()
  for (const c of current) sizes.set(groupOf(c), (sizes.get(groupOf(c)) ?? 0) + 1)
  const conflicted = (c) => sizes.get(groupOf(c)) > 1 && !addGroups.has(groupOf(c))
  const nameGroup = current.filter(c => c.attribute === 'name')
  const deletionGroup = current.filter(c => groupOf(c) === DELETION && (c.verb === 'delete' || c.verb === 'del'))
  const deletion = deletionGroup.length > 1 ? CONFLICTED
    : deletionGroup.length === 1 && deletionGroup[0].verb === 'delete' ? DELETED : NONE
  const retainGroup = current.filter(c => c.attribute === 'retain')
  const retain = retainGroup.length > 1 ? CONFLICTED
    : retainGroup.length === 1 && retainGroup[0].verb === 'set' && retainGroup[0].value === 'lineage' ? RELEASED : NONE
  const pins = current.filter(c => c.attribute === 'pin' && c.verb === 'add').map(c => ({ cid: c.cid, note: String(c.value) }))
  return {
    current: current.map(c => ({ ...c, conflicted: conflicted(c) })),
    names: nameGroup.filter(c => c.verb === 'set').map(c => String(c.value)),
    nameClaims: nameGroup.map(c => c.cid),
    nameConflicted: nameGroup.length > 1,
    deletion,
    deletionClaims: deletionGroup.map(c => c.cid),
    hidden: deletion === DELETED,
    retain,
    retainClaims: retainGroup.map(c => c.cid),
    released: retain === RELEASED,
    pins,
    pinClaims: pins.map(p => p.cid),
    pinned: pins.length > 0,
  }
}
```

The page reads a Claim's `value` through `model.js`'s value decoding (a string as itself); the `'lineage'` comparison therefore sees the plain string.

- [ ] **Step 5: Run both to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.core.ClaimStateTest' --tests 'robsyme.cas.core.IndexTest'` and `cd web && node --test`
Expected: PASS, all 22 vectors on both sides; the index and page suites unchanged.

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Claim.groovy src/main/groovy/robsyme/cas/core/ClaimState.groovy \
  web/src/claims.js web/test/fixtures/claim-vectors.json \
  src/test/groovy/robsyme/cas/core/ClaimStateTest.groovy web/test/claims.test.mjs
git commit -s -m "feat(claims): retain and pin groups; add groups never conflict (ticket 21)"
```

---

### Task 2: `put` accepts `retain` and `pin` Claims

**Files:**
- Modify: `src/main/groovy/robsyme/cas/core/Put.groovy` (`CLAIM_VERBS`, `claimDraft`, `validateClaim`, the dry-run return)
- Modify: `src/main/groovy/robsyme/cas/core/PutResult.groovy` (dry-run fields and body)
- Modify: `web/src/write.js` (four request builders; the response decoding of the new claim lists)
- Test: `src/test/groovy/robsyme/cas/core/PutTest.groovy`, `web/test/write.test.mjs`

**Interfaces:**
- Consumes: Task 1's `Claim.RETAIN`, `Claim.PIN`, `Claim.LINEAGE`, `ClaimState.retain/retainClaims/pinClaims/pinNotes`.
- Produces: `Put` accepts, and only these new shapes:
  - `set retain "lineage"`, subject a RunCompletion;
  - `del retain`, subject a RunCompletion, every superseded Claim a `retain` Claim on that subject;
  - `add pin "<note>"`, note a non-blank string of at most `Put.MAX_NOTE_CHARS = 256` characters, subject a RunCompletion, OutputCollection, OutputItem, DirectoryManifest or raw block held by the composition, supersedes empty;
  - `del pin`, same subjects, every superseded Claim an `add pin` on that subject.
  - Refused with `invalid`: `add` with any attribute but `pin`; `set pin`; `set retain` with any value but `"lineage"`; `add pin` with supersedes. Refused with `wrong_kind`: a `retain` subject that is not a RunCompletion, a `pin` subject of another kind, a `del retain`/`del pin` superseding a Claim of another attribute or verb.
- Produces: `static PutResult dryRun(Cid address, boolean exists, boolean here, ClaimState state)` (replaces the seven-argument form; its one caller is `Put`). The dry-run body gains `retain` (string), `retain_claims` (links), `pin_claims` (links) and `pins` (notes, strings), after the existing keys.
- Produces (JS): `writer(...)` gains `release(subject, retainClaims)`, `restore(subject, retainClaims)`, `pin(subject, note)`, `unpin(subject, pinClaim)`; the decoded dry-run response carries `retain_claims` and `pin_claims` as strings.

- [ ] **Step 1: Write the failing tests** (append to `PutTest.groovy`, reusing its existing fixture helpers for a store holding one run; name them as the file already names its run, collection, item and a raw leaf; read the file's `setup()` first)

```groovy
    def 'set retain lineage on a run is written and releases it'() {
        when:
        final PutResult r = put.put(claimRequest(run, 'set', 'retain', 'lineage', []), false)

        then:
        r.written
        index.claimState(run).released
    }

    def 'del retain restores, superseding the release'() {
        given:
        final Cid release = put.put(claimRequest(run, 'set', 'retain', 'lineage', []), false).address

        when:
        put.put(claimRequest(run, 'del', 'retain', null, [release]), false)

        then:
        index.claimState(run).retain == ClaimState.NONE
    }

    def 'retain is refused on anything but a RunCompletion'() {
        when:
        put.put(claimRequest(item, 'set', 'retain', 'lineage', []), false)

        then:
        final PutError e = thrown()
        e.code == PutError.WRONG_KIND
        e.at == '/subject'
    }

    def 'set retain takes only the value lineage'() {
        when:
        put.put(claimRequest(run, 'set', 'retain', 'all', []), false)

        then:
        final PutError e = thrown()
        e.code == PutError.INVALID
        e.at == '/value'
    }

    def 'add pin with a note is written on #what, and pins it'() {
        when:
        put.put(claimRequest(subject, 'add', 'pin', 'figure 3', []), false)

        then:
        index.claimState(subject).pinNotes == ['figure 3']

        where:
        what         | subject
        'a run'      | run
        'an item'    | item
        'a raw leaf' | leaf
    }

    def 'two pins on one subject are a set'() {
        when:
        put.put(claimRequest(item, 'add', 'pin', 'paper', []), false)
        put.put(claimRequest(item, 'add', 'pin', 'figure 3', []), false)

        then:
        index.claimState(item).pinNotes.sort() == ['figure 3', 'paper']
        !index.claimState(item).currentRows().any { it.conflicted }
    }

    def 'del pin removes one pin'() {
        given:
        final Cid one = put.put(claimRequest(item, 'add', 'pin', 'paper', []), false).address
        put.put(claimRequest(item, 'add', 'pin', 'figure 3', []), false)

        when:
        put.put(claimRequest(item, 'del', 'pin', null, [one]), false)

        then:
        index.claimState(item).pinNotes == ['figure 3']
    }

    def '#shape is refused as #code at #at'() {
        when:
        put.put(request, false)

        then:
        final PutError e = thrown()
        e.code == code
        e.at == at

        where:
        shape                         | request                                                  | code               | at
        'add of another attribute'    | claimRequest(item, 'add', 'name', 'x', [])               | PutError.INVALID   | '/attribute'
        'set pin'                     | claimRequest(item, 'set', 'pin', 'x', [])                | PutError.INVALID   | '/attribute'
        'a blank note'                | claimRequest(item, 'add', 'pin', '  ', [])               | PutError.INVALID   | '/value'
        'a note that is not a string' | claimRequest(item, 'add', 'pin', 3L, [])                 | PutError.INVALID   | '/value'
        'a 257-character note'        | claimRequest(item, 'add', 'pin', 'x' * 257, [])          | PutError.INVALID   | '/value'
        'add pin with supersedes'     | claimRequest(item, 'add', 'pin', 'x', [run])             | PutError.INVALID   | '/supersedes'
        'a pin on a Selection'        | claimRequest(selection, 'add', 'pin', 'x', [])           | PutError.WRONG_KIND | '/subject'
    }

    def 'del pin superseding a name Claim is refused'() {
        given:
        final Cid name = put.put(claimRequest(item, 'set', 'name', 'n', []), false).address

        when:
        put.put(claimRequest(item, 'del', 'pin', null, [name]), false)

        then:
        final PutError e = thrown()
        e.code == PutError.WRONG_KIND
        e.at == '/supersedes/0'
    }

    def 'a dry run reports the retain and pin groups'() {
        given:
        final Cid release = put.put(claimRequest(run, 'set', 'retain', 'lineage', []), false).address
        final Cid pin = put.put(claimRequest(run, 'add', 'pin', 'paper', []), false).address

        when:
        final Map body = (Map) DagJson.decode(put.put(claimRequest(run, 'del', 'retain', null, [release]), true).body())

        then: 'the dry run is about the would-be Claim, so its own state is empty; ask about the run instead'
        body.retain == 'none'

        when:
        final PutResult about = PutResult.dryRun(run, true, true, index.claimState(run))
        final Map b = (Map) DagJson.decode(about.body())

        then:
        b.retain == 'released'
        b.retain_claims == [release]
        b.pin_claims == [pin]
        b.pins == ['paper']
    }
```

`claimRequest(subject, verb, attribute, value, supersedes)` builds the DAG-JSON-shaped Map `{kind: 'Claim', subject, verb, attribute, value, supersedes, timestamp: Index.isoMillis(clockNow)}`; if `PutTest` has no such helper, add it as a private method using the test's clock. `selection` is a Selection the fixture writes; if the fixture has none, write one in the feature's `given:` through `put.put(...)` with the item as its member.

Append to `web/test/write.test.mjs` (follow its existing `fetchFn` capture pattern):

```js
test('release, restore, pin and unpin send the Claims put accepts', async () => {
  const sent = []
  const w = writer({ endpoint: '/api/put', token: 't', now: () => new Date('2026-09-30T10:00:00.000Z'),
    fetchFn: async (url, init) => { sent.push(dagJson.decode(init.body)); return okResponse({ address: CID.parse(RUN), written: true }) } })
  await w.release(RUN, [])
  await w.restore(RUN, [CLAIM])
  await w.pin(ITEM, 'figure 3')
  await w.unpin(ITEM, CLAIM)
  assert.deepEqual(sent.map(r => [r.verb, r.attribute, r.value, r.supersedes.map(String)]), [
    ['set', 'retain', 'lineage', []],
    ['del', 'retain', null, [CLAIM]],
    ['add', 'pin', 'figure 3', []],
    ['del', 'pin', null, [CLAIM]],
  ])
})
```

(`RUN`, `ITEM`, `CLAIM` are valid CID strings; `okResponse` is the file's helper for a DAG-JSON 200, or write one beside the test.)

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.PutTest'` and `cd web && node --test test/write.test.mjs`
Expected: FAIL (`verb 'add' is not built yet`; no `release` on the writer).

- [ ] **Step 3: Implement**

`Put.groovy`:

```groovy
    static final int MAX_NOTE_CHARS = 256
    private static final Set<String> CLAIM_VERBS = [Claim.SET, Claim.ADD, Claim.DELETE, Claim.DEL] as Set
    /** What a pin may name (ticket 21 answer 6); a raw block is any held raw address. */
    private static final Set<String> PINNABLE = [Records.RUN_COMPLETION, Records.OUTPUT_COLLECTION,
        Records.OUTPUT_ITEM, Records.DIRECTORY_MANIFEST] as Set
```

In `claimDraft`, delete the `if( verb == Claim.ADD ) throw ...` refusal, change the verb message to `'verb is one of set, add, delete, del'`, and extend the `switch`:

```groovy
            case Claim.SET:
                if( attribute == null ) throw invalid('set needs an attribute', '/attribute')
                if( value == null ) throw invalid('set needs a value', '/value')
                if( attribute == Claim.NAME && !(value instanceof String && ((String) value).trim() && ((String) value).length() <= MAX_NAME_CHARS) )
                    throw invalid("a name is a non-blank string of at most ${MAX_NAME_CHARS} characters", '/value')
                if( attribute == Claim.PIN )
                    throw invalid('a pin is add pin "<note>", never set', '/attribute')
                if( attribute == Claim.RETAIN && value != Claim.LINEAGE )
                    throw invalid('set retain takes the value "lineage" (keep the lineage, release the content)', '/value')
                break
            case Claim.ADD:
                if( attribute != Claim.PIN ) throw invalid('add is for pins only: add pin "<note>"', '/attribute')
                if( !(value instanceof String && ((String) value).trim() && ((String) value).length() <= MAX_NOTE_CHARS) )
                    throw invalid("a pin's note is a non-blank string of at most ${MAX_NOTE_CHARS} characters", '/value')
                if( !supersedes.isEmpty() ) throw invalid('add pin supersedes nothing; pins on one subject are a set', '/supersedes')
                break
```

(`DELETE` and `DEL` cases stay.) In `validateClaim`, after the clock check and before the supersedes loop:

```groovy
        if( claim.attribute == Claim.RETAIN && kindAt(claim.subject, '/subject') != Records.RUN_COMPLETION )
            throw new PutError(PutError.WRONG_KIND, "retain names a run's RunCompletion; ${claim.subject} is ${describe(kindAt(claim.subject, '/subject'))}", '/subject')
        if( claim.attribute == Claim.PIN && !claim.subject.isRaw() && !(kindAt(claim.subject, '/subject') in PINNABLE) )
            throw new PutError(PutError.WRONG_KIND, "a pin names a run, collection, item, directory or file; ${claim.subject} is ${describe(kindAt(claim.subject, '/subject'))}", '/subject')
        if( claim.attribute == Claim.PIN && claim.subject.isRaw() && !store.has(claim.subject) )
            throw new PutError(PutError.NOT_FOUND, "${claim.subject} is not in any member of this composition", '/subject')
```

and inside the supersedes loop, after the `subject` check:

```groovy
            if( claim.verb == Claim.DEL && claim.attribute in [Claim.RETAIN, Claim.PIN] ) {
                final boolean fits = block.get('attribute') == claim.attribute &&
                    (claim.attribute == Claim.RETAIN || block.get('verb') == Claim.ADD)
                if( !fits )
                    throw new PutError(PutError.WRONG_KIND, "claim ${s} is not ${claim.attribute == Claim.PIN ? 'an add pin' : 'a retain'} Claim", at)
            }
```

The dry-run return becomes:

```groovy
        if( dryRun )
            return PutResult.dryRun(address, store.has(address), writable.has(address), index.claimState(address))
```

`PutResult.groovy`: store `final ClaimState state` in place of the four name/deletion fields' sources, keep the existing public fields (`names`, `nameClaims`, `deletion`, `deletionClaims`, read by `CasCommands.nameSelection`) filled from it, and add `retain`, `retainClaims`, `pinClaims`, `pinNotes`:

```groovy
    static PutResult dryRun(Cid address, boolean exists, boolean here, ClaimState state) {
        return new PutResult(address, true, exists, here, state.names, state.nameClaims.collect { String c -> Cid.parse(c) },
            state.deletion, state.deletionClaims.collect { String c -> Cid.parse(c) },
            state.retain, state.retainClaims.collect { String c -> Cid.parse(c) },
            state.pinClaims.collect { String c -> Cid.parse(c) }, state.pinNotes, null, null, false)
    }
```

(extend the private constructor with the four new parameters after `deletionClaims`; `written(...)` passes nulls for them.) In `body()`, inside `if( dryRun )`, after `deletion_claims`:

```groovy
            out.put('retain', retain)
            out.put('retain_claims', retainClaims)
            out.put('pin_claims', pinClaims)
            out.put('pins', pinNotes)
```

`web/src/write.js`: in `post`, decode the new lists as the existing two are:

```js
        ...(body.retain_claims ? { retain_claims: body.retain_claims.map(String) } : {}),
        ...(body.pin_claims ? { pin_claims: body.pin_claims.map(String) } : {}),
```

and add to the returned object:

```js
    release: (subject, supersedes) => claim(subject, 'set', 'retain', 'lineage', supersedes),
    restore: (subject, supersedes) => claim(subject, 'del', 'retain', null, supersedes),
    pin: (subject, note) => claim(subject, 'add', 'pin', note, []),
    unpin: (subject, pinClaim) => claim(subject, 'del', 'pin', null, [pinClaim]),
```

`release` takes the current `retainClaims` so a release after a restore supersedes the `del` instead of conflicting with it.

- [ ] **Step 4: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.core.PutTest' --tests 'robsyme.cas.cli.*' --tests 'robsyme.cas.explore.*'` and `cd web && node --test`
Expected: PASS; the explore server's `POST /api/put` tests unchanged (they go through the same `Put`).

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Put.groovy src/main/groovy/robsyme/cas/core/PutResult.groovy \
  web/src/write.js src/test/groovy/robsyme/cas/core/PutTest.groovy web/test/write.test.mjs
git commit -s -m "feat(put): set/del retain on a run, add/del pin on a run, collection, item, directory or file (ticket 21 answer 6)"
```

---

### Task 3: The S3 seam lists ages, deletes in batches and lists open uploads

**Files:**
- Modify: `src/main/groovy/robsyme/cas/s3/S3Ops.groovy`, `S3Types.groovy` (`S3Listed`; new `S3Upload`), `SdkS3Ops.groovy`
- Modify: `src/test/groovy/robsyme/cas/s3/MemoryS3Ops.groovy`
- Test: `src/test/groovy/robsyme/cas/s3/MemoryS3OpsTest.groovy`, `src/test/groovy/robsyme/cas/s3/SdkS3OpsTest.groovy` (create if absent; else add to the existing SDK-level test that uses a stubbed `S3Client`)

**Interfaces:**
- Produces: `S3Listed` gains `long lastModifiedMillis` as its third property (the two-argument constructor still compiles: `@Canonical` gives defaults).
- Produces on `S3Ops`:
  - `Long lastServerDateMillis()`: the `Date` header of the latest response this instance saw (null before any).
  - `List<String> deleteMany(List<String> keys)`: one `DeleteObjects` for at most 1,000 keys, quiet mode; returns the keys S3 reported as errors. More than 1,000 is an `IllegalArgumentException`.
  - `List<S3Upload> listUploads(String prefix)`: every incomplete multipart upload under `prefix`, paginated.
- Produces: `@Canonical class S3Upload { String key; String uploadId; long initiatedMillis }` in `S3Types.groovy`.
- Produces on `MemoryS3Ops`: `lastServerDateMillis()` returns `serverDateMillis` when set, else the fake's clock; `void advance(long millis)` moves the fake's clock; `list` fills `lastModifiedMillis`; `createMultipart` records an initiation time; `listUploads` and `deleteMany` behave as S3 does.

- [ ] **Step 1: Write the failing tests** (append to `MemoryS3OpsTest.groovy`)

```groovy
    def 'a listing carries each object LastModified, and the clock is the latest Date'() {
        given:
        final ops = new MemoryS3Ops('b')
        ops.putText('p/a', 'x')
        ops.advance(60_000L)
        ops.putText('p/b', 'y')

        when:
        final List<S3Listed> listed = ops.list('p/', 0)

        then:
        listed*.key == ['p/a', 'p/b']
        listed[1].lastModifiedMillis - listed[0].lastModifiedMillis >= 60_000L
        ops.lastServerDateMillis() >= listed[1].lastModifiedMillis
    }

    def 'deleteMany removes up to a thousand keys and refuses more'() {
        given:
        final ops = new MemoryS3Ops('b')
        (1..3).each { ops.putText("p/${it}", 'x') }

        expect:
        ops.deleteMany(['p/1', 'p/3', 'p/missing']) == []
        ops.objects.keySet() == ['p/2'] as Set

        when:
        ops.deleteMany((1..1001).collect { "k${it}".toString() })

        then:
        thrown(IllegalArgumentException)
    }

    def 'an open multipart upload is listed until it completes or is aborted'() {
        given:
        final ops = new MemoryS3Ops('b')
        final String open = ops.createMultipart('p/tmp/x', S3PutOptions.create())
        final String done = ops.createMultipart('p/blocks/aa/y', S3PutOptions.create())
        ops.abortMultipart('p/blocks/aa/y', done)

        expect:
        ops.listUploads('p/')*.uploadId == [open]
        ops.listUploads('p/')[0].key == 'p/tmp/x'
        ops.listUploads('q/') == []
    }
```

For `SdkS3Ops`, add a unit test against a stubbed `S3Client` (the existing SDK tests show how one is built; if there is none, use Spock's `Stub(S3Client)`):

```groovy
    def 'deleteMany sends one quiet DeleteObjects and returns the keys that failed'() {
        given:
        final S3Client client = Mock()
        final ops = new SdkS3Ops(client, 'b', S3WriteOptions.NONE)

        when:
        final List<String> failed = ops.deleteMany(['k1', 'k2'])

        then:
        1 * client.deleteObjects({ DeleteObjectsRequest r ->
            r.bucket() == 'b' && r.delete().quiet() && r.delete().objects()*.key() == ['k1', 'k2'] }) >>
            DeleteObjectsResponse.builder().errors(S3Error.builder().key('k2').code('AccessDenied').build()).build()
        failed == ['k2']
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.s3.MemoryS3OpsTest' --tests 'robsyme.cas.s3.SdkS3OpsTest'`
Expected: FAIL, compilation (`advance`, `deleteMany`, `listUploads`, `lastModifiedMillis` unknown).

- [ ] **Step 3: Implement**

`S3Types.groovy`:

```groovy
@Canonical
@CompileStatic
class S3Listed {
    String key
    long size
    long lastModifiedMillis
}

/** An incomplete multipart upload (ticket 20 answer 3: the sweep aborts those older than the age floor). */
@Canonical
@CompileStatic
class S3Upload {
    String key
    String uploadId
    long initiatedMillis
}
```

`S3Ops.groovy`, after `firstServerDateMillis()`:

```groovy
    /** The Date header of the latest response this instance saw, in epoch millis; null before one. The store's clock (ticket 20 answer 6). */
    Long lastServerDateMillis()

    /** One DeleteObjects of at most 1,000 keys; the keys S3 could not delete. A missing key is not an error. */
    List<String> deleteMany(List<String> keys)

    /** Every incomplete multipart upload whose key starts with prefix. */
    List<S3Upload> listUploads(String prefix)
```

`SdkS3Ops.groovy`: keep a second `AtomicReference<Long> lastDate`; `note(SdkHttpResponse)` sets it on every response that carries a parseable `Date` (and still sets `firstDate` once). `list` fills `o.lastModified()?.toEpochMilli() ?: 0L`. Then:

```groovy
    @Override
    Long lastServerDateMillis() { lastDate.get() }

    @Override
    List<String> deleteMany(List<String> keys) {
        if( keys.size() > 1000 )
            throw new IllegalArgumentException("DeleteObjects takes at most 1,000 keys, got ${keys.size()}")
        if( keys.isEmpty() )
            return []
        final DeleteObjectsResponse r = client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucket).requestPayer(payer())
            .delete(Delete.builder().quiet(true).objects(keys.collect { String k -> ObjectIdentifier.builder().key(k).build() }).build())
            .build())
        note(r)
        return (r.errors() ?: Collections.<S3Error> emptyList()).collect { S3Error e -> e.key() }
    }

    @Override
    List<S3Upload> listUploads(String prefix) {
        final List<S3Upload> out = new ArrayList<S3Upload>()
        for( ListMultipartUploadsResponse page : client.listMultipartUploadsPaginator(
                ListMultipartUploadsRequest.builder().bucket(bucket).prefix(prefix).requestPayer(payer()).build()) ) {
            note(page)
            for( MultipartUpload u : page.uploads() )
                out.add(new S3Upload(u.key(), u.uploadId(), u.initiated()?.toEpochMilli() ?: 0L))
        }
        return out
    }
```

`MemoryS3Ops.groovy`:

```groovy
    private final Map<String, S3Upload> openUploads = new TreeMap<>()

    void advance(long millis) { clock += millis }

    @Override Long lastServerDateMillis() { serverDateMillis ?: clock }

    @Override List<String> deleteMany(List<String> keys) {
        if( keys.size() > 1000 ) throw new IllegalArgumentException("DeleteObjects takes at most 1,000 keys, got ${keys.size()}")
        calls << "DELETEMANY ${keys.size()}".toString()
        keys.each { objects.remove(it) }
        return []
    }

    @Override List<S3Upload> listUploads(String prefix) {
        calls << "LISTUPLOADS ${prefix}".toString()
        return openUploads.values().findAll { it.key.startsWith(prefix) }.toList()
    }
```

`createMultipart` adds `openUploads[id] = new S3Upload(key, id, ++clock)`; `completeMultipart` and `abortMultipart` remove `openUploads[uploadId]`; `list` builds `new S3Listed(k, (long) v.bytes.length, v.lastModified)`.

- [ ] **Step 4: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.s3.*'`
Expected: PASS, every existing S3 test unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/s3/S3Ops.groovy src/main/groovy/robsyme/cas/s3/S3Types.groovy \
  src/main/groovy/robsyme/cas/s3/SdkS3Ops.groovy src/test/groovy/robsyme/cas/s3/
git commit -s -m "feat(s3): listings carry LastModified; DeleteObjects batches; open multipart uploads; the latest Date"
```

---

### Task 4: `RetentionStorage`, local and S3, under one contract

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/RetentionStorage.groovy` (interface plus the small value classes `Versioned`, `Stamped`, `BlockStat`)
- Create: `src/main/groovy/robsyme/cas/core/RetainedStore.groovy` (`interface RetainedStore { RetentionStorage retentionStorage() }`, as `LoggedStore` is for the Store Log)
- Create: `src/main/groovy/robsyme/cas/core/LocalRetentionStorage.groovy`
- Create: `src/main/groovy/robsyme/cas/s3/S3RetentionStorage.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/LocalBlockStore.groovy`, `src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy` (implement `RetainedStore`)
- Test: create `src/test/groovy/robsyme/cas/core/RetentionStorageContract.groovy` (abstract), `LocalRetentionStorageTest.groovy`, `src/test/groovy/robsyme/cas/s3/S3RetentionStorageTest.groovy`

**Interfaces:**
- Consumes: Task 3's `S3Ops.lastServerDateMillis`, `deleteMany`, `listUploads`, `S3Listed.lastModifiedMillis`.
- Produces (all in `robsyme.cas.core`):

```groovy
@CompileStatic
interface RetentionStorage {
    /** The store's clock as of its latest answer: S3's Date header, a local directory's System.currentTimeMillis(). */
    long nowMillis()

    /** sweep.lock, or null when absent. */
    Versioned readLock()
    /** Creates sweep.lock; its version, or null when a lock object already exists (S3 If-None-Match: *; local create-exclusive). */
    String createLock(byte[] body)
    /** Replaces sweep.lock only while it is still `version`; the new version, or null when it is not (S3 If-Match; local compare-and-move). */
    String replaceLock(String version, byte[] body)

    /** live/<session>, created or rewritten (a rewrite is the heartbeat). */
    void putLive(String session, byte[] body)
    void deleteLive(String session)
    /** A registration's bytes, or null when absent. */
    byte[] readLive(String session)
    /** Every registration, name = the session, with its LastModified. */
    List<Stamped> listLive()

    /** Names under trash/, ascending. */
    List<String> listLedgers()
    /** A ledger's bytes, or null when absent. */
    byte[] readLedger(String name)
    void writeLedger(String name, byte[] body)
    void deleteLedger(String name)

    /** Every block of this member with its size and LastModified. */
    List<BlockStat> listBlockStats()
    /** Deletes the blocks; returns those that could not be deleted. A block already gone is not a failure. */
    List<Cid> deleteBlocks(Collection<Cid> cids)

    /** Upload scratch: S3 tmp/ keys and open multipart uploads under the prefix; local blocks/.tmp-* and blocks/<xx>/.tmp-*. */
    List<Stamped> listScratch()
    void deleteScratch(Stamped scratch)

    /** Removes one Store Log entry by name (decision 7). Absent is success. */
    void deleteLogEntry(String name)

    String describe()
}

@Canonical @CompileStatic class Versioned { byte[] body; String version; long lastModifiedMillis }
@Canonical @CompileStatic class Stamped { String name; long lastModifiedMillis; String token }  // token: an S3 upload id, else null
@Canonical @CompileStatic class BlockStat { Cid cid; long size; long lastModifiedMillis }
```

- Produces: `LocalBlockStore.retentionStorage()` → `new LocalRetentionStorage(root)`; `S3BlockStore.retentionStorage()` → `new S3RetentionStorage(ops, prefix)`.

Layout, the same on both backends under the member root or prefix: `sweep.lock`, `live/<session>`, `trash/<name>`. Local `version` is the lowercase hex SHA-256 of the lock body (every body differs: the `beat` counter moves); S3 `version` is the ETag.

- [ ] **Step 1: Write the contract** (`RetentionStorageContract.groovy`)

```groovy
package robsyme.cas.core

import spock.lang.Specification

/** One contract for every RetentionStorage (as CoordinateTreeContract is for coordinate trees). */
abstract class RetentionStorageContract extends Specification {

    /** A fresh, empty member. */
    abstract RetentionStorage storage()
    /** Writes a block into that member through its own BlockStore, returning its cid. */
    abstract Cid writeBlock(byte[] bytes)
    /** Leaves one scratch object in that member. */
    abstract void leaveScratch()
    /** Writes one Store Log entry by name. */
    abstract void writeLogEntry(String name)
    abstract boolean logEntryExists(String name)

    def 'a lock is created once, replaced only at its version, and read back'() {
        given:
        final RetentionStorage s = storage()

        expect:
        s.readLock() == null

        when:
        final String v1 = s.createLock('one'.bytes)

        then:
        v1 != null
        s.createLock('two'.bytes) == null
        new String(s.readLock().body) == 'one'
        s.readLock().version == v1

        when:
        final String v2 = s.replaceLock(v1, 'three'.bytes)

        then:
        v2 != null && v2 != v1
        s.replaceLock(v1, 'four'.bytes) == null
        new String(s.readLock().body) == 'three'
        s.readLock().lastModifiedMillis > 0
    }

    def 'registrations are written, rewritten, listed and deleted'() {
        given:
        final RetentionStorage s = storage()

        when:
        s.putLive('s1', '{"session":"s1"}'.bytes)
        s.putLive('s2', '{}'.bytes)
        s.putLive('s1', '{"session":"s1"}'.bytes)

        then:
        s.listLive()*.name.sort() == ['s1', 's2']
        new String(s.readLive('s1')) == '{"session":"s1"}'
        s.readLive('absent') == null
        s.listLive().every { it.lastModifiedMillis > 0 }

        when:
        s.deleteLive('s1')
        s.deleteLive('absent')

        then:
        s.listLive()*.name == ['s2']
    }

    def 'ledgers are listed in name order, read, replaced and deleted'() {
        given:
        final RetentionStorage s = storage()

        when:
        s.writeLedger('0000000000002-b', 'b'.bytes)
        s.writeLedger('0000000000001-a', 'a'.bytes)
        s.writeLedger('0000000000002-b', 'b2'.bytes)

        then:
        s.listLedgers() == ['0000000000001-a', '0000000000002-b']
        new String(s.readLedger('0000000000002-b')) == 'b2'
        s.readLedger('absent') == null

        when:
        s.deleteLedger('0000000000001-a')

        then:
        s.listLedgers() == ['0000000000002-b']
    }

    def 'blocks are listed with sizes and ages, and deleted'() {
        given:
        final RetentionStorage s = storage()
        final Cid a = writeBlock('alpha'.bytes)
        final Cid b = writeBlock('bravo!'.bytes)

        expect:
        s.listBlockStats().collectEntries { [(it.cid): it.size] } == [(a): 5L, (b): 6L]
        s.listBlockStats().every { it.lastModifiedMillis > 0 }

        when:
        final List<Cid> failed = s.deleteBlocks([a, Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')])

        then:
        failed == []
        s.listBlockStats()*.cid == [b]
    }

    def 'scratch is listed and deleted, and a log entry is deleted by name'() {
        given:
        final RetentionStorage s = storage()
        leaveScratch()
        writeLogEntry('0000000000001-claim-bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')

        when:
        final List<Stamped> scratch = s.listScratch()

        then:
        scratch.size() == 1

        when:
        s.deleteScratch(scratch[0])
        s.deleteLogEntry('0000000000001-claim-bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
        s.deleteLogEntry('absent')

        then:
        s.listScratch() == []
        !logEntryExists('0000000000001-claim-bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
    }

    def 'the store clock is a real time'() {
        given:
        final RetentionStorage s = storage()
        s.listLive()

        expect:
        s.nowMillis() > 1_700_000_000_000L
    }
}
```

`LocalRetentionStorageTest extends RetentionStorageContract` with a `@TempDir Path root`, a `LocalBlockStore(root, 'lab', true)`, `leaveScratch()` writing `root/blocks/.tmp-x`, and log entries under `root/log/`. `S3RetentionStorageTest extends RetentionStorageContract` over `MemoryS3Ops('b')` with prefix `'m/'`, an `S3BlockStore(ops, 'm/', 'lab', true, tmpDir)`, `leaveScratch()` calling `ops.createMultipart('m/tmp/x', S3PutOptions.create())`, and log entries as `ops.putText('m/log/' + name, '')`. Add to `S3RetentionStorageTest`:

```groovy
    def 'a lock is taken with If-None-Match and replaced with If-Match'() {
        when:
        final String v = storage().createLock('x'.bytes)
        storage().replaceLock(v, 'y'.bytes)

        then:
        ops.calls.findAll { it.startsWith('PUT m/sweep.lock') }.size() == 2
    }

    def 'scratch holds tmp keys and open uploads, and an upload is aborted, not deleted'() {
        given:
        ops.putText('m/tmp/stage-1', 'x')
        final String id = ops.createMultipart('m/blocks/aa/y', S3PutOptions.create())

        when:
        final List<Stamped> scratch = storage().listScratch()
        scratch.each { storage().deleteScratch(it) }

        then:
        scratch*.name.sort() == ['m/blocks/aa/y', 'm/tmp/stage-1']
        scratch.find { it.token == id } != null
        ops.calls.contains('ABORT m/blocks/aa/y')
        ops.listUploads('m/') == []
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.LocalRetentionStorageTest' --tests 'robsyme.cas.s3.S3RetentionStorageTest'`
Expected: FAIL, compilation.

- [ ] **Step 3: Implement `LocalRetentionStorage`**

```groovy
package robsyme.cas.core

import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.security.MessageDigest

import groovy.transform.CompileStatic

/**
 * Retention objects in a local member (ticket 20 answer 5): sweep.lock by
 * create-exclusive and compare-then-move, live/<session> rewritten to
 * heartbeat, trash/<name> ledgers. Ages are mtimes against this host's clock,
 * which is the clock that wrote them.
 */
@CompileStatic
class LocalRetentionStorage implements RetentionStorage {

    private final Path root
    private final LocalBlockStore blocks

    LocalRetentionStorage(Path root) {
        this.root = root
        this.blocks = new LocalBlockStore(root, 'retention', true)
    }

    @Override long nowMillis() { System.currentTimeMillis() }

    private Path lock() { root.resolve('sweep.lock') }

    @Override
    Versioned readLock() {
        try {
            final byte[] body = Files.readAllBytes(lock())
            return new Versioned(body, sha(body), Files.getLastModifiedTime(lock()).toMillis())
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    @Override
    String createLock(byte[] body) {
        try {
            Files.write(lock(), body, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.SYNC)
            return sha(body)
        }
        catch( FileAlreadyExistsException e ) {
            return null
        }
    }

    /**
     * Compare, then move a temp file over the lock. Two local processes can
     * still both pass the compare; the loser learns at its next heartbeat,
     * which compares again, and a sweep heartbeats before every batch (plan
     * decision 10). One writer per host is the common case.
     */
    @Override
    synchronized String replaceLock(String version, byte[] body) {
        final Versioned current = readLock()
        if( current == null || current.version != version )
            return null
        final Path temp = root.resolve(".sweep.lock-${UUID.randomUUID()}")
        Files.write(temp, body, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.SYNC)
        try {
            Files.move(temp, lock(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }
        catch( AtomicMoveNotSupportedException e ) {
            Files.move(temp, lock(), StandardCopyOption.REPLACE_EXISTING)
        }
        return sha(body)
    }

    @Override
    void putLive(String session, byte[] body) {
        final Path dir = root.resolve('live')
        Files.createDirectories(dir)
        Files.write(dir.resolve(session), body)
    }

    @Override void deleteLive(String session) { Files.deleteIfExists(root.resolve('live').resolve(session)) }

    @Override
    byte[] readLive(String session) {
        try {
            return Files.readAllBytes(root.resolve('live').resolve(session))
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    @Override List<Stamped> listLive() { stamped(root.resolve('live')) }

    @Override
    List<String> listLedgers() {
        final Path dir = root.resolve('trash')
        if( !Files.isDirectory(dir) )
            return []
        return Files.list(dir).withCloseable { s -> s.map { Path p -> p.fileName.toString() }.filter { String n -> !n.startsWith('.') }.sorted().toList() }
    }

    @Override
    byte[] readLedger(String name) {
        try {
            return Files.readAllBytes(root.resolve('trash').resolve(name))
        }
        catch( NoSuchFileException e ) {
            return null
        }
    }

    @Override
    void writeLedger(String name, byte[] body) {
        final Path dir = root.resolve('trash')
        Files.createDirectories(dir)
        final Path temp = dir.resolve(".${name}-${UUID.randomUUID()}")
        Files.write(temp, body, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.SYNC)
        Files.move(temp, dir.resolve(name), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    }

    @Override void deleteLedger(String name) { Files.deleteIfExists(root.resolve('trash').resolve(name)) }

    @Override
    List<BlockStat> listBlockStats() {
        return blocks.listBlocks().withCloseable { s ->
            s.map { Cid c -> new BlockStat(c, Files.size(blocks.blockPath(c)), Files.getLastModifiedTime(blocks.blockPath(c)).toMillis()) }.toList()
        }
    }

    @Override
    List<Cid> deleteBlocks(Collection<Cid> cids) {
        final List<Cid> failed = new ArrayList<Cid>()
        for( Cid c : cids ) {
            try {
                Files.deleteIfExists(blocks.blockPath(c))
            }
            catch( IOException e ) {
                failed.add(c)
            }
        }
        return failed
    }

    @Override
    List<Stamped> listScratch() {
        final Path dir = root.resolve('blocks')
        if( !Files.isDirectory(dir) )
            return []
        return Files.walk(dir, 2).withCloseable { s ->
            s.filter { Path p -> p.fileName.toString().startsWith('.tmp-') && Files.isRegularFile(p) }
             .map { Path p -> new Stamped(root.relativize(p).toString(), Files.getLastModifiedTime(p).toMillis(), null) }
             .toList()
        }
    }

    @Override void deleteScratch(Stamped scratch) { Files.deleteIfExists(root.resolve(scratch.name)) }

    @Override void deleteLogEntry(String name) { Files.deleteIfExists(root.resolve('log').resolve(name)) }

    @Override String describe() { root.toString() }

    private static List<Stamped> stamped(Path dir) {
        if( !Files.isDirectory(dir) )
            return []
        return Files.list(dir).withCloseable { s ->
            s.filter { Path p -> Files.isRegularFile(p) }
             .map { Path p -> new Stamped(p.fileName.toString(), Files.getLastModifiedTime(p).toMillis(), null) }
             .toList()
        }
    }

    private static String sha(byte[] body) {
        return MessageDigest.getInstance('SHA-256').digest(body).encodeHex().toString()
    }
}
```

(If `Files.list(...).withCloseable` does not type-check under `@CompileStatic`, use try/finally around the `Stream`; the behaviour is what matters.) `LocalBlockStore` implements `RetainedStore` with `RetentionStorage retentionStorage() { new LocalRetentionStorage(root) }`.

- [ ] **Step 4: Implement `S3RetentionStorage`**

```groovy
package robsyme.cas.s3

import groovy.transform.CompileStatic
import robsyme.cas.core.BlockStat
import robsyme.cas.core.Cid
import robsyme.cas.core.RetentionStorage
import robsyme.cas.core.Stamped
import robsyme.cas.core.Versioned

/**
 * Retention objects in an S3 member (ticket 20 answer 5): the lock by
 * conditional PUTs (If-None-Match to take, If-Match to heartbeat and take
 * over), registrations by plain PUT, ledgers as objects. The clock is S3's
 * Date header of the latest response (answer 6).
 */
@CompileStatic
class S3RetentionStorage implements RetentionStorage {

    static final String JSON = 'application/json'

    private final S3Ops ops
    private final String prefix

    S3RetentionStorage(S3Ops ops, String prefix) {
        this.ops = ops
        this.prefix = prefix ?: ''
    }

    @Override
    long nowMillis() {
        Long date = ops.lastServerDateMillis()
        if( date == null ) {
            ops.head(prefix + 'sweep.lock')
            date = ops.lastServerDateMillis()
        }
        return date ?: System.currentTimeMillis()
    }

    @Override
    Versioned readLock() {
        for( int attempt = 0; attempt < 3; attempt++ ) {
            final S3Head head = ops.head(prefix + 'sweep.lock')
            if( head == null )
                return null
            try {
                final InputStream in = ops.get(prefix + 'sweep.lock', head.etag, 0L, -1L)
                if( in == null )
                    return null
                final byte[] body = in.withCloseable { InputStream s -> s.readAllBytes() }
                return new Versioned(body, head.etag, head.lastModifiedMillis)
            }
            catch( S3PreconditionFailed e ) {
                // Replaced between the HEAD and the GET: read again.
            }
        }
        throw new IOException("sweep.lock at ${ops.describe()}/${prefix} kept changing while it was read")
    }

    @Override
    String createLock(byte[] body) {
        final S3Written w = ops.put(prefix + 'sweep.lock', S3Body.ofBytes(body), S3PutOptions.create().ifNoneMatch().contentType(JSON))
        return w.status == S3Written.Status.WRITTEN ? w.etag : null
    }

    @Override
    String replaceLock(String version, byte[] body) {
        final S3Written w = ops.put(prefix + 'sweep.lock', S3Body.ofBytes(body), S3PutOptions.create().ifMatch(version).contentType(JSON))
        return w.status == S3Written.Status.WRITTEN ? w.etag : null
    }

    @Override
    void putLive(String session, byte[] body) {
        ops.put(prefix + 'live/' + session, S3Body.ofBytes(body), S3PutOptions.create().contentType(JSON))
    }

    @Override void deleteLive(String session) { ops.delete(prefix + 'live/' + session) }

    @Override
    byte[] readLive(String session) {
        final InputStream in = ops.get(prefix + 'live/' + session, null, 0L, -1L)
        return in == null ? null : in.withCloseable { InputStream s -> s.readAllBytes() }
    }

    @Override
    List<Stamped> listLive() {
        final String under = prefix + 'live/'
        return ops.list(under, 0).collect { S3Listed o -> new Stamped(o.key.substring(under.length()), o.lastModifiedMillis, null) }
    }

    @Override
    List<String> listLedgers() {
        final String under = prefix + 'trash/'
        return ops.list(under, 0).collect { S3Listed o -> o.key.substring(under.length()) }.sort()
    }

    @Override
    byte[] readLedger(String name) {
        final InputStream in = ops.get(prefix + 'trash/' + name, null, 0L, -1L)
        return in == null ? null : in.withCloseable { InputStream s -> s.readAllBytes() }
    }

    @Override
    void writeLedger(String name, byte[] body) {
        ops.put(prefix + 'trash/' + name, S3Body.ofBytes(body), S3PutOptions.create().contentType(JSON))
    }

    @Override void deleteLedger(String name) { ops.delete(prefix + 'trash/' + name) }

    @Override
    List<BlockStat> listBlockStats() {
        final List<BlockStat> out = new ArrayList<BlockStat>()
        for( S3Listed o : ops.list(prefix + 'blocks/', 0) ) {
            final String name = o.key.substring(o.key.lastIndexOf('/') + 1)
            if( Cid.isCid(name) )
                out.add(new BlockStat(Cid.parse(name), o.size, o.lastModifiedMillis))
        }
        return out
    }

    @Override
    List<Cid> deleteBlocks(Collection<Cid> cids) {
        final List<Cid> all = new ArrayList<Cid>(cids)
        final List<Cid> failed = new ArrayList<Cid>()
        for( int from = 0; from < all.size(); from += 1000 ) {
            final List<Cid> batch = all.subList(from, Math.min(all.size(), from + 1000))
            final Map<String, Cid> byKey = batch.collectEntries { Cid c -> [(keyOf(c)): c] }
            for( String key : ops.deleteMany(new ArrayList<String>(byKey.keySet())) )
                failed.add(byKey.get(key))
        }
        return failed
    }

    @Override
    List<Stamped> listScratch() {
        final List<Stamped> out = new ArrayList<Stamped>()
        for( S3Listed o : ops.list(prefix + 'tmp/', 0) )
            out.add(new Stamped(o.key, o.lastModifiedMillis, null))
        for( S3Upload u : ops.listUploads(prefix) )
            out.add(new Stamped(u.key, u.initiatedMillis, u.uploadId))
        return out
    }

    @Override
    void deleteScratch(Stamped scratch) {
        if( scratch.token != null )
            ops.abortMultipart(scratch.name, scratch.token)
        else
            ops.delete(scratch.name)
    }

    @Override void deleteLogEntry(String name) { ops.delete(prefix + 'log/' + name) }

    @Override String describe() { "${ops.describe()}/${prefix}".toString() }

    private String keyOf(Cid cid) {
        final String text = cid.toString()
        return "${prefix}blocks/${text.substring(text.length() - 2)}/${text}".toString()
    }
}
```

`S3BlockStore` implements `RetainedStore` with `RetentionStorage retentionStorage() { new S3RetentionStorage(ops, prefix) }`.

- [ ] **Step 5: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.core.LocalRetentionStorageTest' --tests 'robsyme.cas.s3.S3RetentionStorageTest' --tests 'robsyme.cas.core.LocalBlockStoreTest' --tests 'robsyme.cas.s3.S3BlockStoreTest'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/RetentionStorage.groovy src/main/groovy/robsyme/cas/core/RetainedStore.groovy \
  src/main/groovy/robsyme/cas/core/LocalRetentionStorage.groovy src/main/groovy/robsyme/cas/s3/S3RetentionStorage.groovy \
  src/main/groovy/robsyme/cas/core/LocalBlockStore.groovy src/main/groovy/robsyme/cas/s3/S3BlockStore.groovy \
  src/test/groovy/robsyme/cas/core/RetentionStorageContract.groovy src/test/groovy/robsyme/cas/core/LocalRetentionStorageTest.groovy \
  src/test/groovy/robsyme/cas/s3/S3RetentionStorageTest.groovy
git commit -s -m "feat(retention): RetentionStorage for sweep.lock, live/, trash/ and block ages, local and S3 (ticket 20 answer 5)"
```

---

### Task 5: The sweep lock, live registrations and the Live Writer

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/SweepLock.groovy`
- Create: `src/main/groovy/robsyme/cas/core/LiveRegistry.groovy`
- Create: `src/main/groovy/robsyme/cas/core/LiveWriter.groovy`
- Test: `src/test/groovy/robsyme/cas/core/SweepLockTest.groovy`, `LiveRegistryTest.groovy`, `LiveWriterTest.groovy`, and `src/test/groovy/robsyme/cas/s3/SweepLockS3Test.groovy`

**Interfaces:**
- Consumes: Task 4's `RetentionStorage`, `Versioned`, `Stamped`.
- Produces:

```groovy
class SweepLock {
    static final long HEARTBEAT_MILLIS = 60_000L
    static final long STALE_MILLIS = 600_000L
    SweepLock(RetentionStorage storage, String sweepId, Closure<Long> localClock)
    /** Takes the lock, taking over a released or stale one. Null on success, else the fresh holder. */
    Holder take()
    /** Rewrites the lock at our version. False when someone took it over: stop now. */
    boolean heartbeat()
    /** Writes a released body at our version. Idempotent; a lock taken over is left alone. */
    void release()
    boolean isHeld()
    /** The fresh holder of the lock, or null when it is absent, released or stale. What a run checks. */
    static Holder holder(RetentionStorage storage)
    static String newSweepId(long nowMillis)
}
@Canonical static class SweepLock.Holder { String sweepId; String startedAt; long ageMillis }   // nested in SweepLock

class LiveRegistry {
    LiveRegistry(RetentionStorage storage)
    /** Registrations whose last heartbeat is at most STALE_MILLIS old by the store's clock. */
    List<Registration> fresh()
    List<Registration> stale()
    void deleteStale()
}
@Canonical static class LiveRegistry.Registration { String session; long ageMillis; String runName; String pipeline }   // nested

class LiveWriter implements Closeable {
    static final long POLL_MILLIS = 30_000L
    LiveWriter(RetentionStorage storage, String session, Map<String, String> info, Closure<Void> say, Closure<Void> sleeper, ScheduledExecutorService heartbeats)
    /** Registers, starts the heartbeat, then waits while a fresh sweep lock is held. Never throws: a failure warns through `say`. */
    void start()
    /** Stops the heartbeat and deletes the registration. Idempotent. */
    void close()
}
```

Lock body is decision 15's JSON, built and parsed with `groovy.json.JsonOutput`/`JsonSlurper`. Staleness is `storage.nowMillis() - lastModifiedMillis > STALE_MILLIS`, read after the request that returned the `lastModifiedMillis` (the listing or the HEAD), so the two times come from one clock.

- [ ] **Step 1: Write the failing tests**

`SweepLockTest.groovy` (local storage over a `@TempDir`; staleness is made by backdating the file's mtime with `Files.setLastModifiedTime`):

```groovy
    def 'an absent lock is taken, heartbeats, and is released'() {
        given:
        final lock = new SweepLock(storage, 'sweep-1', { -> System.currentTimeMillis() })

        expect:
        lock.take() == null
        lock.held
        lock.heartbeat()
        SweepLock.holder(storage).sweepId == 'sweep-1'

        when:
        lock.release()

        then:
        !lock.held
        SweepLock.holder(storage) == null
        new JsonSlurper().parse(storage.readLock().body).state == 'released'
    }

    def 'a fresh lock held by another sweep is refused, naming it'() {
        given:
        new SweepLock(storage, 'first', clock).take()

        when:
        final Holder h = new SweepLock(storage, 'second', clock).take()

        then:
        h.sweepId == 'first'
        h.ageMillis >= 0
    }

    def 'a released lock is taken, not refused'() {
        given:
        final first = new SweepLock(storage, 'first', clock)
        first.take()
        first.release()

        expect:
        new SweepLock(storage, 'second', clock).take() == null
        SweepLock.holder(storage).sweepId == 'second'
    }

    def 'a lock whose heartbeat stopped over ten minutes ago is taken over'() {
        given:
        new SweepLock(storage, 'crashed', clock).take()
        Files.setLastModifiedTime(root.resolve('sweep.lock'), FileTime.fromMillis(System.currentTimeMillis() - 601_000L))

        expect:
        SweepLock.holder(storage) == null
        new SweepLock(storage, 'next', clock).take() == null
        SweepLock.holder(storage).sweepId == 'next'
    }

    def 'a heartbeat that meets a takeover reports the lock lost'() {
        given:
        final slow = new SweepLock(storage, 'slow', clock)
        slow.take()
        Files.setLastModifiedTime(root.resolve('sweep.lock'), FileTime.fromMillis(System.currentTimeMillis() - 601_000L))
        new SweepLock(storage, 'fast', clock).take()

        expect:
        !slow.heartbeat()
        !slow.held

        when: 'a lost lock is not released over its new holder'
        slow.release()

        then:
        SweepLock.holder(storage).sweepId == 'fast'
    }

    def 'sweep ids sort by time and are unique'() {
        expect:
        SweepLock.newSweepId(1_790_000_000_000L) ==~ /20260921T\d{6}Z-[a-z2-7]{8}/
        SweepLock.newSweepId(1L) != SweepLock.newSweepId(1L)
    }
```

`SweepLockS3Test.groovy`: the same take/heartbeat/release, fresh-refusal and released-takeover features over `S3RetentionStorage(MemoryS3Ops('b'), 'm/')`, staleness made with `ops.advance(601_000L)` after the lock is written; plus:

```groovy
    def 'a stale S3 lock is judged by S3 Date, not this machine clock'() {
        given:
        new SweepLock(storage, 'first', { -> 0L }).take()   // a client clock far in the past changes nothing
        ops.advance(599_000L)

        expect: 'nine minutes 59 seconds by S3: still fresh'
        SweepLock.holder(storage).sweepId == 'first'

        when:
        ops.advance(2_000L)

        then:
        SweepLock.holder(storage) == null
    }
```

`LiveRegistryTest.groovy`:

```groovy
    def 'registrations are fresh until ten minutes without a heartbeat, then stale and deletable'() {
        given:
        storage.putLive('s1', JsonOutput.toJson([session: 's1', run_name: 'happy_turing', pipeline: 'p', started_at: 'x']).bytes)
        storage.putLive('s2', '{}'.bytes)
        Files.setLastModifiedTime(root.resolve('live/s2'), FileTime.fromMillis(System.currentTimeMillis() - 601_000L))
        final registry = new LiveRegistry(storage)

        expect:
        registry.fresh()*.session == ['s1']
        registry.fresh()[0].runName == 'happy_turing'
        registry.stale()*.session == ['s2']

        when:
        registry.deleteStale()

        then:
        storage.listLive()*.name == ['s1']
    }
```

`LiveWriterTest.groovy` (a real single-thread `ScheduledExecutorService`, shut down in `cleanup()`; `sleeper` records calls and releases the lock on its second call):

```groovy
    def 'a run registers, waits while a fresh lock is held, then proceeds; close deregisters'() {
        given:
        final sweep = new SweepLock(storage, 'sweep-1', clock)
        sweep.take()
        final List<String> said = []
        int sleeps = 0
        final writer = new LiveWriter(storage, 'sess', [run_name: 'r', pipeline: 'p', started_at: 't'],
            { String m -> said << m }, { long ms -> if( ++sleeps == 2 ) sweep.release() }, executor)

        when:
        writer.start()

        then:
        storage.listLive()*.name == ['sess']
        sleeps == 2
        said.size() == 1
        said[0].contains('sweep-1')
        said[0].contains('waiting')

        when:
        writer.close()
        writer.close()

        then:
        storage.listLive() == []
    }

    def 'no lock, no wait'() {
        given:
        int sleeps = 0
        final writer = new LiveWriter(storage, 'sess', [:], { String m -> }, { long ms -> sleeps++ }, executor)

        when:
        writer.start()

        then:
        sleeps == 0
        storage.listLive()*.name == ['sess']

        cleanup:
        writer.close()
    }

    def 'a registration that cannot be written warns and the run goes on'() {
        given:
        final RetentionStorage broken = Stub(RetentionStorage) { putLive(_, _) >> { throw new IOException('denied') } }
        final List<String> said = []

        when:
        new LiveWriter(broken, 'sess', [:], { String m -> said << m }, { long ms -> }, executor).start()

        then:
        notThrown(Exception)
        said.any { it.contains('could not register') && it.contains('denied') }
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.SweepLockTest' --tests 'robsyme.cas.core.LiveRegistryTest' --tests 'robsyme.cas.core.LiveWriterTest' --tests 'robsyme.cas.s3.SweepLockS3Test'`
Expected: FAIL, compilation.

- [ ] **Step 3: Implement `SweepLock`**

```groovy
package robsyme.cas.core

import java.security.SecureRandom
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * The store-wide sweep lock (ticket 20 answers 5 and 6): sweep.lock under the
 * writable member, taken create-if-absent, heartbeated and taken over at its
 * version, released by a `released` body. Stale after 10 minutes without a
 * heartbeat, by the store's clock.
 */
@CompileStatic
class SweepLock {

    static final long HEARTBEAT_MILLIS = 60_000L
    static final long STALE_MILLIS = 600_000L
    private static final SecureRandom RANDOM = new SecureRandom()
    private static final DateTimeFormatter ID_TIME = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)

    @Canonical
    @CompileStatic
    static class Holder {
        String sweepId
        String startedAt
        long ageMillis
    }

    private final RetentionStorage storage
    private final String sweepId
    private final Closure<Long> localClock
    private String version
    private int beat
    private String startedAt

    SweepLock(RetentionStorage storage, String sweepId, Closure<Long> localClock) {
        this.storage = storage
        this.sweepId = sweepId
        this.localClock = localClock
    }

    static String newSweepId(long nowMillis) {
        final byte[] bytes = new byte[5]
        RANDOM.nextBytes(bytes)
        return ID_TIME.format(Instant.ofEpochMilli(nowMillis)) + '-' + Multibase.base32Encode(bytes).substring(0, 8)
    }

    synchronized boolean isHeld() { version != null }

    synchronized Holder take() {
        startedAt = Index.isoMillis(localClock.call())
        beat = 0
        final String created = storage.createLock(body('held'))
        if( created != null ) {
            version = created
            return null
        }
        final Versioned current = storage.readLock()
        if( current == null )
            return take()            // released and deleted by hand between the two calls
        final Holder h = holderOf(current, storage.nowMillis())
        if( h != null )
            return h
        version = storage.replaceLock(current.version, body('held'))
        if( version != null )
            return null
        final Versioned now = storage.readLock()
        return now == null ? new Holder('unknown', null, 0L) : (holderOf(now, storage.nowMillis()) ?: new Holder('unknown', null, 0L))
    }

    synchronized boolean heartbeat() {
        if( version == null )
            return false
        beat++
        version = storage.replaceLock(version, body('held'))
        return version != null
    }

    synchronized void release() {
        if( version == null )
            return
        storage.replaceLock(version, body('released'))
        version = null
    }

    static Holder holder(RetentionStorage storage) {
        final Versioned current = storage.readLock()
        return current == null ? null : holderOf(current, storage.nowMillis())
    }

    /** The holder when the lock is held and fresh; null when released or stale. */
    private static Holder holderOf(Versioned lock, long nowMillis) {
        Map parsed
        try {
            parsed = (Map) new JsonSlurper().parse(lock.body)
        }
        catch( Exception e ) {
            parsed = [:]    // unreadable: judged by age alone
        }
        final long age = nowMillis - lock.lastModifiedMillis
        if( parsed.state == 'released' || age > STALE_MILLIS )
            return null
        return new Holder(String.valueOf(parsed.sweep ?: 'unknown'), parsed.started_at as String, age)
    }

    private byte[] body(String state) {
        return JsonOutput.toJson([sweep: sweepId, state: state, started_at: startedAt, beat: beat]).getBytes('UTF-8')
    }
}
```

(`Multibase.base32Encode` is the existing lowercase RFC 4648 base32; `Index.isoMillis` formats ISO-8601 UTC with milliseconds. If `core` must not reach for `Index` here, copy its two-line formatter.)

- [ ] **Step 4: Implement `LiveRegistry` and `LiveWriter`**

```groovy
package robsyme.cas.core

import groovy.json.JsonSlurper
import groovy.transform.Canonical
import groovy.transform.CompileStatic

/** The live/ registrations of a member, judged by the store's clock (ticket 20 answer 6). */
@CompileStatic
class LiveRegistry {

    @Canonical
    @CompileStatic
    static class Registration {
        String session
        long ageMillis
        String runName
        String pipeline
    }

    private final RetentionStorage storage

    LiveRegistry(RetentionStorage storage) { this.storage = storage }

    List<Registration> fresh() { all().findAll { Registration r -> r.ageMillis <= SweepLock.STALE_MILLIS } }

    List<Registration> stale() { all().findAll { Registration r -> r.ageMillis > SweepLock.STALE_MILLIS } }

    void deleteStale() {
        for( Registration r : stale() )
            storage.deleteLive(r.session)
    }

    private List<Registration> all() {
        final List<Stamped> listed = storage.listLive()
        final long now = storage.nowMillis()
        return listed.collect { Stamped s -> new Registration(s.name, now - s.lastModifiedMillis, null, null) }
            .sort { Registration r -> r.session }
    }
}
```

`fresh()` fills `runName` and `pipeline` from `storage.readLive(session)` (Task 4), parsed with `JsonSlurper` inside a try/catch that leaves them null on an absent or unreadable body; `stale()` does not read bodies.

```groovy
package robsyme.cas.core

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

import groovy.json.JsonOutput
import groovy.transform.CompileStatic

/**
 * A run as a Live Writer (ticket 20 answers 1 and 5): live/<session> is written
 * first, then sweep.lock is read, waiting while a fresh one is held, so a sweep
 * and a run always see each other. Heartbeat every 60 s by plain PUT; deleted
 * at the end. Never fails the run (plan decision 11).
 */
@CompileStatic
class LiveWriter implements Closeable {

    static final long POLL_MILLIS = 30_000L

    private final RetentionStorage storage
    private final String session
    private final byte[] body
    private final Closure<Void> say
    private final Closure<Void> sleeper
    private final ScheduledExecutorService heartbeats
    private volatile ScheduledFuture<?> beating
    private volatile boolean registered

    LiveWriter(RetentionStorage storage, String session, Map<String, String> info, Closure<Void> say,
               Closure<Void> sleeper, ScheduledExecutorService heartbeats) {
        this.storage = storage
        this.session = session
        final Map<String, String> content = new LinkedHashMap<String, String>(info)
        content.put('session', session)
        this.body = JsonOutput.toJson(content).getBytes('UTF-8')
        this.say = say
        this.sleeper = sleeper
        this.heartbeats = heartbeats
    }

    void start() {
        try {
            storage.putLive(session, body)
            registered = true
        }
        catch( Exception e ) {
            say.call("nf-blocks could not register this run in ${storage.describe()}/live/ (${e.message}); a sweep started now would not wait for it".toString())
            return
        }
        beating = heartbeats.scheduleAtFixedRate({ -> beat() } as Runnable,
            SweepLock.HEARTBEAT_MILLIS, SweepLock.HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
        boolean told = false
        while( true ) {
            final SweepLock.Holder h
            try {
                h = SweepLock.holder(storage)
            }
            catch( Exception e ) {
                say.call("nf-blocks could not read ${storage.describe()}/sweep.lock (${e.message}); not waiting".toString())
                return
            }
            if( h == null )
                return
            if( !told ) {
                say.call("sweep ${h.sweepId} holds ${storage.describe()}/sweep.lock (started ${h.startedAt}); this run is registered and waiting, checking every 30 s. The sweep stops at its next batch, or its lock goes stale 10 minutes after its last heartbeat.".toString())
                told = true
            }
            sleeper.call(POLL_MILLIS)
        }
    }

    private void beat() {
        try {
            storage.putLive(session, body)
        }
        catch( Exception e ) {
            // The next beat tries again; ten minutes of failures make this run look dead to a sweep.
        }
    }

    synchronized void close() {
        beating?.cancel(false)
        beating = null
        if( !registered )
            return
        registered = false
        try {
            storage.deleteLive(session)
        }
        catch( Exception e ) {
            say.call("nf-blocks could not remove this run's registration ${storage.describe()}/live/${session} (${e.message}); the next sweep deletes it once it is stale".toString())
        }
    }
}
```

The waiting `say` message is said once; a long wait is shown by the run not starting, and the lock's own staleness bounds it.

- [ ] **Step 5: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.core.*Lock*' --tests 'robsyme.cas.core.Live*' --tests 'robsyme.cas.s3.*' --tests 'robsyme.cas.core.LocalRetentionStorageTest'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/SweepLock.groovy src/main/groovy/robsyme/cas/core/LiveRegistry.groovy \
  src/main/groovy/robsyme/cas/core/LiveWriter.groovy src/test/groovy/robsyme/cas/core/SweepLockTest.groovy \
  src/test/groovy/robsyme/cas/core/LiveRegistryTest.groovy src/test/groovy/robsyme/cas/core/LiveWriterTest.groovy \
  src/test/groovy/robsyme/cas/s3/SweepLockS3Test.groovy
git commit -s -m "feat(retention): sweep lock, live registrations and the Live Writer (ticket 20 answers 1, 5, 6)"
```

---

### Task 6: The mark reads blocks from every member's roots

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/Mark.groovy`
- Create: `src/main/groovy/robsyme/cas/core/MemberLog.groovy` (`@Canonical class MemberLog { String alias; boolean writable; List<StoreLogEntry> entries }`)
- Test: `src/test/groovy/robsyme/cas/core/MarkTest.groovy`, and a test helper `src/test/groovy/robsyme/cas/core/RetentionFixture.groovy`

**Interfaces:**
- Consumes: Task 1's `ClaimState` (`deletion`, `released`, `pinned`); `Records` kinds; `StoreLogEntry` (`kind`, `cid`, `name`); `BlockStore.open` (throws `NoSuchBlockException`).
- Produces:

```groovy
class Mark {
    @Canonical static class Roots {
        int runs; int contentRoots; int released; int hidden; int hiddenButPinned
        int selections; int deletedSelections; int pinnedSubjects; int claims
    }
    /** Marks from every entry of every member's Store Log; reads blocks through `store` with `threads` reads in flight. */
    static Mark of(BlockStore store, List<MemberLog> logs, int threads)
    /** Adds what new Store Log entries root. Never removes from `live` (plan decision 10). */
    void extend(List<MemberLog> newEntries)
    boolean isLive(Cid cid)
    final Set<Cid> live                   // unmodifiable view
    final List<String> missingMetadata    // "<cid> (needed by <cid>)": a --apply refuses while non-empty
    final List<Cid> missingContent        // content already gone: reported only
    final List<String> danglingEntries    // names of the writable member's entries whose block is in no member
    final Roots roots
    long getBlocksRead()
}
```

The rules are plan decisions 1 to 6 and ticket 21 answers 1 to 4. A block's `want` is how much below it is kept: `SELF` (the block and, for a collection, its RunManifest: a Selection's via collections), `META` (the metadata closure) or `ALL` (metadata and content). Visiting a block again with a stronger want expands it further; with an equal or weaker one, does nothing.

| Kind reached | Kept | Then visits |
|---|---|---|
| RunCompletion | itself | `run` at META; each collection at the same want |
| RunManifest | itself, and its `script` raw block | nothing |
| OutputCollection | itself | `run` at META; at META or ALL each item at the same want; at ALL the `index` Leaf's address as content |
| OutputItem | itself | at ALL, each Leaf address as content |
| DirectoryManifest | itself | each `regular` entry's address, each `directory` entry's address as content |
| Selection | itself | each `item` member at ALL, its `via` collections at SELF; each nested `selection` at ALL |
| raw | itself | nothing |

Roots, from every member's Store Log (deduplicated by cid):

- a `run` entry: clean `delete` and not pinned: not a root (`hidden`); clean `delete` and pinned: ALL (`hiddenButPinned`); released and not pinned: META (`released`); otherwise ALL (`contentRoots`);
- a `selection` entry: clean `delete` and not pinned: not a root; otherwise ALL;
- any other subject with a current `add pin`: ALL (`pinnedSubjects`);
- a `claim` entry: live when its subject is live, repeated until nothing changes.

A root whose own block is in no member is dangling (decision 6). A dag-cbor block reached as content (a Directory Manifest) that is absent is `missingContent`; any other absent dag-cbor block is `missingMetadata`. Raw blocks are never looked up: a raw address is simply live (on S3 a HEAD per file would dominate the sweep's cost, and an absent raw block has nothing under it).

- [ ] **Step 1: Write the fixture** (`RetentionFixture.groovy`, used by this task and Task 7)

```groovy
package robsyme.cas.core

import java.nio.file.Path

/**
 * A local writable member with runs built from real blocks: each run's items
 * hold file leaves (raw) and optionally a directory leaf, logged like a real run.
 */
class RetentionFixture {

    final LocalBlockStore store
    final Path root
    long clock = 1_790_000_000_000L

    RetentionFixture(Path root) {
        this.root = root
        this.store = new LocalBlockStore(root, 'lab', true)
    }

    Cid raw(String text) { store.putStreaming(new ByteArrayInputStream(text.getBytes('UTF-8'))) }

    Cid dir(Map<String, Cid> files) {
        final List<ManifestEntry> entries = files.collect { String n, Cid c -> ManifestEntry.regular(n, c, 1L) }
        return store.putDagCbor(new DirectoryManifest(entries).toCbor())
    }

    /** A run with one output `out` whose items each hold the given leaves; returns [completion, collection, items]. */
    Map run(String name, List<Map<String, Cid>> items) {
        final Cid script = raw("script of ${name}")
        final Cid manifest = store.putDagCbor(RetentionFixture.manifest(name, script))
        final List<Cid> itemCids = items.collect { Map<String, Cid> leaves ->
            store.putDagCbor(OutputItem.of(leaves.collectEntries { String n, Cid c -> [(n): Leaf.of(n, c, 1L)] }).toCbor())
        }.sort { it.toString() }
        final Cid collection = store.putDagCbor(new OutputCollection('test', manifest, 'out', itemCids, itemCids.collect { [] as List<String> }).toCbor())
        final Cid completion = store.putDagCbor(RetentionFixture.completion(manifest, [collection]))
        StoreLog.append(store, StoreLogKind.RUN, completion, ++clock)
        return [completion: completion, collection: collection, items: itemCids, manifest: manifest, script: script]
    }

    Cid claim(Cid subject, String verb, String attribute, Object value, List<Cid> supersedes = []) {
        final Cid c = store.putDagCbor(new Claim('test', subject, verb, attribute, value, supersedes, Index.isoMillis(++clock)).toCbor())
        StoreLog.append(store, StoreLogKind.CLAIM, c, clock)
        return c
    }

    Cid selection(Cid item, Cid via) {
        final Cid s = store.putDagCbor(new Selection('test', [Selection.item(item, [via])], []).toCbor())
        StoreLog.append(store, StoreLogKind.SELECTION, s, ++clock)
        return s
    }

    List<MemberLog> logs() { [new MemberLog('lab', true, StoreLog.read(store))] }

    static Map manifest(String name, Cid script) {
        return new RunManifest(RecordsTest.manifestArgs() + [runName: name, nfRunHash: "hash-${name}".toString(), script: script]).toCbor()
    }

    static Map completion(Cid manifest, List<Cid> collections) {
        return new RunCompletion([assertedBy: 'test', run: manifest, collections: collections, inputSet: null,
            status: 'succeeded', exitStatus: 0, possiblyIncomplete: false,
            startedAt: '2026-09-28T10:00:00.000Z', finishedAt: '2026-09-28T10:05:00.000Z',
            anomalies: Anomalies.NONE, error: null]).toCbor()
    }
}
```

`RecordsTest.manifestArgs()` is already a public static builder; `RunCompletion(Map)` takes the same keys `RecordsTest.providerArgs` passes.

- [ ] **Step 2: Write the failing tests** (`MarkTest.groovy`)

```groovy
package robsyme.cas.core

import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

class MarkTest extends Specification {

    @TempDir Path root
    RetentionFixture f

    def setup() { f = new RetentionFixture(root) }

    private Mark mark() { Mark.of(f.store, f.logs(), 4) }

    def 'straight after a run everything it wrote is live'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')], [d: f.dir([one: f.raw('one')])]])

        when:
        final Mark m = mark()
        final Set<Cid> all = f.store.listBlocks().withCloseable { it.toList() } as Set

        then:
        m.live.containsAll(all)
        m.roots.runs == 1
        m.roots.contentRoots == 1
        m.missingMetadata == []
    }

    def 'a released run keeps its metadata and loses its content, directories included'() {
        given:
        final Cid x = f.raw('x'); final Cid one = f.raw('one'); final Cid d = f.dir([one: one])
        final Map r = f.run('a', [[x: x], [d: d]])
        f.claim(r.completion, 'set', 'retain', 'lineage')

        when:
        final Mark m = mark()

        then:
        !m.isLive(x) && !m.isLive(d) && !m.isLive(one)
        [r.completion, r.manifest, r.script, r.collection, *r.items].every { m.isLive((Cid) it) }
        m.roots.released == 1
    }

    def 'content shared with a kept run stays live'() {
        given:
        final Cid shared = f.raw('shared')
        final Map a = f.run('a', [[s: shared]])
        final Map b = f.run('b', [[s: shared, u: f.raw('only b')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')

        expect:
        mark().isLive(shared)
    }

    def 'a file shared inside two directories stays live when one run is released'() {
        given:
        final Cid two = f.raw('two')
        final Cid oneA = f.raw('one a'); final Cid oneB = f.raw('one b')
        final Map a = f.run('a', [[d: f.dir([one: oneA, two: two])]])
        final Map b = f.run('b', [[d: f.dir([one: oneB, two: two])]])
        f.claim(b.completion, 'set', 'retain', 'lineage')

        when:
        final Mark m = mark()

        then:
        m.isLive(two) && m.isLive(oneA)
        !m.isLive(oneB)
    }

    def 'a pinned item in a released run keeps its content, the rest goes'() {
        given:
        final Cid keep = f.raw('keep'); final Cid lose = f.raw('lose')
        final Map r = f.run('a', [[k: keep], [l: lose]])
        final Cid pinnedItem = r.items.find { Cid i -> leavesOf(i).contains(keep) }
        f.claim(r.completion, 'set', 'retain', 'lineage')
        final Cid pin = f.claim(pinnedItem, 'add', 'pin', 'figure 3')

        when:
        final Mark m = mark()

        then:
        m.isLive(keep) && m.isLive(pin)
        !m.isLive(lose)
    }

    def 'a deleted run gives up metadata and content, and its delete Claim goes with it'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        final Cid del = f.claim(r.completion, 'delete', null, null)

        when:
        final Mark m = mark()

        then:
        !m.isLive(r.completion) && !m.isLive(r.manifest) && !m.isLive(del)
        m.roots.hidden == 1
    }

    def 'a pin beats delete: hidden but pinned keeps the whole closure'() {
        given:
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        f.claim(r.completion, 'delete', null, null)
        f.claim(r.completion, 'add', 'pin', 'keep it')

        when:
        final Mark m = mark()

        then:
        m.isLive(r.completion) && m.isLive(r.manifest) && m.isLive(x)
        m.roots.hiddenButPinned == 1
    }

    def 'a conflicted retain or deletion keeps everything'() {
        given:
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        f.claim(r.completion, verb, attribute, value)
        f.claim(r.completion, verb2, attribute, value2)

        expect:
        mark().isLive(x)

        where:
        verb     | attribute | value     | verb2    | value2
        'set'    | 'retain'  | 'lineage' | 'set'    | 'lineage'
        'delete' | null      | null      | 'delete' | null
    }

    def 'del retain superseding the release makes the content live again'() {
        given:
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        final Cid release = f.claim(r.completion, 'set', 'retain', 'lineage')
        f.claim(r.completion, 'del', 'retain', null, [release])

        expect:
        mark().isLive(x)
    }

    def 'a Selection roots its items content and keeps its via collection as metadata only'() {
        given:
        final Cid picked = f.raw('picked'); final Cid other = f.raw('other')
        final Map r = f.run('a', [[p: picked], [o: other]])
        final Cid item = r.items.find { Cid i -> leavesOf(i).contains(picked) }
        f.selection(item, r.collection)
        f.claim(r.completion, 'set', 'retain', 'lineage')

        when:
        final Mark m = mark()

        then:
        m.isLive(picked) && m.isLive(r.collection) && m.isLive(r.manifest)
        !m.isLive(other)
        m.roots.selections == 1
    }

    def 'a missing item refuses; a missing directory manifest is only reported'() {
        given:
        final Cid gone = f.dir([one: f.raw('one')])
        final Map r = f.run('a', [[g: gone]])
        f.store.blockPath(gone).toFile().delete()

        when:
        Mark m = mark()

        then:
        m.missingContent == [gone]
        m.missingMetadata == []

        when:
        f.store.blockPath((Cid) r.items[0]).toFile().delete()
        m = mark()

        then:
        m.missingMetadata.size() == 1
        m.missingMetadata[0].startsWith(r.items[0].toString())
    }

    def 'a log entry whose block is in no member is dangling, not a root'() {
        given:
        final Cid ghost = Cid.parse('bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua')
        StoreLog.append(f.store, StoreLogKind.RUN, ghost, ++f.clock)

        when:
        final Mark m = mark()

        then:
        m.danglingEntries.size() == 1
        m.danglingEntries[0].endsWith("-run-${ghost}")
        m.missingMetadata == []
    }

    def 'extend adds what a new pin roots and never removes'() {
        given:
        final Cid x = f.raw('x')
        final Map r = f.run('a', [[x: x]])
        f.claim(r.completion, 'set', 'retain', 'lineage')
        final Mark m = mark()
        final List<StoreLogEntry> before = StoreLog.read(f.store)
        f.claim((Cid) r.items[0], 'add', 'pin', 'late')
        final List<StoreLogEntry> added = StoreLog.read(f.store).findAll { !(it in before) }

        expect:
        !m.isLive(x)

        when:
        m.extend([new MemberLog('lab', true, added)])

        then:
        m.isLive(x)
    }

    def 'a read-only member roots too, and its blocks are read through the composition'() {
        given: 'a second member holding a run whose content is in the writable member'
        final Cid x = f.raw('x')
        final RetentionFixture other = new RetentionFixture(root.resolve('other'))
        other.store.put(x, new ByteArrayInputStream('x'.bytes), 1L)
        other.run('b', [[x: x]])
        final CompositeStore both = new CompositeStore([f.store, new LocalBlockStore(root.resolve('other'), 'shared', false)])

        when:
        final Mark m = Mark.of(both, [f.logs()[0], new MemberLog('shared', false, StoreLog.read(other.store))], 4)

        then:
        m.isLive(x)
    }

    private Set<Cid> leavesOf(Cid item) {
        return OutputItem.fromCbor((Map) DagCbor.decode(f.store.open(item).readAllBytes())).leaves()*.address as Set
    }
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.MarkTest'`
Expected: FAIL, compilation (`Mark`, `MemberLog` unknown).

- [ ] **Step 4: Implement `Mark`**

```groovy
package robsyme.cas.core

import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * What a sweep may not remove (ticket 20 answer 4, ticket 21 answers 1 to 4;
 * plan decisions 1 to 6). Reads blocks, never the Index: deletion is where
 * correctness beats speed. Level by level, so up to `threads` blocks are read
 * at once; each level is decided before the next is read.
 */
@CompileStatic
class Mark {

    static final int SELF = 0
    static final int META = 1
    static final int ALL = 2

    @Canonical
    @CompileStatic
    static class Roots {
        int runs
        int contentRoots
        int released
        int hidden
        int hiddenButPinned
        int selections
        int deletedSelections
        int pinnedSubjects
        int claims
    }

    @Canonical
    @CompileStatic
    private static class Visit {
        Cid cid
        int want
        boolean content
        Cid parent
    }

    private final BlockStore store
    private final int threads
    private final Map<Cid, Integer> seen = new HashMap<Cid, Integer>()
    private final Set<Cid> liveSet = ConcurrentHashMap.newKeySet()
    private final Set<Cid> runs = new LinkedHashSet<Cid>()
    private final Set<Cid> selections = new LinkedHashSet<Cid>()
    private final Map<Cid, Cid> claimSubject = new HashMap<Cid, Cid>()
    private final Map<Cid, List<ClaimState.Row>> claimsOf = new HashMap<Cid, List<ClaimState.Row>>()
    private final Map<Cid, String> writableEntry = new HashMap<Cid, String>()
    private final Set<Cid> rooted = new HashSet<Cid>()
    private final AtomicLong reads = new AtomicLong()

    final List<String> missingMetadata = new ArrayList<String>()
    final List<Cid> missingContent = new ArrayList<Cid>()
    final List<String> danglingEntries = new ArrayList<String>()
    final Roots roots = new Roots()

    private Mark(BlockStore store, int threads) {
        this.store = store
        this.threads = Math.max(1, threads)
    }

    static Mark of(BlockStore store, List<MemberLog> logs, int threads) {
        final Mark m = new Mark(store, threads)
        m.extend(logs)
        return m
    }

    Set<Cid> getLive() { Collections.unmodifiableSet(liveSet) }

    boolean isLive(Cid cid) { liveSet.contains(cid) }

    long getBlocksRead() { reads.get() }

    synchronized void extend(List<MemberLog> logs) {
        final Set<Cid> touched = new LinkedHashSet<Cid>()
        final List<Cid> newClaims = new ArrayList<Cid>()
        for( MemberLog log : logs ) {
            for( StoreLogEntry e : log.entries ) {
                if( log.writable )
                    writableEntry.putIfAbsent(e.cid, e.name)
                switch( e.kind ) {
                    case StoreLogKind.RUN:
                        if( runs.add(e.cid) ) touched.add(e.cid)
                        break
                    case StoreLogKind.SELECTION:
                        if( selections.add(e.cid) ) touched.add(e.cid)
                        break
                    case StoreLogKind.CLAIM:
                        if( !claimSubject.containsKey(e.cid) ) newClaims.add(e.cid)
                        break
                }
            }
        }
        final Map<Cid, Map> claimBlocks = readAll(newClaims)
        for( Cid c : newClaims ) {
            final Map block = claimBlocks.get(c)
            if( block == null ) {
                dangling(c)
                continue
            }
            final Claim claim
            try {
                claim = Claim.fromCbor(block)
            }
            catch( IllegalArgumentException e ) {
                continue     // not a Claim this build reads: roots nothing
            }
            claimSubject.put(c, claim.subject)
            claimsOf.computeIfAbsent(claim.subject) { new ArrayList<ClaimState.Row>() }
                .add(new ClaimState.Row(c.toString(), claim.verb, claim.attribute, claim.value, claim.supersedes*.toString()))
            touched.add(claim.subject)
        }
        root(touched)
        keepClaims()
    }

    private ClaimState stateOf(Cid subject) {
        return ClaimState.of(claimsOf.get(subject) ?: Collections.<ClaimState.Row> emptyList())
    }

    /** Roots the touched subjects by the rules of the table above; a subject already rooted at ALL is left alone. */
    private void root(Set<Cid> touched) {
        final List<Visit> frontier = new ArrayList<Visit>()
        for( Cid s : touched ) {
            final ClaimState st = stateOf(s)
            final boolean deleted = st.deletion == ClaimState.DELETED
            if( runs.contains(s) ) {
                if( deleted && !st.pinned ) { count(s) { roots.hidden++ }; continue }
                if( deleted ) { count(s) { roots.hiddenButPinned++ }; frontier.add(new Visit(s, ALL, false, null)); continue }
                if( st.released && !st.pinned ) { count(s) { roots.released++ }; frontier.add(new Visit(s, META, false, null)); continue }
                count(s) { roots.contentRoots++ }
                frontier.add(new Visit(s, ALL, false, null))
            }
            else if( selections.contains(s) ) {
                if( deleted && !st.pinned ) { count(s) { roots.deletedSelections++ }; continue }
                count(s) { roots.selections++ }
                frontier.add(new Visit(s, ALL, false, null))
            }
            else if( st.pinned ) {
                count(s) { roots.pinnedSubjects++ }
                frontier.add(new Visit(s, ALL, false, null))
            }
        }
        roots.runs = runs.size()
        walk(frontier)
    }

    /** Counts a subject under one root heading once, even when extend reconsiders it. */
    private void count(Cid s, Closure tally) {
        if( rooted.add(s) )
            tally.call()
    }

    private void keepClaims() {
        boolean changed = true
        while( changed ) {
            changed = false
            for( Map.Entry<Cid, Cid> e : claimSubject.entrySet() )
                if( !liveSet.contains(e.key) && liveSet.contains(e.value) ) {
                    liveSet.add(e.key)
                    changed = true
                }
        }
        roots.claims = (int) claimSubject.keySet().count { Cid c -> liveSet.contains(c) }
    }

    private void walk(List<Visit> start) {
        List<Visit> frontier = start
        while( !frontier.isEmpty() ) {
            final List<Visit> todo = new ArrayList<Visit>()
            for( Visit v : frontier ) {
                final Integer had = seen.get(v.cid)
                if( had != null && had >= v.want )
                    continue
                seen.put(v.cid, v.want)
                todo.add(v)
            }
            final Map<Cid, Map> blocks = readAll(todo.findAll { Visit v -> !v.cid.isRaw() }*.cid)
            final List<Visit> next = new ArrayList<Visit>()
            for( Visit v : todo ) {
                if( v.cid.isRaw() ) {
                    liveSet.add(v.cid)
                    continue
                }
                final Map block = blocks.get(v.cid)
                if( block == null ) {
                    absent(v)
                    continue
                }
                liveSet.add(v.cid)
                expand(v, block, next)
            }
            frontier = next
        }
    }

    private void absent(Visit v) {
        if( v.parent == null )
            dangling(v.cid)
        else if( v.content )
            missingContent.add(v.cid)
        else
            missingMetadata.add("${v.cid} (needed by ${v.parent})".toString())
    }

    private void dangling(Cid cid) {
        final String entry = writableEntry.get(cid)
        if( entry != null && !danglingEntries.contains(entry) )
            danglingEntries.add(entry)
    }

    private static void content(Object address, Cid parent, List<Visit> next) {
        if( address instanceof Cid )
            next.add(new Visit((Cid) address, ALL, true, parent))
    }

    private static void meta(Object address, int want, Cid parent, List<Visit> next) {
        if( address instanceof Cid )
            next.add(new Visit((Cid) address, want, false, parent))
    }

    private void expand(Visit v, Map block, List<Visit> next) {
        switch( Records.kindOf(block) ) {
            case Records.RUN_COMPLETION:
                meta(block.get('run'), META, v.cid, next)
                for( Object c : (List) (block.get('collections') ?: []) )
                    meta(c, v.want, v.cid, next)
                break
            case Records.RUN_MANIFEST:
                content(block.get('script'), v.cid, next)
                break
            case Records.OUTPUT_COLLECTION:
                meta(block.get('run'), META, v.cid, next)
                if( v.want >= META )
                    for( Object i : (List) (block.get('items') ?: []) )
                        meta(i, v.want, v.cid, next)
                if( v.want == ALL && block.get('index') instanceof Map )
                    content(((Map) ((Map) block.get('index')).get('leaf'))?.get('address'), v.cid, next)
                break
            case Records.OUTPUT_ITEM:
                if( v.want == ALL )
                    for( Map leaf : Index.leavesOf(block.get('value')) )
                        content(leaf.get('address'), v.cid, next)
                break
            case Records.DIRECTORY_MANIFEST:
                for( Object e : (List) (block.get('entries') ?: []) )
                    content(((Map) e).get('address'), v.cid, next)
                break
            case Records.SELECTION:
                for( Object m : (List) (block.get('members') ?: []) ) {
                    final Map member = (Map) m
                    if( member.get('item') instanceof Map ) {
                        final Map item = (Map) member.get('item')
                        meta(item.get('address'), ALL, v.cid, next)
                        for( Object via : (List) (item.get('via') ?: []) )
                            meta(via, SELF, v.cid, next)
                    }
                    else
                        meta(member.get('selection'), ALL, v.cid, next)
                }
                break
            default:
                break   // a Claim or a kind this build does not know: itself only
        }
    }

    /** Reads and decodes dag-cbor blocks, up to `threads` at once; an absent or undecodable block maps to null. */
    private Map<Cid, Map> readAll(Collection<Cid> cids) {
        final Map<Cid, Map> out = new HashMap<Cid, Map>()
        if( cids.isEmpty() )
            return out
        final ExecutorService pool = Executors.newFixedThreadPool(Math.min(threads, cids.size()))
        try {
            final Map<Cid, Future<Map>> pending = new LinkedHashMap<Cid, Future<Map>>()
            for( Cid c : cids )
                pending.put(c, pool.submit({ -> read(c) } as Callable<Map>))
            for( Map.Entry<Cid, Future<Map>> e : pending.entrySet() )
                out.put(e.key, e.value.get())
        }
        finally {
            pool.shutdownNow()
        }
        return out
    }

    private Map read(Cid cid) {
        try {
            final InputStream in = store.open(cid)
            try {
                reads.incrementAndGet()
                final Object value = DagCbor.decode(in.readAllBytes())
                return value instanceof Map ? (Map) value : null
            }
            finally {
                in.close()
            }
        }
        catch( NoSuchBlockException e ) {
            return null
        }
    }
}
```

Two checks for the implementer: `Index.leavesOf(Object)` is the existing static that finds Leaf maps in an item's `value` (line ~1201); if it is private, make it package-visible or copy it. `Cid.isRaw()` exists (used in `Put.blockAt`). A block that exists but does not decode (`DagCbor.decode` throws) must not be read as "absent": let that exception propagate out of `Mark.of`, since a corrupt metadata block makes the mark unknowable.

- [ ] **Step 5: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.core.MarkTest'`
Expected: PASS, 14 features.

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Mark.groovy src/main/groovy/robsyme/cas/core/MemberLog.groovy \
  src/test/groovy/robsyme/cas/core/MarkTest.groovy src/test/groovy/robsyme/cas/core/RetentionFixture.groovy
git commit -s -m "feat(retention): the mark, from every member's Store Log roots, reading blocks (ticket 20 answer 4, ticket 21)"
```

---

### Task 7: The sweep: dry run, Trash ledger, deletion past the deadline

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/SweepPolicy.groovy`
- Create: `src/main/groovy/robsyme/cas/core/TrashLedger.groovy`
- Create: `src/main/groovy/robsyme/cas/core/SweepPlan.groovy`
- Create: `src/main/groovy/robsyme/cas/core/SweepReport.groovy`
- Create: `src/main/groovy/robsyme/cas/core/Sweep.groovy`
- Test: `src/test/groovy/robsyme/cas/core/TrashLedgerTest.groovy`, `SweepPlanTest.groovy`, `SweepTest.groovy`, and `src/test/groovy/robsyme/cas/s3/SweepS3Test.groovy`

**Interfaces:**
- Consumes: Tasks 4 to 6 (`RetentionStorage`, `RetainedStore`, `SweepLock`, `LiveRegistry`, `Mark`, `MemberLog`); `StoreLog.of(member).read()`; `CompositeStore.members`.
- Produces:

```groovy
@Canonical class SweepPolicy {
    static final long DAY = 86_400_000L
    long ageFloorMillis; long graceMillis
    static SweepPolicy defaults()                  // 14 days each
    void validate()                                // IllegalArgumentException: ageFloor < SweepLock.STALE_MILLIS, or grace < 0
    List<String> warnings()                        // either under a day
}

class TrashLedger {
    final String sweepId; final long deadlineMillis; final String trashedAt
    final SortedMap<Cid, Long> blocks              // cid -> size, cid order
    TrashLedger(String sweepId, long deadlineMillis, String trashedAt, Map<Cid, Long> blocks)
    String getName()                               // String.format('%013d', deadlineMillis) + '-' + sweepId
    byte[] toJson()
    static TrashLedger parse(String name, byte[] body)   // IllegalArgumentException on a bad ledger
    TrashLedger without(Collection<Cid> cids)
}

@Canonical class SweepPlan {
    List<BlockStat> dead; List<BlockStat> young; List<BlockStat> trash; List<BlockStat> overBudget
    List<BlockStat> due; List<BlockStat> waiting; Set<Cid> rescued
    static SweepPlan of(Mark mark, List<BlockStat> stats, List<TrashLedger> ledgers, long nowMillis, SweepPolicy policy, long budgetBytes)
}

class SweepReport { ... see Step 5; Map toJson(); String toText() }

class Sweep {
    static final int BATCH = 1000
    static final int THREADS = 32
    Sweep(BlockStore store, SweepPolicy policy)    // store: the composition (or one member); its first member is swept
    SweepReport dryRun()
    SweepReport apply(boolean wait, long budgetBytes, Closure<Boolean> stopRequested, Closure<Void> say, Closure<Void> sleeper)
}
```

Definitions, over the writable member's block listing (`stats`) and every ledger:

- **dead**: listed and not live in the mark. **young**: dead and younger than the age floor (never touched). 
- **due**: in a ledger whose deadline has passed, dead, not young. Deleted by `--apply`.
- **waiting**: in a ledger, dead, deadline not yet passed.
- **rescued**: in a ledger and live now, or in a ledger and no longer listed. Dropped from its ledger by `--apply`.
- **trash**: dead, not young, in no ledger, in cid order while the running size stays within the budget (a budget ≤ 0 is none). **overBudget**: the rest. `--apply` writes these in one new ledger with deadline now + grace.

- [ ] **Step 1: Write the failing tests**

`TrashLedgerTest.groovy`:

```groovy
    def 'a ledger round-trips, sorts its blocks and names itself by deadline'() {
        given:
        final Cid a = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')
        final Cid b = Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
        final TrashLedger l = new TrashLedger('20260930T120000Z-abcdefgh', 1_791_000_000_000L, '2026-09-30T12:00:00.000Z', [(b): 0L, (a): 6L])

        when:
        final TrashLedger back = TrashLedger.parse(l.name, l.toJson())
        final Map json = (Map) new JsonSlurper().parse(l.toJson())

        then:
        l.name == '1791000000000-20260930T120000Z-abcdefgh'
        back.blocks == l.blocks
        back.deadlineMillis == 1_791_000_000_000L
        json.blocks == [[cid: a.toString(), size: 6], [cid: b.toString(), size: 0]]
        json.deadline == Index.isoMillis(1_791_000_000_000L)
        l.without([a]).blocks.keySet() == [b] as Set
    }

    def 'a ledger whose name and body disagree, or that does not parse, is refused'() {
        when:
        TrashLedger.parse(name, body.bytes)

        then:
        thrown(IllegalArgumentException)

        where:
        name                     | body
        '1791000000000-other'    | '{"sweep":"s","trashed_at":"x","deadline":"2026-10-01T00:00:00.000Z","blocks":[]}'
        '1791000000000-s'        | 'not json'
        'nodigits-s'             | '{"sweep":"s","trashed_at":"x","deadline":"x","blocks":[]}'
    }
```

`SweepPlanTest.groovy` (pure: a `Mark` over a `RetentionFixture`, hand-built `BlockStat` lists and ledgers, `now` fixed):

```groovy
    def 'dead, young, due, waiting, rescued and trash are what the plan says'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        final Mark m = Mark.of(f.store, f.logs(), 2)
        final long now = 2_000_000_000_000L
        final long old = now - 15 * SweepPolicy.DAY
        final Cid liveOne = (Cid) r.completion
        final Cid dueOne = f.raw('due'), waitOne = f.raw('wait'), newOne = f.raw('new'), youngOne = f.raw('young')
        final List<BlockStat> stats = [new BlockStat(liveOne, 10, old), new BlockStat(dueOne, 3, old),
            new BlockStat(waitOne, 4, old), new BlockStat(newOne, 3, old), new BlockStat(youngOne, 5, now - 1000)]
        final List<TrashLedger> ledgers = [
            new TrashLedger('s1', now - 1, 'x', [(dueOne): 3L, (liveOne): 10L]),
            new TrashLedger('s2', now + SweepPolicy.DAY, 'x', [(waitOne): 4L])]

        when:
        final SweepPlan p = SweepPlan.of(m, stats, ledgers, now, SweepPolicy.defaults(), 0L)

        then:
        p.dead*.cid as Set == [dueOne, waitOne, newOne, youngOne] as Set
        p.young*.cid == [youngOne]
        p.due*.cid == [dueOne]
        p.waiting*.cid == [waitOne]
        p.rescued == [liveOne] as Set
        p.trash*.cid == [newOne]
    }

    def 'the budget takes blocks in address order while they fit'() {
        given: 'three dead old blocks of 4 bytes and a budget of 9'
        // build as above with no ledgers; three f.raw(...) blocks listed with size 4 each
        when:
        final SweepPlan p = SweepPlan.of(m, stats, [], now, SweepPolicy.defaults(), 9L)

        then:
        p.trash.size() == 2
        p.overBudget.size() == 1
        p.trash*.cid == stats*.cid.sort().take(2)
    }

    def 'policy: the age floor may not be under the stale threshold; grace may be zero; either under a day warns'() {
        when:
        new SweepPolicy(599_999L, 0L).validate()

        then:
        thrown(IllegalArgumentException)

        expect:
        new SweepPolicy(600_000L, 0L).warnings().size() == 2
        SweepPolicy.defaults().warnings() == []
    }
```

(Write out the budget feature's `given:` in full: a fixture, `Mark.of`, three `BlockStat`s of size 4 at `old`, no ledgers.)

`SweepTest.groovy` (local; a `RetentionFixture`; blocks backdated past the floor with `Files.setLastModifiedTime` on `f.store.blockPath(c)` for every listed block, a helper `ageAll(long millis)`; `say`/`sleeper` recorders; the sweep's clock is the local clock, so ledger deadlines are made to pass by a helper `expireLedgers()` that, for every ledger, writes `new TrashLedger(l.sweepId, System.currentTimeMillis() - 1, l.trashedAt, l.blocks)` under its new name and deletes the old name):

```groovy
    def 'a dry run straight after a run reports an empty dead set and writes nothing'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        ageAll(15 * SweepPolicy.DAY)
        final Set<String> before = listing(root)

        when:
        final SweepReport r = new Sweep(f.store, SweepPolicy.defaults()).dryRun()

        then:
        r.dead == 0
        !r.applied
        listing(root) == before
    }

    def 'apply after a release ledgers exactly the unshared content; blocks stay readable'() {
        given:
        final Cid shared = f.raw('shared'), only = f.raw('only b')
        f.run('a', [[s: shared]])
        final Map b = f.run('b', [[s: shared, u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)
        final List<String> ledgers = f.store.retentionStorage().listLedgers()

        then:
        r.applied
        r.trashed == 1
        ledgers.size() == 1
        TrashLedger.parse(ledgers[0], f.store.retentionStorage().readLedger(ledgers[0])).blocks.keySet() == [only] as Set
        f.store.has(only)
        SweepLock.holder(f.store.retentionStorage()) == null
    }

    def 'restored content is dropped from the ledger, not deleted'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        final Cid release = f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        f.claim(b.completion, 'del', 'retain', null, [release])
        expireLedgers()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.rescued == 1
        r.deleted == []
        f.store.has(only)
        f.store.retentionStorage().listLedgers() == []
    }

    def 'a later sweep past the deadline deletes what is still dead, and its log entries'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        final Map gone = f.run('gone', [[g: f.raw('gone')]])
        f.claim(gone.completion, 'delete', null, null)
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        !f.store.has(only)
        !f.store.has((Cid) gone.completion)
        f.store.has((Cid) b.completion)
        r.deleted.contains(gone.completion)
        !StoreLog.read(f.store).any { it.cid == gone.completion }
        f.store.retentionStorage().listLedgers() == []
    }

    def 'nothing younger than the age floor is trashed'() {
        given:
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')

        when:
        final SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.young == 1
        r.trashed == 0
        f.store.retentionStorage().listLedgers() == []
    }

    def 'a fresh registration refuses the sweep; a stale one is ignored and deleted'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        f.store.retentionStorage().putLive('running', '{"run_name":"busy_bee"}'.bytes)

        when:
        SweepReport r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        !r.applied
        r.stopped.contains('busy_bee')
        SweepLock.holder(f.store.retentionStorage()) == null

        when:
        Files.setLastModifiedTime(root.resolve('live/running'), FileTime.fromMillis(System.currentTimeMillis() - 601_000L))
        r = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        r.applied
        f.store.retentionStorage().listLive() == []
    }

    def 'with wait, the sweep polls until the registration is gone, holding no lock meanwhile'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        f.store.retentionStorage().putLive('running', '{}'.bytes)
        int polls = 0
        final Closure<Void> poll = { long ms ->
            assert SweepLock.holder(f.store.retentionStorage()) == null
            if( ++polls == 2 ) f.store.retentionStorage().deleteLive('running')
        } as Closure<Void>

        when:
        final SweepReport r = sweep().apply(true, 0L, { -> false }, say, poll)

        then:
        r.applied
        polls == 2
    }

    def 'a registration that appears mid-sweep stops it before the next batch'() {
        given: 'a store with due blocks; the stop check writes a registration on its first call'
        final Map b = f.run('b', [[u: f.raw('only')]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        final Closure<Boolean> writerArrives = { -> f.store.retentionStorage().putLive('late', '{}'.bytes); false } as Closure<Boolean>

        when:
        final SweepReport r = sweep().apply(false, 0L, writerArrives, say, sleeper)

        then:
        r.stopped.contains('live')
        r.deleted == []
        f.store.retentionStorage().listLedgers().size() == 1
    }

    def 'a pin written during the sweep rescues its content before the delete batch'() {
        given:
        final Cid only = f.raw('only')
        final Map b = f.run('b', [[u: only]])
        f.claim(b.completion, 'set', 'retain', 'lineage')
        ageAll(15 * SweepPolicy.DAY)
        sweep().apply(false, 0L, { -> false }, say, sleeper)
        expireLedgers()
        boolean pinned = false
        final Closure<Boolean> pinArrives = { -> if( !pinned ) { f.claim((Cid) b.items[0], 'add', 'pin', 'late'); pinned = true }; false } as Closure<Boolean>

        when:
        final SweepReport r = sweep().apply(false, 0L, pinArrives, say, sleeper)

        then:
        f.store.has(only)
        r.deleted == []
        r.rescued == 1
    }

    def 'a missing metadata block refuses --apply and a dry run reports it'() {
        given:
        final Map r = f.run('a', [[x: f.raw('x')]])
        f.store.blockPath((Cid) r.collection).toFile().delete()

        when:
        final SweepReport dry = sweep().dryRun()
        final SweepReport applied = sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        dry.missingMetadata.size() == 1
        !applied.applied
        applied.stopped.contains('cannot read')
    }

    def 'old scratch is cleared; young scratch stays'() {
        given:
        f.run('a', [[x: f.raw('x')]])
        Files.write(root.resolve('blocks/.tmp-old'), 'x'.bytes)
        Files.setLastModifiedTime(root.resolve('blocks/.tmp-old'), FileTime.fromMillis(System.currentTimeMillis() - 15 * SweepPolicy.DAY))
        Files.write(root.resolve('blocks/.tmp-new'), 'x'.bytes)

        when:
        sweep().apply(false, 0L, { -> false }, say, sleeper)

        then:
        !Files.exists(root.resolve('blocks/.tmp-old'))
        Files.exists(root.resolve('blocks/.tmp-new'))
    }

    def 'a read-only member is never swept'() {
        given: 'a composition whose read-only member holds a dead block'
        // f is the writable member; a second LocalBlockStore(root.resolve('ro'), 'ro', false) holds raw('dead in ro'),
        // aged past the floor; the composition is new CompositeStore([f.store, ro])
        when:
        new Sweep(both, SweepPolicy.defaults()).apply(false, 0L, { -> false }, say, sleeper)

        then:
        Files.exists(ro.blockPath(deadInRo))
        !Files.exists(root.resolve('ro/trash'))
    }
```

(Write the read-only feature's `given:` in full. `sweep()` is `new Sweep(f.store, SweepPolicy.defaults())`; `listing(root)` is every path under `root` with its size and mtime, as a set of strings.)

`SweepS3Test.groovy`: over `S3BlockStore(new MemoryS3Ops('b'), 'm/', 'lab', true, tmpDir)`, write a run and a release through a fixture that targets the S3 store (generalise `RetentionFixture` to take any `BlockStore & LoggedStore` if needed), `ops.advance(15 * DAY)` to age everything, then: apply writes one ledger under `m/trash/`; `ops.advance(15 * DAY)`; apply again deletes with one `DELETEMANY` call; an open multipart upload older than the floor is aborted; `m/sweep.lock` ends `released`.

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.TrashLedgerTest' --tests 'robsyme.cas.core.SweepPlanTest' --tests 'robsyme.cas.core.SweepTest' --tests 'robsyme.cas.s3.SweepS3Test'`
Expected: FAIL, compilation.

- [ ] **Step 3: Implement `SweepPolicy` and `TrashLedger`**

```groovy
@Canonical
@CompileStatic
class SweepPolicy {
    static final long DAY = 86_400_000L
    long ageFloorMillis
    long graceMillis

    static SweepPolicy defaults() { new SweepPolicy(14 * DAY, 14 * DAY) }

    /** Plan decision 8: the age floor covers a run that looks dead before it is. */
    void validate() {
        if( ageFloorMillis < SweepLock.STALE_MILLIS )
            throw new IllegalArgumentException("cas.sweep.ageFloor must be at least 10m, the time after which a run's registration counts as dead; got ${ageFloorMillis} ms")
        if( graceMillis < 0 )
            throw new IllegalArgumentException("cas.sweep.grace cannot be negative; got ${graceMillis} ms")
    }

    List<String> warnings() {
        final List<String> out = []
        if( ageFloorMillis < DAY )
            out << "cas.sweep.ageFloor is under a day: a block written by a pipeline that does not register here (a reader, a put) could be trashed soon after it is written".toString()
        if( graceMillis < DAY )
            out << "cas.sweep.grace is under a day: trashed blocks can be deleted by the next sweep, leaving little time to restore them".toString()
        return out
    }
}
```

```groovy
@CompileStatic
class TrashLedger {

    final String sweepId
    final long deadlineMillis
    final String trashedAt
    final SortedMap<Cid, Long> blocks

    TrashLedger(String sweepId, long deadlineMillis, String trashedAt, Map<Cid, Long> blocks) {
        this.sweepId = sweepId
        this.deadlineMillis = deadlineMillis
        this.trashedAt = trashedAt
        final TreeMap<Cid, Long> sorted = new TreeMap<Cid, Long>({ Cid a, Cid b -> a.toString() <=> b.toString() } as Comparator<Cid>)
        sorted.putAll(blocks)
        this.blocks = Collections.unmodifiableSortedMap(sorted)
    }

    String getName() { String.format('%013d', deadlineMillis) + '-' + sweepId }

    byte[] toJson() {
        return JsonOutput.toJson([sweep: sweepId, trashed_at: trashedAt, deadline: Index.isoMillis(deadlineMillis),
            blocks: blocks.collect { Cid c, Long size -> [cid: c.toString(), size: size] }]).getBytes('UTF-8')
    }

    TrashLedger without(Collection<Cid> cids) {
        final Map<Cid, Long> kept = new LinkedHashMap<Cid, Long>(blocks)
        cids.each { Cid c -> kept.remove(c) }
        return new TrashLedger(sweepId, deadlineMillis, trashedAt, kept)
    }

    static TrashLedger parse(String name, byte[] body) {
        final int dash = name.indexOf('-')
        if( dash != 13 || !(name.substring(0, 13) ==~ /\d{13}/) )
            throw new IllegalArgumentException("not a Trash ledger name, <13 digits>-<sweep id>: '${name}'")
        final Map json
        try {
            json = (Map) new JsonSlurper().parse(body)
        }
        catch( Exception e ) {
            throw new IllegalArgumentException("Trash ledger '${name}' is not JSON: ${e.message}")
        }
        final long deadline = Long.parseLong(name.substring(0, 13))
        if( json.sweep != name.substring(14) )
            throw new IllegalArgumentException("Trash ledger '${name}' names sweep '${json.sweep}'")
        final Map<Cid, Long> blocks = new LinkedHashMap<Cid, Long>()
        for( Object o : (List) (json.blocks ?: []) )
            blocks.put(Cid.parse((String) ((Map) o).cid), ((Number) ((Map) o).size).longValue())
        return new TrashLedger((String) json.sweep, deadline, (String) json.trashed_at, blocks)
    }
}
```

(Replace the `final Map json` / try assignment with a non-final local if the compiler refuses it. The name's deadline, not the body's `deadline` text, is authoritative: a listing sorts by it.)

- [ ] **Step 4: Implement `SweepPlan`**

```groovy
@Canonical
@CompileStatic
class SweepPlan {
    List<BlockStat> dead
    List<BlockStat> young
    List<BlockStat> trash
    List<BlockStat> overBudget
    List<BlockStat> due
    List<BlockStat> waiting
    Set<Cid> rescued

    static SweepPlan of(Mark mark, List<BlockStat> stats, List<TrashLedger> ledgers, long nowMillis, SweepPolicy policy, long budgetBytes) {
        final Map<Cid, TrashLedger> ledgerOf = new HashMap<Cid, TrashLedger>()
        for( TrashLedger l : ledgers )
            for( Cid c : l.blocks.keySet() )
                if( !ledgerOf.containsKey(c) || l.deadlineMillis < ledgerOf.get(c).deadlineMillis )
                    ledgerOf.put(c, l)
        final Set<Cid> listed = new HashSet<Cid>(stats*.cid)
        final List<BlockStat> sorted = new ArrayList<BlockStat>(stats).sort { BlockStat s -> s.cid.toString() }
        final List<BlockStat> dead = [], young = [], trash = [], over = [], due = [], waiting = []
        final Set<Cid> rescued = new LinkedHashSet<Cid>()
        long used = 0
        for( BlockStat s : sorted ) {
            final TrashLedger l = ledgerOf.get(s.cid)
            if( mark.isLive(s.cid) ) {
                if( l != null ) rescued.add(s.cid)
                continue
            }
            dead.add(s)
            if( nowMillis - s.lastModifiedMillis < policy.ageFloorMillis ) {
                young.add(s)
                continue
            }
            if( l != null ) {
                (l.deadlineMillis <= nowMillis ? due : waiting).add(s)
                continue
            }
            if( budgetBytes > 0 && used + s.size > budgetBytes ) {
                over.add(s)
                continue
            }
            used += s.size
            trash.add(s)
        }
        for( Cid c : ledgerOf.keySet() )
            if( !listed.contains(c) )
                rescued.add(c)      // gone already: nothing to delete, drop it from the ledger
        return new SweepPlan(dead, young, trash, over, due, waiting, rescued)
    }
}
```

- [ ] **Step 5: Implement `SweepReport` and `Sweep`**

`SweepReport` is a plain mutable holder (fields public, `@CompileStatic`), with:
`String sweepId; boolean applied; String stopped; Mark.Roots roots; int live; int blocks; long bytes; int dead; long deadBytes; int young; int trashed; long trashedBytes; int overBudget; int due; long dueBytes; int waiting; int rescued; List<Cid> deleted = []; long deletedBytes; int scratch; List<LiveRegistry.Registration> fresh = []; int staleRegistrations; List<String> missingMetadata = []; List<Cid> missingContent = []; int dangling; String ledger; long requests`.
`toJson()` returns a `Map` with those keys in snake_case (`deleted` as cid strings, `fresh` as `[{session, run_name, pipeline, age_seconds}]`, `roots` as its fields in snake_case); `toText()` renders, for a dry run:

```
dry run over lab (/path or s3://...), nothing written
roots      3 runs: 2 with content, 1 released, 0 hidden (0 hidden but pinned); 1 Selection; 2 pinned subjects; 4 Claims live
blocks     1,204 in lab (5.2 GB); 1,190 live
dead       14 (1.1 GB): 2 younger than the age floor (14d), 9 would be trashed (0.9 GB), 3 in Trash
trash      2 past their deadline would be deleted (0.2 GB), 1 waiting, 0 rescued
scratch    0 upload leftovers older than the age floor
live       none registered            (or: 1 run registered: happy_turing (session 9c1e...), heartbeat 20 s ago: --apply would refuse)
requests   about 1,230 (1,204 block reads, 2 listing pages, ...)
```

and for an applied sweep the same lines in the past tense, plus `ledger     trash/<name>` and `stopped    <reason>` when it stopped. Use one decimal and B/KB/MB/GB/TB for sizes, thousands separators for counts.

```groovy
@CompileStatic
class Sweep {

    static final int BATCH = 1000
    static final int THREADS = 32

    private final BlockStore store
    private final BlockStore writable
    private final RetentionStorage storage
    private final SweepPolicy policy

    Sweep(BlockStore store, SweepPolicy policy) {
        policy.validate()
        this.store = store
        this.writable = store instanceof CompositeStore ? ((CompositeStore) store).members[0] : store
        if( !(writable instanceof RetainedStore) || !writable.isWritable() )
            throw new IllegalArgumentException("store member '${writable.alias()}' is not a writable member this build can sweep")
        this.storage = ((RetainedStore) writable).retentionStorage()
        this.policy = policy
    }

    private List<BlockStore> members() {
        return store instanceof CompositeStore ? ((CompositeStore) store).members : [store]
    }

    private List<MemberLog> logs() {
        return members().collect { BlockStore m -> new MemberLog(m.alias(), m.is(writable), StoreLog.of(m).read()) }
    }

    SweepReport dryRun() {
        final SweepReport r = new SweepReport()
        r.fresh = new LiveRegistry(storage).fresh()
        final Mark mark = Mark.of(store, logs(), THREADS)
        final List<BlockStat> stats = storage.listBlockStats()
        fill(r, mark, stats, SweepPlan.of(mark, stats, ledgers(), storage.nowMillis(), policy, 0L))
        r.scratch = oldScratch().size()
        r.staleRegistrations = new LiveRegistry(storage).stale().size()
        return r
    }

    SweepReport apply(boolean wait, long budgetBytes, Closure<Boolean> stopRequested, Closure<Void> say, Closure<Void> sleeper) {
        final String id = SweepLock.newSweepId(System.currentTimeMillis())
        final SweepReport r = new SweepReport()
        r.sweepId = id
        final SweepLock lock = new SweepLock(storage, id, { -> System.currentTimeMillis() } as Closure<Long>)
        final LiveRegistry registry = new LiveRegistry(storage)
        // Take the lock, then list live/ (ticket 20 answer 5). With --wait, wait holding nothing,
        // so runs that register while we wait are not stuck behind us.
        while( true ) {
            final SweepLock.Holder h = lock.take()
            if( h == null ) {
                final List<LiveRegistry.Registration> fresh = registry.fresh()
                if( fresh.isEmpty() )
                    break
                lock.release()
                r.fresh = fresh
                if( !wait ) {
                    r.stopped = "a pipeline is running against ${storage.describe()}: ${describe(fresh)}; a sweep never runs beside a Live Writer (retry, or pass --wait)".toString()
                    return r
                }
                say.call("waiting for ${describe(fresh)} to finish; checking every 30 s".toString())
            }
            else {
                if( !wait ) {
                    r.stopped = "sweep ${h.sweepId} holds ${storage.describe()}/sweep.lock (heartbeat ${h.ageMillis.intdiv(1000L)} s ago)".toString()
                    return r
                }
                say.call("waiting for sweep ${h.sweepId} to release the lock; checking every 30 s".toString())
            }
            sleeper.call(LiveWriter.POLL_MILLIS)
        }
        final ScheduledExecutorService beats = Executors.newSingleThreadScheduledExecutor({ Runnable task ->
            final Thread t = new Thread(task, 'nf-blocks-sweep-heartbeat'); t.daemon = true; t } as ThreadFactory)
        final AtomicBoolean lost = new AtomicBoolean(false)
        beats.scheduleAtFixedRate({ -> if( !lock.heartbeat() ) lost.set(true) } as Runnable,
            SweepLock.HEARTBEAT_MILLIS, SweepLock.HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
        try {
            return applyLocked(r, lock, lost, registry, budgetBytes, stopRequested)
        }
        finally {
            beats.shutdownNow()
            lock.release()
        }
    }

    private SweepReport applyLocked(SweepReport r, SweepLock lock, AtomicBoolean lost, LiveRegistry registry,
                                    long budgetBytes, Closure<Boolean> stopRequested) {
        final List<MemberLog> seenLogs = logs()
        final Mark mark = Mark.of(store, seenLogs, THREADS)
        final List<BlockStat> stats = storage.listBlockStats()
        final List<TrashLedger> ledgers = ledgers()
        final long now = storage.nowMillis()
        SweepPlan plan = SweepPlan.of(mark, stats, ledgers, now, policy, budgetBytes)
        fill(r, mark, stats, plan)
        if( mark.missingMetadata ) {
            r.stopped = "the mark cannot read ${mark.missingMetadata.size()} metadata block(s) it needs, so it cannot tell what lies under them: ${mark.missingMetadata.take(20).join('; ')}".toString()
            return r
        }
        final Closure<String> recheck = { ->
            if( stopRequested.call() ) return 'interrupted'
            if( lost.get() || !lock.heartbeat() ) return 'the sweep lock was taken over'
            final List<LiveRegistry.Registration> fresh = registry.fresh()
            if( fresh ) { r.fresh = fresh; return "a pipeline started: ${describe(fresh)} is live".toString() }
            mark.extend(newEntries(seenLogs))
            return (String) null
        } as Closure<String>

        final List<Cid> deleted = new ArrayList<Cid>()
        final List<BlockStat> due = new ArrayList<BlockStat>(plan.due)
        for( int from = 0; from < due.size() && r.stopped == null; from += BATCH ) {
            r.stopped = recheck.call()
            if( r.stopped != null )
                break
            final List<BlockStat> batch = due.subList(from, Math.min(due.size(), from + BATCH)).findAll { BlockStat s -> !mark.isLive(s.cid) }
            final List<Cid> failed = storage.deleteBlocks(batch*.cid)
            for( BlockStat s : batch )
                if( !failed.contains(s.cid) ) {
                    deleted.add(s.cid)
                    r.deletedBytes += s.size
                }
        }
        r.deleted = deleted
        deleteLogEntries(deleted)
        // Rewrite every ledger without what was deleted, is live now, or is gone (plan decision 10).
        final Set<Cid> drop = new HashSet<Cid>(deleted)
        final Set<Cid> listed = new HashSet<Cid>(stats*.cid)
        r.rescued = 0      // what this sweep actually dropped, not the plan's count
        for( TrashLedger l : ledgers ) {
            final Set<Cid> leaving = l.blocks.keySet().findAll { Cid c -> drop.contains(c) || mark.isLive(c) || !listed.contains(c) } as Set<Cid>
            r.rescued += (int) leaving.count { Cid c -> !drop.contains(c) }
            final TrashLedger rest = l.without(leaving)
            if( rest.blocks.isEmpty() ) storage.deleteLedger(l.name)
            else if( !leaving.isEmpty() ) storage.writeLedger(l.name, rest.toJson())
        }
        if( r.stopped == null )
            r.stopped = recheck.call()
        if( r.stopped == null ) {
            final Map<Cid, Long> trash = plan.trash.findAll { BlockStat s -> !mark.isLive(s.cid) }.collectEntries { BlockStat s -> [(s.cid): s.size] }
            if( !trash.isEmpty() ) {
                final TrashLedger ledger = new TrashLedger(r.sweepId, storage.nowMillis() + policy.graceMillis, Index.isoMillis(System.currentTimeMillis()), trash)
                storage.writeLedger(ledger.name, ledger.toJson())
                r.ledger = ledger.name
            }
            r.trashed = trash.size()
            r.trashedBytes = (long) trash.values().sum(0L)
            for( Stamped s : oldScratch() ) { storage.deleteScratch(s); r.scratch++ }
            r.staleRegistrations = registry.stale().size()
            registry.deleteStale()
            for( String entry : mark.danglingEntries ) storage.deleteLogEntry(entry)
            r.applied = true
        }
        return r
    }
```

Remaining private helpers of `Sweep`, each a few lines:

- `List<TrashLedger> ledgers()`: `storage.listLedgers()`, each read and parsed; a ledger that does not parse is skipped with a warning in the report (`r.stopped` is not set: an unreadable ledger holds nothing we would delete).
- `List<Stamped> oldScratch()`: `storage.listScratch()` whose `nowMillis - lastModifiedMillis >= policy.ageFloorMillis`.
- `List<MemberLog> newEntries(List<MemberLog> seen)`: re-reads each member's Store Log and returns, per member, the entries whose name was not in `seen`; then adds them to `seen` so the next re-check reads only what is newer.
- `void deleteLogEntries(List<Cid> deleted)`: every entry of the writable member's Store Log whose cid is in `deleted`, through `storage.deleteLogEntry(name)` (decision 7).
- `void fill(SweepReport r, Mark m, List<BlockStat> stats, SweepPlan p)`: the counts (`roots`, `live = m.live.size()`, `blocks`, `bytes`, `dead`, `deadBytes`, `young`, `trashed = p.trash.size()` for a dry run, `overBudget`, `due`, `dueBytes`, `waiting`, `rescued = p.rescued.size()`, `missingMetadata`, `missingContent`, `dangling = m.danglingEntries.size()`, `requests = m.blocksRead + ceil(blocks / 1000) + ledgers + 2`).
- `static String describe(List<LiveRegistry.Registration> fresh)`: `"run happy_turing of pipeline p (session 9c1e..., heartbeat 20 s ago)"`, joined by `; `.

Keep imports explicit (`java.util.concurrent.*`, `AtomicBoolean`). `@CompileStatic` everywhere; where a closure's typing fights the compiler, a small private method is fine.

- [ ] **Step 6: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.core.TrashLedgerTest' --tests 'robsyme.cas.core.SweepPlanTest' --tests 'robsyme.cas.core.SweepTest' --tests 'robsyme.cas.s3.SweepS3Test'`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/SweepPolicy.groovy src/main/groovy/robsyme/cas/core/TrashLedger.groovy \
  src/main/groovy/robsyme/cas/core/SweepPlan.groovy src/main/groovy/robsyme/cas/core/SweepReport.groovy \
  src/main/groovy/robsyme/cas/core/Sweep.groovy src/test/groovy/robsyme/cas/core/TrashLedgerTest.groovy \
  src/test/groovy/robsyme/cas/core/SweepPlanTest.groovy src/test/groovy/robsyme/cas/core/SweepTest.groovy \
  src/test/groovy/robsyme/cas/s3/SweepS3Test.groovy src/test/groovy/robsyme/cas/core/RetentionFixture.groovy
git commit -s -m "feat(retention): sweep dry run and apply, Trash ledger, deletion past the deadline (ticket 20 answers 1 to 6)"
```

---

### Task 8: `Prune`, and the index forgets deleted blocks

**Files:**
- Create: `src/main/groovy/robsyme/cas/core/Prune.groovy`
- Create: `src/main/groovy/robsyme/cas/core/IndexedRun.groovy`
- Modify: `src/main/groovy/robsyme/cas/core/Index.groovy` (add `pipelines()`, `runsOf(String)`, `forget(Collection<Cid>)`)
- Test: `src/test/groovy/robsyme/cas/core/PruneTest.groovy`, `src/test/groovy/robsyme/cas/core/IndexTest.groovy` (append)

**Interfaces:**
- Consumes: Task 1's `ClaimState` (`released`, `retain`, `retainClaims`, `deletion`, `pinned`); `Index.claimState(Cid)`.
- Produces:

```groovy
@Canonical class IndexedRun { Cid completion; String pipeline; String runName; String status; boolean possiblyIncomplete; String finishedAt
    boolean isSuccessful() { status == 'succeeded' && !possiblyIncomplete } }

// Index
List<String> pipelines()                          // distinct run.pipeline, sorted
List<IndexedRun> runsOf(String pipeline)          // every run of the pipeline, newest finished_at first, ties by completion cid
/** Removes every row keyed by these cids as a run, collection, item, Selection or Claim, and their claim_current, in one transaction. Producer rows are kept (plan decision 7). */
void forget(Collection<Cid> cids)

@Canonical class PruneDecision { String pipeline; IndexedRun run; String action; String reason; List<Cid> supersedes }
// action: 'keep' | 'release' | 'skip'

class Prune {
    Prune(Index index)
    /** keepLast null or >= 0; keepNewerMillis null or > 0; at least one set (else IllegalArgumentException). pipeline null: every pipeline. */
    List<PruneDecision> plan(String pipeline, Integer keepLast, Long keepNewerMillis, long nowMillis)
    /** The put request for a release decision: set retain "lineage", superseding the run's current retain Claims. */
    static Map<String, Object> request(PruneDecision d, String timestamp)
}
```

Per run, in this order (ticket 21 answer 5, plan decision 12): a clean `delete` is `skip` ("deleted"); a `released` run is `skip` ("already released"); a conflicted `retain` group is `skip` ("retain Claims in conflict; content kept until one supersedes them"); then `keep` when `--keep-last` keeps it (successful, and among the newest N successful of its pipeline) or `--keep-newer` keeps it (`finished_at` at or after `now - period`), else `release`. A release of a pinned run carries the reason "pinned: its pins still hold". Failed and possibly-incomplete runs are never counted in N, so `--keep-last` releases them.

- [ ] **Step 1: Write the failing tests**

Append to `IndexTest.groovy` (use its existing helpers that ingest a run into a fresh index; read the file's fixture first):

```groovy
    def 'runsOf lists every run of a pipeline newest first; pipelines lists them all'() {
        given: 'three runs of p finished at t1 < t2 < t3, one failed, and one run of q'
        // ingest through the file's existing run builder, varying finished_at, status and pipeline

        expect:
        index.pipelines() == ['p', 'q']
        index.runsOf('p')*.finishedAt == [t3, t2, t1]
        index.runsOf('p').find { it.status == 'failed' }.successful == false
    }

    def 'forget removes a deleted run and its Claims but keeps the producers of its content'() {
        given: 'an ingested run with a delete Claim ingested too'
        final Cid content = /* one leaf address of that run */

        when:
        index.forget([completion, collection, item, deleteClaim])

        then:
        index.countRows('run') == 0
        index.countRows('collection') == 0
        index.countRows('claim') == 0
        index.countRows('claim_current') == 0
        !index.runByManifest(manifest).present
        index.producersOf(content).size() == 1
    }
```

(Write both `given:` blocks in full with the file's own builders; `countRows` exists.)

`PruneTest.groovy`, over a real `Index` in a `@TempDir` with runs ingested from a `RetentionFixture` (Task 6) whose completion blocks carry chosen `finished_at`, status and pipeline, and Claims written through `f.claim(...)` then ingested with `index.catchUp(f.store, StoreLog.of(f.store), 'lab')`:

```groovy
    def 'keep-last keeps the newest N successful runs and releases the rest, failed ones included'() {
        given: 'p: s1 (oldest, ok), f2 (failed), s3 (ok), s4 (newest, ok)'

        when:
        final List<PruneDecision> d = new Prune(index).plan('p', 2, null, now)

        then:
        d.collectEntries { [(it.run.runName): it.action] } == [s4: 'keep', s3: 'keep', f2: 'release', s1: 'release']
    }

    def 'keep-newer releases runs finished before the cutoff'() {
        when:
        final List<PruneDecision> d = new Prune(index).plan('p', null, 2 * SweepPolicy.DAY, now)

        then: 'runs finished 1, 3, 5 and 7 days ago'
        d.findAll { it.action == 'release' }*.run*.runName.sort() == ['f2', 's1']
    }

    def 'either policy keeping a run keeps it'() {
        expect:
        new Prune(index).plan('p', 1, 4 * SweepPolicy.DAY, now).findAll { it.action == 'keep' }*.run*.runName.sort() == ['f2', 's3', 's4']
    }

    def 'deleted, already released and conflicted runs are skipped; a pinned run is released with a note'() {
        given:
        f.claim(s1, 'delete', null, null)
        f.claim(f2, 'set', 'retain', 'lineage')
        f.claim(s3, 'set', 'retain', 'lineage'); f.claim(s3, 'set', 'retain', 'lineage')   // two blocks: timestamps differ
        f.claim(s4, 'add', 'pin', 'paper')
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        when:
        final Map<String, PruneDecision> d = new Prune(index).plan('p', 0, null, now).collectEntries { [(it.run.runName): it] }

        then:
        d.s1.action == 'skip' && d.s1.reason == 'deleted'
        d.f2.action == 'skip' && d.f2.reason == 'already released'
        d.s3.action == 'skip' && d.s3.reason.contains('conflict')
        d.s4.action == 'release' && d.s4.reason.contains('pinned')
    }

    def 'a release after a restore supersedes the del'() {
        given:
        final Cid release = f.claim(s1, 'set', 'retain', 'lineage')
        final Cid restore = f.claim(s1, 'del', 'retain', null, [release])
        index.catchUp(f.store, StoreLog.of(f.store), 'lab')

        when:
        final PruneDecision d = new Prune(index).plan('p', 0, null, now).find { it.run.runName == 's1' }
        final Map req = Prune.request(d, '2026-09-30T12:00:00.000Z')

        then:
        d.action == 'release'
        req == [kind: 'Claim', subject: s1, verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [restore],
                timestamp: '2026-09-30T12:00:00.000Z']
    }

    def 'no policy is refused'() {
        when:
        new Prune(index).plan(null, null, null, now)

        then:
        thrown(IllegalArgumentException)
    }
```

(`RetentionFixture.run` needs optional `finishedAt`, `status` and `pipeline` parameters for this: add them as a trailing `Map options = [:]` passed to `completion(...)` and `manifest(...)`.)

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.core.PruneTest' --tests 'robsyme.cas.core.IndexTest'`
Expected: FAIL, compilation.

- [ ] **Step 3: Implement the index methods**

```groovy
    List<String> pipelines() {
        final List<String> out = []
        query('SELECT DISTINCT pipeline FROM run WHERE pipeline IS NOT NULL ORDER BY pipeline', []) { ResultSet rs -> out.add(rs.getString(1)) }
        return out
    }

    List<IndexedRun> runsOf(String pipeline) {
        final List<IndexedRun> out = []
        query('SELECT completion_cid, pipeline, run_name, status, possibly_incomplete, finished_at FROM run ' +
              'WHERE pipeline = ? ORDER BY finished_at DESC, completion_cid', [pipeline]) { ResultSet rs ->
            out.add(new IndexedRun(Cid.parse(rs.getString(1)), rs.getString(2), rs.getString(3), rs.getString(4),
                rs.getInt(5) != 0, rs.getString(6)))
        }
        return out
    }

    /** Plan decision 7: after a sweep deletes blocks, their rows go too, so a snapshot seeds nothing whose blocks are gone. */
    void forget(Collection<Cid> cids) {
        if( !cids )
            return
        withTransaction {
            for( Cid c : cids ) {
                final String t = c.toString()
                for( String sql : [
                        'DELETE FROM run WHERE completion_cid = ?',
                        'DELETE FROM collection WHERE collection_cid = ?',
                        'DELETE FROM collection_item WHERE collection_cid = ?',
                        'DELETE FROM collection_item WHERE item_cid = ?',
                        'DELETE FROM item WHERE item_cid = ?',
                        'DELETE FROM item_attr WHERE item_cid = ?',
                        'DELETE FROM selection_child WHERE parent_cid = ?',
                        'DELETE FROM selection_derived WHERE selection_cid = ?',
                        'DELETE FROM claim_supersedes WHERE claim_cid = ?',
                        'DELETE FROM claim_current WHERE claim_cid = ?',
                        'DELETE FROM claim_current WHERE subject_cid = ?',
                        'DELETE FROM claim WHERE claim_cid = ?',
                        'DELETE FROM log_entry WHERE cid = ?',
                        'DELETE FROM missing WHERE have_cid = ? OR needed_cid = ?'] )
                    update(sql, sql.count('?') == 2 ? [t, t] as List<Object> : [t] as List<Object>)
            }
            // A subject some of whose Claims went keeps the rest: recompute its current state.
            final Set<String> subjects = new LinkedHashSet<String>()
            query('SELECT DISTINCT subject_cid FROM claim', []) { ResultSet rs -> subjects.add(rs.getString(1)) }
            for( String s : subjects )
                ClaimCurrent.rewrite(connection, s)
        }
    }
```

(`withTransaction` and `update` are the file's own; if `withTransaction`'s closure cannot call `update` under `@CompileStatic`, inline a `PreparedStatement` loop as `ClaimCurrent.rewrite` does. Recomputing every subject is fine at sweep frequency; if a test shows it slow at year scale, limit it to subjects of the forgotten Claims.)

- [ ] **Step 4: Implement `Prune`**

```groovy
@CompileStatic
class Prune {

    private final Index index

    Prune(Index index) { this.index = index }

    List<PruneDecision> plan(String pipeline, Integer keepLast, Long keepNewerMillis, long nowMillis) {
        if( keepLast == null && keepNewerMillis == null )
            throw new IllegalArgumentException('prune needs --keep-last <n> or --keep-newer <period>, or both')
        final List<PruneDecision> out = []
        for( String p : (pipeline != null ? [pipeline] : index.pipelines()) ) {
            int successfulSeen = 0
            for( IndexedRun run : index.runsOf(p) ) {
                final ClaimState st = index.claimState(run.completion)
                final boolean keptByCount = keepLast != null && run.successful && successfulSeen < keepLast
                if( run.successful ) successfulSeen++
                final boolean keptByAge = keepNewerMillis != null && run.finishedAt != null &&
                    Instant.parse(run.finishedAt).toEpochMilli() >= nowMillis - keepNewerMillis
                final List<Cid> supersedes = st.retainClaims.collect { String c -> Cid.parse(c) }
                if( st.deletion == ClaimState.DELETED )
                    out << new PruneDecision(p, run, 'skip', 'deleted', supersedes)
                else if( st.released )
                    out << new PruneDecision(p, run, 'skip', 'already released', supersedes)
                else if( st.retain == ClaimState.CONFLICTED )
                    out << new PruneDecision(p, run, 'skip', 'retain Claims in conflict; content kept until one supersedes them', supersedes)
                else if( keptByCount || keptByAge )
                    out << new PruneDecision(p, run, 'keep', keptByCount ? "one of the newest ${keepLast} successful".toString() : 'newer than the cutoff', supersedes)
                else
                    out << new PruneDecision(p, run, 'release', st.pinned ? 'pinned: its pins still hold' : (run.successful ? 'older' : 'failed or possibly incomplete'), supersedes)
            }
        }
        return out
    }

    static Map<String, Object> request(PruneDecision d, String timestamp) {
        final Map<String, Object> r = new LinkedHashMap<String, Object>()
        r.put('kind', Records.CLAIM)
        r.put('subject', d.run.completion)
        r.put('verb', Claim.SET)
        r.put('attribute', Claim.RETAIN)
        r.put('value', Claim.LINEAGE)
        r.put('supersedes', new ArrayList<Cid>(d.supersedes))
        r.put('timestamp', timestamp)
        return r
    }
}
```

(`successfulSeen` counts successful runs in newest-first order whatever their Claims, so a deleted or released recent run still takes one of the N places. That is the reading of "keeps the newest N successful runs": a person who pruned to 3 and then deleted one of the 3 does not expect a fourth to be released, nor an older one to be kept in its place. Record it in DESIGN §19.)

- [ ] **Step 5: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.core.PruneTest' --tests 'robsyme.cas.core.IndexTest' --tests 'robsyme.cas.core.IndexSnapshotTest'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/robsyme/cas/core/Prune.groovy src/main/groovy/robsyme/cas/core/IndexedRun.groovy \
  src/main/groovy/robsyme/cas/core/Index.groovy src/test/groovy/robsyme/cas/core/PruneTest.groovy \
  src/test/groovy/robsyme/cas/core/IndexTest.groovy src/test/groovy/robsyme/cas/core/RetentionFixture.groovy
git commit -s -m "feat(retention): prune by --keep-last and --keep-newer; the index forgets swept blocks (ticket 21 answer 5)"
```

---

### Task 9: The verbs `sweep`, `prune` and `untrash`, and `cas.sweep`

**Files:**
- Create: `src/main/groovy/robsyme/cas/cli/RetentionCommands.groovy`
- Create: `src/main/groovy/robsyme/cas/SweepSettings.groovy`
- Modify: `src/main/groovy/robsyme/cas/cli/CasCommands.groovy` (`VERBS`, dispatch, usage)
- Modify: `src/main/groovy/robsyme/cas/CasConfigScope.groovy` (nested `CasSweepScope`)
- Test: `src/test/groovy/robsyme/cas/cli/RetentionCommandsTest.groovy`, `src/test/groovy/robsyme/cas/SweepSettingsTest.groovy`, `src/test/groovy/robsyme/cas/CasConfigScopeTest.groovy` (append, if it lists the scope's options)

**Interfaces:**
- Consumes: Tasks 2, 7 and 8 (`Put`, `Sweep`, `SweepReport`, `SweepPolicy`, `TrashLedger`, `SweepLock`, `Prune`, `Index.forget`); `CasSession` (`store`, `members()`, `openIndex()`, `catchUpIndex`, `newPut`, `snapshotBase`, `snapshotWritable`, `checkClock`).
- Produces: `SweepSettings.of(Map config) → SweepPolicy` (reads `cas.sweep.ageFloor` and `cas.sweep.grace` as `nextflow.util.Duration`, a `Duration`, or text such as `'14d'`; defaults 14 days; `validate()`s; its `warnings()` are printed by the verbs on stderr).
- Produces verbs (exit 0 on success, 1 on a failure the verb reports, 2 on a usage error):

```
nextflow plugin nf-blocks:sweep [--apply] [--wait] [--budget <size>] [--format text|json]
nextflow plugin nf-blocks:prune (--keep-last <n> | --keep-newer <period>)... [--pipeline <id>] [--apply]
nextflow plugin nf-blocks:untrash (<cid>... | --sweep <id>)
```

`sweep` without `--apply` prints `SweepReport.toText()` (or `toJson()` as one line of JSON) and exits 0; `--apply` exits 1 when `stopped` is set and 0 otherwise. After an applied sweep that deleted anything, the verb calls `index.forget(report.deleted)` and rewrites the writable member's Index Snapshot (`snapshotWritable(index, 0L, base, failed)`), printing the snapshot line as `snapshot` does. A shutdown hook sets the stop flag and waits up to 30 s for the sweep to finish its batch and release the lock. `--budget` takes `MemoryUnit` text (`'10 GB'`, `'500MB'`).

`prune` prints one line per run, `<action>  <pipeline>  <run name>  <finished_at>  <completion cid>  <reason>`, then a count line; with `--apply` it writes each `release` through one `Put` builder (one catch-up; every Claim timestamped by the verb's clock) and prints each response. A failed write stops, prints what was written, exits 1.

`untrash` takes the sweep lock (refusing, exit 1, while another sweep holds it), rewrites every ledger without the named addresses (or deletes the ledgers of `--sweep <id>`), releases the lock, prints how many blocks it took out of which ledgers, and prints on stderr: "a block nothing reaches is trashed again by the next sweep; to keep it, pin it (put add pin) or restore its run (put del retain)". An address in no ledger is reported, not an error; no address found at all exits 1.

- [ ] **Step 1: Write the failing tests** (`RetentionCommandsTest.groovy`, driving `new CasCommands().run(verb, args, config, out, err)` over a local store in a `@TempDir` built with `RetentionFixture`, and `config` a map with `lineage.store.location = 'cas://lab'`, `cas.stores.lab.location = <dir>`, `cas.index.path = <tmp>/index.sqlite` as the existing `CasCommandsTest` builds it)

```groovy
    def 'sweep without --apply is a dry run: exit 0, a report, nothing written'() {
        when:
        final int code = run('sweep', [])

        then:
        code == 0
        out.toString().startsWith('dry run over lab')
        !Files.exists(store.resolve('sweep.lock'))
        !Files.exists(store.resolve('trash'))
    }

    def 'sweep --format json prints the report as one JSON object'() {
        when:
        run('sweep', ['--format', 'json'])
        final Map json = (Map) new JsonSlurper().parseText(out.toString())

        then:
        json.applied == false
        json.dead == 0
        json.roots.runs == 1
    }

    def 'sweep --apply after a release writes a ledger and exits 0'() {
        given:
        f.claim(run.completion, 'set', 'retain', 'lineage')
        ageAll()

        expect:
        run('sweep', ['--apply', 'true']) == 0
        Files.list(store.resolve('trash')).count() == 1
    }

    def 'sweep --apply beside a fresh registration exits 1 naming it'() {
        given:
        Files.createDirectories(store.resolve('live'))
        Files.write(store.resolve('live/s1'), '{"run_name":"busy_bee"}'.bytes)

        expect:
        run('sweep', ['--apply', 'true']) == 1
        err.toString().contains('busy_bee') || out.toString().contains('busy_bee')
    }

    def 'an applied sweep that deletes a hidden run forgets it in the index and rewrites the snapshot'() {
        given: 'a deleted run, aged, swept once (ledgered), its ledger expired'
        // f.claim(run.completion, 'delete', null, null); ageAll(); run('sweep', ['--apply', 'true']); expireLedgers()

        when:
        run('sweep', ['--apply', 'true'])

        then:
        Index.open(indexPath).withCloseable { Index i -> i.countRows('run') } == 0
        out.toString().contains('wrote')
    }

    def 'prune dry run lists decisions; --apply writes set retain Claims'() {
        given: 'runs s1 (older) and s2 (newer) of pipeline p'

        when:
        int code = run('prune', ['--keep-last', '1'])

        then:
        code == 0
        out.toString().readLines().find { it.startsWith('release') }.contains('s1')
        StoreLog.read(f.store).count { it.kind == StoreLogKind.CLAIM } == 0

        when:
        code = run('prune', ['--keep-last', '1', '--apply', 'true'])

        then:
        code == 0
        index().claimState(s1).released
        !index().claimState(s2).released
    }

    def 'prune without a policy is a usage error'() {
        expect:
        run('prune', []) == 2
    }

    def 'untrash takes an address out of its ledger and says it will come back'() {
        given: 'a sweep has ledgered block x'

        when:
        final int code = run('untrash', [x.toString()])

        then:
        code == 0
        ledgerBlocks() == [] as Set
        err.toString().contains('trashed again by the next sweep')
    }

    def 'untrash refuses while another sweep holds the lock'() {
        given:
        new SweepLock(new LocalRetentionStorage(store), 'other', { -> System.currentTimeMillis() }).take()

        expect:
        run('untrash', ['--sweep', 'whatever']) == 1
    }
```

(Write every `given:` in full with the fixture. `ageAll()` backdates every block file; `expireLedgers()` is Task 7's helper; `index()` opens `indexPath` and catches it up. Keep `RetentionFixture` in `robsyme.cas.core` and import it.)

`SweepSettingsTest.groovy`:

```groovy
    def 'defaults are 14 days each; durations and text are read; bad values refused'() {
        expect:
        SweepSettings.of([:]) == SweepPolicy.defaults()
        SweepSettings.of([cas: [sweep: [ageFloor: '1h', grace: '0s']]]) == new SweepPolicy(3_600_000L, 0L)
        SweepSettings.of([cas: [sweep: [ageFloor: Duration.of('2d')]]]).ageFloorMillis == 2 * SweepPolicy.DAY

        when:
        SweepSettings.of([cas: [sweep: [ageFloor: '5m']]])

        then:
        thrown(IllegalArgumentException)
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.cli.RetentionCommandsTest' --tests 'robsyme.cas.SweepSettingsTest'`
Expected: FAIL (`unknown command 'nf-blocks:sweep'`, `SweepSettings` unknown).

- [ ] **Step 3: Implement**

`CasConfigScope.groovy`, beside `snapshot`:

```groovy
    @Description('What nf-blocks:sweep protects (DESIGN.md §19).')
    CasSweepScope sweep

    /** {@code cas.sweep}: the age floor and the Trash grace period (ticket 20; plan decision 8). */
    @CompileStatic
    static class CasSweepScope implements ConfigScope {
        CasSweepScope() {}

        @ConfigOption
        @Description('A block younger than this is never trashed, whatever reaches it. At least 10m. Defaults to 14d.')
        Duration ageFloor

        @ConfigOption
        @Description('How long a trashed block waits before a sweep may delete it. Defaults to 14d.')
        Duration grace
    }
```

(`nextflow.util.Duration`; the build's `PluginSpecWriter` step then lists both in `META-INF/spec.json`; check the build still refuses an empty spec and passes.)

`SweepSettings.groovy`:

```groovy
package robsyme.cas

import groovy.transform.CompileStatic
import nextflow.util.Duration
import robsyme.cas.core.SweepPolicy

/** cas.sweep.ageFloor and cas.sweep.grace as a SweepPolicy (plan decision 8). */
@CompileStatic
class SweepSettings {

    static SweepPolicy of(Map config) {
        final Object scope = ((Map) (config?.get('cas') ?: [:])).get('sweep')
        final Map sweep = scope instanceof Map ? (Map) scope : [:]
        final SweepPolicy defaults = SweepPolicy.defaults()
        final SweepPolicy policy = new SweepPolicy(
            millis(sweep.get('ageFloor'), defaults.ageFloorMillis, 'cas.sweep.ageFloor'),
            millis(sweep.get('grace'), defaults.graceMillis, 'cas.sweep.grace'))
        policy.validate()
        return policy
    }

    private static long millis(Object value, long fallback, String key) {
        if( value == null )
            return fallback
        if( value instanceof Duration )
            return ((Duration) value).toMillis()
        try {
            return Duration.of(value.toString()).toMillis()
        }
        catch( IllegalArgumentException e ) {
            throw new IllegalArgumentException("${key} is a duration such as '14d' or '12h'; got '${value}'")
        }
    }
}
```

`CasCommands.groovy`: `VERBS = ['explore', 'items', 'prune', 'put', 'snapshot', 'sweep', 'untrash']`; dispatch

```groovy
                case 'sweep':
                    return RetentionCommands.sweep(Options.parse(args, ['apply', 'wait', 'budget', 'format'] as Set), config, out, err)
                case 'prune':
                    return RetentionCommands.prune(Options.parse(args, ['keep-last', 'keep-newer', 'pipeline', 'apply'] as Set), config, out, err, clock)
                case 'untrash':
                    return RetentionCommands.untrash(Options.parse(args, ['sweep'] as Set), config, out, err)
```

and three usage lines:

```
  sweep [--apply] [--wait] [--budget <size>] [--format text|json]
                         dry run unless --apply: roots, live, dead and Trash; --apply trashes the newly dead and deletes what is past its grace
  prune (--keep-last <n> | --keep-newer <period>) [--pipeline <id>] [--apply]
                         dry run unless --apply: release the content of older runs, keeping their lineage (set retain "lineage")
  untrash (<cid>... | --sweep <id>)
                         take blocks out of the Trash ledger; a block nothing reaches is trashed again by the next sweep
```

`RetentionCommands.groovy` (static methods, the style of `ItemsCommand`):

```groovy
@CompileStatic
class RetentionCommands {

    static int sweep(Options o, Map config, PrintStream out, PrintStream err) {
        if( o.positionals ) throw new UsageException("sweep takes no arguments, got ${o.positionals}")
        final boolean apply = bool(o, 'apply'), wait = bool(o, 'wait')
        final String format = o.flag('format') ?: 'text'
        if( !(format in ['text', 'json']) ) throw new UsageException("--format is text or json, got '${format}'")
        if( wait && !apply ) throw new UsageException('--wait applies to --apply only; a dry run never waits')
        final long budget = o.flag('budget') ? bytes(o.flag('budget')) : 0L
        final SweepPolicy policy = policy(config, err)
        final CasSession cas = new CasSession(CasConfig.fromSession(config))
        cas.checkClock()
        final Sweep sweep = new Sweep(cas.store, policy)
        if( !apply ) {
            final SweepReport r = sweep.dryRun()
            out.println(format == 'json' ? JsonOutput.toJson(r.toJson()) : r.toText())
            return 0
        }
        final AtomicBoolean stop = new AtomicBoolean(false)
        final CountDownLatch done = new CountDownLatch(1)
        final Thread hook = new Thread({ -> stop.set(true); done.await(30, TimeUnit.SECONDS) } as Runnable, 'nf-blocks-sweep-stop')
        Runtime.runtime.addShutdownHook(hook)
        final SweepReport r
        try {
            r = sweep.apply(wait, budget, { -> stop.get() } as Closure<Boolean>,
                { String m -> err.println("nf-blocks:sweep: ${m}") } as Closure<Void>,
                { long ms -> Thread.sleep(ms) } as Closure<Void>)
        }
        finally {
            done.countDown()
            try { Runtime.runtime.removeShutdownHook(hook) } catch( IllegalStateException e ) { /* shutting down */ }
        }
        out.println(format == 'json' ? JsonOutput.toJson(r.toJson()) : r.toText())
        if( r.deleted )
            forgetAndSnapshot(cas, r, out)
        return r.stopped == null ? 0 : 1
    }

    /** Plan decision 7. Derived: a failure warns and the sweep's result stands. */
    private static void forgetAndSnapshot(CasSession cas, SweepReport r, PrintStream out) {
        final SnapshotBase base = cas.snapshotBase()
        final Index index = cas.openIndex()
        try {
            index.forget(r.deleted)
            final Set<String> failed = cas.catchUpIndex(index)
            final IndexSnapshot.Result s = cas.snapshotWritable(index, 0L, base, failed)
            out.println(s.skipped ? "snapshot   not rewritten: ${s.skipped}" : "snapshot   wrote ${cas.snapshotsOf(cas.config.writableAlias).describe()} (${s.runs} runs)")
        }
        catch( Exception e ) {
            out.println("snapshot   not rewritten: ${e.message}; run nf-blocks:snapshot")
        }
        finally {
            index.close()
        }
    }
```

`prune` builds `Prune(index).plan(...)` after `cas.catchUpIndex(index)`; `--keep-last` through `o.intFlag`, `--keep-newer` through `Duration.of(...)` (a usage error, exit 2, on a bad value, and on neither flag); prints the table; with `--apply`, `cas.checkClock()`, one `cas.newPut(index)` and, for each `release`, `builder.put((Object) Prune.request(d, Index.isoMillis(clock.call())), false)`, printing each response body; a `PutError` prints its body, `nf-blocks:prune: wrote <n> of <m> Claims; stopped at <run>: <message>` on stderr, exit 1.

`untrash`: positionals are cids (a non-cid is a usage error) or `--sweep <id>` (not both); `SweepLock(storage, SweepLock.newSweepId(now), clock).take()`; a non-null holder exits 1 naming it; else for each `storage.listLedgers()` parse, `without(named)` or (for `--sweep`) delete the ledgers whose `sweepId` matches, write or delete, then `release()` in a `finally`.

Helpers: `bool(o, name)` accepts only null, `'true'`, `'false'` (else usage error, as `put --dry-run`); `bytes(text)` through `new MemoryUnit(text).toBytes()` (usage error on a bad value); `policy(config, err)` calls `SweepSettings.of(config)` (an `IllegalArgumentException` is exit 1 with its message) and prints each `warnings()` line on `err`.

- [ ] **Step 4: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.cli.*' --tests 'robsyme.cas.SweepSettingsTest' --tests 'robsyme.cas.CasConfigScopeTest'` then `./gradlew assemble` (the spec writer runs, `META-INF/spec.json` lists `cas.sweep.ageFloor` and `cas.sweep.grace`: check with `unzip -p build/distributions/nf-blocks-*.zip META-INF/spec.json | python3 -m json.tool | /usr/bin/grep -a -c ageFloor`).
Expected: PASS; count ≥ 1.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/cli/RetentionCommands.groovy src/main/groovy/robsyme/cas/SweepSettings.groovy \
  src/main/groovy/robsyme/cas/cli/CasCommands.groovy src/main/groovy/robsyme/cas/CasConfigScope.groovy src/test/groovy/robsyme/cas/
git commit -s -m "feat(cli): sweep, prune and untrash; cas.sweep.ageFloor and cas.sweep.grace (ticket 07 answers 2 and 6)"
```

---

### Task 10: Every run is a Live Writer

**Files:**
- Modify: `src/main/groovy/robsyme/cas/CasSession.groovy` (start and stop a `LiveWriter` on the writable member)
- Modify: `src/main/groovy/robsyme/cas/trace/CasObserver.groovy` (`onFlowCreate`, `completeRun`)
- Test: `src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy` (append), `src/test/groovy/robsyme/cas/CasSessionTest.groovy` (append)

**Interfaces:**
- Consumes: Task 5's `LiveWriter`, `SweepLock`; Task 4's `RetainedStore`.
- Produces on `CasSession`:
  - `static Closure<Void> liveSleeper = { long ms -> Thread.sleep(ms) }` (a test seam, as `s3OpsFactory` is);
  - `void startLiveWriter(String session, Map<String, String> info)`: when `members()[0]` is a writable `RetainedStore`, builds a `LiveWriter` whose `say` is `ConsoleLog.LOG.warn`, whose heartbeat executor is one daemon thread named `nf-blocks-live-heartbeat`, and starts it; also adds a JVM shutdown hook that closes it;
  - `void stopLiveWriter()`: closes it, shuts the executor down, removes the hook; idempotent.
- Wiring: `CasObserver.onFlowCreate` calls `cas.startLiveWriter(session.uniqueId.toString(), [run_name: runName(meta), pipeline: pipelineIdentity(meta, meta?.manifest), started_at: iso(meta?.start)])` right after `cas.checkClock()`. `completeRun` calls `cas.stopLiveWriter()` in its `finally`, after `cas.completionWritten()`, on the winning notification only (the losing one returns early and leaves it to the winner).

At v26.04.6 `Session.start()` calls `notifyFlowCreate()` (Session.groovy:606) before the script runs, so the registration precedes every `fromStore` read and every publish; confirm with `git -C /Users/robsyme/dev/github.com/nextflow-io/nextflow show v26.04.6:modules/nextflow/src/main/groovy/nextflow/Session.groovy | sed -n 590,615p` and record the line in DESIGN §19.

- [ ] **Step 1: Write the failing tests**

Append to `CasObserverTest.groovy`, using its existing way of building a `Session` and a `CasSession` over a local store in a temp dir:

```groovy
    def 'a run registers in live/ at flow create and deregisters when its completion is written'() {
        given:
        final Path live = storeRoot.resolve('live')

        when:
        observer.onFlowCreate(session)

        then:
        Files.list(live).count() == 1
        new JsonSlurper().parse(live.resolve(session.uniqueId.toString())).session == session.uniqueId.toString()

        when:
        observer.onFlowComplete()

        then:
        Files.list(live).count() == 0
    }

    def 'a run waits while a fresh sweep lock is held, then proceeds'() {
        given:
        final SweepLock sweep = new SweepLock(new LocalRetentionStorage(storeRoot), 'sweep-9', { -> System.currentTimeMillis() })
        sweep.take()
        int slept = 0
        CasSession.liveSleeper = { long ms -> if( ++slept == 1 ) sweep.release() } as Closure<Void>

        when:
        observer.onFlowCreate(session)

        then:
        slept == 1
        Files.exists(storeRoot.resolve('live').resolve(session.uniqueId.toString()))

        cleanup:
        CasSession.liveSleeper = { long ms -> Thread.sleep(ms) } as Closure<Void>
        observer.onFlowComplete()
    }
```

Append to `CasSessionTest.groovy`:

```groovy
    def 'a read-only-only session has nothing to register; stop is idempotent'() {
        when:
        cas.stopLiveWriter()
        cas.stopLiveWriter()

        then:
        notThrown(Exception)
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests 'robsyme.cas.trace.CasObserverTest' --tests 'robsyme.cas.CasSessionTest'`
Expected: FAIL (no `live/` directory; `stopLiveWriter` unknown).

- [ ] **Step 3: Implement** (in `CasSession`)

```groovy
    /** How a waiting run sleeps between looks at sweep.lock; a test seam. */
    static Closure<Void> liveSleeper = { long ms -> Thread.sleep(ms) } as Closure<Void>

    private LiveWriter liveWriter
    private ScheduledExecutorService liveBeats
    private Thread liveHook

    /** Ticket 20 answers 1 and 5: register as a Live Writer, then wait while a sweep holds the lock. */
    synchronized void startLiveWriter(String session, Map<String, String> info) {
        final BlockStore writable = members()[0]
        if( liveWriter != null || !(writable instanceof RetainedStore) || !writable.isWritable() )
            return
        liveBeats = Executors.newSingleThreadScheduledExecutor({ Runnable r ->
            final Thread t = new Thread(r, 'nf-blocks-live-heartbeat'); t.daemon = true; t } as ThreadFactory)
        liveWriter = new LiveWriter(((RetainedStore) writable).retentionStorage(), session, info,
            { String m -> ConsoleLog.LOG.warn(m) } as Closure<Void>, liveSleeper, liveBeats)
        liveHook = new Thread({ -> liveWriter?.close() } as Runnable, 'nf-blocks-live-deregister')
        Runtime.runtime.addShutdownHook(liveHook)
        liveWriter.start()
    }

    synchronized void stopLiveWriter() {
        liveWriter?.close()
        liveWriter = null
        liveBeats?.shutdownNow()
        liveBeats = null
        if( liveHook != null ) {
            try { Runtime.runtime.removeShutdownHook(liveHook) } catch( IllegalStateException e ) { /* already shutting down */ }
            liveHook = null
        }
    }
```

`ConsoleLog` lives in `robsyme.cas.trace`; `CasSession` already imports it for `checkClock`.

- [ ] **Step 4: Run to verify they pass**

Run: `./gradlew test --tests 'robsyme.cas.trace.*' --tests 'robsyme.cas.CasSessionTest'`
Expected: PASS; no other observer test changes (they run over local stores, which now also get `live/` during a test run and none after it).

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/robsyme/cas/CasSession.groovy src/main/groovy/robsyme/cas/trace/CasObserver.groovy \
  src/test/groovy/robsyme/cas/trace/CasObserverTest.groovy src/test/groovy/robsyme/cas/CasSessionTest.groovy
git commit -s -m "feat(trace): a run registers in live/, heartbeats, and waits while a sweep holds the lock (ticket 20 answer 5)"
```

---

### Task 11: The explorer releases, restores, pins and unpins

**Files:**
- Modify: `web/src/views.js` (`run`, `collection`, `item`, `content`; a shared `retentionPanel`)
- Modify: `web/src/model.js` (the run, collection and item loaders return the subject's claim state)
- Test: `web/test/views.test.mjs` (append), `web/test/model.test.mjs` (append if the loaders' shape is tested there)
- Browser: `gate/browser_b_assert.py` and `gate/browser/tier_b.sh` gain one step (Step 5)

**Interfaces:**
- Consumes: Task 1's `claimState` fields (`retain`, `retainClaims`, `released`, `pins`, `pinClaims`, `pinned`, `hidden`); Task 2's `writer.release/restore/pin/unpin`.
- Produces, in the rendered DOM (the stable hooks the tests and tier B use):
  - on a run: a badge `[data-badge="content-released"]` "content released" when `released`; `[data-badge="hidden-but-pinned"]` "hidden but pinned" when `hidden && pinned`; a button **Release content** (`#release`) when not released, **Restore content** (`#restore`) when released; a line explaining release: "Releasing keeps this run's lineage and lets a sweep reclaim its files after the grace period. Pinned items stay."
  - on a run, collection, item, and file or directory content page: a badge `[data-badge="pinned"]` "pinned" with the notes as a list `[data-pin]` (each `data-claim` its cid) and an **Unpin** button per note (`button[data-unpin]`); a **Pin** form: a text input `#pin-note` (placeholder "why keep this? e.g. figure 3") and a **Pin** button (`#pin`), disabled while the note is blank.
  - writes are offered where the page's existing rules offer Claims (`ctx.write.available` and the subject's member is the writable one, as `actions()` does for Selections at `views.js:543`); elsewhere the badges show and the buttons are replaced by the same unavailable note the Selection page uses.
  - after a write the view re-renders from the Claims the tail now holds (the same path rename and delete use), so the badge changes without a reload.

- [ ] **Step 1: Write the failing tests** (append to `web/test/views.test.mjs`, following its existing fake explorer and `ctx` builders, and the Selection-actions tests' fake writer that records calls)

```js
test('a released run shows the badge and Restore content; restore supersedes the release', async () => {
  const { ex, ctx, calls } = fixture({ runClaims: [{ cid: C1, verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [] }] })
  const node = await views.run(ex, RUN, ctx)
  assert.ok(node.querySelector('[data-badge="content-released"]'))
  assert.equal(node.querySelector('#release'), null)
  node.querySelector('#restore').click()
  await settle()
  assert.deepEqual(calls, [['restore', RUN, [C1]]])
})

test('a run not released offers Release content with its current retain claims', async () => {
  const { ex, ctx, calls } = fixture({ runClaims: [] })
  const node = await views.run(ex, RUN, ctx)
  node.querySelector('#release').click()
  await settle()
  assert.deepEqual(calls, [['release', RUN, []]])
})

test('a hidden run that is pinned says so', async () => {
  const { ex, ctx } = fixture({ runClaims: [
    { cid: C1, verb: 'delete', attribute: null, value: null, supersedes: [] },
    { cid: C2, verb: 'add', attribute: 'pin', value: 'paper', supersedes: [] }] })
  const node = await views.run(ex, RUN, ctx)
  assert.ok(node.querySelector('[data-badge="hidden-but-pinned"]'))
  assert.equal(node.querySelector('[data-pin]').textContent.includes('paper'), true)
})

test('pin asks for a note, and each pin can be removed on its own', async () => {
  const { ex, ctx, calls } = fixture({ itemClaims: [{ cid: C2, verb: 'add', attribute: 'pin', value: 'paper', supersedes: [] }] })
  const node = await views.item(ex, COLLECTION, ITEM, ctx)
  const button = node.querySelector('#pin')
  assert.equal(button.disabled, true)
  const note = node.querySelector('#pin-note')
  note.value = 'figure 3'; note.dispatchEvent(new Event('input'))
  assert.equal(button.disabled, false)
  button.click()
  node.querySelector(`button[data-unpin="${C2}"]`).click()
  await settle()
  assert.deepEqual(calls, [['pin', ITEM, 'figure 3'], ['unpin', ITEM, C2]])
})

test('outside the writable member the badges show and no write is offered', async () => {
  const { ex, ctx } = fixture({ runClaims: [{ cid: C1, verb: 'set', attribute: 'retain', value: 'lineage', supersedes: [] }], writable: false })
  const node = await views.run(ex, RUN, ctx)
  assert.ok(node.querySelector('[data-badge="content-released"]'))
  assert.equal(node.querySelector('#restore'), null)
  assert.ok(node.querySelector('[data-unavailable]'))
})
```

(`fixture`, `settle`, and the CID constants: extend the file's existing helpers so the fake explorer's `run`, `item`, `collection` and `content` loaders return `state` built with `claimState(claims)` from `../src/claims.js`, and the fake writer records `[method, ...args]`.)

- [ ] **Step 2: Run to verify they fail**

Run: `cd web && node --test test/views.test.mjs`
Expected: FAIL (no badges or buttons).

- [ ] **Step 3: Implement**

`model.js`: `run(completionCid)`, `item(collectionCid, itemCid)`, `collection(collectionCid, ...)` and the content view's loader each add `state: (await this.claimStates([cid])).get(cid)` for their subject (the run's completion cid, the item, the collection, the content cid). `claimStates` already merges the snapshot's Claims with the tail's.

`views.js`: one shared builder, used by the four views:

```js
/** Ticket 21 answer 6: pins on any subject; release and restore on a run. */
function retentionPanel(subject, state, ctx, { isRun = false } = {}) {
  const status = h('p', { class: 'muted', 'data-retention-status': '' })
  const can = ctx.write.available && ctx.write.here
  const badges = [
    isRun && state.released ? h('span', { class: 'badge', 'data-badge': 'content-released' }, 'content released') : null,
    isRun && state.hidden && state.pinned ? h('span', { class: 'badge', 'data-badge': 'hidden-but-pinned' }, 'hidden but pinned') : null,
    state.pinned ? h('span', { class: 'badge', 'data-badge': 'pinned' }, 'pinned') : null,
  ]
  const pins = state.pins.length ? h('ul', {}, state.pins.map(p => h('li', { 'data-pin': '', 'data-claim': p.cid }, p.note, ' ',
    can ? h('button', { type: 'button', 'data-unpin': p.cid,
      onclick: (e) => ctx.write.run(status, () => ctx.write.writer.unpin(subject, p.cid), e.currentTarget) }, 'Unpin') : null))) : null
  const note = h('input', { id: 'pin-note', type: 'text', maxlength: '256', placeholder: 'why keep this? e.g. figure 3' })
  const pin = h('button', { type: 'button', id: 'pin', disabled: true,
    onclick: (e) => ctx.write.run(status, () => ctx.write.writer.pin(subject, note.value.trim()), e.currentTarget) }, 'Pin')
  note.addEventListener('input', () => { pin.disabled = !note.value.trim() })
  const release = isRun ? (state.released
    ? h('button', { type: 'button', id: 'restore', onclick: (e) => ctx.write.run(status, () => ctx.write.writer.restore(subject, state.retainClaims), e.currentTarget) }, 'Restore content')
    : h('button', { type: 'button', id: 'release', onclick: (e) => ctx.write.run(status, () => ctx.write.writer.release(subject, state.retainClaims), e.currentTarget) }, 'Release content')) : null
  return h('section', { 'data-retention': subject },
    h('p', {}, badges),
    pins,
    can ? [
      isRun ? h('p', { class: 'muted' }, "Releasing keeps this run's lineage and lets a sweep reclaim its files after the grace period. Pinned items stay.") : null,
      h('p', {}, release, ' ', note, ' ', pin),
      status]
      : unavailableNote(ctx))
}
```

`ctx.write.run(status, fn, button)` and the unavailable note are what `actions()` (Selections) uses: read `views.js:543-575` and reuse them exactly (if the note is built inline there, lift it into `unavailableNote(ctx)` and call it from both). `ctx.write.here` is the "this subject is in the writable member" test the Selection page makes; compute it for a run from the run row's `member`, and for an item, collection or content page from the run it was reached through (or the writable member when the page cannot tell, so the write lands beside the Claims a sweep of that member reads). Add `retentionPanel(completionCid, state, ctx, { isRun: true })` to `run` below the `dl`, and `retentionPanel(<cid>, state, ctx)` to `collection`, `item` and `content` below their headings. Add `.badge` to the page CSS beside the existing muted styles: a small rounded outline, readable in both themes.

- [ ] **Step 4: Run to verify they pass**

Run: `cd web && node --test` then `./gradlew assemble` (the page is rebuilt into the jar).
Expected: PASS.

- [ ] **Step 5: One browser step in tier B**

In `gate/browser/tier_b.sh`, after the existing Selection steps, drive the run page of the Gate's `cold` run in the explore server: type a note, press **Pin**, wait for `[data-badge="pinned"]`; press **Release content**, wait for `[data-badge="content-released"]`; press **Restore content**, wait for it to go. In `gate/browser_b_assert.py`, add assertion B13 "pin, release and restore from the page": read the writable member's `log/` for the three new Claim entries and decode each block with `gate/cas.py` (hashing it, never trusting the page): an `add pin` whose value is the typed note and whose subject is the `cold` RunCompletion; a `set retain "lineage"`; a `del retain` superseding exactly that set. Follow the file's existing B-assertion structure and its `test_browser_b_assert.py` fixture pattern for a unit test of the new check.

- [ ] **Step 6: Commit**

```bash
git add web/src/views.js web/src/model.js web/test/views.test.mjs web/test/model.test.mjs \
  gate/browser/tier_b.sh gate/browser_b_assert.py gate/test_browser_b_assert.py
git commit -s -m "feat(web): Release content, Restore content, Pin and Unpin; pinned, content released and hidden but pinned badges (ticket 21 answer 6)"
```

---

### Task 12: Gate tier one, assertion 9

**Files:**
- Create: `gate/retention/main.nf`, `gate/retention/nextflow.config`, `gate/retention/overlay.config`, `gate/retention/grace0.config`
- Modify: `gate/gate.sh` (a retention section after the milestone 5 runs; the cleanup list; `store-retention`)
- Modify: `gate/assert.py` (assertion 9 registered; the `NOT_IN_SKELETON` row for 9 removed; three helper flags)
- Test: `gate/test_assert.py` (append)
- Modify: `gate/README.md` (assertion 9 and the new store)

**Interfaces:**
- Consumes: the verbs of Task 9, `put` of Task 2, the lock and registrations of Tasks 5 and 10, all through the real CLI.
- Produces: `python3 gate/assert.py <GATE_ROOT> --retention-refs` (shell assignments `RET_A`, `RET_B`, `RET_PIN_ITEM`, `RET_RELEASE`, `RET_RESTORE`: the two RunCompletions, `b`'s `pin_b` item, the newest current `retain` Claim on `RET_B`); `--retention-age <days>` (backdates every block of `store-retention` by that many days with `os.utime`, since a local member's clock is its mtimes); `--retention-checkpoint <name>` (writes `logs/retention/<name>.json`: every block address present, every ledger's parsed JSON by name, the lock body, the `live/` names).

Assertion 9's wording (ticket 21 answer 7), which is also its title row: "A dry-run sweep straight after a run reports an empty dead set; after `set retain "lineage"` on one run, a real sweep trashes exactly its unshared content and none of its metadata, while a pinned item in it and content it shares with another run survive; `del retain` before the deadline brings the content back as live without `untrash`; a later sweep past the deadline deletes what is still released."

The in-repo pipeline has its own store (`store-retention`), as milestone 5's did, so no other assertion or browser tier sees a sweep. Run `b` goes first so `prune --keep-last 1` releases it.

- [ ] **Step 1: The pipeline**

`gate/retention/main.nf`:

```nextflow
// Milestone 6's Gate pipeline (plan 2026-09-30): two runs (--tag b, then --tag a) that share
// one file and one file inside a directory, and each hold files of their own. Its own store,
// store-retention, so no other assertion or browser tier sees a sweep.
params.tag = 'a'

process FILE {
    input:
    val name

    output:
    tuple val(meta), path("${name}.txt")

    script:
    meta = [id: name]
    """
    printf '%s\\n' ${name} > ${name}.txt
    """
}

process DIR {
    output:
    tuple val(meta), path("dir_${params.tag}")

    script:
    meta = [id: "dir_${params.tag}"]
    """
    mkdir dir_${params.tag}
    printf '%s one\\n' ${params.tag} > dir_${params.tag}/one.txt
    printf 'shared two\\n' > dir_${params.tag}/two.txt
    """
}

workflow {
    main:
    files = FILE(channel.of('shared', "only_${params.tag}", "pin_${params.tag}"))
    dirs = DIR()

    publish:
    files = files
    dirs = dirs
}

output {
    files {
        path { meta, f -> "files/${meta.id}" }
    }
    dirs {
        path { meta, d -> "dirs/${meta.id}" }
    }
}
```

`nextflow.config`: `lineage.enabled = true`. `overlay.config`: `manifest.name = 'cas-gate-retention'`. `grace0.config`: `cas.sweep.grace = '0s'` with a comment: "the last two sweeps of assertion 9: trash with a deadline of now, then delete; a real store keeps the 14-day default".

- [ ] **Step 2: The gate.sh section** (after the two `outputs` runs; add `store-retention`, `retention` and `retention-repo` to the `rm -rf` and `mkdir` lists at the top)

```bash
# --------------------------------------------------------------------------
# Assertion 9 (milestone 6): release, pin, sweep, restore, delete past the deadline
# --------------------------------------------------------------------------

RET="$GATE_ROOT/retention"; rm -rf "$RET"; mkdir -p "$RET" "$GATE_ROOT/logs/retention"
cp "$REPO/gate/retention/main.nf" "$REPO/gate/retention/nextflow.config" "$RET/"
GATE_STORE="$GATE_ROOT/store-retention" run "$RET" retention-b --tag b -c "$REPO/gate/retention/overlay.config"
GATE_STORE="$GATE_ROOT/store-retention" run "$RET" retention-a --tag a -c "$REPO/gate/retention/overlay.config"

ret_json="$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$GATE_ROOT/retention-repo")"
verb() {   # <step name> [-c extra.config] <verb and args...>: never aborts; exit code in logs/retention/<step>.exit
    local step="$1"; shift
    local extra=()
    if [[ "${1:-}" == "-c" ]]; then extra=(-c "$2"); shift 2; fi
    local log="$GATE_ROOT/logs/retention/$step" status=0
    ( cd "$RET" && unset NXF_OFFLINE && export GATE_STORE="$GATE_ROOT/store-retention" \
        NXF_PLUGINS_TEST_REPOSITORY="file://$ret_json" XDG_CACHE_HOME="$GATE_ROOT/cache-retention" \
      && "$NEXTFLOW" -q -c "$REPO/gate/gate.config" -c "$REPO/gate/retention/overlay.config" "${extra[@]+"${extra[@]}"}" \
           plugin "nf-blocks:$1" "${@:2}" ) > "$log.out" 2> "$log.err" || status=$?
    echo "$status" > "$log.exit"
    echo "    nf-blocks:$1 ($step) exit $status"
}
checkpoint() { python3 "$REPO/gate/assert.py" "$GATE_ROOT" --retention-checkpoint "$1"; }
claim() {   # <step name> <subject> <verb> <attribute|null> <value json|null> <supersedes cid|->
    local sup='[]'; [[ "$6" != "-" ]] && sup="[{\"/\":\"$6\"}]"
    local attr='null'; [[ "$4" != "null" ]] && attr="\"$4\""
    printf '{"kind":"Claim","subject":{"/":"%s"},"verb":"%s","attribute":%s,"value":%s,"supersedes":%s,"timestamp":"%s"}\n' \
        "$2" "$3" "$attr" "$5" "$sup" "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)" > "$GATE_ROOT/logs/retention/$1.request"
    verb "$1" put "$GATE_ROOT/logs/retention/$1.request"
}
refs() { eval "$(python3 "$REPO/gate/assert.py" "$GATE_ROOT" --retention-refs)"; }

checkpoint after-runs
verb dry-1 sweep --format json
checkpoint after-dry-1
verb prune-dry prune --keep-last 1
verb prune prune --keep-last 1 --apply true
refs
claim pin "$RET_PIN_ITEM" add pin '"figure 3"' -
# A fresh registration refuses --apply; once stale (11 minutes by its mtime) it is ignored and deleted.
mkdir -p "$GATE_ROOT/store-retention/live"
printf '{"session":"gate-fake","run_name":"gate_fake_run","pipeline":"x","started_at":"x"}' > "$GATE_ROOT/store-retention/live/gate-fake"
verb live-refused sweep --apply true
touch -t "$(date -v-11M +%Y%m%d%H%M.%S 2> /dev/null || date -d '-11 minutes' +%Y%m%d%H%M.%S)" "$GATE_ROOT/store-retention/live/gate-fake"
python3 "$REPO/gate/assert.py" "$GATE_ROOT" --retention-age 15
checkpoint before-sweep
verb sweep-1 sweep --apply true --format json
checkpoint after-sweep-1
verb untrash untrash "$(python3 "$REPO/gate/assert.py" "$GATE_ROOT" --retention-untrash-pick)"
checkpoint after-untrash
refs
claim restore "$RET_B" del retain null "$RET_RELEASE"
verb sweep-2 sweep --apply true --format json
checkpoint after-sweep-2
refs
claim release-again "$RET_B" set retain '"lineage"' "$RET_RESTORE"
python3 "$REPO/gate/assert.py" "$GATE_ROOT" --retention-age 15
verb sweep-3 -c "$REPO/gate/retention/grace0.config" sweep --apply true --format json
checkpoint after-sweep-3
verb sweep-4 -c "$REPO/gate/retention/grace0.config" sweep --apply true --format json
checkpoint after-sweep-4
```

(`--retention-untrash-pick` prints the address of `b`'s `one.txt` from the one ledger: add it to the helper flags. `--retention-age` before sweep-3 ages the new Claims too, which changes nothing: they are live. `RET_RESTORE` is the `del retain` the restore step wrote. The `date -v` / `date -d` pair covers macOS and Linux.)

- [ ] **Step 3: Write the failing tests for the assertion** (append to `gate/test_assert.py`, following its existing fixtures that build a tiny store directory with real DAG-CBOR blocks via `cas.encode`)

```python
class RetentionAssertionTest(unittest.TestCase):
    """Assertion 9 over a hand-built GATE_ROOT: checkpoints, ledgers and a store."""

    def test_passes_when_every_checkpoint_matches(self):
        root = build_retention_root(self)          # writes store-retention, logs/retention/*.json, *.exit, *.out
        status, message = assert_mod.assertion_9(assert_mod.Gate(root))
        self.assertEqual(status, assert_mod.PASS, message)

    def test_fails_when_a_shared_block_was_trashed(self):
        root = build_retention_root(self, trash_extra="shared_two")
        status, message = assert_mod.assertion_9(assert_mod.Gate(root))
        self.assertEqual(status, assert_mod.FAIL)
        self.assertIn("shared", message)

    def test_fails_when_metadata_was_trashed(self):
        root = build_retention_root(self, trash_extra="b_item")
        status, message = assert_mod.assertion_9(assert_mod.Gate(root))
        self.assertEqual(status, assert_mod.FAIL)
        self.assertIn("metadata", message)

    def test_fails_when_the_dry_run_found_dead_blocks(self):
        root = build_retention_root(self, dry_dead=3)
        self.assertEqual(assert_mod.assertion_9(assert_mod.Gate(root))[0], assert_mod.FAIL)

    def test_fails_when_restored_content_stayed_in_a_ledger(self):
        root = build_retention_root(self, keep_ledger_after_restore=True)
        self.assertEqual(assert_mod.assertion_9(assert_mod.Gate(root))[0], assert_mod.FAIL)

    def test_fails_when_the_last_sweep_left_released_content(self):
        root = build_retention_root(self, survivor="only_b")
        self.assertEqual(assert_mod.assertion_9(assert_mod.Gate(root))[0], assert_mod.FAIL)

    def test_fails_when_the_live_refusal_did_not_refuse(self):
        root = build_retention_root(self, live_refused_exit=0)
        self.assertEqual(assert_mod.assertion_9(assert_mod.Gate(root))[0], assert_mod.FAIL)
```

`build_retention_root(test, **faults)` is a helper in the test file: it builds two runs' blocks the way the plugin would (RunCompletion, RunManifest, OutputCollections `files` and `dirs`, items with Leaf maps, a DirectoryManifest per run, raw blocks for the bytes the pipeline writes), the Store Log entries, the Claims (`set retain` on b by prune, `add pin` on b's `pin_b` item, `del retain`, `set retain` again), and the checkpoint JSON files and exit files an honest run would leave, then applies the named fault. Write it once, in full, with `cas.encode` and `cas.cid_dagcbor`/`cas.cid_raw`.

- [ ] **Step 4: Run to verify they fail**

Run: `cd gate && python3 -m unittest test_assert.RetentionAssertionTest -v`
Expected: FAIL (`assertion_9` undefined).

- [ ] **Step 5: Implement assertion 9 and the helper flags** (in `gate/assert.py`)

Structure (each step's problem appended to one list; PASS when it is empty, else FAIL with the first five):

```python
RETENTION_STORE = "store-retention"
RETENTION_IDENTITY = "cas-gate-retention"


def retention_runs(store):
    """{'a': (cid, block), 'b': (cid, block)} from store-retention's Store Log, by run name suffix."""


def content_closure(store, completion_block, only_items=None):
    """Every content address under a run (or under only_items): each item's Leaf addresses,
    and for a DirectoryManifest leaf, the manifest and everything under it. Every block is
    read through cas.Store.read, which re-hashes it."""


def metadata_closure(store, completion_cid):
    """The RunCompletion, its RunManifest and script, its collections and items."""


@assertion(9, "sweep and trash")
def assertion_9(gate):
    store = cas.Store(os.path.join(gate.root, RETENTION_STORE))
    logs = os.path.join(gate.root, "logs", "retention")
    problems = []
    runs = retention_runs(store)
    a, b = runs["a"], runs["b"]
    pinned_item = ...          # b's item whose Leaf name is pin_b.txt
    shared_with_a = content_closure(store, a[1])
    unshared = content_closure(store, b[1]) - shared_with_a - content_closure(store, b[1], only_items=[pinned_item])
    meta_b = metadata_closure(store, b[0])
    # 1. dry run: exit 0, applied false, dead 0; after-dry-1 identical to after-runs (no trash, no lock)
    # 2. prune: a set retain "lineage" Claim on b's RunCompletion in the Store Log (decoded, hashed), none on a
    # 3. live-refused: exit 1, its output names gate_fake_run
    # 4. after-sweep-1: exactly one ledger; its blocks == unshared; no address of meta_b in it;
    #    every block of before-sweep still present; the live/ fake registration gone; lock state released
    # 5. after-untrash: the one ledger without the picked address, nothing else changed
    # 6. after-sweep-2: no ledger; every unshared block present (brought back without untrash)
    # 7. after-sweep-3: one ledger whose blocks == unshared
    # 8. after-sweep-4: no unshared block present; every block of meta_b, of a's closures, of the pinned
    #    item's content, and b's shared two.txt present; no ledger; lock released; live/ empty
    ...
    if problems:
        return FAIL, "; ".join(problems[:5])
    return PASS, ("released run's %d unshared blocks trashed, restored by del retain, then deleted past the "
                  "deadline; its %d metadata blocks, the pinned item and shared content kept"
                  % (len(unshared), len(meta_b)))
```

Fill every function in full. The expected sets come only from blocks read and re-hashed by `cas.Store`, never from the plugin's JSON report; the JSON is read only for `applied`, `dead` and exit status of the dry run. Remove the `(9, "sweep and trash", ...)` row from `NOT_IN_SKELETON` and update the module docstring's "Numbers 8, 9, 11 and 12" to "8, 11 and 12". Add `--retention-refs`, `--retention-age`, `--retention-checkpoint` and `--retention-untrash-pick` to `main()`'s `known` flags and dispatch.

- [ ] **Step 6: Run the tests, then the Gate**

Run: `cd gate && python3 -m unittest -v` (every test), then in the background with a long timeout: `make gate GATE_ROOT=$CLAUDE_JOB_DIR/tmp/gate-m6` (or the job's scratch dir; never `/tmp` shared with other jobs).
Expected: unit tests PASS; the Gate reports `PASS   9  sweep and trash`, lineage 16 PASS, 0 FAIL, 5 SKIP, and the browser tiers as before (A 5/5, B 13/13 with Task 11's B13). Browser A1 and lineage assertion 4 can flake on the documented chmod race: rerun once before debugging.

- [ ] **Step 7: Commit**

```bash
git add gate/retention gate/gate.sh gate/assert.py gate/test_assert.py gate/README.md
git commit -s -m "test(gate): assertion 9, release, pin, sweep, restore and delete past the deadline (ticket 21 answer 7)"
```

---

### Task 13: Gate tier two, T7 on an S3 member

**Files:**
- Modify: `gate/tier2/tier2.sh` (T7's runs early, its sweep steps after TS)
- Modify: `gate/tier2/assert_tier2.py` (`t7`, and a `t7-refs` subcommand as `t6-refs` is)
- Test: `gate/tier2/test_assert_tier2.py` (append)
- Modify: `gate/tier2/README.md`

**Interfaces:**
- Consumes: Task 12's pipeline, `grace0.config` and step sequence; tier two's `run_nf`, `verb`, member prefixes and teardown.
- Produces: check `T7` in the tier-two table: the same eight checks as assertion 9 on a fresh member prefix `cas-t7` of the throwaway bucket, with the retention pipeline run locally (the local executor: T7 is about the S3 member, not Batch), plus: `sweep.lock` was taken and released with conditional writes (its final body `released`), the ledger is an object under `cas-t7/trash/`, the deletion used `DeleteObjects`, and a stale `live/` object (its `LastModified` 11 minutes old by S3's clock) was deleted.

S3 `LastModified` cannot be backdated, so T7 uses `cas.sweep.ageFloor = '10m'` (the minimum, decision 8) in a `t7.config`, runs its two pipeline runs before T1 starts, and does its sweep steps after TS, by which time its blocks are well over 10 minutes old (tier two runs about 40 minutes). If T1 to TS ever take under 11 minutes, T7 waits for the remainder, printing why. The fake registration is written at the start of T7's runs and is stale by the sweep steps; the refusal check therefore writes a second, fresh one, and deletes it again before sweep-1.

- [ ] **Step 1:** Write `t7` in `assert_tier2.py` reusing Task 12's closure logic (import it from `gate/assert.py`, or move `content_closure`/`metadata_closure` into `gate/cas.py` and import them from both), reading blocks from the S3 member through `s3gate.py`'s existing member reader, which re-hashes. Its unit test in `test_assert_tier2.py` follows the file's existing fake-member pattern: one passing member and one fault (a shared block ledgered).
- [ ] **Step 2:** Add the T7 steps to `tier2.sh`: `produce t7b cas-t7 "$REPO/gate/retention" -c t7.config` and `t7a` right after tier two's setup (with `--tag`), then after `ts` the checkpoint/verb/claim sequence of Task 12 Step 2 with `verb` pointed at `cas-t7` and every checkpoint read from S3 by `assert_tier2.py t7-checkpoint <name>`.
- [ ] **Step 3:** Run the tier-two unit tests: `cd gate/tier2 && python3 -m unittest -v`. Expected PASS. Do not run tier two itself: Rob runs it (it needs his SSO session and spends money).
- [ ] **Step 4:** Update `gate/tier2/README.md` (T7 row, the 10-minute age floor, the wait) and commit:

```bash
git add gate/tier2
git commit -s -m "test(tier2): T7, the retention sequence on an S3 member (ticket 21 answer 7)"
```

---

### Task 14: Documentation and acceptance

**Files:**
- Modify: `DESIGN.md`: §0 rule 5 (the three verbs landed), §1 (package list: the retention classes in `core`, `S3RetentionStorage` in `s3`), §2 (`cas.sweep.ageFloor`, `cas.sweep.grace`), §5 (the layout gains `sweep.lock`, `live/<session>`, `trash/<deadline>-<sweep id>`; the hardening bucket policy's "`DeleteObject` on `blocks/*` except to a sweep role" now names what the sweep role needs: `s3:DeleteObject` on `blocks/*`, `log/*`, `live/*`, `trash/*`, `tmp/*`, `s3:AbortMultipartUpload`, `s3:ListBucketMultipartUploads`), §12 (`Index.forget`, `runsOf`, `pipelines`; claim state's `retain` and `pin` groups under decision 5), §14 (assertion 9 PASS; the Gate's counts), §15 "Plugin verbs" (the three usage lines and exit codes), and a new **§19 Milestone 6: retention (2026-09-30)** holding: what was built, with a pointer to each ticket answer; this plan's decisions 1 to 15 plus the ones made in Tasks 8 and 10 (prune counts every successful run toward N whatever its Claims; the registration is written at `onFlowCreate`, Session.groovy:606); what is documented and not protected (a pipeline that only reads a member; a Claim or Selection written between a sweep's last re-check and a delete batch); and a status line.
- Modify: `README.md`: a "Getting space back" section: releasing a run's content (`put` with `set retain "lineage"`, or the explorer's **Release content**), pinning (`add pin "<note>"`, the explorer's **Pin**), `prune`, `sweep` (dry run first; `--apply`; `--wait`; `--budget`), `untrash`, the grace period and the age floor, the rule that a sweep never runs beside a running pipeline and that a run waits for a sweep, and the S3 permissions a sweeper needs. Give one complete example session:

```bash
nextflow plugin nf-blocks:prune --keep-last 3            # what would be released
nextflow plugin nf-blocks:prune --keep-last 3 --apply true
nextflow plugin nf-blocks:sweep                          # dry run: roots, live, dead, Trash
nextflow plugin nf-blocks:sweep --apply true             # trash the newly dead; delete what is past its grace
```

- Modify: `gate/README.md` if Task 12 left it out of step.

- [ ] **Step 1:** Write the documentation above. Keep DESIGN's voice: facts, decisions with their reasons, no restating of tickets beyond a pointer.
- [ ] **Step 2:** Full verification, fresh: `./gradlew check` (every unit test; the dependency check), `cd web && npm test`, `cd gate && python3 -m unittest -v`, `cd gate/tier2 && python3 -m unittest -v`, then `make gate` on a fresh `GATE_ROOT` under the job's scratch directory, in the background with a long timeout. Expected: all green; the Gate 16 PASS 0 FAIL 5 SKIP, A 5/5, B 13/13.
- [ ] **Step 3:** Record the counts (unit tests, web tests, Gate rows) in DESIGN §19's status line: "Built on `feat/m6-retention`; `make gate` 16/0/5, A 5/5, B 13/13; tier two (T1 to T7, TS) not yet run: Rob runs `make gate-tier2`."
- [ ] **Step 4:** Commit:

```bash
git add DESIGN.md README.md gate/README.md
git commit -s -m "docs: DESIGN §19 milestone 6 (retention); README: getting space back"
```

The branch is not merged here. Rob runs tier two (`GATE_PYTHON=<venv python> AWS_PROFILE=scidev make gate-tier2 GATE_ROOT=<the root that passed make gate>`), then decides the merge and the release (the recipe in `../.scratch/post-gate/m6-handoff.md`).
