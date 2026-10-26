# P1b report: identity (explorer v0.3)

Branch `explorer/p1b-identity`, based on `explorer/v0.3` at `185a2f3`. Code head `7204bde`. This report, the screenshots and the live-check scripts are committed on top.
Plan: `docs/plans/2026-10-24-explorer-v0.3-p1b-identity.md`. Spec: `docs/design/03-agent-prompts.md` (preamble + P1b) and the P1b addendum (S2 release claims, read-only pins, walk/shoot paths).

## What was built

**Plugin.**
- `asserted_by` is now a resolved principal, `Principal(name, source)`, worked out once per session by `Identity.resolve` (`src/main/groovy/robsyme/cas/Identity.groovy`).
- The order is an explicit `cas.asserted_by`, then Seqera Platform (`GET <endpoint>/user-info` when `tower.accessToken` or `TOWER_ACCESS_TOKEN` is set), then `NXF_BLOCKS_USER`, then `git config user.email`, then `anonymous` with a warning.
- A blank value, or one that says `anonymous`, counts as unset at every step.
- The Platform client (`PlatformIdentity.groovy`) and the git lookup are seams with test doubles. The Platform read has a 5 s bound on the whole response, and git has a 2 s bound. A Platform failure warns and falls through.
- Only the name goes into blocks. The source goes to the console and to `members.json`.

**Console.**
- A run and `nf-blocks:explore` print `nf-blocks: signing as <name> (from <source>)` once, or the anonymous warning, through the `nextflow.cas` console logger.
- Other plugin verbs (`put`, `items`, `sweep` and the rest) keep stdout clean. The line goes to debug, and identity warnings go to stderr.

**Explore server.** `members.json` gains `identity: {principal, source}`. Older servers simply omit it.

**Page.**
- `resolveStore` reads `identity`. Views get `ctx.identity` and `ctx.viewer`. `ctx.viewer` is the principal string, or null when there is no identity or it is anonymous. P2 reads the same seam.
- The top bar shows the viewer after Picked. An anonymous identity gives a one-line warning chip instead. Both link to the new `#/identity` page, which shows who you sign as, where that came from and the five-step order.
- The anonymous avatar is the kit's dashed `?`, and it links to `#/identity`.
- Claim ages use the viewer's local time zone, which closes the P0 deferred "UTC" item. The Selection header's name tag reads `Oct 24, 8:36 PM`, the mockup's format.
- "Mine" is an All / Mine toggle on a pipeline's runs and on the Selections list, filtered in SQL (`…By` queries on `run.asserted_by` and `collection.asserted_by`) and on tail rows, trimmed and case-insensitive.

**Docs.** The resolution order is in `docs/identity.md`, with pointers from `README.md` and `DESIGN.md` §2.

## The spec items

| Item | Status | Evidence |
|---|---|---|
| `asserted_by` is a real principal, resolved Platform -> `NXF_BLOCKS_USER` -> git email -> anonymous | Met, with explicit `cas.asserted_by` first (orchestrator's ruling) | `IdentityTest` (doubles and loopback HTTP: 200, 401, header timeout, stalled body). Live run below: each source resolved through the real `nf-blocks:explore` |
| `anonymous` with a visible warning | Met | Live, anonymous start: `WARN: nf-blocks: no identity is configured, so this run's blocks and claims will say 'anonymous'. Set NXF_BLOCKS_USER or git config user.email (see docs/identity.md)`. On a CLI verb it goes to stderr. Top-bar warning chip, `#/identity` |
| Initials avatar + email on every claim and Selection | Met (email on hover on claim tags, see the rulings) | Claim tags on name, pin, release and deletion claims, on the run, item, Selection, home and Selections pages. The Selections list and "assembled by" show the avatar plus the email as text |
| "Mine" filter on runs and Selections | Met | `web/test/mine.test.mjs` (SQL and tail, case and whitespace, static page, anonymous). Live: Mine on roblab shows "None of these runs were signed by ada.lovelace@example.org.", which is true, see below |
| Document the resolution order | Met | `docs/identity.md`. It says that an explicit `cas.asserted_by` overrides a verified Platform identity, that only the name is stored, what each command prints, and what Mine compares |
| Mockup 08: no "anonymous" when an identity is configured; otherwise the dashed avatar and a link to set identity | Met | `build-08-selection-*.png`. After the live rename, the header reads "RS named Oct 24, 8:36 PM". `build-identity-anonymous-*.png` and `build-pipeline-anonymous-*.png` show the anonymous state |
| Addendum S2: release claims as claim tags with who and when | Met | `run.js` Released chip + `released` claim tag. `common.js` `content released` badge wraps a claim tag (`data-badge` kept). `item.js` Run released + tag. `claimState().releaseClaim` (JS only; `ClaimState.groovy` and the vectors are unchanged) |
| Addendum S2: read-only pins list on the Selection page | Met | `pinArea` renders a Pins panel of claim tags with no form and no Unpin when the viewer can't write here. `pages.test.mjs` |
| Addendum: walk.mjs and shoot.mjs resolve Playwright from their own repo | Met | `5912f5a`. `shoot.mjs` took this phase's mockup shots with `NF_BLOCKS_PLAYWRIGHT` |
| Platform path behind a seam, with a test double | Met, and tested live | A token was in my shell, so I made one read-only `GET /user-info` through the real explore start. It printed `nf-blocks: signing as ada.lovelace@example.org (from Seqera Platform)`, and `members.json` said `{'principal': 'ada.lovelace@example.org', 'source': 'platform'}`. `make test` blanks `TOWER_*` for every Test task, so the suite never reaches Platform |

**Live check.** I ran the real `nextflow plugin nf-blocks:explore` (Nextflow 26.04.6, scratch `NXF_HOME` and `NXF_PLUGINS_DIR`, nothing written to `~/.nextflow`) over a scratch copy of roblab. The script is `docs/design/screenshots/p1b/live.sh`, with `drive.mjs` beside it.

| Start | Console | members.json `identity` |
|---|---|---|
| `NXF_BLOCKS_USER=ada.lovelace@example.org` | `nf-blocks: signing as ada.lovelace@example.org (from NXF_BLOCKS_USER)` | `ada.lovelace@example.org`, `env` |
| no token, no env, no git identity | the anonymous WARN line above | `anonymous`, `anonymous` |
| git only | `nf-blocks: signing as ada.lovelace@example.com (from git config user.email)` | `ada.lovelace@example.com`, `git` |
| Platform token | `nf-blocks: signing as ada.lovelace@example.org (from Seqera Platform)` | `ada.lovelace@example.org`, `platform` |

- In the first start, the page renamed wigshop through `/api/put`. The new name Claim in the copy's log is signed `ada.lovelace@example.org`, and the header tag's avatar title is that principal. No page errors were reported.
- `sweep --format json` on the same copy prints one JSON object on stdout and nothing from nf-blocks besides it. Nextflow itself prints its "un-official plugin repository" banner, because the check uses `NXF_PLUGINS_TEST_REPOSITORY`. The anonymous warning went to stderr.

**roblab and "Mine".** Every run, Selection and claim in roblab was written with the old default, `anonymous`. So with any real identity, Mine correctly shows nothing on roblab. The only signed claim in these screenshots is the one the live check wrote into its scratch copy. I did not edit or fake any records to make Mine show rows. The model tests cover non-empty Mine.

## Deliberate differences from the mockups
- An avatar's colour comes from a hash of the principal (`toneFor`), so `ada.lovelace@example.org` is blue. The mockups' purple `rs` is a fixed demo tone, which the `#/kit` page still uses.
- Claim tags show the email on hover (`title` and the accessible name) and not as visible text, as in mockups 01 and 08 and design doc §1 ("email on hover"). The email is visible text in the top bar, on `#/identity`, in "assembled by" and in the Selections list's by column.
- The Selection rail's Claims section lists name claims only, followed by "No pins in roblab.". Pins and deletions have their own panels on the page. The mockup's "Held while distraught_murdock is held." line isn't shown.
- The All / Mine toggle and the `#/identity` page have no mockup. They are built from kit parts (`seg-toggle`, `panel`, `note`). Without explore there is no identity, and Mine isn't shown. With an anonymous identity, Mine is a disabled button with a reason.
- The pipeline runs table gained a narrow "By" column (avatar only).
- Carried over from P0 and later phases: the header reads "Held", not "Held by run". The Selection page has no Copy to... (P3) or Tidy samplesheet (P4). Fonts fall back to the system fonts.

## Screenshots
Everything is in `docs/design/screenshots/p1b/`, at 1440x900, in light and dark:
- Mockups: `mock-01-components-*` and `mock-08-selection-*`.
- Build (the fixed head, through the real explore server): `build-01-kit-*` (`#/kit`), `build-08-selection-*` (wigshop after the signed rename), `build-identity-*`, `build-identity-anonymous-*`, `build-pipeline-all-*`, `build-pipeline-mine-*`, `build-pipeline-anonymous-*` (the top-bar warning chip and the disabled Mine), and `build-selections-mine-*`.

## Tests (commands and results at `7204bde`)
```
$ make test
BUILD SUCCESSFUL in 56s        (1404 Gradle tests, 0 failures, 0 errors, per build/test-results)

$ cd web && npm test
ℹ tests 563
ℹ pass 563
ℹ fail 0                       (523 at the P0 base)

$ cd web && npm run build
dist/index.html 1704751 bytes

$ python3 -m unittest discover -s gate
Ran 333 tests in 9.481s
OK (skipped=1)
```
The Gate, with Nextflow 26.04.6, a scratch `NXF_HOME` and `GATE_ROOT`, and `TOWER_*` unset. The Gate configs pin `asserted_by = 'gate'`:
```
$ gate/gate.sh                 (exit 0)
lineage tier:      18 PASS, 0 FAIL, 5 SKIP
browser tier A:    6 PASS, 0 FAIL   (A2 producers 8 req / 32768 B, latest 4 req / 16384 B, items 43 req; A4 250 block fetches, all hash-verified)
browser tier B:    14 PASS, 0 FAIL  (B10 rename/delete/undo Claims, B12 token/Origin refusals, B20 Pin/Release/Restore)
```
P0's one lineage FAIL (assertion 4, issue 17) did not recur in this run.

## Schema and format changes
- Index schema is unchanged at v3. The four new queries read `run.asserted_by` and `collection.asserted_by`, which already existed.
- Block and Store Log formats are unchanged. What changes is the value: new blocks carry the resolved principal where they used to carry `anonymous`, unless `cas.asserted_by` is set.
- `members.json` gains an optional `identity` object. This is additive, and the Gate passes.
- Console behaviour changes. A run prints one "signing as" line. Plugin CLI verbs send identity warnings to stderr.
- `build.gradle`: every Test task blanks `TOWER_ACCESS_TOKEN` and `TOWER_API_ENDPOINT` and sets `NXF_BLOCKS_USER=test`.

## Rulings made during the phase
Each ruling lists what it costs if it's wrong.
- Tasks 1, 3 and 4 ran in parallel in isolated worktrees, because their files were disjoint, and I merged them. If wrong, the cost is merge work, and all merges were clean.
- An explicit `cas.asserted_by` wins over Platform. The orchestrator confirmed this and is checking it with the user. If wrong, it is a one-line reorder plus doc.
- Claim tags show the principal on hover, not as visible text, except in the Selections list, "assembled by", the top bar and `#/identity`. This follows mockups 01 and 08 and the design doc. If wrong, claim tags need the email shown and get wider.
- Mine is hidden without an identity (a static page) and disabled when anonymous. No mockup covers it. If wrong, a static viewer can't filter by person.
- `ctx.viewer` is a principal string or null, and also null when anonymous. It is the one viewer seam, shared with P2, and Mine filters by it. The orchestrator asked for this. If wrong, P2 shows a "Yours" bar for anonymous viewers.
- Task 1 could edit `build.gradle`'s Test env, which was outside its file list. That was the only way to stop `make test` sending a developer's real Platform token. If wrong, the cost is nothing beyond one extra file in the diff.
- Explore adds no second signing line, because CasSession's `nextflow.cas` line already reaches the terminal there. The live check confirmed it. If wrong, explore prints it twice.
- A configured `cas.asserted_by` or `NXF_BLOCKS_USER` that says `anonymous` counts as unset. README used to tell people to write it, and the design doc calls "anonymous" a bug. If wrong, someone who typed anonymous on purpose is signed by git or Platform instead.
- The signing line and identity warnings reach the console only for runs and explore. Other plugin verbs log the line at debug and send the warnings to stderr, so `--format json` stdout stays one object. If wrong, CLI users don't see who signed a `put`.
- The rail's Claims section omits pins and deletions, and its rows carry `data-rail-claim` only, so `[data-claim]` stays unique. If wrong, the rail is a less complete history.

## Open questions and follow-ups
- The order with `cas.asserted_by` first is waiting on the user's confirmation through the orchestrator.
- Pre-existing, not changed here: `CasSession.warnOnce` (used by the S3 storage-class warning) logs through `robsyme.cas.CasSession`, which never reaches the console. Clock-skew warnings on command sessions go through `nextflow.cas`, so they can still land on stdout under `sweep --format json`.
- Command-session identity warnings go to `System.err`, not to the `err` stream the command methods receive, so a test that injects `err` won't see them.
- I didn't check whether debug lines from `robsyme.cas` reach `.nextflow.log` during `nextflow plugin` commands. The docs say only that the line is "logged at debug level and never printed".
- SQLite `lower()` folds ASCII only, so a non-ASCII principal that differs only in case matches on tail rows but not in the snapshot. `docs/identity.md` says so.
- Parked minors from the task reviews, triaged by the final review as able to wait:
  - a new HttpClient per Platform call, and git process streams that aren't closed after a timeout
  - session I/O inside `computeIfAbsent`, now bounded at about 7 s
  - four top-level types in `Identity.groovy`
  - the rail shows a bare "set" verb for attributes other than name, which matters once P4 adds attributes
  - the release-claim tests build their claims inline
  - `store.js` passes a non-object `identity` through (`isAnon` copes with it)
  - the kit's Avatar signature line doesn't mention `link`
  - the view tests stub the Mine filtering, and the model tests cover the SQL
  - the `by ? SQL.xBy : SQL.x` ternary appears four times
  - the identity console-line test doesn't reset its dedupe set in cleanup

---

## Follow-up round (after the merge at 23c4c91)

Branch `explorer/p1b-identity`, continuing from `9152fdd`. Code head `82e702b`; this addendum and the re-shot screenshots are committed on top.
Plan: `docs/plans/2026-10-24-explorer-v0.3-p1b-identity-followup.md`.

### The orchestrator's items

| Item | Status | Evidence |
|---|---|---|
| S1. Resolve identity lazily, so only verbs that sign pay for it. Print "signing as" to stderr for those verbs | Met | `CasSession.getPrincipal()` resolves on first use and is memoised under one lock. `Put` and `Sweep` take a `Supplier<String>`. items, snapshot, reindex, untrash and dry-run sweep/prune never resolve. put, `sweep --apply` and `prune --apply` resolve when they first write. A run resolves at `onFlowBegin`. Explore listens, prints the URL, then resolves on a background thread, and `/members.json` waits for that result. Live: `items --format json` with a dummy token and a Platform listener that never answers took 1975 ms, wrote no `nf-blocks:` line to stderr, and produced one JSON document. Explore printed the URL, then `nf-blocks: signing as ada.lovelace@example.org (from NXF_BLOCKS_USER)` |
| S2. The rail's Claims list keeps history | Met | Current name claims come first, then every superseded claim, muted and newest first. Each row has avatar, verb, value and age, uses `.claim.old` and is titled "Superseded". Superseded pins, releases and deletions say "pinned", "released" or "deleted". Live: after two renames, the rail shows current RS "wigshop", superseded RS "wigshop-draft" and the original anonymous "wigshop" (`build-08-selection-*.png`) |
| A shared Spock fixture keeps tests off Platform and the developer's git | Met | `IdentitySeamsExtension` is a global extension. It installs a refusing Platform, a null git and an env with no `TOWER_*` for every spec, and restores them afterwards. The `build.gradle` env block is gone |
| After a Platform failure, the anonymous warning says what failed | Met | One combined warning: `could not read your Seqera Platform identity (<reason>) and no other identity is configured, so …` |
| DESIGN.md §16 on explore's stdout | Met | Explore's stdout is the URL line. Identity notices go to stderr, after the URL. Explore resolves once it is listening |
| live.sh step 4 opt-in | Met | Step 4 needs `P1B_LIVE_PLATFORM=1`. Step 5 points Platform at a listener that never answers. It fails if items waits 3 s or more, exits non-zero, or prints an `nf-blocks:` line |
| The #/identity note covers the Platform-failure case | Met | "`NXF_BLOCKS_USER` applies when neither `cas.asserted_by` nor Seqera Platform gives a name: no Platform token is set, or the Platform read failed." |
| SQL and JS compare principals the same way | Met | Both trim ASCII whitespace (space, tab, LF, CR, FF, VT) and lowercase ASCII letters only. In SQL that is `lower(trim(x, char(32,9,10,13,12,11)))`, in `samePrincipal` and `principalKey`, and in avatar colour. A test pins SQLite's `lower('É') = 'É'`. `docs/identity.md` states the rule |
| Remove `isMine` | Met | It is gone, and a test checks that nothing exports it |
| Close the HttpClient and git's streams | Met | The client is released in a `finally`. git's and xcode-select's streams are closed in a `finally` |
| No Xcode dialog from git on macOS | Met | On macOS, when `git` on PATH is `/usr/bin/git`, or PATH has no git, `xcode-select -p` must succeed (2 s bound) before git runs. This is documented in `docs/identity.md`. It is tested through seams only, not on a Mac without the Command Line Tools |
| warnOnce's storage-class warning, and clock-skew on stdout | Met | Every `CasSession` notice goes through one channel. A run's goes to the console (`nextflow.cas`). A command's goes to the `err` stream it was given. The final review found that `Index` had its own `nextflow.cas` logger, so it is routed too. Live: `items --format json` over a member with no snapshot and a fresh cache put the "no usable Index Snapshot" line on stderr. Stdout held one JSON document, apart from Nextflow's own test-repository banner |

### Rulings made in this round
Each ruling lists what it costs if it's wrong.
- Explore became a command session. Its stdout is the URL, its notices go to stderr, and it resolves on a background thread after it listens. If wrong, the page's first load can wait up to about 7 s (9 s on macOS with the git shim) when Platform is down. Only `/members.json` and a write wait.
- After a Platform failure that leaves nothing else, one combined warning replaces the two. If wrong, a reader loses the separate Platform line.
- The Spock global extension replaced build.gradle's Test env block, so there is one mechanism. If wrong, a non-Spock test could reach Platform. There are none today.
- Principal comparison is ASCII-only on both sides, because SQLite's `lower()` is ASCII-only without ICU. If wrong, two non-ASCII case variants don't match anywhere, consistently.
- The rail shows current name claims, then every superseded claim of any attribute, muted. If wrong, the rail shows more history than wanted.
- `CasObserver` reads the principal at the top of `onFlowBegin`, so a run says who it signs as when it starts. If wrong, the cost is nothing beyond one extra file.
- Put asks for the signer before taking its lock, so a samplesheet export doesn't wait behind a write's first resolution. If wrong, nothing is lost. A put that fails validation still never resolves.
- Undoing a release or a deletion reads "restored" in the rail, matching the page's Restore button. If wrong, it is a one-word change.

### Behaviour changes to know about
- A CLI `put` now prints `nf-blocks: signing as <name> (from <source>)` on stderr. `CasCommandsPutTest` expects that line.
- Runs now show the storage-class and index notices on the console, where they used to go only to `.nextflow.log`. They print once per JVM.
- Superseded rows differ from current ones only by their ink-3 text and the "Superseded" title. In dark mode the difference is visible but small.

### Tests (commands and results at `82e702b`)
```
$ make test
BUILD SUCCESSFUL in 57s        (1446 Gradle tests, 0 failures, 0 errors, per build/test-results)

$ cd web && npm test
ℹ tests 569
ℹ pass 569
ℹ fail 0

$ cd web && npm run build
dist/index.html 1705642 bytes

$ python3 -m unittest discover -s gate
OK (skipped=1)                 (333 tests)

$ gate/gate.sh                 (Nextflow 26.04.6, scratch NXF_HOME and GATE_ROOT, TOWER_* unset)
lineage tier:      17 PASS, 1 FAIL, 5 SKIP   (first run)
browser tier A:    6 PASS, 0 FAIL
browser tier B:    14 PASS, 0 FAIL

$ GATE_SKIP_BROWSER=1 gate/gate.sh   (lineage tier again, fresh GATE_ROOT)
at 82e702b:        18 PASS, 0 FAIL, 5 SKIP
at 9152fdd (base): 18 PASS, 0 FAIL, 5 SKIP
```
The first run's FAIL was assertion 4, "resumed run has a complete output layer": it found only `stats`. The Gate's own text tied it to issue 17. Nextflow's PublishDir could not copy 80 files on resume, the resumed run exited 1 before its outputs completed, and its log shows only `nextflow.processor.PublishDir` errors. The same tier passed on a fresh re-run at this head and at the round's base, so I read it as P0's intermittent issue 17, not a change from this round.

The live check is `docs/design/screenshots/p1b/live.sh`, run at `82e702b`:
- named, anonymous and git starts behaved as in the first round;
- step 4 (Platform) was skipped because it is opt-in;
- step 5 (`items --format json`, a dummy token and a Platform listener that never answers) took 1975 ms, wrote no `nf-blocks:` line to stderr, and its stdout held one JSON document.

A separate `items --format json` over a copy of roblab with `index/` removed and a fresh cache sent the "no usable Index Snapshot" warning to stderr. Its stdout was one JSON document after Nextflow's test-repository banner.

### Open, carried forward
- Waiting on the user: the config-first order, and email shown on hover.
- live.sh step 5's 3 s limit includes a `nextflow plugin` JVM start. It took 1975 ms here, but a cold or slow machine could fail it falsely.
- The isMine-gone test shells out to `grep`, which works on macOS and Linux.

---

## Second follow-up (after the merge at d485c6b)

Branch `explorer/p1b-identity`. It starts by merging `explorer/v0.3` @ `30b8ef5` (P1a, P2 and P3), as `d09fc3b`. Code head `99d18f7`; this section is committed on top.
Plan: `docs/plans/2026-10-25-explorer-v0.3-p1b-identity-followup2.md`.

| Integrator item | Status | Evidence |
|---|---|---|
| An undone deletion or pin leaves the reversing claim nowhere | Met | `railClaims` now leaves out only a current deletion-group `delete` (the deleted banner shows it) and a current pin `add` (the Pins panel shows it). An undo reads "restored" and an unpin reads "unpinned", as current rows with who and when, above the superseded "deleted" and "pinned" rows. `pages.test.mjs` covers delete then undo, pin then unpin, and delete, undo, delete |
| An applied sweep resolves mid-run under `sweep.lock` | Met | `apply()` resolves the signer before the `--wait` loop and before taking `sweep.lock`, when a past-deadline ledger holds metadata (deleting those writes a Forget). An apply with nothing due never resolves. A supplier that throws is caught there, and the old path (a warning plus unannounced blocks) still applies. SweepTest records the order: signer, then lock, then delete |
| Put validation runs after the signer is asked | Met, with two disclosed exceptions | These refusals now never resolve: shape or size (checked on a probe block), wrong kind, missing block, item not in its collection, and a member that isn't writable. Clock-skew and already-superseded refusals still come after resolving. An identical retry must first match the block it already wrote, and that address includes `asserted_by`. `docs/identity.md` and commit `c2c2206` say so |
| docs/identity.md says explore's page doesn't wait | Met (doc fixed) | The doc now says the page waits for `/members.json`: up to about 7 s with Platform down, or 9 s on macOS with the git shim. Rendering the shell before identity would need a new endpoint and view re-rendering, which this round's tight scope ruled out |
| A leftover `nf-blocks-identity` thread between specs | Met | ExploreCommandTest joins the thread in `cleanup()`'s `finally`, bounded at 15 s, and asserts it is gone |
| `Identity.onPath` doesn't `toRealPath` | Met | The git found on PATH is judged by its real path, so a link to `/usr/bin/git` counts as the shim. Tests use a temp-dir link and a link to `/usr/bin/git` |
| ItemsCommandTest `millis < 1000` may flake | Met | The wall-clock bound is gone. The test asserts zero Platform calls, zero git spawns and an empty `err` |
| CasSessionTest uses a real personal email | Met | `someone@example.org` throughout |
| User ruling: `docs/identity.md` says plainly that `cas.asserted_by` overrides Platform | Met | It is a paragraph right under the introduction |

### Rulings made in this round
Each ruling lists what it costs if it's wrong.
- I fixed the explore-wait doc rather than render the page before identity. If wrong, the page stays blank for up to about 7 to 9 s when Platform is down.
- Clock-skew and stale-supersedes put refusals still resolve identity, for the dedupe reason above. If wrong, a skewed Claim costs one resolution.
- A sweep that will be refused (a running pipeline, or a held lock) can still wait on identity first, because resolution now happens before the lock. If wrong, a refused `sweep --apply` with metadata due waits up to 7 to 9 s before it refuses when Platform is down. `docs/identity.md` says so.
- ItemsCommandTest counts seam calls instead of timing them. There is no cost.
- The final whole-branch review verified Task 2's last fix commit (`2f45ce2`) in place of a separate re-review. If wrong, a regression in those four small items slips past one reviewer.

### Tests (commands and results at `d148390`; the two later commits touch only `docs/identity.md`)
```
$ make test
BUILD SUCCESSFUL in 4m 30s     (2013 Gradle tests, 0 failures, 0 errors, 3 skipped: the Roblab specs, as before)

$ cd web && npm test
ℹ tests 884
ℹ pass 884
ℹ fail 0

$ cd web && npm run build
dist/index.html 1845008 bytes

$ python3 -m unittest discover -s gate
Ran 339 tests in 11.496s
OK (skipped=1)

$ gate/gate.sh                 (fresh GATE_ROOT, Nextflow 26.04.6, scratch NXF_HOME, TOWER_* unset)
lineage tier:      18 PASS, 0 FAIL, 5 SKIP
browser tier A:    6 PASS, 0 FAIL
browser tier B:    14 PASS, 0 FAIL
```

### Deferred (from the reviews, none blocking)
- Put calls `requireWritable()` twice on a write. The second call, in `write()`, is now dead.
- ExploreCommandTest's new spec name says more than it tests: it calls `joinIdentity()` itself.
- `Sweep.metadataDue` over-approximates. It can resolve when due metadata turns out to be rescued, live, gone or under the age floor. What gets deleted is unaffected.
- An identical put retry whose member was since swept gets `not_found` instead of success-unchanged. This is close to unreachable.
- No rail test for a deletion conflict (a current `delete` plus a `del`). The rail then shows the `del` as a "restored" row with the conflict flag, beside the banner.
- Older test fixtures still use real-looking addresses:
  - `ada.lovelace@example.org` in `web/test/components.test.mjs` and `panel.test.mjs`
  - `ada@example.org` in `ExploreServerTest` and `IdentityTest`
  - `ada@example.org` in `ExploreCopyTest`

  A mechanical swap to `example.org` belongs in a later round.
