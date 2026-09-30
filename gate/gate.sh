#!/usr/bin/env bash
#
# The Gate: build the plugin, run the Test Pipeline against a throwaway
# cas:// store six ways, then hand the whole tree to gate/assert.py, which
# hashes bytes for itself and never asks the plugin what it stored.
#
#   gate/gate.sh                    # fresh GATE_ROOT under $TMPDIR
#   GATE_ROOT=/tmp/g gate/gate.sh   # reuse one while developing
#   NEXTFLOW=/path/to/nextflow gate/gate.sh
#   GATE_SKIP_BUILD=1 gate/gate.sh  # reuse the plugin already in GATE_ROOT
#   GATE_SKIP_BROWSER=1 gate/gate.sh  # lineage tier only
#
# Exit status is assert.py's, else browser tier A's (gate/browser/tier.sh),
# else browser tier B's (gate/browser/tier_b.sh): any tier failing fails it.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NEXTFLOW="${NEXTFLOW:-nextflow}"
REQUIRED_VERSION="26.04.6"
PIPELINE_SRC="${PIPELINE_SRC:-$REPO/../.scratch/content-addressed-lineage/test-pipeline}"

GATE_ROOT="${GATE_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/nf-blocks-gate.XXXXXXXX")}"
mkdir -p "$GATE_ROOT"
GATE_ROOT="$(cd "$GATE_ROOT" && pwd)"
echo "GATE_ROOT=$GATE_ROOT"
echo

export NXF_PLUGINS_DIR="$GATE_ROOT/plugins"
export XDG_CACHE_HOME="$GATE_ROOT/cache"
export GATE_STORE="$GATE_ROOT/store"
export GATE_STORE_OUT="$GATE_ROOT/store-out"
export NXF_ANSI_LOG=false

# A reused GATE_ROOT must not carry a previous attempt's store, index, logs or
# snapshots: every one of them is evidence, and stale evidence is worse than
# none. The built plugin is the one thing worth keeping.
rm -rf "${GATE_ROOT:?}/store" "${GATE_ROOT:?}/store-out" "${GATE_ROOT:?}/store-outputs" \
       "${GATE_ROOT:?}/cache" \
       "${GATE_ROOT:?}/logs" "${GATE_ROOT:?}"/blocks-after-*.txt \
       "${GATE_ROOT:?}/browser" "${GATE_ROOT:?}/snapshot-after-fail.sqlite" \
       "${GATE_ROOT:?}/browser-b" "${GATE_ROOT:?}/selection" "${GATE_ROOT:?}/selection-typed" \
       "${GATE_ROOT:?}/seeding.json" "${GATE_ROOT:?}/snapshot-aside.sqlite" \
       "${GATE_ROOT:?}/store-retention" "${GATE_ROOT:?}/retention" "${GATE_ROOT:?}/retention-repo" \
       "${GATE_ROOT:?}/cache-retention"
mkdir -p "$NXF_PLUGINS_DIR" "$XDG_CACHE_HOME" "$GATE_STORE" "$GATE_STORE_OUT" \
         "$GATE_ROOT/store-outputs" "$GATE_ROOT/logs" \
         "$GATE_ROOT/store-retention" "$GATE_ROOT/retention" "$GATE_ROOT/retention-repo"

# --------------------------------------------------------------------------
# Preconditions
# --------------------------------------------------------------------------

for tool in python3 unzip find $([[ -n "${GATE_SKIP_BROWSER:-}" ]] || echo node); do
    if ! command -v "$tool" > /dev/null 2>&1; then
        echo "gate: $tool is required" >&2
        exit 2
    fi
done
python3 --version

# Escape the dots: 26.04.6 as a regex would also match 26x04x6.
version_line="$("$NEXTFLOW" -version 2>&1 || true)"
if ! grep -qE "version ${REQUIRED_VERSION//./\\.}([^0-9]|$)" <<< "$version_line"; then
    echo "gate: need Nextflow $REQUIRED_VERSION, \`$NEXTFLOW -version\` said:" >&2
    echo "$version_line" >&2
    exit 2
fi
echo "nextflow $REQUIRED_VERSION ($NEXTFLOW)"

if [[ ! -f "$PIPELINE_SRC/main.nf" ]]; then
    echo "gate: no Test Pipeline at $PIPELINE_SRC (set PIPELINE_SRC)" >&2
    exit 2
fi

# --------------------------------------------------------------------------
# Build and install the plugin into the throwaway NXF_PLUGINS_DIR
# --------------------------------------------------------------------------

if [[ -z "${GATE_SKIP_BUILD:-}" ]]; then
    echo "building nf-blocks into $NXF_PLUGINS_DIR"
    ( cd "$REPO" && ./gradlew -q assemble installPlugin ) \
        > "$GATE_ROOT/logs/build.log" 2>&1 || {
            echo "gate: build failed, see $GATE_ROOT/logs/build.log" >&2
            tail -30 "$GATE_ROOT/logs/build.log" >&2
            exit 2
        }
    # installPlugin may honour NXF_PLUGINS_DIR or install into ~/.nextflow;
    # either way, replace this run's copy with the zip just built. A reused
    # GATE_ROOT otherwise keeps a stale plugin, and `nextflow plugin
    # nf-blocks:explore` fails against it with "Invalid target plugin".
    zip="$(ls -t "$REPO"/build/distributions/nf-blocks-*.zip 2>/dev/null | head -1 || true)"
    if [[ -z "$zip" ]]; then
        echo "gate: no zip under $REPO/build/distributions after the build" >&2
        exit 2
    fi
    installed="$NXF_PLUGINS_DIR/$(basename "${zip%.zip}")"
    rm -rf "${installed:?}"
    unzip -q -o "$zip" -d "$installed"
fi

# nf-blocks declares `requirePlugins = ['nf-amazon@>=3.9.2']` (Task 1), but a
# plugin already unpacked on disk at its pinned version is skipped by
# PluginUpdater.isAlreadyInstalled, so nf-blocks' own metadata (and with it
# its declared dependency) is never prefetched; pf4j's generic
# downloadPlugin() then finds nf-amazon in no repository's cached metadata
# and a fresh GATE_ROOT fails before any pipeline runs. `nextflow plugin
# install` prefetches the plugin it is asked for directly, so this puts
# nf-amazon on disk once; every run below then finds it already there under
# NXF_PLUGINS_DIR and never needs the registry for it. Idempotent: a no-op
# once nf-amazon is already unpacked, so a reused GATE_ROOT (including
# GATE_SKIP_BUILD) pays this cost at most once.
echo "installing nf-amazon@3.9.2 into $NXF_PLUGINS_DIR"
"$NEXTFLOW" plugin install nf-amazon@3.9.2 \
    > "$GATE_ROOT/logs/nf-amazon-install.log" 2>&1 || {
        echo "gate: nf-amazon install failed, see $GATE_ROOT/logs/nf-amazon-install.log" >&2
        tail -30 "$GATE_ROOT/logs/nf-amazon-install.log" >&2
        exit 2
    }
ls -1 "$NXF_PLUGINS_DIR" 2> /dev/null || true
echo

# --------------------------------------------------------------------------
# Two launch directories, so assertion 10 has something to compare
# --------------------------------------------------------------------------

for launch in pipeline-a pipeline-b; do
    rm -rf "${GATE_ROOT:?}/$launch"
    mkdir -p "$GATE_ROOT/$launch"
    cp "$PIPELINE_SRC/main.nf" "$PIPELINE_SRC/nextflow.config" "$GATE_ROOT/$launch/"
done
rm -rf "${GATE_ROOT:?}/consumer"
mkdir -p "$GATE_ROOT/consumer"
cp "$REPO/gate/consumer/main.nf" "$REPO/gate/consumer/nextflow.config" \
   "$GATE_ROOT/consumer/"

# --------------------------------------------------------------------------
# run <launch dir> <name> [extra nextflow args...]
#   never aborts the script; the exit code lands in logs/<name>/exit
# --------------------------------------------------------------------------

run() {
    local dir="$1" name="$2"; shift 2
    local log="$GATE_ROOT/logs/$name"
    mkdir -p "$log"
    echo "--- run $name  (in $(basename "$dir"))"
    local status=0
    (
        cd "$dir"
        "$NEXTFLOW" run . -c "$REPO/gate/gate.config" -name "$name" "$@"
    ) > "$log/stdout.log" 2> "$log/stderr.log" || status=$?
    echo "$status" > "$log/exit"
    if [[ -f "$dir/.nextflow.log" ]]; then
        cp "$dir/.nextflow.log" "$log/nextflow.log"
    fi
    echo "    exit $status  -> $log"
    return 0
}

snapshot() {
    echo -n "    snapshot $1: "
    python3 "$REPO/gate/assert.py" "$GATE_ROOT" "$GATE_ROOT/$1" --snapshot
}

# Every published source file in pipeline-a's work directory. Directories and
# .command.*/.exitcode are deliberately not listed: Nextflow reads those to
# satisfy the resume cache, and locking them would prove nothing about us.
published_sources() {
    find "$GATE_ROOT/pipeline-a/work" -type f \
        \( -name '*.bam' -o -name '*.stats' -o -name '*.report' \
           -o -name 'chunk_*.txt' -o -path '*_qc/*' \) 2> /dev/null | sort
}

run "$GATE_ROOT/pipeline-a" cold
snapshot blocks-after-cold.txt

run "$GATE_ROOT/pipeline-a" again -c "$REPO/gate/node-hash.config"
# assertion 2 diffs this against the snapshot above: `again` must add no
# content block and lose no record, run-log entry, nf record or coordinate.
snapshot blocks-after-again.txt

# Expected to exit non-zero: MAYBE_FAIL exits 7 for sample B.
run "$GATE_ROOT/pipeline-a" fail --fail
# Browser assertion A3 needs a snapshot two runs stale: keep the one `fail` wrote.
cp "$GATE_STORE"/index/v*.sqlite "$GATE_ROOT/snapshot-after-fail.sqlite" 2> /dev/null \
    || echo "    no Index Snapshot after fail; browser assertion A3 will fail"

# Assertion 4c, "re-hashed nothing", proved by the filesystem rather than by a
# counter: make every published source file unreadable for the duration of the
# resumed run. -resume needs only the directory entries and .command.*, so a
# run that touches no content byte succeeds; anything that re-reads a published
# file to re-address it gets AccessDenied and the run fails.
mkdir -p "$GATE_ROOT/logs/resumed"
published_sources > "$GATE_ROOT/logs/resumed/sources-locked"
locked_count=$(wc -l < "$GATE_ROOT/logs/resumed/sources-locked" | tr -d ' ')
echo "--- locking $locked_count published source file(s) at mode 000"
while IFS= read -r f; do [[ -n "$f" ]] && chmod 000 "$f"; done \
    < "$GATE_ROOT/logs/resumed/sources-locked"
unlock() {
    while IFS= read -r f; do [[ -n "$f" ]] && chmod 644 "$f" 2> /dev/null || true; done \
        < "$GATE_ROOT/logs/resumed/sources-locked"
}
trap unlock EXIT
run "$GATE_ROOT/pipeline-a" resumed -resume cold
unlock
trap - EXIT

# A second launch directory into the same store: same bytes, same addresses.
run "$GATE_ROOT/pipeline-b" elsewhere

# Milestone 5 (plan 2026-09-29): index files and a publishDir process, in a store of their own.
for p in outputs outputs-badindex; do
    rm -rf "$GATE_ROOT/$p"; mkdir -p "$GATE_ROOT/$p"
    cp "$REPO/gate/$p/main.nf" "$REPO/gate/$p/nextflow.config" "$GATE_ROOT/$p/"
done
GATE_STORE="$GATE_ROOT/store-outputs" run "$GATE_ROOT/outputs" outputs -c "$REPO/gate/outputs/overlay.config"
GATE_STORE="$GATE_ROOT/store-outputs" run "$GATE_ROOT/outputs-badindex" outputs-badindex -c "$REPO/gate/outputs/overlay.config"

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
# A failing helper never aborts the Gate: assertion 9 then fails on the checkpoint it lacks.
checkpoint() { python3 "$REPO/gate/assert.py" "$GATE_ROOT" --retention-checkpoint "$1" || echo "    checkpoint $1 failed"; }
claim() {   # <step name> <subject> <verb> <attribute|null> <value json|null> <supersedes cid|->
    local sup='[]'; [[ "$6" != "-" ]] && sup="[{\"/\":\"$6\"}]"
    local attr='null'; [[ "$4" != "null" ]] && attr="\"$4\""
    printf '{"kind":"Claim","subject":{"/":"%s"},"verb":"%s","attribute":%s,"value":%s,"supersedes":%s,"timestamp":"%s"}\n' \
        "$2" "$3" "$attr" "$5" "$sup" "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)" > "$GATE_ROOT/logs/retention/$1.request"
    verb "$1" put "$GATE_ROOT/logs/retention/$1.request"
}
refs() { eval "$(python3 "$REPO/gate/assert.py" "$GATE_ROOT" --retention-refs)"; }
age() { python3 "$REPO/gate/assert.py" "$GATE_ROOT" --retention-age "$1" || echo "    backdating failed"; }

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
age 15
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
age 15
verb sweep-3 -c "$REPO/gate/retention/grace0.config" sweep --apply true --format json
checkpoint after-sweep-3
verb sweep-4 -c "$REPO/gate/retention/grace0.config" sweep --apply true --format json
checkpoint after-sweep-4

# --------------------------------------------------------------------------
# The consumer, reading back four ways
# --------------------------------------------------------------------------

# The consumer needs references that only exist once the producer has run. Its
# Pipeline Identity is fixed by manifest.name in gate.config, so only the two
# read-back URIs are read out of the store here.
GATE_PIPELINE=''; GATE_LID=''; GATE_CAS=''
refs="$(python3 "$REPO/gate/assert.py" "$GATE_ROOT" --refs 2> "$GATE_ROOT/logs/refs.log" || true)"
eval "$refs"
echo "--- refs from the store"
echo "    pipeline=${GATE_PIPELINE:-<none>}"
echo "    lid=${GATE_LID:-<none>}"
echo "    cas=${GATE_CAS:-<none>}"
if [[ -s "$GATE_ROOT/logs/refs.log" ]]; then
    sed 's/^/    /' "$GATE_ROOT/logs/refs.log"
fi

consumer_args=()
if [[ -n "$GATE_LID" ]]; then consumer_args+=(--lid "$GATE_LID"); fi
if [[ -n "$GATE_CAS" ]]; then consumer_args+=(--cas "$GATE_CAS"); fi

log="$GATE_ROOT/logs/consumer"
mkdir -p "$log"
echo "--- run consumer"
status=0
(
    cd "$GATE_ROOT/consumer"
    "$NEXTFLOW" run . -name consumer "${consumer_args[@]+"${consumer_args[@]}"}"
) > "$log/stdout.log" 2> "$log/stderr.log" || status=$?
echo "$status" > "$log/exit"
if [[ -f "$GATE_ROOT/consumer/.nextflow.log" ]]; then
    cp "$GATE_ROOT/consumer/.nextflow.log" "$log/nextflow.log"
fi
echo "    exit $status  -> $log"

# --------------------------------------------------------------------------
# Assertion 13: a cold cache seeds from the Index Snapshot (ticket 04 decision 11)
# --------------------------------------------------------------------------

consumer_cache_delete() {
    python3 "$REPO/gate/assert.py" "$GATE_ROOT" --delete-consumer-cache
}

consumer_again() {   # <name>: the consumer, as above, under another run name
    local name="$1" log="$GATE_ROOT/logs/$1" status=0
    mkdir -p "$log"
    echo "--- run $name"
    ( cd "$GATE_ROOT/consumer" && "$NEXTFLOW" run . -name "$name" "${consumer_args[@]+"${consumer_args[@]}"}" ) \
        > "$log/stdout.log" 2> "$log/stderr.log" || status=$?
    echo "$status" > "$log/exit"
    cp "$GATE_ROOT/consumer/.nextflow.log" "$log/nextflow.log" 2> /dev/null || true
    echo "    exit $status  -> $log"
}

python3 "$REPO/gate/assert.py" "$GATE_ROOT" --seeding-before > "$GATE_ROOT/seeding.json"
consumer_cache_delete
python3 "$REPO/gate/assert.py" "$GATE_ROOT" --seeding-lock > "$GATE_ROOT/logs/seeding-lock.txt"
mkdir -p "$GATE_ROOT/logs/consumer-seeded"
cp "$GATE_ROOT/logs/seeding-lock.txt" "$GATE_ROOT/logs/consumer-seeded/locked"
# Mode 444 is the r--r--r-- LocalBlockStore writes blocks with, so the browser
# tiers after this see the blocks exactly as the plugin left them.
relock() {
    while IFS= read -r f; do [[ -n "$f" ]] && chmod 444 "$f" 2> /dev/null || true; done < "$GATE_ROOT/logs/consumer-seeded/locked"
}
trap relock EXIT
while IFS= read -r f; do [[ -n "$f" ]] && chmod 000 "$f"; done < "$GATE_ROOT/logs/consumer-seeded/locked"
consumer_again consumer-seeded
relock
trap - EXIT

consumer_cache_delete
snap="$(ls "$GATE_STORE"/index/v*.sqlite)"
mv "$snap" "$GATE_ROOT/snapshot-aside.sqlite"
consumer_again consumer-scan
mv "$GATE_ROOT/snapshot-aside.sqlite" "$snap"

# --------------------------------------------------------------------------
echo
lineage=0
python3 "$REPO/gate/assert.py" "$GATE_ROOT" || lineage=$?
browser=0
browser_b=0
if [[ -z "${GATE_SKIP_BROWSER:-}" ]]; then
    echo
    NEXTFLOW="$NEXTFLOW" "$REPO/gate/browser/tier.sh" "$GATE_ROOT" || browser=$?
    echo
    NEXTFLOW="$NEXTFLOW" "$REPO/gate/browser/tier_b.sh" "$GATE_ROOT" || browser_b=$?
fi
# Any tier failing fails the Gate.
exit $(( lineage != 0 ? lineage : browser != 0 ? browser : browser_b ))
