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
rm -rf "${GATE_ROOT:?}/store" "${GATE_ROOT:?}/store-out" "${GATE_ROOT:?}/cache" \
       "${GATE_ROOT:?}/logs" "${GATE_ROOT:?}"/blocks-after-*.txt \
       "${GATE_ROOT:?}/browser" "${GATE_ROOT:?}/snapshot-after-fail.sqlite" \
       "${GATE_ROOT:?}/browser-b" "${GATE_ROOT:?}/selection"
mkdir -p "$NXF_PLUGINS_DIR" "$XDG_CACHE_HOME" "$GATE_STORE" "$GATE_STORE_OUT" \
         "$GATE_ROOT/logs"

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

run "$GATE_ROOT/pipeline-a" again
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
