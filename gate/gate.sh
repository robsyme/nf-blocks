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
#
# Exit status is assert.py's.

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
mkdir -p "$NXF_PLUGINS_DIR" "$XDG_CACHE_HOME" "$GATE_STORE" "$GATE_STORE_OUT" \
         "$GATE_ROOT/logs"

# --------------------------------------------------------------------------
# Preconditions
# --------------------------------------------------------------------------

if ! command -v python3 > /dev/null 2>&1; then
    echo "gate: python3 is required" >&2
    exit 2
fi
python3 --version

version_line="$("$NEXTFLOW" -version 2>&1 || true)"
if ! grep -q "version $REQUIRED_VERSION\b" <<< "$version_line"; then
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
    # either way, make sure this run's plugins dir holds the zip we just built.
    if ! compgen -G "$NXF_PLUGINS_DIR/nf-blocks-*" > /dev/null; then
        zip="$(ls -t "$REPO"/build/distributions/nf-blocks-*.zip 2>/dev/null | head -1 || true)"
        if [[ -z "$zip" ]]; then
            echo "gate: no plugin in $NXF_PLUGINS_DIR and no zip under $REPO/build/distributions" >&2
            exit 2
        fi
        ( cd "$NXF_PLUGINS_DIR" && unzip -q -o "$zip" -d "$(basename "${zip%.zip}")" )
    fi
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
rm -rf "$GATE_ROOT/consumer"
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
    # An empty or missing blocks/ is a real, reportable state: never abort here.
    ( cd "$GATE_STORE" && find blocks -type f 2> /dev/null | sort ) \
        > "$GATE_ROOT/$1" || true
    echo "    $(wc -l < "$GATE_ROOT/$1" | tr -d ' ') blocks -> $1"
}

run "$GATE_ROOT/pipeline-a" cold
snapshot blocks-after-cold.txt

run "$GATE_ROOT/pipeline-a" again
# assertion 2 compares this against the snapshot above: `again` must add no
# content block and lose no record.
snapshot blocks-after-again.txt

# Expected to exit non-zero: MAYBE_FAIL exits 7 for sample B.
run "$GATE_ROOT/pipeline-a" fail --fail

run "$GATE_ROOT/pipeline-a" resumed -resume cold

# A second launch directory into the same store: same bytes, same addresses.
run "$GATE_ROOT/pipeline-b" elsewhere

# --------------------------------------------------------------------------
# The consumer, reading back three ways
# --------------------------------------------------------------------------

# The consumer needs a lid:// and a cas:// reference that only exist once the
# producer has run. Its Pipeline Identity is fixed by manifest.name in
# gate.config, so the consumer names it literally and only these two are read
# back out of the store here.
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

# channel.fromLineage must still work alongside ours; capture the evidence.
(
    cd "$GATE_ROOT/pipeline-a"
    "$NEXTFLOW" -c "$REPO/gate/gate.config" lineage find 'type=FileOutput'
) > "$log/lineage-find.txt" 2>&1 || rm -f "$log/lineage-find.txt"

# --------------------------------------------------------------------------
echo
python3 "$REPO/gate/assert.py" "$GATE_ROOT"
