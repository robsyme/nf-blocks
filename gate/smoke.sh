#!/usr/bin/env bash
#
# Smoke test for the Nextflow boundary (DESIGN.md sections 2, 8 and 10).
#
# Builds the plugin into a throwaway NXF_PLUGINS_DIR, runs the Test Pipeline
# once with the lineage store and outputDir both on `cas://lab`, and checks
# that a published file reached the coordinate tree and that Nextflow's own
# lineage records reached `<store>/nf`.
#
# This is not the Gate. It answers one question: does the plugin load and
# publish through `cas://` on released Nextflow?
#
set -euo pipefail

REPO_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PIPELINE_SRC=${PIPELINE_SRC:-$REPO_DIR/../.scratch/content-addressed-lineage/test-pipeline}
NEXTFLOW=${NEXTFLOW:-nextflow}

if [[ ! -f "$PIPELINE_SRC/main.nf" ]]; then
    echo "smoke: no Test Pipeline at $PIPELINE_SRC (override with PIPELINE_SRC)" >&2
    exit 2
fi

NF_VERSION=$("$NEXTFLOW" -version 2>&1 | sed -n 's/.*version \([0-9][^ ]*\).*/\1/p' | head -1)
if [[ "$NF_VERSION" != "26.04.6" ]]; then
    echo "smoke: expected Nextflow 26.04.6, found '${NF_VERSION}' (override with NEXTFLOW)" >&2
    exit 2
fi

SMOKE_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/nf-blocks-smoke.XXXXXX")
export NXF_PLUGINS_DIR="$SMOKE_ROOT/plugins"
export XDG_CACHE_HOME="$SMOKE_ROOT/cache"
STORE="$SMOKE_ROOT/store"
PIPELINE="$SMOKE_ROOT/pipeline"
LOG="$SMOKE_ROOT/.nextflow.log"

echo "smoke: root $SMOKE_ROOT"

"$REPO_DIR/gradlew" -q -p "$REPO_DIR" assemble installPlugin

PLUGIN_DIR=$(find "$NXF_PLUGINS_DIR" -maxdepth 1 -type d -name 'nf-blocks-*' | head -1)
if [[ -z "$PLUGIN_DIR" ]]; then
    echo "smoke: the plugin did not install into $NXF_PLUGINS_DIR" >&2
    exit 1
fi
PLUGIN_VERSION=${PLUGIN_DIR##*/nf-blocks-}

mkdir -p "$PIPELINE" "$STORE"
cp "$PIPELINE_SRC/main.nf" "$PIPELINE_SRC/nextflow.config" "$PIPELINE/"

cat > "$SMOKE_ROOT/smoke.config" <<EOF
plugins {
    id 'nf-blocks@${PLUGIN_VERSION}'
}

lineage.enabled = true
lineage.store.location = 'cas://lab'

outputDir = 'cas://lab'

cas {
    stores {
        lab {
            location = '${STORE}'
        }
    }
    asserted_by = 'smoke'
}
EOF

set +e
( cd "$PIPELINE" && "$NEXTFLOW" -log "$LOG" run . -c "$SMOKE_ROOT/smoke.config" -name smoke ) \
    > "$SMOKE_ROOT/run.out" 2>&1
RUN_STATUS=$?
set -e

echo "smoke: nextflow log $LOG"
if [[ $RUN_STATUS -ne 0 ]]; then
    echo "smoke: FAIL -- the run exited $RUN_STATUS" >&2
    tail -40 "$SMOKE_ROOT/run.out" >&2
    exit 1
fi

failures=0

check() {
    local what=$1; shift
    if "$@"; then
        echo "smoke: ok   $what"
    else
        echo "smoke: FAIL $what" >&2
        failures=$((failures+1))
    fi
}

# The store holds real content-addressed blocks now, and a Publish Coordinate is
# a one-line Pointer File naming the Store URI -- not a copied tree (DESIGN section 7).
check "content-addressed blocks written under <store>/blocks" \
    bash -c "find '$STORE/blocks' -type f -name 'bafk*' | grep -q ."
check "metadata (dag-cbor) blocks written under <store>/blocks" \
    bash -c "find '$STORE/blocks' -type f -name 'bafy*' | grep -q ."
check "file coordinate is a pointer file at <store>/coords/aligned/A/A.bam" test -f "$STORE/coords/aligned/A/A.bam"
check "the file pointer names a raw Store URI" \
    bash -c "grep -qE '^cas://bafk[a-z2-7]+/A\\.bam$' '$STORE/coords/aligned/A/A.bam'"
check "directory coordinate is a pointer file at <store>/coords/qc/A/A_qc" test -f "$STORE/coords/qc/A/A_qc"
check "the directory pointer names a manifest Store URI" \
    bash -c "grep -qE '^cas://bafy[a-z2-7]+' '$STORE/coords/qc/A/A_qc'"
check "lineage records at <store>/nf" test -d "$STORE/nf"
check "a WorkflowRun record in <store>/nf" \
    bash -c "find '$STORE/nf' -name .data.json -print0 | xargs -0 grep -lE '\"kind\"[[:space:]]*:[[:space:]]*\"WorkflowRun\"' | grep -q ."
# save() rewrites FileOutput.path to the immutable Store URI, so a published
# FileOutput names a cas://<cid> address rather than the mutable coordinate.
check "a FileOutput record names a cas://<cid> Store URI" \
    bash -c "find '$STORE/nf' -name .data.json -print0 | xargs -0 grep -lE 'cas://bafk[a-z2-7]+' | grep -q ."

if [[ $failures -ne 0 ]]; then
    echo "smoke: FAIL -- $failures check(s) failed; log at $LOG" >&2
    exit 1
fi

echo "smoke: PASS -- store $STORE, log $LOG"
