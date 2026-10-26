#!/usr/bin/env bash
#
# Nextflow's own lineage commands over scrubbed nf/ records (DESIGN.md
# section 10). CasLinStore redacts host paths, the home directory and the user
# name from every record but FileOutput before DefaultLinStore writes it; this
# checks, on released Nextflow, that `nextflow lineage list`, `view`, `find`,
# `diff` and `check`, a downstream lid:// read and channel.fromLineage still
# work, and that no record but a FileOutput's path holds a host string.
#
#   gate/nf-scrub/check.sh <SCRATCH_ROOT>
#
# Run it directly; there is no make target. Everything lives under
# SCRATCH_ROOT: NXF_HOME, NXF_PLUGINS_DIR, XDG_CACHE_HOME and the store.
# SCRATCH_ROOT must be empty or one this script made before (it holds a
# .nf-scrub-root marker); only the subdirectories the script creates are
# removed on a rerun. The 26.04.6 framework jar is copied from
# ~/.nextflow/framework when it is there (a read), otherwise the launcher
# downloads it into the scratch NXF_HOME. Inputs: NEXTFLOW, GATE_SKIP_BUILD=1
# to reuse the zip already under build/distributions.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HERE="$REPO/gate/nf-scrub"
ROOT="${1:?usage: gate/nf-scrub/check.sh <SCRATCH_ROOT>}"
NEXTFLOW="${NEXTFLOW:-nextflow}"

say() { echo "nf-scrub: $*" >&2; }

mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd)"
MARKER="$ROOT/.nf-scrub-root"
if [[ ! -f "$MARKER" && -n "$(ls -A "$ROOT")" ]]; then
    say "refusing to use $ROOT: it is not empty and holds no .nf-scrub-root marker"
    exit 2
fi
touch "$MARKER"
for sub in aws nxf-home plugins cache out store launch consume; do
    rm -rf "${ROOT:?}/$sub"
done

# No cloud credentials can reach any process below.
for v in $(env | sed -n 's/^\(AWS_[A-Za-z0-9_]*\)=.*/\1/p'); do unset "$v"; done
mkdir -p "$ROOT/aws"; : > "$ROOT/aws/config"; : > "$ROOT/aws/credentials"
export AWS_CONFIG_FILE="$ROOT/aws/config" AWS_SHARED_CREDENTIALS_FILE="$ROOT/aws/credentials" AWS_EC2_METADATA_DISABLED=true
export NXF_VER=26.04.6
export NXF_HOME="$ROOT/nxf-home" NXF_PLUGINS_DIR="$ROOT/plugins" XDG_CACHE_HOME="$ROOT/cache"
export NXF_ANSI_LOG=false
unset NXF_CONFIG_FILE NXF_OFFLINE NXF_PLUGINS_TEST_REPOSITORY NXF_ASSETS NXF_WORK NXF_CACHE_DIR NXF_TEMP
mkdir -p "$NXF_HOME/framework" "$NXF_PLUGINS_DIR" "$XDG_CACHE_HOME" "$ROOT/out"
if [[ -d "$HOME/.nextflow/framework/$NXF_VER" ]]; then
    cp -R "$HOME/.nextflow/framework/$NXF_VER" "$NXF_HOME/framework/"
fi

# Build, then unzip into the scratch plugins dir (never installPlugin, which
# may write ~/.nextflow/plugins).
if [[ -z "${GATE_SKIP_BUILD:-}" ]]; then
    "$REPO/gradlew" -q -p "$REPO" assemble > "$ROOT/out/build.txt" 2>&1 || { tail -30 "$ROOT/out/build.txt" >&2; exit 2; }
fi
ZIP="$(ls -t "$REPO"/build/distributions/nf-blocks-*.zip 2> /dev/null | head -1 || true)"
[[ -n "$ZIP" ]] || { say "no zip under build/distributions"; exit 2; }
NAME="$(basename "${ZIP%.zip}")"
VER="${NAME#nf-blocks-}"
unzip -q -o "$ZIP" -d "$NXF_PLUGINS_DIR/$NAME"
"$NEXTFLOW" plugin install nf-amazon@3.9.2 > "$ROOT/out/nf-amazon-install.txt" 2>&1 \
    || { say "nf-amazon install failed, see $ROOT/out/nf-amazon-install.txt"; exit 2; }

STORE="$ROOT/store"
mkdir -p "$STORE" "$ROOT/launch" "$ROOT/consume"
cp "$HERE/producer/main.nf" "$ROOT/launch/"
cp "$HERE/consumer/main.nf" "$ROOT/consume/"
printf 'reference line\n' > "$ROOT/launch/ref.txt"
cat > "$ROOT/nf-scrub.config" << EOF
plugins { id 'nf-blocks@$VER' }
lineage.enabled = true
lineage.store.location = 'cas://lab'
outputDir = 'cas://lab'
cas {
    stores { lab { location = '$STORE' } }
    asserted_by = 'nf-scrub'
}
EOF

failures=0
# run <name> <cmd...>: output to out/<name>.txt; a non-zero exit is a FAIL.
run() {
    local name=$1; shift
    if "$@" > "$ROOT/out/$name.txt" 2>&1; then
        say "ok   $name exits 0"
    else
        say "FAIL $name exited non-zero, see $ROOT/out/$name.txt"
        failures=$((failures + 1))
    fi
}
# expect <name> <grep -E pattern> <description>
expect() {
    if grep -qE -- "$2" "$ROOT/out/$1.txt"; then
        say "ok   $3"
    else
        say "FAIL $3 (no /$2/ in $ROOT/out/$1.txt)"
        failures=$((failures + 1))
    fi
}

cd "$ROOT/launch"
# The first run's note is an absolute path outside every run directory; STAGE's
# script echoes it, so only the params rule can take it out of the TaskRun.
ELSEWHERE=/nf-scrub-elsewhere
# A secret param the script reads: Nextflow records it in TaskRun.globalVars,
# so the leftover check below also asserts no record holds its value.
DB_PASSWORD='Tr0ub4dor&3xyz'
run produce1 "$NEXTFLOW" -log "$ROOT/out/produce1.log" run main.nf -c "$ROOT/nf-scrub.config" -name scrub_one \
    --ref "$ROOT/launch/ref.txt" --note "$ELSEWHERE/notes" --db_password "$DB_PASSWORD"
run produce2 "$NEXTFLOW" -log "$ROOT/out/produce2.log" run main.nf -c "$ROOT/nf-scrub.config" -name scrub_two \
    --ref "$ROOT/launch/ref.txt" --note "$ROOT/launch/other" --db_password "$DB_PASSWORD"
eval "$(python3 "$HERE/records.py" keys "$STORE/nf")"

L=("$NEXTFLOW" -log "$ROOT/out/lineage.log" -c "$ROOT/nf-scrub.config" lineage)
run list "${L[@]}" list
expect list "lid://$RUN1" "lineage list names the first run"
expect list "lid://$RUN2" "lineage list names the second run"
run view-run "${L[@]}" view "lid://$RUN1"
expect view-run '"homeDir": "\[redacted-location\]"' "the WorkflowRun's homeDir is redacted"
expect view-run '"userName": "\[redacted-user\]"' "the WorkflowRun's userName is redacted"
run view-task "${L[@]}" view "lid://$TASK1"
expect view-task "\"workflowRun\": \"lid://" "the TaskRun keeps its workflowRun reference"
run view-work-fo "${L[@]}" view "lid://$WORK_FO"
expect view-work-fo "\"path\": \"$ROOT/launch/work/" "a work-dir FileOutput keeps its path"
run find-fo "${L[@]}" find type=FileOutput "workflowRun=lid://$RUN1"
expect find-fo "^lid://$RUN1/counts/count.txt$" "find by type and run returns the published FileOutput"
run find-label "${L[@]}" find label=count
expect find-label "^lid://$CAS_FO$" "find by label returns the published FileOutput"
run diff-tasks "${L[@]}" diff "lid://$TASK1" "lid://$TASK2"
expect diff-tasks "^diff --git $TASK1 $TASK2" "diff renders two TaskRuns"
run check-work-fo "${L[@]}" check "lid://$WORK_FO"
expect check-work-fo "Checksum validation succeed" "lineage check validates a work-dir FileOutput"
run check-cas-fo "${L[@]}" check "lid://$CAS_FO"
expect check-cas-fo "Checksum validation succeed" "lineage check validates a published FileOutput"

cd "$ROOT/consume"
run consume-lid "$NEXTFLOW" -log "$ROOT/out/consume-lid.log" run main.nf -c "$ROOT/nf-scrub.config" \
    --lid "lid://$WORK_FO,lid://$CAS_FO"
expect consume-lid "staged out.txt: reference line" "a downstream run stages a work-dir file through lid://"
expect consume-lid "staged count.txt: 2" "a downstream run stages a published file through lid://"
run consume-fromlineage "$NEXTFLOW" -log "$ROOT/out/consume-fromlineage.log" run main.nf -c "$ROOT/nf-scrub.config" \
    --lid "lid://$CAS_FO" --run "lid://$RUN1"
expect consume-fromlineage "staged out.txt: reference line" "channel.fromLineage(workflowRun:) stages the run's files"

if python3 "$HERE/records.py" leftovers "$STORE/nf" "$ROOT" "$HOME" "$(id -un)" "$ELSEWHERE" "$DB_PASSWORD" > "$ROOT/out/leftovers.txt"; then
    say "ok   no record but a FileOutput's path holds a host string or the secret param"
else
    say "FAIL a host string outside FileOutput.path:"
    cat "$ROOT/out/leftovers.txt" >&2
    failures=$((failures + 1))
fi

if [[ $failures -ne 0 ]]; then
    say "FAIL -- $failures check(s); outputs under $ROOT/out"
    exit 1
fi
say "PASS -- outputs under $ROOT/out"
