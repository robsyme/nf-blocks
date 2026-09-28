#!/usr/bin/env bash
# gate/tier2/tier2.sh -- Gate tier two (ticket 11), on demand:
#   GATE_PYTHON=<python with boto3> AWS_PROFILE=scidev gate/tier2/tier2.sh <GATE_ROOT>
# <GATE_ROOT> must have passed tier one (it reuses the plugin built there, its
# plugins.json recipe and its local store/ for T3's cross-backend comparison).
# Everything it creates is under one run id and deleted on every exit.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[[ $# -eq 1 && -d "$1" ]] || { echo "usage: gate/tier2/tier2.sh <GATE_ROOT that passed tier one>" >&2; exit 2; }
GATE_ROOT="$(cd "$1" && pwd)"
PY="${GATE_PYTHON:-python3}"
NEXTFLOW="${NEXTFLOW:-nextflow}"
export AWS_PROFILE="${AWS_PROFILE:-scidev}" GATE_REPO="$REPO" NXF_ANSI_LOG=false
export NXF_PLUGINS_DIR="$GATE_ROOT/plugins"
T2_PLUGIN_VERSION="$(sed -n "s/^version = '\(.*\)'/\1/p" "$REPO/build.gradle")"
export T2_PLUGIN_VERSION
[[ -d "$NXF_PLUGINS_DIR/nf-blocks-$T2_PLUGIN_VERSION" ]] || { echo "tier two: run make gate on $GATE_ROOT first" >&2; exit 2; }
[[ -f "$GATE_ROOT/store/coords/qc/A/A_qc" ]] || { echo "tier two: $GATE_ROOT/store has no tier-one qc/A/A_qc; run make gate on it first" >&2; exit 2; }
"$PY" -c 'import boto3' 2> /dev/null || { echo "tier two: $PY has no boto3; set GATE_PYTHON" >&2; exit 2; }

T2_RUN_ID="t2-$(date -u +%Y%m%d-%H%M%S)-$RANDOM"
export T2_RUN_ID
T2="$GATE_ROOT/tier2/$T2_RUN_ID"; mkdir -p "$T2/logs" "$T2/trace" "$T2/evidence"
WORK_PREFIX="robsyme/nf-blocks-gate/$T2_RUN_ID/"   # s3gate.work_prefix_of; teardown refuses any other
# Known before setup runs, so a Ctrl-C during setup still tears the bucket down (a missing bucket is success).
export T2_BUCKET="nf-blocks-t2-$T2_RUN_ID"          # s3gate.bucket_of; teardown refuses any other
QUEUE='TowerForge-3skcexigeJwK0Jb71pThbJ'           # batch.config's; named in the warning below
T2_TIMEOUT="${T2_TIMEOUT:-2700}"                    # ticket 11 decision 6: 45 minutes (a test may shorten it)
echo "tier two: run $T2_RUN_ID, evidence under $T2"
# T6's plugin verbs find the built plugin through this (gate/browser/plugin-repo.sh); made before anything costs money.
plugins_json="$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$T2")"

# The trap first: no exit path leaves a bucket, a work prefix, an open upload, a
# running nextflow or the watchdog behind. A nextflow still running at exit
# would keep submitting Batch jobs into a bucket about to be deleted, so each
# recorded one is sent TERM (Nextflow's shutdown hook cancels its jobs), given
# a minute, then KILLed; teardown's final listing catches anything it wrote
# late. Once cleanup starts, INT, TERM and HUP are ignored, by it and by the
# teardown it starts, so a second Ctrl-C cannot cut the teardown short. A
# teardown that fails is loud and fails the harness (cloud.sh's rule).
WATCHDOG=''
PIDS="$T2/pids"; : > "$PIDS"          # every nextflow the harness started
DONE="$T2/pids.done"; : > "$DONE"     # those already reaped by run_nf's wait, whose PIDs the OS may reuse

ours() {   # <pid>: a recorded nextflow of this harness, not yet reaped, and still a launcher or JVM
    local comm
    grep -qx "$1" "$DONE" && return 1
    comm="$(ps -o comm= -p "$1" 2> /dev/null)" || return 1
    case "${comm##*/}" in java|env|bash|nextflow) return 0 ;; *) return 1 ;; esac
}

cleanup() {
    local status=$? live=() left=() pid
    trap '' INT TERM HUP
    if [[ -n "$WATCHDOG" ]]; then
        pkill -P "$WATCHDOG" 2> /dev/null || true
        kill "$WATCHDOG" 2> /dev/null || true
    fi
    while read -r pid; do
        if [[ -n "$pid" ]] && ours "$pid"; then live+=("$pid"); fi
    done < "$PIDS"
    if [[ ${#live[@]} -gt 0 ]]; then
        echo "tier two: stopping ${#live[@]} nextflow process(es) before the teardown" >&2
        kill -TERM "${live[@]}" 2> /dev/null || true
        for _ in $(seq 60); do
            left=()
            for pid in "${live[@]}"; do if ours "$pid"; then left+=("$pid"); fi; done
            [[ ${#left[@]} -eq 0 ]] && break
            sleep 1
        done
        if [[ ${#left[@]} -gt 0 ]]; then
            echo "tier two: WARNING: ${#left[@]} nextflow process(es) ignored TERM for 60 s and were KILLed; Batch jobs they" \
                 "submitted may still be running on queue $QUEUE (us-east-1): check and cancel them" >&2
            kill -KILL "${left[@]}" 2> /dev/null || true
        fi
    fi
    "$PY" "$REPO/gate/tier2/s3gate.py" teardown "$T2_RUN_ID" "$T2_BUCKET" "$WORK_PREFIX" \
        || { echo "tier two: teardown FAILED; delete s3://$T2_BUCKET and s3://scidev-playground-us-east-1/$WORK_PREFIX by hand" >&2; exit 1; }
    exit "$status"
}
trap cleanup EXIT
trap 'exit 124' TERM                                 # a TERM from the watchdog is an ordinary exit, so cleanup runs
trap 'exit 130' INT
trap 'exit 129' HUP
# The watchdog TERMs the harness itself, whose cleanup stops the recorded nextflow processes first: no new run
# can start after the timeout. Every nextflow runs in the background with its PID in $PIDS (run_nf), because a
# shell waiting on a foreground nextflow would act on the TERM only after that run ended.
# Its own stderr is closed so the cleanup's kill of its sleep prints no job notice; fd 3 carries its message.
exec 3>&2
( sleep "$T2_TIMEOUT"; echo "tier two: timeout after ${T2_TIMEOUT} s (ticket 11 decision 6)" >&3; kill -TERM $$ ) 2> /dev/null & WATCHDOG=$!

run_nf() {   # <log> <dir> <env args and command...>: runs it in <dir>, backgrounded and recorded; returns its status
    local log="$1" dir="$2"; shift 2
    local status=0 pid
    ( cd "$dir" && exec env "$@" ) > "$log" 2>&1 &
    pid=$!
    echo "$pid" >> "$PIDS"
    wait "$pid" || status=$?
    echo "$pid" >> "$DONE"
    return "$status"
}

made="$("$PY" "$REPO/gate/tier2/s3gate.py" setup "$T2_RUN_ID")"
[[ "$made" == "$T2_BUCKET" ]] || { echo "tier two: setup made '$made', expected $T2_BUCKET" >&2; exit 1; }
printf 'BUCKET=%s\nWORK=%s\nRUN_ID=%s\n' "$T2_BUCKET" "$WORK_PREFIX" "$T2_RUN_ID" > "$T2/ids.env"
echo "tier two: bucket $T2_BUCKET, work s3://scidev-playground-us-east-1/$WORK_PREFIX"

# gate.sh's pattern: a failing run is recorded in its exit file, never the end of the harness (set -e).
produce() {   # <run> <member prefix> <pipeline dir> [extra -c ...]
    local run="$1" member="$2" src="$3"; shift 3
    local launch="$T2/$run" status=0; rm -rf "$launch"; mkdir -p "$launch" "$T2/logs/$run"
    cp "$src"/main.nf "$launch/"; if [[ -f "$src/nextflow.config" ]]; then cp "$src/nextflow.config" "$launch/"; fi
    run_nf "$T2/logs/$run/stdout.log" "$launch" T2_MEMBER="$member" T2_TRACE="$T2/trace/$run.txt" XDG_CACHE_HOME="$T2/cache-$run" \
      "$NEXTFLOW" -log "$T2/logs/$run/nextflow.log" run . -name "$run" -c "$REPO/gate/gate.config" \
        -c "$REPO/gate/tier2/member.config" -c "$REPO/gate/tier2/batch.config" "$@" || status=$?
    echo "$status" > "$T2/logs/$run/exit"
    echo "--- $run exit $status"
}
consume() {   # <run> <cache dir>
    local run="$1" cache="$2" launch="$T2/$1" status=0 args=(); rm -rf "$launch"; mkdir -p "$launch" "$T2/logs/$run"
    cp "$REPO/gate/consumer/main.nf" "$launch/"
    local T2_LID='' T2_CAS='' T2_DIR=''
    "$PY" "$REPO/gate/tier2/assert_tier2.py" refs "$T2" "$GATE_ROOT" > "$T2/evidence/refs-$run.env" \
        2> "$T2/logs/$run/refs.log" || true
    eval "$(cat "$T2/evidence/refs-$run.env")"                                   # T2_LID, T2_CAS, T2_DIR
    if [[ -n "$T2_LID" ]]; then args+=(--lid "$T2_LID"); fi
    if [[ -n "$T2_CAS" ]]; then args+=(--cas "$T2_CAS"); fi
    if [[ -n "$T2_DIR" ]]; then args+=(--dir "$T2_DIR"); fi
    run_nf "$T2/logs/$run/stdout.log" "$launch" T2_TRACE="$T2/trace/$run.txt" XDG_CACHE_HOME="$cache" \
      "$NEXTFLOW" -log "$T2/logs/$run/nextflow.log" run . -name "$run" -c "$REPO/gate/tier2/consumer.config" \
        -c "$REPO/gate/tier2/batch.config" "${args[@]+"${args[@]}"}" || status=$?
    echo "$status" > "$T2/logs/$run/exit"
    echo "--- $run exit $status"
}

TP="${PIPELINE_SRC:-$REPO/../.scratch/content-addressed-lineage/test-pipeline}"
produce t1  cas    "$TP"
produce t2  cas-t2 "$TP" -c "$REPO/gate/tier2/fusion.config"
produce t2b cas    "$REPO/gate/tier2/small" -c "$REPO/gate/tier2/fusion.config"
consume t4 "$T2/cache-consumer"
# T5 compares cas-out's snapshot after t5 with this one.
"$PY" "$REPO/gate/tier2/s3gate.py" snapshot "$T2_BUCKET" cas-out "$T2/evidence/cas-out-after-t4.sqlite" \
    > "$T2/evidence/cas-out-after-t4.json" || true
rm -rf "$T2/cache-consumer"                          # T5: the head node starts cold
consume t5 "$T2/cache-consumer"

# T6: two writers into one fresh member, started together from two launch dirs.
produce t6a cas-t6 "$TP" & A=$!
produce t6b cas-t6 "$TP" & B=$!
wait "$A" "$B"
verb() {   # <log> <cache> <nf-blocks verb and args...>
    local log="$1" cache="$2"; shift 2
    run_nf "$log" "$T2/t6a" -u NXF_OFFLINE T2_MEMBER=cas-t6 XDG_CACHE_HOME="$cache" \
      NXF_PLUGINS_TEST_REPOSITORY="file://$plugins_json" \
      "$NEXTFLOW" -q -c "$REPO/gate/gate.config" -c "$REPO/gate/tier2/member.config" plugin "nf-blocks:$@" || true
}
T6_RUNS=''
eval "$("$PY" "$REPO/gate/tier2/assert_tier2.py" t6-refs "$T2" || true)"      # T6_RUNS=lid://a,lid://b
verb "$T2/logs/t6-items.txt" "$T2/cache-t6-items" items aligned --run "$T6_RUNS" --format occurrences
verb "$T2/logs/t6-snapshot.txt" "$T2/cache-t6-snap" snapshot
for attempt in 1 2 3; do                                             # the stale-ETag step (silent decision 24)
    rm -rf "$T2/cache-t6-x" "$T2/cache-t6-y"
    verb "$T2/logs/t6-race-$attempt-x.txt" "$T2/cache-t6-x" snapshot & X=$!
    verb "$T2/logs/t6-race-$attempt-y.txt" "$T2/cache-t6-y" snapshot & Y=$!
    wait "$X" "$Y"
    if grep -l 'not rewritten: replaced_meanwhile' "$T2"/logs/t6-race-"$attempt"-*.txt > /dev/null; then break; fi
done
# The deterministic half of ticket 03 decision 6 on AWS itself: a PutObject whose If-Match names a replaced
# ETag is refused with 412 (the plugin's skip on that 412 is pinned by S3SnapshotStorageTest, Task 7).
"$PY" "$REPO/gate/tier2/s3gate.py" if-match "$T2_BUCKET" > "$T2/evidence/if-match.json" || true

echo
status=0
"$PY" "$REPO/gate/tier2/assert_tier2.py" check "$T2" "$GATE_ROOT" || status=$?
echo
"$PY" "$REPO/gate/tier2/assert_tier2.py" summary "$T2" || true
exit "$status"
