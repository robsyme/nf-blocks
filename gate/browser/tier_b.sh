#!/usr/bin/env bash
#
# Gate browser tier B (block explorer spec section 1.3, assertions 8-16): the
# page composes, renames, deletes and undoes through nf-blocks:explore over a
# copy of this Gate run's store; the Gate probes the write endpoint, fetches
# the samplesheet, runs gate/selection, and checks it all with its own encoder.
#
#   gate/browser/tier_b.sh <GATE_ROOT>        # NEXTFLOW and NXF_PLUGINS_DIR from gate.sh
#
# One script, since the Bash sandbox forbids a later command connecting to a
# server an earlier command started. Needs tier A's setup (npm ci, Playwright).

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GATE_ROOT="$1"
NEXTFLOW="${NEXTFLOW:-nextflow}"
B="$GATE_ROOT/browser-b"

python3 "$REPO/gate/browser_b_assert.py" prepare "$GATE_ROOT"
plugins_json="$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$B")"
port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')

# SIGTERM, not SIGINT: a non-interactive shell starts `&` jobs with SIGINT
# ignored, and the JVM then never runs its shutdown hook (DESIGN.md section 15).
S=''
stop() { if [[ -n "$S" ]]; then kill -TERM "$S" 2> /dev/null || true; wait "$S" 2> /dev/null || true; S=''; fi; }
trap stop EXIT

( cd "$B" && unset NXF_OFFLINE && XDG_CACHE_HOME="$B/cache" \
  NXF_PLUGINS_TEST_REPOSITORY="file://$plugins_json" \
  exec "$NEXTFLOW" -c "$B/explore.config" plugin nf-blocks:explore --port "$port" ) > "$B/explore.log" 2>&1 & S=$!

for _ in $(seq 240); do
    grep -q 'nf-blocks explorer:' "$B/explore.log" 2> /dev/null && break
    kill -0 "$S" 2> /dev/null || { echo "browser tier B: explore exited, see $B/explore.log" >&2; exit 2; }
    sleep 0.5
done
url="$(grep -o 'http://127.0.0.1:[0-9]*/?token=[a-z2-7]*' "$B/explore.log" | head -1 || true)"
[[ -n "$url" ]] || { echo "browser tier B: explore printed no URL with a token, see $B/explore.log" >&2; exit 2; }
token="${url##*token=}"

echo "--- browser tier B: driving $(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["steps"]))' "$B/scenario.json") steps"
node "$REPO/gate/browser/drive.mjs" "$B/scenario.json" "$B/observed.json" \
     "explore=http://127.0.0.1:$port" "token=$token" > "$B/drive.log" 2>&1 \
     || echo "browser tier B: the driver failed, see $B/drive.log" >&2
python3 "$REPO/gate/browser_b_assert.py" probe "$GATE_ROOT" "$port" "$token" > "$B/probe.log" 2>&1 \
     || echo "browser tier B: the probe failed, see $B/probe.log" >&2
stop

s2="$(python3 -c 'import json,sys; o={s["id"]: s for s in json.load(open(sys.argv[1]))["steps"]}; print(o["B.second"]["extracts"]["after"]["written"] or "")' "$B/observed.json" 2> /dev/null || true)"
echo "--- browser tier B: selection pipeline (selection ${s2:-<none>})"
rm -rf "${GATE_ROOT:?}/selection" && mkdir -p "$GATE_ROOT/selection"
cp "$REPO/gate/selection/main.nf" "$REPO/gate/selection/nextflow.config" "$GATE_ROOT/selection/"
status=0
( cd "$GATE_ROOT/selection" && GATE_B_STORE="$B/store" GATE_B_OUT="$B/store-out" XDG_CACHE_HOME="$B/cache-run" \
  "$NEXTFLOW" run . -name selection --selection "$s2" --samplesheet "$B/samplesheet.csv" ) \
  > "$B/selection.log" 2>&1 || status=$?
echo "$status" > "$B/selection.exit"
if [[ -f "$GATE_ROOT/selection/.nextflow.log" ]]; then cp "$GATE_ROOT/selection/.nextflow.log" "$B/selection-nextflow.log"; fi

echo
python3 "$REPO/gate/browser_b_assert.py" check "$GATE_ROOT"
