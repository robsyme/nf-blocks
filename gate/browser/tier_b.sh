#!/usr/bin/env bash
#
# Gate browser tier B (block explorer spec section 1.3, assertions 8-19): the
# page composes, renames, deletes and undoes through nf-blocks:explore over a
# copy of this Gate run's store; the Gate probes the write endpoint, fetches
# the samplesheet, pipes nf-blocks:items into nf-blocks:put --name through the
# real launcher, runs gate/selection and gate/selection-typed on the page's
# own snippets, and checks it all with its own encoder.
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

# B19: the command-line route to a named Selection (DESIGN.md section 15), a
# real pipe between two launchers, over the same store once explore has
# stopped. -q keeps the "un-official plugin repository" banner that
# NXF_PLUGINS_TEST_REPOSITORY prints off items' stdout; an installed release
# prints none.
read -r cli_output cli_condition cli_runs cli_name <<< "$(python3 "$REPO/gate/browser_b_assert.py" cli-args "$GATE_ROOT")"
echo "--- browser tier B: items $cli_output $cli_condition --run $cli_runs --format selection | put /dev/stdin --name $cli_name"
( cd "$B" && unset NXF_OFFLINE && export XDG_CACHE_HOME="$B/cache" NXF_PLUGINS_TEST_REPOSITORY="file://$plugins_json" && set +e
  "$NEXTFLOW" -q -c "$B/explore.config" plugin nf-blocks:items "$cli_output" "$cli_condition" --run "$cli_runs" \
      --format selection 2> "$B/cli-items.err" \
  | "$NEXTFLOW" -q -c "$B/explore.config" plugin nf-blocks:put /dev/stdin --name "$cli_name" > "$B/cli-put.out" 2> "$B/cli-put.err"
  echo "${PIPESTATUS[*]}" > "$B/cli.exit" )

s2="$(python3 -c 'import json,sys; o={s["id"]: s for s in json.load(open(sys.argv[1]))["steps"]}; print(o["B.second"]["extracts"]["after"]["written"] or "")' "$B/observed.json" 2> /dev/null || true)"
# Each consumer runs the call line the page's Selection view showed for S2
# (step B.snippets), put verbatim on its `// @snippet` line; without one it
# does not run, and B9 or B18 fails on the exit status recorded here.
run_consumer() {   # <untyped|typed> <gate dir> <launch dir> <store-out> <cache> [args...]
    local mode="$1" src="$2" launch="$3" store_out="$4" cache="$5"; shift 5
    local name; name="$(basename "$launch")"
    local status=0
    echo "--- browser tier B: $name (selection ${s2:-<none>}, the page's $mode snippet)"
    rm -rf "${launch:?}" && mkdir -p "$launch" "$store_out"
    if python3 "$REPO/gate/browser_b_assert.py" consumer "$GATE_ROOT" "$mode" "$src" "$launch"; then
        ( cd "$launch" && GATE_B_STORE="$B/store" GATE_B_OUT="$store_out" XDG_CACHE_HOME="$cache" \
          "$NEXTFLOW" run . -name "$name" --selection "$s2" ${1+"$@"} ) > "$B/$name.log" 2>&1 || status=$?
    else
        status=no-snippet
    fi
    echo "$status" > "$B/$name.exit"
    if [[ -f "$launch/.nextflow.log" ]]; then cp "$launch/.nextflow.log" "$B/$name-nextflow.log"; fi
}

run_consumer untyped "$REPO/gate/selection" "$GATE_ROOT/selection" "$B/store-out" "$B/cache-run" \
    --samplesheet "$B/samplesheet.csv"
run_consumer typed "$REPO/gate/selection-typed" "$GATE_ROOT/selection-typed" "$B/store-typed" "$B/cache-typed"

echo
python3 "$REPO/gate/browser_b_assert.py" check "$GATE_ROOT"
