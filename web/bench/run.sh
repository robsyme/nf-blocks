#!/usr/bin/env bash
# web/bench/run.sh: the VFS spike, end to end, in one command (the Bash sandbox
# forbids a later command connecting to a server an earlier one started).
#   web/bench/run.sh <schema-2 index or snapshot> [port]
# SNAPSHOT=<file> measures that file instead of the generated year;
# NO_RANGE=1 serves it the way stock `python -m http.server` does.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
SCHEMA="$1"; PORT="${2:-8811}"
WORK="${BENCH_DIR:-${TMPDIR:-/tmp}/nf-blocks-bench}"
mkdir -p "$WORK/site/snap/index"
TARGET="${SNAPSHOT:-$WORK/year-v2.sqlite}"
[[ -f "$TARGET" ]] || python3 "$REPO/gate/gen_year.py" "$SCHEMA" "$TARGET" >&2   # stdout stays one JSON object
ln -sf "$TARGET" "$WORK/site/snap/index/v2.sqlite"
( cd "$REPO/web" && node build.mjs > /dev/null )
cp "$REPO/web/dist/bench.html" "$WORK/site/bench.html"
python3 "$REPO/gate/year_params.py" "$TARGET" 0 > "$WORK/params0.json"
python3 "$REPO/gate/year_params.py" "$TARGET" 3 > "$WORK/params3.json"
python3 "$REPO/gate/browser/serve.py" "$WORK/site" "$PORT" ${NO_RANGE:+--no-range} & SERVER=$!
trap 'kill $SERVER' EXIT
for _ in $(seq 50); do curl -s -o /dev/null "http://127.0.0.1:$PORT/bench.html" && break; sleep 0.1; done
node "$REPO/gate/browser/bench.mjs" "http://127.0.0.1:$PORT/bench.html" \
     "http://127.0.0.1:$PORT/snap/index/v2.sqlite" "$WORK/params0.json" "$WORK/params3.json"
