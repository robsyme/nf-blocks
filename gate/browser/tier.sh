#!/usr/bin/env bash
#
# Gate browser tier A, local part (block explorer spec section 1.3): the page
# in Playwright's pinned Chromium against the store this Gate run wrote, served
# three ways: a static server with Range, one without, and nf-blocks:explore.
#
#   gate/browser/tier.sh <GATE_ROOT>        # NEXTFLOW and NXF_PLUGINS_DIR from gate.sh
#
# Servers and driver run in this one script: the Bash sandbox forbids a later
# command connecting to a server an earlier command started.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GATE_ROOT="$1"
NEXTFLOW="${NEXTFLOW:-nextflow}"
B="$GATE_ROOT/browser"
mkdir -p "$B"
rm -f "$B/observed.json"

echo "--- browser tier: setup"
( cd "$REPO/gate/browser" && npm ci --no-audit --no-fund && npx playwright install chromium ) > "$B/setup.log" 2>&1 || {
    echo "browser tier: npm or Playwright setup failed, see $B/setup.log" >&2
    exit 2
}
python3 "$REPO/gate/browser_assert.py" prepare "$GATE_ROOT"

# The zip lookup and plugins.json generation are shared with cloud/cloud.sh
# (gate/browser/plugin-repo.sh); see that script's header for why.
plugins_json="$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$B")"
free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
P_RANGE=$(free_port); P_PLAIN=$(free_port); P_EXPLORE=$(free_port)

# The trap goes in before the first server, so no exit path leaves one running.
# SIGTERM, not SIGINT, for explore: a non-interactive shell starts `&` jobs with
# SIGINT ignored, and the JVM then never runs its shutdown hook (DESIGN.md section 15).
S1=''; S2=''; S3=''
stop() {
    if [[ -n "$S3" ]]; then kill -TERM "$S3" 2> /dev/null || true; fi
    for pid in "$S1" "$S2"; do if [[ -n "$pid" ]]; then kill "$pid" 2> /dev/null || true; fi; done
    wait 2> /dev/null || true
}
trap stop EXIT

python3 "$REPO/gate/browser/serve.py" "$B/site" "$P_RANGE" > "$B/serve-range.log" 2>&1 & S1=$!
python3 "$REPO/gate/browser/serve.py" "$B/site" "$P_PLAIN" --no-range > "$B/serve-plain.log" 2>&1 & S2=$!
( cd "$GATE_ROOT/pipeline-a" && unset NXF_OFFLINE && \
  NXF_PLUGINS_TEST_REPOSITORY="file://$plugins_json" \
  exec "$NEXTFLOW" -c "$REPO/gate/gate.config" plugin nf-blocks:explore --port "$P_EXPLORE" ) \
  > "$B/explore.log" 2>&1 & S3=$!

for _ in $(seq 240); do
    grep -q 'nf-blocks explorer:' "$B/explore.log" 2> /dev/null && break
    kill -0 "$S3" 2> /dev/null || { echo "browser tier: explore exited, see $B/explore.log" >&2; exit 2; }
    sleep 0.5
done
grep -q 'nf-blocks explorer:' "$B/explore.log" || { echo "browser tier: explore never listened, see $B/explore.log" >&2; exit 2; }
for port in "$P_RANGE" "$P_PLAIN"; do
    for _ in $(seq 50); do curl -s -o /dev/null "http://127.0.0.1:$port/" && break; sleep 0.1; done
done

echo "--- browser tier: driving $(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["steps"]))' "$B/scenario.json") steps"
node "$REPO/gate/browser/drive.mjs" "$B/scenario.json" "$B/observed.json" \
     "range=http://127.0.0.1:$P_RANGE" "plain=http://127.0.0.1:$P_PLAIN" "explore=http://127.0.0.1:$P_EXPLORE" \
     > "$B/drive.log" 2>&1 || echo "browser tier: the driver failed, see $B/drive.log" >&2
echo
python3 "$REPO/gate/browser_assert.py" check "$GATE_ROOT"
