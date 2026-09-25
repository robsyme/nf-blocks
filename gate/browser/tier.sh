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

free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
P_RANGE=$(free_port); P_PLAIN=$(free_port); P_EXPLORE=$(free_port)

python3 "$REPO/gate/browser/serve.py" "$B/site" "$P_RANGE" > "$B/serve-range.log" 2>&1 & S1=$!
python3 "$REPO/gate/browser/serve.py" "$B/site" "$P_PLAIN" --no-range > "$B/serve-plain.log" 2>&1 & S2=$!
# `nextflow plugin` starts the plugin unpinned, so a locally built, unpublished
# one is found through a plugins.json naming the built zip, with NXF_OFFLINE
# unset (DESIGN.md section 15, "Plugin verbs"). The zip is already unpacked in
# NXF_PLUGINS_DIR by gate.sh, so nothing is downloaded; the registry is asked
# once for dependencies.
zip="$(ls -t "$REPO"/build/distributions/nf-blocks-*.zip 2> /dev/null | head -1 || true)"
[[ -n "$zip" ]] || { echo "browser tier: no plugin zip under $REPO/build/distributions" >&2; exit 2; }
version="$(basename "$zip" .zip)"; version="${version#nf-blocks-}"
python3 - "$zip" "$version" "$B/plugins.json" << 'PY'
import datetime, hashlib, json, sys
zip_path, version, out = sys.argv[1:]
with open(zip_path, "rb") as fh:
    digest = hashlib.sha512(fh.read()).hexdigest()
date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
with open(out, "w") as fh:
    json.dump([{"id": "nf-blocks", "releases": [{"version": version, "date": date, "url": "file://" + zip_path,
                                                  "requires": ">=26.04.6", "sha512sum": digest}]}], fh)
PY
( cd "$GATE_ROOT/pipeline-a" && unset NXF_OFFLINE && \
  NXF_PLUGINS_TEST_REPOSITORY="file://$B/plugins.json" \
  exec "$NEXTFLOW" -c "$REPO/gate/gate.config" plugin nf-blocks:explore --port "$P_EXPLORE" ) \
  > "$B/explore.log" 2>&1 & S3=$!
# SIGTERM, not SIGINT: a non-interactive shell starts `&` jobs with SIGINT
# ignored, and the JVM then never runs its shutdown hook (DESIGN.md section 15).
stop() { kill -TERM "$S3" 2> /dev/null || true; kill "$S1" "$S2" 2> /dev/null || true; wait 2> /dev/null || true; }
trap stop EXIT

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
