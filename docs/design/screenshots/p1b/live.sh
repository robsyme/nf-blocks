#!/usr/bin/env bash
# Live P1b check: the real nf-blocks:explore over a scratch copy of roblab, one start per identity source.
#   docs/design/screenshots/p1b/live.sh [<scratch dir>]   (store: P1B_STORE, default /tmp/cas-exploration/store)
# Needs Nextflow 26.04.6 and nf-amazon 3.9.2 in ~/.nextflow (copied, never written), and Playwright in gate/browser.
# Step 4 calls the real Seqera Platform (one read-only GET /user-info with TOWER_ACCESS_TOKEN) only with P1B_LIVE_PLATFORM=1.
# One script, because the sandbox forbids a later command connecting to a server an earlier one started.
set -euo pipefail
LIVE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$LIVE/../../../.." && pwd)"
SB="${1:-$(mktemp -d)}/sb"
SHOTS="$REPO/docs/design/screenshots/p1b"
export NF_BLOCKS_PLAYWRIGHT="${NF_BLOCKS_PLAYWRIGHT:-$REPO/gate/browser/node_modules/playwright/index.mjs}"
rm -rf "$SB"; mkdir -p "$SB/home/framework" "$SB/plugins" "$SB/cache" "$SB/work" "$SHOTS"
cp -R ~/.nextflow/framework/26.04.6 "$SB/home/framework/"
cp -R ~/.nextflow/plugins/nf-amazon-3.9.2 "$SB/plugins/"
cp -R "${P1B_STORE:-/tmp/cas-exploration/store}" "$SB/roblab"
export NXF_HOME="$SB/home" NXF_PLUGINS_DIR="$SB/plugins" XDG_CACHE_HOME="$SB/cache" NXF_VER=26.04.6 NXF_ANSI_LOG=false
unset NXF_OFFLINE
( cd "$REPO" && ./gradlew -q assemble installPlugin )
plugins_json="$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$SB")"
cat > "$SB/explore.config" <<EOF
lineage.enabled = true
lineage.store.location = 'cas://roblab'
cas {
  stores { roblab { location = '$SB/roblab' } }
  index { path = '$SB/cache/index.sqlite' }
}
EOF

S=''
H=''
stop() { if [[ -n "$S" ]]; then kill -TERM "$S" 2> /dev/null || true; wait "$S" 2> /dev/null || true; S=''; fi; }
# The step 5 listener that never answers goes with explore's server.
finish() { stop; if [[ -n "$H" ]]; then kill "$H" 2> /dev/null || true; wait "$H" 2> /dev/null || true; H=''; fi; }
trap finish EXIT

# start <label> <env...>: starts explore with the given environment, waits for the URL, sets URL.
start() {
  local label="$1"; shift
  local port; port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
  ( cd "$SB/work" && exec env "$@" NXF_PLUGINS_TEST_REPOSITORY="file://$plugins_json" \
      nextflow -c "$SB/explore.config" plugin nf-blocks:explore --port "$port" ) > "$SB/explore-$label.log" 2>&1 & S=$!
  for _ in $(seq 360); do
    grep -q 'nf-blocks explorer:' "$SB/explore-$label.log" 2> /dev/null && break
    kill -0 "$S" 2> /dev/null || { echo "$label: explore exited"; tail -30 "$SB/explore-$label.log"; exit 2; }
    sleep 0.5
  done
  URL="$(grep -o 'http://127.0.0.1:[0-9]*/?token=[a-z2-7]*' "$SB/explore-$label.log" | head -1)"
  # The identity resolves after the server listens, so its line follows the URL.
  for _ in $(seq 40); do grep -q -E 'nf-blocks: (signing as|no identity|could not read)' "$SB/explore-$label.log" && break; sleep 0.5; done
  echo "== $label console:"; grep -E 'nf-blocks: |explorer:' "$SB/explore-$label.log" | sed -E 's/token=[a-z2-7]+/token=<redacted>/'
  echo "== $label members.json identity:"; curl -s -H "Origin: http://127.0.0.1:$port" "http://127.0.0.1:$port/members.json" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("identity"))'
}

NOGIT=(GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1)

# 1. NXF_BLOCKS_USER: rename wigshop for real, then the screenshots, signed and shown as the address below.
start named -u TOWER_ACCESS_TOKEN -u TOWER_API_ENDPOINT NXF_BLOCKS_USER=alice@example.org
node "$LIVE/drive.mjs" "$URL" "$SHOTS" named
stop
echo "== claim written into the scratch copy:"; ls "$SB/roblab/log" | grep claim | tail -2

# 2. anonymous: no token, no NXF_BLOCKS_USER, no git identity.
start anon -u TOWER_ACCESS_TOKEN -u TOWER_API_ENDPOINT -u NXF_BLOCKS_USER "${NOGIT[@]}"
node "$LIVE/drive.mjs" "$URL" "$SHOTS" anon
stop

# 3. git: no token, no NXF_BLOCKS_USER.
start git -u TOWER_ACCESS_TOKEN -u TOWER_API_ENDPOINT -u NXF_BLOCKS_USER
stop

# 4. Platform, opt-in: the token from this shell, one read-only GET /user-info.
if [[ "${P1B_LIVE_PLATFORM:-}" == 1 && -n "${TOWER_ACCESS_TOKEN:-}" ]]; then
  start platform -u NXF_BLOCKS_USER
  stop
else
  echo "== platform: skipped (set P1B_LIVE_PLATFORM=1 with TOWER_ACCESS_TOKEN to run it)"
fi

# 5. A read-only verb never resolves: with a dummy token and a Platform endpoint that accepts and never
# answers, items must exit 0 in under 3 s with no nf-blocks: line on stderr. A closed port would refuse at
# once and prove nothing, so the endpoint is a listener started here, which holds every connection open.
rm -f "$SB/hole.port"
python3 -c '
import os, socket, sys
s = socket.socket(); s.bind(("127.0.0.1", 0)); s.listen(16)
with open(sys.argv[1] + ".tmp", "w") as f: f.write(str(s.getsockname()[1]))
os.replace(sys.argv[1] + ".tmp", sys.argv[1])
held = []
while True: held.append(s.accept()[0])
' "$SB/hole.port" & H=$!
for _ in $(seq 40); do [[ -s "$SB/hole.port" ]] && break; sleep 0.25; done
[[ -s "$SB/hole.port" ]] || { echo "items: the silent Platform listener did not start"; exit 2; }
hole="http://127.0.0.1:$(cat "$SB/hole.port")"
run_cid="$(python3 "$LIVE/runcid.py" "$SB/roblab/index/v3.sqlite" distraught_murdock)"
now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
t0=$(now_ms)
( cd "$SB/work" && env -u NXF_BLOCKS_USER TOWER_ACCESS_TOKEN=dummy-not-a-token TOWER_API_ENDPOINT="$hole" \
    NXF_PLUGINS_TEST_REPOSITORY="file://$plugins_json" \
    nextflow -c "$SB/explore.config" plugin nf-blocks:items bigwig --run "cas://$run_cid" --format json ) > "$SB/items.out" 2> "$SB/items.err" && rc=0 || rc=$?
took=$(( $(now_ms) - t0 ))
echo "== items (read-only, Platform at a listener that never answers) took ${took} ms, exit $rc"
echo "== items stderr nf-blocks: lines:"; grep -E 'nf-blocks:' "$SB/items.err" || echo '(none)'
failed=''
[[ "$rc" == 0 ]] || { echo "FAIL: items exited $rc"; tail -20 "$SB/items.err"; failed=1; }
if grep -q -E 'nf-blocks:' "$SB/items.err"; then echo "FAIL: items printed an nf-blocks: line on stderr"; failed=1; fi
(( took < 3000 )) || { echo "FAIL: items took ${took} ms, 3 s or more, so something waited"; failed=1; }
[[ -z "$failed" ]] || exit 1
python3 "$LIVE/runcid.py" --check-json "$SB/items.out"
