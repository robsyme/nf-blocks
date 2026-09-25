#!/usr/bin/env bash
#
# Gate browser tier A, cloud part (block explorer spec section 1.3), on demand:
# before each milestone is accepted and whenever the HTTP reader or the serving
# code changes. Creates two throwaway buckets in the scidev account, drives the
# page against them, and deletes them, also on failure.
#
#   GATE_PYTHON=<python with boto3> AWS_PROFILE=scidev gate/cloud/cloud.sh <GATE_ROOT>
#
# <GATE_ROOT> must have passed the local tier (it reuses browser/site and expected.json).

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GATE_ROOT="$1"
B="$GATE_ROOT/browser"
PY="${GATE_PYTHON:-python3}"
REGION="${GATE_CLOUD_REGION:-ca-central-1}"
NEXTFLOW="${NEXTFLOW:-nextflow}"
export AWS_PROFILE="${AWS_PROFILE:-scidev}"
export NXF_PLUGINS_DIR="$GATE_ROOT/plugins" XDG_CACHE_HOME="$GATE_ROOT/cache" GATE_STORE="$GATE_ROOT/store"

[[ -f "$B/expected.json" ]] || { echo "cloud tier: run the local tier on $GATE_ROOT first" >&2; exit 2; }
"$PY" -c 'import boto3' 2> /dev/null || { echo "cloud tier: $PY has no boto3; point GATE_PYTHON at a venv with it" >&2; exit 2; }

# The trap goes in before anything is started -- including the buckets -- so no
# exit path leaves a bucket or a server behind; every step below tolerates its
# own PID or bucket name being unset. SIGTERM, not SIGINT, for explore: a
# non-interactive shell starts `&` jobs with SIGINT ignored, and the JVM then
# never runs its shutdown hook (DESIGN.md section 15). Never `kill 0`: that
# signals this whole process group.
#
# Buckets are torn down first and unconditionally, before waiting on the local
# processes: they are the costly, leaky resource, and `wait` could in
# principle hang if explore ignores SIGTERM. Each bucket is torn down
# independently -- one failing must not skip the other -- and a teardown that
# fails is loud and fails the script, rather than the usual `|| true`.
PUBLIC=''; PRIVATE=''; LOCAL=''; EXPLORE=''
cleanup() {
    local failed=0
    if [[ -n "$PUBLIC" ]]; then
        "$PY" "$REPO/gate/cloud/s3tier.py" teardown "$PUBLIC" "$REGION" \
            || { echo "cloud tier: bucket $PUBLIC NOT deleted; delete it by hand" >&2; failed=1; }
    fi
    if [[ -n "$PRIVATE" ]]; then
        "$PY" "$REPO/gate/cloud/s3tier.py" teardown "$PRIVATE" "$REGION" \
            || { echo "cloud tier: bucket $PRIVATE NOT deleted; delete it by hand" >&2; failed=1; }
    fi
    if [[ -n "$EXPLORE" ]]; then kill -TERM "$EXPLORE" 2> /dev/null || true; fi
    if [[ -n "$LOCAL" ]]; then kill "$LOCAL" 2> /dev/null || true; fi
    wait 2> /dev/null || true
    [[ "$failed" -eq 0 ]] || exit 1
}
trap cleanup EXIT

read -r PUBLIC PRIVATE < <("$PY" "$REPO/gate/cloud/s3tier.py" setup "$B/site" "$REGION") || true
# setup deletes whatever it made if it fails, so there is nothing extra to clean up here.
[[ -n "$PRIVATE" ]] || { echo "cloud tier: bucket setup failed" >&2; exit 1; }
echo "buckets: $PUBLIC (public), $PRIVATE (private), $REGION"

free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
PORT_LOCAL=$(free_port); PORT_EXPLORE=$(free_port)

cat > "$B/cloud.config" <<EOF
includeConfig '$REPO/gate/gate.config'
cas.stores.priv.location = 's3://$PRIVATE/'
EOF

# The zip lookup and plugins.json generation are shared with browser/tier.sh
# (gate/browser/plugin-repo.sh); see that script's header for why.
plugins_json="$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$B")"

python3 "$REPO/gate/browser/serve.py" "$B/site" "$PORT_LOCAL" > "$B/cloud-serve.log" 2>&1 & LOCAL=$!
( cd "$GATE_ROOT/pipeline-a" && unset NXF_OFFLINE && \
  NXF_PLUGINS_TEST_REPOSITORY="file://$plugins_json" \
  exec "$NEXTFLOW" -c "$B/cloud.config" plugin nf-blocks:explore --port "$PORT_EXPLORE" ) \
    > "$B/cloud-explore.log" 2>&1 & EXPLORE=$!

for _ in $(seq 240); do
    grep -q 'nf-blocks explorer:' "$B/cloud-explore.log" 2> /dev/null && break
    kill -0 "$EXPLORE" 2> /dev/null || { echo "cloud tier: explore exited, see $B/cloud-explore.log" >&2; exit 2; }
    sleep 0.5
done
grep -q 'nf-blocks explorer:' "$B/cloud-explore.log" || { echo "cloud tier: explore never listened, see $B/cloud-explore.log" >&2; exit 2; }
sleep 5    # a new bucket policy can take a few seconds to apply

# A stale cloud-observed.json (or cloud-scenario.json) from a previous, crashed
# run must never let cloud-check pass over old evidence.
rm -f "$B/cloud-observed.json" "$B/cloud-scenario.json"
python3 "$REPO/gate/browser_assert.py" cloud-prepare "$GATE_ROOT" "$PUBLIC" "$PRIVATE" "$REGION"
node "$REPO/gate/browser/drive.mjs" "$B/cloud-scenario.json" "$B/cloud-observed.json" \
     "local=http://127.0.0.1:$PORT_LOCAL" "explore=http://127.0.0.1:$PORT_EXPLORE" > "$B/cloud-drive.log" 2>&1 || true
python3 "$REPO/gate/browser_assert.py" cloud-check "$GATE_ROOT"
