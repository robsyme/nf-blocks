#!/usr/bin/env bash
#
# P3 acceptance (docs/design/03-agent-prompts.md, P3): copy a Selection from a
# local store to an S3-compatible store running on 127.0.0.1, then check that
# the same `fromStore(selection: ...)` resolves from the destination with only
# the store config changed, and serve the destination alone for the explorer.
#
#   gate/p3/s3-accept.sh <SCRATCH_ROOT>
#
# The S3 server is a parameter, so any S3-compatible server works unchanged:
#   S3_IMAGE=<image>            run it as a container (docker), or
#   S3_CMD='<command>'          run a local binary; {data} and {port} are replaced
#   S3_CONTAINER_PORT=9000      the port inside the container
#   S3_DATA_MOUNT=/data         where the container keeps its data
#   S3_IMAGE_ARGS='server /data'  arguments after the image (empty for images with their own entrypoint)
#   S3_ENV='MINIO_ROOT_USER MINIO_ROOT_PASSWORD'  env names that receive the access key and secret
#   S3_PORT=9000                the port on 127.0.0.1
# Examples:
#   S3_IMAGE=bitnamilegacy/minio:2025.7.23-debian-12-r5 S3_DATA_MOUNT=/bitnami/minio/data S3_IMAGE_ARGS= gate/p3/s3-accept.sh "$ROOT"
#   S3_CMD='minio server {data} --address 127.0.0.1:{port} --console-address 127.0.0.1:0' gate/p3/s3-accept.sh "$ROOT"
#
# Other inputs: STORE_SRC (default /tmp/cas-exploration/store, read only; a copy
# is made), SELECTION (default the wigshop Selection), NEXTFLOW, NXF_VER,
# P3_SHOOT (a command run with the explorer URL of the S3-only server as its
# argument, for screenshots), GATE_SKIP_BUILD=1 to reuse an installed plugin.
#
# Guards, all checked before any copy: every AWS_* variable is unset, the AWS
# config and credentials files are empty scratch files, NXF_HOME and the plugin
# and cache dirs are scratch, the endpoint must be 127.0.0.1 (AWS_ENDPOINT_URL
# and AWS_ENDPOINT_URL_S3 name it too), and the server binds 127.0.0.1 only
# with throwaway credentials (an S3_CMD must name 127.0.0.1 and {port}). The container (or process)
# and its data are removed on every exit; images are kept.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="${1:?usage: gate/p3/s3-accept.sh <SCRATCH_ROOT>}"
mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd)"

S3_PORT="${S3_PORT:-9000}"
S3_ENDPOINT="http://127.0.0.1:${S3_PORT}"
S3_BUCKET="${S3_BUCKET:-p3-accept}"
S3_IMAGE="${S3_IMAGE:-}"
S3_CMD="${S3_CMD:-}"
S3_CONTAINER_PORT="${S3_CONTAINER_PORT:-9000}"
S3_DATA_MOUNT="${S3_DATA_MOUNT:-/data}"
S3_IMAGE_ARGS="${S3_IMAGE_ARGS-server /data}"
S3_ENV="${S3_ENV:-MINIO_ROOT_USER MINIO_ROOT_PASSWORD}"
STORE_SRC="${STORE_SRC:-/tmp/cas-exploration/store}"
SELECTION="${SELECTION:-bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce}"
NEXTFLOW="${NEXTFLOW:-nextflow}"
export NXF_VER="${NXF_VER:-26.04.6}"

say() { echo "s3-accept: $*" >&2; }
die() { say "$*"; exit 1; }

[[ -n "$S3_IMAGE" || -n "$S3_CMD" ]] || die "set S3_IMAGE (a container image) or S3_CMD (a local server command)"
[[ -z "$S3_IMAGE" || -z "$S3_CMD" ]] || die "set only one of S3_IMAGE and S3_CMD"
[[ -d "$STORE_SRC/blocks" ]] || die "no store at $STORE_SRC"
# A local server binds where its command says: the command must name 127.0.0.1 and take the port from {port}.
if [[ -n "$S3_CMD" ]]; then
    [[ "$S3_CMD" == *127.0.0.1* && "$S3_CMD" == *'{port}'* ]] \
        || die "S3_CMD must bind 127.0.0.1 and use {port} (e.g. --address 127.0.0.1:{port}); refusing: $S3_CMD"
fi

# --- Guards: no real AWS credentials can reach any process started below.
for v in $(env | sed -n 's/^\(AWS_[A-Za-z0-9_]*\)=.*/\1/p'); do unset "$v"; done
mkdir -p "$ROOT/aws"
: > "$ROOT/aws/config"
: > "$ROOT/aws/credentials"
export AWS_CONFIG_FILE="$ROOT/aws/config" AWS_SHARED_CREDENTIALS_FILE="$ROOT/aws/credentials"
export AWS_EC2_METADATA_DISABLED=true
# A config that lost its endpoint still reaches only the local server, never AWS.
export AWS_ENDPOINT_URL_S3="$S3_ENDPOINT" AWS_ENDPOINT_URL="$S3_ENDPOINT"
export NXF_HOME="$ROOT/nxf-home" NXF_PLUGINS_DIR="$ROOT/plugins" XDG_CACHE_HOME="$ROOT/cache"
mkdir -p "$NXF_HOME" "$NXF_PLUGINS_DIR" "$XDG_CACHE_HOME"
unset NXF_CONFIG_FILE NXF_OFFLINE

# Throwaway credentials, made here and never written outside ROOT.
ACCESS_KEY="p3$(LC_ALL=C tr -dc 'a-z0-9' < /dev/urandom | head -c 14)"
SECRET_KEY="$(LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32)"

endpoint_is_local() {
    local host
    host="$(python3 -c 'import sys, urllib.parse; print(urllib.parse.urlsplit(sys.argv[1]).hostname or "")' "$1")"
    [[ "$host" == "127.0.0.1" ]]
}
endpoint_is_local "$S3_ENDPOINT" || die "endpoint $S3_ENDPOINT is not on 127.0.0.1; refusing"

CONTAINER=''; SERVER_PID=''; EXPLORE_PID=''; PROXY_PID=''
cleanup() {
    local rc=$?
    if [[ -n "$EXPLORE_PID" ]]; then kill -TERM "$EXPLORE_PID" 2> /dev/null || true; fi
    if [[ -n "$PROXY_PID" ]]; then kill "$PROXY_PID" 2> /dev/null || true; fi
    if [[ -n "$CONTAINER" ]]; then docker rm -f -v "$CONTAINER" > /dev/null 2>&1 || say "container $CONTAINER NOT removed; remove it by hand"; fi
    if [[ -n "$SERVER_PID" ]]; then kill "$SERVER_PID" 2> /dev/null || true; fi
    wait 2> /dev/null || true
    rm -rf "$ROOT/s3data"
    exit "$rc"
}
trap cleanup EXIT

# --- The S3 server.
mkdir -p "$ROOT/s3data"
read -r -a env_names <<< "$S3_ENV"
if [[ -n "$S3_IMAGE" ]]; then
    CONTAINER="p3-s3-$$"
    env_args=()
    [[ ${#env_names[@]} -ge 1 ]] && env_args+=(-e "${env_names[0]}=$ACCESS_KEY")
    [[ ${#env_names[@]} -ge 2 ]] && env_args+=(-e "${env_names[1]}=$SECRET_KEY")
    read -r -a image_args <<< "$S3_IMAGE_ARGS"
    say "starting $S3_IMAGE as $CONTAINER on 127.0.0.1:$S3_PORT"
    docker run -d --name "$CONTAINER" -p "127.0.0.1:${S3_PORT}:${S3_CONTAINER_PORT}" \
        "${env_args[@]}" -v "$ROOT/s3data:$S3_DATA_MOUNT" "$S3_IMAGE" ${image_args[@]+"${image_args[@]}"} > /dev/null
else
    cmd="${S3_CMD//\{data\}/$ROOT/s3data}"; cmd="${cmd//\{port\}/$S3_PORT}"
    say "starting: $cmd"
    env "${env_names[0]}=$ACCESS_KEY" "${env_names[1]}=$SECRET_KEY" bash -c "exec $cmd" > "$ROOT/s3-server.log" 2>&1 &
    SERVER_PID=$!
fi

# The bucket, made with a signed PUT (works on any S3-compatible server); retried until the server answers.
made=''
for _ in $(seq 1 60); do
    code="$(curl -s -o /dev/null -w '%{http_code}' --aws-sigv4 'aws:amz:us-east-1:s3' --user "$ACCESS_KEY:$SECRET_KEY" \
        -X PUT "$S3_ENDPOINT/$S3_BUCKET" || true)"
    if [[ "$code" == 200 || "$code" == 409 ]]; then made=1; break; fi
    sleep 1
done
[[ -n "$made" ]] || die "the S3 server at $S3_ENDPOINT never created bucket $S3_BUCKET (last status $code)"
say "bucket s3://$S3_BUCKET ready at $S3_ENDPOINT"

# G1: a local server (and any child it forked) listens on loopback only, console ports included.
# A container's host side is bound by docker's -p 127.0.0.1: above.
if [[ -n "$SERVER_PID" ]]; then
    pids="$SERVER_PID"; frontier="$SERVER_PID"
    while [[ -n "$frontier" ]]; do
        next=''
        for pid in $frontier; do next="$next $(pgrep -P "$pid" || true)"; done
        frontier="$(echo $next)"
        [[ -n "$frontier" ]] && pids="$pids $frontier"
    done
    listening="$(lsof -nP -a -p "${pids// /,}" -iTCP -sTCP:LISTEN -Fn 2> /dev/null | sed -n 's/^n//p' | sort -u || true)"
    [[ -n "$listening" ]] || die "found no listening socket for the S3 server (pids $pids); cannot check it binds 127.0.0.1 only"
    while IFS= read -r addr; do
        case "$addr" in
            127.0.0.1:*|'[::1]:'*) ;;
            *) die "the S3 server listens on $addr, not on 127.0.0.1 only; refusing (pids $pids)" ;;
        esac
    done <<< "$listening"
    say "S3 server listeners, all loopback: $(echo $listening)"
fi

# --- The plugin, in the scratch plugins dir.
if [[ -z "${GATE_SKIP_BUILD:-}" ]]; then
    "$REPO/gradlew" -q -p "$REPO" assemble installPlugin
    "$NEXTFLOW" plugin install nf-amazon@3.9.2 > "$ROOT/nf-amazon-install.log" 2>&1 || die "nf-amazon install failed, see $ROOT/nf-amazon-install.log"
fi
PLUGIN_DIR="$(find "$NXF_PLUGINS_DIR" -maxdepth 1 -type d -name 'nf-blocks-*' | head -1)"
[[ -n "$PLUGIN_DIR" ]] || die "no nf-blocks plugin in $NXF_PLUGINS_DIR"
PLUGIN_VERSION="${PLUGIN_DIR##*/nf-blocks-}"
export NXF_PLUGINS_TEST_REPOSITORY="file://$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$ROOT")"

# --- The source store, copied so the original is never written.
rm -rf "$ROOT/roblab"
cp -R "$STORE_SRC" "$ROOT/roblab"

aws_scope() {
    cat << EOF
aws {
    accessKey = '$ACCESS_KEY'
    secretKey = '$SECRET_KEY'
    region = 'us-east-1'
    client {
        endpoint = '$S3_ENDPOINT'
        s3PathStyleAccess = true
    }
}
EOF
}

# Copy config: roblab writable (it is the source), org-s3 a configured member.
cat > "$ROOT/copy.config" << EOF
plugins { id 'nf-blocks@$PLUGIN_VERSION' }
lineage.enabled = true
lineage.store.location = 'cas://roblab'
cas {
    stores {
        roblab { location = '$ROOT/roblab' }
        'org-s3' { location = 's3://$S3_BUCKET/blocks' }
    }
    asserted_by = 'p3-accept@example.org'
}
$(aws_scope)
EOF

# The two run configs differ only in the store: the pipeline is the same file.
cat > "$ROOT/local.config" << EOF
plugins { id 'nf-blocks@$PLUGIN_VERSION' }
lineage.enabled = true
lineage.store.location = 'cas://roblab'
cas { stores { roblab { location = '$ROOT/roblab' } } }
EOF
cat > "$ROOT/s3.config" << EOF
plugins { id 'nf-blocks@$PLUGIN_VERSION' }
lineage.enabled = true
lineage.store.location = 'cas://org-s3'
cas { stores { 'org-s3' { location = 's3://$S3_BUCKET/blocks' } } }
$(aws_scope)
EOF

# Every endpoint any config names must be local, checked before the copy.
for c in "$ROOT"/copy.config "$ROOT"/s3.config; do
    while read -r ep; do
        endpoint_is_local "$ep" || die "$c names endpoint $ep, which is not 127.0.0.1; refusing"
    done < <(sed -n "s/.*endpoint *= *'\([^']*\)'.*/\1/p" "$c")
    grep -q "endpoint = '$S3_ENDPOINT'" "$c" || die "$c has no local endpoint"
done
[[ ! -s "$AWS_CONFIG_FILE" && ! -s "$AWS_SHARED_CREDENTIALS_FILE" ]] || die "the scratch AWS files are not empty"
env | grep -q '^AWS_ACCESS_KEY_ID=\|^AWS_PROFILE=\|^AWS_SECRET' && die "AWS credentials are in the environment"
[[ "${AWS_ENDPOINT_URL_S3:-}" == "$S3_ENDPOINT" && "${AWS_ENDPOINT_URL:-}" == "$S3_ENDPOINT" ]] || die "the AWS endpoint variables do not name $S3_ENDPOINT"

cd "$ROOT"

# --- The copy: dry run, then the copy, both kept as JSON.
say "dry run"
"$NEXTFLOW" -q -c "$ROOT/copy.config" plugin nf-blocks:copy "selection:$SELECTION" --to org-s3 --depth results \
    --format json --dry-run > "$ROOT/plan.json"
say "copy"
"$NEXTFLOW" -q -c "$ROOT/copy.config" plugin nf-blocks:copy "selection:$SELECTION" --to org-s3 --depth results \
    --format json > "$ROOT/copied.json"
say "second dry run (everything should already be there)"
"$NEXTFLOW" -q -c "$ROOT/copy.config" plugin nf-blocks:copy "selection:$SELECTION" --to org-s3 --depth results \
    --format json --dry-run > "$ROOT/plan-after.json"

# The same checks as local-accept.sh: the copy wrote what the plan said, and the second dry run finds it all there.
python3 - "$ROOT" << 'PY'
import json, sys
root = sys.argv[1]
plan, done, after = (json.load(open(f'{root}/{n}.json')) for n in ('plan', 'copied', 'plan-after'))
p, a = plan['plan'], after['plan']
print(f"plan: {p['blocks']} blocks, {p['present']} present, {p['transfer']} to transfer, {p['transfer_bytes']} bytes")
print(f"copied: {done.get('copied')} blocks, {done.get('copied_bytes')} bytes, transfer {done.get('transfer_entry')}, snapshot {done.get('snapshot')}")
print(f"after: {a['present']} of {a['blocks']} present, {a['transfer']} to transfer")
assert not done.get('failures'), done.get('failures')
assert done.get('copied') == p['transfer'], 'copied differs from the plan'
assert a['transfer'] == 0 and a['present'] == a['blocks'], 'the second dry run still has blocks to transfer'
PY

# --- The same pipeline against each store.
mkdir -p "$ROOT/pipeline"
cat > "$ROOT/pipeline/main.nf" << 'EOF'
include { fromStore } from 'plugin/nf-blocks'

process HASH {
    input:
    path f
    output:
    stdout
    script:
    """
    shasum -a 256 ${f} | sed 's/ .*//' | tr -d '\\n'; echo " ${f}"
    """
}

def paths(x) {
    if( x instanceof java.nio.file.Path ) return [x]
    if( x instanceof Map ) return x.values().collectMany { paths(it) }
    if( x instanceof Collection ) return x.collectMany { paths(it) }
    return []
}

workflow {
    items = channel.fromStore(selection: params.selection)
    files = items.flatMap { item -> paths(item) }
    HASH(files) | collectFile(name: 'hashes.txt', storeDir: params.out, sort: true)
}
EOF
for which in local s3; do
    say "fromStore run against the $which store"
    rm -rf "$ROOT/run-$which"; mkdir -p "$ROOT/run-$which"
    (cd "$ROOT/run-$which" && "$NEXTFLOW" -q run "$ROOT/pipeline/main.nf" -c "$ROOT/$which.config" \
        --selection "$SELECTION" --out "$ROOT/run-$which/out" > "$ROOT/run-$which/stdout.log" 2>&1) \
        || die "the $which run failed, see $ROOT/run-$which"
done
if diff "$ROOT/run-local/out/hashes.txt" "$ROOT/run-s3/out/hashes.txt" > /dev/null; then
    say "fromStore resolves the same files from both stores: $(wc -l < "$ROOT/run-s3/out/hashes.txt") files"
else
    diff "$ROOT/run-local/out/hashes.txt" "$ROOT/run-s3/out/hashes.txt" >&2 || true
    die "fromStore resolved different files from the S3 store"
fi

# --- The explorer on the S3 store alone, shot before the measurement below writes its Claim.
if [[ -n "${P3_SHOOT:-}" ]]; then
    "$NEXTFLOW" -c "$ROOT/s3.config" plugin nf-blocks:explore > "$ROOT/explore.log" 2>&1 &
    EXPLORE_PID=$!
    url=''
    for _ in $(seq 1 120); do
        url="$(sed -n 's/^nf-blocks explorer: \(http[^ ]*\).*/\1/p' "$ROOT/explore.log" | head -1)"
        [[ -n "$url" ]] && break
        sleep 1
    done
    [[ -n "$url" ]] || die "explore never printed its URL, see $ROOT/explore.log"
    say "explorer on org-s3 alone: $url"
    DEST=org-s3 $P3_SHOOT "$url" dest
    kill -TERM "$EXPLORE_PID" 2> /dev/null || true; wait "$EXPLORE_PID" 2> /dev/null || true; EXPLORE_PID=''
fi

# --- The presence cost (P3 open question): after any Claim, the next snapshot
# build of the destination re-asks every copied file. Count the HEADs on its
# blocks through a counting proxy, then again with no new Claim.
PROXY_PORT="${PROXY_PORT:-$((S3_PORT + 1))}"
PROXY_LOG="$ROOT/proxy.log"
: > "$PROXY_LOG"
python3 "$REPO/gate/p3/count_proxy.py" "$PROXY_PORT" "$S3_PORT" "$PROXY_LOG" &
PROXY_PID=$!
sed "s|endpoint = '$S3_ENDPOINT'|endpoint = 'http://127.0.0.1:$PROXY_PORT'|" "$ROOT/s3.config" > "$ROOT/s3-proxy.config"
cat > "$ROOT/claim.json" << EOF
{"kind": "Claim", "subject": {"/": "$SELECTION"}, "verb": "set", "attribute": "p3_measure", "value": "1",
 "supersedes": [], "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"}
EOF
"$NEXTFLOW" -q -c "$ROOT/s3.config" plugin nf-blocks:put "$ROOT/claim.json" > "$ROOT/claim-put.json"
heads() { grep -c "^HEAD /$S3_BUCKET/blocks/blocks/" "$PROXY_LOG" || true; }
: > "$PROXY_LOG"
"$NEXTFLOW" -q -c "$ROOT/s3-proxy.config" plugin nf-blocks:snapshot > "$ROOT/snapshot-after-claim.log" 2>&1
after_claim="$(heads)"
: > "$PROXY_LOG"
"$NEXTFLOW" -q -c "$ROOT/s3-proxy.config" plugin nf-blocks:snapshot > "$ROOT/snapshot-quiet.log" 2>&1
quiet="$(heads)"
kill "$PROXY_PID" 2> /dev/null || true
echo "{\"heads_after_claim\": $after_claim, \"heads_without_claim\": $quiet, \"copied\": $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("copied", "null"))' "$ROOT/copied.json")}" > "$ROOT/presence-cost.json"
say "snapshot HEADs on blocks: $after_claim after a Claim, $quiet without one ($(cat "$ROOT/presence-cost.json"))"

say "PASS: plan, copy and fromStore results are in $ROOT"
