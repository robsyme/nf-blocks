#!/usr/bin/env bash
#
# P3 copy path, end to end, against a second LOCAL filesystem store. This is
# not the S3 acceptance (gate/p3/s3-accept.sh); it needs no server and installs
# nothing. It copies a Selection from a scratch copy of roblab into a fresh
# local member, then checks that the same `fromStore(selection: ...)` resolves
# from the destination alone with only the store config changed.
#
#   gate/p3/local-accept.sh <SCRATCH_ROOT>
#
# Inputs: STORE_SRC (default /tmp/cas-exploration/store, read only; a copy is
# made), SELECTION (default the wigshop Selection), DEPTH (default results),
# NEXTFLOW, NXF_VER, P3_SHOOT (a command run with the explorer URL of a server
# over both members before the copy, the same after it, then one over the
# destination alone, then, after the release below, one over both members again;
# the second argument is before, after, dest or notkept), GATE_SKIP_BUILD=1
# to reuse an installed plugin.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="${1:?usage: gate/p3/local-accept.sh <SCRATCH_ROOT>}"
mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd)"
STORE_SRC="${STORE_SRC:-/tmp/cas-exploration/store}"
SELECTION="${SELECTION:-bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce}"
DEPTH="${DEPTH:-results}"
NEXTFLOW="${NEXTFLOW:-nextflow}"
export NXF_VER="${NXF_VER:-26.04.6}"

say() { echo "local-accept: $*" >&2; }
die() { say "$*"; exit 1; }
[[ -d "$STORE_SRC/blocks" ]] || die "no store at $STORE_SRC"

# Scratch everything; no cloud credentials can reach any process below.
for v in $(env | sed -n 's/^\(AWS_[A-Za-z0-9_]*\)=.*/\1/p'); do unset "$v"; done
mkdir -p "$ROOT/aws"; : > "$ROOT/aws/config"; : > "$ROOT/aws/credentials"
export AWS_CONFIG_FILE="$ROOT/aws/config" AWS_SHARED_CREDENTIALS_FILE="$ROOT/aws/credentials" AWS_EC2_METADATA_DISABLED=true
export NXF_HOME="$ROOT/nxf-home" NXF_PLUGINS_DIR="$ROOT/plugins" XDG_CACHE_HOME="$ROOT/cache"
mkdir -p "$NXF_HOME" "$NXF_PLUGINS_DIR" "$XDG_CACHE_HOME"
unset NXF_CONFIG_FILE NXF_OFFLINE

EXPLORE_PID=''
cleanup() {
    local rc=$?
    if [[ -n "$EXPLORE_PID" ]]; then kill -TERM "$EXPLORE_PID" 2> /dev/null || true; fi
    wait 2> /dev/null || true
    exit "$rc"
}
trap cleanup EXIT

if [[ -z "${GATE_SKIP_BUILD:-}" ]]; then
    "$REPO/gradlew" -q -p "$REPO" assemble installPlugin
    "$NEXTFLOW" plugin install nf-amazon@3.9.2 > "$ROOT/nf-amazon-install.log" 2>&1 || die "nf-amazon install failed, see $ROOT/nf-amazon-install.log"
fi
PLUGIN_DIR="$(find "$NXF_PLUGINS_DIR" -maxdepth 1 -type d -name 'nf-blocks-*' | head -1)"
[[ -n "$PLUGIN_DIR" ]] || die "no nf-blocks plugin in $NXF_PLUGINS_DIR"
PLUGIN_VERSION="${PLUGIN_DIR##*/nf-blocks-}"
export NXF_PLUGINS_TEST_REPOSITORY="file://$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$ROOT")"

rm -rf "$ROOT/roblab" "$ROOT/org-local"
cp -R "$STORE_SRC" "$ROOT/roblab"
mkdir -p "$ROOT/org-local"

cat > "$ROOT/copy.config" << EOF
plugins { id 'nf-blocks@$PLUGIN_VERSION' }
lineage.enabled = true
lineage.store.location = 'cas://roblab'
cas {
    stores {
        roblab { location = '$ROOT/roblab' }
        'org-local' { location = '$ROOT/org-local' }
    }
    asserted_by = 'p3-accept@example.org'
}
EOF
cat > "$ROOT/local.config" << EOF
plugins { id 'nf-blocks@$PLUGIN_VERSION' }
lineage.enabled = true
lineage.store.location = 'cas://roblab'
cas { stores { roblab { location = '$ROOT/roblab' } }; asserted_by = 'p3-accept@example.org' }
EOF
cat > "$ROOT/dest.config" << EOF
plugins { id 'nf-blocks@$PLUGIN_VERSION' }
lineage.enabled = true
lineage.store.location = 'cas://org-local'
cas { stores { 'org-local' { location = '$ROOT/org-local' } }; asserted_by = 'p3-accept@example.org' }
EOF

cd "$ROOT"
serve() {
    "$NEXTFLOW" -c "$1" plugin nf-blocks:explore > "$ROOT/explore-$2.log" 2>&1 &
    EXPLORE_PID=$!
    local url=''
    for _ in $(seq 1 120); do
        url="$(sed -n 's/^nf-blocks explorer: \(http[^ ]*\).*/\1/p' "$ROOT/explore-$2.log" | head -1)"
        [[ -n "$url" ]] && break
        sleep 1
    done
    [[ -n "$url" ]] || die "explore never printed its URL, see $ROOT/explore-$2.log"
    say "explorer ($2): $url"
    $P3_SHOOT "$url" "$2"
    kill -TERM "$EXPLORE_PID" 2> /dev/null || true; wait "$EXPLORE_PID" 2> /dev/null || true; EXPLORE_PID=''
}
# The source's own v4 snapshot (scratch copy only), so storage figures are real.
"$NEXTFLOW" -q -c "$ROOT/local.config" plugin nf-blocks:snapshot > "$ROOT/snapshot-roblab.log" 2>&1 || die "snapshot of the roblab copy failed, see $ROOT/snapshot-roblab.log"
if [[ -n "${P3_SHOOT:-}" ]]; then serve "$ROOT/copy.config" before; fi

copy() { "$NEXTFLOW" -q -c "$ROOT/copy.config" plugin nf-blocks:copy "selection:$SELECTION" --to org-local --depth "$DEPTH" --format json "$@"; }
say "dry run"; copy --dry-run > "$ROOT/plan.json"
say "copy"; copy > "$ROOT/copied.json"
say "second dry run"; copy --dry-run > "$ROOT/plan-after.json"

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

# The same pipeline against each store, the destination alone the second time.
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
for which in local dest; do
    say "fromStore run against the $which store"
    rm -rf "$ROOT/run-$which"; mkdir -p "$ROOT/run-$which"
    (cd "$ROOT/run-$which" && "$NEXTFLOW" -q run "$ROOT/pipeline/main.nf" -c "$ROOT/$which.config" \
        --selection "$SELECTION" --out "$ROOT/run-$which/out" > "$ROOT/run-$which/stdout.log" 2>&1) \
        || die "the $which run failed, see $ROOT/run-$which"
done
diff "$ROOT/run-local/out/hashes.txt" "$ROOT/run-dest/out/hashes.txt" > /dev/null \
    || { diff "$ROOT/run-local/out/hashes.txt" "$ROOT/run-dest/out/hashes.txt" >&2 || true; die "fromStore resolved different files from the destination"; }
say "fromStore resolves the same $(wc -l < "$ROOT/run-dest/out/hashes.txt" | tr -d ' ') files from the destination alone"

if [[ -n "${P3_SHOOT:-}" ]]; then
    serve "$ROOT/copy.config" after
    serve "$ROOT/dest.config" dest
fi

# D9, last because it leaves the run released in org-local: a destination that released
# a run is not kept, so a dry run says so and a real copy refuses and writes nothing.
RUN_COMPLETION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["plan"]["runs"][0]["completion"])' "$ROOT/copied.json")"
[[ -n "$RUN_COMPLETION" ]] || die "copied.json names no run"
# The release Claim is the one the explorer's Release button writes (web/src/write.js release()):
# set retain "lineage" on the run's RunCompletion, superseding nothing since none is current.
cat > "$ROOT/release.json" << EOF
{"kind": "Claim", "subject": {"/": "$RUN_COMPLETION"}, "verb": "set", "attribute": "retain", "value": "lineage",
 "supersedes": [], "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"}
EOF
say "release $RUN_COMPLETION in org-local"
"$NEXTFLOW" -q -c "$ROOT/dest.config" plugin nf-blocks:put "$ROOT/release.json" > "$ROOT/release-put.json" \
    || die "writing the release Claim failed: $(cat "$ROOT/release-put.json")"

if [[ -n "${P3_SHOOT:-}" ]]; then serve "$ROOT/copy.config" notkept; fi

say "dry run against the released run"; copy --dry-run > "$ROOT/plan-released.json"
# What org-local holds, by path and content, so a refused copy can be shown to have written nothing.
dest_state() { (cd "$ROOT/org-local" && find log blocks -type f | LC_ALL=C sort | while IFS= read -r f; do shasum -a 256 "$f"; done); }
dest_state > "$ROOT/org-local-before-refused.txt"
[[ -s "$ROOT/org-local-before-refused.txt" ]] || die "org-local lists no files under log/ and blocks/"
say "real copy against the released run (expects a refusal, exit 1)"
refused_rc=0
copy > "$ROOT/copy-refused.json" 2> "$ROOT/copy-refused.err" || refused_rc=$?
dest_state > "$ROOT/org-local-after-refused.txt"

python3 - "$ROOT" "$RUN_COMPLETION" "$refused_rc" << 'PY'
import json, sys
root, run, rc = sys.argv[1], sys.argv[2], int(sys.argv[3])
plan = json.load(open(f'{root}/plan-released.json'))['plan']
assert plan['kept'] is False, f"the dry run says kept {plan['kept']}, not false"
entries = [d for d in plan['destination_state'] if d['cid'] == run]
assert len(entries) == 1, f'destination_state lists the run {len(entries)} times: {plan["destination_state"]}'
assert entries[0]['state'] == 'released', f"the run is {entries[0]['state']} in org-local, not released"
assert rc == 1, f'the real copy exited {rc}, not 1'
refused = json.load(open(f'{root}/copy-refused.json'))
assert refused.get('refused') == 'not_kept', f"refused is {refused.get('refused')}"
print(f"released run: kept {plan['kept']}, state {entries[0]['state']}; the real copy exited {rc}, refused {refused['refused']}")
PY
cmp "$ROOT/org-local-before-refused.txt" "$ROOT/org-local-after-refused.txt" \
    || die "the refused copy changed what org-local holds under log/ or blocks/"
say "the refused copy wrote nothing ($(wc -l < "$ROOT/org-local-after-refused.txt" | tr -d ' ') files unchanged)"

say "PASS (local destination, not S3): results are in $ROOT"
