#!/usr/bin/env bash
#
# P4 web acceptance: projects and the differentialabundance hand-off, through
# the real page and the real nf-blocks:explore server, over scratch copies of
# roblab (/tmp/cas-exploration/store) and the plugin half's input sandbox.
# Screenshots go to docs/design/screenshots/p4/ (shoot.mjs there drives the page).
#
#   gate/p4/web-accept.sh <SCRATCH_ROOT> [stage ...]
#
# Stages, in order (default: all of them):
#   setup     build the plugin (assemble only, unpacked into the scratch
#             NXF_PLUGINS_DIR), copy both stores, write the configs, snapshot
#   a         roblab: New project, Add to project from three run pages, the
#             retention policy, conditions as alice@example.org
#   ana       roblab: RAP1_IAA_30M_REP1's condition as ana.k@example.org
#   pin       roblab: ana.k@example.org pins the wigshop Selection with its own
#             Pin… (P2), so the policy pin below has someone else's pin to name
#   handoff   roblab: the hand-off from distraught_murdock, failing then saved;
#   verify    checks the samplesheet and contrasts blocks against the page's
#             previews and resolves cas://<samplesheet>/samplesheet.csv
#   c         the wigshop Selection added to the project (policy pin), copied
#             to org-local, default store org-local, then the project's policy
#             pin of wigshop written in org-local through an explorer over
#             org-local alone; snapshot of both members
#   b         the sandbox: the shared-inputs suggestion, Create project
#   shots     the build shots and the mockups, light and dark
#   a2        request counts of the project routes over the roblab copy
#
# Not in the default list:
#   build     only the plugin build of setup, with the stores left as they are
#
# Inputs: STORE_SRC (default /tmp/cas-exploration/store), SANDBOX_SRC (the
# <SCRATCH_ROOT> of gate/p4/input-sandbox.sh, required unless NO_SANDBOX=1), NEXTFLOW, NXF_VER (default 26.10.3), GATE_SKIP_BUILD=1
# to reuse the plugin already unpacked under <SCRATCH_ROOT>/plugins.
# Neither source store is ever opened for writing: each is copied first.
# NO_SANDBOX=1 leaves the sandbox out (setup copies and snapshots only roblab;
# use it while the plugin half rebuilds its sandbox), and then stage b and the
# sandbox's shots are skipped.
#
# SHOOT_ONLY=1 re-shoots the build over the stores an earlier full run left,
# with no scenario: only the build and shots stages run. The shots stage then
# also shoots the hand-off (failing, then saved) over a throwaway copy of the
# roblab copy, and the Projects list's suggestion over a fresh sandbox copy,
# each with its own index cache; it skips the mockups. After a web change:
#   gate/p4/web-accept.sh <SCRATCH_ROOT> build
#   SHOOT_ONLY=1 gate/p4/web-accept.sh <SCRATCH_ROOT> shots

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="${1:?usage: gate/p4/web-accept.sh <SCRATCH_ROOT> [stage ...]}"
shift
mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd)"
STAGES=("$@")
[[ ${#STAGES[@]} -gt 0 ]] || STAGES=(setup a ana pin handoff verify c b shots a2)
STORE_SRC="${STORE_SRC:-/tmp/cas-exploration/store}"
SANDBOX_SRC="${SANDBOX_SRC:-}"
NEXTFLOW="${NEXTFLOW:-nextflow}"
export NXF_VER="${NXF_VER:-26.10.3}"
SHOTS="$REPO/docs/design/screenshots/p4"
SHOOT="$SHOTS/shoot.mjs"
WIGSHOP='bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce'

say() { echo "web-accept: $*" >&2; }
die() { say "$*"; exit 1; }
[[ -n "$SANDBOX_SRC" || -n "${NO_SANDBOX:-}" ]] \
    || die "set SANDBOX_SRC to the <SCRATCH_ROOT> of gate/p4/input-sandbox.sh, or NO_SANDBOX=1 to leave the sandbox out"

# Scratch everything; no cloud credential or Platform token reaches any process below.
for v in $(env | sed -nE 's/^((TOWER|SEQERA|AWS)_[A-Za-z0-9_]*)=.*/\1/p'); do unset "$v"; done
mkdir -p "$ROOT/aws"; : > "$ROOT/aws/config"; : > "$ROOT/aws/credentials"
export AWS_CONFIG_FILE="$ROOT/aws/config" AWS_SHARED_CREDENTIALS_FILE="$ROOT/aws/credentials" AWS_EC2_METADATA_DISABLED=true
export NXF_HOME="$ROOT/nxf-home" NXF_PLUGINS_DIR="$ROOT/plugins" XDG_CACHE_HOME="$ROOT/cache" NXF_ANSI_LOG=false
mkdir -p "$NXF_HOME" "$NXF_PLUGINS_DIR" "$XDG_CACHE_HOME" "$ROOT/logs" "$ROOT/state"
unset NXF_CONFIG_FILE NXF_OFFLINE NXF_WORK NXF_TEMP

EXPLORE_PID=''
cleanup() {
    local rc=$?
    if [[ -n "$EXPLORE_PID" ]]; then kill -TERM "$EXPLORE_PID" 2> /dev/null || true; wait "$EXPLORE_PID" 2> /dev/null || true; fi
    exit "$rc"
}
trap cleanup EXIT

# A plugin verb with one config. On Nextflow 26.08 and later plugin verbs read
# NXF_CONFIG_FILE, not -c.
verb() {   # <config> <verb> [args...]
    local config="$1"; shift
    (cd "$ROOT" && NXF_CONFIG_FILE="$config" NXF_PLUGINS_TEST_REPOSITORY="$PLUGINS_REPO" "$NEXTFLOW" -q plugin "$@")
}

# Serves one config, runs `node shoot.mjs <outdir> <url> <phase>` against it,
# then stops exactly that server.
serve() {   # <config> <phase> [env assignments for node...]
    local config="$1" phase="$2"; shift 2
    local log="$ROOT/logs/explore-$phase.log"
    (cd "$ROOT" && NXF_CONFIG_FILE="$config" NXF_PLUGINS_TEST_REPOSITORY="$PLUGINS_REPO" \
        exec "$NEXTFLOW" plugin nf-blocks:explore) > "$log" 2>&1 &
    EXPLORE_PID=$!
    local url=''
    for _ in $(seq 1 240); do
        url="$(sed -n 's/^nf-blocks explorer: \(http[^ ]*\).*/\1/p' "$log" | head -1)"
        [[ -n "$url" ]] && break
        kill -0 "$EXPLORE_PID" 2> /dev/null || die "explore exited, see $log"
        sleep 0.5
    done
    [[ -n "$url" ]] || die "explore never printed its URL, see $log"
    say "explorer ($phase, pid $EXPLORE_PID): $url"
    local rc=0
    env "$@" STATE="$ROOT/state" node "$SHOOT" "$SHOTS" "$url" "$phase" > "$ROOT/logs/shoot-$phase.log" 2>&1 || rc=$?
    cat "$ROOT/logs/shoot-$phase.log" >&2
    kill -TERM "$EXPLORE_PID" 2> /dev/null || true; wait "$EXPLORE_PID" 2> /dev/null || true; EXPLORE_PID=''
    [[ $rc -eq 0 ]] || die "shoot.mjs $phase failed ($rc), see $ROOT/logs/shoot-$phase.log"
}

plugin_version() {
    local dir; dir="$(find "$NXF_PLUGINS_DIR" -maxdepth 1 -type d -name 'nf-blocks-*' | head -1)"
    [[ -n "$dir" ]] || die "no nf-blocks plugin in $NXF_PLUGINS_DIR"
    PLUGIN_VERSION="${dir##*/nf-blocks-}"
    PLUGINS_REPO="file://$(GATE_SKIP_BUILD=1 "$REPO/gate/browser/plugin-repo.sh" "$REPO" "$ROOT")"
}
PLUGIN_VERSION=''; PLUGINS_REPO=''

snapshot() {   # <config> <name>
    verb "$1" nf-blocks:snapshot > "$ROOT/logs/snapshot-$2.log" 2>&1 || die "snapshot $2 failed, see $ROOT/logs/snapshot-$2.log"
    say "snapshot $2: $(tr '\n' ' ' < "$ROOT/logs/snapshot-$2.log")"
}

stage_build() {
    say "building (assemble; bundles web/ into the plugin)"
    "$REPO/gradlew" -q -p "$REPO" assemble > "$ROOT/logs/build.log" 2>&1 || die "build failed, see $ROOT/logs/build.log"
    local zip; zip="$(ls -t "$REPO"/build/distributions/nf-blocks-*.zip | head -1)"
    rm -rf "$NXF_PLUGINS_DIR"/nf-blocks-*
    unzip -q -o "$zip" -d "$NXF_PLUGINS_DIR/$(basename "${zip%.zip}")"
}

# SHOOT_ONLY: the shots a full run takes after its scenarios, over the stores that run left. The hand-off Save
# and the suggestion need state the scenarios changed, so each gets a scratch copy and its own index cache.
stage_reshoot() {
    serve "$ROOT/two.config" shots-two
    serve "$ROOT/roblab.config" shots-roblab
    [[ -n "${NO_SANDBOX:-}" ]] || serve "$ROOT/sandbox.config" shots-sandbox
    rm -rf "$ROOT/roblab-reshoot" "$ROOT/p4sandbox-base" "$ROOT/cache-reshoot"
    cp -R "$ROOT/roblab" "$ROOT/roblab-reshoot"
    sed "s|$ROOT/roblab'|$ROOT/roblab-reshoot'|" "$ROOT/roblab.config" > "$ROOT/roblab-reshoot.config"
    XDG_CACHE_HOME="$ROOT/cache-reshoot" serve "$ROOT/roblab-reshoot.config" reshoot-handoff
    [[ -z "${NO_SANDBOX:-}" ]] || return 0
    cp -R "$SANDBOX_SRC/store" "$ROOT/p4sandbox-base"; chmod -R u+w "$ROOT/p4sandbox-base"
    sed "s|$ROOT/p4sandbox'|$ROOT/p4sandbox-base'|" "$ROOT/sandbox.config" > "$ROOT/sandbox-base.config"
    XDG_CACHE_HOME="$ROOT/cache-reshoot" snapshot "$ROOT/sandbox-base.config" sandbox-base
    # The plugin half rebuilds its sandbox from time to time; a copy taken mid-rebuild has no runs to suggest.
    ! grep -q ' 0 runs' "$ROOT/logs/snapshot-sandbox-base.log" || die "the sandbox copy has no runs (is $SANDBOX_SRC being rebuilt?); try again later"
    XDG_CACHE_HOME="$ROOT/cache-reshoot" serve "$ROOT/sandbox-base.config" reshoot-suggestion
}

stage_setup() {
    [[ -d "$STORE_SRC/blocks" ]] || die "no store at $STORE_SRC"
    [[ -n "${NO_SANDBOX:-}" || -d "$SANDBOX_SRC/store/blocks" ]] || die "no sandbox store at $SANDBOX_SRC/store"
    # The Nextflow framework and nf-amazon from the sandbox's scratch home, so nothing downloads them.
    if [[ ! -d "$NXF_HOME/framework/$NXF_VER" && -d "$SANDBOX_SRC/nxf-home/framework/$NXF_VER" ]]; then
        mkdir -p "$NXF_HOME/framework"; cp -R "$SANDBOX_SRC/nxf-home/framework/$NXF_VER" "$NXF_HOME/framework/"
    fi
    for p in "$SANDBOX_SRC"/plugins/nf-amazon-*; do
        [[ -d "$p" && ! -d "$NXF_PLUGINS_DIR/$(basename "$p")" ]] && cp -R "$p" "$NXF_PLUGINS_DIR/"
    done
    [[ -n "${GATE_SKIP_BUILD:-}" ]] || stage_build
    "$NEXTFLOW" -version 2>&1 | grep -q "version ${NXF_VER//./\\.}" || die "NXF_VER=$NXF_VER does not start"
    plugin_version

    # The index cache goes too: it is keyed by member, and it would vouch for entries of an earlier copy.
    rm -rf "$ROOT/roblab" "$ROOT/p4sandbox" "$ROOT/org-local" "$ROOT/state"/* "$ROOT/resolve" "$ROOT/resolve-sink" "${XDG_CACHE_HOME:?}/nf-blocks"
    cp -R "$STORE_SRC" "$ROOT/roblab"
    [[ -n "${NO_SANDBOX:-}" ]] || cp -R "$SANDBOX_SRC/store" "$ROOT/p4sandbox"
    mkdir -p "$ROOT/org-local" "$ROOT/resolve-sink"
    chmod -R u+w "$ROOT/roblab"; [[ -n "${NO_SANDBOX:-}" ]] || chmod -R u+w "$ROOT/p4sandbox"

    local v="$PLUGIN_VERSION"
    config() {   # <file> <asserted_by> <stores block> <lineage alias>
        cat > "$1" << EOF
plugins { id 'nf-blocks@$v' }
lineage.enabled = true
lineage.store.location = 'cas://$4'
cas {
    stores {
$3
    }
    asserted_by = '$2'
}
EOF
    }
    local roblab="        roblab { location = '$ROOT/roblab' }"
    local org="        'org-local' { location = '$ROOT/org-local' }"
    config "$ROOT/roblab.config" alice@example.org "$roblab" roblab
    config "$ROOT/ana.config" ana.k@example.org "$roblab" roblab
    config "$ROOT/two.config" alice@example.org "$roblab
$org" roblab
    config "$ROOT/org.config" alice@example.org "$org" org-local
    # For reading cas:// in a pipeline: the run's own lineage goes to an empty scratch member, so it
    # records nothing into the roblab copy, and the samplesheet can only come from roblab.
    config "$ROOT/resolve.config" alice@example.org "$roblab
        'resolve-sink' { location = '$ROOT/resolve-sink' }" resolve-sink
    config "$ROOT/sandbox.config" alice@example.org "        p4sandbox { location = '$ROOT/p4sandbox' }" p4sandbox
    snapshot "$ROOT/roblab.config" roblab-0
    [[ -n "${NO_SANDBOX:-}" ]] || snapshot "$ROOT/sandbox.config" sandbox-0
}

stage_handoff_checks() {
    # The samplesheet block: present under blocks/, its bytes the previewed text.
    python3 -I - "$ROOT" << 'PY'
import hashlib, json, os, sys
root = sys.argv[1]
st = json.load(open(f'{root}/state/handoff.json'))
B32 = 'abcdefghijklmnopqrstuvwxyz234567'
def raw_cid(data):
    mh = bytes([0x01, 0x55, 0x12, 0x20]) + hashlib.sha256(data).digest()
    bits = ''.join(f'{b:08b}' for b in mh)
    bits += '0' * (-len(bits) % 5)
    return 'b' + ''.join(B32[int(bits[i:i + 5], 2)] for i in range(0, len(bits), 5))
for name in ('samplesheet', 'contrasts'):
    cid, text = st[f'{name}_cid'], st[f'{name}_text']
    path = f'{root}/roblab/blocks/{cid[-2:]}/{cid}'
    assert os.path.isfile(path), f'no block {path}'
    data = open(path, 'rb').read()
    assert data == text.encode(), f'{name}: the block bytes differ from the previewed text'
    assert raw_cid(data) == cid, f'{name}: sha2-256 of the block is {raw_cid(data)}, not {cid}'
    print(f'{name}.csv: block {cid}, {len(data)} bytes, equal to the preview, hash checked')
hp = st['handoff_address']
assert os.path.isfile(f'{root}/roblab/blocks/{hp[-2:]}/{hp}'), 'no Handoff block'
logs = [n for n in os.listdir(f'{root}/roblab/log') if n.endswith('-handoff-' + hp)]
assert len(logs) == 1, f'handoff log entries: {logs}'
print(f'Handoff {hp}: block present, log entry {logs[0]}')
PY
    # cas://<cid>/samplesheet.csv through the plugin's file system, in a pipeline run on the store's config.
    local cid; cid="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["samplesheet_cid"])' "$ROOT/state/handoff.json")"
    mkdir -p "$ROOT/resolve"
    LOGS_BEFORE="$(ls "$ROOT/roblab/log" | wc -l | tr -d ' ')"
    cat > "$ROOT/resolve/main.nf" << 'EOF'
workflow {
    def f = file(params.address)
    def out = file(params.out)
    out.text = f.text
    println "resolved ${params.address}: ${f.size()} bytes, name ${f.name}"
}
EOF
    (cd "$ROOT/resolve" && "$NEXTFLOW" -q run main.nf -c "$ROOT/resolve.config" \
        --address "cas://$cid/samplesheet.csv" --out "$ROOT/resolve/samplesheet.csv" > "$ROOT/logs/resolve.log" 2>&1) \
        || die "cas://$cid/samplesheet.csv did not resolve, see $ROOT/logs/resolve.log"
    cat "$ROOT/logs/resolve.log" >&2
    local logs; logs="$(ls "$ROOT/roblab/log" | wc -l | tr -d ' ')"
    [[ "$logs" == "$LOGS_BEFORE" ]] || die "the resolving run wrote into the roblab copy's log ($LOGS_BEFORE entries before, $logs after)"
    cmp "$ROOT/resolve/samplesheet.csv" "$ROOT/roblab/blocks/${cid: -2}/$cid" || die "the resolved file differs from the block"
    say "cas://$cid/samplesheet.csv resolves to the block's bytes"
}

stage_c_copy() {
    say "copy wigshop to org-local"
    verb "$ROOT/two.config" nf-blocks:copy "selection:$WIGSHOP" --to org-local --depth results --claims names,projects \
        --format json > "$ROOT/state/copy.json" 2> "$ROOT/logs/copy.err" || die "copy failed: $(cat "$ROOT/logs/copy.err")"
    python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print("copied", d.get("copied"), "blocks,", d.get("copied_bytes"), "bytes; failures", d.get("failures"))' "$ROOT/state/copy.json" >&2
}


if [[ -n "${SHOOT_ONLY:-}" ]]; then
    for stage in "${STAGES[@]}"; do [[ "$stage" == build || "$stage" == shots ]] || die "SHOOT_ONLY runs only the build and shots stages, not $stage"; done
fi

for stage in "${STAGES[@]}"; do
    say "--- stage $stage"
    [[ "$stage" == setup || "$stage" == build || -n "$PLUGIN_VERSION" ]] || plugin_version
    case "$stage" in
        setup) stage_setup ;;
        build) stage_build; plugin_version ;;
        a) serve "$ROOT/roblab.config" a ;;
        ana) serve "$ROOT/ana.config" ana ;;
        pin) serve "$ROOT/ana.config" pin ;;
        handoff) serve "$ROOT/roblab.config" handoff ;;
        verify) stage_handoff_checks ;;
        c)
            serve "$ROOT/roblab.config" c-add
            stage_c_copy
            serve "$ROOT/two.config" c-policy
            # Pins are per store: the copied wigshop gets its policy pin in org-local from a page that writes there.
            serve "$ROOT/org.config" c-org-pin
            snapshot "$ROOT/roblab.config" roblab-1
            snapshot "$ROOT/org.config" org-local
            ;;
        b)
            if [[ -n "${NO_SANDBOX:-}" ]]; then say "NO_SANDBOX: stage b skipped"; continue; fi
            serve "$ROOT/sandbox.config" b; snapshot "$ROOT/sandbox.config" sandbox-1 ;;
        shots)
            if [[ -n "${SHOOT_ONLY:-}" ]]; then stage_reshoot; continue; fi
            serve "$ROOT/two.config" shots-two
            serve "$ROOT/roblab.config" shots-roblab
            if [[ -z "${NO_SANDBOX:-}" ]]; then serve "$ROOT/sandbox.config" shots-sandbox; fi
            STATE="$ROOT/state" node "$SHOOT" "$SHOTS" "file://$REPO/docs/design/mockups/" mock
            ;;
        a2)
            serve "$ROOT/roblab.config" a2
            # The same routes over a fresh roblab copy with no project, for the cost the project adds.
            rm -rf "$ROOT/roblab-base"; cp -R "$STORE_SRC" "$ROOT/roblab-base"; chmod -R u+w "$ROOT/roblab-base"
            sed "s|$ROOT/roblab'|$ROOT/roblab-base'|" "$ROOT/roblab.config" > "$ROOT/roblab-base.config"
            snapshot "$ROOT/roblab-base.config" roblab-base
            serve "$ROOT/roblab-base.config" a2 A2_BASE=1
            ;;
        # Not in the default list: PROBE names a module shoot.mjs runs against PROBE_CONFIG (default two).
        probe) serve "$ROOT/${PROBE_CONFIG:-two}.config" probe PROBE="${PROBE:?probe needs PROBE}" ;;
        *) die "unknown stage $stage" ;;
    esac
done
say "PASS: results in $ROOT, shots in $SHOTS"
