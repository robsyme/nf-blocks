#!/usr/bin/env bash
#
# P3 acceptance on real AWS S3 (docs/design/03-agent-prompts.md, P3): copy a
# Selection from a scratch copy of a local store to an S3 prefix, check that the
# same `fromStore(selection: ...)` resolves from the S3 store alone with only the
# store config changed, count the S3 requests a dry run and a snapshot build
# make, check the D9 refusal, and check what landed in S3 for secrets.
#
#   AWS_PROFILE=<sso profile> AWS_REGION=<region> S3_DEST=s3://<bucket>/<prefix> gate/p3/aws-accept.sh <SCRATCH_ROOT>
#
# The prefix must be empty at start. The script writes only under S3_DEST. With
# DELETE_PREFIX=1 it deletes exactly S3_DEST/ on every exit and lists it to show
# it is empty; otherwise it leaves the objects and prints the delete command. It
# never logs in, never writes ~/.aws, and never touches bucket configuration.
# Credentials come from the profile only: any AWS_ACCESS_KEY_ID/SECRET/SESSION
# variables and endpoint overrides are unset. If the SSO token has expired the
# first `aws` call fails and the script stops.
#
# Requests are counted client-side from the AWS SDK's own request log
# (`nextflow -trace software.amazon.awssdk.request`): one "Sending Request" line
# per HTTP request, with method, host and path (header names only).
#
# Other inputs: STORE_SRC (default /tmp/cas-exploration/store, read only; a copy
# is made), SELECTION (default the wigshop Selection), NEXTFLOW, NXF_VER,
# P3_SHOOT (as in local-accept.sh; called with the S3-alone explorer URL and
# phase `dest`), GATE_SKIP_BUILD=1 to reuse an installed plugin under ROOT.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="${1:?usage: gate/p3/aws-accept.sh <SCRATCH_ROOT>}"
mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd)"
STORE_SRC="${STORE_SRC:-/tmp/cas-exploration/store}"
SELECTION="${SELECTION:-bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce}"
NEXTFLOW="${NEXTFLOW:-nextflow}"
export NXF_VER="${NXF_VER:-26.04.6}"
ASSERTED_BY='p3-accept@example.org'

say() { echo "aws-accept: $*" >&2; }
die() { say "$*"; exit 1; }

: "${AWS_PROFILE:?set AWS_PROFILE (an SSO profile you are logged in with)}"
: "${AWS_REGION:?set AWS_REGION}"
S3_DEST="${S3_DEST:?set S3_DEST=s3://<bucket>/<prefix>}"
S3_DEST="${S3_DEST%/}"
[[ "$S3_DEST" =~ ^s3://([a-z0-9][a-z0-9.-]+)/([A-Za-z0-9._/-]+)$ ]] || die "S3_DEST must be s3://<bucket>/<prefix> with a non-empty prefix, got $S3_DEST"
BUCKET="${BASH_REMATCH[1]}"; PREFIX="${BASH_REMATCH[2]}"
[[ "$PREFIX" != *..* && "$PREFIX" != */ && "$PREFIX" != /* ]] || die "refusing prefix $PREFIX"
[[ -d "$STORE_SRC/blocks" ]] || die "no store at $STORE_SRC"
export AWS_PROFILE AWS_REGION
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 AWS_DEFAULT_PROFILE
export NXF_HOME="$ROOT/nxf-home" NXF_PLUGINS_DIR="$ROOT/plugins" XDG_CACHE_HOME="$ROOT/cache"
mkdir -p "$NXF_HOME" "$NXF_PLUGINS_DIR" "$XDG_CACHE_HOME"
unset NXF_CONFIG_FILE NXF_OFFLINE

aws sts get-caller-identity > /dev/null || die "the $AWS_PROFILE credentials don't work (expired SSO token?); log in yourself and run again"
[[ -z "$(aws s3 ls "$S3_DEST/" --recursive)" ]] || die "$S3_DEST/ is not empty; refusing to start"

EXPLORE_PID=''
cleanup() {
    local rc=$?
    if [[ -n "$EXPLORE_PID" ]]; then kill -TERM "$EXPLORE_PID" 2> /dev/null || true; fi
    wait 2> /dev/null || true
    if [[ "${DELETE_PREFIX:-0}" == 1 ]]; then
        say "deleting $S3_DEST/ (that prefix only)"
        aws s3 rm "$S3_DEST/" --recursive --only-show-errors || say "the delete of $S3_DEST/ FAILED; delete it by hand"
        local left
        left="$(aws s3 ls "$S3_DEST/" --recursive || true)"
        if [[ -z "$left" ]]; then say "$S3_DEST/ is empty"; else say "$S3_DEST/ still holds:"; echo "$left" >&2; rc=1; fi
    else
        say "left in place: $(aws s3 ls "$S3_DEST/" --recursive | wc -l | tr -d ' ') objects under $S3_DEST/. Delete them with:"
        say "  AWS_PROFILE=$AWS_PROFILE AWS_REGION=$AWS_REGION aws s3 rm $S3_DEST/ --recursive && AWS_PROFILE=$AWS_PROFILE AWS_REGION=$AWS_REGION aws s3 ls $S3_DEST/ --recursive"
    fi
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

rm -rf "$ROOT/roblab"
cp -R "$STORE_SRC" "$ROOT/roblab"

# Every config pins the signer, so nothing depends on the environment's Platform token.
# The region and profile come from the environment, so no config text names the profile.
cat > "$ROOT/copy.config" << EOF
plugins { id 'nf-blocks@$PLUGIN_VERSION' }
lineage.enabled = true
lineage.store.location = 'cas://roblab'
aws { region = '$AWS_REGION' }
cas {
    stores {
        roblab { location = '$ROOT/roblab' }
        'org-s3' { location = '$S3_DEST' }
    }
    asserted_by = '$ASSERTED_BY'
}
EOF
cat > "$ROOT/local.config" << EOF
plugins { id 'nf-blocks@$PLUGIN_VERSION' }
lineage.enabled = true
lineage.store.location = 'cas://roblab'
cas { stores { roblab { location = '$ROOT/roblab' } }; asserted_by = '$ASSERTED_BY' }
EOF
cat > "$ROOT/s3.config" << EOF
plugins { id 'nf-blocks@$PLUGIN_VERSION' }
lineage.enabled = true
lineage.store.location = 'cas://org-s3'
aws { region = '$AWS_REGION' }
cas { stores { 'org-s3' { location = '$S3_DEST' } }; asserted_by = '$ASSERTED_BY' }
EOF

# One traced command per directory, so each .nextflow.log counts one command's requests.
traced() {  # traced <name> <config> <plugin verb args...>   (stdout to <name>/out.json)
    local name="$1" cfg="$2"; shift 2
    mkdir -p "$ROOT/trace/$name"
    (cd "$ROOT/trace/$name" && "$NEXTFLOW" -q -trace software.amazon.awssdk.request -c "$cfg" plugin "$@" > out.json 2> err.log) \
        || die "$name failed, see $ROOT/trace/$name"
}
count() {   # count <name>: requests to S3 by method, and HEADs on blocks/
    python3 - "$ROOT/trace/$1/.nextflow.log" "$1" "$PREFIX" << 'PY'
import collections, json, re, sys
log, name, prefix = sys.argv[1:]
by = collections.Counter(); heads_blocks = 0
for line in open(log, errors='replace'):
    if 'Sending Request: DefaultSdkHttpFullRequest(' not in line: continue
    m = re.search(r'httpMethod=([A-Z]+), protocol=\w+, host=([^,]+), (?:port=\d+, )?encodedPath=([^,]*)', line)
    if not m or '.s3.' not in m.group(2): continue
    by[m.group(1)] += 1
    if m.group(1) == 'HEAD' and m.group(3).startswith(f'/{prefix}/blocks/'): heads_blocks += 1
print(json.dumps({'command': name, 's3_requests': sum(by.values()), 'by_method': dict(by), 'head_blocks': heads_blocks}))
PY
}

copy() { "$NEXTFLOW" -q -c "$ROOT/copy.config" plugin nf-blocks:copy "selection:$SELECTION" --to org-s3 --depth results --format json "$@"; }

cd "$ROOT"
say "dry run (traced)"
traced dryrun "$ROOT/copy.config" nf-blocks:copy "selection:$SELECTION" --to org-s3 --depth results --format json --dry-run
cp "$ROOT/trace/dryrun/out.json" "$ROOT/plan.json"
count dryrun > "$ROOT/cost-dryrun.json"
say "copy (traced)"
traced copy "$ROOT/copy.config" nf-blocks:copy "selection:$SELECTION" --to org-s3 --depth results --format json
cp "$ROOT/trace/copy/out.json" "$ROOT/copied.json"
count copy > "$ROOT/cost-copy.json"
say "second dry run (traced)"
traced dryrun2 "$ROOT/copy.config" nf-blocks:copy "selection:$SELECTION" --to org-s3 --depth results --format json --dry-run
cp "$ROOT/trace/dryrun2/out.json" "$ROOT/plan-after.json"
count dryrun2 > "$ROOT/cost-dryrun2.json"

python3 - "$ROOT" << 'PY'
import json, sys
root = sys.argv[1]
plan, done, after = (json.load(open(f'{root}/{n}.json')) for n in ('plan', 'copied', 'plan-after'))
p, a = plan['plan'], after['plan']
print(f"plan: {p['blocks']} blocks, {p['present']} present, {p['transfer']} to transfer, {p['transfer_bytes']} bytes, audit {len(p['audit']['findings'])} findings, kept {p['kept']}")
print(f"copied: {done.get('copied')} blocks, {done.get('copied_bytes')} bytes, transfer {done.get('transfer_entry')}, snapshot {done.get('snapshot')}")
print(f"after: {a['present']} of {a['blocks']} present, {a['transfer']} to transfer")
assert not done.get('failures'), done.get('failures')
assert done.get('copied') == p['transfer'], 'copied differs from the plan'
assert a['transfer'] == 0 and a['present'] == a['blocks'], 'the second dry run still has blocks to transfer'
PY

# The same pipeline against each store, the S3 store alone the second time.
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
diff "$ROOT/run-local/out/hashes.txt" "$ROOT/run-s3/out/hashes.txt" > /dev/null \
    || { diff "$ROOT/run-local/out/hashes.txt" "$ROOT/run-s3/out/hashes.txt" >&2 || true; die "fromStore resolved different files from S3"; }
say "fromStore resolves the same $(wc -l < "$ROOT/run-s3/out/hashes.txt" | tr -d ' ') files from the S3 store alone"

# The explorer on the S3 store alone, before any measurement Claim.
if [[ -n "${P3_SHOOT:-}" ]]; then
    "$NEXTFLOW" -c "$ROOT/s3.config" plugin nf-blocks:explore > "$ROOT/explore-dest.log" 2>&1 &
    EXPLORE_PID=$!
    url=''
    for _ in $(seq 1 180); do
        url="$(sed -n 's/^nf-blocks explorer: \(http[^ ]*\).*/\1/p' "$ROOT/explore-dest.log" | head -1)"
        [[ -n "$url" ]] && break
        sleep 1
    done
    [[ -n "$url" ]] || die "explore never printed its URL, see $ROOT/explore-dest.log"
    say "explorer (S3 alone): $url"
    DEST=org-s3 $P3_SHOOT "$url" dest
    kill -TERM "$EXPLORE_PID" 2> /dev/null || true; wait "$EXPLORE_PID" 2> /dev/null || true; EXPLORE_PID=''
fi

# The presence cost: a snapshot build after a Claim, then one without a new Claim.
cat > "$ROOT/claim.json" << EOF
{"kind": "Claim", "subject": {"/": "$SELECTION"}, "verb": "set", "attribute": "p3_measure", "value": "1",
 "supersedes": [], "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S).000Z"}
EOF
"$NEXTFLOW" -q -c "$ROOT/s3.config" plugin nf-blocks:put "$ROOT/claim.json" > "$ROOT/claim-put.json" || die "the measurement Claim failed: $(cat "$ROOT/claim-put.json")"
say "snapshot after a Claim (traced)"
traced snap-after-claim "$ROOT/s3.config" nf-blocks:snapshot
count snap-after-claim > "$ROOT/cost-snapshot-after-claim.json"
say "snapshot without a new Claim (traced)"
traced snap-quiet "$ROOT/s3.config" nf-blocks:snapshot
count snap-quiet > "$ROOT/cost-snapshot-quiet.json"

# D9 on S3: release the copied run in org-s3; the plan says not kept and the copy writes nothing.
RUN_CID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["plan"]["runs"][0]["completion"])' "$ROOT/copied.json")"
cat > "$ROOT/release.json" << EOF
{"kind": "Claim", "subject": {"/": "$RUN_CID"}, "verb": "set", "attribute": "retain", "value": "lineage",
 "supersedes": [], "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S).000Z"}
EOF
"$NEXTFLOW" -q -c "$ROOT/s3.config" plugin nf-blocks:put "$ROOT/release.json" > "$ROOT/release-put.json" || die "the release Claim failed: $(cat "$ROOT/release-put.json")"
copy --dry-run > "$ROOT/plan-released.json"
aws s3 ls "$S3_DEST/" --recursive | awk '{print $3, $4}' | sort > "$ROOT/listing-before.txt"
set +e; copy > "$ROOT/copy-released.json" 2> "$ROOT/copy-released.err"; rc=$?; set -e
aws s3 ls "$S3_DEST/" --recursive | awk '{print $3, $4}' | sort > "$ROOT/listing-after.txt"
python3 - "$ROOT" "$RUN_CID" "$rc" << 'PY'
import json, sys
root, run, rc = sys.argv[1], sys.argv[2], int(sys.argv[3])
p = json.load(open(f'{root}/plan-released.json'))['plan']
st = [d for d in p['destination_state'] if d['cid'] == run]
print(f"released run: kept {p['kept']}, state {st[0]['state'] if st else None}; the real copy exited {rc}")
assert p['kept'] is False and st and st[0]['state'] == 'released'
assert rc == 1
PY
cmp -s "$ROOT/listing-before.txt" "$ROOT/listing-after.txt" || die "the refused copy changed $S3_DEST/"
say "the refused copy wrote nothing ($(wc -l < "$ROOT/listing-after.txt" | tr -d ' ') objects unchanged)"

# Secrets: pull every object under the prefix and grep it, and every log and stdout, for credentials and local paths.
rm -rf "$ROOT/s3-dump"
aws s3 cp "$S3_DEST/" "$ROOT/s3-dump/" --recursive --only-show-errors
# The live credentials go to the checker through its environment only, never to disk or stdout.
P3_CREDS="$(aws configure export-credentials --format process)" python3 - "$ROOT" "$AWS_PROFILE" "$HOME" << 'PY' > "$ROOT/secrets-check.json"
import json, os, re, sys
root, profile, home = sys.argv[1:]
creds = json.loads(os.environ.pop('P3_CREDS'))
literal = {'access key id': creds.get('AccessKeyId'), 'secret access key': creds.get('SecretAccessKey'), 'session token': creds.get('SessionToken')}
patterns = {'AKIA/ASIA key id': re.compile(rb'\b(?:AKIA|ASIA)[0-9A-Z]{16}\b'),
            'aws_secret_access_key': re.compile(rb'aws_secret_access_key', re.I),
            'aws_session_token': re.compile(rb'aws_session_token', re.I),
            '~/.aws': re.compile(rb'(?:~|' + re.escape(home.encode()) + rb')/\.aws'),
            'sso cache path': re.compile(rb'\.aws/sso/cache'),
            'home directory': re.compile(re.escape(home.encode()) + rb'(?![\w.-])|' + re.escape(home.encode()) + rb'/'),
            f'profile name "{profile}" outside the bucket name': re.compile(re.escape(profile.encode()) + rb'(?!-playground)')}
groups = {'s3 objects': [f'{root}/s3-dump'],
          'logs and stdout': [f'{root}/trace', f'{root}/run-s3', f'{root}/run-local', root]}
out = {}
for group, dirs in groups.items():
    files = set()
    for d in dirs:
        if os.path.isfile(d): files.add(d); continue
        for base, _, names in os.walk(d):
            if group == 'logs and stdout' and ('s3-dump' in base or '/roblab' in base or '/plugins' in base or '/nxf-home' in base or '/cache' in base or '/work' in base): continue
            if group == 'logs and stdout' and base == root:
                names = [n for n in names if n.endswith(('.json', '.log', '.err', '.txt'))]
            files.update(os.path.join(base, n) for n in names)
    hits = {}
    for f in sorted(files):
        data = open(f, 'rb').read()
        for label, value in literal.items():
            if value and value.encode() in data: hits.setdefault(label, []).append(os.path.relpath(f, root))
        for label, rx in patterns.items():
            if rx.search(data): hits.setdefault(label, []).append(os.path.relpath(f, root))
    out[group] = {'files': len(files), 'hits': hits}
print(json.dumps(out, indent=1))
PY
say "secrets check: $(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print({g: (v["files"], sorted(v["hits"])) for g, v in d.items()})' "$ROOT/secrets-check.json")"

for f in cost-dryrun cost-copy cost-dryrun2 cost-snapshot-after-claim cost-snapshot-quiet; do cat "$ROOT/$f.json"; done
say "PASS (S3 on AWS): results are in $ROOT"
