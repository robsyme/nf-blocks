#!/usr/bin/env bash
#
# P4 input sandbox: a local store whose runs carry populated InputSets, for the
# explorer's sample identity by input reads and its project suggestions.
#
#   gate/p4/input-sandbox.sh <SCRATCH_ROOT>
#
# Into <SCRATCH_ROOT>/store it writes four runs:
#   p4-rnaseq-1         nf-core/rnaseq (the output-records fork) -profile test
#   p4-rnaseq-2         the same pipeline and inputs again, a fresh run
#   p4-rnaseq-wt-short  the test samplesheet with WT_REP1's reads cut to their
#                       first 40,000 lines (new bytes, new addresses), every
#                       other row unchanged; -resume from p4-rnaseq-1
#   p4-tiny-reads       gate/p4/tiny-reads, local reads sharing nothing with them
# then writes the member's snapshot and checks the InputSets and run_input rows
# (gate/p4/sandbox_checks.py), exiting non-zero when a check fails.
#
# Inputs: RNASEQ (required: the nf-core/rnaseq output-records fork's directory,
# run in place, never written),
# NEXTFLOW, NXF_VER (default 26.10.3; the fork needs >= 26.09.0-edge),
# PROFILE (default test,docker,arm64), SECOND_RUN (fresh, the default, or
# resume), GATE_SKIP_BUILD=1 to reuse the plugin already in <SCRATCH_ROOT>/plugins.
# Downloaded WT_REP1 reads are kept in <SCRATCH_ROOT>/inputs/download between
# attempts; everything else under <SCRATCH_ROOT> except nxf-home and plugins is
# rebuilt.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="${1:?usage: gate/p4/input-sandbox.sh <SCRATCH_ROOT>}"
mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd)"
RNASEQ="${RNASEQ:-}"
NEXTFLOW="${NEXTFLOW:-nextflow}"
PROFILE="${PROFILE:-test,docker,arm64}"
SECOND_RUN="${SECOND_RUN:-fresh}"
export NXF_VER="${NXF_VER:-26.10.3}"
ASSERTED_BY='p4-test@example.org'
# The test profile's samplesheet (conf/test.config in the fork).
TEST_SHEET='https://raw.githubusercontent.com/nf-core/test-datasets/626c8fab639062eade4b10747e919341cbf9b41a/samplesheet/v3.10/samplesheet_test.csv'

say() { echo "input-sandbox: $*" >&2; }
die() { say "$*"; exit 1; }
[[ -n "$RNASEQ" ]] || die "set RNASEQ to the nf-core/rnaseq output-records fork's directory"
[[ -f "$RNASEQ/main.nf" ]] || die "no pipeline at $RNASEQ"
[[ "$SECOND_RUN" == fresh || "$SECOND_RUN" == resume ]] || die "SECOND_RUN is fresh or resume, not $SECOND_RUN"

# Scratch everything: no Platform token, cloud credential or user config reaches a run.
for v in $(env | sed -nE 's/^((TOWER|SEQERA|AWS)_[A-Za-z0-9_]*)=.*/\1/p'); do unset "$v"; done
mkdir -p "$ROOT/aws"; : > "$ROOT/aws/config"; : > "$ROOT/aws/credentials"
export AWS_CONFIG_FILE="$ROOT/aws/config" AWS_SHARED_CREDENTIALS_FILE="$ROOT/aws/credentials" AWS_EC2_METADATA_DISABLED=true
export NXF_HOME="$ROOT/nxf-home" NXF_PLUGINS_DIR="$ROOT/plugins" XDG_CACHE_HOME="$ROOT/cache" NXF_ANSI_LOG=false
unset NXF_CONFIG_FILE NXF_OFFLINE NXF_WORK NXF_TEMP
rm -rf "${ROOT:?}/store" "${ROOT:?}/cache" "${ROOT:?}/runs" "${ROOT:?}/inputs/wt-short" "${ROOT:?}/inputs/tiny" \
       "${ROOT:?}/logs" "${ROOT:?}/checks.txt"
mkdir -p "$NXF_HOME" "$NXF_PLUGINS_DIR" "$XDG_CACHE_HOME" "$ROOT/store" "$ROOT/runs" "$ROOT/logs" \
         "$ROOT/inputs/download" "$ROOT/inputs/wt-short" "$ROOT/inputs/tiny"
"$NEXTFLOW" -version 2>&1 | grep -q "version ${NXF_VER//./\\.}" || die "NXF_VER=$NXF_VER does not start: $("$NEXTFLOW" -version 2>&1 | tail -3)"

# The plugin, built and unpacked into the scratch NXF_PLUGINS_DIR as gate.sh does.
if [[ -z "${GATE_SKIP_BUILD:-}" ]]; then
    say "building nf-blocks into $NXF_PLUGINS_DIR"
    "$REPO/gradlew" -q -p "$REPO" assemble installPlugin > "$ROOT/logs/build.log" 2>&1 \
        || die "build failed, see $ROOT/logs/build.log"
    zip="$(ls -t "$REPO"/build/distributions/nf-blocks-*.zip 2> /dev/null | head -1 || true)"
    [[ -n "$zip" ]] || die "no zip under $REPO/build/distributions after the build"
    rm -rf "$NXF_PLUGINS_DIR"/nf-blocks-*
    unzip -q -o "$zip" -d "$NXF_PLUGINS_DIR/$(basename "${zip%.zip}")"
fi
# nf-blocks requires nf-amazon. An unpacked nf-blocks is never asked for its
# dependencies' metadata, so a run cannot fetch nf-amazon itself (gate.sh says
# more). Unpinned, this installs the version this Nextflow names; a no-op once there.
"$NEXTFLOW" plugin install nf-amazon > "$ROOT/logs/nf-amazon-install.log" 2>&1 \
    || die "nf-amazon install failed, see $ROOT/logs/nf-amazon-install.log"
PLUGIN_DIR="$(find "$NXF_PLUGINS_DIR" -maxdepth 1 -type d -name 'nf-blocks-*' | head -1)"
[[ -n "$PLUGIN_DIR" ]] || die "no nf-blocks plugin in $NXF_PLUGINS_DIR"
PLUGIN_VERSION="${PLUGIN_DIR##*/nf-blocks-}"
# Only the plugin verbs, which start nf-blocks unpinned, look in this repository.
# It replaces the registry, so the runs go without it: they find the pinned
# nf-blocks unpacked already and fetch nf-amazon (the version this Nextflow
# wants) and nf-schema from the registry.
PLUGINS_REPO="file://$("$REPO/gate/browser/plugin-repo.sh" "$REPO" "$ROOT")"

# The plugins block replaces the pipeline's, so it pins nf-schema again.
cat > "$ROOT/cas.config" << EOF
plugins {
    id 'nf-blocks@$PLUGIN_VERSION'
    id 'nf-schema@2.7.2'
}
outputDir = 'cas://p4sandbox'
lineage {
    enabled = true
    store.location = 'cas://p4sandbox'
}
cas {
    stores { p4sandbox { location = '$ROOT/store' } }
    asserted_by = '$ASSERTED_BY'
}
EOF
# The tiny pipeline needs no nf-schema; its own config, so nothing downloads for it.
sed "/nf-schema/d" "$ROOT/cas.config" > "$ROOT/cas-tiny.config"

# Run 3's samplesheet: WT_REP1's two read pairs cut to their first 40,000 lines.
say "making the short WT_REP1 reads"
curl -sSfL --retry 3 "$TEST_SHEET" -o "$ROOT/inputs/samplesheet_test.csv" || die "could not fetch $TEST_SHEET"
python3 -I - "$ROOT/inputs" << 'PY'
import csv, gzip, os, sys, urllib.request
inputs = sys.argv[1]
rows = list(csv.DictReader(open(f'{inputs}/samplesheet_test.csv', newline='')))
wt = [r for r in rows if r['sample'] == 'WT_REP1']
assert len(wt) == 2, f'the test samplesheet has {len(wt)} WT_REP1 rows, expected 2'
for r in wt:
    for col in ('fastq_1', 'fastq_2'):
        url = r[col]
        name = url.rsplit('/', 1)[1]
        src = f'{inputs}/download/{name}'
        if not os.path.exists(src):
            urllib.request.urlretrieve(url, src + '.part')
            os.rename(src + '.part', src)
        out = f'{inputs}/wt-short/{name.replace(".fastq.gz", "_short.fastq.gz")}'
        with gzip.open(src, 'rt') as fin, open(out, 'wb') as raw:
            # mtime=0 and no name: the same lines give the same bytes on every attempt.
            with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as fout:
                for i, line in enumerate(fin):
                    if i == 40000:
                        break
                    fout.write(line.encode())
        r[col] = out
with open(f'{inputs}/samplesheet_wt_short.csv', 'w', newline='') as fh:
    w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), lineterminator='\n')
    w.writeheader()
    w.writerows(rows)
PY

# The unrelated run's reads: three small pairs, seeded, so their bytes are the same every attempt.
python3 -I - "$ROOT/inputs/tiny" << 'PY'
import gzip, random, sys
out = sys.argv[1]
rng = random.Random(20261025)
sheet = ['sample,fastq_1,fastq_2']
for sample in ('TINY_A', 'TINY_B', 'TINY_C'):
    paths = []
    for mate in (1, 2):
        path = f'{out}/{sample}_R{mate}.fastq.gz'
        lines = []
        for n in range(200):
            seq = ''.join(rng.choice('ACGT') for _ in range(50))
            lines += [f'@{sample}.{n}/{mate}', seq, '+', 'I' * 50]
        with open(path, 'wb') as raw, gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as fh:
            fh.write(('\n'.join(lines) + '\n').encode())
        paths.append(path)
    sheet.append(','.join([sample] + paths))
open(f'{out}/samplesheet.csv', 'w').write('\n'.join(sheet) + '\n')
PY

# One run, timed; its launch directory and log under runs/<launch>.
TIMES=()
run() {
    local name="$1" launch="$2"; shift 2
    mkdir -p "$ROOT/runs/$launch"
    say "run $name (in runs/$launch)"
    local t0=$SECONDS
    (cd "$ROOT/runs/$launch" && "$NEXTFLOW" run "$@" -name "$name" > "$ROOT/logs/$name.log" 2>&1) \
        || die "run $name failed, see $ROOT/logs/$name.log and $ROOT/runs/$launch/.nextflow.log"
    TIMES+=("$name $((SECONDS - t0))s")
    say "run $name took $((SECONDS - t0))s"
}
rnaseq() { echo "$RNASEQ" -profile "$PROFILE" -c "$ROOT/cas.config" -w "$ROOT/runs/work-$1"; }

run p4-rnaseq-1 rnaseq-1 $(rnaseq rnaseq-1)
if [[ "$SECOND_RUN" == fresh ]]; then
    run p4-rnaseq-2 rnaseq-2 $(rnaseq rnaseq-2)
else
    run p4-rnaseq-2 rnaseq-1 $(rnaseq rnaseq-1) -resume p4-rnaseq-1
fi
run p4-rnaseq-wt-short rnaseq-1 $(rnaseq rnaseq-1) -resume p4-rnaseq-1 --input "$ROOT/inputs/samplesheet_wt_short.csv"
run p4-tiny-reads tiny "$REPO/gate/p4/tiny-reads/main.nf" -c "$ROOT/cas-tiny.config" -w "$ROOT/runs/work-tiny" \
    --input "$ROOT/inputs/tiny/samplesheet.csv"

say "snapshot"
# On Nextflow 26.08.0-edge and later, -c does not reach plugin verbs; NXF_CONFIG_FILE does.
(cd "$ROOT" && NXF_CONFIG_FILE="$ROOT/cas.config" NXF_PLUGINS_TEST_REPOSITORY="$PLUGINS_REPO" "$NEXTFLOW" -q plugin nf-blocks:snapshot) > "$ROOT/logs/snapshot.log" 2>&1 \
    || die "snapshot failed, see $ROOT/logs/snapshot.log"
cat "$ROOT/logs/snapshot.log" >&2

python3 "$REPO/gate/p4/sandbox_checks.py" "$ROOT/store" "$ROOT/inputs" | tee "$ROOT/checks.txt" \
    || die "checks failed (above, and in $ROOT/checks.txt)"

echo
echo "store:   $ROOT/store"
echo "nextflow: $NXF_VER, nf-blocks $PLUGIN_VERSION, second run $SECOND_RUN"
printf 'run time: %s\n' "${TIMES[@]}"
echo "serve it (Ctrl-C stops it):"
echo "  cd $ROOT && NXF_VER=$NXF_VER NXF_HOME=$NXF_HOME NXF_PLUGINS_DIR=$NXF_PLUGINS_DIR XDG_CACHE_HOME=$XDG_CACHE_HOME \\"
echo "    NXF_CONFIG_FILE=$ROOT/cas.config NXF_PLUGINS_TEST_REPOSITORY=$PLUGINS_REPO $NEXTFLOW plugin nf-blocks:explore"
