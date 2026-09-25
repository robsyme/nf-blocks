#!/usr/bin/env bash
#
# The built plugin as a plugins.json for NXF_PLUGINS_TEST_REPOSITORY, shared by
# browser/tier.sh and cloud/cloud.sh: `nextflow plugin` starts the plugin
# unpinned, so a locally built, unpublished one is found through a plugins.json
# naming the built zip, with NXF_OFFLINE unset (DESIGN.md section 15, "Plugin
# verbs"). The zip is already unpacked in NXF_PLUGINS_DIR by gate.sh, so
# nothing is downloaded; the registry is asked once for dependencies. Under
# GATE_SKIP_BUILD the zip is the one matching the installed version.
#
#   gate/browser/plugin-repo.sh <REPO> <OUT_DIR>   # writes <OUT_DIR>/plugins.json, prints its path

set -euo pipefail

REPO="$1"
OUT_DIR="$2"

if [[ -n "${GATE_SKIP_BUILD:-}" ]]; then
    # No build this run: the zip must be the version gate.sh installed earlier,
    # not whichever zip is newest in build/distributions.
    shopt -s nullglob
    installed=("${NXF_PLUGINS_DIR:?plugin-repo: GATE_SKIP_BUILD needs NXF_PLUGINS_DIR}"/nf-blocks-*/)
    shopt -u nullglob
    if [[ ${#installed[@]} -ne 1 ]]; then
        echo "plugin-repo: GATE_SKIP_BUILD wants exactly one nf-blocks installed in $NXF_PLUGINS_DIR, found ${#installed[@]}" >&2
        exit 2
    fi
    name="$(basename "${installed[0]}")"
    zip="$REPO/build/distributions/$name.zip"
    [[ -f "$zip" ]] || { echo "plugin-repo: $name is installed in $NXF_PLUGINS_DIR but $zip does not exist; rebuild (unset GATE_SKIP_BUILD)" >&2; exit 2; }
else
    zip="$(ls -t "$REPO"/build/distributions/nf-blocks-*.zip 2> /dev/null | head -1 || true)"
    [[ -n "$zip" ]] || { echo "plugin-repo: no plugin zip under $REPO/build/distributions" >&2; exit 2; }
fi
version="$(basename "$zip" .zip)"; version="${version#nf-blocks-}"
out="$OUT_DIR/plugins.json"
python3 - "$zip" "$version" "$out" << 'PY'
import datetime, hashlib, json, sys
zip_path, version, out = sys.argv[1:]
with open(zip_path, "rb") as fh:
    digest = hashlib.sha512(fh.read()).hexdigest()
date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
with open(out, "w") as fh:
    json.dump([{"id": "nf-blocks", "releases": [{"version": version, "date": date, "url": "file://" + zip_path,
                                                  "requires": ">=26.04.6", "sha512sum": digest}]}], fh)
PY
echo "$out"
