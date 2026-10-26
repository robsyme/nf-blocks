"""Acceptance for gate/p4/input-sandbox.sh: the InputSets and run_input rows of
the sandbox's four runs, read from the member's snapshot (read only) and the
store's own blocks (gate/cas.py, every block verified against its address).

    python3 gate/p4/sandbox_checks.py <STORE> <INPUTS>

<INPUTS> is the script's inputs directory: download/ (the real WT_REP1 reads),
wt-short/ (their first 40,000 lines), samplesheet_wt_short.csv and tiny/.
Prints what it found and exits 1 on the first failed check.
"""

import itertools
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import cas  # noqa: E402

RNASEQ = ('p4-rnaseq-1', 'p4-rnaseq-2', 'p4-rnaseq-wt-short')
TINY = 'p4-tiny-reads'
# Runs whose params.input is the test profile's https samplesheet: never read to be hashed.
REMOTE_SHEET = ('p4-rnaseq-1', 'p4-rnaseq-2')
TEST_SHEET_NAME = 'samplesheet_test.csv'
SAMPLES = ('WT_REP1', 'WT_REP2', 'RAP1_UNINDUCED_REP1', 'RAP1_UNINDUCED_REP2', 'RAP1_IAA_30M_REP1')
CHANGED = 'WT_REP1'


class Failed(Exception):
    pass


def check(ok, message):
    if not ok:
        raise Failed(message)


def nulls_first(row):
    return tuple((v is not None, '' if v is None else str(v)) for v in row)


def main(store_dir, inputs):
    snapshot = os.path.join(store_dir, 'index', 'v6.sqlite')
    # The sqlite3 module creates a missing file even read only; ask first.
    check(os.path.isfile(snapshot), f'no snapshot at {snapshot}')
    db = sqlite3.connect(f'file:{snapshot}?mode=ro', uri=True)
    store = cas.Store(store_dir)

    runs = {name: (cid, status) for cid, name, status in
            db.execute('SELECT completion_cid, run_name, status FROM run')}
    print(f'runs in the snapshot: {", ".join(sorted(runs))}')
    for name in RNASEQ + (TINY,):
        check(name in runs, f'no run named {name} in the snapshot')
        check(runs[name][1] == 'succeeded', f'{name} is {runs[name][1]}')
    completion = {name: runs[name][0] for name in RNASEQ + (TINY,)}

    # Every run: an InputSet, run_input rows matching it, and params.input redacted.
    rows = {}
    input_sets = {}
    for name, cid in completion.items():
        rc = store.read_block(cid)
        check(rc.get('input_set') is not None, f'{name}: RunCompletion {cid} has a null input_set')
        input_set = store.read_block(rc['input_set'].text)
        check(input_set.get('kind') == 'InputSet', f'{name}: input_set {rc["input_set"].text} is a {input_set.get("kind")}')
        input_sets[name] = input_set
        unfinished = [i['name'] for i in input_set['inputs'] if i.get('reason') == 'unfinished']
        check(not unfinished, f'{name}: inputs still being read when the run stopped waiting (reason unfinished): {unfinished}')
        rows[name] = [dict(zip(('role', 'name', 'address', 'size', 'sample'), r)) for r in db.execute(
            'SELECT role, name, address, size, sample FROM run_input WHERE completion_cid = ?', (cid,))]
        check(rows[name], f'{name}: no run_input rows')
        from_block = sorted(((i['role'], i['name'], i['address'].text if i['address'] else None, i['size'], i['sample'])
                             for i in input_set['inputs']), key=nulls_first)
        from_index = sorted(((r['role'], r['name'], r['address'], r['size'], r['sample']) for r in rows[name]), key=nulls_first)
        check(from_block == from_index,
              f'{name}: run_input differs from InputSet {rc["input_set"].text}:\n'
              f'  only in the block: {sorted(set(from_block) - set(from_index), key=nulls_first)}\n'
              f'  only in the index: {sorted(set(from_index) - set(from_block), key=nulls_first)}')
        manifest = store.read_block(rc['run'].text)
        param = (manifest.get('params') or {}).get('input')
        check(param == '[redacted-location]', f'{name}: RunManifest params.input is {param!r}')
        roles = {}
        for r in rows[name]:
            roles[r['role']] = roles.get(r['role'], 0) + 1
        print(f'{name}: RunCompletion {cid}, InputSet {rc["input_set"].text}, {len(rows[name])} run_input rows '
              f'({", ".join(f"{k} {v}" for k, v in sorted(roles.items()))}); params.input {param}')

    # The samplesheet: one row per run. Runs 1 and 2 name the test profile's https samplesheet.
    # The plugin never downloads it to hash it, but the pipeline stages it into work/stage-*
    # (Nextflow downloads it for that task), and that local copy is hashed and stands for it:
    # the same address in both runs, under the plain base name, with no query string. The
    # wt-short and tiny runs' --input is a local file, hashed: the address of that file's bytes.
    sheet = {}
    for name in RNASEQ + (TINY,):
        found = [r for r in rows[name] if r['role'] == 'samplesheet']
        check(len(found) == 1, f'{name}: samplesheet rows {found}')
        sheet[name] = found[0]
        reasons = [i.get('reason') for i in input_sets[name]['inputs'] if i['role'] == 'samplesheet']
        print(f'{name}: samplesheet {found[0]["name"]} {found[0]["address"]} ({found[0]["size"]} bytes, reason {reasons[0]})')
        check(found[0]['address'] and reasons == [None], f'{name}: the samplesheet is {found[0]}, reason {reasons}: no local copy was hashed')
        if name in REMOTE_SHEET:
            check(found[0]['name'] == TEST_SHEET_NAME, f'{name}: the https samplesheet is named {found[0]["name"]!r}, expected {TEST_SHEET_NAME!r}')
    check(len({sheet[name]['address'] for name in REMOTE_SHEET}) == 1,
          f'the first two rnaseq runs give the https samplesheet addresses {[sheet[n]["address"] for n in REMOTE_SHEET]}')
    short_sheet = cas.cid_of_file(os.path.join(inputs, 'samplesheet_wt_short.csv'))
    check(sheet['p4-rnaseq-wt-short']['address'] == short_sheet,
          f'p4-rnaseq-wt-short samplesheet is {sheet["p4-rnaseq-wt-short"]["address"]}, the --input file is {short_sheet}: --input did not take')
    tiny_sheet = cas.cid_of_file(os.path.join(inputs, 'tiny', 'samplesheet.csv'))
    check(sheet[TINY]['address'] == tiny_sheet,
          f'{TINY} samplesheet is {sheet[TINY]["address"]}, the --input file is {tiny_sheet}')

    # Reads: every fastq a task staged carries a sample, and each rnaseq run has all five.
    for name in RNASEQ + (TINY,):
        unsampled = [r for r in rows[name] if r['role'] == 'task' and r['name'].endswith(('.fastq.gz', '.fq.gz')) and not r['sample']]
        check(not unsampled, f'{name}: fastq task rows with no sample: {unsampled}')
    for name in RNASEQ:
        have = {r['sample'] for r in rows[name] if r['role'] == 'task' and r['sample']}
        check(set(SAMPLES) <= have, f'{name}: task rows name samples {sorted(have)}, expected all of {sorted(SAMPLES)}')
        if have - set(SAMPLES):
            print(f'note: {name}: task rows also name {sorted(have - set(SAMPLES))}, which the samplesheet does not')

    # Per run and sample, the task addresses the page's sample identity uses (DESIGN 1.6):
    # an address that carries more than one sample within a run is a shared reference.
    def by_sample(name):
        samples_of = {}
        for r in rows[name]:
            if r['role'] == 'task' and r['sample'] and r['address']:
                samples_of.setdefault(r['address'], set()).add(r['sample'])
        out = {}
        for address, samples in samples_of.items():
            if len(samples) == 1:
                out.setdefault(next(iter(samples)), set()).add(address)
        return out, {a: s for a, s in samples_of.items() if len(s) > 1}

    grouped = {}
    for name in RNASEQ + (TINY,):
        grouped[name], shared_refs = by_sample(name)
        for address, samples in sorted(shared_refs.items()):
            names = sorted({r['name'] for r in rows[name] if r['address'] == address})
            print(f'note: {name}: {names} {address} carries samples {sorted(samples)} (skipped as a shared reference)')
        for r in rows[name]:
            if r['role'] == 'task' and r['sample'] and not r['name'].endswith(('.fastq.gz', '.fq.gz')):
                print(f'note: {name}: {r["name"]} {r["address"]} is a task input with sample {r["sample"]} and is not a fastq')
            if r['role'] != 'task' and r['sample']:
                print(f'note: {name}: {r["role"]} {r["name"]} {r["address"]} carries sample {r["sample"]}')
            if r['address'] is None:
                print(f'note: {name}: {r["role"]} {r["name"]} has no address')

    # The real WT_REP1 reads are runs 1 and 2's WT_REP1 addresses; the short ones are run 3's.
    real = {cas.cid_of_file(os.path.join(inputs, 'download', f)) for f in os.listdir(os.path.join(inputs, 'download'))
            if f.endswith('.fastq.gz')}
    short = {cas.cid_of_file(os.path.join(inputs, 'wt-short', f)) for f in os.listdir(os.path.join(inputs, 'wt-short'))}
    check(len(real) == 4 and len(short) == 4, f'expected 4 real and 4 short WT_REP1 files, found {len(real)} and {len(short)}')
    for name in ('p4-rnaseq-1', 'p4-rnaseq-2'):
        check(real <= grouped[name].get(CHANGED, set()), f'{name}: WT_REP1 addresses lack the real reads: {sorted(real - grouped[name].get(CHANGED, set()))}')
    check(short <= grouped['p4-rnaseq-wt-short'].get(CHANGED, set()),
          f'p4-rnaseq-wt-short: WT_REP1 addresses lack the short reads: {sorted(short - grouped["p4-rnaseq-wt-short"].get(CHANGED, set()))}')

    # Shared task input addresses (sample not null) per pair of runs.
    def task_addresses(name):
        return {r['address'] for r in rows[name] if r['role'] == 'task' and r['sample'] and r['address']}

    def all_addresses(name):
        return {r['address'] for r in rows[name] if r['address']}

    print()
    shared = {}
    for a, b in itertools.combinations(RNASEQ + (TINY,), 2):
        shared[a, b] = len(task_addresses(a) & task_addresses(b))
        print(f'{a} and {b} share {shared[a, b]} task input files with a sample '
              f'({len(all_addresses(a) & all_addresses(b))} input addresses in any role)')
    check(shared['p4-rnaseq-1', 'p4-rnaseq-2'] > 0, 'the two same-input rnaseq runs share no task input')
    for name in RNASEQ:
        both = all_addresses(name) & all_addresses(TINY)
        check(not both, f'{TINY} shares inputs with {name}: {sorted(both)}')

    # Same name, different reads: WT_REP1 in run 3 shares nothing with WT_REP1 in runs 1 and 2;
    # the other four samples share with both.
    print()
    three = grouped['p4-rnaseq-wt-short']
    for sample in SAMPLES:
        for other in ('p4-rnaseq-1', 'p4-rnaseq-2'):
            common = three.get(sample, set()) & grouped[other].get(sample, set())
            print(f'{sample}: p4-rnaseq-wt-short shares {len(common)} task input addresses with {other}')
            if sample == CHANGED:
                check(not common, f'{sample} in p4-rnaseq-wt-short shares {sorted(common)} with {other}')
            else:
                check(common, f'{sample} in p4-rnaseq-wt-short shares nothing with {other}')
    print('\nPASS')


if __name__ == '__main__':
    try:
        main(sys.argv[1], sys.argv[2])
    except Failed as e:
        print(f'FAIL: {e}')
        sys.exit(1)
