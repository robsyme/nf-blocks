"""Reads a member's nf/ for gate/nf-scrub/check.sh (DESIGN.md section 10).

    records.py keys <nf dir>
        Prints shell assignments: RUN1, RUN2 (WorkflowRun keys in .history
        order), WORK_FO (a FileOutput whose path is a work-dir file), CAS_FO (a
        FileOutput at a cas:// Store URI), TASK1, TASK2 (two STAGE TaskRuns).

    records.py leftovers <nf dir> <host string> [<host string> ...]
        Prints, per record kind and JSON field, how many string values still
        contain any of the host strings. Exits 1 when any field other than
        FileOutput.path holds one: that path is what a lid:// read opens, so it
        is the one place a host path may stay.

Python 3 standard library only, like the rest of the Gate.
"""
import collections
import json
import os
import sys


def records(nf):
    out = {}
    for dirpath, _dirs, files in os.walk(nf):
        if '.data.json' in files:
            with open(os.path.join(dirpath, '.data.json')) as fh:
                out[os.path.relpath(dirpath, nf)] = json.load(fh)
    return out


def history(nf):
    runs = []
    hist = os.path.join(nf, '.history')
    for name in sorted(os.listdir(hist)):
        with open(os.path.join(hist, name)) as fh:
            for line in fh:
                cols = line.rstrip('\n').split('\t')
                if len(cols) == 4 and cols[3].startswith('lid://'):
                    runs.append((cols[0], cols[3][len('lid://'):]))
    return [key for _ts, key in sorted(runs)]


def keys(nf):
    recs = records(nf)
    runs = history(nf)
    work = sorted(k for k, r in recs.items() if r['kind'] == 'FileOutput'
                  and not r['spec']['path'].startswith('cas://') and r['spec']['path'].endswith('/out.txt'))
    cas = sorted(k for k, r in recs.items() if r['kind'] == 'FileOutput' and r['spec']['path'].startswith('cas://'))
    tasks = sorted(k for k, r in recs.items() if r['kind'] == 'TaskRun' and r['spec']['name'].startswith('STAGE'))
    print('RUN1=%s' % runs[0])
    print('RUN2=%s' % runs[1])
    print('WORK_FO=%s' % work[0])
    print('CAS_FO=%s' % cas[0])
    print('TASK1=%s' % tasks[0])
    print('TASK2=%s' % tasks[1])


def leftovers(nf, hosts):
    hits = collections.Counter()

    def walk(kind, path, value):
        if isinstance(value, dict):
            for k, v in value.items():
                walk(kind, path + '.' + k, v)
        elif isinstance(value, list):
            for v in value:
                walk(kind, path + '[]', v)
        elif isinstance(value, str) and any(h in value for h in hosts):
            hits[(kind, path)] += 1

    kinds = collections.Counter()
    for rec in records(nf).values():
        kinds[rec['kind']] += 1
        walk(rec['kind'], 'spec', rec['spec'])
    print('records: %s' % ', '.join('%s %d' % kv for kv in sorted(kinds.items())))
    for (kind, path), n in sorted(hits.items()):
        print('host string in %s %s (%d)' % (kind, path, n))
    return 1 if any(key != ('FileOutput', 'spec.path') for key in hits) else 0


if __name__ == '__main__':
    if sys.argv[1] == 'keys':
        keys(sys.argv[2])
    elif sys.argv[1] == 'leftovers':
        sys.exit(leftovers(sys.argv[2], [h for h in sys.argv[3:] if h]))
    else:
        sys.exit('usage: records.py keys|leftovers <nf dir> ...')
