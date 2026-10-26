"""Helpers for live.sh.

    runcid.py <v3.sqlite> <run name>      prints the run's completion CID
    runcid.py --check-json <stdout file>  checks the file is one JSON document,
                                          after Nextflow's test-repository banner
"""
import json
import sqlite3
import sys

if sys.argv[1] == '--check-json':
    lines = [l for l in open(sys.argv[2]).read().splitlines()
             if not (l.startswith('WARN: ====') or l.startswith('='))]
    json.loads('\n'.join(lines))
    print('== stdout is one JSON document (after the test-repository banner)')
else:
    row = sqlite3.connect(sys.argv[1]).execute(
        'SELECT completion_cid FROM run WHERE run_name = ?', (sys.argv[2],)).fetchone()
    print(row[0])
