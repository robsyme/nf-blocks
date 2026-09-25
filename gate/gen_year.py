#!/usr/bin/env python3
"""A year of synthetic runs as one Index Snapshot, for Gate browser assertion 2.

    python3 gate/gen_year.py <schema.sqlite> <out.sqlite>     # RUNS=1825 by default

<schema.sqlite> is any schema-2 index or snapshot. Only its DDL is read, so
the synthetic file has exactly the plugin's tables and indexes. Scale and seed
are the block explorer prototype's (spec section 1.3, assertion 2): 5 runs a
day for 365 days, 20 pipelines, 4 outputs, 100 items per run, 3 files per item,
15 Meta Map keys per item, random.seed(42). The random calls are made in the
prototype's order, so request counts stay comparable with its RESULTS.md
(branch prototype/index-snapshot-http).
"""
import json
import os
import random
import sqlite3
import sys

SCHEMA_VERSION = 2
ITEMS = 100
OUTPUTS = ['aligned', 'qc', 'variants', 'reports']
LEAVES = 3
PAGE_SIZE = 4096
HORIZON_MILLIS = 9999999999999
B32 = 'abcdefghijklmnopqrstuvwxyz234567'


def cid(prefix='bafyrei'):
    return prefix + ''.join(random.choice(B32) for _ in range(52))


def ddl_of(schema_path):
    """CREATE statements of a schema-2 database, in creation order."""
    con = sqlite3.connect('file:%s?mode=ro' % schema_path, uri=True)
    try:
        row = con.execute('SELECT version FROM schema_version').fetchone()
        if not row or row[0] != SCHEMA_VERSION:
            raise SystemExit('%s has schema version %r, not %d'
                             % (schema_path, row and row[0], SCHEMA_VERSION))
        return [r[0] for r in con.execute(
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid")]
    finally:
        con.close()


def build(schema_path, out_path, runs):
    ddl = ddl_of(schema_path)
    live = out_path + '.live'
    for path in (live, live + '-wal', live + '-shm', out_path):
        if os.path.exists(path):
            os.remove(path)
    random.seed(42)
    con = sqlite3.connect(live)
    con.execute('PRAGMA journal_mode=wal')
    for sql in ddl:
        con.execute(sql)
    con.execute('INSERT INTO schema_version VALUES (?)', (SCHEMA_VERSION,))
    pipelines = ['pipeline-%02d' % i for i in range(20)]
    samples = ['S%05d' % i for i in range(20000)]
    t0 = 1_758_000_000_000
    content_pool = []
    comp = fin = None
    for r in range(runs):
        comp, man = cid(), cid()
        status = 'succeeded' if random.random() > 0.1 else 'failed'
        fin = t0 + r * 17_280_000
        con.execute('INSERT INTO run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (comp, man, random.choice(pipelines), 'main', cid('')[:40], cid('')[:32],
                     cid('')[:36], 'run_%d' % r, 'lab', status, 0 if status == 'succeeded' else 1,
                     '2026-%02d-01T00:00:00.%03dZ' % (1 + r * 12 // runs, r % 1000), None))
        colls = {o: cid() for o in OUTPUTS}
        for o, c in colls.items():
            con.execute('INSERT INTO collection VALUES (?,?,?)', (c, comp, o))
        for i in range(ITEMS):
            item = cid()
            coll = colls[OUTPUTS[i % len(OUTPUTS)]]
            con.execute('INSERT OR IGNORE INTO item VALUES (?)', (item,))
            con.execute('INSERT INTO collection_item VALUES (?,?)', (coll, item))
            sample = random.choice(samples)
            attrs = [('id', 'string', sample), ('sample', 'string', sample),
                     ('lane', 'int', str(random.randint(1, 8))),
                     ('condition', 'string', random.choice(['tumour', 'normal', 'control'])),
                     ('library.strand', 'string', random.choice(['fwd', 'rev', 'none'])),
                     ('library.kit', 'string', random.choice(['kitA', 'kitB'])),
                     ('patient', 'string', 'P%04d' % random.randint(0, 5000)),
                     ('batch', 'int', str(r)), ('single_end', 'bool', 'false'),
                     ('read_group', 'string', '%s.L%d' % (sample, i % 8)),
                     ('platform', 'string', 'ILLUMINA'), ('genome', 'string', 'GRCh38'),
                     ('status', 'int', str(random.randint(0, 1))),
                     ('sex', 'string', random.choice(['XX', 'XY'])),
                     ('depth', 'float', '%.2f' % random.uniform(10, 60))]
            con.executemany('INSERT INTO item_attr VALUES (?,?,?,?,0)',
                            [(item, p, t, v) for p, t, v in attrs])
            for leaf in range(LEAVES):
                content = (random.choice(content_pool)
                           if content_pool and random.random() < 0.2 else cid('bafkrei'))
                content_pool.append(content)
                if len(content_pool) > 5000:
                    content_pool.pop(0)
                con.execute('INSERT INTO producer VALUES (?,?,?,?,?)',
                            (content, item, coll, comp, '%s.%d.dat' % (sample, leaf)))
        for k in range(30):
            con.execute('INSERT INTO nf_record VALUES (?,?,?,?,?,?)',
                        ('%s/%d' % (cid('')[:32], k), 'TaskRun', cid('')[:32], None,
                         json.dumps({'process': 'P%d' % k}), cid()))
        if r % 200 == 0:
            con.commit()
            print('runs', r, file=sys.stderr)
    if comp is not None:
        # The watermark a real snapshot carries: the newest Store Log entry name.
        con.execute("INSERT INTO meta VALUES ('store_log_watermark', ?)",
                    ('%013d-run-%s' % (HORIZON_MILLIS - fin, comp),))
    con.commit()
    con.execute('PRAGMA page_size=%d' % PAGE_SIZE)
    con.execute('VACUUM INTO ?', (out_path,))
    con.close()
    for path in (live, live + '-wal', live + '-shm'):
        if os.path.exists(path):
            os.remove(path)


def main(argv):
    if len(argv) != 3:
        sys.stderr.write(__doc__)
        return 2
    build(argv[1], argv[2], int(os.environ.get('RUNS', 5 * 365)))
    print(argv[2], os.path.getsize(argv[2]))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
