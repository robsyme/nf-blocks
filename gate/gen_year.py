#!/usr/bin/env python3
"""A year of synthetic runs as one Index Snapshot, for Gate browser assertion 2.

    python3 gate/gen_year.py <schema.sqlite> <out.sqlite>     # RUNS=1825 by default

<schema.sqlite> is any schema-6 index or snapshot. Only its DDL is read, so
the synthetic file has exactly the plugin's tables and indexes. Scale and seed
are the block explorer prototype's (spec section 1.3, assertion 2): 5 runs a
day for 365 days, 20 pipelines, 4 outputs, 100 items per run, 3 files per item,
15 Meta Map keys per item, random.seed(42). The random calls are made in the
prototype's order, so request counts stay comparable with its RESULTS.md
(branch prototype/index-snapshot-http).

Schema 4 adds the storage tables. They are filled from the same leaves, with
plausible rather than computed reachability: every run is one root (one in
twenty released, the rest held), a file's size is a function of its cid, and
the totals are sums over the rows written. No real sweep logic is involved.
"""
import hashlib
import json
import os
import random
import sqlite3
import sys

SCHEMA_VERSION = 6
ITEMS = 100
OUTPUTS = ['aligned', 'qc', 'variants', 'reports']
LEAVES = 3
PAGE_SIZE = 4096
HORIZON_MILLIS = 9999999999999
RELEASED_EVERY = 20
SHARED_TOP = 5
B32 = 'abcdefghijklmnopqrstuvwxyz234567'


def cid(prefix='bafyrei'):
    return prefix + ''.join(random.choice(B32) for _ in range(52))


def size_of(content):
    """A file's size in bytes: 1 KB to about 9 MB, skewed small, the same for a cid every time."""
    n = int(hashlib.sha256(content.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
    return 1000 + int(n ** 4 * 9_000_000)


def ddl_of(schema_path):
    """CREATE statements of a schema-6 database, in creation order."""
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
    sizes = {}          # content cid -> bytes
    run_files = []      # per run: (completion cid, pipeline, set of content cids)
    coll_files = {}     # collection cid -> set of content cids
    item_files = {}     # item cid -> set of content cids
    for r in range(runs):
        comp, man = cid(), cid()
        files = set()
        status = 'succeeded' if random.random() > 0.1 else 'failed'
        fin = t0 + r * 17_280_000
        pipeline = random.choice(pipelines)
        con.execute('INSERT INTO run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (comp, man, pipeline, 'main', cid('')[:40], cid('')[:32],
                     cid('')[:36], 'run_%d' % r, 'lab', status, 0 if status == 'succeeded' else 1,
                     '2026-%02d-01T00:00:00.%03dZ' % (1 + r * 12 // runs, r % 1000), None))
        colls = {o: cid() for o in OUTPUTS}
        for o, c in colls.items():
            con.execute('INSERT INTO collection VALUES (?,?,?,?,?)', (c, 'output', comp, o, 'lab'))
        for i in range(ITEMS):
            item = cid()
            coll = colls[OUTPUTS[i % len(OUTPUTS)]]
            con.execute('INSERT OR IGNORE INTO item VALUES (?)', (item,))
            con.execute('INSERT INTO collection_item VALUES (?,?,?)', (coll, item, None))
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
                if content not in sizes:
                    sizes[content] = size_of(content)
                    con.execute('INSERT INTO block VALUES (?,?,?)', (content, sizes[content], 'raw'))
                con.execute('INSERT INTO block_ref VALUES (?,?,?)',
                            (item, content, '%s.%d.dat' % (sample, leaf)))
                files.add(content)
                coll_files.setdefault(coll, set()).add(content)
                item_files.setdefault(item, set()).add(content)
        run_files.append((comp, pipeline, files))
        for k in range(30):
            con.execute('INSERT INTO nf_record VALUES (?,?,?,?,?,?)',
                        ('%s/%d' % (cid('')[:32], k), 'TaskRun', cid('')[:32], None,
                         json.dumps({'process': 'P%d' % k}), cid()))
        if r % 200 == 0:
            con.commit()
            print('runs', r, file=sys.stderr)
    storage_rows(con, sizes, run_files, coll_files, item_files)
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


def storage_rows(con, sizes, run_files, coll_files, item_files):
    """root, root_stats, root_shared and the store, asserter, pipeline, collection, item and kind totals."""
    owners = {}  # content cid -> ids of the runs that reach it
    for rid, (_, _, files) in enumerate(run_files, 1):
        for f in files:
            owners.setdefault(f, []).append(rid)
    state = {rid: 'released' if rid % RELEASED_EVERY == 0 else 'held'
             for rid in range(1, len(run_files) + 1)}
    reach, excl, shared = {}, {}, {}
    pair = {}
    for f, ids in owners.items():
        for rid in ids:
            reach[rid] = reach.get(rid, 0) + sizes[f]
            if len(ids) == 1:
                excl[rid] = excl.get(rid, 0) + sizes[f]
        for a in ids:
            for b in ids:
                if a != b:
                    pair[(a, b)] = pair.get((a, b), 0) + sizes[f]
    top = {}
    for (a, b), n in pair.items():
        top.setdefault(a, []).append((n, b))
    pipelines = {}
    for rid, (comp, pipeline, files) in enumerate(run_files, 1):
        since = '2026-%02d-01T00:00:00.000Z' % (1 + (rid - 1) * 12 // len(run_files))
        con.execute('INSERT INTO root VALUES (?,?,?,?,?,?)', (rid, comp, 'run', 'lab', state[rid], since))
        exclusive = [f for f in files if len(owners[f]) == 1]
        con.execute('INSERT INTO root_stats VALUES (?,?,?,?,?,?,?)',
                    (rid, sum(sizes[f] for f in files), len(files),
                     sum(sizes[f] for f in exclusive), len(exclusive), 0, 0))
        for n, other in sorted(top.get(rid, []), reverse=True)[:SHARED_TOP]:
            con.execute('INSERT INTO root_shared VALUES (?,?,?)', (rid, other, n))
        entry = pipelines.setdefault(pipeline, [0, set(), 0])
        if state[rid] == 'held':
            entry[0] += 1
            entry[1].update(files)
        else:
            entry[2] += sum(sizes[f] for f in exclusive)
    held = {f for f, ids in owners.items() if any(state[i] == 'held' for i in ids)}
    stored = sum(sizes.values())
    held_bytes = sum(sizes[f] for f in held)
    con.execute('INSERT INTO store_stats VALUES (?,?,?,?,?,?,?,?)',
                (stored, len(sizes), sum(reach.values()), 0, held_bytes, stored - held_bytes,
                 sum(sizes[f] for f, ids in owners.items() if len(ids) > 1), 0))
    con.execute('INSERT INTO asserter_stats VALUES (?,?,?,?,?)',
                ('lab', sum(reach.values()), sum(excl.values()), 0, stored - held_bytes))
    con.executemany('INSERT INTO pipeline_stats VALUES (?,?,?,?,?)',
                    [(p, e[0], sum(sizes[f] for f in e[1]), len(e[1]), e[2])
                     for p, e in sorted(pipelines.items())])
    con.executemany('INSERT INTO collection_size VALUES (?,?,?)',
                    [(c, sum(sizes[f] for f in fs), len(fs)) for c, fs in coll_files.items()])
    con.executemany('INSERT INTO item_size VALUES (?,?,?)',
                    [(i, sum(sizes[f] for f in fs), len(fs)) for i, fs in item_files.items()])
    con.execute('INSERT INTO kind_stats VALUES (?,?,?)', ('dat', len(sizes), stored))


def main(argv):
    if len(argv) != 3:
        sys.stderr.write(__doc__)
        return 2
    build(argv[1], argv[2], int(os.environ.get('RUNS', 5 * 365)))
    print(argv[2], os.path.getsize(argv[2]))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
