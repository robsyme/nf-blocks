#!/usr/bin/env python3
"""Query parameters for the year snapshot, the prototype's formulas verbatim.

    python3 gate/year_params.py <snapshot.sqlite> <offset>

Offset 0 is the cold set, offset 3 the "other parameters" set, as in the
prototype's drive.mjs, so request counts compare with its RESULTS.md.
"""
import json
import sqlite3
import sys


def params(path, offset):
    con = sqlite3.connect('file:%s?mode=ro' % path, uri=True)
    try:
        def one(sql):
            return con.execute(sql).fetchone()
        content = one("SELECT content_cid FROM producer LIMIT 1 OFFSET (%d * 7919 %% "
                      "(SELECT count(*) FROM producer))" % offset)[0]
        pipeline = one("SELECT pipeline FROM run ORDER BY completion_cid LIMIT 1 OFFSET "
                       "(%d %% (SELECT count(*) FROM run))" % offset)[0]
        comp, out, sample = one(
            "SELECT c.completion_cid, c.output_name, a.value FROM collection c "
            "JOIN collection_item ci USING(collection_cid) "
            "JOIN item_attr a ON a.item_cid = ci.item_cid AND a.path = 'sample' "
            "WHERE c.rowid = (SELECT rowid FROM collection ORDER BY rowid LIMIT 1 OFFSET "
            "(%d * 37 %% (SELECT count(*) FROM collection))) LIMIT 1" % offset)
        return {'producersOf': [content], 'latestSuccessfulRun': [pipeline],
                'itemsWhere': [comp, out, 'sample', 'string', sample]}
    finally:
        con.close()


if __name__ == '__main__':
    print(json.dumps(params(sys.argv[1], int(sys.argv[2]))))
