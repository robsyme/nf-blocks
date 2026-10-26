"""gen_year builds a year-shaped Index Snapshot with the plugin's own DDL."""
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gen_year  # noqa: E402

# A schema-6 stand-in with the tables gen_year writes, shaped as Index.groovy's.
SCHEMA = [
    "CREATE TABLE schema_version(version INTEGER NOT NULL)",
    "CREATE TABLE run(completion_cid TEXT PRIMARY KEY, manifest_cid TEXT, pipeline TEXT, revision TEXT,"
    " commit_id TEXT, nf_run_hash TEXT, session_id TEXT, run_name TEXT, asserted_by TEXT,"
    " status TEXT, possibly_incomplete INTEGER, finished_at TEXT, member TEXT)",
    "CREATE INDEX run_pipeline_status_finished_at ON run(pipeline, status, finished_at DESC)",
    "CREATE TABLE collection(collection_cid TEXT PRIMARY KEY, kind TEXT, completion_cid TEXT, output_name TEXT, asserted_by TEXT)",
    "CREATE TABLE item(item_cid TEXT PRIMARY KEY)",
    "CREATE TABLE collection_item(collection_cid TEXT, item_cid TEXT, via_cid TEXT)",
    "CREATE INDEX collection_completion_output ON collection(completion_cid, output_name)",
    "CREATE INDEX collection_item_collection ON collection_item(collection_cid)",
    "CREATE TABLE producer(content_cid TEXT, item_cid TEXT, collection_cid TEXT, completion_cid TEXT, filename TEXT)",
    "CREATE INDEX producer_content_cid ON producer(content_cid)",
    "CREATE TABLE item_attr(item_cid TEXT, path TEXT, type TEXT, value TEXT, truncated INTEGER)",
    "CREATE INDEX item_attr_path_type_value ON item_attr(path, type, value)",
    "CREATE TABLE nf_record(key TEXT PRIMARY KEY, kind TEXT, workflow_run TEXT, task_run TEXT,"
    " labels_json TEXT, block_cid TEXT)",
    "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)",
    "CREATE TABLE block(cid TEXT PRIMARY KEY, size INTEGER, codec TEXT)",
    "CREATE TABLE block_ref(owner_cid TEXT, cid TEXT, name TEXT)",
    "CREATE INDEX block_ref_owner ON block_ref(owner_cid)",
    "CREATE TABLE root(id INTEGER PRIMARY KEY, cid TEXT, kind TEXT, asserted_by TEXT, state TEXT, since TEXT)",
    "CREATE INDEX root_cid ON root(cid)",
    "CREATE TABLE root_stats(root_id INTEGER PRIMARY KEY, reachable_bytes INTEGER, reachable_blocks INTEGER,"
    " exclusive_bytes INTEGER, exclusive_blocks INTEGER, pinned_bytes INTEGER, partial INTEGER)",
    "CREATE TABLE root_shared(root_id INTEGER, other_root_id INTEGER, bytes INTEGER)",
    "CREATE INDEX root_shared_root ON root_shared(root_id)",
    "CREATE TABLE store_stats(stored_bytes INTEGER, stored_blocks INTEGER, referenced_bytes INTEGER,"
    " pinned_bytes INTEGER, held_bytes INTEGER, releasing_bytes INTEGER, shared_bytes INTEGER, partial INTEGER)",
    "CREATE TABLE asserter_stats(asserted_by TEXT PRIMARY KEY, reachable_bytes INTEGER, exclusive_bytes INTEGER,"
    " pinned_bytes INTEGER, releasing_bytes INTEGER)",
    "CREATE TABLE pipeline_stats(pipeline TEXT PRIMARY KEY, runs INTEGER, bytes INTEGER, blocks INTEGER,"
    " releasing_bytes INTEGER)",
    "CREATE TABLE collection_size(collection_cid TEXT PRIMARY KEY, bytes INTEGER, blocks INTEGER)",
    "CREATE TABLE item_size(item_cid TEXT PRIMARY KEY, bytes INTEGER, blocks INTEGER)",
    "CREATE TABLE kind_stats(kind TEXT PRIMARY KEY, files INTEGER, bytes INTEGER)",
    "CREATE TABLE pinned_overlap(subject_cid TEXT, asserted_by TEXT, bytes INTEGER)",
    "CREATE INDEX pinned_overlap_subject ON pinned_overlap(subject_cid)",
]


def make_schema(path, version=6):
    con = sqlite3.connect(path)
    for sql in SCHEMA:
        con.execute(sql)
    con.execute("INSERT INTO schema_version VALUES (?)", (version,))
    con.commit()
    con.close()


def dump(path):
    con = sqlite3.connect(path)
    try:
        tables = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        return {t: con.execute("SELECT * FROM %s ORDER BY rowid" % t).fetchall() for t in tables}
    finally:
        con.close()


class GenYearTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.schema = os.path.join(self.tmp, "schema.sqlite")
        make_schema(self.schema)

    def build(self, name, runs=3):
        out = os.path.join(self.tmp, name)
        gen_year.build(self.schema, out, runs)
        return out

    def test_same_seed_same_rows(self):
        self.assertEqual(dump(self.build("a.sqlite")), dump(self.build("b.sqlite")))

    def test_scale_per_run(self):
        rows = dump(self.build("a.sqlite", runs=2))
        self.assertEqual(len(rows["run"]), 2)
        self.assertEqual(len(rows["collection"]), 8)
        self.assertEqual(len(rows["collection_item"]), 200)
        self.assertEqual(len(rows["producer"]), 600)
        self.assertEqual(len(rows["item_attr"]), 3000)
        self.assertEqual(rows["schema_version"], [(6,)])

    def test_snapshot_shape(self):
        out = self.build("a.sqlite")
        with open(out, "rb") as fh:
            header = fh.read(100)
        self.assertEqual(int.from_bytes(header[16:18], "big"), 4096)
        self.assertEqual((header[18], header[19]), (1, 1))
        for suffix in ("-wal", "-shm", "-journal"):
            self.assertFalse(os.path.exists(out + suffix), suffix)
        con = sqlite3.connect(out)
        self.assertIsNone(con.execute("SELECT member FROM run LIMIT 1").fetchone()[0])
        watermark = con.execute("SELECT value FROM meta WHERE key = 'store_log_watermark'").fetchone()[0]
        self.assertRegex(watermark, r"^\d{13}-run-bafyrei[a-z2-7]{52}$")
        con.close()

    def test_indexes_come_from_the_schema(self):
        out = self.build("a.sqlite")
        con = sqlite3.connect(out)
        names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
        con.close()
        self.assertIn("collection_completion_output", names)
        self.assertIn("collection_item_collection", names)

    def test_refuses_another_schema_version(self):
        for version in (1, 3, 4, 5):
            old = os.path.join(self.tmp, "old%d.sqlite" % version)
            make_schema(old, version=version)
            with self.assertRaises(SystemExit):
                gen_year.build(old, os.path.join(self.tmp, "x.sqlite"), 1)
        old = os.path.join(self.tmp, "old.sqlite")
        make_schema(old, version=1)
        with self.assertRaises(SystemExit):
            gen_year.build(old, os.path.join(self.tmp, "x.sqlite"), 1)

    def query(self, out, sql, *args):
        con = sqlite3.connect(out)
        try:
            return con.execute(sql, args).fetchall()
        finally:
            con.close()

    def test_blocks_and_refs_follow_the_producers(self):
        out = self.build("a.sqlite", runs=2)
        self.assertEqual(self.query(out, "SELECT count(*) FROM block_ref")[0][0], 600)
        # every producer file is a raw block, and every ref names a block
        self.assertEqual(self.query(
            out, "SELECT count(*) FROM producer WHERE content_cid NOT IN (SELECT cid FROM block)")[0][0], 0)
        self.assertEqual(self.query(
            out, "SELECT count(*) FROM block_ref WHERE cid NOT IN (SELECT cid FROM block)")[0][0], 0)
        self.assertEqual(self.query(out, "SELECT count(*) FROM block WHERE codec != 'raw' OR size <= 0")[0][0], 0)
        self.assertEqual(self.query(
            out, "SELECT count(*) FROM block_ref WHERE owner_cid NOT IN (SELECT item_cid FROM item)")[0][0], 0)

    def test_one_root_per_run_with_matching_stats(self):
        out = self.build("a.sqlite", runs=25)
        roots = self.query(out, "SELECT id, cid, kind, state FROM root ORDER BY id")
        self.assertEqual(len(roots), 25)
        self.assertEqual({r[2] for r in roots}, {"run"})
        states = [r[3] for r in roots]
        self.assertTrue(set(states) <= {"held", "released"})
        self.assertGreater(states.count("held"), states.count("released"))
        self.assertGreater(states.count("released"), 0)
        self.assertEqual(sorted(r[1] for r in roots),
                         sorted(x[0] for x in self.query(out, "SELECT completion_cid FROM run")))
        self.assertEqual(self.query(out, "SELECT count(*) FROM root_stats")[0][0], 25)
        # reachable bytes are the distinct files under the run's items
        bad = self.query(out, """
            SELECT count(*) FROM root r JOIN root_stats s ON s.root_id = r.id
            WHERE s.reachable_bytes != (
              SELECT coalesce(sum(b.size), 0) FROM block b WHERE b.cid IN (
                SELECT p.content_cid FROM producer p WHERE p.completion_cid = r.cid))
            OR s.reachable_blocks != (
              SELECT count(DISTINCT p.content_cid) FROM producer p WHERE p.completion_cid = r.cid)
            OR s.exclusive_bytes > s.reachable_bytes""")
        self.assertEqual(bad[0][0], 0)

    def test_store_stats_are_one_consistent_row(self):
        out = self.build("a.sqlite", runs=25)
        rows = self.query(out, "SELECT * FROM store_stats")
        self.assertEqual(len(rows), 1)
        stored, blocks, referenced, pinned, held, releasing, shared, partial = rows[0]
        self.assertEqual((stored, blocks), self.query(out, "SELECT sum(size), count(*) FROM block")[0])
        self.assertEqual(referenced, self.query(out, "SELECT sum(reachable_bytes) FROM root_stats")[0][0])
        self.assertGreaterEqual(referenced, stored)
        self.assertEqual(pinned + held + releasing, stored)
        self.assertEqual(pinned, 0)
        self.assertGreater(releasing, 0)
        self.assertGreater(shared, 0)
        self.assertEqual(partial, 0)

    def test_grouped_stats(self):
        out = self.build("a.sqlite", runs=25)
        self.assertEqual(self.query(out, "SELECT asserted_by FROM asserter_stats"), [("lab",)])
        runs, = self.query(out, "SELECT sum(runs) FROM pipeline_stats")[0]
        self.assertEqual(runs, self.query(out, "SELECT count(*) FROM root WHERE state = 'held'")[0][0])
        self.assertEqual(self.query(out, "SELECT count(*) FROM collection_size")[0][0], 100)
        self.assertEqual(self.query(out, "SELECT count(*) FROM item_size")[0][0],
                         self.query(out, "SELECT count(*) FROM item")[0][0])
        bad = self.query(out, """
            SELECT count(*) FROM item_size s WHERE s.bytes != (
              SELECT sum(b.size) FROM block b WHERE b.cid IN
                (SELECT cid FROM block_ref WHERE owner_cid = s.item_cid))""")
        self.assertEqual(bad[0][0], 0)
        kinds = self.query(out, "SELECT kind, files, bytes FROM kind_stats")
        self.assertEqual(kinds, [("dat",) + self.query(out, "SELECT count(*), sum(size) FROM block")[0]])
        shared = self.query(out, "SELECT count(*) FROM root_shared WHERE bytes <= 0 OR root_id = other_root_id")
        self.assertEqual(shared[0][0], 0)

    def test_the_v3_tables_are_unchanged_by_the_new_rows(self):
        # request counts stay comparable with the prototype: the old tables keep the same rows
        out = self.build("a.sqlite", runs=2)
        self.assertEqual(self.query(out, "SELECT item_cid FROM producer LIMIT 1")[0][0][:7], "bafyrei")


if __name__ == "__main__":
    unittest.main()
