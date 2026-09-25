"""gen_year builds a year-shaped Index Snapshot with the plugin's own DDL."""
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gen_year  # noqa: E402

# A schema-2 stand-in with the tables gen_year writes, shaped as Index.groovy's.
SCHEMA = [
    "CREATE TABLE schema_version(version INTEGER NOT NULL)",
    "CREATE TABLE run(completion_cid TEXT PRIMARY KEY, manifest_cid TEXT, pipeline TEXT, revision TEXT,"
    " commit_id TEXT, nf_run_hash TEXT, session_id TEXT, run_name TEXT, asserted_by TEXT,"
    " status TEXT, possibly_incomplete INTEGER, finished_at TEXT, member TEXT)",
    "CREATE INDEX run_pipeline_status_finished_at ON run(pipeline, status, finished_at DESC)",
    "CREATE TABLE collection(collection_cid TEXT PRIMARY KEY, completion_cid TEXT, output_name TEXT)",
    "CREATE TABLE item(item_cid TEXT PRIMARY KEY)",
    "CREATE TABLE collection_item(collection_cid TEXT, item_cid TEXT)",
    "CREATE INDEX collection_completion_output ON collection(completion_cid, output_name)",
    "CREATE INDEX collection_item_collection ON collection_item(collection_cid)",
    "CREATE TABLE producer(content_cid TEXT, item_cid TEXT, collection_cid TEXT, completion_cid TEXT, filename TEXT)",
    "CREATE INDEX producer_content_cid ON producer(content_cid)",
    "CREATE TABLE item_attr(item_cid TEXT, path TEXT, type TEXT, value TEXT, truncated INTEGER)",
    "CREATE INDEX item_attr_path_type_value ON item_attr(path, type, value)",
    "CREATE TABLE nf_record(key TEXT PRIMARY KEY, kind TEXT, workflow_run TEXT, task_run TEXT,"
    " labels_json TEXT, block_cid TEXT)",
    "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)",
]


def make_schema(path, version=2):
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
        self.assertEqual(rows["schema_version"], [(2,)])

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
        old = os.path.join(self.tmp, "old.sqlite")
        make_schema(old, version=1)
        with self.assertRaises(SystemExit):
            gen_year.build(old, os.path.join(self.tmp, "x.sqlite"), 1)


if __name__ == "__main__":
    unittest.main()
