"""Unit tests for the machinery gate/assert.py builds its assertions on.

Run with:  python3 -m unittest discover -s gate

The assertions themselves are exercised end to end against gate/fixtures/root.
What is tested here is the part that decides whether an assertion passes: the
independent directory walk, the manifest diff, the leaf and string searches,
run resolution, and the failure paths that must not be swallowed.
"""

import importlib.util
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import cas  # noqa: E402

# "assert" is a keyword, so gate/assert.py cannot be imported by name.
_spec = importlib.util.spec_from_file_location(
    "gate_assert", os.path.join(HERE, "assert.py"))
gate_assert = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate_assert)


class StoreBuilder(object):
    """A minimal writable store, so tests can state exactly what is on disk."""

    def __init__(self, root):
        self.root = root
        self.store = cas.Store(root)

    def raw(self, data):
        return self._write(cas.cid_raw(data), data)

    def block(self, value):
        data = cas.encode(value)
        return self._write(cas.cid_dagcbor(data), data)

    def corrupt_block(self, cid, data):
        """File `data` under `cid` even though it does not hash to it."""
        return self._write(cid, data, check=False)

    def _write(self, cid, data, check=True):
        d = os.path.join(self.root, "blocks", cid[-2:])
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, cid), "wb") as fh:
            fh.write(data)
        return cid

    def store_log(self, written_millis, kind, cid):
        d = os.path.join(self.root, "log")
        os.makedirs(d, exist_ok=True)
        open(os.path.join(d, "%013d-%s-%s" % (9999999999999 - written_millis, kind, cid)),
             "w").close()

    def manifest(self, entries):
        return self.block({"kind": "DirectoryManifest", "schema": 1,
                           "entries": entries})


def entry(name, mode, size, address=None, target=None):
    return {"name": name, "mode": mode, "size": size,
            "address": cas.Cid(address) if address else None, "target": target}


class TempTree(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="gate-assert-")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def path(self, *parts):
        return os.path.join(self.tmp, *parts)

    def write(self, rel, data, mode=None):
        full = self.path(*rel.split("/"))
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as fh:
            fh.write(data)
        if mode is not None:
            os.chmod(full, mode)
        return full

    def link(self, rel, target):
        full = self.path(*rel.split("/"))
        os.makedirs(os.path.dirname(full), exist_ok=True)
        os.symlink(target, full)
        return full


class TestWalk(TempTree):
    """_walk(dirpath, root) applies DESIGN section 6's rules to one directory."""

    def test_regular_file_gets_its_raw_cid_and_size(self):
        self.write("tree/summary.txt", b"summary for A\n", 0o644)
        walked = gate_assert._walk(self.path("tree"), self.path("tree"))
        self.assertEqual(walked["summary.txt"]["mode"], "regular")
        self.assertEqual(walked["summary.txt"]["size"], 14)
        self.assertEqual(walked["summary.txt"]["address"],
                         cas.cid_raw(b"summary for A\n"))
        self.assertIsNone(walked["summary.txt"]["target"])

    def test_executable_bit_changes_the_mode(self):
        self.write("tree/run.sh", b"#!/bin/sh\n", 0o755)
        walked = gate_assert._walk(self.path("tree"), self.path("tree"))
        self.assertEqual(walked["run.sh"]["mode"], "executable")

    def test_relative_symlink_inside_the_tree_is_a_link(self):
        self.write("tree/summary.txt", b"x\n")
        self.link("tree/alias.txt", "summary.txt")
        walked = gate_assert._walk(self.path("tree"), self.path("tree"))
        self.assertEqual(walked["alias.txt"]["mode"], "symlink")
        self.assertEqual(walked["alias.txt"]["target"], "summary.txt")
        self.assertEqual(walked["alias.txt"]["size"], len("summary.txt"))
        self.assertIsNone(walked["alias.txt"]["address"])

    def test_symlink_up_into_the_tree_root_is_still_a_link(self):
        """Containment is judged against the published root, not the parent.

        nested/up.txt -> ../summary.txt leaves its own directory but stays
        inside the tree being published, so DESIGN section 6 records a link.
        """
        self.write("tree/summary.txt", b"x\n")
        self.write("tree/nested/detail.txt", b"y\n")
        self.link("tree/nested/up.txt", "../summary.txt")
        walked = gate_assert._walk(self.path("tree", "nested"), self.path("tree"))
        self.assertEqual(walked["up.txt"]["mode"], "symlink")
        self.assertEqual(walked["up.txt"]["target"], "../summary.txt")

    def test_relative_symlink_escaping_the_tree_is_followed(self):
        self.write("outside.txt", b"outside\n")
        self.write("tree/keep.txt", b"k\n")
        self.link("tree/escape.txt", "../outside.txt")
        walked = gate_assert._walk(self.path("tree"), self.path("tree"))
        self.assertEqual(walked["escape.txt"]["mode"], "regular")
        self.assertEqual(walked["escape.txt"]["address"], cas.cid_raw(b"outside\n"))

    def test_dangling_symlink_is_unresolvable(self):
        self.write("tree/keep.txt", b"k\n")
        self.link("tree/broken.txt", "../nowhere.txt")
        walked = gate_assert._walk(self.path("tree"), self.path("tree"))
        self.assertEqual(walked["broken.txt"]["mode"], "unresolvable")
        self.assertEqual(walked["broken.txt"]["target"], "../nowhere.txt")
        self.assertIsNone(walked["broken.txt"]["address"])

    def test_directory_entry_has_size_zero_and_no_address(self):
        self.write("tree/nested/detail.txt", b"y\n")
        walked = gate_assert._walk(self.path("tree"), self.path("tree"))
        self.assertEqual(walked["nested"]["mode"], "directory")
        self.assertEqual(walked["nested"]["size"], 0)
        self.assertIsNone(walked["nested"]["address"])


class TestCompareManifest(TempTree):
    def setUp(self):
        super(TestCompareManifest, self).setUp()
        self.builder = StoreBuilder(self.path("store"))
        self.gate = gate_assert.Gate(self.tmp)

    def _tree(self):
        self.write("tree/summary.txt", b"summary\n")
        self.write("tree/nested/detail.txt", b"detail\n")
        self.link("tree/alias.txt", "summary.txt")

    def _good_manifest(self):
        detail = self.builder.raw(b"detail\n")
        summary = self.builder.raw(b"summary\n")
        nested = self.builder.manifest([entry("detail.txt", "regular", 7, detail)])
        return self.builder.manifest([
            entry("alias.txt", "symlink", len("summary.txt"), None, "summary.txt"),
            entry("nested", "directory", 0, nested),
            entry("summary.txt", "regular", 8, summary),
        ])

    def test_a_matching_manifest_reports_no_problems(self):
        self._tree()
        cid = self._good_manifest()
        self.assertEqual(
            gate_assert._compare_manifest(self.gate, cid, self.path("tree"),
                                          "tree", self.path("tree")), [])

    def test_a_wrong_size_is_reported_with_both_values(self):
        self._tree()
        summary = self.builder.raw(b"summary\n")
        detail = self.builder.raw(b"detail\n")
        nested = self.builder.manifest([entry("detail.txt", "regular", 7, detail)])
        cid = self.builder.manifest([
            entry("alias.txt", "symlink", len("summary.txt"), None, "summary.txt"),
            entry("nested", "directory", 0, nested),
            entry("summary.txt", "regular", 999, summary),
        ])
        problems = gate_assert._compare_manifest(
            self.gate, cid, self.path("tree"), "tree", self.path("tree"))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("999", problems[0])
        self.assertIn("size", problems[0])

    def test_an_entry_missing_from_disk_is_reported(self):
        self._tree()
        cid = self.builder.manifest([
            entry("alias.txt", "symlink", len("summary.txt"), None, "summary.txt"),
            entry("ghost.txt", "regular", 1, self.builder.raw(b"g")),
            entry("nested", "directory", 0,
                  self.builder.manifest([entry("detail.txt", "regular", 7,
                                               self.builder.raw(b"detail\n"))])),
            entry("summary.txt", "regular", 8, self.builder.raw(b"summary\n")),
        ])
        problems = gate_assert._compare_manifest(
            self.gate, cid, self.path("tree"), "tree", self.path("tree"))
        self.assertTrue(any("ghost.txt" in p and "not on disk" in p
                            for p in problems), problems)

    def test_a_file_missing_from_the_manifest_is_reported(self):
        self._tree()
        cid = self.builder.manifest([
            entry("alias.txt", "symlink", len("summary.txt"), None, "summary.txt"),
            entry("nested", "directory", 0,
                  self.builder.manifest([entry("detail.txt", "regular", 7,
                                               self.builder.raw(b"detail\n"))])),
        ])
        problems = gate_assert._compare_manifest(
            self.gate, cid, self.path("tree"), "tree", self.path("tree"))
        self.assertTrue(any("summary.txt" in p and "not in the manifest" in p
                            for p in problems), problems)

    def test_entries_out_of_byte_order_are_reported(self):
        self._tree()
        detail = self.builder.raw(b"detail\n")
        cid = self.builder.manifest([
            entry("summary.txt", "regular", 8, self.builder.raw(b"summary\n")),
            entry("alias.txt", "symlink", len("summary.txt"), None, "summary.txt"),
            entry("nested", "directory", 0,
                  self.builder.manifest([entry("detail.txt", "regular", 7, detail)])),
        ])
        problems = gate_assert._compare_manifest(
            self.gate, cid, self.path("tree"), "tree", self.path("tree"))
        self.assertTrue(any("sorted" in p for p in problems), problems)


class TestLeaves(unittest.TestCase):
    def test_finds_a_leaf_inside_a_list(self):
        leaf = {"kind": "Leaf", "name": "A.bam", "address": None}
        self.assertEqual(list(gate_assert._leaves([{"sample": "A"}, leaf])), [leaf])

    def test_finds_leaves_nested_in_maps_and_lists(self):
        a = {"kind": "Leaf", "name": "a"}
        b = {"kind": "Leaf", "name": "b"}
        value = {"meta": {"sample": "A"}, "files": [a, [b]]}
        self.assertEqual(list(gate_assert._leaves(value)), [a, b])

    def test_does_not_descend_into_a_leaf(self):
        leaf = {"kind": "Leaf", "name": "a", "extra": {"kind": "Leaf", "name": "z"}}
        self.assertEqual(list(gate_assert._leaves(leaf)), [leaf])

    def test_no_leaves_in_a_plain_structure(self):
        self.assertEqual(list(gate_assert._leaves({"a": [1, "x", None]})), [])


class TestFindString(unittest.TestCase):
    def test_reports_the_dotted_path_of_a_hit(self):
        value = {"config": {"env": {"HOME": "/Users/nobody"}}}
        self.assertEqual(gate_assert._find_string(value, "/Users/nobody"),
                         "$.config.env.HOME")

    def test_reports_a_list_index(self):
        value = {"files": ["ok", "/tmp/gate/x"]}
        self.assertEqual(gate_assert._find_string(value, "/tmp/gate"),
                         "$.files[1]")

    def test_reports_a_hit_in_a_key(self):
        self.assertEqual(gate_assert._find_string({"/tmp/gate": 1}, "/tmp/gate"),
                         "$./tmp/gate (key)")

    def test_returns_none_when_absent(self):
        self.assertIsNone(gate_assert._find_string({"a": ["b", 1, None]}, "zzz"))


class TestRunResolution(TempTree):
    def setUp(self):
        super(TestRunResolution, self).setUp()
        self.builder = StoreBuilder(self.path("store"))

    def _manifest(self, name, nf_hash):
        return self.builder.block({
            "kind": "RunManifest", "schema": 1, "asserted_by": "gate",
            "pipeline": "p", "run_name": name, "nf_run_hash": nf_hash,
            "session_id": "s", "resumed": False, "nextflow_version": "26.04.6",
            "repository": None, "revision": None, "commit_id": None,
            "params": {}, "config": {}, "script": None,
            "started_at": "2026-01-01T00:00:00.000Z"})

    def test_two_run_manifests_with_one_name_is_an_error(self):
        self._manifest("cold", "aa" * 16)
        self._manifest("cold", "bb" * 16)
        gate = gate_assert.Gate(self.tmp)
        with self.assertRaises(cas.GateError) as ctx:
            gate.runs
        self.assertIn("cold", str(ctx.exception))
        self.assertIn("2", str(ctx.exception))

    def test_distinct_names_resolve(self):
        self._manifest("cold", "aa" * 16)
        self._manifest("again", "bb" * 16)
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(sorted(gate.runs), ["again", "cold"])


class TestLatestSuccessful(TempTree):
    def setUp(self):
        super(TestLatestSuccessful, self).setUp()
        self.builder = StoreBuilder(self.path("store"))

    def _run(self, name, status, finished, incomplete=False,
             finished_at="2026-01-01T00:01:00.000Z"):
        manifest = self.builder.block({
            "kind": "RunManifest", "schema": 1, "asserted_by": "gate",
            "pipeline": "p", "run_name": name, "nf_run_hash": "aa" * 16,
            "session_id": "s", "resumed": False, "nextflow_version": "26.04.6",
            "repository": None, "revision": None, "commit_id": None,
            "params": {}, "config": {}, "script": None,
            "started_at": "2026-01-01T00:00:00.000Z"})
        completion = self.builder.block({
            "kind": "RunCompletion", "schema": 1, "asserted_by": "gate",
            "run": cas.Cid(manifest), "collections": [], "input_set": None,
            "status": status, "exit_status": 0,
            "possibly_incomplete": incomplete,
            "started_at": "2026-01-01T00:00:00.000Z",
            "finished_at": finished_at,
            "anomalies": {"unresolvable": 0, "unaddressed": 0, "declined": 0,
                          "never_published": 0},
            "error": None})
        self.builder.store_log(finished, "run", completion)
        return completion

    def test_store_log_returns_the_newest_successful_run(self):
        older = self._run("cold", "succeeded", 1000, finished_at="2026-01-01T00:01:00.000Z")
        newer = self._run("again", "succeeded", 2000, finished_at="2026-01-01T00:02:00.000Z")
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._latest_successful_from_store_log(gate, "p"), newer)
        self.assertNotEqual(newer, older)

    def test_latest_is_by_finished_at_not_by_log_order(self):
        # The run that finished later was logged first (a merged Bundle, or a
        # skewed clock): latest must still be the later finish.
        later_finish = self._run("cold", "succeeded", 1000, finished_at="2026-01-01T02:00:00.000Z")
        earlier_finish = self._run("again", "succeeded", 2000, finished_at="2026-01-01T01:00:00.000Z")
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._latest_successful_from_store_log(gate, "p"), later_finish)

    def test_store_log_skips_a_failed_run_even_when_newest(self):
        good = self._run("cold", "succeeded", 1000)
        self._run("fail", "failed", 3000, incomplete=True)
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._latest_successful_from_store_log(gate, "p"),
                         good)

    def test_store_log_skips_a_possibly_incomplete_run(self):
        good = self._run("cold", "succeeded", 1000)
        self._run("partial", "succeeded", 3000, incomplete=True)
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._latest_successful_from_store_log(gate, "p"),
                         good)

    def test_store_log_breaks_a_finished_at_tie_by_the_smaller_cid(self):
        # One rule everywhere (the plugin's index: finished_at DESC, completion_cid ASC).
        a = self._run("cold", "succeeded", 1000, finished_at="2026-01-01T01:00:00.000Z")
        b = self._run("again", "succeeded", 2000, finished_at="2026-01-01T01:00:00.000Z")
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._latest_successful_from_store_log(gate, "p"), min(a, b))

    def test_index_breaks_a_finished_at_tie_by_the_smaller_cid(self):
        d = self.path("cache", "nf-blocks")
        os.makedirs(d)
        con = sqlite3.connect(os.path.join(d, "x.sqlite"))
        con.execute("CREATE TABLE run(completion_cid TEXT, pipeline TEXT, status TEXT, "
                    "possibly_incomplete INTEGER, finished_at TEXT)")
        # Inserted larger cid first, so insertion order cannot pass the test.
        for cid in ("bafyreizzzz", "bafyreiaaaa"):
            con.execute("INSERT INTO run VALUES (?, ?, 'succeeded', 0, '2026-01-01T01:00:00.000Z')",
                        (cid, gate_assert.PIPELINE_IDENTITY))
        con.commit(); con.close()
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._latest_successful_from_index(gate, gate_assert.PIPELINE_IDENTITY),
                         "bafyreiaaaa")

    def test_a_missing_index_is_raised_not_swallowed(self):
        self._run("cold", "succeeded", 1000)
        gate = gate_assert.Gate(self.tmp)
        with self.assertRaises(cas.GateError):
            gate_assert._latest_successful_from_index(gate, "p")

    def test_a_broken_index_query_is_raised_not_swallowed(self):
        self._run("cold", "succeeded", 1000)
        d = self.path("cache", "nf-blocks")
        os.makedirs(d)
        sqlite3.connect(os.path.join(d, "x.sqlite")).close()   # no run table
        gate = gate_assert.Gate(self.tmp)
        with self.assertRaises(Exception) as ctx:
            gate_assert._latest_successful_from_index(gate, "p")
        self.assertIn("run", str(ctx.exception))


class TestBlockIntegrityAssertion(TempTree):
    """Assertion 0: an unreadable or non-canonical block is a FAIL, never a note."""

    def setUp(self):
        super(TestBlockIntegrityAssertion, self).setUp()
        self.builder = StoreBuilder(self.path("store"))

    def test_a_sound_store_passes(self):
        self.builder.raw(b"hello\n")
        self.builder.block({"kind": "OutputItem", "schema": 1, "value": []})
        status, message = gate_assert.assert_blocks_sound(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.PASS, message)

    def test_a_block_that_does_not_hash_to_its_name_fails(self):
        cid = cas.cid_dagcbor(cas.encode({}))
        self.builder.corrupt_block(cid, b"\xa1\x61a\x01")     # {"a": 1}, not {}
        status, message = gate_assert.assert_blocks_sound(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn(cid, message)

    def test_a_block_that_does_not_decode_fails(self):
        data = b"\x9f\x01\xff"                                 # indefinite array
        self.builder.corrupt_block(cas.cid_dagcbor(data), data)
        status, message = gate_assert.assert_blocks_sound(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("indefinite", message)

    def test_a_block_that_is_not_canonically_encoded_fails(self):
        data = b"\xa2\x62bb\x01\x61a\x02"                      # keys out of order
        self.builder.corrupt_block(cas.cid_dagcbor(data), data)
        status, message = gate_assert.assert_blocks_sound(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("canonical", message.lower())

    def test_a_corrupt_raw_block_fails(self):
        self.builder.corrupt_block(cas.cid_raw(b"hello\n"), b"tampered\n")
        status, message = gate_assert.assert_blocks_sound(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("tampered" if "tampered" in message else "hash",
                      message.lower())


class TestRunExitAssertion(TempTree):
    def _exit(self, name, code):
        d = self.path("logs", name)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "exit"), "w") as fh:
            fh.write("%d\n" % code)

    def test_all_expected_exits_pass(self):
        for name in ("cold", "again", "resumed", "elsewhere", "consumer"):
            self._exit(name, 0)
        self._exit("fail", 1)
        status, message = gate_assert.assert_runs_exited(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.PASS, message)

    def test_a_producer_run_that_failed_is_a_failure(self):
        for name in ("cold", "again", "resumed", "elsewhere", "consumer"):
            self._exit(name, 0)
        self._exit("fail", 1)
        self._exit("elsewhere", 1)
        status, message = gate_assert.assert_runs_exited(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("elsewhere", message)

    def test_the_fail_run_exiting_zero_is_a_failure(self):
        for name in ("cold", "again", "resumed", "elsewhere", "consumer"):
            self._exit(name, 0)
        self._exit("fail", 0)
        status, message = gate_assert.assert_runs_exited(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("fail", message)

    def test_resumed_failing_on_nextflows_own_publish_copy_is_not_our_failure(self):
        """The lock for assertion 4c is caught by PublishDir first (issue 17)."""
        for name in ("cold", "again", "elsewhere", "consumer"):
            self._exit(name, 0)
        self._exit("fail", 1)
        self._exit("resumed", 1)
        with open(self.path("logs", "resumed", "nextflow.log"), "w") as fh:
            fh.write("DEBUG nextflow.processor.PublishDir - Failed to publish "
                     "file: /w/A.bam; to: cas://lab/aligned/A/A.bam [copy]\n")
        status, message = gate_assert.assert_runs_exited(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.PASS, message)
        self.assertIn("PublishDir", message)

    def test_resumed_failing_for_any_other_reason_is_a_failure(self):
        for name in ("cold", "again", "elsewhere", "consumer"):
            self._exit(name, 0)
        self._exit("fail", 1)
        self._exit("resumed", 1)
        status, message = gate_assert.assert_runs_exited(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("resumed", message)

    def test_a_missing_exit_file_is_a_failure(self):
        self._exit("cold", 0)
        status, message = gate_assert.assert_runs_exited(gate_assert.Gate(self.tmp))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("again", message)


class TestFailedRunPartiality(unittest.TestCase):
    """Assertion 3's "collections are partial and say so" decision."""

    FULL = {"aligned", "stats", "qc", "chunks", "reports"}

    def test_a_run_missing_an_output_is_partial(self):
        counts = {"aligned": 3, "chunks": 3, "stats": 3}      # no qc, no reports
        self.assertTrue(gate_assert._failed_run_is_partial(counts, self.FULL))

    def test_a_run_with_a_short_collection_is_partial(self):
        counts = {"aligned": 3, "chunks": 3, "stats": 3, "qc": 3, "reports": 1}
        self.assertTrue(gate_assert._failed_run_is_partial(counts, self.FULL))

    def test_a_run_with_every_output_full_is_not_partial(self):
        counts = {k: 3 for k in self.FULL}
        self.assertFalse(gate_assert._failed_run_is_partial(counts, self.FULL))

    def test_reports_absent_is_still_partial(self):
        """On a failed run reports may never publish; that is partiality itself."""
        counts = {"aligned": 3, "chunks": 3, "stats": 3}
        self.assertTrue(gate_assert._failed_run_is_partial(counts, self.FULL))


class TestUserNeedle(unittest.TestCase):
    def test_the_os_user_name_is_determined(self):
        self.assertTrue(gate_assert.os_user_name())


if __name__ == "__main__":
    unittest.main()
