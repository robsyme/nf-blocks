"""Unit tests for the machinery gate/assert.py builds its assertions on.

Run with:  python3 -m unittest discover -s gate

The assertions themselves are exercised end to end against gate/fixtures/root.
What is tested here is the part that decides whether an assertion passes: the
independent directory walk, the manifest diff, the leaf and string searches,
run resolution, and the failure paths that must not be swallowed.
"""

import importlib.util
import json
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

    def coord(self, rel_path, cid):
        """A coords/<rel_path> Store URI pointer file, as gate/fixtures/make_fixture.py's
        Writer.coord writes one."""
        path = os.path.join(self.root, "coords", *rel_path.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write("cas://%s/%s\n" % (cid, rel_path.rsplit("/", 1)[-1]))

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
        for name in ("cold", "again", "resumed", "elsewhere", "outputs",
                     "outputs-badindex", "consumer"):
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
        for name in ("cold", "again", "elsewhere", "outputs", "outputs-badindex",
                     "consumer"):
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


class TestConsumerOutputDir(TempTree):
    """Assertion 6 reads the consumer's lineage WorkflowRun from store-out/nf/."""

    def _workflow_run(self, key, name, metadata):
        envelope = {"version": "lineage/v1beta1", "kind": "WorkflowRun",
                    "spec": {"name": name, "sessionId": "s", "params": [],
                             "config": {}, "metadata": metadata}}
        self.write("store-out/nf/%s/.data.json" % key,
                   json.dumps(envelope).encode("utf-8"))

    def test_reads_the_consumer_runs_output_dir(self):
        self._workflow_run("c0" * 16, "consumer", {"outputDir": "cas://out"})
        self._workflow_run("c1" * 16, "someone-else", {"outputDir": "/tmp/results"})
        self.write("store-out/nf/c0c0/task/.data.json",
                   json.dumps({"kind": "TaskRun", "spec": {"name": "consumer"}}).encode("utf-8"))
        gate = gate_assert.Gate(self.tmp)
        self.assertEqual(gate_assert._consumer_output_dir(gate), "cas://out")

    def test_a_record_without_metadata_reads_as_none(self):
        self._workflow_run("c0" * 16, "consumer", None)
        gate = gate_assert.Gate(self.tmp)
        self.assertIsNone(gate_assert._consumer_output_dir(gate))

    def test_no_consumer_record_is_an_error(self):
        self._workflow_run("c1" * 16, "someone-else", {"outputDir": "cas://out"})
        gate = gate_assert.Gate(self.tmp)
        with self.assertRaises(cas.GateError) as ctx:
            gate_assert._consumer_output_dir(gate)
        self.assertIn("consumer", str(ctx.exception))
        self.assertIn("found 0", str(ctx.exception))

    def test_two_consumer_records_is_an_error(self):
        self._workflow_run("c0" * 16, "consumer", {"outputDir": "cas://out"})
        self._workflow_run("c2" * 16, "consumer", {"outputDir": "cas://out"})
        gate = gate_assert.Gate(self.tmp)
        with self.assertRaises(cas.GateError) as ctx:
            gate_assert._consumer_output_dir(gate)
        self.assertIn("found 2", str(ctx.exception))


class TestConsumerLogHasOutputDirLine(TempTree):
    """Assertion 6 also checks plugin log.* calls reach the consumer's nextflow.log."""

    def test_the_line_present_is_true(self):
        self.write("logs/consumer/nextflow.log",
                   b"INFO nextflow.cas - outputDir not "
                   b"set; publishing to cas://out\n")
        gate = gate_assert.Gate(self.tmp)
        self.assertTrue(gate_assert._consumer_log_has_output_dir_line(gate))

    def test_the_line_absent_is_false(self):
        self.write("logs/consumer/nextflow.log",
                   b"INFO nextflow.Session - Session start\n")
        gate = gate_assert.Gate(self.tmp)
        self.assertFalse(gate_assert._consumer_log_has_output_dir_line(gate))

    def test_no_log_file_is_false(self):
        gate = gate_assert.Gate(self.tmp)
        self.assertFalse(gate_assert._consumer_log_has_output_dir_line(gate))

    def test_the_line_on_the_console_is_true(self):
        self.write("logs/consumer/stdout.log",
                   b"Launching `main.nf` [x] - revision: 1\noutputDir not set; publishing to cas://out\n")
        gate = gate_assert.Gate(self.tmp)
        self.assertTrue(gate_assert._consumer_console_has_output_dir_line(gate))

    def test_the_line_only_in_nextflow_log_is_not_on_the_console(self):
        # Before the fix: a robsyme.cas.* logger, which Nextflow's console filter drops.
        self.write("logs/consumer/nextflow.log",
                   b"INFO robsyme.cas.trace.CasObserverFactory - outputDir not "
                   b"set; publishing to cas://out\n")
        self.write("logs/consumer/stdout.log", b"Launching `main.nf` [x] - revision: 1\n")
        gate = gate_assert.Gate(self.tmp)
        self.assertFalse(gate_assert._consumer_console_has_output_dir_line(gate))


class ProvidersTest(unittest.TestCase):
    def test_addresses_by_provider(self):
        a, b = str(cas.cid_raw(b"a")), str(cas.cid_raw(b"b"))
        rc = {"providers": {"head-node": [cas.Cid(a), cas.Cid(b)], "fusion-node": [cas.Cid(b)]}}
        self.assertEqual(gate_assert._provider_of(rc), {a: {"head-node"}, b: {"head-node", "fusion-node"}})

    def test_command_cas_lines(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "x.txt"), "wb") as f:
                f.write(b"x\n")
            digest = cas.sha256_of_file(os.path.join(d, "x.txt"))
            with open(os.path.join(d, ".command.cas"), "w") as f:
                f.write("%s  x.txt\n%s  gone.txt\n" % (digest, "0" * 64))
            self.assertEqual(gate_assert._command_cas_problems(d), ["%s: .command.cas names gone.txt, which is not in the task directory" % d])


class WalkTest(unittest.TestCase):
    def test_an_executable_file_is_regular(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "tool.sh")
            with open(p, "w") as f:
                f.write("#!/bin/sh\n")
            os.chmod(p, 0o755)
            self.assertEqual(gate_assert._walk(d, d)["tool.sh"]["mode"], "regular")


class SeedingTest(unittest.TestCase):
    def test_permission_failures_name_a_locked_block(self):
        # Index.groovy:642 logs "<kind> <cid> could not be read (<e.message>)", and an
        # AccessDeniedException's message is the path alone.
        locked = ["/g/store/blocks/yx/bafyx"]
        log = "WARN nextflow.cas - run bafyx could not be read (/g/store/blocks/yx/bafyx); it will be retried"
        self.assertEqual(gate_assert._permission_failures(log, locked), [log])
        self.assertEqual(gate_assert._permission_failures("run bafyz could not be read (/g/store/blocks/yz/bafyz); it will be retried", locked), [])
        self.assertEqual(gate_assert._permission_failures("fine", locked), [])

    def test_a_locked_completion_read_through_block_of_kind_counts(self):
        # Measured on the Gate (2026-09-28): a RunCompletion read that hits mode
        # 000 is caught in Index.groovy's blockOfKind and logged as "could not
        # be decoded as RunCompletion: <path>", never "could not be read".
        locked = ["/g/store/blocks/yx/bafyx"]
        log = "WARN  robsyme.cas.core.Index - block bafyx could not be decoded as RunCompletion: /g/store/blocks/yx/bafyx"
        self.assertEqual(gate_assert._permission_failures(log, locked), [log])


class NodeHashEvidenceTest(TempTree):
    """Review I1: again's leaves must be tied to verified .command.cas digests."""

    def task(self, rel, data):
        full = self.write(rel + "/out.txt", data)
        with open(os.path.join(os.path.dirname(full), ".command.cas"), "w") as f:
            f.write("%s  out.txt\n" % cas.sha256_of_file(full))
        return os.path.dirname(full)

    def test_no_command_cas_anywhere_fails(self):
        os.makedirs(self.path("work", "ab", "cdef"))
        problems = gate_assert._node_hash_problems({cas.cid_raw(b"x")}, [self.path("work", "ab", "cdef")])
        self.assertEqual(len(problems), 1)
        self.assertIn("no task directory holds a .command.cas line", problems[0])

    def test_a_leaf_no_command_cas_covers_fails(self):
        d = self.task("work/ab/cdef", b"x\n")
        problems = gate_assert._node_hash_problems({cas.cid_raw(b"x\n"), cas.cid_raw(b"other")}, [d])
        self.assertEqual(len(problems), 1)
        self.assertIn(cas.cid_raw(b"other"), problems[0])

    def test_a_digest_the_file_does_not_hash_to_covers_nothing(self):
        d = self.task("work/ab/cdef", b"x\n")
        self.write("work/ab/cdef/out.txt", b"changed\n")
        problems = gate_assert._node_hash_problems({cas.cid_raw(b"x\n")}, [d])
        self.assertTrue(any("it hashes to" in p for p in problems))
        self.assertTrue(any("no task directory holds" in p for p in problems))

    def test_every_leaf_covered_passes(self):
        d = self.task("work/ab/cdef", b"x\n")
        self.assertEqual(gate_assert._node_hash_problems({cas.cid_raw(b"x\n")}, [d]), [])


class HeadNodeTest(unittest.TestCase):
    """Review I3: a leaf absent from cold's providers is not under head-node."""

    def test_a_leaf_missing_from_cold_providers_fails(self):
        a = cas.cid_raw(b"a")
        self.assertEqual(len(gate_assert._head_node_problems({a}, {})), 1)

    def test_a_leaf_under_another_provider_fails(self):
        a = cas.cid_raw(b"a")
        self.assertEqual(len(gate_assert._head_node_problems({a}, {a: {"fusion-node"}})), 1)

    def test_head_node_passes(self):
        a = cas.cid_raw(b"a")
        self.assertEqual(gate_assert._head_node_problems({a}, {a: {"head-node"}}), [])


class SeedingRunsTest(TempTree):
    """Review I2: the locked runs come from the Store Log through the watermark."""

    def setUp(self):
        super(SeedingRunsTest, self).setUp()
        self.b = StoreBuilder(self.path("store"))
        self.old = self.b.block({"kind": "RunCompletion", "n": 1})
        self.mid = self.b.block({"kind": "RunCompletion", "n": 2})
        self.new = self.b.block({"kind": "RunCompletion", "n": 3})
        self.b.store_log(1000, "run", self.old)
        self.b.store_log(2000, "run", self.mid)
        self.b.store_log(3000, "run", self.new)
        self.watermark = "%013d-run-%s" % (9999999999999 - 2000, self.mid)

    def test_runs_after_the_watermark_are_left_out(self):
        self.assertEqual(gate_assert._store_log_runs_through(self.b.store, self.watermark),
                         sorted([self.old, self.mid]))

    def test_matching_snapshot_passes(self):
        self.assertEqual(gate_assert._seeding_problems(self.b.store, self.watermark, [self.mid, self.old]), [])

    def test_a_snapshot_missing_a_logged_run_fails(self):
        problems = gate_assert._seeding_problems(self.b.store, self.watermark, [self.mid])
        self.assertEqual(len(problems), 1)
        self.assertIn(self.old, problems[0])

    def test_a_snapshot_holding_a_later_run_fails(self):
        problems = gate_assert._seeding_problems(self.b.store, self.watermark, [self.old, self.mid, self.new])
        self.assertEqual(len(problems), 1)
        self.assertIn(self.new, problems[0])

    def test_no_watermark_fails(self):
        self.assertEqual(len(gate_assert._seeding_problems(self.b.store, None, [self.old])), 1)


class OutputsAssertionsTest(TempTree):
    """gate/outputs and gate/outputs-badindex (milestone 5, plan 2026-09-29): a fake
    store-outputs shaped like the real runs, so assertions 14 to 16 run against it
    without the Gate. run "outputs" carries tuples (index.json), records (index.csv,
    header true), LEGACY's two unjoined publishDir files and one collectFile(storeDir:)
    file stored with no publish event; run "outputs-badindex"
    carries a tuples collection whose CSV index write failed (never_published)."""

    def setUp(self):
        super(OutputsAssertionsTest, self).setUp()
        self.b = StoreBuilder(self.path("store-outputs"))
        self.gate = gate_assert.Gate(self.tmp)

    def _manifest(self, run_name):
        return self.b.block({
            "kind": "RunManifest", "schema": 1, "asserted_by": "gate",
            "pipeline": "cas-gate-outputs", "run_name": run_name,
            "nf_run_hash": "a" * 32, "session_id": "s", "resumed": False,
            "nextflow_version": "26.04.6", "repository": None, "revision": None,
            "commit_id": None, "params": {}, "config": "", "script": None,
            "started_at": "2026-01-01T00:00:00.000Z"})

    def _item(self, value):
        return self.b.block({"kind": "OutputItem", "schema": 2, "value": value})

    def _addressed_leaf(self, name, data):
        """A Leaf whose address is the raw CID of `data`, also written to the
        store; returns (leaf, cid)."""
        cid = self.b.raw(data)
        return {"kind": "Leaf", "name": name, "address": cas.Cid(cid),
                "size": len(data), "reason": None}, cid

    def _never_published_leaf(self, name):
        return {"kind": "Leaf", "name": name, "address": None, "size": None,
                "reason": "never_published"}

    def _completion(self, run_link, collection_cids, unjoined=0, never_published=0):
        return self.b.block({
            "kind": "RunCompletion", "schema": 2, "asserted_by": "gate",
            "run": cas.Cid(run_link),
            "collections": [cas.Cid(c) for c in collection_cids],
            "input_set": None, "status": "succeeded", "exit_status": 0,
            "possibly_incomplete": False,
            "started_at": "2026-01-01T00:00:00.000Z",
            "finished_at": "2026-01-01T00:00:01.000Z",
            "anomalies": {"unresolvable": 0, "unaddressed": 0, "declined": 0,
                         "never_published": never_published, "unjoined": unjoined},
            "error": None, "providers": {}})

    def _exit(self, name, code):
        d = self.path("logs", name)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "exit"), "w") as fh:
            fh.write("%d\n" % code)

    def _log(self, name, text):
        d = self.path("logs", name)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "nextflow.log"), "w") as fh:
            fh.write(text)

    def _build_outputs_run(self, tuples_leaf_address=None, unjoined=3, collected=True):
        """Run "outputs": tuples/records join with a Meta Map carrying "id",
        each with an index Nextflow wrote; LEGACY's two publishDir files and
        the collectFile(storeDir:) file are addressed coordinates no item or
        index leaf names.

        `tuples_leaf_address` overrides the tuples index leaf's recorded
        address (assertion 14's FAIL case); `unjoined` overrides
        RunCompletion.anomalies.unjoined (assertion 16's FAIL case);
        `collected=False` leaves the collectFile coordinate out.
        """
        run_link = self._manifest("outputs")
        a_leaf, _a_cid = self._addressed_leaf("A.txt", b"sample A\n")
        b_leaf, _b_cid = self._addressed_leaf("B.txt", b"sample B\n")
        tuples_items = [self._item([{"id": "A"}, a_leaf]),
                        self._item([{"id": "B"}, b_leaf])]
        records_items = [self._item({"id": "A", "file": a_leaf}),
                         self._item({"id": "B", "file": b_leaf})]

        tuples_index_bytes = b'[[{"id":"A"},"tuples/A/A.txt"],[{"id":"B"},"tuples/B/B.txt"]]'
        tuples_index_cid = self.b.raw(tuples_index_bytes)
        self.b.coord("tuples/index.json", tuples_index_cid)
        tuples_leaf = {"kind": "Leaf", "name": "index.json",
                       "address": cas.Cid(tuples_leaf_address or tuples_index_cid),
                       "size": len(tuples_index_bytes), "reason": None}

        records_index_bytes = b'"id","file"\n"A","records/A/A.txt"\n"B","records/B/B.txt"\n'
        records_index_cid = self.b.raw(records_index_bytes)
        self.b.coord("records/index.csv", records_index_cid)
        records_leaf = {"kind": "Leaf", "name": "index.csv",
                        "address": cas.Cid(records_index_cid),
                        "size": len(records_index_bytes), "reason": None}

        tuples_cid = self.b.block({
            "kind": "OutputCollection", "schema": 1, "asserted_by": "gate",
            "run": cas.Cid(run_link), "name": "tuples",
            "items": [cas.Cid(c) for c in tuples_items],
            "paths": [["tuples/A/A.txt"], ["tuples/B/B.txt"]],
            "index": {"leaf": tuples_leaf, "path": "tuples/index.json"}})
        records_cid = self.b.block({
            "kind": "OutputCollection", "schema": 1, "asserted_by": "gate",
            "run": cas.Cid(run_link), "name": "records",
            "items": [cas.Cid(c) for c in records_items],
            "paths": [["records/A/A.txt"], ["records/B/B.txt"]],
            "index": {"leaf": records_leaf, "path": "records/index.csv"}})

        legacy_a = self.b.raw(b"legacy A\n")
        legacy_b = self.b.raw(b"legacy B\n")
        self.b.coord("legacy/A.legacy", legacy_a)
        self.b.coord("legacy/B.legacy", legacy_b)
        if collected:
            self.b.coord("collected/samples.txt", self.b.raw(b"A\nB\n"))

        self._completion(run_link, [tuples_cid, records_cid], unjoined=unjoined)
        self._exit("outputs", 0)
        self._log("outputs", "%s\n%s%s\n" % (
            gate_assert.LEGACY_PUBLISHDIR_WARNING,
            gate_assert.LEGACY_UNJOINED_WARNING,
            "cas://lab/collected/samples.txt, cas://lab/legacy/A.legacy, cas://lab/legacy/B.legacy"))

    def _build_badindex_run(self, index_leaf=None, write_coord=False):
        """Run "outputs-badindex": tuples joins with a Meta Map carrying "id";
        its CSV index write failed, so the index leaf is never_published
        unless `index_leaf` overrides it (assertion 15's FAIL case).
        `write_coord` additionally writes a real coords/tuples/index.csv
        pointer (and the raw bytes it names) in store-outputs, independent of
        whatever the index leaf itself claims — assertion 15 must read this,
        not just trust the leaf's own recorded fields."""
        run_link = self._manifest("outputs-badindex")
        a_leaf, _a_cid = self._addressed_leaf("A.txt", b"sample A\n")
        b_leaf, _b_cid = self._addressed_leaf("B.txt", b"sample B\n")
        items = [self._item([{"id": "A"}, a_leaf]), self._item([{"id": "B"}, b_leaf])]
        if index_leaf is None:
            index_leaf = self._never_published_leaf("index.csv")
        if write_coord:
            coord_bytes = b'"id","file"\n"A","tuples/A/A.txt"\n"B","tuples/B/B.txt"\n'
            coord_cid = self.b.raw(coord_bytes)
            self.b.coord("tuples/index.csv", coord_cid)
        tuples_cid = self.b.block({
            "kind": "OutputCollection", "schema": 1, "asserted_by": "gate",
            "run": cas.Cid(run_link), "name": "tuples",
            "items": [cas.Cid(c) for c in items],
            "paths": [["tuples/A/A.txt"], ["tuples/B/B.txt"]],
            "index": {"leaf": index_leaf, "path": "tuples/index.csv"}})
        self._completion(run_link, [tuples_cid], never_published=1)
        self._exit("outputs-badindex", 0)

    # -- assertion 14 -----------------------------------------------------

    def test_fourteen_passes(self):
        self._build_outputs_run()
        status, message = gate_assert.assert_fourteen(self.gate)
        self.assertEqual(status, gate_assert.PASS, message)

    def test_fourteen_fails_when_the_index_leaf_address_is_wrong(self):
        self._build_outputs_run(tuples_leaf_address=cas.cid_raw(b"not the bytes at the coordinate"))
        status, message = gate_assert.assert_fourteen(self.gate)
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("tuples", message)

    # -- assertion 15 -----------------------------------------------------

    def test_fifteen_passes(self):
        self._build_badindex_run()
        status, message = gate_assert.assert_fifteen(self.gate)
        self.assertEqual(status, gate_assert.PASS, message)

    def test_fifteen_fails_when_the_bad_runs_index_leaf_is_addressed(self):
        addressed, _cid = self._addressed_leaf("index.csv", b"id,file\n")
        self._build_badindex_run(index_leaf=addressed)
        status, message = gate_assert.assert_fifteen(self.gate)
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("never_published", message)

    def test_fifteen_fails_when_a_tuples_index_csv_coordinate_exists_and_the_leaf_is_addressed(self):
        """The store itself, not just the leaf's own recorded fields: a real
        coords/tuples/index.csv pointer plus an addressed leaf must fail even
        though both individually look plausible."""
        addressed, _cid = self._addressed_leaf("index.csv", b"id,file\n")
        self._build_badindex_run(index_leaf=addressed, write_coord=True)
        status, message = gate_assert.assert_fifteen(self.gate)
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("coords/tuples/index.csv", message)

    def test_fifteen_passes_when_a_tuples_index_csv_coordinate_exists_but_the_leaf_is_not_addressed(self):
        """The brief's "or, if it does" clause: a coordinate may exist as
        long as the leaf still is not addressed."""
        self._build_badindex_run(write_coord=True)
        status, message = gate_assert.assert_fifteen(self.gate)
        self.assertEqual(status, gate_assert.PASS, message)

    # -- assertion 16 -----------------------------------------------------

    def test_sixteen_passes(self):
        self._build_outputs_run()
        status, message = gate_assert.assert_sixteen(self.gate)
        self.assertEqual(status, gate_assert.PASS, message)

    def test_sixteen_fails_when_unjoined_undercounts_the_legacy_publishes(self):
        self._build_outputs_run(unjoined=1)
        status, message = gate_assert.assert_sixteen(self.gate)
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("unjoined", message)

    def test_sixteen_fails_when_the_file_stored_with_no_publish_event_is_not_counted(self):
        """Final review C1: the plugin once counted only publish events, so
        the collectFile(storeDir:) coordinate went uncounted."""
        self._build_outputs_run(unjoined=2)
        status, message = gate_assert.assert_sixteen(self.gate)
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("anomalies.unjoined is 2, but 3", message)

    def test_sixteen_fails_when_the_collected_file_is_missing(self):
        self._build_outputs_run(unjoined=2, collected=False)
        status, message = gate_assert.assert_sixteen(self.gate)
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("collected/samples.txt", message)

    def test_sixteen_fails_on_a_stray_coordinate_no_collection_names(self):
        self._build_outputs_run()
        self.b.coord("stray/x.txt", self.b.raw(b"x\n"))
        status, message = gate_assert.assert_sixteen(self.gate)
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("stray/x.txt", message)

    def test_sixteen_passes_beside_the_badindex_run_and_its_own_coordinates(self):
        """outputs-badindex shares store-outputs: a coordinate its collection
        names is claimed, not counted against run outputs."""
        self._build_outputs_run()
        self._build_badindex_run(write_coord=True)
        status, message = gate_assert.assert_sixteen(self.gate)
        self.assertEqual(status, gate_assert.PASS, message)



# --------------------------------------------------------------------------
# Assertion 9 (milestone 6): a hand-built GATE_ROOT, stepped the way gate.sh
# steps the real one, with gate_assert's own checkpoint writer at each step
# --------------------------------------------------------------------------

def _retention_write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(text)


def build_retention_root(test, trash_extra=None, dry_dead=0, keep_ledger_after_restore=False,
                         survivor=None, live_refused_exit=1):
    """A GATE_ROOT whose store-retention, logs/retention/*.exit, *.out and
    checkpoint JSON files are what an honest assertion-9 run leaves, then with
    one named fault applied the way a buggy plugin would apply it.

    trash_extra: "shared_two" (b's two.txt, which a shares) or "b_item" (one of
    b's OutputItems, metadata) is trashed by sweeps 1 and 3 and deleted by
    sweep 4. dry_dead: the dry run's reported dead count. keep_ledger_after_restore:
    sweep 2 leaves the ledger although restore made its blocks live. survivor:
    "only_b" survives sweep 4. live_refused_exit: the exit status of the sweep
    run beside a fresh registration.
    """
    root = tempfile.mkdtemp(prefix="gate-retention-")
    test.addCleanup(shutil.rmtree, root, True)
    b = StoreBuilder(os.path.join(root, "store-retention"))
    gate = gate_assert.Gate(root)
    logs = os.path.join(root, "logs", "retention")
    clock = [1759200000000]

    def step(name, code, out=""):
        _retention_write(os.path.join(logs, name + ".exit"), "%d\n" % code)
        _retention_write(os.path.join(logs, name + ".out"), out)
        _retention_write(os.path.join(logs, name + ".err"), "")

    def checkpoint(name):
        gate_assert.retention_checkpoint(gate, name)

    def log(kind, cid):
        clock[0] += 1000
        b.store_log(clock[0], kind, cid)

    def claim(subject, verb, attribute, value, supersedes=()):
        clock[0] += 1000
        cid = b.block({"kind": "Claim", "schema": 1, "asserted_by": "gate",
                       "subject": cas.Cid(subject), "verb": verb, "attribute": attribute,
                       "value": value, "supersedes": [cas.Cid(s) for s in supersedes],
                       "timestamp": "2026-09-30T00:00:%02d.000Z" % (clock[0] // 1000 % 60)})
        log("claim", cid)
        return cid

    script = b.raw(b"params.tag = 'a'\n")
    blocks = {}

    def leaf(name, cid, size):
        return {"kind": "Leaf", "name": name, "address": cas.Cid(cid), "size": size, "reason": None}

    def run(tag):
        manifest = b.block({"kind": "RunManifest", "schema": 1, "asserted_by": "gate",
                            "pipeline": gate_assert.RETENTION_IDENTITY, "run_name": "retention-" + tag,
                            "script": cas.Cid(script), "params": {"tag": tag}})
        file_items = []
        for name in ("shared", "only_" + tag, "pin_" + tag):
            data = ("%s\n" % name).encode()
            raw = b.raw(data)
            blocks["%s_%s" % (tag, name)] = raw
            item = b.block({"kind": "OutputItem", "schema": 2,
                            "value": [{"id": name}, leaf(name + ".txt", raw, len(data))]})
            blocks["%s_item_%s" % (tag, name)] = item
            file_items.append(item)
        one = ("%s one\n" % tag).encode()
        one_cid = b.raw(one)
        two_cid = b.raw(b"shared two\n")
        blocks[tag + "_one"], blocks["shared_two"] = one_cid, two_cid
        directory = b.manifest([entry("one.txt", "regular", len(one), one_cid),
                                entry("two.txt", "regular", 11, two_cid)])
        blocks[tag + "_dir"] = directory
        dir_item = b.block({"kind": "OutputItem", "schema": 2,
                            "value": [{"id": "dir_" + tag}, leaf("dir_" + tag, directory, 0)]})
        files = b.block({"kind": "OutputCollection", "schema": 1, "asserted_by": "gate",
                         "run": cas.Cid(manifest), "name": "files",
                         "items": [cas.Cid(i) for i in file_items], "paths": [], "index": None})
        dirs = b.block({"kind": "OutputCollection", "schema": 1, "asserted_by": "gate",
                        "run": cas.Cid(manifest), "name": "dirs",
                        "items": [cas.Cid(dir_item)], "paths": [], "index": None})
        completion = b.block({"kind": "RunCompletion", "schema": 2, "asserted_by": "gate",
                              "run": cas.Cid(manifest), "collections": [cas.Cid(files), cas.Cid(dirs)],
                              "status": "succeeded", "exit_status": 0,
                              "finished_at": "2026-09-30T00:00:00.000Z"})
        log("run", completion)
        d = os.path.join(root, "logs", "retention-" + tag)
        _retention_write(os.path.join(d, "exit"), "0\n")
        return completion

    run_b = run("b")
    run_a = run("a")
    unshared = [blocks["b_only_b"], blocks["b_dir"], blocks["b_one"]]
    extra = {"shared_two": blocks["shared_two"], "b_item": blocks.get("b_item_only_b"), None: None}[trash_extra]
    trashed = unshared + ([extra] if extra else [])
    store = b.root
    checkpoint("after-runs")

    step("dry-1", 0, json.dumps({"applied": False, "dead": dry_dead, "trashed": dry_dead}) + "\n")
    checkpoint("after-dry-1")
    step("prune-dry", 0)
    step("prune", 0)
    release = claim(run_b, "set", "retain", "lineage")
    step("pin", 0)
    claim(blocks["b_item_pin_b"], "add", "pin", "figure 3")
    _retention_write(os.path.join(store, "live", "gate-fake"), "{}")
    step("live-refused", live_refused_exit,
         "live       1 run registered: gate_fake_run (session gate-fake), heartbeat 0 s ago\n"
         if live_refused_exit else "applied\n")
    checkpoint("before-sweep")

    def ledger(name, cids):
        body = {"sweep": name.split("-", 1)[1], "trashed_at": "2026-09-30T00:00:00.000Z",
                "deadline": "2026-10-14T00:00:00.000Z",
                "blocks": [{"cid": c, "size": 1} for c in sorted(cids)]}
        _retention_write(os.path.join(store, "trash", name), json.dumps(body))

    def released_lock():
        _retention_write(os.path.join(store, "sweep.lock"),
                         json.dumps({"sweep": "x", "state": "released", "started_at": "x", "beat": 1}))

    first = "1760400000000-20260930T000000Z-aaaaaaaa"
    step("sweep-1", 0, json.dumps({"applied": True, "ledger": first}) + "\n")
    ledger(first, trashed)
    os.remove(os.path.join(store, "live", "gate-fake"))
    released_lock()
    checkpoint("after-sweep-1")

    step("untrash", 0)
    ledger(first, [c for c in trashed if c != blocks["b_one"]])
    checkpoint("after-untrash")

    step("restore", 0)
    restore = claim(run_b, "del", "retain", None, [release])
    step("sweep-2", 0, json.dumps({"applied": True}) + "\n")
    if not keep_ledger_after_restore:
        os.remove(os.path.join(store, "trash", first))
    checkpoint("after-sweep-2")

    step("release-again", 0)
    claim(run_b, "set", "retain", "lineage", [restore])
    second = "1759200100000-20260930T000100Z-bbbbbbbb"
    step("sweep-3", 0, json.dumps({"applied": True, "ledger": second}) + "\n")
    ledger(second, trashed)
    checkpoint("after-sweep-3")

    step("sweep-4", 0, json.dumps({"applied": True}) + "\n")
    for c in trashed:
        if survivor and c == blocks["b_" + survivor]:
            continue
        path = b.store.block_path(c)
        if os.path.exists(path):
            os.remove(path)
    os.remove(os.path.join(store, "trash", second))
    if keep_ledger_after_restore:
        os.remove(os.path.join(store, "trash", first))
    checkpoint("after-sweep-4")
    return root


class RetentionAssertionTest(unittest.TestCase):
    """Assertion 9 over a hand-built GATE_ROOT: checkpoints, ledgers and a store."""

    def test_passes_when_every_checkpoint_matches(self):
        root = build_retention_root(self)
        status, message = gate_assert.assertion_9(gate_assert.Gate(root))
        self.assertEqual(status, gate_assert.PASS, message)

    def test_fails_when_a_shared_block_was_trashed(self):
        root = build_retention_root(self, trash_extra="shared_two")
        status, message = gate_assert.assertion_9(gate_assert.Gate(root))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("shared", message)

    def test_fails_when_metadata_was_trashed(self):
        root = build_retention_root(self, trash_extra="b_item")
        status, message = gate_assert.assertion_9(gate_assert.Gate(root))
        self.assertEqual(status, gate_assert.FAIL)
        self.assertIn("metadata", message)

    def test_fails_when_the_dry_run_found_dead_blocks(self):
        root = build_retention_root(self, dry_dead=3)
        self.assertEqual(gate_assert.assertion_9(gate_assert.Gate(root))[0], gate_assert.FAIL)

    def test_fails_when_restored_content_stayed_in_a_ledger(self):
        root = build_retention_root(self, keep_ledger_after_restore=True)
        self.assertEqual(gate_assert.assertion_9(gate_assert.Gate(root))[0], gate_assert.FAIL)

    def test_fails_when_the_last_sweep_left_released_content(self):
        root = build_retention_root(self, survivor="only_b")
        self.assertEqual(gate_assert.assertion_9(gate_assert.Gate(root))[0], gate_assert.FAIL)

    def test_fails_when_the_live_refusal_did_not_refuse(self):
        root = build_retention_root(self, live_refused_exit=0)
        self.assertEqual(gate_assert.assertion_9(gate_assert.Gate(root))[0], gate_assert.FAIL)

    def test_refs_name_both_runs_the_pinned_item_and_the_current_retain_claims(self):
        root = build_retention_root(self)
        refs = gate_assert.retention_refs(gate_assert.Gate(root))
        self.assertTrue(cas.is_cid(refs["RET_A"]) and cas.is_cid(refs["RET_B"]))
        self.assertNotEqual(refs["RET_A"], refs["RET_B"])
        store = cas.Store(os.path.join(root, "store-retention"))
        item = store.read_block(refs["RET_PIN_ITEM"])
        self.assertEqual(item["value"][1]["name"], "pin_b.txt")
        again = store.read_block(refs["RET_RELEASE"])
        self.assertEqual((again["verb"], again["attribute"], again["value"]), ("set", "retain", "lineage"))
        self.assertEqual(refs["RET_RESTORE"], again["supersedes"][0].text)

    def test_age_backdates_every_block(self):
        root = build_retention_root(self)
        gate_assert.retention_age(gate_assert.Gate(root), 15)
        store = cas.Store(os.path.join(root, "store-retention"))
        import time
        for _cid, path in store.blocks():
            self.assertLess(os.stat(path).st_mtime, time.time() - 14 * 86400)


if __name__ == "__main__":
    unittest.main()
