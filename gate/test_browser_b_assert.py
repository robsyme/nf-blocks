# gate/test_browser_b_assert.py
"""Browser tier B's checks (block explorer spec section 1.3, assertions 8-16)
over a small hand-made world: blocks written with the Gate's own encoder into
a writable member and a read-only one, a synthetic observed.json and
probes.json, and a consumer store of hashes. Each
assertion has one passing and at least one failing case."""
import csv
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import browser_b_assert as B  # noqa: E402
import cas  # noqa: E402
import dagjson  # noqa: E402


def dcid(tag):
    return cas.cid_dagcbor(cas.encode({"tag": tag}))


ITEMS = {"A": dcid("item A"), "B": dcid("item B"), "C": dcid("item C")}
COLLS = {"aligned": dcid("aligned"), "stats": dcid("stats"), "again": dcid("again aligned")}
NAMES = {"A": "A.bam", "B": "B.bam", "C": "C.stats"}
CONTENT = {k: b"bytes of %s\n" % v.encode() for k, v in NAMES.items()}
FILES = {k: {"name": NAMES[k], "sha256": hashlib.sha256(CONTENT[k]).hexdigest(), "cid": cas.cid_raw(CONTENT[k])}
         for k in NAMES}
STAMP = "2026-09-25T10:00:%02d.000Z"


def link(cid):
    return {"/": cid}


def item_member(key, coll):
    return {"item": {"address": link(ITEMS[key]), "via": [link(COLLS[coll])]}}


def claim_request(subject, verb, attribute=None, value=None, supersedes=(), second=0):
    return {"kind": "Claim", "subject": link(subject), "verb": verb, "attribute": attribute, "value": value,
            "supersedes": [link(s) for s in supersedes], "timestamp": STAMP % second}


class World(object):
    """A tier B run where every assertion passes; tests break one thing each."""

    def __init__(self):
        self.root = tempfile.mkdtemp()
        self.out = os.path.join(self.root, "browser-b")
        self.store = os.path.join(self.out, "store")
        self.store_out = os.path.join(self.out, "store-out")
        os.makedirs(self.store)
        os.makedirs(self.store_out)
        self.expected = {"items": dict(ITEMS), "collections": dict(COLLS), "files": FILES}
        self.steps = {}
        self.second = 0

        self.s1 = self.compose("B.first", [item_member("A", "aligned"), item_member("B", "aligned")], "first")
        self.s2 = self.compose("B.second", [{"selection": link(self.s1)}, item_member("B", "again"),
                                            item_member("C", "stats")], "second")
        second_name = self.name_claim_of("B.second")
        self.rename = self.claim_post("B.rename", claim_request(self.s2, "set", "name", "second-renamed", [second_name],
                                                                self.tick()))
        self.steps["B.rename"]["extracts"] = {"after": self.extract(written=self.s2)}
        self.log(self.rename)
        delete = self.claim_post("B.delete", claim_request(self.s2, "delete", second=self.tick()))
        self.claim_post("B.delete", claim_request(self.s2, "del", supersedes=[delete], second=self.tick()))
        self.steps["B.delete"]["extracts"] = {
            "list": self.extract(selections=[self.row(self.s1, "none", ["first"])]),
            "deleted": self.extract(selections=[self.row(self.s2, "deleted", ["second-renamed"])]),
            "after": self.extract(view=self.s2, deletion="none")}
        third_a = self.claim_post("B.conflict", claim_request(self.s2, "set", "name", "third-a", [self.rename],
                                                              self.tick()), page=0)
        self.post("B.conflict", claim_request(self.s2, "set", "name", "third-b", [self.rename], self.tick()),
                  400, {"error": "stale_supersedes", "message": "superseded", "at": "/supersedes/0"},
                  page=1)
        self.third_a = third_a
        self.steps["B.conflict"]["extracts"] = {
            "a": self.extract(writeOutcome="written", names=[{"name": "third-a", "claim": third_a, "conflicted": False}]),
            "b": self.extract(writeOutcome="stale_supersedes", errors=[{"error": "stale_supersedes", "cid": None}])}

        self.shared = os.path.join(self.out, "shared")
        os.makedirs(self.shared)
        self.build_shared()

        self.probes = {"replay": {"status": 200, "body": self.response(self.rename, False)},
                       "refusals": {"no_token": {"status": 403, "body": "refused"},
                                    "foreign_origin": {"status": 403, "body": "refused"},
                                    "text_plain": {"status": 415, "body": "refused"}},
                       "blocks_before": 20, "blocks_after_replay": 20, "blocks_after": 20,
                       "log_before": 9, "log_after_replay": 9, "log_after": 9,
                       "refusal_address": dcid("a Selection never written"),
                       "samplesheet_csv": 200, "samplesheet_json": 200,
                       "foreign_dry": {"status": 200, "body": json.dumps({
                           "address": link(self.s4), "exists": True, "here": True, "names": ["lab-renamed", "shared-name"],
                           "name_claims": [link(self.n2), link(self.lab_renamed)]})}}
        self.hashes = {source: {"%s.sha256" % f["name"]: f["sha256"] for f in FILES.values()}
                       for source in ("fromstore", "samplesheet")}
        order = sorted("ABC", key=lambda k: ITEMS[k])
        self.sheet = [{"sample": k, "1": "cas://%s/%s" % (FILES[k]["cid"], FILES[k]["name"])} for k in order]
        self.exit = "0"
        self.tasks = ["%s:%s" % (source, f["name"]) for source in ("fromstore", "samplesheet") for f in FILES.values()]

    def build_shared(self):
        """S3 held and named only in shared, copied by B.copy; S4 in lab, renamed by shared and then by B.foreign."""
        def claim(subject, value, supersedes):
            return dagjson.expected_claim(dagjson.loads(json.dumps(
                claim_request(subject, "set", "name", value, supersedes, self.tick()))), "gate")

        s3_request = {"kind": "Selection", "members": [item_member("A", "aligned")], "derived_from": []}
        s3_block = dagjson.expected_selection(dagjson.loads(json.dumps(s3_request)), "gate")
        self.s3 = B._put_block(self.shared, s3_block)
        B._log(self.shared, "selection", self.s3, 1767225600000)
        self.n_s = B._put_block(self.shared, claim(self.s3, "from-shared", []))
        B._log(self.shared, "claim", self.n_s, 1767225600000)
        s4_request = {"kind": "Selection", "members": [item_member("C", "stats")], "derived_from": []}
        self.s4 = self.put_block(dagjson.expected_selection(dagjson.loads(json.dumps(s4_request)), "gate"))
        self.log(self.s4, "selection")
        self.n1 = self.put_block(claim(self.s4, "lab-name", []))
        self.log(self.n1)
        self.n2 = B._put_block(self.shared, claim(self.s4, "shared-name", [self.n1]))
        B._log(self.shared, "claim", self.n2, 1767225600000)
        self.expected["shared"] = {"s3": self.s3, "n_s": self.n_s, "s4": self.s4, "n1": self.n1, "n2": self.n2}

        self.post("B.copy", s3_request, 200, json.dumps({"address": link(self.s3), "exists": True, "here": False,
                                                         "names": ["from-shared"], "name_claims": [link(self.n_s)]}), dry=True)
        self.put_block(s3_block)
        self.log(self.s3, "selection")
        self.post("B.copy", s3_request, 200, self.response(self.s3))
        self.copy_name = self.claim_post("B.copy", claim_request(self.s3, "set", "name", "from-shared", [self.n_s], self.tick()))
        self.log(self.copy_name)
        self.steps["B.copy"]["extracts"] = {
            "offer": self.extract(writeOutcome="elsewhere", heldElsewhere=self.s3, heldElsewhereNames=["from-shared"],
                                  composeName="from-shared"),
            "after": self.extract(written=self.s3, writeOutcome="written")}

        self.lab_renamed = self.claim_post("B.foreign", claim_request(self.s4, "set", "name", "lab-renamed", [self.n1],
                                                                      self.tick()))
        self.log(self.lab_renamed)
        self.steps["B.foreign"]["extracts"] = {
            "before": self.extract(view=self.s4, names=[{"name": "lab-name", "claim": self.n1, "conflicted": False}]),
            "after": self.extract(view=self.s4, names=[{"name": "lab-renamed", "claim": self.lab_renamed,
                                                        "conflicted": False}])}
        self.build_deleted()

    def build_deleted(self):
        """S5 held, named and deleted only in shared, restored by B.restore; S6 in lab, deleted in shared (B.deleted)."""
        def shared_claim(request):
            cid = B._put_block(self.shared, dagjson.expected_claim(dagjson.loads(json.dumps(request)), "gate"))
            B._log(self.shared, "claim", cid, 1767225600000)
            return cid

        s5_request = {"kind": "Selection", "members": [item_member("B", "aligned")], "derived_from": []}
        s5_block = dagjson.expected_selection(dagjson.loads(json.dumps(s5_request)), "gate")
        self.s5 = B._put_block(self.shared, s5_block)
        B._log(self.shared, "selection", self.s5, 1767225600000)
        self.n5 = shared_claim(claim_request(self.s5, "set", "name", "restored-name", [], self.tick()))
        self.d5 = shared_claim(claim_request(self.s5, "delete", second=self.tick()))
        s6_request = {"kind": "Selection", "members": [item_member("B", "again")], "derived_from": []}
        self.s6 = self.put_block(dagjson.expected_selection(dagjson.loads(json.dumps(s6_request)), "gate"))
        self.log(self.s6, "selection")
        self.d6 = shared_claim(claim_request(self.s6, "delete", second=self.tick()))
        self.expected["shared"].update(s5=self.s5, n5=self.n5, d5=self.d5, s6=self.s6, d6=self.d6)

        self.post("B.restore", s5_request, 200, json.dumps({
            "address": link(self.s5), "exists": True, "here": False, "names": ["restored-name"],
            "name_claims": [link(self.n5)], "deletion": "deleted", "deletion_claims": [link(self.d5)]}), dry=True)
        self.put_block(s5_block)
        self.log(self.s5, "selection")
        self.post("B.restore", s5_request, 200, self.response(self.s5))
        self.restore_name = self.claim_post("B.restore", claim_request(self.s5, "set", "name", "restored-name", [self.n5],
                                                                       self.tick()))
        self.log(self.restore_name)
        self.restore_del = self.claim_post("B.restore", claim_request(self.s5, "del", supersedes=[self.d5], second=self.tick()))
        self.log(self.restore_del)
        self.steps["B.restore"]["extracts"] = {
            "offer": self.extract(writeOutcome="elsewhere", heldElsewhere=self.s5, heldElsewhereNames=["restored-name"],
                                  heldElsewhereDeletion="deleted", copyLabel="Restore a copy here",
                                  composeName="restored-name"),
            "after": self.extract(written=self.s5, writeOutcome="written", view=self.s5, deletion="none",
                                  names=[{"name": "restored-name", "claim": self.restore_name, "conflicted": False}])}

        self.post("B.deleted", s6_request, 200, json.dumps({
            "address": link(self.s6), "exists": True, "here": True, "names": [], "name_claims": [],
            "deletion": "deleted", "deletion_claims": [link(self.d6)]}), dry=True)
        self.deleted_del = self.claim_post("B.deleted", claim_request(self.s6, "del", supersedes=[self.d6], second=self.tick()))
        self.log(self.deleted_del)
        self.steps["B.deleted"]["extracts"] = {
            "offer": self.extract(writeOutcome="exists", exists=self.s6, existsDeletion="deleted"),
            "after": self.extract(view=self.s6, deletion="none")}

    # -- building ---------------------------------------------------------
    def tick(self):
        self.second += 1
        return self.second

    def put_block(self, block):
        data = cas.encode(block)
        cid = cas.cid_dagcbor(data)
        path = os.path.join(self.store, "blocks", cid[-2:], cid)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)
        return cid

    def log(self, cid, kind="claim"):
        os.makedirs(os.path.join(self.store, "log"), exist_ok=True)
        with open(os.path.join(self.store, "log", "%013d-%s-%s" % (9000000000000 - self.tick(), kind, cid)), "w"):
            pass

    @staticmethod
    def response(cid, written=True):
        return json.dumps({"address": link(cid), "block": {}, "entry": "e", "written": written})

    def post(self, step, request, status, response, page=0, dry=False):
        s = self.steps.setdefault(step, {"id": step, "requests": [], "pageErrors": [], "consoleErrors": []})
        url = "http://127.0.0.1:1/api/put" + ("?dry_run=true" if dry else "")
        s["requests"].append({"url": url, "method": "POST", "status": status, "page": page, "body": json.dumps(request),
                              "responseBody": json.dumps(response) if isinstance(response, dict) else response})

    def claim_post(self, step, request, page=0):
        cid = self.put_block(dagjson.expected_claim(dagjson.loads(json.dumps(request)), "gate"))
        self.post(step, request, 200, self.response(cid), page)
        return cid

    def compose(self, step, members, name):
        request = {"kind": "Selection", "members": members, "derived_from": []}
        block = dagjson.expected_selection(dagjson.loads(json.dumps(request)), "gate")
        cid = self.put_block(block)
        self.log(cid, "selection")
        self.post(step, request, 200, json.dumps({"address": link(cid), "exists": False, "names": []}), dry=True)
        self.post(step, request, 200, self.response(cid))
        self.claim_post(step, claim_request(cid, "set", "name", name, second=self.tick()))
        self.steps[step]["extracts"] = {"after": self.extract(written=cid, writeOutcome="written")}
        self.steps[step]["saved"] = {"S1" if step == "B.first" else "S2": cid}
        return cid

    def name_claim_of(self, step):
        return json.loads(self.steps[step]["requests"][-1]["responseBody"])["address"]["/"]

    @staticmethod
    def extract(**kw):
        base = {"write": "available", "writeOutcome": "written", "writeSeq": "1", "written": None, "tray": "0",
                "selections": [], "view": None, "deletion": None, "names": [], "members": [], "errors": []}
        base.update(kw)
        return base

    @staticmethod
    def row(cid, deletion, names):
        return {"cid": cid, "deletion": deletion, "source": "tail", "names": names}

    # -- writing out ------------------------------------------------------
    def write(self):
        def dump(name, value):
            with open(os.path.join(self.out, name), "w") as fh:
                json.dump(value, fh)
        dump("expected.json", self.expected)
        dump("observed.json", {"steps": list(self.steps.values())})
        dump("probes.json", self.probes)
        with open(os.path.join(self.out, "selection.exit"), "w") as fh:
            fh.write(self.exit + "\n")
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=["sample", "1"], lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(self.sheet)
        with open(os.path.join(self.out, "samplesheet.csv"), "w", newline="") as fh:
            fh.write(buf.getvalue())
        dump("samplesheet.json", self.sheet)
        shutil.rmtree(self.store_out)
        os.makedirs(self.store_out)
        for source, files in self.hashes.items():
            for name, digest in files.items():
                data = ("%s  %s\n" % (digest, name[:-len(".sha256")])).encode()
                cid = cas.cid_raw(data)
                path = os.path.join(self.store_out, "blocks", cid[-2:], cid)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as fh:
                    fh.write(data)
                pointer = os.path.join(self.store_out, "coords", "hashes", source, name)
                os.makedirs(os.path.dirname(pointer), exist_ok=True)
                with open(pointer, "w") as fh:
                    fh.write("cas://%s\n" % cid)
        work = os.path.join(self.root, "selection", "work")
        shutil.rmtree(work, ignore_errors=True)
        for n, tag in enumerate(self.tasks):
            task = os.path.join(work, "%02x" % n, "task%d" % n)
            os.makedirs(task)
            with open(os.path.join(task, ".command.run"), "w") as fh:
                fh.write("#!/bin/bash\n### ---\n### name: 'HASH (%s)'\n### outputs:\n" % tag)
        return self.root

    def results(self):
        return {number: (status, message) for status, number, _title, message in B.evaluate(self.write())}


class CheckTest(unittest.TestCase):
    def setUp(self):
        self.w = World()

    def tearDown(self):
        shutil.rmtree(self.w.root, ignore_errors=True)

    def status(self, number):
        status, message = self.w.results()[number]
        return status, message

    def assertPass(self, number):
        status, message = self.status(number)
        self.assertEqual(status, B.PASS, message)

    def assertFail(self, number, fragment=None):
        status, message = self.status(number)
        self.assertEqual(status, B.FAIL, message)
        if fragment:
            self.assertIn(fragment, message)

    def test_the_whole_world_passes(self):
        results = self.w.results()
        self.assertEqual(sorted(results), [8, 9, 10, 11, 12, 13, 14, 15, 16])
        for number, (status, message) in results.items():
            self.assertEqual(status, B.PASS, "B%d: %s" % (number, message))

    def test_check_prints_every_line_and_the_total(self):
        self.w.write()
        buf = io.StringIO()
        stdout, sys.stdout = sys.stdout, buf
        try:
            code = B.check(self.w.root)
        finally:
            sys.stdout = stdout
        self.assertEqual(code, 0, buf.getvalue())
        for n in range(8, 17):
            self.assertIn("B%d" % n, buf.getvalue())
        self.assertIn("browser tier B: 9 PASS, 0 FAIL", buf.getvalue())

    # -- B8 ---------------------------------------------------------------
    def test_b8_a_response_address_other_than_the_gates_fails(self):
        req = self.w.steps["B.first"]["requests"][1]
        req["responseBody"] = self.w.response(dcid("something else"))
        self.assertFail(8, "B.first")

    def test_b8_a_written_other_than_the_response_fails(self):
        self.w.steps["B.second"]["extracts"]["after"]["written"] = dcid("elsewhere")
        self.assertFail(8, "written")

    def test_b8_a_block_missing_from_the_store_fails(self):
        os.remove(os.path.join(self.w.store, "blocks", self.w.s2[-2:], self.w.s2))
        self.assertFail(8)

    def test_b8_a_tampered_block_fails(self):
        path = os.path.join(self.w.store, "blocks", self.w.s1[-2:], self.w.s1)
        with open(path, "ab") as fh:
            fh.write(b"\x00")
        self.assertFail(8)

    def test_b8_second_without_the_nested_selection_fails(self):
        # The page composed {B, C} only: its address is its own, but not the Selection the scenario asked for.
        step = self.w.steps["B.second"]
        request = {"kind": "Selection", "members": [item_member("B", "again"), item_member("C", "stats")], "derived_from": []}
        cid = self.w.put_block(dagjson.expected_selection(dagjson.loads(json.dumps(request)), "gate"))
        step["requests"][1].update(body=json.dumps(request), responseBody=self.w.response(cid))
        step["extracts"]["after"]["written"] = cid
        self.assertFail(8, "members")

    def test_b8_no_selection_post_fails(self):
        self.w.steps["B.first"]["requests"] = [r for r in self.w.steps["B.first"]["requests"]
                                               if json.loads(r["body"])["kind"] != "Selection"]
        self.assertFail(8, "B.first")

    # -- B9 ---------------------------------------------------------------
    def test_b9_a_failed_pipeline_fails(self):
        self.w.exit = "1"
        self.assertFail(9, "exit")

    def test_b9_an_item_staged_twice_or_missing_fails(self):
        del self.w.hashes["fromstore"]["C.stats.sha256"]
        self.assertFail(9)

    def test_b9_a_wrong_digest_fails(self):
        self.w.hashes["fromstore"]["A.bam.sha256"] = "0" * 64
        self.assertFail(9)

    def test_b9_an_extra_file_fails(self):
        self.w.hashes["fromstore"]["A.bam.bai.sha256"] = "1" * 64
        self.assertFail(9)

    def test_b9_an_item_staged_twice_fails(self):
        # Two stagings of B publish one hashes/fromstore/B.bam.sha256; only the task count shows it.
        self.w.tasks.append("fromstore:B.bam")
        self.assertFail(9, "fromstore:B.bam")

    # -- B10 --------------------------------------------------------------
    def test_b10_a_claim_at_another_address_fails(self):
        self.w.steps["B.delete"]["requests"][0]["responseBody"] = self.w.response(dcid("not the claim"))
        self.assertFail(10, "B.delete")

    def test_b10_a_replay_that_wrote_fails(self):
        self.w.probes["replay"]["body"] = self.w.response(self.w.rename, True)
        self.assertFail(10, "replay")

    def test_b10_a_replay_that_wrote_a_block_fails(self):
        self.w.probes["blocks_after_replay"] = 21
        self.assertFail(10, "block")

    def test_b10_a_replay_refused_fails(self):
        self.w.probes["replay"] = {"status": 400, "body": json.dumps({"error": "stale_supersedes"})}
        self.assertFail(10, "replay")

    def test_b10_a_second_log_entry_fails(self):
        self.w.log(self.w.rename)
        self.assertFail(10, "log")

    def test_b10_a_claim_about_another_subject_fails(self):
        step = self.w.steps["B.rename"]
        request = claim_request(self.w.s1, "set", "name", "second-renamed", [], 40)
        cid = self.w.put_block(dagjson.expected_claim(dagjson.loads(json.dumps(request)), "gate"))
        step["requests"][0].update(body=json.dumps(request), responseBody=self.w.response(cid))
        self.assertFail(10, "subject")

    def test_b10_an_undo_not_superseding_the_delete_fails(self):
        step = self.w.steps["B.delete"]
        request = claim_request(self.w.s2, "del", supersedes=[self.w.rename], second=41)
        cid = self.w.put_block(dagjson.expected_claim(dagjson.loads(json.dumps(request)), "gate"))
        step["requests"][1].update(body=json.dumps(request), responseBody=self.w.response(cid))
        self.assertFail(10, "supersede")

    def test_b10_undo_missing_fails(self):
        self.w.steps["B.delete"]["requests"].pop()
        self.assertFail(10)

    def test_b10_a_deleted_selection_still_listed_fails(self):
        self.w.steps["B.delete"]["extracts"]["list"]["selections"].append(self.w.row(self.w.s2, "deleted", []))
        self.assertFail(10, "list")

    # -- B11 --------------------------------------------------------------
    def test_b11_an_overwrite_fails(self):
        # The second session's rename was accepted: two current names, or the last writer wins.
        self.w.steps["B.conflict"]["extracts"]["b"] = self.w.extract(writeOutcome="written")
        self.w.put_block(dagjson.expected_claim(dagjson.loads(json.dumps(
            claim_request(self.w.s2, "set", "name", "third-b", [self.w.rename], 30))), "gate"))
        status, message = self.status(11)
        self.assertEqual(status, B.FAIL, message)
        self.assertIn("stale_supersedes", message)
        self.assertIn("third-b", message)

    def test_b11_a_conflict_not_surfaced_fails(self):
        self.w.steps["B.conflict"]["extracts"]["b"]["errors"] = []
        self.assertFail(11, "data-error")

    # -- B12 --------------------------------------------------------------
    def test_b12_an_accepted_post_fails(self):
        self.w.probes["refusals"]["text_plain"]["status"] = 200
        self.assertFail(12, "text_plain")

    def test_b12_a_block_written_by_a_refusal_fails(self):
        self.w.probes["blocks_after"] = 21
        self.assertFail(12, "block")

    def test_b12_the_refused_block_in_the_store_fails(self):
        # An endpoint that wrote the block and then answered 403 or 415.
        fresh = {"kind": "Selection", "schema": 1, "asserted_by": "gate", "members": [], "derived_from": []}
        self.w.probes["refusal_address"] = self.w.put_block(fresh)
        self.assertFail(12, "holds")

    def test_b12_a_log_entry_written_by_a_refusal_fails(self):
        self.w.probes["log_after"] = 10
        self.assertFail(12, "log")

    def test_b12_no_refusal_address_fails(self):
        del self.w.probes["refusal_address"]
        self.assertFail(12)

    # -- B13 --------------------------------------------------------------
    def test_b13_a_missing_row_fails(self):
        self.w.sheet = self.w.sheet[:2]
        self.assertFail(13)

    def test_b13_a_wrong_cell_fails(self):
        self.w.sheet[0]["1"] = "cas://%s/%s" % (FILES["A"]["cid"], "renamed.bam")
        self.assertFail(13)

    def test_b13_a_failed_export_fails(self):
        self.w.probes["samplesheet_json"] = 404
        self.assertFail(13, "json")

    def test_b13_a_cell_staged_twice_fails(self):
        self.w.tasks.append("samplesheet:C.stats")
        self.assertFail(13, "samplesheet:C.stats")

    def test_b13_samplesheet_hashes_must_match(self):
        self.w.hashes["samplesheet"]["B.bam.sha256"] = "2" * 64
        self.assertFail(13, "samplesheet")


    # -- B14 --------------------------------------------------------------
    def test_b14_a_dry_run_not_offering_a_copy_fails(self):
        self.w.steps["B.copy"]["extracts"]["offer"].update(writeOutcome="written", heldElsewhere=None)
        self.assertFail(14, "elsewhere")

    def test_b14_a_name_not_prefilled_fails(self):
        self.w.steps["B.copy"]["extracts"]["offer"]["composeName"] = ""
        self.assertFail(14, "from-shared")

    def test_b14_a_name_claim_not_superseding_shareds_fails(self):
        del self.w.steps["B.copy"]["requests"][-1]
        self.w.claim_post("B.copy", claim_request(self.w.s3, "set", "name", "from-shared", [], 40))
        status, message = self.status(14)
        self.assertEqual(status, B.FAIL, message)
        self.assertIn("superseding", message)
        self.assertIn("conflicted True", message)

    def test_b14_a_copy_without_a_log_entry_fails(self):
        for name in os.listdir(os.path.join(self.w.store, "log")):
            if name.endswith("-selection-" + self.w.s3):
                os.remove(os.path.join(self.w.store, "log", name))
        self.assertFail(14, "log/")

    def test_b14_no_copy_fails(self):
        self.w.steps["B.copy"]["requests"] = self.w.steps["B.copy"]["requests"][:1]
        self.assertFail(14, "0 Selection and 0 Claim")

    # -- B15 --------------------------------------------------------------
    def test_b15_a_rename_superseding_the_other_members_claim_fails(self):
        # The page (or the endpoint) made lab's rename supersede shared's Claim too: no conflict remains.
        self.w.steps["B.foreign"]["requests"] = []
        self.w.claim_post("B.foreign", claim_request(self.w.s4, "set", "name", "lab-renamed", [self.w.n1, self.w.n2], 41))
        status, message = self.status(15)
        self.assertEqual(status, B.FAIL, message)
        self.assertIn("the rename supersedes", message)
        self.assertIn("expected a conflict", message)

    def test_b15_a_refused_rename_fails(self):
        self.w.steps["B.foreign"]["requests"] = []
        self.assertFail(15, "0 Claim POSTs")

    def test_b15_a_dry_run_not_here_fails(self):
        self.w.probes["foreign_dry"]["body"] = json.dumps({"address": link(self.w.s4), "exists": True, "here": False,
                                                           "names": ["lab-renamed", "shared-name"], "name_claims": []})
        self.assertFail(15, "dry run of S4")

    def test_b15_a_view_showing_the_other_members_name_fails(self):
        self.w.steps["B.foreign"]["extracts"]["after"]["names"].append(
            {"name": "shared-name", "claim": self.w.n2, "conflicted": True})
        self.assertFail(15, "lab-renamed alone")

    # -- B16 --------------------------------------------------------------
    def test_b16_the_whole_world_passes(self):
        self.assertPass(16)

    def test_b16_a_restore_without_the_del_claim_fails(self):
        self.w.steps["B.restore"]["requests"].pop()
        os.remove(os.path.join(self.w.store, "blocks", self.w.restore_del[-2:], self.w.restore_del))
        status, message = self.status(16)
        self.assertEqual(status, B.FAIL, message)
        self.assertIn("['set']", message)
        self.assertIn("deletion 'deleted'", message)

    def test_b16_a_del_superseding_nothing_fails(self):
        self.w.steps["B.restore"]["requests"].pop()
        self.w.claim_post("B.restore", claim_request(self.w.s5, "del", second=42))
        status, message = self.status(16)
        self.assertEqual(status, B.FAIL, message)
        self.assertIn("the del Claim supersedes []", message)

    def test_b16_a_deletion_held_elsewhere_not_named_fails(self):
        self.w.steps["B.deleted"]["extracts"]["offer"]["existsDeletion"] = "none"
        self.assertFail(16, "composing S6")

    def test_b16_a_restore_not_labelled_as_one_fails(self):
        self.w.steps["B.restore"]["extracts"]["offer"]["copyLabel"] = "Save a copy here"
        self.assertFail(16, "Restore a copy here")

    def test_b16_a_view_still_showing_the_deletion_after_the_restore_fails(self):
        self.w.steps["B.restore"]["extracts"]["after"]["deletion"] = "deleted"
        self.assertFail(16, "deletion 'deleted'")

    def test_b16_the_exists_path_without_a_claim_post_fails(self):
        self.w.steps["B.deleted"]["requests"].pop()
        os.remove(os.path.join(self.w.store, "blocks", self.w.deleted_del[-2:], self.w.deleted_del))
        status, message = self.status(16)
        self.assertEqual(status, B.FAIL, message)
        self.assertIn("B.deleted: 0 writing POST(s)", message)
        self.assertIn("across both members S6", message)

    def test_b16_the_exists_path_del_superseding_another_claim_fails(self):
        self.w.steps["B.deleted"]["requests"].pop()
        self.w.claim_post("B.deleted", claim_request(self.w.s6, "del", supersedes=[self.w.d5], second=43))
        self.assertFail(16, "the exists path's del supersedes")

    def test_b16_the_exists_path_view_still_deleted_fails(self):
        self.w.steps["B.deleted"]["extracts"]["after"]["deletion"] = "deleted"
        self.assertFail(16, "after Restore on the exists path")


class MemberWriteTest(unittest.TestCase):
    def test_a_block_and_its_log_entry_land_where_the_plugin_reads_them(self):
        root = tempfile.mkdtemp()
        try:
            block = {"kind": "Claim", "schema": 1, "asserted_by": "gate", "subject": cas.Cid(ITEMS["A"]), "verb": "set",
                     "attribute": "name", "value": "x", "supersedes": [], "timestamp": "2026-01-01T00:00:00.000Z"}
            cid = B._put_block(root, block)
            B._log(root, "claim", cid, 1767225600000)
            store = cas.Store(root)
            self.assertEqual(store.read_block(cid), block)
            self.assertEqual(store.store_log(), [("%013d" % (9999999999999 - 1767225600000), "claim", cid)])
        finally:
            shutil.rmtree(root)


class PostsTest(unittest.TestCase):
    def test_dry_runs_and_other_paths_are_not_posts(self):
        step = {"requests": [
            {"url": "http://h/api/put?dry_run=true", "method": "POST", "status": 200, "body": "{}", "responseBody": "{}"},
            {"url": "http://h/api/put", "method": "POST", "status": 200, "body": "{\"a\":1}", "responseBody": "{}"},
            {"url": "http://h/members.json", "method": "GET", "status": 200},
        ]}
        self.assertEqual([p["body"] for p in B._posts(step)], ["{\"a\":1}"])


if __name__ == "__main__":
    unittest.main()
