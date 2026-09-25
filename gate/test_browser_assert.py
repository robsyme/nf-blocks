# gate/test_browser_assert.py
"""The browser tier's checks, over synthetic observations."""
import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import browser_assert as B  # noqa: E402

CID = "bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua"
YEAR = "http://127.0.0.1:1/stores/year/index/v2.sqlite"
CLOUD_YEAR_URL = "https://pub-bucket.s3.ca-central-1.amazonaws.com/year/index/v2.sqlite"


def step(**kw):
    base = {"state": "ready", "requests": [], "verified": [], "errors": [], "runs": [], "producers": [],
            "items": [], "latest": None, "pageErrors": [], "consoleErrors": []}
    base.update(kw)
    return base


def reads(n, size, phase="query", url=YEAR):
    return [{"url": url, "method": "GET", "status": 206, "range": "bytes=0-4095", "bytes": size, "phase": phase}] * n


class QueryCostTest(unittest.TestCase):
    def test_counts_only_query_phase_reads_of_the_snapshot(self):
        s = step(requests=reads(3, 4096, "open") + reads(7, 4096) + reads(2, 99, url="http://127.0.0.1:1/x"))
        self.assertEqual(B.query_cost(s, "/stores/year/index/v2.sqlite"), (7, 7 * 4096))

    def test_limits(self):
        self.assertTrue(B.within((8, 65536), (8, 65536)))
        self.assertFalse(B.within((9, 1000), (8, 65536)))
        self.assertFalse(B.within((2, 65537), (8, 65536)))


class VerifiedTest(unittest.TestCase):
    def test_every_fetched_block_must_be_verified(self):
        fetched = {"url": "http://h/stores/current/blocks/%s/%s" % (CID[-2:], CID), "method": "GET",
                   "status": 200, "range": None, "bytes": 1, "phase": "query"}
        self.assertEqual(B.unverified(step(requests=[fetched], verified=[CID])), [])
        self.assertEqual(B.unverified(step(requests=[fetched], verified=[])), [CID])

    def test_a_block_refused_with_hash_mismatch_was_hash_checked(self):
        fetched = {"url": "http://h/stores/tampered/blocks/%s/%s" % (CID[-2:], CID), "method": "GET",
                   "status": 200, "range": None, "bytes": 1, "phase": "open"}
        refused = [{"error": "hash_mismatch", "cid": CID}]
        self.assertEqual(B.unverified(step(requests=[fetched], errors=refused)), [])
        other = [{"error": "schema_invalid", "cid": CID}]
        self.assertEqual(B.unverified(step(requests=[fetched], errors=other)), [CID])

    def test_a_404_block_is_not_a_fetched_block(self):
        missing = {"url": "http://h/blocks/%s/%s" % (CID[-2:], CID), "method": "GET", "status": 404,
                   "range": None, "bytes": 0, "phase": "query"}
        self.assertEqual(B.unverified(step(requests=[missing])), [])


class ProducersTest(unittest.TestCase):
    def test_producer_rows_compare_as_sets_of_tuples(self):
        row = {"content": "c", "item": "i", "collection": "k", "completion": "r", "filename": "A.bam"}
        self.assertEqual(B.producer_set([row, row]), {("c", "i", "k", "r", "A.bam")})


class CloudPrepareTest(unittest.TestCase):
    """cloud_prepare (gate/cloud/cloud.sh): the scenario against real bucket names."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.root, "browser"))
        self.expected = {
            "content": CID,
            "producers": [[CID, "item1", "coll1", "run1", "A.bam"]],
            "year": {"params": {"producersOf": ["year-content-cid"],
                                 "itemsWhere": ["year-run-cid", "aligned", "sample", "string", "B"]}},
        }
        with open(os.path.join(self.root, "browser", "expected.json"), "w") as fh:
            json.dump(self.expected, fh)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def scenario(self):
        B.cloud_prepare(self.root, "pub-bucket", "priv-bucket", "ca-central-1")
        with open(os.path.join(self.root, "browser", "cloud-scenario.json")) as fh:
            return {s["id"]: s for s in json.load(fh)["steps"]}

    def test_step_ids_match_assertions_6_and_7(self):
        steps = self.scenario()
        self.assertEqual(set(steps), {"A6.producers", "A6.home", "A6.year.producers", "A6.year.items",
                                       "A7.explore", "A7.direct"})

    def test_A6_steps_use_the_public_bucket_url_and_HEAD_the_snapshot(self):
        steps = self.scenario()
        pub = "https://pub-bucket.s3.ca-central-1.amazonaws.com"
        for step_id in ("A6.producers", "A6.home"):
            self.assertEqual(steps[step_id]["query"], "?store=%s/" % pub, step_id)
        for step_id in ("A6.year.producers", "A6.year.items"):
            self.assertEqual(steps[step_id]["query"], "?store=%s/year/" % pub, step_id)
        self.assertEqual(steps["A6.producers"]["head"], "%s/%s" % (pub, B.SNAPSHOT))

    def test_A7_explore_step_asks_explore_for_the_priv_member(self):
        steps = self.scenario()
        s = steps["A7.explore"]
        self.assertEqual(s["server"], "explore")
        self.assertEqual(s["query"], "?member=priv")

    def test_private_bucket_url_is_used_only_by_A7_direct(self):
        steps = self.scenario()
        priv = "https://priv-bucket.s3.ca-central-1.amazonaws.com"
        self.assertEqual(steps["A7.direct"]["query"], "?store=%s/" % priv)
        for step_id, s in steps.items():
            if step_id == "A7.direct":
                continue
            self.assertNotIn("priv-bucket", json.dumps(s), "%s must not name the private bucket" % step_id)


class CloudCheckTest(unittest.TestCase):
    """cloud_check (gate/cloud/cloud.sh): assertions 6 and 7 over synthetic observations."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.root, "browser"))
        self.producer_row = {"content": CID, "item": "item1", "collection": "coll1", "completion": "run1", "filename": "A.bam"}
        with open(os.path.join(self.root, "browser", "expected.json"), "w") as fh:
            json.dump({"content": CID, "producers": [[CID, "item1", "coll1", "run1", "A.bam"]]}, fh)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def good_steps(self):
        s3_requests = [
            {"url": "https://pub-bucket.s3.ca-central-1.amazonaws.com/blocks/ua/%s" % CID,
             "method": "GET", "status": 206, "bytes": 12},
            {"url": "https://pub-bucket.s3.ca-central-1.amazonaws.com/?list-type=2&prefix=blocks/",
             "method": "GET", "status": 200, "bytes": 512},
        ]
        return [
            {"id": "A6.producers", "state": "ready", "producers": [self.producer_row],
             "head": {"status": 200}, "requests": s3_requests, "consoleErrors": []},
            {"id": "A6.home", "state": "ready", "requests": [], "consoleErrors": []},
            {"id": "A6.year.producers", "state": "ready", "consoleErrors": [],
             "requests": reads(5, 4096, url=CLOUD_YEAR_URL)},
            {"id": "A6.year.items", "state": "ready", "consoleErrors": [],
             "requests": reads(10, 4096, url=CLOUD_YEAR_URL)},
            {"id": "A7.explore", "state": "ready", "producers": [self.producer_row]},
            {"id": "A7.direct", "state": "error"},
        ]

    def write_observed(self, steps):
        with open(os.path.join(self.root, "browser", "cloud-observed.json"), "w") as fh:
            json.dump({"steps": steps}, fh)

    def check(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = B.cloud_check(self.root)
        return code, out.getvalue()

    def test_pass_when_every_condition_holds(self):
        self.write_observed(self.good_steps())
        code, out = self.check()
        self.assertEqual(code, 0)
        self.assertIn("PASS  A6", out)
        self.assertIn("PASS  A7", out)

    def test_fails_when_the_anonymous_HEAD_is_not_200(self):
        steps = self.good_steps()
        steps[0]["head"] = {"status": 403}
        self.write_observed(steps)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("anonymous HEAD", out)

    def test_fails_when_no_ranged_GET_answers_206(self):
        steps = self.good_steps()
        for s in steps:
            if s["id"].startswith("A6"):
                for r in s.get("requests", []):
                    if r["status"] == 206:
                        r["status"] = 200
        self.write_observed(steps)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("no ranged GET answered 206", out)

    def test_fails_when_no_ListObjectsV2_answers_200(self):
        steps = self.good_steps()
        steps[0]["requests"] = [r for r in steps[0]["requests"] if "list-type=2" not in r["url"]]
        self.write_observed(steps)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("no ListObjectsV2 answered 200", out)

    def test_fails_on_a_CORS_console_error(self):
        steps = self.good_steps()
        steps[0]["consoleErrors"] = ["Access to fetch at '...' has been blocked by CORS policy"]
        self.write_observed(steps)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("CORS errors", out)

    def test_fails_when_A7_direct_is_not_refused(self):
        steps = self.good_steps()
        for s in steps:
            if s["id"] == "A7.direct":
                s["state"] = "ready"
        self.write_observed(steps)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("readable directly", out)

    def test_fails_when_year_scale_limits_are_exceeded(self):
        steps = self.good_steps()
        for s in steps:
            if s["id"] == "A6.year.items":
                s["requests"] = reads(60, 4096, url=CLOUD_YEAR_URL)  # 60 > QUERY3_LIMIT's 50 requests
        self.write_observed(steps)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("year-scale limits exceeded", out)


if __name__ == "__main__":
    unittest.main()
