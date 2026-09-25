# gate/test_browser_assert.py
"""The browser tier's checks, over synthetic observations."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import browser_assert as B  # noqa: E402

CID = "bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua"
YEAR = "http://127.0.0.1:1/stores/year/index/v2.sqlite"


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


if __name__ == "__main__":
    unittest.main()
