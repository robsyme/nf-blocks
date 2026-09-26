"""The Gate's DAG-JSON reader and expected-block builder (block explorer spec
section 1.3 assertions 8 and 10), and its current-state rules against the
vectors the plugin and the page share."""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cas  # noqa: E402
import dagjson  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(HERE, "..", "web", "test", "fixtures")

I1 = cas.cid_dagcbor(cas.encode({"n": 1}))
I2 = cas.cid_dagcbor(cas.encode({"n": 2}))
C1 = cas.cid_dagcbor(cas.encode({"c": 1}))
C2 = cas.cid_dagcbor(cas.encode({"c": 2}))


def link(cid):
    return '{"/":"%s"}' % cid


class DagJsonTest(unittest.TestCase):
    def test_links_and_bytes(self):
        value = dagjson.loads('{"l":%s,"b":{"/":{"bytes":"AQID+g"}}}' % link(I1))
        self.assertEqual(value["l"], cas.Cid(I1))
        self.assertEqual(value["b"], bytes([1, 2, 3, 250]))

    def test_every_page_vector_loads(self):
        with open(os.path.join(FIXTURES, "dag-json-vectors.json")) as fh:
            for v in json.load(fh):
                dagjson.loads(v["json"])

    def test_a_slash_map_that_is_neither_form_is_refused(self):
        with self.assertRaises(cas.GateError):
            dagjson.loads('{"/":5}')

    def test_selection_normalisation_merges_orders_and_matches_a_hand_built_block(self):
        request = dagjson.loads('{"kind":"Selection","members":["cas://%s/%s",{"item":{"address":%s,"via":[%s]}},{"selection":%s}],"derived_from":[]}'
                                % (C1, I1, link(I1), link(C2), link(I2)))
        block = dagjson.expected_selection(request, "gate")
        members = sorted([(I1, {"item": {"address": cas.Cid(I1), "via": [cas.Cid(c) for c in sorted([C1, C2])]}}),
                          (I2, {"selection": cas.Cid(I2)})])
        self.assertEqual(block, {"kind": "Selection", "schema": 1, "asserted_by": "gate",
                                 "members": [m for _k, m in members], "derived_from": []})
        self.assertEqual(dagjson.address(block), cas.cid_dagcbor(cas.encode(block)))

    def test_member_order_is_not_content(self):
        a = dagjson.loads('{"kind":"Selection","members":[{"item":{"address":%s,"via":[]}},{"item":{"address":%s,"via":[]}}],"derived_from":[]}' % (link(I1), link(I2)))
        b = dagjson.loads('{"kind":"Selection","members":[{"item":{"address":%s,"via":[]}},{"item":{"address":%s,"via":[]}}],"derived_from":[]}' % (link(I2), link(I1)))
        self.assertEqual(dagjson.address(dagjson.expected_selection(a, "gate")), dagjson.address(dagjson.expected_selection(b, "gate")))

    def test_claim_supersedes_sorted_and_deduplicated(self):
        request = dagjson.loads('{"kind":"Claim","subject":%s,"verb":"set","attribute":"name","value":"x","supersedes":[%s,%s,%s],"timestamp":"2026-09-25T10:00:00.000Z"}'
                                % (link(I1), link(C2), link(C1), link(C2)))
        block = dagjson.expected_claim(request, "gate")
        self.assertEqual(block["supersedes"], [cas.Cid(c) for c in sorted([C1, C2])])
        self.assertEqual(list(sorted(block)), sorted(["kind", "schema", "asserted_by", "subject", "verb", "attribute", "value", "supersedes", "timestamp"]))

    def test_claim_state_passes_the_shared_vectors(self):
        with open(os.path.join(FIXTURES, "claim-vectors.json")) as fh:
            for v in json.load(fh):
                with self.subTest(v["name"]):
                    s = dagjson.claim_state(v["claims"])
                    self.assertEqual(s["current"], v["expect"]["current"])
                    self.assertEqual(s["names"], v["expect"]["names"])
                    self.assertEqual(s["name_conflicted"], v["expect"]["nameConflicted"])
                    self.assertEqual(s["deletion"], v["expect"]["deletion"])


if __name__ == "__main__":
    unittest.main()
