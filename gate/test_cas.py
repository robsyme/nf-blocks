"""Unit tests for gate/cas.py.

Run with:  python3 -m unittest discover -s gate

These test the Gate's own CID encoder and DAG-CBOR decoder. The Gate never
trusts the plugin, so this module is the thing the whole harness stands on:
if it is wrong, every assertion is wrong.
"""

import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cas  # noqa: E402


RAW = 0x55
DAG_CBOR = 0x71

# Vectors fixed by DESIGN.md section 3, each paired with the digest it must
# encode. test_every_vector_encodes_the_digest_it_claims re-derives all three
# with hashlib, so a transcription error in either file cannot survive: an
# earlier revision of DESIGN carried a CID string for b"hello\n" that decoded
# to a digest nothing hashes to, and that is exactly the check that caught it.
VECTORS = [
    (b"", RAW, "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"),
    (b"hello\n", RAW, "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"),
    (bytes.fromhex("a0"), DAG_CBOR,
     "bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua"),
]
SHA256_HELLO_HEX = "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
SHA256_A0_HEX = "c19a797fa1fd590cd2e5b42d1cf5f246e29b91684e2f87404b81dc345c7a56a0"
CID_RAW_EMPTY = VECTORS[0][2]
CID_RAW_HELLO = VECTORS[1][2]
CID_DAGCBOR_EMPTY_MAP = "bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua"


class TestCidEncoder(unittest.TestCase):
    def test_raw_cid_of_empty_input(self):
        self.assertEqual(cas.cid_raw(b""), CID_RAW_EMPTY)

    def test_raw_cid_of_hello(self):
        self.assertEqual(hashlib.sha256(b"hello\n").hexdigest(), SHA256_HELLO_HEX)
        self.assertEqual(cas.cid_raw(b"hello\n"), CID_RAW_HELLO)

    def test_every_vector_encodes_the_digest_it_claims(self):
        """A CID text form nothing hashes to is a transcription error."""
        for data, codec, text in VECTORS:
            digest = hashlib.sha256(data).digest()
            self.assertEqual(cas.cid_digest(text), digest,
                             "%r: the CID decodes to a digest that is not "
                             "sha256(%r)" % (text, data))
            self.assertEqual(cas.cid_codec(text), codec, text)
            self.assertEqual(cas.cid_from_sha256(digest, codec), text)

    def test_designs_parenthetical_digests(self):
        self.assertEqual(hashlib.sha256(b"hello\n").hexdigest(), SHA256_HELLO_HEX)
        self.assertEqual(hashlib.sha256(bytes.fromhex("a0")).hexdigest(),
                         SHA256_A0_HEX)

    def test_dagcbor_cid_of_empty_map(self):
        self.assertEqual(cas.cid_dagcbor(bytes.fromhex("a0")), CID_DAGCBOR_EMPTY_MAP)

    def test_cid_from_sha256_matches_cid_raw(self):
        digest = hashlib.sha256(b"hello\n").digest()
        self.assertEqual(cas.cid_from_sha256(digest, RAW), CID_RAW_HELLO)

    def test_cid_from_sha256_rejects_wrong_digest_length(self):
        with self.assertRaises(ValueError):
            cas.cid_from_sha256(b"\x00" * 31, RAW)

    def test_cid_text_is_59_characters(self):
        self.assertEqual(len(CID_RAW_EMPTY), 59)
        self.assertEqual(len(cas.cid_raw(b"anything")), 59)

    def test_cid_codec_reports_raw_and_dagcbor(self):
        self.assertEqual(cas.cid_codec(CID_RAW_EMPTY), RAW)
        self.assertEqual(cas.cid_codec(CID_DAGCBOR_EMPTY_MAP), DAG_CBOR)

    def test_cid_digest_round_trips(self):
        digest = hashlib.sha256(b"hello\n").digest()
        self.assertEqual(cas.cid_digest(CID_RAW_HELLO), digest)

    def test_parse_rejects_non_cid_text(self):
        for bad in ["lab", "", "bafk", "zzzz", "lid://abc",
                    CID_RAW_EMPTY.upper(), CID_RAW_EMPTY + "="]:
            self.assertFalse(cas.is_cid(bad), bad)
        self.assertTrue(cas.is_cid(CID_RAW_EMPTY))

    def test_rejects_non_canonical_base32_padding_bits(self):
        """The three neighbours of a valid CID decode to the same 36 bytes.

        base32 leaves two spare bits in the last character of a 58-character
        body; only the form whose spare bits are zero is the CID.
        """
        self.assertTrue(cas.is_cid(CID_RAW_HELLO))
        for last in "nop":
            neighbour = CID_RAW_HELLO[:-1] + last
            self.assertFalse(cas.is_cid(neighbour), neighbour)

    def test_rejects_a_non_minimal_codec_varint(self):
        # codec 0x55 written as the two-byte varint d5 00
        binary = bytes([0x01, 0xD5, 0x00, 0x12, 0x20]) + b"\x00" * 32
        text = "b" + __import__("base64").b32encode(binary).decode().lower().rstrip("=")
        self.assertFalse(cas.is_cid(text))


class TestDagCborDecoder(unittest.TestCase):
    def test_decodes_map_with_nested_list(self):
        data = bytes.fromhex("a2616101616283f5f66178")
        self.assertEqual(cas.decode(data), {"a": 1, "b": [True, None, "x"]})

    def test_decodes_empty_map(self):
        self.assertEqual(cas.decode(bytes.fromhex("a0")), {})

    def test_decodes_integers_as_int(self):
        self.assertIsInstance(cas.decode(bytes.fromhex("01")), int)
        self.assertEqual(cas.decode(bytes.fromhex("1903e8")), 1000)
        self.assertEqual(cas.decode(bytes.fromhex("20")), -1)
        self.assertEqual(cas.decode(bytes.fromhex("3903e7")), -1000)

    def test_decodes_byte_string_as_bytes(self):
        value = cas.decode(bytes.fromhex("43010203"))
        self.assertIsInstance(value, bytes)
        self.assertEqual(value, b"\x01\x02\x03")

    def test_decodes_double_float(self):
        self.assertEqual(cas.decode(bytes.fromhex("fb3ff0000000000000")), 1.0)

    def test_rejects_half_and_single_precision_floats(self):
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("f93c00"))
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("fa3f800000"))

    def test_decodes_tag_42_as_cid_link(self):
        digest = hashlib.sha256(b"").digest()
        binary = bytes([0x01, 0x55, 0x12, 0x20]) + digest
        data = bytes([0xD8, 0x2A, 0x58, len(binary) + 1, 0x00]) + binary
        link = cas.decode(data)
        self.assertIsInstance(link, cas.Cid)
        self.assertEqual(link.text, CID_RAW_EMPTY)
        self.assertEqual(str(link), CID_RAW_EMPTY)

    def test_rejects_tag_other_than_42(self):
        digest = hashlib.sha256(b"").digest()
        binary = bytes([0x01, 0x55, 0x12, 0x20]) + digest
        data = bytes([0xD8, 0x2B, 0x58, len(binary) + 1, 0x00]) + binary
        with self.assertRaises(cas.DagCborError):
            cas.decode(data)

    def test_rejects_indefinite_length_array(self):
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("9f01ff"))

    def test_rejects_indefinite_length_map(self):
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("bf6161 01 ff".replace(" ", "")))

    def test_rejects_indefinite_length_text(self):
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("7f61616161ff"))

    def test_rejects_undefined(self):
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("f7"))

    def test_rejects_non_string_map_key(self):
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("a10101"))

    def test_rejects_reserved_slash_key(self):
        # {"/": 1}
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("a1612f01"))

    def test_rejects_trailing_bytes(self):
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("a000"))

    def test_rejects_non_minimal_integer_encoding(self):
        # 1 encoded in a two-byte form.
        with self.assertRaises(cas.DagCborError):
            cas.decode(bytes.fromhex("1801"))

    def test_rejects_non_canonical_map_key_order(self):
        # {"bb": 1, "a": 2}: longer key first violates length-then-bytewise.
        with self.assertRaises(cas.DagCborOrderError):
            cas.decode(bytes.fromhex("a262626201616102"))

    def test_accepts_canonical_map_key_order(self):
        # {"a": 2, "bb": 1}
        self.assertEqual(cas.decode(bytes.fromhex("a2616102626262 01".replace(" ", ""))),
                         {"a": 2, "bb": 1})


class TestDagCborEncoder(unittest.TestCase):
    """encode() exists so gate/fixtures/make_fixture.py can build a store."""

    def test_encodes_empty_map_to_a0(self):
        self.assertEqual(cas.encode({}), bytes.fromhex("a0"))

    def test_encode_sorts_keys_canonically(self):
        self.assertEqual(cas.encode({"bb": 1, "a": 2}),
                         bytes.fromhex("a2616102626262" + "01"))

    def test_round_trips_a_record_shape(self):
        value = {
            "kind": "OutputItem",
            "schema": 1,
            "value": {"sample": "A", "ok": True, "n": None, "xs": [1, 2]},
        }
        self.assertEqual(cas.decode(cas.encode(value)), value)

    def test_round_trips_a_cid_link(self):
        link = cas.Cid(CID_RAW_HELLO)
        out = cas.decode(cas.encode({"run": link}))
        self.assertEqual(out["run"].text, CID_RAW_HELLO)

    def test_encoded_cid_matches_cid_dagcbor(self):
        self.assertEqual(cas.cid_dagcbor(cas.encode({})), CID_DAGCBOR_EMPTY_MAP)


class TestStore(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="gate-store-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = cas.Store(self.root)

    def _put_raw(self, data):
        cid = cas.cid_raw(data)
        path = os.path.join(self.root, "blocks", cid[-2:])
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, cid), "wb") as fh:
            fh.write(data)
        return cid

    def _put_dagcbor(self, value):
        data = cas.encode(value)
        cid = cas.cid_dagcbor(data)
        path = os.path.join(self.root, "blocks", cid[-2:])
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, cid), "wb") as fh:
            fh.write(data)
        return cid

    def test_read_returns_the_stored_bytes(self):
        cid = self._put_raw(b"hello\n")
        self.assertEqual(self.store.read(cid), b"hello\n")

    def test_read_verifies_the_address(self):
        cid = self._put_raw(b"hello\n")
        with open(os.path.join(self.root, "blocks", cid[-2:], cid), "wb") as fh:
            fh.write(b"tampered\n")
        with self.assertRaises(cas.StoreError):
            self.store.read(cid)

    def test_blocks_lists_raw_and_dagcbor_separately(self):
        raw = self._put_raw(b"hello\n")
        meta = self._put_dagcbor({"kind": "OutputItem", "schema": 1})
        self.assertEqual([c for c, _ in self.store.blocks("raw")], [raw])
        self.assertEqual([c for c, _ in self.store.blocks("dagcbor")], [meta])

    def test_blocks_returns_existing_paths(self):
        self._put_raw(b"hello\n")
        for _cid, path in self.store.blocks("raw"):
            self.assertTrue(os.path.isfile(path))

    def test_blocks_on_missing_store_is_empty(self):
        empty = cas.Store(os.path.join(self.root, "nope"))
        self.assertEqual(list(empty.blocks("raw")), [])

    def test_read_block_decodes_dagcbor(self):
        cid = self._put_dagcbor({"kind": "RunManifest", "schema": 1})
        self.assertEqual(self.store.read_block(cid),
                         {"kind": "RunManifest", "schema": 1})

    def test_run_log_is_newest_first(self):
        runs = os.path.join(self.root, "runs")
        os.makedirs(runs)
        older = "%013d" % (9999999999999 - 1000)
        newer = "%013d" % (9999999999999 - 5000)
        # bigger reverse timestamp = older finish time
        open(os.path.join(runs, newer + "-" + CID_DAGCBOR_EMPTY_MAP), "w").close()
        open(os.path.join(runs, older + "-" + CID_RAW_EMPTY), "w").close()
        log = self.store.run_log()
        self.assertEqual([cid for _rts, cid in log],
                         [CID_DAGCBOR_EMPTY_MAP, CID_RAW_EMPTY])

    def test_run_log_on_missing_directory_is_empty(self):
        self.assertEqual(self.store.run_log(), [])

    def test_nf_records_reads_data_json_by_kind(self):
        key = "abc123/aligned/A/A.bam"
        d = os.path.join(self.root, "nf", key)
        os.makedirs(d)
        with open(os.path.join(d, ".data.json"), "w") as fh:
            fh.write('{"version":"lineage/v1beta1","kind":"FileOutput",'
                     '"spec":{"size":22}}')
        records = self.store.nf_records("FileOutput")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0][0], key)
        self.assertEqual(records[0][1]["size"], 22)

    def test_nf_records_filters_by_kind(self):
        for key, kind in (("k1", "FileOutput"), ("k2", "TaskRun")):
            d = os.path.join(self.root, "nf", key)
            os.makedirs(d)
            with open(os.path.join(d, ".data.json"), "w") as fh:
                fh.write('{"kind":"%s","spec":{}}' % kind)
        self.assertEqual([k for k, _ in self.store.nf_records("TaskRun")], ["k2"])

    def test_nf_records_on_missing_directory_is_empty(self):
        self.assertEqual(self.store.nf_records("TaskRun"), [])

    def test_coords_pointer_reads_the_leaf_line(self):
        d = os.path.join(self.root, "coords", "aligned", "A")
        os.makedirs(d)
        with open(os.path.join(d, "A.bam"), "w") as fh:
            fh.write("cas://%s/A.bam\n" % CID_RAW_HELLO)
        self.assertEqual(self.store.coords_pointer("aligned/A/A.bam"),
                         "cas://%s/A.bam" % CID_RAW_HELLO)

    def test_coords_pointer_returns_none_when_absent(self):
        self.assertIsNone(self.store.coords_pointer("aligned/A/A.bam"))


class TestIndex(unittest.TestCase):
    def setUp(self):
        self.cache = tempfile.mkdtemp(prefix="gate-cache-")
        self.addCleanup(shutil.rmtree, self.cache, True)
        self.dir = os.path.join(self.cache, "nf-blocks")
        os.makedirs(self.dir)

    def _make_db(self, name):
        path = os.path.join(self.dir, name)
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE producer (content_cid TEXT, item_cid TEXT, "
                    "collection_cid TEXT, completion_cid TEXT, filename TEXT)")
        con.commit()
        con.close()
        return path

    def test_locate_finds_the_single_sqlite_file(self):
        path = self._make_db("aabbccdd11223344.sqlite")
        self.assertEqual(cas.Index.locate(self.cache), path)

    def test_locate_fails_clearly_when_absent(self):
        with self.assertRaises(cas.IndexLocateError) as ctx:
            cas.Index.locate(self.cache)
        self.assertIn("no", str(ctx.exception).lower())

    def test_locate_fails_clearly_when_several(self):
        self._make_db("one.sqlite")
        self._make_db("two.sqlite")
        with self.assertRaises(cas.IndexLocateError) as ctx:
            cas.Index.locate(self.cache)
        self.assertIn("2", str(ctx.exception))

    def test_query_returns_rows_as_dicts(self):
        path = self._make_db("one.sqlite")
        con = sqlite3.connect(path)
        con.execute("INSERT INTO producer VALUES ('c1','i1','col1','run1','A.bam')")
        con.commit()
        con.close()
        index = cas.Index(path)
        rows = index.query("SELECT * FROM producer WHERE content_cid = ?", ("c1",))
        self.assertEqual(rows[0]["filename"], "A.bam")

    def test_has_table_reports_missing_tables(self):
        index = cas.Index(self._make_db("one.sqlite"))
        self.assertTrue(index.has_table("producer"))
        self.assertFalse(index.has_table("run"))


if __name__ == "__main__":
    unittest.main()
