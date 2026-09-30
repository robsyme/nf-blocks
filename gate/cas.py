"""The Gate's own CID encoder, DAG-CBOR codec and store reader.

Python 3 standard library only. Nothing here calls the plugin or reads a value
the plugin computed and believes it: addresses are derived from bytes with
hashlib, and every block read is verified against the address it was filed
under. See DESIGN.md sections 3, 4, 5, 6, 7 and 12.
"""

import base64
import glob
import hashlib
import json
import os
import sqlite3
import struct

RAW = 0x55
DAG_CBOR = 0x71
SHA2_256 = 0x12

_MULTIHASH_PREFIX = bytes([SHA2_256, 0x20])


class GateError(Exception):
    pass


class DagCborError(GateError):
    pass


class DagCborOrderError(DagCborError):
    """Map keys are not in canonical DAG-CBOR order (length, then bytewise)."""


class StoreError(GateError):
    pass


class IndexLocateError(GateError):
    """The one expected SQLite index could not be found."""


# --------------------------------------------------------------------------
# CIDv1, multibase base32 lower, sha2-256
# --------------------------------------------------------------------------

def _varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _read_varint(data, pos):
    """Unsigned LEB128, rejecting any non-minimal encoding."""
    value = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise GateError("truncated varint")
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            if shift and byte == 0x00:
                raise GateError("non-minimal varint: trailing zero group")
            return value, pos
        shift += 7
        if shift > 63:
            raise GateError("varint too long")


def _b32_encode(data):
    return "b" + base64.b32encode(data).decode("ascii").lower().rstrip("=")


def _b32_decode(text):
    """Decode multibase base32 lower, no padding, rejecting a non-canonical form.

    base64.b32decode silently discards the bits left over in the final
    character, so `bafkrei…v6am`, `…v6an`, `…v6ao` and `…v6ap` would all decode
    to the same bytes. Re-encoding and comparing is what rules the other three
    out: only the form whose trailing bits are zero survives.
    """
    if not text or text[0] != "b":
        raise GateError("not multibase base32: %r" % (text,))
    body = text[1:]
    if not body or body.strip("abcdefghijklmnopqrstuvwxyz234567"):
        raise GateError("not base32 lower: %r" % (text,))
    padded = body.upper() + "=" * (-len(body) % 8)
    data = base64.b32decode(padded)
    if _b32_encode(data) != text:
        raise GateError("non-canonical base32 (non-zero padding bits): %r" % (text,))
    return data


def cid_from_sha256(digest, codec):
    """CIDv1 text form for a sha2-256 digest under the given multicodec."""
    if len(digest) != 32:
        raise ValueError("sha2-256 digest must be 32 bytes, got %d" % len(digest))
    return _b32_encode(b"\x01" + _varint(codec) + _MULTIHASH_PREFIX + digest)


def cid_raw(data):
    return cid_from_sha256(hashlib.sha256(data).digest(), RAW)


def cid_dagcbor(data):
    return cid_from_sha256(hashlib.sha256(data).digest(), DAG_CBOR)


def cid_of_file(path, chunk=1 << 20):
    """Raw CID of a file's bytes, streamed."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                break
            h.update(block)
    return cid_from_sha256(h.digest(), RAW)


def sha256_of_file(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def _parse_cid(text):
    binary = _b32_decode(text)
    if len(binary) < 4 or binary[0] != 0x01:
        raise GateError("not a CIDv1: %r" % (text,))
    codec, pos = _read_varint(binary, 1)
    mh_code, pos = _read_varint(binary, pos)
    mh_len, pos = _read_varint(binary, pos)
    if mh_code != SHA2_256 or mh_len != 32:
        raise GateError("not sha2-256/32 multihash: %r" % (text,))
    digest = binary[pos:]
    if len(digest) != 32:
        raise GateError("multihash digest length mismatch: %r" % (text,))
    return codec, digest


def cid_codec(text):
    return _parse_cid(text)[0]


def cid_digest(text):
    return _parse_cid(text)[1]


def is_cid(text):
    if not isinstance(text, str):
        return False
    try:
        _parse_cid(text)
        return True
    except Exception:
        return False


class Cid(object):
    """A DAG-CBOR link (tag 42), addressed by its base32 text form."""

    __slots__ = ("text",)

    def __init__(self, text):
        if not is_cid(text):
            raise GateError("not a CID: %r" % (text,))
        self.text = text

    @property
    def codec(self):
        return cid_codec(self.text)

    def binary(self):
        return _b32_decode(self.text)

    def __eq__(self, other):
        return isinstance(other, Cid) and other.text == self.text

    def __hash__(self):
        return hash(self.text)

    def __str__(self):
        return self.text

    def __repr__(self):
        return "Cid(%r)" % (self.text,)


# --------------------------------------------------------------------------
# DAG-CBOR
# --------------------------------------------------------------------------

def _canonical_key(key):
    encoded = key.encode("utf-8")
    return (len(encoded), encoded)


class _Decoder(object):
    def __init__(self, data, check_order):
        self.data = data
        self.pos = 0
        self.check_order = check_order

    def take(self, n):
        if self.pos + n > len(self.data):
            raise DagCborError("truncated input at offset %d" % self.pos)
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out

    def head(self):
        byte = self.take(1)[0]
        return byte >> 5, byte & 0x1F

    def argument(self, ai):
        """Definite-length argument with minimal-width enforcement."""
        if ai < 24:
            return ai
        if ai == 24:
            value = self.take(1)[0]
            if value < 24:
                raise DagCborError("non-minimal integer encoding of %d" % value)
            return value
        if ai == 25:
            value = struct.unpack(">H", self.take(2))[0]
            if value <= 0xFF:
                raise DagCborError("non-minimal integer encoding of %d" % value)
            return value
        if ai == 26:
            value = struct.unpack(">I", self.take(4))[0]
            if value <= 0xFFFF:
                raise DagCborError("non-minimal integer encoding of %d" % value)
            return value
        if ai == 27:
            value = struct.unpack(">Q", self.take(8))[0]
            if value <= 0xFFFFFFFF:
                raise DagCborError("non-minimal integer encoding of %d" % value)
            return value
        if ai == 31:
            raise DagCborError("indefinite length is not valid DAG-CBOR")
        raise DagCborError("reserved additional information %d" % ai)

    def value(self):
        major, ai = self.head()
        if major == 0:
            return self.argument(ai)
        if major == 1:
            return -1 - self.argument(ai)
        if major == 2:
            return bytes(self.take(self.argument(ai)))
        if major == 3:
            return self.take(self.argument(ai)).decode("utf-8")
        if major == 4:
            return [self.value() for _ in range(self.argument(ai))]
        if major == 5:
            return self.map(self.argument(ai))
        if major == 6:
            return self.tag(self.argument(ai))
        if major == 7:
            return self.simple(ai)
        raise DagCborError("unreachable major type %d" % major)

    def map(self, count):
        out = {}
        previous = None
        for _ in range(count):
            major, ai = self.head()
            if major != 3:
                raise DagCborError("map key is not a text string (major type %d)" % major)
            key = self.take(self.argument(ai)).decode("utf-8")
            if key == "/":
                raise DagCborError('reserved map key "/"')
            if key in out:
                raise DagCborError("duplicate map key %r" % key)
            if self.check_order and previous is not None:
                if _canonical_key(previous) >= _canonical_key(key):
                    raise DagCborOrderError(
                        "map keys out of canonical order: %r before %r" % (previous, key))
            previous = key
            out[key] = self.value()
        return out

    def tag(self, number):
        if number != 42:
            raise DagCborError("tag %d is not valid DAG-CBOR (only 42)" % number)
        major, ai = self.head()
        if major != 2:
            raise DagCborError("tag 42 content is not a byte string")
        binary = self.take(self.argument(ai))
        if not binary or binary[0] != 0x00:
            raise DagCborError("tag 42 content lacks the 0x00 multibase prefix")
        return Cid(_b32_encode(binary[1:]))

    def simple(self, ai):
        if ai == 20:
            return False
        if ai == 21:
            return True
        if ai == 22:
            return None
        if ai == 27:
            value = struct.unpack(">d", self.take(8))[0]
            if value != value or value in (float("inf"), float("-inf")):
                raise DagCborError("NaN and Infinity are not valid DAG-CBOR")
            return value
        if ai in (25, 26):
            raise DagCborError("only 64-bit floats are valid DAG-CBOR")
        raise DagCborError("simple value %d is not valid DAG-CBOR" % ai)


def decode(data, check_order=True):
    """Strict DAG-CBOR decode. Raises DagCborError on anything non-canonical."""
    if not isinstance(data, (bytes, bytearray)):
        raise DagCborError("decode expects bytes, got %s" % type(data).__name__)
    decoder = _Decoder(bytes(data), check_order)
    value = decoder.value()
    if decoder.pos != len(data):
        raise DagCborError("%d trailing byte(s) after the top-level value"
                           % (len(data) - decoder.pos))
    return value


def _head(major, argument):
    if argument < 24:
        return bytes([(major << 5) | argument])
    if argument <= 0xFF:
        return bytes([(major << 5) | 24, argument])
    if argument <= 0xFFFF:
        return bytes([(major << 5) | 25]) + struct.pack(">H", argument)
    if argument <= 0xFFFFFFFF:
        return bytes([(major << 5) | 26]) + struct.pack(">I", argument)
    if argument <= 0xFFFFFFFFFFFFFFFF:
        return bytes([(major << 5) | 27]) + struct.pack(">Q", argument)
    raise DagCborError("integer out of range: %d" % argument)


def encode(value):
    """Canonical DAG-CBOR encode. Used by gate/fixtures/make_fixture.py."""
    if value is None:
        return b"\xf6"
    if value is True:
        return b"\xf5"
    if value is False:
        return b"\xf4"
    if isinstance(value, Cid):
        binary = b"\x00" + value.binary()
        return b"\xd8\x2a" + _head(2, len(binary)) + binary
    if isinstance(value, int):
        if value >= 0:
            return _head(0, value)
        return _head(1, -1 - value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise DagCborError("NaN and Infinity are not valid DAG-CBOR")
        return b"\xfb" + struct.pack(">d", value)
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        return _head(3, len(encoded)) + encoded
    if isinstance(value, (bytes, bytearray)):
        return _head(2, len(value)) + bytes(value)
    if isinstance(value, (list, tuple)):
        return _head(4, len(value)) + b"".join(encode(v) for v in value)
    if isinstance(value, dict):
        keys = sorted(value.keys(), key=_canonical_key)
        out = [_head(5, len(keys))]
        for key in keys:
            if not isinstance(key, str):
                raise DagCborError("map key is not a string: %r" % (key,))
            if key == "/":
                raise DagCborError('reserved map key "/"')
            encoded = key.encode("utf-8")
            out.append(_head(3, len(encoded)) + encoded)
            out.append(encode(value[key]))
        return b"".join(out)
    raise DagCborError("cannot encode %s" % type(value).__name__)


# --------------------------------------------------------------------------
# The block store on disk (DESIGN section 5)
# --------------------------------------------------------------------------

class Store(object):
    """Read-only view of a store directory. Every read verifies the address."""

    def __init__(self, root):
        self.root = os.path.abspath(str(root))

    def path(self, *parts):
        return os.path.join(self.root, *parts)

    def block_path(self, cid):
        return os.path.join(self.root, "blocks", cid[-2:], cid)

    def has(self, cid):
        return os.path.isfile(self.block_path(cid))

    def blocks(self, kind=None):
        """Yield (cid, path) for every block, optionally filtered by codec.

        kind is 'raw', 'dagcbor', or None for everything. Files whose name is
        not a CID (temp files, stray junk) are skipped.
        """
        want = {"raw": RAW, "dagcbor": DAG_CBOR, None: None}[kind]
        root = os.path.join(self.root, "blocks")
        if not os.path.isdir(root):
            return
        for shard in sorted(os.listdir(root)):
            shard_dir = os.path.join(root, shard)
            if not os.path.isdir(shard_dir):
                continue
            for name in sorted(os.listdir(shard_dir)):
                if not is_cid(name):
                    continue
                if want is not None and cid_codec(name) != want:
                    continue
                yield name, os.path.join(shard_dir, name)

    def read(self, cid):
        path = self.block_path(cid)
        if not os.path.isfile(path):
            raise StoreError("no block %s at %s" % (cid, path))
        with open(path, "rb") as fh:
            data = fh.read()
        actual = cid_from_sha256(hashlib.sha256(data).digest(), cid_codec(cid))
        if actual != cid:
            raise StoreError("block %s at %s hashes to %s" % (cid, path, actual))
        return data

    def read_block(self, cid):
        if cid_codec(cid) != DAG_CBOR:
            raise StoreError("%s is not a dag-cbor block" % cid)
        return decode(self.read(cid))

    def metadata_blocks_of_runs(self, completions):
        """Sorted block paths of each RunCompletion, its RunManifest and its
        OutputCollections, found by following the completion's own `run` and
        `collections` links. Every block is read and verified on the way."""
        out = set()
        for cid in completions:
            completion = self.read_block(cid)
            if not isinstance(completion, dict) or completion.get("kind") != "RunCompletion":
                raise StoreError("%s is not a RunCompletion" % cid)
            out.add(self.block_path(cid))
            links = [completion.get("run")] + list(completion.get("collections") or [])
            for link in links:
                if isinstance(link, Cid):
                    self.read(link.text)
                    out.add(self.block_path(link.text))
        return sorted(out)

    STORE_LOG_KINDS = ("run", "selection", "claim")

    def store_log(self):
        """[(reverse_ts, kind, cid)] from log/<rts>-<kind>-<cid>, newest first.
        Unknown kinds and malformed names are ignored, as the plugin does."""
        root = os.path.join(self.root, "log")
        if not os.path.isdir(root):
            return []
        out = []
        for name in sorted(os.listdir(root)):
            parts = name.split("-", 2)
            if len(parts) != 3 or len(parts[0]) != 13 or not parts[0].isdigit():
                continue
            rts, kind, cid = parts
            if kind not in self.STORE_LOG_KINDS or not is_cid(cid):
                continue
            out.append((rts, kind, cid))
        return out

    def nf_records(self, kind=None):
        """[(key, spec)] from nf/**/.data.json, key relative to nf/."""
        root = os.path.join(self.root, "nf")
        if not os.path.isdir(root):
            return []
        out = []
        for dirpath, _dirnames, filenames in os.walk(root):
            if ".data.json" not in filenames:
                continue
            key = os.path.relpath(dirpath, root)
            with open(os.path.join(dirpath, ".data.json")) as fh:
                envelope = json.load(fh)
            if kind is not None and envelope.get("kind") != kind:
                continue
            out.append((key, envelope.get("spec") or {}))
        out.sort(key=lambda pair: pair[0])
        return out

    def nf_envelopes(self):
        """[(key, envelope)] for every nf/**/.data.json, kind included."""
        root = os.path.join(self.root, "nf")
        if not os.path.isdir(root):
            return []
        out = []
        for dirpath, _dirnames, filenames in os.walk(root):
            if ".data.json" not in filenames:
                continue
            with open(os.path.join(dirpath, ".data.json")) as fh:
                out.append((os.path.relpath(dirpath, root), json.load(fh)))
        out.sort(key=lambda pair: pair[0])
        return out

    def coords_pointer(self, rel_path):
        """The single Store URI line of coords/<rel_path>, or None."""
        path = os.path.join(self.root, "coords", *rel_path.strip("/").split("/"))
        if not os.path.isfile(path):
            return None
        with open(path) as fh:
            return fh.read().strip()

    def coords_paths(self):
        """Relative paths of every leaf pointer file under coords/."""
        root = os.path.join(self.root, "coords")
        if not os.path.isdir(root):
            return []
        out = []
        for dirpath, _dirnames, filenames in os.walk(root):
            for name in filenames:
                full = os.path.join(dirpath, name)
                out.append(os.path.relpath(full, root).replace(os.sep, "/"))
        return sorted(out)


# --------------------------------------------------------------------------
# The SQLite index (DESIGN section 12)
# --------------------------------------------------------------------------

class Index(object):
    def __init__(self, path):
        self.path = str(path)
        self.con = sqlite3.connect("file:%s?mode=ro" % self.path, uri=True)
        self.con.row_factory = sqlite3.Row

    @staticmethod
    def _sqlite_files(cache_home):
        pattern = os.path.join(str(cache_home), "nf-blocks", "*.sqlite")
        return pattern, sorted(glob.glob(pattern))

    @staticmethod
    def locate(cache_home):
        """The one *.sqlite under <cache_home>/nf-blocks. Fails clearly otherwise."""
        pattern, found = Index._sqlite_files(cache_home)
        if not found:
            raise IndexLocateError("no index: nothing matches %s" % pattern)
        if len(found) > 1:
            raise IndexLocateError("expected exactly 1 index, found %d matching %s: %s"
                             % (len(found), pattern, ", ".join(found)))
        return found[0]

    @staticmethod
    def locate_for_pipeline(cache_home, pipeline, exclude=None):
        """The one index whose run table has `pipeline` but not `exclude`.

        A composite store keeps its own per-composition index, so
        <cache_home>/nf-blocks can hold several. The producer store's index and
        the consumer's composite [out, lab] index both carry the producer's
        runs (the consumer ingests the read-only member's Store Log), so the
        pipeline column alone no longer disambiguates. The producer-only index
        is the one carrying `pipeline` and NOT the consumer's `exclude`
        pipeline.
        """
        pattern, found = Index._sqlite_files(cache_home)
        if not found:
            raise IndexLocateError("no index: nothing matches %s" % pattern)

        def pipelines_of(path):
            con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
            try:
                return {row[0] for row in con.execute("SELECT DISTINCT pipeline "
                                                      "FROM run")}
            except sqlite3.Error:
                return None                # no run table, or unreadable
            finally:
                con.close()

        matches = []
        for path in found:
            pipelines = pipelines_of(path)
            if pipelines is None or pipeline not in pipelines:
                continue
            if exclude is not None and exclude in pipelines:
                continue
            matches.append(path)
        if not matches:
            note = "" if exclude is None else " and not %r" % exclude
            raise IndexLocateError(
                "no index under %s has a run table with pipeline %r%s "
                "(scanned %d file(s): %s)"
                % (os.path.dirname(pattern), pipeline, note, len(found),
                   ", ".join(os.path.basename(p) for p in found)))
        if len(matches) > 1:
            raise IndexLocateError(
                "expected exactly 1 index for pipeline %r, found %d: %s"
                % (pipeline, len(matches), ", ".join(matches)))
        return matches[0]

    @staticmethod
    def open(cache_home):
        return Index(Index.locate(cache_home))

    @staticmethod
    def open_for_pipeline(cache_home, pipeline, exclude=None):
        return Index(Index.locate_for_pipeline(cache_home, pipeline, exclude))

    def query(self, sql, params=()):
        return [dict(row) for row in self.con.execute(sql, params).fetchall()]

    def scalar(self, sql, params=()):
        row = self.con.execute(sql, params).fetchone()
        return None if row is None else row[0]

    def tables(self):
        return sorted(row[0] for row in self.con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"))

    def has_table(self, name):
        return name in self.tables()

    def close(self):
        self.con.close()


# --------------------------------------------------------------------------
# Retention closures, shared by Gate assertion 9 (a local member, cas.Store)
# and tier two's T7 (an S3 member, gate/tier2/s3gate.Blocks). `store` is
# anything with read(cid) (verified bytes), read_block(cid) (a verified,
# decoded dag-cbor block) and store_log() ([(rts, kind, cid)], newest first).
# --------------------------------------------------------------------------

def leaves(value):
    """Every Leaf map in an item value, depth first."""
    if isinstance(value, dict):
        if value.get("kind") == "Leaf":
            yield value
            return
        for element in value.values():
            for leaf in leaves(element):
                yield leaf
    elif isinstance(value, list):
        for element in value:
            for leaf in leaves(element):
                yield leaf


def run_items(store, completion_block):
    """[(item cid, item block)] of every collection of a run."""
    out = []
    for link in completion_block.get("collections") or []:
        collection = store.read_block(link.text)
        for item in collection.get("items") or []:
            if isinstance(item, Cid):
                out.append((item.text, store.read_block(item.text)))
    return out


def _content_under(store, address, out):
    """`address` and, for a DirectoryManifest, everything under it."""
    if address in out:
        return
    out.add(address)
    if cid_codec(address) != DAG_CBOR:
        store.read(address)                       # re-hashes the bytes
        return
    block = store.read_block(address)
    if isinstance(block, dict) and block.get("kind") == "DirectoryManifest":
        for e in block.get("entries") or []:
            if isinstance(e, dict) and isinstance(e.get("address"), Cid):
                _content_under(store, e["address"].text, out)


def content_closure(store, completion_block, only_items=None):
    """Every content address under a run (or under only_items): each item's Leaf
    addresses, and for a DirectoryManifest leaf, the manifest and everything
    under it; with no only_items, each collection's index leaf too. Every
    block is read through store.read, which re-hashes it."""
    out = set()
    for cid, item in run_items(store, completion_block):
        if only_items is not None and cid not in only_items:
            continue
        for leaf in leaves(item.get("value")):
            if isinstance(leaf.get("address"), Cid):
                _content_under(store, leaf["address"].text, out)
    if only_items is None:
        for link in completion_block.get("collections") or []:
            index = (store.read_block(link.text) or {}).get("index")
            if isinstance(index, dict) and isinstance((index.get("leaf") or {}).get("address"), Cid):
                _content_under(store, index["leaf"]["address"].text, out)
    return out


def metadata_closure(store, completion_cid):
    """The RunCompletion, its RunManifest and script, its collections and items."""
    completion = store.read_block(completion_cid)
    out = {completion_cid}
    run = completion.get("run")
    if isinstance(run, Cid):
        out.add(run.text)
        script = store.read_block(run.text).get("script")
        if isinstance(script, Cid):
            store.read(script.text)
            out.add(script.text)
    for link in completion.get("collections") or []:
        out.add(link.text)
    out.update(cid for cid, _item in run_items(store, completion))
    return out


def item_by_leaf_name(store, completion_block, name):
    found = [cid for cid, item in run_items(store, completion_block)
             if any(leaf.get("name") == name for leaf in leaves(item.get("value")))]
    if len(found) != 1:
        raise GateError("expected one item with a Leaf named %s, found %d" % (name, len(found)))
    return found[0]


def retention_closures(store, runs):
    """Everything assertion 9 and T7 expect of gate/retention's two runs,
    computed from the blocks themselves. runs is {'a': (cid, block), 'b': (cid, block)}
    (the --tag a and --tag b runs; b is the one prune releases)."""
    (a_cid, a_block), (b_cid, b_block) = runs["a"], runs["b"]
    pinned = item_by_leaf_name(store, b_block, "pin_b.txt")
    dir_item = item_by_leaf_name(store, b_block, "dir_b")
    dir_leaf = [leaf for leaf in leaves(store.read_block(dir_item).get("value"))
                if leaf.get("name") == "dir_b"][0]
    manifest = store.read_block(dir_leaf["address"].text)
    ones = [e["address"].text for e in manifest.get("entries") or []
            if e.get("name") == "one.txt" and isinstance(e.get("address"), Cid)]
    if len(ones) != 1:
        raise GateError("dir_b's DirectoryManifest has no one.txt entry")
    return {
        "runs": {"a": a_cid, "b": b_cid},
        "a_content": sorted(content_closure(store, a_block)),
        "b_content": sorted(content_closure(store, b_block)),
        "pinned_item": pinned,
        "pinned_content": sorted(content_closure(store, b_block, only_items={pinned})),
        "a_meta": sorted(metadata_closure(store, a_cid)),
        "b_meta": sorted(metadata_closure(store, b_cid)),
        "b_one": ones[0],
    }


def parse_claim(store, cid):
    """The Claim block at cid, or None when it is unreadable or not a Claim."""
    try:
        block = store.read_block(cid)
    except GateError:
        return None
    if not isinstance(block, dict) or block.get("kind") != "Claim":
        return None
    return block


def _link_text(value):
    return value.text if isinstance(value, Cid) else value


def retain_claims(store, subject):
    """[(rts, cid, block)] of every retain Claim on `subject` in the Store Log,
    oldest first."""
    out = []
    for rts, kind, cid in store.store_log():
        if kind != "claim":
            continue
        block = parse_claim(store, cid)
        if block and _link_text(block.get("subject")) == subject and block.get("attribute") == "retain":
            out.append((rts, cid, block))
    out.sort(key=lambda row: row[0], reverse=True)   # reverse timestamps: oldest first
    return out


def pin_claims(store, subject):
    """[cid] of every `add pin` Claim on `subject` in the Store Log."""
    return [cid for _rts, kind, cid in store.store_log() if kind == "claim"
            for block in [parse_claim(store, cid)]
            if block and _link_text(block.get("subject")) == subject
            and block.get("verb") == "add" and block.get("attribute") == "pin"]


def retention_refs(store, runs):
    """RET_A, RET_B, RET_PIN_ITEM, RET_RELEASE (the newest current `set retain`
    on b, what a restore supersedes) and RET_RESTORE (the newest `del retain`,
    what a second release supersedes)."""
    b_cid, b_block = runs["b"]
    claims = retain_claims(store, b_cid)
    superseded = {s.text for _r, _c, block in claims for s in block.get("supersedes") or []
                  if isinstance(s, Cid)}
    current_sets = [cid for _r, cid, block in claims
                    if cid not in superseded and block.get("verb") == "set"]
    dels = [cid for _r, cid, block in claims if block.get("verb") == "del"]
    return {"RET_A": runs["a"][0], "RET_B": b_cid,
            "RET_PIN_ITEM": item_by_leaf_name(store, b_block, "pin_b.txt"),
            "RET_RELEASE": (current_sets or [""])[-1], "RET_RESTORE": (dels or [""])[-1]}
