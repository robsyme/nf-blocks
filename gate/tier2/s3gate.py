"""Tier two's view of S3, independent of the plugin (rule 6): boto3 only.

    s3gate.py setup <run-id>            prints the bucket it created (us-east-1)
    s3gate.py teardown <run-id> <bucket> <work-prefix>
                                         refuses any bucket or prefix but the run id's own; a missing bucket is gone
    s3gate.py if-match <bucket>          prints {"status": ..., "body": ...} of the stale If-Match probe
    s3gate.py snapshot <bucket> <member> <file>
                                         downloads <member>/index/v3.sqlite to <file>, prints
                                         {"meta_runs": x-amz-meta-runs, "count": count(*) of its run table}
    s3gate.py live-put <bucket> <member> <session> <run name>
    s3gate.py live-delete <bucket> <member> <session>
                                         T7's fake Live Writer registration <member>/live/<session>; only
                                         in a tier-two run's bucket
Everything else is imported by tier2.sh's assertions.
"""
import email.utils, hashlib, json, os, re, sqlite3, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import cas  # noqa: E402

REGION = "us-east-1"
WORK_BUCKET = "scidev-playground-us-east-1"
# A run id as tier2.sh makes it: t2-<UTC date>-<UTC time>-<$RANDOM>. Only such a run's bucket and prefix are ever emptied.
RUN_ID = re.compile(r"^t2-[0-9]{8}-[0-9]{6}-[0-9]{1,5}$")
RUN_BUCKET = re.compile(r"^nf-blocks-t2-t2-[0-9]{8}-[0-9]{6}-[0-9]{1,5}$")


def client():
    import boto3
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "scidev")).client("s3", region_name=REGION)


def bucket_of(run_id):
    """The run's writable-member bucket, derived from its id."""
    return "nf-blocks-t2-%s" % run_id


def work_prefix_of(run_id):
    """The run's key prefix in the shared work bucket; batch.config's workDir is this plus work/."""
    return "robsyme/nf-blocks-gate/%s/" % run_id


def setup(run_id):
    s3 = client()
    bucket = bucket_of(run_id)
    s3.create_bucket(Bucket=bucket)   # us-east-1 takes no LocationConstraint
    try:
        s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={"Rules": [
            {"ID": "staging", "Filter": {"Prefix": ""}, "Status": "Enabled",
             "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}},
            {"ID": "tmp", "Filter": {"Prefix": "cas/tmp/"}, "Status": "Enabled", "Expiration": {"Days": 1}}]})
    except Exception:
        s3.delete_bucket(Bucket=bucket)   # tier2.sh's teardown would also remove it; this keeps setup's failure self-contained
        raise
    return bucket


def _keys(s3, bucket, prefix):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            yield o["Key"]


def _uploads(s3, bucket, prefix):
    for page in s3.get_paginator("list_multipart_uploads").paginate(Bucket=bucket, Prefix=prefix):
        for u in page.get("Uploads", []):
            yield u


def _empty(s3, bucket, prefix=""):
    """Abort open uploads, delete every object, then list again: anything left (a failed key, a late
    writer) raises, so a teardown never reports success over a non-empty prefix."""
    for u in list(_uploads(s3, bucket, prefix)):
        s3.abort_multipart_upload(Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"])
    keys = list(_keys(s3, bucket, prefix))
    for i in range(0, len(keys), 1000):
        resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
        errors = resp.get("Errors") or []
        if errors:
            raise IOError("s3://%s: %d key(s) not deleted, first %s: %s" % (
                bucket, len(errors), errors[0].get("Key"), errors[0].get("Message") or errors[0].get("Code")))
    left, uploads = list(_keys(s3, bucket, prefix)), list(_uploads(s3, bucket, prefix))
    if left or uploads:
        raise IOError("s3://%s/%s still holds %d object(s) (first %s) and %d open upload(s) after emptying"
                      % (bucket, prefix, len(left), left[0] if left else "-", len(uploads)))


def _code(exc):
    return (getattr(exc, "response", None) or {}).get("Error", {}).get("Code")


def teardown(run_id, bucket, work_prefix, s3=None):
    """Empty this run's work prefix and delete this run's bucket; refuse anything else.

    Both halves always run; a failure in either is loud (cloud.sh's rule). A bucket that does not
    exist (setup never ran, or failed) is already gone, which is success."""
    if not RUN_ID.match(run_id or ""):
        sys.stderr.write("tier two teardown: refusing: %r is not a tier-two run id\n" % (run_id,))
        return 1
    failed = False
    if bucket != bucket_of(run_id):
        sys.stderr.write("tier two teardown: refusing to delete bucket %r: run %s's is %s\n" % (bucket, run_id, bucket_of(run_id)))
        failed = True
    if work_prefix != work_prefix_of(run_id):
        sys.stderr.write("tier two teardown: refusing to empty s3://%s/%s: run %s's prefix is %s\n"
                         % (WORK_BUCKET, work_prefix, run_id, work_prefix_of(run_id)))
        failed = True
    if failed:
        return 1
    s3 = s3 or client()

    def work():
        _empty(s3, WORK_BUCKET, work_prefix)

    def member():
        try:
            _empty(s3, bucket)
            s3.delete_bucket(Bucket=bucket)
        except s3.exceptions.ClientError as exc:
            if _code(exc) != "NoSuchBucket":
                raise

    for step in (work, member):
        try:
            step()
        except Exception as exc:
            sys.stderr.write("tier two teardown: %s\n" % exc)
            failed = True
    return 1 if failed else 0


class Member(object):
    """A member in S3 read as gate/cas.Store reads a local one."""

    def __init__(self, s3, bucket, prefix):
        self.s3, self.bucket, self.prefix = s3, bucket, prefix.rstrip("/") + "/" if prefix else ""

    def keys(self, under):
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix + under):
            for o in page.get("Contents", []):
                yield o["Key"][len(self.prefix):]

    def read(self, rel):
        return self.s3.get_object(Bucket=self.bucket, Key=self.prefix + rel)["Body"].read()

    def block(self, cid):
        return self.read("blocks/%s/%s" % (cid[-2:], cid))

    def verified(self, cid):
        """The block's bytes, after checking they hash to cid (gate/cas.Store.read's rule)."""
        data = self.block(cid)
        actual = cas.cid_from_sha256(hashlib.sha256(data).digest(), cas.cid_codec(cid))
        if actual != cid:
            raise cas.StoreError("block %s in s3://%s/%s hashes to %s" % (cid, self.bucket, self.prefix, actual))
        return data

    def decoded(self, cid):
        return cas.decode(self.verified(cid))

    def blocks(self):
        return [k.rsplit("/", 1)[1] for k in self.keys("blocks/")]

    def log(self):
        return [k[len("log/"):] for k in self.keys("log/")]

    def completions(self):
        return [name.split("-", 2)[2] for name in self.log() if name.split("-", 2)[1] == "run"]

    def coords(self):
        return {k[len("coords/"):]: self.read(k).decode().strip() for k in self.keys("coords/")}

    def nf_records(self, kind):
        """[(key, spec)] of nf/**/.data.json whose kind is `kind`, key relative to nf/."""
        out = []
        for k in self.keys("nf/"):
            if k.endswith("/.data.json"):
                envelope = json.loads(self.read(k).decode())
                if envelope.get("kind") == kind:
                    out.append((k[len("nf/"):-len("/.data.json")], envelope.get("spec") or {}))
        return sorted(out, key=lambda pair: pair[0])

    def store_log(self):
        """[(reverse_ts, kind, cid)] of log/<rts>-<kind>-<cid>, as gate/cas.Store.store_log reads a local one."""
        out = []
        for name in sorted(self.log()):
            parts = name.split("-", 2)
            if len(parts) != 3 or len(parts[0]) != 13 or not parts[0].isdigit():
                continue
            rts, kind, cid = parts
            if kind in cas.Store.STORE_LOG_KINDS and cas.is_cid(cid):
                out.append((rts, kind, cid))
        return out

    def listing(self, under):
        """([(key relative to the member, LastModified in epoch seconds)], S3's Date in epoch seconds or None).
        The Date is the last page's response header: the member's clock, as the plugin reads it (ticket 20
        answer 6)."""
        out, date = [], None
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix + under):
            header = ((page.get("ResponseMetadata") or {}).get("HTTPHeaders") or {}).get("date")
            if header:
                date = email.utils.parsedate_to_datetime(header).timestamp()
            for o in page.get("Contents", []):
                modified = o.get("LastModified")
                out.append((o["Key"][len(self.prefix):], modified.timestamp() if modified is not None else None))
        return out, date

    def body_or_none(self, rel):
        return self.read(rel) if self.head(rel) is not None else None

    def head(self, rel):
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=self.prefix + rel, ChecksumMode="ENABLED")
        except self.s3.exceptions.ClientError:
            return None

    def download(self, rel, path):
        with open(path, "wb") as fh:
            fh.write(self.read(rel))
        return path


class Blocks(object):
    """A Member seen as gate/cas.Store is by the shared retention code: read(cid) gives verified bytes,
    read_block(cid) a verified, decoded dag-cbor block, store_log() the Store Log. A missing block is a
    cas.StoreError, as it is locally."""

    def __init__(self, member):
        self.member = member

    def read(self, cid):
        try:
            return self.member.verified(cid)
        except self.member.s3.exceptions.ClientError as exc:
            raise cas.StoreError("no block %s in s3://%s/%s (%s)" % (cid, self.member.bucket, self.member.prefix, _code(exc)))

    def read_block(self, cid):
        if cas.cid_codec(cid) != cas.DAG_CBOR:
            raise cas.StoreError("%s is not a dag-cbor block" % cid)
        return cas.decode(self.read(cid))

    def store_log(self):
        return self.member.store_log()


def _live_key(bucket, member, session):
    if not RUN_BUCKET.match(bucket or "") or not re.match(r"^[A-Za-z0-9._-]+$", member or "") \
            or not re.match(r"^[A-Za-z0-9._-]+$", session or ""):
        raise ValueError("refusing live/ write: %r %r %r is not a tier-two bucket, member and session" % (bucket, member, session))
    return "%s/live/%s" % (member, session)


def live_put(s3, bucket, member, session, run_name):
    """A Live Writer registration as a run writes one (ticket 20 answer 5); its LastModified is S3's."""
    body = json.dumps({"session": session, "run_name": run_name, "pipeline": "x", "started_at": "x"}).encode()
    s3.put_object(Bucket=bucket, Key=_live_key(bucket, member, session), Body=body)


def live_delete(s3, bucket, member, session):
    s3.delete_object(Bucket=bucket, Key=_live_key(bucket, member, session))


def work_objects(s3, prefix):
    """{path in task: [keys]} for every task output in the work prefix, .command.* and .exitcode left out."""
    out = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=WORK_BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):].split("/", 2)
            if len(rel) == 3 and not rel[2].startswith(".command") and rel[2] != ".exitcode":
                out.setdefault(rel[2], []).append(o["Key"])
    return out


def stale_if_match(s3, bucket, key="probe/if-match"):
    """Upload an object, replace it, then PutObject with If-Match on the first ETag: S3 must answer 412
    and keep the second body (ticket 03 decision 6; S3's If-Match was not measured in ticket 14)."""
    stale = s3.put_object(Bucket=bucket, Key=key, Body=b"first")["ETag"]
    s3.put_object(Bucket=bucket, Key=key, Body=b"second")
    status = 200
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"third", IfMatch=stale)
    except s3.exceptions.ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    s3.delete_object(Bucket=bucket, Key=key)
    return {"status": status, "body": body}


def sha256_of_object(s3, bucket, key):
    h = hashlib.sha256()
    for chunk in s3.get_object(Bucket=bucket, Key=key)["Body"].iter_chunks(1 << 20):
        h.update(chunk)
    return h.hexdigest()


def snapshot_runs(path):
    """count(*) of an Index Snapshot's run table, read-only."""
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    try:
        return con.execute("SELECT count(*) FROM run").fetchone()[0]
    finally:
        con.close()


def snapshot(s3, bucket, member, path):
    """What tier two records of a member's Index Snapshot: its x-amz-meta-runs and its run rows."""
    m = Member(s3, bucket, member)
    head = m.head("index/v3.sqlite")
    if head is None:
        return {"meta_runs": None, "count": None}
    m.download("index/v3.sqlite", path)
    runs = (head.get("Metadata") or {}).get("runs")
    return {"meta_runs": int(runs) if runs is not None and str(runs).isdigit() else runs,
            "count": snapshot_runs(path)}


if __name__ == "__main__":
    if sys.argv[1:2] == ["setup"]:
        print(setup(sys.argv[2]))
    elif sys.argv[1:2] == ["teardown"]:
        sys.exit(teardown(sys.argv[2], sys.argv[3], sys.argv[4]))
    elif sys.argv[1:2] == ["if-match"]:
        print(json.dumps(stale_if_match(client(), sys.argv[2])))
    elif sys.argv[1:2] == ["snapshot"]:
        print(json.dumps(snapshot(client(), sys.argv[2], sys.argv[3], sys.argv[4])))
    elif sys.argv[1:2] == ["live-put"] and len(sys.argv) == 6:
        live_put(client(), *sys.argv[2:6])
    elif sys.argv[1:2] == ["live-delete"] and len(sys.argv) == 5:
        live_delete(client(), *sys.argv[2:5])
    else:
        sys.stderr.write(__doc__)
        sys.exit(2)
