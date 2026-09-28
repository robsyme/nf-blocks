"""Tier two's view of S3, independent of the plugin (rule 6): boto3 only.

    s3gate.py setup <run-id>            prints the bucket it created (us-east-1)
    s3gate.py teardown <bucket> <work-prefix>
    s3gate.py if-match <bucket>          prints {"status": ..., "body": ...} of the stale If-Match probe
    s3gate.py snapshot <bucket> <member> <file>
                                         downloads <member>/index/v3.sqlite to <file>, prints
                                         {"meta_runs": x-amz-meta-runs, "count": count(*) of its run table}
Everything else is imported by tier2.sh's assertions.
"""
import hashlib, json, os, sqlite3, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import cas  # noqa: E402

REGION = "us-east-1"
WORK_BUCKET = "scidev-playground-us-east-1"
# The only work prefixes teardown will empty: an empty or foreign prefix would delete other people's data.
WORK_PREFIX_ROOT = "robsyme/nf-blocks-gate/t2-"


def client():
    import boto3
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "scidev")).client("s3", region_name=REGION)


def setup(run_id):
    s3 = client()
    bucket = "nf-blocks-t2-%s" % run_id
    s3.create_bucket(Bucket=bucket)   # us-east-1 takes no LocationConstraint
    try:
        s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={"Rules": [
            {"ID": "staging", "Filter": {"Prefix": ""}, "Status": "Enabled",
             "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}},
            {"ID": "tmp", "Filter": {"Prefix": "cas/tmp/"}, "Status": "Enabled", "Expiration": {"Days": 1}}]})
    except Exception:
        # The harness learns the bucket's name only from a successful setup, so a half-made one is removed here.
        s3.delete_bucket(Bucket=bucket)
        raise
    return bucket


def _empty(s3, bucket, prefix=""):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
    for page in s3.get_paginator("list_multipart_uploads").paginate(Bucket=bucket, Prefix=prefix):
        for u in page.get("Uploads", []):
            s3.abort_multipart_upload(Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"])


def _check_work_prefix(work_prefix):
    if not work_prefix.startswith(WORK_PREFIX_ROOT) or not work_prefix.endswith("/"):
        raise ValueError("refusing to empty s3://%s/%s: not a tier-two run prefix (%s...)"
                         % (WORK_BUCKET, work_prefix, WORK_PREFIX_ROOT))


def teardown(bucket, work_prefix, s3=None):
    """Both halves always run; a failure in either is loud (cloud.sh's rule)."""
    s3, failed = s3 or client(), False

    def work():
        _check_work_prefix(work_prefix)
        _empty(s3, WORK_BUCKET, work_prefix)

    def member():
        if bucket:
            _empty(s3, bucket)
            s3.delete_bucket(Bucket=bucket)

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

    def head(self, rel):
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=self.prefix + rel, ChecksumMode="ENABLED")
        except self.s3.exceptions.ClientError:
            return None

    def download(self, rel, path):
        with open(path, "wb") as fh:
            fh.write(self.read(rel))
        return path


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
        sys.exit(teardown(sys.argv[2], sys.argv[3]))
    elif sys.argv[1:2] == ["if-match"]:
        print(json.dumps(stale_if_match(client(), sys.argv[2])))
    elif sys.argv[1:2] == ["snapshot"]:
        print(json.dumps(snapshot(client(), sys.argv[2], sys.argv[3], sys.argv[4])))
    else:
        sys.stderr.write(__doc__)
        sys.exit(2)
