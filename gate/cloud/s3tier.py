#!/usr/bin/env python3
"""Throwaway buckets for the cloud browser tier (block explorer spec section 1.3).

    python3 gate/cloud/s3tier.py setup <site dir> <region>    # prints "<public> <private>"
    python3 gate/cloud/s3tier.py teardown <bucket> <region>

The public bucket carries the policy and CORS rule of spec section 6 and holds
the Gate's store at its root and the year snapshot under year/. The private
bucket holds the same store with every public access blocked. Both are tagged
and deleted by teardown; setup deletes what it made if it fails half way.
Needs boto3 and an AWS session (AWS_PROFILE, default scidev).
"""
import json
import os
import re
import sys
import time
import uuid

TAGS = [{"Key": "purpose", "Value": "nf-blocks Gate cloud browser tier - throwaway"},
        {"Key": "owner", "Value": os.environ.get("GATE_OWNER", "rob.syme")}]
CORS = {"CORSRules": [{"AllowedOrigins": ["*"], "AllowedMethods": ["GET", "HEAD"], "AllowedHeaders": ["range"],
                       "ExposeHeaders": ["Content-Range", "Content-Length", "Accept-Ranges", "ETag"],
                       "MaxAgeSeconds": 3000}]}

# An Index Snapshot key, at any nesting depth (a member subdirectory, or
# directly under the year/ prefix): DESIGN.md §15 "What a member serves".
_SNAPSHOT_KEY = re.compile(r'(^|/)index/v\d+\.sqlite$')


def cache_control_for(key):
    """The Cache-Control this key gets, matching what nf-blocks:explore sends
    for the same two kinds of file (ExploreServer.groovy): the snapshot is
    revalidated every load, a block is immutable and cached for a year."""
    return 'no-cache' if _SNAPSHOT_KEY.search(key) else 'public, max-age=31536000, immutable'


def session():
    """Built lazily, only when a function actually needs AWS: a module-level
    boto3.Session(...) would run at import time, which means every unit test
    that merely imports this file needs boto3 installed and a real profile to
    resolve. `py_compile` and `import s3tier` must both have no side effects."""
    import boto3
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "scidev"))


def create(s3, region, public, on_created=lambda bucket: None):
    """`on_created` fires the instant the bucket exists, before tagging, the
    public access block, CORS or the policy retry loop (which can raise
    SystemExit) run. Any of those can fail on a bucket that was still created
    -- an account-level Block Public Access is a plausible way for the policy
    step to fail -- so the caller must learn the bucket's name before this
    function can raise, or setup()'s own cleanup never sees it and leaks it."""
    bucket = "nf-blocks-gate-%s-%s" % ("pub" if public else "priv", uuid.uuid4().hex[:10])
    s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
    on_created(bucket)
    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": TAGS})
    if not public:
        s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
        return bucket
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": False, "RestrictPublicBuckets": False})
    s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=CORS)
    policy = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::%s/*" % bucket},
        {"Effect": "Allow", "Principal": "*", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::%s" % bucket}]}
    for _ in range(10):
        try:
            s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
            return bucket
        except s3.exceptions.ClientError as exc:
            print("policy retry: %s" % exc.response["Error"]["Code"], file=sys.stderr)
            time.sleep(3)
    raise SystemExit("could not set the public bucket's policy")


def upload(s3, bucket, root, prefix=""):
    from boto3.s3.transfer import TransferConfig
    config = TransferConfig(max_concurrency=16)
    count = total = 0
    started = time.time()
    for dirpath, _dirs, files in os.walk(root, followlinks=True):
        for name in files:
            full = os.path.join(dirpath, name)
            key = prefix + os.path.relpath(full, root).replace(os.sep, "/")
            s3.upload_file(full, bucket, key, Config=config,
                            ExtraArgs={"CacheControl": cache_control_for(key)})
            count += 1
            total += os.path.getsize(full)
    print("uploaded %d objects, %d MB to %s in %.0f s" % (count, total // 1_000_000, bucket, time.time() - started), file=sys.stderr)


def delete(region, bucket):
    b = session().resource("s3", region_name=region).Bucket(bucket)
    b.objects.all().delete()
    b.delete()
    print("deleted %s" % bucket, file=sys.stderr)


def setup(site, region):
    s3 = session().client("s3", region_name=region)
    made = []
    try:
        public = create(s3, region, True, made.append)
        private = create(s3, region, False, made.append)
        store = os.path.join(site, "stores", "current")
        upload(s3, public, store)
        upload(s3, public, os.path.join(site, "stores", "year"), prefix="year/")
        upload(s3, private, store)
    except BaseException:
        # Each bucket is torn down independently: one delete failing (e.g. a
        # transient error, or objects.all().delete() missing something) must
        # not skip the other bucket, and a bucket that could not be deleted
        # here is reported loudly rather than silently leaked.
        undeleted = []
        for bucket in made:
            try:
                delete(region, bucket)
            except BaseException as exc:  # noqa: BLE001, we want every bucket attempted regardless
                undeleted.append(bucket)
                print("bucket %s NOT deleted; delete it by hand (%s: %s)"
                      % (bucket, type(exc).__name__, exc), file=sys.stderr)
        if undeleted:
            print("cloud tier: %d bucket(s) could not be torn down: %s"
                  % (len(undeleted), ", ".join(undeleted)), file=sys.stderr)
        raise
    print("%s %s" % (public, private))


if __name__ == "__main__":
    if len(sys.argv) != 4 or sys.argv[1] not in ("setup", "teardown"):
        sys.stderr.write(__doc__)
        sys.exit(2)
    if sys.argv[1] == "setup":
        setup(sys.argv[2], sys.argv[3])
    else:
        delete(sys.argv[3], sys.argv[2])
