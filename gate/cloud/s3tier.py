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
import sys
import time
import uuid

TAGS = [{"Key": "purpose", "Value": "nf-blocks Gate cloud browser tier - throwaway"},
        {"Key": "owner", "Value": os.environ.get("GATE_OWNER", "rob.syme")}]
CORS = {"CORSRules": [{"AllowedOrigins": ["*"], "AllowedMethods": ["GET", "HEAD"], "AllowedHeaders": ["range"],
                       "ExposeHeaders": ["Content-Range", "Content-Length", "Accept-Ranges", "ETag"],
                       "MaxAgeSeconds": 3000}]}


def session():
    """Built lazily, only when a function actually needs AWS: a module-level
    boto3.Session(...) would run at import time, which means every unit test
    that merely imports this file needs boto3 installed and a real profile to
    resolve. `py_compile` and `import s3tier` must both have no side effects."""
    import boto3
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "scidev"))


def create(s3, region, public):
    bucket = "nf-blocks-gate-%s-%s" % ("pub" if public else "priv", uuid.uuid4().hex[:10])
    s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
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
            s3.upload_file(full, bucket, key, Config=config)
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
        public = create(s3, region, True)
        made.append(public)
        private = create(s3, region, False)
        made.append(private)
        store = os.path.join(site, "stores", "current")
        upload(s3, public, store)
        upload(s3, public, os.path.join(site, "stores", "year"), prefix="year/")
        upload(s3, private, store)
    except BaseException:
        for bucket in made:
            delete(region, bucket)
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
