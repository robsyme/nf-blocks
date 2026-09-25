# gate/cloud/test_s3tier.py
"""s3tier.py must have no AWS side effects at import time: the brief's draft
built a boto3.Session at module scope, which means every unit test that only
imports the file needs boto3 installed and a resolvable AWS profile. These
tests pin that importing it (and `py_compile`-ing it) does nothing but define
names, so the cloud tier's Python can be exercised offline. Talking to AWS for
real is Step 4, with Rob at the keyboard and his SSO session."""
import contextlib
import io
import os
import sys
import unittest
import unittest.mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _fresh_import():
    sys.modules.pop("s3tier", None)
    import s3tier
    return s3tier


class ModuleImportHasNoAwsSideEffectsTest(unittest.TestCase):
    def test_import_succeeds_with_no_module_level_session(self):
        # A module-level `SESSION = boto3.Session(...)` would build a real
        # session object (and need boto3 importable) the moment this module is
        # imported, regardless of whether setup/teardown are ever called.
        module = _fresh_import()
        self.assertNotIn("SESSION", vars(module),
                          "a module-level SESSION would build a boto3.Session at import time")

    def test_module_source_defines_no_top_level_import_of_boto3(self):
        # boto3 is only needed inside session()/upload(); importing it at
        # module scope is exactly the side effect this task removes.
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "s3tier.py")) as fh:
            lines = fh.readlines()
        top_level_imports = [ln for ln in lines if ln.startswith("import ") or ln.startswith("from ")]
        self.assertFalse(any("boto3" in ln for ln in top_level_imports),
                          "boto3 must be imported inside a function, not at module scope: %r" % top_level_imports)

    def test_importing_never_calls_boto3_session(self):
        try:
            import boto3
        except ImportError:
            self.skipTest("boto3 not installed")
        with unittest.mock.patch.object(boto3, "Session",
                                         side_effect=AssertionError("boto3.Session() called at import time")):
            _fresh_import()  # must not raise


# --------------------------------------------------------------------------
# Fix round 1, issue 4: create() must record a bucket the instant it exists,
# so setup()'s cleanup can delete it even if a later step (tagging, the
# public access block, CORS, or the policy retry's SystemExit -- plausible if
# an account-level Block Public Access refuses the public bucket's policy)
# fails. These stub the boto3 *client*/*resource* shape by hand: create() and
# upload() are duck-typed over `s3` and never import boto3 themselves, so
# this needs no boto3 installed, and setup()'s own `session()` call is
# monkeypatched for the same reason.
# --------------------------------------------------------------------------

class FakeClientError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class FakeExceptions:
    ClientError = FakeClientError


class FakeS3Client:
    """`fail_on` maps a method name to the 1-based call number (across every
    bucket) that should raise, so a test can make the *second* bucket's
    create() fail partway while the first still succeeds in full."""

    def __init__(self, fail_on=None):
        self.exceptions = FakeExceptions()
        self.calls = []
        self.fail_on = fail_on or {}
        self._counts = {}

    def _step(self, name):
        self.calls.append(name)
        self._counts[name] = self._counts.get(name, 0) + 1
        if self._counts[name] == self.fail_on.get(name):
            raise RuntimeError("%s call #%d failed (stub)" % (name, self._counts[name]))

    def create_bucket(self, **_kw):
        self._step("create_bucket")

    def put_bucket_tagging(self, **_kw):
        self._step("put_bucket_tagging")

    def put_public_access_block(self, **_kw):
        self._step("put_public_access_block")

    def put_bucket_cors(self, **_kw):
        self._step("put_bucket_cors")

    def put_bucket_policy(self, **_kw):
        self._step("put_bucket_policy")


class FakeObjectCollection:
    def all(self):
        return self

    def delete(self):
        return None  # dropping objects before the bucket delete; nothing to assert here


class FakeBucket:
    def __init__(self, recorder, name):
        self._recorder = recorder
        self.name = name
        self.objects = FakeObjectCollection()

    def delete(self):
        self._recorder.append(self.name)


class FakeResource:
    def __init__(self, recorder):
        self._recorder = recorder

    def Bucket(self, name):  # noqa: N802, matches boto3's own method name
        return FakeBucket(self._recorder, name)


class FakeSession:
    def __init__(self, client, deleted):
        self._client = client
        self._deleted = deleted

    def client(self, *_a, **_kw):
        return self._client

    def resource(self, *_a, **_kw):
        return FakeResource(self._deleted)


class CreateRecordsTheBucketBeforeItCanFailTest(unittest.TestCase):
    def setUp(self):
        self.s3tier = _fresh_import()

    def test_on_created_fires_before_put_bucket_tagging_can_fail(self):
        client = FakeS3Client(fail_on={"put_bucket_tagging": 1})
        recorded = []
        with self.assertRaises(RuntimeError):
            self.s3tier.create(client, "ca-central-1", True, recorded.append)
        self.assertEqual(len(recorded), 1)
        self.assertTrue(recorded[0].startswith("nf-blocks-gate-pub-"), recorded)

    def test_on_created_fires_before_the_policy_retry_loops_to_SystemExit(self):
        client = FakeS3Client(fail_on={})  # put_bucket_policy always raises FakeClientError below
        client.put_bucket_policy = unittest.mock.Mock(side_effect=FakeClientError("AccessDenied"))
        recorded = []
        with unittest.mock.patch.object(self.s3tier.time, "sleep", return_value=None):
            with self.assertRaises(SystemExit):
                self.s3tier.create(client, "ca-central-1", True, recorded.append)
        self.assertEqual(len(recorded), 1)
        self.assertTrue(recorded[0].startswith("nf-blocks-gate-pub-"), recorded)

    def test_on_created_fires_for_the_private_bucket_too(self):
        client = FakeS3Client(fail_on={"put_public_access_block": 1})
        recorded = []
        with self.assertRaises(RuntimeError):
            self.s3tier.create(client, "ca-central-1", False, recorded.append)
        self.assertEqual(len(recorded), 1)
        self.assertTrue(recorded[0].startswith("nf-blocks-gate-priv-"), recorded)

    def test_the_default_callback_is_a_no_op(self):
        client = FakeS3Client()
        bucket = self.s3tier.create(client, "ca-central-1", False)  # no on_created passed
        self.assertTrue(bucket.startswith("nf-blocks-gate-priv-"))


class SetupDeletesEveryBucketItMadeEvenOnAPartialFailureTest(unittest.TestCase):
    def setUp(self):
        self.s3tier = _fresh_import()

    def test_a_failure_partway_through_the_private_bucket_still_deletes_both(self):
        # Call order inside setup(): create_bucket/tagging/PAB/CORS/policy for
        # the public bucket (all succeed), then create_bucket/on_created for
        # the private bucket, then its put_bucket_tagging -- the second call
        # to that method overall -- fails.
        client = FakeS3Client(fail_on={"put_bucket_tagging": 2})
        deleted = []
        fake_session = FakeSession(client, deleted)
        with unittest.mock.patch.object(self.s3tier, "session", return_value=fake_session), \
                unittest.mock.patch.object(self.s3tier.time, "sleep", return_value=None):
            with self.assertRaises(RuntimeError):
                self.s3tier.setup("/unused-site-dir", "ca-central-1")
        self.assertEqual(len(deleted), 2, "both buckets must be deleted, not just the one setup() returned: %r" % deleted)
        self.assertTrue(any(name.startswith("nf-blocks-gate-pub-") for name in deleted), deleted)
        self.assertTrue(any(name.startswith("nf-blocks-gate-priv-") for name in deleted), deleted)

    def test_one_bucket_failing_to_delete_does_not_skip_the_other(self):
        client = FakeS3Client(fail_on={"put_bucket_tagging": 2})
        deleted = []

        class FlakyResource(FakeResource):
            def Bucket(self, name):  # noqa: N802
                if name.startswith("nf-blocks-gate-pub-"):
                    raise RuntimeError("delete of the public bucket failed (stub)")
                return super().Bucket(name)

        class FlakySession(FakeSession):
            def resource(self, *_a, **_kw):
                return FlakyResource(self._deleted)

        fake_session = FlakySession(client, deleted)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with unittest.mock.patch.object(self.s3tier, "session", return_value=fake_session), \
                    unittest.mock.patch.object(self.s3tier.time, "sleep", return_value=None):
                with self.assertRaises(RuntimeError):
                    self.s3tier.setup("/unused-site-dir", "ca-central-1")
        # The public bucket's delete raised; the private one must still have
        # been attempted and recorded, and the failure printed loudly.
        self.assertEqual(len(deleted), 1, deleted)
        self.assertTrue(deleted[0].startswith("nf-blocks-gate-priv-"), deleted)
        self.assertIn("NOT deleted; delete it by hand", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
