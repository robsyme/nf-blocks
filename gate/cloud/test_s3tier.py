# gate/cloud/test_s3tier.py
"""s3tier.py must have no AWS side effects at import time: the brief's draft
built a boto3.Session at module scope, which means every unit test that only
imports the file needs boto3 installed and a resolvable AWS profile. These
tests pin that importing it (and `py_compile`-ing it) does nothing but define
names, so the cloud tier's Python can be exercised offline. Talking to AWS for
real is Step 4, with Rob at the keyboard and his SSO session."""
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


if __name__ == "__main__":
    unittest.main()
