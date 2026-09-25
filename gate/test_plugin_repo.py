"""gate/browser/plugin-repo.sh: the plugins.json names the zip that is installed."""
import json
import os
import subprocess
import tempfile
import time
import unittest

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser", "plugin-repo.sh")


class PluginRepoTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.repo = os.path.join(self.root, "repo")
        self.plugins = os.path.join(self.root, "plugins")
        os.makedirs(os.path.join(self.repo, "build", "distributions"))
        os.makedirs(self.plugins)
        for version in ("0.1.0", "0.2.0"):     # 0.2.0 written last: the newest
            with open(self.zip(version), "wb") as fh:
                fh.write(version.encode())
            time.sleep(0.01)

    def zip(self, version):
        return os.path.join(self.repo, "build", "distributions", "nf-blocks-%s.zip" % version)

    def run_script(self, **env):
        return subprocess.run(["bash", SCRIPT, self.repo, self.root], capture_output=True, text=True,
                              env={**os.environ, "NXF_PLUGINS_DIR": self.plugins, **env})

    def release(self):
        with open(os.path.join(self.root, "plugins.json")) as fh:
            return json.load(fh)[0]["releases"][0]

    def test_a_build_uses_the_newest_zip(self):
        env = {k: v for k, v in os.environ.items() if k != "GATE_SKIP_BUILD"}
        r = subprocess.run(["bash", SCRIPT, self.repo, self.root], capture_output=True, text=True,
                           env={**env, "NXF_PLUGINS_DIR": self.plugins})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.release()["version"], "0.2.0")

    def test_skip_build_uses_the_zip_of_the_installed_version(self):
        os.makedirs(os.path.join(self.plugins, "nf-blocks-0.1.0"))
        r = self.run_script(GATE_SKIP_BUILD="1")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.release()["version"], "0.1.0")
        self.assertEqual(self.release()["url"], "file://" + self.zip("0.1.0"))

    def test_skip_build_fails_clearly_without_a_matching_zip(self):
        os.makedirs(os.path.join(self.plugins, "nf-blocks-0.3.0"))
        r = self.run_script(GATE_SKIP_BUILD="1")
        self.assertEqual(r.returncode, 2)
        self.assertIn("nf-blocks-0.3.0", r.stderr)

    def test_skip_build_fails_clearly_with_nothing_or_two_installed(self):
        r = self.run_script(GATE_SKIP_BUILD="1")
        self.assertEqual(r.returncode, 2)
        self.assertIn("installed", r.stderr)
        os.makedirs(os.path.join(self.plugins, "nf-blocks-0.1.0"))
        os.makedirs(os.path.join(self.plugins, "nf-blocks-0.2.0"))
        r = self.run_script(GATE_SKIP_BUILD="1")
        self.assertEqual(r.returncode, 2)
        self.assertIn("installed", r.stderr)


if __name__ == "__main__":
    unittest.main()
