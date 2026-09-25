"""The counting static server the browser tier measures through."""
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser"))

import serve  # noqa: E402


class ServeTest(unittest.TestCase):
    def start(self, no_range=False):
        self.root = tempfile.mkdtemp()
        with open(os.path.join(self.root, "f.bin"), "wb") as fh:
            fh.write(bytes(range(256)) * 40)          # 10240 bytes
        server = serve.make_server(self.root, 0, no_range)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        self.base = "http://127.0.0.1:%d" % server.server_address[1]

    def get(self, path, headers=None):
        req = urllib.request.Request(self.base + path, headers=headers or {})
        try:
            with urllib.request.urlopen(req) as res:
                return res.status, dict(res.headers), res.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers), exc.read()

    def stats(self):
        return json.loads(self.get("/__stats")[2])

    def test_range_is_206_with_exact_bytes(self):
        self.start()
        status, headers, body = self.get("/f.bin", {"Range": "bytes=4096-8191"})
        self.assertEqual(status, 206)
        self.assertEqual(headers["Content-Range"], "bytes 4096-8191/10240")
        self.assertEqual(body, (bytes(range(256)) * 40)[4096:8192])

    def test_suffix_and_open_ranges(self):
        self.start()
        self.assertEqual(len(self.get("/f.bin", {"Range": "bytes=-100"})[2]), 100)
        self.assertEqual(len(self.get("/f.bin", {"Range": "bytes=10000-"})[2]), 240)

    def test_range_past_the_end_is_416(self):
        self.start()
        self.assertEqual(self.get("/f.bin", {"Range": "bytes=20000-20001"})[0], 416)

    def test_no_range_mode_answers_200_with_the_whole_file(self):
        self.start(no_range=True)
        status, headers, body = self.get("/f.bin", {"Range": "bytes=0-99"})
        self.assertEqual(status, 200)
        self.assertEqual(len(body), 10240)
        self.assertNotIn("Accept-Ranges", headers)

    def test_stats_count_and_reset(self):
        self.start()
        self.get("/f.bin", {"Range": "bytes=0-4095"})
        self.get("/f.bin", {"Range": "bytes=4096-8191"})
        first = self.stats()
        self.assertEqual((first["requests"], first["bytes"]), (2, 8192))
        self.assertEqual(self.stats()["requests"], 0)

    def test_cors_exposes_content_range(self):
        self.start()
        headers = self.get("/f.bin", {"Range": "bytes=0-9", "Origin": "http://elsewhere"})[1]
        self.assertEqual(headers["Access-Control-Allow-Origin"], "*")
        self.assertIn("Content-Range", headers["Access-Control-Expose-Headers"])


if __name__ == "__main__":
    unittest.main()
