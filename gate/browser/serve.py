#!/usr/bin/env python3
"""A static file server with Range, CORS and request counting.

    python3 gate/browser/serve.py <root> <port> [--no-range]

Stock `python -m http.server` ignores Range and answers 200 with the whole
file, which is exactly the "plain static server" of spec section 6, so
--no-range reproduces it on purpose. GET /__stats returns and resets
{requests, bytes, log}; page assets (.html, .js, .wasm) are not counted.
"""
import json
import os
import re
import sys
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

RANGE = re.compile(r'^bytes=(\d*)-(\d*)$')


def make_server(root, port, no_range=False):
    lock = threading.Lock()
    stats = {'requests': 0, 'bytes': 0, 'log': []}

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *a, **k):
            super().__init__(*a, directory=root, **k)

        def log_message(self, *a):
            pass

        def end_headers(self):
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Headers', 'Range')
            self.send_header('Access-Control-Expose-Headers',
                             'Content-Range, Content-Length, Accept-Ranges, ETag')
            self.send_header('Cache-Control', 'no-store')
            super().end_headers()

        def do_OPTIONS(self):
            self.send_response(204)
            self.end_headers()

        def record(self, n, rng):
            path = self.path.split('?')[0]
            if path.startswith('/__') or path.endswith(('.js', '.wasm', '.html', '.map')):
                return
            with lock:
                stats['requests'] += 1
                stats['bytes'] += n
                stats['log'].append([self.command, path, rng, n])

        def file_path(self):
            return self.translate_path(self.path.split('?')[0])

        def do_HEAD(self):
            path = self.file_path()
            if not os.path.isfile(path):
                return super().do_HEAD()
            self.record(0, 'HEAD')
            self.send_response(200)
            self.send_header('Content-Length', str(os.path.getsize(path)))
            if not no_range:
                self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()

        def do_GET(self):
            if self.path == '/__stats':
                with lock:
                    body = json.dumps(stats).encode()
                    stats.update(requests=0, bytes=0, log=[])
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            path = self.file_path()
            header = self.headers.get('Range')
            if os.path.isfile(path) and header and not no_range:
                return self.ranged(path, header)
            if os.path.isfile(path):
                self.record(os.path.getsize(path), None)
            return super().do_GET()

        def ranged(self, path, header):
            size = os.path.getsize(path)
            m = RANGE.match(header.strip())
            if not m or (m.group(1) == '' and m.group(2) == ''):
                self.record(size, None)
                return super().do_GET()
            if m.group(1) == '':
                start, end = max(0, size - int(m.group(2))), size - 1
            else:
                start = int(m.group(1))
                end = min(int(m.group(2)) if m.group(2) else size - 1, size - 1)
            if start >= size or start > end:
                self.send_response(416)
                self.send_header('Content-Range', 'bytes */%d' % size)
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            n = end - start + 1
            self.send_response(206)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Range', 'bytes %d-%d/%d' % (start, end, size))
            self.send_header('Content-Length', str(n))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()
            with open(path, 'rb') as fh:
                fh.seek(start)
                self.wfile.write(fh.read(n))
            self.record(n, header)

    return ThreadingHTTPServer(('127.0.0.1', port), Handler)


if __name__ == '__main__':
    make_server(sys.argv[1], int(sys.argv[2]), '--no-range' in sys.argv).serve_forever()
