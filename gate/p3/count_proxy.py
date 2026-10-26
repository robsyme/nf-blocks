#!/usr/bin/env python3
"""A counting HTTP proxy in front of a local S3-compatible server.

    python3 gate/p3/count_proxy.py <listen_port> <upstream_port> <log_file>

Listens on 127.0.0.1:<listen_port> and forwards every request unchanged to
127.0.0.1:<upstream_port>, Host header included, so SigV4 signatures still
verify. Appends one line per request to <log_file>: "<METHOD> <path> <status>".
Only 127.0.0.1 is ever contacted. Used by s3-accept.sh to count the HEADs a
snapshot build makes (there is no per-build counter in the plugin).
"""
import http.client
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOP = {'connection', 'keep-alive', 'proxy-connection', 'transfer-encoding', 'te', 'trailer', 'upgrade'}


def make_server(listen_port, upstream_port, log_file):
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *a):
            pass

        def body(self):
            if 'chunked' in (self.headers.get('Transfer-Encoding') or '').lower():
                parts = []
                while True:
                    size = int(self.rfile.readline().split(b';')[0].strip() or b'0', 16)
                    if size == 0:
                        while self.rfile.readline() not in (b'\r\n', b'\n', b''):
                            pass
                        break
                    parts.append(self.rfile.read(size))
                    self.rfile.readline()
                return b''.join(parts), True
            n = int(self.headers.get('Content-Length') or 0)
            return (self.rfile.read(n) if n else b''), False

        def forward(self):
            data, chunked = self.body()
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
            if chunked:
                headers['Content-Length'] = str(len(data))
            conn = http.client.HTTPConnection('127.0.0.1', upstream_port, timeout=300)
            try:
                conn.request(self.command, self.path, body=data if data or self.command in ('PUT', 'POST') else None,
                             headers=headers)
                resp = conn.getresponse()
                payload = resp.read()
                self.send_response(resp.status, resp.reason)
                for k, v in resp.getheaders():
                    if k.lower() not in HOP and k.lower() != 'content-length':
                        self.send_header(k, v)
                self.send_header('Content-Length', resp.getheader('Content-Length') if self.command == 'HEAD'
                                 and resp.getheader('Content-Length') else str(len(payload)))
                self.end_headers()
                if self.command != 'HEAD':
                    self.wfile.write(payload)
                status = resp.status
            finally:
                conn.close()
            with lock, open(log_file, 'a') as fh:
                fh.write(f'{self.command} {self.path} {status}\n')

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = forward

    return ThreadingHTTPServer(('127.0.0.1', listen_port), Handler)


if __name__ == '__main__':
    listen, upstream, log = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
    make_server(listen, upstream, log).serve_forever()
