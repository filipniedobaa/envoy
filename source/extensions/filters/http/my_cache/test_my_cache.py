#!/usr/bin/env python3
"""
End-to-end tests for the my_cache Envoy filter: caching, request coalescing,
HTTP caching rules, client disconnects, origin failures, slow clients
(watermarks), size limits and LRU eviction.

The script runs its own test origin on 127.0.0.1:8000 and talks to Envoy on
127.0.0.1:10000 (listener with the my_cache filter, cluster "origin" pointing at
127.0.0.1:8000 - see envoy.yaml). Every test uses a fresh random URL, so earlier
cache contents never interfere and the suite can be re-run at any time.

Usage:
  1. start Envoy (ideally once with --concurrency 1 and once with --concurrency 4)
  2. python3 test_my_cache.py            # all tests
     python3 test_my_cache.py -k abort   # only tests whose name contains "abort"
     python3 test_my_cache.py --list
     python3 test_my_cache.py --max-object-mib 32 --max-total-mib 256   # if the limits differ

The test origin understands these query parameters (any path works):
  id=<seed>        body bytes are derived from this, so the client knows what to expect
  size=<bytes>     body size                                   (default 1024)
  chunks=<n>       send the body in n pieces                   (default 1)
  delay=<sec>      pause between pieces                        (default 0)
  first_delay=<s>  pause before sending headers                (default 0)
  cc=<value>       Cache-Control value, "none" = no header     (default max-age=60)
  status=<code>    response status                             (default 200)
  mode=length|chunked|abort|abort_early
                   abort: close the connection after abort_after pieces
                   abort_early: close without sending anything
  set_cookie=1     add a Set-Cookie header
  age=<sec>        add an Age header
  It also answers If-None-Match (ETag is "<id>") with 304 and Range: bytes=a-b with 206.

Output: PASS / FAIL / SKIP per test, plus a few informational lines at the end
(e.g. how connections were spread over Envoy's worker threads).

This test harness was written with AI assistance.
"""

import argparse
import hashlib
import http.client
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

ENVOY = ("127.0.0.1", 10000)
ORIGIN_PORT = 8000
CACHE_HEADER = "my-cache"

# ----------------------------------------------------------------------------
# Test origin
# ----------------------------------------------------------------------------


def make_body(seed: str, size: int) -> bytes:
    return random.Random(seed).randbytes(size)


def split(data: bytes, n: int):
    if not data:
        return [b""]
    step = -(-len(data) // n)
    return [data[i:i + step] for i in range(0, len(data), step)]


class Counter:
    def __init__(self):
        self._lock = threading.Lock()
        self._counts = {}

    def inc(self, key):
        with self._lock:
            self._counts[key] = self._counts.get(key, 0) + 1

    def get(self, method, path):
        with self._lock:
            return self._counts.get(f"{method} {path}", 0)


COUNTS = Counter()


ACTIVE = set()
ACTIVE_LOCK = threading.Lock()


class OriginHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        with ACTIVE_LOCK:
            ACTIVE.add(self.connection)

    def finish(self):
        with ACTIVE_LOCK:
            ACTIVE.discard(self.connection)
        try:
            super().finish()
        except OSError:
            pass

    def log_message(self, *args):
        pass

    def do_GET(self):
        self._respond(send_body=True)

    def do_HEAD(self):
        self._respond(send_body=False)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        if n:
            self.rfile.read(n)
        self._respond(send_body=True)

    def _respond(self, send_body):
        COUNTS.inc(f"{self.command} {self.path}")
        q = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
        size = int(q.get("size", 1024))
        chunks = max(1, int(q.get("chunks", 1)))
        delay = float(q.get("delay", 0))
        first_delay = float(q.get("first_delay", 0))
        cc = q.get("cc", "max-age=60")
        status = int(q.get("status", 200))
        mode = q.get("mode", "length")
        abort_after = int(q.get("abort_after", -1)) if mode == "abort" else -1
        chunked = mode == "chunked" or q.get("framing") == "chunked"

        if first_delay:
            time.sleep(first_delay)
        if mode == "abort_early":
            self._hard_close()
            return

        body = make_body(q.get("id", "x"), size)
        etag = '"%s"' % q.get("id", "x")

        # Conditional request -> 304 without body
        if status == 200 and self.headers.get("If-None-Match") == etag:
            self.send_response(304)
            self.send_header("ETag", etag)
            if cc != "none":
                self.send_header("Cache-Control", cc)
            self.end_headers()
            return

        # Range request -> 206 with a slice
        content_range = None
        rng = self.headers.get("Range", "")
        if status == 200 and rng.startswith("bytes="):
            a, _, b = rng[len("bytes="):].partition("-")
            a = int(a)
            b = int(b) if b else size - 1
            body = body[a:b + 1]
            status = 206
            content_range = f"bytes {a}-{a + len(body) - 1}/{size}"

        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("ETag", etag)
        if cc != "none":
            self.send_header("Cache-Control", cc)
        if content_range:
            self.send_header("Content-Range", content_range)
        if q.get("set_cookie"):
            self.send_header("Set-Cookie", "session=secret-of-whoever-asked-first")
        if q.get("age"):
            self.send_header("Age", q["age"])
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
        else:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not send_body:
            return

        parts = split(body, chunks)
        try:
            for i, part in enumerate(parts):
                if i == abort_after:
                    self._hard_close()
                    return
                if chunked:
                    if part:
                        self.wfile.write(b"%x\r\n" % len(part) + part + b"\r\n")
                else:
                    self.wfile.write(part)
                self.wfile.flush()
                if delay and i < len(parts) - 1:
                    time.sleep(delay)
            if chunked:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _hard_close(self):
        self.close_connection = True
        try:
            self.wfile.flush()
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


class Origin:
    def __init__(self, port):
        self.port = port
        self.server = None

    def start(self):
        ThreadingHTTPServer.allow_reuse_address = True
        ThreadingHTTPServer.daemon_threads = True
        self.server = ThreadingHTTPServer(("127.0.0.1", self.port), OriginHandler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.server = None
            # Also kill keep-alive connections Envoy still has pooled,
            # otherwise the "dead" origin keeps answering on them.
            with ACTIVE_LOCK:
                conns = list(ACTIVE)
            for c in conns:
                try:
                    c.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass


# ----------------------------------------------------------------------------
# Client helpers
# ----------------------------------------------------------------------------


class Result:
    def __init__(self):
        self.status = None
        self.headers = {}
        self.body = b""
        self.error = None          # None = complete, clean response
        self.disconnected = False  # we hung up on purpose

    @property
    def hit(self):
        return self.headers.get(CACHE_HEADER, "").upper() == "HIT"

    def __repr__(self):
        return (f"<status={self.status} len={len(self.body)} hit={self.hit} "
                f"error={self.error} disconnected={self.disconnected}>")


def fetch(path, method="GET", start_delay=0.0, read_bytes=None, timeout=30, body=None,
          headers=None, read_rate=None, stalls=None, rcvbuf=None):
    """read_bytes=N: read N body bytes, then hang up (simulates a client leaving).
    read_rate=B:  read at about B bytes/s (a slow client).
    stalls=[(offset, seconds), ...]: read at full speed, but pause for `seconds`
                  once `offset` bytes have been read (a client that stalls, then catches up).
    rcvbuf=B:     small kernel receive buffer, so stalls push back on Envoy quickly
                  instead of being absorbed by the client's kernel."""
    if start_delay:
        time.sleep(start_delay)
    r = Result()
    conn = http.client.HTTPConnection(*ENVOY, timeout=timeout)
    parts = []
    try:
        if rcvbuf:
            conn.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
            conn.sock.settimeout(timeout)
            conn.sock.connect(ENVOY)
        else:
            conn.connect()
        sock = conn.sock
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        r.status = resp.status
        r.headers = {k.lower(): v for k, v in resp.getheaders()}
        if read_bytes is not None:
            r.body = resp.read(read_bytes)
            r.disconnected = True
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        elif read_rate is not None or stalls:
            pending = sorted(stalls or [])
            got, t0 = 0, time.time()
            while True:
                piece = resp.read(64 * 1024)
                if not piece:
                    break
                parts.append(piece)
                got += len(piece)
                while pending and got >= pending[0][0]:
                    time.sleep(pending.pop(0)[1])
                if read_rate:
                    ahead = got / read_rate - (time.time() - t0)
                    if ahead > 0:
                        time.sleep(ahead)
            r.body = b"".join(parts)
            expected_len = int(r.headers.get("content-length", -1))
            if expected_len >= 0 and len(r.body) != expected_len:
                r.error = "incomplete"
        else:
            r.body = resp.read()
    except http.client.IncompleteRead as e:
        r.body = e.partial
        r.error = "incomplete-read"
    except (ConnectionResetError, http.client.RemoteDisconnected, BrokenPipeError) as e:
        r.body = b"".join(parts)
        r.error = type(e).__name__
    except ConnectionRefusedError:
        r.error = "connection-refused"
    except socket.timeout:
        r.error = "timeout"
    finally:
        conn.close()
    return r


def parallel(specs):
    results = [None] * len(specs)

    def run(i, kw):
        results[i] = fetch(**kw)

    threads = [threading.Thread(target=run, args=(i, kw)) for i, kw in enumerate(specs)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def url(name, **params):
    """Fresh path for every call. Returns (path, expected_body)."""
    params.setdefault("id", uuid.uuid4().hex)
    path = f"/t/{name}?" + urlencode(params)
    expected = make_body(params["id"], int(params.get("size", 1024)))
    return path, expected


def sha(b):
    return hashlib.sha256(b).hexdigest()[:12]


# ----------------------------------------------------------------------------
# Tiny test framework
# ----------------------------------------------------------------------------


class Fail(Exception):
    pass


class Skip(Exception):
    pass


TESTS = []
NOTES = []


def test(fn):
    TESTS.append(fn)
    return fn


def worker_connections():
    """Per-worker downstream connection counters from the Envoy admin (:9901), or None."""
    try:
        c = http.client.HTTPConnection("127.0.0.1", 9901, timeout=2)
        c.request("GET", "/stats?filter=worker_.*downstream_cx_total")
        text = c.getresponse().read().decode()
        c.close()
    except OSError:
        return None
    out = {}
    for line in text.splitlines():
        name, _, value = line.partition(":")
        if f"_{ENVOY[1]}.worker_" in name:
            worker = name.split("worker_")[1].split(".")[0]
            out[worker] = out.get(worker, 0) + int(value)
    return out or None


def check(cond, msg):
    if not cond:
        raise Fail(msg)


def note(msg):
    NOTES.append(msg)


def check_full(results, expected, label="client"):
    for i, r in enumerate(results):
        check(r.error is None, f"{label} {i}: error {r.error} ({r})")
        check(r.status == 200, f"{label} {i}: status {r.status}")
        check(len(r.body) == len(expected),
              f"{label} {i}: got {len(r.body)} bytes, expected {len(expected)}")
        check(r.body == expected, f"{label} {i}: body differs (sha {sha(r.body)} vs {sha(expected)})")


def check_count(path, expected, method="GET"):
    got = COUNTS.get(method, path)
    check(got == expected, f"origin saw {got} {method} request(s) for this URL, expected {expected}")


def envoy_alive():
    path, exp = url("alive", cc="no-store")
    r = fetch(path, timeout=5)
    return r.error is None and r.status == 200 and r.body == exp


# ----------------------------------------------------------------------------
# Tests: basic cache
# ----------------------------------------------------------------------------


@test
def basic_miss_then_hit():
    path, exp = url("basic", size=100_000)
    r1 = fetch(path)
    check_full([r1], exp, "miss")
    check(not r1.hit, "first request already says HIT")
    r2 = fetch(path)
    check_full([r2], exp, "hit")
    check(r2.hit, f"second request is not a HIT (headers: {r2.headers})")
    check_count(path, 1)


@test
def hit_large_body_integrity():
    path, exp = url("large", size=8 * 1024 * 1024, chunks=32)
    check_full([fetch(path)], exp, "miss")
    r = fetch(path)
    check(r.hit, "second request is not a HIT")
    check_full([r], exp, "hit")
    check_count(path, 1)


@test
def empty_body_cacheable():
    path, exp = url("empty", size=0)
    r1 = fetch(path)
    check_full([r1], exp, "miss")
    r2 = fetch(path)
    check(r2.hit, "empty 200 was not cached")
    check_full([r2], exp, "hit")
    check_count(path, 1)


@test
def expiry_max_age_1():
    path, exp = url("expiry", cc="max-age=1")
    check_full([fetch(path)], exp)
    time.sleep(2.2)
    r = fetch(path)
    check_full([r], exp)
    check(not r.hit, "expired entry was served as HIT")
    check_count(path, 2)
    r = fetch(path)
    check(r.hit, "the refetched response was not cached again")
    check_count(path, 2)


# ----------------------------------------------------------------------------
# Tests: cacheability rules
# ----------------------------------------------------------------------------


@test
def no_store_not_cached():
    path, exp = url("nostore", cc="no-store")
    rs = [fetch(path), fetch(path)]
    check_full(rs, exp)
    check(not rs[1].hit, "no-store response was served as HIT")
    check_count(path, 2)


@test
def private_not_cached():
    path, exp = url("private", cc="private, max-age=60")
    rs = [fetch(path), fetch(path)]
    check_full(rs, exp)
    check(not rs[1].hit, "private response was served as HIT")
    check_count(path, 2)


@test
def no_max_age_not_cached():
    path, exp = url("nomaxage", cc="none")
    rs = [fetch(path), fetch(path)]
    check_full(rs, exp)
    check_count(path, 2)


@test
def non_200_passed_through_not_cached():
    path, exp = url("notfound", status=404, size=300)
    rs = [fetch(path), fetch(path)]
    for i, r in enumerate(rs):
        check(r.error is None, f"request {i}: error {r.error}")
        check(r.status == 404, f"request {i}: status {r.status}, expected 404 passed through")
        check(r.body == exp, f"request {i}: body differs")
        check(not r.hit, f"request {i}: 404 served as HIT")
    check_count(path, 2)


@test
def post_and_head_pass_through():
    path, _ = url("post", cc="max-age=60")
    r1 = fetch(path, method="POST", body=b"hello")
    r2 = fetch(path, method="POST", body=b"hello")
    check(r1.status == 200 and r2.status == 200, f"POST statuses {r1.status}, {r2.status}")
    check(not r2.hit, "POST served as HIT")
    check_count(path, 2, method="POST")
    r3 = fetch(path, method="HEAD")
    check(r3.status == 200 and r3.body == b"", f"HEAD: {r3}")
    check_count(path, 1, method="HEAD")
    check_count(path, 0, method="GET")


# ----------------------------------------------------------------------------
# Tests: coalescing
# ----------------------------------------------------------------------------


@test
def coalesce_simultaneous():
    path, exp = url("simul", size=2 * 1024 * 1024, chunks=20, delay=0.1)
    rs = parallel([dict(path=path) for _ in range(5)])
    check_full(rs, exp)
    check(not any(r.hit for r in rs), "a client got a HIT although the fetch was still running")
    check_count(path, 1)


@test
def coalesce_staggered_catch_up():
    # first_delay: first two clients join before headers exist,
    # the rest join mid-body and need the catch-up snapshot.
    path, exp = url("stagger", size=2 * 1024 * 1024, chunks=20, delay=0.1, first_delay=0.5)
    # The fetch takes ~0.5 + 19*0.1 = 2.4 s. Clients starting before ~1.8 s MUST have been
    # followers (no HIT header) - otherwise the catch-up path wasn't exercised at all.
    delays = [0, 0.2, 0.8, 1.3, 1.8, 2.3]
    rs = parallel([dict(path=path, start_delay=d) for d in delays])
    check_full(rs, exp)
    for d, r in zip(delays, rs):
        if d <= 1.8:
            check(not r.hit, f"client starting at {d}s was a HIT, so catch-up was never tested")
    check_count(path, 1)
    r = fetch(path)
    check(r.hit, "request after the coalesced fetch is not a HIT")
    check_full([r], exp, "hit")


@test
def coalesce_chunked_origin():
    path, exp = url("chunked", size=1024 * 1024, chunks=10, delay=0.1, mode="chunked")
    rs = parallel([dict(path=path, start_delay=d) for d in (0, 0.3, 0.6)])
    check_full(rs, exp)
    check_count(path, 1)
    check(fetch(path).hit, "chunked response was not cached")


@test
def coalesce_50_clients():
    path, exp = url("fifty", size=512 * 1024, chunks=10, delay=0.1)
    before = worker_connections()
    rs = parallel([dict(path=path, start_delay=random.uniform(0, 0.8)) for _ in range(50)])
    after = worker_connections()
    check_full(rs, exp)
    check_count(path, 1)
    if before is not None and after is not None:
        spread = {w: after[w] - before.get(w, 0) for w in after}
        used = sum(1 for v in spread.values() if v > 0)
        note(f"coalesce_50_clients: connections per worker {spread} -> {used} worker(s) involved"
             + ("" if used > 1 else " (run Envoy with --concurrency > 1 to test cross-worker coalescing)"))


@test
def coalesce_many_keys():
    keys = [url(f"key{i}", size=256 * 1024, chunks=5, delay=0.1) for i in range(10)]
    specs, expect = [], []
    for path, exp in keys:
        for _ in range(5):
            specs.append(dict(path=path, start_delay=random.uniform(0, 0.4)))
            expect.append(exp)
    rs = parallel(specs)
    for i, (r, exp) in enumerate(zip(rs, expect)):
        check_full([r], exp, f"client {i}")
    for path, _ in keys:
        check_count(path, 1)


@test
def coalesce_no_store_shared_but_not_stored():
    # no-store forbids *storing* a response. Concurrent requests may still share the
    # response that is in flight anyway (it is never written to the cache).
    # Personal responses (private, Set-Cookie) are NOT shared - see http_private_not_coalesced.
    path, exp = url("nocache_coal", size=1024 * 1024, chunks=10, delay=0.1, cc="no-store")
    rs = parallel([dict(path=path, start_delay=d) for d in (0, 0.3, 0.6)])
    check_full(rs, exp)
    check_count(path, 1)
    r = fetch(path)
    check(not r.hit, "no-store response was stored")
    check_count(path, 2)


# ----------------------------------------------------------------------------
# Tests: client disconnects
# ----------------------------------------------------------------------------


@test
def follower_disconnect_others_unaffected():
    path, exp = url("fdisc", size=2 * 1024 * 1024, chunks=20, delay=0.1)
    specs = [dict(path=path, start_delay=d) for d in (0, 0.2, 0.4, 0.6)]
    specs[2]["read_bytes"] = 100_000
    rs = parallel(specs)
    check_full([rs[0], rs[1], rs[3]], exp)
    check_count(path, 1)
    check(fetch(path).hit, "fetch did not complete into the cache after a follower left")


@test
def leader_disconnect_others_unaffected():
    path, exp = url("ldisc", size=2 * 1024 * 1024, chunks=20, delay=0.1)
    specs = [dict(path=path, start_delay=d) for d in (0, 0.3, 0.6)]
    specs[0]["read_bytes"] = 50_000   # the client that triggered the fetch leaves
    rs = parallel(specs)
    check_full(rs[1:], exp)
    check_count(path, 1)
    check(fetch(path).hit, "fetch did not complete into the cache after the leader left")


@test
def all_clients_disconnect_fetch_completes_into_cache():
    # The origin fetch is independent of the clients: when all of them leave, it
    # keeps going and fills the cache (popular objects are usually requested again).
    path, exp = url("alldisc", size=1024 * 1024, chunks=10, delay=0.1)
    rs = parallel([dict(path=path, start_delay=d, read_bytes=20_000) for d in (0, 0.2, 0.4)])
    check(all(r.disconnected for r in rs), f"clients did not get far enough to disconnect: {rs}")
    time.sleep(1.5)  # let the origin finish
    r = fetch(path)
    check_full([r], exp, "after")
    check(r.hit, "fetch was cancelled or not cached after all clients left")
    check_count(path, 1)


# ----------------------------------------------------------------------------
# Tests: origin failures
# ----------------------------------------------------------------------------


def _check_not_complete(rs, size):
    for i, r in enumerate(rs):
        complete = r.error is None and r.status == 200 and len(r.body) == size
        check(not complete, f"client {i} got a 'complete' response from an aborted origin: {r}")


@test
def origin_abort_mid_body():
    size = 1024 * 1024
    path, _ = url("abort", size=size, chunks=10, delay=0.1, mode="abort", abort_after=5)
    rs = parallel([dict(path=path, start_delay=d) for d in (0, 0.2, 0.4)])
    _check_not_complete(rs, size)
    check_count(path, 1)
    r = fetch(path)
    check(not r.hit, "truncated response was cached!")
    check_count(path, 2)


@test
def origin_abort_mid_body_chunked():
    size = 1024 * 1024
    path, _ = url("abortch", size=size, chunks=10, delay=0.1, mode="abort",
                  abort_after=5, framing="chunked")
    rs = parallel([dict(path=path, start_delay=d) for d in (0, 0.2, 0.4)])
    _check_not_complete(rs, size)
    r = fetch(path)
    check(not r.hit, "truncated chunked response was cached!")


@test
def origin_abort_before_headers():
    path, _ = url("abortearly", mode="abort_early", first_delay=0.5)
    rs = parallel([dict(path=path, start_delay=d) for d in (0, 0.1, 0.2)])
    for i, r in enumerate(rs):
        check(r.status is not None and 500 <= r.status < 600,
              f"client {i}: expected a 5xx, got {r}")
    before = COUNTS.get("GET", path)
    check(before == 1, f"origin saw {before} requests, expected 1 (coalesced)")
    r = fetch(path)
    check(not r.hit, "error was cached")
    check_count(path, 2)


@test
def http2_clients_coalesce():
    if not shutil.which("curl"):
        raise Skip("curl not installed")
    path, exp = url("h2", size=1024 * 1024, chunks=10, delay=0.1)
    with tempfile.TemporaryDirectory() as d:
        procs = []
        for i in range(3):
            time.sleep(0.2)
            procs.append(subprocess.Popen(
                ["curl", "-s", "--http2-prior-knowledge", "-o", f"{d}/{i}",
                 "-w", "%{http_code} %{http_version}",
                 f"http://{ENVOY[0]}:{ENVOY[1]}{path}"],
                stdout=subprocess.PIPE, text=True))
        outs = [p.communicate()[0] for p in procs]
        if any(not o.startswith("200 2") for o in outs):
            raise Skip(f"listener does not speak h2c (curl said {outs})")
        for i in range(3):
            with open(f"{d}/{i}", "rb") as f:
                check(f.read() == exp, f"h2 client {i}: body differs")
    check_count(path, 1)


# ----------------------------------------------------------------------------
# Tests: size limits and LRU eviction
# (limits come from --max-object-mib / --max-total-mib, default 32 / 256)
# ----------------------------------------------------------------------------

MIB = 1024 * 1024
LIMITS = {"object": 32 * MIB, "total": 256 * MIB}


@test
def object_over_limit_coalesced_but_not_cached():
    # Everyone joins BEFORE the headers arrive -> they all share the one fetch.
    size = LIMITS["object"] + MIB
    path, exp = url("toobig", size=size, chunks=20, delay=0.05, first_delay=0.5)
    rs = parallel([dict(path=path, start_delay=d) for d in (0, 0.1, 0.2)])
    check_full(rs, exp)
    check_count(path, 1)
    r = fetch(path)
    check_full([r], exp, "after")
    check(not r.hit, "object larger than the per-object limit was cached")
    check_count(path, 2)


@test
def object_over_limit_late_joiner_gets_full_body():
    # Joins AFTER the headers, when there is no buffer to catch up from.
    # It must still get the complete, correct body (from its own origin fetch).
    for name, extra in (("toobig_cl", {}), ("toobig_chunked", {"mode": "chunked"})):
        size = LIMITS["object"] + 4 * MIB
        path, exp = url(name, size=size, chunks=40, delay=0.05, **extra)
        # 1.2 s: mid-stream, still under the limit for chunked (buffer exists)
        # 1.9 s: past the limit (buffer dropped) - must be rejected, not half-served
        rs = parallel([dict(path=path), dict(path=path, start_delay=1.2),
                       dict(path=path, start_delay=1.9)])
        check_full(rs, exp, name)
        n = COUNTS.get("GET", path)
        check(n in (1, 2, 3), f"{name}: origin saw {n} requests")


@test
def object_under_limit_cached():
    path, exp = url("justfits", size=LIMITS["object"] - MIB)
    check_full([fetch(path)], exp, "miss")
    r = fetch(path)
    check(r.hit, "object just under the per-object limit was not cached")
    check_full([r], exp, "hit")


@test
def lru_evicts_least_recently_used():
    # Objects of 3/4 of the object limit; enough of them to overflow the total budget
    # by one. 'hot' is touched after every insert and must survive; 'cold' is the
    # least recently used of the batch and must be the one that is evicted.
    obj = LIMITS["object"] * 3 // 4
    fits = LIMITS["total"] // obj            # how many such objects fit
    hot_path, hot_exp = url("hot", size=obj)
    cold_path, cold_exp = url("cold", size=obj)
    check_full([fetch(hot_path)], hot_exp, "hot miss")
    check_full([fetch(cold_path)], cold_exp, "cold miss")
    for i in range(fits - 1):                # hot + cold + (fits-1) fillers = fits+1 objects
        filler, filler_exp = url(f"filler{i}", size=obj)
        check_full([fetch(filler)], filler_exp, f"filler {i}")
        r = fetch(hot_path)
        check(r.hit, f"hot object was evicted after filler {i} although it was just used")
        check(r.body == hot_exp, "hot object body corrupted")
    r = fetch(cold_path)
    check_full([r], cold_exp, "cold after")
    check(not r.hit, f"cold object survived although {fits + 1} objects of "
                     f"{obj // MIB} MiB cannot fit into {LIMITS['total'] // MIB} MiB")
    check_count(cold_path, 2)
    check(fetch(hot_path).hit, "hot object did not survive")


@test
def lru_many_small_objects_still_work():
    # Lots of inserts/evictions of small objects must not corrupt anything.
    paths = [url(f"small{i}", size=64 * 1024) for i in range(200)]
    for path, exp in paths:
        check_full([fetch(path)], exp)
    for path, exp in paths[-20:]:
        r = fetch(path)
        check(r.hit, "recently inserted small object is not a HIT")
        check(r.body == exp, "small object body corrupted")


# ----------------------------------------------------------------------------
# Tests: slow clients (watermarks, push-mode limit)
# ----------------------------------------------------------------------------


@test
def pull_mode_slow_reader_gets_exact_body():
    # Normal-sized object, client much slower than the origin: it is paused by
    # the watermark and resumed many times - every byte must arrive, in order.
    size = 8 * MIB
    path, exp = url("pull_slow", size=size, chunks=16)
    r = fetch(path, read_rate=4 * MIB, rcvbuf=64 * 1024, timeout=60)
    check_full([r], exp, "slow reader")


@test
def push_mode_stalling_reader_gets_exact_body():
    # Oversized object (push mode). The client stalls three times for 1 s, so it
    # goes above the high watermark, then catches up at full speed. Each stall
    # stays well under the push-mode limit, so it must NOT be reset, and it must
    # receive every byte: nothing may be dropped while it is above the watermark.
    size = LIMITS["object"] + 32 * MIB          # 64 MiB with default limits
    path, exp = url("push_stall", size=size, chunks=64, delay=0.05)   # ~20 MiB/s
    stalls = [(4 * MIB, 1.0), (24 * MIB, 1.0), (44 * MIB, 1.0)]
    r = fetch(path, stalls=stalls, rcvbuf=64 * 1024, timeout=60)
    check_full([r], exp, "stalling reader")


@test
def push_mode_too_slow_client_reset_fast_one_unaffected():
    # Oversized object from a fast origin. One client reads at 2 MiB/s and falls
    # far behind -> it must be cut off (not served a "complete" wrong body).
    # A fast client on the same fetch must be completely unaffected.
    size = 100 * MIB
    path, exp = url("push_reset", size=size, chunks=50, first_delay=0.5)
    slow, fast = parallel([
        dict(path=path, read_rate=2 * MIB, rcvbuf=64 * 1024, timeout=60),
        dict(path=path, start_delay=0.1, timeout=60),
    ])
    check_full([fast], exp, "fast client")
    check(slow.error is not None and len(slow.body) < size,
          f"slow client was not cut off: {slow}")
    check(slow.body == exp[:len(slow.body)], "slow client's partial body is corrupted")
    check_count(path, 1)


# ----------------------------------------------------------------------------
# Tests: requests with cookies
# Requests carrying a Cookie header bypass the cache entirely: the response may be
# personalised, and the safe default is not to share or store it.
# ----------------------------------------------------------------------------


@test
def request_with_cookie_bypasses_cache():
    path, exp = url("cookie_req")
    for i in range(2):
        r = fetch(path, headers={"Cookie": "session=alice"})
        check_full([r], exp, f"cookie request {i}")
        check(not r.hit, "request with Cookie was served from cache")
    check_count(path, 2)
    r = fetch(path)
    check(not r.hit, "a response fetched for a Cookie request was stored in the cache")
    check_count(path, 3)
    check(fetch(path).hit, "plain requests are not cached any more")


# ----------------------------------------------------------------------------
# Tests: the leader's request headers must not decide what everyone else gets
# ----------------------------------------------------------------------------


@test
def leader_range_does_not_leak_to_followers():
    path, exp = url("range", size=1024 * 1024, chunks=10, delay=0.1, first_delay=0.3)
    rs = parallel([dict(path=path, headers={"Range": "bytes=0-99"}),
                   dict(path=path, start_delay=0.1)])
    leader, plain = rs
    check(leader.error is None and (
        (leader.status == 206 and leader.body == exp[:100]) or
        (leader.status == 200 and leader.body == exp)),
        f"Range client got a wrong answer: {leader}")
    check(plain.status == 200 and plain.body == exp,
          f"plain client got {plain}: the first client's Range header leaked into the shared fetch")
    later = fetch(path)
    check(later.status == 200 and later.body == exp, f"later plain request got {later}")


@test
def leader_conditional_does_not_leak_to_followers():
    rid = uuid.uuid4().hex
    path, exp = url("inm", id=rid, size=256 * 1024, first_delay=0.3)
    rs = parallel([dict(path=path, headers={"If-None-Match": f'"{rid}"'}),
                   dict(path=path, start_delay=0.1)])
    cond, plain = rs
    check(cond.status in (200, 304), f"conditional client got {cond}")
    check(plain.status == 200 and plain.body == exp,
          f"plain client got {plain}: the first client's If-None-Match leaked into the shared fetch")


# ----------------------------------------------------------------------------
# Tests: HTTP caching rules (RFC 9111)
# ----------------------------------------------------------------------------


@test
def http_no_cache_requires_revalidation():
    path, _ = url("nocache", cc="no-cache, max-age=60")
    fetch(path)
    # The cache does not implement revalidation, so no-cache responses are not stored.
    check(not fetch(path).hit, "'no-cache' means: do not serve without revalidating")


@test
def http_cache_control_case_insensitive():
    path, _ = url("upper", cc="MAX-AGE=60")
    fetch(path)
    check(fetch(path).hit, "directives are case-insensitive; 'MAX-AGE=60' was not cached")


@test
def http_s_maxage_wins_for_shared_cache():
    path, _ = url("smaxage", cc="max-age=60, s-maxage=1")
    fetch(path)
    time.sleep(1.5)
    check(not fetch(path).hit, "s-maxage (for shared caches) should override max-age")


@test
def http_origin_age_header_respected():
    path, _ = url("age", cc="max-age=3", age="2")
    fetch(path)
    time.sleep(1.5)
    check(not fetch(path).hit, "origin said Age: 2 with max-age=3, so it was stale after ~1 s")


@test
def http_hit_carries_age_header():
    path, _ = url("hitage")
    fetch(path)
    time.sleep(1.1)
    r = fetch(path)
    check(r.hit, "not a HIT")
    check(int(r.headers.get("age", "0")) >= 1, f"HIT without a correct Age header: {r.headers}")


@test
def http_set_cookie_not_shared():
    path, _ = url("cookie", set_cookie="1")
    fetch(path)
    r = fetch(path)
    check(not r.hit, "a response with Set-Cookie was served from cache to another client")


@test
def http_authorization_request_not_stored():
    path, _ = url("auth")
    fetch(path, headers={"Authorization": "Bearer alice"})
    r = fetch(path)
    check(not r.hit, "response to an Authorization request was stored and served to someone else")


@test
def http_private_not_coalesced():
    path, exp = url("privcoal", size=512 * 1024, chunks=5, delay=0.1, cc="private")
    rs = parallel([dict(path=path, start_delay=d) for d in (0, 0.1, 0.2)])
    check_full(rs, exp)
    check_count(path, 3)


# Must stay last: stops the origin.
@test
def origin_down():
    ORIGIN.stop()
    time.sleep(0.3)
    try:
        path, _ = url("down")
        rs = parallel([dict(path=path) for _ in range(3)])
        for i, r in enumerate(rs):
            check(r.status is not None and 500 <= r.status < 600,
                  f"client {i}: expected 5xx with origin down, got {r}")
    finally:
        ORIGIN.start()


# ----------------------------------------------------------------------------
# Runner
# ----------------------------------------------------------------------------

ORIGIN = None


def main():
    global ORIGIN, ENVOY
    ap = argparse.ArgumentParser()
    ap.add_argument("-k", help="only run tests whose name contains this")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--envoy", default="127.0.0.1:10000")
    ap.add_argument("--origin-port", type=int, default=ORIGIN_PORT)
    ap.add_argument("--max-object-mib", type=int, default=32)
    ap.add_argument("--max-total-mib", type=int, default=256)
    args = ap.parse_args()
    LIMITS["object"] = args.max_object_mib * MIB
    LIMITS["total"] = args.max_total_mib * MIB

    if args.list:
        for t in TESTS:
            print(t.__name__)
        return 0

    host, port = args.envoy.split(":")
    ENVOY = (host, int(port))
    ORIGIN = Origin(args.origin_port)
    try:
        ORIGIN.start()
    except OSError as e:
        print(f"Cannot bind origin port {args.origin_port}: {e}\n-> is something else using that port?")
        return 2

    if not envoy_alive():
        print(f"Envoy at {args.envoy} does not answer correctly. Is it running?")
        return 2

    tests = [t for t in TESTS if not args.k or args.k in t.__name__]
    green, red, yellow = "\033[32m", "\033[31m", "\033[33m"
    reset = "\033[0m"
    passed = failed = skipped = 0
    for t in tests:
        start = time.time()
        try:
            t()
            status, color = "PASS", green
            passed += 1
            detail = ""
        except Fail as e:
            status, color, detail = "FAIL", red, f"  -> {e}"
            failed += 1
        except Skip as e:
            status, color, detail = "SKIP", yellow, f"  -> {e}"
            skipped += 1
        except Exception:
            status, color = "ERROR", red
            detail = "\n" + traceback_str()
            failed += 1
        print(f"{color}{status:5}{reset} {t.__name__:42} {time.time() - start:5.1f}s{detail}")
        if not envoy_alive():
            print(f"{red}Envoy stopped answering after {t.__name__}! "
                  f"Check its terminal for a crash/backtrace.{reset}")
            failed += 1
            break

    if NOTES:
        print("\nInfo:")
        for n in NOTES:
            print("  - " + n)
    print(f"\n{passed} passed, {failed} failed, {skipped} skipped")
    ORIGIN.stop()
    return 1 if failed else 0


def traceback_str():
    import traceback
    return traceback.format_exc()


if __name__ == "__main__":
    sys.exit(main())