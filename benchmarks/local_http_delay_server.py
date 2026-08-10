#!/usr/bin/env python3
"""Local delayed/throttled HTTP service for MWF performance tests.

Modes:
  HTTP/1.1: python benchmarks/local_http_delay_server.py --port 8765
  HTTP/2 TLS: python benchmarks/local_http_delay_server.py --port 8766 --http2

Endpoints:
  GET /health
  GET /transfer?bytes=65536&bps=1048576&delay_ms=5&chunk=4096

`bps=0` means unlimited. Throttling is per response/stream. HTTP/2 mode
auto-generates a one-day self-signed localhost certificate with openssl unless
--cert/--key are supplied.
"""
from __future__ import annotations

import argparse
import asyncio
import ssl
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


def parse_target(target: str):
    parsed = urlsplit(target)
    if parsed.path == "/health":
        return "health", {}
    if parsed.path != "/transfer":
        return "error", {}
    q = parse_qs(parsed.query)
    try:
        return "transfer", {
            "total_bytes": max(0, int(q.get("bytes", ["65536"])[0])),
            "bps": max(0, int(q.get("bps", ["0"])[0])),
            "delay_ms": max(0.0, float(q.get("delay_ms", ["0"])[0])),
            "chunk_size": max(256, int(q.get("chunk", ["4096"])[0])),
        }
    except ValueError:
        return "error", {}


async def read_h1_request(reader):
    line = await reader.readline()
    if not line:
        return None
    try:
        method, target, _ = line.decode("latin1").rstrip("\r\n").split(" ", 2)
    except ValueError:
        return None
    headers = {}
    while True:
        raw = await reader.readline()
        if not raw or raw in {b"\r\n", b"\n"}:
            break
        name, _, value = raw.decode("latin1").partition(":")
        headers[name.strip().lower()] = value.strip()
    length = int(headers.get("content-length", "0") or 0)
    if length:
        await reader.readexactly(length)
    return method, target, headers


async def h1_simple(writer, status, body, keep_alive=True):
    writer.write(
        f"HTTP/1.1 {status} {'OK' if status == 200 else 'Bad Request'}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Content-Type: application/octet-stream\r\n"
        f"Connection: {'keep-alive' if keep_alive else 'close'}\r\n\r\n".encode("ascii")
    )
    writer.write(body)
    await writer.drain()


async def h1_transfer(writer, total_bytes, bps, delay_ms, chunk_size):
    if delay_ms:
        await asyncio.sleep(delay_ms / 1000.0)
    writer.write(
        f"HTTP/1.1 200 OK\r\nContent-Length: {total_bytes}\r\n"
        "Content-Type: application/octet-stream\r\nConnection: keep-alive\r\n\r\n".encode("ascii")
    )
    await writer.drain()
    if not total_bytes:
        return
    chunk = b"x" * min(chunk_size, total_bytes)
    sent = 0
    started = asyncio.get_running_loop().time()
    while sent < total_bytes:
        amount = min(len(chunk), total_bytes - sent)
        writer.write(chunk[:amount])
        sent += amount
        await writer.drain()
        if bps and sent < total_bytes:
            remaining = started + sent / bps - asyncio.get_running_loop().time()
            if remaining > 0:
                await asyncio.sleep(remaining)


async def handle_h1(reader, writer):
    try:
        while True:
            req = await read_h1_request(reader)
            if req is None:
                return
            method, target, headers = req
            if method != "GET":
                await h1_simple(writer, 400, b"GET only", False)
                return
            kind, values = parse_target(target)
            if kind == "health":
                await h1_simple(writer, 200, b"ok")
            elif kind == "transfer":
                await h1_transfer(writer, **values)
            else:
                await h1_simple(writer, 400, b"bad request", False)
                return
            if headers.get("connection", "").lower() == "close":
                return
    except (ConnectionError, asyncio.IncompleteReadError, BrokenPipeError):
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


class H2Session:
    def __init__(self, reader, writer):
        from h2.config import H2Configuration
        from h2.connection import H2Connection
        self.reader = reader
        self.writer = writer
        self.conn = H2Connection(H2Configuration(client_side=False, header_encoding="utf-8"))
        self.lock = asyncio.Lock()
        self.flow_event = asyncio.Event()
        self.flow_event.set()
        self.tasks = set()

    async def flush_locked(self):
        data = self.conn.data_to_send()
        if data:
            self.writer.write(data)
            await self.writer.drain()

    async def headers(self, stream_id, headers, end_stream=False):
        async with self.lock:
            self.conn.send_headers(stream_id, headers, end_stream=end_stream)
            await self.flush_locked()

    async def data(self, stream_id, payload, end_stream=False):
        offset = 0
        while offset < len(payload):
            async with self.lock:
                try:
                    window = self.conn.local_flow_control_window(stream_id)
                except Exception:
                    return
                amount = min(window, self.conn.max_outbound_frame_size, len(payload) - offset)
                if amount:
                    final = end_stream and offset + amount == len(payload)
                    self.conn.send_data(stream_id, payload[offset:offset + amount], end_stream=final)
                    offset += amount
                    await self.flush_locked()
                    continue
                self.flow_event.clear()
            await self.flow_event.wait()
        if not payload and end_stream:
            async with self.lock:
                self.conn.end_stream(stream_id)
                await self.flush_locked()

    async def serve_stream(self, stream_id, target):
        kind, values = parse_target(target)
        if kind == "health":
            await self.headers(stream_id, [(":status", "200"), ("content-length", "2")])
            await self.data(stream_id, b"ok", True)
            return
        if kind != "transfer":
            body = b"bad request"
            await self.headers(stream_id, [(":status", "400"), ("content-length", str(len(body)))])
            await self.data(stream_id, body, True)
            return
        total = values["total_bytes"]
        bps = values["bps"]
        delay_ms = values["delay_ms"]
        chunk_size = values["chunk_size"]
        if delay_ms:
            await asyncio.sleep(delay_ms / 1000.0)
        await self.headers(
            stream_id,
            [(":status", "200"), ("content-length", str(total)), ("content-type", "application/octet-stream")],
            end_stream=(total == 0),
        )
        if not total:
            return
        chunk = b"x" * min(chunk_size, total)
        sent = 0
        started = asyncio.get_running_loop().time()
        while sent < total:
            amount = min(len(chunk), total - sent)
            await self.data(stream_id, chunk[:amount], end_stream=(sent + amount == total))
            sent += amount
            if bps and sent < total:
                remaining = started + sent / bps - asyncio.get_running_loop().time()
                if remaining > 0:
                    await asyncio.sleep(remaining)

    async def run(self):
        from h2.events import DataReceived, RequestReceived, StreamEnded, WindowUpdated
        self.conn.initiate_connection()
        async with self.lock:
            await self.flush_locked()
        targets = {}
        try:
            while True:
                incoming = await self.reader.read(65536)
                if not incoming:
                    break
                async with self.lock:
                    events = self.conn.receive_data(incoming)
                    for event in events:
                        if isinstance(event, DataReceived):
                            self.conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                        elif isinstance(event, RequestReceived):
                            targets[event.stream_id] = dict(event.headers).get(":path", "/")
                        elif isinstance(event, WindowUpdated):
                            self.flow_event.set()
                    await self.flush_locked()
                for event in events:
                    if isinstance(event, StreamEnded):
                        task = asyncio.create_task(self.serve_stream(event.stream_id, targets.pop(event.stream_id, "/")))
                        self.tasks.add(task)
                        task.add_done_callback(self.tasks.discard)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            for task in list(self.tasks):
                task.cancel()
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass


async def handle_h2(reader, writer):
    await H2Session(reader, writer).run()


def ephemeral_cert():
    tmp = tempfile.TemporaryDirectory(prefix="mwf-h2-cert-")
    root = Path(tmp.name)
    cert, key = root / "cert.pem", root / "key.pem"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(key), "-out", str(cert), "-days", "1",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return cert, key, tmp


async def main_async(args):
    ssl_context = None
    cert_tmp = None
    handler = handle_h1
    if args.http2:
        handler = handle_h2
        cert = Path(args.cert) if args.cert else None
        key = Path(args.key) if args.key else None
        if cert is None or key is None:
            cert, key, cert_tmp = ephemeral_cert()
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(str(cert), str(key))
        ssl_context.set_alpn_protocols(["h2"])
    server = await asyncio.start_server(handler, args.host, args.port, ssl=ssl_context, backlog=args.backlog, limit=2**20)
    print(f"mwf delay service {'h2' if args.http2 else 'h1'} {args.host}:{args.port}", flush=True)
    try:
        async with server:
            await server.serve_forever()
    finally:
        if cert_tmp:
            cert_tmp.cleanup()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--backlog", type=int, default=8192)
    p.add_argument("--http2", action="store_true")
    p.add_argument("--cert", default="")
    p.add_argument("--key", default="")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
