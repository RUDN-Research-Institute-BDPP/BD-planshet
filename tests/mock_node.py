#!/usr/bin/env python3
"""Mock storage node for Planshet sync tests (SYNC_SPEC section 6).

Stdlib only. Mirrors the real node as closely as it needs to:

  GET  /api/persons?limit=&q=   -> 3 synthetic people          (X-Api-Key)
  GET  /api/ingest/status       -> channel lights, always 200  (X-Api-Key)
  POST /api/ingest/planshet     -> multipart batch intake      (X-Api-Key)

Batch validation is a port of storage-core/common/manifest.py:parse_manifest and
storage-core/webapp/routers/api_ingest.py:planshet_ingest -- same batch_id regex,
same "multipart parts must exactly match manifest.files" rule, same sha256/size
checks, same 202 received / 200 duplicate answers.

Accepted batches are written to --dir as <machine>/planshet/<batch_id>/ with
manifest.json and a _complete marker (written last), like the node does.

Failure modes for the test plan:
  --unauth        every authenticated request answers 401
  --fail-next N   the next N ingest requests answer 500 (then it heals)

Both can also be flipped at runtime without a restart:
  curl -X POST localhost:8765/_control -d '{"unauth": true}'
  curl -X POST localhost:8765/_control -d '{"fail_next": 2}'
  curl localhost:8765/_control

Usage:
    python tests/mock_node.py --port 8765 --key devkey --dir tests/_batches
"""

import argparse
import hashlib
import json
import pathlib
import re
import shutil
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# --- contract constants (storage-core/common/manifest.py) ---------------------

BATCH_ID_RE = re.compile(r"^\d{8}T\d{6}Z_[0-9a-z]{8}$")
SOURCE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MANIFEST_NAME = "manifest.json"
COMPLETE_MARKER = "_complete"

PERSONS = [
    {"id": "01J0AAAA0000000000000001", "display_code": "A-014",
     "full_name": "Иванов Иван Иванович", "status": "active"},
    {"id": "01J0AAAA0000000000000002", "display_code": "B-207",
     "full_name": "Петрова Мария Сергеевна", "status": "active"},
    {"id": "01J0AAAA0000000000000003", "display_code": "C-031",
     "full_name": "Сидоров Пётр Алексеевич", "status": "active"},
]

STATE = {"unauth": False, "fail_next": 0}
STATE_LOCK = threading.Lock()
CONFIG = {"key": "devkey", "dir": pathlib.Path("tests/_batches"), "lax": False}


class BadBatch(Exception):
    """422: the batch is structurally invalid."""


# --- manifest validation (port of common/manifest.py) -------------------------

def clean_relative_path(path):
    if not isinstance(path, str) or not path:
        raise BadBatch("manifest: file path must be a non-empty string")
    if "\\" in path:
        raise BadBatch(f"manifest: backslash in path {path!r}")
    if path.startswith("/"):
        raise BadBatch(f"manifest: absolute path {path!r}")
    parts = path.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise BadBatch(f"manifest: unsafe path {path!r}")
    if parts[0] in (MANIFEST_NAME, COMPLETE_MARKER):
        raise BadBatch(f"manifest: reserved name in path {path!r}")
    return path


def parse_manifest(data):
    if not isinstance(data, dict):
        raise BadBatch("manifest: root must be a JSON object")
    if data.get("manifest_version") != 1:
        raise BadBatch(f"manifest: unsupported manifest_version {data.get('manifest_version')!r}")

    batch_id = data.get("batch_id")
    if not isinstance(batch_id, str) or not BATCH_ID_RE.match(batch_id):
        raise BadBatch(f"manifest: bad batch_id {batch_id!r} (want <UTC yyyymmddThhmmssZ>_<8 hex>)")

    source = data.get("source")
    if not isinstance(source, str) or not SOURCE_RE.match(source):
        raise BadBatch(f"manifest: bad source {source!r}")
    if source != "planshet":
        raise BadBatch("source must be 'planshet'")

    machine = data.get("machine")
    if not isinstance(machine, str) or not machine.strip():
        raise BadBatch("manifest: machine must be a non-empty string")

    raw_files = data.get("files")
    if not isinstance(raw_files, list):
        raise BadBatch("manifest: files must be a list")
    files, seen = [], set()
    for item in raw_files:
        if not isinstance(item, dict):
            raise BadBatch("manifest: files[] entries must be objects")
        path = clean_relative_path(item.get("path"))
        if path in seen:
            raise BadBatch(f"manifest: duplicate path {path!r}")
        seen.add(path)
        sha256 = item.get("sha256")
        if not isinstance(sha256, str) or not SHA256_RE.match(sha256):
            raise BadBatch(f"manifest: bad sha256 for {path!r}")
        size = item.get("size")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise BadBatch(f"manifest: bad size for {path!r}")
        files.append({"path": path, "sha256": sha256, "size": size})

    for name in ("exporter", "counts", "source_state"):
        value = data.get(name) or {}
        if not isinstance(value, dict):
            raise BadBatch(f"manifest: {name} must be an object")
    if not isinstance(data.get("created_at", ""), str):
        raise BadBatch("manifest: created_at must be a string")

    return {"batch_id": batch_id, "machine": machine, "files": files, "raw": data}


def check_records_jsonl(blob):
    """Stub-only strictness: registry/records.jsonl must be readable JSON lines
    carrying a uid. The real node defers this to the ingester; here it turns a
    silent data bug into a loud 422. Disable with --lax."""
    text = blob.decode("utf-8")
    lines = [line for line in text.split("\n") if line.strip()]
    if not lines:
        raise BadBatch("registry/records.jsonl is empty")
    for number, line in enumerate(lines, 1):
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise BadBatch(f"registry/records.jsonl line {number}: not JSON ({exc})") from exc
        for field in ("uid", "patient_ref", "test", "created_at", "payload"):
            if field not in record:
                raise BadBatch(f"registry/records.jsonl line {number}: missing {field!r}")
        if record["test"] not in ("words", "motor", "draw"):
            raise BadBatch(f"registry/records.jsonl line {number}: bad test {record['test']!r}")
    return len(lines)


# --- multipart parsing --------------------------------------------------------

def parse_multipart(body, boundary):
    """Return [(name, filename, bytes)]. Enough for well-formed browser FormData."""
    sep = b"--" + boundary.encode()
    chunks = body.split(sep)
    if len(chunks) < 2:
        raise BadBatch("multipart: no parts found")
    parts = []
    for chunk in chunks[1:]:
        if chunk[:2] == b"--":  # closing boundary
            break
        chunk = chunk[2:] if chunk[:2] == b"\r\n" else chunk.lstrip(b"\r\n")
        head, _, data = chunk.partition(b"\r\n\r\n")
        if not _:
            raise BadBatch("multipart: part without headers")
        if data.endswith(b"\r\n"):
            data = data[:-2]
        name = filename = None
        for header in head.decode("utf-8", "replace").split("\r\n"):
            if header.lower().startswith("content-disposition:"):
                for token in header.split(";")[1:]:
                    key, _, value = token.strip().partition("=")
                    value = value.strip('"')
                    if key == "name":
                        name = value
                    elif key == "filename":
                        filename = value
        if name is None:
            raise BadBatch("multipart: part without a name")
        parts.append((name, filename, data))
    return parts


# --- HTTP handler -------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "mock-node/1.0"

    def log_message(self, fmt, *args):  # quieter, one line per request
        sys.stderr.write("  %s\n" % (fmt % args))

    # -- helpers --

    def cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "X-Api-Key, Content-Type")
        self.send_header("Access-Control-Max-Age", "600")

    def reply(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.cors()
        self.end_headers()
        self.wfile.write(body)

    def authorized(self):
        with STATE_LOCK:
            unauth = STATE["unauth"]
        if unauth:
            self.reply(401, {"error": "unauthorized", "detail": "--unauth mode"})
            return False
        if self.headers.get("X-Api-Key", "") != CONFIG["key"]:
            self.reply(401, {"error": "unauthorized", "detail": "unknown api key"})
            return False
        return True

    def read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    # -- routes --

    def do_OPTIONS(self):
        self.send_response(204)
        self.cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        route = urlparse(self.path)
        if route.path == "/_control":
            with STATE_LOCK:
                return self.reply(200, dict(STATE))
        if route.path == "/api/persons":
            if not self.authorized():
                return None
            query = parse_qs(route.query)
            rows = PERSONS
            needle = (query.get("q") or [""])[0].strip().lower()
            if needle:
                rows = [p for p in rows
                        if needle in p["display_code"].lower() or needle in p["full_name"].lower()]
            limit = int((query.get("limit") or ["500"])[0])
            return self.reply(200, rows[:limit])
        if route.path == "/api/ingest/status":
            if not self.authorized():
                return None
            return self.reply(200, [{"machine": "planshet", "source": "planshet",
                                     "freshness": "green", "pending": 0, "last_error": None}])
        return self.reply(404, {"error": "not_found", "detail": route.path})

    def do_POST(self):
        route = urlparse(self.path)
        if route.path == "/_control":
            try:
                payload = json.loads(self.read_body() or b"{}")
            except ValueError as exc:
                return self.reply(400, {"error": "bad_json", "detail": str(exc)})
            with STATE_LOCK:
                if "unauth" in payload:
                    STATE["unauth"] = bool(payload["unauth"])
                if "fail_next" in payload:
                    STATE["fail_next"] = int(payload["fail_next"])
                snapshot = dict(STATE)
            print(f"[control] {snapshot}", flush=True)
            return self.reply(200, snapshot)

        if route.path != "/api/ingest/planshet":
            return self.reply(404, {"error": "not_found", "detail": route.path})
        if not self.authorized():
            return None

        with STATE_LOCK:
            if STATE["fail_next"] > 0:
                STATE["fail_next"] -= 1
                remaining = STATE["fail_next"]
                fail = True
            else:
                fail = False
        if fail:
            print(f"[ingest] forced 500 (--fail-next, {remaining} left)", flush=True)
            return self.reply(500, {"error": "internal", "detail": "forced failure"})

        body = self.read_body()
        content_type = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type or "boundary=" not in content_type:
            return self.reply(422, {"error": "validation", "detail": "expected multipart/form-data"})
        boundary = content_type.split("boundary=", 1)[1].strip().strip('"')

        try:
            return self.ingest(parse_multipart(body, boundary))
        except BadBatch as exc:
            print(f"[ingest] 422 {exc}", flush=True)
            return self.reply(422, {"error": "validation", "detail": str(exc)})

    def ingest(self, parts):
        by_name = {}
        for name, _filename, data in parts:
            if name in by_name:
                raise BadBatch(f"multipart: duplicate part {name!r}")
            by_name[name] = data
        if "manifest" not in by_name:
            raise BadBatch("multipart part 'manifest' is required")

        manifest_bytes = by_name.pop("manifest")
        try:
            manifest = parse_manifest(json.loads(manifest_bytes))
        except ValueError as exc:
            raise BadBatch(f"bad manifest: {exc}") from exc

        batch_id, machine = manifest["batch_id"], manifest["machine"]
        batch_dir = CONFIG["dir"] / machine / "planshet" / batch_id
        if batch_dir.exists():
            print(f"[ingest] 200 duplicate {batch_id}", flush=True)
            return self.reply(200, {"batch_id": batch_id, "status": "duplicate"})

        expected = {entry["path"]: entry for entry in manifest["files"]}
        missing = sorted(set(expected) - set(by_name))
        extra = sorted(set(by_name) - set(expected))
        if missing or extra:
            raise BadBatch(f"multipart does not match manifest.files: "
                           f"missing={missing} extra={extra}")

        for path, entry in expected.items():
            data = by_name[path]
            digest = hashlib.sha256(data).hexdigest()
            if digest != entry["sha256"]:
                raise BadBatch(f"sha256 mismatch for {path} "
                               f"(manifest {entry['sha256'][:12]}..., body {digest[:12]}...)")
            if len(data) != entry["size"]:
                raise BadBatch(f"size mismatch for {path} ({len(data)} != {entry['size']})")

        records = 0
        if not CONFIG["lax"] and "registry/records.jsonl" in by_name:
            records = check_records_jsonl(by_name["registry/records.jsonl"])

        try:
            for path in expected:
                destination = batch_dir / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(by_name[path])
            (batch_dir / MANIFEST_NAME).write_bytes(manifest_bytes)
            (batch_dir / COMPLETE_MARKER).touch()  # written LAST
        except OSError:
            shutil.rmtree(batch_dir, ignore_errors=True)
            raise

        print(f"[ingest] 202 received {batch_id} machine={machine} "
              f"files={len(expected)} records={records} -> {batch_dir}", flush=True)
        return self.reply(202, {"batch_id": batch_id, "status": "received"})


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--key", default="devkey", help="expected X-Api-Key")
    parser.add_argument("--dir", default="tests/_batches", help="where accepted batches land")
    parser.add_argument("--unauth", action="store_true", help="answer 401 to everything")
    parser.add_argument("--fail-next", type=int, default=0, metavar="N",
                        help="answer 500 to the next N ingest requests")
    parser.add_argument("--lax", action="store_true",
                        help="do not parse registry/records.jsonl")
    args = parser.parse_args()

    CONFIG["key"] = args.key
    CONFIG["dir"] = pathlib.Path(args.dir).resolve()
    CONFIG["lax"] = args.lax
    CONFIG["dir"].mkdir(parents=True, exist_ok=True)
    STATE["unauth"] = args.unauth
    STATE["fail_next"] = args.fail_next

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"mock node on http://{args.host}:{args.port}", flush=True)
    print(f"  api key : {args.key}", flush=True)
    print(f"  batches : {CONFIG['dir']}", flush=True)
    print(f"  state   : unauth={STATE['unauth']} fail_next={STATE['fail_next']}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbye", flush=True)
        server.server_close()


if __name__ == "__main__":
    main()
