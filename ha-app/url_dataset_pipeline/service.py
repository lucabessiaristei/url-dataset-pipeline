#!/usr/bin/env python3
"""
Home Assistant side of the URL dataset pipeline:
- starts the generator every day at `daily_start` (after the free quotas reset), or on demand
- serves the ingress panel (status, live workers, progress, per-model record, daily quota, log)
- posts a summary notification when a run ends
"""

import importlib.util
import json
import os
import signal
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APP_DIR = os.environ.get("PIPELINE_APP_DIR", "/app")
GENERATOR = os.path.join(APP_DIR, "tools", "MULTI-PROVIDER_output_generator_API_v6.py")
UI_FILE = os.path.join(APP_DIR, "ui.html")
OPTIONS_FILE = os.environ.get("PIPELINE_OPTIONS_FILE", "/data/options.json")
DATA_ROOT = os.environ.get("PIPELINE_DATA_ROOT", "/share/url-dataset-pipeline")
DATA_DIR = os.path.join(DATA_ROOT, "in_out-s")
RUN_LOG = os.path.join(DATA_ROOT, "run.log")
PORT = int(os.environ.get("PIPELINE_PANEL_PORT", "8099"))
# Only Home Assistant's ingress proxy may talk to the panel (it handles authentication)
ALLOWED_CLIENTS = {"172.30.32.2", "127.0.0.1"}
PROVIDERS = ("gemini", "groq", "openrouter", "nvidia")

os.environ.setdefault("PIPELINE_DATA_DIR", DATA_DIR)
os.environ.setdefault("PIPELINE_LOG_FILE", os.path.join(DATA_ROOT, "generator.log"))


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def load_options():
    try:
        with open(OPTIONS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


OPTIONS = load_options()


def read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def next_start(now=None):
    now = now or datetime.now()
    hour, minute = (int(x) for x in str(OPTIONS.get("daily_start", "09:15")).split(":"))
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return target if target > now else target + timedelta(days=1)


# ---------------------------------------------------------------- generator stats (imported lazily)
_generator = None
_stats_cache = {"at": 0.0, "data": None}


def generator_module():
    global _generator
    if _generator is None:
        spec = importlib.util.spec_from_file_location("generator_v6", GENERATOR)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _generator = module
    return _generator


def stats():
    if time.time() - _stats_cache["at"] > 30 or _stats_cache["data"] is None:
        try:
            _stats_cache["data"] = generator_module().report_summary(days=10)
        except Exception as e:  # the panel must keep working even if the report is odd
            _stats_cache["data"] = {"models": [], "quota": {"days": [], "rows": []}, "error": str(e)}
        _stats_cache["at"] = time.time()
    return _stats_cache["data"]


_progress_cache = {"at": 0.0, "data": []}


def progress():
    if time.time() - _progress_cache["at"] < 15:
        return _progress_cache["data"]
    rows = []
    try:
        names = sorted(n for n in os.listdir(DATA_DIR) if n.startswith("working_split_IN--"))
    except OSError:
        names = []
    for name in names:
        suffix = name.split("--")[-1]
        in_dir = os.path.join(DATA_DIR, name)
        out_dir = os.path.join(DATA_DIR, f"working_split_OUT--API-{suffix}")
        inputs = {n for n in os.listdir(in_dir) if n.startswith("in_") and n.endswith(".json")}
        try:
            outputs = {n.replace("out_", "in_", 1) for n in os.listdir(out_dir) if n.endswith(".json")}
        except OSError:
            outputs = set()
        rows.append({"dir": name, "total": len(inputs), "done": len(inputs & outputs)})
    _progress_cache.update(at=time.time(), data=rows)
    return rows


# ---------------------------------------------------------------- runs
class Runner:
    def __init__(self):
        self.lock = threading.Lock()
        self.proc = None
        self.started = None
        self.reason = None
        self.stopping = False
        self.last_exit = None
        self.output = deque(maxlen=400)
        self.next_run = next_start()

    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, reason):
        with self.lock:
            if self.running():
                return False
            if not os.path.isdir(DATA_DIR):
                log(f"No dataset at {DATA_DIR}: copy in_out-s there first")
                return False
            args = [sys.executable, GENERATOR, "--dir", "all", "--headless",
                    "--max-hours", str(OPTIONS.get("max_hours", 20))]
            models = (OPTIONS.get("models") or "").strip()
            if models:
                args += ["--models", models]
            env = {**os.environ, "PIPELINE_PROGRESS_SECONDS": "60", "COLUMNS": "160"}
            for provider in PROVIDERS:
                key = (OPTIONS.get(f"{provider}_api_key") or "").strip()
                if key:
                    env[f"{provider.upper()}_API_KEY"] = key
            trim_log()
            self.output.clear()
            self.proc = subprocess.Popen(args, cwd=APP_DIR, env=env, text=True, bufsize=1,
                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.started = time.time()
            self.reason = reason
            self.stopping = False
            threading.Thread(target=self._pump, daemon=True).start()
            log(f"Run started ({reason})")
            return True

    def stop(self):
        with self.lock:
            if not self.running():
                return False
            # First SIGINT = clean stop: finished files are kept, requests in flight are dropped
            self.proc.send_signal(signal.SIGINT)
            self.stopping = True
            log("Stop requested")
            return True

    def _pump(self):
        proc = self.proc
        with open(RUN_LOG, "a", encoding="utf-8") as out:
            out.write(f"\n=== {datetime.now():%Y-%m-%d %H:%M} run started ({self.reason})\n")
            for line in proc.stdout:
                line = line.rstrip("\n")
                self.output.append(line)
                out.write(line + "\n")
                out.flush()
                print(line, flush=True)
            code = proc.wait()
            out.write(f"=== {datetime.now():%Y-%m-%d %H:%M} run ended (exit {code})\n")
        self.last_exit = code
        self.stopping = False
        _stats_cache["at"] = 0.0
        _progress_cache["at"] = 0.0
        log(f"Run ended (exit {code})")
        notify()

    def state(self):
        running = self.running()
        return {
            "running": running,
            "stopping": self.stopping and running,
            "started": datetime.fromtimestamp(self.started).isoformat(timespec="seconds") if self.started else None,
            "reason": self.reason,
            "last_exit": self.last_exit,
            "next_run": self.next_run.isoformat(timespec="minutes"),
        }


def trim_log(limit=5 * 1024 * 1024, keep=1024 * 1024):
    try:
        if os.path.getsize(RUN_LOG) > limit:
            with open(RUN_LOG, "rb") as f:
                f.seek(-keep, os.SEEK_END)
                tail = f.read()
            with open(RUN_LOG, "wb") as f:
                f.write(tail[tail.find(b"\n") + 1:])
    except OSError:
        pass


def log_tail(lines=200):
    try:
        with open(RUN_LOG, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - 64 * 1024))
            return f.read().decode("utf-8", "replace").splitlines()[-lines:]
    except OSError:
        return []


def notify():
    summary = read_json(os.path.join(DATA_DIR, "last_run.json"))
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not summary or not token:
        return
    lines = [f"{summary.get('done', 0)} files done in {summary.get('minutes', 0)} min, "
             f"{summary.get('files_left', '?')} left."]
    lines += [f"• {m}: {n}" for m, n in (summary.get("done_by_model") or {}).items()]
    if summary.get("retired"):
        lines.append("\nStopped: " + "; ".join(f"{k} ({v})" for k, v in summary["retired"].items()))
    payload = json.dumps({"title": "URL dataset pipeline", "message": "\n".join(lines),
                          "notification_id": "url_dataset_pipeline"}).encode()
    request = urllib.request.Request(
        "http://supervisor/core/api/services/persistent_notification/create", data=payload, method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        urllib.request.urlopen(request, timeout=10).read()
    except Exception as e:
        log(f"Could not post the notification: {e}")


def scheduler(runner):
    if OPTIONS.get("run_on_start"):
        runner.start("app start")
    while True:
        runner.next_run = next_start()
        log(f"Next run at {runner.next_run:%Y-%m-%d %H:%M}")
        while datetime.now() < runner.next_run:
            time.sleep(min(30, max(1, (runner.next_run - datetime.now()).total_seconds())))
        if not runner.start("schedule"):
            log("Scheduled run skipped: a run is already in progress")


# ---------------------------------------------------------------- panel
runner = Runner()


class Handler(BaseHTTPRequestHandler):
    server_version = "UrlDatasetPipeline/1.1"

    def log_message(self, fmt, *args):  # keep the app log for runs, not for every panel refresh
        pass

    def _allowed(self):
        if self.client_address[0] in ALLOWED_CLIENTS:
            return True
        self.send_error(403)
        return False

    def _json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if not self._allowed():
            return
        path = self.path.split("?", 1)[0].rstrip("/").rsplit("/", 2)
        route = "/".join(path[-2:]) if len(path) >= 2 and path[-2] == "api" else path[-1]
        if route in ("", "index.html"):
            with open(UI_FILE, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif route == "api/state":
            status = read_json(os.path.join(DATA_DIR, "run_status.json"))
            self._json({
                "runner": runner.state(),
                "status": status,
                "last_run": read_json(os.path.join(DATA_DIR, "last_run.json")),
                "progress": progress(),
                "log": list(runner.output) if runner.running() else log_tail(),
                "options": {k: OPTIONS.get(k) for k in ("daily_start", "max_hours", "models")},
            })
        elif route == "api/stats":
            self._json(stats())
        elif route == "api/download":
            self.send_download()
        else:
            self.send_error(404)

    def do_POST(self):
        if not self._allowed():
            return
        route = "/".join(self.path.split("?", 1)[0].rstrip("/").rsplit("/", 2)[-2:])
        if route == "api/run":
            self._json({"ok": runner.start("manual")})
        elif route == "api/stop":
            self._json({"ok": runner.stop()})
        else:
            self.send_error(404)

    def send_download(self):
        """Streams in_out-s as .tar.gz (inputs, outputs, report); backups and rejected raw files are left out."""
        skip = {"split_backup_cleaner", "RAW"}

        def keep(info):
            parts = info.name.split("/")
            if any(p in skip for p in parts) or info.name.endswith((".tmp", ".DS_Store")):
                return None
            return info

        self.send_response(200)
        self.send_header("Content-Type", "application/gzip")
        self.send_header("Content-Disposition",
                         f'attachment; filename="url-dataset_{datetime.now():%Y-%m-%d}.tar.gz"')
        self.end_headers()
        try:
            with tarfile.open(fileobj=self.wfile, mode="w|gz", compresslevel=6) as tar:
                tar.add(DATA_DIR, arcname="in_out-s", filter=keep)
        except (BrokenPipeError, ConnectionResetError):
            pass


def main():
    signal.signal(signal.SIGTERM, lambda *_: (runner.stop(), sys.exit(0)))
    threading.Thread(target=scheduler, args=(runner,), daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log(f"Panel on :{PORT}, daily start {OPTIONS.get('daily_start', '09:15')}")
    server.serve_forever()


if __name__ == "__main__":
    main()
