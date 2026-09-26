#!/usr/bin/env python3
"""
Home Assistant side of the URL dataset pipeline, a passive dataset creator:
- keeps the generator running in --forever mode (restarts it after a crash; Pause/Resume from the panel)
- the generator itself waits out quota resets, retries resting files and creates new input folders
- serves the ingress panel (status, live workers, providers, progress, per-model record, log)
- posts a daily digest notification at `digest_time`
"""

import importlib.util
import json
import os
import re
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


PAUSED_FLAG = os.path.join(DATA_ROOT, ".paused")   # survives app restarts and updates
POOLS_DIR = os.path.join(DATA_ROOT, "pools")        # URL pools for new input folders (working_expanded*.json)


def next_digest(now=None):
    now = now or datetime.now()
    hour, minute = (int(x) for x in str(OPTIONS.get("digest_time", "09:00")).split(":"))
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
        names = sorted((n for n in os.listdir(DATA_DIR) if re.match(r"^working_split_IN--\d+$", n)),
                       key=lambda n: int(n.split("--")[-1]))  # IN--2 before IN--10
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


# ---------------------------------------------------------------- the generator process
class Keeper:
    """Keeps one generator running in --forever mode. It never stops on its own, so any exit that was
    not a pause is a crash: restart it, waiting longer when it keeps crashing right after starting."""

    def __init__(self):
        self.lock = threading.Lock()
        self.proc = None
        self.started = None
        self.stopping = False
        self.last_exit = None
        self.crashes = 0                  # quick crashes in a row
        self.output = deque(maxlen=400)

    def paused(self):
        return os.path.exists(PAUSED_FLAG)

    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self):
        with self.lock:
            if self.running() or self.paused():
                return False
            if not os.path.isdir(DATA_DIR):
                log(f"No dataset at {DATA_DIR}: copy in_out-s there first")
                return False
            args = [sys.executable, GENERATOR, "--forever"]
            models = (OPTIONS.get("models") or "").strip()
            if models:
                args += ["--models", models]
            env = {**os.environ, "PIPELINE_PROGRESS_SECONDS": "300", "COLUMNS": "160",
                   "PIPELINE_NVIDIA_WORKERS": str(OPTIONS.get("nvidia_workers", 6)),
                   "PIPELINE_MAX_INPUT_FOLDERS": str(OPTIONS.get("max_input_folders", 10)),
                   "PIPELINE_POOLS_DIR": POOLS_DIR}
            for provider in PROVIDERS:
                key = (OPTIONS.get(f"{provider}_api_key") or "").strip()
                if key:
                    env[f"{provider.upper()}_API_KEY"] = key
            trim_log()
            self.output.clear()
            self.proc = subprocess.Popen(args, cwd=APP_DIR, env=env, text=True, bufsize=1,
                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.started = time.time()
            self.stopping = False
            threading.Thread(target=self._pump, args=(self.proc,), daemon=True).start()
            log("Generator started")
            return True

    def stop(self, wait=0):
        with self.lock:
            if not self.running():
                return False
            # SIGINT = clean stop: finished files are kept, requests in flight are dropped
            self.proc.send_signal(signal.SIGINT)
            self.stopping = True
            proc = self.proc
        try:
            proc.wait(timeout=wait) if wait else None
        except subprocess.TimeoutExpired:
            proc.kill()
        return True

    def pause(self):
        open(PAUSED_FLAG, "w").close()
        log("Paused from the panel")
        self.stop()
        return True

    def resume(self):
        try:
            os.remove(PAUSED_FLAG)
        except FileNotFoundError:
            pass
        log("Resumed from the panel")
        self.crashes = 0
        return self.start()

    def _pump(self, proc):
        with open(RUN_LOG, "a", encoding="utf-8") as out:
            out.write(f"\n=== {datetime.now():%Y-%m-%d %H:%M} generator started\n")
            for line in proc.stdout:
                line = line.rstrip("\n")
                self.output.append(line)
                out.write(line + "\n")
                out.flush()
                print(line, flush=True)
            code = proc.wait()
            out.write(f"=== {datetime.now():%Y-%m-%d %H:%M} generator ended (exit {code})\n")
        self.last_exit = code
        quick = time.time() - (self.started or 0) < 600
        self.crashes = self.crashes + 1 if (quick and not self.stopping) else 0
        self.stopping = False
        _stats_cache["at"] = 0.0
        _progress_cache["at"] = 0.0
        log(f"Generator ended (exit {code})")

    def keep_alive(self):
        """Starts the generator at boot and after a crash (1 min, doubling up to 1 h if it keeps crashing)."""
        while True:
            if not self.running() and not self.paused():
                wait = 0 if self.last_exit is None else min(3600, 60 * 2 ** max(0, min(self.crashes, 7) - 1))
                if wait:
                    log(f"Generator not running (last exit {self.last_exit}): restarting in {wait // 60} min")
                    time.sleep(wait)
                if not self.paused():
                    self.start()
            time.sleep(10)

    def state(self):
        running = self.running()
        return {
            "running": running,
            "paused": self.paused(),
            "stopping": self.stopping and running,
            "started": datetime.fromtimestamp(self.started).isoformat(timespec="seconds") if self.started else None,
            "last_exit": self.last_exit,
            "next_digest": next_digest().isoformat(timespec="minutes"),
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


def recent():
    try:
        return generator_module().recent_summary(24)
    except Exception as e:
        return {"hours": 24, "done": 0, "per_hour": 0, "models": {}, "new_folders": [], "abandoned": 0, "error": str(e)}


def digest_text():
    """Daily digest: last 24 h per model, files left and a finish estimate, providers on pause."""
    r = recent()
    rows = progress()
    total, done = sum(p["total"] for p in rows), sum(p["done"] for p in rows)
    left = total - done
    status = read_json(os.path.join(DATA_DIR, "run_status.json")) or {}
    lines = [f"{r['done']} files in the last 24 h ({r['per_hour']}/h) · {left:,} left of {total:,}"]
    if r["done"]:
        lines[0] += f" · about {left / r['done']:.1f} days at this pace"
    lines += [f"• {m}: {v['done']} done, {v['rejected']} rejected, {v['errors']} API errors"
              for m, v in r["models"].items()]
    if r["new_folders"]:
        lines.append("New input folders: " + ", ".join(r["new_folders"]))
    paused = [p for p in status.get("paused") or []]
    if paused:
        lines.append("Paused: " + "; ".join(
            f"{p['who']} ({p['reason']}{', back ' + p['until'][11:16] if p.get('until') else ', for good'})" for p in paused))
    if keeper.paused():
        lines.append("The pipeline is paused from the panel.")
    elif not keeper.running():
        lines.append(f"The generator is not running (last exit {keeper.last_exit}).")
    return "\n".join(lines)


def notify(message):
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        return
    payload = json.dumps({"title": "URL dataset pipeline", "message": message,
                          "notification_id": "url_dataset_pipeline"}).encode()
    request = urllib.request.Request(
        "http://supervisor/core/api/services/persistent_notification/create", data=payload, method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        urllib.request.urlopen(request, timeout=10).read()
    except Exception as e:
        log(f"Could not post the notification: {e}")


def digest_loop():
    while True:
        target = next_digest()
        time.sleep(max(1, (target - datetime.now()).total_seconds()))
        try:
            notify(digest_text())
            log("Daily digest posted")
        except Exception as e:
            log(f"Daily digest failed: {e}")
        time.sleep(61)


# ---------------------------------------------------------------- panel
keeper = Keeper()


class Handler(BaseHTTPRequestHandler):
    server_version = "UrlDatasetPipeline/2.0"

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
                "runner": keeper.state(),
                "status": status,
                "recent": recent(),
                "progress": progress(),
                "log": list(keeper.output) if keeper.running() else log_tail(),
                "options": {k: OPTIONS.get(k) for k in ("nvidia_workers", "max_input_folders", "digest_time", "models")},
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
        if route == "api/resume":
            self._json({"ok": keeper.resume()})
        elif route == "api/pause":
            self._json({"ok": keeper.pause()})
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
    # App stop/update: stop the generator cleanly (finished files are kept) before the container goes
    signal.signal(signal.SIGTERM, lambda *_: (keeper.stop(wait=20), sys.exit(0)))
    threading.Thread(target=keeper.keep_alive, daemon=True).start()
    threading.Thread(target=digest_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log(f"Panel on :{PORT}; generator runs continuously"
        f"{' (paused from the panel)' if keeper.paused() else ''}; daily digest at {OPTIONS.get('digest_time', '09:00')}")
    server.serve_forever()


if __name__ == "__main__":
    main()
