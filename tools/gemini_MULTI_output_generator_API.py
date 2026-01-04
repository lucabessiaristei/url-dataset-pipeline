#!/usr/bin/env python3
"""
Gemini 2.5 Parallel Batch Processor (Flash + Pro)
- One worker per (API key × model)
- Per-key throttling, flash boost when pro exhausted
- Compact Rich TUI with spinner, horizontal stats, and no flicker
"""

# =========================
# Imports & Global Silencing
# =========================
import os, re, json, time, signal, threading, logging, warnings
from dataclasses import dataclass
from queue import Queue, Empty
from itertools import cycle
from datetime import datetime

# Suppress all unnecessary backend noise
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)

os.environ["GRPC_VERBOSITY"] = "NONE"
os.environ["GLOG_minloglevel"] = "3"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["ABSL_LOGGING_STDERR_THRESHOLD"] = "3"

try:
    import absl.logging
    absl.logging.set_verbosity("error")
    absl.logging.use_absl_handler()
except Exception:
    pass

for lib in ("grpc", "absl", "google.generativeai"):
    logging.getLogger(lib).setLevel(logging.CRITICAL)

# =========================
# Dependencies
# =========================
from dotenv import load_dotenv
import google.generativeai as genai
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

# =========================
# Configuration
# =========================
load_dotenv()

BASE_DIR = "./in_out-s"
RULES_FILE = "./ai_rules.txt"

MODELS = {"flash": "gemini-2.5-flash", "pro": "gemini-2.5-pro"}
REQUESTS_PER_MIN_BASE = {"flash": 10, "pro": 2}
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = [5, 10, 20]

# Load API keys
API_KEYS = [
    v.strip() for k, v in os.environ.items()
    if "API_KEY" in k.upper() and v and v.strip()
]
if not API_KEYS:
    raise RuntimeError("No Gemini API keys found in environment (.env).")

import argparse

# =========================
# Data structures
# =========================
@dataclass
class Job:
    in_path: str
    out_path: str
    filename: str

@dataclass(frozen=True)
class WorkerId:
    key_idx: int
    model_type: str  # "flash" or "pro"

# =========================
# Globals
# =========================
console = Console()
shutdown_event = threading.Event()

last_call_ts, exhausted, worker_status, worker_done = {}, {}, {}, {}
locks = dict(last_call=threading.Lock(), exhausted=threading.Lock(),
             flash=threading.Lock(), status=threading.Lock())
flash_boost_enabled = False
exhausted_cycle = cycle([
    "( ˘³˘)zz",
    "( ˘³˘)zz",
    "( ˘³˘)zz",
    "( ˘3˘)Zz",
    "( ˘3˘)Zz",
    "( ˘3˘)Zz",
    "( ˊ³ˋ)ZZ",
    "( ˊ³ˋ)ZZ",
    "( ˊ³ˋ)ZZ",
    "( ˊ3ˋ)zZ",
    "( ˊ3ˋ)zZ",
    "( ˊ3ˋ)zZ",
    "( ˊ°ˋ)zz",
    "( ˊ°ˋ)zz",
    "( ˊ°ˋ)zz",
    "( ˊ¤ˋ)Zz",
    "( ˊ¤ˋ)Zz",
    "( ˊ¤ˋ)Zz",
    "( ˊOˋ)ZZ",
    "( ˊOˋ)ZZ",
    "( ˊOˋ)ZZ",
    "( ˊ૦ˋ)zZ",
    "( ˊ૦ˋ)zZ",
    "( ˊ૦ˋ)zZ",
    "( ˊ°ˋ)zz",
    "( ˊ°ˋ)zz",
    "( ˊ°ˋ)zz",
    "( ˊ³ˋ)Zz",
    "( ˊ³ˋ)Zz",
    "( ˊ³ˋ)Zz",
])


working_cycle = cycle([
    "ᓚ( `□´)ງ",
    "ᓚ( `□´)ງ",
    "ᓚ( `□´)ງ",
    "ᕦ(✧˙ж˙)ງ",
    "ᕦ(✧˙ж˙)ງ",
    "ᕦ(✧˙ж˙)ງ",
    "ᕦ(⊹°■°)ᕤ",
    "ᕦ(⊹°■°)ᕤ",
    "ᕦ(⊹°■°)ᕤ",
    "ᕦ(˚`▽´)ᕤ",
    "ᕦ(˚`▽´)ᕤ",
    "ᕦ(˚`▽´)ᕤ",
    "ᓚ(˚ˊ▽ˋ)ᕤ",
    "ᓚ(˚ˊ▽ˋ)ᕤ",
    "ᓚ(˚ˊ▽ˋ)ᕤ",
    "ᓚ( ˊ□ˋ)ງ",
    "ᓚ( ˊ□ˋ)ງ",
    "ᓚ( ˊ□ˋ)ງ",
])

with open(RULES_FILE, "r", encoding="utf-8") as f:
    AI_RULES = f.read().strip()

# =========================
# Utility
# =========================
def rate_sleep(worker):
    with locks["last_call"]:
        now = time.time()
        key = (worker.key_idx, worker.model_type)
        with locks["flash"]:
            rpm = REQUESTS_PER_MIN_BASE[worker.model_type]
        min_gap = 60.0 / max(1, rpm)
        diff = now - last_call_ts.get(key, 0.0)
        if diff < min_gap:
            time.sleep(min_gap - diff)
        last_call_ts[key] = time.time()

def get_model(worker):
    """Return a thread-local GenerativeModel configured with its own API key."""
    api_key = API_KEYS[worker.key_idx]
    # configure() is idempotent but not thread-safe globally — we reconfigure per thread
    genai.configure(api_key=api_key)
    return genai.GenerativeModel(MODELS[worker.model_type])

def mark_exhausted(worker):
    global flash_boost_enabled
    with locks["exhausted"]:
        exhausted[(worker.key_idx, worker.model_type)] = True
        all_pro_exhausted = all(exhausted.get((i,"pro"), False) for i in range(len(API_KEYS)))
    with locks["flash"]:
        flash_boost_enabled = all_pro_exhausted

def is_exhausted(worker):
    with locks["exhausted"]:
        return exhausted.get((worker.key_idx, worker.model_type), False)

def detect_io_pair(base_dir=BASE_DIR):
    in_dirs = sorted([d for d in os.listdir(base_dir) if re.match(r"^working_split_IN--\d+$", d)])
    if not in_dirs:
        raise RuntimeError("No input directories found.")
    for in_dir in in_dirs:
        suffix = in_dir.split("--")[-1]
        out_dir = f"working_split_OUT--API-{suffix}"
        in_path = os.path.join(base_dir, in_dir)
        out_path = os.path.join(base_dir, out_dir)
        os.makedirs(out_path, exist_ok=True)
        in_files = sorted([f for f in os.listdir(in_path) if f.startswith("in_") and f.endswith(".json")])
        out_files = {f for f in os.listdir(out_path) if f.startswith("out_") and f.endswith(".json")}
        remaining = [f for f in in_files if f.replace("in_", "out_", 1) not in out_files]
        if remaining:
            return in_path, out_path, remaining
    raise RuntimeError("All directories fully processed.")

def build_queue(in_path, out_path, remaining):
    def sort_key(name):
        m = re.search(r"(\d{4})", name)
        return int(m.group(1)) if m else 10**9
    q = Queue()
    for f in sorted(remaining, key=sort_key):
        q.put(Job(in_path, out_path, f))
    return q

# =========================
# Processing
# =========================
def process_one(job, worker):
    in_file = os.path.join(job.in_path, job.filename)
    out_filename = job.filename.replace("in_", "out_", 1)
    out_file = os.path.join(job.out_path, out_filename)

    # console.log(f"[DEBUG] Worker {worker} starting to process {job.filename}")

    with open(in_file, "r", encoding="utf-8") as f:
        content = json.load(f)

    prompt = f"""{AI_RULES}

Now categorize the following data according to the above rules.
Additional constraints:
- Split folders >20 links into coherent subgroups.
- Keep balanced grouping.
- CRITICAL: Only include `url` and `title` fields.
- CRITICAL: Avoid folders with only 1 or 2 links.

Return only valid JSON.

{json.dumps(content, ensure_ascii=False, indent=2)}"""

    with locks["status"]:
        worker_status[worker] = {"file": job.filename, "state": "processing"}

    for attempt in range(1, MAX_RETRIES + 1):
        if shutdown_event.is_set():
           # console.log(f"[DEBUG] Worker {worker} - shutdown detected on {job.filename}")
            return False

        try:
            rate_sleep(worker)
            model = get_model(worker)
           # console.log(f"[DEBUG] Worker {worker} calling API for {job.filename} (attempt {attempt})")
            r = model.generate_content(prompt)

            # --- Estrai il testo in modo robusto (0.8.5) ---
            raw_text = None
            if hasattr(r, "text") and r.text:
                raw_text = r.text
            elif getattr(r, "candidates", None):
                try:
                    parts = r.candidates[0].content.parts
                    if parts and hasattr(parts[0], "text"):
                        raw_text = parts[0].text
                except Exception:
                    raw_text = None

            if not raw_text or not raw_text.strip():
                raise RuntimeError("Gemini returned no textual content")

            # --- Pulisci eventuali code fences ```
            text = raw_text.strip()
            if text.startswith("```"):
                # rimuove eventuale '```json' o simili
                text = re.sub(r"^```[a-zA-Z0-9]*\n?", "", text)
            if text.endswith("```"):
                text = text[: text.rfind("```")].strip()

            # --- Prova il parse JSON
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError as je:
                # Ensure RAW directory exists
                raw_dir = os.path.join(BASE_DIR, "RAW")
                os.makedirs(raw_dir, exist_ok=True)

                # Build RAW file path with same base name
                raw_path = os.path.join(raw_dir, out_filename.replace(".json", "_RAW.txt"))

                # Save raw content
                with open(raw_path, "w", encoding="utf-8") as rf:
                    rf.write(raw_text)

                # Update worker state
                with locks["status"]:
                    worker_status[worker] = {"file": job.filename, "state": "raw_saved"}
                    worker_done[worker] = worker_done.get(worker, 0) + 1

                return True

            # --- Salva JSON valido
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(parsed, f, ensure_ascii=False, indent=2)

           # console.log(f"[DEBUG] Worker {worker} SAVED {out_filename}")
            with locks["status"]:
                worker_status[worker] = {"file": job.filename, "state": "done"}
                worker_done[worker] = worker_done.get(worker, 0) + 1
            return True

        except Exception as e:
            msg = str(e).lower()
           # console.log(f"[DEBUG] Worker {worker} error on {job.filename}: {type(e).__name__} - {str(e)[:120]}")
            # Quota / rate
            if any(x in msg for x in ["429", "quota", "rate limit", "exceeded", "resource exhausted"]):
                mark_exhausted(worker)
               # console.log(f"[DEBUG] Worker {worker} EXHAUSTED on {job.filename}")
                with locks["status"]:
                    worker_status[worker] = {"file": job.filename, "state": "exhausted"}
                return False  # job NON consumato → rientra in coda per altri
            # Transienti → backoff
            if any(x in msg for x in ["timeout", "temporarily", "connection", "unavailable", "internal"]):
               # console.log(f"[DEBUG] Worker {worker} retrying {job.filename} after transient error")
                time.sleep(RETRY_BACKOFF_SECONDS[min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)])
                continue
            # Altri errori → consumato ma marcato error
           # console.log(f"[DEBUG] Worker {worker} FAILED {job.filename} with unrecoverable error")
            with locks["status"]:
                worker_status[worker] = {"file": job.filename, "state": f"error:{type(e).__name__}"}
                worker_done[worker] = worker_done.get(worker, 0) + 1
            return True

   # console.log(f"[DEBUG] Worker {worker} exhausted retries for {job.filename}")
    with locks["status"]:
        worker_status[worker] = {"file": job.filename, "state": "failed"}
        worker_done[worker] = worker_done.get(worker, 0) + 1
    return True

def worker_loop(worker, q):
    with locks["status"]:
        worker_status[worker] = {"file": None, "state": "idle"}
        worker_done[worker] = 0
    
    while not shutdown_event.is_set():
        # Controllo exhausted qui, prima di prendere un nuovo job
        if is_exhausted(worker):
            with locks["status"]:
                worker_status[worker] = {"file": None, "state": "exhausted"}
            break
        
        try:
            # Calcola il timeout dinamicamente basato sul rate limiting
            with locks["flash"]:
                rpm = REQUESTS_PER_MIN_BASE[worker.model_type]
            max_gap = 60.0 / max(1, rpm)
            timeout = max_gap + 5  # Gap massimo + 5 secondi di margine
            
            job = q.get(timeout=timeout)
            # DEBUG
           # console.log(f"[DEBUG] Worker {worker} got job: {job.filename}, qsize={q.qsize()}")
        except Empty:
            # IMPORTANTE: se la coda è vuota, usciamo
           # console.log(f"[DEBUG] Worker {worker} - queue empty after {timeout:.1f}s, exiting. qsize={q.qsize()}")
            break
        
        consumed = process_one(job, worker)
        
        if consumed:
            # Job completato con successo - PRIMA task_done POI continua
           # console.log(f"[DEBUG] Worker {worker} marking {job.filename} as done")
            q.task_done()
            # Continua il loop per prendere il prossimo job
        else:
            # Job non completato (worker exhausted o errore critico)
            # Rimettiamo il job in coda per altri worker
           # console.log(f"[DEBUG] Worker {worker} re-queuing {job.filename} (not consumed)")
            q.put(job)
            # NON chiamare task_done() perché il job non è stato consumato
            
            # Se questo worker è exhausted, usciamo dal loop
            if is_exhausted(worker):
               # console.log(f"[DEBUG] Worker {worker} exiting (exhausted)")
                with locks["status"]:
                    worker_status[worker] = {"file": None, "state": "exhausted"}
                break
            # Altrimenti continua (potrebbe essere un errore temporaneo)
    
    with locks["status"]:
        if worker_status[worker]["state"] not in {"exhausted", "aborted"}:
            worker_status[worker] = {"file": None, "state": "stopped"}
# =========================
# Rich UI
# =========================
def render_dashboard(total, done, start_time, qsize):
    elapsed = (time.time() - start_time) / 60
    rate = done / elapsed if elapsed > 0 else 0.0
    remaining = qsize
    pct = (done / total * 100) if total else 0

    with locks["status"]:
        active = sum(1 for st in worker_status.values() if st["state"].startswith("processing"))
        # --- Statistics table (horizontal flex) ---
    stats_table = Table(box=box.SQUARE, expand=True, show_header=False, pad_edge=True)
    stats_table.add_column("Metric", justify="left", ratio=2, style="bright_green")
    stats_table.add_column("Value", justify="left", ratio=2, style="green")

    stats_table.add_row("Files", f"{done}/{total} ({pct:.1f}%)")
    stats_table.add_row("Remaining", str(remaining))
    stats_table.add_row("Active", str(active))
    stats_table.add_row("Rate", f"{rate:.1f}/min")
    stats_table.add_row("Elapsed", f"{elapsed:.1f}m")
        # --- Workers table (flex layout) ---
    table = Table(
        box=box.SQUARE,
        expand=True,                
        show_header=True,
        header_style="bright_green",
        pad_edge=True
    )

    # ratio = "peso" di ogni colonna, simile a flex-grow
    table.add_column("Worker / API Key", style="green", ratio=2)
    table.add_column("Model", justify="center", ratio=1)
    table.add_column("RPM", justify="center", ratio=1)
    table.add_column("Done", justify="center", ratio=1)
    table.add_column("Status", justify="left", ratio=2)
    table.add_column("File", justify="left", ratio=3)
    spin = next(working_cycle)
    sleep = next(exhausted_cycle)

    with locks["status"], locks["flash"]:
        total_keys = len(API_KEYS)
        for k_idx, key in enumerate(API_KEYS, start=1):
            key_suffix = "..." + key[-6:]

            # --- righe Flash + Pro ---
            for model in ("flash", "pro"):
                w = WorkerId(k_idx - 1, model)
                st = worker_status.get(w, {"file": None, "state": "idle"})
                done_w = worker_done.get(w, 0)
                rpm = REQUESTS_PER_MIN_BASE[model]
                state = st["state"]

                if state.startswith("processing"):
                    s = Text(f"PROCESSING {spin}", style="yellow")
                elif state.startswith("done"):
                    s = Text("DONE", style="bright_green")
                elif state.startswith("exhausted"):
                    s = Text(f"EXHAUSTED {sleep}", style="bright_red")
                elif state.startswith("error"):
                    s = Text("ERROR", style="red")
                elif state.startswith("idle"):
                    s = Text(f"IDLE {sleep}", style="dim")
                else:
                    s = Text(state.upper(), style="dim")

                key_label = f"#{k_idx} {key_suffix}" if model == "flash" else ""

                table.add_row(
                    key_label,
                    model.upper(),
                    str(rpm),
                    str(done_w),
                    s,
                    st["file"] or "-"
                )
                
    layout = Table.grid(expand=True)

    # Create header panel
    stats_height = len(stats_table.rows) + 4  # heuristic: rows + borders/padding
    header_text = "\n" * (stats_height // 2 - 1) + "GEMINI BATCH PROCESSOR 3.0" + "\n" * (stats_height // 2 - 1 )

    header_panel = Panel(
        Text(header_text, style="bright_green", justify="center"),
        box=box.DOUBLE,
        border_style="bright_green",
    )

    # Create stats panel
    stats_panel = Panel(
        stats_table,
        title="[green]Statistics[/green]",
        border_style="green",
        box=box.DOUBLE,
    )

    # Create a sub-grid to hold header + stats side by side
    top_row = Table.grid(expand=True, padding=(0, 1))  # (verticale, orizzontale)
    top_row.add_column(ratio=2)
    top_row.add_column(ratio=2)
    top_row.add_row(header_panel, stats_panel)

    # Assemble the layout
    layout.add_row(top_row)
    layout.add_row(
        Panel(
            table,
            title="[green]Workers[/green]",
            border_style="green",
            box=box.DOUBLE,
        )
    )

    return layout

# =========================
# Main
# =========================
def main():
    # ------------------------
    # Parse CLI arguments
    # ------------------------
    parser = argparse.ArgumentParser(description="Gemini 2.5 Parallel Batch Processor")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--flash", action="store_true", help="Run only Flash workers")
    group.add_argument("--pro", action="store_true", help="Run only Pro workers")
    args = parser.parse_args()

    # Determine which models to run
    if args.flash:
        active_models = ("flash",)
    elif args.pro:
        active_models = ("pro",)
    else:
        active_models = ("flash", "pro")

    # ------------------------
    # Setup
    # ------------------------
    signal.signal(signal.SIGINT, lambda s, f: shutdown_event.set())
    start = time.time()
    in_path, out_path, remaining = detect_io_pair(BASE_DIR)
    q = build_queue(in_path, out_path, remaining)
    total = q.qsize()

    for k in range(len(API_KEYS)):
        for mt in ("flash", "pro"):
            exhausted[(k, mt)] = False
            last_call_ts[(k, mt)] = 0.0

    # ------------------------
    # Threads (spawn only selected model types)
    # ------------------------
    threads = []
    for k in range(len(API_KEYS)):
        for mt in active_models:
            w = WorkerId(k, mt)
            t = threading.Thread(target=worker_loop, args=(w, q), daemon=True)
            t.start()
            threads.append(t)

    # ------------------------
    # Dashboard
    # ------------------------
    with Live(
        render_dashboard(total, 0, start, q.qsize()),
        console=console,
        refresh_per_second=4,
        transient=False,
        redirect_stdout=False,
        redirect_stderr=False,
    ) as live:
        done_last = 0
        while any(t.is_alive() for t in threads):
            if shutdown_event.is_set():
                break
            done_now = sum(worker_done.values())
            if done_now != done_last:
                done_last = done_now
            live.update(render_dashboard(total, done_now, start, q.qsize()))
            time.sleep(0.2)

    for t in threads:
        t.join(timeout=1.0)

    console.print(render_dashboard(total, sum(worker_done.values()), start, q.qsize()))
    console.print(f"[green]Completed {sum(worker_done.values())}/{total} files.[/green]")
    
if __name__ == "__main__":
    main()