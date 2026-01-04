#!/usr/bin/env python3
"""
Multi-Provider Parallel Batch Processor (Gemini + Groq + DeepSeek)
- Supports Google Gemini (Flash + Pro), Groq (Llama models), and DeepSeek
- Multiple concurrent workers per provider
- Per-key throttling with provider-specific handling
- Manual directory selection
"""

# =========================
# Imports & Global Silencing
# =========================
import os, re, json, time, signal, threading, logging, warnings
from dataclasses import dataclass
from queue import Queue, Empty
from itertools import cycle
from datetime import datetime

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
from groq import Groq
from openai import OpenAI
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
import argparse

# =========================
# Configuration
# =========================
load_dotenv()

BASE_DIR = "./in_out-s"
RULES_FILE = "./ai_rules.txt"
ERROR_LOG = "./errors.log"

error_logger = logging.getLogger("batch_errors")
error_logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(ERROR_LOG, mode='a', encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
error_logger.addHandler(fh)

PROVIDERS = {
    "gemini": {
        "models": {
            "flash": "gemini-2.5-flash",
            "pro": "gemini-2.5-pro"
        },
        "rpm": {
            "flash": 10,
            "pro": 2
        }
    },
    "groq": {
        "models": {
            "scout": "meta-llama/llama-4-scout-17b-16e-instruct",
            "versatile": "llama-3.3-70b-versatile"
        },
        "rpm": {
            "scout": 30,
            "versatile": 30
        },
        "tpm": {
            "scout": 30000,
            "versatile": 12000
        },
        "context_window": {
            "scout": 30000,
            "versatile": 12000
        }
    },
    "deepseek": {
        "models": {
            "chat": "deepseek-chat"
        },
        "rpm": {
            "chat": 300
        },
        "tpm": {
            "chat": 64000
        },
        "context_window": {
            "chat": 64000
        }
    }
}

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = [5, 10, 20]

GEMINI_KEY = os.getenv("GEMINI_API_KEY")
GROQ_KEY = os.getenv("GROQ_API_KEY")
DEEPSEEK_KEY = os.getenv("DEEPSEEK_API_KEY")

if not GEMINI_KEY and not GROQ_KEY and not DEEPSEEK_KEY:
    raise RuntimeError("No API keys found. Set GEMINI_API_KEY, GROQ_API_KEY, and/or DEEPSEEK_API_KEY in .env")

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
    provider: str
    model_type: str
    worker_id: int = 0

# =========================
# Globals
# =========================
console = Console()
shutdown_event = threading.Event()

last_call_ts, exhausted, worker_status, worker_done = {}, {}, {}, {}
locks = dict(last_call=threading.Lock(), exhausted=threading.Lock(), status=threading.Lock())

sleep_cycle = cycle([
    "( ˊ³ˋ)ZZ", "( ˊ³ˋ)ZZ", "( ˊ³ˋ)ZZ",
    "( ˊ°ˋ)zZ", "( ˊ°ˋ)zZ", "( ˊ°ˋ)zZ",
    "( ˊ¤ˋ)zz", "( ˊ¤ˋ)zz", "( ˊ¤ˋ)zz",
    "( ˊOˋ)zZ", "( ˊOˋ)zZ", "( ˊOˋ)zZ",
    "( ˊ૦ˋ)ZZ", "( ˊ૦ˋ)ZZ", "( ˊ૦ˋ)ZZ",
    "( ˊ°ˋ)zZ", "( ˊ°ˋ)zZ", "( ˊ°ˋ)zZ",
    "( ˊ³ˋ)zz", "( ˊ³ˋ)zz", "( ˊ³ˋ)zz",
    "( ˊ3ˋ)zZ", "( ˊ3ˋ)zZ", "( ˊ3ˋ)zZ",
])

work_cycle = cycle([
    "ᓚ( `□´)ງ", "ᓚ( `□´)ງ", "ᓚ( `□´)ງ",
    "ᕦ(✧˙ж˙)ງ", "ᕦ(✧˙ж˙)ງ", "ᕦ(✧˙ж˙)ງ",
    "ᕦ(⊹°■°)ᕤ", "ᕦ(⊹°■°)ᕤ", "ᕦ(⊹°■°)ᕤ",
])

idle_cycle = cycle([
    "( ˙Ⱉ˙)⊹", "( ˙Ⱉ˙)⊹", "( ˙Ⱉ˙)⊹",
    "( ˙Ⱉ˙)✧", "( ˙Ⱉ˙)✧", "( ˙Ⱉ˙)✧",
    "( ˙Ⱉ˙)⭒", "( ˙Ⱉ˙)⭒", "( ˙Ⱉ˙)⭒",
    "( ˙Ⱉ˙)*", "( ˙Ⱉ˙)*", "( ˙Ⱉ˙)*",
    "( ˙Ⱉ˙)₊", "( ˙Ⱉ˙)₊", "( ˙Ⱉ˙)₊",
])

with open(RULES_FILE, "r", encoding="utf-8") as f:
    AI_RULES = f.read().strip()

groq_client = Groq(api_key=GROQ_KEY) if GROQ_KEY else None

if DEEPSEEK_KEY:
    deepseek_client = OpenAI(
        api_key=DEEPSEEK_KEY,
        base_url="https://api.deepseek.com"
    )
else:
    deepseek_client = None

# =========================
# Utility
# =========================
def rate_sleep(worker):
    with locks["last_call"]:
        now = time.time()
        key = (worker.provider, worker.model_type)
        rpm = PROVIDERS[worker.provider]["rpm"][worker.model_type]
        min_gap = 60.0 / max(1, rpm)
        diff = now - last_call_ts.get(key, 0.0)
        if diff < min_gap:
            time.sleep(min_gap - diff)
        last_call_ts[key] = time.time()

def call_gemini(worker, prompt):
    model_name = PROVIDERS["gemini"]["models"][worker.model_type]
    model = genai.GenerativeModel(model_name)
    r = model.generate_content(prompt)
    
    if not r or not r.candidates:
        raise RuntimeError("Gemini returned no candidates")
    
    candidate = r.candidates[0]
    if not candidate.content or not candidate.content.parts:
        raise RuntimeError("Gemini returned no content parts")
    
    raw_text = candidate.content.parts[0].text
    
    if not raw_text or not raw_text.strip():
        raise RuntimeError("Gemini returned no textual content")
    
    return raw_text

def call_groq(worker, prompt):
    model_name = PROVIDERS["groq"]["models"][worker.model_type]
    
    response = groq_client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": "You are a helpful assistant that categorizes bookmarks. Return only valid JSON."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=8000
    )
    
    raw_text = response.choices[0].message.content
    
    if not raw_text or not raw_text.strip():
        raise RuntimeError("Groq returned no textual content")
    
    return raw_text

def call_deepseek(worker, prompt):
    model_name = PROVIDERS["deepseek"]["models"][worker.model_type]
    
    response = deepseek_client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": "You are a helpful assistant that categorizes bookmarks. Return only valid JSON."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=8000
    )
    
    raw_text = response.choices[0].message.content
    
    if not raw_text or not raw_text.strip():
        raise RuntimeError("DeepSeek returned no textual content")
    
    return raw_text

def call_llm(worker, prompt):
    if worker.provider == "gemini":
        return call_gemini(worker, prompt)
    elif worker.provider == "groq":
        return call_groq(worker, prompt)
    elif worker.provider == "deepseek":
        return call_deepseek(worker, prompt)
    else:
        raise ValueError(f"Unknown provider: {worker.provider}")

def mark_exhausted(worker):
    with locks["exhausted"]:
        error_logger.critical(f"MARKING AS EXHAUSTED: {worker.provider}/{worker.model_type}")
        exhausted[(worker.provider, worker.model_type)] = True

def is_exhausted(worker):
    with locks["exhausted"]:
        return exhausted.get((worker.provider, worker.model_type), False)

def list_available_directories(base_dir=BASE_DIR):
    in_dirs = sorted([d for d in os.listdir(base_dir) if re.match(r"^working_split_IN--\d+$", d)])
    return in_dirs

def setup_io_paths(in_dir_name, base_dir=BASE_DIR):
    in_path = os.path.join(base_dir, in_dir_name)
    suffix = in_dir_name.split("--")[-1]
    out_dir = f"working_split_OUT--API-{suffix}"
    out_path = os.path.join(base_dir, out_dir)
    os.makedirs(out_path, exist_ok=True)
    return in_path, out_path

def get_remaining_files(in_path, out_path):
    in_files = sorted([f for f in os.listdir(in_path) if f.startswith("in_") and f.endswith(".json")])
    out_files = {f for f in os.listdir(out_path) if f.startswith("out_") and f.endswith(".json")}
    remaining = [f for f in in_files if f.replace("in_", "out_", 1) not in out_files]
    return remaining

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

    if os.path.exists(out_file):
        error_logger.warning(f"Output file {out_file} already exists, skipping")
        with locks["status"]:
            worker_status[worker] = {"file": job.filename, "state": "skipped:exists"}
            worker_done[worker] = worker_done.get(worker, 0) + 1
        return True

    with open(in_file, "r", encoding="utf-8") as f:
        content = json.load(f)
    
    if worker.provider in ("groq", "deepseek"):
        content_str = json.dumps(content, ensure_ascii=False)
        estimated_tokens = len(content_str) // 4 + len(AI_RULES) // 4
        
        context_limit = PROVIDERS[worker.provider]["context_window"][worker.model_type]
        max_input_tokens = context_limit - 4000
        
        error_logger.debug(f"File {job.filename} estimated tokens: {estimated_tokens}, limit: {max_input_tokens} ({worker.provider}/{worker.model_type})")
        
        if estimated_tokens > max_input_tokens:
            error_logger.info(f"File {job.filename} too large (~{estimated_tokens} tokens) for {worker.provider}/{worker.model_type} ({context_limit} context limit), skipping")
            with locks["status"]:
                worker_status[worker] = {"file": job.filename, "state": "skipped:too_large"}
            return False

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
            return False

        try:
            rate_sleep(worker)
            raw_text = call_llm(worker, prompt)

            text = raw_text.strip()
            if text.startswith("```"):
                text = re.sub(r"^```[a-zA-Z0-9]*\n?", "", text)
            if text.endswith("```"):
                text = text[:text.rfind("```")].strip()

            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                raw_dir = os.path.join(BASE_DIR, "RAW")
                os.makedirs(raw_dir, exist_ok=True)
                raw_path = os.path.join(raw_dir, out_filename.replace(".json", "_RAW.txt"))
                
                with open(raw_path, "w", encoding="utf-8") as rf:
                    rf.write(raw_text)
                
                with locks["status"]:
                    worker_status[worker] = {"file": job.filename, "state": "raw_saved"}
                    worker_done[worker] = worker_done.get(worker, 0) + 1
                return True

            if os.path.exists(out_file):
                error_logger.warning(f"Race condition: {out_file} was created by another worker, skipping")
                with locks["status"]:
                    worker_status[worker] = {"file": job.filename, "state": "skipped:race"}
                    worker_done[worker] = worker_done.get(worker, 0) + 1
                return True

            temp_file = out_file + ".tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(parsed, f, ensure_ascii=False, indent=2)
            
            os.replace(temp_file, out_file)

            with locks["status"]:
                worker_status[worker] = {"file": job.filename, "state": "done"}
                worker_done[worker] = worker_done.get(worker, 0) + 1
            return True

        except Exception as e:
            msg = str(e).lower()
            error_type = type(e).__name__
            
            error_logger.error(f"Worker {worker.provider}/{worker.model_type}-{worker.worker_id} | File {job.filename} | {error_type}: {str(e)}")
            error_logger.debug(f"Full error message (lowercase): {msg}")
            
            token_limit_keywords = [
                "413", "request too large", "context length", "context_length_exceeded",
                "maximum context", "token limit", "tokens exceeded", "input too long",
                "prompt is too long", "exceeds maximum"
            ]
            
            matched_keyword = None
            for keyword in token_limit_keywords:
                if keyword in msg:
                    matched_keyword = keyword
                    break
            
            if matched_keyword:
                error_logger.warning(f"Token limit error detected (keyword: '{matched_keyword}') for file {job.filename}, skipping for {worker.provider}/{worker.model_type}")
                with locks["status"]:
                    worker_status[worker] = {"file": job.filename, "state": "skipped:too_large"}
                return False
            
            quota_keywords = ["429", "quota exceeded", "rate_limit_exceeded"]
            matched_quota = None
            for keyword in quota_keywords:
                if keyword in msg:
                    matched_quota = keyword
                    break
            
            if matched_quota:
                error_logger.warning(f"QUOTA/RATE LIMIT EXHAUSTED (keyword: '{matched_quota}'): {worker.provider}/{worker.model_type}")
                mark_exhausted(worker)
                with locks["status"]:
                    worker_status[worker] = {"file": job.filename, "state": "exhausted"}
                return False
            
            if any(x in msg for x in ["timeout", "temporarily", "connection", "unavailable", "internal"]):
                error_logger.info(f"Transient error, retry {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_BACKOFF_SECONDS[min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)])
                continue
            
            error_logger.error(f"Unrecoverable error: {type(e).__name__}")
            with locks["status"]:
                worker_status[worker] = {"file": job.filename, "state": f"error:{type(e).__name__}"}
                worker_done[worker] = worker_done.get(worker, 0) + 1
            return True
        
    with locks["status"]:
        worker_status[worker] = {"file": job.filename, "state": "failed"}
        worker_done[worker] = worker_done.get(worker, 0) + 1
    return True

def worker_loop(worker, q):
    error_logger.info(f"Worker {worker.provider}/{worker.model_type}-{worker.worker_id} starting")
    with locks["status"]:
        worker_status[worker] = {"file": None, "state": "idle"}
        worker_done[worker] = 0
    
    while not shutdown_event.is_set():
        if is_exhausted(worker):
            error_logger.warning(f"Worker {worker.provider}/{worker.model_type}-{worker.worker_id} detected as exhausted, exiting loop")
            with locks["status"]:
                worker_status[worker] = {"file": None, "state": "exhausted"}
            break
        
        try:
            rpm = PROVIDERS[worker.provider]["rpm"][worker.model_type]
            max_gap = 60.0 / max(1, rpm)
            timeout = max_gap + 5
            
            job = q.get(timeout=timeout)
            error_logger.debug(f"Worker {worker.provider}/{worker.model_type}-{worker.worker_id} got job: {job.filename}")
        except Empty:
            error_logger.info(f"Worker {worker.provider}/{worker.model_type}-{worker.worker_id} queue timeout, exiting")
            break
        
        consumed = process_one(job, worker)
        
        if consumed:
            q.task_done()
            error_logger.debug(f"Worker {worker.provider}/{worker.model_type}-{worker.worker_id} consumed {job.filename}")
        else:
            error_logger.debug(f"Worker {worker.provider}/{worker.model_type}-{worker.worker_id} requeued {job.filename}")
            q.put(job)
            if is_exhausted(worker):
                error_logger.warning(f"Worker {worker.provider}/{worker.model_type}-{worker.worker_id} exhausted after requeue, exiting")
                with locks["status"]:
                    worker_status[worker] = {"file": None, "state": "exhausted"}
                break
    
    error_logger.info(f"Worker {worker.provider}/{worker.model_type}-{worker.worker_id} stopped (done: {worker_done.get(worker, 0)})")
    with locks["status"]:
        if worker_status[worker]["state"] not in {"exhausted", "aborted"}:
            worker_status[worker] = {"file": None, "state": "stopped"}

# =========================
# Rich UI
# =========================
def render_dashboard(total, done, start_time, qsize, in_dir_name):
    elapsed = (time.time() - start_time) / 60
    rate = done / elapsed if elapsed > 0 else 0.0
    remaining = qsize
    pct = (done / total * 100) if total else 0

    with locks["status"]:
        active = sum(1 for st in worker_status.values() if st["state"].startswith("processing"))
    
    stats_table = Table(box=box.DOUBLE, expand=True, show_header=False, pad_edge=True, border_style="#48A630")
    stats_table.add_column("Metric", justify="left", ratio=2, style="#63D746 bold")
    stats_table.add_column("Value", justify="left", ratio=2, style="#63D746")
    stats_table.add_row("Directory", in_dir_name)
    stats_table.add_row("Files", f"{done}/{total} ({pct:.1f}%)")
    stats_table.add_row("Remaining", str(remaining))
    stats_table.add_row("Active", str(active))
    stats_table.add_row("Rate", f"{rate:.1f}/min")
    stats_table.add_row("Elapsed", f"{elapsed:.1f}m")
    
    table = Table(
        box=box.DOUBLE_EDGE,
        expand=True,
        show_header=True,
        header_style="bold #63D746",
        border_style="#48A630",
        pad_edge=True
    )
    
    table.add_column("Provider", style="#63D746", ratio=1)
    table.add_column("Model", justify="center", style="#63D746", ratio=2)
    table.add_column("RPM", justify="center", style="#63D746", ratio=1)
    table.add_column("Done", justify="center", style="#63D746", ratio=1)
    table.add_column("Status", justify="left", style="#63D746", ratio=2)
    table.add_column("File", justify="left", style="#63D746", ratio=3)
    
    work = next(work_cycle)
    sleep = next(sleep_cycle)
    idle = next(idle_cycle)

    with locks["status"]:
        if GEMINI_KEY:
            for model in ("flash", "pro"):
                w = WorkerId("gemini", model, 0)
                st = worker_status.get(w, {"file": None, "state": "idle"})
                done_w = worker_done.get(w, 0)
                rpm = PROVIDERS["gemini"]["rpm"][model]
                state = st["state"]

                if state.startswith("processing"):
                    s = Text(f"PROCESSING {work}", style="#63D746")
                elif state.startswith("done"):
                    s = Text(f"DONE {idle}", style="#48A630")
                elif state.startswith("exhausted"):
                    s = Text(f"EXHAUSTED {sleep}", style="#48A630")
                elif state.startswith("error"):
                    s = Text("ERROR", style="#FF5F1F")
                elif state.startswith("idle"):
                    s = Text(f"IDLE {idle}", style="#A8EE59")
                elif state.startswith("stopped"):
                    s = Text(f"STOPPED {sleep}", style="#48A630")
                else:
                    s = Text(state.upper(), style="#48A630")

                table.add_row(
                    "GEMINI" if model == "flash" else "",
                    model.upper(),
                    str(rpm),
                    str(done_w),
                    s,
                    st["file"] or "-"
                )
        
        if GROQ_KEY:
            for model in ("scout", "versatile"):
                w = WorkerId("groq", model, 0)
                st = worker_status.get(w, {"file": None, "state": "idle"})
                done_w = worker_done.get(w, 0)
                rpm = PROVIDERS["groq"]["rpm"][model]
                state = st["state"]

                if state.startswith("processing"):
                    s = Text(f"PROCESSING {work}", style="#63D746")
                elif state.startswith("done"):
                    s = Text(f"DONE {idle}", style="#48A630")
                elif state.startswith("skipped"):
                    s = Text("SKIPPED", style="#48A630")
                elif state.startswith("exhausted"):
                    s = Text(f"EXHAUSTED {sleep}", style="#48A630")
                elif state.startswith("error"):
                    s = Text("ERROR", style="#FF5F1F")
                elif state.startswith("idle"):
                    s = Text(f"IDLE {idle}", style="#A8EE59")
                elif state.startswith("stopped"):
                    s = Text(f"STOPPED {sleep}", style="#48A630")
                else:
                    s = Text(state.upper(), style="#48A630")

                table.add_row(
                    "GROQ" if model == "scout" else "",
                    model.upper(),
                    str(rpm),
                    str(done_w),
                    s,
                    st["file"] or "-"
                )
        
        if DEEPSEEK_KEY:
            for worker_id in range(3):
                w = WorkerId("deepseek", "chat", worker_id)
                st = worker_status.get(w, {"file": None, "state": "idle"})
                done_w = worker_done.get(w, 0)
                rpm = PROVIDERS["deepseek"]["rpm"]["chat"]
                state = st["state"]

                if state.startswith("processing"):
                    s = Text(f"PROCESSING {work}", style="#63D746")
                elif state.startswith("done"):
                    s = Text(f"DONE {idle}", style="#48A630")
                elif state.startswith("skipped"):
                    s = Text("SKIPPED", style="#48A630")
                elif state.startswith("exhausted"):
                    s = Text(f"EXHAUSTED {sleep}", style="#48A630")
                elif state.startswith("error"):
                    s = Text("ERROR", style="#FF5F1F")
                elif state.startswith("idle"):
                    s = Text(f"IDLE {idle}", style="#A8EE59")
                elif state.startswith("stopped"):
                    s = Text(f"STOPPED {sleep}", style="#48A630")
                else:
                    s = Text(state.upper(), style="#48A630")

                table.add_row(
                    "DEEPSEEK" if worker_id == 0 else "",
                    f"CHAT-{worker_id+1}",
                    str(rpm),
                    str(done_w),
                    s,
                    st["file"] or "-"
                )

    layout = Table.grid(expand=True)
    
    stats_height = len(stats_table.rows)
    header_text = (
        "\n" * (stats_height // 2 - 2)
        + "MULTI-PROVIDER BATCH PROCESSOR"
        + "\n"
        + "gemini + groq + deepseek"
        + "\n" * (stats_height // 2 - 2)
    )
    
    inner_panel = Panel(
        Text(header_text, style="#63D746 bold", justify="center"),
        box=box.DOUBLE,
        border_style="#48A630",
        padding=(1, 4),
    )

    header_panel = Panel(
        inner_panel,
        title="[#48A630]v 5.1[/#48A630]",
        box=box.ROUNDED,
        border_style="#48A630",
    )
    
    stats_panel = Panel(
        stats_table,
        title="[#48A630]Statistics[/#48A630]",
        border_style="#48A630",
        box=box.ROUNDED,
    )
    
    top_row = Table.grid(expand=True, padding=(0, 1))
    top_row.add_column(ratio=2)
    top_row.add_column(ratio=2)
    top_row.add_row(header_panel, stats_panel)
    
    layout.add_row(top_row)
    layout.add_row(
        Panel(
            table,
            title="[#48A630]Workers[/#48A630]",
            border_style="#48A630",
            box=box.ROUNDED,
        )
    )
    
    return layout

# =========================
# Main
# =========================
def main():
    parser = argparse.ArgumentParser(description="Multi-Provider Batch Processor")
    parser.add_argument("--gemini-only", action="store_true", help="Run only Gemini workers")
    parser.add_argument("--groq-only", action="store_true", help="Run only Groq workers")
    parser.add_argument("--deepseek-only", action="store_true", help="Run only DeepSeek workers")
    parser.add_argument("--dir", type=str, help="Input directory name (e.g., working_split_IN--001)")
    args = parser.parse_args()

    if GEMINI_KEY:
        genai.configure(api_key=GEMINI_KEY)

    active_providers = []
    if args.gemini_only:
        if not GEMINI_KEY:
            raise RuntimeError("GEMINI_API_KEY not found")
        active_providers = [("gemini", ["flash", "pro"])]
    elif args.groq_only:
        if not GROQ_KEY:
            raise RuntimeError("GROQ_API_KEY not found")
        active_providers = [("groq", ["scout", "versatile"])]
    elif args.deepseek_only:
        if not DEEPSEEK_KEY:
            raise RuntimeError("DEEPSEEK_API_KEY not found")
        active_providers = [("deepseek", ["chat"])]
    else:
        if GEMINI_KEY:
            active_providers.append(("gemini", ["flash", "pro"]))
        if GROQ_KEY:
            active_providers.append(("groq", ["scout", "versatile"]))
        if DEEPSEEK_KEY:
            active_providers.append(("deepseek", ["chat"]))

    if not active_providers:
        raise RuntimeError("No valid API keys found")

    available_dirs = list_available_directories(BASE_DIR)
    
    if not available_dirs:
        raise RuntimeError(f"No input directories found in {BASE_DIR}")
    
    if args.dir:
        if args.dir not in available_dirs:
            console.print(f"[red]Error: Directory '{args.dir}' not found[/red]")
            console.print(f"[yellow]Available directories:[/yellow]")
            for d in available_dirs:
                console.print(f"  - {d}")
            return
        selected_dir = args.dir
    else:
        console.print("[#63D746]Available input directories:[/#63D746]")
        for i, d in enumerate(available_dirs, 1):
            in_path, out_path = setup_io_paths(d, BASE_DIR)
            remaining = get_remaining_files(in_path, out_path)
            total_files = len([f for f in os.listdir(in_path) if f.startswith("in_") and f.endswith(".json")])
            processed = total_files - len(remaining)
            console.print(f"  [{i}] {d} ({processed}/{total_files} processed, {len(remaining)} remaining)")
        
        while True:
            try:
                choice = input("\nSelect directory number (or 'q' to quit): ").strip()
                if choice.lower() == 'q':
                    return
                choice_num = int(choice)
                if 1 <= choice_num <= len(available_dirs):
                    selected_dir = available_dirs[choice_num - 1]
                    break
                else:
                    console.print("[red]Invalid selection. Please try again.[/red]")
            except (ValueError, KeyboardInterrupt):
                console.print("[yellow]Exiting...[/yellow]")
                return

    in_path, out_path = setup_io_paths(selected_dir, BASE_DIR)
    remaining = get_remaining_files(in_path, out_path)
    
    if not remaining:
        console.print(f"[green]All files in '{selected_dir}' have been processed![/green]")
        return
    
    console.print(f"\n[#48A630]Processing directory: {selected_dir}[/#48A630]")
    console.print(f"[#48A630]Input path: {in_path}[/#48A630]")
    console.print(f"[#48A630]Output path: {out_path}[/#48A630]")
    console.print(f"[#48A630]Files to process: {len(remaining)}[/#48A630]\n")

    signal.signal(signal.SIGINT, lambda s, f: shutdown_event.set())
    start = time.time()
    q = build_queue(in_path, out_path, remaining)
    total = q.qsize()

    for provider, models in active_providers:
        for model in models:
            exhausted[(provider, model)] = False
            last_call_ts[(provider, model)] = 0.0

    threads = []
    for provider, models in active_providers:
        for model in models:
            # Se è DeepSeek, crea 3 worker
            if provider == "deepseek":
                for worker_id in range(3):
                    w = WorkerId(provider, model, worker_id)
                    t = threading.Thread(target=worker_loop, args=(w, q), daemon=True)
                    t.start()
                    threads.append(t)
            else:
                w = WorkerId(provider, model, 0)
                t = threading.Thread(target=worker_loop, args=(w, q), daemon=True)
                t.start()
                threads.append(t)

    with Live(
        render_dashboard(total, 0, start, q.qsize(), selected_dir),
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
            live.update(render_dashboard(total, done_now, start, q.qsize(), selected_dir))
            time.sleep(0.2)

    for t in threads:
        t.join(timeout=1.0)

    console.print(render_dashboard(total, sum(worker_done.values()), start, q.qsize(), selected_dir))
    console.print(f"[#48A630]Completed {sum(worker_done.values())}/{total} files in '{selected_dir}'.[/#48A630]")

if __name__ == "__main__":
    main()