#!/usr/bin/env python3
"""
Multi-Provider Parallel Batch Processor v6 (Gemini + Groq + DeepSeek + OpenRouter + NVIDIA)

- Every provider is called through its OpenAI-compatible endpoint (one client, one code path)
- Several keys per provider: GEMINI_API_KEY, GEMINI_<LABEL>_API_KEY, GROQ_..., DEEPSEEK_..., OPENROUTER_..., NVIDIA_...
- Model availability is checked at startup: dead or renamed models are skipped instead of failing every file
- Rate limits per model and per account; slots are reserved without blocking the other providers
- 429s: per-minute limits cool down and retry, daily quotas retire the model (or the whole account)
- Files too large for a model are left to the models that can take them (no requeue spin)
- Built-in cleaners (fields_OUT_cleaner + gemini_mismatch_cleaner + small_folders_OUT_cleaner):
    only url/title kept, URL/title synced from input, extra and duplicate links dropped,
    missing links trimmed from the input (backup first), folder slugs normalized and merged,
    counts and sort order recomputed; outputs with too many missing links or tiny folders
    are rejected and handed to another model
- --clean-only runs the same cleaners over outputs that already exist

Usage:
    python tools/MULTI-PROVIDER_output_generator_API_v6.py [--dir working_split_IN--3]
        [--only gemini,openrouter] [--models gemini-flash,or-gemma-31b]
    python tools/MULTI-PROVIDER_output_generator_API_v6.py --list-models
    python tools/MULTI-PROVIDER_output_generator_API_v6.py --clean-only --dir working_split_IN--2 [--apply]
"""

# =========================
# Imports
# =========================
import os, re, json, time, shutil, signal, threading, logging, warnings, argparse, unicodedata
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime
from itertools import cycle
from logging.handlers import RotatingFileHandler

warnings.filterwarnings("ignore", category=UserWarning)

from dotenv import load_dotenv
import openai
from openai import OpenAI
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

# =========================
# Configuration
# =========================
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TOOLS_DIR)
load_dotenv(os.path.join(TOOLS_DIR, ".env"))

# Overridable so the Home Assistant add-on can keep the data on /share
BASE_DIR = os.environ.get("PIPELINE_DATA_DIR", os.path.join(ROOT, "in_out-s"))
RULES_FILE = os.environ.get("PIPELINE_RULES_FILE", os.path.join(ROOT, "ai_rules.txt"))
LOG_FILE = os.environ.get("PIPELINE_LOG_FILE", os.path.join(ROOT, "generator.log"))
RAW_DIR = os.path.join(BASE_DIR, "RAW")
REJECTED_DIR = os.path.join(RAW_DIR, "rejected")
BACKUP_DIR = os.path.join(BASE_DIR, "split_backup_cleaner")
REPORT_FILE = os.path.join(BASE_DIR, "generation_report.jsonl")
LAST_RUN_FILE = os.path.join(BASE_DIR, "last_run.json")

PROVIDERS = {
    "gemini": {"base_url": "https://generativelanguage.googleapis.com/v1beta/openai/", "account_rpm": None},
    "groq": {"base_url": "https://api.groq.com/openai/v1", "account_rpm": None},
    "deepseek": {"base_url": "https://api.deepseek.com", "account_rpm": None},
    # Free models share one account budget: 20 req/min, 50 req/day (1000/day once 10 credits were bought)
    "openrouter": {"base_url": "https://openrouter.ai/api/v1", "account_rpm": 20, "timeout": 900},
    # NVIDIA API catalog: free endpoints (DeepSeek included), 40 req/min per account, key from build.nvidia.com
    "nvidia": {"base_url": "https://integrate.api.nvidia.com/v1", "account_rpm": 40, "timeout": 900},
}

# max_input: prompt tokens accepted per request (learned downwards from 413/"Limit N" errors)
# max_tokens: output cap, None = provider default; json_mode: send response_format=json_object
# workers: parallel workers per API key; extra: provider-specific body params
# Probed 2026-09-26 on a 103-link file (--stats shows the live record from generation_report.jsonl):
#   gemini-3.5-flash best (0 missing, ~2 min); gemini-2.5-flash ok but loses links; nemotron-ultra good but ~9 min;
#   dots-3-note ok, coarser names; 3.6/3.7/3.8-flash were overloaded (untested).
#   Dropped: gemini-2.5-pro (worked in Jan 2026; now "limit: 0" on every free-tier quota; add back with billing), gemini-3.5-flash-lite (6/6 rejected), gemma-4 (504s, invalid JSON),
#   nemotron-3.5-lightning (errors), inkling (agent apps only), Groq gpt-oss (8k TPM incl. output fits ~1% of files).
# Groq free tier: qwen ITPM is 7000 real input tokens, ~9000 in our estimate units, so only small files go there.
MODELS = {
    "gemini-3-flash": dict(provider="gemini", id="gemini-3.5-flash", rpm=10, max_input=900_000),
    "gemini-flash": dict(provider="gemini", id="gemini-2.5-flash", rpm=10, max_input=900_000),
    # "gemini-pro": dict(provider="gemini", id="gemini-2.5-pro", rpm=2, max_input=900_000),  # needs a paid tier now
    "gemini-38-flash": dict(provider="gemini", id="gemini-3.8-flash", rpm=10, max_input=900_000),
    "gemini-37-flash": dict(provider="gemini", id="gemini-3.7-flash", rpm=10, max_input=900_000),
    "groq-qwen": dict(provider="groq", id="qwen/qwen3.8-27b", rpm=30, max_input=9_000,
                      max_tokens=8_000, extra={"reasoning_format": "hidden"}),
    "deepseek-chat": dict(provider="deepseek", id="deepseek-chat", rpm=60, max_input=56_000,
                          max_tokens=8_000, workers=3),
    # Free DeepSeek via NVIDIA (the official DeepSeek API has no free tier beyond a sign-up grant).
    # Probed 2026-09-26: requests wait 4-9 min in NVIDIA's queue, then answer in seconds; output passed the cleaner.
    "nv-deepseek-flash": dict(provider="nvidia", id="deepseek-ai/deepseek-v4.1-flash", rpm=40, max_input=200_000,
                              max_tokens=16_000, json_mode=False, stream=True, workers=3),
    "or-nemotron-ultra": dict(provider="openrouter", id="nvidia/nemotron-3-ultra-550b-a55b:free", rpm=20, max_input=900_000),
    "or-nemotron-super": dict(provider="openrouter", id="nvidia/nemotron-3-super-120b-a12b:free", rpm=20, max_input=250_000),
    "or-dots": dict(provider="openrouter", id="dots-studio/dots-3-note-preview:free", rpm=20, max_input=450_000),
    "or-qwen": dict(provider="openrouter", id="qwen/qwen3.8-27b:free", rpm=20, max_input=250_000),
}
for _cfg in MODELS.values():
    _cfg.setdefault("max_tokens", None)
    _cfg.setdefault("json_mode", True)
    _cfg.setdefault("workers", 1)
    _cfg.setdefault("extra", None)
    _cfg.setdefault("stream", False)

TEMPERATURE = 0.3           # low: the dataset needs consistent labels across models and runs
REQUEST_TIMEOUT = 300      # per provider override: PROVIDERS[...]['timeout']
MAX_TRANSIENT_RETRIES = 3
RETRY_BACKOFF_SECONDS = [5, 15, 30]
MAX_CONSECUTIVE_429 = 5     # cooldowns in a row before a model is considered exhausted
MAX_CONSECUTIVE_FATAL = 3   # API errors in a row before a model is considered broken
MAX_JOB_ATTEMPTS = 3        # rejected outputs per file before giving up on it
PRE_TAKE_WAIT = 5           # a worker waiting longer than this leaves jobs to the others

# Cleaner thresholds (same as the standalone cleaners)
MAX_MISSING_RATIO = 0.25
MAX_SINGLE_FOLDERS = 2
MAX_DOUBLE_FOLDERS = 3
MAX_FOLDER_SIZE = 30         # rules say ~20; beyond 30 the category is clearly too broad
MAX_CATCH_ALL_RATIO = 0.30   # share of links allowed in misc/general/other folders
CATCH_ALL_RE = re.compile(r"(^|-)(misc|miscellaneous|general|other|others|uncategorized|various|stuff|homepage|homepages)(-|$)")
LANGUAGE_FOLDER_RE = re.compile(
    r"(^|-)(german|italian|french|spanish|polish|dutch|czech|russian|japanese|chinese|portuguese)"
    r"-(sites|websites|pages|content|resources|links|language)(-|$)|(^|-)(foreign|non-english|multilingual)(-|$)"
)
# Stats that describe the result rather than count fixes
INFO_STATS = ("input_total", "single_folders", "double_folders", "max_folder", "catch_all_links", "language_folders")

with open(RULES_FILE, "r", encoding="utf-8") as f:
    AI_RULES = f.read().strip()

SYSTEM_PROMPT = "You categorize bookmarks. Reply with a single valid JSON object and nothing else."

logger = logging.getLogger("batch_v6")
logger.setLevel(logging.INFO)
_fh = RotatingFileHandler(LOG_FILE, maxBytes=20 * 1024 * 1024, backupCount=3, encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_fh)
logger.propagate = False

# =========================
# Data structures
# =========================
@dataclass
class Job:
    filename: str
    in_file: str
    out_file: str
    tokens: int
    tried: set = field(default_factory=set)       # models that already failed this file
    too_large: set = field(default_factory=set)   # models whose request limit it exceeds
    attempts: int = 0
    last_reason: str = ""


@dataclass(frozen=True)
class WorkerId:
    model: str
    key_label: str
    idx: int = 0

    @property
    def provider(self):
        return MODELS[self.model]["provider"]

    @property
    def label(self):
        s = self.model
        if self.key_label != "main":
            s += f"@{self.key_label}"
        if MODELS[self.model]["workers"] > 1:
            s += f"#{self.idx + 1}"
        return s


class CleanError(Exception):
    pass

# =========================
# Globals
# =========================
console = Console()
shutdown_event = threading.Event()
state_lock = threading.Lock()
report_lock = threading.Lock()

worker_state = {}      # WorkerId -> {"state", "file", "done", "rejected", "note"}
live_workers = set()
retired = {}           # scope tuple -> reason
counters_429 = Counter()
counters_fatal = Counter()
model_limits = {}      # model -> learned max_input
clients = {}           # (provider, key_label) -> OpenAI
api_keys = {}          # provider -> [(label, key)]
run_totals = Counter()
run_done_by_model = Counter()
vocab = None           # NameVocabulary, built in main()

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

# =========================
# Keys, clients, availability
# =========================
KEY_RE = re.compile(r"(GEMINI|GROQ|DEEPSEEK|OPENROUTER|NVIDIA)(?:_(\w+?))?_API_KEY")


def discover_keys():
    found = {}
    for name, value in sorted(os.environ.items()):
        m = KEY_RE.fullmatch(name)
        if m and value.strip():
            found.setdefault(m.group(1).lower(), []).append(((m.group(2) or "main").lower(), value.strip()))
    return found


def get_client(provider, key_label):
    return clients[(provider, key_label)]


def check_availability(provider, models):
    """Drop models the provider no longer serves; returns the models kept."""
    label = api_keys[provider][0][0]
    try:
        listed = get_client(provider, label).models.list().data
    except openai.AuthenticationError as e:
        console.print(f"[red]{provider}: key rejected ({e.status_code}), provider skipped[/red]")
        return []
    except Exception as e:
        console.print(f"[yellow]{provider}: could not list models ({type(e).__name__}), keeping configuration[/yellow]")
        return models

    by_id = {m.id.removeprefix("models/"): m for m in listed}
    kept = []
    for name in models:
        cfg = MODELS[name]
        info = by_id.get(cfg["id"])
        if info is None:
            console.print(f"[yellow]{provider}: {cfg['id']} is not available anymore, {name} skipped[/yellow]")
            continue
        if provider == "openrouter":
            extra = info.model_extra or {}
            params = extra.get("supported_parameters") or []
            cfg["json_mode"] = "response_format" in params
            if extra.get("context_length"):
                cfg["max_input"] = min(cfg["max_input"], int(extra["context_length"]) - 16_000)
        kept.append(name)
    return kept

# =========================
# Rate limiting & retirement
# =========================
class RateLimiter:
    """Reserves call slots per model and per account; sleeps happen outside the lock."""

    def __init__(self):
        self.lock = threading.Lock()
        self.next_free = {}

    @staticmethod
    def buckets(w):
        out = [(("model", w.model, w.key_label), MODELS[w.model]["rpm"])]
        acc_rpm = PROVIDERS[w.provider]["account_rpm"]
        if acc_rpm:
            out.append((("account", w.provider, w.key_label), acc_rpm))
        return out

    def delay(self, w):
        with self.lock:
            start = max(self.next_free.get(b, 0.0) for b, _ in self.buckets(w))
        return max(0.0, start - time.time())

    def acquire(self, w):
        with self.lock:
            buckets = self.buckets(w)
            start = max([time.time()] + [self.next_free.get(b, 0.0) for b, _ in buckets])
            for b, rpm in buckets:
                self.next_free[b] = start + 60.0 / max(1, rpm)
        wait = start - time.time()
        return not (wait > 0 and shutdown_event.wait(wait))

    def cooldown(self, bucket, seconds):
        with self.lock:
            self.next_free[bucket] = max(self.next_free.get(bucket, 0.0), time.time() + seconds)


limiter = RateLimiter()


def retire(scope, reason):
    with state_lock:
        if scope not in retired:
            retired[scope] = reason
            logger.warning(f"RETIRED {scope}: {reason}")
            new = True
        else:
            new = False
    if new:
        report({"status": "retired", "model": scope[1] if scope[0] == "model" else f"{scope[1]} (account)",
                "reason": reason, "done_this_run": run_totals["done_total"]})


def retired_reason(w):
    with state_lock:
        for scope in (("model", w.model, w.key_label), ("model", w.model, "*"), ("account", w.provider, w.key_label)):
            if scope in retired:
                return retired[scope]
    return None


def model_limit(model):
    with state_lock:
        return model_limits.get(model, MODELS[model]["max_input"])


def learn_limit(model, limit):
    with state_lock:
        model_limits[model] = min(model_limits.get(model, MODELS[model]["max_input"]), limit)


def active_models():
    with state_lock:
        workers = list(live_workers)
    return {w.model for w in workers if not retired_reason(w)}


def set_status(w, state, file=None, note=None, done=0, rejected=0):
    with state_lock:
        st = worker_state.setdefault(w, {"state": "idle", "file": None, "done": 0, "rejected": 0, "note": ""})
        st["state"], st["file"] = state, file
        st["done"] += done
        st["rejected"] += rejected
        if note is not None:
            st["note"] = note

# =========================
# Job pool
# =========================
class JobPool:
    """Hands each worker the first job it can actually process; no requeue spinning."""

    def __init__(self, jobs):
        self.pending = deque(jobs)
        self.in_flight = 0
        self.failed = []
        self.cond = threading.Condition()

    def eligible(self, w, job):
        if job.attempts >= MAX_JOB_ATTEMPTS or w.model in job.too_large:
            return False
        if job.tokens > model_limit(w.model):
            return False
        return w.model not in job.tried or active_models() <= job.tried

    def take(self, w):
        with self.cond:
            while not shutdown_event.is_set():
                if retired_reason(w):
                    return None
                for job in self.pending:
                    if self.eligible(w, job):
                        self.pending.remove(job)
                        self.in_flight += 1
                        return job
                if self.in_flight == 0:
                    return None
                self.cond.wait(timeout=1.0)
            return None

    def finish(self, job, outcome):
        with self.cond:
            self.in_flight -= 1
            if outcome == "retry":
                self.pending.appendleft(job)
            elif outcome == "failed":
                self.failed.append(job)
            self.cond.notify_all()

    def remaining(self):
        with self.cond:
            return len(self.pending) + self.in_flight

# =========================
# Cleaners
# =========================
def normalize_url(url):
    if not isinstance(url, str) or not url.strip():
        return ""
    u = url.strip()
    u = re.sub(r"^https?://", "", u, flags=re.IGNORECASE)
    u = re.sub(r"^www\.", "", u, flags=re.IGNORECASE)
    u = u.split("?", 1)[0].split("#", 1)[0]
    if u.endswith("/"):
        u = u[:-1]
    return u.lower()


TRACKING_PARAM_RE = re.compile(r"^(utm_\w*|gclid|fbclid|dclid|msclkid|mc_cid|mc_eid|igshid|ref_src)$", re.IGNORECASE)


def url_key(url):
    """normalize_url plus the query minus tracking params, so ?id=1 and ?id=2 stay distinct links."""
    base = normalize_url(url)
    if not base:
        return ""
    query = url.split("#", 1)[0].partition("?")[2]
    params = sorted(p for p in query.split("&") if p and not TRACKING_PARAM_RE.match(p.split("=", 1)[0]))
    return f"{base}?{'&'.join(params)}".lower() if params else base


# Spellings that can only mean one thing; context-dependent ones (dev, apps, docs, info) are left alone
SLUG_ALIASES = {"non-profit": "nonprofit", "non-profits": "nonprofits", "e-commerce": "ecommerce",
                "orgs": "organizations", "govt": "government"}
# Plural and singular carry different meanings for these, so they are never folded together
NO_PLURAL_FOLD = {"news", "goods", "interests", "economics", "civics", "outdoors", "arts", "sports", "series", "species"}


def slugify_folder(name):
    raw = str(name or "")
    s = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
    if not s:
        # Non-Latin name (e.g. 料理): keep its letters instead of losing the label
        s = re.sub(r"[\W_]+", "-", raw.lower()).strip("-")
    while "-and-" in s:
        s = s.replace("-and-", "-")
    for alias, target in SLUG_ALIASES.items():
        s = re.sub(rf"(^|-){alias}(?=-|$)", rf"\g<1>{target}", s)
    return s or "general-uncategorized"


class NameVocabulary:
    """Folder names seen across the dataset; word-order and plural variants of the same words map to
    the most used form (blog-personal -> personal-blogs), so a label is always spelled the same way.
    Different words are never merged: synonyms can carry context (learning vs resources)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.forms = {}   # sorted words -> Counter of spellings

    @classmethod
    def from_outputs(cls, base_dir=BASE_DIR):
        vocab = cls()
        for d in os.listdir(base_dir):
            if not re.match(r"^working_split_OUT--API-\d+$", d):
                continue
            for name in os.listdir(os.path.join(base_dir, d)):
                if not (name.startswith("out_") and name.endswith(".json")):
                    continue
                try:
                    folders = load_json(os.path.join(base_dir, d, name)).get("folders", [])
                except (json.JSONDecodeError, AttributeError, OSError):
                    continue
                vocab.add(slugify_folder(f.get("name")) for f in folders if isinstance(f, dict))
        return vocab

    @staticmethod
    def _fold(word):
        if word in NO_PLURAL_FOLD or len(word) < 4:
            return word
        if word.endswith("ies"):
            return word[:-3] + "y"
        if word.endswith("s") and not word.endswith("ss"):
            return word[:-1]
        return word

    @classmethod
    def _key(cls, slug):
        return tuple(sorted(cls._fold(w) for w in slug.split("-")))

    def add(self, slugs):
        with self.lock:
            for slug in slugs:
                self.forms.setdefault(self._key(slug), Counter())[slug] += 1

    def canonical(self, slug):
        with self.lock:
            forms = self.forms.get(self._key(slug))
            if not forms:
                return slug
            return min(forms.items(), key=lambda kv: (-kv[1], kv[0]))[0]


def clean_pair(in_data, out_data, vocab=None):
    """Returns (cleaned output, trimmed input or None, stats). Raises CleanError on unusable output."""
    stats = Counter()
    input_map, loose_index = {}, {}
    for item in in_data.get("data", []):
        if isinstance(item, dict):
            n = url_key(item.get("url"))
            if not n:
                stats["input_invalid"] += 1
            elif n in input_map:
                stats["input_duplicates"] += 1
            else:
                input_map[n] = item
                loose_index.setdefault(normalize_url(item["url"]), []).append(n)
    stats["input_total"] = len(input_map)

    def resolve(url):
        # Exact match first; without the query only when that points to a single input link
        n = url_key(url)
        if n in input_map:
            return n
        candidates = loose_index.get(normalize_url(url), [])
        return candidates[0] if len(candidates) == 1 else None

    folders_in = out_data.get("folders") if isinstance(out_data, dict) else out_data
    if not isinstance(folders_in, list):
        raise CleanError("output has no folders list")

    merged, seen = {}, set()
    for folder in folders_in:
        if not isinstance(folder, dict) or not isinstance(folder.get("bookmarks"), list):
            stats["bad_folders"] += 1
            continue
        slug = slugify_folder(folder.get("name"))
        if slug != folder.get("name"):
            stats["renamed_folders"] += 1
        if vocab is not None:
            canonical = vocab.canonical(slug)
            if canonical != slug:
                stats["canonical_names"] += 1
                slug = canonical
        if slug in merged:
            stats["merged_folders"] += 1
        bucket = merged.setdefault(slug, [])
        for bm in folder["bookmarks"]:
            if isinstance(bm, str):
                bm = {"url": bm}
                stats["string_bookmarks"] += 1
            if not isinstance(bm, dict):
                continue
            if not isinstance(bm.get("url"), str) or not bm["url"].strip():
                continue
            n = resolve(bm["url"])
            if n is None:
                stats["extra"] += 1
                continue
            if n in seen:
                stats["duplicates"] += 1
                continue
            seen.add(n)
            if set(bm) - {"url", "title"}:
                stats["stripped_fields"] += 1
            src = input_map[n]
            url = src["url"].strip()
            out_title = str(bm.get("title") or "").strip()
            title = str(src.get("title") or "").strip() or out_title
            if bm.get("url") != url:
                stats["url_synced"] += 1
            if title != out_title:
                stats["title_synced"] += 1
            bucket.append({"url": url, "title": title})

    folders = [
        {"name": slug, "bookmarks": sorted(bms, key=lambda b: b["title"].casefold()), "count": len(bms)}
        for slug, bms in merged.items() if bms
    ]
    folders.sort(key=lambda f: (-f["count"], f["name"]))
    cleaned = {
        "folders": folders,
        "total_bookmarks": sum(f["count"] for f in folders),
        "num_folders": len(folders),
    }

    missing = set(input_map) - seen
    stats["missing"] = len(missing)
    stats["single_folders"] = sum(1 for f in folders if f["count"] == 1)
    stats["double_folders"] = sum(1 for f in folders if f["count"] == 2)
    stats["max_folder"] = max((f["count"] for f in folders), default=0)
    stats["catch_all_links"] = sum(f["count"] for f in folders if CATCH_ALL_RE.search(f["name"]))
    stats["language_folders"] = sum(1 for f in folders if LANGUAGE_FOLDER_RE.search(f["name"]))

    # The input must list exactly the links of the output: drop missing, duplicate and URL-less items
    data = in_data.get("data", [])
    kept, kept_keys = [], set()
    for it in data:
        n = url_key(it.get("url")) if isinstance(it, dict) else ""
        if not n or n in missing or n in kept_keys:
            continue
        kept_keys.add(n)
        kept.append(it)

    trimmed_in = None
    if len(kept) != len(data):
        trimmed_in = dict(in_data)
        trimmed_in["data"] = kept
        meta = dict(in_data.get("_meta") or {})
        meta["total_bookmarks"] = len(kept)
        trimmed_in["_meta"] = meta
    return cleaned, trimmed_in, stats


def rejection_reason(cleaned, stats):
    if not cleaned["folders"]:
        return "no usable bookmarks"
    total = stats["input_total"]
    if total and stats["missing"] / total > MAX_MISSING_RATIO:
        return f"missing {stats['missing']}/{total} links"
    if stats["single_folders"] > MAX_SINGLE_FOLDERS:
        return f"{stats['single_folders']} single-link folders"
    if stats["double_folders"] > MAX_DOUBLE_FOLDERS:
        return f"{stats['double_folders']} two-link folders"
    if stats["max_folder"] > MAX_FOLDER_SIZE:
        return f"folder with {stats['max_folder']} links"
    kept = sum(f["count"] for f in cleaned["folders"])
    if kept and stats["catch_all_links"] / kept > MAX_CATCH_ALL_RATIO:
        return f"{stats['catch_all_links']}/{kept} links in catch-all folders"
    if stats["language_folders"]:
        return "folders grouped by language"
    return None

# =========================
# File helpers
# =========================
def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json_atomic(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def backup_once(path):
    """Keeps the first version of a file in split_backup_cleaner/<its folder>/."""
    dst_dir = os.path.join(BACKUP_DIR, os.path.basename(os.path.dirname(path)))
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, os.path.basename(path))
    if not os.path.exists(dst):
        shutil.copy2(path, dst)


def save_rejected(job, model, text):
    os.makedirs(REJECTED_DIR, exist_ok=True)
    name = os.path.basename(job.out_file).replace(".json", f"__{model}.txt")
    with open(os.path.join(REJECTED_DIR, name), "w", encoding="utf-8") as f:
        f.write(text)


def report(entry):
    entry = {"ts": datetime.now().isoformat(timespec="seconds"), **entry}
    with report_lock, open(REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def extract_json(text):
    t = text.strip()
    t = re.sub(r"^```[a-zA-Z0-9]*\s*", "", t)
    t = re.sub(r"\s*```\s*$", "", t)
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass
    # Models sometimes wrap the object in prose or leave reasoning blocks in the content
    t = re.sub(r"<think>.*?</think>", "", t, flags=re.S)
    start, end = t.find("{"), t.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(t[start:end + 1])
        except json.JSONDecodeError:
            pass
    return None

# =========================
# API calls & error classification
# =========================
DAILY_MARKERS = ("per day", "per-day", "perday", "(tpd)", "(rpd)", "daily")
CONTEXT_MARKERS = (
    "request too large", "context length", "context_length_exceeded", "maximum context",
    "token limit", "tokens exceeded", "input too long", "prompt is too long", "exceeds maximum",
    "prompt length", "too many tokens", "reduce the length", "context window",
)


def build_prompt(in_data):
    return f"""{AI_RULES}

Now categorize the following data according to the above rules.
Additional constraints:
- Every input URL must appear exactly once in the output.
- Bookmarks can be in any language: categorize by meaning, never by language or country
  (no folders like "german-sites"); links on the same topic share a folder whatever their language.
- Folder names are always English slugs. Copy titles exactly as given, in their original language.
- Split folders >20 links into coherent subgroups.
- Keep balanced grouping.
- CRITICAL: Only include `url` and `title` fields.
- CRITICAL: Avoid folders with only 1 or 2 links.

Return only valid, minified JSON.

{json.dumps(in_data, ensure_ascii=False, separators=(",", ":"))}"""


def estimate_tokens(prompt):
    return len(prompt) // 3


def call_model(w, prompt):
    cfg = MODELS[w.model]
    kwargs = dict(
        model=cfg["id"],
        messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
        temperature=TEMPERATURE,
    )
    if cfg["max_tokens"]:
        kwargs["max_tokens"] = cfg["max_tokens"]
    if cfg["json_mode"]:
        kwargs["response_format"] = {"type": "json_object"}
    if cfg["extra"]:
        kwargs["extra_body"] = cfg["extra"]

    client = get_client(w.provider, w.key_label)
    if cfg["stream"]:
        # Streaming keeps the connection alive while a slow free endpoint queues the request
        # (NVIDIA's gateway returns 504 on a non-streamed request after ~300s)
        parts, finish = [], None
        for chunk in client.chat.completions.create(stream=True, **kwargs):
            if shutdown_event.is_set():
                raise RuntimeError("interrupted (temporarily unavailable)")
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            if delta and delta.content:
                parts.append(delta.content)
            finish = chunk.choices[0].finish_reason or finish
        text = "".join(parts)
        if not text.strip():
            raise RuntimeError(f"empty response (finish_reason={finish}, temporarily unavailable)")
        return text, finish

    resp = client.chat.completions.create(**kwargs)
    body_error = (resp.model_extra or {}).get("error")
    if body_error:
        raise RuntimeError(f"provider error in body: {body_error}")
    if not resp.choices:
        raise RuntimeError("provider returned no choices (temporarily unavailable)")
    choice = resp.choices[0]
    text = (choice.message.content or "") if choice.message else ""
    if not text.strip():
        raise RuntimeError(f"empty response (finish_reason={choice.finish_reason}, temporarily unavailable)")
    return text, choice.finish_reason


def error_text(e):
    parts = [str(e)]
    body = getattr(e, "body", None)
    if body:
        parts.append(json.dumps(body, default=str))
    return " ".join(parts).lower()


def classify_error(e):
    if isinstance(e, (openai.APITimeoutError, openai.APIConnectionError)):
        return "transient"
    msg = error_text(e)
    status = getattr(e, "status_code", None)
    if status is None:
        return "transient" if any(x in msg for x in ("temporarily", "timeout", "unavailable", "overloaded")) else "fatal"
    if status == 413 or (status in (400, 429) and any(m in msg for m in CONTEXT_MARKERS)):
        return "too_large"
    if status == 429:
        if "free-models-per-day" in msg:
            return "account_quota"
        if any(m in msg for m in DAILY_MARKERS):
            return "daily_quota"
        return "rate_limit"
    if status == 402:
        return "account_quota"
    if status == 401:
        return "auth"
    if status == 403:
        # 403 is usually about the model (restricted, region, moderation), not the key
        if any(x in msg for x in ("api key", "api_key", "invalid key", "unauthorized", "no auth")):
            return "auth"
        return "model_forbidden"
    if status == 404:
        return "model_gone"
    if status == 503 and any(x in msg for x in ("high demand", "overloaded")):
        return "busy"
    if status in (408, 409) or status >= 500:
        return "transient"
    return "fatal"


def retry_after_seconds(e, default=60.0):
    msg = error_text(e)
    resp = getattr(e, "response", None)
    headers = resp.headers if resp is not None else {}
    candidates = []
    if headers.get("retry-after"):
        try:
            candidates.append(float(headers["retry-after"]))
        except ValueError:
            pass
    m = re.search(r"x-ratelimit-reset['\"]?\s*:\s*['\"]?(\d{13})", msg)
    if m:
        candidates.append(int(m.group(1)) / 1000 - time.time())
    m = re.search(r"(?:retry in|try again in|retrydelay['\"]?:\s*['\"]?)\s*(?:(\d+)m)?(\d+(?:\.\d+)?)s", msg)
    if m:
        candidates.append(int(m.group(1) or 0) * 60 + float(m.group(2)))
    wait = max(candidates) if candidates else default
    return min(max(wait, 5.0), 300.0)


def short(msg, n=70):
    msg = " ".join(str(msg).split())
    return msg if len(msg) <= n else msg[:n - 1] + "…"

# =========================
# Processing
# =========================
def reject(w, job, reason, raw=None, count_attempt=True):
    job.tried.add(w.model)
    job.last_reason = f"{w.model}: {reason}"
    if count_attempt:
        job.attempts += 1
        run_totals["rejected"] += 1
    if raw:
        save_rejected(job, w.model, raw)
    logger.info(f"{w.label} | {job.filename} rejected: {reason}")
    report({"file": job.filename, "model": w.model, "status": "rejected", "reason": reason})
    set_status(w, "rejected", job.filename, rejected=1 if count_attempt else 0)
    return "failed" if job.attempts >= MAX_JOB_ATTEMPTS else "retry"


def learn_from_too_large(w, job, msg):
    """Turns 'Limit 8000, Requested 14306' into a max_input in our estimate units, so the model
    stops receiving files it can never accept. TPM limits count max_tokens too, ITPM only input."""
    m = re.search(r"limit:?\s*(\d+),\s*requested:?\s*(\d+)", msg)
    if not m:
        return
    limit, requested = int(m.group(1)), int(m.group(2))
    output_budget = 0 if "itpm" in msg else (MODELS[w.model]["max_tokens"] or 0)
    real_input = requested - output_budget
    if real_input <= 0:
        return
    allowed = limit - output_budget
    if allowed <= 1000:
        retire(("model", w.model, w.key_label), f"request limit {limit} tokens leaves no room for input")
        return
    learned = int(job.tokens * allowed / real_input * 0.95)
    learn_limit(w.model, learned)
    logger.info(f"{w.label} learned max input ~{learned} (limit {limit}, requested {requested})")


def handle_api_error(w, job, e):
    kind = classify_error(e)
    detail = short(error_text(e), 200)
    logger.error(f"{w.label} | {job.filename} | {kind} | {type(e).__name__}: {detail}")

    if kind == "too_large":
        job.too_large.add(w.model)
        learn_from_too_large(w, job, error_text(e))
        return "retry"
    if kind == "busy":
        limiter.cooldown(("model", w.model, w.key_label), 60)
        set_status(w, "cooldown", note="provider busy, waiting 60s")
        return "retry"
    if kind == "rate_limit":
        bucket = ("model", w.model, w.key_label)
        if w.provider == "openrouter" and "free-models-per-min" in detail:
            bucket = ("account", w.provider, w.key_label)
        wait = retry_after_seconds(e)
        limiter.cooldown(bucket, wait)
        with state_lock:
            counters_429[(w.model, w.key_label)] += 1
            n = counters_429[(w.model, w.key_label)]
        if n >= MAX_CONSECUTIVE_429:
            retire(("model", w.model, w.key_label), f"{n} rate limits in a row")
        set_status(w, "cooldown", note=f"429, waiting {wait:.0f}s")
        return "retry"
    if kind == "daily_quota":
        if re.search(r"limit: 0\b", error_text(e)):
            retire(("model", w.model, w.key_label), "no free-tier quota for this model (limit 0)")
        else:
            retire(("model", w.model, w.key_label), "daily quota reached")
        return "retry"
    if kind == "account_quota":
        retire(("account", w.provider, w.key_label), "account quota/credits exhausted")
        return "retry"
    if kind == "auth":
        retire(("account", w.provider, w.key_label), "key rejected")
        return "retry"
    if kind == "model_forbidden":
        retire(("model", w.model, w.key_label), f"model refused: {short(detail, 60)}")
        return "retry"
    if kind == "model_gone":
        retire(("model", w.model, "*"), "model not found (404)")
        return "retry"

    # fatal: skip this file for this model; retire models that keep failing
    with state_lock:
        counters_fatal[w.model] += 1
        n = counters_fatal[w.model]
    if n >= MAX_CONSECUTIVE_FATAL:
        retire(("model", w.model, "*"), f"API errors: {short(detail, 60)}")
    return reject(w, job, f"api error: {short(detail, 80)}", count_attempt=False)


def run_job(w, job):
    if os.path.exists(job.out_file):
        set_status(w, "skipped", job.filename)
        return "done"

    in_data = load_json(job.in_file)
    prompt = build_prompt(in_data)
    set_status(w, "processing", job.filename)

    for attempt in range(1, MAX_TRANSIENT_RETRIES + 1):
        if not limiter.acquire(w):
            return "retry"
        try:
            text, finish = call_model(w, prompt)
        except Exception as e:
            if classify_error(e) == "transient" and attempt < MAX_TRANSIENT_RETRIES:
                logger.info(f"{w.label} | {job.filename} transient error, retry {attempt}: {short(error_text(e), 120)}")
                if shutdown_event.wait(RETRY_BACKOFF_SECONDS[attempt - 1]):
                    return "retry"
                continue
            # Transient errors that outlive the retries count as API errors for this model
            return handle_api_error(w, job, e)
        break

    with state_lock:
        counters_429[(w.model, w.key_label)] = 0
        counters_fatal[w.model] = 0

    if finish == "length":
        return reject(w, job, "output truncated", raw=text)
    parsed = extract_json(text)
    if parsed is None:
        return reject(w, job, "invalid JSON", raw=text)
    try:
        cleaned, trimmed_in, stats = clean_pair(in_data, parsed, vocab)
    except CleanError as e:
        return reject(w, job, str(e), raw=text)
    reason = rejection_reason(cleaned, stats)
    if reason:
        return reject(w, job, reason, raw=text)

    if os.path.exists(job.out_file):
        set_status(w, "skipped", job.filename)
        return "done"
    if trimmed_in is not None:
        backup_once(job.in_file)
        write_json_atomic(job.in_file, trimmed_in)
    write_json_atomic(job.out_file, cleaned)
    if vocab is not None:
        vocab.add(f["name"] for f in cleaned["folders"])

    run_totals["done"] += 1
    run_totals["done_total"] += 1
    with state_lock:
        run_done_by_model[w.model] += 1
    logger.info(f"{w.label} | {job.filename} done {dict(stats)}")
    report({"file": job.filename, "model": w.model, "status": "done", "stats": dict(stats)})
    set_status(w, "done", job.filename, note="", done=1)
    return "done"


def worker_loop(w, pool):
    logger.info(f"{w.label} starting")
    set_status(w, "idle")
    try:
        while not shutdown_event.is_set():
            reason = retired_reason(w)
            if reason:
                set_status(w, "retired", note=reason)
                break
            delay = limiter.delay(w)
            if delay > PRE_TAKE_WAIT:
                # Leave the jobs to the others while cooling down
                with state_lock:
                    if worker_state[w]["state"] != "cooldown":
                        worker_state[w]["state"] = "waiting"
                shutdown_event.wait(min(delay, 5.0))
                continue
            job = pool.take(w)
            if job is None:
                break
            try:
                outcome = run_job(w, job)
            except Exception as e:
                logger.exception(f"{w.label} | {job.filename} crashed")
                outcome = reject(w, job, f"crash: {type(e).__name__}: {e}")
            pool.finish(job, outcome)
    finally:
        with state_lock:
            live_workers.discard(w)
            st = worker_state[w]
            if st["state"] != "retired":
                st["state"], st["file"] = "stopped", None
        logger.info(f"{w.label} stopped")

# =========================
# Rich UI
# =========================
STATUS_STYLES = {
    "processing": ("PROCESSING {work}", "#63D746"),
    "done": ("DONE {idle}", "#48A630"),
    "skipped": ("SKIPPED", "#48A630"),
    "rejected": ("REJECTED", "#FFB000"),
    "cooldown": ("COOLDOWN {sleep}", "#A8EE59"),
    "waiting": ("WAITING {sleep}", "#A8EE59"),
    "retired": ("RETIRED {sleep}", "#FF5F1F"),
    "idle": ("IDLE {idle}", "#A8EE59"),
    "stopped": ("STOPPED {sleep}", "#48A630"),
}


def render_dashboard(workers, total, start_time, pool, in_dir_name):
    elapsed = (time.time() - start_time) / 60
    done = run_totals["done"]
    rate = done / elapsed if elapsed > 0 else 0.0
    pct = (done / total * 100) if total else 0

    with state_lock:
        snapshot = {w: dict(st) for w, st in worker_state.items()}
    active = sum(1 for st in snapshot.values() if st["state"] == "processing")

    stats_table = Table(box=box.DOUBLE, expand=True, show_header=False, pad_edge=True, border_style="#48A630")
    stats_table.add_column("Metric", justify="left", ratio=2, style="#63D746 bold")
    stats_table.add_column("Value", justify="left", ratio=2, style="#63D746")
    stats_table.add_row("Directory", in_dir_name)
    stats_table.add_row("Files", f"{done}/{total} ({pct:.1f}%)")
    stats_table.add_row("Remaining", str(pool.remaining()))
    stats_table.add_row("Rejected", f"{run_totals['rejected']} outputs / {len(pool.failed)} files given up")
    stats_table.add_row("Active", str(active))
    stats_table.add_row("Rate", f"{rate:.1f}/min")
    stats_table.add_row("Elapsed", f"{elapsed:.1f}m")

    table = Table(box=box.DOUBLE_EDGE, expand=True, show_header=True, header_style="bold #63D746",
                  border_style="#48A630", pad_edge=True)
    table.add_column("Provider", style="#63D746", ratio=2)
    table.add_column("Model", style="#63D746", ratio=3)
    table.add_column("RPM", justify="center", style="#63D746", ratio=1)
    table.add_column("Done", justify="center", style="#63D746", ratio=1)
    table.add_column("Rej", justify="center", style="#63D746", ratio=1)
    table.add_column("Status", justify="left", style="#63D746", ratio=3)
    table.add_column("File / note", justify="left", style="#63D746", ratio=4)

    faces = dict(work=next(work_cycle), sleep=next(sleep_cycle), idle=next(idle_cycle))
    last_provider = None
    for w in workers:
        st = snapshot.get(w, {"state": "idle", "file": None, "done": 0, "rejected": 0, "note": ""})
        fmt, style = STATUS_STYLES.get(st["state"], (st["state"].upper(), "#48A630"))
        info = st["file"] or "-"
        if st["note"] and st["state"] in ("retired", "cooldown", "waiting", "stopped"):
            info = st["note"]
        table.add_row(
            w.provider.upper() if w.provider != last_provider else "",
            w.label,
            str(MODELS[w.model]["rpm"]),
            str(st["done"]),
            str(st["rejected"]),
            Text(fmt.format(**faces), style=style),
            short(info, 48),
        )
        last_provider = w.provider

    stats_height = len(stats_table.rows)
    pad = "\n" * max(0, stats_height // 2 - 2)
    header_text = pad + "MULTI-PROVIDER BATCH PROCESSOR\ngemini + groq + deepseek + openrouter + nvidia" + pad
    inner_panel = Panel(Text(header_text, style="#63D746 bold", justify="center"),
                        box=box.DOUBLE, border_style="#48A630", padding=(1, 4))
    header_panel = Panel(inner_panel, title="[#48A630]v 6.0[/#48A630]", box=box.ROUNDED, border_style="#48A630")
    stats_panel = Panel(stats_table, title="[#48A630]Statistics[/#48A630]", border_style="#48A630", box=box.ROUNDED)

    top_row = Table.grid(expand=True, padding=(0, 1))
    top_row.add_column(ratio=2)
    top_row.add_column(ratio=2)
    top_row.add_row(header_panel, stats_panel)

    layout = Table.grid(expand=True)
    layout.add_row(top_row)
    layout.add_row(Panel(table, title="[#48A630]Workers[/#48A630]", border_style="#48A630", box=box.ROUNDED))
    return layout

# =========================
# Directories
# =========================
def list_available_directories(base_dir=BASE_DIR):
    return sorted(d for d in os.listdir(base_dir) if re.match(r"^working_split_IN--\d+$", d))


def setup_io_paths(in_dir_name, base_dir=BASE_DIR):
    in_path = os.path.join(base_dir, in_dir_name)
    out_path = os.path.join(base_dir, f"working_split_OUT--API-{in_dir_name.split('--')[-1]}")
    os.makedirs(out_path, exist_ok=True)
    return in_path, out_path


def list_inputs(in_path):
    return sorted(f for f in os.listdir(in_path) if f.startswith("in_") and f.endswith(".json"))


def get_remaining_files(in_path, out_path):
    out_files = {f for f in os.listdir(out_path) if f.startswith("out_") and f.endswith(".json")}
    return [f for f in list_inputs(in_path) if f.replace("in_", "out_", 1) not in out_files]


def select_directory(arg_dir):
    available = list_available_directories()
    if not available:
        raise RuntimeError(f"No input directories found in {BASE_DIR}")
    if arg_dir:
        if arg_dir not in available:
            console.print(f"[red]Directory '{arg_dir}' not found. Available: {', '.join(available)}[/red]")
            return None
        return arg_dir

    console.print("[#63D746]Available input directories:[/#63D746]")
    for i, d in enumerate(available, 1):
        in_path, out_path = setup_io_paths(d)
        total = len(list_inputs(in_path))
        remaining = len(get_remaining_files(in_path, out_path))
        console.print(f"  [{i}] {d} ({total - remaining}/{total} processed, {remaining} remaining)")
    while True:
        try:
            choice = input("\nSelect directory number (or 'q' to quit): ").strip()
        except (EOFError, KeyboardInterrupt):
            return None
        if choice.lower() == "q":
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(available):
            return available[int(choice) - 1]
        console.print("[red]Invalid selection. Please try again.[/red]")

# =========================
# Clean-only mode
# =========================
def clean_existing(in_dir_name, apply, vocab):
    in_path, out_path = setup_io_paths(in_dir_name)
    rejected_dir = os.path.join(REJECTED_DIR, os.path.basename(out_path))
    changed = rejected = checked = 0
    totals = Counter()

    for in_name in list_inputs(in_path):
        out_name = in_name.replace("in_", "out_", 1)
        in_file, out_file = os.path.join(in_path, in_name), os.path.join(out_path, out_name)
        if not os.path.exists(out_file):
            continue
        checked += 1
        try:
            in_data, out_data = load_json(in_file), load_json(out_file)
            cleaned, trimmed_in, stats = clean_pair(in_data, out_data, vocab)
            reason = rejection_reason(cleaned, stats)
        except (json.JSONDecodeError, CleanError) as e:
            cleaned, trimmed_in, stats, reason = None, None, Counter(), f"unreadable: {e}"

        if reason:
            rejected += 1
            console.print(f"[#FF5F1F]✗ {out_name}: {reason}[/#FF5F1F]")
            if apply:
                os.makedirs(rejected_dir, exist_ok=True)
                shutil.move(out_file, os.path.join(rejected_dir, out_name))
            continue

        if cleaned == out_data and trimmed_in is None:
            continue
        changed += 1
        totals.update(stats)
        fixes = {k: v for k, v in stats.items()
                 if v and k not in INFO_STATS}
        console.print(f"[#63D746]~ {out_name}: {fixes or 'sort/counts'}[/#63D746]")
        if apply:
            backup_once(out_file)
            write_json_atomic(out_file, cleaned)
            if trimmed_in is not None:
                backup_once(in_file)
                write_json_atomic(in_file, trimmed_in)

    verb = "" if apply else " (preview, use --apply to write)"
    console.print(f"\n[#48A630]Checked {checked} outputs in {os.path.basename(out_path)}{verb}[/#48A630]")
    console.print(f"[#48A630]  cleaned: {changed}  |  rejected: {rejected}"
                  f"{' (moved to ' + os.path.relpath(rejected_dir, ROOT) + ', they will be regenerated)' if apply and rejected else ''}[/#48A630]")
    if totals:
        console.print(f"[#48A630]  fixes: {dict((k, v) for k, v in totals.items() if k not in INFO_STATS)}[/#48A630]")
    if apply and changed:
        console.print(f"[#48A630]  originals backed up in {os.path.relpath(BACKUP_DIR, ROOT)}[/#48A630]")

# =========================
# Main
# =========================
def print_stats():
    """Per-model record from generation_report.jsonl: which workers are worth keeping."""
    if not os.path.exists(REPORT_FILE):
        console.print("No report yet: run the generator first.")
        return
    rows = {}
    with open(REPORT_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            r = rows.setdefault(e.get("model", "?"), {"done": 0, "rejected": 0, "missing": 0, "links": 0, "reasons": Counter()})
            if e.get("status") == "done":
                r["done"] += 1
                r["missing"] += e.get("stats", {}).get("missing", 0)
                r["links"] += e.get("stats", {}).get("input_total", 0)
            elif e.get("status") == "rejected":
                r["rejected"] += 1
                r["reasons"][re.sub(r"\d+", "N", e.get("reason", ""))[:40]] += 1

    table = Table(box=box.ROUNDED, border_style="#48A630", header_style="bold #63D746")
    for col in ("Model", "Done", "Rejected", "Accept %", "Links lost %", "Top rejection reasons"):
        table.add_column(col, style="#63D746")
    for model, r in sorted(rows.items(), key=lambda kv: -kv[1]["done"]):
        tried = r["done"] + r["rejected"]
        table.add_row(
            model + ("" if model in MODELS else " (removed)"),
            str(r["done"]), str(r["rejected"]),
            f"{r['done'] * 100 / tried:.0f}" if tried else "-",
            f"{r['missing'] * 100 / r['links']:.1f}" if r["links"] else "-",
            "; ".join(f"{k} ×{v}" for k, v in r["reasons"].most_common(3)),
        )
    console.print(table)
    print_daily_quota()


def print_daily_quota(days=7):
    """Files accepted per model per day, and what stopped it: the practical daily quota."""
    per_day = {}
    with open(REPORT_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            day = (e.get("ts") or "")[:10]
            if not day:
                continue
            cell = per_day.setdefault(day, {}).setdefault(e.get("model", "?"), {"done": 0, "stop": ""})
            if e.get("status") == "done":
                cell["done"] += 1
            elif e.get("status") == "retired":
                cell["stop"] = e.get("reason", "")
    if not per_day:
        return
    recent = sorted(per_day)[-days:]
    models = sorted({m for d in recent for m in per_day[d]})
    table = Table(title="Accepted files per day (the practical daily quota)", box=box.ROUNDED,
                  border_style="#48A630", header_style="bold #63D746")
    table.add_column("Model", style="#63D746")
    for d in recent:
        table.add_column(d[5:], justify="right", style="#63D746")
    for m in models:
        cells = []
        for d in recent:
            c = per_day[d].get(m)
            if not c:
                cells.append("-")
            else:
                mark = "*" if "quota" in c["stop"] or "rate limits" in c["stop"] else ""
                cells.append(f"{c['done']}{mark}")
        table.add_row(m, *cells)
    console.print(table)
    console.print("[#48A630]* = the model ran out of quota that day, so the number is its real daily capacity[/#48A630]")


def select_models(args):
    names = list(MODELS)
    if args.only:
        wanted = {p.strip().lower() for p in args.only.split(",")}
        unknown = wanted - set(PROVIDERS)
        if unknown:
            raise SystemExit(f"Unknown providers: {', '.join(unknown)} (available: {', '.join(PROVIDERS)})")
        names = [n for n in names if MODELS[n]["provider"] in wanted]
    if args.models:
        wanted = [m.strip() for m in args.models.split(",")]
        unknown = [m for m in wanted if m not in MODELS]
        if unknown:
            raise SystemExit(f"Unknown models: {', '.join(unknown)} (available: {', '.join(MODELS)})")
        names = [n for n in names if n in wanted]
    return names


def prepare_models(names):
    """Creates clients and drops models without keys or no longer served."""
    for provider, keys in api_keys.items():
        for label, key in keys:
            clients[(provider, label)] = OpenAI(api_key=key, base_url=PROVIDERS[provider]["base_url"],
                                                max_retries=0, timeout=PROVIDERS[provider].get("timeout", REQUEST_TIMEOUT))
    kept = []
    for provider in PROVIDERS:
        mine = [n for n in names if MODELS[n]["provider"] == provider]
        if not mine:
            continue
        if provider not in api_keys:
            console.print(f"[yellow]{provider}: no {provider.upper()}_API_KEY, skipped[/yellow]")
            continue
        kept += check_availability(provider, mine)
    return kept


def main():
    parser = argparse.ArgumentParser(description="Multi-Provider Batch Processor v6")
    parser.add_argument("--dir", type=str, help="Input directory name (e.g. working_split_IN--3) or 'all' for every directory in order")
    parser.add_argument("--headless", action="store_true", help="Plain progress lines instead of the live dashboard (servers, cron, add-on)")
    parser.add_argument("--max-hours", type=float, help="Stop cleanly after this many hours")
    parser.add_argument("--only", type=str, help=f"Comma-separated providers ({', '.join(PROVIDERS)})")
    parser.add_argument("--models", type=str, help=f"Comma-separated models ({', '.join(MODELS)})")
    parser.add_argument("--list-models", action="store_true", help="Show configured models and whether they are available")
    parser.add_argument("--clean-only", action="store_true", help="Only run the cleaners on existing outputs (preview)")
    parser.add_argument("--apply", action="store_true", help="With --clean-only: write the cleaned files")
    parser.add_argument("--stats", action="store_true", help="Show each model's accepted/rejected record and exit")
    parser.add_argument("--debug", action="store_true", help="Verbose log file")
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.stats:
        print_stats()
        return

    global vocab
    vocab = NameVocabulary.from_outputs()

    if args.clean_only:
        if args.dir == "all":
            for d in list_available_directories():
                console.print(f"\n[#63D746 bold]== {d}[/#63D746 bold]")
                clean_existing(d, args.apply, vocab)
            return
        selected_dir = select_directory(args.dir)
        if selected_dir:
            clean_existing(selected_dir, args.apply, vocab)
        return

    api_keys.update(discover_keys())
    if not api_keys:
        raise RuntimeError("No API keys found. Set GEMINI_API_KEY, GROQ_API_KEY, DEEPSEEK_API_KEY, OPENROUTER_API_KEY and/or NVIDIA_API_KEY in tools/.env")

    models = prepare_models(select_models(args))
    if args.list_models:
        for name in MODELS:
            cfg = MODELS[name]
            ok = "[#63D746]available[/#63D746]" if name in models else "[#FF5F1F]unavailable / no key / filtered[/#FF5F1F]"
            console.print(f"  {name:<20} {cfg['provider']:<11} {cfg['id']:<45} {ok}")
        return
    if not models:
        raise RuntimeError("No usable models (check keys and --only/--models)")

    def on_signal(signum, frame):
        if shutdown_event.is_set():
            raise KeyboardInterrupt
        shutdown_event.set()
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    if args.max_hours:
        timer = threading.Timer(args.max_hours * 3600, shutdown_event.set)
        timer.daemon = True
        timer.start()

    started = time.time()
    if args.dir == "all":
        dirs = list_available_directories()
    else:
        selected_dir = select_directory(args.dir)
        if not selected_dir:
            return
        dirs = [selected_dir]

    leftover_total, processed = 0, set()
    for d in dirs:
        if shutdown_event.is_set():
            break
        active = [m for m in models if not all(
            retired_reason(WorkerId(m, label)) for label, _ in api_keys[MODELS[m]["provider"]])]
        if not active:
            console.print("[#FFB000]Every model is out of quota: stopping until the next run.[/#FFB000]")
            break
        leftover_total += run_directory(d, active, args.headless)
        processed.add(d)

    # Directories never reached still count as left over
    for other in dirs:
        if other not in processed:
            leftover_total += len(get_remaining_files(*setup_io_paths(other)))
    write_last_run(started, leftover_total)


def run_directory(selected_dir, models, headless):
    """Processes one input directory; returns how many of its files are still to do."""
    in_path, out_path = setup_io_paths(selected_dir)
    remaining = get_remaining_files(in_path, out_path)
    if not remaining:
        console.print(f"[green]All files in '{selected_dir}' have been processed![/green]")
        return 0

    jobs = []
    for name in remaining:
        in_file = os.path.join(in_path, name)
        tokens = estimate_tokens(build_prompt(load_json(in_file)))
        jobs.append(Job(name, in_file, os.path.join(out_path, name.replace("in_", "out_", 1)), tokens))
    pool = JobPool(jobs)

    workers = [
        WorkerId(model, label, idx)
        for model in models
        for label, _ in api_keys[MODELS[model]["provider"]]
        for idx in range(MODELS[model]["workers"])
    ]
    workers = [w for w in workers if not retired_reason(w)]

    console.print(f"\n[#48A630]Processing directory: {selected_dir}[/#48A630]")
    console.print(f"[#48A630]Output path: {out_path}[/#48A630]")
    console.print(f"[#48A630]Files to process: {len(jobs)}  |  workers: {len(workers)}  |  log: {LOG_FILE}[/#48A630]\n")

    run_totals["done"] = 0
    run_totals["rejected"] = 0
    start = time.time()
    threads = []
    with state_lock:
        live_workers.update(workers)
    for w in workers:
        set_status(w, "idle")
        t = threading.Thread(target=worker_loop, args=(w, pool), daemon=True)
        t.start()
        threads.append(t)

    total = len(jobs)
    if headless:
        last_print = 0.0
        while any(t.is_alive() for t in threads) and not shutdown_event.is_set():
            if time.time() - last_print >= HEADLESS_PROGRESS_SECONDS:
                print_progress_line(selected_dir, total, pool, start)
                last_print = time.time()
            time.sleep(1)
    else:
        with Live(render_dashboard(workers, total, start, pool, selected_dir), console=console,
                  refresh_per_second=4, transient=False) as live:
            while any(t.is_alive() for t in threads) and not shutdown_event.is_set():
                live.update(render_dashboard(workers, total, start, pool, selected_dir))
                time.sleep(0.25)
            live.update(render_dashboard(workers, total, start, pool, selected_dir))

    for t in threads:
        t.join(timeout=1.0)

    console.print(f"[#48A630]Completed {run_totals['done']}/{total} files in '{selected_dir}'.[/#48A630]")
    if retired:
        console.print("[#FFB000]Retired:[/#FFB000]")
        for scope, reason in retired.items():
            console.print(f"  {' / '.join(scope[1:])}: {reason}")
    if pool.failed:
        console.print(f"[#FF5F1F]Gave up on {len(pool.failed)} files (raw outputs in {REJECTED_DIR}):[/#FF5F1F]")
        for job in pool.failed[:20]:
            console.print(f"  {job.filename}: {job.last_reason}")
    with pool.cond:
        leftover = list(pool.pending)
    if leftover and not shutdown_event.is_set():
        too_big = sum(1 for j in leftover if j.too_large or all(j.tokens > model_limit(m) for m in models))
        console.print(f"[#FFB000]{len(leftover)} files left for the next run"
                      f"{f' ({too_big} too large for the models that were still active)' if too_big else ''}.[/#FFB000]")
    return len(get_remaining_files(in_path, out_path))


HEADLESS_PROGRESS_SECONDS = 300


def print_progress_line(selected_dir, total, pool, start):
    with state_lock:
        states = Counter(st["state"] for st in worker_state.values())
    elapsed = (time.time() - start) / 60
    print(f"[{datetime.now():%H:%M}] {selected_dir}: {run_totals['done']}/{total} done, "
          f"{pool.remaining()} remaining, {run_totals['rejected']} rejected, {len(pool.failed)} given up | "
          f"workers {dict(states)} | {elapsed:.0f} min", flush=True)


def write_last_run(started, leftover_total):
    """Summary for the Home Assistant notification (and a quick look at how the day went)."""
    summary = {
        "finished": datetime.now().isoformat(timespec="seconds"),
        "minutes": round((time.time() - started) / 60),
        "done": run_totals["done_total"],
        "done_by_model": dict(run_done_by_model),
        "retired": {" / ".join(scope[1:]): reason for scope, reason in retired.items()},
        "files_left": leftover_total,
        "stopped_early": shutdown_event.is_set(),
    }
    with open(LAST_RUN_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    console.print(f"[#48A630]Run summary: {summary['done']} files done, {leftover_total} left "
                  f"(details in {LAST_RUN_FILE})[/#48A630]")

if __name__ == "__main__":
    main()
