#!/usr/bin/env python3
"""
Adds user-style bookmark names to the URL pool, for the Chrome-extension training data.

People often rename bookmarks ("nonna lasagna", "tax forms", "bank"), and that name is often the best hint
of why they saved the link. The pool only has page titles, so this asks DeepSeek (free on NVIDIA) to write
the kind of name a person would type, for a fixed ~60% of the links. The input-folder creator in the generator
then uses the name for part of each link's appearances.

Resumable: names already in the output file are kept. Usage:
    python tools/pool_rename_bookmarks.py [--pool json_lists/working_expanded.json] [--out json_lists/pool_renames.json]
"""

import argparse
import hashlib
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from openai import OpenAI

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TOOLS_DIR)
load_dotenv(os.path.join(TOOLS_DIR, ".env"))

MODEL = "deepseek-ai/deepseek-v4.1-flash"
BASE_URL = "https://integrate.api.nvidia.com/v1"
RENAME_SHARE = 60          # % of links that get a user-style name (chosen by URL hash, so it is stable)
BATCH = 60                 # links per request
WORKERS = 4                # parallel requests; the generator shares the 40 requests/min key

PROMPT = """These are web pages someone saved as browser bookmarks. For each one, write the name a real person
might have given the bookmark when they renamed it. Vary the style across the list the way real people do:
- the brand or site only ("netflix", "Diffchecker", "BBC")
- a few key words, often lowercase ("lasagna recipe", "python diff tool", "ptsd veterans stats")
- personal or purpose-driven ("nonna lasagna", "gift idea mom", "read later - ai safety", "my bank")
- abbreviations or shorthand ("ASA", "NYT tech", "tax fr 2025")
Mix these styles evenly across the list: roughly half of the names all lowercase, and at least 1 in 4 personal
or purpose-driven. Rules: 1 to 5 words, never the full page title, no quotes or emojis. Usually keep the
page's language; sometimes (about 1 in 5) use English for a non-English page, as a bilingual user would.

Reply with one JSON object mapping each number to its name, nothing else: {{"1": "...", "2": "..."}}

{items}"""


def chosen(url):
    return int(hashlib.sha1(url.encode()).hexdigest(), 16) % 100 < RENAME_SHARE


def describe(i, item):
    desc = item.get("description") or ""
    desc = "" if desc == "void" else desc[:120]
    return f"{i}. {item['url']} | {item.get('title', '')}" + (f" | {desc}" if desc else "")


def ask(client, batch):
    prompt = PROMPT.format(items="\n".join(describe(i, it) for i, it in enumerate(batch, 1)))
    parts = []
    stream = client.chat.completions.create(
        model=MODEL, messages=[{"role": "user", "content": prompt}], temperature=0.8, max_tokens=4000,
        stream=True, extra_body={"chat_template_kwargs": {"thinking": False}})
    for chunk in stream:
        if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
            parts.append(chunk.choices[0].delta.content)
    text = "".join(parts)
    m = re.search(r"\{.*\}", text, re.S)
    names = json.loads(m.group(0)) if m else {}
    out = {}
    for i, item in enumerate(batch, 1):
        name = str(names.get(str(i)) or "").strip().strip('"')
        if 0 < len(name) <= 60 and len(name.split()) <= 6 and name.lower() != (item.get("title") or "").lower():
            out[item["url"]] = name
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--pool", default=os.path.join(ROOT, "json_lists", "working_expanded.json"))
    parser.add_argument("--out", default=os.path.join(ROOT, "json_lists", "pool_renames.json"))
    args = parser.parse_args()

    key = os.environ.get("NVIDIA_API_KEY")
    if not key:
        sys.exit("NVIDIA_API_KEY missing (tools/.env)")
    client = OpenAI(api_key=key, base_url=BASE_URL, timeout=900, max_retries=0)

    pool = json.load(open(args.pool, encoding="utf-8"))
    done = json.load(open(args.out, encoding="utf-8")) if os.path.exists(args.out) else {}
    todo = [it for it in pool if chosen(it["url"]) and it["url"] not in done]
    print(f"{len(pool)} links, {sum(chosen(it['url']) for it in pool)} chosen, {len(done)} named already, {len(todo)} to do")
    batches = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]
    lock = threading.Lock()

    def run(batch):
        for attempt in range(3):
            try:
                return ask(client, batch)
            except Exception as e:  # 504s and queue timeouts are common on the free endpoint
                print(f"  batch retry {attempt + 1}: {type(e).__name__}: {str(e)[:100]}", flush=True)
                time.sleep(20)
        return {}

    with ThreadPoolExecutor(WORKERS) as pool_ex:
        futures = [pool_ex.submit(run, b) for b in batches]
        for n, fut in enumerate(as_completed(futures), 1):
            names = fut.result()
            with lock:
                done.update(names)
                tmp = args.out + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(done, f, ensure_ascii=False, indent=0)
                os.replace(tmp, args.out)
            print(f"[{time.strftime('%H:%M')}] {n}/{len(batches)} batches, {len(done)} names", flush=True)


if __name__ == "__main__":
    main()
