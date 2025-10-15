#!/usr/bin/env python3
"""
Automated batch processing of split JSON files using Gemini 2.5 Pro.

Processes each file from working_split_IN--{n} inside ./in_out-s/,
and saves categorized JSON results to the corresponding working_split_OUT--API-{n}.

✨ Features:
- Dynamically loads all .env variables containing "API_KEY"
- Auto-detects the first incomplete IN/OUT pair (IN--{n}, OUT--API-{n})
- Automatic key rotation on quota/rate limit errors
- Prints current key in use
- Stops gracefully if all keys are exhausted
- Respects ~5 requests/min per key (free tier)
- Skips already processed files
- Cleans any ```json ... ``` wrapping from Gemini output
"""

import os
import time
import json
import re
import google.generativeai as genai
from dotenv import load_dotenv

# ---------- CONFIG ----------
load_dotenv()

# Base directory containing all IN/OUT folders
BASE_DIR = "./in_out-s"

# --- Dynamic API key discovery ---
API_KEYS = [
    v.strip()
    for k, v in os.environ.items()
    if "API_KEY" in k.upper() and v and v.strip()
]

if not API_KEYS:
    raise RuntimeError("❌ No Gemini API keys found in .env file")

# --- Static parameters ---
MODEL_NAME = "gemini-2.5-pro"
RULES_FILE = "./ai_rules.txt"
REQUESTS_PER_MIN = 5
SLEEP_BETWEEN = 60 / REQUESTS_PER_MIN

# ---------- INITIALIZATION ----------
current_key_index = 0
exhausted_keys = set()


def configure_model():
    """Configures Gemini with the current API key."""
    key = API_KEYS[current_key_index]
    genai.configure(api_key=key)
    print(f"🔑 Using Gemini API key #{current_key_index + 1}/{len(API_KEYS)}: {key[:8]}... (active)")
    return genai.GenerativeModel(MODEL_NAME)


model = configure_model()

# Load AI rules
with open(RULES_FILE, "r", encoding="utf-8") as f:
    AI_RULES = f.read().strip()


# ============================================================
# DETECT FIRST UNFINISHED IN/OUT PAIR
# ============================================================
def detect_io_pair(base_dir=BASE_DIR):
    """Finds the first IN/OUT pair (IN--{n}, OUT--API-{n}) that isn't fully processed."""
    in_dirs = sorted([d for d in os.listdir(base_dir) if re.match(r"^working_split_IN--\d+$", d)])
    if not in_dirs:
        raise RuntimeError("❌ No input directories found (working_split_IN--{n}).")

    for in_dir in in_dirs:
        suffix = in_dir.split("--")[-1]
        out_dir = f"working_split_OUT--API-{suffix}"

        in_path = os.path.join(base_dir, in_dir)
        out_path = os.path.join(base_dir, out_dir)
        os.makedirs(out_path, exist_ok=True)

        in_files = [f for f in os.listdir(in_path) if f.startswith("in_") and f.endswith(".json")]
        out_files = [f for f in os.listdir(out_path) if f.startswith("out_") and f.endswith(".json")]

        if len(out_files) < len(in_files):
            print(f"📁 Selected pair:")
            print(f"   IN:  {in_dir} ({len(in_files)} files)")
            print(f"   OUT: {out_dir} ({len(out_files)} files)")
            return in_path, out_path

    raise RuntimeError("✅ All IN/OUT directories are fully processed.")


# ============================================================
# GEMINI HANDLERS
# ============================================================
def switch_api_key():
    """Rotate to the next available API key; return False if all exhausted."""
    global current_key_index, model
    exhausted_keys.add(current_key_index)

    if len(exhausted_keys) >= len(API_KEYS):
        print("❌ All API keys exhausted. Exiting batch process.")
        exit(1)

    available = [i for i in range(len(API_KEYS)) if i not in exhausted_keys]
    current_key_index = available[0]
    model = configure_model()
    print("⏳ Cooldown 10s before retry...")
    time.sleep(10)
    return True


def clean_response_text(text: str) -> str:
    """Cleans Gemini output from markdown/code fences."""
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        parts = text.split("\n", 1)
        if len(parts) > 1:
            text = parts[1]
    if text.endswith("```"):
        text = text[: text.rfind("```")].strip()
    return text.strip()


def process_file(in_path, out_path, filename):
    """Send a single file to Gemini and save the categorized output."""
    in_file = os.path.join(in_path, filename)
    out_filename = filename.replace("in_", "out_", 1)
    out_file = os.path.join(out_path, out_filename)

    with open(in_file, "r", encoding="utf-8") as f:
        content = json.load(f)

    prompt = f"""{AI_RULES}

Now categorize the following data according to the above rules.

Additional constraints for grouping:
- Avoid creating categories (folders) with only one link, unless it is truly unique or cannot logically belong elsewhere.
- Avoid creating categories with more than 20 links. If a category exceeds this, split it into smaller, coherent subgroups.
- Prefer balanced, meaningful grouping across categories.

Return the result as valid JSON.

{json.dumps(content, ensure_ascii=False, indent=2)}"""

    print(f"\n→ Processing {filename} ({len(content.get('data', []))} links)...")

    global model
    try:
        response = model.generate_content(prompt)
        result_text = clean_response_text(response.text)

    except Exception as e:
        err_msg = str(e).lower()
        if any(x in err_msg for x in ["429", "quota", "exceeded"]):
            print(f"⚠️  Quota exceeded on key #{current_key_index + 1}. Rotating...")
            if not switch_api_key():
                return
            try:
                response = model.generate_content(prompt)
                result_text = clean_response_text(response.text)
            except Exception as e2:
                print(f"   ❌ Retry failed after rotation: {e2}")
                return
        else:
            print(f"   ❌ Error processing {filename}: {e}")
            return

    # Validate JSON output
    try:
        parsed_json = json.loads(result_text)
    except Exception as e:
        print(f"   ⚠️ Invalid JSON in {filename}: {e}")
        raw_path = out_file.replace(".json", "_RAW.txt")
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(result_text.replace("\r", ""))
        print(f"   ⛔ Saved raw output for manual fix: {raw_path}")
        return

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(parsed_json, f, ensure_ascii=False, indent=2)

    print(f"   ✅ Saved clean JSON to {out_file}")


# ============================================================
# MAIN LOOP
# ============================================================
def main():
    while True:
        try:
            INPUT_DIR, OUTPUT_DIR = detect_io_pair(BASE_DIR)
        except RuntimeError as e:
            print(str(e))
            break

        in_files = sorted(f for f in os.listdir(INPUT_DIR) if f.endswith(".json") and f.startswith("in_"))
        out_files = {f for f in os.listdir(OUTPUT_DIR) if f.endswith(".json") and f.startswith("out_")}
        remaining = [f for f in in_files if f.replace("in_", "out_", 1) not in out_files]

        print(f"📂 Found {len(in_files)} total files in {INPUT_DIR}")
        print(f"📁 Already processed: {len(out_files)} → Remaining: {len(remaining)}")
        print(f"🔐 Loaded {len(API_KEYS)} API keys (starting from key #{current_key_index + 1})\n")

        for i, filename in enumerate(remaining, 1):
            process_file(INPUT_DIR, OUTPUT_DIR, filename)
            if i < len(remaining):
                print(f"   ⏱ Waiting {SLEEP_BETWEEN:.1f}s before next...")
                time.sleep(SLEEP_BETWEEN)

        print(f"\n✅ Finished pair {os.path.basename(INPUT_DIR)} → {os.path.basename(OUTPUT_DIR)}\n")


if __name__ == "__main__":
    main()