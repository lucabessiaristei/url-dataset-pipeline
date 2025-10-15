#!/usr/bin/env python3
"""
Automated batch processing of split JSON files using Gemini 2.5 Pro.

Processes each file from INPUT_DIR (in_split_XXXX.json) according to ai_rules.txt,
and saves categorized JSON results to OUTPUT_DIR (out_split_XXXX.json).

✨ Features:
- Multiple API keys with automatic rotation on quota/rate limit errors
- Prints current key in use
- Stops gracefully if all keys are exhausted
- Respects ~5 requests/min and ~100 requests/day per key (free tier)
- Skips already processed files
- Cleans any ```json ... ``` wrapping from Gemini output
"""

import os
import time
import json
import google.generativeai as genai
from dotenv import load_dotenv

# ---------- CONFIG ----------
load_dotenv()

# Load all available API keys for rotation
API_KEYS = [
    os.getenv("GEMINI_API_KEY_1"),
    os.getenv("GEMINI_API_KEY_2"),
    os.getenv("GEMINI_API_KEY_3"),
    os.getenv("FEDE_API_KEY"),
    os.getenv("BOOK_API_KEY"),
    os.getenv("BOOK1_API_KEY")
]

API_KEYS = [k.strip() for k in API_KEYS if k and k.strip()]

if not API_KEYS:
    raise RuntimeError("❌ No Gemini API keys found in .env file")

# Model and runtime params
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.5-pro")
INPUT_DIR = os.getenv("INPUT_DIR", "working_split_IN--1")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "working_split_OUT--API-1")
RULES_FILE = os.getenv("RULES_FILE", "ai_rules.txt")
REQUESTS_PER_MIN = float(os.getenv("REQUESTS_PER_MIN", "5"))
SLEEP_BETWEEN = 60 / REQUESTS_PER_MIN

# Initialize model with first key
current_key_index = 0
exhausted_keys = set()

def configure_model():
    """Configures Gemini with the current API key."""
    key = API_KEYS[current_key_index]
    genai.configure(api_key=key)
    print(f"🔑 Using Gemini API key #{current_key_index + 1}: {key[:8]}... (active)")
    return genai.GenerativeModel(MODEL_NAME)

model = configure_model()

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load AI rules
with open(RULES_FILE, "r", encoding="utf-8") as f:
    AI_RULES = f.read().strip()


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
    """Cleans Gemini output from markdown and code fences."""
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        parts = text.split("\n", 1)
        if len(parts) > 1:
            if parts[0].startswith("json"):
                text = parts[1]
            else:
                text = parts[1]
    if text.endswith("```"):
        text = text[: text.rfind("```")].strip()
    return text.strip()


def process_file(filename):
    """Send a single file to Gemini and save the categorized output."""
    in_path = os.path.join(INPUT_DIR, filename)
    out_filename = filename.replace("in_", "out_", 1)
    out_path = os.path.join(OUTPUT_DIR, out_filename)

    with open(in_path, "r", encoding="utf-8") as f:
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
        if "429" in err_msg or "quota" in err_msg or "exceeded" in err_msg:
            print(f"⚠️  Quota exceeded on key #{current_key_index + 1}.")
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
        out_path_raw = out_path.replace(".json", "_RAW.txt")
        with open(out_path_raw, "w", encoding="utf-8") as f:
            f.write(result_text.replace("\r", ""))
        print(f"   ⛔ Saved raw output for manual fix: {out_path_raw}")
        return

    # Save cleaned JSON
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(parsed_json, f, ensure_ascii=False, indent=2)

    print(f"   ✅ Saved clean JSON to {out_path}")


def main():
    files = sorted(f for f in os.listdir(INPUT_DIR) if f.endswith(".json") and f.startswith("in_"))
    total = len(files)
    done_files = {f for f in os.listdir(OUTPUT_DIR) if f.endswith(".json") and f.startswith("out_")}
    remaining = [f for f in files if f.replace("in_", "out_", 1) not in done_files]

    if not remaining:
        print("✅ All files already processed.")
        return

    print(f"📂 Found {total} input files in {INPUT_DIR}")
    print(f"📁 Already processed: {len(done_files)} → Remaining: {len(remaining)}")
    print(f"🔐 Loaded {len(API_KEYS)} API keys (starting from key #{current_key_index + 1})\n")

    for i, filename in enumerate(remaining, 1):
        process_file(filename)
        if i < len(remaining):
            print(f"   ⏱ Waiting {SLEEP_BETWEEN:.1f}s before next...")
            time.sleep(SLEEP_BETWEEN)

    print("\n✅ Batch processing complete.")


if __name__ == "__main__":
    main()