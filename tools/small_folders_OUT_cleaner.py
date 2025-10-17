#!/usr/bin/env python3
import os
import json
import argparse

# === CONFIG ===
OUT_DIR = "./in_out-s/working_split_OUT--API-1"
DELETE_SINGLE_THRESHOLD = 3  # più di 3 cartelle con 1 bookmark
DELETE_DOUBLE_THRESHOLD = 6  # più di 5 cartelle con 2 bookmark

# === ARGPARSE ===
parser = argparse.ArgumentParser(description="Delete output JSONs with too many small folders")
parser.add_argument("--apply", action="store_true", help="Actually delete the files (otherwise dry run)")
args = parser.parse_args()

# === MAIN ===
deleted_files = []
checked_files = 0

for filename in sorted(os.listdir(OUT_DIR)):
    if not filename.startswith("out_") or not filename.endswith(".json"):
        continue

    file_path = os.path.join(OUT_DIR, filename)
    checked_files += 1

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"⚠️  Error reading {filename}: {e}")
        continue

    if not isinstance(data, dict) or "folders" not in data:
        continue

    folders = data.get("folders", [])
    count_single = sum(1 for f in folders if isinstance(f, dict) and len(f.get("bookmarks", [])) == 1)
    count_double = sum(1 for f in folders if isinstance(f, dict) and len(f.get("bookmarks", [])) == 2)

    if count_single > DELETE_SINGLE_THRESHOLD or count_double > DELETE_DOUBLE_THRESHOLD:
        deleted_files.append(filename)
        if args.apply:
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"⚠️  Error deleting {filename}: {e}")

# === REPORT ===
mode = "APPLY (deletion performed)" if args.apply else "DRY RUN (no files deleted)"
print("\n=== DELETION REPORT ===")
print(f"📂 Directory: {OUT_DIR}")
print(f"🧩 Files checked: {checked_files}")
print(f"⚙️  Mode: {mode}")
print(f"🗑️  Files matching criteria: {len(deleted_files)}")

if deleted_files:
    print("\nMatching files:")
    for name in deleted_files:
        print(f"  • {name}")

if not args.apply:
    print("\n💡 Run again with '--apply' to actually delete these files.\n")
else:
    print("\n✅ Deletion completed.\n")