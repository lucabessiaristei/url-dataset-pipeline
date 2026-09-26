#!/usr/bin/env python3
import os
import json

# === CONFIG ===
OUT_DIR = "./in_out-s/working_split_OUT--API-3"
DELETE_SINGLE_THRESHOLD = 2
DELETE_DOUBLE_THRESHOLD = 3

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
    count_single = sum(
        1 for f in folders
        if isinstance(f, dict) and len(f.get("bookmarks", [])) == 1
    )
    count_double = sum(
        1 for f in folders
        if isinstance(f, dict) and len(f.get("bookmarks", [])) == 2
    )

    if count_single > DELETE_SINGLE_THRESHOLD or count_double > DELETE_DOUBLE_THRESHOLD:
        deleted_files.append(filename)

# === REPORT (DRY RUN) ===
print("\n=== DELETION PREVIEW ===")
print(f"📂 Directory: {OUT_DIR}")
print(f"🧩 Files checked: {checked_files}")
print(f"🗑️  Files matching criteria: {len(deleted_files)}")

if deleted_files:
    print("\nMatching files:")
    for name in deleted_files:
        print(f"  • {name}")

# === CONFIRMATION ===
if not deleted_files:
    print("\n✅ No files to delete.\n")
    exit(0)

answer = input(
    f"\n⚠️  Do you want to DELETE these {len(deleted_files)} files? [y/N]: "
).strip().lower()

if answer not in ("y", "yes"):
    print("\n❌ Deletion aborted.\n")
    exit(0)

# === DELETE ===
errors = 0
for filename in deleted_files:
    file_path = os.path.join(OUT_DIR, filename)
    try:
        os.remove(file_path)
    except Exception as e:
        errors += 1
        print(f"⚠️  Error deleting {filename}: {e}")

# === FINAL REPORT ===
print("\n=== DELETION COMPLETED ===")
print(f"🗑️  Files deleted: {len(deleted_files) - errors}")
if errors:
    print(f"⚠️  Errors: {errors}")
print()