#!/usr/bin/env python3
import os
import json

# === CONFIG ===
OUT_DIR = "./in_out-s/working_split_OUT--API-1"
TARGET_KEYS = ("description", "preview")

fixed_files = []
checked_files = 0

for filename in sorted(os.listdir(OUT_DIR)):
    if not filename.startswith("out_") or not filename.endswith(".json"):
        continue

    file_path = os.path.join(OUT_DIR, filename)
    checked_files += 1
    changed = False

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"⚠️  Error reading {filename}: {e}")
        continue

    if isinstance(data, dict) and "folders" in data:
        for folder in data.get("folders", []):
            bookmarks = folder.get("bookmarks", [])
            for bm in bookmarks:
                if isinstance(bm, dict):
                    for key in TARGET_KEYS:
                        if key in bm:
                            del bm[key]
                            changed = True

        # Se sono state fatte modifiche, salva il file
        if changed:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            fixed_files.append(filename)

# === REPORT ===
print("\n=== CLEANUP REPORT ===")
print(f"📂 Directory: {OUT_DIR}")
print(f"🧩 Files checked: {checked_files}")
print(f"✅ Files fixed: {len(fixed_files)}")

if fixed_files:
    print("\nModified files:")
    for name in fixed_files:
        print(f"  • {name}")

print("\n✨ Done.\n")