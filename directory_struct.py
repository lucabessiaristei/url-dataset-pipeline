#!/usr/bin/env python3
import os

# --- CONFIG ---
ROOT_DIR = "."             # change this to the folder you want to scan
OUTPUT_FILE = "directory_structure.txt"
# ---------------

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for root, dirs, files in os.walk(ROOT_DIR):
        level = root.replace(ROOT_DIR, "").count(os.sep)
        indent = "    " * level
        f.write(f"{indent}{os.path.basename(root)}/\n")
        sub_indent = "    " * (level + 1)
        for filename in files:
            f.write(f"{sub_indent}{filename}\n")

print(f"Directory structure saved to {OUTPUT_FILE}")