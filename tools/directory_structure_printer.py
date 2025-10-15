#!/usr/bin/env python3
import os
import sys

# --- CONFIG ---
ROOT_DIR = "."
OUTPUT_FILE = "directory_structure.txt"
COMPACT_MODE = "--compact" in sys.argv
COMPACT_THRESHOLD = 20
HEAD_TAIL_COUNT = 9
# ---------------


def list_dir(path, prefix=""):
    entries = []
    with os.scandir(path) as it:
        for entry in sorted(it, key=lambda e: e.name.lower()):
            # skip hidden except .env
            if entry.name.startswith(".") and entry.name != ".env":
                continue
            entries.append(entry)

    last_index = len(entries) - 1
    for i, entry in enumerate(entries):
        connector = "└── " if i == last_index else "├── "
        line_prefix = prefix + connector

        if entry.is_dir():
            f.write(f"{line_prefix}{entry.name}/\n")
            new_prefix = prefix + ("    " if i == last_index else "│   ")
            list_dir(entry.path, new_prefix)
        else:
            f.write(f"{line_prefix}{entry.name}\n")


def list_dir_compact(path, prefix=""):
    entries = []
    with os.scandir(path) as it:
        for entry in sorted(it, key=lambda e: e.name.lower()):
            if entry.name.startswith(".") and entry.name != ".env":
                continue
            entries.append(entry)

    last_index = len(entries) - 1
    files = [e for e in entries if e.is_file()]
    dirs = [e for e in entries if e.is_dir()]

    # First list directories
    for i, d in enumerate(dirs):
        is_last = (i == len(dirs) - 1) and not files
        connector = "└── " if is_last else "├── "
        f.write(f"{prefix}{connector}{d.name}/\n")
        new_prefix = prefix + ("    " if is_last else "│   ")
        list_dir_compact(d.path, new_prefix)

    # Then handle files
    total_files = len(files)
    if total_files == 0:
        return

    if total_files > COMPACT_THRESHOLD:
        display = files[:HEAD_TAIL_COUNT] + files[-HEAD_TAIL_COUNT:]
        hidden_count = total_files - (HEAD_TAIL_COUNT * 2)
        for i, file_entry in enumerate(display):
            connector = "└── " if i == len(display) - 1 and hidden_count <= 0 else "├── "
            f.write(f"{prefix}{connector}{file_entry.name}\n")
            if i == HEAD_TAIL_COUNT - 1:
                f.write(f"{prefix}│   ... ({hidden_count} files hidden) ...\n")
    else:
        for i, file_entry in enumerate(files):
            connector = "└── " if i == len(files) - 1 else "├── "
            f.write(f"{prefix}{connector}{file_entry.name}\n")


with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(f"{os.path.basename(os.path.abspath(ROOT_DIR))}/\n")
    if COMPACT_MODE:
        list_dir_compact(ROOT_DIR)
    else:
        list_dir(ROOT_DIR)

print(f"Directory structure saved to {OUTPUT_FILE} (compact mode: {COMPACT_MODE})")