#!/usr/bin/env python3
import os
import sys

# --- CONFIG ---
ROOT_DIR = "."
OUTPUT_FILE = "directory_structure.txt"
README_FILE = "README.md"
COMPACT_MODE = "--compact" in sys.argv  # only affects .txt output
COMPACT_THRESHOLD = 20
HEAD_TAIL_COUNT = 9
# ---------------


def list_dir(path, prefix="", f=None):
    """Full non-compact listing"""
    entries = []
    with os.scandir(path) as it:
        for entry in sorted(it, key=lambda e: e.name.lower()):
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
            list_dir(entry.path, new_prefix, f)
        else:
            f.write(f"{line_prefix}{entry.name}\n")


def list_dir_compact(path, prefix="", f=None):
    """Compact listing mode for long directories"""
    entries = []
    with os.scandir(path) as it:
        for entry in sorted(it, key=lambda e: e.name.lower()):
            if entry.name.startswith(".") and entry.name != ".env":
                continue
            entries.append(entry)

    files = [e for e in entries if e.is_file()]
    dirs = [e for e in entries if e.is_dir()]

    # Directories first
    for i, d in enumerate(dirs):
        is_last = (i == len(dirs) - 1) and not files
        connector = "└── " if is_last else "├── "
        f.write(f"{prefix}{connector}{d.name}/\n")
        new_prefix = prefix + ("    " if is_last else "│   ")
        list_dir_compact(d.path, new_prefix, f)

    # Then files
    total_files = len(files)
    if total_files == 0:
        return

    if total_files > COMPACT_THRESHOLD:
        display = files[:HEAD_TAIL_COUNT] + files[-HEAD_TAIL_COUNT:]
        hidden_count = total_files - (HEAD_TAIL_COUNT * 2)
        for i, fe in enumerate(display):
            connector = "└── " if i == len(display) - 1 and hidden_count <= 0 else "├── "
            f.write(f"{prefix}{connector}{fe.name}\n")
            if i == HEAD_TAIL_COUNT - 1:
                f.write(f"{prefix}│   ... ({hidden_count} files hidden) ...\n")
    else:
        for i, fe in enumerate(files):
            connector = "└── " if i == len(files) - 1 else "├── "
            f.write(f"{prefix}{connector}{fe.name}\n")


def get_directory_tree_string():
    """Always return compact version as string (for README update)"""
    from io import StringIO
    buffer = StringIO()

    def write_tree(path, prefix=""):
        entries = []
        with os.scandir(path) as it:
            for entry in sorted(it, key=lambda e: e.name.lower()):
                if entry.name.startswith(".") and entry.name != ".env":
                    continue
                entries.append(entry)

        files = [e for e in entries if e.is_file()]
        dirs = [e for e in entries if e.is_dir()]

        for i, d in enumerate(dirs):
            is_last = (i == len(dirs) - 1) and not files
            connector = "└── " if is_last else "├── "
            buffer.write(f"{prefix}{connector}{d.name}/\n")
            new_prefix = prefix + ("    " if is_last else "│   ")
            write_tree(d.path, new_prefix)

        total_files = len(files)
        if total_files == 0:
            return

        if total_files > COMPACT_THRESHOLD:
            display = files[:HEAD_TAIL_COUNT] + files[-HEAD_TAIL_COUNT:]
            hidden_count = total_files - (HEAD_TAIL_COUNT * 2)
            for i, fe in enumerate(display):
                connector = "└── " if i == len(display) - 1 and hidden_count <= 0 else "├── "
                buffer.write(f"{prefix}{connector}{fe.name}\n")
                if i == HEAD_TAIL_COUNT - 1:
                    buffer.write(f"{prefix}│   ... ({hidden_count} files hidden) ...\n")
        else:
            for i, fe in enumerate(files):
                connector = "└── " if i == len(files) - 1 else "├── "
                buffer.write(f"{prefix}{connector}{fe.name}\n")

    buffer.write(f"{os.path.basename(os.path.abspath(ROOT_DIR))}/\n")
    write_tree(ROOT_DIR)
    return buffer.getvalue()


def update_readme_with_tree(tree_string):
    """Replace directory structure section inside README.md"""
    if not os.path.exists(README_FILE):
        print(f"⚠️ README file not found: {README_FILE}")
        return

    with open(README_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    start_marker = "## Complete Directory Structure"
    end_marker = "*Directory structure generated automatically"

    start_idx = content.find(start_marker)
    if start_idx == -1:
        print("⚠️ Section not found in README.md")
        return

    end_idx = content.find(end_marker, start_idx)
    if end_idx == -1:
        print("⚠️ End marker not found")
        return

    end_line_idx = content.find("```", end_idx)
    if end_line_idx == -1:
        end_line_idx = len(content)
    else:
        end_line_idx = content.find("\n", end_line_idx) + 1

    new_section = (
        f"{start_marker}\n\n```\n{tree_string}```\n\n"
        "*Directory structure generated automatically by `directory_structure_printer.py`*\n```"
    )

    new_content = content[:start_idx] + new_section + content[end_line_idx:]

    with open(README_FILE, "w", encoding="utf-8") as f:
        f.write(new_content)

    print("✅ README.md updated with compact directory structure.")


# --- MAIN EXECUTION ---
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(f"{os.path.basename(os.path.abspath(ROOT_DIR))}/\n")
    if COMPACT_MODE:
        list_dir_compact(ROOT_DIR, f=f)
    else:
        list_dir(ROOT_DIR, f=f)

print(f"✅ Directory structure saved to {OUTPUT_FILE} (compact mode: {COMPACT_MODE})")

# Always update README with compact tree
tree_string = get_directory_tree_string()
update_readme_with_tree(tree_string)