#!/usr/bin/env python3
"""
Gemini Outputs Cleaner v11 — Full Sync + URL/Title Alignment + Auto-Fix

✔ Mostra solo file con differenze reali
✔ Corregge bookmark non validi (string → oggetto JSON)
✔ Evita duplicazione prefissi nei backup
✔ Conteggio accurato file modificati su totale
✔ Normalizza URL (schema, www, slash, query/hash)
✔ Sincronizza URL e titoli dall’input all’output
✔ Rimuove link extra e cartelle vuote
✔ Aggiorna tutti i conteggi
✔ Backup automatico e report leggibile
"""

import os
import re
import json
import shutil
import argparse
from typing import Dict, Tuple

# -----------------------------
# Config
# -----------------------------
INPUT_DIR = "./in_out-s/working_split_IN--1"
OUTPUT_DIR = "./in_out-s/working_split_OUT--API-1"
BACKUP_DIR = "./in_out-s/split_backup_cleaner"
REPORT_PATH = "./in_out-s/gemini_clean_report.json"

os.makedirs(BACKUP_DIR, exist_ok=True)
ID_RE = re.compile(r"(?:^|_)split_(\d{4})\.json$", re.IGNORECASE)


# -----------------------------
# URL normalization
# -----------------------------
def normalize_url(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        return ""
    u = url.strip()
    u = re.sub(r"^https?://", "", u, flags=re.IGNORECASE)
    u = re.sub(r"^www\.", "", u, flags=re.IGNORECASE)
    u = u.split("?", 1)[0].split("#", 1)[0]
    if u.endswith("/"):
        u = u[:-1]
    return u.lower()


# -----------------------------
# Extract data maps
# -----------------------------
def extract_input_map(data: dict) -> Dict[str, Tuple[str, str]]:
    """Crea mappa URL normalizzato → (url originale, titolo) dall’input."""
    if not isinstance(data, dict):
        return {}
    m = {}
    for item in data.get("data", []):
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if not url:
            continue
        n = normalize_url(url)
        if n:
            m[n] = (url.strip(), (item.get("title") or "").strip())
    return m


def extract_output_index(data: dict) -> Dict[str, Tuple[dict, dict]]:
    """
    Crea indice URL normalizzato → (folder, bookmark) dall’output.
    Corregge bookmark non validi (string → {"url": ..., "title": ""}).
    """
    if not isinstance(data, dict):
        return {}

    folders = data.get("folders")
    if not isinstance(folders, list):
        return {}

    idx = {}
    fix_count = 0

    for folder in folders:
        if not isinstance(folder, dict):
            continue

        bms = folder.get("bookmarks", [])
        if not isinstance(bms, list):
            continue

        new_bms = []
        for bm in bms:
            # --- correzione automatica ---
            if isinstance(bm, str):
                bm = {"url": bm.strip(), "title": ""}
                fix_count += 1
            elif not isinstance(bm, dict):
                continue

            url = bm.get("url")
            if not isinstance(url, str) or not url.strip():
                continue

            n = normalize_url(url)
            if n:
                idx[n] = (folder, bm)
            new_bms.append(bm)

        folder["bookmarks"] = new_bms
        folder["count"] = len(new_bms)

    data["_auto_fix_count"] = fix_count
    return idx


# -----------------------------
# File pairing (in/out)
# -----------------------------
def pair_files() -> Dict[str, Tuple[str, str]]:
    pairs = {}
    for name in os.listdir(INPUT_DIR):
        if not name.endswith(".json"):
            continue
        m = ID_RE.search(name.replace("in_", ""))
        if not m:
            continue
        sid = m.group(1)
        in_path = os.path.join(INPUT_DIR, name)
        for cand in [f"out_split_{sid}.json", f"split_{sid}.json"]:
            out_path = os.path.join(OUTPUT_DIR, cand)
            if os.path.exists(out_path):
                pairs[sid] = (in_path, out_path)
                break
    return pairs


# -----------------------------
# Backup helper
# -----------------------------
def ensure_backup(src_path: str, prefix: str):
    base = os.path.basename(src_path)
    if not base.startswith(f"{prefix}_"):
        base = f"{prefix}_{base}"
    dst = os.path.join(BACKUP_DIR, base)
    if not os.path.exists(dst):
        shutil.copy2(src_path, dst)


# -----------------------------
# Core processing
# -----------------------------
def process_pair(sid: str, in_path: str, out_path: str, apply: bool, verbose: bool, report: dict):
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            in_json = json.load(f)
        with open(out_path, "r", encoding="utf-8") as f:
            out_json = json.load(f)
    except Exception as e:
        print(f"⚠️  split_{sid}: errore lettura JSON ({e})")
        return False

    input_map = extract_input_map(in_json)
    out_index = extract_output_index(out_json)
    auto_fixes = out_json.pop("_auto_fix_count", 0)

    in_norms = set(input_map.keys())
    out_norms = set(out_index.keys())

    missing = sorted(in_norms - out_norms)
    extra = sorted(out_norms - in_norms)

    url_changes, title_changes = [], []

    # --------------------------
    # Backup
    # --------------------------
    if apply:
        ensure_backup(in_path, "in")
        ensure_backup(out_path, "out")

    # --------------------------
    # Clean input
    # --------------------------
    if apply and missing:
        in_json["data"] = [
            it for it in in_json.get("data", [])
            if normalize_url(it.get("url")) not in set(missing)
        ]
    if "_meta" not in in_json:
        in_json["_meta"] = {}
    in_json["_meta"]["total_bookmarks"] = len(in_json.get("data", []))

    # --------------------------
    # Clean + Sync output
    # --------------------------
    new_folders = []
    for folder in out_json.get("folders", []):
        new_bms = []
        for bm in folder.get("bookmarks", []):
            if not isinstance(bm, dict):
                continue

            cur_url = bm.get("url", "")
            n = normalize_url(cur_url)
            if not n or n in extra:
                continue

            if n in input_map:
                correct_url, correct_title = input_map[n]

                # --- URL sync ---
                if bm.get("url") != correct_url:
                    url_changes.append({"from": bm["url"], "to": correct_url})
                    if apply:
                        bm["url"] = correct_url
                    if verbose:
                        print(f"🔗 [split_{sid}] URL: {bm['url']} → {correct_url}")

                # --- Title sync ---
                cur_title = (bm.get("title") or "").strip()
                if correct_title and cur_title != correct_title:
                    title_changes.append({"url": correct_url, "from": cur_title, "to": correct_title})
                    if apply:
                        bm["title"] = correct_title
                    if verbose:
                        print(f"📝 [split_{sid}] TITLE: {cur_title} → {correct_title}")

            new_bms.append(bm)

        if new_bms:
            folder["bookmarks"] = new_bms
            folder["count"] = len(new_bms)
            new_folders.append(folder)

    # --- Aggiornamento conteggi ---
    out_json["folders"] = new_folders
    out_json["num_folders"] = len(new_folders)
    out_json["total_bookmarks"] = sum(f.get("count", 0) for f in new_folders)

    # --------------------------
    # Skip if no diffs
    # --------------------------
    has_diffs = any([missing, extra, url_changes, title_changes, auto_fixes])
    if not has_diffs:
        return False

    # --------------------------
    # Save if applied
    # --------------------------
    if apply:
        with open(in_path, "w", encoding="utf-8") as f:
            json.dump(in_json, f, ensure_ascii=False, indent=2)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out_json, f, ensure_ascii=False, indent=2)

    # --------------------------
    # Report entry
    # --------------------------
    report[sid] = {
        "file_input": os.path.basename(in_path),
        "file_output": os.path.basename(out_path),
        "missing_count": len(missing),
        "extra_count": len(extra),
        "url_changes_total": len(url_changes),
        "title_changes_total": len(title_changes),
        "auto_fixed_bookmarks": auto_fixes,
        "url_changes": url_changes[:5],
        "title_changes": title_changes[:5],
        "final_num_folders": out_json.get("num_folders", 0),
        "final_total_bookmarks": out_json.get("total_bookmarks", 0),
        "applied": apply,
    }

    # --- Log ---
    if apply:
        print(f"🧹 split_{sid}: {len(url_changes)} URL sync, {len(title_changes)} title sync, "
              f"{len(missing)} missing, {len(extra)} extra, {auto_fixes} auto-fixed → "
              f"folders={out_json['num_folders']}, bookmarks={out_json['total_bookmarks']}")
    else:
        example = (url_changes[:1] or title_changes[:1])
        print(f"👀 split_{sid}: url_fix={len(url_changes)}, title_fix={len(title_changes)}, "
              f"missing={len(missing)}, extra={len(extra)}, auto_fixed={auto_fixes} "
              f"(es: {example})")

    return True


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Gemini input/output sync with visible URL/title diffs.")
    parser.add_argument("--apply", action="store_true", help="Applica le modifiche (default = anteprima).")
    parser.add_argument("--verbose", action="store_true", help="Mostra tutti i cambiamenti URL/titolo in console.")
    args = parser.parse_args()

    pairs = pair_files()
    total_files = len(pairs)
    if not total_files:
        print("❌ Nessuna coppia input/output trovata.")
        return

    report = {}
    diff_count = 0
    for sid, (in_path, out_path) in sorted(pairs.items()):
        changed = process_pair(sid, in_path, out_path, apply=args.apply, verbose=args.verbose, report=report)
        if changed:
            diff_count += 1

    # Save report only if not empty
    if report:
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    print("\n--- FULL SYNC SUMMARY ---")
    if diff_count:
        label = "updated" if args.apply else "with diffs (preview)"
        print(f"🟢 Files {label}: {diff_count} / {total_files}")
        print(f"📄 Report: {REPORT_PATH}")
    else:
        print(f"✅ Nessuna differenza trovata su {total_files} file.")
    print(f"📂 Backups: {BACKUP_DIR}")


if __name__ == "__main__":
    main()