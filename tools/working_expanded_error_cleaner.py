#!/usr/bin/env python3
"""
Error Page Cleaner + Text Sanitizer v3

✨ Features:
- Creates automatic backup before overwrite (suffix: --error-bk.json)
- Cleans Unicode artifacts and invisible characters
- Removes pages with error/404 content
- Removes entries with "void" title + very short preview
- Overwrites original file safely after cleaning
"""

import json
import re
import os
import shutil

# ==============================================================
# CONFIG
# ==============================================================

INPUT_FILE = "./json_lists/working_expanded_eu.json"
BACKUP_FILE = INPUT_FILE.replace(".json", "--error-bk.json")
TEMP_OUTPUT = INPUT_FILE.replace(".json", "--error-clean.json")

# ==============================================================
# TEXT CLEANING / UNICODE SANITIZATION
# ==============================================================

def sanitize_unicode(s: str) -> str:
    """Sostituisce caratteri Unicode ambigui o tipografici con equivalenti ASCII"""
    if not isinstance(s, str):
        return s
    replacements = {
        "\u2013": "-",   # en dash
        "\u2014": "-",   # em dash
        "\u2018": "'",   # single quote open
        "\u2019": "'",   # single quote close
        "\u201C": '"',   # double quote open
        "\u201D": '"',   # double quote close
        "\u2026": "...", # ellipsis
        "\u2212": "-",   # minus sign
        "\u00A0": " ",   # non-breaking space
        "\u202F": " ",   # narrow no-break space
    }
    for bad, good in replacements.items():
        s = s.replace(bad, good)
    return s


def clean_text(value: str) -> str:
    """Rimuove caratteri invisibili, LS/PS, BOM, zero-width e normalizza spazi"""
    if not value:
        return ""
    value = sanitize_unicode(value)
    cleaned = (
        value.replace("\u2028", " ")
             .replace("\u2029", " ")
             .replace("\ufeff", "")
             .replace("\u200b", "")
             .strip()
    )
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned


# ==============================================================
# ERROR / 404 FILTER
# ==============================================================

def contains_error_content(*parts) -> bool:
    """Rileva pagine con contenuto 'error', '404', 'not found', ecc."""
    joined = " ".join(p.lower() for p in parts if p)
    patterns = [
        "error",
        "404",
        "not found",
        "page not found",
        "server error",
        "an error has occurred",
        "forbidden",
        "unavailable",
        "internal error",
    ]
    return any(p in joined for p in patterns)


# ==============================================================
# MAIN CLEANER
# ==============================================================

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File {INPUT_FILE} non trovato.")
        return

    # --- Backup original file ---
    print(f"💾 Creazione backup: {BACKUP_FILE}")
    shutil.copy2(INPUT_FILE, BACKUP_FILE)

    # --- Read JSON ---
    print(f"📂 Lettura file: {INPUT_FILE}")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print("⚠️  Il file non contiene una lista JSON valida.")
        return

    print(f"🔍 Trovate {len(data)} voci totali.")

    cleaned_data = []
    removed = 0

    for item in data:
        url = item.get("url", "")
        title = clean_text(item.get("title", ""))
        desc = clean_text(item.get("description", ""))
        body = clean_text(item.get("preview", ""))

        # Filtra se contiene errori
        if contains_error_content(title, desc, body):
            removed += 1
            print(f"  ⚠️  Rimossa: {url} (pagina di errore)")
            continue

        # Normalizza campi vuoti
        title = title if title else "void"
        desc = desc if desc else "void"
        body = body if body else "void"

        # Nuova regola: rimuovi se title è "void" e preview < 50 caratteri
        if title == "void" and len(body) < 50:
            removed += 1
            print(f"  ⚠️  Rimossa: {url} (title void + preview troppo corto)")
            continue

        cleaned_data.append({
            "url": url,
            "title": title,
            "description": desc,
            "preview": body
        })

    # Pulizia finale del JSON
    json_text = json.dumps(cleaned_data, indent=2, ensure_ascii=False)
    json_text = clean_text(json_text)

    # --- Scrive prima file temporaneo ---
    with open(TEMP_OUTPUT, "w", encoding="utf-8") as f:
        f.write(json_text)

    # --- Sovrascrive l’input con il file pulito ---
    shutil.move(TEMP_OUTPUT, INPUT_FILE)

    # --- Report finale ---
    print(f"\n✅ Pulizia completata!")
    print(f"   Voci originali: {len(data)}")
    print(f"   Voci rimosse:   {removed}")
    print(f"   Voci finali:    {len(cleaned_data)}")
    print(f"   🔙 Backup:       {BACKUP_FILE}")
    print(f"   ✨ File aggiornato: {INPUT_FILE}")


if __name__ == "__main__":
    main()