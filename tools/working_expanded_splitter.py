#!/usr/bin/env python3
"""
Crea 1000 file JSON con numero casuale di siti (10–200), scelti casualmente dal file originale,
privilegiando quelli meno usati per massimizzare la varietà.
"""

import json
import os
import random
import math
from collections import defaultdict

# ---------- CONFIG ----------
INPUT_FILE = "./json_lists/working_expanded_eu.json"
OUTPUT_DIR = "./in_out-s/working_split_IN--2"
BASE_NAME = "split"
NUM_FILES = 1000
MIN_ITEMS = 10
MAX_ITEMS = 200
MEAN_ITEMS = 100
STD_DEV = 30
# ----------------------------


def ensure_output_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def random_items_count():
    n = int(random.gauss(MEAN_ITEMS, STD_DEV))
    return max(MIN_ITEMS, min(MAX_ITEMS, n))


def write_chunk(items, out_path):
    output_data = {
        "data": items,
        "_meta": {"total_bookmarks": len(items)}
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)


def main():
    if not os.path.isfile(INPUT_FILE):
        print(f"❌ File di input non trovato: {INPUT_FILE}")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    total = len(data)
    print(f"📦 Voci disponibili: {total}")

    ensure_output_dir(OUTPUT_DIR)

    used_count = defaultdict(int)

    for i in range(1, NUM_FILES + 1):
        k = random_items_count()

        # Calcola pesi inversi alla frequenza (meno usati = più probabili)
        weights = [1 / (1 + used_count[id(item)]) for item in data]
        selected = random.choices(data, weights=weights, k=k)

        # Aggiorna contatori d’uso
        for item in selected:
            used_count[id(item)] += 1

        filename = f"{BASE_NAME}_{i:04d}.json"
        out_path = os.path.join(OUTPUT_DIR, filename)
        write_chunk(selected, out_path)

        if i % 50 == 0 or i == NUM_FILES:
            avg_uses = sum(used_count.values()) / len(used_count)
            print(f"   ✅ {i}/{NUM_FILES} file creati (ultimo: {k} voci) — media usi: {avg_uses:.2f}")

    print(f"\n✅ Completato. {NUM_FILES} file generati con distribuzione random bilanciata.")
    print(f"   Output: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()