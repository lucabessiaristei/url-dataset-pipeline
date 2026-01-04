#!/usr/bin/env python3
"""
Crea 1000 file JSON con numero casuale di siti (10–200), scelti casualmente dal file originale,
privilegiando quelli meno usati per massimizzare la varietà.
✨ Riprende da dove si era interrotto se trova file esistenti
"""

import json
import os
import random
import math
import re
from collections import defaultdict

# ---------- CONFIG ----------
INPUT_FILE = "./json_lists/working_expanded_eu.json"
OUTPUT_DIR = "./in_out-s/working_split_IN--4"
BASE_NAME = "in_split"
NUM_FILES = 1000
MIN_ITEMS = 10
MAX_ITEMS = 200
MEAN_ITEMS = 100
STD_DEV = 30
# ----------------------------


def ensure_output_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def get_existing_files(output_dir: str, base_name: str):
    """Trova file esistenti e ritorna i loro numeri"""
    if not os.path.exists(output_dir):
        return set()
    
    pattern = re.compile(rf"^{re.escape(base_name)}_(\d{{4}})\.json$")
    existing = set()
    
    for filename in os.listdir(output_dir):
        match = pattern.match(filename)
        if match:
            existing.add(int(match.group(1)))
    
    return existing


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

    # Controlla file esistenti
    existing_numbers = get_existing_files(OUTPUT_DIR, BASE_NAME)
    existing_count = len(existing_numbers)
    
    if existing_count > 0:
        print(f"📂 Trovati {existing_count} file esistenti in {OUTPUT_DIR}")
        print(f"   Riprendo dalla creazione dei file mancanti...")
    
    # Calcola quali file creare
    all_numbers = set(range(1, NUM_FILES + 1))
    to_create = sorted(all_numbers - existing_numbers)
    
    if not to_create:
        print(f"✅ Tutti i {NUM_FILES} file sono già presenti!")
        return
    
    print(f"🔨 Creerò {len(to_create)} file mancanti (da {NUM_FILES} totali)")

    used_count = defaultdict(int)
    created = 0

    for i in to_create:
        k = random_items_count()

        # Calcola pesi inversi alla frequenza (meno usati = più probabili)
        weights = [1 / (1 + used_count[id(item)]) for item in data]
        selected = random.choices(data, weights=weights, k=k)

        # Aggiorna contatori d'uso
        for item in selected:
            used_count[id(item)] += 1

        filename = f"{BASE_NAME}_{i:04d}.json"
        out_path = os.path.join(OUTPUT_DIR, filename)
        write_chunk(selected, out_path)
        created += 1

        if created % 50 == 0 or created == len(to_create):
            total_now = existing_count + created
            avg_uses = sum(used_count.values()) / len(used_count) if used_count else 0
            print(f"   ✅ {created}/{len(to_create)} creati ({total_now}/{NUM_FILES} totali) "
                  f"— ultimo: {k} voci, media usi: {avg_uses:.2f}")

    final_total = existing_count + created
    print(f"\n✅ Completato!")
    print(f"   File esistenti: {existing_count}")
    print(f"   File creati ora: {created}")
    print(f"   Totale finale: {final_total}/{NUM_FILES}")
    print(f"   Output: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()