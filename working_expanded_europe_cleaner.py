import json
import os
import re
import requests
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory, LangDetectException

DetectorFactory.seed = 0

INPUT_FILE = "working_expanded.json"
OUTPUT_FILE = "working_expanded_eu.json"
TIMEOUT = 5  # seconds per site

# Lingue europee considerate valide
EURO_LANGS = {
    "it", "en", "fr", "de", "es", "pt", "nl", "da", "sv", "no", "fi",
    "pl", "cs", "sk", "sl", "hu", "ro", "bg", "el"
}


# ==============================================================
# UTILS
# ==============================================================

def clean_text(text):
    """Rimuove caratteri invisibili e normalizza spazi"""
    if not text:
        return ""
    text = re.sub(r"[\u2028\u2029\u200b\ufeff]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def detect_language(text):
    """Ritorna il codice della lingua o 'unknown'"""
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"


def fetch_html_text(url):
    """Scarica la pagina e ritorna il testo pulito dal body HTML"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
        }
        resp = requests.get(url, timeout=TIMEOUT, headers=headers, allow_redirects=True)
        if resp.status_code >= 400:
            return ""
        soup = BeautifulSoup(resp.content, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        text = clean_text(text)
        return text[:2000]  # massimo 2000 caratteri per evitare testi enormi
    except Exception:
        return ""


# ==============================================================
# MAIN
# ==============================================================

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File {INPUT_FILE} non trovato.")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print("⚠️  Il file non contiene una lista JSON valida.")
        return

    print(f"📊 Voci totali: {len(data)}")

    kept, removed = [], 0
    stats = {}

    for i, item in enumerate(data, 1):
        url = item.get("url", "")
        title = clean_text(item.get("title", ""))
        desc = clean_text(item.get("description", ""))
        body = clean_text(item.get("preview", ""))

        combined = " ".join([title, desc, body]).strip()
        lang = detect_language(combined)

        # Se non riconosciuta → prova via HTML
        if lang == "unknown":
            html_text = fetch_html_text(url)
            if html_text:
                lang = detect_language(html_text)
                print(f"  🌐 ({i}) Lingua dedotta da HTML: {lang} → {url}")
            else:
                print(f"  ❔ ({i}) Impossibile determinare lingua → {url}")

        # Se ancora sconosciuta, segna come unknown
        if not lang:
            lang = "unknown"

        # Filtra non europee
        if lang not in EURO_LANGS and lang != "unknown":
            removed += 1
            print(f"  ❌ ({i}) Rimosso ({lang}) → {url}")
            continue

        kept.append({
            "url": url,
            "title": title,
            "description": desc,
            "preview": body,
        })

        stats[lang] = stats.get(lang, 0) + 1

    # Salva file finale
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, indent=2, ensure_ascii=False)

    # Log finale
    print(f"\n✅ Completato!")
    print(f"   Voci originali: {len(data)}")
    print(f"   Rimosse (non europee): {removed}")
    print(f"   Voci finali: {len(kept)}")
    print(f"   Salvato in: {OUTPUT_FILE}")
    print("\n📈 Statistiche per lingua:")
    for lang, count in sorted(stats.items(), key=lambda x: -x[1]):
        print(f"   {lang}: {count}")


if __name__ == "__main__":
    main()