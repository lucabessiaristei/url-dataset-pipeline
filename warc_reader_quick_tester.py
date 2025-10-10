from warcio.archiveiterator import ArchiveIterator
import json
import requests
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configurazione
INPUT_WARC = "CC-MAIN-20220116093137-20220116123137-00029.warc"
OUTPUT_FILE = "working.json"
SAMPLE_SIZE = 5000
TIMEOUT = 1
MAX_WORKERS = 20  # numero di thread paralleli
BATCH_SAVE = 10   # salva ogni N URL trovati

# Session globale con connection pooling
session = None

def get_session():
    """Crea una session riutilizzabile con connection pooling"""
    global session
    if session is None:
        session = requests.Session()
        # Retry strategy
        retry = Retry(total=0, backoff_factor=0)
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=MAX_WORKERS,
            pool_maxsize=MAX_WORKERS * 2
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    return session

def extract_urls_from_warc(warc_file):
    """Estrae tutti gli URL dal file WARC"""
    urls = []
    print(f"📂 Lettura file WARC: {warc_file}")
    
    with open(warc_file, "rb") as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == "response":
                url = record.rec_headers.get_header("WARC-Target-URI")
                if url:
                    urls.append(url)
    
    print(f"✅ Trovati {len(urls)} URL totali nel WARC")
    return urls

def is_working(url, timeout=TIMEOUT):
    """Verifica se un URL è raggiungibile usando solo HEAD"""
    try:
        sess = get_session()
        # Solo HEAD request (più veloce)
        r = sess.head(
            url, 
            allow_redirects=True, 
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        return 200 <= r.status_code < 400
    except Exception:
        return False

def save_results(working_set, output_file):
    """Salva i risultati su file"""
    working_list = sorted(list(working_set))
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(working_list, f, ensure_ascii=False, indent=2)

def main():
    # Step 1: Estrai URL dal WARC
    all_urls = extract_urls_from_warc(INPUT_WARC)
    
    # Step 2: Carica URL già verificati
    existing_urls = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                existing_urls = set(json.load(f))
            print(f"📋 Trovati {len(existing_urls)} URL già verificati")
        except json.JSONDecodeError:
            print(f"⚠️  {OUTPUT_FILE} non valido, verrà sovrascritto")
    
    # Step 3: Filtra URL già verificati PRIMA del campionamento
    unverified_urls = [u for u in all_urls if u not in existing_urls]
    print(f"🔍 URL da verificare: {len(unverified_urls)}")
    
    # Step 4: Campiona solo dagli URL non verificati
    sampled_urls = random.sample(
        unverified_urls, 
        min(SAMPLE_SIZE, len(unverified_urls))
    )
    print(f"🎲 Selezionati {len(sampled_urls)} URL casuali da testare")
    
    if not sampled_urls:
        print("✅ Tutti gli URL sono già stati verificati!")
        return
    
    # Step 5: Test parallelo
    working_set = existing_urls.copy()
    newly_added = 0
    tested = 0
    
    print(f"\n🔍 Inizio verifica con {MAX_WORKERS} thread paralleli...\n")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Sottometti tutti i task
        future_to_url = {
            executor.submit(is_working, url): url 
            for url in sampled_urls
        }
        
        # Processa i risultati man mano che arrivano
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            tested += 1
            
            try:
                is_ok = future.result()
                status = "✅" if is_ok else "❌"
                print(f"[{tested}/{len(sampled_urls)}] {status} {url}")
                
                if is_ok:
                    working_set.add(url)
                    newly_added += 1
                    
                    # Salva periodicamente
                    if newly_added % BATCH_SAVE == 0:
                        save_results(working_set, OUTPUT_FILE)
                        print(f"💾 Salvati {len(working_set)} URL")
                        
            except Exception as e:
                print(f"[{tested}/{len(sampled_urls)}] ⚠️  Errore su {url}: {e}")
    
    # Salvataggio finale
    save_results(working_set, OUTPUT_FILE)
    
    # Riepilogo
    print(f"\n{'='*60}")
    print(f"✅ Completato!")
    print(f"{'='*60}")
    print(f"   URL totali nel WARC:    {len(all_urls)}")
    print(f"   URL testati:            {len(sampled_urls)}")
    print(f"   URL funzionanti totali: {len(working_set)}")
    print(f"   Nuovi URL aggiunti:     {newly_added}")
    print(f"   Salvati in:             {OUTPUT_FILE}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()