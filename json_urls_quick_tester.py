import json
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

# Configurazione
INPUT_FILE = "bookmark_urls_all.json"
OUTPUT_FILE = "working.json"
TIMEOUT = 1
MAX_WORKERS = 30  # thread paralleli
BATCH_SAVE = 15   # salva ogni N URL trovati

# Session globale con connection pooling
session = None

def get_session():
    """Crea una session riutilizzabile con connection pooling"""
    global session
    if session is None:
        session = requests.Session()
        retry = Retry(total=0, backoff_factor=0)
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=MAX_WORKERS,
            pool_maxsize=MAX_WORKERS * 2
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    return session

def is_working(url, timeout=TIMEOUT):
    """Verifica se un URL è raggiungibile"""
    try:
        sess = get_session()
        # Usa HEAD per essere più veloce
        r = sess.head(
            url, 
            allow_redirects=True, 
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
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
    start_time = time.time()
    
    # Step 1: Leggi il file di input
    print(f"📂 Lettura file: {INPUT_FILE}")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Estrai lista di URL
    if isinstance(data, dict):
        urls = data.get("urls", [])
    else:
        urls = data
    
    print(f"✅ Trovati {len(urls)} URL totali nel file di input")

    # Step 2: Carica URL già verificati
    existing_urls = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                existing_urls = set(json.load(f))
            print(f"📋 Trovati {len(existing_urls)} URL già verificati")
        except json.JSONDecodeError:
            print(f"⚠️  {OUTPUT_FILE} non valido, verrà sovrascritto")
    
    # Step 3: Filtra URL già verificati
    urls_to_test = [u for u in urls if u not in existing_urls]
    print(f"🔍 URL da verificare: {len(urls_to_test)}")
    
    if not urls_to_test:
        print("✅ Tutti gli URL sono già stati verificati!")
        return

    # Step 4: Test parallelo
    working_set = existing_urls.copy()
    newly_added = 0
    tested = 0
    failed = 0
    
    print(f"\n🚀 Inizio verifica con {MAX_WORKERS} thread paralleli...\n")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Sottometti tutti i task
        future_to_url = {
            executor.submit(is_working, url): url 
            for url in urls_to_test
        }
        
        # Processa i risultati man mano che arrivano
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            tested += 1
            
            try:
                is_ok = future.result()
                
                if is_ok:
                    status = "✅"
                    working_set.add(url)
                    newly_added += 1
                    
                    # Salva periodicamente
                    if newly_added % BATCH_SAVE == 0:
                        save_results(working_set, OUTPUT_FILE)
                        elapsed = time.time() - start_time
                        rate = tested / elapsed if elapsed > 0 else 0
                        print(f"💾 [{tested}/{len(urls_to_test)}] Salvati {len(working_set)} URL | {rate:.1f} URL/s")
                else:
                    status = "❌"
                    failed += 1
                
                # Mostra progresso ogni 50 URL
                if tested % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = tested / elapsed if elapsed > 0 else 0
                    eta = (len(urls_to_test) - tested) / rate if rate > 0 else 0
                    print(f"📊 Progresso: {tested}/{len(urls_to_test)} ({tested/len(urls_to_test)*100:.1f}%) | "
                          f"Funzionanti: {newly_added} | Falliti: {failed} | "
                          f"Velocità: {rate:.1f} URL/s | ETA: {eta/60:.1f}m")
                        
            except Exception as e:
                print(f"⚠️  Errore su {url}: {e}")
                failed += 1
    
    # Salvataggio finale
    save_results(working_set, OUTPUT_FILE)
    
    # Statistiche finali
    elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"✅ Completato in {elapsed/60:.2f} minuti!")
    print(f"{'='*70}")
    print(f"   URL totali nel file:        {len(urls)}")
    print(f"   URL testati:                {len(urls_to_test)}")
    print(f"   URL funzionanti trovati:    {newly_added}")
    print(f"   URL non funzionanti:        {failed}")
    print(f"   URL funzionanti totali:     {len(working_set)}")
    print(f"   Velocità media:             {tested/elapsed:.1f} URL/s")
    print(f"   Salvati in:                 {OUTPUT_FILE}")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()