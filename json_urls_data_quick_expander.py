import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import os
import time

INPUT_FILE = "working.json"
OUTPUT_FILE = "working_data_expanded.json"
TIMEOUT = 2
MAX_WORKERS = 25
BATCH_SAVE = 20  # Salva ogni N URL espansi con successo

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

def extract_text_content(soup):
    """Estrae i primi 150 caratteri di testo utile dal body"""
    # Rimuovi script, style e altri tag non necessari
    for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
        tag.decompose()
    
    # Prendi il testo dal body
    body = soup.find('body')
    if not body:
        return ""
    
    # Estrai tutto il testo
    text = body.get_text(separator=' ', strip=True)
    
    # Pulisci spazi multipli e newline
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Ritorna i primi 150 caratteri
    return text[:150] if text else ""

def scrape_url(url):
    """Scrape singolo URL con timeout di 2s"""
    try:
        sess = get_session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = sess.get(url, timeout=TIMEOUT, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        # Usa lxml parser se disponibile (più veloce), altrimenti html.parser
        soup = BeautifulSoup(response.content, 'lxml' if 'lxml' in BeautifulSoup.builder_registry.builders else 'html.parser')
        
        # Estrai meta title
        title = ""
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text().strip()
        
        # Estrai meta description
        description = ""
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if not meta_desc:
            meta_desc = soup.find('meta', attrs={'property': 'og:description'})
        if meta_desc:
            description = meta_desc.get('content', '').strip()
        
        # Estrai testo dal body
        body_text = extract_text_content(soup)
        
        return {
            'url': url,
            'title': title,
            'description': description,
            'body_preview': body_text
        }
        
    except Exception as e:
        return None

def save_results(all_results, output_file):
    """Salva i risultati su file"""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

def main():
    start_time = time.time()
    
    # Leggi gli URL dal file JSON
    print(f"📂 Lettura file: {INPUT_FILE}")
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ File {INPUT_FILE} non trovato!")
        return
    
    # Estrai lista di URL
    if isinstance(data, dict):
        urls = data.get("urls", [])
    else:
        urls = data
    
    print(f"✅ Trovati {len(urls)} URL totali nel file di input")
    
    # Carica i dati già espansi (se esistono) - IMPORTANTE: non sovrascrivere
    existing_data = []
    existing_urls = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                existing_urls = {item['url'] for item in existing_data}
            print(f"📋 Trovati {len(existing_data)} URL già espansi (verranno preservati)")
        except json.JSONDecodeError:
            print(f"⚠️  {OUTPUT_FILE} non è un JSON valido, verrà sovrascritto")
            existing_data = []
            existing_urls = set()
    
    # Filtra solo gli URL nuovi da processare
    urls_to_process = [url for url in urls if url not in existing_urls]
    
    if not urls_to_process:
        print("\n✅ Tutti gli URL sono già stati espansi!")
        return
    
    print(f"🔍 URL da processare: {len(urls_to_process)}")
    print(f"\n🚀 Inizio espansione con {MAX_WORKERS} thread paralleli...\n")
    
    new_results = []
    processed = 0
    failed = 0
    
    # Usa ThreadPoolExecutor per parallelizzare le richieste
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Sottometti tutti i task
        future_to_url = {executor.submit(scrape_url, url): url for url in urls_to_process}
        
        # Processa i risultati man mano che completano
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            processed += 1
            
            try:
                result = future.result()
                if result:
                    new_results.append(result)
                    status = "✅"
                    
                    # Salva periodicamente (SEMPRE preservando i dati esistenti)
                    if len(new_results) % BATCH_SAVE == 0:
                        all_results = existing_data + new_results
                        save_results(all_results, OUTPUT_FILE)
                        elapsed = time.time() - start_time
                        rate = processed / elapsed if elapsed > 0 else 0
                        print(f"💾 [{processed}/{len(urls_to_process)}] Salvati {len(all_results)} URL totali | {rate:.1f} URL/s")
                else:
                    status = "❌"
                    failed += 1
                
                # Mostra progresso ogni 50 URL
                if processed % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta = (len(urls_to_process) - processed) / rate if rate > 0 else 0
                    print(f"📊 Progresso: {processed}/{len(urls_to_process)} ({processed/len(urls_to_process)*100:.1f}%) | "
                          f"Espansi: {len(new_results)} | Falliti: {failed} | "
                          f"Velocità: {rate:.1f} URL/s | ETA: {eta/60:.1f}m")
                    
            except Exception as e:
                print(f"⚠️  Errore su {url}: {e}")
                failed += 1
    
    # Combina i risultati esistenti con i nuovi - PRESERVA SEMPRE I DATI ESISTENTI
    all_results = existing_data + new_results
    
    # Salvataggio finale
    save_results(all_results, OUTPUT_FILE)
    
    # Statistiche finali
    elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"✅ Completato in {elapsed/60:.2f} minuti!")
    print(f"{'='*70}")
    print(f"   URL totali nel file input:     {len(urls)}")
    print(f"   URL già espansi (preservati):  {len(existing_data)}")
    print(f"   URL processati ora:            {len(urls_to_process)}")
    print(f"   Nuovi URL espansi:             {len(new_results)}")
    print(f"   URL falliti:                   {failed}")
    print(f"   Totale URL nel file output:    {len(all_results)}")
    print(f"   Velocità media:                {processed/elapsed:.1f} URL/s")
    print(f"   Salvati in:                    {OUTPUT_FILE}")
    print(f"{'='*70}")

if __name__ == '__main__':
    main()