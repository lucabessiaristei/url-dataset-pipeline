import json
from urllib.parse import urlparse
from collections import defaultdict

input_file = "./url_resources_urls.txt"
output_file = "./url_resources_simple_to_json.json"
MAX_PER_DOMAIN = 2

# Domini da ignorare
IGNORED_DOMAINS = [
    "facebook.com", "fb.com", "m.facebook.com", "www.facebook.com",
    "google.com", "google.it", "google.co.uk", "www.google.com",
    "youtube.com", "www.youtube.com"
]

# Parole chiave da escludere ovunque nell’URL (anche su altri domini)
IGNORED_KEYWORDS = [
    "google", "youtube", "ytcreator", "teamyoutube", "ytcreators"
]

def get_domain(url):
    """Estrae il dominio da un URL"""
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower()
    except:
        return None

def should_ignore(url, domain):
    """Verifica se l'URL o il dominio devono essere ignorati"""
    if not domain:
        return True

    # Escludi se contiene parole chiave specifiche ovunque
    url_lower = url.lower()
    for keyword in IGNORED_KEYWORDS:
        if keyword in url_lower:
            return True

    # Escludi se dominio è o termina con uno di quelli ignorati
    for ignored in IGNORED_DOMAINS:
        if domain == ignored or domain.endswith('.' + ignored):
            return True

    return False


# Dizionari di conteggio
domain_count = defaultdict(int)
urls = []
skipped = defaultdict(int)
ignored = defaultdict(int)

print(f"📂 Lettura file: {input_file}\n")

with open(input_file, "r", encoding="utf-8") as f:
    for line_num, line in enumerate(f, 1):
        url = line.strip()
        if not url:
            continue

        domain = get_domain(url)
        if not domain:
            print(f"⚠️  Linea {line_num}: URL non valido ignorato: {url}")
            continue

        # Controlla se l’URL deve essere ignorato
        if should_ignore(url, domain):
            ignored[domain] += 1
            continue

        # Rispetta il limite per dominio
        if domain_count[domain] >= MAX_PER_DOMAIN:
            skipped[domain] += 1
            continue

        urls.append(url)
        domain_count[domain] += 1

# Salva in JSON
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(urls, f, indent=2)

# Statistiche
print(f"\n{'='*60}")
print(f"✅ Completato!")
print(f"{'='*60}")
print(f"   URL totali estratti: {len(urls)}")
print(f"   Domini unici:        {len(domain_count)}")
print(f"   Salvati in:          {output_file}")

if ignored:
    total_ignored = sum(ignored.values())
    print(f"\n🚫 URL ignorati (Google/YouTube/collegati): {total_ignored}")
    for domain, count in sorted(ignored.items(), key=lambda x: x[1], reverse=True):
        print(f"   {domain}: {count} URL")

if skipped:
    print(f"\n📊 URL saltati per limite ({MAX_PER_DOMAIN} per dominio):")
    for domain, count in sorted(skipped.items(), key=lambda x: x[1], reverse=True):
        print(f"   {domain}: {count} URL saltati ({domain_count[domain]} mantenuti)")

# Top 10 domini
print(f"\n🏆 Top 10 domini per numero di URL:")
top_domains = sorted(domain_count.items(), key=lambda x: x[1], reverse=True)[:10]
for domain, count in top_domains:
    print(f"   {domain}: {count} URL")

print(f"{'='*60}")