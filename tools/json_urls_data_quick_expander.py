import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import os


# ==============================================================
# CONFIGURATION
# ==============================================================

INPUT_FILE = "./json_lists/working.json"
OUTPUT_FILE = "./json_lists/working_expanded.json"
TIMEOUT = 10
MAX_WORKERS = 10


# ==============================================================
# TEXT CLEANING / UNICODE SANITIZATION
# ==============================================================

def sanitize_unicode(s: str) -> str:
    """Replace ambiguous Unicode characters with ASCII equivalents."""
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
    """Remove invisible or invalid Unicode characters and normalize spaces."""
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
# HTML EXTRACTION
# ==============================================================

def extract_text_content(soup):
    """Extracts the first 150 chars of readable body text."""
    for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
        tag.decompose()
    body = soup.find('body')
    if not body:
        return ""
    text = body.get_text(separator=' ', strip=True)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:150] if text else ""


# ==============================================================
# SCRAPER
# ==============================================================

def contains_error_content(*parts) -> bool:
    """Detect 'error' or '404' messages in content."""
    joined = " ".join(p.lower() for p in parts if p)
    patterns = [
        "error",
        "404",
        "not found",
        "page not found",
        "server error",
        "an error has occurred",
        "forbidden",
    ]
    return any(p in joined for p in patterns)


def scrape_url(url):
    """Scrape single URL with timeout and full cleaning."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        response = requests.get(url, timeout=TIMEOUT, headers=headers, allow_redirects=True)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        title_tag = soup.find('title')
        title = title_tag.get_text().strip() if title_tag else ""

        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if not meta_desc:
            meta_desc = soup.find('meta', attrs={'property': 'og:description'})
        description = meta_desc.get('content', '').strip() if meta_desc else ""

        body_text = extract_text_content(soup)

        # Clean up all text fields
        title = clean_text(title)
        description = clean_text(description)
        body_text = clean_text(body_text)

        # Skip pages with no useful content
        if not (title or description or body_text):
            print(f"  ⚪ Empty content → {url}")
            return None

        # Skip error or 404 pages
        if contains_error_content(title, description, body_text):
            print(f"  ⚠️ Error/404 detected → {url}")
            return None

        # Replace empty fields with 'void'
        title = title if title else "void"
        description = description if description else "void"
        body_text = body_text if body_text else "void"

        return {
            'url': url,
            'title': title,
            'description': description,
            'preview': body_text
        }

    except Exception as e:
        print(f"  ❌ Error on {url}: {e}")
        return None


# ==============================================================
# MAIN
# ==============================================================

def main():
    # Load input file
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Input file {INPUT_FILE} not found!")
        return

    urls = data.get("urls", []) if isinstance(data, dict) else data

    # Load existing data
    existing_data = []
    existing_urls = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                existing_urls = {item['url'] for item in existing_data}
            print(f"✅ Found {len(existing_data)} already expanded URLs in {OUTPUT_FILE}")
        except json.JSONDecodeError:
            print(f"⚠️  {OUTPUT_FILE} is invalid JSON, will be overwritten")

    urls_to_process = [url for url in urls if url not in existing_urls]
    if not urls_to_process:
        print("\n✅ All URLs are already expanded!")
        return

    print(f"\n📋 Total URLs: {len(urls)}")
    print(f"   Already expanded: {len(existing_urls)}")
    print(f"   To process: {len(urls_to_process)}")
    print(f"\n🚀 Starting expansion with {MAX_WORKERS} threads...\n")

    new_results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(scrape_url, url): url for url in urls_to_process}
        for i, future in enumerate(as_completed(future_to_url), 1):
            url = future_to_url[future]
            print(f"[{i}/{len(urls_to_process)}] Checking {url} ...")
            result = future.result()
            if result:
                new_results.append(result)
                print("  ✅ Added")
            else:
                print("  ❌ Skipped")

    all_results = existing_data + new_results

    # Final cleaning of the output JSON
    json_text = json.dumps(all_results, indent=2, ensure_ascii=False)
    json_text = clean_text(json_text)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(json_text)

    print(f"\n✅ Done!")
    print(f"   Total expanded: {len(all_results)}")
    print(f"   New added: {len(new_results)}")
    print(f"   Saved to: {OUTPUT_FILE}")


# ==============================================================
# RUN SCRIPT
# ==============================================================

if __name__ == '__main__':
    main()