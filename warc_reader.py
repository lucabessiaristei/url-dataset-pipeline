from warcio.archiveiterator import ArchiveIterator
import json, random

input_file = "CC-MAIN-20220116093137-20220116123137-xxx.warc"  # cambia nome file
output_file = "warc_to_json.json"
sample_size = 500

urls = []

with open(input_file, "rb") as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == "response":
            url = record.rec_headers.get_header("WARC-Target-URI")
            if url:
                urls.append(url)

# Estrae 500 URL casuali
sampled = random.sample(urls, min(sample_size, len(urls)))

# Salva in JSON
with open(output_file, "w") as f:
    json.dump(sampled, f, indent=2)

print(f"✅ Estratti {len(sampled)} URL casuali salvati in {output_file}")