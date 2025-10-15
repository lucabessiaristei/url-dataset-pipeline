# URL Dataset Pipeline

A modular pipeline for extracting, processing, and transforming web URLs and bookmarks into structured datasets suitable for large language model (LLM) fine-tuning.

## Purpose

This project automates the transformation of web content—collected from URLs, bookmarks, or WARC archives—into high-quality training datasets.  
The pipeline covers every stage of the process, from data acquisition to final prompt construction.

## Pipeline Overview

```
URLs / Bookmarks → Filter → Expand → Split → API Processing → Prompt Generation → Training Dataset
```

### Processing Stages

1. **Collection** – Extraction and cataloging of URLs from multiple sources (bookmarks, WARC files, etc.)  
2. **Filtering** – Cleaning and validation of URL sets  
3. **Expansion** – Enrichment with metadata and page content  
4. **Splitting** – Division into balanced input batches  
5. **Generation** – API-based processing (Gemini) to produce structured outputs  
6. **Combination** – Assembly of prompt–response pairs from generated data  
7. **Dataset Creation** – Final organization for fine-tuning and evaluation

## Project Structure

### Main Directories

- **`in_out-s/`** — Workflow directories for inputs and outputs  
  - `working_split_IN--1/` – Input JSON splits (1000 files)  
  - `working_split_OUT--API-1/` – API-generated output splits (≈300 files)

- **`json_lists/`** — Master URL collections  
  - `working.json` – Base list of raw URLs  
  - `working_expanded.json` – Enriched URL data  
  - `working_expanded_eu.json` – Filtered list for EU sources  

- **`tools/`** — Processing and automation scripts  
  - `gemini_output_generator_API.py` – Handles API-based generation  
  - `working_expanded_splitter.py` – Batch splitter utility  
  - `json_urls_data_quick_expander.py` – Metadata expansion  
  - `working_expanded_error_cleaner.py` – Data validation and correction  
  - `working_expanded_europe_cleaner.py` – Regional filtering  
  - `gemini_mismatch_cleaner.py` – Output verification  
  - `simple_urls_list_to_json.py` – Format converter for raw lists  
  - `json_urls_quick_tester.py` – URL integrity testing  
  - `warc_to_json_quick_tester.py` – Extraction from WARC archives  
  - `directory_structure_printer.py` – Generates repository maps  

- **`url_resources/`** — Source materials  
  Contains exported bookmarks and Common Crawl WARC files.  

- **`split_backup_cleaner/`** — Backup directory for processed batches  

## Getting Started

### Requirements

```bash
Python 3.8+
pip install -r requirements.txt
```

### Basic Usage

1. **Add URLs to a collection**
   ```bash
   python tools/simple_urls_list_to_json.py
   ```

2. **Expand URL metadata**
   ```bash
   python tools/json_urls_data_quick_expander.py
   ```

3. **Split into batches**
   ```bash
   python tools/working_expanded_splitter.py
   ```

4. **Generate API-based outputs**
   ```bash
   python tools/gemini_output_generator_API.py
   ```

## Key Features

- **Batch-oriented workflow** — Efficient processing of large collections  
- **API integration** — Automated enrichment and data generation through Gemini  
- **Data validation** — Built-in checks and cleaning utilities  
- **Regional filtering** — Optional EU content isolation  
- **Modular design** — Each stage operates independently  
- **Backup system** — Automatic archival of processed data  

## Data Flow Summary

```
Raw URLs → JSON Collections → Expanded Data → Input Splits → API Outputs → Prompt–Response Pairs → Final Dataset
```

## Configuration

Environment variables are defined in `tools/.env`, including:
- API keys (auto-detected if containing `API_KEY`)
- Processing and timing parameters
- Directory paths

## Output Specification

The pipeline produces standardized JSON structures optimized for:
- Instruction fine-tuning  
- Conversational and QA datasets  
- Multi-turn dialogue systems  
- Context–response alignment tasks  

## File Naming Convention

| Type | Pattern | Description |
|------|----------|-------------|
| Input splits | `in_split_####.json` | 4-digit sequence for batch inputs |
| Output splits | `out_split_####.json` | Generated outputs via API |
| Backups | Mirror of input/output names | Maintained in `split_backup_cleaner/` |

## Current Progress

- **Input batches**: 1000 JSON files generated  
- **Output batches**: 302 files processed via API  
- **Next stage**: Dataset aggregation and prompt construction  

## Contributing

This repository serves as a personal dataset preparation and research environment.  
Forks, extensions, or integrations into other pipelines are welcome.

## License

Free to use.  
Credits for dataset URLs: contributions and Common Crawl WARC files (https://commoncrawl.org).

## Author

**Luca Bessi Aristei**

## Directory Map

The following section is automatically generated and kept up to date using:

```bash
python tools/directory_structure_printer.py --compact
```

---

## Complete Directory Structure

```
url-dataset-pipeline/
├── in_out-s/
│   ├── working_split_IN--1/
│   │   ├── in_split_0001.json
│   │   ├── in_split_0002.json
│   │   ├── in_split_0003.json
│   │   ├── in_split_0004.json
│   │   ├── in_split_0005.json
│   │   ├── in_split_0006.json
│   │   ├── in_split_0007.json
│   │   ├── in_split_0008.json
│   │   ├── in_split_0009.json
│   │   │   ... (982 files hidden) ...
│   │   ├── in_split_0992.json
│   │   ├── in_split_0993.json
│   │   ├── in_split_0994.json
│   │   ├── in_split_0995.json
│   │   ├── in_split_0996.json
│   │   ├── in_split_0997.json
│   │   ├── in_split_0998.json
│   │   ├── in_split_0999.json
│   │   ├── in_split_1000.json
│   └── working_split_OUT--API-1/
│       ├── out_split_0001.json
│       ├── out_split_0002.json
│       ├── out_split_0003.json
│       ├── out_split_0004.json
│       ├── out_split_0005.json
│       ├── out_split_0006.json
│       ├── out_split_0007.json
│       ├── out_split_0008.json
│       ├── out_split_0009.json
│       │   ... (282 files hidden) ...
│       ├── out_split_0292.json
│       ├── out_split_0293.json
│       ├── out_split_0294.json
│       ├── out_split_0295.json
│       ├── out_split_0296.json
│       ├── out_split_0297.json
│       ├── out_split_0299.json
│       ├── out_split_0300.json
│       ├── out_split_0302.json
├── json_lists/
│   ├── working.json
│   ├── working_expanded.json
│   └── working_expanded_eu.json
├── split_backup_cleaner/
│   ├── in_split_0001.json
│   ├── in_split_0002.json
│   ├── in_split_0003.json
│   ├── in_split_0004.json
│   ├── in_split_0005.json
│   ├── in_split_0006.json
│   ├── in_split_0007.json
│   ├── in_split_0008.json
│   ├── in_split_0009.json
│   │   ... (582 files hidden) ...
│   ├── out_split_0292.json
│   ├── out_split_0293.json
│   ├── out_split_0294.json
│   ├── out_split_0295.json
│   ├── out_split_0296.json
│   ├── out_split_0297.json
│   ├── out_split_0299.json
│   ├── out_split_0300.json
│   ├── out_split_0302.json
├── tools/
│   ├── .env
│   ├── directory_structure_printer.py
│   ├── gemini_mismatch_cleaner.py
│   ├── gemini_output_generator_API.py
│   ├── json_urls_data_quick_expander.py
│   ├── json_urls_quick_tester.py
│   ├── simple_urls_list_to_json.py
│   ├── warc_to_json_quick_tester.py
│   ├── working_expanded_error_cleaner.py
│   ├── working_expanded_europe_cleaner.py
│   └── working_expanded_splitter.py
├── url_resources/
│   ├── bookmark_json_batch.json
│   ├── CC-MAIN-20220116093137-20220116123137-00007.warc
│   └── CC-MAIN-20220116093137-20220116123137-00029.warc
├── ai_rules.txt
├── directory_structure.txt
└── README.md
```

*Directory structure generated automatically by `directory_structure_printer.py`*
```