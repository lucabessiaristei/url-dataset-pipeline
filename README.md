# URL Dataset Pipeline

A modular pipeline for extracting, processing, and transforming web URLs and bookmarks into structured datasets suitable for large language model (LLM) fine-tuning.

## Purpose

This repository automates the transformation of web content—collected from URLs, bookmarks, or Common Crawl WARC archives—into structured datasets. The pipeline covers data acquisition, cleaning, enrichment, batching, API-based generation, and final prompt/dataset construction.

## Quick start

Requirements:

```bash
python3 -m pip install -r requirements.txt
```

Common workflows:

```bash
# convert a raw list to JSON
python tools/simple_urls_list_to_json.py

# quick test/filter a JSON url list
python tools/json_urls_quick_tester.py

# expand metadata and content
python tools/json_urls_data_quick_expander.py

# split the expanded dataset into input batches
python tools/working_expanded_splitter.py

# generate structured outputs via API
python tools/gemini_output_generator_API.py
```

## Steps (done / to do / updates)

**Done**
- Core utilities for collecting, cleaning and expanding URL lists
- Split generator: created 1000 input batches
- Initial Gemini-based API output generation built
- Backup and mismatch-cleaner utilities

**To do**
- Complete API processing to produce matching outputs for all input splits
- Aggregate outputs into prompt–response pairs and finalize schema for finetuning
- Standardize output schema and prepare export for training workflows

**Recent updates**
- README and directory-printer improvements to ensure stable README updates
- Dynamic API key discovery added to generator script (auto-detects `API_KEY` env vars)

## Configuration

Environment variables live in `tools/.env`. The pipeline auto-detects any environment variable name containing `API_KEY` for use by API scripts. Additional parameters (paths, rates) can be configured via `.env` or script-level constants.

## Output formats

The pipeline produces normalized JSON suitable for:
- Instruction fine-tuning
- Conversational and QA datasets
- Multi-turn dialogue datasets
- Context–response alignment

## File naming conventions

- Input splits: `in_split_####.json` (4-digit)
- Output splits: `out_split_####.json`
- Backup files: mirror input/output names in `split_backup_cleaner/`

## License & Credits

Free to use. Initial URL contributions and seeds come from community contributions and Common Crawl WARC files (https://commoncrawl.org). Please retain attribution when redistributing derived datasets.

## Author

Luca Bessi Aristei

## Directory Map

The following section is automatically generated and kept up to date using:

```bash
python tools/directory_structure_printer.py
```

---

## Complete Directory Structure

```
url-dataset-pipeline/
├── in_out-s/
│   ├── RAW/
│   │   ├── out_split_0010_RAW.txt
│   │   ├── out_split_0026_RAW.txt
│   │   ├── out_split_0027_RAW.txt
│   │   ├── out_split_0029_RAW.txt
│   │   ├── out_split_0038_RAW.txt
│   │   ├── out_split_0059_RAW.txt
│   │   ├── out_split_0069_RAW.txt
│   │   ├── out_split_0071_RAW.txt
│   │   ├── out_split_0074_RAW.txt
│   │   │   ... (134 files hidden) ...
│   │   ├── out_split_0949_RAW.txt
│   │   ├── out_split_0966_RAW.txt
│   │   ├── out_split_0970_RAW.txt
│   │   ├── out_split_0972_RAW.txt
│   │   ├── out_split_0984_RAW.txt
│   │   ├── out_split_0985_RAW.txt
│   │   ├── out_split_0991_RAW.txt
│   │   ├── out_split_0993_RAW.txt
│   │   ├── out_split_0999_RAW.txt
│   ├── split_backup_cleaner/
│   │   ├── in_split_0001.json
│   │   ├── in_split_0002.json
│   │   ├── in_split_0003.json
│   │   ├── in_split_0004.json
│   │   ├── in_split_0005.json
│   │   ├── in_split_0006.json
│   │   ├── in_split_0007.json
│   │   ├── in_split_0008.json
│   │   ├── in_split_0009.json
│   │   │   ... (776 files hidden) ...
│   │   ├── out_split_0391.json
│   │   ├── out_split_0392.json
│   │   ├── out_split_0393.json
│   │   ├── out_split_0394.json
│   │   ├── out_split_0396.json
│   │   ├── out_split_0398.json
│   │   ├── out_split_0399.json
│   │   ├── out_split_0400.json
│   │   ├── out_split_0401.json
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
│   ├── working_split_IN--2/
│   │   ├── split_0001.json
│   │   ├── split_0002.json
│   │   ├── split_0003.json
│   │   ├── split_0004.json
│   │   ├── split_0005.json
│   │   ├── split_0006.json
│   │   ├── split_0007.json
│   │   ├── split_0008.json
│   │   ├── split_0009.json
│   │   │   ... (982 files hidden) ...
│   │   ├── split_0992.json
│   │   ├── split_0993.json
│   │   ├── split_0994.json
│   │   ├── split_0995.json
│   │   ├── split_0996.json
│   │   ├── split_0997.json
│   │   ├── split_0998.json
│   │   ├── split_0999.json
│   │   ├── split_1000.json
│   ├── working_split_OUT--API-1/
│   │   ├── out_split_0002.json
│   │   ├── out_split_0003.json
│   │   ├── out_split_0004.json
│   │   ├── out_split_0005.json
│   │   ├── out_split_0006.json
│   │   ├── out_split_0007.json
│   │   ├── out_split_0008.json
│   │   ├── out_split_0009.json
│   │   ├── out_split_0010.json
│   │   │   ... (720 files hidden) ...
│   │   ├── out_split_0987.json
│   │   ├── out_split_0989.json
│   │   ├── out_split_0990.json
│   │   ├── out_split_0992.json
│   │   ├── out_split_0994.json
│   │   ├── out_split_0995.json
│   │   ├── out_split_0996.json
│   │   ├── out_split_0997.json
│   │   ├── out_split_0998.json
│   ├── working_split_OUT--API-2/
│   └── gemini_clean_report.json
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
│   ├── fields_OUT_cleaner.py
│   ├── gemini_mismatch_cleaner.py
│   ├── gemini_output_generator_API.py
│   ├── json_urls_data_quick_expander.py
│   ├── json_urls_quick_tester.py
│   ├── simple_urls_list_to_json.py
│   ├── small_folders_OUT_cleaner.py
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
├── notes.txt
├── README.md
└── requirements.txt
```

*END