---
title: AFDB Complexes Metadata Dashboard
emoji: "🧬"
colorFrom: blue
colorTo: indigo
sdk: streamlit
sdk_version: 1.33.0
app_file: app.py
pinned: false
---

# AFDB Complexes Metadata Dashboard

Interactive Streamlit dashboard for exploring the AFDB complexes metadata parquet with:
- Numeric column distributions
- Categorical histograms
- Dynamic filtering and text search

## Dataset

Source parquet file:
- [model_entity_metadata_mapping.parquet](https://huggingface.co/datasets/yantipin/afdb-complexes-metadata-19mil/blob/main/model_entity_metadata_mapping.parquet)

The app defaults to this dataset URL and automatically rewrites Hugging Face `/blob/` links to `/resolve/` for direct parquet reads.

## Local development

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Spaces configuration

This repository is configured for **Hugging Face Spaces (Streamlit SDK)** via the README front matter above.

Optional environment variable:
- `AFDB_PARQUET_URL`: override the default parquet URL at runtime.
