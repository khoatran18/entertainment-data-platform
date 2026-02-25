# Collector


This module is responsible for fetching raw entertainment data (Movies, TV Series, and People) from the **TMDB API**. It uses a sophisticated ID-tracking mechanism to differentiate between existing, new, and updated records.

---

## 📂 Module Structure

```text
collector/
├── craw_api/             # Core crawling logic using TMDB API
│   └── tmdb/
│       ├── .env          # API credentials
│       ├── craw_movie.ipynb
│       ├── craw_person.ipynb
│       └── craw_tv_series.ipynb
├── data/                 # Raw storage for crawled records
│   ├── id_list/          # ID lists (JSONL) from TMDB daily exports
│   ├── movie/            # Raw JSONL for Movies
│   ├── person/           # Raw JSONL for People
│   └── tv_series/        # Raw JSONL for TV Series
├── util/                 # Data processing utilities
│   ├── extract_new_id.ipynb    # Logic to identify NEW and CHANGED IDs
│   └── convert_into_batch.ipynb # Re-batching logic for Kaggle optimization
└── README.md
```

---

## 📊 Pre-crawled Datasets

### If you prefer to skip the crawling process, you can access the ready-to-use data here:

Kaggle: [TMDb Craw Dataset](https://www.kaggle.com/datasets/khoatm2k4/tmdb-craw-dataset) <br>
HuggingFace: [TMDb Craw Dataset](https://huggingface.co/datasets/tmkhoa/tmdb-craw-dataset/tree/main)

---

## ⚙️ How it Works

The crawler leverages TMDB Daily ID Exports (files provided by TMDB containing all valid IDs up to a specific date) to manage incremental updates.

### 1️⃣ ID Diffing & Versioning

#### The system tracks changes by comparing two snapshots of TMDB ID lists (in this project: comparing 2025-12-31 with 2026-02-02):

**Baseline**: All IDs from the first snapshot (Dec 2025) are crawled and stored as “Old Data”.

**Extraction**: The util/extract_new_id.ipynb script performs a comparison between the old and new ID lists to determine:

- New IDs: IDs added to TMDB after the baseline date.
- Changed IDs: Existing IDs that require a re-crawl to capture metadata updates.

### 2️⃣  Crawling Process

The notebooks in craw_api/tmdb/ read the IDs identified from data/id_list/. They fetch full details (metadata, credits, etc.) from the TMDB API and save them as JSONL files in the corresponding data/ sub-folders.

### 3️⃣ Kaggle Integration

#### Due to the large volume of records, crawling is often offloaded to Kaggle.

**The Issue**: Different crawl sessions or worker nodes may produce JSONL files with inconsistent record counts per file.

**The Solution**: util/convert_into_batch.ipynb loads these irregular files and redistributes the records into uniform, balanced batches after crawing successfully. This ensures high throughput and stability when the data is eventually pushed to the Spark processing pipeline.

---

## 🚀 Quick Start

### 1️⃣ Setup API Key

Create a .env file in craw_api/tmdb/:

```bash
MDB_API_KEY_MOVIE=
TMDB_API_KEY_TV_SERIES=
TMDB_API_KEY_PERSON=
```

### 2️⃣ Run Crawler

Open the notebooks in craw_api/tmdb/ and execute cells sequentially.

### 3️⃣ Identify Deltas

Use util/extract_new_id.ipynb to compare ID lists to get new IDs.
