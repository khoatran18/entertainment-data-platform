# Ingestion

This module simulates a streaming ingestion pipeline that reads raw data produced by the Collector, enriches it, and publishes events to Kafka.

The goal is to mimic real-time data flow where different data types and update states are processed together.

---

## 📂 Module Structure

```text
ingestion/
├── config/              # Kafka & pipeline configs
├── data/
│   ├── movie/           # Example structure (expanded)
│   │   ├── new/
│   │   ├── change/
│   │   └── old/
│   ├── tv_series/       # Same structure as movie
│   └── person/          # Same structure as movie
├── loader/              # Load & mix data into a simulated stream
├── preprocessor/        # Data enrichment & normalization
├── producer/            # Kafka producer logic
├── __init__.py
├── main.py              # Entry point
├── Dockerfile
└── README.md
```

---

## 🌊 Streaming Concept

Instead of sending a single batch, the pipeline mixes records from:

- Data Types: `movie`, `tv_series`, `person`
- Data Labels: `new`, `change`, `old`

This simulates how real systems receive heterogeneous updates continuously.

---

## ⚙️ Components

### 1️⃣ Loader

#### Responsible for reading raw JSONL files from the `data/` directory.

**Behavior**:

- Iterates across all **data types**
- Randomly mixes records from **new / change / old**
- Emits a unified stream of events

**Purpose**: simulate a real event stream instead of static batch input.

---

### 2️⃣ Preprocessor

#### Enriches and standardizes events before publishing.

**Typical transformations**:

- Add ingestion metadata
- Update processing timestamp (`timestamp`)
- Minor cleaning / validation

---

### 3️⃣ Producer

#### Publishes processed events to Kafka topics.

**Responsibilities**:

- Serialize events (JSON)
- Assign event key (tmdb_id)
- Send to topic based on data type
- Handle delivery callbacks / retries


---

## 🚀 Quick Start

### 1️⃣ Prepare Environment

Update configs in `config/` (Kafka bootstrap servers, topics).

### 2️⃣ Run Pipeline

#### Run the from the root directory:

```bash
export PYTHONPATH=$(pwd)/src
python -m ingestion.main
```

The pipeline will:

1. Load mixed records  
2. Enrich events  
3. Produce messages to Kafka  

---



