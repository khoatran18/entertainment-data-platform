# Batch Jobs

This module contains scheduled data pipelines responsible for transforming and moving data across storage and serving layers.

It orchestrates the transition from **Bronze → Silver → Gold** and integrates analytical and serving systems.

---

## 🧱 Processing Overview

There are **4 main tasks** executed in this module:

### 1️⃣ Bronze → Silver (Delta Lake)

- Parse full schema  
- Detect changes  
- Deduplicate records using timestamp  

### 2️⃣ Silver → ClickHouse  

- Transform and load structured data into analytical tables  

### 3️⃣ ClickHouse → Neo4j  

- Build graph relationships for entity connections  

### 4️⃣ ClickHouse → Pinecone  

- Generate and store embeddings for semantic retrieval  

---

## 📂 Module Structure

```text
batch_jobs/
├── config/        # Pipeline & environment configuration
├── dags/          # Airflow DAG definitions
├── io/            # Database & storage readers/writers
├── pipelines/     # Main pipeline entrypoints
├── run_time/      # Runtime helpers & context
├── schema/        # Full bronze-layer schemas
├── script/        # Setup scripts (e.g., create ClickHouse tables)
├── transforms/    # Data transformation logic
├── __init__.py
├── Dockerfile
└── README.md
```

---

## ⚙️ Components

### 1️⃣ IO

Contains abstractions for interacting with external systems.

Examples:

- Delta Lake reader  
- ClickHouse writer  
- Neo4j writer  
- Pinecone client  

This layer isolates storage logic from pipeline logic.

---

### 2️⃣ Schema

Defines the **full schema** of records stored in the Bronze layer.

Unlike the stream processor (which only parses critical fields),  
batch jobs operate on complete records for correctness and consistency.

---

### 3️⃣ Transforms

Responsible for transforming data between source and destination.

Position in flow:

Reader → Transform → Writer

Typical operations:

- Change detection  
- Deduplication  
- Field normalization  
- Aggregation  

---

### 4️⃣ Script

Contains setup utilities required before running pipelines.

Example:

Create ClickHouse tables before loading data.

---

### 5️⃣ Pipelines

Contains the main functions invoked by Airflow.

Responsibilities:

- Load configuration  
- Initialize IO clients  
- Execute transforms  
- Trigger write operations  

Example entrypoints:

- Bronze → Silver  
- Silver → ClickHouse  
- ClickHouse → Neo4j  
- ClickHouse → Pinecone  

---

### 6️⃣ DAGs

Airflow orchestration layer.

These DAGs must be mounted into the Airflow `dags_folder` to be detected and executed.

Two types of DAGs:

1️⃣ Standard DAG → for local or VM deployments  
2️⃣ KubernetesPodOperator DAG → for running jobs on Kubernetes  

---

## 🧠 Execution Flow

Bronze Delta → Dedup & Change Detection → Silver Delta  
→ Transform → ClickHouse  
→ Neo4j + Pinecone

---

## 🚀 Quick Start

For quick testing without Airflow, you can run pipelines manually in order:

```bash
python -m batch_jobs.pipelines.bronze_silver.minio_to_minio
python -m batch_jobs.pipelines.silver_silver.minio_to_clickhouse
python -m batch_jobs.pipelines.silver_gold.clickhouse_to_neo4j
python -m batch_jobs.pipelines.silver_gold.clickhouse_to_pinecone
```

Execution order:

1️⃣ Dedup & change detection  
2️⃣ Load to ClickHouse  
3️⃣ Write to Neo4j  
4️⃣ Write to Pinecone  

---

## 💡 Notes

- Designed for scheduled execution (Airflow)  
- Supports both local and Kubernetes deployments  
- Ensures data consistency across analytical and serving layers  
- Works together with Stream Processor outputs  

---