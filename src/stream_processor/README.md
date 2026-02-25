# Stream Processing

This module consumes streaming events from Kafka, processes them, and writes structured data into Delta tables stored on MinIO.

A key design principle is **fault tolerance**: records that fail parsing will NOT stop the pipeline.  
Instead, they are redirected to a **Dead Letter Queue (DLQ)** stored in Delta on MinIO, ensuring no data loss.

---

## 🧱 High-Level Flow

Kafka → Source → Processor → Delta Sink (MinIO)

If parsing fails → DLQ Delta Table (MinIO)

---

## 📂 Module Structure

```text
stream_processor/
├── config/        # Runtime & pipeline configuration
├── processor/     # Core transformation logic
├── runtime/       # Client wrappers (MinIO, configs, shared context)
├── schema/        # Lightweight schemas for parsing
├── sinks/         # Output writers (Delta on MinIO)
├── sources/       # Kafka ingestion logic
├── __init__.py
├── main.py        # Entry point
├── Dockerfile
└── README.md
```

---

## ⚙️ Core Concepts

### ✅ Delta Lake on MinIO

Processed records are stored as Delta tables on MinIO, enabling:

- ACID transactions  
- Schema evolution  
- Efficient batch & analytics queries  

---

### ⚠️ Dead Letter Queue (DLQ)

Streaming systems must never stop because of bad records.

If an event:

- Fails schema parsing  
- Has missing required fields  
- Contains corrupted JSON  

➡️ It will be written to a **DLQ Delta table** on MinIO instead of breaking the pipeline.

This guarantees:

- No data loss  
- Easier debugging & replay  
- Stable long-running streaming jobs  

---

## 🧩 Components

### 1️⃣ Sources

Handles Kafka consumption.

Responsibilities:

- Subscribe to configured topics  
- Deserialize messages  
- Convert Kafka records → internal event format  

---

### 2️⃣ Schema

Defines lightweight schemas used to parse incoming events.

Design choice:

- Only critical fields are parsed  
- Avoid strict full-schema enforcement  
- Improves performance and resilience  

This is important because streaming payloads may evolve over time.

---

### 3️⃣ Processor

Transforms events before writing.

Typical tasks:

- Apply schema parsing  
- Normalize fields  
- Add ingestion metadata  
- Route invalid events → DLQ  

---

### 4️⃣ Runtime

Provides shared infrastructure clients.

Examples:

- MinIO client wrapper  
- Config loader  
- Shared Spark / streaming context (if applicable)  

This layer isolates external dependencies from business logic.

---

### 5️⃣ Sinks

Responsible for writing output data.

Outputs:

- ✅ Valid records → Delta tables (MinIO)  
- ⚠️ Invalid records → DLQ Delta table (MinIO)  

Handles:

- Partitioning  
- Table creation  
- Upserts / append logic  

---

## 🧠 Processing Flow

1. Consume event from Kafka  
2. Parse using lightweight schema  
3. If valid → transform & write to Delta  
4. If invalid → write to DLQ  

---

## 🚀 Quick Start

### 1️⃣ Configure Environment

#### Update configs in `config/`

### 2️⃣ Run Stream Job

```bash
export PYTHONPATH=$(pwd)/src
python -m stream_processor.main
```

---


