# Common

This module provides shared utilities used across all services in the platform.  
It ensures consistent logging behavior and centralized path resolution for data stored on MinIO.

The goal is to avoid duplicated infrastructure code and keep service modules focused on business logic.

---

## 📂 Module Structure

```text
common/
├── logging_config.py     # Global logging setup
├── load_path_config.py   # MinIO path & storage config loader
├── __init__.py
└── README.md
```

---

## ⚙️ Components

### 1️⃣ Logging Configuration

`logging_config.py` defines a unified logging setup for all services.

**Responsibilities**:

- Standard log format across services  
- Centralized log level configuration  
- Console / file handlers (if enabled)  
- Consistent timestamps and service identifiers  

**This ensures logs from**:

- Collector  
- Ingestion  
- Stream Processor  
- Batch Jobs  

have the same structure, making debugging and monitoring easier.

---

### 2️⃣ Path Config Loader

`load_path_config.py` provides helpers to load data paths from module configs.

Responsibilities:

- Read storage paths defined in each module’s `config/`  
- Build full MinIO URIs (bucket + prefix)  
- Provide reusable helpers to access:

  - Bronze paths  
  - Silver paths  
  - Gold paths  
  - DLQ paths  

This abstraction prevents hardcoding storage paths across services.

---
