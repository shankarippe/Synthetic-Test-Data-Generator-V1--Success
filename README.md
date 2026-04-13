# Enterprise Synthetic Data Generation Framework v2

Zero-config, schema-driven synthetic data at any scale.  
Supports **PostgreSQL · Oracle · SQL Server · MySQL** with **T24/Temenos precision**.  
Fully automated via **LangGraph + Groq LLM**.  
Exposed as **FastAPI** for UI integration.

---

## What's New in v2

| Feature | v1 | v2 |
|---|---|---|
| Database support | PostgreSQL only | **Postgres + Oracle + SQL Server + MySQL** |
| Data quality | Random values | **T24-precise banking values** |
| API | CLI only | **FastAPI REST API** |
| Postman collection | ❌ | **✅ included** |
| Docker-ready | ❌ | **✅** |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  FastAPI Server  (api/main.py)                                       │
│  POST /pipeline/run  │  POST /pipeline/run-async  │  GET /jobs/{id} │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │  LangGraph Pipeline          │
                    │  (Intelligence/graph.py)     │
                    └──────────────┬──────────────┘
                                   │
          ┌───────────────┬────────┴──────────┬──────────────┐
          ▼               ▼                   ▼              ▼
    Node 1           Node 2             Node 3          Node 4
  Schema Reader    Domain Detector   Column Intel    Volume Infer
  (Multi-DB)      (T24-aware LLM)   (T24 Library    (LLM ratios)
                                    + LLM augment)
          │               │               │              │
          └───────────────┴───────────────┴──────────────┘
                                   │
          ┌───────────────┬─────────┴─────────┬──────────────┐
          ▼               ▼                   ▼              ▼
    Node 5           Node 6             Node 7
  Scenario Gen     Config Writer    Pipeline Executor
  (LLM)           (YAML files)     (Generate + Load)
                                   ↓
                             Adapter Factory
                    ┌────────────────────────────┐
                    │  Postgres │ Oracle │ MSSQL  │
                    │  MySQL (extensible)         │
                    └────────────────────────────┘
```

---

## Quick Start

### 1. Install

```bash
pip install -r requirements.txt

# For Oracle:    pip install oracledb
# For MSSQL:     pip install pyodbc   (needs ODBC Driver 18)
# For MySQL:     pip install mysql-connector-python
```

### 2. Configure

Edit `config.yaml` — set your engine and connection details:

```yaml
database:
  engine: postgres    # ← change to: oracle | sqlserver | mysql
  host: localhost
  port: 5432
  dbname: DatagenDB
  user: postgres
  password: "secret"
  schema: banking
```

### 3. Run CLI

```bash
# Full automatic run
python auto_pipeline.py

# Dry run (no DB load)
python auto_pipeline.py --dry-run

# Oracle
python auto_pipeline.py --engine oracle --host localhost \
    --service-name ORCL --user system --password secret

# SQL Server  
python auto_pipeline.py --engine sqlserver --host localhost \
    --db DatagenDB --user sa --password secret
```

### 4. Run as API

```bash
python auto_pipeline.py --serve
# → http://localhost:8000/docs
```

---

## FastAPI Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Health check |
| POST | `/api/v1/schema/test-connection` | Test DB connectivity |
| POST | `/api/v1/schema/read` | Read schema (no generation) |
| POST | `/api/v1/pipeline/run` | Run pipeline (synchronous) |
| POST | `/api/v1/pipeline/run-async` | Run pipeline (async, returns job_id) |
| GET | `/api/v1/jobs/{job_id}` | Poll job status |
| GET | `/api/v1/jobs` | List all jobs |
| DELETE | `/api/v1/jobs/{job_id}` | Cancel job |
| GET | `/api/v1/scenarios` | List scenarios |
| POST | `/api/v1/scenarios/run` | Run a scenario |

### Example: Run pipeline via API

```bash
curl -X POST http://localhost:8000/api/v1/pipeline/run-async \
  -H "Content-Type: application/json" \
  -d '{
    "database": {
      "engine": "postgres",
      "host": "localhost",
      "port": 5432,
      "dbname": "DatagenDB",
      "user": "postgres",
      "password": "secret",
      "schema": "banking"
    },
    "groq_api_key": "gsk_xxxx",
    "dry_run": false
  }'

# Response: {"job_id": "abc-123", "status": "queued", ...}

# Poll:
curl http://localhost:8000/api/v1/jobs/abc-123
```

---

## T24 Data Precision

When the LLM detects a **Temenos T24** schema, the framework uses the
**T24DataLibrary** (`core/t24_data_library.py`) for exact field values:

| Column | v1 (random) | v2 (T24-precise) |
|--------|-------------|-----------------|
| `CURRENCY` | `"ABCD"` | `"USD"`, `"EUR"`, `"GBP"` |
| `CUSTOMER_STATUS` | `"QWER"` | `"LIVE"`, `"INACT"`, `"PEND"` |
| `ACCOUNT_OFFICER` | `"ZXC"` | `"100023"` |
| `TRANSACTION_CODE` | `"XYZ"` | `"AC"`, `"DR"`, `"FT"` |
| `COUNTRY` | `"AB"` | `"GB"`, `"US"`, `"AE"` |
| `PRODUCT_LINE` | `"QWE"` | `"DEPOSITS"`, `"LOANS"` |
| `CHANNEL` | `"RTY"` | `"INTERNET"`, `"MOBILE"`, `"ATM"` |
| `INTEREST_KEY` | `"ABC"` | `"LIBOR3M"`, `"SOFR"`, `"EURIBOR3M"` |
| `BIC_CODE` | `"ABCD"` | `"MIDLGB22"`, `"BARCGB22"` |
| `ARRANGEMENT_ID` | `"12345678"` | `"AA240101000042"` |

---

## Database Support Matrix

| Feature | PostgreSQL | Oracle | SQL Server | MySQL |
|---------|-----------|--------|------------|-------|
| Schema read | ✅ | ✅ | ✅ | ✅ |
| Bulk load | ✅ COPY | ✅ executemany | ✅ executemany | ✅ LOAD DATA |
| Index disable | ✅ | ❌ (use hints) | ❌ | ❌ |
| FK detection | ✅ | ✅ | ✅ | ✅ |
| Cycle breaking | ✅ | ✅ | ✅ | ✅ |
| T24 precision | ✅ | ✅ | ✅ | ✅ |

---

## Adding a New Database Engine

1. Create `adapters/your_engine.py` — subclass `BaseDBAdapter`
2. Implement `read_all()`, `bulk_load()`, `test_connection()`
3. Register in `adapters/__init__.py`
4. Done — no other changes needed

```python
from adapters.base import BaseDBAdapter

class MyDBAdapter(BaseDBAdapter):
    def test_connection(self): ...
    def read_all(self): ...
    def bulk_load(self, table, csv, cols): ...
```

---

## Project Structure

```
synthetic_datagen/
│
├── auto_pipeline.py          ← CLI entry point + --serve flag
│
├── api/
│   ├── main.py               ← FastAPI application
│   ├── models.py             ← Pydantic request/response models
│   └── job_store.py          ← In-memory job tracking
│
├── adapters/
│   ├── __init__.py           ← Adapter factory (get_adapter())
│   ├── base.py               ← Abstract BaseDBAdapter
│   ├── postgres.py           ← PostgreSQL
│   ├── oracle.py             ← Oracle DB
│   ├── sqlserver.py          ← SQL Server / SSMS
│   └── mysql.py              ← MySQL / MariaDB
│
├── core/
│   └── t24_data_library.py   ← T24/Temenos-precise values
│
├── Intelligence/
│   ├── nodes.py              ← LangGraph nodes (7 pipeline stages)
│   ├── graph.py              ← LangGraph workflow
│   ├── llm_client.py         ← Groq LLM wrapper
│   └── state.py              ← Pipeline state dataclass
│
├── data_generator.py         ← Row generation engine
├── dependency_graph.py       ← FK-based DAG + topological sort
├── entity_registry.py        ← PK pool for FK sampling
├── file_writer.py            ← CSV streaming
├── auto_ratio_inferrer.py    ← Volume inference heuristics
├── seed_manager.py           ← Reproducible seeding
├── parallel_writer.py        ← Multi-process generation
│
├── config.yaml               ← Connection + generation config
├── requirements.txt
└── SyntheticDataGen_API.postman_collection.json
```

---

## Postman Collection

Import `SyntheticDataGen_API.postman_collection.json` into Postman.

Set these collection variables:
- `base_url`: `http://localhost:8000`
- `groq_api_key`: your Groq API key

Included requests:
- Health check
- Connection test for all 4 DB engines
- Read schema
- Run pipeline (sync + async) for all 4 DB engines
- Poll job status
- List/cancel jobs
- List + run scenarios

---

## Reproducibility

```bash
# Run with named seed profile
python auto_pipeline.py --seed-profile production_v1

# Reproduce exact same data
python auto_pipeline.py --reproduce production_v1

# Via API — pass seed in request body
{"seed": 42, ...}
```

---

## Environment Variables

```bash
# PostgreSQL
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=DatagenDB
export PGUSER=postgres
export PGPASSWORD=secret

# LLM
export GROQ_API_KEY=gsk_xxxx
```