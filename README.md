# 🚖 NYC Taxi DWH Pipeline (Postgres + dbt + Great Expectations)

> **Goal:** Build an incremental ELT pipeline with a quarantine layer (data quality) and reproducible performance benchmarks on NYC TLC Yellow Taxi data.

This project implements a Data Warehouse solution that ingests raw taxi data, cleans it, isolates bad data using a "quarantine" pattern, and models it for analytics. It also includes a robust benchmarking suite to measure query performance before and after database indexing.

---

## ✨ Key Features

- **ELT Pipeline:** Ingests NYC TLC Yellow Taxi trips (Parquet) and taxi zone lookups (CSV) into **Postgres**.
- **Data Quality:** Implements a **Quarantine Layer** to isolate rows that fail validation rules (instead of silently dropping them).
- **dbt Modeling:** Structured transformation layers: `Staging` → `Clean/Quarantine` → `Marts`.
- **Performance Benchmarking:** repeatable SQL benchmarks (measuring execution time in ms) with generated reports.
- **Deep Dive Analysis:** Captures `EXPLAIN (ANALYZE, BUFFERS)` plans to visualize the impact of indexing strategies.

---

## 🛠 Tech Stack

- **Warehouse:** PostgreSQL 16
- **Orchestration & Tooling:** Python (Ingestion + Benchmarking scripts)
- **Transformation:** dbt (Postgres adapter)
- **Infrastructure:** Docker Compose
- **Quality (Planned):** Great Expectations

---

## 🏗 Data Architecture

The project follows a layered DWH approach:

| Layer | Schema | Description |
| :--- | :--- | :--- |
| **Raw** | `raw.*` | Ingested data as close to the source as possible (plus `batch_id`). |
| **Staging** | `stg.*` | Light renaming, casting, and initial cleanup (Views). |
| **Clean** | `clean.*` | Validated, type-safe rows ready for analytics (Tables/Incremental). |
| **Quarantine** | `quarantine.*` | Rows that failed rules (missing fields, invalid values). Kept for audit. |
| **Marts** | `marts.*` | Business-friendly aggregates (e.g., daily metrics, zone stats). |

---

## 🚀 Quick Start

### 1. Configure Environment
Create the environment file from the template:
```bash
cp .env.example .env
```

### 2. Start Services
Launch the database and application containers:
```bash
docker compose up -d --build
```

### 3. Ingest Data
Ingest TLC data into the RAW layer. Example (January 2024):
```bash
docker compose run --rm pipeline ingest --months 2024-01
```

### 4. Run dbt Models
Transform the data through all layers:
```bash
docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt run --full-refresh"
```

### 5. Access Database
You can access the database via **Adminer** at [http://localhost:8080](http://localhost:8080).

- **System:** PostgreSQL
- **Server:** `postgres`
- **Username:** `nyc`
- **Password:** `nyc` (or see `.env`)
- **Database:** `nyc_taxi`

---

## 📊 Performance Benchmarks

This project includes a CLI to run benchmarks and compare performance **Before** and **After** applying database indexes.

### Workflow: Reproduce Benchmarks

**A) Drop Indexes**
```bash
docker compose exec -T postgres psql -U nyc -d nyc_taxi -f /app/sql/perf/000_drop_indexes.sql
```

**B) Bench (Before)**
```bash
docker compose run --rm pipeline bench --iters 7 --warmup 1 --phase before
```

**C) Create Indexes**
```bash
docker compose exec -T postgres psql -U nyc -d nyc_taxi -f /app/sql/perf/001_create_indexes.sql
```

**D) Bench (After)**
```bash
docker compose run --rm pipeline bench --iters 7 --warmup 1 --phase after
```

*(Optional) Generate comparison report:*
```bash
docker compose run --rm pipeline bench-compare
```

### Latest Results (Median, ms)
*Dataset: 2024-01 | Iterations: 7*

| Query | Description | Before (ms) | After (ms) | Speedup |
| :--- | :--- | :--- | :--- | :--- |
| **q1** | Top pickup zones (single day) | 119.4 | 36.9 | **3.24×** |
| **q2** | Revenue by day | 747.7 | 714.0 | 1.05× |
| **q3** | Join zone lookup (Top 20) | 573.1 | 414.3 | 1.38× |
| **q4** | Payment type stats | 332.4 | 233.9 | 1.42× |
| **q5** | Hourly peak | 808.5 | 645.9 | 1.25× |

### 🔎 Analysis
- **q1 (Range Scan):** High speedup. The index on `pickup_ts` allows Postgres to skip the full table scan and jump directly to the relevant rows.
- **q2/q5 (Aggregations):** Minimal speedup. These are global aggregations that still require scanning a significant portion of the table. To improve these, **Materialized Views** or pre-aggregated **Marts** are recommended over simple indexing.

---

## 📂 Project Structure

```text
├── src/                  # Pipeline CLI source code
│   └── pipeline/         # Ingestion & Benchmark logic (Python)
├── dbt/                  # dbt project (transformations)
│   ├── models/           # stg, clean, quarantine, marts
│   └── ...
├── sql/
│   ├── init/             # DB initialization (schemas/tables)
│   └── perf/             # Performance scripts (drop/create indexes)
├── docs/
│   └── explain/          # EXPLAIN ANALYZE plans (before/after)
└── data/
    └── reports/          # Benchmark outputs (CSV/MD)
```

---

## 📝 Common Commands

| Action | Command |
| :--- | :--- |
| **Start** | `docker compose up -d --build` |
| **Stop** | `docker compose down` |
| **Reset DB** | `docker compose down -v` |
| **Run dbt** | `docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt run"` |
| **Test** | `docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt test"` |

---

## 🔮 Future Work

- [ ] Add **Marts** for daily aggregations (e.g., `marts.daily_revenue`) to significantly speed up analytical queries like q2/q5.
- [ ] Integrate **Great Expectations** suites and host HTML data quality reports.
- [ ] Implement **Partitioning** on `pickup_ts` for better multi-month scaling.
- [ ] Add **CI/CD** (GitHub Actions) to run dbt compile and unit tests on Pull Requests.