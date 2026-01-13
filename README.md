# 🚖 NYC Taxi DWH Pipeline — Postgres + dbt (+ Quarantine, Benchmarks, EXPLAIN)

> **Goal:** Build a reproducible, incremental ELT pipeline on NYC TLC Yellow Taxi data with a formal data-quality quarantine layer and performance evidence (benchmarks + `EXPLAIN (ANALYZE, BUFFERS)`).

This repository demonstrates an end-to-end “warehouse-style” workflow:
1.  **Ingest** raw TLC data into Postgres.
2.  **Model** data through dbt layers (`staging` → `clean`/`quarantine` → `marts`).
3.  **Validate** quality through dbt tests and a dedicated **Quarantine** pattern.
4.  **Measure** performance scientifically via repeated runs, statistical aggregation, and query plan analysis.

---

## 🧐 Problem Statement

Analytical queries on large fact tables (millions of trips) often encounter significant bottlenecks and reliability issues:

* **Performance Degradation:** Queries degrade into full table scans and expensive aggregations, causing high I/O and CPU load.
* **Data Integrity:** "Bad" rows are often silently dropped during ingestion, leading to skewed metrics and loss of auditability.
* **Lack of Reproducibility:** Performance claims are often anecdotal ("it feels faster") rather than evidence-based.

**This project addresses these issues via:**
* **Layered Modeling:** Strict separation of concerns (`raw` → `stg` → `clean` → `marts`).
* **Formal Quarantine:** Rows failing validation rules are preserved in `quarantine.*` tables for inspection.
* **Empirical Evidence:** Automated benchmarking suites and `EXPLAIN ANALYZE` comparisons.

---

## 🏗 Architecture & Methodology

### Data Layers
The project follows a standard dimensional modeling approach adapted for ELT:

| Layer | Schema | Purpose |
| :--- | :--- | :--- |
| **Raw** | `raw.*` | Source-shaped ingestion + `batch_id` tracking. |
| **Staging** | `stg.*` | Light casting, renaming, and standardization (Views). |
| **Clean** | `clean.*` | Validated, type-safe fact tables ready for analysis. |
| **Quarantine** | `quarantine.*` | Rows rejected by quality rules (missing keys, invalid values), preserved for audit. |
| **Marts** | `marts.*` | Pre-aggregated BI tables optimized for sub-second analytics. |

### Technology Stack
* **Warehouse:** PostgreSQL 16
* **Transformation:** dbt (Postgres adapter)
* **Orchestration:** Python CLI (Ingestion + Benchmarking)
* **Infrastructure:** Docker Compose
* **Quality:** dbt tests (Great Expectations planned)

---

## 🚀 Quick Start (Reproducible)

### 1. Configure Environment
```bash
cp .env.example .env
```

### 2. Start Services
```bash
docker compose up -d --build
```

### 3. Ingest Data (Raw Layer)
*Example: Ingest January 2024 data.*
```bash
docker compose run --rm pipeline ingest --months 2024-01
```

### 4. Run Transformations (dbt)
Build all layers from Staging to Marts:
```bash
docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt run --full-refresh"
```

### 5. Run Quality Tests
```bash
docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt test"
```

### 6. Access Database
Database management via **Adminer** at [http://localhost:8080](http://localhost:8080).
* **Server:** `postgres` | **User:** `nyc` | **Pass:** `nyc` | **DB:** `nyc_taxi`

---

## 📊 Benchmarks & Performance Evidence

The project includes a CLI tool to run queries multiple times, warm up the cache, and record execution timings.

### Workflow: Reproducing "Before vs After"
1.  **Drop Indexes:** `docker compose exec -T postgres psql -U nyc -d nyc_taxi -f /app/sql/perf/000_drop_indexes.sql`
2.  **Bench (Before):** `docker compose run --rm pipeline bench --iters 7 --warmup 1 --phase before`
3.  **Create Indexes:** `docker compose exec -T postgres psql -U nyc -d nyc_taxi -f /app/sql/perf/001_create_indexes.sql`
4.  **Bench (After):** `docker compose run --rm pipeline bench --iters 7 --warmup 1 --phase after`

### ✅ Latest Benchmark Snapshot
*Configuration: Dataset 2024-01 | Iterations: 7 | Warmup: 1*

| Query | Context | Median (ms) | Min (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- |
| **q1** (Top Zones) | **Indexed Fact Table** | **36.77** | 36.46 | 41.67 |
| **q2** (Daily Rev) | Indexed Fact Table | 774.85 | 766.26 | 784.58 |
| **q2** (Daily Rev) | **Pre-aggregated Mart** | **0.35** | 0.18 | 0.44 |
| **q3** (Join Lookup) | Indexed Fact Table | 457.70 | 421.94 | 476.71 |
| **q4** (Payment Stats)| Indexed Fact Table | 275.31 | 249.89 | 311.03 |
| **q5** (Hourly Peak) | Indexed Fact Table | 698.10 | 691.92 | 711.55 |
| **q5** (Hourly Peak) | **Pre-aggregated Mart** | **0.38** | 0.27 | 0.52 |

**Interpretation:**
* **Indexes** provide drastic improvements for selective queries (q1).
* **Marts** are required for global aggregations (q2, q5), reducing runtime from ~700ms to ~0.3ms (orders of magnitude faster).

---

## 🔎 Deep Dive: Query Plan Analysis

We captured `EXPLAIN (ANALYZE, BUFFERS)` plans to understand the mechanics of performance changes.

### 1. The Impact of Indexing (Query q1)
*Query: Top pickup zones for a single day (highly selective).*

**Before (No Index):**
* **Access Path:** `Parallel Seq Scan` on `clean_yellow_trips`
* **Buffers:** read=67,363 (~526 MB), hit=9,965
* **Execution Time:** 1120.8 ms

**After (With `idx_clean_pickup_ts`):**
* **Access Path:** `Index Scan` using `idx_clean_pickup_ts`
* **Buffers:** read=219 (~1.7 MB), hit=98,640
* **Execution Time:** 73.3 ms

> **Conclusion:** The index changed the access path from a full table scan to a range scan, reducing I/O by over 99%.

### 2. The Limits of Indexing (Query q3)
*Query: Join with taxi_zone_lookup + Top 20 zones (Global Aggregation).*

**Observation:**
The plan remains a `Hash Join` + `Parallel Seq Scan`. Indexes do not materially change the plan because the query must visit almost every row to perform the aggregation.

> **Conclusion:** For "global" joins and aggregations, indexes are insufficient. Performance improvements must come from data modeling (Marts).

### 3. Why Marts Beat Indexes (The Thesis)
For queries like **q2** (Daily Revenue) and **q5** (Hourly Peak), the cost is not finding rows, but aggregating them.

* **Fact Table (Clean):** Requires `Seq Scan` → `HashAggregate` → `Sort` across **2,872,094 rows**. Even with an Index Only Scan, the aggregation cost remains high (~700-900ms).
* **Mart:** The transformation logic moves the "heavy lifting" to the load phase. The query becomes a `Seq Scan` over a tiny table (e.g., 35 rows for daily revenue), resulting in **~0.3ms** execution time.

---

## 📂 Project Structure

```text
├── src/                  # Pipeline CLI (Ingest + Benchmarks logic)
│   └── pipeline/
├── dbt/                  # dbt Project
│   ├── models/           # Layers: stg, clean, quarantine, marts
│   └── ...
├── sql/
│   ├── init/             # Database bootstrapping
│   └── perf/             # SQL scripts for drop/create indexes
├── docs/
│   └── explain/          # Captured EXPLAIN ANALYZE plans
└── data/
    └── reports/          # Benchmark outputs (CSV/MD)
```

---

## 📝 Common Commands

| Action | Command |
| :--- | :--- |
| **Start Services** | `docker compose up -d --build` |
| **Stop Services** | `docker compose down` |
| **Reset Database** | `docker compose down -v` |
| **Run dbt** | `docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt run"` |
| **Test Data** | `docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt test"` |

---

## 🔬 Reproducibility Note
* **Dataset:** NYC TLC Yellow Taxi (2024-01).
* **Settings:** 7 Iterations, 1 Warmup run, JIT compilation disabled.
* **Hardware:** Results will vary by machine, but the *order of magnitude* differences between Scan vs. Index vs. Marts should remain consistent.

---

### 🔮 Future Work
* [ ] Integration of **Great Expectations** for HTML Data Docs.
* [ ] Implementation of **Incremental Marts** for multi-month scaling.
* [ ] **Table Partitioning** on `pickup_ts` for warehouse-scale optimization.
* [ ] CI/CD Pipeline (GitHub Actions) for automated testing.