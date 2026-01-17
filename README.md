# 🚖 NYC Taxi DWH Pipeline — Postgres + dbt + Great Expectations (Quarantine • Benchmarks • EXPLAIN)

> **Goal:** Build a reproducible, incremental ELT pipeline on NYC TLC Yellow Taxi data with a formal data-quality quarantine layer and performance evidence (benchmarks + `EXPLAIN (ANALYZE, BUFFERS)`).

This repository demonstrates an end-to-end “warehouse-style” workflow:
1.  **Ingest** raw TLC data into Postgres (`raw.*`) with `batch_id` lineage.
2.  **Model** data through dbt layers (`stg` → `clean`/`quarantine` → `marts`).
3.  **Validate** data quality via **dbt tests** and **Great Expectations checkpoint** (with HTML Data Docs).
4.  **Measure** performance scientifically via repeated runs, statistical aggregation, and query plan analysis.

---

## 🧐 Problem Statement

Analytical queries on large fact tables (millions of trips) often face three recurring issues:

* **Performance degradation:** queries degrade into full scans and expensive aggregates, causing high I/O and CPU load.
* **Data integrity gaps:** “bad” rows are silently dropped or ignored, skewing metrics and breaking auditability.
* **Low reproducibility:** performance claims are often anecdotal (“feels faster”), not evidence-based.

**This project addresses these issues via:**
* **Layered modeling:** strict separation of concerns (`raw` → `stg` → `clean` → `marts`).
* **Formal quarantine:** rows failing validation rules are preserved in `quarantine.*` for inspection.
* **Empirical evidence:** automated benchmarks + `EXPLAIN ANALYZE` plans captured before/after changes.

---

## 🏗 Architecture & Methodology

### Data Layers

| Layer | Schema | Purpose |
| :--- | :--- | :--- |
| **Raw** | `raw.*` | Source-shaped ingestion + `batch_id` tracking. |
| **Staging** | `stg.*` | Light casting, renaming, and standardization (Views). |
| **Clean** | `clean.*` | Validated, type-safe fact tables ready for analysis. |
| **Quarantine** | `quarantine.*` | Rows rejected by quality rules (missing keys, invalid values), preserved for audit. |
| **Marts** | `marts.*` | Pre-aggregated BI tables optimized for sub-second analytics. |

### Why Marts (The "Thesis")

Indexes help when a query is **selective** (filters can reduce scanned rows). However, many analytical queries are **global aggregations** (group-bys over the whole fact table). In that case, even "index-only" access still needs to read *most rows* to compute aggregates.

**So the plan shifts from:**
1.  **Full-scan aggregation** over millions of rows.
2.  To **tiny table scan** over a pre-aggregated mart (tens/hundreds of rows).

This is why marts yield **1000×+ speedups** on queries like `q2` and `q5`.

### Technology Stack
* **Warehouse:** PostgreSQL 16
* **Transformation:** dbt (Postgres adapter)
* **Orchestration:** Python CLI (ingest + benchmarks + GE checkpoint)
* **Infrastructure:** Docker Compose
* **Quality:** dbt tests + Great Expectations (checkpoint + Data Docs)

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

### 5. Run dbt Tests
```bash
docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt test"
```

### 6. Run Great Expectations Checkpoint
Runs a checkpoint against `clean.clean_yellow_trips`, writes a JSON artifact, and generates HTML Data Docs:
```bash
docker compose run --rm --entrypoint bash pipeline -lc "python -m src.pipeline.ge_checkpoint"
```
* **Output:** Data Docs are available in `docs/ge/data_docs/index.html`.
* *Note:* Some "soft rules" (e.g., `payment_type` set membership) are configured with `mostly < 1.0` to reflect real-world data while keeping the pipeline actionable.

### 7. Access Database
Database management via **Adminer** at [http://localhost:8080](http://localhost:8080).
* **Server:** `postgres` | **User:** `nyc` | **Pass:** `nyc` | **DB:** `nyc_taxi`

---

## ✅ Data Quality Strategy

### dbt Tests
A minimal-but-formal set of schema tests validates key columns (`not_null`, `unique`, etc.) and provides fast feedback during the transformation development cycle.

### Great Expectations Checkpoint
Great Expectations is used to produce:
1.  **Machine-readable validation output:** JSON artifacts in `data/reports/ge/`.
2.  **Human-readable HTML Data Docs:** Located in `docs/ge/data_docs/`.

This establishes auditability and makes data-quality claims reproducible and transparent.

---

## 📊 Benchmarks & Performance Evidence

The project includes a CLI tool that runs queries multiple times, warms up cache, and records execution timings.

### Workflow: Reproducing “Before vs After”

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
* **Indexes** provide dramatic improvements for selective/range queries (q1).
* **Marts** are required for global aggregations (q2, q5), reducing runtime from ~700–800ms to ~0.3–0.4ms.

---

## 🔎 Deep Dive: Query Plan Analysis

We captured `EXPLAIN (ANALYZE, BUFFERS)` plans to understand the mechanics of performance changes. Plans are stored in `docs/explain/`.

### 1. The Impact of Indexing (q1: selective day filter)
**Before (No Index):**
* **Access path:** `Parallel Seq Scan` on `clean_yellow_trips`
* **Buffers:** read=67,363, hit=9,965
* **Execution time:** 1120.8 ms

**After (With `idx_clean_pickup_ts`):**
* **Access path:** `Index Scan` using `idx_clean_pickup_ts` (range condition)
* **Buffers:** read=219, hit=98,640
* **Execution time:** 73.3 ms

> **Conclusion:** The index changes the access path from full scan → range scan, reducing I/O by >99% on the filtered workload.

### 2. The Limits of Indexing (q3: join + global aggregation)
**Observation:**
The query remains `Hash Join` + `Parallel Seq Scan` + aggregation. Even with indexes, the query must touch most rows to compute grouped results, so the plan does not fundamentally change.

> **Conclusion:** For global joins/aggregations, indexes are often insufficient; modeling (marts) matters.

### 3. Why Marts Beat Indexes (q2/q5: the “warehouse” result)
For q2/q5, the dominant cost is aggregation over millions of rows, not row lookup.
* **Clean (Fact Table):** `Seq Scan`/`Index Only Scan` → `HashAggregate` → `Sort` over **2,872,094 rows**.
* **Mart:** `Seq Scan` over a tiny table (e.g., 35 rows for daily revenue).

> **Conclusion:** Marts change the plan from “full-scan aggregation” to “tiny table scan”, which explains the **1000×–2500× speedups**.

---

## 📂 Project Structure

```text
├── src/                  # Pipeline CLI (ingest + benchmarks + GE)
│   └── pipeline/
├── dbt/                  # dbt Project
│   ├── models/           # Layers: stg, clean, quarantine, marts
│   └── ...
├── sql/
│   ├── init/             # Database bootstrapping
│   └── perf/             # SQL scripts for drop/create indexes
├── docs/
│   ├── explain/          # Captured EXPLAIN ANALYZE plans
│   └── ge/data_docs/     # Great Expectations HTML Data Docs (generated)
└── data/
    └── reports/          # Benchmark outputs + GE checkpoint JSON artifacts
```

---

## 📝 Common Commands

| Action | Command |
| :--- | :--- |
| **Start Services** | `docker compose up -d --build` |
| **Stop Services** | `docker compose down` |
| **Reset Database** | `docker compose down -v` |
| **Run dbt** | `docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt run"` |
| **Test (dbt)** | `docker compose run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt test"` |
| **Run Benchmarks** | `docker compose run --rm pipeline bench --iters 7 --warmup 1 --phase after` |
| **GE Checkpoint** | `docker compose run --rm --entrypoint bash pipeline -lc "python -m src.pipeline.ge_checkpoint"` |

---

## 🔬 Reproducibility Notes
* **Dataset:** NYC TLC Yellow Taxi (2024-01).
* **Benchmark Settings:** 7 iterations, 1 warmup, cache warmed.
* **Planner Settings:** JIT disabled for lower noise (where applicable).
* **Caveat:** Absolute times vary by machine, but relative order-of-magnitude differences (scan vs index vs marts) remain consistent.

---

## 🔮 Future Work
* [ ] Tighten and version the GE suites (separate “critical” vs “warning” domains per column).
* [ ] Add **incremental marts** for multi-month scaling (rolling windows, partition-aware builds).
* [ ] Implement **partitioning** on `pickup_ts` for warehouse-scale optimization.
* [ ] **CI/CD** (GitHub Actions): Run dbt compile/tests + optional GE checkpoint on PRs.
* [ ] Extend quality rules for domain-specific anomalies (payment_type=0, negative totals, timestamp edge cases) with documented rationale.