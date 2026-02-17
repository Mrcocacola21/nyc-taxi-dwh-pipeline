COMPOSE := docker compose
MONTH ?= 2024-01
BENCH_ITERS ?= 7
BENCH_WARMUP ?= 1
BENCH_RUN_ID ?= local_run

.PHONY: up down reset-db seed-sample ingest-month dbt dbt-full-refresh dbt-test ge bench-before bench-after bench-compare explain verify-clean-batch

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

reset-db:
	$(COMPOSE) down -v

seed-sample:
	$(COMPOSE) exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 -f /app/sql/dev/seed_ci_sample.sql

ingest-month:
	$(COMPOSE) run --rm pipeline ingest --months $(MONTH)

dbt:
	$(COMPOSE) run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt compile && dbt run"

dbt-full-refresh:
	$(COMPOSE) run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt compile && dbt run --full-refresh"

dbt-test:
	$(COMPOSE) run --rm --entrypoint bash pipeline -lc "cd /app/dbt && dbt test"

ge:
	$(COMPOSE) run --rm --entrypoint bash pipeline -lc "python -m src.pipeline.ge_checkpoint"

bench-before:
	$(COMPOSE) exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 -f /app/sql/perf/000_drop_indexes.sql
	$(COMPOSE) run --rm pipeline bench --iters $(BENCH_ITERS) --warmup $(BENCH_WARMUP) --phase before --run-id $(BENCH_RUN_ID) --batches $(MONTH)

bench-after:
	$(COMPOSE) exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 -f /app/sql/perf/001_create_indexes.sql
	$(COMPOSE) run --rm pipeline bench --iters $(BENCH_ITERS) --warmup $(BENCH_WARMUP) --phase after --run-id $(BENCH_RUN_ID) --batches $(MONTH)

bench-compare:
	$(COMPOSE) run --rm pipeline bench-compare --run-id $(BENCH_RUN_ID)

explain:
	powershell -ExecutionPolicy Bypass -File docs\explain\run_explains.ps1

verify-clean-batch:
	$(COMPOSE) exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 -v batch_id=$(MONTH) -f /app/sql/dev/assert_clean_batch_sync.sql
