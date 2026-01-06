-- Справочник зон
CREATE TABLE IF NOT EXISTS raw.taxi_zone_lookup (
  locationid    INTEGER PRIMARY KEY,
  borough       TEXT,
  zone          TEXT,
  service_zone  TEXT
);

-- Сырые поездки (Yellow)
CREATE TABLE IF NOT EXISTS raw.yellow_trips (
  batch_id                 TEXT NOT NULL,
  vendorid                 BIGINT,
  tpep_pickup_datetime     TIMESTAMP,
  tpep_dropoff_datetime    TIMESTAMP,
  passenger_count          NUMERIC,
  trip_distance            NUMERIC,
  ratecodeid               BIGINT,
  store_and_fwd_flag       TEXT,
  pulocationid             BIGINT,
  dolocationid             BIGINT,
  payment_type             BIGINT,
  fare_amount              NUMERIC,
  extra                    NUMERIC,
  mta_tax                  NUMERIC,
  tip_amount               NUMERIC,
  tolls_amount             NUMERIC,
  improvement_surcharge    NUMERIC,
  total_amount             NUMERIC,
  congestion_surcharge     NUMERIC,
  airport_fee              NUMERIC,
  cbd_congestion_fee       NUMERIC,
  ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- (опционально) лог ошибок парсинга при ingestion
CREATE TABLE IF NOT EXISTS raw.ingest_errors (
  batch_id      TEXT NOT NULL,
  source_file   TEXT,
  error_reason  TEXT,
  raw_payload   TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_raw_yellow_batch ON raw.yellow_trips(batch_id);
