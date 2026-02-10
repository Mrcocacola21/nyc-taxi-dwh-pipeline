from __future__ import annotations

import io
import os

import pandas as pd
import psycopg
import pyarrow.parquet as pq
import requests

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"
TRIP_PATH = "/trip-data"
MISC_PATH = "/misc"


def _pg_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "nyc_taxi"),
        user=os.getenv("POSTGRES_USER", "nyc"),
        password=os.getenv("POSTGRES_PASSWORD", "nyc"),
        autocommit=False,  # важно: управляем commit/rollback вручную
    )


def _download(url: str, dest: str) -> None:
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def _load_zone_lookup(cur: psycopg.Cursor, csv_path: str) -> None:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]

    # expected columns after lower(): locationid, borough, zone, service_zone
    required = {"locationid", "borough", "zone", "service_zone"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"taxi_zone_lookup.csv missing columns: {sorted(missing)}")

    # lookup маленький: проще и надёжнее полностью обновлять
    cur.execute("TRUNCATE raw.taxi_zone_lookup;")
    for row in df.itertuples(index=False):
        cur.execute(
            """
            INSERT INTO raw.taxi_zone_lookup(locationid, borough, zone, service_zone)
            VALUES (%s, %s, %s, %s)
            """,
            (int(row.locationid), str(row.borough), str(row.zone), str(row.service_zone)),
        )


def _table_exists(cur: psycopg.Cursor, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """,
        (schema, table),
    )
    return bool(cur.fetchone()[0])


def _batch_exists_in_raw(cur: psycopg.Cursor, batch_id: str) -> bool:
    # Быстрый EXISTS без COUNT(*)
    cur.execute(
        "SELECT 1 FROM raw.yellow_trips WHERE batch_id = %s LIMIT 1;",
        (batch_id,),
    )
    return cur.fetchone() is not None


def _delete_raw_batch(cur: psycopg.Cursor, batch_id: str) -> int:
    cur.execute("DELETE FROM raw.yellow_trips WHERE batch_id = %s;", (batch_id,))
    # rowcount для psycopg обычно доступен
    return int(cur.rowcount or 0)


def _load_yellow_parquet(
    cur: psycopg.Cursor,
    parquet_path: str,
    batch_id: str,
    batch_rows: int = 200_000,
) -> None:
    cols = [
        "batch_id",
        "vendorid",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "store_and_fwd_flag",
        "pulocationid",
        "dolocationid",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    ]

    pf = pq.ParquetFile(parquet_path)

    # case-insensitive map: lower -> real parquet name
    name_map = {n.lower(): n for n in pf.schema.names}

    read_cols: list[str] = []
    for c in cols:
        if c == "batch_id":
            continue
        real_name = name_map.get(c)
        if real_name is not None:
            read_cols.append(real_name)

    copy_sql = (
        f"COPY raw.yellow_trips ({','.join(cols)}) "
        "FROM STDIN WITH (FORMAT csv, NULL '', HEADER false)"
    )

    INT_COLS = ["vendorid", "ratecodeid", "pulocationid", "dolocationid", "payment_type"]

    total = 0
    with cur.copy(copy_sql) as copy:
        for batch in pf.iter_batches(batch_size=batch_rows, columns=read_cols):
            df = batch.to_pandas()
            df.columns = [c.strip().lower() for c in df.columns]

            # int-колонки -> nullable Int64 (иначе будут "1.0")
            for c in INT_COLS:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

            # batch_id
            df["batch_id"] = batch_id

            # если каких-то колонок нет в месяце — добавим пустыми
            for c in cols:
                if c not in df.columns:
                    df[c] = pd.NA

            df = df[cols]

            buf = io.StringIO()
            # na_rep='' чтобы NULL '' корректно работал в COPY
            df.to_csv(buf, index=False, header=False, lineterminator="\n", na_rep="")
            copy.write(buf.getvalue().encode("utf-8"))

            total += len(df)
            if total % 500_000 == 0:
                print(f"[ingest] inserted ~{total:,} rows...")

    print(f"[ingest] inserted total {total:,} rows for batch {batch_id}")


def ingest_all(
    months: list[str],
    dataset: str = "yellow",
    replace_batch: bool = False,
) -> None:
    """
    Idempotent-ish ingest:
      - If a batch_id already exists in raw.yellow_trips:
          * replace_batch=False -> SKIP that month
          * replace_batch=True  -> DELETE old rows for that batch_id and re-ingest
    """
    if dataset != "yellow":
        raise ValueError("Only dataset='yellow' is supported right now.")

    data_dir = os.path.join("data", "raw")
    zone_url = f"{TLC_BASE}{MISC_PATH}/taxi_zone_lookup.csv"
    zone_path = os.path.join(data_dir, "taxi_zone_lookup.csv")

    print(f"[ingest] downloading zones: {zone_url}")
    _download(zone_url, zone_path)

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            # sanity: tables exist
            if not _table_exists(cur, "raw", "taxi_zone_lookup"):
                raise RuntimeError("Table raw.taxi_zone_lookup not found. Did you run DB init?")
            if not _table_exists(cur, "raw", "yellow_trips"):
                raise RuntimeError("Table raw.yellow_trips not found. Did you run DB init?")

            print("[ingest] loading taxi_zone_lookup -> raw.taxi_zone_lookup")
            _load_zone_lookup(cur, zone_path)
            conn.commit()  # фиксируем lookup

            for month in months:
                month = month.strip()
                if not month:
                    continue

                # ---- idempotency gate ----
                try:
                    exists = _batch_exists_in_raw(cur, month)
                except Exception:
                    # если что-то пошло не так с проверкой — лучше упасть явно
                    raise

                if exists and not replace_batch:
                    print(f"[ingest] batch_id={month} already present in raw.yellow_trips -> SKIP (use --replace-batch to re-ingest)")
                    continue

                if exists and replace_batch:
                    print(f"[ingest] batch_id={month} already present -> deleting old rows (raw.yellow_trips) ...")
                    try:
                        deleted = _delete_raw_batch(cur, month)
                        conn.commit()
                        print(f"[ingest] deleted {deleted:,} rows for batch {month}")
                    except Exception:
                        conn.rollback()
                        raise

                fname = f"yellow_tripdata_{month}.parquet"
                url = f"{TLC_BASE}{TRIP_PATH}/{fname}"
                dest = os.path.join(data_dir, fname)

                print(f"[ingest] downloading {url}")
                _download(url, dest)

                print(f"[ingest] loading {fname} -> raw.yellow_trips (batch_id={month})")
                try:
                    _load_yellow_parquet(cur, dest, batch_id=month)
                    conn.commit()  # фиксируем батч
                except Exception:
                    conn.rollback()  # откат батча, чтобы база не зависла
                    raise

    print("[ingest] done")
