import os
import requests
import pandas as pd
import pyarrow.parquet as pq
import psycopg

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"
TRIP_PATH = "/trip-data"
MISC_PATH = "/misc"

def _pg_conn():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "nyc_taxi"),
        user=os.getenv("POSTGRES_USER", "nyc"),
        password=os.getenv("POSTGRES_PASSWORD", "nyc"),
        autocommit=False,   # <-- важно
    )


def _download(url: str, dest: str):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

def _load_zone_lookup(cur, csv_path: str):
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]
    # expected: LocationID, Borough, Zone, service_zone
    df.rename(columns={"locationid": "locationid"}, inplace=True)

    cur.execute("TRUNCATE raw.taxi_zone_lookup;")
    for row in df.itertuples(index=False):
        cur.execute(
            """
            INSERT INTO raw.taxi_zone_lookup(locationid, borough, zone, service_zone)
            VALUES (%s, %s, %s, %s)
            """,
            (int(row.locationid), str(row.borough), str(row.zone), str(row.service_zone)),
        )

import io
import pandas as pd
import pyarrow.parquet as pq

def _load_yellow_parquet(cur, parquet_path: str, batch_id: str, batch_rows: int = 200_000):
    # Какие колонки хотим положить в RAW (в нужном порядке)
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
    parquet_cols = set(pf.schema.names)

    # batch_id в parquet нет; читаем только то, что есть
    read_cols = [c for c in cols if c != "batch_id" and c in parquet_cols]

    copy_sql = (
        f"COPY raw.yellow_trips ({','.join(cols)}) "
        "FROM STDIN WITH (FORMAT csv, NULL '', HEADER false)"
    )

    total = 0
    with cur.copy(copy_sql) as copy:
        for batch in pf.iter_batches(batch_size=batch_rows, columns=read_cols):
            df = batch.to_pandas()
            df.columns = [c.strip().lower() for c in df.columns]

            # Добавляем batch_id
            df["batch_id"] = batch_id

            # Если каких-то колонок нет в конкретном месяце — добавим пустыми
            for c in cols:
                if c not in df.columns:
                    df[c] = pd.NA

            # Приводим порядок колонок
            df = df[cols]

            # CSV в память и отправляем в COPY
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False, lineterminator="\n")
            copy.write(buf.getvalue().encode("utf-8"))

            total += len(df)
            if total % 500_000 == 0:
                print(f"[ingest] inserted ~{total:,} rows...")

    print(f"[ingest] inserted total {total:,} rows for batch {batch_id}")


def ingest_all(months: list[str], dataset: str = "yellow"):
    data_dir = os.path.join("data", "raw")
    zone_url = f"{TLC_BASE}{MISC_PATH}/taxi_zone_lookup.csv"
    zone_path = os.path.join(data_dir, "taxi_zone_lookup.csv")

    print(f"[ingest] downloading zones: {zone_url}")
    _download(zone_url, zone_path)

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            print("[ingest] loading taxi_zone_lookup -> raw.taxi_zone_lookup")
            _load_zone_lookup(cur, zone_path)
            conn.commit()  # ✅ фиксируем lookup

            for month in months:
                fname = f"yellow_tripdata_{month}.parquet"
                url = f"{TLC_BASE}{TRIP_PATH}/{fname}"
                dest = os.path.join(data_dir, fname)
                print(f"[ingest] downloading {url}")
                _download(url, dest)

                print(f"[ingest] loading {fname} -> raw.yellow_trips (batch_id={month})")
                try:
                    _load_yellow_parquet(cur, dest, batch_id=month)
                    conn.commit()  # ✅ фиксируем батч
                except Exception:
                    conn.rollback()  # ✅ откат батча, чтобы база не зависла
                    raise

    print("[ingest] done")
