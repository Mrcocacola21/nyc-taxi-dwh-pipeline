-- Ensure clean.clean_yellow_trips uses monthly range partitions on pickup_ts.
-- Safe to run repeatedly.

CREATE OR REPLACE FUNCTION clean.ensure_default_partition(
  p_parent regclass,
  p_partition_name text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_schema_name text;
  v_parent_name text;
  v_partition_name text;
BEGIN
  SELECT n.nspname, c.relname
  INTO v_schema_name, v_parent_name
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.oid = p_parent;

  IF v_schema_name IS NULL THEN
    RAISE EXCEPTION 'Parent relation % does not exist', p_parent;
  END IF;

  v_partition_name := NULLIF(btrim(p_partition_name), '');
  IF v_partition_name IS NULL THEN
    v_partition_name := v_parent_name || '_default';
  END IF;

  EXECUTE format(
    'create table if not exists %I.%I partition of %s default',
    v_schema_name,
    v_partition_name,
    p_parent::text
  );
END;
$$;


CREATE OR REPLACE FUNCTION clean.ensure_monthly_range_partitions(
  p_parent regclass,
  p_partition_prefix text,
  p_min_pickup_ts timestamp without time zone,
  p_max_pickup_ts timestamp without time zone
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_schema_name text;
  v_parent_name text;
  v_prefix text;
  v_month_start timestamp without time zone;
  v_month_stop timestamp without time zone;
  v_partition_name text;
  v_partition_regclass regclass;
  v_default_schema_name text;
  v_default_partition_name text;
  v_temp_table_name text;
BEGIN
  IF p_min_pickup_ts IS NULL OR p_max_pickup_ts IS NULL THEN
    RETURN;
  END IF;

  SELECT n.nspname, c.relname
  INTO v_schema_name, v_parent_name
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.oid = p_parent;

  IF v_schema_name IS NULL THEN
    RAISE EXCEPTION 'Parent relation % does not exist', p_parent;
  END IF;

  v_prefix := NULLIF(btrim(p_partition_prefix), '');
  IF v_prefix IS NULL THEN
    v_prefix := v_parent_name;
  END IF;

  v_month_start := date_trunc('month', p_min_pickup_ts);
  v_month_stop := date_trunc('month', p_max_pickup_ts) + interval '1 month';

  SELECT n.nspname, c.relname
  INTO v_default_schema_name, v_default_partition_name
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE i.inhparent = p_parent
    AND pg_get_expr(c.relpartbound, c.oid, true) = 'DEFAULT'
  LIMIT 1;

  WHILE v_month_start < v_month_stop LOOP
    v_partition_name := format('%s_%s', v_prefix, to_char(v_month_start, 'YYYYMM'));
    SELECT to_regclass(format('%I.%I', v_schema_name, v_partition_name))
    INTO v_partition_regclass;

    IF v_partition_regclass IS NOT NULL THEN
      v_month_start := v_month_start + interval '1 month';
      CONTINUE;
    END IF;

    v_temp_table_name := NULL;
    IF v_default_partition_name IS NOT NULL THEN
      v_temp_table_name := format(
        'tmp_rehome_%s_%s',
        to_char(clock_timestamp(), 'YYYYMMDDHH24MISSMS'),
        floor(random() * 1000000)::int
      );
      EXECUTE format(
        'create temporary table %I on commit drop as ' ||
        'select * from %I.%I where pickup_ts >= %L and pickup_ts < %L',
        v_temp_table_name,
        v_default_schema_name,
        v_default_partition_name,
        v_month_start,
        v_month_start + interval '1 month'
      );
      EXECUTE format(
        'delete from %I.%I where pickup_ts >= %L and pickup_ts < %L',
        v_default_schema_name,
        v_default_partition_name,
        v_month_start,
        v_month_start + interval '1 month'
      );
    END IF;

    EXECUTE format(
      'create table if not exists %I.%I partition of %s for values from (%L) to (%L)',
      v_schema_name,
      v_partition_name,
      p_parent::text,
      v_month_start,
      v_month_start + interval '1 month'
    );

    IF v_temp_table_name IS NOT NULL THEN
      EXECUTE format('insert into %s select * from %I', p_parent::text, v_temp_table_name);
      EXECUTE format('drop table if exists %I', v_temp_table_name);
    END IF;
    v_month_start := v_month_start + interval '1 month';
  END LOOP;
END;
$$;


CREATE OR REPLACE FUNCTION clean.ensure_clean_yellow_trips_month_partitions(
  p_min_pickup_ts timestamp without time zone,
  p_max_pickup_ts timestamp without time zone
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM clean.ensure_default_partition(
    'clean.clean_yellow_trips'::regclass,
    'clean_yellow_trips_default'
  );

  IF p_min_pickup_ts IS NULL OR p_max_pickup_ts IS NULL THEN
    RETURN;
  END IF;

  PERFORM clean.ensure_monthly_range_partitions(
    'clean.clean_yellow_trips'::regclass,
    'clean_yellow_trips',
    p_min_pickup_ts,
    p_max_pickup_ts + interval '1 month'
  );
END;
$$;


CREATE OR REPLACE FUNCTION clean.ensure_clean_yellow_trips_partitioned()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_relkind "char";
  v_min_batch_month timestamp without time zone;
  v_max_batch_month timestamp without time zone;
BEGIN
  SELECT c.relkind
  INTO v_relkind
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'clean'
    AND c.relname = 'clean_yellow_trips';

  IF v_relkind IS NULL THEN
    EXECUTE $ddl$
      create table clean.clean_yellow_trips (
        batch_id text,
        vendor_id bigint,
        pickup_ts timestamp without time zone not null,
        dropoff_ts timestamp without time zone,
        passenger_count numeric,
        trip_distance numeric,
        rate_code_id bigint,
        store_and_fwd_flag text,
        pu_location_id bigint,
        do_location_id bigint,
        payment_type bigint,
        fare_amount numeric,
        tip_amount numeric,
        total_amount numeric,
        congestion_surcharge numeric,
        airport_fee numeric,
        cbd_congestion_fee numeric,
        ingested_at timestamp with time zone,
        trip_duration_sec numeric,
        avg_speed_kmh numeric,
        row_fingerprint text
      ) partition by range (pickup_ts)
    $ddl$;
    PERFORM clean.ensure_default_partition(
      'clean.clean_yellow_trips'::regclass,
      'clean_yellow_trips_default'
    );
    RETURN;
  END IF;

  IF v_relkind = 'p' THEN
    PERFORM clean.ensure_default_partition(
      'clean.clean_yellow_trips'::regclass,
      'clean_yellow_trips_default'
    );
    RETURN;
  END IF;

  IF v_relkind <> 'r' THEN
    RAISE EXCEPTION 'clean.clean_yellow_trips exists with unsupported relkind=%', v_relkind;
  END IF;

  EXECUTE 'lock table clean.clean_yellow_trips in access exclusive mode';

  -- Re-check after lock in case another session already migrated.
  SELECT c.relkind
  INTO v_relkind
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'clean'
    AND c.relname = 'clean_yellow_trips';

  IF v_relkind = 'p' THEN
    PERFORM clean.ensure_default_partition(
      'clean.clean_yellow_trips'::regclass,
      'clean_yellow_trips_default'
    );
    RETURN;
  END IF;

  EXECUTE 'drop table if exists clean.clean_yellow_trips__p_new cascade';
  EXECUTE $ddl$
    create table clean.clean_yellow_trips__p_new (
      batch_id text,
      vendor_id bigint,
      pickup_ts timestamp without time zone not null,
      dropoff_ts timestamp without time zone,
      passenger_count numeric,
      trip_distance numeric,
      rate_code_id bigint,
      store_and_fwd_flag text,
      pu_location_id bigint,
      do_location_id bigint,
      payment_type bigint,
      fare_amount numeric,
      tip_amount numeric,
      total_amount numeric,
      congestion_surcharge numeric,
      airport_fee numeric,
      cbd_congestion_fee numeric,
      ingested_at timestamp with time zone,
      trip_duration_sec numeric,
      avg_speed_kmh numeric,
      row_fingerprint text
    ) partition by range (pickup_ts)
  $ddl$;

  PERFORM clean.ensure_default_partition(
    'clean.clean_yellow_trips__p_new'::regclass,
    'clean_yellow_trips_default'
  );

  EXECUTE $sql$
    select
      min(to_date(batch_id || '-01', 'YYYY-MM-DD'))::timestamp,
      max(to_date(batch_id || '-01', 'YYYY-MM-DD'))::timestamp
    from clean.clean_yellow_trips
    where batch_id ~ '^[0-9]{4}-[0-9]{2}$'
  $sql$
  INTO v_min_batch_month, v_max_batch_month;

  PERFORM clean.ensure_monthly_range_partitions(
    'clean.clean_yellow_trips__p_new'::regclass,
    'clean_yellow_trips',
    v_min_batch_month,
    v_max_batch_month + interval '1 month'
  );

  EXECUTE 'insert into clean.clean_yellow_trips__p_new select * from clean.clean_yellow_trips';
  EXECUTE 'alter table clean.clean_yellow_trips rename to clean_yellow_trips__heap_backup';
  EXECUTE 'alter table clean.clean_yellow_trips__p_new rename to clean_yellow_trips';
  EXECUTE 'drop table clean.clean_yellow_trips__heap_backup';
END;
$$;


-- Fresh databases should start with a partitioned parent table.
SELECT clean.ensure_clean_yellow_trips_partitioned();
