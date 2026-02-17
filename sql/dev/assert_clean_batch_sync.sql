\set ON_ERROR_STOP on

\if :{?batch_id}
\else
  \echo 'ERROR: missing required variable batch_id (example: -v batch_id=2024-01)'
  \quit 2
\endif

DO $$
DECLARE
  v_batch text := :'batch_id';
  v_expected_count bigint := 0;
  v_actual_count bigint := 0;
  v_missing_count bigint := 0;
  v_extra_count bigint := 0;
BEGIN
  WITH expected AS (
    SELECT
      md5(
        coalesce(vendor_id::text, '') || '|' ||
        coalesce(pickup_ts::text, '') || '|' ||
        coalesce(dropoff_ts::text, '') || '|' ||
        coalesce(pu_location_id::text, '') || '|' ||
        coalesce(do_location_id::text, '') || '|' ||
        coalesce(trip_distance::text, '') || '|' ||
        coalesce(total_amount::text, '')
      ) AS row_fingerprint
    FROM stg.stg_yellow_trips
    WHERE batch_id = v_batch
      AND pickup_ts IS NOT NULL
      AND dropoff_ts IS NOT NULL
      AND dropoff_ts >= pickup_ts
      AND pu_location_id IS NOT NULL
      AND do_location_id IS NOT NULL
      AND trip_distance IS NOT NULL AND trip_distance > 0
      AND total_amount IS NOT NULL AND total_amount >= 0
      AND (payment_type IS NULL OR payment_type IN (0,1,2,3,4,5,6))
      AND (rate_code_id IS NULL OR rate_code_id IN (1,2,3,4,5,6,99))
  ),
  actual AS (
    SELECT row_fingerprint
    FROM clean.clean_yellow_trips
    WHERE batch_id = v_batch
  ),
  missing AS (
    SELECT row_fingerprint FROM expected
    EXCEPT ALL
    SELECT row_fingerprint FROM actual
  ),
  extra AS (
    SELECT row_fingerprint FROM actual
    EXCEPT ALL
    SELECT row_fingerprint FROM expected
  )
  SELECT
    (SELECT count(*) FROM expected),
    (SELECT count(*) FROM actual),
    (SELECT count(*) FROM missing),
    (SELECT count(*) FROM extra)
  INTO v_expected_count, v_actual_count, v_missing_count, v_extra_count;

  IF v_missing_count <> 0 OR v_extra_count <> 0 THEN
    RAISE EXCEPTION
      'Batch % mismatch: expected=%, actual=%, missing=%, extra=%',
      v_batch, v_expected_count, v_actual_count, v_missing_count, v_extra_count;
  END IF;

  RAISE NOTICE
    'Batch % verified: expected=%, actual=%, missing=%, extra=%',
    v_batch, v_expected_count, v_actual_count, v_missing_count, v_extra_count;
END
$$;
