{% set raw_batch_ids = var('batch_ids', []) %}
{% set rebuild_batch_ids = [] %}
{% if raw_batch_ids is string %}
  {% for batch_id in raw_batch_ids.split(',') %}
    {% set batch_id_trimmed = batch_id | trim %}
    {% if batch_id_trimmed %}
      {% do rebuild_batch_ids.append(batch_id_trimmed) %}
    {% endif %}
  {% endfor %}
{% elif raw_batch_ids is sequence and raw_batch_ids is not mapping %}
  {% for batch_id in raw_batch_ids %}
    {% set batch_id_trimmed = (batch_id | string) | trim %}
    {% if batch_id_trimmed %}
      {% do rebuild_batch_ids.append(batch_id_trimmed) %}
    {% endif %}
  {% endfor %}
{% endif %}

{% set rebuild_batch_ids_sql = [] %}
{% for batch_id in rebuild_batch_ids %}
  {% do rebuild_batch_ids_sql.append("'" ~ (batch_id | replace("'", "''")) ~ "'") %}
{% endfor %}

{% set changed_batches_sql %}
  select s.batch_id
  from {{ ref('stg_yellow_trips') }} s
  left join (
    select
      batch_id,
      max(ingested_at) as max_ingested_at
    from {{ this }}
    group by 1
  ) t on t.batch_id = s.batch_id
  group by s.batch_id, t.max_ingested_at
  having t.max_ingested_at is null or max(s.ingested_at) > t.max_ingested_at
{% endset %}

{% set pre_hooks = [] %}
{% if is_incremental() %}
  {% if rebuild_batch_ids_sql | length > 0 %}
    {% do pre_hooks.append("delete from " ~ this ~ " where batch_id in (" ~ (rebuild_batch_ids_sql | join(', ')) ~ ")") %}
  {% else %}
    {% do pre_hooks.append("delete from " ~ this ~ " where batch_id in (" ~ (changed_batches_sql | trim) ~ ")") %}
  {% endif %}
{% endif %}

{{
  config(
    materialized='incremental',
    unique_key='batch_id',
    incremental_strategy='delete+insert',
    pre_hook=pre_hooks
  )
}}

with src as (
  select *
  from {{ ref('stg_yellow_trips') }}

  {% if is_incremental() %}
    {% if rebuild_batch_ids_sql | length > 0 %}
      where batch_id in ({{ rebuild_batch_ids_sql | join(', ') }})
    {% else %}
      where batch_id in (
        {{ changed_batches_sql }}
      )
    {% endif %}
  {% endif %}
),

good as (
  select *
  from src
  where true
    and pickup_ts is not null
    and dropoff_ts is not null
    and dropoff_ts >= pickup_ts
    and pu_location_id is not null
    and do_location_id is not null
    and trip_distance is not null and trip_distance > 0
    and total_amount is not null and total_amount >= 0
    and (payment_type is null or payment_type in (0,1,2,3,4,5,6))
    and (rate_code_id is null or rate_code_id in (1,2,3,4,5,6,99))
),

fingerprinted as (
  select
    *,
    md5(
      coalesce(vendor_id::text,'') || '|' ||
      coalesce(pickup_ts::text,'') || '|' ||
      coalesce(dropoff_ts::text,'') || '|' ||
      coalesce(pu_location_id::text,'') || '|' ||
      coalesce(do_location_id::text,'') || '|' ||
      coalesce(trip_distance::text,'') || '|' ||
      coalesce(total_amount::text,'')
    ) as row_fingerprint
  from good
)

select *
from fingerprinted
