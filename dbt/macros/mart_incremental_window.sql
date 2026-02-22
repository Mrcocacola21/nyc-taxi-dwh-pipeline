{% macro taxi_mart_lookback_months(default_value=2) -%}
  {%- set raw_value = var('mart_lookback_months', env_var('MART_LOOKBACK_MONTHS', default_value | string)) -%}
  {%- set parsed_value = raw_value | int -%}
  {%- if parsed_value < 1 -%}
    {{ return(1) }}
  {%- endif -%}
  {{ return(parsed_value) }}
{%- endmacro %}


{% macro taxi_mart_anchor_month_sql() -%}
(
  select coalesce(
    max(to_date(batch_id || '-01', 'YYYY-MM-DD'))::timestamp,
    date_trunc('month', current_timestamp)
  )
  from raw.yellow_trips
  where batch_id ~ '^[0-9]{4}-[0-9]{2}$'
)
{%- endmacro %}


{% macro taxi_mart_window_start_ts_sql() -%}
  {%- set lookback_months = taxi_mart_lookback_months() -%}
  ({{ taxi_mart_anchor_month_sql() }} - interval '{{ lookback_months - 1 }} month')
{%- endmacro %}


{% macro taxi_mart_window_end_ts_sql() -%}
  ({{ taxi_mart_anchor_month_sql() }} + interval '1 month')
{%- endmacro %}


{% macro taxi_mart_incremental_source_filter(ts_column='pickup_ts') -%}
{{ ts_column }} >= {{ taxi_mart_window_start_ts_sql() }}
and {{ ts_column }} < {{ taxi_mart_window_end_ts_sql() }}
{%- endmacro %}
