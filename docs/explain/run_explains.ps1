# docs/explain/run_explains.ps1
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$OutDir = "docs\explain"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

function Write-Utf8NoBom([string]$Path, [string]$Content) {
  $enc = New-Object System.Text.UTF8Encoding($false) # UTF-8 no BOM
  [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

function Run-ExplainToFile([string]$Sql, [string]$FilePath) {
  $cmdDiscard = "DISCARD ALL;"
  $cmdSet     = "SET jit = off;"
  $cmdExplain = "EXPLAIN (ANALYZE, BUFFERS) `n$Sql"

  $raw = docker compose exec -T postgres psql -X -q -U nyc -d nyc_taxi `
    -v ON_ERROR_STOP=1 -P pager=off `
    -c $cmdDiscard -c $cmdSet -c $cmdExplain 2>&1 | Out-String

  # убрать статусные строки psql
  $lines = $raw -split "`r?`n" | Where-Object { $_ -notmatch '^(DISCARD ALL|SET)$' }
  $out = ($lines -join "`r`n").TrimEnd() + "`r`n"

  Write-Utf8NoBom $FilePath $out
}


function Print-File([string]$Path) {
  $name = Split-Path -Leaf $Path
  Write-Host ""
  Write-Host ("=" * 80)
  Write-Host ("FILE: {0}" -f $name)
  Write-Host ("PATH: {0}" -f $Path)
  Write-Host ("-" * 80)
  Get-Content $Path -Encoding utf8
}

# -----------------------------
# 0) sanity: docker is up
# -----------------------------
docker compose ps | Out-Null

# -----------------------------
# 1) Drop indexes (BEFORE)
# -----------------------------
docker compose exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 -f /app/sql/perf/000_drop_indexes.sql | Out-Null

# -----------------------------
# 2) Queries (mirror Python QUERIES)
# -----------------------------
$queries = [ordered]@{
  q1_top_pickup_zones_day = @"
select
  pu_location_id,
  count(*) as trips
from clean.clean_yellow_trips
where pickup_ts >= timestamp '2024-01-31 00:00:00'
  and pickup_ts <  timestamp '2024-02-01 00:00:00'
group by 1
order by trips desc
limit 20;
"@

  q2_revenue_by_day = @"
select
  pickup_ts::date as trip_date,
  count(*) as trips,
  sum(total_amount) as revenue
from clean.clean_yellow_trips
group by 1
order by 1;
"@

  q2_mart_daily_revenue = @"
select
  trip_date,
  trips,
  revenue
from marts.marts_daily_revenue
order by 1;
"@

  q3_join_zone_lookup_top20 = @"
select
  z.borough,
  z.zone,
  count(*) as trips,
  avg(t.total_amount) as avg_total
from clean.clean_yellow_trips t
join raw.taxi_zone_lookup z
  on z.locationid = t.pu_location_id
group by 1, 2
order by trips desc
limit 20;
"@

  q4_payment_type_stats = @"
select
  payment_type,
  count(*) as trips,
  avg(tip_amount) as avg_tip
from clean.clean_yellow_trips
group by 1
order by trips desc;
"@

  q5_hourly_peak = @"
select
  extract(hour from pickup_ts)::int as hr,
  count(*) as trips
from clean.clean_yellow_trips
group by 1
order by trips desc;
"@

  q5_mart_hourly_peak = @"
select
  hr,
  sum(trips) as trips
from marts.marts_hourly_peak
group by 1
order by trips desc;
"@
}

$beforeKeys = @(
  "q1_top_pickup_zones_day",
  "q2_revenue_by_day",
  "q3_join_zone_lookup_top20",
  "q4_payment_type_stats",
  "q5_hourly_peak"
)

# -----------------------------
# 3) BEFORE plans (clean q1..q5)
# -----------------------------
foreach ($k in $beforeKeys) {
  Run-ExplainToFile $queries[$k] (Join-Path $OutDir ("{0}_before.txt" -f $k))
}

# clean vs mart extra files (как ты просил)
Run-ExplainToFile $queries["q2_revenue_by_day"]     (Join-Path $OutDir "q2_clean.txt")
Run-ExplainToFile $queries["q2_mart_daily_revenue"] (Join-Path $OutDir "q2_mart.txt")
Run-ExplainToFile $queries["q5_hourly_peak"]        (Join-Path $OutDir "q5_clean.txt")
Run-ExplainToFile $queries["q5_mart_hourly_peak"]   (Join-Path $OutDir "q5_mart.txt")

# -----------------------------
# 4) Create indexes (AFTER)
# -----------------------------
docker compose exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 -f /app/sql/perf/001_create_indexes.sql | Out-Null

# -----------------------------
# 5) AFTER plans (clean q1..q5)
# -----------------------------
foreach ($k in $beforeKeys) {
  Run-ExplainToFile $queries[$k] (Join-Path $OutDir ("{0}_after.txt" -f $k))
}

# -----------------------------
# 6) Print every generated file to console
# -----------------------------
$filesToPrint = @(
  (Join-Path $OutDir "q1_top_pickup_zones_day_before.txt"),
  (Join-Path $OutDir "q1_top_pickup_zones_day_after.txt"),
  (Join-Path $OutDir "q2_revenue_by_day_before.txt"),
  (Join-Path $OutDir "q2_revenue_by_day_after.txt"),
  (Join-Path $OutDir "q3_join_zone_lookup_top20_before.txt"),
  (Join-Path $OutDir "q3_join_zone_lookup_top20_after.txt"),
  (Join-Path $OutDir "q4_payment_type_stats_before.txt"),
  (Join-Path $OutDir "q4_payment_type_stats_after.txt"),
  (Join-Path $OutDir "q5_hourly_peak_before.txt"),
  (Join-Path $OutDir "q5_hourly_peak_after.txt"),
  (Join-Path $OutDir "q2_clean.txt"),
  (Join-Path $OutDir "q2_mart.txt"),
  (Join-Path $OutDir "q5_clean.txt"),
  (Join-Path $OutDir "q5_mart.txt")
)

foreach ($f in $filesToPrint) {
  if (Test-Path $f) { Print-File $f }
}

Write-Host ""
Write-Host "OK: wrote plans to docs/explain/*.txt (UTF-8 no BOM), and printed them to console."
