# docs/explain/run_partition_explains.ps1
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
  $cmdSet = "SET jit = off;"
  $cmdExplain = "EXPLAIN (ANALYZE, BUFFERS) `n$Sql"

  $raw = docker compose exec -T postgres psql -X -q -U nyc -d nyc_taxi `
    -v ON_ERROR_STOP=1 -P pager=off `
    -c $cmdDiscard -c $cmdSet -c $cmdExplain 2>&1 | Out-String

  $lines = $raw -split "`r?`n" | Where-Object { $_ -notmatch '^(DISCARD ALL|SET)$' }
  $out = ($lines -join "`r`n").TrimEnd() + "`r`n"
  Write-Utf8NoBom $FilePath $out
}

function Run-QueryToFile([string]$Sql, [string]$FilePath) {
  $raw = docker compose exec -T postgres psql -X -q -U nyc -d nyc_taxi `
    -v ON_ERROR_STOP=1 -P pager=off -c $Sql 2>&1 | Out-String
  $out = ($raw.TrimEnd()) + "`r`n"
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

# 0) sanity
docker compose ps | Out-Null

# 1) Ensure partitioning is enabled and table is migrated if needed.
docker compose exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 `
  -f /app/sql/partition/000_enable_clean_partitioning.sql | Out-Null

# 2) Drop performance indexes to isolate partition-pruning impact.
docker compose exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 `
  -f /app/sql/perf/000_drop_indexes.sql | Out-Null

# 3) Build an unpartitioned heap baseline copy from the same data snapshot.
$heapBaselineSql = @"
drop table if exists clean.clean_yellow_trips_heap_baseline;
create table clean.clean_yellow_trips_heap_baseline as
select * from clean.clean_yellow_trips;
analyze clean.clean_yellow_trips_heap_baseline;
analyze clean.clean_yellow_trips;
"@
docker compose exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 -c $heapBaselineSql | Out-Null

# 4) Save partition inventory.
Run-QueryToFile "select * from pg_partition_tree('clean.clean_yellow_trips'::regclass);" `
  (Join-Path $OutDir "clean_yellow_trips_partition_tree.txt")
$partitionListRaw = docker compose exec -T postgres psql -X -q -U nyc -d nyc_taxi `
  -v ON_ERROR_STOP=1 -P pager=off -f /app/sql/partition/001_list_clean_partitions.sql 2>&1 | Out-String
Write-Utf8NoBom (Join-Path $OutDir "clean_yellow_trips_partitions.txt") (($partitionListRaw.TrimEnd()) + "`r`n")

# 5) q1 (time-window filter) before/after.
$q1_heap = @"
select
  pu_location_id,
  count(*) as trips
from clean.clean_yellow_trips_heap_baseline
where pickup_ts >= timestamp '2024-01-31 00:00:00'
  and pickup_ts <  timestamp '2024-02-01 00:00:00'
group by 1
order by trips desc
limit 20;
"@

$q1_partitioned = @"
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

Run-ExplainToFile $q1_heap (Join-Path $OutDir "q1_partition_pruning_before_heap.txt")
Run-ExplainToFile $q1_partitioned (Join-Path $OutDir "q1_partition_pruning_after_partitioned.txt")

# 6) q2 (global aggregation) before/after to show limited pruning benefit.
$q2_heap = @"
select
  pickup_ts::date as trip_date,
  count(*) as trips,
  sum(total_amount) as revenue
from clean.clean_yellow_trips_heap_baseline
group by 1
order by 1;
"@

$q2_partitioned = @"
select
  pickup_ts::date as trip_date,
  count(*) as trips,
  sum(total_amount) as revenue
from clean.clean_yellow_trips
group by 1
order by 1;
"@

Run-ExplainToFile $q2_heap (Join-Path $OutDir "q2_partition_scope_before_heap.txt")
Run-ExplainToFile $q2_partitioned (Join-Path $OutDir "q2_partition_scope_after_partitioned.txt")

# 7) Show which partitions contribute to q1.
$q1_partition_hits = @"
select
  tableoid::regclass as scanned_relation,
  count(*) as rows_seen
from clean.clean_yellow_trips
where pickup_ts >= timestamp '2024-01-31 00:00:00'
  and pickup_ts <  timestamp '2024-02-01 00:00:00'
group by 1
order by 1;
"@
Run-QueryToFile $q1_partition_hits (Join-Path $OutDir "q1_partition_hits_after_partitioned.txt")

# 8) Print generated files.
$filesToPrint = @(
  (Join-Path $OutDir "clean_yellow_trips_partition_tree.txt"),
  (Join-Path $OutDir "clean_yellow_trips_partitions.txt"),
  (Join-Path $OutDir "q1_partition_pruning_before_heap.txt"),
  (Join-Path $OutDir "q1_partition_pruning_after_partitioned.txt"),
  (Join-Path $OutDir "q2_partition_scope_before_heap.txt"),
  (Join-Path $OutDir "q2_partition_scope_after_partitioned.txt"),
  (Join-Path $OutDir "q1_partition_hits_after_partitioned.txt")
)

foreach ($f in $filesToPrint) {
  if (Test-Path $f) { Print-File $f }
}

# Cleanup temporary baseline relation.
docker compose exec -T postgres psql -X -U nyc -d nyc_taxi -v ON_ERROR_STOP=1 `
  -c "drop table if exists clean.clean_yellow_trips_heap_baseline;" | Out-Null

Write-Host ""
Write-Host "OK: wrote partition evidence plans to docs/explain/*.txt and printed them."
