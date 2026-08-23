<#
    run-all.ps1  -  Build everything and run the full demo end-to-end.

    A single command that showcases the whole project:
      1. builds via scripts\build.ps1 (vcvars64 -> CMake -> Ninja),
      2. runs the unit tests,
      3. runs the market-making simulator (paced),
      4. runs the queue benchmark (mutex vs spinlock vs lock-free),
      5. runs the affinity / SMT / false-sharing benchmark.

    Usage:
      .\scripts\run-all.ps1                 # build (Release) then run everything
      .\scripts\run-all.ps1 -Clean          # wipe build/ first, then build + run
      .\scripts\run-all.ps1 -Events 5000000 # override the simulator event count
#>
param(
    [switch]$Clean,
    [long]$Events = 2000000,
    [double]$Rate = 1000000
)

$ErrorActionPreference = "Stop"
$Root     = Split-Path -Parent $PSScriptRoot          # repo root = parent of scripts\
$BuildDir = Join-Path $Root "build"

function Section($t) {
    Write-Host ""
    Write-Host ("=" * 74) -ForegroundColor DarkGray
    Write-Host "  $t" -ForegroundColor Cyan
    Write-Host ("=" * 74) -ForegroundColor DarkGray
}

# --- 1. Build ---------------------------------------------------------------
Section "1/5  BUILD"
if ($Clean) { & (Join-Path $PSScriptRoot "build.ps1") -Clean }
else        { & (Join-Path $PSScriptRoot "build.ps1") }

# Helper: run a produced executable, failing loudly if it is missing.
function Run-Exe($name, [string[]]$exeArgs) {
    $exe = Join-Path $BuildDir $name
    if (-not (Test-Path $exe)) { throw "Expected binary not found: $exe (did the build succeed?)" }
    & $exe @exeArgs
    if ($LASTEXITCODE -ne 0) { throw "$name exited with code $LASTEXITCODE" }
}

# --- 2. Tests ---------------------------------------------------------------
Section "2/5  UNIT TESTS"
Run-Exe "qmm_tests.exe" @()

# --- 3. Simulator -----------------------------------------------------------
Section "3/5  MARKET-MAKING SIMULATOR  (--events $Events --rate $Rate)"
$MetricsJson = Join-Path $BuildDir "metrics.json"
Run-Exe "qmm_sim.exe" @("--events", "$Events", "--rate", "$Rate", "--metrics-out", "$MetricsJson")

# --- 4. Queue benchmark -----------------------------------------------------
Section "4/5  QUEUE BENCHMARK  (mutex vs spinlock vs lock-free SPSC)"
$BenchJson = Join-Path $BuildDir "bench.json"
Run-Exe "queue_bench.exe" @("--json", "$BenchJson")

# --- 5. Affinity benchmark --------------------------------------------------
Section "5/5  AFFINITY / SMT / FALSE-SHARING BENCHMARK"
Run-Exe "affinity_bench.exe" @()

# --- Package the metrics into docs\data.js for the HTML dashboard -----------
Section "DASHBOARD  (packaging docs\data.js)"
& (Join-Path $PSScriptRoot "gen-dashboard-data.ps1") -Metrics $MetricsJson -Bench $BenchJson
Write-Host "Open docs\dashboard.html in a browser to view the report." -ForegroundColor Cyan

Write-Host ""
Write-Host "All stages completed successfully." -ForegroundColor Green
