<#
    build.ps1  -  One-command build for quant-mm-sim.

    Why this script exists:
      The MSVC compiler (cl.exe), the bundled CMake and Ninja are only on PATH
      inside a "Developer" environment created by vcvars64.bat. Rather than ask
      you to open a special prompt, this script locates Visual Studio, sources
      vcvars64 into the current cmd process, and runs the CMake configure + build
      in that same process (so the environment actually applies).

    Usage:
      .\scripts\build.ps1                # Release build (default)
      .\scripts\build.ps1 -Config Debug  # Debug build
      .\scripts\build.ps1 -Clean         # wipe build/ first
#>
param(
    [string]$Config = "Release",
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

# Repo root = parent of the folder this script lives in.
$Root = Split-Path -Parent $PSScriptRoot
$BuildDir = Join-Path $Root "build"

# --- Locate Visual Studio + vcvars64.bat via vswhere -------------------------
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (-not (Test-Path $vswhere)) {
    throw "vswhere.exe not found; is Visual Studio installed?"
}
$vsPath = & $vswhere -latest -products * `
    -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
    -property installationPath
if (-not $vsPath) { throw "No Visual Studio with the C++ toolset was found." }
$vcvars = Join-Path $vsPath "VC\Auxiliary\Build\vcvars64.bat"
if (-not (Test-Path $vcvars)) { throw "vcvars64.bat not found at $vcvars" }

if ($Clean -and (Test-Path $BuildDir)) {
    Write-Host "Cleaning $BuildDir ..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force $BuildDir
}

# --- Configure + build inside a single cmd.exe that has the VC env ------------
# We use the Ninja generator (fast, bundled with VS). Everything after `call`
# runs with cl/cmake/ninja on PATH.
$cmd = "call `"$vcvars`" >nul && " +
       "cmake -S `"$Root`" -B `"$BuildDir`" -G Ninja -DCMAKE_BUILD_TYPE=$Config && " +
       "cmake --build `"$BuildDir`" --config $Config"

Write-Host "Building ($Config) ..." -ForegroundColor Cyan
& $env:ComSpec /c $cmd
if ($LASTEXITCODE -ne 0) { throw "Build failed with exit code $LASTEXITCODE" }
Write-Host "Build succeeded. Binaries in $BuildDir" -ForegroundColor Green
