<#
.SYNOPSIS
  Start Streamlit with OFFLINE_LOCAL_ARCHIVE (and default cache dirs) set for local IUCLID / REACH dossiers.

.DESCRIPTION
  Does not modify .env. Pass the path to reach_study_results_dossiers_*.zip or a folder of .i6z files.
  Example:
    pwsh -File scripts/run_streamlit_with_offline_reach.ps1 `
      -ReachArchive 'C:\data\reach_study_results_dossiers_23-05-2023.zip'
#>
param(
    [Parameter(Mandatory = $true)]
    [string] $ReachArchive,

    [string] $AppRoot = "",

    [int] $Port = 8501
)

$__scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $PSCommandPath }
if (-not $AppRoot) {
    $AppRoot = (Resolve-Path (Join-Path $__scriptDir "..")).Path
}

$archive = (Resolve-Path -LiteralPath $ReachArchive -ErrorAction Stop).Path
$env:OFFLINE_LOCAL_ARCHIVE = $archive
$env:OFFLINE_DATA_DIR = Join-Path $AppRoot "data\offline"
$env:OFFLINE_CACHE_DIR = Join-Path $AppRoot "data\offline_cache"

Set-Location $AppRoot
Write-Host "OFFLINE_LOCAL_ARCHIVE=$($env:OFFLINE_LOCAL_ARCHIVE)"
Write-Host "OFFLINE_DATA_DIR=$($env:OFFLINE_DATA_DIR)"
Write-Host "OFFLINE_CACHE_DIR=$($env:OFFLINE_CACHE_DIR)"
python -m streamlit run app.py --server.port=$Port --server.address=127.0.0.1
