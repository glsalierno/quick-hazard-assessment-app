# Creates a Desktop shortcut to run the Quick Hazard Assessment app locally.
# Run from PowerShell (may need: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned):
#   cd path\to\quick-hazard-assessment-app
#   .\scripts\Create-DesktopShortcut.ps1

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$BatPath = Join-Path $RepoRoot "run_hazard_app.bat"
$IconPath = Join-Path $RepoRoot "assets\hazard_app.ico"

if (-not (Test-Path $BatPath)) {
    Write-Error "Not found: $BatPath"
}

if (-not (Test-Path $IconPath)) {
    Write-Host "Generating icon..."
    & python (Join-Path $RepoRoot "scripts\generate_hazard_app_icon.py")
    if (-not (Test-Path $IconPath)) {
        Write-Warning "Icon not created; shortcut will use default icon. Install Pillow: pip install Pillow"
        $IconPath = "$env:SystemRoot\System32\imageres.dll,78"
    }
}

$Desktop = [Environment]::GetFolderPath("Desktop")
$ShortcutPath = Join-Path $Desktop "Quick Hazard Assessment.lnk"

$Wsh = New-Object -ComObject WScript.Shell
$Sc = $Wsh.CreateShortcut($ShortcutPath)
$Sc.TargetPath = $BatPath
$Sc.WorkingDirectory = $RepoRoot
$Sc.Description = "Quick Hazard Assessment (Streamlit) - local hazard analysis"
if (Test-Path (Join-Path $RepoRoot "assets\hazard_app.ico")) {
    $Sc.IconLocation = (Join-Path $RepoRoot "assets\hazard_app.ico")
}
$Sc.WindowStyle = 1
$Sc.Save()

Write-Host "Created: $ShortcutPath"
Write-Host "Double-click to open http://localhost:8501"
