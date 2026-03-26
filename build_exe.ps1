$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

python -m PyInstaller `
  --noconfirm `
  --clean `
  --onefile `
  --name ResolvEVOY `
  --add-data "app.py;." `
  launcher.py
