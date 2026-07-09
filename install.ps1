# oboobot install script for Windows (PowerShell 5.1+)
# Usage: irm https://raw.githubusercontent.com/oboobotenefiok/oboobot/main/install.ps1 | iex

$ErrorActionPreference = "Stop"
$Repo   = "https://github.com/oboobotenefiok/oboobot"
$BinDir = "$env:LOCALAPPDATA\oboobot\bin"

function Write-Step($msg) { Write-Host "  $msg" -ForegroundColor Cyan }
function Write-Ok($msg)   { Write-Host "  ✓ $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "  ○ $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "  oboobot — cryptocurrency price monitor" -ForegroundColor White
Write-Host "  $Repo" -ForegroundColor DarkGray
Write-Host ""

# Create bin directory
New-Item -ItemType Directory -Force -Path $BinDir | Out-Null

# Download binary
$Arch    = if ([Environment]::Is64BitOperatingSystem) { "x86_64" } else { "x86" }
$BinUrl  = "$Repo/releases/latest/download/oboobot-windows-$Arch.exe"
$BinPath = "$BinDir\oboobot.exe"

Write-Step "Downloading oboobot for Windows $Arch..."
try {
    Invoke-WebRequest -Uri $BinUrl -OutFile $BinPath -UseBasicParsing
    Write-Ok "Downloaded to $BinPath"
} catch {
    Write-Warn "No pre-built binary found. Building from source..."
    if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
        Write-Host "  cargo not found. Install Rust from https://rustup.rs" -ForegroundColor Red
        exit 1
    }
    $TmpDir = [System.IO.Path]::GetTempPath() + "oboobot_build"
    git clone --depth=1 $Repo $TmpDir 2>$null
    Push-Location $TmpDir
    cargo build --release --quiet
    Copy-Item "target\release\oboobot.exe" $BinPath
    Pop-Location
    Remove-Item -Recurse -Force $TmpDir
    Write-Ok "Built from source"
}

# Add to PATH
$CurrentPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($CurrentPath -notlike "*$BinDir*") {
    [Environment]::SetEnvironmentVariable("Path", "$CurrentPath;$BinDir", "User")
    $env:Path += ";$BinDir"
    Write-Ok "Added $BinDir to PATH"
}

Write-Host ""
Write-Ok "oboobot installed!"
Write-Host ""

# Create example .env file
$EnvPath = "$env:USERPROFILE\.oboobot"
New-Item -ItemType Directory -Force -Path $EnvPath | Out-Null
$EnvExample = @"
# CoinGecko API configuration
# Leave empty for keyless (public) access
COINGECKO_API_KEY=
"@
$EnvFile = "$EnvPath\.env"
if (-not (Test-Path $EnvFile)) {
    $EnvExample | Out-File -FilePath $EnvFile -Encoding UTF8
    Write-Ok "Created example .env file at $EnvFile"
}

# Run init
$Run = Read-Host "  Run oboobot now? [Y/n]"
if ($Run -eq "" -or $Run -match "^[Yy]") {
    & $BinPath
}

Write-Host ""
