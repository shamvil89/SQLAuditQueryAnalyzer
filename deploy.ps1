$ErrorActionPreference = "Stop"
$ProjectRoot = $PSScriptRoot
if (-not $ProjectRoot) {
    $ProjectRoot = (Get-Location).Path
}
Set-Location $ProjectRoot

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Command,
        [string]$ErrorMessage = "Command failed"
    )
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$ErrorMessage with exit code $LASTEXITCODE"
    }
}

Write-Host "Checking dependencies"
Invoke-Checked { & powershell -ExecutionPolicy Bypass -File (Join-Path $ProjectRoot "install_dependencies.ps1") } "Dependency installation failed"

$venvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    throw "Could not find virtual environment Python at $venvPython"
}
Invoke-Checked { & $venvPython -c "import sys; print(sys.executable)" } "Virtual environment Python is not usable"

Write-Host "Training bundled CPU-only offline model"
Invoke-Checked { & $venvPython (Join-Path $ProjectRoot "audit_ai\analyze_sql_audit.py") --train-model }

Write-Host "Running tests"
Invoke-Checked { & $venvPython -m unittest discover -s (Join-Path $ProjectRoot "tests") }

$analyzerPath = Join-Path $ProjectRoot "audit_ai\analyze_sql_audit.py"
$samplesPath = Join-Path $ProjectRoot "samples"
$outPath = Join-Path $ProjectRoot "out"
$sampleFiles = Get-ChildItem -LiteralPath $samplesPath -Filter "*.csv" -File | Sort-Object Name
if ($sampleFiles.Count -eq 0) {
    throw "No CSV files found in $samplesPath"
}

Write-Host "Creating sample output in .\out for $($sampleFiles.Count) CSV file(s)"
foreach ($sampleFile in $sampleFiles) {
    $safeName = [System.IO.Path]::GetFileNameWithoutExtension($sampleFile.Name) -replace '[^A-Za-z0-9_-]', '_'
    $runId = "deployment_check_{0}_{1}" -f $safeName, (Get-Date -Format "yyyyMMdd_HHmmss")
    Write-Host "Analyzing sample CSV: $($sampleFile.FullName)"
    Write-Host "Analyzer command: $venvPython $analyzerPath $($sampleFile.FullName) --out-dir $outPath --run-id $runId --progress-every 100"
    Invoke-Checked { & $venvPython $analyzerPath $sampleFile.FullName --out-dir $outPath --run-id $runId --progress-every 100 }
}

Write-Host ""
Write-Host "Deployment complete."
Write-Host "Run: $venvPython .\audit_ai\analyze_sql_audit.py .\audit.csv --out-dir .\out"
