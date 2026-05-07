param(
    [string]$Python = "python",
    [string]$VenvPath = ".venv",
    [string]$RequirementsPath = "requirements.txt",
    [string]$PythonVersion = "3.12.10",
    [ValidateSet("User", "Machine")]
    [string]$InstallScope = "User",
    [string]$PythonInstallerPath = "",
    [int]$InstallTimeoutSeconds = 600,
    [int]$InstallerLogTailLines = 80,
    [switch]$PrintInstallerLog,
    [switch]$SkipPythonInstall
)

$ErrorActionPreference = "Stop"
$ProjectRoot = $PSScriptRoot
if (-not $ProjectRoot) {
    $ProjectRoot = (Get-Location).Path
}
Set-Location $ProjectRoot

if (-not [System.IO.Path]::IsPathRooted($VenvPath)) {
    $VenvPath = Join-Path $ProjectRoot $VenvPath
}
if (-not [System.IO.Path]::IsPathRooted($RequirementsPath)) {
    $RequirementsPath = Join-Path $ProjectRoot $RequirementsPath
}
if ($PythonInstallerPath -and -not [System.IO.Path]::IsPathRooted($PythonInstallerPath)) {
    $PythonInstallerPath = Join-Path $ProjectRoot $PythonInstallerPath
}

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

function Test-CommandExists {
    param([string]$CommandName)
    $null -ne (Get-Command $CommandName -ErrorAction SilentlyContinue)
}

function Test-PythonExecutableWorks {
    param([string]$PythonExe)
    if (-not (Test-Path $PythonExe)) {
        return $false
    }
    try {
        & $PythonExe -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" *> $null
        return $LASTEXITCODE -eq 0
    }
    catch {
        return $false
    }
}

function Test-IsAdmin {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Add-PathEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathEntry,
        [Parameter(Mandatory = $true)]
        [ValidateSet("User", "Machine")]
        [string]$Scope
    )

    if (-not (Test-Path $PathEntry)) {
        return
    }

    $target = [EnvironmentVariableTarget]::$Scope
    $existing = [Environment]::GetEnvironmentVariable("Path", $target)
    $parts = @()
    if ($existing) {
        $parts = $existing -split ";" | Where-Object { $_.Trim() }
    }

    $alreadyExists = $false
    foreach ($part in $parts) {
        if ($part.TrimEnd("\") -ieq $PathEntry.TrimEnd("\")) {
            $alreadyExists = $true
            break
        }
    }

    if (-not $alreadyExists) {
        $newPath = (($parts + $PathEntry) -join ";")
        try {
            [Environment]::SetEnvironmentVariable("Path", $newPath, $target)
            Write-Host "Added to $Scope PATH: $PathEntry"
        }
        catch {
            Write-Warning "Could not update persistent $Scope PATH. Current process PATH will still be updated. Details: $($_.Exception.Message)"
        }
    }

    $processParts = @()
    if ($env:Path) {
        $processParts = $env:Path -split ";" | Where-Object { $_.Trim() }
    }
    $processHasPath = $false
    foreach ($part in $processParts) {
        if ($part.TrimEnd("\") -ieq $PathEntry.TrimEnd("\")) {
            $processHasPath = $true
            break
        }
    }
    if (-not $processHasPath) {
        $env:Path = (($processParts + $PathEntry) -join ";")
    }
}

function Find-PythonExecutable {
    param([string]$PreferredCommand)

    $commands = @($PreferredCommand, "python", "py")
    foreach ($command in $commands | Select-Object -Unique) {
        if (-not (Test-CommandExists $command)) {
            continue
        }

        if ($command -ieq "py") {
            try {
                & py -3 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" *> $null
                if ($LASTEXITCODE -eq 0) {
                    $path = (& py -3 -c "import sys; print(sys.executable)").Trim()
                    if ($path -and (Test-Path $path)) {
                        return $path
                    }
                }
            }
            catch {
            }
        }
        else {
            try {
                & $command -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" *> $null
                if ($LASTEXITCODE -eq 0) {
                    $path = (& $command -c "import sys; print(sys.executable)").Trim()
                    if ($path -and (Test-Path $path)) {
                        return $path
                    }
                }
            }
            catch {
            }
        }
    }

    $candidateRoots = @(
        "$env:LocalAppData\Programs\Python",
        "$env:ProgramFiles\Python312",
        "$env:ProgramFiles\Python311",
        "$env:ProgramFiles\Python310",
        "${env:ProgramFiles(x86)}\Python312",
        "${env:ProgramFiles(x86)}\Python311",
        "${env:ProgramFiles(x86)}\Python310"
    )
    foreach ($root in $candidateRoots) {
        if (-not $root -or -not (Test-Path $root)) {
            continue
        }
        $matches = Get-ChildItem -Path $root -Recurse -Filter "python.exe" -ErrorAction SilentlyContinue |
            Sort-Object FullName -Descending
        foreach ($match in $matches) {
            try {
                & $match.FullName -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" *> $null
                if ($LASTEXITCODE -eq 0) {
                    return $match.FullName
                }
            }
            catch {
            }
        }
    }

    return $null
}

function Install-Python {
    param(
        [string]$Version,
        [ValidateSet("User", "Machine")]
        [string]$Scope,
        [string]$InstallerPath,
        [int]$TimeoutSeconds,
        [int]$LogTailLines,
        [bool]$PrintLog
    )

    if ($Scope -eq "Machine" -and -not (Test-IsAdmin)) {
        throw "Machine-scope Python install requires PowerShell to be run as Administrator. Rerun as admin or use -InstallScope User."
    }

    $winget = Get-Command "winget" -ErrorAction SilentlyContinue
    if ($winget -and -not $InstallerPath) {
        Write-Host "Python was not found. Installing Python silently with winget."
        $wingetScope = if ($Scope -eq "Machine") { "machine" } else { "user" }
        & winget install --id "Python.Python.3.12" --exact --silent --scope $wingetScope --accept-package-agreements --accept-source-agreements
        if ($LASTEXITCODE -eq 0 -or $LASTEXITCODE -eq 3010) {
            return
        }
        Write-Host "winget install did not complete successfully. Falling back to Python.org installer."
    }

    $installer = $InstallerPath
    if (-not $installer) {
        $downloadDir = Join-Path $ProjectRoot ".deps"
        New-Item -ItemType Directory -Force -Path $downloadDir | Out-Null
        $installer = Join-Path $downloadDir "python-$Version-amd64.exe"
        if (-not (Test-Path $installer)) {
            $url = "https://www.python.org/ftp/python/$Version/python-$Version-amd64.exe"
            Write-Host "Downloading Python installer: $url"
            Invoke-WebRequest -Uri $url -OutFile $installer
        }
    }

    if (-not (Test-Path $installer)) {
        throw "Python installer was not found at $installer"
    }

    function Write-InstallerLogTail {
        param([string]$Path, [int]$Lines)
        if (-not (Test-Path $Path)) {
            return
        }
        Write-Host ""
        Write-Host "---- Python installer log tail: $Path ----"
        Get-Content -Path $Path -Tail $Lines -ErrorAction SilentlyContinue | ForEach-Object {
            Write-Host $_
        }
        Write-Host "---- End Python installer log tail ----"
        Write-Host ""
    }

    Write-Host "Installing Python silently from $installer"
    Write-Host "This can take a few minutes. Installer output is written to .deps\python-install.log"
    $allUsers = if ($Scope -eq "Machine") { "1" } else { "0" }
    $logDir = Join-Path $ProjectRoot ".deps"
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $logPath = Join-Path $logDir "python-install.log"
    $arguments = @(
        "/quiet",
        "/log",
        "`"$logPath`"",
        "InstallAllUsers=$allUsers",
        "PrependPath=1",
        "Include_launcher=1",
        "Include_pip=1",
        "Include_test=0",
        "Include_tcltk=0",
        "Shortcuts=0",
        "AssociateFiles=0"
    )
    $process = Start-Process -FilePath $installer -ArgumentList $arguments -PassThru
    $startedAt = Get-Date
    while (-not $process.HasExited) {
        $elapsed = [int]((Get-Date) - $startedAt).TotalSeconds
        if ($elapsed -ge $TimeoutSeconds) {
            try {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            }
            catch {
            }
            throw "Python installer timed out after $TimeoutSeconds seconds. Check $logPath, then rerun the script."
        }
        Write-Host ("Still installing Python... {0}s elapsed" -f $elapsed)
        if ($PrintLog) {
            Write-InstallerLogTail -Path $logPath -Lines $LogTailLines
        }
        Start-Sleep -Seconds 15
        $process.Refresh()
    }

    if ($process.ExitCode -ne 0 -and $process.ExitCode -ne 3010) {
        Write-InstallerLogTail -Path $logPath -Lines $LogTailLines
        throw "Python installer failed with exit code $($process.ExitCode)"
    }
    if ($process.ExitCode -eq 3010) {
        Write-Warning "Python installer completed and requested a reboot. Continuing because Python may already be usable in this session."
    }
    if ($PrintLog) {
        Write-InstallerLogTail -Path $logPath -Lines $LogTailLines
    }
}

function Configure-PythonEnvironment {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExe,
        [ValidateSet("User", "Machine")]
        [string]$Scope
    )

    $pythonDir = Split-Path -Parent $PythonExe
    $scriptsDir = Join-Path $pythonDir "Scripts"

    Add-PathEntry -PathEntry $pythonDir -Scope $Scope
    Add-PathEntry -PathEntry $scriptsDir -Scope $Scope

    try {
        [Environment]::SetEnvironmentVariable("PYTHONUTF8", "1", [EnvironmentVariableTarget]::$Scope)
        [Environment]::SetEnvironmentVariable("PIP_DISABLE_PIP_VERSION_CHECK", "1", [EnvironmentVariableTarget]::$Scope)
    }
    catch {
        Write-Warning "Could not update persistent Python environment variables. Current process variables will still be set. Details: $($_.Exception.Message)"
    }
    $env:PYTHONUTF8 = "1"
    $env:PIP_DISABLE_PIP_VERSION_CHECK = "1"
}

$pythonExe = Find-PythonExecutable -PreferredCommand $Python
if (-not $pythonExe) {
    if ($SkipPythonInstall) {
        throw "Python 3.10+ was not found and -SkipPythonInstall was specified."
    }
    Install-Python -Version $PythonVersion -Scope $InstallScope -InstallerPath $PythonInstallerPath -TimeoutSeconds $InstallTimeoutSeconds -LogTailLines $InstallerLogTailLines -PrintLog $PrintInstallerLog.IsPresent
    $pythonExe = Find-PythonExecutable -PreferredCommand $Python
}

if (-not $pythonExe) {
    throw "Python installation completed, but Python 3.10+ could not be found. Open a new PowerShell window and rerun .\install_dependencies.ps1"
}

Write-Host "Using Python: $pythonExe"
Write-Host "Checking Python version"
Invoke-Checked { & $pythonExe -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" } "Python 3.10+ is required"

Configure-PythonEnvironment -PythonExe $pythonExe -Scope $InstallScope

Write-Host "Creating local virtual environment at $VenvPath if needed"
$venvPython = Join-Path $VenvPath "Scripts\python.exe"
if ((Test-Path $VenvPath) -and -not (Test-PythonExecutableWorks -PythonExe $venvPython)) {
    $resolvedVenv = (Resolve-Path $VenvPath).Path
    if (-not $resolvedVenv.StartsWith($ProjectRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove stale virtual environment outside project folder: $resolvedVenv"
    }
    Write-Warning "Existing .venv is broken or points to a missing Python. Recreating it."
    Remove-Item -LiteralPath $resolvedVenv -Recurse -Force
}

if (-not (Test-Path $VenvPath)) {
    Invoke-Checked { & $pythonExe -m venv $VenvPath } "Failed to create virtual environment"
}

if (-not (Test-PythonExecutableWorks -PythonExe $venvPython)) {
    throw "Could not find virtual environment Python at $venvPython"
}

if (Test-Path $RequirementsPath) {
    $requirements = Get-Content $RequirementsPath |
        Where-Object { $_.Trim() -and -not $_.Trim().StartsWith("#") }

    if ($requirements.Count -gt 0) {
        Write-Host "Installing Python package dependencies from $RequirementsPath"
        Invoke-Checked { & $venvPython -m pip install -r $RequirementsPath } "Failed to install dependencies"
    }
    else {
        Write-Host "No third-party Python packages are required."
    }
}
else {
    Write-Host "No requirements.txt found. Skipping package installation."
}

Write-Host "Dependency check complete."
Write-Host "Python: $venvPython"
