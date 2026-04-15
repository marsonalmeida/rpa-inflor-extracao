param(
    [string]$RepoUrl = "https://github.com/marsonalmeida/rpa-inflor-extracao.git",
    [string]$WorkDir = "C:\inflor-deploy",
    [ValidateSet("prd", "stg")]
    [string]$LakeEnv = "prd",
    [string]$AwsRegion = "us-east-1",
    [switch]$DryRun,
    [switch]$ConfigureTasks,
    [switch]$SkipPrereqs,
    [switch]$SkipProjectSetup,
    [string]$LoginInflor,
    [string]$SenhaInflor
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Find-Executable {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Candidates
    )

    foreach ($candidate in $Candidates) {
        if ([string]::IsNullOrWhiteSpace($candidate)) {
            continue
        }

        $command = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($command) {
            return $command.Source
        }

        if (Test-Path $candidate) {
            return $candidate
        }
    }

    return $null
}

function Invoke-CheckedProcess {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [string[]]$Arguments = @(),
        [string]$WorkingDirectory = $PWD.Path
    )

    $process = Start-Process -FilePath $FilePath -ArgumentList $Arguments -WorkingDirectory $WorkingDirectory -Wait -NoNewWindow -PassThru
    if ($process.ExitCode -ne 0) {
        throw "Falha ao executar: $FilePath $($Arguments -join ' ') (exit code $($process.ExitCode))"
    }
}

function Install-WingetPackage {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Id,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    Write-Host "Instalando $Name..."
    Invoke-CheckedProcess -FilePath "winget" -Arguments @(
        "install",
        "--id", $Id,
        "--exact",
        "--silent",
        "--accept-package-agreements",
        "--accept-source-agreements"
    )
}

function New-OrUpdateEnvFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$EnvPath
    )

    $values = @{}
    if (Test-Path $EnvPath) {
        foreach ($line in Get-Content $EnvPath) {
            if ($line -match '^\s*#' -or $line -notmatch '=') {
                continue
            }

            $parts = $line.Split('=', 2)
            $values[$parts[0]] = $parts[1]
        }
    }

    if ($script:PSBoundParameters.ContainsKey("LoginInflor")) {
        $values["LOGIN_INFLOR"] = $LoginInflor
    }
    if ($script:PSBoundParameters.ContainsKey("SenhaInflor")) {
        $values["SENHA_INFLOR"] = $SenhaInflor
    }

    if (-not $values.ContainsKey("LOGIN_INFLOR")) { $values["LOGIN_INFLOR"] = "" }
    if (-not $values.ContainsKey("SENHA_INFLOR")) { $values["SENHA_INFLOR"] = "" }

    $values["AWS_REGION"] = $AwsRegion
    $values["SECRET_NAME"] = "inflor/credentials"
    $values["LAKE_ENV"] = $LakeEnv
    $values["LAKE_S3_BUCKET"] = "re.green-assets"
    $values["DRY_RUN"] = $(if ($DryRun) { "True" } else { "False" })
    $values["DEBUG_S3_BUCKET"] = "datalake-inflor-raw"
    if (-not $values.ContainsKey("CLOUDWATCH_LOG_GROUP")) { $values["CLOUDWATCH_LOG_GROUP"] = "" }
    if (-not $values.ContainsKey("TIPO_PERIODO")) { $values["TIPO_PERIODO"] = "trimestre" }
    if (-not $values.ContainsKey("ANOS_RETROATIVOS")) { $values["ANOS_RETROATIVOS"] = "4" }
    if (-not $values.ContainsKey("PERIODOS_POR_EXECUCAO")) { $values["PERIODOS_POR_EXECUCAO"] = "1" }
    if (-not $values.ContainsKey("SAIDA_LOCAL_APONTAMENTO")) { $values["SAIDA_LOCAL_APONTAMENTO"] = "" }
    if (-not $values.ContainsKey("SAIDA_LOCAL_MODELO")) { $values["SAIDA_LOCAL_MODELO"] = "" }

    $lines = @(
        "# Arquivo gerado/atualizado por bootstrap_workspace.ps1",
        "LOGIN_INFLOR=$($values['LOGIN_INFLOR'])",
        "SENHA_INFLOR=$($values['SENHA_INFLOR'])",
        "AWS_REGION=$($values['AWS_REGION'])",
        "SECRET_NAME=$($values['SECRET_NAME'])",
        "LAKE_ENV=$($values['LAKE_ENV'])",
        "LAKE_S3_BUCKET=$($values['LAKE_S3_BUCKET'])",
        "DRY_RUN=$($values['DRY_RUN'])",
        "DEBUG_S3_BUCKET=$($values['DEBUG_S3_BUCKET'])",
        "CLOUDWATCH_LOG_GROUP=$($values['CLOUDWATCH_LOG_GROUP'])",
        "TIPO_PERIODO=$($values['TIPO_PERIODO'])",
        "ANOS_RETROATIVOS=$($values['ANOS_RETROATIVOS'])",
        "PERIODOS_POR_EXECUCAO=$($values['PERIODOS_POR_EXECUCAO'])",
        "SAIDA_LOCAL_APONTAMENTO=$($values['SAIDA_LOCAL_APONTAMENTO'])",
        "SAIDA_LOCAL_MODELO=$($values['SAIDA_LOCAL_MODELO'])"
    )

    Set-Content -Path $EnvPath -Value $lines -Encoding UTF8
}

if (-not (Test-IsAdministrator)) {
    throw "Execute este script em um PowerShell com privilegios de Administrador."
}

Write-Host "=== INFLOR WorkSpaces Bootstrap ==="

if (-not $SkipPrereqs) {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        throw "winget nao esta disponivel neste WorkSpaces. Instale Python 3.11+, Google Chrome, AWS CLI v2 e Git manualmente antes de continuar."
    }

    Install-WingetPackage -Id "Python.Python.3.11" -Name "Python 3.11"
    Install-WingetPackage -Id "Google.Chrome" -Name "Google Chrome"
    Install-WingetPackage -Id "Amazon.AWSCLI" -Name "AWS CLI v2"
    Install-WingetPackage -Id "Git.Git" -Name "Git"
}

$pythonExe = Find-Executable -Candidates @(
    "py",
    "python",
    "C:\Program Files\Python311\python.exe",
    "C:\Users\$env:USERNAME\AppData\Local\Programs\Python\Python311\python.exe"
)
$chromeExe = Find-Executable -Candidates @(
    "chrome",
    "C:\Program Files\Google\Chrome\Application\chrome.exe",
    "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
)
$awsExe = Find-Executable -Candidates @(
    "aws",
    "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
)
$gitExe = Find-Executable -Candidates @(
    "git",
    "C:\Program Files\Git\cmd\git.exe"
)

if (-not $pythonExe) { throw "Python 3.11+ nao foi encontrado apos a instalacao." }
if (-not $chromeExe) { throw "Google Chrome nao foi encontrado apos a instalacao." }
if (-not $awsExe) { throw "AWS CLI v2 nao foi encontrado apos a instalacao." }
if (-not $gitExe) { throw "Git nao foi encontrado apos a instalacao." }

Write-Host "Python localizado em: $pythonExe"
Write-Host "Chrome localizado em: $chromeExe"
Write-Host "AWS CLI localizado em: $awsExe"
Write-Host "Git localizado em: $gitExe"

if (-not (Test-Path $WorkDir)) {
    $parentDir = Split-Path -Parent $WorkDir
    if ($parentDir -and -not (Test-Path $parentDir)) {
        New-Item -Path $parentDir -ItemType Directory -Force | Out-Null
    }

    Invoke-CheckedProcess -FilePath $gitExe -Arguments @("clone", $RepoUrl, $WorkDir)
} elseif (-not (Test-Path (Join-Path $WorkDir ".git"))) {
    throw "O diretorio $WorkDir ja existe, mas nao e um repositorio git."
} else {
    Write-Host "Repositorio ja existe em $WorkDir; mantendo conteudo atual."
}

$envPath = Join-Path $WorkDir ".env"
New-OrUpdateEnvFile -EnvPath $envPath
Write-Host "Arquivo .env preparado em: $envPath"

if (-not $SkipProjectSetup) {
    Invoke-CheckedProcess -FilePath "cmd.exe" -Arguments @(
        "/c",
        "set INFLOR_NO_PAUSE=1 && call setup.bat"
    ) -WorkingDirectory $WorkDir

    if ($ConfigureTasks) {
        Invoke-CheckedProcess -FilePath "cmd.exe" -Arguments @(
            "/c",
            "set INFLOR_NO_PAUSE=1 && call setup_task_scheduler.bat"
        ) -WorkingDirectory $WorkDir
    }
}

Write-Host "Bootstrap concluido."
Write-Host "Proximos passos recomendados:"
Write-Host "1. aws configure"
Write-Host "2. Validar/atualizar o secret inflor/credentials"
Write-Host "3. Testar manualmente os scripts em C:\inflor-extrator\.venv\Scripts\python.exe"