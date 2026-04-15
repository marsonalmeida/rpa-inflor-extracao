@echo off
setlocal

set "BASE_DIR=C:\inflor-extrator"
set "SRC_DIR=%BASE_DIR%\src"
set "VENV_DIR=%BASE_DIR%\.venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
set "NO_PAUSE=%INFLOR_NO_PAUSE%"

REM ============================================================================
REM INFLOR Extrator - Setup na VM Windows
REM Executa uma vez para configurar o ambiente
REM ============================================================================

echo [1/8] Criando estrutura de diretórios...
mkdir "%SRC_DIR%" 2>nul
mkdir "%BASE_DIR%\logs" 2>nul
mkdir "%BASE_DIR%\downloads\apontamentos" 2>nul
mkdir "%BASE_DIR%\downloads\modelo" 2>nul
mkdir "%BASE_DIR%\output\apontamentos" 2>nul
mkdir "%BASE_DIR%\output\modelo" 2>nul
mkdir "%BASE_DIR%\debug" 2>nul

echo [2/8] Copiando scripts e configurações...
copy /Y inflor_utils.py "%SRC_DIR%\" >nul
copy /Y inflor_extracao_apontamento.py "%SRC_DIR%\" >nul
copy /Y inflor_extracao_model.py "%SRC_DIR%\" >nul
copy /Y requirements.txt "%BASE_DIR%\" >nul
if exist .env (
    copy /Y .env "%SRC_DIR%\" >nul
) else (
    echo AVISO: Arquivo .env nao encontrado no diretorio atual.
    echo        Crie %SRC_DIR%\.env com LOGIN_INFLOR e SENHA_INFLOR.
)

echo [3/8] Criando ambiente virtual Python...
if not exist "%VENV_DIR%" (
    py -3 -m venv "%VENV_DIR%" 2>nul
    if %errorlevel% neq 0 (
        python -m venv "%VENV_DIR%"
    )
)
if %errorlevel% neq 0 (
    echo ERRO: Nao foi possivel criar o ambiente virtual Python.
    echo Verifique se Python 3.11+ esta instalado e no PATH.
    if /I not "%NO_PAUSE%"=="1" pause
    exit /b 1
)

echo [4/8] Instalando dependencias Python na venv...
"%PYTHON_EXE%" -m pip install --upgrade pip
"%PYTHON_EXE%" -m pip install -r "%BASE_DIR%\requirements.txt"
if %errorlevel% neq 0 (
    echo ERRO: Falha ao instalar dependências. Verifique se o Python está instalado.
    if /I not "%NO_PAUSE%"=="1" pause
    exit /b 1
)

echo [5/8] Verificando AWS CLI...
aws --version >nul 2>&1
if %errorlevel% neq 0 (
    echo AVISO: AWS CLI não encontrado. Instale em https://aws.amazon.com/cli/
    echo Após instalar, execute: aws configure
) else (
    echo AWS CLI OK. Verifique se está configurado: aws configure list
)

echo [6/8] Credenciais INFLOR no Secrets Manager...
echo Execute o comando abaixo para criar/atualizar o secret:
echo.
echo   aws secretsmanager create-secret ^
echo       --name inflor/credentials ^
echo       --secret-string "{\"LOGIN_INFLOR\":\"usuario@re.green\",\"SENHA_INFLOR\":\"SUA_SENHA\"}"
echo.
echo   (Se já existe, use put-secret-value no lugar de create-secret)
echo.

echo [7/8] Configurando CloudWatch Logs (opcional)...
echo Para habilitar logs no CloudWatch, execute:
echo.
echo   aws logs create-log-group --log-group-name /inflor/extracao
echo.
echo   Depois preencha CLOUDWATCH_LOG_GROUP=/inflor/extracao no .env
echo.

echo [8/8] Setup concluido.

echo ============================================================================
echo Setup concluído!
echo.
echo Próximos passos:
echo   1. Configure AWS CLI:            aws configure
echo   2. Crie o secret inflor/credentials (passo 6 acima)
echo   3. Configure CloudWatch (passo 7 acima, opcional)
echo   4. Teste manual:
echo        "%PYTHON_EXE%" "%SRC_DIR%\inflor_extracao_apontamento.py"
echo        "%PYTHON_EXE%" "%SRC_DIR%\inflor_extracao_model.py"
echo   5. Agende as tarefas:             setup_task_scheduler.bat
echo ============================================================================
if /I not "%NO_PAUSE%"=="1" pause
