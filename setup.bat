@echo off
REM ============================================================================
REM INFLOR Extrator - Setup na VM Windows
REM Executa uma vez para configurar o ambiente
REM ============================================================================

echo [1/8] Criando estrutura de diretórios...
mkdir C:\inflor-extrator\src 2>nul
mkdir C:\inflor-extrator\logs 2>nul
mkdir C:\inflor-extrator\downloads\apontamentos 2>nul
mkdir C:\inflor-extrator\downloads\modelo 2>nul
mkdir C:\inflor-extrator\output\apontamentos 2>nul
mkdir C:\inflor-extrator\output\modelo 2>nul
mkdir C:\inflor-extrator\debug 2>nul

echo [2/8] Copiando scripts e configurações...
copy /Y inflor_utils.py C:\inflor-extrator\src\
copy /Y inflor_extracao_apontamento.py C:\inflor-extrator\src\
copy /Y inflor_extracao_model.py C:\inflor-extrator\src\
copy /Y requirements.txt C:\inflor-extrator\
copy /Y .env C:\inflor-extrator\src\

echo [3/8] Instalando dependências Python...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo ERRO: Falha ao instalar dependências. Verifique se o Python está instalado.
    pause
    exit /b 1
)

echo [4/8] Verificando AWS CLI...
aws --version >nul 2>&1
if %errorlevel% neq 0 (
    echo AVISO: AWS CLI não encontrado. Instale em https://aws.amazon.com/cli/
    echo Após instalar, execute: aws configure
) else (
    echo AWS CLI OK. Verifique se está configurado: aws configure list
)

echo [5/8] Credenciais INFLOR no Secrets Manager...
echo Execute o comando abaixo para criar/atualizar o secret:
echo.
echo   aws secretsmanager create-secret ^
echo       --name inflor/credentials ^
echo       --secret-string "{\"LOGIN_INFLOR\":\"lucas.castro@re.green\",\"SENHA_INFLOR\":\"SUA_SENHA\"}"
echo.
echo   (Se já existe, use put-secret-value no lugar de create-secret)
echo.

echo [6/8] Credenciais PostgreSQL no Secrets Manager...
echo Execute o comando abaixo após ter as credenciais do banco:
echo.
echo   aws secretsmanager create-secret ^
echo       --name inflor/db ^
echo       --secret-string "{\"DB_HOST\":\"HOST\",\"DB_PORT\":\"5432\",\"DB_NAME\":\"DB\",\"DB_USER\":\"USER\",\"DB_PASSWORD\":\"PASS\"}"
echo.

echo [7/7] Configurando CloudWatch Logs (opcional)...
echo Para habilitar logs no CloudWatch, execute:
echo.
echo   aws logs create-log-group --log-group-name /inflor/extracao
echo.
echo   Depois preencha CLOUDWATCH_LOG_GROUP=/inflor/extracao no .env
echo.

echo ============================================================================
echo Setup concluído!
echo.
echo Próximos passos:
echo   1. Configure AWS CLI:            aws configure
echo   2. Crie os secrets (passos 5 e 6 acima)
echo   3. Configure CloudWatch (passo 7 acima, opcional)
echo   4. Teste manual:
echo        python C:\inflor-extrator\src\inflor_extracao_apontamento.py
echo        python C:\inflor-extrator\src\inflor_extracao_model.py
echo   5. Agende as tarefas:             setup_task_scheduler.bat
echo ============================================================================
pause
