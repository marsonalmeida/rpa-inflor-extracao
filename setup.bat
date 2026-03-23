@echo off
REM ============================================================================
REM INFLOR Extrator - Setup na VM Windows
REM Executa uma vez para configurar o ambiente
REM ============================================================================

echo [1/5] Criando estrutura de diretórios...
mkdir C:\inflor-extrator\src 2>nul
mkdir C:\inflor-extrator\logs 2>nul
mkdir C:\inflor-extrator\downloads\apontamentos 2>nul
mkdir C:\inflor-extrator\downloads\modelo 2>nul
mkdir C:\inflor-extrator\debug 2>nul

echo [2/5] Copiando scripts...
copy /Y src\*.py C:\inflor-extrator\src\
copy /Y requirements.txt C:\inflor-extrator\

echo [3/5] Instalando dependências Python...
pip install -r requirements.txt

echo [4/5] Configurando AWS CLI...
echo Verifique se o AWS CLI está instalado e configurado:
echo   aws configure
echo   - Access Key ID
echo   - Secret Access Key  
echo   - Region: us-east-1
echo.

echo [5/5] Inserindo credenciais no Secrets Manager...
echo Execute manualmente:
echo   aws secretsmanager create-secret --name inflor/credentials --secret-string "{\"LOGIN_INFLOR\":\"SEU_USER\",\"SENHA_INFLOR\":\"SUA_SENHA\"}"
echo.
echo Ou se o secret já existe:
echo   aws secretsmanager put-secret-value --secret-id inflor/credentials --secret-string "{\"LOGIN_INFLOR\":\"SEU_USER\",\"SENHA_INFLOR\":\"SUA_SENHA\"}"
echo.

echo ============================================================================
echo Setup concluído!
echo.
echo Próximos passos:
echo   1. Configure o AWS CLI (aws configure)
echo   2. Insira credenciais no Secrets Manager (comando acima)
echo   3. Teste manual: python C:\inflor-extrator\src\inflor_extracao_apontamento.py
echo   4. Configure o Task Scheduler (veja setup_task_scheduler.bat)
echo ============================================================================
pause
