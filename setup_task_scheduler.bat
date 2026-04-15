@echo off
setlocal

set "BASE_DIR=C:\inflor-extrator"
set "PYTHON_EXE=%BASE_DIR%\.venv\Scripts\python.exe"
set "SRC_DIR=%BASE_DIR%\src"
set "NO_PAUSE=%INFLOR_NO_PAUSE%"

if not exist "%PYTHON_EXE%" (
    echo ERRO: Python da venv nao encontrado em %PYTHON_EXE%
    echo Execute setup.bat antes de configurar o Task Scheduler.
    if /I not "%NO_PAUSE%"=="1" pause
    exit /b 1
)

REM ============================================================================
REM INFLOR Extrator - Configuração do Task Scheduler
REM Cria duas tarefas agendadas: Apontamentos e Modelo
REM Executa como SYSTEM para não depender de usuário logado
REM ============================================================================

echo Criando tarefa: INFLOR - Apontamentos (diário 10:00)...
schtasks /create /tn "INFLOR\Extracao Apontamentos" ^
    /tr "\"%PYTHON_EXE%\" \"%SRC_DIR%\inflor_extracao_apontamento.py\"" ^
    /sc daily ^
    /st 10:00 ^
    /ru SYSTEM ^
    /rl HIGHEST ^
    /f

echo.
echo Criando tarefa: INFLOR - Modelo (diário 08:00)...
schtasks /create /tn "INFLOR\Extracao Modelo" ^
    /tr "\"%PYTHON_EXE%\" \"%SRC_DIR%\inflor_extracao_model.py\"" ^
    /sc daily ^
    /st 08:00 ^
    /ru SYSTEM ^
    /rl HIGHEST ^
    /f

echo.
echo ============================================================================
echo Tarefas criadas! Verifique no Task Scheduler:
echo   - INFLOR\Extracao Modelo         (diário 08:00 - SYSTEM)
echo   - INFLOR\Extracao Apontamentos  (diário 10:00 - SYSTEM)
echo.
echo Para testar manualmente:
echo   schtasks /run /tn "INFLOR\Extracao Apontamentos"
echo   schtasks /run /tn "INFLOR\Extracao Modelo"
echo.
echo Para verificar última execução:
echo   schtasks /query /tn "INFLOR\Extracao Apontamentos" /fo LIST
echo   schtasks /query /tn "INFLOR\Extracao Modelo" /fo LIST
echo.
echo Para remover:
echo   schtasks /delete /tn "INFLOR\Extracao Apontamentos" /f
echo   schtasks /delete /tn "INFLOR\Extracao Modelo" /f
echo ============================================================================
if /I not "%NO_PAUSE%"=="1" pause
