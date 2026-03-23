@echo off
REM ============================================================================
REM INFLOR Extrator - Configuração do Task Scheduler
REM Cria duas tarefas agendadas: Apontamentos e Modelo
REM ============================================================================

echo Criando tarefa: INFLOR - Apontamentos (diário 07:00)...
schtasks /create /tn "INFLOR\Extracao Apontamentos" ^
    /tr "python C:\inflor-extrator\src\inflor_extracao_apontamento.py" ^
    /sc daily ^
    /st 07:00 ^
    /rl HIGHEST ^
    /f

echo.
echo Criando tarefa: INFLOR - Modelo (diário 19:00)...
schtasks /create /tn "INFLOR\Extracao Modelo" ^
    /tr "python C:\inflor-extrator\src\inflor_extracao_model.py" ^
    /sc daily ^
    /st 19:00 ^
    /rl HIGHEST ^
    /f

echo.
echo ============================================================================
echo Tarefas criadas! Verifique no Task Scheduler:
echo   - INFLOR\Extracao Apontamentos  (diário 07:00)
echo   - INFLOR\Extracao Modelo         (diário 19:00)
echo.
echo Para testar manualmente:
echo   schtasks /run /tn "INFLOR\Extracao Apontamentos"
echo   schtasks /run /tn "INFLOR\Extracao Modelo"
echo.
echo Para remover:
echo   schtasks /delete /tn "INFLOR\Extracao Apontamentos" /f
echo   schtasks /delete /tn "INFLOR\Extracao Modelo" /f
echo ============================================================================
pause
