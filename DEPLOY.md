# INFLOR Extrator - Guia de Instalacao em Outra Maquina (Windows)

Este guia cobre o necessario para instalar e colocar em producao os scripts:
- `inflor_extracao_apontamento.py`
- `inflor_extracao_model.py`

Os dois scripts rodam via Task Scheduler e fazem o processamento completo de periodos em uma unica execucao diaria.

## 1. Pre-requisitos da maquina

Instale antes:

1. Python 3.11+
2. Google Chrome
3. AWS CLI v2

Validacoes rapidas:

```powershell
python --version
aws --version
```

## 2. Copiar projeto para a maquina

Exemplo de pasta de trabalho:

```text
C:\caminho\dos\arquivos\
  setup.bat
  setup_task_scheduler.bat
  inflor_utils.py
  inflor_extracao_apontamento.py
  inflor_extracao_model.py
  requirements.txt
```

## 3. Executar setup automatico

No Prompt/PowerShell como administrador:

```powershell
cd C:\caminho\dos\arquivos
.\setup.bat
```

O setup faz:
- cria `C:\inflor-extrator\` e subpastas
- copia scripts para `C:\inflor-extrator\src\`
- cria venv em `C:\inflor-extrator\.venv`
- instala dependencias no venv

## 4. Configurar credenciais INFLOR

Os scripts tentam nesta ordem:
1. `.env`
2. variaveis de ambiente
3. AWS Secrets Manager (`inflor/credentials`)

### Opcao recomendada: Secrets Manager

```powershell
aws secretsmanager create-secret --name inflor/credentials --secret-string "{\"LOGIN_INFLOR\":\"usuario@re.green\",\"SENHA_INFLOR\":\"SUA_SENHA\"}"
```

Se ja existir:

```powershell
aws secretsmanager put-secret-value --secret-id inflor/credentials --secret-string "{\"LOGIN_INFLOR\":\"usuario@re.green\",\"SENHA_INFLOR\":\"SUA_SENHA\"}"
```

### Opcao local (.env)

Crie `C:\inflor-extrator\src\.env`:

```dotenv
LOGIN_INFLOR=usuario@re.green
SENHA_INFLOR=sua_senha
LAKE_ENV=prd
DRY_RUN=false
```

Para testes sem upload ao lake:

```dotenv
DRY_RUN=true
```

Observacao sobre destinos por ambiente:
- `LAKE_ENV=prd` grava local em `C:\inflor-extrator\output\PRD\...` e S3 em `prd-monitoring-onedrive-sync/...`
- `LAKE_ENV=stg` grava local em `C:\inflor-extrator\output\STG\...` e S3 em `stg-monitoring-onedrive-sync/...`
- `DRY_RUN=true` grava somente local (sem upload S3)

## 5. Teste manual antes de agendar

```powershell
C:\inflor-extrator\.venv\Scripts\python.exe C:\inflor-extrator\src\inflor_extracao_apontamento.py
C:\inflor-extrator\.venv\Scripts\python.exe C:\inflor-extrator\src\inflor_extracao_model.py
```

Conferir:
- logs em `C:\inflor-extrator\logs\`
- saidas em `C:\inflor-extrator\output\PRD\...` ou `C:\inflor-extrator\output\STG\...` (conforme `LAKE_ENV`)
- consolidado final:
  - `C:\inflor-extrator\output\<PRD|STG>\apontamentos\Painel de monitoramento\Operações\Atividades executadas\Apontamento de atividades.xlsx`
  - `C:\inflor-extrator\output\<PRD|STG>\apontamentos\Painel de monitoramento\Operações\Detalhamento de Talhões\Inflor\base.xlsx`

## 6. Criar agendamento no Windows

```powershell
cd C:\caminho\dos\arquivos
.\setup_task_scheduler.bat
```

Tarefas criadas:
- `INFLOR\Extracao Modelo` (08:00)
- `INFLOR\Extracao Apontamentos` (10:00)

As tarefas usam o Python da venv:
- `C:\inflor-extrator\.venv\Scripts\python.exe`

## 7. Validacao pos-deploy

### Ver ultima execucao das tarefas

```powershell
schtasks /query /tn "INFLOR\Extracao Apontamentos" /fo LIST
schtasks /query /tn "INFLOR\Extracao Modelo" /fo LIST
```

### Rodar tarefa manualmente

```powershell
schtasks /run /tn "INFLOR\Extracao Apontamentos"
schtasks /run /tn "INFLOR\Extracao Modelo"
```

### Verificar upload no lake

```powershell
aws s3 ls s3://re.green-assets/prd-monitoring-onedrive-sync/ --recursive
aws s3 ls s3://re.green-assets/stg-monitoring-onedrive-sync/ --recursive
```

## 8. Checklist final (go-live)

- Python, Chrome e AWS CLI instalados
- `setup.bat` executado sem erros
- credenciais INFLOR configuradas
- teste manual OK dos dois scripts
- tarefas do Scheduler criadas e visiveis
- logs sem erro em `C:\inflor-extrator\logs\`
- arquivos finais consolidados gerados
- upload no S3 confirmado (ou `DRY_RUN=true` em homologacao)

## 9. Troubleshooting rapido

### Task Scheduler falha com python nao encontrado
- Reexecute `setup.bat` e `setup_task_scheduler.bat`
- Confirme existencia de `C:\inflor-extrator\.venv\Scripts\python.exe`

### Erro de credenciais INFLOR
- Verifique `.env` em `C:\inflor-extrator\src\`
- Ou valide segredo:

```powershell
aws secretsmanager get-secret-value --secret-id inflor/credentials
```

### Upload S3 falha
- Verifique `aws configure`
- Verifique permissoes IAM (`s3:PutObject` no bucket alvo)
- Em homologacao, use `DRY_RUN=true`
