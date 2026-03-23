# INFLOR Extração - Deploy na VM Windows AWS

## Visão Geral

Scripts Python rodando na VM Windows com Task Scheduler.
Extrai dados do INFLOR, salva localmente (compatibilidade) e envia pro S3 (datalake).

```
Task Scheduler (cron) → Python + Chrome → INFLOR (scraping)
                                            ↓
                         Salva local (XLSX) + Upload S3 (Parquet + XLSX)
                                                      ↓
                                              Glue/Lambda → Tabela Fato
```

## Estrutura do Projeto

```
C:\inflor-extrator\
├── src\
│   ├── inflor_utils.py                 # Módulo comum (credenciais, S3, Chrome, logging)
│   ├── inflor_extracao_apontamento.py  # Script: Apontamentos
│   └── inflor_extracao_model.py        # Script: Modelo/Cubo
├── logs\                               # Logs de execução
├── downloads\                          # Arquivos temporários de download
│   ├── apontamentos\
│   └── modelo\
├── debug\                              # Screenshots e HTML em caso de erro
└── requirements.txt
```

## Passo a Passo

### PASSO 1: Preparar a VM (uma vez)

1. **Instalar Python 3.11+**
   - Baixar de https://python.org
   - Marcar "Add to PATH" na instalação

2. **Instalar Google Chrome**
   - Baixar de https://google.com/chrome
   - O `chromedriver_autoinstaller` cuida do driver

3. **Instalar AWS CLI**
   - Baixar de https://aws.amazon.com/cli/
   - Configurar: `aws configure`
     - Access Key ID: (da IAM role/user)
     - Secret Access Key: (da IAM role/user)
     - Region: us-east-1

4. **Executar setup**
   ```cmd
   cd C:\caminho\dos\arquivos
   setup.bat
   ```

### PASSO 2: Configurar credenciais (uma vez)

**Opção A — Secrets Manager (recomendado):**
```cmd
aws secretsmanager create-secret ^
    --name inflor/credentials ^
    --secret-string "{\"LOGIN_INFLOR\":\"usuario@re.green\",\"SENHA_INFLOR\":\"sua_senha\"}"
```

**Opção B — Variáveis de ambiente (alternativa):**
```cmd
setx LOGIN_INFLOR "usuario@re.green"
setx SENHA_INFLOR "sua_senha"
```

**Opção C — Arquivo .env (compatibilidade com original):**
Criar arquivo `.env` em `C:\inflor-extrator\`:
```
LOGIN_INFLOR=usuario@re.green
SENHA_INFLOR=sua_senha
```

O script tenta na ordem: Secrets Manager → env vars → .env

### PASSO 3: Teste manual

```cmd
cd C:\inflor-extrator
python src\inflor_extracao_apontamento.py
```

Observar:
- Login funciona?
- Navegação até o relatório funciona?
- Download completa?
- Upload pro S3 funciona? (`aws s3 ls s3://datalake-inflor-raw/inflor/`)

Se falhar, checar `C:\inflor-extrator\logs\` e `C:\inflor-extrator\debug\`.

### PASSO 4: Agendar no Task Scheduler

```cmd
setup_task_scheduler.bat
```

Isso cria duas tarefas:
- **Apontamentos**: diário às 07:00
- **Modelo**: diário às 19:00

### PASSO 5: Validar dados no S3

```cmd
aws s3 ls s3://datalake-inflor-raw/inflor/apontamentos/ --recursive
aws s3 ls s3://datalake-inflor-raw/inflor/modelo/ --recursive
```

### PASSO 6: Desligar processo antigo

Depois de 3-5 execuções bem-sucedidas:
1. Para o script na máquina do operador antigo
2. Remove/desativa o flow do NiFi (se só servia este fluxo)

---

## O que mudou vs. o original

| Aspecto | Original | Novo |
|---------|----------|------|
| Onde roda | Máquina do operador | VM Windows AWS |
| Credenciais | `.env` local | Secrets Manager (com fallback) |
| Saída | Só local (OneDrive/pasta) | Local + S3 (Parquet + XLSX) |
| Agendamento | Manual ou task scheduler local | Task Scheduler na VM |
| Logs | Arquivo `.log` básico | Log estruturado (arquivo + stdout) |
| Debug em falha | Nada | Screenshot + HTML salvos local + S3 |
| Download wait | `time.sleep(120)` fixo | Polling ativo com timeout |
| Código | 2 scripts monolíticos | 3 arquivos (utils + 2 scripts) |
| Seletores Selenium | Mix de XPath e ID | Preferência por ID onde possível |

## O que NÃO mudou (propositalmente)

- **Lógica de navegação**: mesmos cliques, mesma ordem, mesmos XPaths
- **Processamento de dados**: mesmas conversões, mesmos filtros
- **Saída local**: mantida nos mesmos caminhos pra compatibilidade
- **chromedriver_autoinstaller**: mantido pra VM Windows
- **Lista de períodos (modelo)**: mantida hardcoded

---

## Próximos passos (melhorias futuras)

1. **Períodos dinâmicos**: calcular automaticamente ao invés de hardcode
2. **Headless**: rodar com `HEADLESS=true` se não precisa de GUI
3. **Alertas**: CloudWatch Agent na VM + SNS pra alertar falhas
4. **Eliminar NiFi**: se o S3 já é o destino, NiFi vira desnecessário
5. **Ingestão direto do S3**: Lambda/Glue trigger no S3 → tabela fato

---

## Troubleshooting

### Chrome não abre / chromedriver falha
- Verifique se Chrome está instalado e atualizado
- `chromedriver_autoinstaller` baixa o driver correto automaticamente
- Se bloqueado por proxy/firewall, baixe manualmente e coloque em PATH

### Login falha
- Verifique credenciais no Secrets Manager: `aws secretsmanager get-secret-value --secret-id inflor/credentials`
- O INFLOR pode ter mudado a tela de login

### Download não completa (timeout)
- O relatório de 120 meses pode ser grande
- Aumente `timeout` em `wait_for_download()` no código
- Verifique espaço em disco em `C:\inflor-extrator\downloads\`

### Upload S3 falha
- Verifique `aws configure` (credenciais AWS)
- Verifique se o bucket existe: `aws s3 ls s3://datalake-inflor-raw/`
- Verifique permissões IAM (s3:PutObject no bucket)

### Script de modelo demora muito
- 14 períodos × ~1-2 min cada = 15-30 min é normal
- Se > 45 min, o INFLOR pode estar lento

### Onde estão os logs de erro?
- `C:\inflor-extrator\logs\inflor_apontamentos.log`
- `C:\inflor-extrator\logs\inflor_modelo.log`
- `C:\inflor-extrator\debug\` (screenshots e HTML de páginas com erro)
