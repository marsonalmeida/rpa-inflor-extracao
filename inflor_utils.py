"""
INFLOR - Módulo Comum (VM Windows)
Funções compartilhadas entre os scripts de apontamento e modelo.

Centraliza: credenciais, S3, logging, Chrome driver, waits, PostgreSQL.
"""

import os
import sys
import json
import time
import logging
import boto3
from contextlib import contextmanager
from datetime import datetime
from logging.handlers import RotatingFileHandler

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
S3_BUCKET = os.environ.get("S3_BUCKET", "datalake-inflor-raw")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SECRET_NAME = os.environ.get("SECRET_NAME", "inflor/credentials")
DB_SECRET_NAME = os.environ.get("DB_SECRET_NAME", "inflor/db")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "inflor")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
CW_LOG_GROUP = os.environ.get("CLOUDWATCH_LOG_GROUP", "")

# Diretório base na VM Windows (adaptar se necessário)
BASE_DIR = os.environ.get("INFLOR_BASE_DIR", r"C:\inflor-extrator")
LOG_DIR = os.path.join(BASE_DIR, "logs")
DOWNLOAD_DIR_APONTAMENTO = os.path.join(BASE_DIR, "downloads", "apontamentos")
DOWNLOAD_DIR_MODELO = os.path.join(BASE_DIR, "downloads", "modelo")
OUTPUT_DIR_APONTAMENTO = os.environ.get("SAIDA_LOCAL_APONTAMENTO",
                                         os.path.join(BASE_DIR, "output", "apontamentos"))
OUTPUT_DIR_MODELO = os.environ.get("SAIDA_LOCAL_MODELO",
                                    os.path.join(BASE_DIR, "output", "modelo"))

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

class _RunAdapter(logging.LoggerAdapter):
    """Injeta [script][run=ID] em todas as mensagens automaticamente."""
    def process(self, msg, kwargs):
        return f"[{self.extra['script']}][run={self.extra['run_id']}] {msg}", kwargs


def setup_logging(script_name: str) -> _RunAdapter:
    """
    Configura logging com:
    - Run ID único por execução (para separar execuções no mesmo arquivo)
    - Rotação automática de arquivo (5 MB, mantém 3 backups)
    - Formato padronizado: timestamp | level | [script][run=ID] mensagem
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"{script_name}.log")

    logger = logging.getLogger(f"{script_name}.{run_id}")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    # Arquivo com rotação: 5MB por arquivo, mantém 3 backups
    fh = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024,
                             backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # stdout (Task Scheduler / terminal)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # CloudWatch Logs (opcional — só ativa se CLOUDWATCH_LOG_GROUP estiver configurado)
    if CW_LOG_GROUP:
        try:
            import watchtower
            cw = watchtower.CloudWatchLogHandler(
                log_group=CW_LOG_GROUP,
                stream_name=f"{script_name}/{run_id}",
                boto3_client=boto3.client("logs", region_name=AWS_REGION),
            )
            cw.setFormatter(fmt)
            logger.addHandler(cw)
        except Exception as e:
            logger.warning(f"CloudWatch handler não iniciado: {e}")

    adapter = _RunAdapter(logger, {"script": script_name, "run_id": run_id})
    adapter.run_id = run_id
    return adapter


@contextmanager
def log_step(log: _RunAdapter, nome: str):
    """
    Context manager que loga início, fim e duração de cada etapa.
    Em caso de erro, loga a falha e re-lança a exceção.

    Uso:
        with log_step(log, "Download"):
            ...
    """
    inicio = time.time()
    log.info(f">> {nome}")
    try:
        yield
        log.info(f"<< {nome} OK ({time.time() - inicio:.1f}s)")
    except Exception:
        log.error(f"!! {nome} FALHOU ({time.time() - inicio:.1f}s)")
        raise


def log_summary(log: _RunAdapter, script: str, inicio: float, **metricas):
    """
    Loga resumo final da execução com métricas consolidadas.

    Uso:
        log_summary(log, "apontamentos", t0, linhas=15234, destinos="S3+PostgreSQL")
    """
    duracao = time.time() - inicio
    mins, secs = divmod(int(duracao), 60)
    detalhes = " | ".join(f"{k}={v}" for k, v in metricas.items())
    log.info("=" * 60)
    log.info(f"RESUMO [{script}] | tempo={mins}m{secs:02d}s | {detalhes}")
    log.info("=" * 60)


def with_retry(func, retries: int = 3, delay: int = 30, log=None, label: str = ""):
    """
    Executa func() com retry automático em caso de falha transitória.
    Útil para operações de rede (S3, banco, credenciais).

    Uso:
        with_retry(lambda: upload_to_s3(...), retries=3, delay=30, log=log, label="Upload S3")
    """
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == retries:
                raise
            if log:
                log.warning(
                    f"[tentativa {attempt}/{retries}] {label} falhou: {e}. "
                    f"Retentando em {delay}s..."
                )
            time.sleep(delay)


# ---------------------------------------------------------------------------
# ALERTAS (SNS)
# ---------------------------------------------------------------------------

def send_alert(subject: str, message: str, log=None):
    """
    Envia alerta via AWS SNS.
    Silencioso se SNS_TOPIC_ARN não estiver configurado.
    """
    if not SNS_TOPIC_ARN:
        if log:
            log.warning("SNS_TOPIC_ARN não configurado — alerta não enviado")
        return
    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
        if log:
            log.info(f"Alerta SNS enviado: {subject}")
    except Exception as e:
        if log:
            log.warning(f"Falha ao enviar alerta SNS: {e}")


# ---------------------------------------------------------------------------
# CREDENCIAIS
# ---------------------------------------------------------------------------

def get_credentials(log: logging.Logger) -> tuple[str, str]:
    """
    Busca credenciais do AWS Secrets Manager.
    Fallback: variáveis de ambiente (pra teste local).
    """
    # Tenta Secrets Manager primeiro
    try:
        client = boto3.client("secretsmanager", region_name=AWS_REGION)
        response = client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(response["SecretString"])
        log.info("Credenciais obtidas do Secrets Manager")
        return secret["LOGIN_INFLOR"], secret["SENHA_INFLOR"]
    except Exception as e:
        log.warning(f"Secrets Manager indisponível ({e}), tentando variáveis de ambiente")

    # Fallback: variáveis de ambiente
    login = os.environ.get("LOGIN_INFLOR")
    senha = os.environ.get("SENHA_INFLOR")
    if login and senha:
        log.info("Credenciais obtidas de variáveis de ambiente")
        return login, senha

    # Fallback 2: arquivo .env (compatibilidade com processo atual)
    try:
        from decouple import config
        login = config("LOGIN_INFLOR")
        senha = config("SENHA_INFLOR")
        log.info("Credenciais obtidas do arquivo .env (decouple)")
        return login, senha
    except Exception:
        pass

    raise RuntimeError("Não foi possível obter credenciais. Configure Secrets Manager, env vars ou .env")


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def upload_to_s3(local_path: str, s3_key: str, log: logging.Logger, retries: int = 3):
    """Upload arquivo para S3 com retry automático."""
    def _upload():
        s3 = get_s3_client()
        s3.upload_file(local_path, S3_BUCKET, s3_key)

    log.info(f"Upload: {local_path} -> s3://{S3_BUCKET}/{s3_key}")
    try:
        with_retry(_upload, retries=retries, delay=20, log=log,
                   label=f"upload {os.path.basename(local_path)}")
        log.info(f"Upload concluído: {s3_key}")
    except Exception as e:
        log.error(f"Falha no upload para S3 após {retries} tentativas: {e}")
        raise


# ---------------------------------------------------------------------------
# DEBUG EM CASO DE ERRO
# ---------------------------------------------------------------------------

def screenshot_on_error(driver, step_name: str, s3_prefix: str, log: logging.Logger):
    """Salva screenshot localmente e no S3 para debug."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_dir = os.path.join(BASE_DIR, "debug")
        os.makedirs(debug_dir, exist_ok=True)

        # Screenshot
        screenshot_path = os.path.join(debug_dir, f"error_{step_name}_{timestamp}.png")
        driver.save_screenshot(screenshot_path)
        log.info(f"Screenshot salvo: {screenshot_path}")

        # HTML da página
        html_path = os.path.join(debug_dir, f"error_{step_name}_{timestamp}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        log.info(f"Page source salvo: {html_path}")

        # Tenta upload pro S3 (se falhar, pelo menos tem local)
        try:
            upload_to_s3(screenshot_path, f"{s3_prefix}/debug/error_{step_name}_{timestamp}.png", log)
            upload_to_s3(html_path, f"{s3_prefix}/debug/error_{step_name}_{timestamp}.html", log)
        except Exception:
            log.warning("Upload de debug pro S3 falhou, mas arquivos estão locais")

    except Exception as e:
        log.warning(f"Falha ao salvar debug: {e}")


# ---------------------------------------------------------------------------
# CHROME DRIVER
# ---------------------------------------------------------------------------

def create_driver(download_dir: str, log: logging.Logger, headless: bool = False):
    """
    Cria Chrome driver.
    Na VM Windows: usa chromedriver_autoinstaller (mantém compatibilidade).
    headless=False por padrão na VM (pode mudar pra True se não precisa de GUI).
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService

    chrome_options = webdriver.ChromeOptions()

    if headless:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")

    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    # chromedriver_autoinstaller: mantém do original
    import chromedriver_autoinstaller
    chromedriver_path = chromedriver_autoinstaller.install()
    log.info(f"Chromedriver: {chromedriver_path}")

    driver = webdriver.Chrome(
        service=ChromeService(chromedriver_path),
        options=chrome_options
    )
    return driver


# ---------------------------------------------------------------------------
# POSTGRESQL
# ---------------------------------------------------------------------------

def _get_db_params(log: logging.Logger) -> dict:
    """
    Busca parâmetros de conexão do banco.
    Ordem: Secrets Manager → variáveis de ambiente → .env
    """
    # Tenta Secrets Manager
    try:
        client = boto3.client("secretsmanager", region_name=AWS_REGION)
        response = client.get_secret_value(SecretId=DB_SECRET_NAME)
        params = json.loads(response["SecretString"])
        log.info("Credenciais do banco obtidas do Secrets Manager")
        return params
    except Exception as e:
        log.warning(f"Secrets Manager indisponível para DB ({e}), tentando variáveis de ambiente")

    # Fallback: variáveis de ambiente / .env
    try:
        from decouple import config as dconfig
        _get = dconfig
    except Exception:
        _get = lambda k, default=None: os.environ.get(k, default)

    params = {
        "DB_HOST":     _get("DB_HOST"),
        "DB_PORT":     _get("DB_PORT", "5432"),
        "DB_NAME":     _get("DB_NAME"),
        "DB_USER":     _get("DB_USER"),
        "DB_PASSWORD": _get("DB_PASSWORD"),
    }

    if not all([params["DB_HOST"], params["DB_NAME"], params["DB_USER"], params["DB_PASSWORD"]]):
        raise RuntimeError(
            "Credenciais do banco incompletas. "
            "Configure inflor/db no Secrets Manager ou DB_HOST/DB_NAME/DB_USER/DB_PASSWORD no .env"
        )

    log.info("Credenciais do banco obtidas de variáveis de ambiente / .env")
    return params


def get_db_engine(log: logging.Logger):
    """Cria SQLAlchemy engine para PostgreSQL."""
    from sqlalchemy import create_engine

    p = _get_db_params(log)
    # URL sem expor senha nos logs
    url = (
        f"postgresql+psycopg2://{p['DB_USER']}:{p['DB_PASSWORD']}"
        f"@{p['DB_HOST']}:{p['DB_PORT']}/{p['DB_NAME']}"
    )
    engine = create_engine(
        url,
        pool_pre_ping=True,   # verifica conexão antes de usar
        pool_size=2,
        max_overflow=0,
        connect_args={"connect_timeout": 10},
    )
    log.info(f"Engine PostgreSQL criado: {p['DB_HOST']}:{p['DB_PORT']}/{p['DB_NAME']}")
    return engine


def load_to_postgres(df, table: str, log: logging.Logger, schema: str = None):
    """
    Carga atômica na tabela fato do PostgreSQL.

    Estratégia full-refresh segura:
      1. Cria schema se não existir
      2. Se a tabela já existe → TRUNCATE
      3. INSERT de todos os dados
    Tudo dentro de uma única transação: se o INSERT falhar,
    o TRUNCATE é revertido automaticamente (rollback).
    """
    from sqlalchemy import text

    schema = schema or DB_SCHEMA
    engine = get_db_engine(log)

    with engine.begin() as conn:
        # Garante que o schema existe
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        # Verifica se a tabela já existe
        tabela_existe = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = :schema
                AND   table_name   = :table
            )
        """), {"schema": schema, "table": table}).scalar()

        if tabela_existe:
            conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))
            log.info(f"TRUNCATE em {schema}.{table}")

        df.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists="append",   # append pois já truncamos (ou tabela nova)
            index=False,
            method="multi",       # INSERT em lote (mais rápido)
            chunksize=500,
        )

    log.info(f"PostgreSQL: {len(df)} linhas inseridas em {schema}.{table}")


def registrar_execucao(script: str, run_id: str, inicio: float, status: str,
                        log=None, linhas: int = 0, erro: str = None, destinos: str = ""):
    """
    Grava resultado da execução na tabela de controle (inflor.controle_execucoes).
    Cria a tabela automaticamente se não existir.
    Nunca lança exceção — falha silenciosa com warning.
    """
    try:
        from sqlalchemy import text
        _log = log or logging.getLogger("inflor")
        engine = get_db_engine(_log)
        schema = DB_SCHEMA

        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS "{schema}".controle_execucoes (
                    id          SERIAL PRIMARY KEY,
                    script      VARCHAR(50),
                    run_id      VARCHAR(20),
                    inicio      TIMESTAMP,
                    fim         TIMESTAMP,
                    status      VARCHAR(10),
                    linhas      INTEGER,
                    erro        TEXT,
                    destinos    VARCHAR(200)
                )
            """))
            conn.execute(text(f"""
                INSERT INTO "{schema}".controle_execucoes
                    (script, run_id, inicio, fim, status, linhas, erro, destinos)
                VALUES
                    (:script, :run_id, :inicio, :fim, :status, :linhas, :erro, :destinos)
            """), {
                "script":   script,
                "run_id":   run_id,
                "inicio":   datetime.fromtimestamp(inicio),
                "fim":      datetime.now(),
                "status":   status,
                "linhas":   linhas,
                "erro":     erro,
                "destinos": destinos,
            })

        if log:
            log.info(f"Execução registrada em {schema}.controle_execucoes "
                     f"[status={status} | linhas={linhas}]")
    except Exception as e:
        if log:
            log.warning(f"Falha ao registrar execução no banco (não crítico): {e}")


# ---------------------------------------------------------------------------
# WAITS INTELIGENTES
# ---------------------------------------------------------------------------

def wait_for_download(directory: str, timeout: int = 300,
                      extension: str = ".zip", log: logging.Logger = None) -> str:
    """
    Espera download completar (substitui time.sleep fixo).
    Retorna o caminho do arquivo baixado.
    """
    start = time.time()
    while time.time() - start < timeout:
        files = [f for f in os.listdir(directory)
                 if f.endswith(extension) and not f.endswith(".crdownload")]
        crdownloads = [f for f in os.listdir(directory) if f.endswith(".crdownload")]

        if files and not crdownloads:
            path = os.path.join(directory, files[0])
            if log:
                log.info(f"Download concluído: {files[0]} ({time.time()-start:.0f}s)")
            return path

        time.sleep(5)

    raise TimeoutError(f"Download não completou em {timeout}s no diretório {directory}")


def wait_for_downloads_count(directory: str, expected_count: int,
                             timeout: int = 120, extension: str = ".xls",
                             log: logging.Logger = None) -> list:
    """Espera N arquivos de download."""
    start = time.time()
    while time.time() - start < timeout:
        files = [f for f in os.listdir(directory)
                 if f.endswith(extension) and not f.endswith(".crdownload")]
        crdownloads = [f for f in os.listdir(directory) if f.endswith(".crdownload")]

        if len(files) >= expected_count and not crdownloads:
            if log:
                log.info(f"{len(files)} arquivos prontos ({time.time()-start:.0f}s)")
            return files

        time.sleep(3)

    raise TimeoutError(f"Esperava {expected_count} arquivos, encontrou {len(files)} em {timeout}s")
