"""
INFLOR - Módulo Comum (VM Windows)
Funções compartilhadas entre os scripts de apontamento e modelo.

Centraliza: credenciais, S3, logging, Chrome driver, waits.
"""

import os
import sys
import json
import time
import logging
import boto3
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
S3_BUCKET = os.environ.get("S3_BUCKET", "datalake-inflor-raw")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SECRET_NAME = os.environ.get("SECRET_NAME", "inflor/credentials")

# Diretório base na VM Windows (adaptar se necessário)
BASE_DIR = os.environ.get("INFLOR_BASE_DIR", r"C:\inflor-extrator")
LOG_DIR = os.path.join(BASE_DIR, "logs")
DOWNLOAD_DIR_APONTAMENTO = os.path.join(BASE_DIR, "downloads", "apontamentos")
DOWNLOAD_DIR_MODELO = os.path.join(BASE_DIR, "downloads", "modelo")

# ---------------------------------------------------------------------------
# LOGGING (arquivo local + stdout + CloudWatch opcional)
# ---------------------------------------------------------------------------

def setup_logging(script_name: str) -> logging.Logger:
    """Configura logging para arquivo local + stdout."""
    os.makedirs(LOG_DIR, exist_ok=True)

    log_file = os.path.join(LOG_DIR, f"{script_name}.log")

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.INFO)

    # Formato
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # Handler: arquivo (rotação manual - mantém últimas 5MB)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Handler: stdout (visível no Task Scheduler / terminal)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


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


def upload_to_s3(local_path: str, s3_key: str, log: logging.Logger):
    """Upload arquivo para S3."""
    try:
        s3 = get_s3_client()
        log.info(f"Upload: {local_path} -> s3://{S3_BUCKET}/{s3_key}")
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        log.info(f"Upload concluído: {s3_key}")
    except Exception as e:
        log.error(f"Falha no upload para S3: {e}")
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
