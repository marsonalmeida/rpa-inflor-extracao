"""
INFLOR - Extração de Apontamentos (VM Windows)

Executa na VM Windows via Task Scheduler.
Melhoria sobre o original:
- Credenciais via Secrets Manager (com fallback pra .env)
- Upload pro S3 (mantém cópia local também)
- Screenshot + HTML em caso de erro
- Wait inteligente no download (não mais sleep 120 fixo)
- Logging estruturado (arquivo + stdout)
- Cópia local mantida para compatibilidade com NiFi/OneDrive
"""

import os
import sys
import time
import shutil
import zipfile
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from inflor_utils import (
    setup_logging, log_step, log_summary,
    get_credentials, upload_to_s3, screenshot_on_error,
    create_driver, wait_for_download, load_to_postgres,
    registrar_execucao,
    DOWNLOAD_DIR_APONTAMENTO, OUTPUT_DIR_APONTAMENTO, BASE_DIR, S3_BUCKET
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
S3_PREFIX = "inflor/apontamentos"
MESES_RETROATIVOS = int(os.environ.get("MESES_RETROATIVOS", "120"))
HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"

# Caminho local de saída — configurável via .env (SAIDA_LOCAL_APONTAMENTO)
# Default: C:\inflor-extrator\output\apontamentos (não depende de usuário logado)
SAIDA_LOCAL = OUTPUT_DIR_APONTAMENTO

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    log = setup_logging("inflor_apontamentos")
    t0 = time.time()
    log.info("=" * 60)
    log.info("INFLOR EXTRAÇÃO APONTAMENTOS - VM WINDOWS")
    log.info("=" * 60)

    driver = None
    try:
        with log_step(log, "Credenciais"):
            login, senha = get_credentials(log)

        # Prepara diretório de download
        if os.path.exists(DOWNLOAD_DIR_APONTAMENTO):
            shutil.rmtree(DOWNLOAD_DIR_APONTAMENTO)
        os.makedirs(DOWNLOAD_DIR_APONTAMENTO, exist_ok=True)

        # Período
        data_hoje = datetime.now()
        dtmenos = data_hoje - relativedelta(months=MESES_RETROATIVOS)
        datain = dtmenos.strftime("%d/%m/%Y")
        datafim = data_hoje.strftime("%d/%m/%Y")
        log.info(f"Período: {datain} a {datafim}")

        with log_step(log, "Iniciar Chrome"):
            driver = create_driver(DOWNLOAD_DIR_APONTAMENTO, log, headless=HEADLESS)

        # ---------------------------------------------------------------
        # LOGIN
        # ---------------------------------------------------------------
        with log_step(log, "Login INFLOR"):
            driver.get("https://regreen.inflor.cloud/SGF/Default.aspx?")
            campoLogin = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.ID, "txtLogin"))
            )
            campoSenha = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "txtSenha"))
            )
            campoLogin.send_keys(login)
            campoSenha.send_keys(senha)
            campoSenha.send_keys(Keys.RETURN)
            time.sleep(5)

        # ---------------------------------------------------------------
        # NAVEGAÇÃO + FILTROS + EXPORTAR
        # ---------------------------------------------------------------
        with log_step(log, "Navegação e exportação"):
            driver.get("https://regreen.inflor.cloud/SGF/DefaultSilviculturaControle.aspx")

            BtRelatorios = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, "//div[normalize-space()='Relatórios']"))
            )
            BtRelatorios.click()

            BtApontamentos = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH,
                    "/html[1]/body[1]/table[1]/tbody[1]/tr[1]/td[1]/table[2]/tbody[1]"
                    "/tr[1]/td[1]/form[1]/table[8]/tbody[1]/tr[2]/td[1]"))
            )
            BtApontamentos.click()

            BtConsulta = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH,
                    "/html[1]/body[1]/table[1]/tbody[1]/tr[1]/td[1]/table[2]/tbody[1]"
                    "/tr[1]/td[1]/form[1]/table[5]/tbody[1]/tr[2]/td[1]"))
            )
            BtConsulta.click()

            iframe = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "conteudo"))
            )
            driver.switch_to.frame(iframe)

            dtinicial = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "txtDataIni"))
            )
            dtinicial.send_keys(datain)

            dtfinal = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "txtDataFim"))
            )
            dtfinal.send_keys(datafim)

            Btfiltro = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "btnGerar"))
            )
            Btfiltro.click()
            time.sleep(15)

            Btdetalhes = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@title='Exibir colunas extras']"))
            )
            Btdetalhes.click()
            time.sleep(30)

            BtExport = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@title='Exportar dados para Excel']"))
            )
            BtExport.click()

        with log_step(log, "Download"):
            zip_path = wait_for_download(
                DOWNLOAD_DIR_APONTAMENTO, timeout=300, extension=".zip", log=log
            )

        # ---------------------------------------------------------------
        # LOGOUT
        # ---------------------------------------------------------------
        with log_step(log, "Logout"):
            driver.switch_to.default_content()
            driver.get("https://regreen.inflor.cloud/SGF/DefaultModulos.aspx")
            BtLogout = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "btnLogOut"))
            )
            BtLogout.click()
            driver.quit()
            driver = None

        # ---------------------------------------------------------------
        # UNZIP + PROCESSAR
        # ---------------------------------------------------------------
        with log_step(log, "Unzip e processamento"):
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(DOWNLOAD_DIR_APONTAMENTO)

            arquivos_xls = [f for f in os.listdir(DOWNLOAD_DIR_APONTAMENTO)
                            if f.endswith(".xls") or f.endswith(".xlsx")]
            if not arquivos_xls:
                raise FileNotFoundError("Nenhum XLS encontrado após unzip")

            arquivo_final = os.path.join(DOWNLOAD_DIR_APONTAMENTO, arquivos_xls[0])
            df = pd.read_html(arquivo_final, flavor="html5lib",
                              index_col=None, thousands=".", decimal=",")[0]
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)

            colunas_numericas = [
                "Custo Unitário Recurso", "Custo Recurso", "Rendimento Previsto",
                "Rendimento Real", "Quantidade", "% Realização", "Valor Produção",
                "Custo Total Operação", "Custo Unitário Operação", "Uso Solo",
                "Área (ha)", "Custo Boletim"
            ]
            for col in colunas_numericas:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            if "Tipo Aprovação" in df.columns:
                df = df[df["Tipo Aprovação"].isin(["Aprovado", "Indefinido"])]

            log.info(f"Linhas processadas: {len(df)}")

        # ---------------------------------------------------------------
        # SALVAR LOCAL
        # ---------------------------------------------------------------
        with log_step(log, "Salvar local"):
            os.makedirs(SAIDA_LOCAL, exist_ok=True)
            arquivo_local = os.path.join(SAIDA_LOCAL, "fat_apontamentos_automatico.xlsx")
            df.to_excel(arquivo_local, index=False)
            log.info(f"Destino: {arquivo_local}")

        # ---------------------------------------------------------------
        # UPLOAD S3
        # ---------------------------------------------------------------
        with log_step(log, "Upload S3"):
            timestamp = datetime.now().strftime("%Y-%m-%d")
            year, month, day = datetime.now().strftime("%Y"), datetime.now().strftime("%m"), datetime.now().strftime("%d")

            parquet_local = os.path.join(DOWNLOAD_DIR_APONTAMENTO, f"apontamentos_{timestamp}.parquet")
            df.to_parquet(parquet_local, index=False, engine="pyarrow")
            upload_to_s3(parquet_local, f"{S3_PREFIX}/year={year}/month={month}/day={day}/apontamentos.parquet", log)

            xlsx_s3 = os.path.join(DOWNLOAD_DIR_APONTAMENTO, "fat_apontamentos_automatico.xlsx")
            df.to_excel(xlsx_s3, index=False)
            upload_to_s3(xlsx_s3, f"{S3_PREFIX}/xlsx/fat_apontamentos_automatico.xlsx", log)

        # ---------------------------------------------------------------
        # POSTGRESQL
        # ---------------------------------------------------------------
        with log_step(log, "PostgreSQL"):
            load_to_postgres(df, "fato_apontamentos", log)

        log_summary(log, "apontamentos", t0,
                    linhas=len(df),
                    periodo=f"{datain} a {datafim}",
                    destinos="local+S3+PostgreSQL")

        registrar_execucao(
            script="apontamentos", run_id=log.run_id, inicio=t0,
            status="SUCESSO", linhas=len(df), destinos="local+S3+PostgreSQL", log=log,
        )

    except Exception as e:
        log.error(f"FALHA NA PIPELINE: {e}", exc_info=True)
        if driver:
            screenshot_on_error(driver, "apontamento", S3_PREFIX, log)
            driver.quit()

        registrar_execucao(
            script="apontamentos", run_id=log.run_id, inicio=t0,
            status="FALHA", erro=str(e), log=log,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
