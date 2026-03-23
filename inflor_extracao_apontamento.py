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
    setup_logging, get_credentials, upload_to_s3, screenshot_on_error,
    create_driver, wait_for_download,
    DOWNLOAD_DIR_APONTAMENTO, BASE_DIR, S3_BUCKET
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
S3_PREFIX = "inflor/apontamentos"
MESES_RETROATIVOS = int(os.environ.get("MESES_RETROATIVOS", "120"))
HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"

# Caminho local de saída (mantém compatibilidade com NiFi/OneDrive)
# Ajuste conforme necessidade:
USER = os.environ.get("USERNAME") or os.environ.get("USER")
SAIDA_LOCAL = os.environ.get(
    "SAIDA_LOCAL",
    os.path.join(r"C:\Users", USER,
                 r"Regreen\- Operacional - Documentos\OPERAÇÃO\01. PMO OPERAÇÂO"
                 r"\03.FUP Mensal\01.Resultado Operacional\04.Bases_Portal_Indicadores")
)

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    log = setup_logging("inflor_apontamentos")
    log.info("=" * 60)
    log.info("INFLOR EXTRAÇÃO APONTAMENTOS - VM WINDOWS")
    log.info("=" * 60)

    driver = None
    try:
        # Credenciais
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

        # Chrome
        driver = create_driver(DOWNLOAD_DIR_APONTAMENTO, log, headless=HEADLESS)

        # ---------------------------------------------------------------
        # LOGIN
        # ---------------------------------------------------------------
        log.info("Acessando página de login")
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
        log.info("Login enviado")
        time.sleep(5)

        # ---------------------------------------------------------------
        # NAVEGAÇÃO ATÉ RELATÓRIO
        # ---------------------------------------------------------------
        log.info("Navegando para Silvicultura e Controle")
        driver.get("https://regreen.inflor.cloud/SGF/DefaultSilviculturaControle.aspx")

        log.info("Clicando em Relatórios")
        BtRelatorios = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, "//div[normalize-space()='Relatórios']"))
        )
        BtRelatorios.click()

        log.info("Clicando em Apontamentos")
        BtApontamentos = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH,
                "/html[1]/body[1]/table[1]/tbody[1]/tr[1]/td[1]/table[2]/tbody[1]"
                "/tr[1]/td[1]/form[1]/table[8]/tbody[1]/tr[2]/td[1]"))
        )
        BtApontamentos.click()

        log.info("Clicando em Consulta Boletins e Apontamentos Geral")
        BtConsulta = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH,
                "/html[1]/body[1]/table[1]/tbody[1]/tr[1]/td[1]/table[2]/tbody[1]"
                "/tr[1]/td[1]/form[1]/table[5]/tbody[1]/tr[2]/td[1]"))
        )
        BtConsulta.click()

        # ---------------------------------------------------------------
        # IFRAME + FILTROS
        # ---------------------------------------------------------------
        log.info("Entrando no iframe")
        iframe = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "conteudo"))
        )
        driver.switch_to.frame(iframe)

        log.info(f"Data inicial: {datain}")
        dtinicial = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "txtDataIni"))
        )
        dtinicial.send_keys(datain)

        log.info(f"Data final: {datafim}")
        dtfinal = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "txtDataFim"))
        )
        dtfinal.send_keys(datafim)

        # ---------------------------------------------------------------
        # GERAR + EXPANDIR + EXPORTAR
        # ---------------------------------------------------------------
        log.info("Gerando relatório")
        Btfiltro = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "btnGerar"))
        )
        Btfiltro.click()
        time.sleep(15)

        log.info("Expandindo colunas ocultas")
        Btdetalhes = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@title='Exibir colunas extras']"))
        )
        Btdetalhes.click()
        time.sleep(30)

        log.info("Exportando para Excel")
        BtExport = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@title='Exportar dados para Excel']"))
        )
        BtExport.click()

        # Wait inteligente (substitui sleep 120)
        log.info("Aguardando download...")
        zip_path = wait_for_download(
            DOWNLOAD_DIR_APONTAMENTO, timeout=300, extension=".zip", log=log
        )

        # ---------------------------------------------------------------
        # LOGOUT
        # ---------------------------------------------------------------
        log.info("Fazendo logout")
        driver.switch_to.default_content()
        driver.get("https://regreen.inflor.cloud/SGF/DefaultModulos.aspx")
        BtLogout = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "btnLogOut"))
        )
        BtLogout.click()
        log.info("Logout concluído")
        driver.quit()
        driver = None

        # ---------------------------------------------------------------
        # UNZIP
        # ---------------------------------------------------------------
        log.info(f"Descompactando: {zip_path}")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR_APONTAMENTO)

        # ---------------------------------------------------------------
        # PROCESSAR
        # ---------------------------------------------------------------
        log.info("Processando arquivo extraído")
        arquivos_xls = [f for f in os.listdir(DOWNLOAD_DIR_APONTAMENTO)
                        if f.endswith(".xls") or f.endswith(".xlsx")]
        if not arquivos_xls:
            raise FileNotFoundError("Nenhum XLS encontrado após unzip")

        arquivo_final = os.path.join(DOWNLOAD_DIR_APONTAMENTO, arquivos_xls[0])
        log.info(f"Lendo: {arquivo_final}")

        # INFLOR exporta HTML disfarçado de XLS
        df = pd.read_html(arquivo_final, flavor="html5lib",
                          index_col=None, thousands=".", decimal=",")[0]
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)

        # Conversões numéricas
        colunas_numericas = [
            "Custo Unitário Recurso", "Custo Recurso", "Rendimento Previsto",
            "Rendimento Real", "Quantidade", "% Realização", "Valor Produção",
            "Custo Total Operação", "Custo Unitário Operação", "Uso Solo",
            "Área (ha)", "Custo Boletim"
        ]
        for col in colunas_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Filtro
        if "Tipo Aprovação" in df.columns:
            df = df[df["Tipo Aprovação"].isin(["Aprovado", "Indefinido"])]

        log.info(f"Linhas processadas: {len(df)}")

        # ---------------------------------------------------------------
        # SALVAR LOCAL (compatibilidade)
        # ---------------------------------------------------------------
        os.makedirs(SAIDA_LOCAL, exist_ok=True)
        arquivo_local = os.path.join(SAIDA_LOCAL, "fat_apontamentos_automatico.xlsx")
        df.to_excel(arquivo_local, index=False)
        log.info(f"Salvo local: {arquivo_local}")

        # ---------------------------------------------------------------
        # UPLOAD S3
        # ---------------------------------------------------------------
        timestamp = datetime.now().strftime("%Y-%m-%d")
        year, month, day = datetime.now().strftime("%Y"), datetime.now().strftime("%m"), datetime.now().strftime("%d")

        # Parquet para datalake
        parquet_local = os.path.join(DOWNLOAD_DIR_APONTAMENTO, f"apontamentos_{timestamp}.parquet")
        df.to_parquet(parquet_local, index=False, engine="pyarrow")
        upload_to_s3(parquet_local, f"{S3_PREFIX}/year={year}/month={month}/day={day}/apontamentos.parquet", log)

        # XLSX para consumo humano
        xlsx_s3 = os.path.join(DOWNLOAD_DIR_APONTAMENTO, "fat_apontamentos_automatico.xlsx")
        df.to_excel(xlsx_s3, index=False)
        upload_to_s3(xlsx_s3, f"{S3_PREFIX}/xlsx/fat_apontamentos_automatico.xlsx", log)

        log.info(f"SUCESSO: {len(df)} linhas extraídas e enviadas")

    except Exception as e:
        log.error(f"FALHA NA PIPELINE: {e}", exc_info=True)
        if driver:
            screenshot_on_error(driver, "apontamento", S3_PREFIX, log)
            driver.quit()
        sys.exit(1)


if __name__ == "__main__":
    main()
