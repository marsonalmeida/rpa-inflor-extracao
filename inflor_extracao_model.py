"""
INFLOR - Extração de Modelo/Cubo (VM Windows)

Executa na VM Windows via Task Scheduler.
Acessa relatório cubo (cdRelatorio=397), itera por períodos,
consolida múltiplos XLS e envia pro S3.
"""

import os
import sys
import time
import shutil
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from inflor_utils import (
    setup_logging, get_credentials, upload_to_s3, screenshot_on_error,
    create_driver, DOWNLOAD_DIR_MODELO, BASE_DIR
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
S3_PREFIX = "inflor/modelo"
HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"

USER = os.environ.get("USERNAME") or os.environ.get("USER")
SAIDA_LOCAL = os.environ.get(
    "SAIDA_LOCAL_MODELO",
    os.path.join(r"C:\Users", USER,
                 r"OneDrive - Regreen\Painel de monitoramento\Operações"
                 r"\Detalhamento de Talhões\Inflor")
)

# ---------------------------------------------------------------------------
# PERÍODOS (dinâmico: 4 anos retroativos, trimestres de 3 meses)
# ---------------------------------------------------------------------------
ANOS_RETROATIVOS = int(os.environ.get("ANOS_RETROATIVOS", "4"))


def gerar_periodos(anos_retroativos: int = 4) -> list:
    """
    Gera lista de períodos trimestrais (3 em 3 meses) alinhados ao calendário,
    cobrindo os últimos N anos até hoje.
    """
    hoje = date.today()
    inicio = hoje - relativedelta(years=anos_retroativos)

    # Alinha ao início do trimestre
    mes_ini_trimestre = ((inicio.month - 1) // 3) * 3 + 1
    atual = date(inicio.year, mes_ini_trimestre, 1)

    periodos = []
    while atual <= hoje:
        fim_trimestre = atual + relativedelta(months=3) - relativedelta(days=1)
        fim = min(fim_trimestre, hoje)
        periodos.append((atual, fim))
        atual += relativedelta(months=3)

    return periodos


PERIODOS_VALIDOS = gerar_periodos(ANOS_RETROATIVOS)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    log = setup_logging("inflor_modelo")
    log.info("=" * 60)
    log.info("INFLOR EXTRAÇÃO MODELO/CUBO - VM WINDOWS")
    log.info("=" * 60)
    log.info(f"Períodos a extrair: {len(PERIODOS_VALIDOS)}")

    driver = None
    try:
        login, senha = get_credentials(log)

        # Prepara diretório
        if os.path.exists(DOWNLOAD_DIR_MODELO):
            shutil.rmtree(DOWNLOAD_DIR_MODELO)
        os.makedirs(DOWNLOAD_DIR_MODELO, exist_ok=True)

        # Chrome
        driver = create_driver(DOWNLOAD_DIR_MODELO, log, headless=HEADLESS)

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
        # ITERAÇÃO POR PERÍODO
        # ---------------------------------------------------------------
        url_relatorio = ("https://regreen.inflor.cloud/SGF/Modulos/Relatorios/"
                         "Gerador/AdmParametroCuboFrm.aspx?cdRelatorio=397")

        for i, (dt_ini, dt_fim) in enumerate(PERIODOS_VALIDOS):
            datain = dt_ini.strftime("%d/%m/%Y")
            datafim = dt_fim.strftime("%d/%m/%Y")
            log.info(f"--- Período {i+1}/{len(PERIODOS_VALIDOS)}: {datain} a {datafim} ---")

            driver.get(url_relatorio)
            time.sleep(10)

            # Data inicial (caractere por caractere - necessário pro campo ASP.NET)
            dtinicial = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "ctl08_txtDataInicio_1"))
            )
            dtinicial.click()
            dtinicial.clear()
            time.sleep(0.5)
            for char in datain:
                dtinicial.send_keys(char)
                time.sleep(0.05)
            time.sleep(0.5)
            dtinicial.send_keys(Keys.ENTER)
            time.sleep(2)

            # Data final
            dtfinal = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "ctl08_txtDataFim_1"))
            )
            dtfinal.click()
            dtfinal.clear()
            time.sleep(0.5)
            for char in datafim:
                dtfinal.send_keys(char)
                time.sleep(0.05)
            time.sleep(0.5)
            dtfinal.send_keys(Keys.ENTER)
            time.sleep(10)

            # Gerar
            BtRelatorios = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.NAME, "btnOk"))
            )
            BtRelatorios.click()
            time.sleep(5)

            # Nova janela
            janelas = driver.window_handles
            driver.switch_to.window(janelas[-1])
            time.sleep(10)

            btdados = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "ctl33"))
            )
            btdados.click()
            time.sleep(3)

            btextrair = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.NAME, "ctl14"))
            )
            btextrair.click()
            time.sleep(20)

            driver.close()
            driver.switch_to.window(janelas[0])
            time.sleep(5)
            log.info(f"Período {i+1} concluído")

        # ---------------------------------------------------------------
        # LOGOUT
        # ---------------------------------------------------------------
        log.info("Fazendo logout")
        driver.get("https://regreen.inflor.cloud/SGF/DefaultModulos.aspx")
        BtLogout = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "btnLogOut"))
        )
        BtLogout.click()
        driver.quit()
        driver = None
        log.info("Extração concluída")

        # ---------------------------------------------------------------
        # RENOMEAR ARQUIVOS
        # ---------------------------------------------------------------
        arquivos = sorted([f for f in os.listdir(DOWNLOAD_DIR_MODELO)
                           if os.path.isfile(os.path.join(DOWNLOAD_DIR_MODELO, f))])

        for idx, nome in enumerate(arquivos, start=1):
            ext = os.path.splitext(nome)[1]
            novo_nome = f"arquivo_{idx}{ext}"
            os.rename(
                os.path.join(DOWNLOAD_DIR_MODELO, nome),
                os.path.join(DOWNLOAD_DIR_MODELO, novo_nome)
            )
            log.info(f"Renomeado: {nome} -> {novo_nome}")

        # ---------------------------------------------------------------
        # CONSOLIDAR
        # ---------------------------------------------------------------
        arquivos_xls = sorted([f for f in os.listdir(DOWNLOAD_DIR_MODELO) if f.endswith(".xls")])
        log.info(f"Arquivos XLS: {len(arquivos_xls)}")

        if not arquivos_xls:
            raise FileNotFoundError("Nenhum XLS encontrado")

        dfs = []
        for arq in arquivos_xls:
            caminho = os.path.join(DOWNLOAD_DIR_MODELO, arq)
            try:
                df = pd.read_excel(caminho, engine="xlrd")
                dfs.append(df)
                log.info(f"Lido: {arq} ({len(df)} linhas)")
            except Exception as e:
                log.warning(f"Erro ao ler {arq}: {e}")

        df_consolidado = pd.concat(dfs, ignore_index=True)
        log.info(f"Total consolidado: {len(df_consolidado)} linhas")

        # ---------------------------------------------------------------
        # SALVAR LOCAL
        # ---------------------------------------------------------------
        os.makedirs(SAIDA_LOCAL, exist_ok=True)
        arquivo_local = os.path.join(SAIDA_LOCAL, "base.xlsx")
        df_consolidado.to_excel(arquivo_local, index=False)
        log.info(f"Salvo local: {arquivo_local}")

        # ---------------------------------------------------------------
        # UPLOAD S3
        # ---------------------------------------------------------------
        timestamp = datetime.now().strftime("%Y-%m-%d")
        year, month, day = datetime.now().strftime("%Y"), datetime.now().strftime("%m"), datetime.now().strftime("%d")

        # Parquet
        parquet_local = os.path.join(DOWNLOAD_DIR_MODELO, f"modelo_{timestamp}.parquet")
        df_consolidado.to_parquet(parquet_local, index=False, engine="pyarrow")
        upload_to_s3(parquet_local, f"{S3_PREFIX}/year={year}/month={month}/day={day}/modelo.parquet", log)

        # XLSX
        xlsx_s3 = os.path.join(DOWNLOAD_DIR_MODELO, "base_modelo.xlsx")
        df_consolidado.to_excel(xlsx_s3, index=False)
        upload_to_s3(xlsx_s3, f"{S3_PREFIX}/xlsx/base_modelo.xlsx", log)

        log.info(f"SUCESSO: {len(df_consolidado)} linhas extraídas e enviadas")

    except Exception as e:
        log.error(f"FALHA NA PIPELINE: {e}", exc_info=True)
        if driver:
            screenshot_on_error(driver, "modelo", S3_PREFIX, log)
            driver.quit()
        sys.exit(1)


if __name__ == "__main__":
    main()
