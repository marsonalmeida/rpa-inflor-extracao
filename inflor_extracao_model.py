"""
INFLOR - Extração de Modelo/Cubo (VM Windows)

Executa na VM Windows via Task Scheduler.
Fluxo:
  1. Login INFLOR → itera períodos trimestrais (4 anos retroativos) → exporta XLS por período
  2. Consolida todos os XLS em um único DataFrame
  3. Salva XLSX local (backup/teste)
  4. [se DRY_RUN=False] Upload direto ao lake S3 (re.green-assets)
     → NiFi detecta → EMR processa → operation_land_plot_metrics_fact
  5. Log estruturado + controle_execucoes.csv

DRY_RUN=True no .env: extrai e salva local apenas, sem subir ao lake.
LAKE_ENV=stg|prd: controla qual ambiente do lake recebe os dados.
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
    setup_logging, log_step, log_summary,
    get_credentials, upload_to_s3, upload_to_lake, screenshot_on_error,
    create_driver, registrar_execucao,
    DOWNLOAD_DIR_MODELO, OUTPUT_DIR_MODELO, BASE_DIR,
    LAKE_PATH_MODELO, LAKE_ENV, DRY_RUN,
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DEBUG_S3_PREFIX = "inflor/modelo"   # usado apenas para debug screenshots
HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"

# Caminho local de saída — configurável via .env (SAIDA_LOCAL_MODELO)
# Default: C:\inflor-extrator\output\modelo (não depende de usuário logado)
SAIDA_LOCAL = OUTPUT_DIR_MODELO

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
    t0 = time.time()
    log.info("=" * 60)
    log.info("INFLOR EXTRAÇÃO MODELO/CUBO - VM WINDOWS")
    log.info("=" * 60)
    log.info(f"Períodos a extrair: {len(PERIODOS_VALIDOS)}")

    driver = None
    try:
        with log_step(log, "Credenciais"):
            login, senha = get_credentials(log)

        # Prepara diretório
        if os.path.exists(DOWNLOAD_DIR_MODELO):
            shutil.rmtree(DOWNLOAD_DIR_MODELO)
        os.makedirs(DOWNLOAD_DIR_MODELO, exist_ok=True)

        with log_step(log, "Iniciar Chrome"):
            driver = create_driver(DOWNLOAD_DIR_MODELO, log, headless=HEADLESS)

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
        # ITERAÇÃO POR PERÍODO
        # ---------------------------------------------------------------
        url_relatorio = ("https://regreen.inflor.cloud/SGF/Modulos/Relatorios/"
                         "Gerador/AdmParametroCuboFrm.aspx?cdRelatorio=397")

        for i, (dt_ini, dt_fim) in enumerate(PERIODOS_VALIDOS):
            datain = dt_ini.strftime("%d/%m/%Y")
            datafim = dt_fim.strftime("%d/%m/%Y")

            with log_step(log, f"Período {i+1}/{len(PERIODOS_VALIDOS)}: {datain} a {datafim}"):
                driver.get(url_relatorio)
                time.sleep(10)

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

                BtRelatorios = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.NAME, "btnOk"))
                )
                BtRelatorios.click()
                time.sleep(5)

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

        # ---------------------------------------------------------------
        # LOGOUT
        # ---------------------------------------------------------------
        with log_step(log, "Logout"):
            driver.get("https://regreen.inflor.cloud/SGF/DefaultModulos.aspx")
            BtLogout = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "btnLogOut"))
            )
            BtLogout.click()
            driver.quit()
            driver = None

        # ---------------------------------------------------------------
        # CONSOLIDAR
        # ---------------------------------------------------------------
        with log_step(log, "Consolidar arquivos XLS"):
            arquivos = sorted([f for f in os.listdir(DOWNLOAD_DIR_MODELO)
                               if os.path.isfile(os.path.join(DOWNLOAD_DIR_MODELO, f))])
            for idx, nome in enumerate(arquivos, start=1):
                ext = os.path.splitext(nome)[1]
                novo_nome = f"arquivo_{idx}{ext}"
                os.rename(
                    os.path.join(DOWNLOAD_DIR_MODELO, nome),
                    os.path.join(DOWNLOAD_DIR_MODELO, novo_nome)
                )

            arquivos_xls = sorted([f for f in os.listdir(DOWNLOAD_DIR_MODELO) if f.endswith(".xls")])
            log.info(f"Arquivos XLS encontrados: {len(arquivos_xls)} / esperados: {len(PERIODOS_VALIDOS)}")

            if not arquivos_xls:
                raise FileNotFoundError("Nenhum XLS encontrado")

            if len(arquivos_xls) < len(PERIODOS_VALIDOS):
                msg = (f"Arquivos incompletos: {len(arquivos_xls)} de "
                       f"{len(PERIODOS_VALIDOS)} períodos baixados")
                log.warning(msg)
                log.warning(msg)

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
        with log_step(log, "Salvar local"):
            os.makedirs(SAIDA_LOCAL, exist_ok=True)
            arquivo_local = os.path.join(SAIDA_LOCAL, "base_modelo.xlsx")
            df_consolidado.to_excel(arquivo_local, index=False)
            log.info(f"Destino: {arquivo_local}")

        # ---------------------------------------------------------------
        # UPLOAD LAKE
        # ---------------------------------------------------------------
        destinos = "local"
        with log_step(log, f"Upload lake [{LAKE_ENV}]"):
            lake_url = upload_to_lake(arquivo_local, LAKE_PATH_MODELO, log)
            if lake_url:
                destinos = f"local+lake({LAKE_ENV})"
            else:
                destinos = "local (DRY_RUN)"

        log_summary(log, "modelo", t0,
                    periodos=len(PERIODOS_VALIDOS),
                    linhas=len(df_consolidado),
                    lake_env=LAKE_ENV,
                    destinos=destinos)

        registrar_execucao(
            script="modelo", run_id=log.run_id, inicio=t0,
            status="SUCESSO", linhas=len(df_consolidado),
            destinos=destinos, log=log,
        )

    except Exception as e:
        log.error(f"FALHA NA PIPELINE: {e}", exc_info=True)
        if driver:
            screenshot_on_error(driver, "modelo", DEBUG_S3_PREFIX, log)
            driver.quit()

        registrar_execucao(
            script="modelo", run_id=log.run_id, inicio=t0,
            status="FALHA", erro=str(e), log=log,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
