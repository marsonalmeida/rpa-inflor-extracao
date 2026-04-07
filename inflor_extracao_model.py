"""
INFLOR - Extração de Modelo/Cubo (VM Windows)

Executa na VM Windows via Task Scheduler.
Fluxo:
    1. Login INFLOR → exporta todos os períodos na mesma execução
    2. Períodos trimestrais: 4 anos retroativos a partir de hoje (~16-17 períodos)
    3. Salva arquivo intermediário por período (base_P{indice}.xlsx)
    4. Ao final da execução: consolida tudo em um único base.xlsx
    5. [se DRY_RUN=False] Upload apenas do base.xlsx ao lake

DRY_RUN=True no .env: extrai e salva local apenas, sem subir ao lake.
LAKE_ENV=stg|prd: controla qual ambiente do lake recebe os dados.
"""

import os
import sys
import time
import shutil
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from inflor_utils import (
    setup_logging, log_step, log_summary,
    get_credentials, upload_to_lake, screenshot_on_error,
    create_driver, registrar_execucao,
    DOWNLOAD_DIR_MODELO, OUTPUT_DIR_MODELO,
    FINAL_FILE_MODELO,
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
# PERÍODOS (trimestral, 4 anos retroativos)
# ---------------------------------------------------------------------------
ANOS_RETROATIVOS_MODELO = int(os.environ.get("ANOS_RETROATIVOS_MODELO", "4"))
TIPO_PERIODO = "trimestre"


def gerar_periodos_trimestrais(anos_retroativos: int = 4) -> list:
    """Gera períodos trimestrais alinhados ao calendário cobrindo os últimos N anos até hoje."""
    hoje = date.today()
    inicio = hoje - relativedelta(years=anos_retroativos)
    # Alinha ao início do trimestre
    mes_ini = ((inicio.month - 1) // 3) * 3 + 1
    atual = date(inicio.year, mes_ini, 1)

    periodos = []
    while atual <= hoje:
        fim_periodo = atual + relativedelta(months=3) - relativedelta(days=1)
        fim = min(fim_periodo, hoje)
        periodos.append((atual, fim))
        atual += relativedelta(months=3)

    return periodos


TODOS_PERIODOS = gerar_periodos_trimestrais(ANOS_RETROATIVOS_MODELO)
PERIODOS_VALIDOS = TODOS_PERIODOS


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    log = setup_logging("inflor_modelo")
    t0 = time.time()
    log.info("=" * 60)
    log.info("INFLOR EXTRAÇÃO MODELO/CUBO - VM WINDOWS")
    log.info("=" * 60)
    log.info(f"Destino local por ambiente: {SAIDA_LOCAL}")
    log.info(f"Destino S3 [{LAKE_ENV}]: s3://re.green-assets/{LAKE_PATH_MODELO}")
    log.info(f"Períodos a extrair nesta execução: {len(PERIODOS_VALIDOS)}")
    log.info(f"Total de períodos trimestrais configurados: {len(TODOS_PERIODOS)} ({ANOS_RETROATIVOS_MODELO} anos)")

    if not PERIODOS_VALIDOS:
        log.info("Todos os períodos já foram processados. Nenhuma extração necessária.")
        log_summary(log, "modelo", t0,
                    periodos=0,
                    linhas=0,
                    lake_env=LAKE_ENV,
                    destinos="local (nada a processar)")
        registrar_execucao(
            script="modelo", run_id=log.run_id, inicio=t0,
            status="SUCESSO", linhas=0,
            destinos="local (nada a processar)", log=log,
        )
        return

    driver = None
    linhas_resumo = 0
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
                # Verificar se a sessão do driver ainda está ativa e recriar se necessário.
                try:
                    driver.current_url
                except Exception:
                    log.warning("Sessão do Chrome perdida, recriando driver...")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = create_driver(DOWNLOAD_DIR_MODELO, log, headless=HEADLESS)

                    with log_step(log, "Refazer login INFLOR"):
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

                driver.get(url_relatorio)
                time.sleep(5)  # Reduzido de 10s

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
                time.sleep(3)  # Reduzido de 10s

                BtRelatorios = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.NAME, "btnOk"))
                )
                BtRelatorios.click()
                time.sleep(2)  # Reduzido de 5s

                janelas = driver.window_handles
                driver.switch_to.window(janelas[-1])
                time.sleep(5)  # Reduzido de 10s

                btdados = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.ID, "ctl33"))
                )
                btdados.click()
                time.sleep(1)  # Reduzido de 3s

                btextrair = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.NAME, "ctl14"))
                )
                btextrair.click()
                time.sleep(15)  # Reduzido de 20s (ainda deixa tempo para download)

                driver.close()
                driver.switch_to.window(janelas[0])
                time.sleep(2)  # Reduzido de 5s

            with log_step(log, f"Consolidar período {i+1}/{len(PERIODOS_VALIDOS)}"):
                arquivos_periodo = sorted([
                    f for f in os.listdir(DOWNLOAD_DIR_MODELO)
                    if os.path.isfile(os.path.join(DOWNLOAD_DIR_MODELO, f)) and f.lower().endswith(".xls")
                ])

                if not arquivos_periodo:
                    raise FileNotFoundError(f"Nenhum XLS encontrado para o período {i+1}")

                dfs = []
                for arquivo in arquivos_periodo:
                    caminho_arquivo = os.path.join(DOWNLOAD_DIR_MODELO, arquivo)
                    try:
                        df_tmp = pd.read_excel(caminho_arquivo, engine="xlrd")
                        dfs.append(df_tmp)
                        log.info(f"Lido período {i+1}: {arquivo} ({len(df_tmp)} linhas)")
                    except Exception as e:
                        log.warning(f"Erro ao ler {arquivo} no período {i+1}: {e}")

                if not dfs:
                    raise FileNotFoundError(f"Nenhum DataFrame processado no período {i+1}")

                df_periodo = pd.concat(dfs, ignore_index=True)
                linhas_periodo = len(df_periodo)
                log.info(f"Período {i+1}/{len(PERIODOS_VALIDOS)} consolidado: {linhas_periodo} linhas")

            with log_step(log, f"Salvar local período {i+1}/{len(PERIODOS_VALIDOS)}"):
                os.makedirs(SAIDA_LOCAL, exist_ok=True)
                arquivo_individual = os.path.join(SAIDA_LOCAL, f"base_P{i}.xlsx")
                df_periodo.to_excel(arquivo_individual, index=False)
                log.info(f"Arquivo período: {arquivo_individual} ({linhas_periodo} linhas)")

            # Limpa downloads para evitar mistura de arquivos entre períodos.
            for nome in os.listdir(DOWNLOAD_DIR_MODELO):
                caminho = os.path.join(DOWNLOAD_DIR_MODELO, nome)
                if os.path.isfile(caminho):
                    os.remove(caminho)

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

        destinos = "local"

        # ---------------------------------------------------------------
        # CONSOLIDAÇÃO FINAL
        # ---------------------------------------------------------------
        with log_step(log, "Consolidação final"):
            dfs_consolidados = []
            for idx in range(len(PERIODOS_VALIDOS)):
                arquivo_p = os.path.join(SAIDA_LOCAL, f"base_P{idx}.xlsx")
                if os.path.exists(arquivo_p):
                    try:
                        df_p = pd.read_excel(arquivo_p)
                        dfs_consolidados.append(df_p)
                        log.info(f"Carregado: base_P{idx}.xlsx ({len(df_p)} linhas)")
                    except Exception as e:
                        log.warning(f"Erro ao carregar base_P{idx}.xlsx: {e}")

            if not dfs_consolidados:
                raise FileNotFoundError("Nenhum DataFrame consolidado encontrado para gerar base.xlsx")

            df_final = pd.concat(dfs_consolidados, ignore_index=True)
            linhas_resumo = len(df_final)
            os.makedirs(os.path.dirname(FINAL_FILE_MODELO), exist_ok=True)
            arquivo_final = FINAL_FILE_MODELO
            df_final.to_excel(arquivo_final, index=False)
            log.info(f"Arquivo consolidado final: {arquivo_final} ({linhas_resumo} linhas)")

        with log_step(log, f"Upload lake consolidado [{LAKE_ENV}]"):
            lake_url = upload_to_lake(arquivo_final, LAKE_PATH_MODELO, log)
            if lake_url:
                destinos = f"local({LAKE_ENV.upper()})+s3({LAKE_ENV})"
            else:
                destinos = f"local({LAKE_ENV.upper()}) (DRY_RUN)"

        log_summary(log, "modelo", t0,
                    periodos=len(PERIODOS_VALIDOS),
                    linhas=linhas_resumo,
                    lake_env=LAKE_ENV,
                    destinos=destinos)

        registrar_execucao(
            script="modelo", run_id=log.run_id, inicio=t0,
            status="SUCESSO", linhas=linhas_resumo,
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
