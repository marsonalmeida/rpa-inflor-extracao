"""
INFLOR - Extração de Apontamentos (VM Windows)

Executa na VM Windows via Task Scheduler.
Fluxo (10 períodos anuais em uma única execução):
    1. Login INFLOR → exporta todos os períodos anuais (120 meses / 10 anos)
    2. Consolida ZIP/XLS de cada período em DataFrame
    3. Salva XLSX intermediário por período (fat_apontamentos_P{indice}.xlsx)
    4. Ao final da execução: consolida tudo em fat_apontamentos_automatico.xlsx
    5. [se DRY_RUN=False] Upload apenas do arquivo final ao lake S3 (re.green-assets)
    6. Log estruturado + controle_execucoes.csv

DRY_RUN=True no .env: extrai e salva local apenas, sem subir ao lake.
LAKE_ENV=stg|prd: controla qual ambiente do lake recebe os dados.
"""

import os
import sys
import time
import shutil
import zipfile
import pandas as pd
from datetime import datetime, date
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
    create_driver, wait_for_download, registrar_execucao,
    DOWNLOAD_DIR_APONTAMENTO, OUTPUT_DIR_APONTAMENTO,
    FINAL_FILE_APONTAMENTO,
    LAKE_PATH_APONTAMENTOS, LAKE_ENV, DRY_RUN,
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DEBUG_S3_PREFIX = "inflor/apontamentos"   # usado apenas para debug screenshots
TIPO_PERIODO = "ano"       # 1 consulta por ano para evitar timeout
ANOS_RETROATIVOS = 10      # 120 meses = 10 anos retroativos
HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"

# Caminho local de saída — configurável via .env (SAIDA_LOCAL_APONTAMENTO)
# Default: C:\inflor-extrator\output\apontamentos (não depende de usuário logado)
SAIDA_LOCAL = OUTPUT_DIR_APONTAMENTO

# ---------------------------------------------------------------------------
# PERÍODOS (dinâmicos: trimestre, semestre ou ano)
# ---------------------------------------------------------------------------
def gerar_periodos(anos_retroativos: int = 4, tipo_periodo: str = "trimestre") -> list:
    """
    Gera lista COMPLETA de períodos configuráveis alinhados ao calendário,
    cobrindo os últimos N anos até hoje.
    """
    hoje = date.today()
    inicio = hoje - relativedelta(years=anos_retroativos)

    # Define o tamanho do período em meses
    if tipo_periodo == "trimestre":
        meses_periodo = 3
        # Alinha ao início do trimestre
        mes_ini = ((inicio.month - 1) // 3) * 3 + 1
    elif tipo_periodo == "semestre":
        meses_periodo = 6
        # Alinha ao início do semestre (jan-jun ou jul-dez)
        mes_ini = 1 if inicio.month <= 6 else 7
    elif tipo_periodo == "ano":
        meses_periodo = 12
        # Alinha ao início do ano
        mes_ini = 1
    else:
        raise ValueError(f"Tipo de período inválido: {tipo_periodo}. Use 'trimestre', 'semestre' ou 'ano'")

    atual = date(inicio.year, mes_ini, 1)

    periodos = []
    while atual <= hoje:
        fim_periodo = atual + relativedelta(months=meses_periodo) - relativedelta(days=1)
        fim = min(fim_periodo, hoje)
        periodos.append((atual, fim))
        atual += relativedelta(months=meses_periodo)

    return periodos

# Gera TODOS os períodos possíveis
TODOS_PERIODOS = gerar_periodos(ANOS_RETROATIVOS, TIPO_PERIODO)
PERIODOS_VALIDOS = TODOS_PERIODOS

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    log = setup_logging("inflor_apontamentos")
    t0 = time.time()
    log.info("=" * 60)
    log.info("INFLOR EXTRAÇÃO APONTAMENTOS - VM WINDOWS")
    log.info("=" * 60)
    log.info(f"Destino local por ambiente: {SAIDA_LOCAL}")
    log.info(f"Destino S3 [{LAKE_ENV}]: s3://re.green-assets/{LAKE_PATH_APONTAMENTOS}")
    log.info(f"Períodos a extrair nesta execução: {len(PERIODOS_VALIDOS)} (anual, {ANOS_RETROATIVOS} anos)")
    log.info(f"Total de períodos configurados: {len(TODOS_PERIODOS)}")

    if not PERIODOS_VALIDOS:
        log.info("Todos os períodos já foram processados. Nenhuma extração necessária.")
        log_summary(log, "apontamentos", t0,
                    periodos=0, linhas=0, lake_env=LAKE_ENV,
                    destinos="local (nada a processar)")
        registrar_execucao(
            script="apontamentos", run_id=log.run_id, inicio=t0,
            status="SUCESSO", linhas=0,
            destinos="local (nada a processar)", log=log,
        )
        return

    driver = None
    linhas_resumo = 0
    try:
        with log_step(log, "Credenciais"):
            login, senha = get_credentials(log)

        # Prepara diretório de download
        if os.path.exists(DOWNLOAD_DIR_APONTAMENTO):
            shutil.rmtree(DOWNLOAD_DIR_APONTAMENTO)
        os.makedirs(DOWNLOAD_DIR_APONTAMENTO, exist_ok=True)

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
            time.sleep(2)

        # ---------------------------------------------------------------
        # ITERAÇÃO POR PERÍODO
        # ---------------------------------------------------------------
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
                    driver = create_driver(DOWNLOAD_DIR_APONTAMENTO, log, headless=HEADLESS)

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
                        time.sleep(2)

                # ---------------------------------------------------------------
                # NAVEGAÇÃO + FILTROS + EXPORTAR (por período)
                # ---------------------------------------------------------------
                driver.get("https://regreen.inflor.cloud/SGF/DefaultSilviculturaControle.aspx")
                time.sleep(1)

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
                time.sleep(5)

                Btdetalhes = WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, "//input[@title='Exibir colunas extras']"))
                )
                Btdetalhes.click()
                time.sleep(5)

                BtExport = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.XPATH, "//input[@title='Exportar dados para Excel']"))
                )
                BtExport.click()

                # Aguarda o download completar antes de fazer logout
                # Tenta ZIP primeiro (arquivos grandes), depois XLS direto
                try:
                    wait_for_download(DOWNLOAD_DIR_APONTAMENTO, timeout=300,
                                      extension=".zip", log=log)
                except TimeoutError:
                    # Alguns períodos exportam XLS diretamente sem ZIP
                    wait_for_download(DOWNLOAD_DIR_APONTAMENTO, timeout=60,
                                      extension=".xls", log=log)

                # Retorna ao frame padrão. Se o navegador cair após o export,
                # a próxima iteração recria a sessão automaticamente.
                try:
                    driver.switch_to.default_content()
                    time.sleep(0.5)
                except Exception as e:
                    log.warning(f"Sessão encerrada após exportação do período {i+1}: {e}")

            with log_step(log, f"Consolidar período {i+1}/{len(PERIODOS_VALIDOS)}"):
                zip_files = [
                    f for f in os.listdir(DOWNLOAD_DIR_APONTAMENTO)
                    if f.endswith(".zip")
                ]
                log.info(f"Período {i+1}: arquivos ZIP encontrados: {len(zip_files)}")

                for zip_file in zip_files:
                    zip_path = os.path.join(DOWNLOAD_DIR_APONTAMENTO, zip_file)
                    with zipfile.ZipFile(zip_path, "r") as zip_ref:
                        zip_ref.extractall(DOWNLOAD_DIR_APONTAMENTO)

                arquivos_xls = sorted([
                    f for f in os.listdir(DOWNLOAD_DIR_APONTAMENTO)
                    if f.endswith(".xls") or f.endswith(".xlsx")
                ])
                log.info(f"Período {i+1}: arquivos XLS encontrados: {len(arquivos_xls)}")

                if not arquivos_xls:
                    raise FileNotFoundError(f"Período {i+1}: nenhum XLS encontrado após unzip")

                dfs = []
                for arquivo in arquivos_xls:
                    caminho_arquivo = os.path.join(DOWNLOAD_DIR_APONTAMENTO, arquivo)
                    try:
                        df_temp = pd.read_html(caminho_arquivo, flavor="html5lib",
                                              index_col=None, thousands=".", decimal=",")[0]
                        df_temp.columns = df_temp.iloc[0]
                        df_temp = df_temp[1:].reset_index(drop=True)
                        dfs.append(df_temp)
                        log.info(f"Período {i+1}: lido {arquivo} ({len(df_temp)} linhas)")
                    except Exception as e:
                        log.warning(f"Período {i+1}: erro ao ler {arquivo}: {e}")

                if not dfs:
                    raise FileNotFoundError(f"Período {i+1}: nenhum DataFrame processado")

                df_periodo = pd.concat(dfs, ignore_index=True)

                colunas_numericas = [
                    "Custo Unitário Recurso", "Custo Recurso", "Rendimento Previsto",
                    "Rendimento Real", "Quantidade", "% Realização", "Valor Produção",
                    "Custo Total Operação", "Custo Unitário Operação", "Uso Solo",
                    "Área (ha)", "Custo Boletim"
                ]
                for col in colunas_numericas:
                    if col in df_periodo.columns:
                        df_periodo[col] = pd.to_numeric(df_periodo[col], errors="coerce")

                if "Tipo Aprovação" in df_periodo.columns:
                    df_periodo = df_periodo[df_periodo["Tipo Aprovação"].isin(["Aprovado", "Indefinido"])]

                linhas_periodo = len(df_periodo)
                log.info(f"Período {i+1}/{len(PERIODOS_VALIDOS)} após filtros: {linhas_periodo} linhas")

            with log_step(log, f"Salvar local período {i+1}/{len(PERIODOS_VALIDOS)}"):
                os.makedirs(SAIDA_LOCAL, exist_ok=True)
                arquivo_individual = os.path.join(SAIDA_LOCAL, f"fat_apontamentos_P{i}.xlsx")
                df_periodo.to_excel(arquivo_individual, index=False)
                log.info(f"Arquivo período: {arquivo_individual} ({linhas_periodo} linhas)")

            # Limpa downloads para evitar mistura de arquivos entre períodos.
            for nome in os.listdir(DOWNLOAD_DIR_APONTAMENTO):
                caminho = os.path.join(DOWNLOAD_DIR_APONTAMENTO, nome)
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

        # ---------------------------------------------------------------
        # CONSOLIDAÇÃO FINAL
        # ---------------------------------------------------------------
        with log_step(log, "Consolidação final"):
            log.info(f"Consolidando todos os {len(PERIODOS_VALIDOS)} períodos...")

            dfs_consolidados = []
            for idx in range(len(PERIODOS_VALIDOS)):
                arquivo_p = os.path.join(SAIDA_LOCAL, f"fat_apontamentos_P{idx}.xlsx")
                if os.path.exists(arquivo_p):
                    try:
                        df_p = pd.read_excel(arquivo_p)
                        dfs_consolidados.append(df_p)
                        log.info(f"Carregado: fat_apontamentos_P{idx}.xlsx ({len(df_p)} linhas)")
                    except Exception as e:
                        log.warning(f"Erro ao carregar P{idx}: {e}")

            if not dfs_consolidados:
                raise FileNotFoundError("Nenhum arquivo consolidado foi encontrado")

            df_final = pd.concat(dfs_consolidados, ignore_index=True)
            linhas_resumo = len(df_final)
            log.info(f"Consolidado total: {linhas_resumo} linhas")

            os.makedirs(os.path.dirname(FINAL_FILE_APONTAMENTO), exist_ok=True)
            arquivo_consolidado = FINAL_FILE_APONTAMENTO
            df_final.to_excel(arquivo_consolidado, index=False)
            log.info(f"Arquivo consolidado: {arquivo_consolidado}")

        destinos = "local"
        with log_step(log, f"Upload lake consolidado [{LAKE_ENV}]"):
            lake_url = upload_to_lake(arquivo_consolidado, LAKE_PATH_APONTAMENTOS, log)
            if lake_url:
                destinos = f"local({LAKE_ENV.upper()})+s3({LAKE_ENV})"
            else:
                destinos = f"local({LAKE_ENV.upper()}) (DRY_RUN)"

        log_summary(log, "apontamentos", t0,
                    periodos=len(PERIODOS_VALIDOS),
                    linhas=linhas_resumo,
                    lake_env=LAKE_ENV,
                    destinos=destinos)

        registrar_execucao(
            script="apontamentos", run_id=log.run_id, inicio=t0,
            status="SUCESSO", linhas=linhas_resumo,
            destinos=destinos, log=log,
        )

    except Exception as e:
        log.error(f"FALHA NA PIPELINE: {e}", exc_info=True)
        if driver:
            screenshot_on_error(driver, "apontamento", DEBUG_S3_PREFIX, log)
            driver.quit()

        registrar_execucao(
            script="apontamentos", run_id=log.run_id, inicio=t0,
            status="FALHA", erro=str(e), log=log,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
