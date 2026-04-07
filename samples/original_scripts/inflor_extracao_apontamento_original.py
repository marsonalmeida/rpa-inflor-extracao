import chromedriver_autoinstaller
import time
import shutil
import zipfile
import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from decouple import config

user = r'/'+os.environ.get("USERNAME") or os.environ.get("USER")

arquivo_log = r'C:\Users' + user + r'\Regreen\Public - Documentos\Tecnologia\Tecnologia\Automações\Rotina diária\Scripts\logs\inflor_apontamentos.log'
logging.basicConfig(filename=arquivo_log, level=logging.DEBUG, format="%(asctime)s :: %(levelname)s :: %(message)s")
logging.info('-----------------------------INICIANDO AUTOMAÇÃO-----------------------------------')
logging.info('INFO: Definindo variáveis')
# Definição das variáveis
login_inflor = config('LOGIN_INFLOR')
senha_inflor = config('SENHA_INFLOR')

caminho_base = r"C:\Downloads\extracao_automatica_inflor_apontamentos"
# Período a ser extraído
data_hoje = datetime.now()
dtmenos = data_hoje - relativedelta(months=120)
datain = dtmenos.strftime('%d/%m/%Y')
datafim = data_hoje.strftime('%d/%m/%Y')

chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": caminho_base,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

logging.info('INFO: Instalando o chromedriver mais recente')

class automacao:
    def realizaextracao(self):
        print('INFO: Iniciando processo de extração')
        ##chromedriver_autoinstaller.install()
        import chromedriver_autoinstaller
        caminhoChromeDriver = chromedriver_autoinstaller.install()
        # caminhoChromeDriver = r"C:\Users" + user + r"\AppData\Roaming\Python\Python311\site-packages\chromedriver_autoinstaller\143\chromedriver.exe"
        driver = webdriver.Chrome(service=ChromeService(caminhoChromeDriver), options=chrome_options)
        if os.path.exists(caminho_base):
            logging.info('INFO: Deletando pasta base')
            shutil.rmtree(caminho_base)
            logging.info('INFO: Recriando a pasta base')
            os.makedirs(caminho_base)
        else:
            os.makedirs(caminho_base)
            logging.info('INFO: O caminho da pasta não existe, então foi criado!.')

        logging.info('INFO: Abrindo navegador')
        driver.get("https://regreen.inflor.cloud/SGF/Default.aspx?")
        time.sleep(10)
        campoLogin = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='txtLogin'])[1]")))
        campoSenha = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='txtSenha'])[1]")))
        logging.info('INFO: Preenchendo usuário')
        campoLogin.send_keys(login_inflor)
        logging.info('INFO: Preenchendo senha')
        campoSenha.send_keys(senha_inflor)
        logging.info('INFO: Logando')
        campoSenha.send_keys(Keys.RETURN)
        time.sleep(5)
        logging.info('INFO: Entrando em silvicultura e controle')
        driver.get("https://regreen.inflor.cloud/SGF/DefaultSilviculturaControle.aspx")
        logging.info('INFO: Clicando em relatório')
        BtRelatorios = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//div[normalize-space()='Relatórios'])[1]")))
        BtRelatorios.click()
        logging.info('INFO: Clicando em apontamentos')
        BtApontamentos = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"/html[1]/body[1]/table[1]/tbody[1]/tr[1]/td[1]/table[2]/tbody[1]/tr[1]/td[1]/form[1]/table[8]/tbody[1]/tr[2]/td[1]")))
        BtApontamentos.click()
        logging.info('INFO: Clicando em consulta boletins e apontamentos geral')
        BtConsulta = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"/html[1]/body[1]/table[1]/tbody[1]/tr[1]/td[1]/table[2]/tbody[1]/tr[1]/td[1]/form[1]/table[5]/tbody[1]/tr[2]/td[1]")))
        BtConsulta.click()
        logging.info('INFO: Entrando no iframe do relatório')
        iframe = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//iframe[@id='conteudo']")))
        driver.switch_to.frame(iframe)
        logging.info('INFO: Inserindo data inicial')
        dtinicial = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='txtDataIni'])[1]")))
        dtinicial.send_keys(datain)
        logging.info('INFO: Inserindo data final')
        dtfinal = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='txtDataFim'])[1]")))
        dtfinal.send_keys(datafim)
        logging.info('INFO: CLicando no botão para gerar o relatório')
        Btfiltro = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='btnGerar'])[1]")))
        Btfiltro.click()
        logging.info('INFO: Clicando em detalhes para expandir colunas ocultas')
        time.sleep(10)
        Btdetalhes = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@title='Exibir colunas extras'])[1]")))
        Btdetalhes.click()
        time.sleep(30)
        logging.info('INFO: CLicando no botão de exportação do relatório')
        BtExport = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@title='Exportar dados para Excel'])[1]")))
        BtExport.click()
        time.sleep(120)
        logging.info('INFO: Voltando para página principal')
        driver.get("https://regreen.inflor.cloud/SGF/DefaultModulos.aspx")
        logging.info('INFO: Clicando em logout')
        BtLogout = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='btnLogOut'])[1]")))
        BtLogout.click()
        logging.info('INFO: Saindo do navegador')
        driver.quit()
        print('INFO: Extração Concluida')
    
    def unzip(self):
        logging.info('INFO: Unzip file')
        print('INFO: Unzip file')
        zip_na_pasta = [f for f in os.listdir(caminho_base) if f.endswith('.zip') or f.endswith('.zip')]
        caminho_zip = caminho_base + '/' + zip_na_pasta[0]
        pasta_destino = caminho_base
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            zip_ref.extractall(pasta_destino)
        time.sleep(120)
        print('INFO: Concluido')

    def salvabase_ELO(self):
        logging.info('INFO: Salvando base pro data lake')
        print('INFO: Salvando base pro data lake')
        arquivos_na_pasta = [f for f in os.listdir(caminho_base) if f.endswith('.xls') or f.endswith('.xlsx')]
        arquivo_final = caminho_base + '/' + arquivos_na_pasta[0]
        html_file_path = arquivo_final
        df = pd.read_html(html_file_path, flavor='html5lib', index_col=None, thousands='.', decimal=',')[0]
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        Arquivo_salvo = r'C:\Users' + user + r'\OneDrive - Regreen\Painel de monitoramento\Operações\Atividades executadas\Apontamento de atividades.xlsx'
        df.to_excel(Arquivo_salvo, index=False)
        print('INFO: Concluido')

    def salvabase(self):
        logging.info('INFO: Salvando base Em Tecnologia')
        print('INFO: Salvando bases de reserva')
        arquivos_na_pasta = [f for f in os.listdir(caminho_base) if f.endswith('.xls') or f.endswith('.xlsx')]
        arquivo_final = caminho_base + '/' + arquivos_na_pasta[0]
        html_file_path = arquivo_final
        df = pd.read_html(html_file_path, flavor='html5lib', index_col=None, thousands='.', decimal=',')[0]
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        colunas_para_converter = ['Custo Unitário Recurso','Custo Recurso','Rendimento Previsto','Rendimento Real','Quantidade','% Realização','Valor Produção','Custo Total Operação','Custo Unitário Operação','Uso Solo','Área (ha)','Custo Boletim']
        df[colunas_para_converter] = df[colunas_para_converter].astype(float)
        df = df[df['Tipo Aprovação'].isin(['Aprovado', 'Indefinido'])]
        print('INFO: Tecnologia')
        ## Arquivo_copia = r'C:\Users'+user+'\Regreen\Public - Documentos\Tecnologia\Tecnologia\Automações\Rotina diária\Extrações\Inflor_apontamentos\fat_apontamentos_automatico.xlsx'
        ##Arquivo_salvo1 = r'C:\Users' + user + r'\Regreen\Public - Documentos\Tecnologia\Tecnologia\Automações\Atividades executadas\fat_apontamentos_automatico.xlsx'
        Arquivo_salvo2 = r'C:\Users' + user + r'\Regreen\- Operacional - Documentos\OPERAÇÃO\01. PMO OPERAÇÂO\03.FUP Mensal\01.Resultado Operacional\04.Bases_Portal_Indicadores\fat_apontamentos_automatico.xlsx'
        df.to_excel(Arquivo_salvo2, index=False)
        # time.sleep(5)
        # timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # novo_nome = r"C:\Users" + user +"\Regreen\Public - Documentos\Tecnologia\Tecnologia\Automações\Rotina diária\Extrações\Inflor_apontamentos" + f"\extracao_inflor_{timestamp}.xlsx"
        # print('INFO: Renomeando arquivo em tecnologia automacoes')
        # os.rename(Arquivo_copia, novo_nome)
        print('INFO: Concluido')

start = automacao()
start.realizaextracao()
start.unzip()
start.salvabase_ELO()
start.salvabase()
 