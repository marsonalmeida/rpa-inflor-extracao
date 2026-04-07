import chromedriver_autoinstaller
import time
import shutil
import zipfile
import os
import pandas as pd
import logging
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from decouple import config

# Data de hoje
hoje = date.today()

user = r'/'+os.environ.get("USERNAME") or os.environ.get("USER")

# Função para obter o fim do semestre ou hoje

lista_periodos = [
    [date(2022,7,1),date(2022,12,31)]
    ,[date(2023,1,1),date(2023,6,30)]
    ,[date(2023,7,1),date(2023,12,31)]
    ,[date(2024,1,1), date(2024,2,29)]
    ,[date(2024,3,1), date(2024,4,30)]
    ,[date(2024,5,1), date(2024,6,30)]
    ,[date(2024,7,1), date(2024,8,31)]
    ,[date(2024,9,1), date(2024,10,31)]
    ,[date(2024,11,1), date(2024,12,31)]
    ,[date(2025,1,1),date(2025,3,31)]
    ,[date(2025,4,1),date(2025,6,30)]
    ,[date(2025,7,1),date(2025,10,31)]
    ,[date(2025,11,1),date(2025,12,31)]
    ,[date(2026,1,1), date(2026,2,28)]
    #,[date(2026,3,1), date(2026,4,31)]
    #,[date(2026,5,1), date(2026,6,30)]
]


def fim_semestre(ano, semestre):
    if semestre == 1:
        fim = date(ano, 6, 30)
    else:
        fim = date(ano, 12, 31)
    
    return fim if fim <= hoje else hoje

arquivo_log = r'C:\Users' + user + r'\Regreen\Public - Documentos\Tecnologia\Tecnologia\Automações\Rotina diária\Scripts\logs\inflor_apontamentos_modelos.log'
logging.basicConfig(filename=arquivo_log, level=logging.DEBUG, format="%(asctime)s :: %(levelname)s :: %(message)s")
logging.info('-----------------------------INICIANDO AUTOMAÇÃO-----------------------------------')
logging.info('INFO: Definindo variáveis')
# Definição das variáveis
login_inflor = config('LOGIN_INFLOR')
senha_inflor = config('SENHA_INFLOR')

caminho_base = r"C:\Downloads\extracao_automatica_inflor_apontamentos_modelo"
chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": caminho_base,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

logging.info('INFO: Instalando o chromedriver mais recente')

##class automacao:
    ##def realizaextracao(self):
print('INFO: Iniciando processo de extração')
##chromedriver_autoinstaller.install()
##caminhoChromeDriver = r"C:\Users" + user + r"\AppData\Roaming\Python\Python311\site-packages\chromedriver_autoinstaller\138\chromedriver.exe"
caminhoChromeDriver = chromedriver_autoinstaller.install()
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
logging.info('INFO: Entrando no relatório')
url_driver = "https://regreen.inflor.cloud/SGF/Modulos/Relatorios/Gerador/AdmParametroCuboFrm.aspx?cdRelatorio=397"

for i in range(len(lista_periodos)):
        
    datain = lista_periodos[i][0].strftime('%d/%m/%Y')
    datafim = lista_periodos[i][1].strftime('%d/%m/%Y')

    driver.get(url_driver)
    time.sleep(10)
    logging.info('INFO: Inserindo data inicial '+datain)
    dtinicial = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='ctl08_txtDataInicio_1'])[1]")))
    # Clica e limpa o campo
    dtinicial.click()
    dtinicial.clear()
    time.sleep(0.5)
    for char in datain:
        dtinicial.send_keys(char)
        time.sleep(0.05)  # atraso entre os caracteres
    time.sleep(0.5)
    ##dtinicial.send_keys(datain)
    logging.info('INFO: Dando enter após inserir a data inicial')
    dtinicial.send_keys(Keys.ENTER)
    time.sleep(2)
    logging.info('INFO: Inserindo data final '+datafim)
    dtfinal = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='ctl08_txtDataFim_1'])[1]")))
    # Clica e limpa o campo
    dtfinal.click()
    dtfinal.clear()
    time.sleep(0.5)
    for char in datafim:
        dtfinal.send_keys(char)
        time.sleep(0.05)  # atraso entre os caracteres
    time.sleep(0.5)
    ##dtfinal.send_keys(datafim)
    time.sleep(2)
    logging.info('INFO: Dando enter após inserir a data final')
    dtfinal.send_keys(Keys.ENTER)
    logging.info('INFO: Esperando 12s')
    time.sleep(10)
    logging.info('INFO: Buscando botão para clicar')
    BtRelatorios = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@name='btnOk'])[1]")))
    logging.info('INFO: Clicando no botão')
    BtRelatorios.click()
    time.sleep(5)
    logging.info('INFO: Entrando na janela que abriu')
    janelas = driver.window_handles
    driver.switch_to.window(janelas[-1])
    time.sleep(10)
    btdados = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='ctl33'])[1]")))
    btdados.click()
    time.sleep(3)
    btextrair = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@name='ctl14'])[1]")))
    btextrair.click()
    time.sleep(20)
    driver.close()
    driver.switch_to.window(janelas[0])
    time.sleep(5)
        
logging.info('INFO: Voltando para página principal')
driver.get("https://regreen.inflor.cloud/SGF/DefaultModulos.aspx")

logging.info('INFO: Clicando em logout')
BtLogout = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,"(//input[@id='btnLogOut'])[1]")))
BtLogout.click()
logging.info('INFO: Saindo do navegador')
driver.quit()
print('INFO: Extração Concluida')

# Iniciando etapa de renomeação dos arquivos
# Listar os arquivos na pasta
arquivos = os.listdir(caminho_base)

# Filtrar apenas arquivos (evitar pastas)
arquivos = [f for f in arquivos if os.path.isfile(os.path.join(caminho_base, f))]

# Ordenar para garantir sequência previsível
arquivos.sort()

# Renomear os arquivos
for i, nome_arquivo in enumerate(arquivos, start=1):
    extensao = os.path.splitext(nome_arquivo)[1]
    novo_nome = f'arquivo {i}{extensao}'
    caminho_antigo = os.path.join(caminho_base, nome_arquivo)
    caminho_novo = os.path.join(caminho_base, novo_nome)
    
    os.rename(caminho_antigo, caminho_novo)
    print(f'{nome_arquivo} --> {novo_nome}')


# Lista de arquivos .xls
arquivos_xls = [f for f in os.listdir(caminho_base) if f.endswith('.xls')]

# Lista para armazenar os DataFrames
dfs = []

for arquivo in arquivos_xls:
    caminho_arquivo = os.path.join(caminho_base, arquivo)
    
    # Lê o arquivo .xls
    df = pd.read_excel(caminho_arquivo, engine='xlrd')  # Para arquivos .xls antigos
    dfs.append(df)

# Concatena todos os DataFrames
df_consolidado = pd.concat(dfs, ignore_index=True)

# Caminho para o arquivo consolidado
arquivo_saida = os.path.join(r'C:\Users' + user + r'\OneDrive - Regreen\Painel de monitoramento\Operações\Detalhamento de Talhões\Inflor', 'base.xlsx')

# Salva o DataFrame como .xlsx
df_consolidado.to_excel(arquivo_saida, index=False)

# %% 