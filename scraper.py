import os
import math
import json
import csv

import mysql.connector
import selenium

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from datetime import datetime
from time import sleep

dataScrape = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

csvUrl = 'https://statusinvest.com.br/category/AdvancedSearchResultExport?search=%7B%22Sector%22%3A%22%22%2C%22SubSector%22%3A%22%22%2C%22Segment%22%3A%22%22%2C%22my_range%22%3A%22-20%3B100%22%2C%22forecast%22%3A%7B%22upsidedownside%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22estimatesnumber%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22revisedup%22%3Atrue%2C%22reviseddown%22%3Atrue%2C%22consensus%22%3A%5B%5D%7D%2C%22dy%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_l%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22peg_ratio%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_vp%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margembruta%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemliquida%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22ev_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidaebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidapatrimonioliquido%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_sr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_capitalgiro%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativocirculante%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roe%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roic%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezcorrente%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22pl_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22passivo_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22giroativos%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22receitas_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lucros_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezmediadiaria%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22vpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22valormercado%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%7D&CategoryType=1'

options = webdriver.ChromeOptions()

# Set up the download folder
script_dir = os.path.dirname(os.path.abspath(__file__))
download_folder = os.path.join(script_dir, "cache")
os.makedirs(download_folder, exist_ok=True)

prefs = {
     "download.default_directory": download_folder,
     "download.prompt_for_download": False,
     "download.directory_upgrade": True,
     "safebrowsing.enabled": True
}

options.add_experimental_option("prefs", prefs)

options.add_argument('--headless=new')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=options)

# Remove the existing CSV file if it exists
if os.path.isfile(download_folder + '/statusinvest-busca-avancada.csv'):
    os.remove(download_folder + '/statusinvest-busca-avancada.csv')

# Remove the existing jsonl file if it exists
if os.path.isfile(download_folder + '/stocks_data.jsonl'):
    os.remove(download_folder + '/stocks_data.jsonl')

def formatNumber(number):
    if 'K' in number:
        number = number.replace('K', '')
        number = float(number) * 1000
    elif 'M' in number:
        number = float(number.replace('M', '')) * 1000000
    elif 'B' in number:
        number = float(number.replace('B', '')) * 1000000000
    else:
        number = float(number)

    return number

def getSectors(ticker):
        # Get sectors
        driver.get(f'https://statusinvest.com.br/acoes/{ticker}')
        sectors = driver.find_element("css selector", "div[class*='top-info top-info-1 top-info-sm-2 top-info-md-n sm d-flex justify-between']").text

        sectors = sectors.replace('arrow_forward', '')
        sectors = sectors.replace('\n', '')

        sectors = sectors.replace('SUBSETOR DE ATUAÇÃO', '\nSubsetor: ')
        sectors = sectors.replace('SETOR DE ATUAÇÃO', '\nSetor: ')
        sectors = sectors.replace('SEGMENTO DE ATUAÇÃO', '\nSegmento: ')

        # Parse sectors
        sectors = sectors.split('\n')
        for i, sector in enumerate(sectors):
            if 'Subsetor' in sector:
                subsetor = sector.replace('Subsetor: ', '')
            elif 'Setor' in sector:
                setor = sector.replace('Setor: ', '')
            elif 'Segmento' in sector:
                segmento = sector.replace('Segmento: ', '')

        return setor, subsetor, segmento

def getTagAlong(ticker):
    # Get tag along
    driver.get(f'https://statusinvest.com.br/acoes/{ticker}')
    tagAlong = driver.find_element("css selector", "div[class*='top-info top-info-1 top-info-sm-2 top-info-md-3 top-info-xl-n sm d-flex justify-between']").text

    lines = tagAlong.split('\n')
    
    # Find a line with the text "TAG ALONG"
    for i, line in enumerate(lines):
        if 'TAG ALONG' in line.upper():
            # Reserve the next two lines to three lines as well
            tagAlong = '\n'.join(lines[i:i+3])

            tagAlong = tagAlong.replace('help_outline', '\n')
            tagAlong = tagAlong.replace('\n', '')
            tagAlong = tagAlong.replace('TAG ALONG', '')
            tagAlong = tagAlong.replace(' ', '')
            tagAlong = tagAlong.replace('%', '')

            if not tagAlong == '--':
                float(tagAlong)

            return tagAlong

def getHistorialRentability(ticker):

    values = ['rent1dia', 'rent5dias', 'rent1mes', 'rent6meses', 'rentYTD', 'rent1ano', 'rent5anos', 'rentTotal']

    # Get annual return
    driver.get(f'https://br.tradingview.com/symbols/BMFBOVESPA-{ticker}/')
    try:
        historicalRent = driver.find_element("css selector", "div[class*='block-KzjYSOih']").text
    except Exception as e:
        historicalRent = ''

    historicalRent = historicalRent.replace('\n', '')

    historicalRent = historicalRent.replace('\u202a', '').replace('\u202c', '')

    historicalRent = historicalRent.replace(' K‬', 'K')
    historicalRent = historicalRent.replace('−', '-')
    historicalRent = historicalRent.replace('%', '')
    historicalRent = historicalRent.replace(',', '.')

    historicalRent = historicalRent.replace('1 dia', '\nRentabilidade 1 dia: ')
    historicalRent = historicalRent.replace('5 dias', '\nRentabilidade 5 dias: ')
    historicalRent = historicalRent.replace('1 mês', '\nRentabilidade 1 mês: ')
    historicalRent = historicalRent.replace('6 meses', '\nRentabilidade 6 meses: ')
    historicalRent = historicalRent.replace('Year to date', '\nRentabilidade Year to date: ')
    historicalRent = historicalRent.replace('1 ano', '\nRentabilidade 1 ano: ')
    historicalRent = historicalRent.replace('5 anos', '\nRentabilidade 5 anos: ')
    historicalRent = historicalRent.replace('Todos', '\nRentabilidade Total: ')

    # Parse historical rentability
    try:
        historicalRent = historicalRent.split('\n')
        for i, rent in enumerate(historicalRent):
            if 'Rentabilidade 1 dia: ' in rent:
                rent1dia = formatNumber(rent.replace('Rentabilidade 1 dia: ', '').strip())
            elif 'Rentabilidade 5 dias: ' in rent:
                rent5dias = formatNumber(rent.replace('Rentabilidade 5 dias: ', '').strip())
            elif 'Rentabilidade 1 mês: ' in rent:
                rent1mes = formatNumber(rent.replace('Rentabilidade 1 mês: ', '').strip())
            elif 'Rentabilidade 6 meses: ' in rent:
                rent6mes = formatNumber(rent.replace('Rentabilidade 6 meses: ', '').strip())
            elif 'Rentabilidade Year to date: ' in rent:
                rentYTD = formatNumber(rent.replace('Rentabilidade Year to date: ', '').strip())
            elif 'Rentabilidade 1 ano: ' in rent:
                rent1ano = formatNumber(rent.replace('Rentabilidade 1 ano: ', '').strip())
            elif 'Rentabilidade 5 anos: ' in rent:
                rent5anos = formatNumber(rent.replace('Rentabilidade 5 anos: ', '').strip())
            elif 'Rentabilidade Total: ' in rent:
                rentTotal = formatNumber(rent.replace('Rentabilidade Total: ', '').strip())

        rentMedia5anos = (rent5anos / 5)
    except Exception as e:
        rent1dia = ''
        rent5dias = ''
        rent1mes = ''
        rent6mes = ''
        rentYTD = ''
        rent1ano = ''
        rent5anos = ''
        rentTotal = ''
        rentMedia5anos = ''

    return rentMedia5anos, rent1dia, rent5dias, rent1mes, rent6mes, rentYTD, rent1ano, rent5anos, rentTotal

#
# Scrape data
#

# Download the CSV file
driver.get(csvUrl)
sleep(1)

output_path = os.path.join(download_folder, 'stocks_data.jsonl')

with open(output_path, 'w', encoding='utf-8') as jsonlfile:

    csvPath = download_folder + '\\statusinvest-busca-avancada.csv'
    reader = csv.DictReader(open(csvPath, 'r'), delimiter=';')

    for row in reader:
        row = {k: (v.replace(',', '.') if isinstance(v, str) else v) for k, v in row.items()}

        TICKER = row.get('TICKER', '')
        PRECO = row.get('PRECO', '') or row.get('PREÇO', '')
        DY = row.get('DY', '')
        P_L = row.get('P/L', '')
        P_VP = row.get('P/VP', '')
        P_ATIVOS = row.get('P/ATIVOS', '')
        MARGEM_BRUTA = row.get('MARGEM BRUTA', '')
        MARGEM_EBIT = row.get('MARGEM EBIT', '')
        MARG_LIQUIDA = row.get('MARG. LIQUIDA', '')
        P_EBIT = row.get('P/EBIT', '')
        EV_EBIT = row.get('EV/EBIT', '')
        DIVIDA_LIQUIDA_EBIT = row.get('DIVIDA LIQUIDA / EBIT', '')
        DIV_LIQ_PATRI = row.get('DIV. LIQ. / PATRI.', '')
        PSR = row.get('PSR', '')
        P_CAP_GIRO = row.get('P/CAP. GIRO', '')
        P_AT_CIR_LIQ = row.get('P. AT CIR. LIQ.', '')
        LIQ_CORRENTE = row.get('LIQ. CORRENTE', '')
        ROE = row.get('ROE', '')
        ROA = row.get('ROA', '')
        ROIC = row.get('ROIC', '')
        PATRIMONIO_ATIVOS = row.get('PATRIMONIO / ATIVOS', '')
        PASSIVOS_ATIVOS = row.get('PASSIVOS / ATIVOS', '')
        GIRO_ATIVOS = row.get('GIRO ATIVOS', '')
        CAGR_RECEITAS_5_ANOS = row.get('CAGR RECEITAS 5 ANOS', '')
        CAGR_LUCROS_5_ANOS = row.get('CAGR LUCROS 5 ANOS', '')
        LIQUIDEZ_MEDIA_DIARIA = row.get('LIQUIDEZ MEDIA DIARIA', '')
        VPA = row.get('VPA', '')
        LPA = row.get('LPA', '')
        PEG_RATIO = row.get('PEG Ratio', '')
        VALOR_DE_MERCADO = row.get('VALOR DE MERCADO', '')

        setor, subsetor, segmento = getSectors(TICKER)
        rentMedia5anos, rent1dia, rent5dias, rent1mes, rent6mes, rentYTD, rent1ano, rent5anos, rentTotal = getHistorialRentability(TICKER)
        tagAlong = getTagAlong(TICKER)

        stock = {
            'ticker': TICKER,
            'preco': PRECO,
            'dy': DY,
            'p_l': P_L,
            'p_vp': P_VP,
            'p_ativos': P_ATIVOS,
            'margem_bruta': MARGEM_BRUTA,
            'margem_ebit': MARGEM_EBIT,
            'marg_liquida': MARG_LIQUIDA,
            'p_ebit': P_EBIT,
            'ev_ebit': EV_EBIT,
            'divida_liquida_ebit': DIVIDA_LIQUIDA_EBIT,
            'div_liq_patri': DIV_LIQ_PATRI,
            'psr': PSR,
            'p_cap_giro': P_CAP_GIRO,
            'p_at_cir_liq': P_AT_CIR_LIQ,
            'liq_corrente': LIQ_CORRENTE,
            'roe': ROE,
            'roa': ROA,
            'roic': ROIC,
            'patrimonio_ativos': PATRIMONIO_ATIVOS,
            'passivos_ativos': PASSIVOS_ATIVOS,
            'giro_ativos': GIRO_ATIVOS,
            'cagr_receitas_5_anos': CAGR_RECEITAS_5_ANOS,
            'cagr_lucros_5_anos': CAGR_LUCROS_5_ANOS,
            'liquidez_media_diaria': LIQUIDEZ_MEDIA_DIARIA,
            'vpa': VPA,
            'lpa': LPA,
            'peg_ratio': PEG_RATIO,
            'valor_de_mercado': VALOR_DE_MERCADO,
            'setor': setor,
            'subsetor': subsetor,
            'segmento': segmento,
            'tag_along': tagAlong,
            'rent_media_5_anos': rentMedia5anos,
            'rent_1_dia': rent1dia,
            'rent_5_dias': rent5dias,
            'rent_1_mes': rent1mes,
            'rent_6_meses': rent6mes,
            'rent_ytd': rentYTD,
            'rent_1_ano': rent1ano,
            'rent_5_anos': rent5anos,
            'rent_total': rentTotal,
            'scrape_time': dataScrape
        }

        jsonlfile.write(json.dumps(stock, ensure_ascii=False) + '\n')
        print('\n' + str(stock))

driver.quit()

#
# Insert data into MySQL database
#

mysql_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'b3'
}

# Connect and create table if not exists
conn = mysql.connector.connect(**mysql_config)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS stocks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(16),
    preco FLOAT,
    dy FLOAT,
    p_l FLOAT,
    p_vp FLOAT,
    p_ativos FLOAT,
    margem_bruta FLOAT,
    margem_ebit FLOAT,
    marg_liquida FLOAT,
    p_ebit FLOAT,
    ev_ebit FLOAT,
    divida_liquida_ebit FLOAT,
    div_liq_patri FLOAT,
    psr FLOAT,
    p_cap_giro FLOAT,
    p_at_cir_liq FLOAT,
    liq_corrente FLOAT,
    roe FLOAT,
    roa FLOAT,
    roic FLOAT,
    patrimonio_ativos FLOAT,
    passivos_ativos FLOAT,
    giro_ativos FLOAT,
    cagr_receitas_5_anos FLOAT,
    cagr_lucros_5_anos FLOAT,
    liquidez_media_diaria FLOAT,
    vpa FLOAT,
    lpa FLOAT,
    peg_ratio FLOAT,
    valor_de_mercado FLOAT,
    setor VARCHAR(64),
    subsetor VARCHAR(64),
    segmento VARCHAR(64),
    tag_along VARCHAR(16),
    rent_media_5_anos FLOAT,
    rent_1_dia FLOAT,
    rent_5_dias FLOAT,
    rent_1_mes FLOAT,
    rent_6_meses FLOAT,
    rent_ytd FLOAT,
    rent_1_ano FLOAT,
    rent_5_anos FLOAT,
    rent_total FLOAT,
    scrape_time DATETIME
)
""")
conn.commit()

# Prepare for batch insert
fields = [
    'ticker', 'preco', 'dy', 'p_l', 'p_vp', 'p_ativos', 'margem_bruta', 'margem_ebit', 'marg_liquida',
    'p_ebit', 'ev_ebit', 'divida_liquida_ebit', 'div_liq_patri', 'psr', 'p_cap_giro', 'p_at_cir_liq',
    'liq_corrente', 'roe', 'roa', 'roic', 'patrimonio_ativos', 'passivos_ativos', 'giro_ativos',
    'cagr_receitas_5_anos', 'cagr_lucros_5_anos', 'liquidez_media_diaria', 'vpa', 'lpa', 'peg_ratio',
    'valor_de_mercado', 'setor', 'subsetor', 'segmento', 'tag_along', 'rent_media_5_anos', 'rent_1_dia',
    'rent_5_dias', 'rent_1_mes', 'rent_6_meses', 'rent_ytd', 'rent_1_ano', 'rent_5_anos', 'rent_total',
    'scrape_time'
]

to_insert = []
with open('cache/stocks_data.jsonl', encoding='utf-8') as f:
    for line in f:
        if not line.strip():
            continue
        stock = json.loads(line)
        row = []
        for key in fields:
            v = stock.get(key, None)
            if isinstance(v, str) and v.strip() == '':
                v = None
            if key not in ['ticker', 'setor', 'subsetor', 'segmento', 'tag_along', 'scrape_time'] and v is not None:
                try:
                    v = float(v)
                except Exception:
                    v = None
            row.append(v)
        to_insert.append(tuple(row))

# Batch insert
sql = f"""
    INSERT INTO stocks ({', '.join(fields)})
    VALUES ({', '.join(['%s'] * len(fields))})
"""
cursor.executemany(sql, to_insert)
conn.commit()
cursor.close()
conn.close()