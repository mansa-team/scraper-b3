import os
import math
import json
import csv
import time
import concurrent.futures
import re

import selenium
import seleniumbase
from selenium_stealth import stealth

import pymysql
import chromedriver_autoinstaller

from selenium import webdriver
from seleniumbase import Driver

from dotenv import load_dotenv
from time import sleep
from datetime import datetime

# Configs

max_workers = 4

start_time = time.time()
current_year = datetime.now().year

load_dotenv()

script_dir = os.path.dirname(os.path.abspath(__file__))
download_folder = os.path.join(script_dir, "cache")
os.makedirs(download_folder, exist_ok=True)

csvPath = os.path.join(download_folder, 'statusinvest-busca-avancada.csv')

output_path = os.path.join(download_folder, 'stocks_data.jsonl')
dateScrape = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

csvUrl = 'https://statusinvest.com.br/category/AdvancedSearchResultExport?search=%7B%22Sector%22%3A%22%22%2C%22SubSector%22%3A%22%22%2C%22Segment%22%3A%22%22%2C%22my_range%22%3A%22-20%3B100%22%2C%22forecast%22%3A%7B%22upsidedownside%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22estimatesnumber%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22revisedup%22%3Atrue%2C%22reviseddown%22%3Atrue%2C%22consensus%22%3A%5B%5D%7D%2C%22dy%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_l%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22peg_ratio%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_vp%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margembruta%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemliquida%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22ev_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidaebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidapatrimonioliquido%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_sr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_capitalgiro%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativocirculante%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roe%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roic%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezcorrente%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22pl_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22passivo_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22giroativos%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22receitas_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lucros_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezmediadiaria%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22vpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22valormercado%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%7D&CategoryType=1'

csvUrlTest = 'https://statusinvest.com.br/category/AdvancedSearchResultExport?search=%7B%22Sector%22%3A%2210%22%2C%22SubSector%22%3A%2243%22%2C%22Segment%22%3A%2284%22%2C%22my_range%22%3A%22-20%3B100%22%2C%22forecast%22%3A%7B%22upsidedownside%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22estimatesnumber%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22revisedup%22%3Atrue%2C%22reviseddown%22%3Atrue%2C%22consensus%22%3A%5B%5D%7D%2C%22dy%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_l%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22peg_ratio%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_vp%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margembruta%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemliquida%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22ev_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidaebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidapatrimonioliquido%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_sr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_capitalgiro%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativocirculante%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roe%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roic%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezcorrente%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22pl_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22passivo_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22giroativos%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22receitas_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lucros_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezmediadiaria%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22vpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22valormercado%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%7D&CategoryType=1'

#
# Setup Selenium WebDriver
#

chromedriver_autoinstaller.install()

options = webdriver.ChromeOptions()

prefs = {
    "download.default_directory": download_folder,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "profile.managed_default_content_settings.images": 2,
    "safebrowsing.enabled": False
}

options.add_experimental_option("prefs", prefs)
options.add_argument('--headless=new')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('user-agent=VALID_USER_AGENT')

options.add_argument('--disable-logging')
options.add_argument('--log-level=3')
options.add_argument('--silent')

options.add_extension(os.path.join(script_dir, 'data/ublock.crx'))

driver = webdriver.Chrome(
    options=options
)

stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
)

#
# Util
#

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
    return round(number, 2)

def scrape_stock(row):
    options = webdriver.ChromeOptions()

    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.javascript": 2,
        "safebrowsing.enabled": False
    }

    options.add_experimental_option("prefs", prefs)
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=VALID_USER_AGENT')

    options.add_argument('--disable-logging')
    options.add_argument('--log-level=3')
    options.add_argument('--silent')

    options.add_extension(os.path.join(script_dir, 'data/ublock.crx'))

    driver = webdriver.Chrome(
        options=options
    )

    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
    )

    try:
        TICKER = row.get('TICKER', '')
        PRECO = row.get('PRECO', '') or row.get('PREÇO', '')
        DY_12_MESES = row.get('DY', '')
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

        # Ensure PRECO is a valid float and not zero
        try:
            PRECO = float(PRECO)
        except Exception:
            PRECO = PRECO

        # Get sectors
        driver.get(f'https://statusinvest.com.br/acoes/{TICKER}')
        try:
            sectors = driver.find_element("css selector", "div[class*='top-info top-info-1 top-info-sm-2 top-info-md-n sm d-flex justify-between']").text
            sectors = sectors.replace('arrow_forward', '').replace('\n', '')
            sectors = sectors.replace('SUBSETOR DE ATUAÇÃO', '\nSubsetor: ')
            sectors = sectors.replace('SETOR DE ATUAÇÃO', '\nSetor: ')
            sectors = sectors.replace('SEGMENTO DE ATUAÇÃO', '\nSegmento: ')
            sectors = sectors.split('\n')
            setor = subsetor = segmento = ''
            for sector in sectors:
                if 'Subsetor' in sector:
                    subsetor = sector.replace('Subsetor: ', '')
                elif 'Setor' in sector:
                    setor = sector.replace('Setor: ', '')
                elif 'Segmento' in sector:
                    segmento = sector.replace('Segmento: ', '')
        except Exception:
            setor = subsetor = segmento = ''

        # Get tag along
        try:
            tagAlong = driver.find_element("css selector", "div[class*='top-info top-info-1 top-info-sm-2 top-info-md-3 top-info-xl-n sm d-flex justify-between']").text
            lines = tagAlong.split('\n')
            for i, line in enumerate(lines):
                if 'TAG ALONG' in line.upper():
                    tagAlong = '\n'.join(lines[i:i+3])
                    tagAlong = tagAlong.replace('help_outline', '\n')
                    tagAlong = tagAlong.replace('\n', '')
                    tagAlong = tagAlong.replace('TAG ALONG', '')
                    tagAlong = tagAlong.replace(' ', '')
                    tagAlong = tagAlong.replace('%', '')
                    if tagAlong == '--':
                        tagAlong = ''
                    elif not tagAlong == '--':
                        float(tagAlong)
                    break
        except Exception:
            tagAlong = ''

        # Get historical rentability
        try:
            driver.get(f'https://br.tradingview.com/symbols/BMFBOVESPA-{TICKER}/')
            historicalRent = driver.find_element("css selector", "div[class*='block-KzjYSOih']").text
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
            rent1dia = rent5dias = rent1mes = rent6mes = rent_12meses = rent1ano = rent5anos = rentTotal = rentMedia5anos = ''
            historicalRent = historicalRent.split('\n')
            for rent in historicalRent:
                if 'Rentabilidade 1 dia: ' in rent:
                    rent1dia = formatNumber(rent.replace('Rentabilidade 1 dia: ', '').strip())
                elif 'Rentabilidade 5 dias: ' in rent:
                    rent5dias = formatNumber(rent.replace('Rentabilidade 5 dias: ', '').strip())
                elif 'Rentabilidade 1 mês: ' in rent:
                    rent1mes = formatNumber(rent.replace('Rentabilidade 1 mês: ', '').strip())
                elif 'Rentabilidade 6 meses: ' in rent:
                    rent6mes = formatNumber(rent.replace('Rentabilidade 6 meses: ', '').strip())
                elif 'Rentabilidade Year to date: ' in rent:
                    rent_12meses = formatNumber(rent.replace('Rentabilidade Year to date: ', '').strip())
                elif 'Rentabilidade 1 ano: ' in rent:
                    rent1ano = formatNumber(rent.replace('Rentabilidade 1 ano: ', '').strip())
                elif 'Rentabilidade 5 anos: ' in rent:
                    rent5anos = formatNumber(rent.replace('Rentabilidade 5 anos: ', '').strip())
                elif 'Rentabilidade Total: ' in rent:
                    rentTotal = formatNumber(rent.replace('Rentabilidade Total: ', '').strip())
            try:
                rentMedia5anos = (rent5anos / 5)
            except Exception:
                rentMedia5anos = ''
        except Exception:
            rent1dia = rent5dias = rent1mes = rent6mes = rent_12meses = rent1ano = rent5anos = rentTotal = rentMedia5anos = ''

        # Get historical dividends yields
        try:
            driver.get(f'https://statusinvest.com.br/acao/companytickerprovents?companyName=&ticker={TICKER}&chartProventsType=2')
            jsonData = (driver.find_element('xpath', '/html/body/pre').text)
            jsonData = json.loads(jsonData)

            dividendList = {int(item['rank']): float(item['value']) for item in jsonData.get('assetEarningsYearlyModels', [])}

            try:
                dy_medio_5anos = (((dividendList[(current_year - 1)] + dividendList[(current_year - 2)] + dividendList[(current_year - 3)] + dividendList[(current_year - 4)] + dividendList[(current_year - 5)])/5) / PRECO) * 100
                dy_medio_5anos = round(dy_medio_5anos, 2)
            except Exception:
                dy_medio_5anos = ''

            dy = {}
            for year, value in dividendList.items():
                if PRECO is not None and PRECO > 0:
                    try:
                        dividendyield = (value / PRECO) * 100
                        dividendyield = round(dividendyield, 2)
                    except Exception:
                        dividendyield = None
                else:
                    dividendyield = None
                dy[year] = dividendyield
        except Exception:
            dy_medio_5anos = ''
            dy = {}

        valid_years = lambda y: isinstance(y, int) and 1000 <= y <= 9999
        dividendList = {year: value for year, value in dividendList.items() if valid_years(year)}
        dy = {year: value for year, value in dy.items() if valid_years(year)}

        # Get historical liquidity per year
        try:
            driver.get(f'https://statusinvest.com.br/acao/payoutresult?code={TICKER}&companyid=0&type=2')
            jsonData = (driver.find_element('xpath', '/html/body/pre').text)
            jsonData = json.loads(jsonData)

            years = [int(year) for year in jsonData['chart']['category']]
            annualLiquidity = [float(item['value']) for item in jsonData['chart']['series']['lucroLiquido']]
            annualLiquidity = dict(zip(years, annualLiquidity))

        except Exception:
            annualLiquidity = {}

        # Create stock dictionary
        stock = {
            'scrape_time': dateScrape,
            'ticker': TICKER,
            'setor': setor,
            'subsetor': subsetor,
            'segmento': segmento,
            'preco': PRECO,
            'rent_12meses': rent_12meses,
            'rent_media_5_anos': rentMedia5anos,
            'tag_along': tagAlong,
            'dy_12_meses': DY_12_MESES,
            'dy_medio_5_anos': dy_medio_5anos,
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
            'rent_1_dia': rent1dia,
            'rent_5_dias': rent5dias,
            'rent_1_mes': rent1mes,
            'rent_6_meses': rent6mes,
            'rent_1_ano': rent1ano,
            'rent_5_anos': rent5anos,
            'rent_total': rentTotal
        }
        # Increment dynamic dividendos_{year}, dy_{year}, and annualLiquidity_{year} into stock dict
        for year, value in dividendList.items():
            stock[f'dividendos_{year}'] = value
        for year, value in dy.items():
            stock[f'dy_{year}'] = value
        for year, value in annualLiquidity.items():
            stock[f'annualLiquidity_{year}'] = value
        print('\n', stock)
        return stock
    except Exception as e:
        return None
    finally:
        driver.quit()

# Test Selenium WebDriver
driver.get('http://github.com')
print(driver.title)

# Test Cloudflare Status in StatusInvest
driver.get('https://statusinvest.com.br/')
print(f'StatusInvest: {driver.title}')

# Test Cloudflare Status in TradingView
driver.get('https://br.tradingview.com')
print(f'TradingView: {driver.title}')

# Remove the existing CSV file if it exists
if os.path.isfile(download_folder + '/statusinvest-busca-avancada.csv'):
    os.remove(download_folder + '/statusinvest-busca-avancada.csv')

# Remove the existing jsonl file if it exists
if os.path.isfile(download_folder + '/stocks_data.jsonl'):
    os.remove(download_folder + '/stocks_data.jsonl')

# Set up the download folder
script_dir = os.path.dirname(os.path.abspath(__file__))
download_folder = os.path.join(script_dir, "cache")
os.makedirs(download_folder, exist_ok=True)

# Download the CSV file
print(f"Downloading CSV file to {download_folder}")
driver.get(csvUrl)
print(driver.title)

# Wait for the CSV file to be downloaded
timeout = 15  # seconds
start_wait = time.time()
while not os.path.exists(csvPath):
    if time.time() - start_wait > timeout:
        raise TimeoutError(f'File {csvPath} was not found within {timeout} seconds.')

    time.sleep(1)

driver.quit()

# Read CSV rows
reader = csv.DictReader(open(csvPath, 'r'), delimiter=';')
rows = []
for row in reader:
    row = {k: (v.replace(',', '.') if isinstance(v, str) else v) for k, v in row.items()}
    rows.append(row)

# Threaded scraping
results = []
with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
    for stock in executor.map(scrape_stock, rows):
        if stock:
            results.append(stock)
            print('\n' + str(stock))

# Write results to JSONL
with open(output_path, 'w', encoding='utf-8') as jsonlfile:
    for stock in results:
        jsonlfile.write(json.dumps(stock, ensure_ascii=False) + '\n')

#
# Export to MySQL
#

mysql_config = {
    'user': os.getenv('MYSQL_USER') or os.environ.get('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD') or os.environ.get('MYSQL_PASSWORD'),
    'host': os.getenv('MYSQL_HOST') or os.environ.get('MYSQL_HOST'),
    'database': os.getenv('MYSQL_DATABASE') or os.environ.get('MYSQL_DATABASE')
}

conn = pymysql.connect(
    host=mysql_config['host'],
    user=mysql_config['user'],
    password=mysql_config['password'],
    database=mysql_config['database'],
    charset='utf8mb4',
    autocommit=False
)
cursor = conn.cursor()

# Collect all years for dividendos, dy, and annualLiquidity before CREATE TABLE
all_dividendos_years = set()
all_dy_years = set()
all_annualLiquidity_years = set()
year_pattern = re.compile(r'_(\d{4})$')
for stock in results:
    for key in stock.keys():
        m = year_pattern.search(key)
        if key.startswith('dividendos_') and m:
            year = m.group(1)
            all_dividendos_years.add(year)
        if key.startswith('dy_') and m:
            year = m.group(1)
            all_dy_years.add(year)
        if key.startswith('annualLiquidity_') and m:
            year = m.group(1)
            all_annualLiquidity_years.add(year)
fields = [
    'scrape_time', 'ticker', 'setor', 'subsetor', 'segmento', 'preco', 'rent_12meses', 'rent_media_5_anos',
    'tag_along', 'dy_12_meses', 'dy_medio_5_anos', 'p_l', 'p_vp', 'p_ativos', 'margem_bruta', 'margem_ebit',
    'marg_liquida', 'p_ebit', 'ev_ebit', 'divida_liquida_ebit', 'div_liq_patri', 'psr', 'p_cap_giro',
    'p_at_cir_liq', 'liq_corrente', 'roe', 'roa', 'roic', 'patrimonio_ativos', 'passivos_ativos', 'giro_ativos',
    'cagr_receitas_5_anos', 'cagr_lucros_5_anos', 'liquidez_media_diaria', 'vpa', 'lpa', 'peg_ratio',
    'valor_de_mercado', 'rent_1_dia', 'rent_5_dias', 'rent_1_mes', 'rent_6_meses', 'rent_1_ano', 
    'rent_5_anos', 'rent_total'
]
fields += [f'dividendos_{year}' for year in sorted(all_dividendos_years)]
fields += [f'dy_{year}' for year in sorted(all_dy_years)]
fields += [f'annualLiquidity_{year}' for year in sorted(all_annualLiquidity_years)]

# Ensure all dynamic columns exist in the table (for new years)
def ensure_column_exists(cursor, table, col):
    cursor.execute(f"SHOW COLUMNS FROM {table} LIKE '{col}'")
    if not cursor.fetchone():
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} FLOAT")
        conn.commit()

# Create table if not exists
col_dividendos = ',\n    '.join([f'dividendos_{year} FLOAT' for year in sorted(all_dividendos_years)])
col_dy = ',\n    '.join([f'dy_{year} FLOAT' for year in sorted(all_dy_years)])
col_annualLiquidity = ',\n    '.join([f'annualLiquidity_{year} FLOAT' for year in sorted(all_annualLiquidity_years)])

dynamic_columns = ''
if col_dividendos:
    dynamic_columns += ',\n    ' + col_dividendos
if col_dy:
    dynamic_columns += ',\n    ' + col_dy
if col_annualLiquidity:
    dynamic_columns += ',\n    ' + col_annualLiquidity

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS stocks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    scrape_time DATETIME,
    ticker VARCHAR(16),
    setor VARCHAR(64),
    subsetor VARCHAR(64),
    segmento VARCHAR(64),
    preco FLOAT,
    rent_12meses FLOAT,
    rent_media_5_anos FLOAT,
    tag_along VARCHAR(16),
    dy_12_meses FLOAT,
    dy_medio_5_anos FLOAT,
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
    rent_1_dia FLOAT,
    rent_5_dias FLOAT,
    rent_1_mes FLOAT,
    rent_6_meses FLOAT,
    rent_1_ano FLOAT,
    rent_5_anos FLOAT,
    rent_total FLOAT{dynamic_columns}
)
""")
conn.commit()

# Ensure all dynamic columns exist (for new years)
for col in [f'dividendos_{year}' for year in sorted(all_dividendos_years)] + [f'dy_{year}' for year in sorted(all_dy_years)] + [f'annualLiquidity_{year}' for year in sorted(all_annualLiquidity_years)]:
    ensure_column_exists(cursor, 'stocks', col)

# Add dynamic columns to fields list if not present
for year in sorted(all_dividendos_years):
    col_name = f'dividendos_{year}'
    if col_name not in fields:
        fields.append(col_name)
for year in sorted(all_dy_years):
    col_name = f'dy_{year}'
    if col_name not in fields:
        fields.append(col_name)
for year in sorted(all_annualLiquidity_years):
    col_name = f'annualLiquidity_{year}'
    if col_name not in fields:
        fields.append(col_name)

to_insert = []
for stock in results:
    row = []
    for key in fields:
        v = stock.get(key, None)
        if isinstance(v, str) and v.strip() == '':
            v = None
        if key not in ['ticker', 'setor', 'subsetor', 'segmento', 'scrape_time'] and v is not None:
            try:
                v = float(v)
            except Exception:
                v = None
        row.append(v)
    try:
        # Try to insert this row individually for better error reporting
        sql = f"""
            INSERT INTO stocks ({', '.join(fields)})
            VALUES ({', '.join(['%s'] * len(fields))})
        """
        cursor.execute(sql, tuple(row))
    except Exception as e:
        print(f"Error inserting stock {stock.get('ticker', '')}: {e}")
        print("Row data:", row)
        import traceback
        traceback.print_exc()
        continue

conn.commit()
cursor.close()
conn.close()

print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")