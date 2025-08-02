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

from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from dotenv import load_dotenv
from time import sleep
from datetime import datetime

# Configs
max_workers = 6
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

def ChromeOptions():
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
    options.add_argument('--log-level=3')
    options.add_argument('--disable-logging')
    options.add_argument('--silent')
    options.add_argument('--remote-debugging-port=0')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    return options

driver = webdriver.Chrome(options=ChromeOptions())

stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
)

def formatNumber(number):
    if isinstance(number, (int, float)):
        return round(number, 2)
    
    if not isinstance(number, str):
        return None
        
    number = number.strip()
    if number == '' or number == '--' or number == '-':
        return None
        
    cleaned = re.sub(r'[^\d.,\-KMB]', '', number)
    cleaned = cleaned.replace(',', '.')
    
    try:
        if 'K' in cleaned:
            cleaned = cleaned.replace('K', '')
            return round(float(cleaned) * 1000, 2)
        elif 'M' in cleaned:
            cleaned = cleaned.replace('M', '')
            return round(float(cleaned) * 1000000, 2)
        elif 'B' in cleaned:
            cleaned = cleaned.replace('B', '')
            return round(float(cleaned) * 1000000000, 2)
        else:
            return round(float(cleaned), 2)
    except (ValueError, TypeError):
        return None

def scrape_stock(row):
    driver = webdriver.Chrome(options=ChromeOptions())

    # Set timeouts to speed up operations
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(5)

    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
    )

    try:
        TICKER = row.get('TICKER', '').strip()
        if not TICKER:
            print("ERROR: No ticker found in row")
            return None
        
        # Initialize all variables at the start to avoid scope issues
        setor = subsetor = segmento = ''
        tagAlong = ''
        rent1dia = rent5dias = rent1mes = rent6mes = rent_12meses = rent1ano = rent5anos = rentTotal = rentMedia5anos = ''
        dividendList = {}
        dy = {}
        dy_medio_5_anos = ''
        annualLiquidity = {}
        
        if not hasattr(scrape_stock, '_debug_printed'):
            print("Available CSV columns:", list(row.keys()))
            print("Sample row data:", {k: v for k, v in list(row.items())[:10]})
            scrape_stock._debug_printed = True
        
        # Extract CSV values
        def getCSVvalue(names, return_string=False):
            for name in names:
                if name in row:
                    value = row[name]
                    if value and str(value).strip() not in ['--', '-', '', 'N/A', 'nan', '0,00']:
                        if return_string:
                            return str(value).strip()
                        try:
                            return formatNumber(value)
                        except:
                            return str(value).strip() if return_string else None
            return None
        
        PRECO = getCSVvalue(['PRECO', 'PREÇO'])
        DY_12_MESES = getCSVvalue(['DY'], return_string=True)
        P_L = getCSVvalue(['P/L'], return_string=True)
        P_VP = getCSVvalue(['P/VP'], return_string=True)
        P_ATIVOS = getCSVvalue(['P/ATIVOS'], return_string=True)
        MARGEM_BRUTA = getCSVvalue(['MARGEM BRUTA'], return_string=True)
        MARGEM_EBIT = getCSVvalue(['MARGEM EBIT'], return_string=True)
        MARG_LIQUIDA = getCSVvalue(['MARG. LIQUIDA'], return_string=True)
        P_EBIT = getCSVvalue(['P/EBIT'], return_string=True)
        EV_EBIT = getCSVvalue(['EV/EBIT'], return_string=True)
        DIVIDA_LIQUIDA_EBIT = getCSVvalue(['DIVIDA LIQUIDA / EBIT'], return_string=True)
        DIV_LIQ_PATRI = getCSVvalue(['DIV. LIQ. / PATRI.'], return_string=True)
        PSR = getCSVvalue(['PSR'], return_string=True)
        P_CAP_GIRO = getCSVvalue(['P/CAP. GIRO'], return_string=True)
        P_AT_CIR_LIQ = getCSVvalue(['P. AT CIR. LIQ.'], return_string=True)
        LIQ_CORRENTE = getCSVvalue(['LIQ. CORRENTE'], return_string=True)
        ROE = getCSVvalue(['ROE'], return_string=True)
        ROA = getCSVvalue(['ROA'], return_string=True)
        ROIC = getCSVvalue(['ROIC'], return_string=True)
        PATRIMONIO_ATIVOS = getCSVvalue(['PATRIMONIO / ATIVOS'], return_string=True)
        PASSIVOS_ATIVOS = getCSVvalue(['PASSIVOS / ATIVOS'], return_string=True)
        GIRO_ATIVOS = getCSVvalue(['GIRO ATIVOS'], return_string=True)
        CAGR_RECEITAS_5_ANOS = getCSVvalue(['CAGR RECEITAS 5 ANOS'], return_string=True)
        CAGR_LUCROS_5_ANOS = getCSVvalue(['CAGR LUCROS 5 ANOS'], return_string=True)
        
        liquidez_cols = [k for k in row.keys() if 'LIQUIDEZ' in k.upper()]
        vpa_cols = [k for k in row.keys() if 'VPA' in k.upper()]
        lpa_cols = [k for k in row.keys() if 'LPA' in k.upper()]
        peg_cols = [k for k in row.keys() if 'PEG' in k.upper()]
        valor_cols = [k for k in row.keys() if 'VALOR' in k.upper()]
        
        LIQUIDEZ_MEDIA_DIARIA = getCSVvalue([
            ' LIQUIDEZ MEDIA DIARIA', 'LIQUIDEZ MEDIA DIARIA', 'LIQUIDEZ MÉDIA DIÁRIA',
            'LIQUIDEZ DIÁRIA MÉDIA', ' LIQUIDEZ DIÁRIA MÉDIA', 'LIQUIDEZ_MEDIA_DIARIA'
        ] + liquidez_cols, return_string=True)
        
        VPA = getCSVvalue([
            ' VPA', 'VPA', '_VPA', 'VPA_', ' VPA ', '  VPA'
        ] + vpa_cols, return_string=True)
        
        LPA = getCSVvalue([
            ' LPA', 'LPA', '_LPA', 'LPA_', ' LPA ', '  LPA'
        ] + lpa_cols, return_string=True)
        
        PEG_RATIO = getCSVvalue([
            ' PEG Ratio', 'PEG Ratio', 'PEG RATIO', '_PEG_Ratio', ' PEG_Ratio',
            'PEG_RATIO', 'PEG_Ratio', ' PEG RATIO', '  PEG Ratio'
        ] + peg_cols, return_string=True)
        
        VALOR_DE_MERCADO = getCSVvalue([
            ' VALOR DE MERCADO', 'VALOR DE MERCADO', '_VALOR_DE_MERCADO',
            ' VALOR_DE_MERCADO', 'VALOR_DE_MERCADO', '  VALOR DE MERCADO'
        ] + valor_cols, return_string=True)
        
        try:
            driver.get(f'https://statusinvest.com.br/acoes/{TICKER}')
            
            sectors_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='top-info top-info-1 top-info-sm-2 top-info-md-n sm d-flex justify-between']"))
            )
            sectors = sectors_element.text
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
        except Exception as e:
            print(f"Error getting sectors for {TICKER}: {e}")
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
                    replaceList = ['TAG ALONG', ' ', '%']
                    for item in replaceList:
                        tagAlong = tagAlong.replace(item, '')
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
                dy_medio_5_anos = (((dividendList[(current_year - 1)] + dividendList[(current_year - 2)] + dividendList[(current_year - 3)] + dividendList[(current_year - 4)] + dividendList[(current_year - 5)])/5) / PRECO) * 100
                dy_medio_5_anos = round(dy_medio_5_anos, 2)
            except Exception:
                dy_medio_5_anos = ''

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
            dy_medio_5_anos = ''
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

        # Calculate Barsi's preco teto
        try:
            preco_teto_barsi = ((dividendList[(current_year - 1)] + dividendList[(current_year - 2)] + dividendList[(current_year - 3)] + dividendList[(current_year - 4)] + dividendList[(current_year - 5)])/5)/0.06
            preco_teto_barsi = round(float(preco_teto_barsi), 2)
        except Exception:
            preco_teto_barsi = ''

        # Calculate Graham's preco teto
        try:
            preco_teto_graham = math.sqrt((22.5 * (float(LPA) if LPA else 0) * (float(VPA) if VPA else 0)))
            preco_teto_graham = round(preco_teto_graham, 2)
        except Exception:
            preco_teto_graham = ''

        # Create stock dictionary
        stock = {
            'scrape_time': dateScrape,
            'ticker': TICKER,
            'setor': setor,
            'subsetor': subsetor,
            'segmento': segmento,
            'preco': PRECO,
            'preco_teto_barsi': preco_teto_barsi,
            'preco_teto_graham': preco_teto_graham,
            'rent_12meses': rent_12meses,
            'rent_media_5_anos': rentMedia5anos,
            'tag_along': tagAlong,
            'dy_12_meses': DY_12_MESES,
            'dy_medio_5_anos': dy_medio_5_anos,
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
        print(f"ERROR processing {TICKER}: {e}")
        
        try:
            partial_stock = {
                'scrape_time': dateScrape,
                'ticker': TICKER,
                'setor': setor,
                'subsetor': subsetor,
                'segmento': segmento,
                'preco': PRECO,
                'preco_teto_barsi': preco_teto_barsi,
                'preco_teto_graham': preco_teto_graham,
                'rent_12meses': rent_12meses,
                'rent_media_5_anos': rentMedia5anos,
                'tag_along': tagAlong,
                'dy_12_meses': DY_12_MESES,
                'dy_medio_5_anos': dy_medio_5_anos,
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
            
            # Add dynamic dividend/dy/liquidity data that was successfully scraped
            for year, value in dividendList.items():
                partial_stock[f'dividendos_{year}'] = value
            for year, value in dy.items():
                partial_stock[f'dy_{year}'] = value
            for year, value in annualLiquidity.items():
                partial_stock[f'annualLiquidity_{year}'] = value
                
            print(f"Saved partial data for {TICKER} (some sections may have failed)")
            return partial_stock
        except Exception as save_error:
            print(f"Failed to save even partial data for {TICKER}: {save_error}")
            return None
    finally:
        try:
            driver.quit()
        except:
            pass

# Test connections
driver.get('http://github.com')
print(driver.title)
driver.get('https://statusinvest.com.br/')
print(f'StatusInvest: {driver.title}')
driver.get('https://br.tradingview.com')
print(f'TradingView: {driver.title}')

# Clean up old files
for file in ['statusinvest-busca-avancada.csv', 'stocks_data.jsonl']:
    filepath = os.path.join(download_folder, file)
    if os.path.exists(filepath):
        os.remove(filepath)

# Download CSV
driver.get(csvUrl)

timeout = 15
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
failed_count = 0
total_stocks = len(rows)

print(f"Starting to process {total_stocks} stocks with {max_workers} threads...")

start_scraping_time = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Submit all tasks
    future_to_row = {executor.submit(scrape_stock, row): row for row in rows}
    
    # Process results as they complete
    for future in concurrent.futures.as_completed(future_to_row):
        row = future_to_row[future]
        ticker = row.get('TICKER', 'UNKNOWN')
        
        try:
            stock = future.result(timeout=30)
            if stock:
                results.append(stock)
            else:
                failed_count += 1
                
        except concurrent.futures.TimeoutError:
            failed_count += 1
            print(f"{ticker} timed out")
            
        except Exception as e:
            failed_count += 1
            print(f"{ticker} error: {str(e)[:50]}...")

print(f"\nScraping completed - {len(results)} successful, {failed_count} failed out of {total_stocks} total")

# Write results to JSONL
with open(output_path, 'w', encoding='utf-8') as jsonlfile:
    for stock in results:
        jsonlfile.write(json.dumps(stock, ensure_ascii=False) + '\n')

# Export to MySQL
mysql_config = {
    'user': os.getenv('MYSQL_USER') or os.environ.get('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD') or os.environ.get('MYSQL_PASSWORD'),
    'host': os.getenv('MYSQL_HOST') or os.environ.get('MYSQL_HOST'),
    'database': os.getenv('MYSQL_DATABASE') or os.environ.get('MYSQL_DATABASE')
}

conn = pymysql.connect(**mysql_config, charset='utf8mb4', autocommit=False)
cursor = conn.cursor()

# Collect dynamic years from results
all_dividendos_years = set()
all_dy_years = set()
all_annualLiquidity_years = set()
year_pattern = re.compile(r'_(\d{4})$')

for stock in results:
    for key in stock.keys():
        m = year_pattern.search(key)
        if m:
            year = m.group(1)
            if key.startswith('dividendos_'):
                all_dividendos_years.add(year)
            elif key.startswith('dy_'):
                all_dy_years.add(year)
            elif key.startswith('annualLiquidity_'):
                all_annualLiquidity_years.add(year)

# Build field list
fields = [
    'scrape_time', 'ticker', 'setor', 'subsetor', 'segmento', 'preco', 'preco_teto_barsi', 'preco_teto_graham', 'rent_12meses', 'rent_media_5_anos',
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

# Ensure all dynamic columns exist in the table
def ensure_column_exists(cursor, table, col):
    cursor.execute(f"SHOW COLUMNS FROM {table} LIKE '{col}'")
    if not cursor.fetchone():
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} FLOAT")
        conn.commit()

# Create table
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
    preco_teto_barsi FLOAT,
    preco_teto_graham FLOAT,
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

# Ensure all dynamic columns exist
for col in [f'dividendos_{year}' for year in sorted(all_dividendos_years)] + [f'dy_{year}' for year in sorted(all_dy_years)] + [f'annualLiquidity_{year}' for year in sorted(all_annualLiquidity_years)]:
    ensure_column_exists(cursor, 'stocks', col)

# Insert data
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
        sql = f"INSERT INTO stocks ({', '.join(fields)}) VALUES ({', '.join(['%s'] * len(fields))})"
        cursor.execute(sql, tuple(row))
    except Exception as e:
        print(f"Error inserting stock {stock.get('ticker', '')}: {e}")
        continue

conn.commit()
cursor.close()
conn.close()

print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")