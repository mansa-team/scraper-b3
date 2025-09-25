#
#$ Import Libraries
#
import os
import math
import json
import time

import pandas as pd
import numpy as np

import pymysql
from sqlalchemy import create_engine
from sqlalchemy import types

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from dotenv import load_dotenv
from time import sleep
from datetime import datetime

from tenacity import retry, stop_after_attempt, wait_exponential

#
#$ Script Configuration and Basic Setup
#
saveToMYSQL = True
saveAsJSONL = True

"""
Create a .env file in the root directory and put your user information for your MYSQL database like the following:
MYSQL_USER=user
MYSQL_PASSWORD=password
MYSQL_HOST=ip
MYSQL_DATABASE=database

The os.environ alternative is for automations using Github Actions, in which, you create a secret key in github and use it as your system variable
"""

load_dotenv()

confgMySQL = {
    'host': os.getenv('MYSQL_HOST') or os.environ.get('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER') or os.environ.get('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD') or os.environ.get('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE') or os.environ.get('MYSQL_DATABASE')
}
engine = create_engine(f"mysql+pymysql://{confgMySQL['user']}:{confgMySQL['password']}@{confgMySQL['host']}/{confgMySQL['database']}")

start_time = time.time()
current_year = datetime.now().year
dateScrape = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

scriptDirectory = os.path.dirname(os.path.abspath(__file__))
downloadFolder = os.path.join(scriptDirectory, 'cache')
csvFilePath = os.path.join(downloadFolder, 'statusinvest-busca-avancada.csv')

csvFileURL = f'https://statusinvest.com.br/category/AdvancedSearchResultExport?search=%7B%22Sector%22%3A%22%22%2C%22SubSector%22%3A%22%22%2C%22Segment%22%3A%22%22%2C%22my_range%22%3A%22-20%3B100%22%2C%22forecast%22%3A%7B%22upsidedownside%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22estimatesnumber%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22revisedup%22%3Atrue%2C%22reviseddown%22%3Atrue%2C%22consensus%22%3A%5B%5D%7D%2C%22dy%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_l%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22peg_ratio%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_vp%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margembruta%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemliquida%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22ev_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidaebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidapatrimonioliquido%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_sr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_capitalgiro%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativocirculante%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roe%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roic%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezcorrente%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22pl_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22passivo_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22giroativos%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22receitas_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lucros_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezmediadiaria%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22vpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22valormercado%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%7D&CategoryType=1'

#
#$ Setup Selenium WebDriver
#
def setupSelenium():
    options = webdriver.ChromeOptions()

    advancedPrefs = {
        "download.default_directory": downloadFolder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    }

    options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.8191.896 Safari/537.36')
    options.add_experimental_option("prefs", advancedPrefs)
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--enable-unsafe-swiftshader')

    options.add_argument('--log-level=3')
    options.add_argument('--disable-logging')
    options.add_argument('--silent')
    options.add_argument('--remote-debugging-port=0')

    driver = webdriver.Chrome(
        options=options,
        service=Service(log_output=os.devnull),
    )

    return driver

#
#$ Scraping Functions
#
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def downloadCSVfile(url):
    # Setup download folders
    if not os.path.exists(downloadFolder):
        os.makedirs(downloadFolder)

    # Download the CSV file from the csvFileURL
    driver.get(url)
    sleep(2)

    stocksDataFrame = pd.read_csv(csvFilePath, index_col="TICKER", sep=';', skipinitialspace=True, decimal=',', thousands='.')

    return stocksDataFrame

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getSectorsData(stocksDataFrame):
    stockSectorsURL = f'https://statusinvest.com.br/category/advancedsearchresultpaginated?search=%7B%22Sector%22%3A%22%22%2C%22SubSector%22%3A%22%22%2C%22Segment%22%3A%22%22%2C%22my_range%22%3A%22-20%3B100%22%2C%22forecast%22%3A%7B%22upsidedownside%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22estimatesnumber%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22revisedup%22%3Atrue%2C%22reviseddown%22%3Atrue%2C%22consensus%22%3A%5B%5D%7D%2C%22dy%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_l%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22peg_ratio%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_vp%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margembruta%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemliquida%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22ev_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidaebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidapatrimonioliquido%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_sr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_capitalgiro%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativocirculante%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roe%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roic%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezcorrente%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22pl_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22passivo_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22giroativos%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22receitas_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lucros_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezmediadiaria%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22vpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22valormercado%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%7D&orderColumn=&isAsc=&page=0&take=611&CategoryType=1'
        
    driver.implicitly_wait(10)
    driver.get(stockSectorsURL)
    
    sectorsJSON = json.loads(driver.find_element('xpath', '/html/body/pre').text)

    sectorsDataFrame = pd.json_normalize(sectorsJSON, record_path='list', sep=',')
    sectorsDataFrame.rename(columns={'ticker': 'TICKER', 'sectorname': 'SETOR', 'subsectorname': 'SUBSETOR', 'segmentname': 'SEGMENTO'}, inplace=True)
    sectorsDataFrame.set_index('TICKER', inplace=True)

    stocksDataFrame = pd.merge(stocksDataFrame, sectorsDataFrame[['SETOR', 'SUBSETOR', 'SEGMENTO']], on='TICKER')
    
    return stocksDataFrame

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getTAGAlong(TICKER, stocksDataFrame):
    statusInvestURL = f'https://statusinvest.com.br/acoes/{TICKER}'
 
    driver.implicitly_wait(10)
    driver.get(statusInvestURL)

    tagAlong = driver.find_element('xpath', "//div[@class='top-info top-info-1 top-info-sm-2 top-info-md-3 top-info-xl-n sm d-flex justify-between']/div[@class='info']").text

    replaceList = ['help_outline', 'TAG ALONG', ' %', '\n']
    for item in replaceList:
        tagAlong = tagAlong.replace(item, '')

    if tagAlong == '--':
        tagAlong = np.nan

    stocksDataFrame.at[TICKER, 'TAG ALONG'] = int(tagAlong)

    return stocksDataFrame

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getHistoricalRent(TICKER, stocksDataFrame):
    tradingViewURL = f'https://scanner.tradingview.com/symbol?symbol=BMFBOVESPA%3A{TICKER}&fields=change%2CPerf.5D%2CPerf.W%2CPerf.1M%2CPerf.6M%2CPerf.YTD%2CPerf.Y%2CPerf.5Y%2CPerf.All&no_404=true&label-product=symbols-performance'

    driver.implicitly_wait(10)
    driver.get(tradingViewURL)

    historicalRentJSON = json.loads(driver.find_element('xpath', '/html/body/pre').text)

    histRentDataFrame = pd.json_normalize(historicalRentJSON, sep=',')
    histRentDataFrame.drop(columns={'Perf.W'}, inplace=True)
    histRentDataFrame.rename(columns={'change': 'RENT 1 DIA', 'Perf.5D': 'RENT 5 DIAS', 'Perf.1M': 'RENT 1 MES', 'Perf.6M': 'RENT 6 MESES', 'Perf.YTD': 'RENT 12 MESES', 'Perf.Y': 'RENT 1 ANO', 'Perf.5Y': 'RENT 5 ANOS', 'Perf.All': 'RENT TOTAL'}, inplace=True)

    histRentDataFrame['TICKER'] = TICKER
    histRentDataFrame.set_index('TICKER', inplace=True)

    for row in histRentDataFrame.columns:        
        stocksDataFrame.at[TICKER, row] = histRentDataFrame.at[TICKER, row]

    return stocksDataFrame

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getHistoricalDividends(TICKER, stocksDataFrame):
    dividendYeildsURL = f'https://statusinvest.com.br/acao/companytickerprovents?companyName=&ticker={TICKER}&chartProventsType=2'

    driver.implicitly_wait(10)
    driver.get(dividendYeildsURL)

    yeildsJSON = json.loads(driver.find_element('xpath', '/html/body/pre').text)
    yeildsJSON = pd.json_normalize(yeildsJSON, record_path='assetEarningsYearlyModels', sep='')

    for row in yeildsJSON.itertuples():
        stocksDataFrame.at[TICKER, f'DIVIDENDOS {row.rank}'] = row.value

    return stocksDataFrame

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getHistoricalDY(TICKER, stocksDataFrame):
    driver.implicitly_wait(10)
    driver.get(f'https://statusinvest.com.br/acoes/{TICKER}')
    
    script = f"""
    var callback = arguments[arguments.length - 1];
    fetch('/acao/indicatorhistoricallist', {{
        method: 'POST',
        headers: {{
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest'
        }},
        body: 'codes%5B%5D={TICKER.lower()}&time=5&byQuarter=false&futureData=false'
    }})
    .then(response => response.json())
    .then(data => callback(data))
    .catch(error => callback(null));
    """

    histDataJSON = driver.execute_async_script(script)
    DYDataFrame = histDataJSON['data'][TICKER.lower()]

    for indicator in DYDataFrame:
        if indicator['key'] == 'dy':
            DYDataFrame = pd.json_normalize(indicator['ranks'])

    for row in DYDataFrame.itertuples():
        stocksDataFrame.at[TICKER, f'DY {row.rank}'] = row.value

    return stocksDataFrame

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getHistoricalRevenue(TICKER, stocksDataFrame):
    historicalRevenueURL = f'https://statusinvest.com.br/acao/getrevenue?code={TICKER}&type=2&viewType=0'

    driver.implicitly_wait(10)
    driver.get(historicalRevenueURL)

    revenueJSON = json.loads(driver.find_element('xpath', '/html/body/pre').text)
    revenueJSON = pd.json_normalize(revenueJSON, sep=',')

    revenueDF = {}
    for row in revenueJSON.itertuples():
        revenueDF[f'RECEITA LIQUIDA {row.year}'] = row.receitaLiquida
        revenueDF[f'DESPESAS {row.year}'] = row.despesas
        revenueDF[f'LUCRO LIQUIDO {row.year}'] = row.lucroLiquido
        revenueDF[f'MARGEM BRUTA {row.year}'] = row.margemBruta
        revenueDF[f'MARGEM EBITDA {row.year}'] = row.margemEbitda
        revenueDF[f'MARGEM EBIT {row.year}'] = row.margemEbit
        revenueDF[f'MARGEM LIQUIDA {row.year}'] = row.margemLiquida

    revenueDF = pd.DataFrame(revenueDF, index=[TICKER])

    stocksDataFrame = stocksDataFrame.combine_first(revenueDF)

    return stocksDataFrame

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def calcFundamentalistIndicators(TICKER, stocksDataFrame):
    # EBIT
    try:
        stocksDataFrame.at[TICKER, 'EBIT'] = stocksDataFrame.loc[TICKER, f'MARGEM EBIT {current_year - 1}'] * stocksDataFrame.loc[TICKER, f'RECEITA LIQUIDA {current_year - 1}'] / 100
    except (KeyError, ZeroDivisionError, TypeError):
        stocksDataFrame.at[TICKER, 'EBIT'] = np.nan

    # Average Dividend Yields over 5 years
    try:
        DY5Years = sum(stocksDataFrame.loc[TICKER, f'DY {year}'] for year in range(current_year - 5, current_year) if f'DY {year}' in stocksDataFrame.columns)
        stocksDataFrame.at[TICKER, 'DY MEDIO 5 ANOS'] = DY5Years / 5 if DY5Years else np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stocksDataFrame.at[TICKER, 'DY MEDIO 5 ANOS'] = np.nan

    # Average Rentability over 5 years
    try:
        stocksDataFrame.at[TICKER, 'RENT MEDIA 5 ANOS'] = stocksDataFrame.loc[TICKER, 'RENT 5 ANOS'] / 5
    except (KeyError, ZeroDivisionError, TypeError):
        stocksDataFrame.at[TICKER, 'RENT MEDIA 5 ANOS'] = np.nan

    # Average Net Income over 5 years
    try:
        NetIncome5Years = sum(stocksDataFrame.loc[TICKER, f'LUCRO LIQUIDO {year}'] for year in range(current_year - 5, current_year) if f'LUCRO LIQUIDO {year}' in stocksDataFrame.columns)
        stocksDataFrame.at[TICKER, 'LUCRO LIQUIDO MEDIO 5 ANOS'] = NetIncome5Years / 5 if NetIncome5Years else np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stocksDataFrame.at[TICKER, 'LUCRO LIQUIDO MEDIO 5 ANOS'] = np.nan

    # CAGR Dividends over 5 years
    try:
        div_start = stocksDataFrame.loc[TICKER, f'DIVIDENDOS {current_year - 6}']
        div_end = stocksDataFrame.loc[TICKER, f'DIVIDENDOS {current_year - 1}']
        stocksDataFrame.at[TICKER, 'CAGR DIVIDENDOS 5 ANOS'] = ((div_end / div_start) ** (1/5) - 1) * 100 if div_start and div_start != 0 else np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stocksDataFrame.at[TICKER, 'CAGR DIVIDENDOS 5 ANOS'] = np.nan

    # Sustainable Growth Rate (SGR)
    try:
        roe = stocksDataFrame.loc[TICKER, 'ROE']
        div_yr = stocksDataFrame.loc[TICKER, f'DIVIDENDOS {current_year - 1}']
        net_yr = stocksDataFrame.loc[TICKER, f'LUCRO LIQUIDO {current_year - 1}']
        stocksDataFrame.at[TICKER, 'SGR'] = roe * (1 - div_yr / net_yr) if net_yr and net_yr != 0 else np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stocksDataFrame.at[TICKER, 'SGR'] = np.nan

    # Graham's Top Price
    try:
        lpa = stocksDataFrame.loc[TICKER, 'LPA']
        vpa = stocksDataFrame.loc[TICKER, 'VPA']
        stocksDataFrame.at[TICKER, 'PRECO DE GRAHAM'] = math.sqrt(22.5 * lpa * vpa) if lpa > 0 and vpa > 0 else np.nan
    except (KeyError, ZeroDivisionError, TypeError, ValueError):
        stocksDataFrame.at[TICKER, 'PRECO DE GRAHAM'] = np.nan

    # Bazin's Top Price
    try:
        dy_avg = stocksDataFrame.loc[TICKER, 'DY MEDIO 5 ANOS']
        preco = stocksDataFrame.loc[TICKER, 'PRECO']
        stocksDataFrame.at[TICKER, 'PRECO DE BAZIN'] = ((dy_avg / 100) * preco) / 0.06 if dy_avg and dy_avg != 0 else np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stocksDataFrame.at[TICKER, 'PRECO DE BAZIN'] = np.nan

    # Altman Z-Score
        
    return stocksDataFrame

def normalize(df, order):
    columns = list(df.columns)

    orderedColumns = [col for col in order if col in columns]

    remainingColumns = [col for col in columns if col not in orderedColumns]
    remainingColumns.sort()

    newOrder = orderedColumns + remainingColumns

    return df[newOrder]

#
#$ Main Script Execution
#
if __name__ == "__main__":
    driver = setupSelenium()

    # Remove old files in the download directory
    if os.path.exists(downloadFolder):
        for root, dirs, files in os.walk(downloadFolder, topdown=False):
            for name in files:
                os.remove(os.path.join(downloadFolder, name))

    # Setup the DataFrame
    stocksDataFrame = downloadCSVfile(csvFileURL)
    stocksDataFrame = getSectorsData(stocksDataFrame)
    stocksDataFrame['TIME'] = dateScrape

    driver.quit()

    #
    #$ Scrape items for each stock
    #
    stocksList = stocksDataFrame.index.tolist()

    for TICKER in stocksList:
        funcList = [getTAGAlong, getHistoricalRent, getHistoricalDividends, getHistoricalDY, getHistoricalRevenue, calcFundamentalistIndicators]
        for function in funcList:
            try:
                stocksDataFrame = function(TICKER, stocksDataFrame)
            except Exception as e:
                print(f'{TICKER} failed {function}')

    #
    #$ Normalize and fix stuff
    #
    stocksDataFrame = stocksDataFrame.round(2)

    normalizedColumns = ['TIME', 'TICKER', 'SETOR', 'SUBSETOR', 'SEGMENTO', 'ALTMAN Z-SCORE', 'SGR', 'LIQUIDEZ MEDIA DIARIA', 'PRECO', 'PRECO DE BAZIN', 'PRECO DE GRAHAM', 'TAG ALONG', 'RENT 12 MESES', 'RENT MEDIA 5 ANOS', 'DY', 'DY MEDIO 5 ANOS', 'P/L', 'P/VP', 'P/ATIVOS', 'MARGEM BRUTA', 'MARGEM EBIT', 'MARG. LIQUIDA', 'EBIT', 'P/EBIT', 'EV/EBIT', 'DIVIDA LIQUIDA / EBIT', 'DIV. LIQ. / PATRI.', 'PSR', 'P/CAP. GIRO', 'P. AT CIR. LIQ.', 'LIQ. CORRENTE', 'LUCRO LIQUIDO MEDIO 5 ANOS', 'ROE', 'ROA', 'ROIC', 'PATRIMONIO / ATIVOS', 'PASSIVOS / ATIVOS', 'GIRO ATIVOS', 'CAGR DIVIDENDOS 5 ANOS', 'CAGR RECEITAS 5 ANOS', 'CAGR LUCROS 5 ANOS', 'VPA', 'LPA', 'PEG Ratio', 'VALOR DE MERCADO']

    stocksDataFrame.index.name = 'TICKER'
    stocksDataFrame = stocksDataFrame.reset_index()
    stocksDataFrame = normalize(stocksDataFrame, normalizedColumns)

    #
    #$ Exports
    #
    if saveAsJSONL:
        stocksDataFrame.to_json(f'b3_stocks.json', orient='records', indent=4)

    if saveToMYSQL:
        stocksDataFrame.to_sql('b3_stocks', con=engine, if_exists='append', index=False)
        
print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")