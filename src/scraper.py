#
#$ Import Libraries
#
from imports import *

#
#$ Script Configuration and Basic Setup
#
saveToMYSQL = True
saveAsJSONL = False
MAX_WORKERS = 6

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

    options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.8191.896 Safari/537.36')
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-images')
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--disable-default-apps')
    options.add_argument('--disable-sync')
    options.add_argument('--disable-translate')
    options.add_argument('--disable-breakpad')
    options.add_argument('--disable-client-side-phishing-detection')
    options.add_argument('--disable-component-extensions-with-background-pages')
    options.add_argument('--disable-default-apps')
    options.add_argument('--disable-hang-monitor')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-prompt-on-repost')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-client-side-phishing-detection')
    options.add_argument('--disable-default-apps')
    options.add_argument('--disable-device-discovery-notifications')
    options.add_argument('--disable-hang-monitor')
    options.add_argument('--disable-ipc-flooding-protection')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-prompt-on-repost')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--enable-features=NetworkService,NetworkServiceInProcess')
    options.add_argument('--force-color-profile=srgb')
    options.add_argument('--metrics-recording-only')
    options.add_argument('--mute-audio')
    options.add_argument('--no-default-browser-check')
    options.add_argument('--no-first-run')
    options.add_argument('--password-store=basic')
    options.add_argument('--use-mock-volume-mount-config')
    options.add_argument('--memory-pressure-off')
    options.add_argument('--renderer-process-limit=1')

    prefs = {
        "download.default_directory": downloadFolder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        'profile.default_content_settings.popups': 0,
        'profile.managed_default_content_settings.images': 2,
        'profile.default_content_setting_values.notifications': 2,
    }
    options.add_experimental_option('prefs', prefs)

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
    if not os.path.exists(downloadFolder):
        os.makedirs(downloadFolder)

    driver.get(url)
    sleep(2)

    stocksData = pd.read_csv(csvFilePath, index_col="TICKER", sep=';', skipinitialspace=True, decimal=',', thousands='.')

    return stocksData

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getSectorsData(stocksData):
    stockSectorsURL = f'https://statusinvest.com.br/category/advancedsearchresultpaginated?search=%7B%22Sector%22%3A%22%22%2C%22SubSector%22%3A%22%22%2C%22Segment%22%3A%22%22%2C%22my_range%22%3A%22-20%3B100%22%2C%22forecast%22%3A%7B%22upsidedownside%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22estimatesnumber%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22revisedup%22%3Atrue%2C%22reviseddown%22%3Atrue%2C%22consensus%22%3A%5B%5D%7D%2C%22dy%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_l%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22peg_ratio%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_vp%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margembruta%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemliquida%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22ev_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidaebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidapatrimonioliquido%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_sr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_capitalgiro%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativocirculante%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roe%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roic%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezcorrente%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22pl_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22passivo_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22giroativos%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22receitas_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lucros_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezmediadiaria%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22vpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22valormercado%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%7D&orderColumn=&isAsc=&page=0&take=611&CategoryType=1'
        
    driver.implicitly_wait(10)
    driver.get(stockSectorsURL)
    
    sectorsJSON = json.loads(driver.find_element('xpath', '/html/body/pre').text)

    sectorsData = pd.json_normalize(sectorsJSON, record_path='list', sep=',')
    sectorsData.rename(columns={'ticker': 'TICKER', 'companyname': 'NOME', 'sectorname': 'SETOR', 'subsectorname': 'SUBSETOR', 'segmentname': 'SEGMENTO'}, inplace=True)
    sectorsData.set_index('TICKER', inplace=True)

    stocksData = pd.merge(stocksData, sectorsData[['NOME', 'SETOR', 'SUBSETOR', 'SEGMENTO']], on='TICKER')
    
    return stocksData

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getTAGAlong(TICKER, driver):
    statusInvestURL = f'https://statusinvest.com.br/acoes/{TICKER}'
 
    driver.implicitly_wait(10)
    driver.get(statusInvestURL)

    tagAlong = driver.find_element('xpath', "//div[@class='top-info top-info-1 top-info-sm-2 top-info-md-3 top-info-xl-n sm d-flex justify-between']/div[@class='info']").text

    replaceList = ['help_outline', 'TAG ALONG', ' %', '\n']
    for item in replaceList:
        tagAlong = tagAlong.replace(item, '')

    if tagAlong == '--':
        tagAlong = np.nan

    return int(tagAlong)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getHistoricalRent(TICKER, driver):
    tradingViewURL = f'https://scanner.tradingview.com/symbol?symbol=BMFBOVESPA%3A{TICKER}&fields=change%2CPerf.5D%2CPerf.W%2CPerf.1M%2CPerf.6M%2CPerf.YTD%2CPerf.Y%2CPerf.5Y%2CPerf.All&no_404=true&label-product=symbols-performance'

    driver.implicitly_wait(10)
    driver.get(tradingViewURL)

    historicalRentJSON = json.loads(driver.find_element('xpath', '/html/body/pre').text)

    histRentDataFrame = pd.json_normalize(historicalRentJSON, sep=',')
    histRentDataFrame.drop(columns={'Perf.W'}, inplace=True)
    histRentDataFrame.rename(columns={'change': 'RENT 1 DIA', 'Perf.5D': 'RENT 5 DIAS', 'Perf.1M': 'RENT 1 MES', 'Perf.6M': 'RENT 6 MESES', 'Perf.YTD': 'RENT 12 MESES', 'Perf.Y': 'RENT 1 ANO', 'Perf.5Y': 'RENT 5 ANOS', 'Perf.All': 'RENT TOTAL'}, inplace=True)

    histRentDataFrame['TICKER'] = TICKER
    histRentDataFrame.set_index('TICKER', inplace=True)

    result = {}
    for col in histRentDataFrame.columns:        
        result[col] = histRentDataFrame.at[TICKER, col]
    
    return result

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getHistoricalDividends(TICKER, driver):
    dividendYeildsURL = f'https://statusinvest.com.br/acao/companytickerprovents?companyName=&ticker={TICKER}&chartProventsType=2'

    driver.implicitly_wait(10)
    driver.get(dividendYeildsURL)

    yeildsJSON = json.loads(driver.find_element('xpath', '/html/body/pre').text)
    yeildsJSON = pd.json_normalize(yeildsJSON, record_path='assetEarningsYearlyModels', sep='')

    result = {}
    for row in yeildsJSON.itertuples():
        result[f'DIVIDENDOS {row.rank}'] = row.value

    return result

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getHistoricalDY(TICKER, driver):
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
    
    tickerData = histDataJSON['data'].get(TICKER.lower(), [])

    dy_ranks = []
    for indicator in tickerData:
        if indicator.get('key') == 'dy':
            dy_ranks = indicator.get('ranks', [])
            break

    result = {}
    if dy_ranks:
        dy_df = pd.json_normalize(dy_ranks)
        for row in dy_df.itertuples():
            result[f'DY {row.rank}'] = row.value

    return result

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getHistoricalRevenue(TICKER, driver):
    historicalRevenueURL = f'https://statusinvest.com.br/acao/getrevenue?code={TICKER}&type=2&viewType=0'

    driver.implicitly_wait(10)
    driver.get(historicalRevenueURL)

    revenueJSON = json.loads(driver.find_element('xpath', '/html/body/pre').text)
    revenueJSON = pd.json_normalize(revenueJSON, sep=',')

    result = {}
    for row in revenueJSON.itertuples():
        result[f'RECEITA LIQUIDA {row.year}'] = row.receitaLiquida
        result[f'DESPESAS {row.year}'] = row.despesas
        result[f'LUCRO LIQUIDO {row.year}'] = row.lucroLiquido
        result[f'MARGEM BRUTA {row.year}'] = row.margemBruta
        result[f'MARGEM EBITDA {row.year}'] = row.margemEbitda
        result[f'MARGEM EBIT {row.year}'] = row.margemEbit
        result[f'MARGEM LIQUIDA {row.year}'] = row.margemLiquida

    return result

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def getRecentNews(TICKER, driver):
    recentNewsURL = f''

    result = {}

    return result

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
def calcFundamentalistIndicators(TICKER, stockData):
    # EBIT
    try:
        ebit = stockData.get(f'MARGEM EBIT {current_year - 1}', 0) * stockData.get(f'RECEITA LIQUIDA {current_year - 1}', 0) / 100
        stockData['EBIT'] = ebit if ebit != 0 else np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stockData['EBIT'] = np.nan

    # Average Dividend Yields over 5 years
    try:
        dy_values = []
        for year in range(current_year - 5, current_year):
            dy_value = stockData.get(f'DY {year}')
            if dy_value is not None:
                dy_values.append(dy_value)
        stockData['DY MEDIO 5 ANOS'] = sum(dy_values) / len(dy_values) if dy_values else np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stockData['DY MEDIO 5 ANOS'] = np.nan

    # Average Rentability over 5 years
    try:
        rent_5_anos = stockData.get('RENT 5 ANOS')
        stockData['RENT MEDIA 5 ANOS'] = rent_5_anos / 5 if rent_5_anos else np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stockData['RENT MEDIA 5 ANOS'] = np.nan

    # Average Net Income over 5 years
    try:
        income_values = []
        for year in range(current_year - 5, current_year):
            income_value = stockData.get(f'LUCRO LIQUIDO {year}')
            if income_value is not None:
                income_values.append(income_value)
        stockData['LUCRO LIQUIDO MEDIO 5 ANOS'] = sum(income_values) / len(income_values) if income_values else np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stockData['LUCRO LIQUIDO MEDIO 5 ANOS'] = np.nan

    # CAGR Dividends over 5 years
    try:
        div_start = stockData.get(f'DIVIDENDOS {current_year - 6}')
        div_end = stockData.get(f'DIVIDENDOS {current_year - 1}')
        if div_start and div_start != 0 and div_end:
            stockData['CAGR DIVIDENDOS 5 ANOS'] = ((div_end / div_start) ** (1/5) - 1) * 100
        else:
            stockData['CAGR DIVIDENDOS 5 ANOS'] = np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stockData['CAGR DIVIDENDOS 5 ANOS'] = np.nan

    # Sustainable Growth Rate (SGR)
    try:
        roe = stockData.get('ROE')
        div_yr = stockData.get(f'DIVIDENDOS {current_year - 1}')
        net_yr = stockData.get(f'LUCRO LIQUIDO {current_year - 1}')
        if roe and net_yr and net_yr != 0 and div_yr is not None:
            stockData['SGR'] = roe * (1 - div_yr / net_yr)
        else:
            stockData['SGR'] = np.nan
    except (KeyError, ZeroDivisionError, TypeError):
        stockData['SGR'] = np.nan

    # Graham's Top Price
    try:
        lpa = stockData.get('LPA')
        vpa = stockData.get('VPA')
        if lpa and lpa > 0 and vpa and vpa > 0:
            stockData['PRECO DE GRAHAM'] = math.sqrt(22.5 * lpa * vpa)
        else:
            stockData['PRECO DE GRAHAM'] = np.nan
    except (KeyError, ZeroDivisionError, TypeError, ValueError):
        stockData['PRECO DE GRAHAM'] = np.nan

    # Bazin's Top Price
    try:
        mediaDividendos = 0
        for year in range(current_year - 5, current_year):
            mediaDividendos += stockData.get(f'DIVIDENDOS {year}')
        mediaDividendos = mediaDividendos / 5

        stockData['PRECO DE BAZIN'] = mediaDividendos / 0.06
    except (KeyError, ZeroDivisionError, TypeError):
        stockData['PRECO DE BAZIN'] = np.nan

    return stockData

#
#$ Thread worker function
#
def process_stock(ticker, stocksData):
    driver = setupSelenium()
    stockData = stocksData.loc[ticker].to_dict()

    try:
        funcList = [
            ('getTAGAlong', getTAGAlong),
            ('getHistoricalRent', getHistoricalRent), 
            ('getHistoricalDividends', getHistoricalDividends),
            ('getHistoricalDY', getHistoricalDY),
            ('getHistoricalRevenue', getHistoricalRevenue)
        ]
        
        for funcName, function in funcList:
            try:
                if funcName == 'getTAGAlong':
                    stockData['TAG ALONG'] = function(ticker, driver)
                else:
                    result = function(ticker, driver)
                    stockData.update(result)
            except Exception as e:
                print(f'{ticker} failed {funcName}: {e}')
        
        stockData = calcFundamentalistIndicators(ticker, stockData)
                
    except Exception as e:
        print(f'{ticker} general error: {e}')
    finally:
        try:
            driver.quit()
        except:
            pass
        try:
            driver.close()
        except:
            pass
    
    return ticker, stockData

#
#$ Normalize Function
#
def normalize(data, order):
    columns = list(data.columns)

    orderedColumns = [col for col in order if col in columns]

    remainingColumns = [col for col in columns if col not in orderedColumns]
    remainingColumns.sort()

    newOrder = orderedColumns + remainingColumns

    return data[newOrder]

#
#$ Main Script Execution
#
if __name__ == "__main__":
    driver = setupSelenium()

    try:
        if os.path.exists(downloadFolder):
            for root, dirs, files in os.walk(downloadFolder, topdown=False):
                for name in files:
                    os.remove(os.path.join(downloadFolder, name))

        stocksData = downloadCSVfile(csvFileURL)
        stocksData = getSectorsData(stocksData)
        stocksData['TIME'] = dateScrape

        #
        #$ Scrape items for each stock using ThreadPoolExecutor
        #
        stocksList = stocksData.index.tolist()
        results = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futureToTicker = {
                executor.submit(process_stock, ticker, stocksData): ticker 
                for ticker in stocksList
            }
            
            for future in as_completed(futureToTicker):
                ticker = futureToTicker[future]
                try:
                    processedTicker, stockData = future.result()
                    results[processedTicker] = stockData
                except Exception as e:
                    print(f'{ticker} failed processing: {e}')

        for ticker, data in results.items():
            for key, value in data.items():
                stocksData.at[ticker, key] = value

        #
        #$ Normalize and fix stuff
        #
        stocksData = stocksData.round(2)

        normalizedColumns = ['TIME', 'NOME', 'TICKER', 'SETOR', 'SUBSETOR', 'SEGMENTO', 'ALTMAN Z-SCORE', 'SGR', 'LIQUIDEZ MEDIA DIARIA', 'PRECO', 'PRECO DE BAZIN', 'PRECO DE GRAHAM', 'TAG ALONG', 'RENT 12 MESES', 'RENT MEDIA 5 ANOS', 'DY', 'DY MEDIO 5 ANOS', 'P/L', 'P/VP', 'P/ATIVOS', 'MARGEM BRUTA', 'MARGEM EBIT', 'MARG. LIQUIDA', 'EBIT', 'P/EBIT', 'EV/EBIT', 'DIVIDA LIQUIDA / EBIT', 'DIV. LIQ. / PATRI.', 'PSR', 'P/CAP. GIRO', 'P. AT CIR. LIQ.', 'LIQ. CORRENTE', 'LUCRO LIQUIDO MEDIO 5 ANOS', 'ROE', 'ROA', 'ROIC', 'PATRIMONIO / ATIVOS', 'PASSIVOS / ATIVOS', 'GIRO ATIVOS', 'CAGR DIVIDENDOS 5 ANOS', 'CAGR RECEITAS 5 ANOS', 'CAGR LUCROS 5 ANOS', 'VPA', 'LPA', 'PEG Ratio', 'VALOR DE MERCADO', 'PRECO ABERTURA', 'PRECO ANTERIOR', 'PRECO MAXIMO', 'PRECO MINIMO']

        stocksData.index.name = 'TICKER'
        stocksData = stocksData.reset_index()
        stocksData = normalize(stocksData, normalizedColumns)

        #
        #$ Exports
        #
        if saveAsJSONL:
            stocksData.to_json(f'b3_stocks.json', orient='records', indent=4)

        if saveToMYSQL:
            existingColumns = pd.read_sql("SELECT * FROM b3_stocks LIMIT 1", con=engine)
            newColumns = set(stocksData.columns) - set(existingColumns.columns)

            for col in newColumns:
                colType = stocksData[col].dtype
                with engine.connect() as conn:
                    colType = "TEXT" if stocksData[col].dtype == 'object' else "DOUBLE"
                    query = text(f"ALTER TABLE b3_stocks ADD COLUMN `{col}` {colType} NULL")
                    conn.execute(query)
                    conn.commit()

            stocksData.to_sql('b3_stocks', con=engine, if_exists='append', index=False)

    finally:
        try:
            driver.quit()
        except:
            pass
        try:
            driver.close()
        except:
            pass

        try:
            subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'], 
                         stdout=subprocess.DEVNULL, 
                         stderr=subprocess.DEVNULL)
        except:
            pass
        
        try:
            subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'], 
                         stdout=subprocess.DEVNULL, 
                         stderr=subprocess.DEVNULL)
        except:
            pass

        gc.collect()
print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")