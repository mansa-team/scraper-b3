# B3 Scraper

A Python pipeline to collect, process, and store Brazilian stock market (B3) data from StatusInvest and TradingView. Built for research and educational purposes.

## Features
- Scrapes 50+ financial metrics from StatusInvest and TradingView
- Gets sector, sub-sector, and segment info
- Captures tag along rights and historical returns
- Cleans and normalizes data
- Stores results in JSONL and MySQL
- Adds scrape timestamp for each record
- Handles dynamic columns for annual liquidity and dividend yields per year

## Requirements
- Python 3.10+
- MySQL Server 8.0+
- Python packages: `selenium`, `mysql-connector-python`, `webdriver-manager`

## Quick Start
1. Clone the repo and install dependencies:
   ```bash
   git clone https://github.com/heitorrosa/scraperB3
   cd scraperB3
   pip install -r requirements.txt
   ```
2. Set up MySQL:
   ```sql
   CREATE DATABASE b3;
   ```
   Edit `mysql_config` in the script with your credentials.
3. Run the script:
   ```bash
   python scraper.py
   ```

## Output
- JSONL: `cache/stocks_data.jsonl`
- MySQL: Table `stocks` with 40+ metrics and dynamic columns for each year (e.g., `dividendos_2023`, `dy_2023`, `annualLiquidity_2023`)

## Example Record
```json
{
  "ticker": "PETR4",
  "preco": 34.21,
  "dy": 8.73,
  "setor": "Petróleo, Gás e Biocombustíveis",
  "rent_1_ano": 28.5,
  "tag_along": 100,
  "annualLiquidity_2023": 123456789.0,
  "dividendos_2023": 2.34,
  "dy_2023": 7.89,
  "scrape_time": "2023-11-15 10:30:00"
}
```

## License
GPL 3.0 License. See LICENSE for details.