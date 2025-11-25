# Brazilian Stocks Market Scraper

A high-performance Python scraper to collect, process, and store Brazilian stock market (B3) data from StatusInvest and TradingView. Built for research to the AI models for the [Mansa](https://github.com/mansa-team) project.

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/heitorrosa/scraper-b3
   cd scraper-b3
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create environment configuration:
   ```env
   # Create .env file with your MySQL credentials
   MYSQL_USER=your_username
   MYSQL_PASSWORD=your_password
   MYSQL_HOST=localhost
   MYSQL_DATABASE=scraper_b3
   ```

## Usage

### Basic Usage
```bash
python src/scraper.py
```

### Configuration Options
Edit the script configuration section to customize:

```env
# Script Configuration
saveToMYSQL = True      # Export to MySQL database
saveAsJSONL = True      # Export to JSON file
MAX_WORKERS = 6         # Number of parallel threads (adjust based on your system)
```

## Data Sources
- **StatusInvest**: Financial metrics, sector data, TAG along, dividends, revenue data
- **TradingView**: Historical performance and returns data

## Output Structure

### JSON Export (`b3_stocks.json`)
```json
{
  "TICKER": "PETR4",
  "SETOR": "Petróleo, Gás e Biocombustíveis",
  "SUBSETOR": "Petróleo",
  "SEGMENTO": "Exploração, Refino e Distribuição",
  "PRECO": 34.21,
  "DY": 8.73,
  "TAG ALONG": 100,
  "RENT_12_MESES": 28.5,
  "RENT_MEDIA_5_ANOS": 15.2,
  "DY_MEDIO_5_ANOS": 9.1,
  "PRECO_DE_GRAHAM": 45.67,
  "PRECO_DE_BAZIN": 52.34,
  "EBIT": 123456789.0,
  "SGR": 12.5,
  "DIVIDENDOS_2023": 2.34,
  "DIVIDENDOS_2022": 2.10,
  "DY_2023": 7.89,
  "DY_2022": 6.45,
  "RECEITA_LIQUIDA_2023": 987654321.0,
  "LUCRO_LIQUIDO_2023": 123456789.0,
  "TIME": "2024-12-09 14:30:00"
}
```

## License
GPL 3.0 MODIFIED Mansa Team's License. See LICENSE for details.
