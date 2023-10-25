# Assets Downloader

## Purpose
The primary goal of this program is to maintain a local Postgres database, augmented with the TimescaleDB plugin, to store OHLCV (Open, High, Low, Close, Volume) data on a daily timeframe for all stocks listed in the Russell 1000, Dow Jones Industrial, and NASDAQ 100 indexes. It also allows for a configurable set of additional tickers to be included.

This program was developed to reduce the dependency on Yahoo! Finance for historical data retrieval, thereby improving response times and reducing the number of API calls.

## Requirements

- It is advisable to run this program in a virtual environment as it requires Pandas version 1.5.4.
  
- Ensure a Postgres database with the TimescaleDB extension is operational and accessible.
  
- Store all database access variables in the `.env` configuration file. Refer to `env-example` for the correct format, or simply copy it to `.env`.
  
- Ensure the running user has proper access controls.
  
- The database access and maintenance of the necessary tables are idempotent; the program will ensure the database and all tables are created and maintained accordingly.

## Defaults
By default, the program only downloads OHLCV data, excluding other financial information which might be beneficial for different models. It initiates data retrieval from January 1st, 2015, which can be altered via the `BEGINNING_DATE` variable.

You can set up a cron job to execute this program daily after market close, to maintain an up-to-date database, provided all dependencies are installed.

Manually update these three files as needed:

1. `exclusion_list.txt` - A space-separated list of tickers that you wish to exclude, even if they are part of the major indexes. All previous data related to these tickers will be purged from the database.

2. `inclusion_list.txt` - A similar file for tickers not traded on the major exchanges but you wish to include.

3. `mypicks.csv` - Place a `mypicks.csv` file in the running directory with a single column labeled 'Tickers' listing your tickers, and optionally, a last line labeled 'Summary'. This is compatible with StockRover CSV output.

Now, indexes can be specified in the inclusion/exclusion lists using the Yahoo! Finance format (e.g., the Russell 1000 would be '^RUI').

The program generates three data files: `DJI_list.gzip`, `NASDAQ_100_list.gzip`, and `Russell_1000_list.gzip`, containing the current list of assets for each index as detailed on their respective Wikipedia pages. To refresh these lists, delete the files. The program will recreate them with updated data during the next run.

## Installation

```bash
git clone https://github.com/hextropian/assetdownloader.git
cd assetdownloader
python -m venv venv
source venv/bin/activate
cp env-example .env
pip install -r requirements.txt --no-cache --upgrade
```

Edit the `.env` file to reflect your environment.
To execute the program, run:

```bash
python3 ./assets_downloader.py
```

## Caveats

Currently, there's no test coverage, and error handling is limited. The `yfinance` download function sometimes fails to retrieve ticker data for certain symbols without a clear cause. The remedy is to re-run the program, which will then download only the data missing from the last unsuccessful run.

## Disclaimer

This program is not affiliated with Yahoo! Finance and relies on the `yfinance` module for data access. Please refer to [their repository](https://github.com/ranaroussi/yfinance) for appropriate usage guidelines.

This program and its data come with no warranties regarding their accuracy. Nothing herein constitutes financial advice. Use at your own risk.

---

I've made several changes to improve readability, clarity, and grammatical correctness. Your original content was well-structured, and these modifications aim to enhance its presentation and understanding for your audience.