# Assets Downloader

## Purpose
This program was written to maintain a local Postgres database with OHLCV data for all stocks in the Russell 1000 and NASDAQ 100 indexes.

The motivation to write this is not having to constantly relying on Yahoo! Finance to obtain historical data, increasing response time and decreasing API calls.

## Requirements

A Postgres database must be running in the host specified in the `creds.py` file. Proper access controls for the running user is expected to be fully functional.

## Defaults
The program only downloads OHLCV data, not other financial information that could be useful in different models. It defaults to data from January 1st, 2021 and newer, which can be changed in the `start_date` variable.

The program can be placed on a cron job to be executed after market close daily in order to keep an up-to-date database, provided all the dependencies are installed.

There are two lists that can be updated manually, `exclusion` and `inclusion`, where specific tickers that you may want to exclude or include should be placed. The lists are kept in a separate file, `handpicks.py`, for easy updating.

Additionally, a `creds.py` file needs to be present in this directory with the connection details for the database. See `creds-example.py` as an example.

Two data files are included here, `NASDAQ_100_list.gzip` and `Russell_1000_list.gzip` which contains the current list of assets in each one of those indexes as detailed in their respective Wikipedia page. In order to refresh the list, delete the files. The program will re-generate them in the next run with updated data.

## Installation

```
git clone https://github.com/hextropian/assetdownloader.git
cd assetdownloader
pip install -r requirements.txt
```

To run it, simply run:

```
python3 ./download_assets.py
```

## Caveats

There is currently no test coverage, and very little error handling.

## Disclaimer

We are not affiliated in any way with Yahoo! Finance, and this program relies on the `yfinance` module to access ticker data. Please refer to [their repository](https://github.com/ranaroussi/yfinance) for appropriate usage.

Nothing here is financial advice, and I make no warranty of the accuracy of the data or the program. Use at your own risk.