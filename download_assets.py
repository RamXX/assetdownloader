import pandas as pd 
from sqlalchemy import create_engine
import yfinance as yf
from collections import defaultdict
from datetime import date, timedelta
import pandas_market_calendars as mcal

import creds as c

engine = create_engine(f"postgresql+psycopg2://{c.DBUSER}:{c.DBPW}@{c.DBHOST}/{c.DBNAME}")

start_date = '2021-01-01'

#### Russell 1000
R1000_filename="Russell_1000_list.gzip"

try:
    ticker_df = pd.read_parquet(R1000_filename)
except:
    ticker_df = pd.read_html("https://en.wikipedia.org/wiki/Russell_1000_Index")[2]
    ticker_df.to_parquet(R1000_filename, compression="gzip")

#### NASDAQ 100
N100_filename = "NASDAQ_100_list.gzip"
try:
    n100_ticker_df = pd.read_parquet(N100_filename)
except:
    n100_ticker_df = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
    n100_ticker_df.to_parquet(N100_filename, compression="gzip")

ticker_df = pd.concat([ticker_df['Ticker'], n100_ticker_df['Ticker']], ignore_index=True)
tickers = list(set(ticker_df.to_list()))

#### Explicit inclusion or exclusion
exclusion=['BF.A', 'HEI.A', 'LEN.B', 'BRK.B', 'BF.B']
inclusion=['SPY']

for i in exclusion:
    try:
        tickers.remove(i)
    except:
        pass

for i in inclusion:
    if (i not in tickers):
        tickers.append(i)

all_dates = []

nyse = mcal.get_calendar('NYSE')
start_date_nyse = date.today() - timedelta(days = 5)
end_date_nyse=date.today()
nyse_schedule = nyse.schedule(start_date=start_date_nyse, end_date=end_date_nyse)

for i in tickers:
    try:
        m = pd.read_sql(f"SELECT MAX(\"Date\") FROM \"{i}\"", engine).values[0][0]
        md = str(m)
        ts = pd.to_datetime(md) 
        d = ts.strftime('%Y-%m-%d')
        if (nyse_schedule['market_close'][-1:].index > m):
            all_dates.append([d, i])
    except:
        all_dates.append([start_date, i])

all_downloads = defaultdict(list)
for date, id in all_dates:
    all_downloads[date].append(id)

all_downloads = [[date, " ".join(ids)] for date, ids in all_downloads.items()]

for sdate, ltickers in all_downloads:
    print(f"Downloading market data for {ltickers} starting from {sdate}\n")
    data = yf.download(tickers=ltickers, start=sdate)
    ftickers = ltickers.strip().split(" ")
    if (len(ftickers) > 1):
        for t in ftickers:           
                o = data['Open'][t]
                of = o.to_frame(name='Open')

                hi = data['High'][t]
                hif = hi.to_frame(name='High')

                lo = data['Low'][t]
                lof = lo.to_frame(name='Low')

                ac = data['Adj Close'][t]
                acf = ac.to_frame(name='Close')

                vo = data['Volume'][t]
                vof = vo.to_frame(name='Volume')

                df = pd.concat([of, hif, lof, acf, vof], axis=1)
                df.to_sql(t, engine, if_exists='append')
    else:
        o = data['Open']
        of = o.to_frame(name='Open')

        hi = data['High']
        hif = hi.to_frame(name='High')

        lo = data['Low']
        lof = lo.to_frame(name='Low')

        ac = data['Adj Close']
        acf = ac.to_frame(name='Close')

        vo = data['Volume']
        vof = vo.to_frame(name='Volume')

        df = pd.concat([of, hif, lof, acf, vof], axis=1)
        df.to_sql(ltickers, engine, if_exists='append')