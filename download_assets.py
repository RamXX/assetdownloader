import pandas as pd 
import numpy as np
from sqlalchemy import create_engine
import yfinance as yf
from collections import defaultdict
from datetime import date, timedelta, datetime
from handpicks import exclusion, inclusion
import pandas_market_calendars as mcal
import warnings
import creds as c

warnings.simplefilter(action='ignore')

engine = create_engine(f"postgresql+psycopg2://{c.DBUSER}:{c.DBPW}@{c.DBHOST}/{c.DBNAME}")

engine = create_engine(f"postgresql+psycopg2://{c.DBUSER}:{c.DBPW}@{c.DBHOST}/{c.DBNAME}")
nyse = mcal.get_calendar('NYSE')
start_date_nyse = date.today() - timedelta(days = 7)
end_date_nyse=date.today()
nyse_schedule = nyse.schedule(start_date=start_date_nyse, end_date=end_date_nyse)

start_date_dl = '2015-01-01'
if nyse.is_open_now(nyse_schedule):
    end_date_dl = pd.to_datetime(nyse_schedule[-2:-1].index[0]).strftime('%Y-%m-%d')
else:
    end_date_dl = pd.to_datetime(nyse_schedule[-1:].index[0]).strftime('%Y-%m-%d')
    
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
tickers = [s.replace('.', '-') for s in tickers]

for i in exclusion:
    try:
        tickers.remove(i)
    except:
        pass

for i in inclusion:
    if (i not in tickers):
        tickers.append(i)

all_dates = []

for i in tickers:
    try:
        p = i.replace('^', 'IX-')
        m = pd.read_sql(f'SELECT * FROM "{p}" WHERE "Date" = (SELECT MAX("Date") FROM "{p}")', engine)
        
        if (m["Close"].values[0] == None):
            engine.execute(f'DELETE FROM "{p}" WHERE ("Open" IS NULL AND "High" IS NULL AND "Low" IS NULL AND "Close" IS NULL AND "Volume" IS NULL)')
            m = pd.read_sql(f'SELECT * FROM "{p}" WHERE "Date" = (SELECT MAX("Date") FROM "{p}")', engine)
            print(f"Deleting empty row for ticker {i}")
        
        if m.shape[0] > 1:
            for j in range(m.shape[0] - 1):
                engine.execute(f'DELETE FROM "{p}" WHERE ("Open" = {m["Open"][j]} AND "High" = {m["High"][j]} AND "Low" = {m["Low"][j]} AND "Close" = {m["Close"][j]} AND "Volume" = {m["Volume"][j]})')
                m = pd.read_sql(f'SELECT * FROM "{p}" WHERE "Date" = (SELECT MAX("Date") FROM "{p}")', engine)
                print(f"Deleting duplicate last row for ticker {i}")
        
        md = str(m["Date"].values[0])
        ts = pd.to_datetime(md) + timedelta(days=1)
        d = ts.strftime('%Y-%m-%d')
        if (d != end_date_dl):
            all_dates.append([d, i])            
    except:
        all_dates.append([start_date_dl, i])

all_downloads = defaultdict(list)
for dt, id in all_dates:
    all_downloads[dt].append(id)

all_downloads = [[dt, " ".join(ids)] for dt, ids in all_downloads.items()]

for sdate, ltickers in all_downloads:
    print(f"Downloading market data for {ltickers} starting from {sdate} to {end_date_dl}\n")
    data = yf.download(tickers=ltickers, start=sdate, end=end_date_dl)
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
                df.to_sql(t.replace('^', 'IX-'), engine, if_exists='append')
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
        df.to_sql(ltickers.replace('^', 'IX-'), engine, if_exists='append')

all_close = pd.DataFrame()
for i in tickers:
    t = i.replace('^', 'IX-')
    try:
        m = pd.read_sql(f'SELECT "Date", "Close" FROM "{t}" WHERE "Date" >= \'{start_date_dl}\' ORDER BY "Date"', engine)
    except:
        print(f'Error reading DB for symbol {t}')
        exit
    m['Date'] = pd.to_datetime(m['Date'], format='%Y-%m-%d')
    m.set_index('Date', inplace=True)
    try:
        all_close = pd.concat([all_close, m], axis=1) if len(all_close) > 0 else m
    except:
        pass
    all_close.set_axis([*all_close.columns[:-1], t], axis=1, inplace=True)
    
all_close.to_parquet("all_close.pk.gzip", compression='gzip')