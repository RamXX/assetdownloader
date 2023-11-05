import pandas as pd 
import psycopg2
from psycopg2 import sql
import yfinance as yf
from datetime import datetime
import pandas_market_calendars as mcal
import warnings
import os
from dotenv import load_dotenv
import io
import pytz
import csv

warnings.simplefilter(action='ignore')

# Constants
BEGINNING_DATE = '2015-01-01' # Earliest date used for downloads

# Global variables
today = pytz.UTC.localize(pd.Timestamp.now())
today_str = today.strftime('%Y-%m-%d')
nyse = mcal.get_calendar('NYSE') # NYSE calendar

def last_trading_day(nyse):
    """
    Returns the last completed trading date for NYSE
    """
    today_utc_naive = datetime.utcnow()
    
    first_day_of_year = datetime(today_utc_naive.year, 1, 1)
    valid_days = nyse.valid_days(start_date=first_day_of_year, end_date=today_utc_naive)
    
    schedule_today = nyse.schedule(start_date=valid_days[-1], end_date=valid_days[-1])
    
    market_close_today_utc = schedule_today.iloc[0]['market_close']
    if pd.Timestamp(datetime.utcnow(), tz='UTC') > market_close_today_utc:
        return market_close_today_utc
    else:
        schedule_prev_day = nyse.schedule(start_date=valid_days[-2], end_date=valid_days[-2])
        market_close_prev_day_utc = schedule_prev_day.iloc[0]['market_close']
        return market_close_prev_day_utc

LTD = last_trading_day(nyse)

def next_trading_day(nyse, date_str):
    """ 
    Returns the next valid trading date for a given date.
    """
    future_dates = pd.date_range(start=date_str, periods=10, freq='B')
    trading_days = nyse.valid_days(start_date=future_dates[0], end_date=future_dates[-1])    
    input_date_tz_aware = pd.Timestamp(date_str).tz_localize('UTC')    
    next_day = trading_days[trading_days > input_date_tz_aware].min()    
    return next_day

def market_status(nyse):
    """ 
    Returns 'open' or 'closed' depending on the NYSE market status right now.
    """
    now = datetime.utcnow() 
    schedule = nyse.schedule(start_date=now.date(), end_date=now.date())
    
    if schedule.empty:
        ms = "closed"
    else:
        is_open = mcal.date_range(schedule, frequency='1T')
        market_open = is_open.min().to_pydatetime()
        market_close = is_open.max().to_pydatetime()
        
        if market_open <= now <= market_close:
            ms = "open"
        else:
            ms = "closed"
    
    return ms


def init_db():
    """
    Initializes the Database and returns a connection object ready to work with.
    """
    load_dotenv()

    dbhost = os.environ["DBHOST"]
    dbuser = os.environ["DBUSER"]
    dbpw = os.environ["DBPW"]
    dbport = os.environ["DBPORT"]
    dbname = os.environ["DBNAME"]

    conn = psycopg2.connect(database='postgres', user=dbuser, password=dbpw, host=dbhost, port=dbport)
    conn.autocommit = True 

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"), (dbname,))
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(dbname)))
                print(f"Database {dbname} created successfully.")
    except Exception as e:
        print(f"Error: {str(e)}")
        conn.close()
        quit(1)

    conn.close()

    conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpw, host=dbhost, port=dbport)
    conn.autocommit = True  
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            print("TimescaleDB extension loaded successfully.")
    except Exception as e:
        print(f"Error: {str(e)}")
        conn.close()
        quit(1)
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    timestamp DATE NOT NULL,
                    ticker TEXT NOT NULL,
                    open DOUBLE PRECISION NOT NULL,
                    high DOUBLE PRECISION NOT NULL,
                    low DOUBLE PRECISION NOT NULL,
                    close DOUBLE PRECISION NOT NULL,
                    volume BIGINT NOT NULL,
                    UNIQUE (timestamp, ticker)
                );
                CREATE TABLE IF NOT EXISTS mypicks (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) UNIQUE NOT NULL,
                    date_added TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    date_removed TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS mypicks_history (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    action VARCHAR(10) NOT NULL
                );
            """)
            cursor.execute("""
                SELECT * 
                FROM timescaledb_information.hypertables 
                WHERE hypertable_name = 'stock_data';
            """)
            exists = cursor.fetchone()
            if not exists:
                cursor.execute("""
                    SELECT create_hypertable('stock_data', 'timestamp');
                """)
                print("Hypertable stock_data created successfully.")
            else:
                print("Hypertable stock_data already exists.")
    except Exception as e:
        print(f"Error: {str(e)}")
        conn.close()
        quit(1)
        
    return conn


def close_db(conn):
    """
    Closes the DB connection. Useful wrapper if we every change the DB.
    """
    conn.close()


def process_csv_and_update_db(conn):
    """ 
    Process the 'mypicks.csv' file and keep a record of changes in the DB.
    """

    def get_file_creation_time(filepath):
        """
        Returns the timestamp with the creation date of the file being passed.
        """
        timestamp = os.path.getctime(filepath)
        return datetime.fromtimestamp(timestamp)

    cur = conn.cursor()
    file_creation_time = get_file_creation_time('mypicks.csv')

    with open('mypicks.csv', newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.reader(csvfile)
        headers = next(csv_reader)
        tickers_from_csv = {row[0] for row in csv_reader if row[0] != 'Summary'}

    cur.execute("SELECT ticker FROM mypicks WHERE date_removed IS NULL;")
    tickers_from_db = {row[0] for row in cur.fetchall()}

    new_tickers = tickers_from_csv - tickers_from_db
    removed_tickers = tickers_from_db - tickers_from_csv

    for ticker in new_tickers:
        cur.execute(
            """
            INSERT INTO mypicks (ticker, date_added) VALUES (%s, %s)
            ON CONFLICT (ticker) DO UPDATE SET date_added = EXCLUDED.date_added;
            """,
            (ticker, file_creation_time)
        )
        cur.execute(
            "INSERT INTO mypicks_history (ticker, action, date) VALUES (%s, 'Added', %s);",
            (ticker, file_creation_time)
        )
    
    for ticker in removed_tickers:
        cur.execute(
            "UPDATE mypicks SET date_removed=%s WHERE ticker=%s;",
            (file_creation_time, ticker)
        )
        cur.execute(
            "INSERT INTO mypicks_history (ticker, action, date) VALUES (%s, 'Removed', %s);",
            (ticker, file_creation_time)
        )
    
    cur.close()



def get_last_entry_date(ticker, conn):
    """
    Get the last update timestamp for a specic ticker.
    """
    cur = conn.cursor()
    query = """
        SELECT MAX(timestamp)
        FROM stock_data
        WHERE ticker = %s;
    """
    cur.execute(query, (ticker,))
    result = cur.fetchone()
    cur.close()    
    res = result[0]
    if res != None:
        return res.strftime('%Y-%m-%d')
    return None


def get_tickers_list(conn, picks='./mypicks.csv', inclusion='./inclusion_list.txt', exclusion='./exclusion_list.txt'):
    """ 
    Compiles the list of tickers we'll use. It assumes specific filenames for picks, and explicit inclusion and exclusion lists.
    """
    def read_file(file_path):
        """
        Reads a file into memory. If errors occur, display a message and continue.
        Symbols that contain a dot (like BRK.A) have the dot converted to a dash so yfinance works fine.
        It also removes entries such as 'ticker' or 'summary', likely to be present in CSV files.
        """
        try:
            with open(file_path, 'r') as file:
                tickers = list(set(file.read().replace('\n', ' ').split()))
                items = [s.replace('.', '-') for s in tickers]
                to_remove = {'summary', 'ticker'}
                items = [item for item in items if item.lower() not in to_remove]
                return items
        except FileNotFoundError:
            print(f"The file {file_path} does not exist. Ignoring input.")
            return []
        except Exception as e:
            print(f"An error occurred while reading {file_path}: {e}")
            return []


    def get_exchanges_tickers():
        """
        Get all the tickers we'll use. Downloads and stores major indexes stocks, so we don't have
        to call them every time (we're being nice to Wikipedia).
        """
        #### Russell 1000
        R1000_filename="Russell_1000_list.gzip"

        try:
            r1000_ticker_df = pd.read_parquet(R1000_filename)
        except:
            r1000_ticker_df = pd.read_html("https://en.wikipedia.org/wiki/Russell_1000_Index")[2]
            r1000_ticker_df.to_parquet(R1000_filename, compression="gzip")

        #### Dow Jones
        DJI_filename="DJI_list.gzip"

        try:
            dji_ticker_df = pd.read_parquet(DJI_filename)
        except:
            dji_ticker_df = pd.read_html("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average")[1]
            dji_ticker_df.to_parquet(DJI_filename, compression="gzip")

        #### NASDAQ 100
        N100_filename = "NASDAQ_100_list.gzip"
        try:
            n100_ticker_df = pd.read_parquet(N100_filename)
        except:
            n100_ticker_df = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
            n100_ticker_df.to_parquet(N100_filename, compression="gzip")

        ticker_df = pd.concat([r1000_ticker_df['Ticker'], dji_ticker_df['Symbol'], n100_ticker_df['Ticker']], ignore_index=True)
        ticker_df_list = list(set(ticker_df.to_list()))
        return [s.replace('.', '-') for s in ticker_df_list]

    def get_mypicks(picks):
        """
        Reads a CSV file with a single list of tickers in a column, typically comes from StockRover. 
        It assumes a header row of "Ticker" and optionally a last like with "Summary". 
        Customize it if you want to use a different format.
        """
        try:
            extra = pd.read_csv(picks)['Ticker'] 
            if extra[-1:].values[0] == 'Summary':
                extra = extra[:-1]
        except:
            extra = pd.Series([])
        return extra

    def get_tickers_from_db(conn):
        """
        This function queries all tickers from the DB
        """
        query = f"""
            SELECT ticker
            FROM stock_data;
        """
        df = pd.read_sql(query, conn)
        return df.columns.to_list()
    
    def cleanup_excluded(conn, excluded_tickers):
        """
        Deletes the excluded tickers from the database.
        """
        e = list(set(excluded_tickers))
        if not e:
            return e

        cur = conn.cursor()

        # Step 1: Check which tickers exist in the database
        ticker_str = ','.join(f"'{ticker}'" for ticker in e)
        query = f"""
            SELECT ticker
            FROM stock_data
            WHERE ticker IN ({ticker_str});
        """
        cur.execute(query)
        existing_tickers = [row[0] for row in cur.fetchall()]

        if not existing_tickers:
            cur.close()
            return  e # Exit early if there are no tickers to delete, but still returns the exclusion list

        # Step 2: Delete the existing tickers and log the deletions
        existing_ticker_str = ','.join(f"'{ticker}'" for ticker in existing_tickers)
        ticker_list = [(ticker, False) for ticker in existing_tickers]

        query = f"""
            DELETE
            FROM stock_data
            WHERE ticker IN ({existing_ticker_str});
        """
        cur.execute(query)

        query = "INSERT INTO ticker_log (log_entry, ticker, added) VALUES (NOW(), %s, %s)"
        cur.executemany(query, ticker_list)
        cur.close()
        return e


    # get_tickers_list function logic starts here
    excl = cleanup_excluded(conn, read_file(exclusion))
    all_tickers = (set(read_file(inclusion)) | set(get_exchanges_tickers()) | set(get_tickers_from_db(conn)) | set(get_mypicks(picks))) - set(excl + ['ticker'])
    return list(all_tickers)


def get_stock_from_db(conn, ticker):
    query = f"""
        SELECT timestamp, open, high, low, close, volume
        FROM stock_data
        WHERE ticker = '{ticker}'
        ORDER BY timestamp;
    """
    df = pd.read_sql(query, conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)

    return df


def calculate_downloads(conn, tickers):
    """
    Returns a list of dictionaries with date and the tickers to download starting on that date.
    """

    # Local functions
    def get_latest_stock_data(conn, tickers):
        """
        Get the latest updated timestamps for all tickers in the DB.
        """
        if not tickers:
            return pd.DataFrame(columns=['ticker', 'timestamp']).set_index('timestamp')

        tickers_tuple = tuple(tickers)
        query = f"""
        WITH Latest AS (
            SELECT ticker, MAX(timestamp) AS latest_timestamp
            FROM stock_data
            WHERE ticker IN %s
            GROUP BY ticker
        )
        SELECT sd.ticker, sd.timestamp
        FROM stock_data sd
        JOIN Latest l ON sd.ticker = l.ticker AND sd.timestamp = l.latest_timestamp;
        """
        df = pd.read_sql_query(query, conn, params=(tickers_tuple,), index_col='timestamp')

        for ticker in tickers:
            if ticker not in df['ticker'].values:
                new_row = pd.DataFrame({'ticker': [ticker], 'timestamp': [BEGINNING_DATE]}).set_index('timestamp')
                df = df.append(new_row)

        return df

    def aggregate_dates_and_tickers(df):
        """
        Creates aggregates of stock tickers per date they were last updated.
        """
        df.index = df.index.astype(str)
        grouped = df.groupby(df.index)
        
        result = []
        
        for date, group in grouped:
            nd = next_trading_day(nyse, date)
            if nd < LTD:
                nds = nd.strftime('%Y-%m-%d')
                tickers = group['ticker'].tolist()
                result.append({"date": nds, "tickers": tickers})
        return result
    
    # Main function logic.
    df = get_latest_stock_data(conn, tickers)
    download_lists = aggregate_dates_and_tickers(df)

    return download_lists


def update_db(conn, download_lists):
    """ 
    Download the tickers from YFinance according to the passed lists and updates the DB.
    """
    ms = market_status(nyse)
    for item in download_lists:
        start_date = item['date']
        tickers = item['tickers']
        if (ms == 'closed'): 
            data = yf.download(tickers, start=start_date)
        else:
            data = yf.download(tickers, start=start_date, end=today_str)

        # YFinance returns a MultiIndex dataframe if you download more than 1 ticker, 
        # so we need to account for that (annoying).
        if isinstance(data.columns, pd.MultiIndex):
            grouped = data.groupby(level=1, axis=1)
            for ticker, ticker_data in grouped:
                ticker_data.columns = ticker_data.columns.droplevel(1)
                ticker_data['ticker'] = ticker 
                ticker_data.dropna(inplace=True)  
                ticker_data['Volume'] = ticker_data['Volume'].astype('int64') 
                buffer = io.StringIO()
                ticker_data[['ticker', 'Open', 'High', 'Low', 'Close', 'Volume']].to_csv(buffer, header=False, index=True)
                buffer.seek(0)  
                cur = conn.cursor()
                cur.copy_from(buffer, 'stock_data', columns=('timestamp', 'ticker', 'open', 'high', 'low', 'close', 'volume'), sep=',')
                conn.commit()
                cur.close()
        else:
            ticker = tickers[0]
            data['ticker'] = ticker 
            data.dropna(inplace=True) 
            data['Volume'] = data['Volume'].astype('int64') 
            buffer = io.StringIO()
            data[['ticker', 'Open', 'High', 'Low', 'Close', 'Volume']].to_csv(buffer, header=False, index=True)
            buffer.seek(0)
            cur = conn.cursor()
            cur.copy_from(buffer, 'stock_data', columns=('timestamp', 'ticker', 'open', 'high', 'low', 'close', 'volume'), sep=',')
            conn.commit()
            cur.close()
    
    process_csv_and_update_db(conn)
    return


def get_close_data(conn):
    """ 
    Returns a dataframe with all the 'close' data for all tickers in the DB. 
    Very useful for analytics later on.
    """
    query = """
        SELECT timestamp, ticker, close
        FROM stock_data
        ORDER BY timestamp ASC, ticker ASC;
    """
    close_data = pd.read_sql_query(query, conn)
    close_df = close_data.pivot(index='timestamp', columns='ticker', values='close')
    
    return close_df

