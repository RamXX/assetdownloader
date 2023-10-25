from assets_db import *

####### MAIN Fuction ########
def main():
    print('Initializing the database.')
    conn = init_db()
    print('Obtaining list of tickers and dates.')
    tickers = get_tickers_list(conn, picks='./mypicks.csv', inclusion='./inclusion_list.txt', exclusion='./exclusion_list.txt')
    if tickers != []:
        print(f'{len(tickers)} total tickers found. Checking which ones need updating...')
        download_lists = calculate_downloads(conn, tickers) # Returns a list of sets ordered per date.
        if (download_lists != []):
            print('Downloading tickers and updating the database.')
            update_db(conn, download_lists)
            print('Database update complete.')
        else:
            print('Nothing to download. The database is up-to-date.')
    else:
        print('Nothing to download. The database is up-to-date.')

    close_db(conn)


# Program Main
if __name__ == "__main__":
    main()