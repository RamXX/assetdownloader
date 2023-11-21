from assets_db import *


####### MAIN Fuction ########
def main():
    conn, engine = init_db()
    df = get_stock_counts(conn)
    print('Dates and counts for stocks entered in the system via the mypicks.csv file:')
    print(df)
    close_db(conn, engine)

# Program Main
if __name__ == "__main__":
    main()