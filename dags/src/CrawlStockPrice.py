import requests, psycopg2, pandas
from datetime import datetime, date, timedelta
from .AvailableStockSymbol import AvailableStock
from psycopg2 import OperationalError
from pytz import timezone

# Connect to Postgresql
def connect_postgres():

#   Connect to the PostgreSQL database server
#   Parameters:CrawlStockPrice.py
#       - relative_path: path to ini file which contains connection informations
#       - section: section in ini file which represent for config you want to get in this situation is Postgresql section
    try:
        # read connection parameters
        params = {'host': 'postgres', 'database': 'StockProject', 'port': '5432', 'user': 'airflow', 'password': 'airflow'}

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print("Successfully connect to Postgresql")
        print(db_version)

    except OperationalError as e:
        raise e

    return conn


# Condition function to decide create table or not
def check_table_existing(table_name: str):
    # Get cursor of Postgresql
    cursor = connect_postgres().cursor()

    print("Checking existing table...")

    get_existing_table_script = "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    cursor.execute(get_existing_table_script)

    list_table = [i[0] for i in cursor.fetchall()]

    if table_name in list_table:
        return "InsertStockPrice"
    else:
        return "CreateStockPriceTable"


# Create table if it not exist
def create_stock_price_table():
    create_stock_price = """
    CREATE TABLE public.stock_price (
        stock_date date,
        symbol text, 
        open_price float,
        highest_price float,
        lowest_price float,
        closing_price float,
        volumn numeric,
	CONSTRAINT stock_price_pk PRIMARY KEY (stock_date, symbol));
    """
    conn = connect_postgres()
    cursor = conn.cursor()

    print("Create stock_price_data table.....")

    try:
        cursor.execute(create_stock_price)
        conn.commit()
        cursor.close()
        print("Create stock_price table successfully")
    except:
        raise ValueError("Failed to create stock_pirce table")


# Support Function - Convert date string to seconds
def convert_date_to_second(input_date: str) -> int:
    return int(datetime.strptime(input_date, '%Y-%m-%d').timestamp()) + (
                7 * 60 * 60)  ## Convert 7 hours to seconds to plus to input date because API response time default +7 hours


# Get latest record of input symbol to incremental sync mode - Avoid duplucated data
def get_latest_record_of_symbol(symbol: str):

    conn = connect_postgres()
    cursor = conn.cursor()

    get_lastest_record_script = """
    SELECT MAX(stock_date) AS stock_date
    FROM public.stock_price
    WHERE symbol = '{symbol}'
    GROUP BY symbol
    """.format(symbol=symbol)

    cursor.execute(get_lastest_record_script)
    latest_record = cursor.fetchone()

    if latest_record is not None:
        return latest_record[0]
    else:
        return date(2024,1,1)


# Function to validate range time for query params
def get_range_time(symbol:str) -> dict:
    try:
        # get the latest date of symbol price in database
        latest_record_symbol = get_latest_record_of_symbol(symbol=symbol)

        # set range time parameters
        from_date_converted = convert_date_to_second(str(latest_record_symbol))
        end_date = datetime.now(tz=timezone("Asia/Ho_Chi_Minh")).date() + timedelta(days=-1) # get previous day
        to_date_converted = convert_date_to_second(str(end_date))

    except:
        raise ValueError("Failed to get range time")

    if from_date_converted > to_date_converted: raise ValueError("Invalid range time")

    print("Get range time sucessfully")

    return {
        "from_date_convert": from_date_converted,
        "to_date_convert": to_date_converted,
        "start_date": str(latest_record_symbol),
        "end_date": str(datetime.now(tz=timezone("Asia/Ho_Chi_Minh")).date() + timedelta(days=-1))
    }


# Function crawl stock data from SSI iboard
def crawl_stock_price(symbol:str, date_query_params: dict):

    # Set up URL base and Headers for API
    url_base = "https://iboard.ssi.com.vn/dchart/api/history?"
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36"
    }

    if symbol not in AvailableStock['symbol']:
        raise ValueError("The symbol is not availble. Please try another symbol")

    ## Call SSI API to get stock price and handle Error
    try:
        ## Note -- Response date concluded 7 fields: t: Time, c: ClosingPrice, o: OpenPrice, h: HighestPrice, l: LowerPrice, v: Volumn
        response = requests.get(
            url=url_base + "resolution=1D&symbol={0}&from={1}&to={2}".format(symbol, date_query_params['from_date_convert'], date_query_params['to_date_convert']),
            headers=headers
        )
        print("API Status Code " + str(response.status_code))

    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

    # Parse response
    json_response = response.json()

    if not json_response.get('t'):
        print("There is no data")
        return None
    else:
        try:
            df_stock_data = pandas.DataFrame(json_response)
            df_stock_data['t'] = df_stock_data['t'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
            df_stock_data = df_stock_data.rename(columns={'t': 'stock_date',
                                                            'c': "closing_price",
                                                            'o': 'opening_price',
                                                            'h': 'highest_price',
                                                            'l': 'lowest_price',
                                                            'v': 'volumn'})
            df_stock_data.drop(['s'], axis=1, inplace=True)
            df_stock_data['symbol'] = symbol

            return df_stock_data.to_dict(orient='records')

        except:
            raise ValueError("Failed to parse API response")


# Insert data in to company_profile table
def insert_stock_price(symbol:str):

    # get range time parameters
    date_query_params = get_range_time(symbol=symbol)

    if date_query_params['from_date_convert'] == date_query_params['to_date_convert']:
        print("Data have been up-to-date")
    else:
        # Get connection and change to cursor
        conn = connect_postgres()
        cursor = conn.cursor()

        # Company profile data
        print("Get stock price of {symbol} symbol from {start_date} to {end_date}".format(symbol=symbol, start_date=date_query_params["start_date"], end_date=date_query_params["end_date"]))
        data = crawl_stock_price(symbol=symbol, date_query_params=date_query_params)

        # Get columns name to insert data into table
        columns_list = data[0].keys()
        columns = ','.join(str(x) for x in columns_list)

        # Get value to insert data into table

        list_values = []
        for i in range(1,len(data)):
            str_values = tuple(data[i].values())
            list_values.append(str_values)
        values = ""

        for i in list_values:
            values += str(i)

        insert_date_script = """
                INSERT INTO public.{table_name} ({columns})
                VALUES {values};
            """.format(table_name='stock_price', columns=columns, values=values)

        insert_date_script = insert_date_script.replace("None", "NULL").replace(')(','),(')

        print("Insert profile of {symbol} symbol into table {table_name} in Postgresql.....".format(symbol=symbol, table_name="company_profile"))

        try:
            cursor.execute(insert_date_script)
            conn.commit()
            records = len(data) - 1 # minus to 1 because the first row is column name
            print("Inserted data {records} successfully".format(records=records))
            conn.close()
            print("Close connection")

        except:
            raise ValueError("Failed to insert data into StockPrice table")


