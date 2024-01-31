import requests, psycopg2, pandas
from datetime import datetime
from .AvailableStockSymbol import AvailableStock
from psycopg2 import OperationalError

# Connect to Postgresql
def ConnectPostgres():

#   Connect to the PostgreSQL database server 
#   Parameters: 
#       - relative_path: path to ini file which contains connection informations
#       - section: section in ini file which represent for config you want to get in this situation is Postgresql section
    try:
        # read connection parameters
        params = {'host': 'localhost', 'database': 'StockProject', 'port': '5432', 'user': 'airflow', 'password': 'airflow'}

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


# Create table if it not exist
def CreateStockPriceTable():
    create_stock_price = """
    CREATE TABLE public.stock_price (
        stock_date date,
        symbol text, 
        open_price float,
        highest_price float,
        lowest_price float,
        closing_price float,
        volumn numeric,
	CONSTRAINT company_profile_pk PRIMARY KEY (stock_date, symbol));
    """
    cursor = conn.cursor()
    conn = ConnectPostgres()
    
    print("Create stock_price_data table.....")
    
    try:
        cursor.execute(create_stock_price)
        conn.commit()
        cursor.close()
        print("Create stock_price table successfully")
    except:
        raise ValueError("Failed to create stock_pirce table")
    

## Support Function - Convert date string to seconds
def ConvertDateToSeconds(input_date: str) -> int:
    return int(datetime.strptime(input_date, '%Y-%m-%d').timestamp()) + (
                7 * 60 * 60)  ## Convert 7 hours to seconds to plus to input date because API response time default +7 hours


# Function to validate range time for query params
def ValidateRangeTime(start_date: str, end_date: str) -> dict:
    from_date_converted = ConvertDateToSeconds(start_date)
    to_date_converted = ConvertDateToSeconds(end_date)

    if to_date_converted < from_date_converted:
        raise ValueError("Please import range time again beacause start date is greater end date")

    return {
        "from_date_convert": from_date_converted,
        "to_date_convert": to_date_converted,
    }


# Function crawl stock data from SSI iboard
def CrawlStockPrice(symbol:str, start_date:str, end_date:str):
    
    # Set up URL base and Headers for API
    url_base = "https://iboard.ssi.com.vn/dchart/api/history?"
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36"
    }

    date_query_params = ValidateRangeTime(start_date=start_date, end_date=end_date)

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
def InsertCompanyProfile(symbol:str,start_date:str, end_date:str):
    
    # Get connection and change to cursor
    conn = ConnectPostgres()
    cursor = conn.cursor()

    # Company profile data 
    print("Get stock price of {symbol} symbol from {start_date} to {end_date}".format(symbol=symbol, start_date=start_date, end_date=end_date))
    data = CrawlStockPrice(symbol,start_date=start_date,end_date=end_date)

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

    InsertDataScripts = """
            INSERT INTO public.{table_name} ({columns})
            VALUES {values};
        """.format(table_name='stock_price', columns=columns, values=values)
    
    InsertDataScripts = InsertDataScripts.replace("None", "NULL").replace(')(','),(')
    
    print("Insert profile of {symbol} symbol into table {table_name} in Postgresql.....".format(symbol=symbol, table_name="company_profile"))
    
    # try:
    #     cursor.execute(InsertDataScripts)
    #     conn.commit()
    #     print("Inserted data successfully")
    #
    #     conn.close()
    #     print("Close connection")
    #
    # except:
    #     raise ValueError("Failed to insert data into StockPrice table")

    return InsertDataScripts








# def SymbolStockPrice(self):
#     ## Parse response to Dataframe
#     JsonResponse = self.GetResponse()

# headers = {
#     "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36",
#     "Authorization":"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6IjI4OTczNSIsInV1aWQiOiI5YTE2Yzc5MS01ZjU2LTQ4NDUtYmM5NS0yOWJmNDY2Y2UwNzEiLCJjaGFubmVsIjoid2ViIiwic3lzdGVtVHlwZSI6Imlib2FyZCIsInZlcnNpb24iOiIyIiwiaWF0IjoxNzA1ODMwMzAzLCJleHAiOjE3MDU4NTkxMDN9.0eXyqE8qZLm497EAxkT087IXHlN0S9joKy2ecLaJdAs"
# }

# response = requests.get("https://fiin-fundamental.ssi.com.vn/FinancialStatement/GetBalanceSheet?language=vi&OrganCode=BID", headers=headers)
# print(json.dumps(response.json(), indent=4))