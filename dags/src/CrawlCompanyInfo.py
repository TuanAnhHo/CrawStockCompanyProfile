from .AvailableStockSymbol import AvailableStock                           
from psycopg2 import OperationalError
from datetime import datetime
import pytz, requests, psycopg2


# Connect to Postgresql
def connect_postgres():

#   Connect to the PostgreSQL database server 
#   Parameters: 
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
def check_table_existing(table_name:str):
    
    # Get cursor of Postgresql
    cursor = connect_postgres().cursor()

    print("Checking existing table...")
    
    get_existing_table_script = "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    cursor.execute(get_existing_table_script)
    
    list_table = [i[0] for i in cursor.fetchall()]
    
    if table_name in list_table:
        return "ExistingCompanyProfile"
    else: 
        return "CreateCompanyProfileTable"


# Create table if it not exist
def create_profile_company_table():
    create_company_profile_script = """
    CREATE TABLE public.company_profile (
        institutionid text NOT NULL,\n
        symbol text NOT NULL,\n
        icbcode text NULL,\n
        companyname text NULL,\n
        shortname text NULL,\n
        internationalname text NULL,\n
        phone text NULL,\n
        employees numeric NULL,\n
        branches numeric NULL,\n
        establishmentdate timestamp NULL,\n
        chartercapital numeric NULL,\n
        dateoflisting timestamp NULL,\n
        exchange text NULL,\n
        listingvolume numeric NULL,\n
        stateownership numeric NULL,\n
        foreignownership numeric NULL,\n
        otherownership numeric NULL,\n
        overview text,\n
        created_date timestamp,\n
        updated_date timestamp,\n
	CONSTRAINT company_profile_pk PRIMARY KEY (institutionid, symbol));\n
    """
    conn = connect_postgres()
    cursor = conn.cursor()
    
    print("Create company_profile table.....")
    
    try:
        cursor.execute(create_company_profile_script)
        conn.commit()
        cursor.close()
        print("Create company_profile table successfully")
    except:
        raise ValueError("Failed to create company_proflie table")


# Condition function to decided with branch of code needed to execute
def existing_company_profile(symbol:str) -> str:
    
    # Get cursor connection
    cursor = connect_postgres().cursor()

    # Get existing symbol in company_profile table
    get_existing_symbol_script = "SELECT DISTINCT SYMBOL FROM PUBLIC.COMPANY_PROFILE CP"
    cursor.execute(get_existing_symbol_script)
    existing_symbol_list = [i[0] for i in cursor]
    cursor.close()
    
    if symbol in existing_symbol_list:
        return 'UpdateCompanyProfile'
    else: 
        return 'InsertCompanyProfile_branch_2'


# Function to crawl company profile from FireAnt
def crawl_company_info(symbol:str):

    if symbol not in AvailableStock['symbol']:
        raise ValueError("The symbol is not availble. Please try another symbol")

    # Open "AuthorizationInfo.json" to get Authorization Information
    request_url = "https://restv2.fireant.vn/symbols/{symbol}/profile".format(symbol=symbol)
    # Using ReadConfigFile function to get Authorization information of FireAnt
    headers = {
        "Authorization":"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg",
        "User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    }
        
    try:
        response = requests.get(request_url, headers=headers)
        print("API Status Code " + str(response.status_code))
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

    company_profile = response.json()
    list_crawl_key = ['institutionID', 'symbol', 'icbCode', 'companyName', 'shortName', 'internationalName', 'phone',
                         'employees', 'branches', 'establishmentDate', 'charterCapital', 'dateOfListing',
                         'exchange', 'listingVolume', 'stateOwnership', 'foreignOwnership', 'otherOwnership'
                    ]

    current_time = datetime.now(tz=pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    company_profile_dict = {str(i): company_profile[i] for i in list_crawl_key}
    company_profile_dict['overview'] = company_profile['overview'].replace('\r\n', '')
    company_profile_dict['created_date'] = current_time
    company_profile_dict['updated_date'] = None
    
    return company_profile_dict


# Insert data in to company_profile table
def insert_company_profile(symbol:str, table_name:str):
    
    ## Get connection and change to cursor
    conn = connect_postgres()
    cursor = conn.cursor()

    ## Company profile data 
    print("Get Company profile of {symbol} symbol".format(symbol=symbol))
    data = crawl_company_info(symbol)

    ## Get columns name to insert data into table
    columns_list = data.keys()
    columns = ', '.join(x for x in columns_list)

    ## Get value to insert data into table
    values = tuple(data.values())

    insert_data_script = """
            INSERT INTO public.{table_name} ({columns})
            VALUES {values}
        """.format(table_name=table_name, columns=columns, values=values)
    
    insert_data_script = insert_data_script.replace("None", "NULL")
    
    print("Insert profile of {symbol} symbol into table {table_name} in Postgresql.....".format(symbol=symbol, table_name="company_profile"))
    
    try:
        cursor.execute(insert_data_script)
        conn.commit()
        print("Inserted data successfully")
        
        conn.close()
        print("Close connection")
    
    except:
        raise ValueError("Failed to insert data")


def update_company_profile(symbol:str, table_name:str):

    ## Get connection and change to cursor
    conn = connect_postgres()
    cursor = conn.cursor()

    ## Company profile data 
    print("Get Company profile of {symbol} symbol".format(symbol=symbol))
    data = crawl_company_info(symbol)
    data.pop('created_date')
    data['updated_date'] = datetime.now(tz=pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    values_change_list = []
    for x,y in data.items():
        values_change_list.append(str(x)+'='+"'"+str(y)+"'")

    values_change = ',\n        '.join(values_change_list).replace("'None'",'NULL')

    update_data_scripts = """
    UPDATE {table_name} 
    SET 
        {values_change} 
    WHERE symbol = '{symbol}';
    """.format(table_name=table_name, symbol=symbol, values_change=values_change)

    print("Update profile of {symbol} symbol into table {table_name} in Postgresql.....".format(symbol=symbol, table_name="company_profile"))

    try:
        cursor.execute(update_data_scripts)
        conn.commit()
        print("Update data successfully")

        conn.close()
        print("Close connection")

    except:
        raise ValueError(f"Failed to update data into {table_name} table".format(table_name=table_name))