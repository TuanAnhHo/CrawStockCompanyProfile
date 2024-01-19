from AvailableStockSymbol import AvailableStock
from psycopg2 import OperationalError
from datetime import datetime, date
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import pytz, requests, psycopg2


symbol = "HPG"
table_name = 'company_profile'


# Connect to Postgresql
def ConnectPostgres():

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
def CheckTableExisting(table_name:str):
    
    # Get cursor of Postgresql
    cursor = ConnectPostgres().cursor()

    print("Checking existing table...")
    
    GetExistingTable = "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    cursor.execute(GetExistingTable)
    
    list_table = [i[0] for i in cursor.fetchall()]
    
    if table_name in list_table:
        return "ExistingCompanyProfile"
    else: 
        return "CreateCompanyProfileTable"


# Create table if it not exist
def CreateCompanyProfileTable():
    create_company_profile = """
    CREATE TABLE public.company_profile (
        institutionid text NOT NULL,
        symbol text NOT NULL,
        icbcode text NULL,
        companyname text NULL,
        shortname text NULL,
        internationalname text NULL,
        phone text NULL,
        employees numeric NULL,
        branches numeric NULL,
        establishmentdate timestamp NULL,
        chartercapital numeric NULL,
        dateoflisting timestamp NULL,
        exchange text NULL,
        listingvolume numeric NULL,
        stateownership numeric NULL,
        foreignownership numeric NULL,
        otherownership numeric NULL,
        overview text,
        created_date timestamp,
        updated_date timestamp,
	CONSTRAINT company_profile_pk PRIMARY KEY (institutionid, symbol));
    """
    conn = ConnectPostgres()
    cursor = conn.cursor()
    
    print("Create company_profile table.....")
    
    try:
        cursor.execute(create_company_profile)
        conn.commit()
        cursor.close()
        print("Create company_profile table successfully")
    except:
        raise ValueError("Failed to create company_proflie table")


## Condition function to decided with branch of code needed to execute
def ExistingCompanyProfile(symbol:str) -> list:
    
    # Get cursor connection
    cursor = ConnectPostgres().cursor()

    # Get existing symbol in company_profile table
    GetExistingSymbol = "SELECT DISTINCT SYMBOL FROM PUBLIC.COMPANY_PROFILE CP"
    cursor.execute(GetExistingSymbol)
    existing_symbol_list = [i[0] for i in cursor]
    cursor.close()
    
    if symbol in existing_symbol_list:
        return 'UpdateCompanyProfile'
    else: 
        return 'InsertCompanyProfile_branch_2'


# Function to crawl company profile from FireAnt
def CrawlCompanyInfo(symbol:str):

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

    CompanyProfile = response.json()
    ListCrawlKeys = ['institutionID', 'symbol', 'icbCode', 'companyName', 'shortName', 'internationalName', 'phone',
                         'employees', 'branches', 'establishmentDate', 'charterCapital', 'dateOfListing',
                         'exchange', 'listingVolume', 'stateOwnership', 'foreignOwnership', 'otherOwnership'
                    ]

    current_time = datetime.now(tz=pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    CompanyProfileDict = {str(i): CompanyProfile[i] for i in ListCrawlKeys}
    CompanyProfileDict['overview'] = CompanyProfile['overview'].replace('\r\n', '')
    CompanyProfileDict['created_date'] = current_time
    CompanyProfileDict['updated_date'] = None
    # datetime(1,1,1,0,0,0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    return CompanyProfileDict


# Insert data in to company_profile table
def InsertCompanyProfile(symbol:str):
    
    ## Get connection and change to cursor
    conn = ConnectPostgres()
    cursor = conn.cursor()

    ## Company profile data 
    print("Get Company profile of {symbol} symbol".format(symbol=symbol))
    data = CrawlCompanyInfo(symbol)

    ## Get columns name to insert data into table
    columns_list = data.keys()
    columns = ', '.join(x for x in columns_list)

    ## Get value to insert data into table
    values = tuple(data.values())

    InsertDataScripts = """
            INSERT INTO public.{table_name} ({columns})
            VALUES {values}
        """.format(table_name=table_name, columns=columns, values=values)
    
    InsertDataScripts = InsertDataScripts.replace("None", "NULL")
    
    print("Insert profile of {symbol} symbol into table {table_name} in Postgresql.....".format(symbol=symbol, table_name="company_profile"))
    
    try:
        cursor.execute(InsertDataScripts)
        conn.commit()
        print("Inserted data successfully")
        
        conn.close()
        print("Close connection")
    
    except psycopg2.errors.UniqueViolation as e:
        print("Failed to insert data")
        raise e 


def UpdateCompanyProfile(symbol:str):

    ## Get connection and change to cursor
    conn = ConnectPostgres()
    cursor = conn.cursor()   
    
    ## Company profile data 
    print("Get Company profile of {symbol} symbol".format(symbol=symbol))
    data = CrawlCompanyInfo(symbol)
    data.pop('created_date')
    data['updated_date'] = datetime.now(tz=pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    values_change_list = []
    for x,y in data.items():
        values_change_list.append(str(x)+'='+"'"+str(y)+"'")

    values_change = ',\n        '.join(values_change_list).replace("'None'",'NULL')
    
    UpdatetDataScripts = """
    UPDATE {table_name} 
    SET 
        {values_change} 
    WHERE symbol = '{symbol}';
    """.format(table_name=table_name, symbol=symbol, values_change=values_change)    
    
    print("Update profile of {symbol} symbol into table {table_name} in Postgresql.....".format(symbol=symbol, table_name="company_profile"))
    
    try:
        cursor.execute(UpdatetDataScripts)
        conn.commit()
        print("Update data successfully")
        
        conn.close()
        print("Close connection")
    
    except psycopg2.errors.UniqueViolation as e:
        print("Failed to insert data")
        raise e     


# Declare default arguments in Airflow
default_args = {
    'owner':'anh.ho',
    'start_date':'2024-01-01',
    'email':'anh.ho@tititada.com',
    'retries':0,
}

dag= DAG(
    dag_id="company_profile_pipeline",
    default_args=default_args,
    schedule='@daily',
    catchup=True
)

brand_check_existing_table = BranchPythonOperator(
    task_id='CheckTableExisting',
    python_callable=CheckTableExisting,
    op_kwargs = {'table_name':table_name},
    dag=dag,
)

branch_check_existing_profile = BranchPythonOperator(
    task_id='ExistingCompanyProfile',
    python_callable=ExistingCompanyProfile,
    op_kwargs = {'symbol':symbol},
    dag=dag,
)

create_table = PythonOperator(
    task_id = "CreateCompanyProfileTable",
    python_callable=CreateCompanyProfileTable,
    dag=dag,
)

insert_data1 = PythonOperator(
    task_id = "InsertCompanyProfile_branch_1",
    python_callable=InsertCompanyProfile,
    op_kwargs = {'symbol':symbol},
    dag=dag,
)

insert_data2 = PythonOperator(
    task_id = "InsertCompanyProfile_branch_2",
    python_callable=InsertCompanyProfile,
    op_kwargs = {'symbol':symbol},
    dag=dag,
)

update_data = PythonOperator(
    task_id = "UpdateCompanyProfile",
    python_callable=UpdateCompanyProfile,
    op_kwargs = {'symbol':symbol},
    dag=dag,
)

brand_check_existing_table >> [create_table,branch_check_existing_profile]
create_table >> insert_data1
branch_check_existing_profile >> [insert_data2, update_data]

