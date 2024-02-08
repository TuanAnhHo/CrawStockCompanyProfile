from .AvailableStockSymbol import AvailableStock                           
from datetime import datetime
from pymongo import MongoClient
import requests, pytz

# Connect to Mongo
def connect_mongodb():
    print("Connecting to MongoDB.....")    
    
    # Provide the connection details
    hostname = 'mongo'
    port = 27017  # Default MongoDB port
    username = 'root'  # If authentication is required
    password = 'example'  # If authentication is required

    # Create a MongoClient instance
    try:
        client = MongoClient(hostname, port, username=username, password=password)
        # Return client 
        return client
    except:
        raise ValueError("Failed to connect to MongoDB")


def check_existing_company_profile(symbol: str):
    client = connect_mongodb()
    db = client["StockProject"]
    col = db["CompanyProfile"]
    
    print("Checking existing company profile.....")
    # Check existing profile of input symbol
    my_query = {'symbol':symbol}
    query_result = col.find_one(my_query)
    client.close()
    
    if query_result is None:
        return 'InsertCompanyProfile'
    else:
        return 'UpdateCompanyProfile'


# Function to crawl company profile from FireAnt
def crawl_company_info(symbol: str):

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
        print("Crawl company profile successfully (API Status Code " + str(response.status_code) + ")")
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
    
    return company_profile_dict


# Insert data in to company_profile table
def insert_company_profile(symbol: str):
    
    # Get connection and change to cursor
    client = connect_mongodb()
    db = client['StockProject']
    col = db['CompanyProfile']
    
    # Company profile data 
    print("Get Company profile of {symbol} symbol".format(symbol=symbol))
    company_profile_data = crawl_company_info(symbol)

    # insert data into table    
    print("Insert profile of {symbol} symbol into table {table_name} in MongoDB.....".format(symbol=symbol, table_name="company_profile"))
    
    try:
        col.insert_one(company_profile_data)
        print("Insert company profile of {symbol} into MongoDB successfully".format(symbol=symbol))
        client.close()
    except:
        raise ValueError("Failed to insert company profile of {symbol} into MongoDB".format(symbol=symbol))
    
    client.close()


def update_company_profile(symbol: str):
    
    # Get connection and change to cursor
    client = connect_mongodb()
    db = client['StockProject']
    col = db['CompanyProfile']
    
    # column query
    my_query = {'symbol':symbol}
    
    # delete existing symbol profile in collection
    print("Deleting existing compay profile in collection.....")
    try:
        col.delete_one(my_query)
    except:
        raise ValueError("Failed to delete exising company profile of {symbol}".format(symbol=symbol))
        
    # Crawl company profile
    print("Crawling company profile of {symbol}.....".format(symbol=symbol))
    company_profile_data = crawl_company_info(symbol)
    
    # insert data into collection
    try:
        print("Insert company profile of {symbol} into collection".format(symbol=symbol))
        col.insert_one(company_profile_data)
        client.close()
    except:
        raise ValueError("Failed to insert company profile of {symbol} into collection".format(symbol=symbol))
    
    client.close()