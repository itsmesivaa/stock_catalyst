
import pandas as pd

# Importing the StringIO module.
from io import StringIO
    
from tqdm import tqdm

# import the requests library 
import requests

#Used for concurrent programming
import asyncio 

#httpx is a fast and multi-purpose HTTP toolkit that allows running multiple probes using the retryablehttp library. 
#It is designed to maintain result reliability with an increased number of threads.
import httpx 

#Importing Datetime module
import datetime
import time

#Importing DB connection function
from db_conn import load_config

import pyodbc
import sqlite3

#Defining Start date and End date to load historical prices for the respective stocks

#Passing date as string
#start = datetime.date(2022,1,1)
#end = datetime.date.today()

#Loading Price for Previous day
end = datetime.date.today()
start = end - datetime.timedelta(days=1)

#Loading current day prices for the stock
#start = datetime.date.today()
#end = start + datetime.timedelta(days=1)


print("StartDate:",start)   
print("End Date:",end)
#Converting string to unix time format
start_date = int(time.mktime(start.timetuple()))
end_date = int(time.mktime(end.timetuple()))

print("StartDate:",start_date)
print("End Date:",end_date)

#start_date= 946684800 #Jan 1 2000 in Unix format

#end_date= 1714003200 #Apr 24 2024 in Unix format

#Defining headers with User agent to establish requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}


#Reading excel file to fetch all listed NSE Stocks from local folder
all_equity_lst = pd.read_excel("./Equity.xlsx",sheet_name="Equity_detail")

stock_list = []
stock_not_valid = []
for ind_stock in all_equity_lst['SYMBOL']:
    stock_list.append(ind_stock)

stock_list_length = len(stock_list)
print("Length of StockList:",stock_list_length)

all_stock_url = []
valid_stock_list=[]
temp_df = pd.DataFrame(columns=['stock_name','stock_url'])



def yahoo_url_build(stock_list,start_date,end_date):
    #Building URL for the stocks through iteration
    valid_df = pd.DataFrame()
    not_valid_df = pd.DataFrame()
    for ind_url in range(0,len(stock_list)):
        print("Inside Loop For processing Iteration:",{ind_url})
        if stock_list[ind_url] == '^NSEI':
            stock_url = "https://query1.finance.yahoo.com/v7/finance/download/%5ENSEI?period1={}&period2={}&interval=1d&events=history&includeAdjustedClose=true" \
                    .format(start_date,end_date)
            response = requests.get(url = stock_url, headers = headers)
            print("INSIDE NSEI {}-".format(stock_list[ind_url]),response.status_code)
            if response.status_code == 200:                    
                all_stock_url.append(stock_url)
                valid_df = valid_df._append({'stock_name': stock_list[ind_url], 'stock_url': stock_url},ignore_index = True)
                
            else:
                continue
            
        else:
            # Attempt to format the ind_url with the stock symbol from the dictionary (.NS)
            stock_url = "https://query1.finance.yahoo.com/v7/finance/download/{}.NS?period1={}&period2={}&interval=1d&events=history&includeAdjustedClose=true" \
                .format(stock_list[ind_url],start_date,end_date)
            response = requests.get(url = stock_url, headers = headers)
            print("INSIDE {}.NS-".format(stock_list[ind_url]),response.status_code)
            if response.status_code == 200:
                all_stock_url.append(stock_url)
                valid_df = valid_df._append({'stock_name': stock_list[ind_url], 'stock_url': stock_url},ignore_index = True)
            else:  # Check if ind_url is still empty after the first Else block
                # Attempt to format the ind_url with the stock symbol from the dictionary (.BO)
                stock_url = "https://query1.finance.yahoo.com/v7/finance/download/{}.BO?period1={}&period2={}&interval=1d&events=history&includeAdjustedClose=true" \
                .format(stock_list[ind_url], start_date, end_date)
                response = requests.get(url = stock_url, headers = headers)
                print("Inside {}.BO-".format(stock_list[ind_url]),response.status_code)
                if response.status_code == 200:
                    all_stock_url.append(stock_url)
                    valid_df = valid_df._append({'stock_name': stock_list[ind_url], 'stock_url': stock_url},ignore_index = True)
                else:
                    #Removing stocks which have invalid URL from lists
                    print("Stock not valid-{}.".format(stock_list[ind_url]))
                    stock_not_valid.append(stock_list[ind_url])
                    not_valid_df = not_valid_df._append({'stock_name': stock_list[ind_url]},ignore_index = True)
                    continue
        
        
    print(valid_df)

    # Load the existing workbook
    file_path = "./Equity.xlsx"
    writer = pd.ExcelWriter(path= file_path, engine='openpyxl',mode='a',if_sheet_exists='overlay')
    # Get the existing sheet object
    workbook = writer.book
    valid_url_worksheet = workbook['yahoo_stock_url']
    invalid_stock_worksheet = workbook['invalid_stocks']
    # Get the last row number (assuming data starts from row 1)
    url_last_row = valid_url_worksheet.max_row
    invalid_stock_last_row = invalid_stock_worksheet.max_row
    valid_df.to_excel(writer, sheet_name='yahoo_stock_url', startrow=url_last_row, index=False,header=False)
    not_valid_df.to_excel(writer, sheet_name= 'invalid_stocks', startrow=invalid_stock_last_row, index=False,header=False)
    writer._save()


def remove_common_to_keep_valid(stock_list, stock_not_valid):
    common = set(stock_list) & set(stock_not_valid)
    valid_stock_list.extend(i for i in stock_list if i not in common)
    print("list1 : ", stock_list)

#Commenting out these two functions because we already manipulated the URL's for the stock
#If we need to build new set of URL's then we can re-use it

#yahoo_url_build(stock_list,start_date,end_date)
#remove_common_to_keep_valid(stock_list, stock_not_valid)


stock_detail_df = pd.read_excel("./Equity.xlsx",sheet_name='yahoo_stock_url')

valid_stock_list = stock_detail_df['stock_name'].to_list()
all_stock_url = stock_detail_df['stock_url'].tolist()
print(valid_stock_list)

#Replacing period1 & period2 date to required from date and to date respectively
for x in range(0,len(valid_stock_list)):
    all_stock_url[x] = all_stock_url[x].replace(all_stock_url[x].split('=',2)[1].split('&',1)[0], str(start_date))
    all_stock_url[x] = all_stock_url[x].replace(all_stock_url[x].split('=',2)[2].split('&',1)[0], str(end_date))


print("ValidStockName:{}".format(valid_stock_list),"Inside URLS:{}".format(all_stock_url))
#print("All Stock-{}".format(stock_list))
print("Not valid Stocks:{}".format(stock_not_valid))

MAX = 10000

timeout = httpx.Timeout(None)
limits = httpx.Limits(max_connections=MAX)

async def fetch():
    
    counter = 0
    iteration = 0
    
    #Creating basedataframe to hold complete data
    base_df = pd.DataFrame()
    success = False
    while not success: 
        try: 
            print("Length of ValidStockList:",len(valid_stock_list))
            while(counter < len(all_stock_url)):
                time.sleep(10)
                iteration = iteration + 1
                async with httpx.AsyncClient(timeout = timeout, limits=limits) as client:
                    reqs = [client.get(z,headers = headers) for z in all_stock_url[counter : counter + 50]]
                    print("Inside Fetch function-{}".format(iteration))
                    resultzz = await asyncio.gather(*reqs, return_exceptions=True)
            
                print(resultzz)
                for p in range(0,len(resultzz)):    
                    content = getattr(resultzz[p], 'content')
                    print(content)
                    df = pd.read_csv(StringIO(content.decode('utf-8')),sep = ',')
                    
                    #Rephrasing / Adding dataframe by including new column with script name
                    df.insert(loc=0,column='Stock',value=valid_stock_list[p + counter]) 
                    
                    #Appending the results to the base Dataframe for consolidated view
                    base_df = base_df._append(df)                 
                    print(df)
                    
                
                counter = counter + 50
                del[resultzz]
                print("Final Print",base_df)
                
            success = True    
            
        except:
            print("Failed! Retrying...")
            time.sleep(300)  # wait for 5 minutes second before retrying
        
    #Formating datatypes on Base Dataframe to appropriate format for further SQL data load process
    #Changing Object type to Date format
    base_df['Date'] = pd.to_datetime(base_df['Date'])

    #Formatting Volume to Integer from Float type to make it easier
    base_df.Volume = base_df.Volume.astype('Int64')

    #Rounding the prices to two decimals to multiple columns
    base_df[['Open','High','Low','Close','Adj Close']] = base_df[['Open','High','Low','Close','Adj Close']].round(decimals=2)

    #Changing column name on base Data frame to appropriate format
    base_df.rename(columns={'Adj Close': 'Adj_Close'},inplace= True)

    #Changing all column names to lowercase 
    base_df.columns = map(str.lower,base_df.columns)
    
    #Replacing ^NSEI to NIFTY_50 for appropriate naming convention 
    base_df['stock'] = base_df['stock'].str.replace('^NSEI','NIFTY_50')    

    #Removing NULL records because it holds discrepency data
    #base_df.drop(base_df[base_df.volume.isnull()].index,inplace= True)
    base_df = base_df.drop(base_df.columns[8:],axis=1)
    
    #base_df['volume'].fillna(0)
    print("Final Base_df",base_df)

    #Inserting stock data into SQL database using sqlachemy
    from sqlalchemy import create_engine

    username,password,host,database,driver = load_config("./db_config.ini")
    connection_string = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver={driver}"
    sql_engine = create_engine(connection_string)
    db_conn= sql_engine.connect()
    transaction = db_conn.begin()   
    
    #Managing local sqlite database for faster access on cloud
    sqlite_conn = sqlite3.connect("NSEBhavcopy.sqlite")
    
    #Defining table name on MSSQL server to locate the data
    table_name = 'stock_ohlc_data'
    
    #DB Actions to load data from Pandas Dataframe to MSSQL
    try:
        print("Insert:",base_df)
        #base_df.to_sql(table_name, db_conn, method='multi', chunksize=100,if_exists= 'append',index=True)
        base_df.to_sql(table_name, db_conn, if_exists= 'append',index= False)
        base_df.to_sql(table_name,sqlite_conn,if_exists= 'append',index= False)
        transaction.commit()
    except Exception as ex:
        print(str(ex))
    else:
        print('Stock OHLC price details data successfully inserted into MS SQL table-{}'.format(table_name))
    finally:
        db_conn.close()
        sqlite_conn.close()


begin_time = time.perf_counter()
asyncio.run(fetch())
end_time = time.perf_counter()
        
#print("INSIDE REQUEST function",end_time_normal - begin_time_normal)
print("INSIDE ASYNC function",end_time - begin_time)
print("StockList Size:",len(stock_list))
print('<<<<<STOCK_LIST_LENGTH-{}>>>>>>>'.format(stock_list_length),'<<<<<ind_url LENGTH-{}>>>>>'.format(len(all_stock_url)))        
