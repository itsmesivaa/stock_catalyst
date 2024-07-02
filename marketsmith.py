# Importing the StringIO module.
from bs4 import BeautifulSoup

# Importing the StringIO module.
from io import StringIO

import pandas as pd

import re
from tqdm import tqdm

#Importing DB connection function
from db_conn import load_config

#Inserting stock data into SQL database using sqlachemy
from sqlalchemy import create_engine

import sqlite3

#Defining headers with User agent to establish requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
'X-Requested-With': 'XMLHttpRequest'
}

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
#from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


#Function to scrape Marketsmith stock fundamental data
def marketsmith_scrape(stock_list,security_codes):
    options = Options()
    options.add_argument('--headless')    
    PAGE_LOAD_TIMEOUT = 40  # seconds
    for x in tqdm(range(0,len(stock_list))):
        try:
            time.sleep(1)

            #Firefox browser webdriver
            driver = webdriver.Firefox(options=options)
            #Google chrome webdriver
            #options = webdriver.ChromeOptions()
            #driver = webdriver.Chrome(options=options)
            
            # Set timeouts
            driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

            #Base Url to fetch stock fundamental data through request
            url = "https://marketsmithindia.com/mstool/eval/{}/evaluation.jsp#/".format(stock_list[x])
            print("Firstprint",url)
            driver.get(url)
            
            # Wait for the title element to be present with a timeout of 20 seconds
            WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, 'title')))
            
            #soup = BeautifulSoup(driver.page_source, 'lxml')
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            title_text = str(soup.find('title').text.strip())
            print("SOUP TITLE TEXT:",title_text)
            
            # If the soup object for particualr stock doesn't have any stock related title, try using the security code
            if (title_text == "Stock Share Price - MarketSmith India"):  # Check if soup is empty
                time.sleep(1)
                driver.quit()
                soup.decompose()
                del(url)
                driver = webdriver.Firefox(options=options)
                # Set timeouts
                driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
                url = "https://marketsmithindia.com/mstool/eval/{}/evaluation.jsp#/".format(security_codes[x])
                print("Inside Security Code",url)
                driver.get(url)
                
                # Wait for the title element to be present with a timeout of 20 seconds
                WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, 'title')))
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                title_text_bse = str(soup.find('title').text.strip())
                print("SOUP TITLE TEXT:",title_text_bse)
                if(title_text_bse == "Stock Share Price - MarketSmith India"):
                    continue

            #Populating stock ratings data for individual stocks from Marketsmith evaluation pages
            #Populating stock ownership institution details for individual stocks from Marketsmith evaluation pages
            if x==0:
                stock_detail_header = soup.find_all('div', class_ = "details")
                
            #Stock Evalutation Ratings details-- START
            cols = []        
            #Generating columns through iteration for stock_detail_header
            
            for y in range(0,len(stock_detail_header)):
                cols.append(stock_detail_header[y].get_text().strip())
                
            #Creating a new column for stock wise segregation
            cols.insert(0,"Stock Name")
            
            print(y,"columns","--->>>",cols)
            
            #Generating row values for the stock_detail_header header 
            stock_detail_value = soup.find_all('div', class_ = "value")
            print(format(stock_list[x]),"StockDetailValue>>>",stock_detail_value) 
            

            #Feeding an empty Dataframe with its respective column values through iterating summary_data list
            #Creating an empty Dataframe to feed the column names through iterating summary_cols list

            summary_data_values = []

            #Generating columns data through iteration
            for z in range(0,len(stock_detail_value)):
                print(stock_detail_value[z].get_text().strip())
                summary_data_values.append(stock_detail_value[z].get_text().strip())
            
            #Adding stock name as a new column to every data rows for identification.
            summary_data_values.insert(0,stock_list[x])       
            
            print(z,"summary_data_values","--->>>",summary_data_values)    
            
            #Creating base data frame to hold data for every stock
            print('*****Columns******',cols,'^^^^DATA^^^^^',[summary_data_values])
            share_summary = pd.DataFrame(columns= cols,data = [summary_data_values])
            
            #Removing duplicate rows to avoid discrepency
            share_summary = share_summary.loc[:,~share_summary.columns.duplicated()]
            
            #Creating final Data frame one time and from base dataframe structure
            if x==0:
                #share_summary = pd.DataFrame(columns= cols,data = [summary_data_values])
                
                all_share_summary = pd.DataFrame(columns= share_summary.columns)
            print ("Inside x loop",x)
                
            #Adding individual stock data values through every iteration    
            print("----ABBASS----Before")
            print("Final Datatypes","<<<<<<<<->>>>>>>>",all_share_summary.dtypes)
            print("----ABBASS----After")
            all_share_summary = all_share_summary._append(share_summary,ignore_index=True)
            
            #del(stock_detail_header,stock_detail_value,share_summary)
            summary_data_values.clear()

            #Stock Evalutation Ratings details-- END              
            
            #Stock Ownership & Institution Details - START
            stock_ownership_header = soup.find('table', \
                        class_ = "table table-condensed tableDetails managementTablefont removeTableMargin", \
                        id = "chartTable")
            
            ownership_header_object = stock_ownership_header.find_all('th')
            
            #Iterating ownership headers by element to add into list for further processing
            #Not iterating all rows because last rows is NULL
            stock_ownership_institution_headers = [ head.text.strip() for head in ownership_header_object ]
            stock_ownership_institution_headers.insert(0,"Stock Name")
            print('Institution Details<<<<<<<<<<Final Stock Institution Headers',stock_ownership_institution_headers)
            
            #Skipping to next iteration as current stock doesn't have any Institution related data 
            if(stock_ownership_institution_headers[2] == 'No Data'):
                continue
            
            #Filtering data contents for stock ownership details
            stock_ownership_header_rows = stock_ownership_header.find_all('tr')

            
            #Creating final Data frame for quarterly earnings one time and from base dataframe structure
            if x==0:
                df_temp_ownership = pd.DataFrame(columns= stock_ownership_institution_headers)
                
                print("DF OwnershipColumns",df_temp_ownership.columns)
                
                all_stocks_institutional_ownership = pd.DataFrame(columns= df_temp_ownership)
                
            print("Institution Ownership Dataframe",all_stocks_institutional_ownership)
            
            #Iterating individual quarterly earnings table row by element to add into list for further processing
            stock_institution_data = []
            for data in stock_ownership_header_rows:
                owner_row_data = data.find_all('td')
                institution_data = [ q_data.text.strip() for q_data in owner_row_data]
                #Adding Stock name to institution rows manually
                institution_data.insert(0,stock_list[x])
                
                #Adding iterated institution data to list for a stock on their respective iteration of different quarters
                stock_institution_data.append(institution_data)
                
                print("Stock Institution Data","<<<<<>>>>>>",institution_data)
            #Removing NULL data from the list            
            stock_institution_data.pop(0)
            print("Final List",">><><><><><><><><>",stock_institution_data)
            
            #Appending institution details rows stock specifically to dataframe 
            df_temp_ownership = pd.DataFrame(columns= stock_ownership_institution_headers, data = stock_institution_data)
            all_stocks_institutional_ownership = all_stocks_institutional_ownership._append(df_temp_ownership, ignore_index=True)
            
            print("******Before Institutional Data for each Stocks Dataframe:******",all_stocks_institutional_ownership)
            # melt the dataframe to create a single column for Quarter
            all_stocks_institutional_ownership_melted = pd.melt(all_stocks_institutional_ownership, id_vars=['Stock Name', 'Owner Name'], \
                                                        var_name='Quarter', value_name='Percentage')

            # pivot the dataframe to create separate columns for each Owner Name
            all_stocks_institutional_ownership_pivoted = all_stocks_institutional_ownership_melted.pivot_table(index=['Stock Name', 'Quarter'], \
                                                        columns='Owner Name', values='Percentage', aggfunc='first').reset_index()
            #Handling NAN values with zero
            all_stocks_institutional_ownership_pivoted.fillna(0)
            #Making Column name to Lowercase for further processing to meet standard
            all_stocks_institutional_ownership_pivoted.columns.str.lower()
            #Appropriate naming convention format for column names
            all_stocks_institutional_ownership_pivoted.columns = all_stocks_institutional_ownership_pivoted.columns.str.\
                                                                replace(' ','', regex=True)
            
            print("******After Institutional Data for each Stocks Dataframe:******", all_stocks_institutional_ownership_pivoted)
            #Stock Ownership & Institution Details - END
            
            
            #Quarterly earnings header objects-- START
                            
            stock_detail_quarterly_headers = soup.find('table', class_ = "table table-condensed tableDetails", id = "formattedSalesAndEarningTable")
            #Filtering table header content from soup objects - stock_detail_quarterly_headers
            quarterly_objects = stock_detail_quarterly_headers.find_all('th')

            #Iterating quarterly earnings headers by element to add into list for further processing
            #Not iterating all rows because last rows is NULL
            stock_quarterly_earnings_headers = [ title.text.strip() for title in quarterly_objects ]
            stock_quarterly_earnings_headers.insert(0,"Stock Name")
            #Removing last empty values in list on every iteration
            stock_quarterly_earnings_headers.pop()
            
            
            print("Final Quarterly Earnings Headers","<<<<<>>>>>>",stock_quarterly_earnings_headers)
            
            print(format(stock_list[x]),"StockDetailHeader>>>" ,stock_detail_header)
            #print(format(lst[x]),"QuarterlyEPSHeaders>>>" , stock_detail_quarterly_headers)
            print(stock_detail_quarterly_headers.find_all('th'))
            
            #Filtering data contents for Quarterly earnings
            quarterly_objects_data = stock_detail_quarterly_headers.find_all('tr')

            #Creating final Data frame for quarterly earnings one time and from base dataframe structure
            if x==0:
                quarterly_earnings = pd.DataFrame(columns= stock_quarterly_earnings_headers)
                #quarterly_earnings.rename(columns= {'Stock Name': 'stock_name', 'Date(Transcript )': 'date' , 'EPS' : 'eps', \
                #                                    '%Chg': 'eps_chg_pct', 'Sales(Cr)' : 'sales_in_cr', '%Chg': 'sales_chg_pct'},inplace= True)
                print("Columns",quarterly_earnings.columns)
                
                quarterly_earnings_all_stocks = pd.DataFrame(columns= stock_quarterly_earnings_headers)
                
            print("earnings dataframe",stock_quarterly_earnings_headers)
            
            #Iterating individual quarterly earnings table row by element to add into list for further processing
            quarterly_earnings_lst = []
            for rows in quarterly_objects_data:
                row_data = rows.find_all('td')
                ind_quarterly_earnings_rows = [ q_data.text.strip() for q_data in row_data]
                #Adding Stock name to quarterly earnings rows manually
                ind_quarterly_earnings_rows.insert(0,stock_list[x])
                #Removing last empty values in list on every iteration
                ind_quarterly_earnings_rows.pop()
                
                #Adding iterated quarterly earnings data to list for a stock on their respective iteration of different quarters
                quarterly_earnings_lst.append(ind_quarterly_earnings_rows)
                
                print("ind_quarterly_earnings_rows_Final Quarterly Earnings Headers rows","<<<<<>>>>>>",ind_quarterly_earnings_rows)
            quarterly_earnings_lst.pop(0)
            print("Final List",">><><><><><><><><>",quarterly_earnings_lst)
            
            #Checking if all data values were empty for a respective stock in Marketsmith by 
            #moving list into set to avoid duplicate values and see if it matches '' empty value
            
            #Appending quarterly earnings rows stock specifically to dataframe 
            quarterly_earnings = pd.DataFrame(columns= stock_quarterly_earnings_headers, data = quarterly_earnings_lst)
            quarterly_earnings_all_stocks = quarterly_earnings_all_stocks._append(quarterly_earnings, ignore_index=True)
            
        
            print("QuarterlyEarnings Dataframe:",quarterly_earnings)
            
            #Quarterly earnings header objects-- END
            
        except (Exception, ValueError) as e:
            print(f"Error encountered for stock {stock_list[x]}: {e}")
            # Optional: Log the error for further analysis

        finally:
            # Close the driver and soup object in any case
            soup.decompose()
            driver.quit()

            print(f"Finished processing stock- {stock_list[x]}")


    #Data cleaning - Dropping unncessary columns
    #all_share_summary.drop(list(all_share_summary)[0:23], axis=1,inplace = True)
    
    #Renaming column name for SQL table processing
    
    all_share_summary.rename(columns = {'Stock Name' : 'stock_name', 'Market Capitalization' : 'market_capitalization', 'Sales' : 'sales', \
        'Shares in Float' : 'shares_in_float', 'No of Funds' : 'no_of_funds', 'Shares held by Funds' : 'shares_held_by_funds', 'Yield' : 'yield', \
        'Book Value' : 'book_value', 'U/D Vol Ratio' : 'u/d_vol_ratio', 'LTDebt/Equity' : 'ltdebt_equity', 'Alpha' : 'alpha', 'Beta' : 'beta', \
        'Master Score' : 'master_score', 'EPS Rating' : 'eps_rating', 'Price Strength' : 'price_strength', 'Acc/Dis Rating' : 'buyers_demand', \
        'Group Rank' : 'group_rank_out_of_197', 'EPS Growth Rate' : 'eps_growth_rate', 'Earnings Stability' : 'earnings_stability', 'P/E Ratio' : 'pe_ratio', \
        '5-Year P/E Range' : '5years_pe_range', 'Return on Equity' : 'return_on_equity', 'Cash Flow (INR)': 'cash_flow'},
        inplace = True)
    
    
    #Slicing EPS % Chg and Sales % Chg column as it is having same name so we have issue on processing it individually
    #To overcome that we have renamed that to map EPS & Sales % change meaningfully 
    eps_pct = quarterly_earnings_all_stocks.iloc[:,3]
    eps_pct.columns = ['eps_change_pct']
    print("Sliced w dataframe:",eps_pct.columns)
    print("Sliced w dataframe:",eps_pct)
    
    sales_pct = quarterly_earnings_all_stocks.iloc[:,5]
    sales_pct.columns = ['sales_change_pct']
    print("Sliced w dataframe:",sales_pct.columns)
    print("Sliced w dataframe:",sales_pct)
    
    #Dropping existing %Change column we will add later below with appropriate
    quarterly_earnings_all_stocks = quarterly_earnings_all_stocks.drop(columns = ['%Chg'])
       
    #Adding back the columns for EPS%Change and Sales%Change with right column names    
    quarterly_earnings_all_stocks['EPS_Change_%'] = eps_pct
    quarterly_earnings_all_stocks['SALES_Change_%'] = sales_pct

    
    #Reindexing dataframe in appropriate order
    quarterly_earnings_all_stocks = quarterly_earnings_all_stocks.reindex(['Stock Name','Date(Transcript )',\
                                    'EPS','EPS_Change_%','Sales(Cr)','SALES_Change_%'], axis = 1)
    
    #Renaming the column names appropriately in final dataframe before inserting into SQL
    quarterly_earnings_all_stocks.rename(columns={'Stock Name':'stock_name', 'Date(Transcript )': 'quarter', 'EPS':'eps', 'EPS_Change_%':'eps_pct_chg',\
                                                  'Sales(Cr)':'sales_in_cr', 'SALES_Change_%': 'sales_pct_chg'}, inplace= True)

    print("Final EPS DF columns","##########",quarterly_earnings_all_stocks.columns)
    print("Final EPS Dataframe","##########",quarterly_earnings_all_stocks)
    
    #Removing INR string (text cleansing) from few columns to fit into perfect standard for data manipulation
    
    all_share_summary.market_capitalization = all_share_summary.market_capitalization.str.replace('INR ','')
    all_share_summary.sales = all_share_summary.sales.str.replace('INR ','')
    all_share_summary.group_rank_out_of_197 = all_share_summary.group_rank_out_of_197.str.replace(' of 197','')

    #Changing datatypes for the columns to their appropriate type for further processing
    all_share_summary.master_score = all_share_summary.master_score.str.replace('N/A','0').astype('Int32')
    all_share_summary.eps_rating = all_share_summary.eps_rating.str.replace('N/A','0').astype('Int32')
    all_share_summary.price_strength = all_share_summary.price_strength.str.replace('N/A','0').astype('Int32')
    all_share_summary.earnings_stability = all_share_summary.earnings_stability.str.replace('N/A','0').astype('Int32')
    all_share_summary.pe_ratio = all_share_summary.pe_ratio.str.replace('N/A','0').astype('Int32')
    all_share_summary.group_rank_out_of_197 = all_share_summary.group_rank_out_of_197.str.replace('N/A','0').astype('Int32')
    
    all_share_summary.fillna('')
    quarterly_earnings_all_stocks.fillna('')
    all_stocks_institutional_ownership_pivoted.fillna('')
    
    return all_share_summary,quarterly_earnings_all_stocks,all_stocks_institutional_ownership_pivoted


#Reading CSV file to fetch all listed Stocks from NSE website

all_equity_lst = pd.read_excel("./Equity.xlsx", sheet_name='Equity_detail', dtype=str)

stock_list = []

# Create a list of symbols and security codes
stock_list = all_equity_lst['SYMBOL'].tolist()
security_codes = all_equity_lst['Security_Code'].tolist()
del stock_list[0]
del security_codes[0]

#for ind_stock in all_equity_lst['SYMBOL']:
    #stock_list.append(ind_stock)

print("Inside Stock_List:",stock_list,security_codes)

#Calling the function to download the data
all_share_summary,quarterly_earnings_all_stocks,all_stocks_institutional_ownership_pivoted = marketsmith_scrape(stock_list,security_codes)

#Deleting duplicate entries if exists during exception retry happens to keep data clean without any noise after loading process is done
del_dup_stock_eval = "DELETE DUP FROM (SELECT ROW_NUMBER() OVER (PARTITION BY stock_name ORDER BY stock_name) AS Val, * \
                    FROM market_smith_stock_eval) DUP WHERE DUP.Val > 1"

del_dup_quarterly_earnings = "DELETE DUP FROM (SELECT ROW_NUMBER() OVER (PARTITION BY stock_name,quarter ORDER BY stock_name) AS Val, * \
                    FROM all_stocks_quarterly_earnings) DUP WHERE DUP.Val > 1"

del_dup_institutional_data = "DELETE DUP FROM (SELECT ROW_NUMBER() OVER (PARTITION BY StockName, [quarter] ORDER BY StockName) AS Val, *  \
                    FROM market_smith_stock_institutional_data) DUP WHERE DUP.Val > 1"

#Establishing local Sqlite DB for faster data access on Streamlit UI page..
sqlite_conn = sqlite3.connect("NSEBhavcopy.sqlite")

username,password,host,database,driver = load_config("./db_config.ini")
connection_string = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver={driver}"
sql_engine = create_engine(connection_string)

db_conn= sql_engine.connect()
transaction = db_conn.begin()   

#Defining table name on MSSQL server to locate the data
stock_eval_table_name = "market_smith_stock_eval"
all_stocks_quarterly_earnings = "all_stocks_quarterly_earnings"
stock_institutional_ownership_data = "market_smith_stock_institutional_data"
#DB Actions to load data from Pandas Dataframe to MSSQL
try:
    all_share_summary.to_sql(stock_eval_table_name, db_conn, if_exists= 'replace',index= False)
    quarterly_earnings_all_stocks.to_sql(all_stocks_quarterly_earnings, db_conn, if_exists= 'replace',index= False)
    all_stocks_institutional_ownership_pivoted.to_sql(stock_institutional_ownership_data,db_conn, if_exists= 'replace',index = False)

    #Loading same data into local Sqlite DB for faster data access on streamlit UI page..
    all_share_summary.to_sql(stock_eval_table_name, sqlite_conn, if_exists= 'replace',index= False)
    quarterly_earnings_all_stocks.to_sql(all_stocks_quarterly_earnings, sqlite_conn, if_exists= 'replace',index= False)
    all_stocks_institutional_ownership_pivoted.to_sql(stock_institutional_ownership_data, sqlite_conn, if_exists= 'replace',index = False)
    
except Exception as ex:
    print(str(ex))
else:
    print('MarketSmith details were Scraped & inserted successfully inserted into MS SQL tables-{},{} and {}'.format(stock_eval_table_name,all_stocks_quarterly_earnings,stock_institutional_ownership_data))
finally:
    transaction.commit() 
    db_conn.close()
    sqlite_conn.close()
