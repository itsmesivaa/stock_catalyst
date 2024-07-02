#Filtering Stocks from Stocks with Momentum ie., Having Mansfield Relative Strength emerging to be leaders
import pandas as pd

#Inserting calculated Momentum data for all stocks into table for further filteration
from sqlalchemy import create_engine

#Importing DB connection function
from db_conn import load_config
import logging
import time

#StanWeinstein Data load Weekly
#Defining table name on MSSQL server to locate the data
def stan_weekly_data_load():
    logging.info("Processing - Inside Stanweinstein method, Hangon!")
    print("Processing - Inside Stanweinstein method, Hangon!")
    username,password,host,database,driver = load_config("./db_config.ini")
    connection_string = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver={driver}"
    sql_engine = create_engine(connection_string)
    db_conn= sql_engine.connect()
    
    transaction = db_conn.begin()       
    stan_table_name = 'stanweinstein_stock_with_mansfield_strength_data'
    mansfield_calc = 'EXEC [STANWEINSTEIN_RS_STOCKS_WITH_MANSFIELD_STRENGTH_CALC]'
    mans_calc = pd.read_sql_query(mansfield_calc,db_conn)
    #Inserting into table
    mans_calc.to_sql(stan_table_name, db_conn, if_exists='replace', index= False)#,method='multi', chunksize=100)
    transaction.commit()
    db_conn.close()
    print("Completed - Stanweinstein method!")


#Markminervini Daily Data load
#Defining table name on MSSQL server to locate the data

def mark_minervini():
    logging.info("Processing - Inside Mark Minervini method, Hangon!")
    print("Processing - Inside Mark Minervini method, Hangon!")
    username,password,host,database,driver = load_config("./db_config.ini")
    connection_string = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver={driver}"
    sql_engine = create_engine(connection_string)
    db_conn= sql_engine.connect()
    
    transaction = db_conn.begin()   
    mark_table_name = "markminervini_stock_daily_filter_trend_template"
    mark_minervini_sp = "EXEC MARK_MINERVINI_STOCKS_TREND_TEMPLATE_FILTER"
    minervini_results = pd.read_sql_query(mark_minervini_sp,db_conn)
    #Inserting into table
    minervini_results.to_sql(mark_table_name,db_conn,if_exists = 'replace',index = False)#,method='multi', chunksize=50)
    transaction.commit()
    db_conn.close()
    print("Completed - Mark Minervini method!")


begin_time = time.perf_counter()
stan_weekly_data_load()
end_time = time.perf_counter()
print("INSIDE stan_weekly_data_load() method:",end_time - begin_time)

begin_time = time.perf_counter()
mark_minervini()
end_time = time.perf_counter()
print("INSIDE mark_minervini() method:",end_time - begin_time)