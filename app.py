from datetime import datetime
import pandas as pd
import streamlit as st
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from sqlalchemy import create_engine
#Importing DB connection function
from db_conn import load_config
import sqlite3


# Set page layout to wide
st.set_page_config(layout='wide')
pages = st.tabs(['🏛️HomePage','💰StanWeinstein','📈Mark Minervini','💎CANSLIM','👀Stock Analyzer','🌱Triple Digit Growth'])


# Establish connection to the database
username,password,host,database,driver = load_config("./db_config.ini")
connection_string = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver={driver}"
sql_engine = create_engine(connection_string)

# Function to load stock names from the database
@st.cache_data
def load_stock_names():
    try:
        db_conn = sql_engine.connect()
        #sqlite_conn = sqlite3.connect("NSEBhavcopy.sqlite")
        #Execute SQL query to get distinct stock names
        stocks_name_query = "SELECT DISTINCT [stock] as STOCK FROM stock_ohlc_data ORDER BY stock ASC"
        stocks_df = pd.read_sql_query(stocks_name_query, db_conn)
        
        # Close the database connection
        db_conn.close()
        
        # Extract stock names from the DataFrame
        stocks_lst = stocks_df['STOCK'].tolist()
        return stocks_lst
            
    except Exception as e:
        print(str(e)) 


# Function to load stock prices from the database
@st.cache_data
def load_stock_prices():
    try:
        #Connecting Local Sqlite database
        db_conn = sql_engine.connect()
        #sqlite_conn = sqlite3.connect("NSEBhavcopy.sqlite")
        # Execute SQL query to get stock prices
        stock_price_query = "SELECT [stock], [date], [open], [high], [low], [close], [volume] FROM stock_ohlc_data \
                            WHERE [date] BETWEEN DATEADD(month, -3, GETDATE()) AND GETDATE() ORDER BY [stock],[date];"
        stock_price_det = pd.read_sql_query(stock_price_query, db_conn)
        # Close the database connection
        db_conn.close()
        return stock_price_det
    
    except Exception as e:
        print(str(e))
        return pd.DataFrame()


# Function to load Quarterly earnings detils from the database
@st.cache_data
def load_all_stocks_quarterly_earnings():
    try:
        #Connecting Local Sqlite database
        db_conn = sql_engine.connect()
        #sqlite_conn = sqlite3.connect("NSEBhavcopy.sqlite")
        #Execute SQL query to get earnings details
        stock_earnings_details_query = "SELECT stock_name AS 'STOCK', quarter AS 'QUARTER', eps AS 'EPS', eps_pct_chg AS 'EPS_%CHG', sales_in_cr AS 'SALES_IN_CRORE',\
                                sales_pct_chg AS 'SALES_%CHG' FROM all_stocks_quarterly_earnings"
        stock_earnings_details = pd.read_sql_query(stock_earnings_details_query,db_conn)
        db_conn.close()
        
        return stock_earnings_details
    
    except Exception as e:
        print(str(e))
        return pd.DataFrame()    

# Function to load Institution details from the database
@st.cache_data
def load_all_stocks_institution_details():
    try:
        #Connecting Local Sqlite database
        db_conn = sql_engine.connect()
        #sqlite_conn = sqlite3.connect("NSEBhavcopy.sqlite")
        #Execute SQL query to get earnings details
        stock_institution_data_query = "SELECT StockName AS 'STOCK', Quarter AS 'QUARTERS',[FinancialInstitutions/Banks] AS 'FinancialInstitutions&Banks', \
                                    ForeignPortfolioInvestors ,IndividualInvestors,InsuranceCompanies,MutualFunds,Others,Promoters \
                                    FROM market_smith_stock_institutional_data"
        stock_institution_data = pd.read_sql_query(stock_institution_data_query,db_conn)                                    
        db_conn.close()
        return stock_institution_data
    
    except Exception as e:
        print(str(e))
        return pd.DataFrame()
    
# Function to load all stock evaluation summary details from the database
@st.cache_data
def load_all_stocks_summary_evaluation_data():
    try:
        #Connecting Local Sqlite database
        db_conn = sql_engine.connect()
        #sqlite_conn = sqlite3.connect("NSEBhavcopy.sqlite")
        #Execute SQL query to get stock evaluation metrics data
        stock_eval_summary_query = "select stock_name as 'STOCK', market_capitalization 'Market_Capitalization',sales 'Sales',shares_in_float 'Shares_in_Float',\
                            no_of_funds 'No_of_Funds',shares_held_by_funds 'Shares_held_by_Funds',master_score 'Master_Score',eps_rating 'EPS_Rating',\
                            price_strength 'Price_Strength',buyers_demand 'Buyers_Demand',group_rank_out_of_197 'Group_Rank/197',pe_ratio 'PE_Ratio',\
                            return_on_equity 'Return_on_Equity',cash_flow 'Cash_Flow',book_value 'Book_Value' From market_smith_stock_eval"        
        stock_eval_summary = pd.read_sql_query(stock_eval_summary_query,db_conn)
        db_conn.close()
        return stock_eval_summary
    
    except Exception as e:
        print(str(e))
        return pd.DataFrame()   

# Load stock names and prices only once when the app launches
stocks_lst = load_stock_names()
stock_price_det = load_stock_prices()
stock_earnings_details = load_all_stocks_quarterly_earnings()
stock_institution_data = load_all_stocks_institution_details()
stock_eval_summary = load_all_stocks_summary_evaluation_data()


def stanweinstein_results(current_week,range_value):
    try:
        #Connecting AWS RDS database as we can't execute SP's inside local database
        db_conn= sql_engine.connect()
        stanweinstein_criteria_match = "EXEC [STANWEINSTEIN_RS_STOCKS_WITH_MOMENTUM_START] @FROM_DATE = '" + str(current_week) + "', @RANGE_VALUE =" + str(range_value)
        stanweinstein_stocks = pd.read_sql_query(stanweinstein_criteria_match,db_conn)
        db_conn.close()
        return stanweinstein_stocks
    except Exception as e:
        return str(e)

def markminervini(current_day):
    try:
        #Connecting AWS RDS database as we can't execute SP's inside local database
        db_conn= sql_engine.connect()
        markminervini_sp_results = "EXEC [MARK_MINERVINI_STOCKS_STAGE2_UPTREND_FILTER] @CUR_DATE =  '" + str(current_day) + "' "
        markminervini_final_stocks = pd.read_sql_query(markminervini_sp_results,db_conn)
        db_conn.close()
        return markminervini_final_stocks
    except Exception as e:
        return str(e)
    
@st.cache_data
def canslim():
    try:
        #Connecting AWS RDS database as we can't execute SP's inside local database
        db_conn= sql_engine.connect()
        canslim_results = "EXEC [CANSLIM_STOCKS_EPS_STRENGTH_FILTER]"
        canslim_stocks = pd.read_sql_query(canslim_results,db_conn)
        db_conn.close()
        return canslim_stocks
    except Exception as e:
        return str(e)

@st.cache_data
def triple_digit_growth():
    try:
        #Connecting AWS RDS database as we can't execute SP's inside local database
        db_conn= sql_engine.connect()
        triple_digit_results = "EXEC [TRIPLE_DIGIT_GROWTH_EPS_SALES_STOCKS]"
        triple_digit_stocks = pd.read_sql_query(triple_digit_results,db_conn)
        db_conn.close()
        return triple_digit_stocks
    except Exception as e:
        return str(e)

# Function to plot stock price
def plot_stock_price(stock_price_det):
    try:

        # Create a subplot figure with two rows (price on top, volume below)
        fig = make_subplots(rows=2, shared_xaxes=True,row_heights=[800, 300])
        
        # Candlestick trace for price (top row)
        candle_trace = go.Candlestick(
            x=stock_price_det['date'],
            open=stock_price_det['open'],
            high=stock_price_det['high'],
            low=stock_price_det['low'],
            close=stock_price_det['close'],
            name='Candlestick',
            increasing_line_color='#30D5C8',  # Customize candle colors
            decreasing_line_color='#eb2d3a'
        )
        fig.append_trace(candle_trace, row=1, col=1)
        
        # Secondary y-axis for volume (bottom row)
        max_volume = max(stock_price_det['volume'])
        volume_trace = go.Bar(
            x=stock_price_det['date'],
            y=stock_price_det['volume'],
            name='Volume',
            marker=dict(color='royalblue'),
            opacity=0.7,
            base=0,showlegend= False
        )
        fig.append_trace(volume_trace, row=2, col=1)
        
        # Set y-axis titles and ranges
        fig.update_yaxes(title="Price", range=[min(stock_price_det['close']), max(stock_price_det['close'])], showgrid=True, zeroline=False, row=1, side='right')
        fig.update_yaxes(title="Volume", range=[0, (max_volume + max_volume * 0.3)], showgrid=True, zeroline=False, row=2, side='right')

        # Adjust layout for subplots
        fig.update_layout(width = 600,height=700)

        # Show the combined chart with separate axes
        st.plotly_chart(fig)

    except Exception as e:
        print(str(e))

# Streamlit pages
with pages[0]:
    st.header("StockCatalyst - Powerup⚡ Your Investments!")
    st.write("StockCatalyst is a comprehensive web application designed to empower investors by integrating proven \
            stock trading methodologies from renowned market experts such as Stan Weinstein, Mark Minervini, and William O'Neil. \
            The project aimed to automate and streamline stock analysis processes, enhancing efficiency and accuracy in identifying \
            high-growth potential stocks.")
    book_image = "./BookCover.png"
    process_flow = "./StockCatalyst_workflow.mp4"
    readme = "./README.md"
    st.image(book_image,width=800,caption='Personal Finance Investments')
    st.header("StockCatalyst Workflow:")
    st.video(process_flow,autoplay=True,loop=True)
    with open(readme,'r') as f:
        st.markdown(f.read(),unsafe_allow_html=True)
    
with pages[1]:
    st.header("⚡StanWeinstein Relative Strength Stocks", divider='rainbow')
    current_week = st.date_input(label='For Date').strftime("%d/%m/%Y")
    range_value = st.slider("Select Relative Strength Range to Filter", min_value=1, max_value=100, value=None, 
                        step=1, format=None, key=None, help=None, on_change=None, args=None,disabled=False, label_visibility="visible")
    st.write("Below were the stocks emerging from {}-week Consolidation range with Momentum (Stage-1) by breaking 30WeekSMA".format(range_value))
    results = stanweinstein_results(current_week, range_value)
            
    if results.empty:
        st.write("No results found")
    else:
        st.dataframe(results, width=3000, height=800)

with pages[2]:
    st.header("🌊Mark Minervini Stage 2 Stocks", divider= "rainbow")
    current_day = st.date_input(label='Select a Date').strftime("%Y/%m/%d")
    st.write("Below were the stocks at Stage 2 possible emerging leaders")
    results = markminervini(current_day)
    
    if results.empty:
        st.write("No results found")
    else:
        st.dataframe(results,width=3000, height=1500)

with pages[3]:
    st.header("🔎CANSLIM filtered Stocks - William O'Neil", divider= "rainbow")
    st.write("Stocks met William O'Neil Condition")
    results = canslim()
    
    if results.empty:
        st.write("No results found")
    else:
        st.dataframe(results,width=3000, height=1000)
    
with pages[4]:
    
    stock_selected = st.selectbox("Select a Stock", stocks_lst)
    #Filtering from entire dataframe to selected stock
    stock_price_selected = stock_price_det.loc[stock_price_det['stock'] == stock_selected]
    current_stock_earnings_detail = stock_earnings_details.loc[stock_earnings_details['STOCK'] == stock_selected]
    stock_institution_data_detail = stock_institution_data.loc[stock_institution_data['STOCK'] == stock_selected]
    stock_eval_summary_detail = stock_eval_summary.loc[stock_eval_summary['STOCK'] == stock_selected]
    
    st.header("📰Stock Summary Details",divider='orange')
  
    acol1, acol2,a_col3, a_col4, a_col5 = st.columns(5)
    
    if stock_eval_summary_detail.empty:
        st.write("No Data Found")
    
    else:
        with acol1:
            st.metric(label="Market Capitalization",value=stock_eval_summary_detail['Market_Capitalization'].iloc[0])

        with acol2:
            st.metric(label="Sales",value=stock_eval_summary_detail['Sales'].iloc[0])

        with a_col3:
            st.metric(label="Shares in Float",value=stock_eval_summary_detail['Shares_in_Float'].iloc[0])

        with a_col4:
            st.metric(label="No of Funds",value=stock_eval_summary_detail['No_of_Funds'].iloc[0])    

        with a_col5:
            st.metric(label="Shares held by Funds",value=stock_eval_summary_detail['Shares_held_by_Funds'].iloc[0])   
        
        b_col1, b_col2, b_col3, b_col4, b_col5 = st.columns(5,gap="small")
        
        with b_col1:
            st.metric(label="Master Score",value=stock_eval_summary_detail['Master_Score'].iloc[0])
            
        with b_col2:
            st.metric(label="EPS Rating",value=stock_eval_summary_detail['EPS_Rating'].iloc[0])
    
        with b_col3:
            st.metric(label="Price Strength",value=stock_eval_summary_detail['Price_Strength'].iloc[0])     
            
        with b_col4:
            st.metric(label="Buyers Demand",value=stock_eval_summary_detail['Buyers_Demand'].iloc[0])    
            
        with b_col5:
            st.metric(label="Group Rank Out of 197",value=stock_eval_summary_detail['Group_Rank/197'].iloc[0])
        
        c_col1,c_col2,c_col3,c_col4,c_col5 = st.columns(5,gap="small")   
        
        with c_col1:
            st.metric(label="PE Ratio",value=stock_eval_summary_detail['PE_Ratio'].iloc[0])            
        
        with c_col2:
            st.metric(label="Return on Equity",value= stock_eval_summary_detail['Return_on_Equity'].iloc[0])    
        
        with c_col3:
            st.metric(label="Cash Flow",value= stock_eval_summary_detail['Cash_Flow'].iloc[0])
            
        with c_col4:
            st.metric(label='Book Value',value= stock_eval_summary_detail['Book_Value'].iloc[0])
    
    chart_col1, holdings_quarterly_detcol2 = st.columns([1.8,3])

    with chart_col1:
        st.header("🗠Stock Price Chart & Volume📊",divider="blue")
        plot_stock_price(stock_price_selected)
        
    with holdings_quarterly_detcol2:
        st.header("🏦Institution Ownership Pattern",divider="rainbow")
        if stock_institution_data_detail.empty:
            st.write("No Data Found")
        else:
            st.dataframe(stock_institution_data_detail,width = 1200,hide_index=True)

        st.header("💸Quarterly Earnings",divider="rainbow")
        if current_stock_earnings_detail.empty:
            st.write("No Data Found")
        else:
            st.dataframe(current_stock_earnings_detail, width = 650,hide_index= True)

with pages[5]:
    st.header("Stocks with Triple Digit Growth on EPS & Sales - William O'Neil", divider= "rainbow")
    st.write(f"Fundamentally Strong stocks with Great EPS and Sales growth greater than 100% Current vs Previous Period")
    results = canslim()
    
    if results.empty:
        st.write("No results found")
    else:
        st.dataframe(results,width=3000, height=1000)
    