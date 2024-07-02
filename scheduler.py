from subprocess import call
import schedule
import time as tm
import logging
import datetime as dt

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def yfinance_scrape():
    logging.info("Running yfinance_scrape")
    call(["python","yahoofinance_daily_price_load.py"])

def stan_mark_calc():
    logging.info("Running stan_mark_calc")
    call(["python","stanweinstein_markminervini.py"])
    
def marketsmith_scrape():
    logging.info("Running marketsmith_scrape")
    call(["python","marketsmith.py"])

#[0-6 represents Monday to Sunday]
weekday = dt.date.today().weekday() 

#Calling YAHOO Finance Webscraping to run daily only on weekdays
if (weekday < 5):
    schedule.every().day.at("17:00").do(yfinance_scrape)    
    
#Calling StanWeinstein & Markminervini data imputation to run daily only on weekdays
if (weekday < 5):
    schedule.every().day.at("17:40").do(stan_mark_calc)

#Calling MarketSmith Webscraping python file to run every fortnight on second saturday
if (weekday == 5 and (weekday%2 == 0)):
    schedule.every().day.at("01:00").do(marketsmith_scrape)

while True:
    schedule.run_pending()
    tm.sleep(1)