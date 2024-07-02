/*
Created by Sivashankaran
Applying MarkMinervini conditions to pull stocks for respective day that met the criteria
Here were the conditions

Moving Averages
The current stock price is above the 50-day (10-week), 150-day (30-week) and 200-day (40-week) Simple Moving Average price line.
The 200-day moving average line is trending up for at least 1 month (preferably 4-5 months minimum in most cases).
The 50-day (10-week) moving average is above both the 150-day and 200-day moving averages.
The 150-day moving average is above the 200-day moving average.

52-week High and 52-week Low rules
The current stock price is at least 30 percent above its 52-week low (many of the best selections will be 100, 200, 300 
or even greater above their 52-week low before they emerge from a solid consolidation period and mount a large-scale advance).
The current stock price is within at least 25 percent of its 52-week high (the closer to a new high the better).

Relative Strength
The relative strength ranking is no less than 70, and preferable in the 80s or 90s, which will generally be the case with the better selections.

*/
CREATE PROCEDURE [dbo].[MARK_MINERVINI_STOCKS_STAGE2_UPTREND_FILTER] @CUR_DATE NVARCHAR(30)  
AS   
BEGIN  
  
select stock_name [Stock], format(CONVERT(date,cur_date), 'dd-MMM-yyyy') as 'Date',  
stock_open as [Open], stock_high as [High], stock_low as [Low], stock_close as [Close],volume as [Volume], [50_day_SMA] , [150_day_SMA], [200_day_SMA],   
[52_week_high], [52_week_low], closevs52week_low_pctdiff, closevs52week_high_pct_diff, RS_Score   
From markminervini_stock_daily_filter_trend_template  
WHERE      
  (stock_close > [150_day_SMA]) AND (stock_close > [200_day_SMA])      
        AND ([150_day_SMA] > [200_day_SMA])      
        AND ([50_day_SMA] > [150_day_SMA] AND [50_day_SMA] > [200_day_SMA])      
  AND (stock_close > [200_day_SMA])      
        AND (stock_close > [52_week_low] AND closevs52week_low_pctdiff > 25)      
        AND (stock_close < [52_week_high] AND closevs52week_high_pct_diff <= 25)      
  AND CONVERT(date,cur_date,105) = @CUR_DATE  
  AND RS_Score > 70  
  AND volume > 300000  
    ORDER BY cur_date DESC;      
END