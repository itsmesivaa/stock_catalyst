/*
CREATED BY Sivashankaran 
This procedure converts daily prices to Weekly prices and calculates Mansfield Relative Strength rating
for every weekly close price along with six week price range and relative strength percentage difference 
of previous period range

*/
USE NSEBhavcopy
GO


									

CREATE PROCEDURE [dbo].[STANWEINSTEIN_RS_STOCKS_WITH_MANSFIELD_STRENGTH_CALC]
AS
BEGIN
	--Deleting duplicate entries for stocks in stock daily price table and marketsmith table

	DELETE DUP FROM (SELECT ROW_NUMBER() OVER (PARTITION BY stock,[date] ORDER BY [date]) as Val from stock_ohlc_data) DUP
	WHERE Val > 1

	DELETE DUP FROM (SELECT ROW_NUMBER() OVER (PARTITION BY stock_name ORDER BY stock_name) AS Val, * \
						FROM market_smith_stock_eval) DUP WHERE DUP.Val > 1
	DELETE DUP FROM (SELECT ROW_NUMBER() OVER (PARTITION BY stock_name,quarter ORDER BY stock_name) AS Val, * \
						FROM all_stocks_quarterly_earnings) DUP WHERE DUP.Val > 1
	DELETE DUP FROM (SELECT ROW_NUMBER() OVER (PARTITION BY StockName, [quarter] ORDER BY StockName) AS Val, *  \
						FROM market_smith_stock_institutional_data) DUP WHERE DUP.Val > 1	

	--Converting Daily historical prices to Weekly prices for Listed Stocks        
	WITH weekly_price_closing_calc_CTE
	AS (
		SELECT DISTINCT stock
			,max(convert(INT, datepart(year, [date]))) AS current_year
			,max(convert(INT, datepart(WEEK, [date]))) AS current_week_of_year
			,first_value([open]) OVER (
				PARTITION BY stock
				,datepart(year, [date])
				,datepart(WEEK, [date]) ORDER BY datepart(weekday, [date])
				) AS week_open
			,max(high) OVER (
				PARTITION BY stock
				,datepart(year, [date])
				,datepart(WEEK, [date])
				) AS week_high
			,min(low) OVER (
				PARTITION BY stock
				,datepart(year, [date])
				,datepart(WEEK, [date])
				) AS week_low
			,first_value([close]) OVER (
				PARTITION BY stock
				,datepart(year, [date])
				,datepart(WEEK, [date]) ORDER BY datepart(weekday, [date]) DESC
				) AS week_close
		FROM stock_ohlc_data
		GROUP BY stock
			,[date]
			,[high]
			,[low]
			,[open]
			,[close]
		)
		,
		--Calculating STAN WEINSTEIN Investing method based on Weeking Closing Average and 30 Weekly Simple moving average of Stock and its Closing Price difference with Moving Average        
	STANWEINSTEIN_3OWEEKSMA_PCT_DIFF_CALC
	AS (
		SELECT a.stock
			,cast(FORMAT(DATEADD(day, (7 * (a.current_week_of_year - DATEPART(week, str(a.current_year)))) + 2 - DATEPART(weekday, str(a.current_year)), str(a.current_year)), 'dd-MMM-yy') AS DATE) AS WeekFirstDay
			,a.week_open
			,a.week_high
			,a.week_low
			,a.week_close
			,round(avg(a.week_close) OVER (
					PARTITION BY a.stock ORDER BY a.current_year
						,a.current_week_of_year ROWS BETWEEN 29 PRECEDING
							AND CURRENT row
					), 2) AS [30_week_SMA]
			,(
				a.week_close - round(avg(a.week_close) OVER (
						PARTITION BY a.stock ORDER BY a.current_year
							,a.current_week_of_year ROWS BETWEEN 29 PRECEDING
								AND CURRENT row
						), 2)
				) / round(avg(NULLIF(a.week_close, 0)) OVER (
					PARTITION BY a.stock ORDER BY a.current_year
						,a.current_week_of_year ROWS BETWEEN 29 PRECEDING
							AND CURRENT row
					), 2) * 100 AS [30WeekMA_pct_diff]
			,b.week_open AS 'NIFTY_OPEN'
			,b.week_high AS 'NIFTY_HIGH'
			,b.week_low AS 'NIFTY_LOW'
			,b.week_close AS 'NIFTY_CLOSE'
		FROM weekly_price_closing_calc_CTE a
		JOIN weekly_price_closing_calc_CTE b ON a.current_year = b.current_year
			AND a.current_week_of_year = b.current_week_of_year
			AND b.stock = 'NIFTY_50'
		)
		--Six weeks price range     
		,TIGHTRANGEPCT_CALC
	AS (
		SELECT stock
			,WeekFirstDay
			,ROW_NUMBER() OVER (
				PARTITION BY stock ORDER BY WeekFirstDay
				) AS ROWNUMBERWEEK
			,week_open
			,week_high
			,week_low
			,week_close
			,[30_week_SMA]
			,[30WeekMA_pct_diff]
			,max(week_high) OVER (
				PARTITION BY stock ORDER BY WeekFirstDay rows BETWEEN 6 preceding
						AND CURRENT row
				) AS [MAXWEEKHIGH6WEEKS]
			,min(week_low) OVER (
				PARTITION BY stock ORDER BY WeekFirstDay rows BETWEEN 6 preceding
						AND CURRENT row
				) AS [MINWEEKLOW6WEEKS]
			,round((
					max(week_high) OVER (
						PARTITION BY stock ORDER BY WeekFirstDay rows BETWEEN 6 preceding
								AND CURRENT row
						) - min(week_low) OVER (
						PARTITION BY stock ORDER BY WeekFirstDay rows BETWEEN 6 preceding
								AND CURRENT row
						)
					) / min(NULLIF(week_low, 0)) OVER (
					PARTITION BY stock ORDER BY WeekFirstDay rows BETWEEN 6 preceding
							AND CURRENT row
					) * 100, 2) AS [6WEEKSPCTRANGE]
			,NIFTY_OPEN
			,NIFTY_HIGH
			,NIFTY_LOW
			,NIFTY_CLOSE
		FROM STANWEINSTEIN_3OWEEKSMA_PCT_DIFF_CALC
		)
		--Sixweek average price range percentage calculation      
		,SIXWEEKS_AVGRANGEPCT_CALC
	AS (
		SELECT stock
			,WeekFirstDay
			,ROWNUMBERWEEK
			,week_open
			,week_high
			,week_low
			,week_close
			,[30_week_SMA]
			,[30WeekMA_pct_diff]
			,[MAXWEEKHIGH6WEEKS]
			,[MINWEEKLOW6WEEKS]
			,[6WEEKSPCTRANGE]
			,ROUND(AVG([6WEEKSPCTRANGE]) OVER (
					PARTITION BY STOCK ORDER BY WeekFirstDay ROWS BETWEEN 6 PRECEDING
							AND CURRENT ROW
					), 2) AS SIXWEEKSAVGPCTRANGE
			,NIFTY_OPEN
			,NIFTY_HIGH
			,NIFTY_LOW
			,NIFTY_CLOSE
			,(week_close / NULLIF(NIFTY_CLOSE, 0)) * 100 AS RP
			,avg((week_close / NULLIF(NIFTY_CLOSE, 0)) * 100) OVER (
				PARTITION BY STOCK ORDER BY WeekFirstDay ROWS BETWEEN 51 PRECEDING
						AND CURRENT ROW
				) AS RP_SMA
		FROM TIGHTRANGEPCT_CALC
		)
		--Mansfield Relative Strength Calculation for weekly closing price
		,MANSFIELD_RS_CALC
	AS (
		SELECT a.STOCK
			,ROWNUMBERWEEK AS 'WEEKNUMBER'
			,WeekFirstDay
			,a.week_open
			,a.week_high
			,a.week_low
			,a.week_close
			,a.[30_week_SMA]
			,a.[30WeekMA_pct_diff]
			,a.[6WEEKSPCTRANGE]
			,ROUND(AVG(a.[6WEEKSPCTRANGE]) OVER (
					PARTITION BY a.STOCK ORDER BY WeekFirstDay ROWS BETWEEN 7 PRECEDING
							AND CURRENT ROW
					), 2) AS SIXWEEKSAVGPCTRANGE
			,NIFTY_OPEN
			,NIFTY_HIGH
			,NIFTY_LOW
			,NIFTY_CLOSE
			,RP
			,RP_SMA
			,((rp / NULLIF(rp_sma, 0)) - 1) * 100 AS MANSFIELDRELATIVESTRENGTH
			,LAG(((rp / NULLIF(rp_sma, 0)) - 1) * 100, 1) OVER (
				PARTITION BY a.stock ORDER BY WeekFirstDay
				) AS PREVIOUSMANSFIELDRELATIVESTRENGTH
			,(((rp / NULLIF(rp_sma, 0)) - 1) * 100) - ABS((
					LAG(((rp / NULLIF(rp_sma, 0)) - 1) * 100, 1) OVER (
						PARTITION BY a.stock ORDER BY WeekFirstDay
						)
					)) AS MANSFIELDDIFF_CW_VS_PW
		FROM SIXWEEKS_AVGRANGEPCT_CALC a
		)
		--Currentweek versus Previous week Mansfield Strength percent difference calculation 
		,MANSFIELD_RS_PCT_DIFF_VS_PREVIOUS_PERIOD
	AS (
		SELECT STOCK
			,WEEKNUMBER
			,WeekFirstDay AS 'CALENDAR_DATE'
			,week_close AS 'STOCK_WEEK_CLOSE'
			,NIFTY_CLOSE AS 'NIFTY_WEEK_CLOSE'
			,[30_week_SMA] AS 'CURRENT_30_WEEK_SMA'
			,[30WeekMA_pct_diff] AS 'CURRENT_CLOSING_VS_30_WEEK_SMA_PCT_DIFF'
			,[6WEEKSPCTRANGE] AS 'SIXWEEKSPCTRANGE'
			,SIXWEEKSAVGPCTRANGE
			,ROUND(MANSFIELDRELATIVESTRENGTH, 2) AS MANSFIELDRELATIVESTRENGTH
			,ROUND(PREVIOUSMANSFIELDRELATIVESTRENGTH, 2) AS PREV_MANSFIELDRELATIVESTRENGTH
			,CASE 
				WHEN PREVIOUSMANSFIELDRELATIVESTRENGTH = 0
					THEN 'N/A' -- Handle zero division        
				ELSE CASE 
						WHEN (MANSFIELDRELATIVESTRENGTH - PREVIOUSMANSFIELDRELATIVESTRENGTH) > 0
							THEN '+' + CAST(ROUND((MANSFIELDRELATIVESTRENGTH - PREVIOUSMANSFIELDRELATIVESTRENGTH) / ABS(NULLIF(PREVIOUSMANSFIELDRELATIVESTRENGTH, 0)) * 100, 2) AS VARCHAR(30)) -- Positive difference        
						WHEN (MANSFIELDRELATIVESTRENGTH - PREVIOUSMANSFIELDRELATIVESTRENGTH) < 0
							THEN CAST(ROUND((MANSFIELDRELATIVESTRENGTH - PREVIOUSMANSFIELDRELATIVESTRENGTH) / ABS(NULLIF(PREVIOUSMANSFIELDRELATIVESTRENGTH, 0)) * 100, 2) AS VARCHAR(30)) -- Negative difference        
						ELSE '0.00' -- No change        
						END
				END AS 'CurrentWeek_VS_PrevWweek_MANSFIELD_RS_PCT_DIFF' -- Assign alias here        
		FROM MANSFIELD_RS_CALC
		)
	SELECT STOCK
		,WEEKNUMBER
		,CALENDAR_DATE
		,STOCK_WEEK_CLOSE
		,NIFTY_WEEK_CLOSE
		,CURRENT_30_WEEK_SMA
		,CURRENT_CLOSING_VS_30_WEEK_SMA_PCT_DIFF
		,SIXWEEKSPCTRANGE
		,SIXWEEKSAVGPCTRANGE
		,MANSFIELDRELATIVESTRENGTH
		,PREV_MANSFIELDRELATIVESTRENGTH
		,CurrentWeek_VS_PrevWweek_MANSFIELD_RS_PCT_DIFF
	FROM MANSFIELD_RS_PCT_DIFF_VS_PREVIOUS_PERIOD
END