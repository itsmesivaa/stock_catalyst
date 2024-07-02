/*
Created by Sivashankaran

This procedure calculates MarkMinervini Based RS Rating based on Investor's Business Daily theory to find 
potential stocks coming for next big run

--Compute 50SMA (10-Weeks), 150SMA (30-Weeks) and 200SMA (40-Weeks) for all stocks everyday
--Compute 52Week High and 52Week Low for stocks everyday

*/
USE NSEBhavcopy
GO

CREATE PROCEDURE [dbo].[MARK_MINERVINI_STOCKS_TREND_TEMPLATE_FILTER]
AS
BEGIN
	WITH stock_det
	AS (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY [stock] ORDER BY DATE ASC
				) AS [index]
			,[stock]
			,[date]
			,[open]
			,[high]
			,[low]
			,[close]
			,volume
		FROM stock_ohlc_data
		)
		,
		-- Step 1: Creating a CTE for stock general details              
	stock_gen_detail_cte
	AS (
		SELECT a.[index] AS 'index'
			,a.[stock] AS 'stock_name'
			,a.[date] AS 'cur_date'
			,a.[open] AS 'stock_open'
			,a.[high] AS 'stock_high'
			,a.[low] AS 'stock_low'
			,a.[close] AS 'stock_close'
			,a.volume
			,ROUND(0.4 * ((a.[close] - b.[close]) / NULLIF(b.[close], 0)), 2) AS rs_score_3m
			,ROUND(0.2 * ((a.[close] - c.[close]) / NULLIF(c.[close], 0)), 2) AS rs_score_6m
			,ROUND(0.2 * ((a.[close] - d.[close]) / NULLIF(d.[close], 0)), 2) AS rs_score_9m
			,ROUND(0.2 * ((a.[close] - e.[close]) / NULLIF(e.[close], 0)), 2) AS rs_score_12m
			,ROUND((0.4 * ((a.[close] - b.[close]) / NULLIF(b.[close], 0)) + 0.2 * ((a.[close] - c.[close]) / NULLIF(c.[close], 0)) + 0.2 * ((a.[close] - d.[close]) / NULLIF(d.[close], 0)) + 0.2 * ((a.[close] - e.[close]) / NULLIF(e.[close], 0))) * 100, 2) AS RS_Score
		FROM stock_det a
		LEFT JOIN stock_det b ON a.stock = b.stock
			AND a.[index] - 63 = b.[index]
		LEFT JOIN stock_det c ON a.stock = c.stock
			AND a.[index] - 126 = c.[index]
		LEFT JOIN stock_det d ON a.stock = d.stock
			AND a.[index] - 189 = d.[index]
		LEFT JOIN stock_det e ON a.stock = e.stock
			AND a.[index] - 250 = e.[index]
		)
		,
		-- Step 2: Calculating 50SMA (10-Weeks), 150SMA (30-Weeks) and 200SMA (40-Weeks)              
	stock_moving_avg_calc
	AS (
		SELECT [index]
			,stock_name
			,cur_date
			,DENSE_RANK() OVER (
				ORDER BY DATEPART(year, cur_date)
					,DATEPART(week, cur_date)
				) AS week_row_num
			,stock_open
			,stock_high
			,stock_low
			,stock_close
			,volume
			,rs_score_3m
			,rs_score_6m
			,rs_score_9m
			,rs_score_12m
			,RS_Score
			,ROUND(AVG(stock_close) OVER (
					PARTITION BY stock_name ORDER BY cur_date ROWS BETWEEN 49 PRECEDING
							AND CURRENT ROW
					), 2) AS [50_day_SMA]
			,ROUND(AVG(stock_close) OVER (
					PARTITION BY stock_name ORDER BY cur_date ROWS BETWEEN 149 PRECEDING
							AND CURRENT ROW
					), 2) AS [150_day_SMA]
			,ROUND(AVG(stock_close) OVER (
					PARTITION BY stock_name ORDER BY cur_date ROWS BETWEEN 199 PRECEDING
							AND CURRENT ROW
					), 2) AS [200_day_SMA]
		FROM stock_gen_detail_cte
		)
		,
		-- Step 3: Calculating 52Week High and 52Week Low for stock              
	moving_avg_52_week_high_low_calc
	AS (
		SELECT [index]
			,stock_name
			,cur_date
			,week_row_num
			,stock_open
			,stock_high
			,stock_low
			,stock_close
			,volume
			,rs_score_3m
			,rs_score_6m
			,rs_score_9m
			,rs_score_12m
			,RS_Score
			,[50_day_SMA]
			,[150_day_SMA]
			,[200_day_SMA]
			,LAG([200_day_SMA], 1) OVER (
				PARTITION BY stock_name ORDER BY cur_date
				) AS previous_200_day_sma
			,MIN(stock_low) OVER (
				PARTITION BY stock_name ORDER BY cur_date ROWS BETWEEN 246 PRECEDING
						AND CURRENT ROW
				) AS [52_week_low]
			,MAX(stock_high) OVER (
				PARTITION BY stock_name ORDER BY cur_date ROWS BETWEEN 246 PRECEDING
						AND CURRENT ROW
				) AS [52_week_high]
		FROM stock_moving_avg_calc
		WHERE cur_date BETWEEN DATEADD(WEEK, - 51, cur_date)
				AND cur_date
		)
		,
		-- Step 4: Applying Mark Minervini conditions for Trend Template to filter Stage 2 stocks              
	stock_price_above_moving_avg
	AS (
		SELECT [index]
			,stock_name
			,cur_date
			,week_row_num
			,stock_open
			,stock_high
			,stock_low
			,stock_close
			,volume
			,rs_score_3m
			,rs_score_6m
			,rs_score_9m
			,rs_score_12m
			,RS_Score
			,[50_day_SMA]
			,[150_day_SMA]
			,[200_day_SMA]
			,previous_200_day_sma
			,CASE 
				WHEN [200_day_SMA] > previous_200_day_sma
					THEN 'T'
				ELSE 'F'
				END AS [200_day_SMA_Trending]
			,[52_week_high]
			,[52_week_low]
			,ROUND(((stock_close - [52_week_low]) / NULLIF([52_week_low], 0) * 100), 2) AS closevs52week_low_pctdiff
			,ROUND((([52_week_high] - stock_close) / NULLIF(stock_close, 0) * 100), 2) AS closevs52week_high_pct_diff
		FROM moving_avg_52_week_high_low_calc
		)
	-- Final selection of stocks based on Minervini criteria              
	SELECT [index] AS 'index'
		,stock_name
		,cur_date
		,week_row_num
		,stock_open
		,stock_high
		,stock_low
		,stock_close
		,volume
		,rs_score_3m
		,rs_score_6m
		,rs_score_9m
		,rs_score_12m
		,RS_Score
		,[50_day_SMA]
		,[150_day_SMA]
		,[200_day_SMA]
		,previous_200_day_sma
		,[200_day_SMA_Trending]
		,[52_week_high]
		,[52_week_low]
		,closevs52week_low_pctdiff
		,closevs52week_high_pct_diff
	FROM stock_price_above_moving_avg
		/* WHERE              
  [4_months_trending_count] >= 87 AND [200_day_SMA_Trending] <> 'F'                
        AND (stock_close > [150_day_SMA]) AND (stock_close > [200_day_SMA])              
        AND ([150_day_SMA] > [200_day_SMA])              
        AND ([50_day_SMA] > [150_day_SMA] AND [50_day_SMA] > [200_day_SMA])              
  AND (stock_close > [200_day_SMA])              
        AND (stock_close > [52_week_low] AND closevs52week_low_pctdiff > 25)              
        AND (stock_close < [52_week_high] AND closevs52week_high_pct_diff <= 25)              
    ORDER BY cur_date DESC;              
*/
END;