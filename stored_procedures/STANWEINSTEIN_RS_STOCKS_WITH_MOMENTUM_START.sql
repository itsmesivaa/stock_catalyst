/*
Created by Sivashankaran

Exporting the potential stocks which met StanWeinstein Conditions during the specified data
Here along with current period we are pulling the previous range value that is 
from Streamlit UI page if you're looking to pull for 15 week period previous 15 weeks respective
stocks records were also shown to compare how effectively stocks came out from that specific 
consolidation range period along with Relative Strength metrics to look out to pick next big mover

*/

CREATE PROCEDURE [dbo].[STANWEINSTEIN_RS_STOCKS_WITH_MOMENTUM_START] @FROM_DATE NVARCHAR(30)
	,@RANGE_VALUE INT
AS
DECLARE @FirstDayOfWeek DATE;

-- Calculate the number of days to subtract to get to Monday (assuming Monday is the first day of the week)              
SET @FirstDayOfWeek = DATEADD(DAY, 2 - DATEPART(WEEKDAY, CONVERT(DATE, @FROM_DATE, 105)), CONVERT(DATE, @FROM_DATE, 105));

--SELECT @FirstDayOfWeek AS FirstDayOfWeek, DATENAME(MONTH, @FirstDayOfWeek) AS Month, YEAR(@FirstDayOfWeek) AS Year;              
BEGIN
	WITH STAN_WEINSTEIN_RS_QUERY_CALC_RESULT
	AS (
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
		FROM stanweinstein_stock_with_mansfield_strength_data
		WHERE (
				CONVERT(DATE, CALENDAR_DATE) BETWEEN DATEADD(WEEK, - @RANGE_VALUE, @FirstDayOfWeek)
					AND DATEADD(WEEK, - 1, @FirstDayOfWeek)
				AND MANSFIELDRELATIVESTRENGTH < 0
				)
		
		UNION
		
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
		FROM stanweinstein_stock_with_mansfield_strength_data
		WHERE (
				CONVERT(DATE, CALENDAR_DATE) = CONVERT(DATE, @FirstDayOfWeek)
				AND MANSFIELDRELATIVESTRENGTH > 0
				)
		)
	SELECT STOCK
		,FORMAT(CONVERT(DATE, CALENDAR_DATE), 'dd-MMM-yyyy') AS DATE
		,STOCK_WEEK_CLOSE
		,NIFTY_WEEK_CLOSE
		,CURRENT_30_WEEK_SMA AS '30_WEEK_SMA'
		,CURRENT_CLOSING_VS_30_WEEK_SMA_PCT_DIFF 'STOCK_CLOSE_VS_30_WEEK_SMA_PCT_DIFF'
		,SIXWEEKSPCTRANGE
		,SIXWEEKSAVGPCTRANGE
		,MANSFIELDRELATIVESTRENGTH AS 'MANSFIELD_RS'
		,PREV_MANSFIELDRELATIVESTRENGTH AS 'PREV_MANSFIELD_RS'
		,CurrentWeek_VS_PrevWweek_MANSFIELD_RS_PCT_DIFF AS CW_VS_PW_MRS_PCT_DIFF
	FROM STAN_WEINSTEIN_RS_QUERY_CALC_RESULT
	WHERE stock IN (
			SELECT stock
			FROM STAN_WEINSTEIN_RS_QUERY_CALC_RESULT
			GROUP BY stock
			HAVING COUNT(STOCK) = @RANGE_VALUE + 1
			)
END