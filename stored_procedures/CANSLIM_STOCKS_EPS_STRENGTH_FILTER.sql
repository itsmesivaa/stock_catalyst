/*
Created by Sivashankaran

CANSLIM strategy by William O'Neil
--Relative Strength above 70
--Current vs Annual EPS Percentage Change > 25
--Current vs Previous (Quarter) EPS Percentage Change > 25
--Current vs Previous Sales Percentage Change > 25
--Instituions Promoters holding > 40%
--Floating shares less than 1 Crore
*/
CREATE PROCEDURE [dbo].[CANSLIM_STOCKS_EPS_STRENGTH_FILTER]
AS
BEGIN
	WITH canslim_data_prep
	AS (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY earn.stock_name ORDER BY earn.stock_name
				) AS earn_rownum
			,earn.stock_name AS Stock
			,eval.market_capitalization AS Market_Capitalization
			,eval.sales AS 'Sales'
			,eval.shares_in_float AS 'FloatingShares'
			,CASE 
				WHEN RIGHT(eval.shares_in_float, 1) = 'L'
					THEN TRY_CAST(REPLACE(eval.shares_in_float, ' L','') AS DECIMAL(10, 2)) * 100000
				WHEN RIGHT(eval.shares_in_float, 3) = ' Cr'
					THEN TRY_CAST(REPLACE(eval.shares_in_float, ' Cr','') AS DECIMAL(10, 2)) * 10000000
				END AS 'ActualFloatingSharesinNumbers'
			,eval.eps_rating AS 'EPS_Rating'
			,eval.price_strength AS 'Relative_Strength'
			,earn.[quarter] AS 'Quarter'
			,TRY_CAST(earn.eps AS DECIMAL(10, 2)) AS 'Cur_Qtr_EPS'
			,REPLACE(REPLACE(earn.eps_pct_chg, '%',''), '+','') AS 'Cur_vs_Ann_EPS%_Chg'
			,TRY_CAST(REPLACE(earn.sales_in_cr, ',', '') AS DECIMAL(10, 2)) AS 'Cur_Qtr_Sales_in_Crs'
			,REPLACE(REPLACE(earn.sales_pct_chg, '%',''), '+','') AS 'Cur_vs_Ann_Sales%_PCT'
			,TRY_CAST(REPLACE(inst.[FinancialInstitutions/Banks], '%','') AS DECIMAL(10, 2)) AS [FinancialInstitutions/Banks]
			,TRY_CAST(REPLACE(inst.[ForeignPortfolioInvestors], '%','') AS DECIMAL(10, 2)) AS [ForeignPortfolioInvestors]
			,TRY_CAST(REPLACE(inst.[IndividualInvestors], '%','') AS DECIMAL(10, 2)) AS [IndividualInvestors]
			,TRY_CAST(REPLACE(inst.[InsuranceCompanies], '%','') AS DECIMAL(10, 2)) AS [InsuranceCompanies]
			,TRY_CAST(REPLACE(inst.[MutualFunds], '%','') AS DECIMAL(10, 2)) AS [MutualFunds]
			,TRY_CAST(REPLACE(inst.[Others], '%','') AS DECIMAL(10, 2)) AS [Others]
			,TRY_CAST(REPLACE(inst.[Promoters], '%','') AS DECIMAL(10, 2)) AS [Promoters]
		FROM market_smith_stock_eval eval
		LEFT JOIN all_stocks_quarterly_earnings earn ON eval.stock_name = earn.stock_name
		LEFT JOIN market_smith_stock_institutional_data inst ON earn.stock_name = inst.StockName
			AND earn.[quarter] = inst.[Quarter]
		)
		,EPS_SALES_calc
	AS (
		SELECT earn_rownum
			,Stock
			,Market_Capitalization
			,Sales
			,FloatingShares
			,ActualFloatingSharesinNumbers
			,EPS_Rating
			,Relative_Strength
			,[Quarter]
			,Cur_Qtr_EPS
			,LEAD(Cur_Qtr_EPS, 1) OVER (
				PARTITION BY Stock ORDER BY earn_rownum
				) AS Previous_Qtr_EPS
			,(
				Cur_Qtr_EPS - LEAD(Cur_Qtr_EPS, 1) OVER (
					PARTITION BY Stock ORDER BY earn_rownum
					)
				) / LEAD(NULLIF(Cur_Qtr_EPS, 0), 1) OVER (
				PARTITION BY Stock ORDER BY earn_rownum
				) * 100 AS [Cur_vs_Prev_EPS%_Diff]
			,[Cur_vs_Ann_EPS%_Chg]
			,Cur_Qtr_Sales_in_Crs
			,LEAD(Cur_Qtr_Sales_in_Crs, 1) OVER (
				PARTITION BY Stock ORDER BY earn_rownum
				) AS Previous_Sales
			,(
				Cur_Qtr_Sales_in_Crs - LEAD(Cur_Qtr_Sales_in_Crs, 1) OVER (
					PARTITION BY Stock ORDER BY earn_rownum
					)
				) / LEAD(NULLIF(Cur_Qtr_Sales_in_Crs, 0), 1) OVER (
				PARTITION BY Stock ORDER BY earn_rownum
				) * 100 AS [Cur_vs_Prev_Sales%Diff]
			,[Cur_vs_Ann_Sales%_PCT]
			,[FinancialInstitutions/Banks]
			,[ForeignPortfolioInvestors]
			,[IndividualInvestors]
			,[InsuranceCompanies]
			,[MutualFunds]
			,[Others]
			,[Promoters]
		FROM canslim_data_prep
		WHERE Stock IS NOT NULL
		)
	SELECT Stock
		,Market_Capitalization
		,Sales
		,FloatingShares
		,Relative_Strength
		,[Quarter]
		,Cur_Qtr_EPS
		,Previous_Qtr_EPS
		,cast([Cur_vs_Prev_EPS%_Diff] AS DECIMAL(10, 2)) AS [Cur_vs_Prev_EPS%_Diff]
		,Cur_Qtr_Sales_in_Crs
		,Previous_Sales
		,CAST([Cur_vs_Prev_Sales%Diff] AS DECIMAL(10, 2)) AS [Cur_vs_Prev_Sales%Diff]
		,[Cur_vs_Ann_EPS%_Chg]
		,[Cur_vs_Ann_Sales%_PCT]
	FROM EPS_SALES_calc
	WHERE earn_rownum = 1
		AND Relative_Strength > 70
		AND [Cur_vs_Ann_EPS%_Chg] > 25
		AND [Cur_vs_Prev_EPS%_Diff] > 25
		AND [Cur_vs_Prev_Sales%Diff] > 25
		AND Promoters > 40
		AND ActualFloatingSharesinNumbers < 100000000
		AND Cur_Qtr_Sales_in_Crs IS NOT NULL
		AND Previous_Qtr_EPS IS NOT NULL
END