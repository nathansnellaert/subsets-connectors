from dagster import asset
import pandas as pd
from .utils import make_v3_request

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Analyst Recommendation Data",
        "description": "Provides analyst recommendations for buying, selling, or holding a company's stock. Useful for investors to understand analysts' views on a company's stock.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "date", "type": "date", "description": "Date of the recommendation."},
            {"name": "analyst_ratings_buy", "type": "int", "description": "Number of buy ratings."},
            {"name": "analyst_ratings_hold", "type": "int", "description": "Number of hold ratings."},
            {"name": "analyst_ratings_sell", "type": "int", "description": "Number of sell ratings."},
            {"name": "analyst_ratings_strong_sell", "type": "int", "description": "Number of strong sell ratings."},
            {"name": "analyst_ratings_strong_buy", "type": "int", "description": "Number of strong buy ratings."}
        ]
    }
)
def fmp_analyst_recommendation(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    dfs = []
    for symbol in fmp_company_profiles['symbol'].tolist():
        df = make_v3_request('analyst-stock-recommendations/' + symbol)
        if not df.empty:
            dfs.append(df)
    df = pd.concat(dfs)
    df = df.rename(columns={
        "analystRatingsbuy": "analyst_ratings_buy",
        "analystRatingsHold": "analyst_ratings_hold",
        "analystRatingsSell": "analyst_ratings_sell",
        "analystRatingsStrongSell": "analyst_ratings_strong_sell",
        "analystRatingsStrongBuy": "analyst_ratings_strong_buy"
    })
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df

@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Analyst Estimates Data",
        "description": "Provides analyst estimates for a company's future earnings and revenue, essential for understanding what analysts expect from a company and identifying potential investment opportunities.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "date", "type": "date", "description": "Date of the estimate."},
            {"name": "estimated_revenue_low", "type": "float", "description": "Low estimate of revenue."},
            {"name": "estimated_revenue_high", "type": "float", "description": "High estimate of revenue."},
            {"name": "estimated_revenue_avg", "type": "float", "description": "Average estimate of revenue."},
            {"name": "estimated_ebitda_low", "type": "float", "description": "Low estimate of EBITDA."},
            {"name": "estimated_ebitda_high", "type": "float", "description": "High estimate of EBITDA."},
            {"name": "estimated_ebitda_avg", "type": "float", "description": "Average estimate of EBITDA."},
            {"name": "estimated_ebit_low", "type": "float", "description": "Low estimate of EBIT."},
            {"name": "estimated_ebit_high", "type": "float", "description": "High estimate of EBIT."},
            {"name": "estimated_ebit_avg", "type": "float", "description": "Average estimate of EBIT."},
            {"name": "estimated_net_income_low", "type": "float", "description": "Low estimate of net income."},
            {"name": "estimated_net_income_high", "type": "float", "description": "High estimate of net income."},
            {"name": "estimated_net_income_avg", "type": "float", "description": "Average estimate of net income."},
            {"name": "estimated_sga_expense_low", "type": "float", "description": "Low estimate of SGA expense."},
            {"name": "estimated_sga_expense_high", "type": "float", "description": "High estimate of SGA expense."},
            {"name": "estimated_sga_expense_avg", "type": "float", "description": "Average estimate of SGA expense."},
            {"name": "estimated_eps_avg", "type": "float", "description": "Average estimate of EPS."},
            {"name": "estimated_eps_high", "type": "float", "description": "High estimate of EPS."},
            {"name": "estimated_eps_low", "type": "float", "description": "Low estimate of EPS."},
            {"name": "number_analysts_estimated_revenue", "type": "int", "description": "Number of analysts who estimated revenue."},
            {"name": "number_analysts_estimated_eps", "type": "int", "description": "Number of analysts who estimated EPS."}
        ]
    }
)
def fmp_analyst_estimates(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    dfs = []
    for symbol in fmp_company_profiles['symbol'].tolist():
        df = make_v3_request(f'analyst-estimates/{symbol}')
        if not df.empty:
            dfs.append(df)

    df = pd.concat(dfs)
            
    df = df.rename(columns={
        "estimatedRevenueLow": "estimated_revenue_low",
        "estimatedRevenueHigh": "estimated_revenue_high",
        "estimatedRevenueAvg": "estimated_revenue_avg",
        "estimatedEbitdaLow": "estimated_ebitda_low",
        "estimatedEbitdaHigh": "estimated_ebitda_high",
        "estimatedEbitdaAvg": "estimated_ebitda_avg",
        "estimatedEbitLow": "estimated_ebit_low",
        "estimatedEbitHigh": "estimated_ebit_high",
        "estimatedEbitAvg": "estimated_ebit_avg",
        "estimatedNetIncomeLow": "estimated_net_income_low",
        "estimatedNetIncomeHigh": "estimated_net_income_high",
        "estimatedNetIncomeAvg": "estimated_net_income_avg",
        "estimatedSgaExpenseLow": "estimated_sga_expense_low",
        "estimatedSgaExpenseHigh": "estimated_sga_expense_high",
        "estimatedSgaExpenseAvg": "estimated_sga_expense_avg",
        "estimatedEpsAvg": "estimated_eps_avg",
        "estimatedEpsHigh": "estimated_eps_high",
        "estimatedEpsLow": "estimated_eps_low",
        "numberAnalystEstimatedRevenue": "number_analysts_estimated_revenue",
        "numberAnalystsEstimatedEps": "number_analysts_estimated_eps"
    })

    df['date'] = pd.to_datetime(df['date']).dt.date
    return df