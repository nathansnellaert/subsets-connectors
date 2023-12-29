from dagster import asset, FreshnessPolicy
import pandas as pd
from .utils import make_v4_request

# Asset for FMP Institutional Ownership data
@asset(
    metadata={
        "source": "Financial Modeling Prep",
        "name": "Institutional Ownership Data",
        "description": "Retrieves data on institutional ownership of company stocks, providing insights into the holdings and investment changes of institutional investors.",
        "columns": [
            {"name": "symbol", "type": "string", "description": "Ticker symbol of the company."},
            {"name": "cik", "type": "string", "description": "Central Index Key (CIK) number."},
            {"name": "date", "type": "date", "description": "Date of the record."},
            {"name": "investors_holding", "type": "float", "description": "Number of investors holding."},
            {"name": "last_investors_holding", "type": "float", "description": "Number of investors holding in the last period."},
            {"name": "investors_holding_change", "type": "float", "description": "Change in number of investors holding."},
            {"name": "number_of_13f_shares", "type": "float", "description": "Number of 13F shares."},
            {"name": "last_number_of_13f_shares", "type": "float", "description": "Number of 13F shares in the last period."},
            {"name": "number_of_13f_shares_change", "type": "float", "description": "Change in number of 13F shares."},
            {"name": "total_invested", "type": "float", "description": "Total amount invested."},
            {"name": "last_total_invested", "type": "float", "description": "Total amount invested in the last period."},
            {"name": "total_invested_change", "type": "float", "description": "Change in total amount invested."},
            {"name": "ownership_percent", "type": "float", "description": "Ownership percentage."},
            {"name": "last_ownership_percent", "type": "float", "description": "Last ownership percentage."},
            {"name": "ownership_percent_change", "type": "float", "description": "Change in ownership percentage."},
            {"name": "new_positions", "type": "float", "description": "Number of new positions."},
            {"name": "last_new_positions", "type": "float", "description": "Number of new positions in the last period."},
            {"name": "new_positions_change", "type": "float", "description": "Change in number of new positions."},
            {"name": "increased_positions", "type": "float", "description": "Number of increased positions."},
            {"name": "last_increased_positions", "type": "float", "description": "Number of increased positions in the last period."},
            {"name": "increased_positions_change", "type": "float", "description": "Change in number of increased positions."},
            {"name": "closed_positions", "type": "float", "description": "Number of closed positions."},
            {"name": "last_closed_positions", "type": "float", "description": "Number of closed positions in the last period."},
            {"name": "closed_positions_change", "type": "float", "description": "Change in number of closed positions."},
            {"name": "reduced_positions", "type": "float", "description": "Number of reduced positions."},
            {"name": "last_reduced_positions", "type": "float", "description": "Number of reduced positions in the last period."},
            {"name": "reduced_positions_change", "type": "float", "description": "Change in number of reduced positions."},
            {"name": "total_calls", "type": "float", "description": "Total number of calls."},
            {"name": "last_total_calls", "type": "float", "description": "Total number of calls in the last period."},
            {"name": "total_calls_change", "type": "float", "description": "Change in total number of calls."},
            {"name": "total_puts", "type": "float", "description": "Total number of puts."},
            {"name": "last_total_puts", "type": "float", "description": "Total number of puts in the last period."},
            {"name": "total_puts_change", "type": "float", "description": "Change in total number of puts."},
            {"name": "put_call_ratio", "type": "float", "description": "Put/Call ratio."},
            {"name": "last_put_call_ratio", "type": "float", "description": "Last Put/Call ratio."},
            {"name": "put_call_ratio_change", "type": "float", "description": "Change in Put/Call ratio."}
        ]
    },
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24 * 30, cron_schedule="0 0 1 * *")  # Adjust the freshness policy as needed
)
def fmp_institutional_ownership(fmp_company_profiles: pd.DataFrame) -> pd.DataFrame:
    symbols = fmp_company_profiles['symbol'].tolist()
    institutional_ownership_df = pd.concat([handle_request(ticker) for ticker in symbols])
    return institutional_ownership_df

# Function to handle requests for each ticker symbol
def handle_request(ticker):
    df = make_v4_request('institutional-ownership/symbol-ownership', {'symbol': ticker, 'includeCurrentQuarter': False})
    column_name_mapping = {
        "symbol": "symbol",
        "cik": "cik",
        "date": "date",
        "investorsHolding": "investors_holding",
        "lastInvestorsHolding": "last_investors_holding",
        "investorsHoldingChange": "investors_holding_change",
        "numberOf13Fshares": "number_of_13f_shares",
        "lastNumberOf13Fshares": "last_number_of_13f_shares",
        "numberOf13FsharesChange": "number_of_13f_shares_change",
        "totalInvested": "total_invested",
        "lastTotalInvested": "last_total_invested",
        "totalInvestedChange": "total_invested_change",
        "ownershipPercent": "ownership_percent",
        "lastOwnershipPercent": "last_ownership_percent",
        "ownershipPercentChange": "ownership_percent_change",
        "newPositions": "new_positions",
        "lastNewPositions": "last_new_positions",
        "newPositionsChange": "new_positions_change",
        "increasedPositions": "increased_positions",
        "lastIncreasedPositions": "last_increased_positions",
        "increasedPositionsChange": "increased_positions_change",
        "closedPositions": "closed_positions",
        "lastClosedPositions": "last_closed_positions",
        "closedPositionsChange": "closed_positions_change",
        "reducedPositions": "reduced_positions",
        "lastReducedPositions": "last_reduced_positions",
        "reducedPositionsChange": "reduced_positions_change",
        "totalCalls": "total_calls",
        "lastTotalCalls": "last_total_calls",
        "totalCallsChange": "total_calls_change",
        "totalPuts": "total_puts",
        "lastTotalPuts": "last_total_puts",
        "totalPutsChange": "total_puts_change",
        "putCallRatio": "put_call_ratio",
        "lastPutCallRatio": "last_put_call_ratio",
        "putCallRatioChange": "put_call_ratio_change"
    }

    df = df.rename(columns=column_name_mapping)

    df['date'] = pd.to_datetime(df['date']).dt.date

    float_columns = [
        'investors_holding', 'last_investors_holding', 'investors_holding_change',
        'number_of_13f_shares', 'last_number_of_13f_shares', 'number_of_13f_shares_change',
        'total_invested', 'last_total_invested', 'total_invested_change',
        'ownership_percent', 'last_ownership_percent', 'ownership_percent_change',
        'new_positions', 'last_new_positions', 'new_positions_change',
        'increased_positions', 'last_increased_positions', 'increased_positions_change',
        'closed_positions', 'last_closed_positions', 'closed_positions_change',
        'reduced_positions', 'last_reduced_positions', 'reduced_positions_change',
        'total_calls', 'last_total_calls', 'total_calls_change',
        'total_puts', 'last_total_puts', 'total_puts_change',
        'put_call_ratio', 'last_put_call_ratio', 'put_call_ratio_change'
    ]
    for col in float_columns:
        df[col] = df[col].astype(float)

    return df
