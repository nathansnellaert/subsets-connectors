from dagster import asset
import pandas as pd

def process_zillow_csv(df):
    # Default has a separate column for each date, melt to skinny table
    df = df.melt(
        id_vars=["RegionID", "SizeRank", "RegionName", "RegionType", "StateName"], 
        var_name="date",  
        value_name="value"  
    )
    # Contains a single row for the whole of US. Filter for now.
    df = df[df['RegionType'] == 'msa']
    id_keep_cols = {
        'RegionName': 'region_name',
        'StateName': 'state_code',
    }
    df = df.rename(columns=id_keep_cols)
    df = df.drop(columns=['RegionID', 'SizeRank', 'RegionType'])
    return df

@asset(metadata={
    "source": "zillow",
    "name": "Zillow Home Value Index",
    "description": "Monthly Zillow Home Value Index data for metro areas.",
})
def zillow_home_value_index():
    url = "https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
    df = pd.read_csv(url)
    return process_zillow_csv(df)

@asset(metadata={
    "source": "zillow",
    "name": "Zillow Observed Rent Index",
    "description": "Monthly Zillow Observed Rent Index data for metro areas.",
})
def zillow_observed_rent_index():
    url = "https://files.zillowstatic.com/research/public_csvs/zori/Metro_zori_sm_month.csv"
    df = pd.read_csv(url)
    return process_zillow_csv(df)

@asset(metadata={
    "source": "zillow",
    "name": "Zillow Inventory for Sale",
    "description": "Monthly inventory for sale data for metro areas.",
})
def zillow_inventory_for_sale():
    url = "https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfrcondo_sm_month.csv"
    df = pd.read_csv(url)
    return process_zillow_csv(df)

@asset(metadata={
    "source": "zillow",
    "name": "Zillow Median List Price",
    "description": "Monthly median list price data for metro areas.",
})
def zillow_median_list_price():
    url = "https://files.zillowstatic.com/research/public_csvs/mlp/Metro_mlp_uc_sfrcondo_sm_month.csv"
    df = pd.read_csv(url)
    return process_zillow_csv(df)

@asset(metadata={
    "source": "zillow",
    "name": "Zillow Sales Count Now",
    "description": "Monthly sales count data for metro areas.",
})
def zillow_sales_count_now():
    url = "https://files.zillowstatic.com/research/public_csvs/sales_count_now/Metro_sales_count_now_uc_sfrcondo_month.csv"
    df = pd.read_csv(url)
    return process_zillow_csv(df)