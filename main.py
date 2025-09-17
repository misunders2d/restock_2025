from connectors import gcloud as gc
import pandas as pd

def example_function(query:str) -> pd.DataFrame | str:
    """
    Uses Bigquery to pull sales from our database on Amazon sales

    Arg:
        query (str): an SQL query to run to Bigquery

    Returns:
        DataFrame: pandas dataframe with query result
        or
        str: error message
    """
    try:
        print('This is a test function')
        #your code
    except Exception as e:
        return f'Error happened: {e}'
    return pd.DataFrame()


def get_amazon_sales(query: str):
    """
    Bohdan
    pull sales for last 180 days excluding Prime Day for US market from `mellanni-project-da.reports.all_orders`. group by days.
    must return dataframe or error string
    dataframe columns to return: date, asin, unit_sales, dollar_sales
    """
    with gc.gcloud_connect() as client:
        result = client.query(query).to_dataframe()


def get_amazon_inventory(query: str):
    """
    Vitalii
    pull inventory history for last 180 days for all skus in US from `mellanni-project-da.reports.fba_inventory_planning`
    must return dataframe or error string
    dataframe columns to return: date, sku, asin, Inventory_Supply_at_FBA renamed as "amz_inventory"
    """

def get_wh_inventory(query: str):
    """
    Sergey
    pull latest warehouse inventory including incoming containers from `mellanni-project-da.sellercloud.inventory_bins_partitioned`
    must return dataframe or error string
    dataframe columns to return: sku, wh_inventory, incoming_containers
    """

def calculate_restock(sales:pd.DataFrame, amz_invnetory:pd.DataFrame, wh_inventory:pd.DataFrame) -> pd.DataFrame:
    """
    1. calculate in-stock-rate for the period (amz_inventory)
    2. calculate average sales 180 days and 14 days
    3. calculate average combined as average of (average sales 180 days and 14 days)
    4. calculate units needed (min 0, avoid negative numbers) for 49 days
    combine two dataframes into one and output the following columns:
        asin, average_sales_180, average_sales_14, average_combined, isr, amz_inventory (latest), wh_inventory (latest), units_to_ship
    """

    return pd.DataFrame()