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

def get_amazon_sales() -> pd.DataFrame | str:
    query = """
        SELECT
            CAST(purchase_date AS DATE) AS date,
            asin,
            SUM(quantity) AS unit_sales,
            SUM(item_price) AS dollar_sales
        FROM
            `mellanni-project-da.reports.all_orders`
        WHERE
            CAST(purchase_date AS DATE) BETWEEN '2025-02-17' AND CURRENT_DATE()
            AND sales_channel = 'Amazon.com'
            AND NOT (EXTRACT(MONTH FROM purchase_date) = 7 AND EXTRACT(DAY FROM purchase_date) IN (12, 13))
        GROUP BY
            1, 2
        ORDER BY
            1, 2
    """
    try:
        with gc.gcloud_connect() as client:
            result = client.query(query).to_dataframe()
        return result
    except Exception as e:
        return f'Error happened: {e}'  

def get_amazon_inventory(query: str):
    """
    Vitalii
    pull inventory history for last 180 days for all skus in US from `mellanni-project-da.reports.fba_inventory_planning`
    must return dataframe or error string
    dataframe columns to return: date, sku, asin, Inventory_Supply_at_FBA renamed as "amz_inventory"
    """
    try:
        with gc.gcloud_connect() as client:
            result = client.query(query).to_dataframe()
        return result
    except Exception as e:
        return f'Error happened: {e}'


def get_wh_inventory():
    """
    Sergey
    pull latest warehouse inventory including incoming containers from `mellanni-project-da.sellercloud.inventory_bins_partitioned`
    must return dataframe or error string
    dataframe columns to return: sku, wh_inventory, incoming_containers
    """

    wh_query = """
        WITH LatestInventoryDate AS (
            SELECT
                MAX(date_date) AS max_date
            FROM
                `mellanni-project-da.sellercloud.inventory_bins_partitioned`
        ),
        FilteredInventory AS (
            SELECT
                t1.ProductID AS sku,
                SUM(t1.QtyAvailable) AS wh_inventory,
            FROM
                `mellanni-project-da.sellercloud.inventory_bins_partitioned` AS t1
            INNER JOIN
                LatestInventoryDate AS t_max
                ON t1.date_date = t_max.max_date
            WHERE
                t1.Sellable = TRUE
                AND t1.BinType != "Picking"
                AND NOT STARTS_WITH(t1.BinName, "DS")
            GROUP BY
                t1.ProductID
        )
        SELECT
            fi.sku,
            fi.wh_inventory
        FROM
            FilteredInventory AS fi
        ORDER BY
            fi.wh_inventory DESC
    """

    incoming_query = """
        SELECT
            items.SKU AS sku,
            sum(Items.QtyOrdered) as incoming_containers
        FROM
            `mellanni-project-da.sellercloud.purchase_orders` AS t1,
            UNNEST(t1.Items) AS items
        WHERE
            date(t1.ExpectedDeliveryDate) >= CURRENT_DATE()
        GROUP BY items.SKU
        ORDER BY
            incoming_containers
            DESC
    """
    try:
        with gc.gcloud_connect() as client:
            wh_job = client.query(wh_query)
            incoming_job = client.query(incoming_query)
        wh = wh_job.to_dataframe()
        incoming = incoming_job.to_dataframe()

        result = pd.merge(wh, incoming, how = 'outer', on = 'sku', validate='1:1')
        return result
    except Exception as e:
        return f'Error happened: {e}'  



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

if __name__ == "__main__":
    sales_data = get_amazon_sales()
    wh_inventory = get_wh_inventory()
    print(wh_inventory)