from connectors import gcloud as gc
import pandas as pd
import os
from utils import mellanni_modules as mm

user_folder = os.path.join(os.path.expanduser('~'), 'temp')
os.makedirs(user_folder, exist_ok=True)
days_of_sale = 49


def get_amazon_sales() -> pd.DataFrame:
    """
    Bohdan
    pull sales for last 180 days excluding Prime Day for US market from `mellanni-project-da.reports.all_orders`. group by days.
    must return dataframe or error string
    dataframe columns to return: date, asin, unit_sales, dollar_sales
    """

    query = """
        SELECT
            CAST(purchase_date AS DATE) AS date,
            asin,
            SUM(quantity) AS unit_sales,
            SUM(item_price) AS dollar_sales
        FROM
            `mellanni-project-da.reports.all_orders`
        WHERE
            CAST(purchase_date AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()
            AND sales_channel = 'Amazon.com'
            AND NOT (EXTRACT(MONTH FROM purchase_date) = 7 AND EXTRACT(DAY FROM purchase_date) IN (8,9,10,11))
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
        return pd.DataFrame([f'error happened: {e}'], columns = ["Error"])

def get_amazon_inventory() -> pd.DataFrame:
    """
    Vitalii
    pull inventory history for last 180 days for all skus in US from `mellanni-project-da.reports.fba_inventory_planning`
    must return dataframe or error string
    dataframe columns to return: date, sku, asin, Inventory_Supply_at_FBA renamed as "amz_inventory"
    """
    query = """
        SELECT
            DATE(snapshot_date) AS date,
            sku,
            asin,
            Inventory_Supply_at_FBA AS amz_inventory
        FROM
            `mellanni-project-da.reports.fba_inventory_planning`
        WHERE
            marketplace = 'US'
            AND DATE(snapshot_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()
        ORDER BY
            date DESC, sku ASC
    """

    try:
        with gc.gcloud_connect() as client:
            df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        return pd.DataFrame([f'error happened: {e}'], columns = ["Error"])


def get_wh_inventory() -> pd.DataFrame:
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
        return pd.DataFrame([f'error happened: {e}'], columns = ["Error"])



def calculate_restock() -> pd.DataFrame:
    """
    Ruslan
    1. calculate in-stock-rate for the period (amz_inventory)
    2. calculate average sales 180 days and 14 days
    3. calculate average combined as average of (average sales 180 days and 14 days)
    4. calculate units needed (min 0, avoid negative numbers) for 49 days
    combine two dataframes into one and output the following columns:
        asin, average_sales_180, average_sales_14, average_combined, isr, amz_inventory (latest), wh_inventory (latest), units_to_ship
    """

    amazon_sales = get_amazon_sales()
    amazon_sales['date'] = pd.to_datetime(amazon_sales['date'])
    wh_inventory = get_wh_inventory()
    amazon_inventory = get_amazon_inventory()
    amazon_inventory['date'] = pd.to_datetime(amazon_inventory['date'])
    
      

    def calculate_inventory_isr(amazon_inventory):
        inventory_grouped = amazon_inventory.groupby(['date', 'sku']).agg({'amz_inventory': 'sum','asin': 'first'}).reset_index()
        inventory_grouped['in-stock-rate'] = inventory_grouped['amz_inventory'] > 0
        asin_isr = inventory_grouped.groupby('sku').agg({'in-stock-rate': 'mean','asin': 'first'}).reset_index().round(2)
        asin_isr = pd.merge(asin_isr, wh_inventory, on='sku', how='left')
        asin_isr = asin_isr[['asin','sku','in-stock-rate','wh_inventory','incoming_containers']]
        return asin_isr

    def fill_dates(amazon_sales:pd.DataFrame):
        start_date = amazon_sales['date'].min()
        end_date = amazon_sales['date'].max()
        date_range = pd.date_range(start=start_date, end=end_date)
        full_dates = pd.DataFrame(date_range, columns=['date'])
        asin_list = amazon_sales['asin'].unique()

        all_files = []
        for asin in asin_list:
            temp_df = amazon_sales[amazon_sales['asin']==asin]
            full_asin = pd.merge(full_dates, temp_df, how = 'left', on = 'date')
            full_asin['asin']=asin
            all_files.append(full_asin)
        all_sales = pd.concat(all_files)
        all_sales = all_sales.fillna(0)
        return all_sales


    def last_2_weeks_sales(amazon_sales:pd.DataFrame, amazon_inventory:pd.DataFrame):
        last_date = amazon_sales['date'].max()
        cut_off_date = last_date - pd.Timedelta(days=13)
        latest_sales = amazon_sales[amazon_sales['date'] >= cut_off_date]
        latest_inventory = amazon_inventory[amazon_inventory['date'] >= cut_off_date]
        latest_inventory = latest_inventory.groupby(['date','asin'])[['amz_inventory']].agg('sum').reset_index()
        latest_inventory['in-stock-rate'] = latest_inventory['amz_inventory'] > 0
        latest_isr = latest_inventory.groupby('asin')[['in-stock-rate']].agg('mean').reset_index()
        latest_sales = latest_sales.groupby('asin')[['unit_sales']].agg('sum').reset_index()
        latest_sales = pd.merge(latest_sales, latest_isr, on='asin', how='outer')
        latest_sales['average_sales_14'] = latest_sales['unit_sales'] / 14
        latest_sales['average_sales_14'] = (latest_sales['average_sales_14'] / latest_sales['in-stock-rate']).round(3)
        return latest_sales[['asin','average_sales_14']]


    def get_asin_sales(amazon_sales:pd.DataFrame):
        sales_total_days = amazon_sales['date'].nunique()
        latest_sales = last_2_weeks_sales(amazon_sales, amazon_inventory)
        total_sales = amazon_sales.groupby('asin')[['unit_sales']].agg('sum').reset_index()
        total_sales['average_sales_180'] = (total_sales['unit_sales'] / sales_total_days).round(3)
        total_sales = pd.merge(total_sales, latest_sales, on='asin', how='outer')
        return total_sales, sales_total_days


    asin_isr = calculate_inventory_isr(amazon_inventory[['date','sku', 'asin', 'amz_inventory']].copy())
    total_sales, sales_total_days = get_asin_sales(amazon_sales)
    result = pd.merge(asin_isr, total_sales, on='asin', how='outer')
    result['in-stock-rate'] = result['in-stock-rate'].fillna(0)
    result['unit_sales'] = result['unit_sales'].fillna(0)
    result.rename(columns={'unit_sales':'unit_sales_180'}, inplace=True)
    result['average_sales_180'] = (result['average_sales_180'] / result['in-stock-rate']).round(3)
    result['average_sales_180'] = result['average_sales_180'].fillna(0)
    result['average_sales_14'] = result['average_sales_14'].fillna(0)
    result['average_combined'] = (result['average_sales_180']*0.4 + result['average_sales_14'] *0.6).round(3)
    amazon_inventory_copy = amazon_inventory.copy()
    latest_date = amazon_inventory_copy['date'].max()
    latest_inventory = amazon_inventory_copy[amazon_inventory_copy['date'] == latest_date]
    quantity_map = latest_inventory.groupby('asin')['amz_inventory'].sum().reset_index()
    result = pd.merge(result, quantity_map, on='asin', how='outer')
    result['amz_inventory'] = result['amz_inventory'].fillna(0)
    result['days_of_sale_remaining'] = (result['amz_inventory'] / result['average_combined']).round(0)
    result['days_of_sale_remaining'] = result['days_of_sale_remaining'].fillna(0)
    result['units_to_ship'] = ((result['average_combined'] * days_of_sale - result['amz_inventory']).round(0)).apply(lambda x: x if x > 0 else 0)
    result = result[['asin','sku','in-stock-rate', 'unit_sales_180','average_sales_180','average_sales_14','average_combined','amz_inventory','days_of_sale_remaining','units_to_ship','wh_inventory', 'incoming_containers']]
    mm.export_to_excel([result],['restock'], 'inventory_restock.xlsx', user_folder)
    mm.open_file_folder(os.path.join(user_folder))
    return result

if __name__ == "__main__":
    calculate_restock()