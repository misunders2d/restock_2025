from connectors import gcloud as gc
import pandas as pd
import os
import numpy as np

from utils import mellanni_modules as mm
from restock_utils import calculate_inventory_isr, get_asin_sales
from connectors import gdrive as gd
from typing import Literal
import threading

from date_utils import get_event_days_delta

user_folder = os.path.join(os.path.expanduser("~"), "temp")
os.makedirs(user_folder, exist_ok=True)
days_of_sale = 49
results = dict()


def get_event_spreadsheet(
    event: Literal["BFCM", "BSS", "PD", "PBDD"] | None = None,
) -> pd.DataFrame:
    spreadsheet = gd.download_gspread(
        spreadsheet_id="1_gSk2xSDuyEQ9qzI15NJBxVCBZSJMuTKS1pDsvnfes8"
    )
    if event == "BFCM":
        columns_to_return = [
            "ASIN",
            "Average BFCM sales, units (1 day)",
            "Best BFCM performance",
        ]
    elif event == "BSS":
        columns_to_return = [
            "ASIN",
            "Average BSS sales, units (1 day)",
            "Best BSS performance",
        ]
    elif event == "PD":
        columns_to_return = [
            "ASIN",
            "Average PD sales, units (1 day)",
            "Best PD performance",
        ]
    elif event == "PBDD":
        columns_to_return = [
            "ASIN",
            "Average PBDD sales, units (1 day)",
            "Best PBDD performance",
        ]
    else:
        columns_to_return = spreadsheet.columns.tolist()
    spreadsheet = spreadsheet[columns_to_return]
    spreadsheet = spreadsheet.rename(
        columns={
            "ASIN": "asin",
            f"Average {event} sales, units (1 day)": "average event sales 1 day",
            f"Best {event} performance": "best event performance",
        }
    )
    spreadsheet.loc[
        spreadsheet["average event sales 1 day"] == "", "average event sales 1 day"
    ] = 0
    spreadsheet.loc[
        spreadsheet["best event performance"] == "", "best event performance"
    ] = 0
    return spreadsheet


def get_amazon_sales(to_print) -> pd.DataFrame:
    """
    Bohdan
    pull sales for last 180 days excluding Prime Day for US market from `mellanni-project-da.reports.all_orders`. group by days.
    must return dataframe or error string
    dataframe columns to return: date, asin, unit_sales, dollar_sales
    """
    if to_print:
        print("Starting to run `get_amazon_sales`")
    query = """
        SELECT
            CAST(purchase_date AS DATE) AS date,
            asin,
            SUM(quantity) AS unit_sales,
            SUM(item_price) AS dollar_sales
        FROM
            `mellanni-project-da.reports.all_orders`
        WHERE
            CAST(purchase_date AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 270 DAY) AND CURRENT_DATE()
            AND sales_channel = 'Amazon.com'
        GROUP BY
            1, 2
        ORDER BY
            1, 2
    """
    try:
        with gc.gcloud_connect() as client:
            result = client.query(query).to_dataframe()
        results["get_amazon_sales"] = result
        return result
    except Exception as e:
        return pd.DataFrame([f"error happened: {e}"], columns=["Error"])


def get_amazon_inventory(to_print) -> pd.DataFrame:
    """
    Vitalii
    pull inventory history for last 180 days for all skus in US from `mellanni-project-da.reports.fba_inventory_planning`
    must return dataframe or error string
    dataframe columns to return: date, sku, asin, Inventory_Supply_at_FBA renamed as "amz_inventory"
    """
    if to_print:
        print("Starting to run `get_amazon_inventory`")

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
        results["get_amazon_inventory"] = df
        return df
    except Exception as e:
        return pd.DataFrame([f"error happened: {e}"], columns=["Error"])


def get_wh_inventory(to_print) -> pd.DataFrame:
    """
    Sergey
    pull latest warehouse inventory including incoming containers from `mellanni-project-da.sellercloud.inventory_bins_partitioned`
    must return dataframe or error string
    dataframe columns to return: sku, wh_inventory, incoming_containers
    """
    if to_print:
        print("Starting to run `get_wh_inventory`")
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

        result = pd.merge(wh, incoming, how="outer", on="sku", validate="1:1")
        results["get_wh_inventory"] = result
        return result
    except Exception as e:
        return pd.DataFrame([f"error happened: {e}"], columns=["Error"])


def calculate_restock(include_events: bool) -> pd.DataFrame:
    """
    Ruslan
    1. calculate in-stock-rate for the period (amz_inventory)
    2. calculate average sales 180 days and 14 days
    3. calculate average combined as average of (average sales 180 days and 14 days)
    4. calculate units needed (min 0, avoid negative numbers) for 49 days
    combine two dataframes into one and output the following columns:
        asin, average_sales_180, average_sales_14, average_combined, isr, amz_inventory (latest), wh_inventory (latest), units_to_ship
    """

    threads = []
    threads.append(threading.Thread(target=get_amazon_sales, args=(True,)))
    threads.append(threading.Thread(target=get_wh_inventory, args=(True,)))
    threads.append(threading.Thread(target=get_amazon_inventory, args=(True,)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # amazon_sales = get_amazon_sales()
    amazon_sales = results["get_amazon_sales"]
    amazon_sales["date"] = pd.to_datetime(amazon_sales["date"])
    # wh_inventory = get_wh_inventory()
    wh_inventory = results["get_wh_inventory"]
    # amazon_inventory = get_amazon_inventory()
    amazon_inventory = results["get_amazon_inventory"]
    # amazon_inventory["date"] = pd.to_datetime(amazon_inventory["date"])

    asin_isr = calculate_inventory_isr(
        amazon_inventory[["date", "asin", "amz_inventory"]].copy()
    )

    def fill_dates(amazon_sales: pd.DataFrame):
        start_date = amazon_sales["date"].min()
        end_date = amazon_sales["date"].max()
        date_range = pd.date_range(start=start_date, end=end_date)
        full_dates = pd.DataFrame(date_range, columns=["date"])
        asin_list = amazon_sales["asin"].unique()

        all_files = []
        for asin in asin_list:
            temp_df = amazon_sales[amazon_sales["asin"] == asin]
            full_asin = pd.merge(full_dates, temp_df, how="left", on="date")
            full_asin["asin"] = asin
            all_files.append(full_asin)
        all_sales = pd.concat(all_files)
        all_sales = all_sales.fillna(0)
        return all_sales

    total_sales = get_asin_sales(amazon_sales, asin_isr, include_events=include_events)

    # add event performance
    nearest_event, days_to_event, event_duration = get_event_days_delta()

    event_df = get_event_spreadsheet(nearest_event)

    forecast = pd.merge(total_sales, event_df, how="left", on="asin", validate="1:1")
    outside_event_sales = forecast["avg units"] * (days_to_event + 49)
    during_event_sales = np.where(  # =IF(H2>=3,"more than 3","less than 3")
        forecast["avg units"] >= 3,  # condition
        ((forecast["avg units"] * event_duration * forecast["best event performance"]) + (forecast["average event sales 1 day"] * event_duration)) / 2,  # if condition is true # =AVERAGE(J2*4,IF(H2>=3,H2*K2*4))
        forecast["avg units"] * event_duration * 2,  # if condition is false
    )

    forecast["total units needed"] = outside_event_sales + during_event_sales

    # result = pd.merge(asin_isr, total_sales, on="asin", how="outer")
    # result["in-stock-rate"] = result["in-stock-rate"].fillna(0)
    # result["unit_sales"] = result["unit_sales"].fillna(0)
    # result.rename(columns={"unit_sales": "unit_sales_180"}, inplace=True)
    # result["average_sales_180"] = (
    #     result["average_sales_180"] / result["in-stock-rate"]
    # ).round(3)
    # result["average_sales_180"] = result["average_sales_180"].fillna(0)
    # result["average_sales_14"] = result["average_sales_14"].fillna(0)
    # result["average_combined"] = (
    #     result["average_sales_180"] * 0.4 + result["average_sales_14"] * 0.6
    # ).round(3)

    # #### call event function
    # event, days_to_event, event_duration = get_event_days_delta()
    # event_spreadsheet = get_event_spreadsheet(event)
    # for column in event_spreadsheet.columns.tolist()[2:]:
    #     event_spreadsheet.loc[event_spreadsheet[column] == "", column] = 0
    #     try:
    #         event_spreadsheet[column] = event_spreadsheet[column].astype(float)
    #     except Exception as e:
    #         print(f"Ran into an error: {e}")
    #         pass
    # # event_spreadsheet = event_spreadsheet.fillna(0)
    # event_asin_performance = (
    #     event_spreadsheet.iloc[:, [0, 2, 3]].groupby("ASIN").agg("max").reset_index()
    # )
    # event_asin_performance = pd.merge(
    #     result[["asin", "average_combined"]],
    #     event_asin_performance,
    #     how="outer",
    #     left_on="asin",
    #     right_on="ASIN",
    #     validate="1:1",
    # )

    # #############################################TODO

    # amazon_inventory_copy = amazon_inventory.copy()
    # latest_date = amazon_inventory_copy["date"].max()
    # latest_inventory = amazon_inventory_copy[
    #     amazon_inventory_copy["date"] == latest_date
    # ]
    # quantity_map = latest_inventory.groupby("asin")["amz_inventory"].sum().reset_index()
    # result = pd.merge(result, quantity_map, on="asin", how="outer")
    # result["amz_inventory"] = result["amz_inventory"].fillna(0)
    # result["days_of_sale_remaining"] = (
    #     result["amz_inventory"] / result["average_combined"]
    # ).round(0)
    # result["days_of_sale_remaining"] = result["days_of_sale_remaining"].fillna(0)

    # result["units_to_ship"] = (
    #     (result["average_combined"] * days_of_sale - result["amz_inventory"]).round(0)
    # ).apply(lambda x: x if x > 0 else 0)
    # result = result[
    #     [
    #         "asin",
    #         "sku",
    #         "in-stock-rate",
    #         "unit_sales_180",
    #         "average_sales_180",
    #         "average_sales_14",
    #         "average_combined",
    #         "amz_inventory",
    #         "days_of_sale_remaining",
    #         "units_to_ship",
    #         "wh_inventory",
    #         "incoming_containers",
    #     ]
    # ]
    mm.export_to_excel(
        [forecast], ["restock"], "inventory_restock.xlsx", user_folder
    )
    mm.open_file_folder(os.path.join(user_folder))
    return forecast


if __name__ == "__main__":
    calculate_restock(include_events=False)
