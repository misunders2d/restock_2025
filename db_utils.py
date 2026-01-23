import pandas as pd
from connectors import gdrive as gd
from connectors import gcloud as gc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import size_match

EVENT_SPREADSHEET_ID = "1_gSk2xSDuyEQ9qzI15NJBxVCBZSJMuTKS1pDsvnfes8"  # google spreadsheet with events data


def get_event_spreadsheet(output: dict, to_print: bool = False) -> pd.DataFrame | None:
    """
    Pull event_spreadsheet from google drive and return only the event columns.
    If event is not specified, return the full spreadsheet.
    If full_spreadsheet is provided, skip pulling data from google drive and only limit the columns to event columns.

    """
    if to_print:
        print("Starting to run `get_event_spreadsheet`")

    try:
        full_spreadsheet = gd.download_gspread(spreadsheet_id=EVENT_SPREADSHEET_ID)
        output["get_event_spreadsheet"] = full_spreadsheet
        if to_print:
            print("Saved data to results `get_event_spreadsheet`")
        return full_spreadsheet
    except Exception as e:
        raise BaseException(f"Error happened: {e}")


def get_amazon_sales(
    output: dict,
    to_print: bool = False,
    num_days: int = 180,
    max_date: str | None = None,
) -> pd.DataFrame | None:
    """
    Bohdan
    pull sales for last `num_days` days excluding Prime Day for US market from `mellanni-project-da.reports.all_orders`. group by days.
    must return dataframe or error string
    dataframe columns to return: date, asin, unit_sales, dollar_sales
    """
    MAX_DATE = "CURRENT_DATE()" if not max_date else f'"{max_date}"'
    if to_print:
        print("Starting to run `get_amazon_sales`")
    query = f"""
        SELECT
            CAST(DATETIME(purchase_date, "America/Los_Angeles") AS DATE) AS date,
            sku,
            asin,
            SUM(quantity) AS unit_sales,
            SUM(item_price) AS dollar_sales
        FROM
            `mellanni-project-da.reports.all_orders`
        WHERE
            CAST(DATETIME(purchase_date, "America/Los_Angeles") AS DATE) BETWEEN DATE_SUB({MAX_DATE}, INTERVAL {num_days + 90} DAY) AND {MAX_DATE}
            AND sales_channel = 'Amazon.com'
        GROUP BY
            date, sku, asin
        ORDER BY
            date, sku, asin
    """
    try:
        with gc.gcloud_connect() as client:
            result = client.query(query).to_dataframe()
        output["get_amazon_sales"] = result
        if to_print:
            print("Saved data to results `get_amazon_sales`")
        return result
    except Exception as e:
        raise BaseException(f"error happened: {e}")


def get_amazon_inventory(
    output: dict,
    to_print: bool = False,
    num_days: int = 180,
    max_date: str | None = None,
) -> pd.DataFrame | None:
    """
    Vitalii
    pull inventory history for last `num_days` days for all skus in US from `mellanni-project-da.reports.fba_inventory_planning`
    must return dataframe or error string
    dataframe columns to return: date, sku, asin, Inventory_Supply_at_FBA renamed as "amz_inventory"
    """
    if to_print:
        print("Starting to run `get_amazon_inventory`")
    MAX_DATE = "CURRENT_DATE()" if not max_date else f'"{max_date}"'

    query = f"""
        SELECT
            DATE(snapshot_date) AS date,
            sku,
            asin,
            available as amz_available,
            Inventory_Supply_at_FBA AS amz_inventory
        FROM
            `mellanni-project-da.reports.fba_inventory_planning`
        WHERE
            marketplace = 'US'
            AND DATE(snapshot_date) BETWEEN DATE_SUB({MAX_DATE}, INTERVAL {num_days} DAY) AND {MAX_DATE}
        ORDER BY
            date DESC, sku ASC
    """

    try:
        with gc.gcloud_connect() as client:
            df = client.query(query).to_dataframe()
        output["get_amazon_inventory"] = df
        if to_print:
            print("Saved data to results `get_amazon_inventory`")
        return df
    except Exception as e:
        raise BaseException(f"error happened: {e}")


def get_wh_inventory(output: dict, to_print: bool = False) -> pd.DataFrame | None:
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
            Items.SKU AS sku,
            sum(Items.QtyOrdered) as incoming_containers
        FROM
            `mellanni-project-da.sellercloud.purchase_orders` AS t1,
            UNNEST(t1.Items) AS items
        WHERE
            date(t1.ExpectedDeliveryDate) >= CURRENT_DATE()
        GROUP BY Items.SKU
        ORDER BY
            incoming_containers
            DESC
    """

    incoming_weeks_query = """
        SELECT
            ExpectedDeliveryDate as eta,
            Items as items
        FROM
            `mellanni-project-da.sellercloud.purchase_orders`
        WHERE
            date(ExpectedDeliveryDate) >= CURRENT_DATE()
        ORDER BY
            eta
            ASC
    """
    try:
        with gc.gcloud_connect() as client:
            wh_job = client.query(wh_query)
            incoming_job = client.query(incoming_query)
            incoming_weeks_job = client.query(incoming_weeks_query)
        wh = wh_job.to_dataframe()
        incoming = incoming_job.to_dataframe()
        incoming_weeks = incoming_weeks_job.to_dataframe()

        result = pd.merge(wh, incoming, how="outer", on="sku", validate="1:1")
        output["get_wh_inventory"] = result
        output["incoming_weeks"] = incoming_weeks
        if to_print:
            print("Saved data to results `get_wh_inventory`")
        return result
    except Exception as e:
        raise BaseException(f"error happened: {e}")


def get_dictionary(output: dict, to_print: bool = False) -> pd.DataFrame | None:
    try:
        if to_print:
            print("Starting to run `get_dictionary`")
        # dictionary_obj = gd.download_file(file_id="1RzO_OLIrvgtXYeGUncELyFgG-jJdCheB")
        # dictionary = pd.read_excel(
        #     dictionary_obj,
        #     usecols=[
        #         "SKU",
        #         "ASIN",
        #         "Collection",
        #         "Size",
        #         "Color",
        #         "Actuality",
        #         "Life stage",
        #         "Restockable",
        #     ],
        # )
        dictionary = gd.download_gspread(
            spreadsheet_id="1Y4XhSBCXqmEVHHOnugEpzZZ3NQ5ZRGOlp-AsTE0KmRE",
            sheet_id="449289593",
        )
        dictionary = dictionary[
            [
                "SKU",
                "ASIN",
                "Collection",
                "Size",
                "Color",
                "Actuality",
                "Life stage",
                "Restockable",
            ]
        ]

        dictionary.columns = [x.lower() for x in dictionary.columns]
        output["get_dictionary"] = dictionary
        if to_print:
            print("Saved data to results `get_dictionary`")
        return dictionary
    except Exception as e:
        raise BaseException(f"error happened: {e}")


def pull_data(num_days, max_date=None):
    results = dict()
    date_kwargs = {"to_print": True, "output": results, "num_days": num_days}
    if max_date:
        date_kwargs["max_date"] = max_date
    kwargs = {"to_print": True, "output": results}

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(get_amazon_sales, **date_kwargs): "get_amazon_sales",
            executor.submit(get_wh_inventory, **kwargs): "get_wh_inventory",
            executor.submit(
                get_amazon_inventory, **date_kwargs
            ): "get_amazon_inventory",
            executor.submit(get_event_spreadsheet, **kwargs): "get_event_spreadsheet",
            executor.submit(get_dictionary, **kwargs): "get_dictionary",
            executor.submit(size_match.main, out=False): "size_match",
        }
        for future in as_completed(futures):
            func_name = futures[future]
            try:
                result = future.result()
                if func_name == "size_match":
                    print(f"Received {func_name} results, saving to dict")
                    results[func_name] = result
            except Exception as e:
                raise BaseException(f"Failed to pull data for {func_name}: {e}")
    return results


def pull_data_old(num_days, max_date=None):
    results = dict()
    date_kwargs = {"to_print": True, "output": results, "num_days": num_days}
    if max_date:
        date_kwargs["max_date"] = max_date
    kwargs = {"to_print": True, "output": results}

    threads = []
    threads.append(
        threading.Thread(
            target=get_amazon_sales,
            kwargs=date_kwargs,
        )
    )
    threads.append(threading.Thread(target=get_wh_inventory, kwargs=kwargs))
    threads.append(
        threading.Thread(
            target=get_amazon_inventory,
            kwargs=date_kwargs,
        )
    )
    threads.append(threading.Thread(target=get_event_spreadsheet, kwargs=kwargs))

    threads.append(threading.Thread(target=get_dictionary, kwargs=kwargs))
    threads.append(threading.Thread(target=size_match.main, kwargs={"out": False}))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    return results
