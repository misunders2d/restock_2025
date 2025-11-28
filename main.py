import pandas as pd
import os

from connectors import gdrive as gd
from utils import mellanni_modules as mm
from utils import size_match
from restock_utils import (
    calculate_inventory_isr,
    get_asin_sales,
    calculate_event_forecast,
    calculate_amazon_inventory
)
from db_utils import pull_data
from date_utils import get_event_days_delta

STANDARD_DAYS_OF_SALE = 49

user_folder = os.path.join(os.path.expanduser("~"), "temp")
os.makedirs(user_folder, exist_ok=True)


def calculate_restock(include_events: bool, num_days: int = 180) -> pd.DataFrame:
    """
    Ruslan
    1. calculate in-stock-rate for the period (amz_inventory)
    2. calculate average sales LONG_TERM and SHORT_TERM (180 days and 14 days)
    3. calculate average combined as average of (average sales 180 days and 14 days)
    4. calculate units needed (min 0, avoid negative numbers) for 49 days
    combine two dataframes into one and output the following columns:
        asin, average_sales_180, average_sales_14, average_combined, isr, amz_inventory (latest), wh_inventory (latest), units_to_ship
    """

    results = pull_data(num_days=num_days)

    amazon_sales = results["get_amazon_sales"]
    amazon_sales["date"] = pd.to_datetime(amazon_sales["date"])

    wh_inventory = results["get_wh_inventory"]

    amazon_inventory = results["get_amazon_inventory"]

    asin_isr = calculate_inventory_isr(
        amazon_inventory[["date", "asin", "amz_inventory"]].copy()
    )

    total_sales = get_asin_sales(
        amazon_sales, asin_isr, include_events=include_events, long_term_days=num_days
    )

    # add event performance
    full_event_spreadsheet = results["get_event_spreadsheet"]

    nearest_event, days_to_event, event_duration = get_event_days_delta()

    event_forecast = calculate_event_forecast(
        total_sales=total_sales,
        full_event_df=full_event_spreadsheet,
        event=nearest_event,
    )
    forecast = pd.merge(
        total_sales, event_forecast, how="outer", on="asin", validate="1:1"
    )

    calculated_days_to_event = 0 if days_to_event > 90 else days_to_event

    outside_event_sales = forecast["avg units"] * (
        calculated_days_to_event + STANDARD_DAYS_OF_SALE
    )

    total_units_needed = outside_event_sales if days_to_event > 90 else outside_event_sales + forecast[f"{nearest_event}_forecasted_sales"]
    forecast["total units needed"] = total_units_needed

    asin_inventory = calculate_amazon_inventory(amazon_inventory)

    forecast = pd.merge(forecast, asin_inventory, how = 'left', on = 'asin', validate = '1:1').fillna(0)

    forecast['to_ship_units'] = (forecast['total units needed'] - forecast['amz_inventory']).clip(0)

    dimensions = size_match.main(out = False)
    dimensions = dimensions[['asin','sets in a box']]
    dimensions = dimensions.drop_duplicates('asin')
    forecast = pd.merge(forecast, dimensions, how = 'left', on = 'asin', validate='1:1')
    forecast['to_ship_boxes'] = (forecast['to_ship_units'] / forecast['sets in a box']).round(0)

    dictionary_obj = gd.download_file(file_id = '1RzO_OLIrvgtXYeGUncELyFgG-jJdCheB')
    dictionary = pd.read_excel(dictionary_obj)
    dictionary.columns = [x.lower() for x in dictionary.columns]
    dictionary = dictionary[['sku','asin', 'life stage', 'restockable']]

    wh_inventory = pd.merge(wh_inventory, dictionary[['sku','asin','life stage', 'restockable']], how = 'left', on = 'sku', validate = '1:1')
    asin_wh_inventory = wh_inventory.groupby('asin').agg(
        {
            "wh_inventory":"sum",
            "incoming_containers":"sum",
            "sku":lambda x: ', '.join(x.unique()),
            "life stage":lambda x: ', '.join(x.unique()),
            "restockable":lambda x: ', '.join(x.unique())
        }
        ).reset_index()
    
    forecast = pd.merge(forecast, asin_wh_inventory, how = 'outer', on = 'asin', validate = '1:1')

    mm.export_to_excel([forecast], ["restock"], "inventory_restock.xlsx", user_folder)
    mm.open_file_folder(os.path.join(user_folder))
    return forecast


if __name__ == "__main__":
    calculate_restock(include_events=False, num_days=180)
