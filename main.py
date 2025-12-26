import pandas as pd
import os
from tkinter import messagebox

from connectors import gdrive as gd
from utils import mellanni_modules as mm
from utils import size_match
from restock_utils import (
    calculate_inventory_isr,
    get_asin_sales,
    calculate_event_forecast,
    calculate_amazon_inventory,
)
from db_utils import pull_data
from date_utils import get_event_days_delta

STANDARD_DAYS_OF_SALE = 49


user_folder = os.path.join(os.path.expanduser("~"), "temp")
os.makedirs(user_folder, exist_ok=True)


def calculate_restock(
    include_events: bool, num_days: int = 180, max_date: str | None = None
):
    """
    Ruslan
    1. calculate in-stock-rate for the period (amz_inventory)
    2. calculate average sales LONG_TERM and SHORT_TERM (180 days and 14 days)
    3. calculate average combined as average of (average sales 180 days and 14 days)
    4. calculate units needed (min 0, avoid negative numbers) for 49 days
    combine two dataframes into one and output the following columns:
        asin, average_sales_180, average_sales_14, average_combined, isr, amz_inventory (latest), wh_inventory (latest), units_to_ship
    """

    results = pull_data(num_days=num_days, max_date=max_date)

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

    HARD_COLUMNS = [
        "asin",
        "ISR",
        "ISR_short",
        "avg sales dollar, 14 days",
        "avg sales units, 14 days",
        "avg sales dollar, 180 days",
        "avg sales units, 180 days",
        "avg units",
        "avg $",
        "Average BSS sales, units (total)",
        "Best BSS performance",
        "BSS_forecasted_sales",
        "total units needed",
        "amz_inventory",
        "amz_available",
        "to_ship_units",
        "dos_available",
        "dos_inbound",
        "sets in a box",
        "to_ship_boxes",
        "dos_shipped",
        "wh_inventory",
        "incoming_containers",
        "sku",
        "life stage",
        "restockable",
        "collection",
        "size",
        "color",
    ]

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

    total_units_needed = (
        outside_event_sales
        if days_to_event > 90
        else outside_event_sales + forecast[f"{nearest_event}_forecasted_sales"]
    )
    forecast["total units needed"] = total_units_needed

    asin_inventory = calculate_amazon_inventory(amazon_inventory)

    forecast = pd.merge(
        forecast, asin_inventory, how="left", on="asin", validate="1:1"
    ).fillna(0)

    forecast["to_ship_units"] = (
        (forecast["total units needed"] - forecast["amz_inventory"]).clip(0).round(0)
    )

    forecast["dos_available"] = forecast["amz_available"] / forecast["avg units"]
    forecast["dos_inbound"] = forecast["amz_inventory"] / forecast["avg units"]

    dimensions = size_match.main(out=False)
    dimensions = dimensions[["asin", "sets in a box"]]
    dimensions = dimensions.drop_duplicates("asin")
    forecast = pd.merge(forecast, dimensions, how="left", on="asin", validate="1:1")
    forecast["to_ship_boxes"] = (
        forecast["to_ship_units"] / forecast["sets in a box"]
    ).round(0)

    # forecast['dos_shipped'] = (forecast['to_ship_boxes'] * forecast['sets in a box'] + forecast['amz_inventory'])/ forecast["avg units"]
    forecast["dos_shipped"] = "=(T:T*S:S+N:N)/H:H"

    dictionary = results["get_dictionary"]
    # dictionary_obj = gd.download_file(file_id="1RzO_OLIrvgtXYeGUncELyFgG-jJdCheB")
    # dictionary = pd.read_excel(dictionary_obj)
    dictionary.columns = [x.lower().strip() for x in dictionary.columns]
    dictionary = dictionary[
        ["sku", "asin", "life stage", "restockable", "collection", "size", "color"]
    ]

    wh_inventory = pd.merge(
        wh_inventory,
        dictionary,
        how="left",
        on="sku",
        validate="1:1",
    )
    asin_wh_inventory = (
        wh_inventory.groupby("asin")
        .agg(
            {
                "wh_inventory": "sum",
                "incoming_containers": "sum",
                "sku": lambda x: ", ".join(sorted(x.unique())),
                "life stage": lambda x: ", ".join(sorted(x.unique())),
                "restockable": lambda x: ", ".join(sorted(x.unique())),
                "collection": lambda x: ", ".join(sorted(x.unique())),
                "size": lambda x: ", ".join(sorted(x.unique())),
                "color": lambda x: ", ".join(sorted(x.unique())),
            }
        )
        .reset_index()
    )

    forecast = pd.merge(
        forecast, asin_wh_inventory, how="outer", on="asin", validate="1:1"
    )

    forecast[["to_ship_units", "amz_inventory", "wh_inventory"]] = forecast[
        ["to_ship_units", "amz_inventory", "wh_inventory"]
    ].fillna(0)

    forecast.loc[
        (forecast["to_ship_units"] == 0)
        & (forecast["amz_inventory"] == 0)
        & (forecast["wh_inventory"] > 0),
        ["to_ship_units", "to_ship_boxes"],
    ] = 1

    if not forecast.columns.tolist() == HARD_COLUMNS:
        # raise BaseException("Columns don't match, don't forget to change Excel formula in 'dos_shipped' column")
        messagebox.showwarning(
            title="Warning",
            message="Columns don't match, don't forget to change Excel formula in 'dos_shipped' column",
        )

    mm.export_to_excel([forecast], ["restock"], "inventory_restock.xlsx", user_folder)
    mm.open_file_folder(os.path.join(user_folder))
    return forecast, results


if __name__ == "__main__":
    forecast, results = calculate_restock(include_events=False, num_days=180)


# TODO think about limiting the number of days to account for depending on the event
