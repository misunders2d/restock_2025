import pandas as pd
import os
import sys
from tkinter import messagebox
from datetime import timedelta

from utils import mellanni_modules as mm
from utils_misc import create_column_formatting

from restock_utils import (
    calculate_inventory_isr,
    get_asin_sales,
    calculate_event_forecast,
    calculate_amazon_inventory,
    group_incoming_by_weeks,
)
from db_utils import pull_data
from date_utils import get_event_days_delta

STANDARD_DAYS_OF_SALE = 49


user_folder = os.path.join(os.path.expanduser("~"), "temp")
os.makedirs(user_folder, exist_ok=True)


def calculate_restock(
    include_events: bool,
    num_days: int = 180,
    max_date: str | None = None,
    num_short_term_days=14,
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

    # prepare data block###################
    results = pull_data(num_days=num_days, max_date=max_date)

    amazon_sales_full = results["get_amazon_sales"]
    amazon_sales_full["date"] = pd.to_datetime(amazon_sales_full["date"])
    amazon_sales = (
        amazon_sales_full.groupby(["date", "asin"])
        .agg({"unit_sales": "sum", "dollar_sales": "sum"})
        .reset_index()
    )

    wh_inventory = results["get_wh_inventory"]
    amazon_inventory = results["get_amazon_inventory"]
    full_event_spreadsheet = results["get_event_spreadsheet"]
    dictionary = results["get_dictionary"]
    dimensions = results["size_match"]
    incoming_weeks_raw = results["incoming_weeks"]

    incoming_weeks = group_incoming_by_weeks(incoming_weeks_raw)
    incoming_weeks = incoming_weeks.rename(columns={"SKU": "sku"})
    # end of prepare data block#############

    # prepare total sales block############3
    max_sales_date = amazon_sales["date"].max()
    today = pd.to_datetime("today")
    if max_sales_date.date() == today.date():
        max_sales_date = max_sales_date - timedelta(days=1)
    max_sales_date_str = max_sales_date.strftime("%m-%d")
    latest_sales = amazon_sales[amazon_sales["date"] == max_sales_date][
        ["asin", "unit_sales"]
    ]
    latest_sales = latest_sales.rename(
        columns={"unit_sales": f"{max_sales_date_str} sales"}
    )

    asin_isr = calculate_inventory_isr(
        amazon_inventory[["date", "asin", "amz_inventory"]].copy()
    )

    sku_isr = calculate_inventory_isr(
        amazon_inventory[["date", "sku", "amz_inventory"]].copy(), col_to_use="sku"
    )

    total_sales = get_asin_sales(
        amazon_sales,
        asin_isr,
        include_events=include_events,
        long_term_days=num_days,
        short_term_days=num_short_term_days,
    )
    total_sales = pd.merge(
        total_sales, latest_sales, how="outer", on="asin", validate="1:1"
    )
    total_sales[f"{max_sales_date_str} sales"] = total_sales[
        f"{max_sales_date_str} sales"
    ].fillna(0)
    # end of prepare total sales block#####

    # prepare wh inventory block#########
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

    wh_inventory["sku_mapping"] = (
        wh_inventory["sku"].astype(str) + ":" + wh_inventory["restockable"].astype(str)
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
                "sku_mapping": lambda x: ", ".join(sorted(x.unique())),
            }
        )
        .reset_index()
    )

    nearest_event, days_to_event, _ = get_event_days_delta()

    event_forecast = calculate_event_forecast(
        total_sales=total_sales,
        full_event_df=full_event_spreadsheet,
        event=nearest_event,
    )

    forecast = pd.merge(
        total_sales, event_forecast, how="outer", on="asin", validate="1:1"
    )

    days_threshold = 45 if nearest_event == "BSS" else 90

    calculated_days_to_event = 0 if days_to_event > days_threshold else days_to_event
    outside_event_sales = forecast["avg units"] * (
        calculated_days_to_event + STANDARD_DAYS_OF_SALE
    )

    total_units_needed = (
        outside_event_sales
        if days_to_event > days_threshold
        else outside_event_sales + forecast[f"{nearest_event}_forecasted_sales"]
    )
    forecast["total units needed"] = total_units_needed

    asin_inventory = calculate_amazon_inventory(amazon_inventory)
    sku_inventory = calculate_amazon_inventory(
        amazon_inventory, col_to_use="sku", show_warning=False
    )

    forecast = pd.merge(
        forecast, asin_inventory, how="outer", on="asin", validate="1:1"
    )
    non_date_cols = [x for x in forecast.columns if x != "date"]
    forecast[non_date_cols] = forecast[non_date_cols].fillna(0)

    forecast["to_ship_units"] = (
        (forecast["total units needed"] - forecast["amz_inventory"]).clip(0).round(0)
    )

    forecast["dos_available"] = forecast["amz_available"] / forecast["avg units"]
    forecast["dos_inbound"] = forecast["amz_inventory"] / forecast["avg units"]

    forecast["dos_shipped"] = ""

    # lost sales calculations
    forecast["avg price"] = forecast["avg $"] / forecast["avg units"]
    max_inventory_sales = forecast[
        ["amz_inventory", f"{max_sales_date_str} sales"]
    ].max(axis=1)
    min_inventory_sales = forecast[
        ["amz_available", f"{max_sales_date_str} sales"]
    ].max(axis=1)

    forecast["lost sales min"] = (forecast["avg units"] - max_inventory_sales).clip(
        0
    ) * forecast["avg price"]
    forecast["lost sales max"] = (forecast["avg units"] - min_inventory_sales).clip(
        0
    ) * forecast["avg price"]

    dimensions = dimensions[["asin", "sets in a box"]]
    dimensions = dimensions.drop_duplicates("asin")
    forecast = pd.merge(forecast, dimensions, how="left", on="asin", validate="1:1")
    forecast["to_ship_boxes"] = (
        forecast["to_ship_units"] / forecast["sets in a box"]
    ).round(0)

    # end of prepare wh inventory block#####

    forecast = pd.merge(
        forecast, asin_wh_inventory, how="outer", on="asin", validate="1:1"
    )

    forecast[["to_ship_units", "amz_inventory", "wh_inventory"]] = forecast[
        ["to_ship_units", "amz_inventory", "wh_inventory"]
    ].fillna(0)

    forecast.loc[
        (forecast["life stage"] == "Discontinued") & (forecast["wh_inventory"] == 0),
        ["lost sales min", "lost sales max"],
    ] = 0

    forecast.loc[
        (forecast["to_ship_units"] == 0)
        & (forecast["amz_inventory"] == 0)
        & (forecast["wh_inventory"] > 0),
        ["to_ship_units", "to_ship_boxes"],
    ] = 1

    HARD_COLUMNS = [
        "asin",
        "ISR",
        "ISR_short",
        f"avg sales dollar, {num_short_term_days} days",
        f"avg sales units, {num_short_term_days} days",
        f"avg sales dollar, {num_days} days",
        f"avg sales units, {num_days} days",
        "avg units",
        "avg $",
        f"{max_sales_date_str} sales",
        f"Average {nearest_event} sales, units (total)",
        f"Best {nearest_event} performance",
        f"{nearest_event}_forecasted_sales",
        "total units needed",
        "amz_inventory",
        "amz_available",
        "to_ship_units",
        "dos_available",
        "dos_inbound",
        "dos_shipped",
        "avg price",
        "lost sales min",
        "lost sales max",
        "sets in a box",
        "to_ship_boxes",
        "wh_inventory",
        "incoming_containers",
        "sku",
        "life stage",
        "restockable",
        "collection",
        "size",
        "color",
        "sku_mapping",
        "date",
        "alert",
        "recommended_action",
        "healthy_inventory_level",
        "recommended_removal_quantity",
        "estimated_excess_quantity",
        "fba_minimum_inventory_level",
        "fba_inventory_level_health_status",
        "storage_type",
    ]

    forecast = forecast.loc[:, HARD_COLUMNS]
    forecast["dos_shipped"] = "=(Y:Y*X:X+O:O)/H:H"
    file_date = pd.to_datetime("today").strftime("%Y-%m-%d")

    forecast["date"] = file_date
    forecast_columns = forecast.columns.tolist()
    if not forecast_columns == HARD_COLUMNS:
        # raise BaseException("Columns don't match, don't forget to change Excel formula in 'dos_shipped' column")
        mismatched_cols = ", ".join(
            [x for x in forecast_columns if x not in HARD_COLUMNS]
        )
        messagebox.showwarning(
            title="Warning",
            message=f"Columns don't match, don't forget to change Excel formula in 'dos_shipped' column: {mismatched_cols}",
        )

    forecast["asin"] = (
        '=HYPERLINK("https://www.amazon.com/dp/'
        + forecast["asin"].astype(str)
        + '","'
        + forecast["asin"].astype(str)
        + '")'
    )

    sku_results = pd.merge(
        sku_inventory, wh_inventory, how="outer", on="sku", validate="1:1"
    )
    sku_results = pd.merge(sku_results, sku_isr, how="outer", on="sku", validate="1:1")
    sku_results = pd.merge(
        sku_results, incoming_weeks, how="outer", on="sku", validate="1:1"
    )

    mm.export_to_excel(
        dfs=[forecast, sku_results],
        sheet_names=["restock", "sku_inventory"],
        filename=f"inventory_restock_{file_date}.xlsx",
        out_folder=user_folder,
        column_formats=create_column_formatting(),
    )
    mm.open_file_folder(os.path.join(user_folder))
    return forecast, results


if __name__ == "__main__":
    max_date = None
    if len(sys.argv) > 1:
        max_date = sys.argv[1]
    forecast, results = calculate_restock(
        include_events=False, num_days=180, max_date=max_date
    )


# remove fbm only, do not ship to amazon from Lost sales
