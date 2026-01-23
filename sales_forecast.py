import pandas as pd
import numpy as np
from db_utils import get_amazon_sales
from date_utils import is_event, events
from restock_utils import calculate_event_forecast
from common import event_dates_margins_list, user_folder
from utils import mellanni_modules as mm
from typing import Literal
from main import calculate_restock
import threading
import time

FORECAST_YEAR = 2026
stop = False


def print_threaded():
    phrase = "Please wait, saving to Excel..."
    visible = ""
    while not stop:
        for char in phrase:
            if stop:
                print()
                break
            visible += char
            time.sleep(0.05)
            print(visible, end="\r")
        visible = ""
        print()
        time.sleep(1)


# def main(stack=False):
def main(
    stack: Literal["stacked", "daily", "yearly", "last_year"] = "stacked",
    max_date: str | None = None,
):
    """
    "stacked" - forecast with daily breakdown stacked in single column
    "daily" - forecast with daily breakdown in separate columns
    "yearly" - forecast with yearly totals only
    "last_year" - forecast based on last year's numbers
    """
    global stop
    result = {}

    get_amazon_sales(output=result, to_print=True, num_days=20000, max_date=max_date)
    full_sales = result["get_amazon_sales"].copy()
    full_sales = full_sales[["date", "sku", "asin", "unit_sales", "dollar_sales"]]
    daily_sales = (
        full_sales[["date", "unit_sales"]].groupby("date").agg("sum").reset_index()
    )
    non_event_sales = daily_sales[
        ~daily_sales["date"].isin(event_dates_margins_list)
    ].copy()

    non_event_sales["avg"] = non_event_sales["unit_sales"].rolling(window=180).mean()
    non_event_sales["coeff_raw"] = (
        non_event_sales["unit_sales"] / non_event_sales["avg"]
    )
    non_event_sales["coeff"] = non_event_sales["coeff_raw"].rolling(window=3).mean()

    non_event_sales = non_event_sales[
        non_event_sales["date"].between(
            pd.to_datetime("2023-01-01").date(),
            pd.to_datetime("today"),
            inclusive="left",
        )
    ]

    averages = {}
    for date in non_event_sales["date"].unique():
        coeff = (
            non_event_sales[non_event_sales["date"] == date].loc[:, "coeff"].values[0]
        )
        month, day = date.month, date.day
        if (month, day) not in averages:
            averages[(month, day)] = [coeff]
        else:
            averages[(month, day)].append(coeff)

    for key, value in averages.items():
        averages[key] = np.mean(value)

    def get_nearest_date(date):
        print(f"Checking date: {date}")
        month, day = date.month, date.day
        while (month, day) not in averages:
            print(f"date {month}-{day} not found, incrementing day.")
            if day < 31:
                day += 1
            else:
                month += 1 if month < 12 else 1
                day = 1
        return month, day

    current_restock, results = calculate_restock(
        include_events=False, num_days=365, max_date=max_date
    )
    forecast = current_restock[["asin", "avg units"]].copy()
    forecast["asin"] = forecast["asin"].str.extract(r"(B\w{9})")
    forecast["avg price"] = current_restock["avg $"] / current_restock["avg units"]
    wh_inventory = results["get_wh_inventory"]
    wh_dictionary = results["get_dictionary"][
        [
            "asin",
            "sku",
        ]
    ]
    wh_inventory = pd.merge(
        wh_inventory,
        wh_dictionary,
        how="left",
        on="sku",
        validate="m:1",
    )
    wh_inventory = (
        wh_inventory.groupby("asin")
        .agg({"wh_inventory": "sum", "incoming_containers": "sum"})
        .reset_index()
    )
    amazon_inventory = results["get_amazon_inventory"]
    amazon_inventory = (
        amazon_inventory.groupby(["date", "asin"])
        .agg({"amz_inventory": "sum"})
        .reset_index()
    )
    amazon_inventory = amazon_inventory.sort_values("date", ascending=False)
    amazon_inventory = (
        amazon_inventory.groupby("asin").agg({"amz_inventory": "first"}).reset_index()
    )

    total_inventory = pd.merge(
        wh_inventory,
        amazon_inventory,
        how="outer",
        on="asin",
        validate="1:1",
    ).fillna(0)
    total_inventory["total_inventory"] = total_inventory.sum(axis=1, numeric_only=True)
    forecast = pd.merge(
        forecast,
        total_inventory[["asin", "total_inventory"]],
        how="left",
        on="asin",
        validate="1:1",
    ).fillna(0)

    future_date_range = pd.date_range(
        start=(
            (pd.to_datetime("today")).date()
            if not max_date
            else pd.to_datetime(max_date).date()
        ),
        end=(pd.to_datetime("today") + pd.Timedelta(days=500)).date(),
    )
    life_stage_dictionary = results["get_dictionary"][
        ["asin", "life stage", "restockable"]
    ]
    life_stage_dictionary = (
        life_stage_dictionary.groupby("asin")
        .agg(lambda x: ", ".join(x.unique()))
        .reset_index()
    )

    forecast = pd.merge(
        forecast, life_stage_dictionary, how="left", on="asin", validate="1:1"
    )

    forecast_dollars = forecast.copy()

    if stack == "stacked":
        total = pd.DataFrame()
        for date in future_date_range:
            forecast["date"] = date.date()
            if event := is_event(date.year, date.month, date.day):
                event_forecast = calculate_event_forecast(
                    total_sales=forecast[["asin", "avg units"]],
                    full_event_df=results["get_event_spreadsheet"],
                    event=event,
                )
                forecast["event"] = event
                forecasted_units = (
                    event_forecast[f"{event}_forecasted_sales"]
                    / events[event]["duration"]
                )
            else:
                forecasted_units = (
                    forecast["avg units"] * averages[get_nearest_date(date)]
                )
                forecast["event"] = ""

            forecast["units"] = forecasted_units
            forecast.loc[
                (forecast["restockable"] == "Do not ship to amazon")
                | (forecast["life stage"] == "Discontinued"),
                "units",
            ] = forecast[["total_inventory", "units"]].min(axis=1)
            if not event:
                forecast["avg units"] = forecast["avg units"] * (179 / 180) + forecast[
                    "units"
                ] * (1 / 180)
            forecast["total_inventory"] = forecast["total_inventory"] - forecast[
                "units"
            ].clip(0)

            forecast["$"] = forecast["units"] * forecast["avg price"]

            total = pd.concat(
                [total, forecast[["asin", "date", "event", "units", "$"]]], axis=0
            )

        dictionary = results["get_dictionary"][
            [
                "asin",
                "collection",
                "size",
                "color",
                "actuality",
                "life stage",
                "restockable",
            ]
        ]
        dictionary = (
            dictionary.groupby("asin")
            .agg(lambda x: ", ".join(x.unique()))
            .reset_index()
        )
        total = pd.merge(dictionary, total, how="right", on="asin", validate="1:m")
        total = total[
            total["date"].between(
                pd.to_datetime(f"{FORECAST_YEAR}-01-01").date(),
                pd.to_datetime(f"{FORECAST_YEAR}-12-31").date(),
            )
        ]
        # import os
        # total.to_csv(os.path.join(user_folder, "sales_forecast_stack.csv"), index=False)

    elif stack == "daily":
        for date in future_date_range:
            forecast[date.date()] = (
                forecast["avg units"] * averages[get_nearest_date(date)]
            )

            forecast_dollars[date.date()] = (
                forecast[date.date()] * forecast_dollars["avg price"]
            )

            forecast["avg units"] = forecast["avg units"] * (179 / 180) + forecast[
                date.date()
            ] * (1 / 180)

    elif stack == "last_year":
        full_sales["date"] = pd.to_datetime(
            full_sales["date"],
            format="%Y-%m-%d",
            # unit='D'
        )
        previous_sales = full_sales[
            full_sales["date"].dt.year == FORECAST_YEAR - 1
        ].copy()
        previous_sales_asin = (
            previous_sales.groupby("asin").agg({"unit_sales": "sum"}).reset_index()
        )
        previous_sales_asin["lost_sales"] = ""
        dictionary = results["get_dictionary"]
        year_based_forecast = pd.merge(
            previous_sales_asin, total_inventory, how="outer", on="asin", validate="1:1"
        )
        dictionary = (
            dictionary.groupby("asin")
            .agg(
                {
                    "collection": lambda x: ", ".join(x.unique()),
                    "size": lambda x: ", ".join(x.unique()),
                    "color": lambda x: ", ".join(x.unique()),
                    "actuality": lambda x: ", ".join(x.unique()),
                    "life stage": lambda x: ", ".join(x.unique()),
                    "restockable": lambda x: ", ".join(x.unique()),
                }
            )
            .reset_index()
        )
        year_based_forecast = pd.merge(
            year_based_forecast, dictionary, how="left", on="asin", validate="1:1"
        )

    thread1 = threading.Thread(target=print_threaded, daemon=True)
    thread2 = threading.Thread(
        target=mm.export_to_excel,
        args=(
            [total] if stack else [forecast, forecast_dollars],
            ["forecast"] if stack else ["forecast, units", "forecast, dollars"],
            "sales_forecast.xlsx",
            user_folder,
        ),
    )
    thread1.start()
    thread2.start()
    thread2.join()
    stop = True
    print("Export completed.")
    thread1.join()


if __name__ == "__main__":
    main(stack="stacked")
