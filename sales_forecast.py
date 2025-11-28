import pandas as pd
import numpy as np
from db_utils import get_amazon_sales
from common import excluded_dates, user_folder
from utils import mellanni_modules as mm
from main import calculate_restock
import threading


def print_threaded():
    phrase = "Please wait, saving to Excel..."
    visible = ''
    for char in phrase:
        visible += char

def main():
    result = {}

    get_amazon_sales(output=result, to_print=True, num_days=20000)
    full_sales = result["get_amazon_sales"].copy()
    daily_sales = (
        full_sales[["date", "unit_sales"]].groupby("date").agg("sum").reset_index()
    )
    non_event_sales = daily_sales[~daily_sales["date"].isin(excluded_dates)].copy()

    non_event_sales["avg"] = non_event_sales["unit_sales"].rolling(window=180).mean()
    non_event_sales["coeff_raw"] = non_event_sales["unit_sales"] / non_event_sales["avg"]
    non_event_sales["coeff"] = non_event_sales["coeff_raw"].rolling(window=7).mean()


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
        month, day = date.month, date.day
        while (month, day) not in averages:
            day += 1 if day < 31 else 1
        return month, day

    current_restock = calculate_restock(include_events=False, num_days=365)
    forecast = current_restock[["asin", "avg units"]].copy()
    forecast["avg price"] = current_restock["avg $"] / current_restock["avg units"]

    future_date_range = pd.date_range(
        start=(pd.to_datetime("today")).date(),
        end=(pd.to_datetime("today") + pd.Timedelta(days=500)).date(),
    )

    for date in future_date_range:
        forecast[date.date()] = forecast["avg units"] * averages[get_nearest_date(date)]
        forecast["avg units"] = forecast["avg units"] * (179 / 180) + forecast[
            date.date()
        ] * (1 / 180)

    forecast_dollars = forecast.copy()
    for date in future_date_range:
        forecast_dollars[date.date()] = (
            forecast[date.date()] * forecast_dollars["avg price"]
        )

    print("Please wait, saving to Excel...")
    thread = threading.Thread(target =  mm.export_to_excel, args = (
        [forecast, forecast_dollars],
        ["forecast, units", "forecast, dollars"],
        "sales_forecast.xlsx",
        user_folder,
    ))
    thread.start()
    thread.join()
    print('Export completed.')

if __name__ == "__main__":
    main()
