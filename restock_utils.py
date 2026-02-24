from datetime import timedelta
from tkinter.messagebox import showwarning
from typing import Literal

import numpy as np
import pandas as pd

from date_utils import events, get_last_non_event_days


def calculate_inventory_isr(
    amazon_inventory: pd.DataFrame,
    inv_max_date_input: str | None = None,
    col_to_use: Literal["asin", "sku"] = "asin",
):

    amazon_inventory = amazon_inventory.copy()

    amazon_inventory["date"] = pd.to_datetime(amazon_inventory["date"]).dt.date
    if not inv_max_date_input:
        inv_max_date = amazon_inventory["date"].max()
    else:
        inv_max_date = pd.to_datetime(inv_max_date_input).date()

    inventory_grouped = (
        amazon_inventory.groupby(["date", col_to_use]).agg("sum").reset_index()
    )

    inventory_grouped = inventory_grouped[(inventory_grouped["date"] <= inv_max_date)]

    inventory_grouped["in-stock-rate"] = inventory_grouped["amz_inventory"] > 0

    two_week_inventory = inventory_grouped.loc[
        pd.to_datetime(inventory_grouped["date"])
        >= pd.to_datetime(inv_max_date) - pd.Timedelta(days=13)
    ]

    asin_isr_long_term = (
        inventory_grouped.pivot_table(
            values="in-stock-rate", index=col_to_use, aggfunc="mean"
        )
        .round(2)
        .reset_index()
    )

    asin_isr_short_term = (
        two_week_inventory.pivot_table(
            values="in-stock-rate", index=col_to_use, aggfunc="mean"
        )
        .round(2)
        .reset_index()
    )

    asin_isr_long_term = asin_isr_long_term.rename(columns={"in-stock-rate": "ISR"})
    asin_isr_short_term = asin_isr_short_term.rename(
        columns={"in-stock-rate": "ISR_short"}
    )
    asin_isr = pd.merge(
        asin_isr_long_term,
        asin_isr_short_term,
        on=col_to_use,
        how="outer",
        validate="1:1",
    )

    return asin_isr.fillna(0)


def get_asin_sales(
    amazon_sales: pd.DataFrame,
    asin_isr: pd.DataFrame,
    include_events: bool = False,
    sales_max_date_input: str | None = None,
    long_term_days: int = 180,
    short_term_days: int = 14,
):
    if not sales_max_date_input:
        sales_max_date = (amazon_sales["date"].max() - pd.Timedelta(days=1)).date()
    else:
        sales_max_date = pd.to_datetime(sales_max_date_input).date()
    non_event_days = get_last_non_event_days(
        num_days=long_term_days, max_date=sales_max_date, include_events=include_events
    )
    non_event_days_short = get_last_non_event_days(
        num_days=short_term_days, max_date=sales_max_date, include_events=include_events
    )

    amazon_sales["date"] = pd.to_datetime(amazon_sales["date"]).dt.date

    amazon_sales = amazon_sales.loc[amazon_sales["date"].isin(non_event_days)]
    amazon_sales = amazon_sales.fillna(0)

    latest_sales = amazon_sales[amazon_sales["date"].isin(non_event_days_short)]

    long_term_sales = (
        amazon_sales.groupby("asin")
        .agg({"unit_sales": "sum", "dollar_sales": "sum"})
        .reset_index()
        .fillna(0)
    )
    short_term_sales = (
        latest_sales.groupby("asin")
        .agg({"unit_sales": "sum", "dollar_sales": "sum"})
        .reset_index()
        .fillna(0)
    )
    long_term_sales = pd.merge(
        long_term_sales, asin_isr, how="left", on="asin", validate="1:1"
    ).fillna(0)
    short_term_sales = pd.merge(
        short_term_sales, asin_isr, how="left", on="asin", validate="1:1"
    ).fillna(0)

    long_term_sales[f"avg sales dollar, {long_term_days} days"] = (
        long_term_sales["dollar_sales"]
        / long_term_days
        / long_term_sales["ISR"].replace(0, np.nan)
    ).round(2)
    long_term_sales[f"avg sales units, {long_term_days} days"] = (
        long_term_sales["unit_sales"]
        / long_term_days
        / long_term_sales["ISR"].replace(0, np.nan)
    ).round(2)
    short_term_sales[f"avg sales dollar, {short_term_days} days"] = (
        short_term_sales["dollar_sales"]
        / short_term_days
        / short_term_sales["ISR_short"].replace(0, np.nan)
    ).round(2)
    short_term_sales[f"avg sales units, {short_term_days} days"] = (
        short_term_sales["unit_sales"]
        / short_term_days
        / short_term_sales["ISR_short"].replace(0, np.nan)
    ).round(2)

    total_sales = pd.merge(
        short_term_sales[
            [
                "asin",
                "ISR",
                "ISR_short",
                f"avg sales dollar, {short_term_days} days",
                f"avg sales units, {short_term_days} days",
            ]
        ],
        long_term_sales[
            [
                "asin",
                f"avg sales dollar, {long_term_days} days",
                f"avg sales units, {long_term_days} days",
            ]
        ],
        on="asin",
        how="outer",
        validate="1:1",
    ).fillna(0)

    total_sales["avg units"] = (
        (0.6 * total_sales[f"avg sales units, {short_term_days} days"])
        + (0.4 * total_sales[f"avg sales units, {long_term_days} days"])
    ).round(4)

    total_sales.loc[
        (
            total_sales[f"avg sales units, {short_term_days} days"]
            / total_sales[f"avg sales units, {long_term_days} days"]
        )
        > 5,
        "avg units",
    ] = (0.1 * total_sales[f"avg sales units, {short_term_days} days"]) + (
        0.9 * total_sales[f"avg sales units, {long_term_days} days"]
    ).round(
        4
    )

    total_sales["avg $"] = (
        (0.6 * total_sales[f"avg sales dollar, {short_term_days} days"])
        + (0.4 * total_sales[f"avg sales dollar, {long_term_days} days"])
    ).round(2)
    total_sales.loc[
        (
            total_sales[f"avg sales units, {short_term_days} days"]
            / total_sales[f"avg sales units, {long_term_days} days"]
        )
        > 5,
        "avg $",
    ] = (0.1 * total_sales[f"avg sales dollar, {short_term_days} days"]) + (
        0.9 * total_sales[f"avg sales dollar, {long_term_days} days"]
    ).round(
        2
    )

    total_sales = total_sales.replace("NaN", 0)
    total_sales = total_sales.replace([np.inf, -np.inf], 0)
    total_sales = total_sales.fillna(0)

    return total_sales


def filter_event_spreadsheet(
    full_spreadsheet: pd.DataFrame,
    event: Literal["BFCM", "BSS", "PD", "PBDD"],
) -> pd.DataFrame | None:
    try:
        match event:
            case "BFCM":
                columns_to_return = [
                    "ASIN",
                    "Average BFCM sales, units (total)",
                    "Best BFCM performance",
                ]
            case "BSS":
                columns_to_return = [
                    "ASIN",
                    "Average BSS sales, units (total)",
                    "Best BSS performance",
                ]
            case "PD":
                columns_to_return = [
                    "ASIN",
                    "Average PD sales, units (total)",
                    "Best PD performance",
                ]
            case "PBDD":
                columns_to_return = [
                    "ASIN",
                    "Average PBDD sales, units (total)",
                    "Best PBDD performance",
                ]

        spreadsheet = full_spreadsheet.loc[:, columns_to_return].copy()
        spreadsheet = spreadsheet.rename(
            columns={
                "ASIN": "asin",
                # f"Average {event} sales, units (total)": ,
                # f"Best {event} performance": "best event performance","average event sales 1 day"
            }
        )
        spreadsheet.loc[
            spreadsheet[f"Average {event} sales, units (total)"] == "",
            f"Average {event} sales, units (total)",
        ] = 0
        spreadsheet.loc[
            spreadsheet[f"Best {event} performance"] == "",
            f"Best {event} performance",
        ] = 0

        return spreadsheet
    except Exception as e:
        raise BaseException(f"Error happened: {e}")


def calculate_event_forecast(
    total_sales: pd.DataFrame,
    full_event_df: pd.DataFrame,
    event: Literal["BFCM", "BSS", "PD", "PBDD"],
):

    # verify that total_sales contains "asin" and "avg units" columns
    sales_cols = total_sales.columns
    if "asin" not in sales_cols or "avg units" not in sales_cols:
        raise BaseException(
            "total_sales dataframe MUST contain `asin` and `avg units` columns"
        )

    event_df = filter_event_spreadsheet(full_spreadsheet=full_event_df, event=event)
    if event_df is None:
        raise BaseException("Could not pull create event_df dataframe")

    event_duration = events[event]["duration"]

    forecast = pd.merge(
        total_sales, event_df, how="left", on="asin", validate="1:1"
    ).fillna(0)
    # during_event_sales = np.where(  # =IF(H2>=3,"more than 3","less than 3")
    #     forecast["avg units"] >= 3,  # condition
    #     (
    #         (
    #             forecast["avg units"]
    #             * event_duration
    #             * forecast["best event performance"]
    #         )
    #         + (forecast["average event sales 1 day"] * event_duration)
    #     )
    #     / 2,  # if condition is true # =AVERAGE(J2*4,IF(H2>=3,H2*K2*4))
    #     forecast["avg units"] * event_duration * 2,  # if condition is false
    # )

    strong_performance = (
        forecast["avg units"]
        * forecast[f"Best {event} performance"]  # * event_duration
    )
    poor_performance = forecast["avg units"] * event_duration * 2
    average_event_performance = forecast[
        f"Average {event} sales, units (total)"
    ]  # * event_duration

    forecast[f"{event}_forecasted_sales"] = (
        average_event_performance + poor_performance
    ) / 2

    forecast.loc[forecast["avg units"] >= 3, f"{event}_forecasted_sales"] = (
        average_event_performance + strong_performance
    ) / 2

    return forecast[
        [
            "asin",
            f"Average {event} sales, units (total)",
            f"Best {event} performance",
            f"{event}_forecasted_sales",
        ]
    ]


def calculate_amazon_inventory(
    amazon_inventory: pd.DataFrame,
    max_date: str | None = None,
    col_to_use: Literal["asin", "sku"] = "asin",
    show_warning=True,
) -> pd.DataFrame:
    # max_date = amazon_inventory["date"].max()
    if max_date:
        max_date_dt = pd.to_datetime(max_date).date()
    else:
        max_date_dt = (pd.to_datetime("today") - pd.Timedelta(days=1)).date()
    check_date = max_date_dt - timedelta(days=1)
    last_inventory = amazon_inventory.loc[amazon_inventory["date"] >= check_date]
    # last_inventory = last_inventory.replace("nan", "").replace(np.nan, "")

    attempts = 1
    while len(last_inventory) == 0 and attempts <= 10 and show_warning:

        attempts += 1

        showwarning(
            title="No inv data",
            message="No inventory data found for yesterday!!!",
            icon="warning",
            detail=f"Report shows zero for AMZ inventory on {check_date} - caution!",
        )
        check_date = max_date_dt - timedelta(days=attempts)

        last_inventory: pd.DataFrame = amazon_inventory.loc[
            amazon_inventory["date"] >= check_date
        ]

        if len(last_inventory) > 0:
            showwarning(
                title="Found inventory",
                message=f"Inventory exists for {check_date} only!",
            )
            break

    def _fetch_unique(x):
        x = x.astype(str)
        x = x.replace("nan", "n/a")
        x = x.replace(np.nan, "n/a")
        return ", ".join([x for x in sorted(x.unique().tolist()) if x != "n/a"])

    agg_dict = (
        {
            "amz_inventory": "sum",
            "amz_available": "sum",
            "alert": lambda x: _fetch_unique(x),
            "recommended_action": lambda x: _fetch_unique(x),
            "healthy_inventory_level": "sum",
            "recommended_removal_quantity": "sum",
            "estimated_excess_quantity": "sum",
            "fba_minimum_inventory_level": "sum",
            "fba_inventory_level_health_status": lambda x: _fetch_unique(x),
            "storage_type": lambda x: _fetch_unique(x),
        }
        if col_to_use == "asin"
        else {"amz_inventory": "sum", "amz_available": "sum"}
    )
    last_inventory = (
        last_inventory.groupby(["date", col_to_use]).agg(agg_dict).reset_index()
    )

    last_inventory = last_inventory.sort_values(["date", col_to_use], ascending=False)

    last_inventory = last_inventory.groupby(col_to_use).agg("first").reset_index()
    return last_inventory


def group_incoming_by_weeks(incoming_weeks: pd.DataFrame) -> pd.DataFrame:
    """
    Helper function to group incoming containers ETAs into weeks and transform them into columns.
    """

    tables_list = []
    for _, row in incoming_weeks.iterrows():
        temp_df = pd.DataFrame()
        for item in row["items"]:
            items_df = pd.DataFrame.from_dict(item, orient="index").T
            temp_df = pd.concat([temp_df, items_df])
            temp_df["eta"] = row["eta"]
            temp_df = (
                temp_df.groupby(["eta", "SKU"]).agg({"QtyOrdered": "sum"}).reset_index()
            )
        tables_list.append(temp_df)

    full_containers = pd.concat(tables_list)
    full_containers["week"] = full_containers["eta"].dt.isocalendar().week
    full_containers["year"] = full_containers["eta"].dt.isocalendar().year
    full_containers = (
        full_containers.groupby(["year", "week", "SKU"])
        .agg({"QtyOrdered": "sum"})
        .reset_index()
    )

    full_containers["year-week"] = (
        full_containers["year"].astype(str) + "-" + full_containers["week"].astype(str)
    )
    full_containers = full_containers.pivot_table(
        index="SKU", columns="year-week", values="QtyOrdered"
    ).reset_index()

    current_columns = [col for col in full_containers.columns.tolist() if col != "SKU"]
    sorted_columns = sorted(
        current_columns, key=lambda x: (int(x.split("-")[0]), int(x.split("-")[1]))
    )
    full_containers = full_containers.loc[:, ["SKU"] + sorted_columns]
    return full_containers
