import pandas as pd


def last_2_weeks_sales(amazon_sales: pd.DataFrame, amazon_inventory: pd.DataFrame):
    last_date = amazon_sales["date"].max()
    cut_off_date = last_date - pd.Timedelta(days=13)
    latest_sales = amazon_sales[amazon_sales["date"] >= cut_off_date]
    latest_inventory = amazon_inventory[amazon_inventory["date"] >= cut_off_date]
    latest_inventory = (
        latest_inventory.groupby(["date", "asin"])[["amz_inventory"]]
        .agg("sum")
        .reset_index()
    )
    latest_inventory["in-stock-rate"] = latest_inventory["amz_inventory"] > 0
    latest_isr = (
        latest_inventory.groupby("asin")[["in-stock-rate"]].agg("mean").reset_index()
    )
    latest_sales = latest_sales.groupby("asin")[["unit_sales"]].agg("sum").reset_index()
    latest_sales = pd.merge(latest_sales, latest_isr, on="asin", how="outer")
    latest_sales["average_sales_14"] = latest_sales["unit_sales"] / 14
    latest_sales["average_sales_14"] = (
        latest_sales["average_sales_14"] / latest_sales["in-stock-rate"]
    ).round(3)
    return latest_sales[["asin", "average_sales_14"]]


def calculate_inventory_isr(amazon_inventory):

    inv_max_date = amazon_inventory["date"].max()

    inventory_grouped = (
        amazon_inventory.groupby(["date", "asin"]).agg("sum").reset_index()
    )

    two_week_inventory = inventory_grouped[
        (inventory_grouped["date"] <= inv_max_date - pd.Timedelta(days=13))
    ]

    inventory_grouped["in-stock-rate"] = inventory_grouped["amz_inventory"] > 0
    two_week_inventory["in-stock-rate"] = two_week_inventory["amz_inventory"] > 0

    asin_isr_long_term = (
        inventory_grouped.pivot_table(
            values="in-stock-rate", index="asin", aggfunc="mean"
        )
        .round(2)
        .reset_index()
    )

    asin_isr_short_term = (
        two_week_inventory.pivot_table(
            values="in-stock-rate", index="asin", aggfunc="mean"
        )
        .round(2)
        .reset_index()
    )

    asin_isr_long_term = asin_isr_long_term.rename(columns={"in-stock-rate": "ISR"})
    asin_isr_short_term = asin_isr_short_term.rename(
        columns={"in-stock-rate": "ISR_short"}
    )
    asin_isr = pd.merge(
        asin_isr_long_term, asin_isr_short_term, on="asin", how="outer", validate="1:1"
    )

    return asin_isr
