import pandas as pd


def calculate_inventory_isr(amazon_inventory):  # done

    inv_max_date = amazon_inventory["date"].max()

    inventory_grouped: pd.DataFrame = (
        amazon_inventory.groupby(["date", "asin"]).agg("sum").reset_index()
    )

    inventory_grouped["in-stock-rate"] = inventory_grouped["amz_inventory"] > 0

    two_week_inventory = inventory_grouped[
        (inventory_grouped["date"] >= inv_max_date - pd.Timedelta(days=13))
    ]

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


def get_asin_sales(amazon_sales: pd.DataFrame, asin_isr: pd.DataFrame):
    sales_total_days: pd.Timedelta = (
        amazon_sales["date"].max() - amazon_sales["date"].min()
    ).days
    sales_max_date = amazon_sales["date"].max() - pd.Timedelta(days=1)
    amazon_sales = amazon_sales[amazon_sales["date"] <= sales_max_date]
    amazon_sales = amazon_sales.fillna(0)

    latest_sales = amazon_sales[
        amazon_sales["date"].between(
            sales_max_date - pd.Timedelta(days=13), sales_max_date
        )
    ]
    long_term_sales = (
        amazon_sales.groupby("asin")
        .agg({"unit_sales": "sum", "dollar_sales": "sum"})
        .reset_index()
    )
    short_term_sales = (
        latest_sales.groupby("asin")
        .agg({"unit_sales": "sum", "dollar_sales": "sum"})
        .reset_index()
    )
    long_term_sales = pd.merge(
        long_term_sales, asin_isr, how="left", on="asin", validate="1:1"
    )
    short_term_sales = pd.merge(
        short_term_sales, asin_isr, how="left", on="asin", validate="1:1"
    )

    long_term_sales["avg sales dollar, 180 days"] = (
        long_term_sales["dollar_sales"] / 180 / long_term_sales["ISR"]
    ).round(2)
    long_term_sales["avg sales units, 180 days"] = (
        long_term_sales["unit_sales"] / 180 / long_term_sales["ISR"]
    ).round(2)
    short_term_sales["avg sales dollar, 14 days"] = (
        short_term_sales["dollar_sales"] / 14 / short_term_sales["ISR_short"]
    ).round(2)
    short_term_sales["avg sales units, 14 days"] = (
        short_term_sales["unit_sales"] / 14 / short_term_sales["ISR_short"]
    ).round(2)

    total_sales = pd.merge(
        short_term_sales[
            [
                "asin",
                "ISR",
                "ISR_short",
                "avg sales dollar, 14 days",
                "avg sales units, 14 days",
            ]
        ],
        long_term_sales[
            ["asin", "avg sales dollar, 180 days", "avg sales units, 180 days"]
        ],
        on="asin",
        how="outer",
        validate="1:1",
    ).fillna(0)

    total_sales["avg units"] = (
        (0.6 * total_sales["avg sales units, 14 days"])
        + (0.4 * total_sales["avg sales units, 180 days"])
    ).round(2)
    total_sales["avg $"] = (
        (0.6 * total_sales["avg sales dollar, 14 days"])
        + (0.4 * total_sales["avg sales dollar, 180 days"])
    ).round(2)

    return total_sales
