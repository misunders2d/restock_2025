import datetime
from typing import Literal
import pandas as pd

from common import event_dates

current_year, current_month, current_day = (
    datetime.datetime.now().year,
    datetime.datetime.now().month,
    datetime.datetime.now().day,
)


def get_last_non_event_days(
    num_days: int, max_date: datetime.date, include_events: bool = False
):
    """Get the last `num_days` non-event dates before `max_date`."""
    event_dates_list = [
        date for event_date_range in event_dates.values() for date in event_date_range
    ]

    full_date_range = (
        pd.date_range(start="2020-01-01", end=max_date).to_pydatetime().tolist()
    )
    if include_events:
        return sorted(full_date_range)[-num_days:]
    non_event_dates = []
    for single_date in full_date_range:
        if single_date.date() not in event_dates_list:
            non_event_dates.append(single_date.date())

    return sorted(non_event_dates)[-num_days:]


def get_month_day(
    month: int,
    year: int,
    day_of_week: int,  # 0=Monday, 6=Sunday
    order: Literal["first", "second", "third", "fourth", "last"] = "last",
):
    """Return the day of the month for a specific event based on month and year."""
    first_day = datetime.date(year, month, 1)
    first_weekday = first_day.weekday()

    # Find the first occurrence of the target weekday in the month
    days_until_target = (day_of_week - first_weekday) % 7
    first_occurrence = first_day + datetime.timedelta(days=days_until_target)

    if order == "first":
        target_date = first_occurrence
    elif order == "second":
        target_date = first_occurrence + datetime.timedelta(weeks=1)
    elif order == "third":
        target_date = first_occurrence + datetime.timedelta(weeks=2)
    elif order == "fourth":
        target_date = first_occurrence + datetime.timedelta(weeks=3)
    elif order == "last":
        # Find the last occurrence of the target weekday in the month
        last_day = (
            datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
            if month < 12
            else datetime.date(year, 12, 31)
        )
        last_weekday = last_day.weekday()
        days_since_target = (last_weekday - day_of_week) % 7
        target_date = last_day - datetime.timedelta(days=days_since_target)
    else:
        raise ValueError("Invalid order value")

    return target_date.day


def get_event_days_delta():

    events = {
        "BSS": {"month": 3, "day": 20, "duration": 2},
        "PD": {"month": 7, "day": 10, "duration": 4},
        "PBDD": {"month": 10, "day": 7, "duration": 2},
        "BFCM": {
            "month": 11,
            "day": get_month_day(11, current_year, 4, order="last"),
            "duration": 4,
        },  # Last Friday in November
    }

    distances = {}
    for event, date in events.items():
        current_month_event = (
            current_month == date["month"] and current_day < date["day"]
        )
        if current_month_event:
            distances[event] = 0
        else:
            if date["month"] > current_month:
                distances[event] = date["month"] - current_month
            else:
                distances[event] = (12 - current_month) + date["month"]

    nearest_event = min(distances, key=distances.get)  # type: ignore
    days_to_event = (
        datetime.date(
            year=current_year if current_month < 12 else current_year + 1,
            month=events[nearest_event]["month"],
            day=events[nearest_event]["day"],
        )
        - datetime.date(current_year, current_month, current_day)
    ).days

    return nearest_event, days_to_event, events[nearest_event]["duration"]
