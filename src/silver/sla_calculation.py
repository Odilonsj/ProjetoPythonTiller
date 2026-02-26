"""
SLA Calculation with Business Hours
Aggregated business metrics with proper SLA calculation considering:
- Business hours (8 AM to 6 PM)
- Weekends (Saturday and Sunday)
- National holidays (from holidays.json file)
"""
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


def get_holidays(filename=None):
    try:
        if filename is None:
            filename = Path(__file__).resolve().parents[2] / "data" / "bronze" / "holidays.json"

        with open(filename, "r", encoding="utf-8") as f:
            holidays_data = json.load(f)

        holidays = set()
        for holiday in holidays_data:
            holiday_date = datetime.strptime(holiday["date"], "%Y-%m-%d").date()
            holidays.add(holiday_date)

        return holidays

    except Exception:
        return set()


def calculate_business_hours(start, end, holidays, work_start_hour=8, work_end_hour=18):
    if pd.isna(start) or pd.isna(end) or end <= start:
        return 0

    start = start.tz_localize(None) if start.tz else start
    end = end.tz_localize(None) if end.tz else end

    business_hours = 0.0
    current = start

    while current.date() < end.date():
        if current.weekday() < 5 and current.date() not in holidays:
            day_start = max(current, current.replace(hour=work_start_hour, minute=0, second=0))
            day_end = current.replace(hour=work_end_hour, minute=0, second=0)

            if day_start < day_end:
                business_hours += (day_end - day_start).total_seconds() / 3600

        current = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0)

    if end.weekday() < 5 and end.date() not in holidays:
        day_start = max(current, current.replace(hour=work_start_hour, minute=0, second=0))
        day_end = min(end, end.replace(hour=work_end_hour, minute=0, second=0))

        if day_start < day_end:
            business_hours += (day_end - day_start).total_seconds() / 3600

    return round(business_hours, 2)
