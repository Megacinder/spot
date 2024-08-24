from pendulum import DateTime, timezone, Time, duration, instance
from calendar import TUESDAY

def _get_first_day_of_month(date_time: DateTime) -> DateTime:
    date_time = date_time.set(day=1)
    return date_time


def _get_next_tuesday(date_time: DateTime) -> DateTime:
    offset_days = (TUESDAY - date_time.weekday()) % 7
    next_tuesday = date_time.add(days=offset_days)
    return next_tuesday


def _get_prev_tuesday(date_time: DateTime) -> DateTime:
    offset_days = (TUESDAY - date_time.weekday()) % 7
    next_tuesday = date_time.add(days=offset_days).add(weeks=-1)
    return next_tuesday


a = DateTime(2024, 6, 7, 8)
# a = _get_first_day_of_month(a)
b = _get_next_tuesday(a)
c = _get_prev_tuesday(a)
print(b, c)
