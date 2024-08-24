from calendar import TUESDAY
from pendulum import DateTime, timezone, Time, duration, instance, Interval, datetime, today

UTC = timezone("UTC")

dt = DateTime(2024, 7, 1)

def _get_days_until_next_launch(date_time: DateTime) -> int:
    date_time = date_time.replace(tzinfo=UTC)
    days_until_tuesday = (TUESDAY - date_time.weekday()) % 7
    if days_until_tuesday == 0:
        days_until_tuesday = 7
    if date_time.day == 1:
        days_until_next_first = 30
    else:
        next_month = (date_time.month % 12) + 1
        next_year = date_time.year + (date_time.month // 12)
        next_month_first_day = DateTime(next_year, next_month, 1).replace(tzinfo=UTC)
        days_until_next_first = (next_month_first_day - date_time).days

    return min(days_until_tuesday, days_until_next_first)


def modify_dates(params: dict, dt: DateTime) -> dict:
    #  = today().subtract(days=1)
    dt = dt.subtract(days=1) if dt.day == 1 else dt

    if 'from_dt' not in params['config']:
        params['config']['from_dt'] = int(dt.start_of("month").format('YYYYMMDD'))
    if 'to_dt' not in params['config']:
        params['config']['to_dt'] = int(dt.format('YYYYMMDD'))

    return params


a = _get_days_until_next_launch(dt)
print(a)
params = {}
params['config'] = {}
params = modify_dates(params, dt)
print(params)
