from yfinance import download
from pendulum import now, date
from pandas import DataFrame, date_range

TICKER = ['RUB=X']

# from_dt = now().start_of('month').to_date_string()
from_dt = date(2024, 9, 1)
to_dt = now()

if from_dt.isoweekday() in (6, 7):
    from_dt = from_dt.subtract(days=2 if from_dt.isoweekday() == 7 else 1)

from_dt = from_dt.to_date_string()
to_dt = to_dt.to_date_string()

data = download(tickers=TICKER, start=from_dt, end=to_dt)
data = data["Close"]

df = DataFrame(data)
all_days = date_range(start=from_dt, end=to_dt, freq='D')

df = df.reindex(all_days)
df = df.ffill()
df.index.name = 'dt'
df = df.rename(columns={"Close": "rate"})
# print(df)

filename = f"USD_RUB_rates_{from_dt}-{to_dt}.xlsx"
df.to_excel(filename)
