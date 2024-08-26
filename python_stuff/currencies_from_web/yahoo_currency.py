from yfinance import download
from pendulum import now, date, from_format
from pandas import DataFrame, date_range

TICKER = ['RUB=X']

# from_dt = now().start_of('month').to_date_string()
FROM_DT = date(2024, 9, 1)
TO_DT = date(2024, 9, 30)  # now()


from_dt = FROM_DT
to_dt = TO_DT
if FROM_DT.isoweekday() in (6, 7):
    from_dt = FROM_DT.subtract(days=2 if FROM_DT.isoweekday() == 7 else 1)

from_dt = from_dt.to_date_string()
to_dt = to_dt.to_date_string()

data = download(tickers=TICKER, start=from_dt, end=to_dt)
data = data["Close"]

df = DataFrame(data)
all_days = date_range(start=from_dt, end=to_dt, freq='D')

df = df.reindex(all_days)
df = df.ffill()
# df.index.name = 'dt'
df = df.rename(columns={"Close": "rate"})
df['dt'] = df.index

# print(FROM_DT.to_date_string())
a = FROM_DT.to_date_string()
# df = df.query("dt >= " + a)

print(df.filter(df['dt'] >= FROM_DT))

# filename = f"USD_RUB_rates_{from_dt}-{to_dt}.xlsx"
# df.to_excel(filename)
