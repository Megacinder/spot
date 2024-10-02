from yfinance import download
from pendulum import now
from pandas import DataFrame

TICKER = ['RUB=X']

from_dt = now().start_of('month')
to_dt = from_dt.add(months=1)

from_dt = from_dt.to_date_string()
to_dt = to_dt.to_date_string()

data = download(tickers=TICKER, start=from_dt, end=to_dt)
data = data["Close"]

print(data)

df = DataFrame(data)
df.to_csv('yf_curr', sep=',', index=False)
# print(df)
