import yfinance as yf
import pandas as pd

data = yf.download("TRX-USD USDT-USD", "2022-12-01", "2022-12-15")["Close"]
# data.index = data.index.tz_localize(None)
print(data)
print(yf.__version__)
print(pd.__version__)