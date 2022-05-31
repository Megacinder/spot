from pandas_datareader import data as pdr
data = pdr.get_data_yahoo(
    symbols=["TRX-USD"],
    start="2022-05-01",
    end="2022-05-31",
)
print(data["Close"])

import yfinance as yf
trx = yf.Ticker("TRX-USD")
for k, v in trx.info.items():
   print(k, v)

# COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]
