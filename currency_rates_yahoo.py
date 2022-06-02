from pandas_datareader import data

TICKER = "TRX-USD"
DATA_SOURCE = "yahoo"
START_DT = "2022-05-20"
END_DT = "2022-05-25"

df = data.DataReader(
    name=TICKER,
    data_source=DATA_SOURCE,
    start=START_DT,
    end=END_DT,
    pause=10,
)


COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]


df = df.assign(
    # day_period=df["Date"],
    day_period=df.index.strftime("%Y%m%d"),
    currency_code="TRX",
    currency_name="Tron",
    units_per_currency=round(df["Close"], 10),
    currency_per_unit=round(1 / df["Close"], 10),
)

df = df.drop(columns=[i for i in df.columns if i not in COLUMN_NAMES])
# df.columns = COLUMN_NAMES
# df["Date"] = df.index
# df.index.names = ['day_period'].strftime("%Y%m%d")

# df = df.reset_index(drop=True)
print(df)

df.to_csv("yahoo_currencies.csv", index=False, sep=";")


# import yfinance as yf
# trx = yf.Ticker("TRX-USD")
# for k, v in trx.info.items():
#    print(k, v)

# COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]
