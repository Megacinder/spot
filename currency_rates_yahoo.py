from argparse import ArgumentParser, Namespace
from csv import QUOTE_ALL
from datetime import timedelta, datetime
from pandas_datareader import data

TICKER = "TRX-USD"
DATA_SOURCE = "yahoo"
# START_DT = "2022-05-20"
# END_DT = "2022-05-25"

COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]

parser = ArgumentParser(description=f"Dump data from {DATA_SOURCE} datareader")
parser.add_argument('--start-dt', type=str, default=(datetime.today() - timedelta(days=5)).strftime("%Y%m%d"))
parser.add_argument('--end-dt', type=str, default=(datetime.today() - timedelta(days=1)).strftime("%Y%m%d"))
parser.add_argument('--output-file', type=str, default=f"{DATA_SOURCE}_currencies.csv")
args = parser.parse_args()

df = data.DataReader(
    name=TICKER,
    data_source=DATA_SOURCE,
    start=args.start_dt,
    end=args.end_dt,
    pause=10,
)

df = df.assign(
    day_period=df.index.strftime("%Y%m%d"),
    currency_code="TRX",
    currency_name="Tron",
    units_per_currency=round(df["Close"], 10),
    currency_per_unit=round(1 / df["Close"], 10),
)

df = df.drop(columns=[i for i in df.columns if i not in COLUMN_NAMES])

df.to_csv(args.output_file, index=False, sep=";", quoting=QUOTE_ALL)
