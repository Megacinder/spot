from argparse import ArgumentParser, Namespace
from csv import QUOTE_ALL
from datetime import timedelta, datetime
from pandas import DataFrame, DatetimeIndex, melt
from pandas_datareader import data
from typing import List

TICKER = ["TRX-USD", "USDT-USD", "BTC-USD"]
DATA_SOURCE = "yahoo"
COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]


def get_ticker_params(ticker: str) -> tuple:
    if ticker == "TRX-USD":
        return "TRX", "Tron"
    elif ticker == "USDT-USD":
        return "UST", "USDT Tether"
    elif ticker == "BTC-USD":
        return "UST", "Bitcoin"
    else:
        return ticker.replace("-USD", ""), ticker.replace("-USD", "")


def subtract_days_from_date(date: datetime, days: int) -> str:
    return (date - timedelta(days=days)).strftime("%Y%m%d")


def set_args(data_source: str) -> Namespace:
    today = datetime.today()
    parser = ArgumentParser(description="Dump data from {} datareader".format(data_source))
    parser.add_argument('--start-date', type=str, default=subtract_days_from_date(today, 5))
    parser.add_argument('--end-date', type=str, default=subtract_days_from_date(today, 1))
    parser.add_argument('--output-file', type=str, default="{}_currencies.csv".format(data_source))
    return parser.parse_args()


def get_dates(args: Namespace) -> tuple:
    start_date = args.end_date if args.end_date < args.start_date else args.start_date
    start_date = datetime.strptime(start_date, "%Y%m%d")
    start_date = datetime.strptime(subtract_days_from_date(start_date, 5), "%Y%m%d")
    end_date = datetime.strptime(args.end_date, "%Y%m%d")
    return start_date, end_date


def get_df_from_datareader(ticker: List[str], data_source: str, start_date: datetime, end_date: datetime) -> DataFrame:
    df = data.DataReader(
        name=ticker,
        data_source=data_source,
        start=start_date,
        end=end_date,
        pause=10,
    )["Close"]

    return df


def modify_df(df: DataFrame, output_column_names: list) -> DataFrame:
    tickers = list(df.columns)
    df = df.assign(day_period=DatetimeIndex(df.index).strftime("%Y%m%d"))
    df = melt(df, id_vars="day_period", value_vars=tickers)
    df = df.assign(
        currency_code=df.apply(lambda row: get_ticker_params(row.Symbols)[0], axis=1),
        currency_name=df.apply(lambda row: get_ticker_params(row.Symbols)[1], axis=1),
        units_per_currency=round(df["value"], 10),
        currency_per_unit=round(1 / df["value"], 10),
    )

    df = df.drop(columns=[i for i in df.columns if i not in output_column_names])
    return df


def main():
    args = set_args(DATA_SOURCE)
    start_date, end_date = get_dates(args)

    df = get_df_from_datareader(TICKER, DATA_SOURCE, start_date, end_date)
    df = modify_df(df, COLUMN_NAMES)
    df.to_csv(args.output_file, index=False, sep=";", quoting=QUOTE_ALL, float_format="%.10f")


if __name__ == "__main__":
    main()
