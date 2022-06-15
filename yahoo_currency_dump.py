from argparse import ArgumentParser
from csv import QUOTE_ALL
from datetime import timedelta, datetime
from pandas import melt
from pandas_datareader import data

TICKER = ["TRX-USD", "USDT-USD", "LUNA1-USD"]
DATA_SOURCE = "yahoo"
COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]


def get_ticker_params(ticker):
    if ticker == "TRX-USD":
        return ["TRX", "Tron"]
    elif ticker == "USDT-USD":
        return ["UST", "USDT Tether"]
    else:
        return [ticker.replace("-USD", ""), ticker.replace("-USD", "")]


def subtract_days_from_date(date, days):
    return (date - timedelta(days=days)).strftime("%Y%m%d")


def get_args(data_source):
    today = datetime.today()
    parser = ArgumentParser(description="Dump data from {} datareader".format(data_source))
    parser.add_argument('--start-date', type=str, default=subtract_days_from_date(today, 5))
    parser.add_argument('--end-date', type=str, default=subtract_days_from_date(today, 1))
    parser.add_argument('--output-file', type=str, default="{}_currencies.csv".format(data_source))
    return parser.parse_args()


def get_df_from_datareader(ticker, data_source, args):
    start_date = args.end_date if args.end_date < args.start_date else args.start_date
    start_date = datetime.strptime(start_date, "%Y%m%d")
    start_date = subtract_days_from_date(start_date, 5)

    df = data.DataReader(
        name=ticker,
        data_source=data_source,
        start=start_date,
        end=args.end_date,
        pause=10,
    )["Close"]

    return df


def modify_df(df, output_column_names):
    tickers = list(df.columns)
    df = df.assign(day_period=df.index.strftime("%Y%m%d"))
    df = melt(df, id_vars="day_period", value_vars=tickers)
    df = df.assign(
        currency_code=df.apply(lambda row: get_ticker_params(row.Symbols)[0], axis=1),
        currency_name=df.apply(lambda row: get_ticker_params(row.Symbols)[1], axis=1),
        units_per_currency=round(df["value"], 10),
        currency_per_unit=round(1 / df["value"], 10),
    )

    df = df.drop(columns=[i for i in df.columns if i not in output_column_names])
    print(df)
    return df


def main():
    args = get_args(DATA_SOURCE)
    df = get_df_from_datareader(TICKER, DATA_SOURCE, args)
    df = modify_df(df, COLUMN_NAMES)
    df.to_csv(args.output_file, index=False, sep=";", quoting=QUOTE_ALL, float_format='{:f}'.format)


if __name__ == "__main__":
    main()
