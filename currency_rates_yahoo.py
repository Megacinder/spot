from argparse import ArgumentParser
from csv import QUOTE_ALL
from datetime import timedelta, datetime
from pandas import melt
from pandas_datareader import data

TICKER = ["TRX-USD", "USDT-USD", "BTC-USD"]
DATA_SOURCE = "yahoo"
COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]


def get_currency_params(ticket):
    params = {}
    for i in ticket:
        if i == "TRX-USD":
            params[i] = ("TRX", "Tron")
        elif i == "USDT-USD":
            params[i] = ("UST", "USDT Tether")
        else:
            params[i] = (i.replace("-USD", ""), i.replace("-USD", ""))
    return params


def subtract_days_from_today(days):
    return (datetime.today() - timedelta(days=days)).strftime("%Y%m%d")


def add_args():
    parser = ArgumentParser(description=f"Dump data from {DATA_SOURCE} datareader")
    parser.add_argument('--start-dt', type=str, default=subtract_days_from_today(5))
    parser.add_argument('--end-dt', type=str, default=subtract_days_from_today(1))
    parser.add_argument('--output-file', type=str, default=f"{DATA_SOURCE}_currencies.csv")
    return parser.parse_args()


def create_df(ticker, data_source, output_column_names, args):
    df_from_dr = data.DataReader(
        name=ticker,
        data_source=data_source,
        start=args.start_dt,
        end=args.end_dt,
        pause=10,
    )["Close"]

    currency_params = get_currency_params(ticker)
    df_from_dr = df_from_dr.assign(day_period=df_from_dr.index.strftime("%Y%m%d"))

    df = melt(df_from_dr, id_vars="day_period", value_vars=ticker)
    df = df.assign(
        currency_code=df.apply(lambda row: currency_params[row.Symbols][0], axis=1),
        currency_name=df.apply(lambda row: currency_params[row.Symbols][1], axis=1),
        units_per_currency=round(df["value"], 10),
        currency_per_unit=round(1 / df["value"], 10),
    )

    df = df.drop(columns=[i for i in df.columns if i not in output_column_names])
    return df


def main():
    args = add_args()
    df = create_df(TICKER, DATA_SOURCE, COLUMN_NAMES, args)
    df.to_csv(args.output_file, index=False, sep=";", quoting=QUOTE_ALL)


if __name__ == "__main__":
    main()
