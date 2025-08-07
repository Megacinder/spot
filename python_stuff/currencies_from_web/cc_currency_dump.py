#!/home/ss_kettle/env_dwh/bin/python
from argparse import ArgumentParser, Namespace
from csv import QUOTE_ALL
from datetime import timedelta, datetime
from json import loads
from typing import Iterator
from pandas import DataFrame, concat
from requests import get


COLUMN_NAMES = ["day_period", "currency_code", "units_per_currency", "currency_per_unit"]
URL = "https://currency-converter.eglobal.app/api/historic/rate?from={}&to={}&amount=1.0&date={}"
FROM_CURRENCY = "USD"
TO_CURRENCY = "ETH"  # ,LAK,NGN,RUB,UGX,VND,XBT"


def form_url(url: str, from_currency: str, to_currency: str, date: datetime) -> str:
    date = date.strftime("%Y-%m-%d")
    return url.format(from_currency, to_currency, date)


def get_df_from_url(url: str) -> DataFrame:
    response = get(url)
    data = loads(response.text)
    df = DataFrame(data)
    return df


def daterange(start_date: datetime, end_date: datetime) -> Iterator[datetime]:
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def subtract_days_from_date(date: datetime, days: int) -> str:
    return (date - timedelta(days=days)).strftime("%Y%m%d")


def set_args() -> Namespace:
    today = datetime.today()
    parser = ArgumentParser(description='Dump data from Currency Converter')
    parser.add_argument('--start_date', type=str, default=subtract_days_from_date(datetime(2022, 9, 18), 1))
    parser.add_argument('--end_date', type=str, default=subtract_days_from_date(today, 1))
    parser.add_argument('--output_file', type=str, default="cc_currencies.csv")
    args = parser.parse_args()
    return args


def get_dates(args: Namespace) -> tuple:
    start_date = args.end_date if args.end_date < args.start_date else args.start_date
    start_date = datetime.strptime(start_date, "%Y%m%d")
    start_date = datetime.strptime(subtract_days_from_date(start_date, 5), "%Y%m%d")
    end_date = datetime.strptime(args.end_date, "%Y%m%d")
    return start_date, end_date


def modify_df(df: DataFrame, single_date: datetime, output_column_names: list) -> DataFrame:
    df = df.reset_index(drop=True)
    df = df.assign(
        day_period=single_date.strftime("%Y%m%d"),
        currency_code=df["currency"],
        units_per_currency=df["amount"].round(10),
        currency_per_unit=(1 / df["amount"]).round(10),
    )
    df = df.drop(columns=[i for i in df.columns if i not in output_column_names])
    return df


def main():
    dfs = DataFrame(columns=COLUMN_NAMES)
    print(dfs)

    args = set_args()
    start_date, end_date = get_dates(args)

    for single_date in daterange(start_date, end_date):
        url = form_url(URL, FROM_CURRENCY, TO_CURRENCY, single_date)
        df = get_df_from_url(url)
        if not df.empty:
            df = modify_df(df, single_date, COLUMN_NAMES)
            dfs = concat([dfs, df])

    dfs.to_csv(args.output_file, index=False, sep=";", quoting=QUOTE_ALL, float_format="%.10f")


if __name__ == "__main__":
    main()
