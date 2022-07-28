#!/home/ss_kettle/env_dwh/bin/python
from argparse import ArgumentParser, Namespace
from csv import QUOTE_ALL
from datetime import timedelta, datetime
from typing import Iterator
from pandas import DataFrame, concat, read_html


COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]
UNUSED_CURRENCIES = ['LUNA']
URL = "https://www.xe.com/currencytables/?from={}&date={}#table-section"


def get_url(url: str, from_currency: str, date: datetime) -> str:
    date = date.strftime("%Y-%m-%d")
    return url.format(from_currency, date)


def get_df_from_url(url: str, single_date: datetime) -> DataFrame:
    url = get_url(url, "USD", single_date)
    df = read_html(url, flavor='bs4')[0]
    df = df[~df["Currency"].isin(UNUSED_CURRENCIES)]
    return df


def daterange(start_date: datetime, end_date: datetime) -> Iterator[datetime]:
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def subtract_days_from_date(date: datetime, days: int) -> str:
    return (date - timedelta(days=days)).strftime("%Y%m%d")


def set_args() -> Namespace:
    today = datetime.today()
    parser = ArgumentParser(description='Dump data from currency table from xe webpage')
    parser.add_argument('--start_date', type=str, default=subtract_days_from_date(today, 5))
    parser.add_argument('--end_date', type=str, default=subtract_days_from_date(today, 1))
    parser.add_argument('--output_file', type=str, default="xe_currencies.csv")
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
    if "Name" in df.columns:
        df = df.assign(
            day_period=single_date.strftime("%Y%m%d"),
            currency_code=df["Currency"],
            currency_name=df["Name"],
            units_per_currency=df["Units per USD"].round(10),
            currency_per_unit=df["USD per unit"].round(10),
        )
        df = df.drop(columns=[i for i in df.columns if i not in output_column_names])
        return df


def main():
    dfs = DataFrame(columns=COLUMN_NAMES)

    args = set_args()
    start_date, end_date = get_dates(args)

    for single_date in daterange(start_date, end_date):
        df = get_df_from_url(URL, single_date)
        if not df.empty:
            df = modify_df(df, single_date, COLUMN_NAMES)
            dfs = concat([dfs, df])

    dfs.to_csv(args.output_file, index=False, sep=";", quoting=QUOTE_ALL, float_format="%.10f")


if __name__ == "__main__":
    main()
