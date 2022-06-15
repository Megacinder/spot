from argparse import ArgumentParser, Namespace
from csv import QUOTE_ALL
from datetime import timedelta, datetime
from typing import Iterator
from pandas import DataFrame, concat, read_html


COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]
UNUSED_CURRENCIES = ['LUNA']


def get_url(from_currency, date):
    date = date.strftime("%Y-%m-%d")
    return "https://www.xe.com/currencytables/?from={}&date={}#table-section".format(
        from_currency,
        date,
    )


def get_df_from_url(single_date):
    url = get_url("USD", single_date)
    df = read_html(url, flavor='bs4')[0]
    df = df[~df["Currency"].isin(UNUSED_CURRENCIES)]
    return df


def daterange(start_date, end_date) -> Iterator[datetime]:
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def subtract_days_from_date(date, days):
    return (date - timedelta(days=days)).strftime("%Y%m%d")


def get_args() -> Namespace:
    today = datetime.today()
    parser = ArgumentParser(description='Dump data from currency table from xe webpage')
    parser.add_argument('--start_date', type=str, default=subtract_days_from_date(today, 5))
    parser.add_argument('--end_date', type=str, default=subtract_days_from_date(today, 1))
    parser.add_argument('--output_file', type=str, default="xe_currencies.csv")
    args = parser.parse_args()
    return args


def modify_df(df, single_date, output_column_names):
    df = df.reset_index(drop=True)
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
    args = get_args()

    start_date = args.end_date if args.end_date < args.start_date else args.start_date
    start_date = datetime.strptime(start_date, "%Y%m%d") - timedelta(days=5)

    for single_date in daterange(
            start_date,
            datetime.strptime(args.end_date, '%Y%m%d'),
    ):
        df = get_df_from_url(single_date)
        if not df.empty:
            df = modify_df(df, single_date, COLUMN_NAMES)
            dfs = concat([dfs, df])

    dfs.to_csv(args.output_file, index=False, sep=";", quoting=QUOTE_ALL, float_format='{:f}'.format)


if __name__ == "__main__":
    main()
