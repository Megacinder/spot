#!/home/ss_kettle/env_dwh/bin/python
import re
import csv
from typing import Iterator
import requests
import argparse
from argparse import Namespace
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
from decimal import Decimal, DecimalException

XE_COM_URL = 'https://www.xe.com/currencytables/'
COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]
UNUSED_CURRENCIES = ['LUNA']


def get_table(single_date) -> BeautifulSoup:
    params = (
        ('from', 'USD'),
        ('date', single_date.strftime("%Y-%m-%d")),
    )
    response = requests.get(XE_COM_URL, params=params)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find("table", attrs={'class': re.compile('^currencytables__Table.*')})
    return table


def daterange(start_date, end_date) -> Iterator[datetime]:
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def get_args() -> Namespace:
    parser = argparse.ArgumentParser(description='Dump data from currency table from xe webpage')
    parser.add_argument('--start_date', type=str, default=(datetime.today() - timedelta(days=5)).strftime("%Y%m%d"))
    parser.add_argument('--end_date', type=str, default=(datetime.today() - timedelta(days=1)).strftime("%Y%m%d"))
    parser.add_argument('--output_file', type=str, default="xe_currencies.csv")
    args = parser.parse_args()
    return args


def dump_rates_to_csv(writer, args: Namespace, max_start_date: datetime) -> None:
    empty_day_counter = 0
    for single_date in daterange(max_start_date, datetime.strptime(args.end_date, '%Y%m%d')):

        table = get_table(single_date)
        if not table:
            empty_day_counter += 1
            table = get_table(single_date - timedelta(days=empty_day_counter))
        else:
            empty_day_counter = 0
        output_rows = []
        for table_row in table.find_all('tr')[1:]:
            currency_code = table_row.find('th').text
            columns = table_row.find_all('td')
            output_row = []
            for idx, column in enumerate(columns):
                if idx >= 1:
                    rate = Decimal(column.text.replace(',', ''))
                    try:
                        rate = round(rate, 10)
                    except DecimalException as e:
                        right, left = str(rate).split('.')
                        left = round(Decimal(f'0.{left}'), 10)
                        rate = Decimal(right) + left
                    decimal_len = len(str(rate))
                    if decimal_len > 19:
                        output_row.append(round(rate, 10 + (19 - decimal_len)))
                    else:
                        output_row.append("{:.10f}".format(rate))
                else:
                    output_row.append(column.text)
            output_row = [single_date.strftime("%Y%m%d"), currency_code] + output_row
            output_rows.append(output_row)
        output_rows = list(filter(lambda row: row[1] not in UNUSED_CURRENCIES, output_rows))
        writer.writerows(output_rows)


def main():
    args = get_args()

    ocsvfile = open(args.output_file, 'w')
    writer = csv.writer(ocsvfile, delimiter=';', quoting=csv.QUOTE_ALL)
    writer.writerow(COLUMN_NAMES)

    max_start_date = max(
        datetime.strptime(args.start_date, '%Y%m%d') - timedelta(days=5),
        datetime.strptime('20190101', '%Y%m%d')
    )
    dump_rates_to_csv(writer, args, max_start_date)


if __name__ == "__main__":
    main()
