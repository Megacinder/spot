#!/home/ss_kettle/env_dwh/bin/python

from openpyxl import load_workbook
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description='Convert exported currency exchange rate csv file to xlsx')
parser.add_argument('file', metavar="file")
parser.add_argument('--output_file', type=str, default="CURRENCY EXCHANGE RATES export.xlsx")
args = parser.parse_args()

TEMPLATE_COLUMNS = [
    ('ECB', 'EUR'), ('ECB', 'JPY'), ('ECB', 'BGN'),
    ('ECB', 'CZK'), ('ECB', 'DKK'), ('ECB', 'GBP'),
    ('ECB', 'HUF'), ('ECB', 'PLN'), ('ECB', 'RON'),
    ('ECB', 'SEK'), ('ECB', 'CHF'), ('ECB', 'NOK'),
    ('ECB', 'HRK'), ('ECB', 'RUB'), ('ECB', 'TRY'),
    ('ECB', 'AUD'), ('ECB', 'BRL'), ('ECB', 'CAD'),
    ('ECB', 'CNY'), ('ECB', 'HKD'), ('ECB', 'IDR'),
    ('ECB', 'ILS'), ('ECB', 'INR'), ('ECB', 'KRW'),
    ('ECB', 'MXN'), ('ECB', 'MYR'), ('ECB', 'NZD'),
    ('ECB', 'PHP'), ('ECB', 'SGD'), ('ECB', 'THB'),
    ('ECB', 'ZAR'), ('XE', 'ETH'),  ('XE', 'LAK'),
    ('XE', 'NGN'),  ('XE', 'UGX'),  ('XE', 'VND'),
    ('XE', 'XBT'),
]


def tuple_sort_2(item):
    return item[0], item[1]


def list_difference(lst):
    template_currencies = [col[1] for col in TEMPLATE_COLUMNS]
    return sorted([item for item in lst if item[1] not in template_currencies], key=tuple_sort_2)


df = pd.read_csv(args.file, sep=";", header=0)
df['day_period'] = pd.to_datetime(df['day_period']).dt.strftime("%Y-%m-%d")
pivot = pd.pivot(df, values="currency_rate", index=["day_period"], columns=["source", "currency_code"])
sorted_columns = sorted([col for col in pivot.columns.values], key=tuple_sort_2)
sorted_columns = TEMPLATE_COLUMNS + list_difference(sorted_columns)

# sort pivot table columns
pivot = pivot.reindex(sorted_columns, axis=1)
pivot.to_excel(args.output_file, sheet_name="ECB3", freeze_panes=(2, 0), float_format="%.12f")

# add new currency rate column - values multiplied by 100
df["currency_rate_x_100"] = df["currency_rate"].apply(lambda x: x * 100)
pivot_100 = pd.pivot(df, values="currency_rate_x_100", index=["day_period"], columns=["source", "currency_code"])
pivot_100 = pivot_100.reindex(sorted_columns, axis=1)

with pd.ExcelWriter(args.output_file, mode="a", engine="openpyxl") as writer:
    writer.sheets.update({ws.title: ws for ws in writer.book.worksheets})
    pivot_100.to_excel(writer, freeze_panes=(2, 0), sheet_name="ECB4", float_format="%.12f")

# remove blank line above currency rates
wb = load_workbook(args.output_file)
for ws in wb.worksheets:
    ws.delete_rows(idx=3)
wb.save(args.output_file)
