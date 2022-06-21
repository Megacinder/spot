from datetime import datetime
from pandas import DataFrame

from currencies_from_web.xe_currency_dump import (
    subtract_days_from_date,
    get_df_from_url,
    modify_df,
)


class TestXeCurrencyDump:
    DT = datetime(year=2022, month=4, day=7)
    URL = "https://www.xe.com/currencytables/?from={}&date={}#table-section"
    COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]

    DF = DataFrame(
        data={
            "Currency": ["GBP", "XBT"],
            "Name": ["British Pound", "XBT"],
            "Units per USD": [0.765727, 0.000023],
            "USD per unit": [1.305949, 43358.365563],
        },
        index=[2, 181],
    )

    MODIFY_DF = DataFrame(
        data={
            "day_period": ["20220407", "20220407"],
            "currency_code": ["GBP", "XBT"],
            "currency_name": ["British Pound", "XBT"],
            "units_per_currency": [0.765727, 0.000023],
            "currency_per_unit": [1.305949, 43358.365563],
        },
        index=[0, 1],
    )

    def test_subtract_days_from_date(self):
        assert subtract_days_from_date(self.DT, 5) == "20220402"

    def test_get_df_from_url(self):
        df = get_df_from_url(self.URL, self.DT)
        df = df[df["Currency"].isin(["GBP", "XBT"])]
        df = df.round(6)

        assert df.equals(self.DF)

    def test_modify_df(self):
        df = modify_df(self.DF, self.DT, self.COLUMN_NAMES)
        df = df.round(6)

        assert df.equals(self.MODIFY_DF)
