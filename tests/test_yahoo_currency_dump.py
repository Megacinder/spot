from datetime import datetime, timedelta
from pandas import DataFrame

from currencies_from_web.yahoo_currency_dump import (
    subtract_days_from_date,
    get_df_from_datareader,
    modify_df,
)


class TestYahooCurrencyDump:
    DT = datetime(year=2022, month=4, day=7)
    TICKER = ["TRX-USD", "USDT-USD"]
    DATA_SOURCE = "yahoo"
    ORIGINAL_COLUMN_NAMES = [
        "day_period",
        "currency_code",
        "currency_name",
        "units_per_currency",
        "currency_per_unit",
    ]
    COLUMN_NAMES = ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]
    DF = DataFrame(
        data={"TRX-USD": [0.064430, 0.062519], "USDT-USD": [1.000262, 1.000288]},
        index=[DT, DT + timedelta(days=1)],
    )
    DF.index.name = 'Date'
    DF.columns.name = 'Symbols'

    MODIFY_DF = DataFrame(
        data={
            "day_period": ["20220407", "20220408", "20220407", "20220408"],
            "currency_code": ["TRX", "TRX", "UST", "UST"],
            "currency_name": ["Tron", "Tron", "USDT Tether", "USDT Tether"],
            "units_per_currency": [0.064430, 0.062519, 1.000262, 1.000288],
            "currency_per_unit": [15.520720, 15.995137, 0.999738, 0.999712],
        },
        index=[0, 1, 2, 3],
    )

    def test_subtract_days_from_date(self):
        assert subtract_days_from_date(self.DT, 5) == "20220402"

    def test_get_df_from_datareader(self):
        df = get_df_from_datareader(self.TICKER, self.DATA_SOURCE, self.DT, self.DT)
        df = df.round(6)

        assert df.equals(self.DF)

    def test_modify_df(self):
        df = modify_df(self.DF, self.COLUMN_NAMES)
        df = df.round(6)

        assert df.equals(self.MODIFY_DF)
