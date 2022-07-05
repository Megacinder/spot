from datetime import timedelta

import pytest
from pandas import DataFrame

from currencies_from_web.yahoo_currency_dump import (
    subtract_days_from_date,
    get_df_from_datareader,
    modify_df,
)


@pytest.fixture
def orig_yahoo_df(fix_dt):
    df = DataFrame(
        data={"TRX-USD": [0.064430, 0.062519], "USDT-USD": [1.000262, 1.000288]},
        index=[fix_dt, fix_dt + timedelta(days=1)],
    )
    df.index.name = 'Date'
    df.columns.name = 'Symbols'
    return df


TICKER = ["TRX-USD", "USDT-USD"]
DATA_SOURCE = "yahoo"
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


def test_subtract_days_from_date(fix_dt):
    assert subtract_days_from_date(fix_dt, 5) == "20220402"


def test_get_df_from_datareader(orig_yahoo_df, fix_dt):
    df = get_df_from_datareader(TICKER, DATA_SOURCE, fix_dt, fix_dt)
    df = df.round(6)

    assert df.equals(orig_yahoo_df)


def test_modify_df(orig_yahoo_df, column_names):
    df = modify_df(orig_yahoo_df, column_names)
    df = df.round(6)

    assert df.equals(MODIFY_DF)
