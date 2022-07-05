import pytest
from pandas import DataFrame

from currencies_from_web.xe_currency_dump import (
    subtract_days_from_date,
    get_df_from_url,
    modify_df,
)


@pytest.fixture()
def orig_xe_df():
    return DataFrame(
        data={
            "Currency": ["GBP", "XBT"],
            "Name": ["British Pound", "XBT"],
            "Units per USD": [0.765727, 0.000023],
            "USD per unit": [1.305949, 43358.365563],
        },
        index=[2, 181],
    )


URL = "https://www.xe.com/currencytables/?from={}&date={}#table-section"
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


def test_subtract_days_from_date(fix_dt):
    assert subtract_days_from_date(fix_dt, 5) == "20220402"


def test_get_df_from_url(orig_xe_df, fix_dt):
    df = get_df_from_url(URL, fix_dt)
    df = df[df["Currency"].isin(["GBP", "XBT"])]
    df = df.round(6)

    assert df.equals(orig_xe_df)


def test_modify_df(orig_xe_df, fix_dt, column_names):
    df = modify_df(orig_xe_df, fix_dt, column_names)
    df = df.round(6)

    assert df.equals(MODIFY_DF)
