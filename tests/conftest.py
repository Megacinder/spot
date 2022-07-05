from datetime import datetime
import pytest


@pytest.fixture
def fix_dt():
    return datetime(year=2022, month=4, day=7)


@pytest.fixture
def column_names():
    return ["day_period", "currency_code", "currency_name", "units_per_currency", "currency_per_unit"]
