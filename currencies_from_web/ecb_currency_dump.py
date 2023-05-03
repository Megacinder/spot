from pandas import DataFrame, Series, date_range, read_xml
from requests import get


# ECB_RATES_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"
ECB_RATES_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml?3459e77f43361e2be8f5e776b5691e64"  # Only for tests
NAMESPACES = {"ns": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}
TMP_ECB_RATES_PATH = "/tmp/ecb_currency_rates.csv"
CURRENCY_RATES_CSV = "currency_rates.csv"


def form_xlm():
    # response = get(ECB_RATES_URL)
    # xlmstr = response.content.decode("utf-8")

    file1 = "../t_xml.xml"
    with open(file1, 'r') as f:
        xlmstr = "".join(f.readlines())

    # print(xlmstr)
    return xlmstr


def ecb_to_raw() -> None:
    resp = form_xlm()
    df = read_xml(path_or_buffer=resp, xpath="//ns:Cube/ns:Cube", namespaces=NAMESPACES)
    df = df.drop("Cube", axis=1).fillna(method='ffill').dropna()

    df_dt = DataFrame(date_range(
        start=df.min(axis=0)['time'],
        end=df.max(axis=0)['time'],
        name='time',
    ))

    df_cur = DataFrame(Series(df['currency'].unique()).rename('currency'))
    df_cur_x_dt = df_dt.merge(df_cur, how='cross').sort_values(['currency', 'time'])

    df_cur_x_dt['time'], df['time'] = df_cur_x_dt['time'].astype(str), df['time'].astype(str)

    df = df_cur_x_dt.merge(df, how='left', on=['time', 'currency']).fillna(method='ffill')

    df_usd = df.where(df['currency'] == 'USD').dropna()
    df_usd = df_usd.drop('currency', axis=1).rename(columns={'rate': 'rate_usd'})

    df = df.merge(df_usd, how='left', on=['time'])

    df = (
        df
        .rename(columns={'time': 'date', 'currency': 'currency_code'})
        .assign(currency_rate=1 / df['rate'] * df['rate_usd'])
    )

    df = df.drop(['rate', 'rate_usd'], axis=1)

    print(df)


    # df.to_csv(TMP_ECB_RATES_PATH, sep=";", index=False, quoting=2)



ecb_to_raw()
