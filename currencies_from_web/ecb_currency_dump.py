from pandas import DataFrame, Series, date_range, read_xml, concat
from requests import get


# ECB_RATES_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"
ECB_RATES_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml?3459e77f43361e2be8f5e776b5691e64"  # Only for tests
NAMESPACES = {"ns": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}
TMP_ECB_RATES_PATH = "/tmp/ecb_currency_rates.csv"
CURRENCY_RATES_CSV = "currency_rates.csv"

FINAL_TABLE_COLS = ('dt', 'code', 'rate')
RATE_USD_COL = 'rate_usd'


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
    df = df.drop("Cube", axis=1).fillna(method='ffill').dropna().rename(columns={
        'time': FINAL_TABLE_COLS[0],
        'currency': FINAL_TABLE_COLS[1],
        'rate': FINAL_TABLE_COLS[2],
    })

    print('initial', df)

    df_dt = DataFrame(date_range(
        start=df.min(axis=0)[FINAL_TABLE_COLS[0]],
        end=df.max(axis=0)[FINAL_TABLE_COLS[0]],
        name=FINAL_TABLE_COLS[0],
    ))

    df_cur = DataFrame(Series(df[FINAL_TABLE_COLS[1]].unique()).rename(FINAL_TABLE_COLS[1]))

    df_cur_x_dt = df_dt.merge(df_cur, how='cross').sort_values([FINAL_TABLE_COLS[1], FINAL_TABLE_COLS[0]])

    df_cur_x_dt[FINAL_TABLE_COLS[0]] = df_cur_x_dt[FINAL_TABLE_COLS[0]].astype(str)
    df[FINAL_TABLE_COLS[0]] = df[FINAL_TABLE_COLS[0]].astype(str)

    df = df_cur_x_dt.merge(df, how='left', on=[FINAL_TABLE_COLS[0], FINAL_TABLE_COLS[1]]).fillna(method='ffill')

    df_usd = df.where(df[FINAL_TABLE_COLS[1]] == 'USD').dropna()
    df_usd = df_usd.rename(columns={FINAL_TABLE_COLS[2]: RATE_USD_COL})

    df = df.merge(df_usd.drop(FINAL_TABLE_COLS[1], axis=1), how='left', on=[FINAL_TABLE_COLS[0]])
    df[FINAL_TABLE_COLS[2]] = 1 / df[FINAL_TABLE_COLS[2]] * df[RATE_USD_COL]
    df = df.drop([RATE_USD_COL], axis=1)

    df_eur = df_usd.rename(columns={RATE_USD_COL: FINAL_TABLE_COLS[2]})
    df_eur[FINAL_TABLE_COLS[1]] = 'EUR'

    df = concat([df, df_eur], ignore_index=True, sort=False)

    print(df)

    # df.to_csv(TMP_ECB_RATES_PATH, sep=";", index=False, quoting=2)


ecb_to_raw()
