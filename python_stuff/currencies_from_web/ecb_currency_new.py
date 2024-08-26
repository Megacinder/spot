from pandas import read_xml, DataFrame
from requests import get
from pathlib import Path

ECB_RATES_URL = {
    "last_90_days": "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml",
    "all_time": "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml?3459e77f43361e2be8f5e776b5691e64",
}
TMP_ECB_RATES_PATH = Path("/tmp/ecb_currency_rates.csv")


def form_xlm(source: str = 'file') -> str:
    if source == 'file':
        file1 = "../t_xml.xml"
        with open(file1, 'r') as f:
            xlmstr = "".join(f.readlines())
    else:
        response = get(ECB_RATES_URL["last_90_days"])
        xlmstr = response.content.decode("utf-8")

    return xlmstr


def get_currency(source: str) -> DataFrame:
    resp = form_xlm(source)
    ns = {"ns": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}
    df = read_xml(path_or_buffer=resp, xpath="//ns:Cube/ns:Cube", namespaces=ns)
    df = (
        df
        .drop("Cube", axis=1)
        .fillna(method='ffill')
        .dropna()
        .rename(columns={'time': 'date', 'currency': 'code'})
    )
    return df


def save_to_csv(df: DataFrame, path: Path) -> None:
    df.to_csv(path, sep=";", index=False, quoting=2)


if __name__ == '__main__':
    print(get_currency('file'))
