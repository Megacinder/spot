from src.connection.vertica import VerticaCursor
from pandas import DataFrame


with VerticaCursor() as cur:
    cur.execute("select 1  as id union all select 2 union all select 3")
    df = DataFrame(cur.fetchall())
    print(df)
