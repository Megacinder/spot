from pandas import DataFrame
from pendulum import now
from vertica_python.vertica.cursor import Cursor

from secret.sql.crs_report import SQL

from src.connection.envs import envs
from src.connection.vertica import VerticaCursor


envs = envs()

FROM_DT = "20230101"
TO_DT = "20231231"
DT = now().format("YYYYMMDD")


FILENAME_VS_SQL = {
    f"crs_report_{FROM_DT[0:4]}_bvi": SQL.format(
        from_dt=FROM_DT, to_dt=TO_DT, jurisdiction="'bvi'", should_the_data_be_masked=0
    ),
    f"crs_report_{FROM_DT[0:4]}_svg": SQL.format(
        from_dt=FROM_DT, to_dt=TO_DT, jurisdiction="'svg'", should_the_data_be_masked=0
    ),
    f"crs_report_{FROM_DT[0:4]}_bvi_masked": SQL.format(
        from_dt=FROM_DT, to_dt=TO_DT, jurisdiction="'bvi'", should_the_data_be_masked=1
    ),
    f"crs_report_{FROM_DT[0:4]}_svg_masked": SQL.format(
        from_dt=FROM_DT, to_dt=TO_DT, jurisdiction="'svg'", should_the_data_be_masked=1
    ),
}


def execute_sql(cursor: Cursor, exec_sql: str) -> DataFrame:
    cursor.execute(exec_sql)
    df = DataFrame(cursor.fetchall())
    cols = [col[0] for col in cur.description]

    if not df.empty:
        df.columns = cols

    df.drop(axis=0, index=0)

    return df


with VerticaCursor() as cur:
    for file_name, sql in FILENAME_VS_SQL.items():
        file_path = envs['PATH_FOR_FILES'] + file_name
        df = execute_sql(cur, sql)
        df.to_csv(file_path, sep=";", index=False)
