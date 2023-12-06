from pandas import DataFrame, ExcelWriter
from pendulum import now
from vertica_python.vertica.cursor import Cursor

from secret.sql.opened_acc_cli_tran import SQL_USER, SQL_ACCOUNT, SQL_TRANSACTION, FILENAME

from src.connection.envs import envs
from src.connection.vertica import VerticaCursor
from src.utils import auto_adjust_column_widths

envs = envs()

FROM_DT = "20230925"
TO_DT = now().format("YYYYMMDD")
FILE = FILENAME.format(from_dt=FROM_DT, to_dt=TO_DT)
DIC = {
    "users": SQL_USER.format(from_dt=FROM_DT, to_dt=TO_DT),
    "accounts": SQL_ACCOUNT.format(from_dt=FROM_DT, to_dt=TO_DT),
    "transactions": SQL_TRANSACTION.format(from_dt=FROM_DT, to_dt=TO_DT),
}

df_sql = DataFrame.from_dict({'typ': DIC.keys(), "sql": DIC.values()})


def execute_sql(cursor: Cursor, exec_sql: str) -> DataFrame:
    cursor.execute(exec_sql)
    df = DataFrame(cursor.fetchall())
    cols = [col[0] for col in cur.description]

    if not df.empty:
        df.columns = cols

    df.drop(axis=0, index=0)

    return df


with VerticaCursor() as cur:
    dict_df = {}
    for sheet_name, sql in DIC.items():
        xls_file = envs['PATH_FOR_FILES'] + FILE
        dict_df[sheet_name] = execute_sql(cur, sql)

    with ExcelWriter(xls_file, engine='openpyxl') as writer:
        for sheet_name, df in dict_df.items():
            df.to_excel(excel_writer=writer, sheet_name=sheet_name, index=False)
        df_sql.to_excel(excel_writer=writer, sheet_name="sql", index=False)
        writer.sheets['sql'].sheet_state = 'hidden'

    auto_adjust_column_widths(xls_file)
