from dataclasses import dataclass
from pandas import DataFrame, ExcelWriter
from re import match
from typing import Tuple
from vertica_python.vertica.cursor import Cursor

from secret.sql.balances_and_transactions_audit_2023 import (
    BviCent, SQL_BVI_CENT,
    BviClassic, SQL_BVI_CLASSIC,
    SvgCent, SQL_SVG_CENT,
    SvgClassic, SQL_SVG_CLASSIC,
)

from src.connection.envs import envs
from src.connection.vertica import VerticaCursor
from src.utils import auto_adjust_column_widths

envs = envs()


def take_params(data_class: dataclass, template_sql: str) -> Tuple[str, str]:
    data_class = data_class()
    file_name = data_class.filename

    return template_sql, file_name


def match_all(s: str, ma: list) -> bool:
    regexp_templ = "+|.{0,}".join(ma)
    regexp_templ = ".{0,}" + regexp_templ + "+"
    return bool(match(regexp_templ, s))


def execute_sql(cursor: Cursor, exec_sql: str, round_numbers: bool = False) -> DataFrame:
    cursor.execute(exec_sql)

    df_cur = DataFrame(cursor.fetchall())
    cols = [col[0] for col in cursor.description]

    if not df_cur.empty:
        df_cur.columns = cols

    for col in cols:
        if match_all(
            col.lower(),
            ['usd', 'volume', 'balance', 'equity', 'credit', 'floating', 'in_out', 'revenue', 'transfer', 'payment']
        ):
            df_cur[col] = df_cur[col].astype(float)
            if round_numbers:
                df_cur[col] = df_cur[col].round(2)

    return df_cur


with VerticaCursor() as cur:
    classes_and_scripts = {
        BviCent: SQL_BVI_CENT,
        # BviClassic: SQL_BVI_CLASSIC,
        # SvgCent: SQL_SVG_CENT,
        # SvgClassic: SQL_SVG_CLASSIC,
    }
    for dc, sql_script in classes_and_scripts.items():
        sql, filename = take_params(dc, sql_script)
        df = execute_sql(cur, sql)
        xls_file = envs['PATH_FOR_FILES'] + filename
        with ExcelWriter(xls_file, engine='openpyxl') as writer:
            df.to_excel(excel_writer=writer, sheet_name="rep")
            df_sql = DataFrame(data={"sql": [sql_script]})
            df_sql.to_excel(excel_writer=writer, sheet_name="sql")
            writer.sheets['sql'].sheet_state = 'hidden'

        auto_adjust_column_widths(xls_file)
