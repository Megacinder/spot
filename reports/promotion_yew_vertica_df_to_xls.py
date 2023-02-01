from dataclasses import dataclass
from logging import DEBUG
from os import environ
from pandas import DataFrame
from sshtunnel import SSHTunnelForwarder, create_logger
from vertica_python import connect as vertica_connect

from ignore.nested_stuff.reports_for_yew import (
    Vietnam, India
)

from ignore.nested_stuff.reports_for_yew_Jan23 import (
    SQL_USERS, SQL_PARTNERS, SQL_GALA_INDIA, SQL_GALA_TH,
    ThLaMa, ThLaMaGala, IndiaGala,
)


PATH_FOR_FILES = environ['path_for_files']
HOST = environ['host']
PORT = int(environ['port'])

tunnel_conn = {
    'ssh_address_or_host': (environ['ssh_host'], int(environ['ssh_port'])),
    'ssh_username': environ['ssh_username'],
    'ssh_pkey': environ['ssh_pkey'],
    'remote_bind_address': (HOST, PORT),
    'local_bind_address': (HOST, PORT),
    'logger': create_logger(loglevel=1),
}

db_conn = {
    'host': HOST,
    'port': PORT,
    'user': environ['db_user'],
    'password': environ['db_password'],
    'database': environ['db_name'],
    'log_level': DEBUG,
    'log_path': '../ignore/logs/vertica_conn.log',
}


def take_params(data_class: dataclass, template_sql: str, type: tuple = ('user',)) -> tuple[str, str]:
    dc = data_class()
    if 'jan23' in type:
        country_list = "'" + "','".join(str(i) for i in dc.country_list) + "'"
        sql = template_sql.format(
            from_dt=dc.from_dt,
            to_dt=dc.to_dt,
            country_list=country_list,
        )
        filename = dc.filename
    else:
        partner_user_id = ','.join(str(i) for i in dc.partner_user_id)
        sql = template_sql.format(
            partner_user_id=partner_user_id,
            from_dt=dc.from_dt,
            to_dt=dc.to_dt,
        )
        dict1 = {
            'user': dc.filename,
            'partner': dc.filename_partner,
        }
        filename = dict1[type[0]]

    return sql, filename


def execute_sql(cur, sql: str) -> DataFrame:
    cur.execute(sql)

    df = DataFrame(cur.fetchall())
    cols = [col[0] for col in cur.description]

    if not df.empty:
        df.columns = cols

    for col in cols:
        if 'usd' in col.lower() or 'volume' in col.lower():
            df[col] = df[col].astype(float)
            df[col] = df[col].round(2)

    return df


def auto_adjust_column_widths(excel_file: "Excel File Path", extra_space=1) -> None:
    from openpyxl import load_workbook
    from openpyxl.utils import get_column_letter

    wb = load_workbook(excel_file)

    for ws in wb:
        df = DataFrame(ws.values, )

        for i, r in (df.astype(str).applymap(len).max(axis=0) + extra_space).items():
            ws.column_dimensions[get_column_letter(i + 1)].width = r

    wb.save(excel_file)


with SSHTunnelForwarder(**tunnel_conn) as server:
    with vertica_connect(**db_conn) as conn:
        cur = conn.cursor()

        # classes = (ThLaMa, India, Vietnam)
        # for dc in classes:
        #     sql, filename = take_params(dc, SQL_USERS, ('user',))
        #     df = execute_sql(cur, sql)
        #     df.to_excel(PATH_FOR_FILES + filename)
        #
        #     sql, filename = take_params(dc, SQL_PARTNERS, ('partner',))
        #     df = execute_sql(cur, sql)
        #     df.to_excel(PATH_FOR_FILES + filename)
        #
        # classes = (IndiaGala, )
        # for dc in classes:
        #     sql, filename = take_params(dc, SQL_GALA_INDIA, ('jan23', 'gala',))
        #     df = execute_sql(cur, sql)
        #     print(df)
        #     df.to_excel(PATH_FOR_FILES + filename)

        classes = (ThLaMaGala,)
        for dc in classes:
            sql, filename = take_params(dc, SQL_GALA_TH, ('jan23', 'gala',))
            df = execute_sql(cur, sql)
            df.to_excel(PATH_FOR_FILES + filename)
            auto_adjust_column_widths(PATH_FOR_FILES + filename)
