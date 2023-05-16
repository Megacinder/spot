from fastapi import FastAPI
from src.connection.envs import envs
from src.connection.vertica import VerticaCursor
from pandas import DataFrame
from secret.sql.balance import SQL_BAL

envs = envs()


def take_params(template_sql: str, **kwargs: dict) -> str:
    sql = template_sql.format(
        dt=kwargs["dt"],
        acc_id=kwargs["acc_id"],
        serv_id=kwargs["serv_id"],
    )
    return sql


def execute_sql(cur, sql: str) -> DataFrame:
    cur.execute(sql)

    df = DataFrame(cur.fetchall())
    cols = [col[0] for col in cur.description]

    if not df.empty:
        df.columns = cols

    for col in cols:
        df[col] = df[col].astype(float)
        df[col] = df[col].round(2)

    df.reset_index(drop=True)

    return df


app = FastAPI()


@app.get("/balance")
def get_dict(dt: int = 20230501, acc_id: int = 5700170, serv_id: int = 5):
    params = {
        'dt': dt,
        'acc_id': acc_id,
        'serv_id': serv_id,
    }
    with VerticaCursor() as cur:
        sql = take_params(SQL_BAL, **params)
        df = execute_sql(cur, sql)
    return df.to_dict('records')[0]
