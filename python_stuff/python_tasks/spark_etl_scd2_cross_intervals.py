from dataclasses import dataclass, fields, asdict
from datetime import date, datetime, timedelta
from hashlib import sha1
from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql import types as t

conf = {
    'table_pk': [
        'effective_from_dttm',
        'effective_to_dttm',
        'deleted_flg',
        'account_id',
    ]
}


@dataclass
class TableFields:
    effective_from_dttm: str = 'effective_from_dttm'
    effective_to_dttm: str = 'effective_to_dttm'
    deleted_flg: str = 'deleted_flg'
    is_active_flg: str = 'is_active_flg'
    entity_id: str = 'account_id'


table_schema = [
    'effective_from_dttm',
    'effective_to_dttm',
    'deleted_flg',
    'is_active_flg',
    'account_id',
    'comment',
    'load_dt'
]

load_dt = datetime.utcnow() + timedelta(hours=3)
load_dt.strftime("%Y-%m-%d %H:%M:%S")

b = sha1(b'aSDadaSDada')
print(b.hexdigest())


tar = [
    ('2018-01-01', '2018-12-31', 1, 0, 1, 'first_wrong', load_dt),
    ('2018-01-01', '2018-12-31', 0, 0, 1, 'first', load_dt),
    ('2019-01-01', '5999-12-31', 0, 1, 1, 'second', load_dt),
    # ('2019-02-01', '5999-12-31', 0, 1, 2, 'not jopa', load_dt),
    # ('2019-02-01', '5999-12-31', 0, 1, 3, 'norm', load_dt),
    # ('2019-03-01', '5999-12-31', 0, 1, 4, 'huy', load_dt),
]

load_dt = datetime.utcnow() + timedelta(hours=33)
load_dt.strftime("%Y-%m-%d %H:%M:%S")

sou = [
    # ('2018-01-01', '2018-12-31', 0, 0, 1, 'first_new', load_dt),
    # ('2019-01-01', '2021-10-05', 0, 0, 1, 'second', load_dt),
    ('2021-10-06', '5999-12-31', 0, 1, 1, 'third', load_dt),
    # ('2020-02-15', '5999-12-31', 0, 1, 2, 'not only jopa', load_dt),
    # ('2019-02-01', '5999-12-31', 0, 1, 3, 'norm', load_dt),
    # ('2021-02-01', '5999-12-31', 0, 1, 5, 'new five', load_dt),
]


def deleted(
    df_pda: sql.DataFrame,
    fields_dc: dataclass = TableFields,
):
    df = (
        df_pda
        .where(df_pda[fields_dc.deleted_flg] == 1)
    )
    return df


def unexists(
    df_sda: sql.DataFrame,
    df_pda: sql.DataFrame,
    fields_dc: dataclass = TableFields,
):
    field_list = [c.default for c in fields(fields_dc) if c.default not in (
        fields_dc.effective_from_dttm,
        fields_dc.is_active_flg,
    )]

    pda_alias = 'pda1'
    df = (
        df_pda.alias(pda_alias)
        .join(
            df_sda,
            on=[df_sda[c] == df_pda[c] for c in field_list],
            how='left',
        )
        .where(
            (df_pda[fields_dc.deleted_flg] == 0)
            & (df_sda[fields_dc.deleted_flg].isNull())
        )
        .select(f'{pda_alias}.*')
    )
    return df


def unchanged(
    df_sda: sql.DataFrame,
    df_pda: sql.DataFrame,
    fields_dc: dataclass = TableFields,
):
    field_list = [c.default for c in fields(fields_dc)]  # [:4]

    pda_alias = 'pda1'
    df = (
        df_pda.alias(pda_alias)
        .join(
            df_sda,
            on=[df_sda[c] == df_pda[c] for c in field_list],
            how='inner',
        )
        .where(df_pda[fields_dc.deleted_flg] == 0)
        .select(f'{pda_alias}.*')
    )
    return df


# TODO: same valid_from and valid_to from new and old:
#  in old set deleted_flg = 1 and insert new
def new(
    df_sda: sql.DataFrame,
    df_pda: sql.DataFrame,
    fields_dc: dataclass = TableFields,
):
    field_list = [c.default for c in fields(fields_dc) if c.default not in (
        fields_dc.effective_to_dttm,
        fields_dc.is_active_flg,
    )]

    sda_alias = 'sda1'
    df = (
        df_pda
        .join(
            df_sda.alias(sda_alias),
            on=[df_sda[c] == df_pda[c] for c in field_list],
            how='right',
        )
        .where(df_pda[fields_dc.deleted_flg].isNull())
        .select(f'{sda_alias}.*')
    )
    return df


def changed(
    df_sda: sql.DataFrame,
    df_pda: sql.DataFrame,
    fields_dc: dataclass = TableFields,
):
    field_list = [c.default for c in fields(fields_dc) if c.default not in (
        fields_dc.effective_to_dttm,
        # fields_dc.effective_from_dttm,
        fields_dc.is_active_flg,
    )]

    sda_alias = 'sda1'
    df = (
        df_pda
        .join(
            df_sda.alias(sda_alias),
            on=[df_sda[c] == df_pda[c] for c in field_list],
            how='inner',
        )
        .where(
            (df_pda[fields_dc.is_active_flg] == 1)
            & (df_pda[fields_dc.effective_to_dttm] > df_sda[fields_dc.effective_to_dttm])
        )
        .select(f'{sda_alias}.*')
    )
    return df


def closed(
    df_sda: sql.DataFrame,
    df_pda: sql.DataFrame,
    fields_dc: dataclass = TableFields,
):
    field_list = [c.default for c in fields(fields_dc) if c.default not in (
        # fields_dc.effective_to_dttm,
        fields_dc.effective_from_dttm,
        # fields_dc.is_active_flg,
    )]

    sda_alias = 'sda1'
    pda_alias = 'pda1'

    upsert_columns = [f'{pda_alias}.' + c for c in df_pda.columns]

    df = (
        df_pda.alias(pda_alias)
        .join(
            df_sda.alias(sda_alias),
            on=[df_sda[c] == df_pda[c] for c in field_list],
            how='inner',
        )
        .join(
            df_sda.where(df_sda[fields_dc.is_active_flg] == 0),
            on=[df_sda[c] == df_pda[c] for c in field_list],
            how='left_anti',
        )
        .where(
            (df_pda[fields_dc.is_active_flg] == 1)
            & (df_pda[fields_dc.effective_from_dttm] != df_sda[fields_dc.effective_from_dttm])
        )
        # .withColumn(f'zero_{fields_dc.is_active_flg}', f.lit(0))
        .select([
            df_pda[fields_dc.effective_from_dttm],
            f.date_add(df_sda[fields_dc.effective_from_dttm], -1).alias(fields_dc.effective_to_dttm),
            df_sda[fields_dc.deleted_flg],
            f.lit(0).alias(fields_dc.is_active_flg),
            *upsert_columns[4:],
        ])
    )
    return df


spark = sql.SparkSession.builder.getOrCreate()


# spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")

df_pda = spark.createDataFrame(tar, table_schema)
df_sda = spark.createDataFrame(sou, table_schema)


df1 = deleted(
    df_pda=df_pda,
)
df1.show()


df2 = unexists(
    df_sda=df_sda,
    df_pda=df_pda,
    fields_dc=TableFields,
)
df2.show()

df3 = unchanged(
    df_sda=df_sda,
    df_pda=df_pda,
    fields_dc=TableFields,
)
df3.show()

df4 = new(
    df_sda=df_sda,
    df_pda=df_pda,
    fields_dc=TableFields,
)
df4.show()

df5 = changed(
    df_sda=df_sda,
    df_pda=df_pda,
    fields_dc=TableFields,
)
df5.show()

df6 = closed(
    df_sda=df_sda,
    df_pda=df_pda,
    fields_dc=TableFields,
)
df6.show()


df_all = df1\
    .union(df2)\
    .union(df3)\
    .union(df4)\
    .union(df5)\
    .union(df6)

df_all = df_all.sort(
    TableFields.entity_id,
    TableFields.effective_from_dttm,
    f.col(TableFields.deleted_flg).desc(),
)

df_all.show()


spark.stop()

