from argparse import Namespace
from datetime import timedelta
from pyspark import SparkConf
from pyspark.sql.types import ArrayType, DateType
from pyspark.sql.functions import (
    col, date_add, date_format, explode, last, lit, max, min, regexp_replace, round, sequence, to_date,
    transform, trim,
)
from pyspark.sql import Window, DataFrame
from pyspark.sql.column import Column
from typing import Union

from src.spark.spark_utils import start_spark, get_spark_logger
from secret.constant import CURRENCY_RATE

# if 'dummy' not in df.columns:
#    df.withColumn("dummy",lit(None))


def modify_field(as_is: Union[bool, int, str], to_be: Union[bool, int, str]) -> Column:
    if as_is in ('date', 'day_period'):
        return to_date(col(as_is)).alias(to_be)

    if as_is in ('rate', 'Value', 'units_per_currency'):
        return regexp_replace(col(as_is), ',', '.').cast('decimal(20, 10)').alias(to_be)

    else:
        return trim(col(as_is)).alias(to_be)


def addnew_field(as_is: Union[bool, int, str], to_be: Union[bool, int, str]) -> Column:
    return lit(as_is).alias(to_be)


def unify_initial_dfs(dic: dict) -> tuple:
    df_ecb_common1 = (
        dic['ecb']
        .select(
            modify_field('date', 'dt'),
            modify_field('currency', 'cur'),
            modify_field('currency', 'name'),
            addnew_field(1, 'nom'),
            modify_field('rate', 'rate'),
            addnew_field('ECB', 'sou'),
        )
        .filter(col('dt') >= '2007-01-01')
    )

    df_cbr_common1 = (
        dic['cbr']
        .select(
            modify_field('date', 'dt'),
            modify_field('CharCode', 'cur'),
            modify_field('Name', 'name'),
            modify_field('Nominal', 'nom'),
            modify_field('Value', 'rate'),
            addnew_field('cbr', 'sou'),
        )
        .filter(col('dt').between('2007-01-01', '2016-12-29'))
    )

    df_cco_common1 = (
        dic['cco']
        .select(
            modify_field('day_period', 'dt'),
            modify_field('currency_code', 'cur'),
            modify_field('currency_code', 'name'),
            addnew_field(1, 'nom'),
            modify_field('units_per_currency', 'rate'),
            addnew_field('xe', 'sou'),
        )
        .filter(col('dt') >= '2020-07-01')
    )

    return df_ecb_common1, df_cbr_common1, df_cco_common1


def revert_rates_from_eur_to_usd(df: DataFrame) -> DataFrame:
    df_ecb_rate_eur = (
        df
        .filter(col('cur') == 'USD')
        .withColumn('cur', lit('EUR'))
        .withColumn('name', lit('EUR'))
    )

    df_ecb_usd_eur_rate = (
        df.alias('euro1')
        .join(
            df_ecb_rate_eur.alias('euro_in_usd1'),
            on=[col('euro_in_usd1.dt') == col('euro1.dt')],
            how='left_outer',
        )
    )

    df_ecb_reverse_rate_from_euro_to_usd = (
        df_ecb_usd_eur_rate
        .withColumn('rate1', round(col('euro_in_usd1.rate') / col('euro1.rate'), 10))
        # .withColumn('rate', col('rate1'))
        # .drop('rate1')
        .select(
            col('euro1.dt').alias('dt'),
            col('euro1.cur').alias('cur'),
            col('euro1.name').alias('name'),
            col('euro1.nom').alias('nom'),
            col('rate1').alias('rate'),
            col('euro1.sou').alias('sou'),
        )
    )

    df_updated = df_ecb_reverse_rate_from_euro_to_usd.unionAll(df_ecb_rate_eur)
    return df_updated


def shift_dates(df: DataFrame) -> DataFrame:

    df_ecb_before_20200630_shifted_date = (
        df
        .filter(df.dt.between('2016-12-29', '2020-06-29'))
        .withColumn('dt', date_add(to_date(df.dt), 1))
    )

    df_ecb_after_20200701 = (
        df
        .filter(df.dt > '2020-06-30')
    )

    df_updated = df_ecb_before_20200630_shifted_date.unionAll(df_ecb_after_20200701)
    return df_updated


def revert_rates_from_rub_to_usd(df: DataFrame) -> DataFrame:
    df_cbr_rate_rub = (
        df
        .filter(col('cur') == 'USD')
        .withColumn('cur', lit('RUB'))
        .withColumn('name', lit('Российский рубль'))
        .withColumn('rate', round(lit(1) / col('rate'), 10))
    )

    df_cbr_usd_rub_rate = (
        df.alias('rub1')
        .join(
            df_cbr_rate_rub.alias('rub_in_usd1'),
            on=[col('rub_in_usd1.dt') == col('rub1.dt')],
            how='left_outer',
        )
    )

    df_cbr_reverse_rate_from_rub_to_usd = (
        df_cbr_usd_rub_rate
        .withColumn('rate1', round(col('rub1.rate') / col('rub_in_usd1.rate') / col('rub1.nom'), 10))
        .select(
            col('rub1.dt').alias('dt'),
            col('rub1.cur').alias('cur'),
            col('rub1.name').alias('name'),
            col('rub1.nom').alias('nom'),
            col('rate1').alias('rate'),
            col('rub1.sou').alias('sou'),
        )
    )

    df_updated = df_cbr_reverse_rate_from_rub_to_usd.unionAll(df_cbr_rate_rub)
    return df_updated


def main():
    with start_spark(
        app_name="currency",
        master='local',
        # enable_hive_support=True,
        # instance='remote_spark_shell',
    ) as spark:

        df_ecb = spark.read.csv(CURRENCY_RATE['ecb'], sep=';', header=True, quote='"')
        df_cbr = spark.read.csv(CURRENCY_RATE['cbr'], sep=';', header=True, quote='"')
        df_cco = spark.read.csv(CURRENCY_RATE['cco'], sep=';', header=True)

        df_dict = {
            'ecb': df_ecb,
            'cbr': df_cbr,
            'cco': df_cco,
        }

        df_ecb_common, df_cbr_common, df_cco_common = unify_initial_dfs(df_dict)

        df_ecb_updated = revert_rates_from_eur_to_usd(df_ecb_common)
        df_ecb_updated = shift_dates(df_ecb_updated)

        df_cbr_updated = revert_rates_from_rub_to_usd(df_cbr_common)

        # union {
        df_union = (
            df_ecb_updated
            .unionAll(df_cbr_updated)
            .unionAll(df_cco_common)
        )
        # }

        # fill empty dates - weekends and holidays {
        df_cur_x_dt = (
            df_union
            .groupBy(col('cur'))
            .agg(
                min(df_union.dt).alias('min_dt'),
                max(df_union.dt).alias('max_dt'),
            )
        )

        df_cur_x_dt = (
            df_cur_x_dt
            .select(
                df_cur_x_dt.cur,
                explode(
                    sequence(
                        to_date(df_cur_x_dt.min_dt),
                        to_date(df_cur_x_dt.max_dt),
                    ),
                ).alias('running_dt'),
            )
        )

        df_missed_dt = (
            df_cur_x_dt.alias('a')
            .join(
                df_union.alias('b'),
                on=[
                    (col('b.cur') == col('a.cur'))
                    & (col('b.dt') == col('a.running_dt'))
                ],
                how='left_outer',
            )
            .select(
                col('a.cur').alias('cur'),
                col('a.running_dt').alias('dt'),
                col('b.name').alias('name'),
                col('b.nom').alias('nom'),
                col('b.rate').alias('rate'),
                col('b.sou').alias('sou'),
            )
        )

        df_missed_dt = df_missed_dt.sort([df_missed_dt.cur, df_missed_dt.dt])

        w_forward = (
            Window
            .partitionBy('cur')
            .orderBy('dt')
            .rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow,
            )
        )

        df_fill_null = (
            df_missed_dt
            .withColumn('name1', last('name', ignorenulls=True).over(w_forward))
            .withColumn('nom1', last('nom', ignorenulls=True).over(w_forward))
            .withColumn('rate1', last('rate', ignorenulls=True).over(w_forward))
            .withColumn('sou1', last('sou', ignorenulls=True).over(w_forward))
        )
        # }

        # create final table like dwh.dim_crb_currencies in Vertica DWH
        df_final_table = (
            df_fill_null
            .select(
                date_format(col('dt'), 'yyyyMMdd').cast('int').alias('day_period'),
                col('cur').alias('currency_code'),
                col('name1').alias('currency_name'),
                col('nom1').alias('nomimal'),
                col('rate1').alias('currency_rate'),
                lit(1).alias('usd_currency_rate'),
                col('sou1').alias('source'),
            )
        )
        # }

        # and save it {
        (
            df_final_table
            .repartition(1)
            .write
            .format('delta')
            .mode('overwrite')
            .save("/tmp/currency")
        )
        # }


if __name__ == '__main__':
    main()
