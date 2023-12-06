from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import (
    col, concat_ws, date_add, date_format, explode, expr, last, lit, max, min, regexp_replace,
    round, sequence, to_date, transform, trim, count
)
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from src.spark.spark_utils import start_spark, get_spark_logger, get_folder_content

CONF = [
    # ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    # ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    # ("spark.sql.legacy.createHiveTableByDefault.enabled", "true"),
    # ('hive.exec.dynamic.partition', 'true'),
    # ('hive.exec.max.dynamic.partitions', '2048'),
    # ('hive.exec.dynamic.partition.mode', 'nonstrict'),
    ("spark.driver.memory", "64g"),
    ("spark.driver.cores", "8"),
    ("spark.executor.memory", "64g"),
    ("spark.executor.cores", "8"),
    ("spark.sql.autoBroadcastJoinThreshold", "-1"),
]


OLD_SQL = """
select
     a.user_id
    ,a.partner_user_id
    ,0  as level
    ,a.user_id  as root_partner_id
from 
    clean.clients  a
where 1=1
"""

SQL = """
with wt_pc as (
    select
         a.server  as server_id
        ,a.client_account_id  as account_id
        ,coalesce(b.type, 'trading')  as type
        ,a.partner_account_id
        ,row_number() over (partition by a.client_account_id, a.server order by a.updated_at)  as rn1
    from 
        raw_account.partner_client  a
        left join clean.accounts  b
            on  b.account_id = a.client_account_id
            and (
                    (
                    b.type = 'partner'
                    and b.server_id = 2
                )
                or b.type = 'trading'
            )
    where 1=1
        and a.level = 1
)


,wt_pc_uniq as (
    select
         a.server_id
        ,a.account_id
        ,a.type
        ,a.partner_account_id
    from 
        wt_pc  a
    where 1=1
        and a.rn1 = 1
)


,wt_pch as (
    select
         a.server  as server_id
        ,a.client_account_id  as account_id
        ,coalesce(b.type, 'trading')  as type
        ,a.partner_account_id
        ,row_number() over (partition by a.client_account_id, a.server order by a.time_from)  as rn1
    from 
        raw_account.partner_client_history  a
        left join clean.accounts  b
            on  b.account_id = a.client_account_id
            and (
                    (
                    b.type = 'partner'
                    and b.server_id = 2
                )
                or b.type = 'trading'
            )
    where 1=1
        and a.level = 1
        and current_timestamp between time_from and time_to
)


,wt_pch_uniq_minus_pc as (
    select
         a.server_id
        ,a.account_id
        ,a.type
        ,a.partner_account_id
    from 
        wt_pch  a
    where 1=1
        and a.rn1 = 1
        and (a.server_id, a.account_id, a.partner_account_id) not in (
            select
                 b.server_id
                ,b.account_id
                ,b.partner_account_id
            from 
                wt_pc_uniq  b
            where 1=1
        )
)


,wt_df as (
    select
         a.server_id
        ,a.account_id
        ,a.type
        ,a.partner_account_id        
    from
        wt_pc_uniq  a
    where 1=1
    --  --  -
    union all
    --  --  -
    select
         a.server_id
        ,a.account_id
        ,a.type
        ,a.partner_account_id
    from
        wt_pch_uniq_minus_pc  a
    where 1=1
)

select * from wt_df
"""


def old_perform_recursive_query(df: DataFrame, parent: str, child: str) -> DataFrame:
    df_top = df.filter(df.user_id == 574859)
    df_union = df_top
    while True:
        df_bottom = (
            df_top.alias("a")
            .join(
                df.alias("b"),
                how='inner',
                on=[col(f'b.{parent}') == col(f'a.{child}')],
            )
            .filter(col("b.partner_user_id") != -1)
            .select(
                col("b.user_id").alias("user_id"),
                col("b.partner_user_id").alias("partner_user_id"),
                (col("a.level") + 1).alias("level"),
                col("a.root_partner_id").alias("root_partner_id"),
            )
        )

        if df_bottom.count() == 0:
            break

        df_union = df_union.unionAll(df_bottom)
        df_top = df_bottom

    return df_union.sort([df.level, df.user_id])


def perform_recursive_query(df: DataFrame, parent: str, child: str) -> DataFrame:
    df_top = (
        df.alias("a")
        .join(
            df.alias("b"),
            how='left_anti',
            on=[col(f'a.{parent}') == col(f'b.{child}')],
        )
        .filter(df.partner_account_id == 1613337)
        .select(
            lit(2).alias("server_id"),  # partner server is always Classic (id = 2)
            lit(-1).alias(parent),
            col(f'a.{parent}').alias(child),
            lit(1).alias("level"),
            col(f'a.{parent}').alias("root_partner_account_id"),
            col(f'a.{parent}').cast('string').alias("mat_path"),
        )
        .distinct()
    )

    df_lev2 = (
        df_top.alias("a")
        .join(
            df.alias("b"),
            how='inner',
            on=[
                (col(f'b.{parent}') == col(f'a.{child}'))
                # & (col("b.server_id") == col("a.server_id"))
            ],
        )
        .select(
            col("b.server_id").alias("server_id"),
            col("b.account_id").alias("account_id"),
            col("b.partner_account_id").alias("partner_account_id"),
            (col("a.level") + 1).alias("level"),
            col("a.root_partner_account_id").alias("root_partner_account_id"),
            concat_ws('.', col("a.mat_path"), col("b.account_id").cast('string')).alias("mat_path"),
        )
        .withColumn("acc_cnt", count(lit(1)).over(Window.partitionBy(child)))
        .filter(expr("account_id not like '%' || 'mat_path' || '%'"))
    )

    df_lev2.show(200)

    # print(' ------------------------------------------------------------------------------------------------------- ')
    # print(' ---------------------------------------------------------------------- df_top.show(5) ------------------')
    # print(' ------------------------------------------------------------------------------------------------------- ')
    # df_top.show(5)
    #
    # df_union = df_top
    # i = 1
    # print(' ------------------------------------------------------------------------------------------------------- ')
    # print(' ---------------------------------------------------------------------- df_top.count() = ', df_top.count())
    # print(' ------------------------------------------------------------------------------------------------------- ')
    #
    # while True:
    #     df_bottom = (
    #         df_top.alias("a")
    #         .join(
    #             df.alias("b"),
    #             how='inner',
    #             on=[
    #                 (col(f'b.{parent}') == col(f'a.{child}'))
    #                 # & (col("b.server_id") == col("a.server_id"))
    #             ],
    #         )
    #         # .filter(col("b.partner_user_id") != -1)
    #         .select(
    #             col("b.server_id").alias("server_id"),
    #             col("b.account_id").alias("account_id"),
    #             col("b.partner_account_id").alias("partner_account_id"),
    #             (col("a.level") + 1).alias("level"),
    #             col("a.root_partner_account_id").alias("root_partner_account_id"),
    #             concat_ws('.', col("a.mat_path"), col("b.account_id").cast('string')).alias("mat_path"),
    #         )
    #         .withColumn("acc_cnt", count().over(Window.partitionBy(child)))
    #         .filter(expr("account_id not like '%' || 'mat_path' || '%'"))
    #     )
    #     print(' ---------------------------------------------------------------------------------------------------- ')
    #     print(' ------------------------------------------------------------- df_bottom.count() = ', df_bottom.count())
    #     print(' --------------------------------------------------------------i = ------------------------ ', i)
    #     if df_bottom.count() == 0 or i >= 3:
    #         break
    #
    #     df_union = df_union.unionAll(df_bottom)
    #     df_top = df_bottom
    #     i += 1

    df_union = df_lev2
    return df_union.sort([
        df_union.level,
        df_union.partner_account_id,
        df_union.account_id,
    ])


def main():
    conf = CONF  # SparkConf().setAll(CONF)

    with start_spark(
        app_name='recursive',
        config=conf,
    ) as spark:
        logger = get_spark_logger(spark)
        spark.sparkContext.setLogLevel("WARN")

        df = spark.sql(SQL).cache()

        # for i in spark.sparkContext.getConf().getAll():
        #     print(i)

        df = perform_recursive_query(df, 'partner_account_id', 'account_id')
        print(' ---------------------------------------------------------------------------------------------------- ')
        print(' --------------------------------------------------------------------------- df.count() = ', df.count())
        print(' ---------------------------------------------------------------------------------------------------- ')
        df.show(10)


if __name__ == '__main__':
    main()
