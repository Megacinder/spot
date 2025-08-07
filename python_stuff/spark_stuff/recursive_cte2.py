from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import (
    col, concat_ws, date_add, date_format, explode, expr, last, lit, max, min, regexp_replace,
    round, sequence, to_date, transform, trim, count
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from src.spark.spark_utils import start_spark, get_spark_logger, get_folder_content

CONF = [
    # ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    # ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    # ("spark.sql.legacy.createHiveTableByDefault.enabled", "true"),
    # ('hive.exec.dynamic.partition', 'true'),
    # ('hive.exec.max.dynamic.partitions', '2048'),
    # ('hive.exec.dynamic.partition.mode', 'nonstrict'),
    ("spark.driver.memory", "96g"),
    ("spark.driver.cores", "32"),
    # ("spark.executor.memory", "64g"),
    # ("spark.executor.cores", "8"),
    ("spark.sql.autoBroadcastJoinThreshold", "-1"),
]

SQL_DF = """
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


SQL_LEV2 = """
with wt_root as (
    select distinct
         2  as server_id
        ,-1  as partner_account_id
        ,a.partner_account_id  as account_id
        ,'partner'  as type
        ,1  as level
        ,a.partner_account_id  as root_partner_account_id
        ,cast(a.partner_account_id as string)  as mat_path
    from 
        wt_df  a
    where 1=1
        and a.partner_account_id not in (select account_id from wt_df)
        and a.partner_account_id = 1613337
)


,wt_lev1 as (
    select
         a.server_id
        ,a.partner_account_id
        ,a.account_id
        ,a.type
        ,a.level
        ,a.root_partner_account_id
        ,a.mat_path
        ,count(1) over (partition by a.account_id)  as acc_cnt
    from 
        wt_root  a
    where 1=1
)


,wt_lev2 as (
    select
         b.server_id
        ,b.partner_account_id
        ,b.account_id
        ,b.type
        ,a.level + 1  as level
        ,a.root_partner_account_id
        ,a.mat_path || '.' || cast(b.account_id as string)  as mat_path
        ,count(1) over (partition by b.account_id)  as acc_cnt
    from
        wt_lev1  a
        join wt_df  b
            on  b.partner_account_id = a.account_id
            --and b.server_id = a.server_id
    where 1=1
--        and b.account_id = 1638377
)

select
    * 
from
    wt_lev1
where 1=1
--  --  -
union all
--  --  -
select
    *
from
    wt_lev2
where 1=1
--    and account_id = 1638377
"""

DELIM = '-------------------------------------------------------------------------------------------------------'


def perform_recursive_query(
    spark: SparkSession,
    df_lev2: DataFrame,
) -> DataFrame:

    df_lev2.createOrReplaceTempView("wt_lev")
    df_union = df_lev2
    # print(f'{DELIM}\n{DELIM} df_top.show(5) {DELIM}\n{DELIM}')
    i = 0
    while True:
        sql_lev = """
            select
                 b.server_id
                ,b.partner_account_id
                ,b.account_id
                ,b.type
                ,a.level + 1  as level
                ,a.root_partner_account_id
                ,a.mat_path || '.' || cast(b.account_id as string)  as mat_path
                ,count(1) over (partition by b.account_id)  as acc_cnt
            from
                wt_lev  a
                join wt_df  b
                    on  b.partner_account_id = a.account_id
                    and 1 = case
                        when a.acc_cnt > 1
                         and b.server_id = a.server_id
                            then
                                1
                        else
                            0
                    end
            where 1=1
        """
        print(f'{DELIM}\n{sql_lev}\n{DELIM}')
        df_bottom = spark.sql(sql_lev)

        # print(f'{DELIM}\n{DELIM} df_bottom.count() =  {df_bottom.count()}\n {DELIM}\n{DELIM}')
        if df_bottom.count() == 0:  # or i >= 10:
            break

        df_union = df_union.unionAll(df_bottom)
        df_union.createOrReplaceTempView("wt_lev")

        i += 1

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

        print(f'{DELIM}\n{SQL_DF}\n{SQL_LEV2}\n{DELIM}')
        df = spark.sql(SQL_DF).cache()
        print(df.explain())
        df.coalesce(1)
        print("total df count rows = ", df.count())
        df.createOrReplaceTempView("wt_df")
        df_lev2 = spark.sql(SQL_LEV2)
        # df_lev2.coalesce(1)
        df_lev2.show()

        # for i in spark.sparkContext.getConf().getAll():
        #     print(i)

        df = perform_recursive_query(spark, df_lev2)

        print(f'{DELIM}\n{DELIM} df.count() =  {df.count()}\n {DELIM}\n{DELIM}')
        df.show(500)


if __name__ == '__main__':
    main()
