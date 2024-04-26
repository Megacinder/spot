

set nocount on
set datefirst 1



declare @source_db                nvarchar(100) = 'cor' -- source database
declare @tables_to_backup         nvarchar(100) = 'dim_something,%scd%' -- List of masks for tables we want to backup, e.g. ('STG_Dim_%_SCD', 'STG_Fact_ST_Margin_Analysis_SCD')
declare @tables_to_exclude        nvarchar(100) = 'stg_dim_land_scd_backup' --'scd_tables_backup,%backup%'
declare @days_back_daily_backup   int           = 6   -- By analogy to backup.bat in Export fodler. E.g., 6.
declare @days_back_weekly_backup  int           = 27  -- By analogy to backup.bat in Export fodler. E.g., 27.
declare @days_back_monthly_backup int           = 364 -- By analogy to backup.bat in Export fodler. E.g., 364.
declare @delimiter                nvarchar(3)   = ','
declare @load_dt                  datetime      = '2024-04-08 01:00:00'  --getdate()


declare @sql1       nvarchar(max)
declare @insert_sql nvarchar(1000)


;


--create table stg_dim_land_scd_backup (
--     BackupType             varchar(100)
--    ,BackupDate             datetime
--    ,DivisionCd             int
--    ,DivisionDesc           varchar(200)
--    ,LandCd                 varchar(200)
--    ,PrimaryLandCd          varchar(200)
--    ,LandStatusCd           int
--    ,LandStatusDesc         varchar(200)
--    ,CurrentLandStatusCd    varchar(200)
--    ,CurrentLandStatusDesc  varchar(200)
--    ,UpdateDate             datetime
--    ,HashSCD2               varchar(200)
--    ,EffectiveFromDate      date
--    ,EffectiveToDate        date
--)




declare cur1 cursor for
with wt_par as (
    select
         @source_db  as source_db
        ,@tables_to_backup   as for_include
        ,@tables_to_exclude  as for_exclude
        ,@delimiter  as delimiter

        ,@load_dt  as load_dt
)


,wt_xml as (
    select
         cast('<a>' + replace(for_include, delimiter, '</a><a>') + '</a>' as xml)  as for_include
        ,cast('<a>' + replace(for_exclude, delimiter, '</a><a>') + '</a>' as xml)  as for_exclude
    from
        wt_par
    where 1=1
)


,wt_unnest_include as (
     select
          a.value('.', 'nvarchar(max)')  as backup_table
         ,case when a.value('.', 'nvarchar(max)') like '%\%%' escape '\' then 1 else 0 end  as is_like_filter
    from
        wt_xml  a
        cross apply for_include.nodes('/a')  split(a)
    where 1=1
)
--select * from wt_unnest_include

,wt_unnest_exclude as (
     select
          a.value('.', 'nvarchar(max)')  as backup_table
         ,case when a.value('.', 'nvarchar(max)') like '%\%%' escape '\' then 1 else 0 end  as is_like_filter
    from
        wt_xml  a
        cross apply for_exclude.nodes('/a')  split(a)
    where 1=1
)
--select * from wt_unnest_exclude



,wt_include_table as (
    select
         ss1.name + '.' + st1.name  as table_name
        ,st1.name  as short_table_name
    from
        -- source database and table is here
        cor.sys.tables  st1
        -- source database and table is here
        join cor.sys.schemas  ss1
            on ss1.schema_id = st1.schema_id

        join wt_unnest_include  in1
            on 1 = case
                when in1.is_like_filter = 1
                 and st1.name like in1.backup_table
                    then
                        1
                when in1.is_like_filter = 0
                 and st1.name = in1.backup_table
                    then
                        1
                else
                    0
            end

--        join wt_unnest_exclude  ex1
--            on 1 = case
--                when ex1.is_like_filter = 1
--                 and st1.name like ex1.backup_table
--                    then
--                        0
--                when ex1.is_like_filter = 0
--                 and st1.name = ex1.backup_table
--                    then
--                        0
--                else
--                    1
--            end
    where 1=1
        and st1.is_ms_shipped = 0
)


,wt_exclude_table as (
    select
         a.table_name
    from
        wt_include_table  a

        join wt_unnest_exclude  ex1
            on 1 = case
                when ex1.is_like_filter = 1
                 and a.short_table_name like ex1.backup_table
                    then
                        1
                when ex1.is_like_filter = 0
                 and a.short_table_name = ex1.backup_table
                    then
                        1
                else
                    0
            end
    where 1=1
)


,wt_extra_cols as (
    select 'Daily'    as typ, load_dt from wt_par
    union all
    select 'Weekly'   as typ, load_dt from wt_par where datepart(dw, load_dt) = 1
    union all
    select 'Monthly'  as typ, load_dt from wt_par where datepart(d , load_dt) = 1
)


select
    'select '
        + '''' + b.typ + '''  as backup_type'
        + ', ''' + convert(varchar(20), b.load_dt, 120) + '''  as backup_date'
        + ', *'
        + ' from '
        + table_name  as sql1
from
    wt_include_table  a
    cross join wt_extra_cols  b
where 1=1
    and table_name not in (select table_name from wt_exclude_table)




open cur1
fetch next from cur1 into @insert_sql


while @@fetch_status = 0
begin

    insert into dbo.stg_dim_land_scd_backup
    exec sp_executesql @insert_sql
    --print @insert_sql

    fetch next from cur1 into @insert_sql
end

close cur1
deallocate cur1



delete
from
    dbo.stg_dim_land_scd_backup
where 1=1
    and (
            (BackupType = 'Daily'   and backupdate = dateadd(d, -1 * @days_back_daily_backup  , @load_dt))
        or  (BackupType = 'Weekly'  and backupdate = dateadd(d, -1 * @days_back_weekly_backup , @load_dt))
        or  (BackupType = 'Monthly' and backupdate = dateadd(d, -1 * @days_back_monthly_backup, @load_dt))
    )


select * from dbo.stg_dim_land_scd_backup