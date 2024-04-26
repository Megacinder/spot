alter procedure [dbo].[backup_table] (
      @source_table    nvarchar(100)
     ,@source_table_pk nvarchar(200)
     ,@target_table    nvarchar(100)
)
as
begin


/*
table       row_cnt
land        2459
lot         452273
community   7006
margin      990531
*/
set nocount on
--set datefirst 1

--exec sp_addlinkedserver
--     @server     = 'link_serv'
--    ,@provider   = 'SQLNCLI'
--    ,@datasrc    = 'localhost'
--    ,@srvproduct = ''
--    ,@provstr    = 'Integrated Security=SSPI'


-- ================================================
-- Author:      Mikhail Akhremenko
-- Create date: 2024-04-11
-- Description: copy table with adding extra fields
-- ================================================


-- declare @source_server nvarchar(100) = 'link_serv'
declare @source_server nvarchar(100) = 'ae2prdwvensql01'
-- declare @source_db     nvarchar(100) = 'cor'

declare @source_db     nvarchar(100) = 'vena_dm'
declare @source_schema nvarchar(100) = 'dbo'
declare @target_db     nvarchar(100) = 'vena_dm_backup'
declare @target_schema nvarchar(100) = 'dbo'


-- declare @source_table    nvarchar(100) = 'stg_dim_land_scd'  -- 0:02 - day week month, 0:01 - day
-- declare @source_table_pk nvarchar(200) = 'DivisionCd, LandCd'
-- declare @target_table    nvarchar(100) = 'z_stg_dim_land_scd_backup'

-- declare @source_table    nvarchar(100) = 'STG_Dim_Lot_SCD'  -- 1:41 - day week month, 0:38 - 1:00 - day
-- declare @source_table_pk nvarchar(200) = 'DivisionCd, LotCd'
-- declare @target_table    nvarchar(100) = 'z_stg_dim_lot_scd_backup'

-- declare @source_table    nvarchar(100) = 'STG_Dim_Community_SCD'  -- 0:02 - day week month, 0:01 - day
-- declare @source_table_pk nvarchar(200) = 'DivisionCd, CommunityCd'
-- declare @target_table    nvarchar(100) = 'z_stg_dim_community_scd_backup'

-- declare @source_table    nvarchar(100) = 'STG_Fact_ST_Margin_Analysis_SCD'  -- 2:58 - day week month, 0:55 - 1:00 - day
-- declare @source_table_pk nvarchar(200) = 'DivisionCd, LotCd'
-- declare @target_table    nvarchar(100) = 'z_stg_fact_st_margin_analysis_scd_backup'


-- declare @target_db     nvarchar(100) = 'cor'

--exec sp_addlinkedserver
--     @server     = 'link_serv'
--    ,@provider   = 'SQLNCLI'
--    ,@datasrc    = 'localhost'
--    ,@srvproduct = ''
--    ,@provstr    = 'Integrated Security=SSPI'






declare @days_back_day   int = 6
declare @days_back_week  int = 27
declare @days_back_month int = 364







declare @load_dt   datetime = getdate()  -- '2024-04-01 01:00:00'
declare @first_dow int = 2
declare @indicator int

declare @load_dt_text   nvarchar(20) = format(@load_dt, 'yyyy-MM-dd HH:mm:ss')
declare @days_back_dd   nvarchar(20) = cast(@days_back_day   as nvarchar(20))
declare @days_back_ww   nvarchar(20) = cast(@days_back_week  as nvarchar(20))
declare @days_back_mm   nvarchar(20) = cast(@days_back_month as nvarchar(20))
declare @first_dow_text nvarchar(20) = cast(@first_dow       as nvarchar(20))


-- conditions for delete data from the target table
declare @delete_cond nvarchar(1000) = '
    and (
            (BackupType = ''Daily''   and BackupDate <= dateadd(d, -1 * ' + @days_back_dd + ', ''' + @load_dt_text + '''))
        or  (BackupType = ''Weekly''  and BackupDate <= dateadd(d, -1 * ' + @days_back_ww + ', ''' + @load_dt_text + '''))
        or  (BackupType = ''Monthly'' and BackupDate <= dateadd(d, -1 * ' + @days_back_mm + ', ''' + @load_dt_text + '''))
        or  cast(BackupDate as date) = cast(''' + @load_dt_text + ''' as date)
    )
'

-- forming different types/periods of backup (actual are daily, weekly and monthly)
declare @wt_backup_way nvarchar(1000) = '
with wt_backup_way as (
    select ''Daily''    as typ union all
    select ''Weekly''   as typ where datepart(dw, ''' + @load_dt_text + ''') = ' + @first_dow_text + ' union all
    select ''Monthly''  as typ where datepart(d , ''' + @load_dt_text + ''') = 1
)
'

;

declare @source_table_full_name nvarchar(500) = @source_db  + '.' + @source_schema + '.' + @source_table
declare @target_table_full_name nvarchar(500) = @target_db  + '.' + @target_schema + '.' + @target_table




-- check if source and target table exist
declare @exec_sql nvarchar(max)
set @exec_sql = '
with wt_par as (
    select
         ''' + @source_table + '''  as table_name
)

,wt_ind as (
    select
        1  as indicator
    from
        openquery(
             "' + @source_server + '"
            ,''select name from ' + @source_db + '.sys.tables''
        )  st1
        join wt_par  pa1
            on  st1.name = case
                when pa1.table_name like ''%.%''
                    then
                        right(pa1.table_name, charindex(''.'', reverse(pa1.table_name)) - 1)
                else
                    pa1.table_name
            end
    where 1=1
    --  --  -
    union all
    --  --  -
    select
        2  as indicator
    from
        ' + @target_db + '.sys.tables  st1
        join wt_par  pa1
            on  st1.name = case
                when pa1.table_name like ''%.%''
                    then
                        right(pa1.table_name, charindex(''.'', reverse(pa1.table_name)) - 1)
                else
                    pa1.table_name
            end
    where 1=1
)

select
    @indicator = coalesce(sum(indicator), 0)
from
    wt_ind
'


exec sp_executesql
     @query     = @exec_sql
    ,@params    = N'@indicator int output'
    ,@indicator = @indicator output

print @indicator

-- if source table doesn't exist, raise an error and stop the procedure
if @indicator in (0, 2)
begin
    select
         @source_table  as input_table
        ,'There is no such table'  as error_message
        ,case @indicator
            when 0 then 'source and target'
            when 1 then 'source'
            -- when 2 then 'target'
        end  as server

    return
end

-- if target table doesn't, create it
if @indicator = 1
begin
    set @exec_sql = '
    if object_id(''' + @target_table_full_name + ''', ''u'') is null
        select
             cast(''Monthly'' as varchar(100))  as BackupType
            ,cast(getdate() as datetime)  as BackupDate
            ,a.*
        into
            ' + @target_table_full_name + '
        from
            openquery("' + @source_server + '", ''select * from ' + @source_table_full_name + ' where 1=0'')  a
    '
    exec sp_executesql @exec_sql
end
;


-- create sql scripts to delete and/or insert data from source to target
declare cur1 cursor for
with wt_par as (
    select
         case
             when @source_table like '%.%'
                 then right(@source_table, charindex('.', reverse(@source_table)) - 1)
             else
                 @source_table
         end as source_table

        ,case
             when @target_table like '%.%'
                 then right(@target_table, charindex('.', reverse(@target_table)) - 1)
             else
                 @target_table
         end as target_table

        ,@load_dt_text  as load_dt_text

        ,@days_back_dd  as days_back_dd
        ,@days_back_ww  as days_back_ww
        ,@days_back_mm  as days_back_mm
        ,@first_dow     as first_dow

        ,@source_server as source_server
        ,@source_db     as source_db
        ,@target_db     as target_db

        ,@source_table_full_name  as source_table_full_name
        ,@target_table_full_name  as target_table_full_name
        ,@source_table_pk  as source_table_pk


        ,@delete_cond   as delete_cond
        ,@wt_backup_way  as wt_backup_way
)


,wt_delete as (
    select
         1  as id
        ,'delete from '
        + pa1.target_table_full_name
        + ' '
        + 'where 1=1 '
        + pa1.delete_cond
        as sql1
    from
        wt_par  pa1
    where 1=1
)


,wt_insert as (
    select
         2  as id
        , 'insert into ' + pa1.target_table_full_name
        + ' '
        + 'select * '
        + 'from '
        + 'openquery("'
            + pa1.source_server
            + '"'
            + ','''
            + replace(pa1.wt_backup_way, '''', '''''')
            + '
                select
                     b.typ  as backup_type
                    ,''''' + pa1.load_dt_text + '''''  as backup_date
                    ,a.*
                from
                    ' + pa1.source_db + '.dbo.' + pa1.source_table + '  a
                    cross join wt_backup_way  b
                where 1=1
                order by
                     b.typ
                    ,' + pa1.source_table_pk + '
            ''
        )'
        as sql1
    from
        wt_par  pa1
    where 1=1
)


,wt_final_sql as (
    select id, sql1 from wt_delete
    --  --  -
    union all
    --  --  -
    select id, sql1 from wt_insert
)

-- the deletion is first, and then the insertion
select sql1 from wt_final_sql order by id




open cur1
fetch next from cur1 into @exec_sql

while @@fetch_status = 0
begin
    begin try
--        print @exec_sql
        exec sp_executesql @exec_sql
    end try
    begin catch
        select
             @target_table  as input_table
            ,error_message()  as error_message
            ,@exec_sql as exec_sql

        close cur1
        deallocate cur1

        return
    end catch

    fetch next from cur1 into @exec_sql
end


close cur1
deallocate cur1



end
