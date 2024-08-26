


-- 1. will drop an error "division by zero"
select
    nvl(1, 1 / 0)  as nvl_ex



-- 2. will return "1"
select
    coalesce(1, 1 / 0)  as coalesce_ex




select
    -1 * amt
from
    table


-- 1
select
     price * (1 - discount / 100)  as average_price_per_unit
    ,quantity * price * (discount / 100)  as total_discount
    ,quantity * price * (1 - discount / 100) * (tax_rate / 100)  as total_tax_amount
    ,-(quantity * price)  as sign_inverted_total_cost_before_discount

    ,(quantity * price * (1 - discount / 100))  as sign_inverted_total_cost_after_discount
    ,(quantity * price * (1 - discount / 100) * (1 + tax_rate / 100))  as sign_inverted_total_cost_with_tax
    ,(quantity * price * (1 - discount / 100) * (tax_rate / 100))  as sign_inverted_total_tax
    ,(quantity * price * (discount / 100))  as sign_inverted_total_discount
    ,-quantity * price - (quantity * price * (discount / 100))  as cost_after_discount_subtracted

    ,(quantity * price * (1 - discount / 100)) - (-quantity * price * (1 - discount / 100) * (tax_rate / 100))  as cost_after_discount_minus_tax
    ,(quantity * price * (1 - discount / 100) * (1 + tax_rate / 100)) - (quantity * price)  as total_cost_with_tax_minus_original
    ,(quantity * price) - (quantity * price * (1 - discount / 100))  as discount_amount
    ,(quantity * price * (1 - discount / 100)) - (quantity * price * (1 - discount / 100) * (1 + tax_rate / 100))  as tax_amount
from
    sales


-- 2
select
     price * (1 - discount / 100)  as average_price_per_unit
    ,quantity * price * (discount / 100)  as total_discount
    ,quantity * price * (1 - discount / 100) * (tax_rate / 100)  as total_tax_amount
    ,(quantity * price)  as sign_inverted_total_cost_before_discount

    ,(quantity * price * (1 - discount / 100))  as sign_inverted_total_cost_after_discount
    ,(quantity * price * (1 - discount / 100) * (1 + tax_rate / 100))  as sign_inverted_total_cost_with_tax
    ,(quantity * price * (1 - discount / 100) * (tax_rate / 100))  as sign_inverted_total_tax
    ,(quantity * price * (discount / 100))  as sign_inverted_total_discount
    ,-1 * quantity * price - (quantity * price * (discount / 100))  as cost_after_discount_subtracted

    ,(quantity * price * (1 - discount / 100)) - (-1 * quantity * price * (1 - discount / 100) * (tax_rate / 100))  as cost_after_discount_minus_tax
    ,(quantity * price * (1 - discount / 100) * -(1 + tax_rate / 100)) - (quantity * price)  as total_cost_with_tax_minus_original
    ,(quantity * price) - (quantity * price * (1 - discount / 100))  as discount_amount
    ,(quantity * price * (1 - discount / 100)) - (quantity * price * (1 - discount / 100) * (1 + tax_rate / 100))  as tax_amount
from
    sales



-- 1
select
    *
from
    user
where -- name = 'Vasya'  -- ERROR
    and name = 'Petya'
    and now() between valid_from_dt and valid_to_dt
    and active_flg = 1


-- 2
select
    *
from
    user
where 1=1
    -- and name = 'Vasya'
    and name = 'Petya'
    and now() between valid_from_dt and valid_to_dt
    and active_flg = 1


-- 3
select
     user_name
    ,count(1)  as user_cnt
    ,avg(age)  as user_age_avg
from
    user
where 1=1
group by
    user_name
having 1=1
    and count(1) < 3
    and avg(age) > 50


-- 4
create table user2 as
select
    *
from
    user
where 1=0  -- create a new table from the original with no data
    -- and name = 'Vasya'
    and name = 'Petya'
    and now() between valid_from_dt and valid_to_dt
    and active_flg = 1


--2
select
    data.key
    ,data.summary
    ,data.updatedat
    ,data.createdat

    ,data.createdby['display']  as createdby_display
    ,data.createdby['id']       as createdby_id
    ,data.resolvedby['display'] as resolvedby_display
    ,data.resolvedby['id']      as resolvedby_id

    ,data.status['display']     as status_display
    ,data.resolution['display'] as resolition_display

    ,data.tags
    ,data.components
from
    v_st_api_data
where 1=1

union all

select
    data.key
    ,data.summary
    ,data.updatedat
    ,data.createdat

    ,data.createdby['display']
    ,data.createdby['id']
    ,data.resolvedby['display']
    ,data.resolvedby['id']

    ,data.status['display']
    ,data.resolution['display']

    ,data.tags
    ,data.components
from
    v_st_api_data_with_cnb_token
where 1=1




-- 1
select
     acc_id

    ,sum(bal)                as bal
    ,sum(in_out)             as in_out
    ,sum(revenue_and_costs)  as revenue_and_costs
    ,sum(transfer)           as transfer
    ,sum(end_bal)            as end_bal
    ,sum(diff)               as diff
    ,sum(transaction_cnt)    as transaction_cnt
from
    cash_info
where 1=1
group by
    acc_id



-- 2
select
                price * (1 - discount / 100)                     as average_price_per_unit
    ,quantity * price * (    discount / 100)                     as total_discount
    ,quantity * price * (1 - discount / 100) * (tax_rate / 100)  as total_tax_amount
    ,quantity * price                                            as sign_inverted_total_cost_before_discount

    ,quantity * price * (1 - discount / 100)                         as sign_inverted_total_cost_after_discount
    ,quantity * price * (1 - discount / 100) * (1 + tax_rate / 100)  as sign_inverted_total_cost_with_tax
    ,quantity * price * (1 - discount / 100) * (    tax_rate / 100)  as sign_inverted_total_tax
    ,quantity * price * (    discount / 100)                         as sign_inverted_total_discount

    ,-1 * quantity * price - (quantity * price * (discount / 100))   as cost_after_discount_subtracted

    ,(quantity * price * (1 - discount / 100)) - (-1 * quantity * price * (1 - discount / 100) * (tax_rate / 100))  as cost_after_discount_minus_tax
    ,(quantity * price * (1 - discount / 100)) - (quantity * price * (1 - discount / 100) * (1 + tax_rate / 100))   as tax_amount
    ,(quantity * price * (1 - discount / 100) * (1 + tax_rate / 100)) - (quantity * price)                          as total_cost_with_tax_minus_original
    ,(quantity * price) - (quantity * price * (1 - discount / 100))                                                 as discount_amount
from
    sales




--3


-- 1
select
     a.column_name

    ,coalesce(
         s1.column_id
        ,s2.column_id
        ,s3.column_id
        ,s4.column_id
        ,s5.column_id
        ,s6.column_id
        ,s7.column_id
    )  as column_id

    ,s1.column_type  as data_type_1
    ,s2.column_type  as data_type_2
    ,s3.column_type  as data_type_3
    ,s4.column_type  as data_type_4
    ,s5.column_type  as data_type_5
    ,s6.column_type  as data_type_6
    ,s7.column_type  as data_type_7
from
    table_and_column  a

    left join wt_metadata_prep_source_as  s1 on s1.source = '1' and s1.table_name = a.table_name and s1.column_name = a.column_name
    left join wt_metadata_prep_source_as  s2 on s2.source = '2' and s2.table_name = a.table_name and s2.column_name = a.column_name
    left join wt_metadata_prep_source_as  s3 on s3.source = '3' and s3.table_name = a.table_name and s3.column_name = a.column_name
    left join wt_metadata_prep_source_as  s4 on s4.source = '4' and s4.table_name = a.table_name and s4.column_name = a.column_name
    left join wt_metadata_prep_source_as  s5 on s5.source = '5' and s5.table_name = a.table_name and s5.column_name = a.column_name
    left join wt_metadata_prep_source_as  s6 on s6.source = '6' and s6.table_name = a.table_name and s6.column_name = a.column_name
    left join wt_metadata_prep_source_as  s7 on s7.source = '7' and s7.table_name = a.table_name and s7.column_name = a.column_name
where 1=1




-- 2
select
     a.column_name

    ,max(
        case s.source
            when '1' then s.column_id
            when '2' then s.column_id
            when '3' then s.column_id
            when '4' then s.column_id
            when '5' then s.column_id
            when '6' then s.column_id
            when '7' then s.column_id
            else null
         end
    )  as column_id

    ,max(case when s.source = '1' then s.column_type else null end)  as data_type_1
    ,max(case when s.source = '2' then s.column_type else null end)  as data_type_2
    ,max(case when s.source = '3' then s.column_type else null end)  as data_type_3
    ,max(case when s.source = '4' then s.column_type else null end)  as data_type_4
    ,max(case when s.source = '5' then s.column_type else null end)  as data_type_5
    ,max(case when s.source = '6' then s.column_type else null end)  as data_type_6
    ,max(case when s.source = '7' then s.column_type else null end)  as data_type_7
from
    table_and_column  a
    join wt_metadata_prep_source_as  s
        on  s.table_name  = a.table_name
        and s.column_name = a.column_name
where 1=1
group by
    a.column_name


-- 2
select
     a.column_name

    ,max(
        case s.source
            when '1' then s.column_id
            when '2' then s.column_id
            when '3' then s.column_id
            when '4' then s.column_id
            when '5' then s.column_id
            when '6' then s.column_id
            when '7' then s.column_id
            else null
         end
    )  as column_id

    ,max(
        case
            when s.source = '1'
                then s.column_type
            else null
        end
     )  as data_type_1
    ,max(
        case
            when s.source = '2'
                then s.column_type
            else null
        end
     )  as data_type_2
    ,max(
        case
            when s.source = '3'
                then s.column_type
            else null
        end
     )  as data_type_3
    ,max(
        case
            when s.source = '4'
                then s.column_type
            else null
        end
     )  as data_type_4
    ,max(
        case
            when s.source = '5'
                then s.column_type
            else null
        end
     )  as data_type_5
    ,max(case when s.source = '6' then s.column_type else null end)  as data_type_6
    ,max(case when s.source = '7' then s.column_type else null end)  as data_type_7
from
    table_and_column  a
    join wt_metadata_prep_source_as  s
        on  s.table_name  = a.table_name
        and s.column_name = a.column_name
where 1=1
group by
    a.column_name



--5
    
select
     de1.number
    ,ac1.account_number  as loan_account_number
from
    deposit  de1
    join account  ac1
        on  ac1.account_id = de1.loan_account_id
        and ac1.user_id    = de1.user_id
        and ac1.create_dt >= de1.create_dt - 1
        and ac1.create_dt <  de1.create_dt + 2
        and ac1.delete_flg = 0
where 1=1
    and de1.delete_flg = 0




--6
with wt_par as (
    select
         date'2019-05-01'  as from_dt
        ,date'2019-05-21'  as to_dt
)

,wt_user_name as (
    select
         us1.user_id
        ,us1.user_name
    from
        user  us1
        join wt_par  pa1
            on  us1.dt >= pa1.from_dt
            and us1.dt <  pa1.to_dt + 1
    where 1=1
)

,wt_bal as (
    select
         ba1.user_id
        ,sum(ba1.bal)      as bal
        ,sum(ba1.end_bal)  as end_bal
        ,sum(ba1.transaction_sum)  as transaction_sum
    from
        wt_bal_prep  ba1
        join wt_par  pa1
            on  ba1.dt >= pa1.from_dt
            and ba1.dt <  pa1.to_dt + 1
    where 1=1
    group by
        ba1.user_id
)

...

--7


-- 1
with wt_par as (
    select
        '40817810000001111155'  as acc
)
select
      ba1.account_id
     ,ba1.balance_usd
from
    account_balance   ba1
    join wt_par  pa1
        and 1 = case
            when pa1.acc is null
                then 1
            when pa1.acc is not null
             and ba1.acccount_number = pa1.pa1.acc
                then 1
            else 0
        end


-- 2
select
      ba1.account_id
     ,ba1.balance_usd
from
    account_balance   ba1
    join wt_par  pa1
        and 1 = case
            when {acc} is null
                then 1
            when {acc} is not null
             and ba1.acccount_number = {acc}
                then 1
            else 0
        end



--8
        
with wt_par as (
    select
        'desc'  as sorting_type
)

select
    *
from
    account  a
    cross join wt_par  pa1
order by
     case when pa1.sorting_type = 'desc' then a.create_dt else null end desc
    ,case when pa1.sorting_type = 'asc'  then a.create_dt else null end



--9

-- 1
select
    a.account_number
from
    account  a
where 1=1
    and a.account_id in (
        select
            account_id
        from
            account_balance
    )


-- 2
select distinct
    a.account_number
from
    account  a
    join account_balance  ab
        on ab.account_id = a.account_id
where 1=1




--10


-- 1
select
     a.account_id
    ,ap1.number       as account_number
    ,ba1.balance_usd  as bal_for_20190201
from
    account  a

    join account_features  ap1
        on  ap1.account_id     = a.account_id
        and ap1.valid_from_dt <= date'2019-02-01'
        and ap1.valid_to_dt   >  date'2019-02-01'

    join account_balance   ba1
        on  ba1.account_id = a.account_id
        and ba1.balance_dt = date'2019-02-01'
where 1=1
    and a.account_id = 123


-- 2
with wt_par as (
    select
         123  as acc_id
        ,date'2019-02-01'  as dt
)

,wt_acc_bal_pos as (
    select
         account_id
        ,number
    from
        account_feature
    where 1=1
        and account_id     = (select acc_id from wt_par limit 1)
        and valid_from_df <= (select dt     from wt_par limit 1)
        and valid_to_df   >  (select dt     from wt_par limit 1)
)

,wt_acc_bal as(
    select
         account_id
        ,balance_usd  as bal_for_20190201
    from
        account_balance
    where 1=1
        and account_id = (select acc_id from wt_par limit 1)
        and balance_dt = (select dt     from wt_par limit 1)
)

select
     a.account_id
    ,ap1.number  as account_number
    ,ba1.bal_for_20190201
from
    account  a
    join wt_acc_bal_pos  ap1 on ap1.account_id = a.account_id
    join wt_acc_bal      ba1 on ba1.account_id = a.account_id
where 1=1
    and a.account_id = (select acc_id from wt_par limit 1)
