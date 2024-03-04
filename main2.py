S = """
    select
         1  as "{var}other"
        ,1  as "{var2}other"
    from
        table1
    where 1=1
"""

a = S.format(var="active ")  #.format(var2="fff")

print(a)
