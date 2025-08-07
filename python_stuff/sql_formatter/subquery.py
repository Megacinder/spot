def format_sql(sql_query):
    lines = sql_query.split('\n')
    formatted_sql = ""
    in_cte = False
    in_select = False

    for line in lines:
        line_stripped = line.strip().lower()

        if line_stripped.startswith('with'):
            in_cte = True
            formatted_sql += "with "
            continue

        if in_cte:
            if line_stripped.endswith(')'):
                in_cte = False
            cte_name, cte_body = line.split(' as ', 1)
            if not cte_name.strip().startswith("wt_"):
                cte_name = "wt_" + cte_name.strip()
            formatted_sql += f"\n{cte_name} as (\n    {cte_body.lstrip().rstrip(')').strip()}\n)"
            continue

        if line_stripped.startswith('select'):
            in_select = True
            formatted_sql += "\n\nselect\n    "
            continue

        if in_select:
            if ' from ' in line_stripped:
                in_select = False
            else:
                if ',' in line:
                    formatted_sql = formatted_sql.rstrip('    ') + ',\n    '
                formatted_sql += line_stripped
                continue

        if 'where' in line_stripped:
            formatted_sql += f"\nwhere 1=1\n    and {line_stripped[6:]}"
            continue

        if line_stripped.startswith(('group by', 'order by')):
            formatted_sql += f"\n{line_stripped}\n    "
            continue

        if ' and ' in line_stripped:
            formatted_sql += f"    and {line_stripped[4:]}"
            continue

        formatted_sql += line_stripped

    return formatted_sql


sql_query = """
with par as (select 1  as pid), cte2 as (select 3  as id)
SELECT FIELD0, FIELD1 AS A, FIELD2 B, FIELD3  AS C, FIELD4     D, FIELD5         AS E
FROM TABLE1 WHERE DT='2023-01-01' AND IINT = 10 group by field0, field1
order by field2, field3
"""

formatted_sql_query = format_sql(sql_query)
print(formatted_sql_query)
