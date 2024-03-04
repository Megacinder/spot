import sqlparse
from sqlparse.sql import IdentifierList, Where
from sqlparse.tokens import Keyword, DML
from re import split




def format_sql_correctly(sql: str) -> str:
    parsed = sqlparse.parse(
        sqlparse.format(
            sql=sql,
            reindent=True,
            keyword_case='lower',
            identifier_case='lower',
            use_space_around_operators=True,
            indent_width=4,
            comma_first=True,
        )
    )[0]
    formatted_sql = "select\n"
    is_first_select_field = True

    for token in parsed.tokens:
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                alias = f'  as {identifier.get_alias()}' if identifier.get_alias() else ''
                if is_first_select_field:
                    formatted_sql += f"     {identifier.get_real_name()}{alias}\n"
                    is_first_select_field = False
                else:
                    formatted_sql += f"    ,{identifier.get_real_name()}{alias}\n"
                # print(identifier.get_alias())
        elif token.ttype is Keyword.DML:
            continue
        elif token.ttype is Keyword or isinstance(token, Where):
            if 'where' in token.value.lower():
                formatted_sql += "where 1=1\n"
            else:
                formatted_sql += f"{token.value.lower()}\n"
        elif 'order by' in token.value.lower():
            formatted_sql += "order by\n"
        else:
            content = token.value.lower().strip()
            if content and not content.startswith('and'):
                formatted_sql += f"    {content}\n"
            elif content.startswith('and'):
                formatted_sql = formatted_sql.rstrip('\n') + f" {content}\n"

    # Final adjustments
    formatted_sql = formatted_sql.strip()  # Remove any leading/trailing whitespace

    return formatted_sql

# Example SQL query
sql_query = """
select field0, field1 as a, field2 b, field3  as c, field4     d, field5         as e from table1 where dt='2023-01-01'
"""

# Format the SQL query
formatted_sql = format_sql_correctly(sql_query)

# Print the formatted SQL query
print(formatted_sql)
