import sqlparse
from sqlparse.sql import IdentifierList, Where, Token
from sqlparse.tokens import Keyword, DML
from re import split


def format_aliases(token: IdentifierList) -> str:
    is_first_select_field = True
    o_sql = ''
    for identifier in token.get_identifiers():
        alias = f'  as {identifier.get_alias()}' if identifier.get_alias() else ''
        if is_first_select_field:
            o_sql += f"     {identifier.get_real_name()}{alias}\n"
            is_first_select_field = False
        else:
            o_sql += f"    ,{identifier.get_real_name()}{alias}\n"
    return o_sql


def format_where(token: Where) -> str:
    o_sql = ''
    where_tokens = [i.value for i in token.tokens[1:]]
    o_sql += f"where 1=1\n    and{''.join(where_tokens)}"
    return o_sql



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
    # formatted_sql = "select\n"
    formatted_sql = ''

    for i, token in enumerate(parsed.tokens):
        # print(i, token)
        if isinstance(token, IdentifierList):
            formatted_sql += format_aliases(token)
        elif isinstance(token, Where):
            formatted_sql += format_where(token)
        # elif token.ttype is Keyword.DML:
        #     continue
        elif token.ttype is Keyword:
            formatted_sql += f"{token.value}\n"
        # elif 'order by' in token.value:
        #     formatted_sql += "order by\n"
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
with par (select 1  as pid)
SELECT FIELD0, FIELD1 AS A, FIELD2 B, FIELD3  AS C, FIELD4     D, FIELD5         AS E FROM TABLE1 WHERE DT='2023-01-01' AND IINT = 10 group by field0, field1 order by field2, field3
"""

# Format the SQL query
formatted_sql = format_sql_correctly(sql_query)

# Print the formatted SQL query
print(formatted_sql)
