import sqlparse
from sqlparse.sql import IdentifierList, Where, Token, Statement
from sqlparse.tokens import Keyword, DML, Newline
from re import split
from src.sqlparse_ext import CTE


def format_cols_and_aliases(token: IdentifierList) -> str:
    is_first_field_in_select = True
    o_sql = ''
    for identifier in token.get_identifiers():
        alias = f'  as {identifier.get_alias()}' if identifier.get_alias() else ''
        if is_first_field_in_select:
            o_sql += f"     {identifier.get_real_name()}{alias}\n"
            is_first_field_in_select = False
        else:
            o_sql += f"    ,{identifier.get_real_name()}{alias}\n"
    return o_sql


def format_where(token: Where) -> str:
    o_sql = ''
    where_tokens = [i.value for i in token.tokens[1:]]
    o_sql += f"where 1=1\n    and{''.join(where_tokens)}"
    return o_sql


def format_cte(token: CTE, prefix: str = 'wt_') -> str:
    modified_sql = ""
    within_with_clause = False

    if token.ttype == Keyword.CTE:
        within_with_clause = True
        modified_sql += token.value

    if within_with_clause:
        if token.is_whitespace or token.ttype is Newline:
            modified_sql += token.value

        if isinstance(token, (sqlparse.sql.IdentifierList, sqlparse.sql.Identifier)):
            identifiers = token.get_identifiers()
            new_cte_str = ''
            for identifier in identifiers:
                parts = [part for part in identifier.tokens if not part.is_whitespace]
                cte_name = parts[0].value

                if not cte_name.lower().startswith(prefix):
                    cte_name = prefix + cte_name

                cte_query = ''.join(str(part) for part in parts[2:])
                new_cte_str += f"{cte_name} AS {cte_query}"

            modified_sql += new_cte_str
        else:
            modified_sql += token.value
    else:
        modified_sql += token.value

    return modified_sql


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
        print(i, token, token.ttype, token.__class__)
        if isinstance(token, IdentifierList):
            formatted_sql += format_cols_and_aliases(token)
        elif isinstance(token, Where):
            formatted_sql += format_where(token)
        # elif token.ttype is Keyword.DML:
        #     continue
        elif token.ttype is Keyword:
            formatted_sql += f"{token.value}\n"
        # elif token.ttype is Keyword.CTE:  # isinstance(token, CTE):
        # formatted_sql += format_cte(token)
        # else:
        #     content = token.value.lower().strip()
        #     if content and not content.startswith('and'):
        #         formatted_sql += f"    {content}\n"
        #     elif content.startswith('and'):
        #         formatted_sql = formatted_sql.rstrip('\n') + f" {content}\n"

    # Final adjustments
    formatted_sql = formatted_sql.strip()  # Remove any leading/trailing whitespace

    return formatted_sql

# Example SQL query
sql_query = """
with par as (select 1  as pid), cte2 as (select 3  as id)
SELECT FIELD0, FIELD1 AS A, FIELD2 B, FIELD3  AS C, FIELD4     D, FIELD5         AS E
FROM TABLE1 WHERE DT='2023-01-01' AND IINT = 10 group by field0, field1
order by field2, field3
"""

# Format the SQL query
formatted_sql = format_sql_correctly(sql_query)
# formatted_sql = format_cols_and_aliases(sql_query)

# Print the formatted SQL query
print('---------------------------------')
print(formatted_sql)
