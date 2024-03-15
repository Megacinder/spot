import re

def parse_select_part(select_part):
    # A regex pattern to match columns, taking care not to split inside parentheses (for subqueries)
    columns_pattern = re.compile(r'\s*,\s*(?![^()]*\))')
    columns = re.split(columns_pattern, select_part)
    formatted_columns = []

    for column in columns:
        # Trim whitespace for each column
        column = column.strip()

        # Basic detection of alias using 'as', outside of parentheses
        if ' as ' in column and '(' not in column:
            parts = column.rsplit(' as ', 1)  # Split at the last ' as '
            column_name, alias = parts[0], parts[1]
            formatted_columns.append(f"{column_name} as {alias}")
        else:
            # Attempt to handle subqueries and other expressions without 'as' aliasing
            formatted_columns.append(column)

    return ', '.join(formatted_columns)

# Example usage
select_part = "select 1 as id, (select 2 from table2 b where b.id = a.id)    as b from table1 a"
formatted_select_part = parse_select_part(select_part)
print(formatted_select_part)
