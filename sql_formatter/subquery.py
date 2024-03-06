def custom_format_sql(raw_sql):
    # Splitting the input SQL into lines
    lines = raw_sql.split('\n')

    # Placeholder for formatted SQL
    formatted_lines = []

    for line in lines:
        stripped_line = line.strip()
        if stripped_line.lower().startswith('select'):
            # Adding an indent to the 'select' line
            formatted_lines.append('    ' + stripped_line)
        elif stripped_line.endswith(('as', 'as', ',')):
            # Aligning the columns with additional indent
            formatted_lines.append('        ' + stripped_line)
        else:
            # Keeping other lines as is, but ensuring consistent indentation for blocks
            if stripped_line and not stripped_line.lower().startswith('with'):
                formatted_lines.append('    ' + stripped_line)
            else:
                formatted_lines.append(stripped_line)

    # Joining the lines back into a single string
    return '\n'.join(formatted_lines)


# Your original SQL query
raw_sql = """
with wt_par as (
select 20230101 as from_dt
      ,20231231 as to_dt
      ,'bvi'    as jurisdiction
      ,0        as should_the_data_be_masked
)
"""

# Applying the custom formatter
formatted_sql = custom_format_sql(raw_sql)

print(formatted_sql)
