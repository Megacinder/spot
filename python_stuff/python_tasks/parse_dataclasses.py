import re
import os

COMMON = 'Common'
COMMON_META = 'common_meta'
COMMON_META_IMPORT = f"""from alm_test_tools.table_class import {COMMON_META}
"""
FILE_HEAD = """from dataclasses import dataclass
from alm_test_tools.generator_mixin import GeneratorMixin
from alm_test_tools.table_class.common_meta import Register
{add_import}

@dataclass{dc}"""
DATETIME_IMPORT = 'from datetime import datetime'
HEAD_PKG = 'table_class'


def get_file_content(dc):
    if COMMON in dc:
        if 'datetime' in dc:
            o_str = FILE_HEAD.format(add_import=COMMON_META_IMPORT + '\n' + DATETIME_IMPORT, dc=dc)
        else:
            o_str = FILE_HEAD.format(add_import=COMMON_META_IMPORT, dc=dc)
        o_str = o_str.replace(COMMON, f'{COMMON_META}.{COMMON}')
    else:
        if 'datetime' in dc:
            o_str = FILE_HEAD.format(add_import=DATETIME_IMPORT, dc=dc)
        else:
            o_str = FILE_HEAD.format(add_import='', dc=dc)
    return o_str


def get_schema(dc):
    return re.findall(r"schema = '(\w+)'", dc)[0]


def get_table(dc):
    return re.findall(r"table = '(\w+)'", dc)[0]


def save_module(schema, table, file_content):
    file_name = f'{HEAD_PKG}/{schema}/{table}.py'
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    f1 = open(file_name, 'w')
    f1.write(file_content)
    f1.close()


def save_init(schema, table_list):
    file_name = f'{HEAD_PKG}/{schema}/__init__.py'
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    f1 = open(file_name, 'w')
    table_list_to_string = '[\n    \'' + '\',\n    \''.join(table_list) + '\',\n]'
    f1.write(f'__all__ = {table_list_to_string}\n')
    f1.close()


s = ''
with open('table_classes.py') as f:
    for line in f.readlines():
        s += line

d = dict()
for i in s.split('\n\n@dataclass'):
    if 'schema' in i:
        schema = get_schema(i)
        table = get_table(i)
        if schema in d:
            d[schema].append(table)
        else:
            d[schema] = [table]
        file_content = get_file_content(i)
        save_module(schema=schema, table=table, file_content=file_content)

for schema, table_list in d.items():
    save_init(schema=schema, table_list=table_list)
