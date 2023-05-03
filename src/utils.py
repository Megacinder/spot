from pathlib import Path
from pandas import DataFrame


def get_filename(path: str) -> str:
    return Path(path).name.replace('.py', '')


def auto_adjust_column_widths(excel_file: "Excel File Path", extra_space: int = 1) -> None:
    from openpyxl import load_workbook
    from openpyxl.utils import get_column_letter

    wb = load_workbook(excel_file)

    for ws in wb:
        df1 = DataFrame(ws.values, )
        for i, r in (df1.astype(str).applymap(len).max(axis=0) + extra_space).items():
            ws.column_dimensions[get_column_letter(i + 1)].width = r

    wb.save(excel_file)
