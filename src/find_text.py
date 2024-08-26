import os
import re
from pathlib import Path


PARSED_SQL = "sql_from_parsed_dtsx"
PATTERN_REGEX = r'Inbound_NAV'


def search_text_in_directory(directory: str, text: str, exclude_ext: list) -> list:
    matches = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if Path(file).suffix not in exclude_ext:
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        contents = f.read().lower()
                        text = text.lower()
                        if re.search(text, contents):
                            matches.append(file_path)
                except:
                    continue
    return matches


def find_all_text(directory: str, text_to_search: list, exclude_ext: list):
    for text in text_to_search:
        result = search_text_in_directory(directory, text, exclude_ext)
        if result:
            for path in result:
                path = path.replace(f"{directory}\\{PARSED_SQL}\\", "")
                path = Path(path)
                print(text, ':', ":".join(path.parts))
    # return


EXCUDE_EXT: list = ['.dtsx']
DIRECTORY_PATH = r"C:\Users\guppi\PycharmProjects\ashton_woods"
TEXT_TO_SEARCH = [
    'IN_NAV_GL_Entry_History',
    'IN_NAV_Construction_Stage',
    'IN_NAV_Agent',
    'IN_NAV_Floorplan',
    'IN_NAV_Company_Information',
    'IN_NAV_GL_Budget_Entry',
    'IN_NAV_Indirect_Costs',
    'IN_NAV_GL_Entry_Current',
    'IN_NAV_Job',
    'IN_NAV_Job_Ledger_Entry_Current',
    'IN_NAV_Job_Ledger_Entry_History',
    'IN_NAV_Job_Planning_Line',
    'IN_NAV_Job_Task',
    'IN_NAV_Lot_Type',
    'IN_NAV_Margin_Analysis_Archive_Line',
    'IN_NAV_Project_Assumption_Sales_Margin',
    'IN_NAV_Project_Land',
    'IN_NAV_Reason_Code',
    'IN_NAV_Sales_Header',
    'IN_NAV_Sales_Invoice_Header',
    'IN_NAV_Global_Enums',
    'IN_NAV_Sales_Invoice_Line',
    'IN_NAV_Global_Options',
    'IN_NAV_Sales_Line',
    'IN_NAV_Master_Cost_Class_Group',
    'IN_NAV_Master_Cost_Class_Type_GL_Account',
    'IN_NAV_Master_Cost_Revenue_Class',
    'IN_NAV_Master_Cost_Type',
    'IN_NAV_Master_Division_Group',
    'IN_NAV_Master_GL_Account',
    'IN_NAV_Master_Land_Status',
    'IN_NAV_Master_Project_Type',
    'IN_NAV_Master_Web_Cost_Class',
]


if __name__ == '__main__':
    # find_all_text(DIRECTORY_PATH, TEXT_TO_SEARCH, EXCUDE_EXT)
    for i in TEXT_TO_SEARCH:
        print(i)