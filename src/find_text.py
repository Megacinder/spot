import os
import re
from pathlib import Path


PATH_OF_PARSED_SQL = "sql_from_parsed_dtsx"
PATH_FOR_SEARCH = r"C:\Users\guppi\PycharmProjects\greenfield"
EXTENSION_TO_BE_EXCLUDED_FROM_SEARCH: list = ['.dtsx']
TEXT_TO_SEARCH = [
    'AssetFixedAssetV2Staging',
    'BusinessDocumentSalesInvoiceLineItemStaging',
    'CustCustomerDetailV2Staging',
    'CustCustomerGroupEntityStaging',
    'CustInvoiceJournalHeaderStaging',
    'CustInvoiceJournalLineStaging',
    'DimensionParametersStaging',
    'EcoResProductCategoryHierarchyRoleStaging',
    'EcoResProductCategoryHierarchyStaging',
    'EcoResProductCategoryStaging',
    'EcoResReleasedProductVariantV2Staging',
    'EcoResStorageDimensionGroupStaging',
    'ExchangeRateCurrencyPairStaging',
    'ExchangeRateEntityStaging',
    'ExchangeRateTypeStaging',
    'FinancialDimensionSetStaging',
    'InventInventoryDimensionsParametersStaging',
    'InventOperationalSiteStaging',
    'InventProductGroupStaging',
    'InventWarehouseStaging',
    'LedgerChartOfAccountsStaging',
    'LedgerFiscalYearStaging',
    'LineOfBusinessStaging',
    'MainAccountCategoryStaging',
    'OMBusinessUnitStaging',
    'OMLegalStaging',
    'ReturnOrderLineStaging',
    'SalesInvoiceJournalHeaderStaging',
    'UnitOfMeasureConversionStaging',
    'UnitOfMeasureStaging',
    'WHSItemPhysicalDimensionDetailStaging',
    'WHSPhysicalDimensionGroupDetailStaging',
    'WHSPhysicalDimensionGroupStaging',
]


def search_text_in_directory(directory: str, text: str, exclude_ext: list) -> list:
    matches = []
    text_lower = text.lower()
    pattern = rf'{re.escape(text_lower)}(?=\s|\n|\Z|\')'

    for root, dirs, files in os.walk(directory):
        for file in files:
            if Path(file).suffix not in exclude_ext:
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        contents = f.read().lower()
                        if re.search(pattern, contents):
                            matches.append(file_path)
                except:
                    continue
    return matches


def find_all_text(directory: str, text_to_search: list, exclude_ext: list, parsed_sql_path: str):
    for text in sorted(text_to_search):
        result = search_text_in_directory(directory, text, exclude_ext)
        if result:
            for path in result:
                path = path.replace(f"{directory}\\{parsed_sql_path}\\", "")
                path = Path(path)
                print(text, ':', ":".join(path.parts))
    # return


if __name__ == '__main__':
    find_all_text(PATH_FOR_SEARCH, TEXT_TO_SEARCH, EXTENSION_TO_BE_EXCLUDED_FROM_SEARCH, PATH_OF_PARSED_SQL)
