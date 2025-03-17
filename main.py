import pandas as pd
import math


def create_dataframe(path):

    filetype = path.split('.')[-1]

    if filetype == "xlsx":
        return pd.read_excel(path)

    if filetype == "csv":
        return pd.read_csv(path)


def check_param(param):
    if type(param) == float:
        return math.isnan(param)

    if type(param) == str:
        return param is None or param == ""

def process_supplier(param, index):
    if check_param(param):
        return ""

    lookup_data = create_dataframe("data/MD in System/MD Supplier (Pemasok).csv")

    try:
        return lookup_data[lookup_data['Supplier Name'] == param]['ID'].values[0]
    except:
        error_logs.append(
            ["supplier name", param, index]
        )
        return ""


def process_terms_and_condition():
    return ""


def process_date(param, index):
    if check_param(param):
        return ""

    try:
        return param.strftime("%d/%m/%Y")
    except:
        error_logs.append(
            ["date", param, index]
        )
        return ""


def process_branch(param, index):
    if check_param(param):
        return ""

    lookup_data = create_dataframe("data/MD in System/MD Branch (Cabang).xlsx")

    try:
        return lookup_data[lookup_data['Name'] == param]['ID'].values[0]
    except:
        error_logs.append(
            ["branch", param, index]
        )
        return ""


def process_currency(param, index):
    if check_param(param):
        return ""

    lookup_data = create_dataframe("data/MD in System/MD Currency (Mata Uang).xlsx")

    try:
        return lookup_data[lookup_data['Name'] == param]['ID'].values[0]
    except:
        error_logs.append(
            ["currency", param, index]
        )
        return ""


def process_exchange_rates():
    return ""


def process_price_list():
    return ""


def process_terms(param, index):
    if check_param(param):
        return ""

    lookup_data = create_dataframe("data/MD in System/MD Payment Terms (Term).csv")

    try:
        return lookup_data[lookup_data['Template Name'] == param]['ID'].values[0]
    except:
        error_logs.append(
            ["terms", param, index]
        )
        return ""

def process_template():
    return ""


def process_item_code(param, index):
    if check_param(param):
        return ""

    lookup_data = create_dataframe("data/MD in System/MD Items (Kode & Nama Item).csv")

    try:
        return lookup_data[lookup_data['Item Code'] == param]['ID'].values[0]
    except:
        error_logs.append(
            ["item code", param, index]
        )
        return ""


def process_item_name(param, index):
    if check_param(param):
        return ""

    lookup_data = create_dataframe("data/MD in System/MD Items (Kode & Nama Item).csv")

    try:
        return lookup_data[lookup_data['Item Code'] == param]['Item Name'].values[0]
    except:
        error_logs.append(
            ["item name", param, index]
        )
        return ""


def process_quantity(param, index):
    if check_param(param):
        return ""

    return param


def process_rate(param, index):
    if check_param(param):
        return ""

    return param


def process_required_by():
    return ""


def process_uom(param, index):
    if check_param(param):
        return ""

    return param


def process_target_warehouse(param, index):
    if check_param(param):
        return ""

    lookup_data = create_dataframe("data/MD in System/MD Warehouse (Gudang).csv")

    try:
        return lookup_data.loc[lookup_data['Warehouse Name'] == param]["ID"].values[0]
    except:
        error_logs.append(
            ["warehouse", param, index]
        )
        return ""


def process_account_head():
    return ""


def process_add_or_deduct():
    return ""


def process_consider_tax():
    return ""


def process_description():
    return ""


def process_type():
    return ""


def process_is_tax_included():
    return ""


def process_tax_rate():
    return ""


if __name__ == '__main__':
    dirty_data = create_dataframe("data/RINCIAN PESANAN PEMBELIAN JANUARI 2025.xlsx")

    error_logs = [
        [
            "columns",
            "data",
            "index"
        ]
    ]

    print(dirty_data.head())

    # first cleanup starting getting data from 3rd row

    proc_1_data = dirty_data[3:]

    print(proc_1_data.head())

    # getting columns

    columns = proc_1_data.iloc[0].values

    print(columns)

    # assign columns and dropping the first data

    proc_2_data = proc_1_data[1:]

    proc_2_data.columns = columns

    proc_2_data.reset_index(drop=True, inplace=True)

    print(proc_2_data.head())

    clean_data = proc_2_data.copy()

    # do preprocess pembelian data

    output = [
        ["Terms and Conditions",
        "Supplier",
        "Date",
        "Branch",
        "Currency",
        "Exchange Rate",
        "Price List",
        "Payment Terms",
        "Template",
        "Item Code (Items)",
        "Item Name (Items)",
        "Quantity (Items)",
        "Rate (Company Currency) (Items)",
        "Required By (Items)",
        "UOM (Items)",
        "Target Warehouse (Items)",
        "Account Head (Purchase Taxes and Charges)",
        "Add or Deduct (Purchase Taxes and Charges)",
        "Consider Tax or Charge for (Purchase Taxes and Charges)",
        "Description (Purchase Taxes and Charges)",
        "Type (Purchase Taxes and Charges)",
        "Is this Tax included in Basic Rate? (Purchase Taxes and Charges)",
        "Tax Rate (Purchase Taxes and Charges"]
    ]

    for index, data in clean_data.iterrows():
        print(data)

        processed = [
            process_terms_and_condition(),
            process_supplier(data["Pemasok"], index),
            process_date(data["Tanggal"], index),
            process_branch(data["Cabang"], index),
            process_currency(data["Mata Uang"], index),
            process_exchange_rates(), 
            process_price_list(), 
            process_terms(data["Term"], index),
            process_template(),
            process_item_code(data["Kode #"], index),
            process_item_name(data["Kode #"], index),
            process_quantity(data["Kuantitas"], index),
            process_rate(data["@Harga"], index),
            process_required_by(),
            process_uom(data["Satuan"], index),
            process_target_warehouse(data["Nama Gudang"], index),
            process_account_head(),
            process_add_or_deduct(),
            process_consider_tax(),
            process_description(),
            process_type(),
            process_is_tax_included(),
            process_tax_rate(),
        ]

        print(processed)

        output.append(processed)

    output_df = pd.DataFrame(output[1:], columns=[output[0]])
    output_df.to_csv("output/output.csv")

    error_df = pd.DataFrame(error_logs[1:], columns=[error_logs[0]])
    error_df.to_csv("output/error_log.csv")
