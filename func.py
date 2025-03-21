import pandas as pd
import math

error_logs = [
        [
            "columns",
            "data",
            "index"
        ]
    ]

def create_dataframe(path):

    filetype = path.split('.')[-1]

    if filetype == "xlsx":
        return pd.read_excel(path, index_col=False)

    if filetype == "csv":
        return pd.read_csv(path, index_col=False)


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


def process_currency(param = None, index = None):
    if param is None and index is None:
        return ""

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


def process_terms(param = None, index = None):
    if param is None and index is None:
        return ""

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


def process_rate(param = None, index = None):
    if param is None and index is None:
        return ""

    if check_param(param):
        return ""

    return param


def process_required_by():
    return ""


def process_uom(param = None, index = None):
    if param is None and index is None:
        return ""

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


def clean_data(path):
    dirty_data = create_dataframe(path)
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

    return proc_2_data

def process_terms_and_condition_faktur_pembelian(param, index):
    return param

def process_supplier_invoice_no(param, index):
    return param

def process_faktur_pajak_no(param, index):
    return param

def process_faktur_pembelian(clean_df):
    output = [
        [
            'Terms and Conditions',
            'Supplier',
            'Date',
            'Branch',
            'Supplier Invoice No',
            'Supplier Invoice Date',
            'Faktur Pajak No',
            'Payment Terms Template',
            'Item (Items)',
            'Item Name (Items)',
            'Accepted Qty (Items)',
            'Rate (Items)',
            'UOM (Items)',
            'Purchase Order (Items)',
            'Purchase Order Item (Items)',
            'Purchase Receipt (Items)',
            'Purchase Receipt Detail (Items)',
            'Account Head (Purchase Taxes and Charges)',
            'Add or Deduct (Purchase Taxes and Charges)',
            'Consider Tax or Charge for (Purchase Taxes and Charges)',
            'Description (Purchase Taxes and Charges)',
            'Type (Purchase Taxes and Charges)',
            'Is this Tax included in Basic Rate? (Purchase Taxes and Charges)',
            'Is Tax Withholding Account (Purchase Taxes and Charges)',
            'Reference Type (Advances)',
            'Reference Name (Advances)',
            'Allocated Amount (Advances)',
            'Advance amount (Advances)',
        ]
    ]

    for index, data in clean_df.iterrows():
        print(data)

        processed = [
            process_terms_and_condition_faktur_pembelian(data["Nomor #"], index), # 'Terms and Conditions',
            process_supplier(data["Pemasok"], index),# 'Supplier',
            process_date(data["Tanggal"], index),# 'Date',
            process_branch(data["Nama Cabang Faktur Pembelian"], index), # 'Branch',
            process_supplier_invoice_no(data["No Faktur # Faktur Pembelian"], index), # 'Supplier Invoice No',
            "", # 'Supplier Invoice Date',
            process_faktur_pajak_no(data["No. Faktur Pajak Faktur Pembelian"], index),# 'Faktur Pajak No',
            "", # 'Payment Terms Template',
            process_item_code(data["Kode #"], index), # 'Item (Items)',
            process_item_name(data["Kode #"], index), # 'Item Name (Items)',
            process_quantity(data["Kuantitas"], index), # 'Accepted Qty (Items)',
            "", # 'Rate (Items)',
            "",# 'UOM (Items)',
            process_purchase_order(data["Nomor # Pesanan Pembelian"], index),# 'Purchase Order (Items)',
            "", # 'Purchase Order Item (Items)',
            "", # 'Purchase Receipt (Items)',
            "", # 'Purchase Receipt Detail (Items)',
            "",# 'Account Head (Purchase Taxes and Charges)',
            "",# 'Add or Deduct (Purchase Taxes and Charges)',
            "",# 'Consider Tax or Charge for (Purchase Taxes and Charges)',
            "",# 'Description (Purchase Taxes and Charges)',
            "",# 'Type (Purchase Taxes and Charges)',
            "",# 'Is this Tax included in Basic Rate? (Purchase Taxes and Charges)',
            "",# 'Is Tax Withholding Account (Purchase Taxes and Charges)',
            "",# 'Reference Type (Advances)',
            "",# 'Reference Name (Advances)',
            "",# 'Allocated Amount (Advances)',
            "",# 'Advance amount (Advances)',

        ]

        print(processed)

        output.append(processed)

    return output, error_logs

def process_instruction():
    return ""

def process_supplier_delivery_note(param, index):
    return param

def process_posting_time():
    return ""

def process_received_quantity(param, index):
    return param

def process_accepted_quantity(param, index):
    return param

def process_purchase_order(param, index):
    return param

def process_purchase_order_item():
    return ""

def process_purchase_invoice():
    return ""

def process_purchase_invoice_item():
    return ""

def process_penerimaan_pesanan(clean_df):
    output = [
        [
            "Instructions",
            "Supplier",
            "Supplier Delivery Note",
            "Date",
            "Posting Time",
            "Branch",
            "Currency",
            "Exchange Rate",
            "Item Code (Items)",
            "Item Name (Items)",
            "Rate (Company Currency) (Items)",
            "Received Quantity (Items)",
            "Accepted Quantity (Items)",
            "UOM (Items)",
            "Accepted Warehouse (Items)",
            "Purchase Order (Items)",
            "Purchase Order Item (Items)",
            "Purchase Invoice (Items)",
            "Purchase Invoice Item (Items",
        ]
    ]

    for index, data in clean_df.iterrows():
        print(data)

        processed = [
            process_instruction(),                                                              # "Instructions",
            process_supplier(data['Pemasok'], index),                                           # "Supplier",
            process_supplier_delivery_note(data['Nomor #'], index),                             # "Supplier Delivery Note",
            process_date(data['Tanggal'], index),                                               # "Date",
            process_posting_time(),                                                             # "Posting Time",
            process_branch(data['Nama Cabang Pesanan Pembelian Detail Pesanan Pemb'], index),   # "Branch",
            process_currency(),                                                                 # "Currency",
            process_exchange_rates(),                                                           # "Exchange Rate",
            process_item_code(data['Kode #'], index),                                           # "Item Code (Items)",
            process_item_name(data['Kode #'], index),                                           # "Item Name (Items)",
            process_rate(),                                                                     # "Rate (Company Currency) (Items)",
            process_received_quantity(data['Kuantitas'], index),                                # "Received Quantity (Items)",
            process_accepted_quantity(data['Kuantitas'], index),                                # "Accepted Quantity (Items)",
            process_uom(data['Satuan'], index),                                                 # "UOM (Items)",
            process_target_warehouse(data['Nama Gudang'], index),                               # "Accepted Warehouse (Items)",
            process_purchase_order(data['Nomor # Pesanan Pembelian Detail Pesanan Pembelia'], index),                   # "Purchase Order (Items)",
            process_purchase_order_item(),                                                      # "Purchase Order Item (Items)",
            process_purchase_invoice(),                                                         # "Purchase Invoice (Items)",
            process_purchase_invoice_item(),                                                    # "Purchase Invoice Item (Items)",
        ]

        print(processed)

        output.append(processed)

    return output, error_logs

def process_penerimaan_pembelian(clean_df):
    output = [
        [
         "Terms and Conditions",
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
         "Tax Rate (Purchase Taxes and Charges"
        ]
    ]

    for index, data in clean_df.iterrows():
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

    return output, error_logs

def process_terms_and_condition_pesanan_pembelian(param, index):
    return param

def process_pesanan_pembelian(clean_df):
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

    for index, data in clean_df.iterrows():
        print(data)

        processed = [
            process_terms_and_condition_pesanan_pembelian(data["Nomor #"], index),
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

    return output, error_logs


def process_by_file_type(clean_df, file_type):
    if file_type == "faktur-pembelian":
        output, error_logs = process_faktur_pembelian(clean_df)

        return output, error_logs

    if file_type == "penerimaan-pesanan":
        output, error_logs = process_penerimaan_pesanan(clean_df)

        return output, error_logs

    if file_type == "pesanan-pembelian":
        output, error_logs = process_pesanan_pembelian(clean_df)

        return output, error_logs


def process_distinct_data(df):

    # Drop the index column
    df = df.iloc[:, :-1]

    df = df.iloc[:, 0:]

    # Drop duplicate rows to get distinct data
    df = df.drop_duplicates()

    return df