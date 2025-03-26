import pandas as pd
import math

from func import clean_data, process_by_file_type, process_distinct_data

if __name__ == '__main__':

    # path = "data/RINCIAN PENERIMAAN PESANAN JANUARI 2025.xlsx"
    # file_type = "penerimaan-pesanan"
    #
    # path = "data/RINCIAN FAKTUR PEMBELIAN JANUARI 2025.xlsx"
    # file_type = "faktur-pembelian"
    #
    # path = "data/RINCIAN PESANAN PEMBELIAN JANUARI 2025.xlsx"
    # file_type = "pesanan-pembelian"

    # path = "data/FAKTUR PENJUALAN 01-31 JANUARI 2025.xlsx"
    # file_type = "faktur-penjualan"
    # month = "januari"

    # path = "data/FAKTUR PENJUALAN 01-28 FEBRUARI 2025.xlsx"
    # file_type = "faktur-penjualan"
    # month = "februari"
    # #
    # path = "data/FAKTUR PENJUALAN 01-13 MARET 2025.xlsx"
    # file_type = "faktur-penjualan"
    # month = "maret"

    path = "data/UANG MUKA PEMBELIAN 01 JAN - 19 MAR '25 REVISI.xlsx"
    file_type = "uang-muka"
    month = "januari-maret"

    clean_data = clean_data(path, file_type)

    print(clean_data)

    # # do preprocess data
    output, error_logs = process_by_file_type(clean_data, file_type)

    print("Finished processing data")

    print("Beginning to output all the data")

    file_name = f'{file_type}-{month}'

    output_df = pd.DataFrame(output[1:], columns=[output[0]])
    output_df.to_csv(f"output/output-{file_name}.csv")

    error_df = pd.DataFrame(error_logs[1:], columns=[error_logs[0]])
    error_df.to_csv(f"output/error-log-{file_name}.csv")

    distinct_df = process_distinct_data(error_df)
    distinct_df.to_csv(f"output/distinct-{file_name}.csv")

    print("Finished outputting data")

