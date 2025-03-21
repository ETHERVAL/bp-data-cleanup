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
    path = "data/RINCIAN PESANAN PEMBELIAN JANUARI 2025.xlsx"
    file_type = "pesanan-pembelian"

    # paths = [
    #     "data/RINCIAN PENERIMAAN PESANAN JANUARI 2025.xlsx",
    #     "data/RINCIAN FAKTUR PEMBELIAN JANUARI 2025.xlsx",
    #     "data/RINCIAN PESANAN PEMBELIAN JANUARI 2025.xlsx"
    # ]
    #
    # file_types = [
    #     "penerimaan-pesanan",
    #     "faktur-pembelian",
    #     "pesanan-pembelian"
    # ]

    clean_data = clean_data(path)

    # do preprocess data
    output, error_logs = process_by_file_type(clean_data, file_type)

    output_df = pd.DataFrame(output[1:], columns=[output[0]])
    output_df.to_csv(f"output/output-{file_type}.csv")

    error_df = pd.DataFrame(error_logs[1:], columns=[error_logs[0]])
    error_df.to_csv(f"output/error-log-{file_type}.csv")

    distinct_df = process_distinct_data(error_df)
    distinct_df.to_csv(f"output/distinct_{file_type}.csv")
