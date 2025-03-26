import os

import pandas as pd

from func import create_dataframe


def rename_files(file_name, month):
    return f'{file_name.split(".")[0]}-{month}.{file_name.split(".")[1]}'

def rename_all_files(path):
    files = os.listdir(path)

    for index, file in enumerate(files):
        os.rename(os.path.join(path, file), os.path.join(path, rename_files(file, 'januari')))

def group_by_month(path):
    files = os.listdir(file_path)
    groups = {}

    for file in files:
        last = file.split("-")[-1].split(".")[0]

        if groups.get(last) is None:
            groups[last] = []
            groups[last].append(file)
        else:
            groups[last].append(file)

    print(groups)
    for key in groups.keys():
        try:
            os.mkdir(os.path.join(file_path, key))
        except FileExistsError:
            print("Directory already exists")
            for file in groups[key]:
                os.rename(os.path.join(file_path, file), os.path.join(f'{file_path}{key}', file))

def split_data_per_month(path):
    df = create_dataframe(path)
    df.fillna("", inplace=True)

    print(df.columns)

    df.index = pd.to_datetime(df["Date"], format="%d/%m/%Y")

    grouped = df.groupby(by=[df.index.month, df.index.year])

    for name, group in grouped:
        df = pd.DataFrame.from_records(group.to_dict())

        df = df.reindex(columns=['Unnamed: 0', 'Nomor SO', 'Nomor Dokumen', 'Customer', 'Date',
       'Currency', 'Kurs', 'Received Amount', 'Cash/Bank', 'Catatan'])

        df.drop("Unnamed: 0", axis=1, inplace=True)

        df.to_csv(f"output/output-uang-muka-{name[0]}.csv")



if __name__ == '__main__':
    file_path = 'output/output-uang-muka-januari-maret.csv'

    split_data_per_month(file_path)


