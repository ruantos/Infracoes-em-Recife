import pandas as pd
import numpy as np
import os

data_path = "../data"
raw_path = f"{data_path}/raw"
parquet_path = f"{data_path}/validated/2023_2025_infrations.parquet"

raw_files = [f"{raw_path}/{file}" for file in os.listdir(raw_path)]

dtypes = {
    'datainfracao': str, 
    'horainfracao': str,
    'dataimplantacao': str,
    'agenteequipamento': str,
    'infracao': int,
    'descricaoinfracao': str,
    'amparolegal': str,
    'localcometimento': str,
}

dframes = []

for file in raw_files: 
    try:
        dframes.append( pd.read_csv(file, sep=";", dtype=dtypes))
        print("CSV lido com sucesso!")
    except Exception as error:
        print(error)


df_unified = pd.concat(dframes, ignore_index=True).drop_duplicates()

columns_to_drop = [
    'dataimplantacao', 'descricaoinfracao', 'amparolegal'
]

df_unified.drop(columns=columns_to_drop, inplace=True)


col_types = {
    'datainfracao': np.datetime64,
    'horainfracao': np.datetime64,
    'agenteequipamento': str,
    'infracao': int,
    'localcometimento': str
}

try:
    df_unified = df_unified.astype(col_types)
except Exception as error:
    print(error)


try:
    df_unified.to_parquet(parquet_path, index=False)
    print("Dataframe concatenado salvo com sucesso!")
except Exception as error:
    print(error)