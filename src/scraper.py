import requests
import pandas as pd
import os

links= {
    "2023": "http://dados.recife.pe.gov.br/dataset/6399f689-f1a7-453b-b839-413bd665c355/resource/c269789d-da47-4dde-8ce7-42fba10fe8e2/download/infracoes-01a12-2023.csv",
    "2024": "http://dados.recife.pe.gov.br/dataset/6399f689-f1a7-453b-b839-413bd665c355/resource/4adf9430-35a5-4e88-8ecf-b45748b81c7d/download/infracoestransparencia-janeiro-a-dezembro-2024.csv",
    "2025": "http://dados.recife.pe.gov.br/dataset/6399f689-f1a7-453b-b839-413bd665c355/resource/48bd8822-df18-48d0-bbc1-2de87ca0d70b/download/infracoestransparencia-janeiro-a-maio-2025.csv",
    }

path_raw = "../data/raw"
path_validated = "../data/validated"



try:
    os.makedirs("../data", exist_ok=True)
    os.makedirs(path_raw, exist_ok=True)
    os.makedirs(path_validated, exist_ok=True)
    pass
except:
    pass
     

for year, url in links.items():

    try:
        response = requests.get(url)
        response.raise_for_status()

        with open(f"{path_raw}/{year}.csv", "wb") as file:
            file.write(response.content)

        print("CSV baixado com sucesso!")

    except requests.exceptions.RequestException as error:
        print(f"Houve um erro: {error}")


for year in links.keys():
    data_csv = pd.read_csv(f"{path_raw}/{year}.csv", sep=';')
    data_csv.to_parquet(f"{path_validated}/{year}.parquet", index=False)
