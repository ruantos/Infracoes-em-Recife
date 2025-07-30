import requests
import pandas as pd
import os

ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?"
SQL_QUERY = "sql=SELECT * FROM "

DATA_DIR = "../data"
RAW_DIR = os.path.join(DATA_DIR, "raw")

id_list = {
    "2023": "c269789d-da47-4dde-8ce7-42fba10fe8e2",
    "2024": "4adf9430-35a5-4e88-8ecf-b45748b81c7d",
    "2025": "48bd8822-df18-48d0-bbc1-2de87ca0d70b",
    }


class Extractor:

    def __init__(self, data_dir: str,
                 raw_dir: str,
                 endpoint: str,
                 query: str,
                 set_ids: dict[str, str]):

        self.data_dir = data_dir
        self.raw_dir = raw_dir
        self.endpoint = endpoint
        self.query = query
        self.set_ids = set_ids

    def create_directories(self) -> None:
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            os.makedirs(self.raw_dir, exist_ok=True)
        except OSError as e:
            print(f"Error creating directory: {e}")

    def fetch_dataframe(self, id: str) -> pd.DataFrame:
        url = f'{self.endpoint}{self.query}"{id}"'

        try:
            response = requests.get(url, timeout=30)
            return pd.DataFrame(response.json()["result"]["records"])

        except requests.exceptions.RequestException as error:
            print(f"Houve um erro: {error}")
            return None

    def save_dataframe(self, dataframe: pd.DataFrame,
                       year: str, path: str,) -> None:

        dataframe.to_csv(path, index=False)
        print(f"CSV de {year} baixado com sucesso!")

    def pipeline_extraction(self) -> None:
        self.create_directories()

        for year, id in self.set_ids.items():
            df = self.fetch_dataframe(id)

            if df is None:
                print(f"The {year} dataframe wasnt saved due to an error")
            else:
                path = f"{self.raw_dir}/{year}.csv"
                self.save_dataframe(df, year, path)


if __name__ == "__main__":
    ant = Extractor(DATA_DIR, RAW_DIR, ENDPOINT, SQL_QUERY, id_list)
    ant.pipeline_extraction()
