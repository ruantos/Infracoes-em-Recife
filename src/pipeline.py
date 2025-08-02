import requests
import pandas as pd
import os


class Pipeline:

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

    def drop_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_to_drop = [
            'dataimplantacao', 'descricaoinfracao',
            'amparolegal', '_full_text',
            'horainfracao', 'datainfracao'
            ]
        return df.drop(columns=cols_to_drop)

    def extract_date(self, df: pd.DataFrame) -> pd.DataFrame:
        df['horainfracao'] = pd.to_datetime(df['horainfracao'],
                                            format="%H:%M:%S")
        df['datainfracao'] = pd.to_datetime(df['datainfracao'], yearfirst=True)

        df["ano"] = df["datainfracao"].dt.year
        df["mes"] = df["datainfracao"].dt.month
        df["hora"] = df["horainfracao"].dt.hour

        df["is_feriado"] = df["datainfracao"].dt.weekday > 4

        return df

    def fix_columns(self, df: pd.Dataframe) -> pd.DataFrame:
        df.set_index("_id", inplace=True)

        df = self.extract_date(df)
        df = self.drop_columns(df)

        return df

    def remove_garbage(df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates(
            subset=["infracao", "horainfracao", "datainfracao"])

        df = df.dropna(axis=0, subset=["infracao"])
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.fix_columns(df)
        df = self.remove_garbage(df)
        return df

    def save_dataframe(self, df: pd.DataFrame,
                       year: str, path: str,) -> None:
        if df is None:
            print(f"The {year} dataframe wasnt saved due to an error")
            return

        df.to_csv(path, index=False)
        print(f"CSV de {year} baixado com sucesso!")

    def pipeline_extraction(self) -> None:
        self.create_directories()
