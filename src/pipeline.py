import requests
import pandas as pd


class Pipeline:

    def __init__(self,
                 url: str,
                 query: str,
                 ):

        self.url = url
        self.query = query

    def fetch_dataframe(self, id: str) -> pd.DataFrame:
        url = f'{self.url}{self.query}"{id}"'

        try:
            response = requests.get(url, timeout=30)
            return pd.DataFrame(response.json()["result"]["records"])

        except requests.exceptions.RequestException as error:
            print(f"Houve um erro: {error}")
            return None

    def drop_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_to_drop = [
            'dataimplantacao', 'descricaoinfracao', '_id',
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

    def fix_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.extract_date(df)
        df = self.drop_columns(df)

        return df

    def remove_garbage(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates(
            subset=["infracao", "hora", "ano", "mes"])

        df = df.dropna(axis=0, subset=["infracao"])
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.fix_columns(df)
        df = self.remove_garbage(df)
        return df
