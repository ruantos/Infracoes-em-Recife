import os
from pipeline import Pipeline

URL = "http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?"
SQL_QUERY = "sql=SELECT * FROM "

DATA_DIR = "../data"
RAW_DIR = os.path.join(DATA_DIR, "raw")

id_list = {
    "2023": "c269789d-da47-4dde-8ce7-42fba10fe8e2",
    "2024": "4adf9430-35a5-4e88-8ecf-b45748b81c7d",
    "2025": "48bd8822-df18-48d0-bbc1-2de87ca0d70b",
    }


if __name__ == "__main__":
    ant = Pipeline(DATA_DIR, RAW_DIR, URL, SQL_QUERY, id_list)
    ant.pipeline_extraction()
