import requests
import pandas as pd


def fetch_dataframe(id: str) -> pd.DataFrame | None:
	return None

def fetch_dataframe_api(id: str) -> pd.DataFrame | None:

	url = 'http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?'
	query = 'sql=SELECT * FROM '
	full_url = f'{url}{query}"{id}"'

	try:
		response = requests.get(full_url, timeout=60)
		return pd.DataFrame(response.json()["result"]["records"])

	except requests.exceptions.RequestException as error:
		print(f"An error occurred while trying to fetch dataframe: {error}")
		return None
