import requests
import pandas as pd


URL = 'http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?'


def fetch_dataframe(id: str) -> pd.DataFrame | None:

	query = 'sql=SELECT * FROM '
	url_params = f'{URL}{query}"{id}"'

	try:
		response = requests.get(url_params,
		                        timeout=60)
		records = response.json()["result"]["records"]

		if not records:
			print(f'No records found for dataset: {id}')
			return pd.DataFrame(records)
		return pd.DataFrame(records)

	except requests.exceptions.RequestException as error:
		print(f"An error occurred while trying to fetch dataframe: {error}")
		return None
