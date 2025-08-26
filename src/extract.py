import requests
import pandas as pd
from bs4 import BeautifulSoup

ENDPOINT = 'http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?'
URL = "http://dados.recife.pe.gov.br/dataset/registro-das-infracoes-de-transito"


def fetch_dataframe(identifier: str) -> pd.DataFrame | None:

	query = 'sql=SELECT * FROM '
	url_params = f'{ENDPOINT}{query}"{identifier}"'

	try:
		response = requests.get(url_params,
		                        timeout=60)
		records = response.json()["result"]["records"]

		if not records:
			print(f'No records found for dataset: {identifier}')
			return pd.DataFrame(records)
		return pd.DataFrame(records)

	except requests.exceptions.RequestException as error:
		print(f"An error occurred while trying to fetch dataframe: {error}")
		return None


def get_links() -> list[str]:
	links = []

	try:
		response = requests.get(URL)
		soup = BeautifulSoup(response.text, 'html.parser')

		tags = soup.find_all('a', class_='heading')[2:]
		for tag in tags:
			dataset = {
				'year': tag.get('title').split()[-1],
				'link': tag.get('href')
			}
			links.append(dataset)

		return links
	except requests.exceptions.RequestException as e:
		print(f"An error occurred while fetching links: {e}")
		return links