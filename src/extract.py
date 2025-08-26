import requests
import pandas as pd
from bs4 import BeautifulSoup

ENDPOINT = 'http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?'
URL = "http://dados.recife.pe.gov.br/dataset/registro-das-infracoes-de-transito"


def fetch_dataframe(identifier: str) -> pd.DataFrame | None:

	query = 'sql=SELECT * FROM '
	url_params = f'{ENDPOINT}{query}"{identifier}"'

	try:
		response = requests.get(url_params, timeout=60)
		records = response.json()["result"]["records"]

		if not records:
			print(f'No records found for dataset: {identifier}')
			return pd.DataFrame(records)

		return pd.DataFrame(records)

	except requests.exceptions.RequestException as error:
		print(f"An error occurred while trying to fetch dataframe: {error}")
		return None


def get_links() -> list[dict[str, str]]:
	links = []

	try:
		response = requests.get(URL)
		soup = BeautifulSoup(response.text, 'html.parser')

		tags = soup.find_all('a', class_='heading')[2:]
		for tag in tags:
			dataset = {
				'year': tag.get('title').split()[-1],
				'link': tag.get('href'),
				'id': None
			}
			links.append(dataset)

		return links
	except requests.exceptions.RequestException as e:
		print(f"An error occurred while fetching links: {e}")
		return links


def fetch_id(dataset: dict[str, str]) -> str | None:
	url = f'http://dados.recife.pe.gov.br{dataset['link']}'
	try:
		response = requests.get(url, timeout=60)

		soup = BeautifulSoup(response.text, 'html.parser')
		element = soup.find('th', string='id')
		if element:
			return element.find_next_sibling('td').text

		print(f'ID wasnt found: {dataset["id"]}')
		return None

	except requests.exceptions.RequestException as e:
		print(f'An error occurred while trying to fetch {dataset['year']} ID: {e}')
		return None


def get_ids() -> list[dict[str, str]] | None:
	links = get_links()
	for i in range(len(links)):
		links[i]['id'] = fetch_id(links[i])

	return links


if __name__ == '__main__':
	df = get_ids()
	for dataset in df:
		print(dataset)
		print()