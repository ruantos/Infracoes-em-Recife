import requests
import pandas as pd

def fetch_dataframe(url: str, id: str, query: str) -> pd.DataFrame | None:
	url = f'{url}{query}"{id}"'

	try:
		response = requests.get(url, timeout=30)
		return pd.DataFrame(response.json()["result"]["records"])

	except requests.exceptions.RequestException as error:
		print(f"An error occurred while trying to fetch dataframe: {error}")
		return None
