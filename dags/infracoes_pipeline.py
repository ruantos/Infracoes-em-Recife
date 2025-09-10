from pandas import DataFrame
from dotenv import load_dotenv
from src.extract import fetch_dataframe
from airflow.decorators import dag, task
from pathlib import Path

import pendulum
import pandas as pd
import logging
import sys
import os


DAG_FOLDER = Path(__file__).parent
PROJECT_ROOT = DAG_FOLDER.parent
sys.path.append(str(PROJECT_ROOT))

from src.load import Loader
from src.transform import transform

logger = logging.Logger(__name__)
logging.basicConfig(level=logging.INFO,
                    filename='info.log', filemode='a',
                    format='%(asctime)s - [%(name)s]- [%(levelname)s]: %(message)s')

load_dotenv()
supa_url = os.environ.get('SUPA_URL')
supa_key = os.environ.get('API_KEY')

default_args = {
	'owner': 'ruantos',
	'retries': 2,
	'retry_delay': pendulum.duration(minutes=5)
}

@dag(
	dag_id = 'infracoes_pipeline',
	description = 'dag para gerenciar pipeline do projeto Infrações de Trânsito Recife',
	default_args = default_args,
	start_date = pendulum.datetime(2025, 9, 1, tz='America/Recife'),
	schedule = '0 2 1 * *',
	catchup = False)
def workflow():

	@task()
	def call_update():
		supabase = Loader(supa_url, supa_key)
		supabase.connect()
		try:
			supabase.update_collection()
			logger.info('ID collection updated!')
		except Exception as e:
			logger.error(f'Error updating collection: {e}')


	@task()
	def get_records():
		supabase = Loader(supa_url, supa_key)
		if supabase.connect():
			return supabase.fetch_ids()
		else:
			logger.error('Connection failed in get_records')
			return []

	@task()
	def extract(record: dict) -> DataFrame | None:
		year = record['year']
		identifier = record['identifier']
		logger.info(f"Fetching {year} file...")
		try:
			df = fetch_dataframe(identifier)
			if df is not None:
				df['identifier'] = identifier
			return df
		except Exception as e:
			logger.warning(f"Error caught while fetching {year} file: {e}")
			return None


	@task()
	def transform(df: pd.DataFrame) -> list[dict] | None:
		if df is not None and not df.empty:

			identifier = df['identifier'].iloc[0]

			df = transform(df)
			records = df.to_dict('records')
			for record in records:
				record['identifier'] = identifier
			return records
		else:
			logger.warning("DataFrame is empty or None.")
			return None


	@task()
	def load(records: list):
		if records:
			supabase = Loader(supa_url, supa_key)
			if supabase.connect():

				identifier = records[0]['identifier']
				supabase.insert(records)
				supabase.update_status(identifier)
				logger.info(f"Records with identifier {identifier} loaded successfully!")
			else:
				logger.error('Supabase connection failed in load.')
		else:
			logger.warning("No records to load.")


	update_task = call_update()
	records_to_process = get_records()

	update_task >> records_to_process

	extracted_dfs = extract.expand(record=records_to_process)
	transformed_records = transform.expand(df=extracted_dfs)
	load.expand(records=transformed_records)



my_workflow = workflow()
