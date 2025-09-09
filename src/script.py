from load import Loader
from transform import transform
from extract import fetch_dataframe
from dotenv import load_dotenv
import duckdb
import logging
import os

logging.getLogger('httpx').setLevel(logging.WARNING)

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO,
                        filename='info.log', filemode='a',
                        format='%(asctime)s - [%(name)s]- [%(levelname)s]: %(message)s')

    load_dotenv()
    supa_url = os.environ.get('SUPA_URL')
    supa_key = os.environ.get('API_KEY')

    if not supa_url or not supa_key:
        print('Supabase URL and API key are not set')

    supabase = Loader(supa_url, supa_key)



    supabase.connect()


    for record in supabase.fetch_ids():
        year = record['year']
        identifier = record['identifier']

        try:
            logger.info(f"Fetching {year} file...")
            df = fetch_dataframe(identifier)

            if not df.empty:
                df = transform(df)
                records = df.to_df().to_dict('records')
                supabase.insert(records)
                supabase.update_status(identifier)
                logger.info(f"{year} records loaded successfully!\n")

            else:
                logger.warning(f"{year} file is empty or couldn't be found")

        except Exception as e:
            logger.warning(f"Error caught while fetching {year} file: {e}")