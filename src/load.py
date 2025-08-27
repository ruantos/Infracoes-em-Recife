from supabase import create_client
from extract import get_ids
import logging

logger = logging.getLogger(__name__)

class Loader:
    def __init__(self, project_url: str, api_key: str ) -> None:

        self.url = project_url
        self.key = api_key
        self.supabase = None


    def connect(self) -> bool:
        try:
            self.supabase = create_client(self.url, self.key)
            logger.info('Client created successfully!')
            return True
        except Exception as e:
            logger.error(f'Error caught while connecting to {self.url}: {e}')
            return False


    def fetch_ids(self) -> list:
        try:
            response = (
                self.supabase.table('collections_id')
                .select('year, identifier')
                .eq('fetched', 'false')
                .execute()
            )
            return response.data
        except Exception as e:
            logger.error(f'Error caught fetching IDs: {e}')
            return []


    def update_status(self, identifier: str) -> None:
        try:
            (self.supabase
             .table('collections_id')
             .update( {'fetched': True} )
             .eq('identifier', identifier)
             .execute()
             )
        except Exception as e:
            logger.error(f'Error caught updating status: {e}')


    def insert(self,  records: list) -> None:
        try:
            if records:
                self.supabase.table('infracoes').insert(records).execute()
                logger.info(f'{len(records)} records inserted :)')

            else:
                logger.warning('Empty records')
        except Exception as e:
            logger.error(f"Error during insertion: {e}")


    def id_exist(self, year: str) -> bool:
        match = 0
        try:
            match = (self.supabase
             .table('collections_id')
             .select('year', count='exact')
             .eq('year', year)
             .execute()
             ).count
        except Exception as e:
            logger.error(f'Error during id_exist verification: {e}')
        return match > 0


    def insert_id(self, dataset: dict['str', 'str']):
        (self.supabase
         .table('collections_id')
         .insert(
            {
                'year': dataset['year'],
                'identifier': dataset['id'],
                'fetched': False,
            })
         .execute())


    def update_collection(self):
        collection = get_ids()
        for dataset in collection:
            if self.id_exist(dataset['year']):
                continue
            else:
                self.insert_id(dataset)