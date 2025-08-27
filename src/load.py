from supabase import create_client
from extract import get_ids

class Loader:
    def __init__(self, project_url: str, api_key: str ) -> None:

        self.url = project_url
        self.key = api_key
        self.supabase = None


    def connect(self) -> bool:
        try:
            self.supabase = create_client(self.url, self.key)
            print('Client created successfully!')
            return True
        except Exception as e:
            print(f'Error caught while connecting to {self.url}: {e}')
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
            print(f'Error caught fetching IDs: {e}')
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
            print(f'Error caught updating status: {e}')


    def insert(self,  records: list) -> None:
        try:
            if records:
                self.supabase.table('infracoes').insert(records).execute()
                print(f'{len(records)} records inserted :)')

            else:
                print('Empty records')
        except Exception as e:
            print(f"Error during insertion: {e}")


    def id_exist(self, year: str) -> bool:
        match = 0
        try:
            match = (self.supabase
             .table('infracoes')
             .select('year', count='exact')
             .eq('year', year)
             .execute()
             )
        except Exception as e:
            print(f'Error during id_exist verification: {e}')

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
            if( self.id_exist(dataset['year']) ):
                continue
            else:
                self.insert_id(dataset)



if __name__ == '__main__':
    SUPA_URL = 'https: // wmtzrtxnwjioekyzjctz.supabase.co'
    API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndtdHpydHhud2ppb2VreXpqY3R6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTU1NzA4NDAsImV4cCI6MjA3MTE0Njg0MH0.29nn7MRaBcqAWSB-ApRivsJdnn0l3ykRD3ZgK43oWUg'
    loader = Loader(SUPA_URL, API_KEY)

    loader.connect()
    loader.fetch_ids()