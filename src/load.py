from supabase import create_client


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