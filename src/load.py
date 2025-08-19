from supabase import create_client

class Loader:
    def __init__(self, project_url: str, api_key: str ) -> None:

        self.url = project_url
        self.key = api_key
        self.client = None


    def connect(self) -> None:
        try:
            self.client = create_client(self.url, self.key)
            print('Client created successfully')
        except Exception as e:
            print(f'Error caught while connecting to {self.url}: {e}')


    def insert(self,  records: list) -> None:
        try:
            self.client.insert_many(records)

        except Exception as e:
            print(f"Error during insertion: {e}")
