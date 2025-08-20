from supabase import create_client
from dotenv import load_dotenv
import os

class Loader:
    def __init__(self, project_url: str, api_key: str ) -> None:

        self.url = project_url
        self.key = api_key
        self.supabase = None


    def connect(self) -> None:
        try:
            self.supabase = create_client(self.url, self.key)
            print('Client created successfully!')
        except Exception as e:
            print(f'Error caught while connecting to {self.url}: {e}')


    def insert(self,  records: list) -> None:
        try:
            if records:
                self.supabase.table('infracoes').insert(records).execute()
                print(f'{len(records)} records inserted :)')

            else:
                print('Empty records')
        except Exception as e:
            print(f"Error during insertion: {e}")