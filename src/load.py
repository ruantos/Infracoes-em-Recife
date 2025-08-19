from supabase import create_client

class Manager:
    def __init__(self, project_url: str, api_key: str ) -> None:

        self.url = project_url
        self.key = api_key
        self.client = None


    def connect(self) -> None:
        try:
            self.client = create_client(self.url, self.key)
        except Exception as e:
            print(f'Error caught while connecting to {self.url}: {e}')


    def insert(self,  records: list) -> None:
        try:
            self.client.insert_many(records)

        except Exception as e:
            print(f"Erro durante inserção: {e}")


    def close(self) -> None:
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()
