import psycopg2
import os
from dotenv import load_dotenv


class Manager:
    def __init__(self, host: str,
                 db_name: str, user: str,
                 password: str, port: str) -> None:

        self.host = host
        self.db_name = db_name
        self.user = user
        self.password = password
        self.port = port
        self.conn = self.cur = None

    def connect(self) -> None:
        try:
            self.conn = psycopg2.connect(
                dbname=database,
                user=user,
                password=password,
                host=host,
                port=port)
            self.cur = self.conn.cursor()
        except Exception as e:
            print(e)
            if self.cur is not None:
                self.cur.close()
            if self.conn is not None:
                self.conn.close()

    def create_table(self,) -> None:
        create_script = """
            CREATE TABLE IF NOT EXISTS infracoes(
                id INTEGER PRIMARY KEY,
                infracao INTEGER NOT NULL,
                equipamento CHAR(50),
                local_cometimento TEXT NOT NULL,
                ano CHAR(4),
                mes CHAR(2),
                hora CHAR(2),
                is_feriado BOOLEAN
                );"""

        self.cur.execute(create_script)
        self.conn.commit()

        def insert(self,  record: dict) -> None:
            try:
                insert_script = """
                    INSERT INTO infracoes(id ,infracao, equipamento, local_cometimento, ano, mes, hora, is_feriado)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
                """
                insert_values = (
                     1,
                     501,
                     'Sensor',
                     'Agamenon Magalhães',
                     '2025',
                     '02',
                     '9',
                     True)

                self.cur.execute(insert_script, insert_values)
                self.conn.commit()
                print("Dados inseridos com sucesso")

            except Exception as e:
                print(f"Inserção interrompida por erro: {e}")

    def close(self) -> None:
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()


if __name__ == "__main__":
    load_dotenv()

    host = os.environ["DB_HOST"]
    database = os.environ["DATABASE"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    port = os.environ["PORT"]

