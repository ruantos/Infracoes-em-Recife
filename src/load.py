import psycopg2


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
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port)
            self.cur = self.conn.cursor()
        except Exception as e:
            print(e)
            if self.cur is not None:
                self.cur.close()
            if self.conn is not None:
                self.conn.close()

    def create_table(self) -> None:
        create_script = """
            CREATE TABLE IF NOT EXISTS infracoes(
                id SERIAL PRIMARY KEY,
                equipamento CHAR(50),
                local_cometimento TEXT NOT NULL,
                infracao TEXT NOT NULL,
                ano CHAR(4),
                mes CHAR(2),
                hora CHAR(2),
                is_feriado BOOLEAN
                );"""

        self.cur.execute(create_script)
        self.conn.commit()

    def insert(self,  records: list) -> None:
        try:
            insert_script = """
                INSERT INTO infracoes(equipamento, local_cometimento, infracao, ano, mes, hora, is_feriado)
                VALUES(%s, %s, %s, %s, %s, %s, %s);
            """
            self.cur.executemany(insert_script, records)
            self.conn.commit()
            print(f"{self.cur.rowcount} dados inseridos com sucesso!")

        except Exception as e:
            print(f"Erro durante inserção: {e}")

    def close(self) -> None:
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()
