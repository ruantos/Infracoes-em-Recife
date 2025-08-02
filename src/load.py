import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

host = os.environ["DB_HOST"]
database = os.environ["DATABASE"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]
port = os.environ["PORT"]

conn = cur = None

try:
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port)

    cur = conn.cursor()

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
            );
                """
    cur.execute(create_script)

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
         True
    )

    cur.execute(insert_script, insert_values)

    conn.commit()
    print("Sucesso")

except Exception as e:
    print(e)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

