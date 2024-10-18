from airflow.decorators import dag, task
from datetime import datetime, timedelta
import mysql.connector
import psycopg2
from airflow.models import Connection
from airflow.utils.db import provide_session

# Função que recupera os detalhes da conexão via Airflow
@provide_session
def get_connection(conn_id, session=None):
    connection = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    return connection

# Definindo a DAG usando o decorator @dag
@dag(
    dag_id='Ingestao_Mysql_Postgres_nova',
    default_args={
        'owner': 'matheus',
        'start_date': datetime(2024, 10, 18),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='*/2 * * * *',  # Executa diariamente
    catchup=False,
)
def mysql_to_postgres_dag_taskflow():

    # Task que realiza o SELECT no MySQL
    @task
    def extract_data_from_mysql():
        conn_id = 'mysql_default'
        conn = get_connection(conn_id)

        if conn is None:
            raise ValueError(f"Conexão {conn_id} não encontrada no Airflow")

        connection = mysql.connector.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            database=conn.schema
        )

        cursor = connection.cursor(dictionary=True)
        cursor.execute("""SELECT ROW_NUMBER() OVER (ORDER BY data_mes) + 0 AS id,
            COUNT(provisionDate) AS qtd_ativos,
            DATE(provisionDate) AS data_mes,
            JSON_UNQUOTE(JSON_EXTRACT(
                JSON_EXTRACT(bssdomain, '$.components[1]'),
                '$.key'
            )) AS mvno
        FROM `VIEW_SUBSCRIPTION`
        WHERE status NOT IN (0, 1, 2, 3, 19)
        GROUP BY DATE(provisionDate), mvno""")
        data = cursor.fetchall()

        cursor.close()
        connection.close()

        return data

    # Task que realiza o UPSERT no PostgreSQL
    @task
    def upsert_data_to_postgres(data):
        conn_id = 'postgres_default'
        conn = get_connection(conn_id)

        if conn is None:
            raise ValueError(f"Conexão {conn_id} não encontrada no Airflow")

        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )

        cursor = connection.cursor()

        for row in data:
            # Exemplo de UPSERT usando ON CONFLICT
            upsert_query = """
                INSERT INTO qtd_ativos_mvnos (id, qtd_ativos, data_mes, mvno)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                qtd_ativos = EXCLUDED.qtd_ativos,
                data_mes = EXCLUDED.data_mes,
                mvno = EXCLUDED.mvno;
            """
            cursor.execute(upsert_query, (row['id'], row['qtd_ativos'], row['data_mes'], row['mvno']))

        connection.commit()
        cursor.close()
        connection.close()

    # Definindo a ordem das tarefas
    data = extract_data_from_mysql()
    upsert_data_to_postgres(data)

# Instanciando a DAG
mysql_to_postgres_dag_taskflow()