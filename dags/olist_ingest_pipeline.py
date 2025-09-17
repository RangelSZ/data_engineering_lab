import pendulum
import pandas as pd
from sqlalchemy import create_engine
from clickhouse_driver import Client
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# Tabelas a serem migradas do Postgres para o ClickHouse
TABLES_TO_MIGRATE = [
    'customers',
    'geolocation',
    'order_items',
    'order_payments',
    'order_reviews',
    'orders',
    'products',
    'sellers',
    'product_category_name_translation'
]

def get_postgres_engine():
    """Cria uma engine SQLAlchemy para o PostgreSQL usando a conexão do Airflow."""
    conn = BaseHook.get_connection('postgres_source')
    connection_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(connection_uri)

def get_clickhouse_client():
    """Cria um cliente para o ClickHouse, conectando-se diretamente ao DB 'raw_olist'."""
    conn = BaseHook.get_connection('clickhouse_target')
    return Client(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database='raw_olist'
    )

def migrate_table_to_clickhouse(table_name: str, **kwargs):
    """
    Função Python que extrai uma tabela inteira do PostgreSQL e a carrega
    em um schema 'raw_olist' no ClickHouse.
    """
    print(f"Iniciando a migração da tabela: {table_name}")
    pg_engine = get_postgres_engine()
    ch_client = get_clickhouse_client()

    ch_client.execute('CREATE DATABASE IF NOT EXISTS raw_olist')

    print(f"Extraindo dados de '{table_name}' do PostgreSQL...")
    df = pd.read_sql(f'SELECT * FROM "{table_name}"', pg_engine)

    # --- Início da Limpeza de Dados Nulos ---
    for column in df.select_dtypes(include=['object']).columns:
        df[column] = df[column].fillna('')
    for column in df.select_dtypes(include=['float64', 'int64']).columns:
        df[column] = df[column].fillna(0)
    # --- Fim da Limpeza de Dados Nulos ---

    if df.empty:
        print(f"Tabela '{table_name}' está vazia no PostgreSQL. Nenhuma ação necessária.")
        return

    print(f"Carregando {len(df)} linhas para a tabela 'raw_olist.{table_name}' no ClickHouse...")

    dtypes_map = {
        'int64': 'Int64',
        'float64': 'Float64',
        'object': 'String',
        'datetime64[ns]': 'DateTime'
    }

    create_table_query = f'CREATE TABLE IF NOT EXISTS raw_olist.{table_name} ('
    columns_defs = []
    for column, dtype in df.dtypes.items():
        if 'date' in column:
            ch_type = 'Date'
        else:
            ch_type = dtypes_map.get(str(dtype), 'String')

        columns_defs.append(f'`{column}` {ch_type}')
    create_table_query += ', '.join(columns_defs)
    create_table_query += ') ENGINE = MergeTree() ORDER BY tuple()'

    ch_client.execute(create_table_query)

    ch_client.execute(f'TRUNCATE TABLE IF EXISTS raw_olist.{table_name}')

    data_to_insert = df.to_dict('records')

    if data_to_insert:
        ch_client.execute(
            f'INSERT INTO raw_olist.{table_name} VALUES',
            data_to_insert
        )

    print(f"Tabela '{table_name}' migrada com sucesso!")


# Definição da DAG no Airflow 
with DAG(
    dag_id='olist_ingestion_postgres_to_clickhouse',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['olist', 'ingestion', 'ELT-ExtractLoad'],
) as dag:

    for table in TABLES_TO_MIGRATE:
        task = PythonOperator(
            task_id=f'ingest_{table}_to_clickhouse',
            python_callable=migrate_table_to_clickhouse,
            op_kwargs={'table_name': table},
        )