import pandas as pd
from sqlalchemy import create_engine
import os
import time

DB_HOST = 'postgres_db'
DB_PORT = '5432'
DB_NAME = 'airflow_db'
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
TABLES_TO_LOAD = [
    'olist_customers_dataset',
    'olist_geolocation_dataset',
    'olist_order_items_dataset',
    'olist_order_payments_dataset',
    'olist_order_reviews_dataset',
    'olist_orders_dataset',
    'olist_products_dataset',
    'olist_sellers_dataset',
    'product_category_name_translation'
]
DATA_DIR = '/app/olist_data'

def load_csv_to_postgres():
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    print("Aguardando o PostgreSQL estar pronto...")
    time.sleep(10) 

    print(f"Conectado ao PostgreSQL em {DB_HOST}:{DB_PORT}/{DB_NAME}")

    for table_name_csv in TABLES_TO_LOAD:
        csv_file = os.path.join(DATA_DIR, f'{table_name_csv}.csv')

        if not os.path.exists(csv_file):
            print(f"ERRO: Arquivo {csv_file} não encontrado. Certifique-se de descompactar o dataset na pasta '{DATA_DIR}'.")
            continue

        print(f"Carregando {table_name_csv}...")
        try:
            df = pd.read_csv(csv_file)
            db_table_name = table_name_csv.replace('olist_', '').replace('_dataset', '')
            df.to_sql(db_table_name, engine, if_exists='replace', index=False)
            print(f"Tabela '{db_table_name}' carregada com {len(df)} linhas.")
        except Exception as e:
            print(f"Erro ao carregar {table_name_csv}: {e}")

    print("Carregamento de todos os CSVs concluído.")

if __name__ == "__main__":
    load_csv_to_postgres()