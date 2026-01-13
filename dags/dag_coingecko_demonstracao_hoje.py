from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configurações de API e DB
API_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
POSTGRES_CONN_ID = "postgres_default"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='bitcoin_demonstracao_idp',
    default_args=default_args,
    description='Coleta dados mensais de BTC do CoinGecko e salva no Postgres',
    schedule='@monthly', # Executa uma vez por mês
    start_date=datetime(2025, 1, 1),
    catchup=True, # Garante que meses passados desde a start_date sejam processados
    tags=['crypto', 'bitcoin', 'etl'],
)
def crypto_etl_dag():

    @task
    def create_table():
        """Cria a tabela no Postgres se não existir."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        query = """
            CREATE TABLE IF NOT EXISTS btc_prices (
                timestamp TIMESTAMP PRIMARY KEY,
                price_usd NUMERIC(20, 8),
                market_cap_usd NUMERIC(25, 2),
                total_volume_usd NUMERIC(25, 2)
            );
        """
        pg_hook.run(query)

    @task
    def fetch_monthly_bitcoin_data(data_interval_start, data_interval_end):
        """
        Coleta dados do CoinGecko baseados no intervalo da execução da DAG.
        O Airflow 3.0 passa automaticamente data_interval_start e end.
        """
        # Converter objetos pendulum/datetime para UNIX timestamp
        from_ts = int(data_interval_start.timestamp())
        to_ts = int(data_interval_end.timestamp())

        params = {
            'vs_currency': 'usd',
            'from': from_ts,
            'to': to_ts
        }

        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        # Transformar em DataFrame para facilitar a manipulação
        # O CoinGecko retorna listas de [timestamp_ms, valor]
        prices = data['prices']
        market_caps = data['market_caps']
        total_volumes = data['total_volumes']

        df = pd.DataFrame(prices, columns=['timestamp', 'price_usd'])
        df['market_cap_usd'] = [x[1] for x in market_caps]
        df['total_volume_usd'] = [x[1] for x in total_volumes]
        
        # Converter timestamp de ms para datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Retornar como lista de tuplas para o PostgresHook
        return df.values.tolist()

    @task
    def load_to_postgres(rows):
        """Insere os dados coletados no banco de dados."""
        if not rows:
            return

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # SQL para inserção com tratamento de duplicatas (Upsert)
        insert_sql = """
            INSERT INTO btc_prices (timestamp, price_usd, market_cap_usd, total_volume_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
        """
        pg_hook.insert_rows(table='btc_prices_alex', rows=rows, target_fields=['timestamp', 'price_usd', 'market_cap_usd', 'total_volume_usd'])

    # Definindo o fluxo
    create_table_task = create_table()
    data = fetch_monthly_bitcoin_data()
    load_task = load_to_postgres(data)

    create_table_task >> data >> load_task

# Instanciando a DAG
crypto_etl_dag()
