


from airflow import DAG
import yfinance as yf
import datetime
from datetime import datetime, timedelta  
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

tickers = ['AAPL']

def fetch_prices_function(**kwargs): # <-- Remember to include "**kwargs" in all the defined functions 
    print('1 Fetching stock prices and remove duplicates...')
    stocks_prices = []
    for i in range(0, len(tickers)):
        prices = yf.download(tickers[i], period = 'ytd').iloc[: , :5].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()
        prices.insert(loc = 1, column = 'Stock', value = tickers[i])
        stocks_prices.append(prices)
    stocks_prices_up = stocks_prices[0][['Date', 'Stock', 'Adj Close']]
    stocks_prices_up['Date'] = stocks_prices_up['Date'].dt.strftime('%Y-%m-%d')
    return stocks_prices_up

def insert_stock_data():
    """
    Insert data  to postgres database
    """

    insert_rows_query = """
        INSERT INTO public.stock_prices (
            Date,
            Stock,
            AdjClose
            )
        VALUES(%s, %s, %s)
    """
    rows = fetch_prices_function()
    for row in rows.values:
        row = tuple(row)
        pg_hook = PostgresHook(postgres_conn_id = "postgres_sql")
        pg_hook.run(insert_rows_query, parameters = row)



with DAG (
    dag_id = "postgres_operator_dag",
    start_date= datetime(2021, 11, 23),
    schedule_interval = '@once',
    catchup = False
) as dag:

    create_stock_table = PostgresOperator(
        task_id = "create_pet_table",
        database = "airflow_db",
        postgres_conn_id = "postgres_sql",
        sql = """
            CREATE TABLE IF NOT EXISTS stock_prices (
            Date VARCHAR NOT NULL,
            Stock VARCHAR NOT NULL,
            AdjClose FLOAT NOT NULL
            );
              """
    )

    populate_stock_table = PythonOperator(
        task_id= "populate_stock_table",
        python_callable = insert_stock_data
    )

    
    create_stock_table >> populate_stock_table



