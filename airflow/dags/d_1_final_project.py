from datetime import datetime
import logging

from airflow import DAG
from airflow.models import Variable, Connection
from airflow.operators.python import PythonOperator

from modules.covid_scraper import CovidScraper
from modules.transformer import Transformer
from modules.connector import Connector


def fun_get_data_from_api(**kwargs):
    # get data
    scraper = CovidScraper(Variable.get('url_covid_tracker'))
    data = scraper.get_data()
    print(data.info())

    # create connector
    get_conn = Connection.get_connection_from_secrets("Mysql")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host=get_conn.host,
        user=get_conn.login,
        password=get_conn.password,
        db=get_conn.schema,
        port=get_conn.port
    )

    # drop table if exists
    try:
        p = "DROP table IF EXISTS covid_jabar"
        engine_sql.execute(p)
    except Exception as e:
        logging.error(e)
    
    # insert to mysql
    data.to_sql(con=engine_sql, name='covid_jabar', index=False)
    logging.info("DATA INSERTED SUCCESSFULLY TO MYSQL")

def fun_generate_dim(**kwargs):
    # get data
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )

    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transformer = Transformer(engine_sql, engine_postgres)
    transformer.create_dimension_case()
    transformer.create_dimension_district()
    transformer.create_dimension_province()


def fun_insert_province_daily(*kwargs):
    # get
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )
    
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transformer = Transformer(engine_sql, engine_postgres)
    transformer.create_province_daily()

def fun_insert_district_daily(*kwargs):
    # get 
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    
    # connector
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )
    
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    # insertdata
    transformer = Transformer(engine_sql, engine_postgres)
    transformer.create_district_daily()

with DAG(
    dag_id='d_1_final_project',
    start_date=datetime(2022, 5, 28),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    op_get_data_from_api = PythonOperator(
        task_id = 'get_data_from_api',
        python_callable = fun_get_data_from_api
    )

    op_generate_dim = PythonOperator(
        task_id = 'generate_dim',
        python_callable = fun_generate_dim
    )

    op_insert_province_daily = PythonOperator(
        task_id = 'insert_province_daily',
        python_callable = fun_insert_province_daily
    )

    op_insert_district_daily = PythonOperator(
        task_id = 'insert_district_daily',
        python_callable = fun_insert_district_daily
    )

op_get_data_from_api >> op_generate_dim
op_generate_dim >> op_insert_province_daily
op_generate_dim >> op_insert_district_daily