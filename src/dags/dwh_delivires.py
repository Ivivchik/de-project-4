import json
import logging
import pendulum
import requests

from datetime import datetime
from requests import RequestException
from psycopg2.extras import execute_batch

from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

task_logger = logging.getLogger('dwh_delivires')
local_tz = pendulum.timezone("Europe/Moscow")

postgres_de_conn_id = PostgresHook('postgres_de_conn_id')
http_conn_id = HttpHook.get_connection('http_yandex_cloud_conn_id')

nickname = http_conn_id.extra_dejson.get('nickname')
cohort = http_conn_id.extra_dejson.get('cohort')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


business_dt = "{{ execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}"
next_business_dt = "{{ next_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d %H:%M:%S') }}"


# Вопрос в комментах номер 3, по поводу данной функции
def get_response_content(url):
    try:
        offset = 0
        initial_url = f'{url}&offset={offset}'
        response = requests.get(initial_url, headers=headers)
        response.raise_for_status()
        load_response = json.loads(response.content)
        result = load_response
        while load_response:
            offset +=50
            url = f'{url}&offset={offset}'
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            load_response = json.loads(response.content)
            result.extend(load_response)
    except RequestException: raise Exception('Error while get information from api.')

    return result

def get_deliveries_data(dt, next_dt, restaurant_id):
    task_logger.info('Making get request for deliveries')

    url = f"{base_url}/deliveries?restaurant_id={restaurant_id}&sort_field=date&from={dt}&to={next_dt}&limit=50"

    task_logger.info(f'Url: {url}')

    response = get_response_content(url)
    result = [dict(e,  restaurant_id=restaurant_id) for e in response]
    return result

def get_couriers_and_restaraunts_date(url_method):
    task_logger.info(f'Making get request for {url_method}')

    url = f'{base_url}/{url_method}?limit=50'
    
    task_logger.info(f'Url: {url}')

    response = get_response_content(url)
    return response

def upload_data_from_api(url_method, query, **op_kwargs):
    if url_method == 'deliveries':
        response = get_deliveries_data(op_kwargs['dt'],
                                       op_kwargs['next_dt'],
                                       op_kwargs['restaurant_id'])
    elif url_method in ['couriers', 'restaurants']:
        response = get_couriers_and_restaraunts_date(url_method)
    else:
        raise RequestException(f'The entered method: {url_method} is not available')
    
    task_logger.info(f'Query: {query}')
    with postgres_de_conn_id.get_conn() as conn_postgres:
        with conn_postgres.cursor() as cur_postgres:
                execute_batch(cur_postgres, query, response)


def python_oprator_param(task_id, **op_kwargs):
    operator = PythonOperator(
        task_id=task_id,
        python_callable=upload_data_from_api,
        op_kwargs=op_kwargs
    )
    return operator

def postgres_operator_param(task_id, sql, **params):
    operator = PostgresOperator(
        task_id=task_id,
        postgres_conn_id='postgres_de_conn_id',
        sql=sql,
        params=params
    )
    return operator


# Тут вопрос как лучше сделать. Подробнее описал в comments.txt - вопрос номер 1
def upload_stg_deliveries():
    query = 'SELECT DISTINCT id FROM stg.api_restaurants'
    with postgres_de_conn_id.get_conn() as conn_postgres:
        with conn_postgres.cursor() as cur_postgres:
            cur_postgres.execute(query)
            restaraunts_id = cur_postgres.fetchall()
    insert_query = """INSERT INTO stg.api_deliveries
                      VALUES (%(order_id)s,
                              %(order_ts)s,
                              %(restaurant_id)s,
                              %(delivery_id)s,
                              %(courier_id)s,
                              %(address)s,
                              %(delivery_ts)s,
                              %(rate)s,
                              %(sum)s,
                              %(tip_sum)s)"""
    result = [python_oprator_param(f'upload_deliveries_for_restaraunt_{id[0]}',
                                   url_method='deliveries',
                                   dt=business_dt,
                                   next_dt=next_business_dt,
                                   restaurant_id=id[0],
                                   query=insert_query) for id in restaraunts_id]
    return result

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
        'dwh_delivires',
        default_args=args,
        description='Provide default dag for sprint6',
        catchup=True,
        max_active_runs=1,
        schedule_interval='*/15 * * * *',
        start_date=datetime(2022,7, 1, 17, 30, tzinfo=local_tz),
        end_date=datetime(2022,7, 1, 19, 30, tzinfo=local_tz),
) as dag:
    start = DummyOperator(task_id='start')

    with TaskGroup(group_id='create_stg_tables') as create_stg_group:
        create_stg_deliveries = postgres_operator_param('create_stg_deliveries', 'sql/create-tables/create_stg.deliveries.sql')
        create_stg_couriers = postgres_operator_param('create_stg_couriers', 'sql/create-tables/create_stg.sql', table_name='stg.api_couriers')
        create_stg_restaurants = postgres_operator_param('create_stg_restaurants', 'sql/create-tables/create_stg.sql', table_name='stg.api_restaurants')

    after_create_stg_tables = DummyOperator(task_id='after_create_stg_tables')

    with TaskGroup(group_id='create_src_dds_dm_tables') as create_src_dds_dm_group:
        create_dds_dm_restaurants = postgres_operator_param('create_dds_dm_restaurants', 'sql/create-tables/create_dds.dm_restaurants.sql')
        create_dds_dm_couriers = postgres_operator_param('create_dds_dm_couriers', 'sql/create-tables/create_dds.dm_couriers.sql')
        create_dds_dm_timestamps = postgres_operator_param('create_dds_dm_timestamps', 'sql/create-tables/create_dds.dm_timestamps.sql')
        create_dds_dm_address = postgres_operator_param('create_dds_dm_address', 'sql/create-tables/create_dds.dm_addresses.sql')

    after_create_src_dds_dm_tables = DummyOperator(task_id='after_create_src_dds_dm_tables')

    with TaskGroup(group_id='create_inter_dds_dm_tables') as create_inter_dds_dm_group:
        create_dds_dm_deliveries = postgres_operator_param('create_dds_dm_deliveries', 'sql/create-tables/create_dds.dm_deliveries.sql')
        create_dds_dm_orders = postgres_operator_param('create_dds_dm_orders', 'sql/create-tables/create_dds.orders.sql')

    create_dds_fct_orders_delivery = postgres_operator_param('create_dds_fct_orders_delivery', 'sql/create-tables/create_dds.fct_delivery.sql')

    with TaskGroup(group_id='upload_stg_tables') as upload_stg_tables:
        upload_stg_restaurants = python_oprator_param('upload_restaurants', url_method='restaurants', query='INSERT INTO stg.api_restaurants VALUES(%(_id)s, %(name)s, now())')
        upload_stg_couriers = python_oprator_param('upload_couriers', url_method='couriers', query='INSERT INTO stg.api_couriers VALUES(%(_id)s, %(name)s, now())')

    upload_stg_deliveries = upload_stg_deliveries()

    with TaskGroup(group_id='upload_src_dds_dm_tables') as upload_src_dds_dm_tables:
        update_dds_restaurants = postgres_operator_param('update_dds_restaurants', 'sql/update-tables/update_dds.dm_restaurants.sql')
        update_dds_couriers = postgres_operator_param('update_dds_couriers', 'sql/update-tables/update_dds.dm_couriers.sql')
        upload_dds_dm_timestamps = postgres_operator_param('upload_dds_dm_timestamps', 'sql/update-tables/update_dds.dm_timestamp.sql', dt=business_dt, next_dt=next_business_dt)   
        upload_dds_dm_address = postgres_operator_param('upload_dds_dm_address', 'sql/update-tables/update_dds.dm_adresses.sql', dt=business_dt, next_dt=next_business_dt)   

    after_upload_src_dds_dm_tables = DummyOperator(task_id='after_upload_src_dds_dm_tables')

    with TaskGroup(group_id='upload_inter_dds_dm_tables') as upload_inter_dds_dm_tables:
        upload_dds_dm_deliveries = postgres_operator_param('upload_dds_dm_deliveries', 'sql/update-tables/update_dds.dm_deliveries.sql', dt=business_dt, next_dt=next_business_dt)   
        upload_dds_dm_orders = postgres_operator_param('upload_dds_dm_orders', 'sql/update-tables/update_dds.orders.sql', dt=business_dt, next_dt=next_business_dt)   

    upload_dds_fct_orders_delivery = postgres_operator_param('upload_dds_fct_orders_delivery', 'sql/update-tables/update_dds.fct_orders_deliviery.sql', dt=business_dt, next_dt=next_business_dt)   

    end = DummyOperator(task_id='end')
    (
        start
        >> create_stg_group
        >> after_create_stg_tables
        >> create_src_dds_dm_group
        >> after_create_src_dds_dm_tables
        >> create_inter_dds_dm_group
        >> create_dds_fct_orders_delivery
        >> upload_stg_tables
        >> upload_stg_deliveries
        >> upload_src_dds_dm_tables
        >> after_upload_src_dds_dm_tables
        >> upload_inter_dds_dm_tables
        >> upload_dds_fct_orders_delivery
        >> end
    )