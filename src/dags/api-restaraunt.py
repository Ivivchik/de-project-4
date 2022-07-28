import json
import logging
import pendulum
import requests

from datetime import datetime
from requests import RequestException
from psycopg2.extras import execute_batch

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.http_hook import HttpHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

task_logger = logging.getLogger('project-4')
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



# По поводу функций - Подробнее описал вопрос 4 в comments.txt
def get_data_from_api(url):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except RequestException: raise Exception('Error while get information from api.')

    result = json.loads(response.content)
    return result

def create_url(url_method, dt='', next_dt=''):
    url = None
    if url_method == 'deliveries':
        url = f"{base_url}/{url_method}?sort_field=date&from={dt}&to={next_dt}"
    else:
        url = f'{base_url}/{url_method}'
    task_logger.info(f'Url: {url}')
    return url

def append_response_with_addition_info(url_method, dt, next_dt='', restaurant_id=''):
    task_logger.info(f'Making get request for {url_method}')
    result = None
    if url_method == 'deliveries':
        url = create_url(url_method=url_method, dt=dt, next_dt=next_dt)
        response = get_data_from_api(url)
        result = [dict(e,  restaurant_id=restaurant_id) for e in response]
    elif url_method == 'couriers' or url_method == 'restaurants':
        url = url = create_url(url_method=url_method)
        response = get_data_from_api(url)
        result = response
    else:
        raise RequestException(f'The entered method: {url_method} is not available')
    return result

def upload_data_from_api(url_method, postgres_conn_id, query, dt, next_dt='', restaurant_id=''):
    response = append_response_with_addition_info(url_method=url_method, dt=dt, next_dt=next_dt, restaurant_id=restaurant_id)
    task_logger.info(f'Query: {query}')
    with postgres_conn_id.get_conn() as conn_postgres:
        with conn_postgres.cursor() as cur_postgres:
                execute_batch(cur_postgres, query, response)


# Тут вопрос как лучше сделать. Подробнее описал в comments.txt - вопрос номер 1
@task
def upload_stg_deliveries(postgres_conn_id, dt, next_dt):
    query = 'SELECT DISTINCT id FROM stg.api_restaurants'
    task_logger.info(f'Query: {query}')
    with postgres_conn_id.get_conn() as conn_postgres:
        with conn_postgres.cursor() as cur_postgres:
            cur_postgres.execute(query)
            result = cur_postgres.fetchall()
    for id in result:
        PythonOperator(
        task_id=f'upload_deliveries_for_restaraunt_{id[0]}',
        python_callable=upload_data_from_api,
        op_kwargs={'url_method': 'deliveries',
                   'dt' : dt,
                   'next_dt': next_dt,
                   'postgres_conn_id': postgres_de_conn_id,
                   'restaurant_id': id[0],
                   'query': 'INSERT INTO stg.api_deliveries \
                             VALUES (%(order_id)s, \
                                     %(order_ts)s, \
                                     %(restaurant_id)s, \
                                     %(delivery_id)s, \
                                     %(courier_id)s, \
                                     %(address)s, \
                                     %(delivery_ts)s, \
                                     %(rate)s, \
                                     %(sum)s, \
                                     %(tip_sum)s)'},
        ).execute(dict())
        

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
        'task2',
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
        # По поводу созание таблиц с полем report_dt - Подробнее описал вопрос 3 в comments.txt
        create_stg_deliveries = PostgresOperator(
            task_id='create_stg_deliveries',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/create-tables/create_stg.deliveries.sql'
        )
        create_stg_couriers = PostgresOperator(
            task_id='create_stg_couriers',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/create-tables/create_stg.sql',
            params={'table_name': 'stg.api_couriers'}
        )
        create_stg_restaurants = PostgresOperator(
            task_id='create_stg_restaurants',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/create-tables/create_stg.sql',
            params={'table_name': 'stg.api_restaurants'}
        )

    after_create_stg_tables = DummyOperator(task_id='after_create_stg_tables')

    with TaskGroup(group_id='create_src_dds_dm_tables') as create_src_dds_dm_group:
        create_dds_dm_restaurants = PostgresOperator(
            task_id='create_dds_dm_restaurants',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/create-tables/create_dds.sql',
            params={'table_name': 'dds.dm_restaurants'}
        )
        create_dds_dm_couriers = PostgresOperator(
            task_id='create_dds_dm_couriers',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/create-tables/create_dds.sql',
            params={'table_name': 'dds.dm_couriers'}
        )
        create_dds_dm_timestamps = PostgresOperator(
            task_id='create_dds_dm_timestamps',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/create-tables/create_dds.dm_timestamps.sql'
        )
        create_dds_dm_address = PostgresOperator(
            task_id='create_dds_dm_address',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/create-tables/create_dds.dm_addresses.sql'
        )

    after_create_src_dds_dm_tables = DummyOperator(task_id='after_create_src_dds_dm_tables')

    with TaskGroup(group_id='create_inter_dds_dm_tables') as create_inter_dds_dm_group:
        create_dds_dm_deliveries = PostgresOperator(
            task_id='create_dds_dm_deliveries',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/create-tables/create_dds.dm_deliveries.sql'
        )
        create_dds_dm_orders = PostgresOperator(
            task_id='create_dds_dm_orders',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/create-tables/create_dds.orders.sql'
        )
    create_dds_fct_orders_delivery = PostgresOperator(
        task_id='create_dds_fct_orders_delivery',
        postgres_conn_id='postgres_de_conn_id',
        sql='sql/create-tables/create_dds.fct_delivery.sql'
    )

    with TaskGroup(group_id='upload_stg_tables') as upload_stg_tables:
        upload_stg_restaurants = PythonOperator(
            task_id='upload_restaurants',
            python_callable=upload_data_from_api,
            op_kwargs={'url_method': 'restaurants',
                       'dt' : business_dt,
                       'postgres_conn_id': postgres_de_conn_id,
                       'query': 'INSERT INTO stg.api_restaurants \
                                 VALUES(%(_id)s, %(name)s, now())'},
        )
        upload_stg_couriers = PythonOperator(
            task_id='upload_couriers',
            python_callable=upload_data_from_api,
            op_kwargs={'url_method': 'couriers',
                       'dt' : business_dt,
                       'postgres_conn_id': postgres_de_conn_id,
                       'query': 'INSERT INTO stg.api_couriers \
                                 VALUES(%(_id)s, %(name)s, now())'},
        )

    with TaskGroup(group_id='upload_src_dds_dm_tables') as upload_src_dds_dm_tables:
        update_dds_restaurants = PostgresOperator(
            task_id='update_dds_restaurants',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/update-tables/update.sql',
            params={
                'target_table': 'dds.dm_restaurants',
                'source_table': 'stg.api_restaurants'
            }
        )
        update_dds_couriers = PostgresOperator(
            task_id='update_dds_couriers',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/update-tables/update.sql',
            params={
                'target_table': 'dds.dm_couriers',
                'source_table': 'stg.api_couriers'
            }
        )
        upload_dds_dm_timestamps = PostgresOperator(
            task_id='upload_dds_dm_timestamps',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/update-tables/update_dds.dm_timestamp.sql',
            params={
                'dt': business_dt,
                'next_dt': next_business_dt
            }
        )        
        upload_dds_dm_address = PostgresOperator(
            task_id='upload_dds_dm_address',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/update-tables/update_dds.dm_adresses.sql',
            params={
                'dt': business_dt,
                'next_dt': next_business_dt
            }
        )

    after_upload_src_dds_dm_tables = DummyOperator(task_id='after_upload_src_dds_dm_tables')

    with TaskGroup(group_id='upload_inter_dds_dm_tables') as upload_inter_dds_dm_tables:
        upload_dds_dm_deliveries = PostgresOperator(
            task_id='upload_dds_dm_deliveries',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/update-tables/update_dds.dm_deliveries.sql',
            params={
                'dt': business_dt,
                'next_dt': next_business_dt
            }
        )
        upload_dds_dm_orders = PostgresOperator(
            task_id='upload_dds_dm_orders',
            postgres_conn_id='postgres_de_conn_id',
            sql='sql/update-tables/update_dds.orders.sql',
            params={
                'dt': business_dt,
                'next_dt': next_business_dt
            }
        )

    upload_dds_fct_orders_delivery = PostgresOperator(
        task_id='upload_dds_fct_orders_delivery',
        postgres_conn_id='postgres_de_conn_id',
        sql='sql/update-tables/update_dds.fct_orders_deliviery.sql',
        params={
                'dt': business_dt,
                'next_dt': next_business_dt
            }
    )
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
        >> upload_stg_deliveries(postgres_de_conn_id, business_dt, next_business_dt)
        >> upload_src_dds_dm_tables
        >> after_upload_src_dds_dm_tables
        >> upload_inter_dds_dm_tables
        >> upload_dds_fct_orders_delivery
        >> end
    )