import logging
import pendulum

from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

task_logger = logging.getLogger('project-4')
local_tz = pendulum.timezone("Europe/Moscow")


def postgres_operator_param(task_id, sql, **params):
    operator = PostgresOperator(
        task_id=task_id,
        postgres_conn_id='postgres_de_conn_id',
        sql=sql,
        params=params
    )
    return operator

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
        'datamarts',
        default_args=args,
        description='Provide default dag for sprint6',
        catchup=True,
        max_active_runs=1,
        schedule_interval='0 0 10 * *',
        start_date=datetime(2022, 6, 10, tzinfo=local_tz),
) as dag:
   create_cmd_dm_courier_ledger = postgres_operator_param('create_cmd_dm_courier_ledger', 'sql/datamarts/create_cdm.dm_courier_ledger.sql')
   update_cmd_dm_courier_ledger = postgres_operator_param('update_cmd_dm_courier_ledger', 'sql/datamarts/update_cdm.dm_courier_ledger.sql')

   (
    create_cmd_dm_courier_ledger
    >>update_cmd_dm_courier_ledger
   )