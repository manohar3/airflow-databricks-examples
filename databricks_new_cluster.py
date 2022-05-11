from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5),
    'email': ['manohar_mc@yahoo.co.in'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dbdag = DAG(
    dag_id='databrickstest', default_args=default_args,
    schedule_interval=None)

#replace connection
databricks_conn_id = <connection-id>

json = {
      'new_cluster': {
      'spark_version': '10.3.x-scala2.12',
      'node_type_id': 'm5d.large',
      'num_workers': 1,
      'aws_attributes': {
        'availability': "SPOT",
        'zone_id': "us-east-1a"
      }
      },
     'notebook_task': {
     'notebook_path': '/Users/manohar_mc@yahoo.co.in/manohar'
     },
   }

notebook_run = DatabricksSubmitRunOperator(databricks_conn_id=databricks_conn_id, task_id='notebook_run', json=json, dag=dbdag)

run_this >> notebook_run

