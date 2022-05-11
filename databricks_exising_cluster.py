from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import task

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5),
    'email': ['airflow@my_first_dag.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dbdag = DAG(
    dag_id='databrickstest_existing_cluster', default_args=default_args,
    schedule_interval=None)

#replace connection
databricks_conn_id = <connection-id>

run_this = PythonOperator(
    task_id='task1',
    provide_context=True,
    python_callable=call_stopcluster,
    dag=dbdag)

#replace cluster id
json = {
     'existing_cluster_id':'<cluster-id>',
     'notebook_task': {
     'notebook_path': '/Users/manohar_mc@yahoo.co.in/manohar'
     },
   }

notebook_run = DatabricksSubmitRunOperator(databricks_conn_id=databricks_conn_id, task_id='notebook_run', json=json, dag=dbdag)

run_this >> notebook_run

