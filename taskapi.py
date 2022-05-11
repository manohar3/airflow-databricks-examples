from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import time
from airflow.operators.bash_operator import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python import BranchPythonOperator
import copy
from airflow.utils.task_group import TaskGroup


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5),
    'email': ['manohar_mc@yahoo.co.in'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'trigger_rule': 'one_success'
}

#replace connection
databricks_conn_id = <connection-id>

hook = DatabricksHook(
              databricks_conn_id,
              timeout_seconds = 180,
              retry_limit = 3,
              retry_delay = 1.0
          )

@dag(dag_id='databrickstesttaskapi1', default_args=default_args, schedule_interval=None)
def analytics_pipeline():

  @task
  def create_cluster():
    CREATE_CLUSTERS_ENDPOINT = ('POST', 'api/2.0/clusters/create')
    json = {
      'cluster_name': 'mycluster',
      'spark_version': '9.1.x-scala2.12',
      'node_type_id': 'm5d.large',
      'num_workers': 1,
      'spark_conf': {
        'spark.executor.cores': 1,
        'spark.driver.cores': 1,
        'spark.driver.memory':'1gb',
        'spark.executor.memory': '1gb'
      },
      'aws_attributes': {
        'zone_id': "us-east-1a"
      }
    }
    return hook._do_api_call(CREATE_CLUSTERS_ENDPOINT, json)
  
  @task
  def wait_create_cluster(clusterInfo):
    while True:
      GET_CLUSTERS_ENDPOINT = ('GET', 'api/2.0/clusters/get')
      response = hook._do_api_call(GET_CLUSTERS_ENDPOINT, clusterInfo)
      json_object = json.loads(json.dumps(response,indent=4))
      print(json_object)
      if json_object['state'] == 'PENDING':
         print('waiting for 30 seconds')
         time.sleep(30)
      else:
         return clusterInfo;

  @task
  def run_notebook_task(clusterInfo,**context):
    json_object = json.loads(json.dumps(clusterInfo,indent=4))
    json_request = {
     'existing_cluster_id': json_object['cluster_id'],
     'notebook_task': {
       'notebook_path': '/Users/manohar_mc@yahoo.co.in/manohar'
      }
    }
    op = DatabricksSubmitRunOperator(databricks_conn_id=databricks_conn_id, task_id='notebook_run', json=json_request)   
    op.execute(context)
    return clusterInfo

  @task
  def run_spark_job(clusterInfo, **context):
    json_object = json.loads(json.dumps(clusterInfo,indent=4))
    op = DatabricksSubmitRunOperator(
    databricks_conn_id=databricks_conn_id,
    task_id='spark_jar_task',
    existing_cluster_id=json_object['cluster_id'],
    spark_jar_task={'main_class_name': 'org.apache.spark.examples.SparkPi'},
    libraries=[{'jar': 'dbfs:/FileStore/jars/bcf1672e_d635_4a34_aef7_7c698d8cc2e9-spark_examples_1_1-64ac9.jar'}],
    trigger_rule=TriggerRule.ALL_DONE)
    op.execute(context)
    return clusterInfo

   
  @task
  def delete_cluster(clusterInfo):
    DELETE_CLUSTERS_ENDPOINT = ('POST', 'api/2.0/clusters/permanent-delete')
    response = hook._do_api_call(DELETE_CLUSTERS_ENDPOINT, clusterInfo)
    return 'echo hello'

  def runpisparkjob(cluster_info):
    try:
      run_spark_job(cluster_info)
      return 'runsparkjob.success'
    except: 
      return 'runsparkjob.failed'

  def runnotebookjob(cluster_info):
    try:
      run_notebook_task(cluster_info)
      return 'runnotebook.success'
    except:
      return 'runnotebook.failed'

  @task
  def success():
    return

  @task
  def failed():
    return

  @task
  def success1():
    return

  @task
  def failed1():
    return

  with TaskGroup("createcluster") as group1:
    clusterInfo = create_cluster()
    c=copy.deepcopy(clusterInfo)
    clusterInfo >> wait_create_cluster(c)

  with TaskGroup("runsparkjob") as group2:
    s = success()
    f = failed()
    sparkpitask = BranchPythonOperator(
     task_id='runpisparkjob',
     python_callable=runpisparkjob,
     provide_context=True,
     op_kwargs={
      'cluster_info': c
     }
    )
    sparkpitask >> [s,f]

  with TaskGroup("runnotebook") as group3:
    s1 = success()
    f1 = failed()
    notebooktask = BranchPythonOperator(
     task_id='runnotebookjob',
     python_callable=runnotebookjob,
     provide_context=True,
     op_kwargs={
      'cluster_info': c
    }
    )
    notebooktask >> [s1,f1]

  d = delete_cluster(c)
  group1 >> group2
  s >> group3 >> d
  f >> d

mydag = analytics_pipeline()

