# Databricks notebook source
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

# The first step is to set some default arguments which will be applied to each task in our DAG
default_args = {
    'owner': '0ab336d6fcf7_Nick',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag_id = '0ab336d6fcf7_dag'
start_date = datetime(2024, 1, 27)

# The next section of our DAG script actually instantiates the DAG.
with DAG(dag_id,
         start_date=start_date,
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args) as dag:

    # Define common parameters for Submit Run Operator
    common_params = {
        'databricks_conn_id': 'databricks_default',
        'existing_cluster_id': '1108-162752-8okw8dgg',
    }

    # Define tasks using a loop
    notebook_tasks = [
        {'task_id': 'load_data', 'notebook_path': '/Repos/nickwarmstrong@gmail.com/pinterest-data-pipeline545/databricks/airflow/airflow_load_data'},
        {'task_id': 'clean_pin_data', 'notebook_path': '/Repos/nickwarmstrong@gmail.com/pinterest-data-pipeline545/databricks/airflow/airflow_clean_pin'},
        {'task_id': 'clean_geo_data', 'notebook_path': '/Repos/nickwarmstrong@gmail.com/pinterest-data-pipeline545/databricks/airflow/airflow_clean_geo'},
        {'task_id': 'clean_user_data', 'notebook_path': '/Repos/nickwarmstrong@gmail.com/pinterest-data-pipeline545/databricks/airflow/airflow_clean_user'},
        {'task_id': 'query_data', 'notebook_path': '/Repos/nickwarmstrong@gmail.com/pinterest-data-pipeline545/databricks/airflow/airflow_query_data'},
    ]

    operators = []
    for params in notebook_tasks:
        task_id = params.pop('task_id')
        params['notebook_task'] = {'notebook_path': params.pop('notebook_path')}
        operator = DatabricksSubmitRunOperator(task_id=task_id, **common_params, **params)
        operators.append(operator)

    # Set task dependencies
    operators[0] >> [operators[1], operators[2], operators[3]] >> operators[4]

