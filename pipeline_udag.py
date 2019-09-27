import airflowlib.emr_lib as emr
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql.functions import udf, col, lower, lit, to_date, current_date, datediff, split, trim
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, date_add, monotonically_increasing_id, to_date, expr


default_args = {
    'owner': 'adrianbrown',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 20),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email_on_retry': False,
    'catchup': False
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('pipeline_data', concurrency=3, schedule_interval=None, default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)

# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='movielens_cluster', num_core_nodes=2)
    return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

# Converts city demo csv to parquet tables
def city_demo_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                              '/root/airflow/dags/transform/city_demo.py')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

# Converts airport csv to parquet tables
def airport_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                              '/root/airflow/dags/transform/airport.py')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)
    
# Converts imm csvs to parquet tables
def immigration_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                              '/root/airflow/dags/transform/imm.py')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

# Converts land temperature csv to parquet tables
def land_temp_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                              '/root/airflow/dags/transform/land_temp.py')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)

# Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

city_demo = PythonOperator(
    task_id='city_demo',
    python_callable=city_demo_to_parquet,
    dag=dag)

airport = PythonOperator(
    task_id='airport',
    python_callable=airport_to_parquet,
    dag=dag)

imm = PythonOperator(
    task_id='imm',
    python_callable=immigration_to_parquet,
    dag=dag)

land_temp = PythonOperator(
    task_id='land_temp',
    python_callable=land_temp_to_parquet,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

# DAG dependencies
create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >>> airport >> terminate_cluster
wait_for_cluster_completion >>> city_demo >> terminate_cluster
wait_for_cluster_completion >>> imm >> terminate_cluster
wait_for_cluster_completion >>> land_temp >> terminate_cluster