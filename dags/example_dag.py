from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define default_args dictionary to specify default parameters for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple Airflow DAG with dependencies',
    schedule_interval=None,  # Set the frequency of DAG runs
)

# Define the tasks
source_task_a1 = DummyOperator(task_id='source_task_a1', dag=dag)
source_task_b1 = PythonOperator(task_id='source_task_b1', python_callable=lambda: print("Executing task b1"), dag=dag)
source_task_b2 = PythonOperator(task_id='source_task_b2', python_callable=lambda: print("Executing task b2"), dag=dag)
staging_task_a2 = DummyOperator(task_id='staging_task_a2', dag=dag)
staging_task_b3 = PythonOperator(task_id='staging_task_b3', python_callable=lambda: print("Executing task b3"), dag=dag)
staging_task_b4 = PythonOperator(task_id='staging_task_b4', python_callable=lambda: print("Executing task b4"), dag=dag)
landing_task_a3 = PythonOperator(task_id='landing_task_a3', python_callable=lambda: print("Executing task a3"), dag=dag)
landing_task_b5 = PythonOperator(task_id='landing_task_b5', python_callable=lambda: print("Executing task b5"), dag=dag)
landing_task_c1 = PythonOperator(task_id='landing_task_c1', python_callable=lambda: print("Executing task c1"), dag=dag)

# Define the dependencies
source_task_a1 >> staging_task_a2
[source_task_b1, source_task_b2] >> staging_task_b3
staging_task_b3 >> staging_task_b4
staging_task_a2 >> landing_task_a3
staging_task_b4 >> landing_task_b5
[landing_task_a3, landing_task_b5] >> landing_task_c1