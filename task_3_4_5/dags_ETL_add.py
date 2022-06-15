

"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ETLPipeline',
    default_args=default_args,
    description='ETL',
    schedule_interval=None,
    )

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id='extractandLoad',
    bash_command='python /home/airflow/gcs/dags/ETL.py',
    dag=dag,
    depends_on_past=False,
   )

