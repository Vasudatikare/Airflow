from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from yt_etl import yt_comments, process_comments

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 9),  # Use a recent date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'youtube_etl_dag',
    default_args=default_args,
    description='DAG for extracting and processing YouTube comments',
    schedule_interval=timedelta(days=1),  # Adjust as needed
)

# Define the YouTube ETL task
def run_youtube_etl():
    video_id = "q8q3OFFfY6c"  # Replace with a dynamic value if needed
    response = yt_comments(video_id)
    process_comments(response)

# Create the task
run_etl_task = PythonOperator(
    task_id='run_youtube_etl',
    python_callable=run_youtube_etl,
    dag=dag,
)

# Define task dependencies
run_etl_task
