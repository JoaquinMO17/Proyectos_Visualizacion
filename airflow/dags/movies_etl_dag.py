from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'movies_etl_pipeline',
    default_args=default_args,
    description='Movie data ETL pipeline with incremental loading',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['etl', 'movies', 'imdb'],
)

# Task 1: Simple data quality check
data_quality_check = BashOperator(
    task_id='check_data_quality',
    bash_command='echo "Data quality check - ETL will verify CSV file exists during execution"',
    dag=dag,
)

# Task 2: Run ETL pipeline (main task)
run_etl = BashOperator(
    task_id='run_etl_pipeline',
    bash_command='docker-compose -f /opt/airflow/docker-compose-airflow.yml exec -T fastapi-app python -m scripts.etl',
    dag=dag,
)

# Task 3: Success confirmation
success_confirmation = BashOperator(
    task_id='success_confirmation',
    bash_command='echo "ETL pipeline completed successfully at $(date)"',
    dag=dag,
)

# Set task dependencies
data_quality_check >> run_etl >> success_confirmation