from airflow import DAG #type:ignore
from airflow.operators.bash import BashOperator #type:ignore
from datetime import datetime, timedelta

default_args = {
    'owner': 'pokemon_master',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'smogon_elt_pipeline',
    default_args=default_args,
    description='A pipeline to analyze Pokemon Usage Data',
    schedule_interval=timedelta(days=30),
    catchup=False,
    tags=['pokemon', 'smogon', 'etl']
) as dag:

    ingest_stats = BashOperator(
        task_id='ingest_smogon_chaos',
        bash_command='python /opt/airflow/scripts/ingest_smogon.py 2025-11'
    )

    fetch_metadata = BashOperator(
        task_id='fetch_pokeapi_metadata',
        bash_command='python /opt/airflow/scripts/fetch_metadata.py'
    )

    process_spark = BashOperator(
        task_id='process_data_spark',
        bash_command='python /opt/airflow/scripts/process_smogon_spark.py'
    )

    ingest_stats >> fetch_metadata >> process_spark