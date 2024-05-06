from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from extract import get_product_data
from transform import transform_data

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='openfood_etl',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily'
)

extract_task = PythonOperator(
    task_id = 'extract_data',
    python_callable=get_product_data,
    op_args=['barcode1', 'barcode2'],
    dag=dag
)

transform_task = PythonOperator(
    task_id = 'transform_data',
    python_callable=transform_data,
    dag=dag 
)

extract_task >> transform_task