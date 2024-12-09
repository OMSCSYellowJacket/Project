from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging

default_args = {
    'owner': 'mlproj',
    'start_date': datetime(2024, 12, 8, 10, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    try:
        
        with requests.get('http://127.0.0.1:8000/stream', stream = True, timeout = 30) as r:
            for chunk in r.iter_lines():
                if chunk:
                    producer.send('price_data', chunk.decode("utf-8"))
                else:
                    logging.info("No chunks returned...")

        
    except Exception as e:
        logging.error(f'An error occured: {e}')
        

with DAG('user_automation',
         default_args=default_args,
         schedule_interval=timedelta(minutes = 1),
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
