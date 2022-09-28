from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
import main

default_args = {
 'owner': 'airflow',
 'depends_on_past': False,
 'email_on_failure': False,
 'email_on_retry': False,
 'retries': 1,
 'retry_delay': timedelta(minutes=2)
}
state_names = ["Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]

with DAG('covid_data_pipeline',
 start_date=datetime(2021, 1, 1),
 schedule_interval='@daily',
 catchup=False,
 default_args=default_args
) as dag:
 extract = PythonOperator(
  task_id='extract',
  python_callable=main.extract,
  dag=dag
 )
 load = PythonOperator(
  task_id='load',
  python_callable=main.load,
  op_kwargs={'state_names': state_names},
  dag=dag
 )
 extract >> load
