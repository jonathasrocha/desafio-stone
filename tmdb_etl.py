from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
import airflow.hooks.S3_hook
import os

def upload_file_to_S3(path, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook(os.environ.get('bucket_name'))
    for file_ in os.listdir(path):
        filename = os.path.join(path,file_)
        print(filename)
        hook.load_file(filename, file_, bucket_name, replace=True)

default_args = {
   'owner': 'jonathas rocha',
   'depends_on_past': False,
   'start_date': datetime(2019, 11, 27),
   'retries': 0,
   }

with DAG(
   'Desafio_tecnico_Stones',
   schedule_interval='@daily',
   catchup=False,
   default_args=default_args
   ) as dag:
    
    task_read_movie = BashOperator(
        task_id='read_movies',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_script_stones/
        python getMovies.py
    """)

    task_read_genre = BashOperator(
        task_id='read_genre',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_script_stones/
        python getGenre.py
    """)

    task_read_crew = BashOperator(
        task_id='read_crew',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_script_stones/
        python getCrew.py
    """)
    task_read_details = BashOperator(
        task_id='read_details',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_script_stones/
        python getDetails.py
   """)
    
    task_movies_from_crew = BashOperator(
        task_id='task_movies_from_crew',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_script_stones/
        python getMoviesFromCrew.py
    """)
    task_upload = PythonOperator(
        task_id='upload_to_S3',
        python_callable=upload_file_to_S3,
        op_kwargs={
        'path': '/home/jonathas/airflow/dags/etl_script_stones/data/',
        'key': 'movies_nowplaying.csv',
        'bucket_name': 'stones11323',
        })
    
    task_read_movie >> task_read_details
    task_read_movie >> task_read_crew
    task_read_crew >> task_movies_from_crew 
    task_read_details >> task_upload
    task_read_genre >> task_upload
    task_movies_from_crew >> task_upload

if __name__ == "__main__":
    dag.cli()


