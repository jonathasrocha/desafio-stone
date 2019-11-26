from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
   'owner': 'jonathas rocha',
   'depends_on_past': False,
   'start_date': datetime(2019, 25, 1),
   'retries': 0,
   }

with DAG(
   'Desafio tecnico Stones',
   schedule_interval=timedelta(minutes=1),
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
        cd $AIRFLOW_HOME/dags/etl_scripts_stones/
        python getGenre.py
    """)

    task_read_crew = BashOperator(
        task_id='read_crew',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_scripts_stones/
        python getCrew.py
    """)
    task_read_details = BashOperator(
        task_id='read_details',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_scripts_stones/
        python getDetails.py
   """)
    
    task_upload = BashOperator(
        task_id='upload',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_scripts_stones/upload_data
        upload_data.sh
   """)
    
    task_read_movie >> task_read_details
    task_read_movie >> task_read_crew
    task_read_crew >> task_upload
    task_read_details >> task_upload
    task_read_genre >> task_upload

if __name__ == "__main__":
    dag.cli()


