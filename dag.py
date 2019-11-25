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

t1 = BashOperator(
   task_id='read_movies',
   bash_command="""
   cd $AIRFLOW_HOME/dags/etl_scripts/
   python getMovies.py
   """)

t2 = BashOperator(
   task_id='read_genre',
   bash_command="""
   cd $AIRFLOW_HOME/dags/etl_scripts/
   python getGenre.py
   """)
t1 >> t2

t3 = BashOperator(
   task_id='read_crew',
   bash_command="""
   cd $AIRFLOW_HOME/dags/etl_scripts/
   python getCrew.py
   """)
t1 >> t3
t4 = BashOperator(
   task_id='read_details',
   bash_command="""
   cd $AIRFLOW_HOME/dags/etl_scripts/
   python getDetails.py
   """)
t1 >> t4
t5 = BashOperator(
   task_id='read_details',
   bash_command="""
   cd $AIRFLOW_HOME/dags/etl_scripts/upload_data
   upload_data.sh
   """)
t2 >> t5
t3 >> t5
t4 >> t5

if __name__ == "__main__":
    dag.cli()


