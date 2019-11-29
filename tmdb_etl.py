from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
import airflow.hooks.S3_hook
import os
from airflow.hooks.postgres_hook import PostgresHook 

dest_tables = (['d_movie','d_genre','d_person', 'f_production_countries', 'f_cost', 'f_crew', 'f_classification', 'f_status'])
dest_tables.append({'table_name': 'd_movie', 'keys': ['movie_id'], 'field': ['title', 'original_title', 'popularity', 'poster_path', 'adult', 'status', 'vote_average']})
dest_tables.append({'table_name': 'd_person', 'keys': ['person_id'], 'field': ['name', 'profile_path', 'gender']})
dest_tables.append({'table_name': 'd_genre', 'keys': ['genre_id'], 'field': ['name']})
dest_tables.append({'table_name': 'f_crew', 'keys': ['person_sk', 'person_sk', 'job'], 'field': ['department']})
dest_tables.append({'table_name': 'f_status', 'keys': ['movie_sk', 'date_status'], 'field': ['status']})
dest_tables.append({'table_name': 'f_cost', 'keys': ['movie_id', 'company_name', 'release_date'], 'field': ['budget', 'revenue']})
dest_tables.append({'table_name': 'f_production_countries', 'keys': ['movie_id', 'release_date', 'iso_3166_1'], 'field': ['name']})

#src_tables = [item+"_stage" for item in dest_tables]

def upload_file_to_S3(path, key, s3_bucket):
    hook = airflow.hooks.S3_hook.S3Hook(os.environ.get('bucket_name'))
    for file_ in os.listdir(path):
        filename = os.path.join(path,file_)
        print(filename)
        hook.load_file(filename, file_, s3_bucket, replace=True)
        os.rename(path, os.path.join('sent', key))

def load_S3_Redshift(table, s3_bucket, s3_path, iam, role, delimiter, region, ignoreheader):
    
    pg_hook = PostgresHook(postgres_conn_id='dw_stones')
    conn = pg_hook.get_conn() 
    cursor = conn.cursor()
 
    load_statement = """
        delete from {0};
        copy
        {0}
        from 's3://{1}/{2}'
        iam_role 'arn:aws:iam::{3}:role/{4}'
        delimiter '{5}' region '{6}' ignoreheader {7}""".format(table, s3_bucket, s3_path, iam, role, delimiter, region, ignoreheader)
    print(load_statement) 
    cursor.execute(load_statement)
    cursor.close()
    conn.commit()
    print("Load command completed")
 
    return True
 
def merge_insert(src_table, dest_table, src_keys, dest_keys, src_field, dest_field):
    hook = PostgresHook(postgres_conn_id=self.src_redshift_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    log.info("Connected with ")
    # build the SQL statement
    sql_statement = "begin transaction; "
    
    sql_statement += "update " + self.dest_table + "set"
    for i in range (0,len(self.src_field)):
        sql_statement += self.dest_table + "." + self.dest_field[i] + " = " + self.src_table + "." + src_field[i]
        if(i < len(self.src_field)-1):
            sql_statement += " , "
    " from " + self.src_table + " where "
    for i in range (0,len(self.src_keys)):
        sql_statement += self.src_table + "." + self.src_keys[i] + " = " + self.dest_table + "." + self.dest_keys[i]
        if(i < len(self.src_keys)-1):
            sql_statement += " and "

    sql_statement += "; "
    sql_statement = "delete from "+src_table+" using "+dest_table+" where "
    for i in range (0,len(self.src_keys)):
        sql_statement += self.src_table + "." + self.src_keys[i] + " = " + self.dest_table + "." + self.dest_keys[i]
        if(i < len(self.src_keys)-1):
            sql_statement += " and "
    
    
    sql_statement += " insert into " + self.dest_table + " select * from " + self.src_table + " ; "
    sql_statement += " end transaction; "

    print(sql_statement)
    cursor.execute(sql_statement)
    cursor.close()
    conn.commit()
    log.info("Upsert command completed")


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
        's3_bucket': 'stones11323',
        })

    t5 = PythonOperator(
        task_id ='load_s3',
        python_callable=load_S3_Redshift,
        op_kwargs={
            'table': 'd_movie_stage', 
            's3_bucket' : os.environ.get('bucket_name'), 
            's3_path': 'd_movies_',
            'iam': '019215203341',
            'role': 'myRedshiftRole', 
            'delimiter': ',',
            'region': 'us-east-1', 
            'ignoreheader': 1
            
        })


    task_read_movie >> task_read_details
    task_read_movie >> task_read_crew
    task_read_crew >> task_movies_from_crew 
    task_read_details >> task_upload
    task_read_genre >> task_upload
    task_movies_from_crew >> task_upload
    task_upload >> t5
if __name__ == "__main__":
    dag.cli()


