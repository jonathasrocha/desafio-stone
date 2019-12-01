from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
import airflow.hooks.S3_hook
import os
from airflow.hooks.postgres_hook import PostgresHook 
import shutil

dest_tables = []
dest_tables.append({'table_name': 'd_movie', 'keys': ['movie_id'], 'field': ['title', 'original_language', 'popularity', 'poster_path', 'adult', 'status', 'vote_average'], 'haveheader': 1 })
dest_tables.append({'table_name': 'd_person', 'keys': ['person_id'], 'field': ['name', 'profile_path', 'gender'], 'haveheader': 1 })
dest_tables.append({'table_name': 'd_genre', 'keys': ['genre_id'], 'field': ['name'], 'haveheader': 1 })
dest_tables.append({'table_name': 'f_crew', 'keys': ['person_sk', 'person_sk', 'job'], 'field': ['departament'], 'haveheader': 0 })
dest_tables.append({'table_name': 'f_status', 'keys': ['movie_sk', 'date_status'], 'field': ['status'], 'haveheader': 1 })
dest_tables.append({'table_name': 'f_cost', 'keys': ['movie_sk', 'company_name', 'release_date'], 'field': ['budget', 'revenue'], 'haveheader': 1 })
dest_tables.append({'table_name': 'f_production_countries', 'keys': ['movie_sk', 'release_date', 'iso_3166_1'], 'field': ['name'], 'haveheader': 1 })
dest_tables.append({'table_name': 'f_classification', 'keys': ['movie_sk', 'genre_sk'], 'field': ['release_date'], 'haveheader': 1 })
#src_tables = [item+"_stage" for item in dest_tables]

def upload_file_to_S3(path, key, s3_bucket):
    print("{} ".format(path))
    hook = airflow.hooks.S3_hook.S3Hook(os.environ.get('bucket_name'))
    sent_path = path+'sent' 
    list_dir = os.listdir(path)
    
    i =0
    while i < len(list_dir):
        old_path = os.path.join(path, list_dir[i])
        new_path = os.path.join(sent_path, list_dir[i])
        if os.path.isfile(old_path):
            print( "old {} new{}".format(old_path, new_path)) 
            hook.load_file(old_path, list_dir[i], s3_bucket, replace=True)
            shutil.move(old_path, new_path)
        i+=1
def load_S3_Redshift(table, s3_bucket, s3_path, iam, role, delimiter, region, ignoreheader, dateformat, formated, quote):
    
    pg_hook = PostgresHook(postgres_conn_id='dw_stones')
    conn = pg_hook.get_conn() 
    cursor = conn.cursor()
 
    load_statement = """
        delete from {0};
        copy
        {0}
        from 's3://{1}/{2}'
        iam_role 'arn:aws:iam::{3}:role/{4}'
        format {9} quote '{10}' delimiter '{5}' region '{6}' dateformat '{8}' ignoreheader {7}""".format(table, s3_bucket, s3_path, iam, role, delimiter, region, ignoreheader, dateformat, formated, quote)
    print(load_statement) 
    cursor.execute(load_statement)
    cursor.close()
    conn.commit()
    print("Load command completed")
 
    return True
 
def merge_insert(src_table, dest_table, src_keys, dest_keys, src_field, dest_field):
    hook = PostgresHook(postgres_conn_id='dw_stones')
    conn = hook.get_conn()
    cursor = conn.cursor()
    # build the SQL statement
    print("src: {} dst{}".format(src_table, dest_table))
    sql_statement = "begin transaction; "
    
    sql_statement += "update " + dest_table + " set "
    for i in range (0,len(src_field)):
        sql_statement +=  dest_field[i] + " = " + src_table + "." + src_field[i]
        if(i < len(src_field)-1):
            sql_statement += " , "
    sql_statement +=" from " +src_table + " where "
    for i in range (0,len(src_keys)):
        sql_statement += src_table + "." + src_keys[i] + " = " + dest_table + "." + dest_keys[i]
        if(i < len(src_keys)-1):
            sql_statement += " and "

    sql_statement += "; "
    sql_statement += "delete from "+src_table+" using "+dest_table+" where "
    for i in range (0,len(src_keys)):
        sql_statement += src_table + "." + src_keys[i] + " = " + dest_table + "." + dest_keys[i]
        if(i < len(src_keys)-1):
            sql_statement += " and "
    sql_statement += ";"

    sql_statement += " insert into " + dest_table + " select * from " + src_table + " ; "
    sql_statement += " end transaction; "

    print(sql_statement)
    cursor.execute(sql_statement)
    cursor.close()
    conn.commit()

def task_create_load(tablename, bucket_name, s3_path, iam, role, delimiter, region, ignoreheader, dateformat, formated, quote):
    return PythonOperator(
    task_id ='load_s3_{}'.format(tablename),
    python_callable=load_S3_Redshift,
    op_kwargs={
        'table': tablename,
        's3_bucket' : bucket_name,
        's3_path': s3_path,
        'iam': iam,
        'role': role,
        'delimiter': delimiter,
        'region': region,
        'ignoreheader': ignoreheader,
        'dateformat': dateformat,
        'formated': formated,
        'quote': quote
        })

def task_create_merge(table_name,src_table, dest_table, src_keys, dest_keys, src_field, dest_field):
    return PythonOperator(
    task_id ='merge_s3_to_redshift_{}'.format(table_name),
    python_callable=merge_insert,
    op_kwargs={
        'src_table': src_table,
        'dest_table': dest_table,
        'src_keys': src_keys,
        'dest_keys': dest_keys,
        'src_field': src_field,
        'dest_field': dest_field
        })


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


    task_read_movie >> task_read_details
    task_read_movie >> task_read_crew
    task_read_crew >> task_movies_from_crew 
    task_read_details >> task_upload
    task_read_genre >> task_upload
    task_movies_from_crew >> task_upload
    
    for table in dest_tables:
        task_upload >> task_create_load(table.get('table_name')+"_stage", os.environ.get('bucket_name'), table.get('table_name'), '019215203341', 'myRedshiftRole', ',', 'us-east-1', table.get('haveheader'),'YYYY-MM-DD', 'csv', '"' ) >>task_create_merge( table.get('table_name'), table.get('table_name')+"_stage", table.get('table_name'), table.get('keys'), table.get('keys'), table.get('field'), table.get('field'))

    
if __name__ == "__main__":
    dag.cli()


