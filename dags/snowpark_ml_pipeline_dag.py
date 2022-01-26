from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.version import version
from datetime import datetime, timedelta
import snowflake.snowpark as snp
import json, getpass, uuid
from citibike_ml.ingest import incremental_elt
from citibike_ml.mlops_pipeline import deploy_pred_train_udf
from citibike_ml.mlops_pipeline import materialize_holiday_weather
from citibike_ml.mlops_pipeline import generate_feature_views
from citibike_ml.mlops_pipeline import train_predict_feature_views
from citibike_ml.model_eval import deploy_eval_udf
from citibike_ml.model_eval import evaluate_station_predictions

def snowpark_connect():
    with open('/usr/local/airflow/include/creds.json') as f:
        data = json.load(f)
    connection_parameters = {
    'account': data['account'],
    'user': data['username'],
    'password': data['password'], #getpass.getpass(),
    'role': data['role'],
    'warehouse': data['warehouse']}

    session = snp.Session.builder.configs(connection_parameters).create()
    return session

def _snowpark_database_setup():
    session = snowpark_connect()
    project_db_name = 'CITIBIKEML_JF'
    project_schema_name = 'DEMO'
    project_db_schema = str(project_db_name)+'.'+str(project_schema_name)
    top_n = 2
    model_id = str(uuid.uuid1()).replace('-', '_')
    download_base_url = 'https://s3.amazonaws.com/tripdata/'
    load_table_name = str(project_db_schema)+'.'+'RAW_'
    trips_table_name = str(project_db_schema)+'.'+'TRIPS'
    holiday_table_name = str(project_db_schema)+'.'+'HOLIDAYS'
    precip_table_name = str(project_db_schema)+'.'+'WEATHER'
    model_stage_name = str(project_db_schema)+'.'+'model_stage'
    clone_table_name = str(project_db_schema)+'.'+'CLONE_'+str(model_id)
    feature_view_name = str(project_db_schema)+'.'+'STATION_<station_id>_VIEW_'+str(model_id)
    pred_table_name = str(project_db_schema)+'.'+'PREDICTIONS_'+str(model_id)
    eval_table_name = str(project_db_schema)+'.'+'EVAL_'+str(model_id)
    load_stage_name = 'load_stage'
    _ = session.sql('USE DATABASE ' + str(project_db_name)).collect()
    _ = session.sql('USE SCHEMA ' + str(project_schema_name)).collect()
    _ = session.sql('CREATE STAGE IF NOT EXISTS ' + str(model_stage_name)).collect()
    #_ = session.sql('CREATE OR REPLACE TEMPORARY STAGE ' + str(load_stage_name)).collect()
    _ = session.sql('CREATE OR REPLACE STAGE ' + str(load_stage_name)).collect()
    _ = session.sql('CREATE OR REPLACE TABLE '+str(clone_table_name)+" CLONE "+str(trips_table_name)).collect()
    _ = session.sql('CREATE TAG IF NOT EXISTS model_id_tag').collect()
    _ = session.sql("ALTER TABLE "+str(clone_table_name)+" SET TAG model_id_tag = '"+str(model_id)+"'").collect()
    _ = session.sql('DROP TABLE IF EXISTS '+pred_table_name).collect()
    _ = session.sql('DROP TABLE IF EXISTS '+eval_table_name).collect()
    session.close()

def _incremental_elt_task():
    session = snowpark_connect()
    file_name_end2 = '202102-citibike-tripdata.csv.zip'
    file_name_end1 = '201402-citibike-tripdata.zip'
    files_to_download = [file_name_end1, file_name_end2]
    # trips_table_name = incremental_elt(session=session, 
    #                                 load_stage_name=load_stage_name, 
    #                                 files_to_download=files_to_download, 
    #                                 download_base_url=download_base_url, 
    #                                 load_table_name=load_table_name, 
    #                                 trips_table_name=trips_table_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('snowpark_ml_pipeline_dag',
         start_date=datetime(2022, 1, 24),
         max_active_runs=1,
         schedule_interval=None,  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    snowpark_database_setup = PythonOperator(
        task_id='snowpark_database_setup',
        python_callable=_snowpark_database_setup,  # make sure you don't include the () of the function
        provide_context=True
    )

    incremental_elt_task = PythonOperator(
        task_id = 'incremental_elt_task',
        python_callable=_incremental_elt_task
    )


    #     t0 >> tn # indented inside for loop so each task is added downstream of t0

    # t0 >> t1
    # t1 >> [t2, t3] # lists can be used to specify multiple tasks