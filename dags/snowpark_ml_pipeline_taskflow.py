# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.version import version
from datetime import datetime, timedelta
from webbrowser import get
from xml.etree.ElementInclude import include
import snowflake.snowpark as snp
import json, getpass, uuid
from citibike_ml.ingest import incremental_elt
from citibike_ml.mlops_pipeline import deploy_pred_train_udf
from citibike_ml.mlops_pipeline import materialize_holiday_weather
from citibike_ml.mlops_pipeline import generate_feature_views
from citibike_ml.mlops_pipeline import train_predict_feature_views
from citibike_ml.model_eval import deploy_eval_udf
from citibike_ml.model_eval import evaluate_station_predictions
from airflow.decorators import dag, task

local_airflow_path = '/usr/local/airflow/'

def snowpark_connect():
    with open('{}{}creds.json'.format(local_airflow_path,"include/")) as f:
        data = json.load(f)
    connection_parameters = {
    'account': data['account'],
    'user': data['username'],
    'password': data['password'], #getpass.getpass(),
    'role': data['role'],
    'warehouse': data['warehouse'],
    'database': data['database'],
    'schema': data['schema']}
    session = snp.Session.builder.configs(connection_parameters).create()
    return session

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(default_args=default_args, schedule_interval=None, start_date=datetime(2022, 1, 24), catchup=False, tags=['test'])
def snowpark_citibike_ml_taskflow():
    """
    End to end Astronomer / Snowflake ML Demo
    """
    @task()
    def snowpark_database_setup():
        return_data = {}
        session = snowpark_connect()
        #project_db_name = 'CITIBIKEML_JF'
        #project_schema_name = 'DEMO'
        project_db_schema = str(session.getCurrentDatabase())+'.'+str(session.getCurrentSchema())
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
        return_data = {
            "download_base_url":download_base_url,
            "load_table_name":load_table_name,
            "trips_table_name":trips_table_name,
            "holiday_table_name":holiday_table_name,
            "precip_table_name":precip_table_name,
            "model_stage_name":model_stage_name,
            "clone_table_name":clone_table_name,
            "feature_view_name":feature_view_name,
            "pred_table_name":pred_table_name,
            "eval_table_name":eval_table_name,
            "load_stage_name":load_stage_name
        }
        #_ = session.sql('USE DATABASE ' + str(session.getCurrentDatabase())).collect()
        #_ = session.sql('USE SCHEMA ' + str(session.getCurrentSchema())).collect()
        _ = session.sql('CREATE STAGE IF NOT EXISTS ' + str(model_stage_name)).collect()
        #_ = session.sql('CREATE OR REPLACE TEMPORARY STAGE ' + str(load_stage_name)).collect()
        _ = session.sql('CREATE OR REPLACE STAGE ' + str(load_stage_name)).collect()
        _ = session.sql('CREATE OR REPLACE TABLE '+str(clone_table_name)+" CLONE "+str(trips_table_name)).collect()
        _ = session.sql('CREATE TAG IF NOT EXISTS model_id_tag').collect()
        _ = session.sql("ALTER TABLE "+str(clone_table_name)+" SET TAG model_id_tag = '"+str(model_id)+"'").collect()
        _ = session.sql('DROP TABLE IF EXISTS '+pred_table_name).collect()
        _ = session.sql('DROP TABLE IF EXISTS '+eval_table_name).collect()
        session.close()
        print(return_data)
        return return_data
    
    # @task()
    # def something_with_bob(bob):
    #     print(bob)
    #     return bob + " More"
    
    @task()
    def incremental_elt_task(table_name_dict: dict):
        session = snowpark_connect()
        file_name_end2 = '202102-citibike-tripdata.csv.zip'
        file_name_end1 = '201402-citibike-tripdata.zip'
        files_to_download = [file_name_end1, file_name_end2]
        trips_table_name = incremental_elt(session=session, 
                                        load_stage_name=table_name_dict["load_stage_name"], 
                                        files_to_download=files_to_download, 
                                        download_base_url=table_name_dict["download_base_url"], 
                                        load_table_name=table_name_dict["load_table_name"], 
                                        trips_table_name=table_name_dict["trips_table_name"]
                                        )
        session.close()
        return trips_table_name
    
    @task()
    def get_model_udf_name(setup_database_names):
        #print(model_stage_name)
        session = snowpark_connect()
        model_udf_name = deploy_pred_train_udf(session=session, 
                                         function_name='station_train_predict_func', 
                                         model_stage_name=str(setup_database_names['model_stage_name']),
                                         path_pytorch_tabnet='{}include/'.format(local_airflow_path),
                                         path_citibike_ml='{}dags/'.format(local_airflow_path))
        session.close()
        return model_udf_name

    @task()
    def get_holiday_and_precip_table_names(trips_table_name,table_name_dict: dict):
        session = snowpark_connect()
        holiday_table_name, precip_table_name = materialize_holiday_weather(
            session=session,                                                         
            trips_table_name=trips_table_name, 
            holiday_table_name=table_name_dict["holiday_table_name"],
            precip_table_name=table_name_dict["precip_table_name"],
            path = "{}include/".format(local_airflow_path)
            )
        session.close()
        return {
            "holiday_table_name":holiday_table_name,
            "precip_table_name":precip_table_name
        }

    @task()
    def get_feature_view_names(table_name_dict, holiday_and_precip_table_names, top_n): 
        session = snowpark_connect()
        feature_view_names = generate_feature_views(session=session, 
                                            clone_table_name=table_name_dict["clone_table_name"], 
                                            feature_view_name=table_name_dict["feature_view_name"], 
                                            holiday_table_name=holiday_and_precip_table_names["holiday_table_name"], 
                                            precip_table_name=holiday_and_precip_table_names["precip_table_name"],
                                            target_column='COUNT', 
                                            top_n=top_n,
                                            path='{}include/'.format(local_airflow_path))
        session.close()
        return feature_view_names
    
    @task()
    def get_pred_table_name(model_udf_name, feature_view_names, setup_database_names): 
        session = snowpark_connect()
        # same_pred_table_name = train_predict_feature_views(session=session, 
        #                                       station_train_pred_udf_name=model_udf_name, 
        #                                       feature_view_names=feature_view_names, 
        #                                       pred_table_name=setup_database_names["pred_table_name"])
        session.close()
        return {
            "model_udf_name":model_udf_name,
            "feature_view_names":feature_view_names,
            "setup_database_names":setup_database_names
            }

        #return same_pred_table_name

    @task()
    def get_eval_model_udf_name(setup_database_names):
        session = snowpark_connect()
        eval_model_udf_name = deploy_eval_udf(session=session, 
                                      function_name='eval_model_output_func', 
                                      model_stage_name=str(setup_database_names['model_stage_name']),
                                      path_rexmex='{}include/'.format(local_airflow_path),
                                      path_citibike_ml='{}dags/'.format(local_airflow_path)
                                      )
        session.close()
        return eval_model_udf_name

    @task()
    def get_eval_table_name(pred_table_name, eval_model_udf_name, eval_table_name):
        session = snowpark_connect()
        same_eval_table_name = evaluate_station_predictions(session=session, 
                                               pred_table_name=pred_table_name, 
                                               eval_model_udf_name=eval_model_udf_name, 
                                               eval_table_name=eval_table_name)
        session.close()
        return same_eval_table_name                                               
    
    #Task order
    #setup_database_names = dict(snowpark_database_setup())
    top_n = 2
    setup_database_names = snowpark_database_setup()
    trips_table_name = incremental_elt_task(setup_database_names)
    # model_stage_name = str(setup_database_names['model_stage_name'])
    model_udf_name = get_model_udf_name(setup_database_names)
    #incremental_elt_task() >> get_model_udf_name()
    #trips_table_name)
    holiday_and_precip_table_names = get_holiday_and_precip_table_names(trips_table_name,setup_database_names)
    feature_view_names = get_feature_view_names(setup_database_names,holiday_and_precip_table_names,top_n)
    pred_table_name = get_pred_table_name(model_udf_name,feature_view_names,setup_database_names)
    eval_model_udf_name = get_eval_model_udf_name(setup_database_names)
    eval_table_name = get_eval_table_name(pred_table_name,eval_model_udf_name,setup_database_names['eval_table_name'])
    return eval_table_name
    # bob = snowpark_database_setup()
    # hello_bob = something_with_bob(bob)
snowpark_citibike_ml = snowpark_citibike_ml_taskflow()