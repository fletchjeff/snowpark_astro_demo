
def bulk_load_external(session):
    from citibike_ml import elt as cbelt

    import snowflake.snowpark as snp

    import pandas as pd
    from datetime import datetime
    import os
    import uuid 

    start = datetime.now()
    print("Start Time =", start.strftime("%H:%M:%S"))

    download_base_url = 'https://s3.amazonaws.com/tripdata/'

    project_db_name = 'CITIBIKEML'
    project_schema_name = 'DEMO'
    project_db_schema = str(project_db_name)+'.'+str(project_schema_name)

    load_table_name = str(project_db_schema)+'.'+'RAW_'
    trips_table_name = str(project_db_schema)+'.'+'TRIPS'

    _ = session.sql('CREATE OR REPLACE DATABASE '+str(project_db_name)).collect()
    _ = session.sql('USE DATABASE '+str(project_db_name)).collect()

    _ = session.sql('CREATE SCHEMA '+str(project_db_schema)).collect()
    _ = session.sql('USE SCHEMA '+str(project_db_schema)).collect()

    stage_id = str(uuid.uuid1()).replace('-', '_')
    stage_name = 'load_stage_'+str(stage_id)

    session.sql('CREATE OR REPLACE STAGE '+str(stage_name)).collect()
    #session.sql('CREATE OR REPLACE TEMPORARY STAGE '+str(stage_name)).collect()

    #For files like 201306-citibike-tripdata.zip
    date_range1 = pd.period_range(start=datetime.strptime("201306", "%Y%m"), 
                                 end=datetime.strptime("201612", "%Y%m"), 
                                 freq='M').strftime("%Y%m")
    file_name_end1 = '-citibike-tripdata.zip'
    files_to_extract = [date+file_name_end1 for date in date_range1.to_list()]

    #For files like 201701-citibike-tripdata.csv.zip
    date_range2 = pd.period_range(start=datetime.strptime("201701", "%Y%m"), 
                                 end=datetime.strptime("202002", "%Y%m"), 
                                 freq='M').strftime("%Y%m")

    file_name_end2 = '-citibike-tripdata.csv.zip'
    files_to_extract = files_to_extract + [date+file_name_end2 for date in date_range2.to_list()]

    stage_name, files_to_load = cbelt.extract_trips_to_stage(session, 
                                                             files_to_extract, 
                                                             download_base_url, 
                                                             stage_name)

    stage_table_names = cbelt.load_trips_to_raw(session, files_to_load, stage_name, load_table_name)

    trips_table_name = cbelt.transform_trips(session, stage_table_names, trips_table_name)

    session.close()

    end = datetime.now()
    print("End Time =", end.strftime("%H:%M:%S"))

    run_time = end-start
    print("Total Run Time (min) =", run_time.total_seconds()/60)
