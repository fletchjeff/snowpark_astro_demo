
def extract_trips_to_stage(session, files_to_download: list, download_base_url: str, load_stage_name:str):
    import os 
    import requests
    from zipfile import ZipFile
    import gzip
    
    files_to_load = list()
    
    for zip_file_name in files_to_download:
        gz_file_name = os.path.splitext(zip_file_name)[0]+'.gz'
        url = download_base_url+zip_file_name

        print('Downloading file '+url)
        r = requests.get(url)
        with open(zip_file_name, 'wb') as fh:
            fh.write(r.content)

        with ZipFile(zip_file_name, 'r') as zipObj:
            csv_file_names = zipObj.namelist()
            with zipObj.open(name=csv_file_names[0], mode='r') as zf:
                print('Gzipping file '+csv_file_names[0])
                with gzip.open(gz_file_name, 'wb') as gzf:
                    gzf.write(zf.read())

        print('Putting file '+gz_file_name+' to stage '+load_stage_name)
        session.file.put(gz_file_name, '@'+load_stage_name)
        
        files_to_load.append(gz_file_name)
        os.remove(zip_file_name)
        os.remove(gz_file_name)
    
    return load_stage_name, files_to_load
        
def load_trips_to_raw(session, files_to_load:list, load_stage_name:str, load_table_name:str):
    from snowflake.snowpark import functions as F
    from snowflake.snowpark import types as T
    from datetime import datetime

    stage_table_names = list()
    schema1_files = list()
    schema2_files = list()
    schema2_start_date = datetime.strptime('202102', "%Y%m")
    
    for file_name in files_to_load:
        file_start_date = datetime.strptime(file_name.split("-")[0], "%Y%m")
        if file_start_date < schema2_start_date:
            schema1_files.append(file_name)
        else:
            schema2_files.append(file_name)

    if len(schema1_files) > 0:
        load_schema1 = T.StructType([T.StructField("tripduration", T.StringType()),
                             T.StructField("STARTTIME", T.StringType()), 
                             T.StructField("STOPTIME", T.StringType()), 
                             T.StructField("START_STATION_ID", T.StringType()),
                             T.StructField("START_STATION_NAME", T.StringType()), 
                             T.StructField("START_STATION_LATITUDE", T.StringType()),
                             T.StructField("START_STATION_LONGITUDE", T.StringType()),
                             T.StructField("END_STATION_ID", T.StringType()),
                             T.StructField("END_STATION_NAME", T.StringType()), 
                             T.StructField("END_STATION_LATITUDE", T.StringType()),
                             T.StructField("END_STATION_LONGITUDE", T.StringType()),
                             T.StructField("BIKEID", T.StringType()),
                             T.StructField("USERTYPE", T.StringType()), 
                             T.StructField("birth_year", T.StringType()),
                             T.StructField("gender", T.StringType())])
        csv_file_format_options = {"FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'", "skip_header": 1}
        
        stage_table_name = load_table_name + str('schema1')
        
        loaddf = session.read.option("SKIP_HEADER", 1)\
                              .option("FIELD_OPTIONALLY_ENCLOSED_BY", "\042")\
                              .option("COMPRESSION", "GZIP")\
                              .option("NULL_IF", "\\\\N")\
                              .option("NULL_IF", "NULL")\
                              .schema(load_schema1)\
                              .csv("@"+load_stage_name)\
                              .copy_into_table(stage_table_name, 
                                               files=schema1_files, 
                                               format_type_options=csv_file_format_options)
        stage_table_names.append(stage_table_name)


    if len(schema2_files) > 0:
        load_schema2 = T.StructType([T.StructField("ride_id", T.StringType()), 
                             T.StructField("rideable_type", T.StringType()), 
                             T.StructField("STARTTIME", T.StringType()), 
                             T.StructField("STOPTIME", T.StringType()), 
                             T.StructField("START_STATION_NAME", T.StringType()), 
                             T.StructField("START_STATION_ID", T.StringType()),
                             T.StructField("END_STATION_NAME", T.StringType()), 
                             T.StructField("END_STATION_ID", T.StringType()),
                             T.StructField("START_STATION_LATITUDE", T.StringType()),
                             T.StructField("START_STATION_LONGITUDE", T.StringType()),
                             T.StructField("END_STATION_LATITUDE", T.StringType()),
                             T.StructField("END_STATION_LONGITUDE", T.StringType()),
                             T.StructField("USERTYPE", T.StringType())])
        csv_file_format_options = {"FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'", "skip_header": 1}

        stage_table_name = load_table_name + str('schema2')
        loaddf = session.read.option("SKIP_HEADER", 1)\
                              .option("FIELD_OPTIONALLY_ENCLOSED_BY", "\042")\
                              .option("COMPRESSION", "GZIP")\
                              .option("NULL_IF", "\\\\N")\
                              .option("NULL_IF", "NULL")\
                              .schema(load_schema2)\
                              .csv("@"+load_stage_name)\
                              .copy_into_table(stage_table_name, 
                                               files=schema2_files, 
                                               format_type_options=csv_file_format_options)
        stage_table_names.append(stage_table_name)
        
    return list(set(stage_table_names))
    
def transform_trips(session, stage_table_names:list, trips_table_name:str):
    from snowflake.snowpark import functions as F
        
    #Change all dates to YYYY-MM-DD HH:MI:SS format
    date_format_match = "^([0-9]?[0-9])/([0-9]?[0-9])/([0-9][0-9][0-9][0-9]) ([0-9]?[0-9]):([0-9][0-9])(:[0-9][0-9])?.*$"
    date_format_repl = "\\3-\\1-\\2 \\4:\\5\\6"
    
    for stage_table_name in stage_table_names:
        
        transdf = session.table(stage_table_name)
        transdf.withColumn('STARTTIME', F.regexp_replace(F.col('STARTTIME'),
                                                F.lit(date_format_match), 
                                                F.lit(date_format_repl)))\
               .withColumn('STARTTIME', F.to_timestamp('STARTTIME'))\
               .withColumn('STOPTIME', F.regexp_replace(F.col('STOPTIME'),
                                                F.lit(date_format_match), 
                                                F.lit(date_format_repl)))\
               .withColumn('STOPTIME', F.to_timestamp('STOPTIME'))\
               .select(F.col('STARTTIME'), 
                       F.col('STOPTIME'), 
                       F.col('START_STATION_ID'), 
                       F.col('START_STATION_NAME'), 
                       F.col('START_STATION_LATITUDE'), 
                       F.col('START_STATION_LONGITUDE'), 
                       F.col('END_STATION_ID'), 
                       F.col('END_STATION_NAME'), F.col('END_STATION_LATITUDE'), 
                       F.col('END_STATION_LONGITUDE'), 
                       F.col('USERTYPE'))\
               .write.saveAsTable(trips_table_name)

    return trips_table_name
    
