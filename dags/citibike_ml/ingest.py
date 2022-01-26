
def incremental_elt(session, load_stage_name, files_to_download, download_base_url, load_table_name, trips_table_name) -> str:
    from citibike_ml import elt as ELT

    load_stage_name, files_to_load = ELT.extract_trips_to_stage(session=session, 
                                                                files_to_download=files_to_download, 
                                                                download_base_url=download_base_url, 
                                                                load_stage_name=load_stage_name)
    stage_table_names = ELT.load_trips_to_raw(session, 
                                              files_to_load=files_to_load, 
                                              load_stage_name=load_stage_name, 
                                              load_table_name=load_table_name)
    
    trips_table_name = ELT.transform_trips(session=session, 
                                           stage_table_names=stage_table_names, 
                                           trips_table_name=trips_table_name)
    return trips_table_name

