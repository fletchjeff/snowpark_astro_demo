
def eval_model_output_func(input_data: list, 
                           y_true_name: str, 
                           y_score_name: str,
                           group_id_name: str) -> str:
    import pandas as pd
    from rexmex import RatingMetricSet, ScoreCard
    
    metric_set = RatingMetricSet()
    score_card = ScoreCard(metric_set)
    
    input_column_names = [y_true_name, y_score_name, group_id_name]
    df = pd.DataFrame(input_data, columns = input_column_names)
    df.rename(columns={y_true_name: 'y_true', y_score_name:'y_score'}, inplace=True)
    
    df = score_card.generate_report(df,grouping=[group_id_name]).reset_index()
    df.drop('level_1', axis=1, inplace=True)
    
    return [df.values.tolist(), df.columns.tolist()]

def deploy_eval_udf(session, function_name, model_stage_name,path_rexmex, path_citibike_ml) -> str:
    from citibike_ml.model_eval import eval_model_output_func

    dep = 'rexmex.zip'
    source_dir = path_rexmex

    session.clearImports()
    session.addImport(source_dir+dep)
    session.addImport('{}citibike_ml'.format(path_citibike_ml))

    eval_model_output_udf = session.udf.register(eval_model_output_func, 
                                                  name=function_name,
                                                  is_permanent=True,
                                                  stage_location='@'+str(model_stage_name), 
                                                  replace=True)

    return eval_model_output_udf.name

def evaluate_station_predictions(session, pred_table_name, eval_model_udf_name, eval_table_name) -> str:
    from snowflake.snowpark import functions as F
    import pandas as pd
    import ast
    
    eval_df = session.table(pred_table_name)\
                     .select(F.array_agg(F.array_construct('COUNT', 'PRED', 'STATION_ID')).alias('input_data'))

    output_df = eval_df.select(F.call_udf(eval_model_udf_name,
                                          'INPUT_DATA',
                                          F.lit('COUNT'), 
                                          F.lit('PRED'),
                                          F.lit('STATION_ID'))).collect()
    
    df = pd.DataFrame(data = ast.literal_eval(output_df[0][0])[0], 
                      columns = ast.literal_eval(output_df[0][0])[1])

    eval_df = session.createDataFrame(df).write.saveAsTable(eval_table_name)


    return eval_table_name
