
def station_train_predict_func(input_data: list, 
                               input_columns_str: str, 
                               target_column: str,
                               cutpoint: int, 
                               max_epochs: int) -> str:
    
    input_columns = input_columns_str.split(' ')
    feature_columns = input_columns.copy()
    feature_columns.remove('DATE')
    feature_columns.remove(target_column)
    
    from torch import tensor
    import pandas as pd
    from pytorch_tabnet.tab_model import TabNetRegressor
    
    model = TabNetRegressor()

    df = pd.DataFrame(input_data, columns = input_columns)
    
    y_valid = df[target_column][-cutpoint:].values.reshape(-1, 1)
    X_valid = df[feature_columns][-cutpoint:].values
    y_train = df[target_column][:-cutpoint].values.reshape(-1, 1)
    X_train = df[feature_columns][:-cutpoint].values

    model.fit(
        X_train, y_train,
        eval_set=[(X_valid, y_valid)],
        max_epochs=max_epochs,
        patience=100,
        batch_size=1024, 
        virtual_batch_size=128,
        num_workers=0,
        drop_last=False)
    
    
    df['PRED'] = model.predict(tensor(df[feature_columns].values))
    df = pd.concat([df, pd.DataFrame(model.explain(df[feature_columns].values)[0], 
                           columns = feature_columns).add_prefix('EXPL_')], axis=1)
    return [df.values.tolist(), df.columns.tolist()]
