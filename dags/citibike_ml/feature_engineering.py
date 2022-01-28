
def generate_holiday_df(session, start_date, end_date):
    from snowflake.snowpark import functions as F 
    from pandas.tseries.holiday import USFederalHolidayCalendar
    import pandas as pd


    cal = USFederalHolidayCalendar()
    holiday_list = pd.DataFrame(cal.holidays(start=start_date, end=end_date), columns=['DATE'], dtype=str)
    holiday_df = session.createDataFrame(holiday_list) \
                        .toDF(holiday_list.columns[0]) \
                        .withColumn("HOLIDAY", F.lit(1))
    
    return holiday_df

def generate_precip_df(session, start_date, end_date, path):
    import pandas as pd
    from snowflake.snowpark import functions as F 

    tempdf = pd.read_csv('{}weather.csv'.format(path))
    tempdf.set_index(pd.to_datetime(tempdf['dt_iso'].str.replace(' UTC', ''), 
                                    format='%Y-%m-%d %H:%M:%S %z', 
                                    utc=True), inplace=True)
    tempdf['DATE'] = tempdf.index.tz_convert("America/New_York")
    tempdf['DATE'] = tempdf['DATE'].dt.date
    tempdf = tempdf.groupby('DATE', as_index=False).agg(PRECIP=('rain_1h', 'sum'))

    precip_df = session.createDataFrame(tempdf).filter((F.col('DATE') >= start_date) &
                                                        (F.col('DATE') <= end_date))
    return precip_df


def generate_features(session, input_df, holiday_table_name, precip_table_name,path):
    import snowflake.snowpark as snp
    from snowflake.snowpark import functions as F 

    agg_period = 'DAY'
    date_win = snp.Window.orderBy('DATE')
    
    start_date, end_date = input_df.select(F.min('STARTTIME'), F.max('STARTTIME')).collect()[0][0:2]

    holiday_df = session.table(holiday_table_name)
    try: 
        _ = holiday_df.columns()
    except:
        holiday_df = generate_holiday_df(session=session,
                                         start_date=start_date,
                                         end_date=end_date)
        
    precip_df = session.table(precip_table_name)
    try: 
        _ = precip_df.columns()
    except:
        precip_df = generate_precip_df(session=session,
                                       start_date=start_date,
                                       end_date=end_date,
                                       path=path)

    feature_df = input_df.withColumn('DATE', 
                                   F.call_builtin('DATE_TRUNC', 
                                                  (agg_period, F.col('STARTTIME')))) \
                       .groupBy('DATE').count() 

    #Impute missing values for lag columns using mean of the previous period.
    mean_1 = round(feature_df.sort('DATE').limit(1).select(F.mean('COUNT')).collect()[0][0])
    mean_7 = round(feature_df.sort('DATE').limit(7).select(F.mean('COUNT')).collect()[0][0])

    feature_df = feature_df.withColumn('LAG_1', F.lag('COUNT', offset=1, default_value=mean_1) \
                                         .over(date_win)) \
                   .withColumn('LAG_7', F.lag('COUNT', offset=7, default_value=mean_7) \
                                         .over(date_win)) \
                   .join(holiday_df, 'DATE', join_type='left').na.fill({'HOLIDAY':0}) \
                   .join(precip_df, 'DATE', 'inner') \
                   .na.drop() 

    return feature_df
