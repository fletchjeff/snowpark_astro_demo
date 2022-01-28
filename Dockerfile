FROM fletchjeff/ap-airflow:2.2.3-2-onbuild-py38
COPY include/snowflake_snowpark_python-0.3.0-py3-none-any.whl snowflake_snowpark_python-0.3.0-py3-none-any.whl
RUN pip install snowflake_snowpark_python-0.3.0-py3-none-any.whl[pandas]
COPY requirements.txt requirements.txt  
RUN pip install -r requirements.txt