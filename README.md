## Citibike Machine Learning Orchestration with Astronomer and Snowpark Python  

### Requirements:  
- You need a version of Docker running locally.
- Install the [Astro CLI](https://github.com/astronomer/astro-cli)
- Clone this repository to your local machine and `cd` into that folder. 
- Add a `creds.json` file under the `include/` folder with the following details. You can copy and modify the `creds.template.json` file that is part of this repo.
```
{
    "username": "<username>",
    "password": "<password>",
    "warehouse": "<warehouse_name>",
    "account": "<account_name",
    "role": "<role_name>",
    "database":"<DATABASE_NAME>",
    "schema":"<SCHEMA_NAME>"
}
```
- From the repo folder, run:  
`astro dev start`  
- This will launch the astro managed airflow instance which you can browse to on [http://localhost:8080/](http://localhost:8080/) and a Jupyter Notebook instance which you browse to on [http://localhost:8888/](http://localhost:8888/)