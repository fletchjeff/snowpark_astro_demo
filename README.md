## Citibike Machine Learning Hands-on-Lab with Snowpark Python  

### Requirements:  
-Install [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/macos.html)  
-Activate your Snowflake account for [Snowpark Private Preview access](https://docs.google.com/forms/d/e/1FAIpQLSf0-ns-PQxjYJLCeSDybZyZeGtcab7NNNdU27IssZdRTEJ3Tg/viewform)  
-Complete the Snowflake SE [Python Training](https://snowflake.udemy.com/learning-paths/2001710/) and/or the Test-Out Option at the bottom.  
  
  
!!! NOTE if you have an existing conda environment named `snowpark_030` it will be over-written.
  

### Setup Steps:
```bash
git clone https://github.com/sfc-gh-mgregory/citibike-ml-tko-HOL
cd citibike-ml-tko-HOL
conda env create -f ./conda-env.yml  --force
conda activate snowpark_030
jupyter notebook
```
  
In Jupyter file browser open `creds.json` and update with your Snowflake account credentials.
