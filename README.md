## Test Databricks App - NYC Taxis

Deploy a Databricks App 
+ Streamlit  
+ Databricks SQL   
+ OAUTH M2M Authentication  

Setup:  
The following env vars need to be set:  
+ `DATABRICKS_HOST`  - the Databricks workspace URL
+ `DATABRICKS_CLIENT_ID`  - The OAUTH client ID
+ `DATABRICKS_CLIENT_SECRET`  - The OAUTH secret
+ `DATABRICKS_HTTP_PATH` - The SQL warehouse URL


Deploy:  



Notes to tidy up

1. Clone your repo to the Workspace
Databricks -> Workspace -> "Create" -> Git Folder 

2. Create an App
Databricks -> Compute -> Apps tab -> "Create app" -> "Create a custom app"


3. From the main Overview page for the app (Compute -> Apps -> App Name
"Deploy"
Select the path to the cloned repo, and depoy



The URL for the deployed app is shown with a green "Running" icon if it has deployed and started.

Environment vars for 
DATABRICKS_CLIENT_ID
DATABRICKS_CLIENT_SECRET
DATABRICKS_HOST

Get set automatically

However, need to manually set the 
DATABRICKS_HTTP_PATH

This is done in the app.yaml file
The env: secti is used to configure environment variables, following this format. Eg
env:
  - name: WAREHOUSE_ID
    valueFrom: sql_warehouse


Find the URL for the SQL warehouse to be used by the app and configure it in the the YAML file, commit it and push to the repo local to where the app is running  in the workspace.

env:
- name: 'DATABRICKS_HTTP_PATH'
  value: '/sql/1.0/warehouses/75fd8278393d07eb'

command: ["streamlit", "run", "app.py"]


Grant access to the App to use the SQL Warehouse.
Edit , click "Next" to get to App resurces, add SQL Warehouse and select the SQL warehouse that is configured in the YAML file