## Test Databricks App - NYC Taxis

### Deploy a Databricks App 
+ Streamlit  
+ Databricks SQL   
+ OAUTH M2M Authentication  

### Setup:  
The following env vars need to be set:  
+ `DATABRICKS_SERVER_HOSTNAME`  - the Databricks workspace URL, example: `dbc-xxxx.cloud.databricks.com`
+ `DATABRICKS_CLIENT_ID`  - The OAUTH client ID
+ `DATABRICKS_CLIENT_SECRET`  - The OAUTH secret
+ `DATABRICKS_HTTP_PATH` - The SQL warehouse URL, example: `/sql/1.0/warehouses/xxxx` 

#### Configuring the SQL Warehouse and Access

`DATABRICKS_HTTP_PATH` must be custom configured in the app.yaml file to match the SQL Warehouse that will be used.   

- Grant access to the App to use the SQL Warehouse.  
- Edit, click "Next" to get to App resources, add SQL Warehouse 
- select the SQL warehouse that is already  configured in the YAML file

### Deploy to Databricks Compute:  

1. Clone your repo to the Workspace:  
*Databricks -> Workspace -> "Create" -> Git Folder* 

2. Create an App:  
*Databricks -> Compute -> Apps tab -> "Create app" -> "Create a custom app"*

3. From the main Overview page for the App (Compute -> Apps -> App Name):  
Select "Deploy"   
Select the path to the cloned repo, and deploy it  

### Push development changes to Databricks App

Changes being developed on a laptop IDE can be syncronised by commiting to a local git repo, pushing to the central shared repo, then pulling into the Databricks workspace repo from the central shared repo.

1. Commit changes  
2. `git push origin main` / `git push origin <branch>`  
3. Go to the Workspace git repo and pull new changes on the branch being used.
4. Go to the app and re-deploy it


### Running Locally for Development and Testing

1. export the environment variables listed in the setup instrucions:

`export DATABRICKS_SERVER_HOSTNAME=<myworkspace>.cloud.databricks.com` (note NO `https` prefix or trailing `/`)  
`export DATABRICKS_CLIENT_ID=########-####-####-####-############`   
`export DATABRICKS_CLIENT_SECRET=********************************`  
`export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/################`  

2. Run the app:  
`streamlit run app.py`

3. Go to the browser:   
`http://localhost:8501/`  

