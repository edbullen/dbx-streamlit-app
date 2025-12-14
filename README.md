## Test Databricks App - NYC Taxis

![App Screenshot](./doc/apps_screenshot.png "screenshot") 


### Deploy a Databricks App 
+ Streamlit  
+ Databricks SQL   
+ OAUTH M2M Authentication  

### Setup:  
The following environment vars need to be set in app.yaml for when the app is deployed to Databricks workspace compute:  
+ `DATABRICKS_SERVER_HOSTNAME`  - the Databricks workspace URL, example: `dbc-xxxx.cloud.databricks.com`
+ `DATABRICKS_HTTP_PATH` - The SQL warehouse URL, example: `/sql/1.0/warehouses/xxxx` 

There is no need to set up a service principle for connecting the to the warehouse (this is only needed for local dev environment testing).  See the next section for granting access on the warehouse.

The command to run the app is set in the app.yaml file:

+ `command: ["streamlit", "run", "app.py"]`   

#### Configuring the SQL Warehouse and Access

`DATABRICKS_HTTP_PATH` must be custom configured in the `app.yaml` file to match the SQL Warehouse that will be used.   

- Grant access to the App to use the SQL Warehouse.  
- Edit, click "Next" to get to App resources, add SQL Warehouse 
- select the SQL warehouse that is already  configured in the YAML file

### Running Locally for Development and Testing

1. Setup the local environment variables in the `.env` file, to be picked up by `load_dotenv()`:

```
# Workspace Web address and path to SQL Warehouse
DATABRICKS_SERVER_HOSTNAME=<myworkspace>.cloud.databricks.com  # No `https` prefix or trailing `/`  
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/################

# Service Principal for SQL Warehouse
DATABRICKS_ACCOUNT_ID=########-####-####-####-############
DATABRICKS_CLIENT_ID=########-####-####-####-############
DATABRICKS_CLIENT_SECRET=####################################

LOCAL_DEV_EMAIL=dummy@email.com
```

- Make sure the .env file is not committed in the source code repo

2. Run the app:  
`streamlit run app.py`

3. Go to the browser:   
`http://localhost:8501/`  

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


![Web GUI Deploy](./doc/apps_git_deploy_screenshot.png "deploying via the web gui") 



