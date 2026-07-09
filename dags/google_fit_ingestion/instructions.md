# About the Google Fitness Ingestion Project
To prove my family wrong, who always claim I sit the whole day and walk only 800 steps a day. I decided to over-engineer my watch report dashboard with Airflow and Superset (In Future seprate video). Jokes apart but, we are creating this project to cover majority of core conccepts in Apache airflow with a simple project. 
We are covering following topics with this project which will help you crack interview.
- **DAGs**
- **Operators**
- **PythonOperator / `@task` (TaskFlow API)**
- **Hooks + Connections**
- **Scheduling (cron + catchup)**
- **Executors**
- **XComs**
- **Plugins**
- **Task dependencies**
- **Context (`**context`)**

# Project completion steps
Make sure your airflow is runing if you do not have it refer https://github.com/shantanukhond/learn-airflow. This will help you quickstart airflow. Inside the same folder clone this repo into `airflow-code/` folder using

```bash
git clone git@github.com:shantanukhond/learn-airflow-dags.git airflow-code
```

Now you should see all the dags in this repo into your airflow

# Enable GCP Health API and Get API key
- Create a GCP account and Go to https://console.cloud.google.com/marketplace/product/google/health.googleapis.com and make sure this api is enabled

- Once it is enabled go to https://console.cloud.google.com/apis/credentials and Create OAuth Client In Application Type Select Desktop App and name it appropriatly like `airflow-fitness-app`. Save the json as client_secret.json in root of airflow-code. We will generate token using this.

- Create venv to run `get_token.py` script using following command
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    python scripts/get_token.py
    ```
    This will redirect to google login page for the app you created and then you sign in there with yout google account. once you click it it will create `token.json` file. It will have all the information we need to create airflow connection.

# Its Airflow time!
Now all our ingrediants for receipe are ready now we can jump to airflow topics
## Connection
    We need to create two connections
    1. One using this `token.json` to pull data connection_id `google_fit_api` connection type `Generic` and ignore all fields just put Extra Json feild in following
    ```
    {
        "token": "***"
    }
    ```
    2. Another to push data into database. We are using postgresql to do that 
        Select postgresql and put your creds there



 




