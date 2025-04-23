import sys
import os
# ------- Project Directory -------
PROJECT_DIR = '/Users/joshhager/josh/classes/csds397/finalProject/pokemonDataPipeline'

# needed so Python has access to this directory when running the dag from the ~airflow/dags folder
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(PROJECT_DIR, '.env'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from scripts.load_raw_data import load_raw_data
from scripts.data_cleaning import clean_data
from scripts.load_clean_data import load_clean_data
from scripts.data_profiling import pokemon_data_profile

# email configuration
SMTP_SERVER = "smtp.gmail.com"  
SMTP_PORT = 587
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")

def send_email(context):
    """Sends an email given a task context."""

    try:
        # create the email
        task_instance = context.get('task_instance')
        subject = f"Task {task_instance.task_id} failed in DAG {task_instance.dag_id}"
        body = f"The task failed. Here's the error details:\n\n{str(context)}"
        
        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = RECIPIENT_EMAIL
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # connect to the SMTP server and send the email
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()  # upgrade the connection to secure
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        server.quit()
        
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

# default settings for the dag
default_args = { 
    'owner': 'admin',
    'start_date': datetime(2025, 4, 23),
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'on_failure_callback': send_email,
}

# dag definition
dag = DAG(
    'pokemon_data_dag',
    default_args=default_args,
    description='A DAG to run a Pokemon data pipeline',
    schedule_interval='@daily'
)


# task definitions
load_raw_data_task = PythonOperator(
    task_id='load_raw_data_task', 
    python_callable=load_raw_data,
    retries=5,
    retry_delay=timedelta(seconds=10),
    dag=dag
)

clean_data_task = PythonOperator(
    task_id='clean_data_task', 
    python_callable=lambda: load_clean_data(clean_data()), 
    retries=5,
    retry_delay=timedelta(seconds=10),
    dag=dag
)

with open(os.path.join(PROJECT_DIR, 'scripts/1_average_bst_by_type1.sql')) as avg_bst_by_type1_query:
    avg_bst_by_type1_task = SQLExecuteQueryOperator(
        task_id='avg_bst_by_type1_task',
        sql=avg_bst_by_type1_query.read(),
        conn_id='postgres_pokemon_db',
        autocommit=True,
        dag=dag
    )

with open(os.path.join(PROJECT_DIR, 'scripts/2_average_bst_by_generation.sql')) as avg_bst_by_generation_query:
    avg_bst_by_generation_task = SQLExecuteQueryOperator(
        task_id='avg_bst_by_generation_task',
        sql=avg_bst_by_generation_query.read(),
        conn_id='postgres_pokemon_db',
        autocommit=True,
        dag=dag
    )

with open(os.path.join(PROJECT_DIR, 'scripts/3_pokemon_per_type1.sql')) as pokemon_per_type1_query:
    pokemon_per_type1_task = SQLExecuteQueryOperator(
        task_id='pokemon_per_type1_task',
        sql=pokemon_per_type1_query.read(),
        conn_id='postgres_pokemon_db',
        autocommit=True,
        dag=dag
    )

with open(os.path.join(PROJECT_DIR, 'scripts/4_pokemon_per_type2.sql')) as pokemon_per_type2_query:
    pokemon_per_type2_task = SQLExecuteQueryOperator(
        task_id='pokemon_per_type2_task',
        sql=pokemon_per_type2_query.read(),
        conn_id='postgres_pokemon_db',
        autocommit=True,
        dag=dag
    )

# set the task dependencies
load_raw_data_task >> clean_data_task
clean_data_task >> avg_bst_by_type1_task
clean_data_task >> avg_bst_by_generation_task
clean_data_task >> pokemon_per_type1_task
clean_data_task >> pokemon_per_type2_task
