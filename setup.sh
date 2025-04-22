# create python venv
python3.11 -m venv .venv
source ./.venv/bin/activate

# install required packages
pip install -r requirements.txt

# init airflow
airflow db init

# create an admin user for airflow
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email your-email@example.com

# disable example dags
sed -i '' 's/load_examples = True/load_examples =false/g' $HOME/airflow/airflow.cfg

# create a dag directory if it doesn't exist
AIRFLOW_DAG_PATH="$HOME/airflow/dags"
mkdir -p $AIRFLOW_DAG_PATH

# copy the dag file to the dag directory
cp employee_data_dag.py ~/airflow/dags

# create the needed database in postgres
python3 create_database_setup.py

# create a .env file
touch .env
