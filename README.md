# CSDS 397 Final Project: Pokemon Data Pipeline
**Josh Hager (jrh236)**

## Description
This repository holds the code to run a data pipeline on a set of Pokemon data. The pipeline includes ingestion, cleaning, and transformation. Data is stored in a PostgreSQL database, and tasks are orchestrated using Airflow.

## Prerequisites
To run the code in this repository, you must:

1. Have an installation of **Python 3.11** on your machine (https://www.python.org/downloads/).
2. Have an installation of PostgreSQL on your machine. If you don't, you can run `brew install postgresql` on MacOS to install using Homebrew. Otherwise, see https://www.postgresql.org/download/.
3. Have PostgreSQL started on your machine. On MacOS, you can run `brew services start postgresql`.

## Instructions
1. Set up a [Kaggle API token](https://www.kaggle.com/docs/api#:~:text=is%20%24PYTHON_HOME/Scripts.-,Authentication,-In%20order%20to), and save the `kaggle.json` file to `~/.kaggle/kaggle.json`. This will be used to fetch the dataset.
2. Clone this git repo to your machine using: 
   
    ```
    git clone https://github.com/joshhager603/pokemonDataPipeline.git
    ```
3. In a terminal, `cd` into the repo you just cloned.
4. In the file `scripts/constants.py`, change the `POSTGRES_USER` and `POSTGRES_PASS` variables to the username and password you use for PostgreSQL on this machine. You may also need to change `POSTGRES_HOST` and `POSTGRES_PORT` if you have changed these from the default.
5. In the file `pokemon_data_dag.py`, replace the `PROJECT_DIR` variable with the absolute path to this repo on your machine. You can use the `pwd` command to find this.
6. Run the setup script using `source ./setup.sh`. This will perform basic setup for the project, like creating a Python virtual environment and moving files to the required locations to be used by Airflow.
7. Create a PostgreSQL connection to Airflow using the following command. Replace `<YOUR-POSTGRES-USERNAME>` and `<YOUR-POSTGRES-PASSWORD>` with the username and password you use for PostgreSQL on this machine. You may also need to change the host and port parameters if these have been changed from the default.
   
   ```bash
   airflow connections add 'postgres_pokemon_db' \
    --conn-type 'postgres' \
    --conn-host '127.0.0.1' \
    --conn-login '<YOUR-POSTGRES-USERNAME>' \
    --conn-password '<YOUR-POSTGRES-PASSWORD>' \
    --conn-schema 'pokemon_db' \
    --conn-port 5432
    ```
8. The `setup.sh` script should have created a `.env` file in the root directory of the project. Add the following lines to the `.env` file to configure email notifications of failed tasks using Gmail. Replace `<YOUR-GMAIL-ADDRESS>` with the Gmail address you would like the notifications to be sent from. Replace `<YOUR-GMAIL-APP-PASSWORD>` with an app password generated from http://myaccount.google.com/apppasswords (2FA must be enabled for your Gmail account). Replace `<EMAIL-RECIPIENT-ADDRESS>` with the email address you would like the notifications sent to.
   
   ```
   SENDER_EMAIL="<YOUR-GMAIL-ADDRESS>"
   SENDER_PASSWORD="<YOUR-GMAIL-APP-PASSWORD>"
   RECIPIENT_EMAIL="<EMAIL-RECIPIENT-ADDRESS>"
   ```
9. Run the `airflow scheduler` command to start the Airflow scheduler.
10. In a different terminal, run the `airflow webserver` command to start the Airflow webserver UI.
11. In a browser, navigate to localhost:8080 to view DAG status through the Airflow UI. For this project, the default username and password are "admin" and "admin".


