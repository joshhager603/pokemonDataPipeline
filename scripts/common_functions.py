from scripts.constants import *
from sqlalchemy.engine import Engine
import sqlalchemy


def create_sqlalchemy_engine(postgres_user=POSTGRES_USER, 
                             postgres_pass=POSTGRES_PASS, 
                             postgres_host=POSTGRES_HOST, 
                             postgres_port=POSTGRES_PORT, 
                             db_name=DB_NAME) -> Engine:
    return sqlalchemy.create_engine(
        f'postgresql://{postgres_user}:{postgres_pass}@{postgres_host}:{postgres_port}/{db_name}')