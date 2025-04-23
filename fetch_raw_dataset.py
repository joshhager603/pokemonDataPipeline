import pandas as pd
from scripts.constants import *
import kagglehub
from kagglehub import KaggleDatasetAdapter

def fetch_data():
    print('Fetching dataset from Kaggle...')
    df: pd.DataFrame        
    df = kagglehub.load_dataset(KaggleDatasetAdapter.PANDAS, "rounakbanik/pokemon", "pokemon.csv")
    print('Dataset has been fetched from Kaggle.')
    df.to_csv(RAW_DATA_FILEPATH)
    print(f"Raw data has been written to {RAW_DATA_FILEPATH}")

if __name__ == '__main__':
    fetch_data()