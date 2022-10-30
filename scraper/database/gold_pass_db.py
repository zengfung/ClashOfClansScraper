import pandas as pd
from datetime import datetime
from scraper.api import GoldPass
from typing import Dict
from pathlib import Path

FILENAME = "goldpass.csv"

def update_goldpass_season_table(token:str, dir:str):
    season_id = datetime.now().strftime('%Y-%m') 
    goldpass_info = GoldPass(token).get_current_season()
    df = create_dataframe(season_id, goldpass_info)
    create_or_append_table(df, dir)

def create_dataframe(season_id:str, info:Dict):
    df = pd.DataFrame({
        'seasonId': [season_id],
        'startTime': [info['startTime']],
        'endTime': [info['endTime']]
    })
    return df
    
def create_or_append_table(df:pd.DataFrame, dir:str):
    path = Path(dir + "/" + FILENAME)
    if path.is_file():
        df.to_csv(path, mode='a', index=False, header=False)
    else:
        df.to_csv(path, mode='w', index=False, header=True)