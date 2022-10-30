import logging
import pandas as pd
import scraper.database.common
from datetime import datetime
from scraper.api import GoldPass
from typing import Dict
from pathlib import Path

FILENAME = "goldpass.csv"
LOGGER = logging.getLogger(__name__)

def update_goldpass_season_table(token:str, dir:str):

    LOGGER.debug('Scraping Gold Pass data.')
    season_id = datetime.now().strftime('%Y-%m') 
    goldpass_info = GoldPass(token).get_current_season()

    LOGGER.debug('Creating local Gold Pass table.')
    df = create_dataframe(season_id, goldpass_info)

    LOGGER.debug(f'Creating or appending data to {dir}/{FILENAME}')
    scraper.database.common.create_or_append_table(df, dir, FILENAME)

def create_dataframe(season_id:str, info:Dict):
    LOGGER.debug(f'Creating Gold Pass data frame with 1 row of data...')
    df = pd.DataFrame({
        'seasonId': [season_id],
        'startTime': [info['startTime']],
        'endTime': [info['endTime']]
    })
    return df
    
