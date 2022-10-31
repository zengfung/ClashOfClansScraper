import logging
import coc
import pandas as pd
import scraper.database.common
from datetime import datetime
from typing import Dict
from pathlib import Path

FILENAME = "goldpass.csv"
LOGGER = logging.getLogger(__name__)

async def update_goldpass_season_table(client:coc.Client, dir:str):

    LOGGER.debug('Scraping Gold Pass data.')
    season_id = datetime.now().strftime('%Y-%m') 
    goldpass_info = await client.get_current_goldpass_season()

    LOGGER.debug('Creating local Gold Pass table.')
    df = create_dataframe(season_id, goldpass_info)

    LOGGER.debug(f'Creating or appending data to {dir}/{FILENAME}')
    scraper.database.common.create_or_append_table(df, dir, FILENAME)

def create_dataframe(season_id:str, info):
    LOGGER.debug(f'Creating Gold Pass data frame with 1 row of data...')
    df = pd.DataFrame({
        'seasonId': [season_id],
        'startTime': [info.start_time],
        'endTime': [info.end_time],
        'duration': [info.duration]
    })
    return df
    
