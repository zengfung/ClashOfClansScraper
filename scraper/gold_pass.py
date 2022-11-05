import logging
import coc
import pandas as pd
import scraper.common
from datetime import datetime

FILENAME = "goldpass.csv"
LOGGER = logging.getLogger(__name__)

async def update_goldpass_season_table(client:coc.Client, dir:str) -> None:

    LOGGER.debug('Scraping Gold Pass data.')
    season_id = datetime.now().strftime('%Y-%m') 
    goldpass_info = await client.get_current_goldpass_season()

    LOGGER.debug('Creating local Gold Pass table.')
    df = create_goldpass_dataframe(season_id, goldpass_info)

    LOGGER.debug(f'Creating or appending data to {dir}/{FILENAME} if needed.')
    scraper.common.create_or_append_table_if_needed(df, dir, FILENAME)

def create_goldpass_dataframe(season_id:str, info:coc) -> pd.DataFrame:
    LOGGER.debug(f'Creating Gold Pass data frame with 1 row of data.')
    df = pd.DataFrame({
        'seasonId': [season_id],
        'startTime': [info.start_time.time],
        'endTime': [info.end_time.time],
        'duration': [info.duration]
    })
    return df
    
