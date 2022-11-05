import logging
import coc
import pandas as pd
import scraper.common
from datetime import datetime

LOGGER = logging.getLogger(__name__)

# def get_all_hero_data()

# def get_hero_data()
def get_hero_data(client:coc.Client, name:str) -> pd.DataFrame:
    LOGGER.debug(f'Getting {name} Hero uninitialized object...')
    hero = client.get_hero(name)
    print(create_hero_dataframe(hero))

def create_hero_dataframe(info:coc) -> pd.DataFrame:
    LOGGER.debug(f'Creating {info.name} data frame...')
    df = pd.DataFrame({
        'id': info.id,
        'name': info.name,
        'range': info.range,
        'dps': info.dps,
        'hitpoints': info.hitpoints,
        'ground_target': info.ground_target,
        'speed': info.speed,
        'upgrade_cost': info.upgrade_cost,
        'upgrade_resource': info.upgrade_resource,
        'upgrade_time': info.upgrade_time
    })
    return df