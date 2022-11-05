import logging
from typing import Callable
import coc
import numpy as np
import pandas as pd
import scraper.common
from pathlib import Path

LOGGER = logging.getLogger(__name__)
HEROES_FILENAME = "heroes.csv"

def get_output_filename(func:Callable[[coc.Client],pd.DataFrame]) -> str:
    if func == get_all_hero_data:
        return HEROES_FILENAME
    else:
        LOGGER.error(f'No available file to write {func} output!')

def create_dataframe_for_ingame_data(client:coc.Client, func:Callable[[coc.Client],pd.DataFrame], dir:str) -> None:
    LOGGER.debug(f'Calling {func}...')
    df = func(client)

    filename = get_output_filename(func)
    LOGGER.debug(f'Creating or appending data to {dir}/{filename} if needed.')
    scraper.common.create_or_append_table_if_needed(df, dir, filename)

def get_all_hero_data(client:coc.Client) -> pd.DataFrame:
    LOGGER.debug(f'Getting all Hero uninitialized objects.')

    results = list()
    for hero in coc.HERO_ORDER:
        LOGGER.debug(f'Getting {hero} data...')
        result = get_hero_data(client, hero)
        results.append(result)
    
    LOGGER.debug(f'Concatenating all heroes data into 1 dataframe.')
    return pd.concat(results, ignore_index=True)

def get_hero_data(client:coc.Client, name:str) -> pd.DataFrame:
    LOGGER.debug(f'Getting {name} Hero uninitialized object.')
    hero = client.get_hero(name)
    return create_hero_dataframe(hero)

def create_hero_dataframe(info:coc) -> pd.DataFrame:
    LOGGER.debug(f'Creating {info.name} data frame.')

    df = pd.DataFrame({
        'id': info.id if info.id is not None else np.nan,
        'name': info.name if info.name is not None else np.nan,
        'range': info.range if info.range is not None else np.nan,
        'dps': info.dps,
        'hitpoints': info.hitpoints,
        'ground_target': info.ground_target,
        'speed': info.speed,
        'upgrade_cost': info.upgrade_cost,
        'upgrade_resource': info.upgrade_resource.name,
        'upgrade_time': scraper.common.convert_time_to_seconds(info.upgrade_time),
        'ability_time': info.ability_time,
        'required_th_level': info.required_th_level,
        'regeneration_time': scraper.common.convert_time_to_seconds(info.regeneration_time) if len(info.regeneration_time) > 0 else 0,
        'level': info.level
    })
    return df