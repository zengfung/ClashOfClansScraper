import logging
import coc
import numpy as np
import pandas as pd
import scraper.common
from typing import Callable, List
from pathlib import Path

LOGGER = logging.getLogger(__name__)

HEROES_FILENAME = "heroes.csv"
PETS_FILENAME = "pets.csv"
TROOPS_FILENAME = "troops.csv"
SPELLS_FILENAME = "spells.csv"

class AttackItemSetup:
    def __init__(self, item_type:str) -> None:
        assert self.is_valid_item(item_type), f'Invalid item_type {item_type}!'

        self.item_type = item_type
        self.item_list = self.get_item_list(item_type)
        self.func = self.get_func(item_type)
        self.output_filename = self.get_output_filename(item_type)

    def is_valid_item(self, item_type:str) -> bool:
        if (item_type == "hero" or \
            item_type == "pet" or \
            item_type == "troop" or \
            item_type == "super_troop" or \
            item_type == "siege_machine" or \
            item_type == "home_troop" or \
            item_type == "builder_troop" or \
            item_type == "spell" or \
            item_type == "elixir_spell" or \
            item_type == "dark_elixir_spell"):
            return True

        return False

    def get_item_list(self, item_type:str) -> List[str]:
        if item_type == "hero":
            return coc.HERO_ORDER
        elif item_type == "pet":
            return coc.HERO_PETS_ORDER
        elif item_type == "troop":
            return coc.HOME_TROOP_ORDER + coc.BUILDER_TROOPS_ORDER
        elif item_type == "siege_machine":
            return coc.SIEGE_MACHINE_ORDER         
        elif item_type == "super_troop":
            return coc.SUPER_TROOP_ORDER         
        elif item_type == "home_troop":
            return coc.HOME_TROOP_ORDER
        elif item_type == "builder_troop":
            return coc.BUILDER_TROOPS_ORDER
        elif item_type == "spell":
            return coc.SPELL_ORDER
        elif item_type == "elixir_spell":
            return coc.ELIXIR_SPELL_ORDER
        elif item_type == "dark_elixir_spell":
            return coc.DARK_ELIXIR_SPELL_ORDER
        else:
            LOGGER.error(f'No available list for item type {item_type}!')
    
    def get_func(self, item_type:str) -> Callable[[coc.Client,str],pd.DataFrame]:
        if item_type == "hero":
            return get_hero_data
        elif item_type == "pet":
            return get_pet_data
        elif (item_type == "troop" or \
              item_type == "elixir_troop" or \
              item_type == "dark_elixir_troop" or \
              item_type == "siege_machine" or \
              item_type == "super_troop" or \
              item_type == "home_troop" or \
              item_type == "builder_troop"):
            return get_troop_data
        elif (item_type == "spell" or \
              item_type == "elixir_spell" or \
              item_type == "dark_elixir_spell"):
              return get_spell_data
        else:
            LOGGER.error(f'No available function to collect data for {item_type}!')

    def get_output_filename(self, item_type:str) -> str:
        if item_type == "hero":
            return HEROES_FILENAME
        elif item_type == "pet":
            return PETS_FILENAME
        elif (item_type == "troop" or \
              item_type == "elixir_troop" or \
              item_type == "dark_elixir_troop" or \
              item_type == "siege_machine" or \
              item_type == "super_troop" or \
              item_type == "home_troop" or \
              item_type == "builder_troop"):
            return TROOPS_FILENAME
        elif (item_type == "spell" or \
              item_type == "elixir_spell" or \
              item_type == "dark_elixir_spell"):
              return SPELLS_FILENAME
        else:
            LOGGER.error(f'No available file to write {item_type} output!')

def create_dataframe_for_ingame_data(client:coc.Client, item_type:str, dir:str) -> None:
    LOGGER.debug(f'Setup for collecting data for all {item_type}.')
    setup = AttackItemSetup(item_type)

    LOGGER.debug(f'Calling {setup.func}.')
    df = get_all_data(client, setup)

    LOGGER.debug(f'Creating or appending data to {dir}/{setup.output_filename} if needed.')
    scraper.common.create_or_append_table_if_needed(df, dir, setup.output_filename)

def get_all_data(client:coc.Client, setup:AttackItemSetup) -> pd.DataFrame:
    LOGGER.debug(f'Getting all Hero uninitialized objects.')

    results = list()
    for item in setup.item_list:
        LOGGER.debug(f'Getting {item} data.')
        result = setup.func(client, item)
        results.append(result)
    
    LOGGER.debug(f'Concatenating all data into 1 dataframe.')
    return pd.concat(results, ignore_index=True)

# =========
# HERO DATA
# =========
def get_hero_data(client:coc.Client, name:str) -> pd.DataFrame:
    LOGGER.debug(f'Getting {name} Hero uninitialized object.')
    hero = client.get_hero(name)
    return create_hero_dataframe(hero)

def create_hero_dataframe(info:coc) -> pd.DataFrame:
    LOGGER.debug(f'Creating {info.name} data frame.')

    df = pd.DataFrame({
        'id': info.id,
        'name': info.name,
        'range': info.range,
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

# ========
# PET DATA
# ========
def get_pet_data(client:coc.Client, name:str) -> pd.DataFrame:
    LOGGER.debug(f'Getting {name} Pet uninitialized object.')
    pet = client.get_pet(name)
    return create_pet_dataframe(pet)

def create_pet_dataframe(info:coc) -> pd.DataFrame:
    LOGGER.debug(f'Creating {info.name} data frame.')

    df = pd.DataFrame({
        'id': info.id,
        'name': info.name,
        'range': info.range,
        'dps': info.dps,
        'ground_target': info.ground_target,
        'hitpoints': info.hitpoints,
        'speed': info.speed,
        'upgrade_cost': info.upgrade_cost,
        'upgrade_resource': info.upgrade_resource.name,
        'upgrade_time': scraper.common.convert_time_to_seconds(info.upgrade_time),
        'level': info.level
    })
    return df

# ========
# TROOP DATA
# ========
def get_troop_data(client:coc.Client, name:str) -> pd.DataFrame:
    LOGGER.debug(f'Getting {name} Troop uninitialized object.')
    troop = client.get_troop(name)
    return create_troop_dataframe(troop)

def create_troop_dataframe(info:coc) -> pd.DataFrame:
    LOGGER.debug(f'Creating {info.name} data frame.')

    # TODO: Figure out a better way to deal with inconsistent list length
    df_len = len(info.level) if info.level is not None else 1
    df = pd.DataFrame({
        'id': info.id,
        'name': info.name,
        'range': info.range,
        'dps': info.dps,
        'ground_target': info.ground_target,
        'hitpoints': info.hitpoints,
        'speed': info.speed,
        'upgrade_cost': info.upgrade_cost if info.upgrade_cost is not None else np.nan,
        'upgrade_resource': info.upgrade_resource.name,
        'upgrade_time': scraper.common.convert_time_to_seconds(info.upgrade_time) if len(info.upgrade_time) > 0 else np.nan,
        'training_time': info.training_time,
        'is_elixir_troop': info.is_elixir_troop,
        'is_dark_troop': info.is_dark_troop,
        'is_siege_machine': info.is_siege_machine,
        'is_super_troop': info.is_super_troop,
        'cooldown': scraper.common.convert_time_to_seconds(info.cooldown) * df_len if hasattr(info, 'cooldown') else np.nan,
        'duration': scraper.common.convert_time_to_seconds(info.duration) * df_len if hasattr(info, 'duration') else np.nan,
        'min_original_level': info.min_original_level if hasattr(info, 'min_original_level') else np.nan,
        'original_troop_id': info.original_troop.id if hasattr(info, 'original_troop') else np.nan,
        'level': info.level
    })
    return df

# ========
# SPELL DATA
# ========
def get_spell_data(client:coc.Client, name:str) -> pd.DataFrame:
    LOGGER.debug(f'Getting {name} Spell uninitialized object.')
    spell = client.get_spell(name)
    return create_spell_dataframe(spell)

def create_spell_dataframe(info:coc) -> pd.DataFrame:
    LOGGER.debug(f'Creating {info.name} data frame.')

    df = pd.DataFrame({
        'id': info.id,
        'name': info.name,
        'range': info.range,
        'dps': info.dps,
        'upgrade_cost': info.upgrade_cost,
        'upgrade_resource': info.upgrade_resource.name,
        'upgrade_time': scraper.common.convert_time_to_seconds(info.upgrade_time) if len(info.upgrade_time) > 0 else np.nan,
        'training_time': info.training_time,
        'is_elixir_spell': info.is_elixir_spell,
        'is_dark_spell': info.is_dark_spell,
        'level': info.level
    })
    return df