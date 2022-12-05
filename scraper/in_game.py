import logging
import coc
import datetime

from typing import List
from typing import Callable
from typing import Dict
from typing import Generator
from typing import Union
from scraper import CONFIG
from scraper.storage import StorageHandler


LOGGER = logging.getLogger(__name__)

class TroopTableHandler(StorageHandler):

    configs = CONFIG['TroopSettings']
    table = configs['TableName']
    categories = configs['Categories']
    scrape_enabled = configs['ScrapeEnabled']
    null_id_scrape_enabled = configs['NullIdScrapeEnabled']

    def __init__(self, coc_client:coc.Client, **kwargs) -> None:
        super().__init__(self.table, **kwargs)
        self.coc_client = coc_client
        self.categories = [category for category in self.categories if self.__is_valid_category__(category)]

    def __is_valid_category__(self, category:str) -> bool:
        match category:
            case "hero" | \
                 "pet" | \
                 "troop" | \
                 "super_troop" | \
                 "siege_machine" | \
                 "home_troop" | \
                 "builder_troop" | \
                 "spell" | \
                 "elixir_spell" | \
                 "dark_elixir_spell":
                return True
            case _:
                LOGGER.debug(f'{category} is not a valid category.')
                return False

    def __get_item_list__(self, category:str) -> List[str]:
        match category:
            case "hero":
                return coc.HERO_ORDER
            case "pet":
                return coc.HERO_PETS_ORDER
            case "troop":
                return coc.HOME_TROOP_ORDER + coc.BUILDER_TROOPS_ORDER
            case "siege_machine":
                return coc.SIEGE_MACHINE_ORDER         
            case "super_troop":
                return coc.SUPER_TROOP_ORDER         
            case "home_troop":
                return coc.HOME_TROOP_ORDER
            case "builder_troop":
                return coc.BUILDER_TROOPS_ORDER
            case "spell":
                return coc.SPELL_ORDER
            case "elixir_spell":
                return coc.ELIXIR_SPELL_ORDER
            case "dark_elixir_spell":
                return coc.DARK_ELIXIR_SPELL_ORDER
            case _:
                LOGGER.error(f'No available list for item type {category}!')

    def __get_entity_count__(self, data:coc.abc.DataContainer) -> int:
        attrs = dir(data)
        count = 0
        for attr in attrs:
            a = getattr(data, attr)
            if isinstance(a, list):
                count = max(count, len(a))
        return count

    def __try_get_attr__(self, data:coc.abc.DataContainer, attr:str, index:int = None) -> Union[float,int,str]:
        out = getattr(data, attr, None)
        if out is not None and index is not None:
            try:
                return out[index]
            except IndexError as ex:
                LOGGER.warning(f'IndexError obtained at {attr}.')
                LOGGER.warning(str(ex))
                return None
        return out

    def __convert_data_to_entity_list__(self, data:coc.abc.DataContainer) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        LOGGER.debug(f'Creating entity for {self.__try_get_attr__(data, "name")}.')
        for i in range(self.__get_entity_count__(data)):
            entity = dict()
            # Mandatory keys
            entity['PartitionKey'] = f'{self.__try_get_attr__(data, "id")}_{self.__try_get_attr__(data, "level", i+1)}'
            entity['RowKey'] = f'{datetime.datetime.now().strftime("%Y-%m")}'

            # Identity keys
            entity['SeasonId'] = datetime.datetime.now().strftime('%Y-%m')
            entity['Id'] = self.__try_get_attr__(data, "id")
            entity['Name'] = self.__try_get_attr__(data, "name")

            # Details
            lab_level = self.__try_get_attr__(data, "lab_level", i+1)
            townhall_level = self.__try_get_attr__(data, "lab_to_townhall", lab_level) if lab_level is not None else None
            entity['Range'] = self.__try_get_attr__(data, "range", i+1)
            entity['Dps'] = self.__try_get_attr__(data, "dps", i+1)
            entity['GroundTarget'] = self.__try_get_attr__(data, "ground_target")
            entity['Hitpoints'] = self.__try_get_attr__(data, "hitpoints", i+1)
            entity['HousingSpace'] = self.__try_get_attr__(data, "housing_space")
            entity['LabLevel'] = self.__try_get_attr__(data, "lab_level", i+1)
            entity['TownhallLevel'] = townhall_level
            entity['Speed'] = self.__try_get_attr__(data, "speed", i+1)
            entity['Level'] = self.__try_get_attr__(data, "level", i+1)
            entity['UpgradeCost'] = self.__try_get_attr__(data, "upgrade_cost", i+1)
            entity['UpgradeResource'] = self.__try_get_attr__(data, "upgrade_resource").name
            entity['UpgradeTime'] = self.__try_get_attr__(data, "upgrade_time", i+1).total_seconds()
            entity['IsHomeVillage'] = self.__try_get_attr__(data, "_is_home_village")

            # Spells and troops
            cooldown = self.__try_get_attr__(data, "cooldown", i+1)
            duration = self.__try_get_attr__(data, "duration", i+1)
            entity['TrainingCost'] = self.__try_get_attr__(data, "training_cost", i+1)
            entity['TrainingTime'] = self.__try_get_attr__(data, "training_time", i+1)
            entity['IsElixirSpell'] = self.__try_get_attr__(data, "is_elixir_spell")
            entity['IsDarkSpell'] = self.__try_get_attr__(data, "is_dark_spell")
            entity['IsElixirTroop'] = self.__try_get_attr__(data, "is_elixir_troop")
            entity['IsDarkTroop'] = self.__try_get_attr__(data, "is_dark_troop")
            entity['IsSiegeMachine'] = self.__try_get_attr__(data, "is_siege_machine")
            entity['IsSuperTroop'] = self.__try_get_attr__(data, "is_super_troop")
            entity['Cooldown'] = cooldown.total_seconds() if cooldown is not None else None
            entity['Duration'] = duration.total_seconds() if duration is not None else None
            entity['MinOriginalLevel'] = self.__try_get_attr__(data, "min_original_level")
            entity['OriginalTroopId'] = self.__try_get_attr__(data, "original_troop_id")

            # Heroes and pets
            regeneration_time = self.__try_get_attr__(data, "regeneration_time", i+1)
            entity['AbilityTime'] = self.__try_get_attr__(data, "ability_time", i+1)
            entity['AbilityTroopCount'] = self.__try_get_attr__(data, "ability_troop_count", i+1)
            entity['RequiredTownhallLevel'] = self.__try_get_attr__(data, "required_th_level", i+1)
            entity['RegenerationTime'] = regeneration_time.total_seconds() if regeneration_time is not None else None

            yield entity

    def __get_item_data__(self, func:Callable[str,coc.abc.DataContainer], category:str) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        items = self.__get_item_list__(category)
        for item in items:
            data = func(item)
            if (data is not None and data.id is not None and data.level is not None) or \
                self.null_id_scrape_enabled:
                try:
                    LOGGER.debug(f'Scraping {item} data from {category} category.')
                    yield from self.__convert_data_to_entity_list__(data)
                except Exception as ex:
                    LOGGER.error(f'Unable to update table with {item} data from {category} category.')
                    LOGGER.error(str(ex))
            else:
                LOGGER.debug(f'{item} data from {category} category is not scrape-able.')

    def __get_function__(self, category:str) -> Callable[str,coc.abc.DataContainer]:
        match category:
            case "hero":
                return self.coc_client.get_hero
            case "pet":
                return self.coc_client.get_pet
            case "troop" | \
                 "elixir_troop" | \
                 "dark_elixir_troop" | \
                 "siege_machine" | \
                 "super_troop" | \
                 "home_troop" | \
                 "builder_troop":
                return self.coc_client.get_troop
            case "spell" | \
                 "elixir_spell" | \
                 "dark_elixir_spell":
                return self.coc_client.get_spell
            case _:
                LOGGER.debug(f'{category} is not a valid category.')

    def __get_data__(self, category:str) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        func = self.__get_function__(category)
        return self.__get_item_data__(func, category)

    def __update_table__(self, category:str) -> None:
        entities = self.__get_data__(category)
        self.__write_data_to_table__(entities=entities)

    def process_table(self) -> None:
        if self.scrape_enabled:
            LOGGER.info(f'Troop table {self.table} is updating.')
            for category in self.categories:
                try:
                    LOGGER.debug(f'Updating table with {category} data.')
                    self.__update_table__(category)
                except Exception as ex:
                    LOGGER.error(f'Unable to update table with {category} data.')
                    LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Troop table {self.table} is not updated because TroopSettings.ScrapeEnabled is {self.scrape_enabled}.')