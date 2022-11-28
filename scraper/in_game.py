import logging
import coc
import datetime

from typing import List
from scraper import CONFIG
from scraper.storage import StorageHandler


LOGGER = logging.getLogger(__name__)

class TroopTableHandler(StorageHandler):

    configs = CONFIG['TroopSettings']
    table = configs['TableName']
    categories = configs['Categories']
    scrape_enabled = configs['ScrapeEnabled']

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

    def __get_entity_count__(self, data):
        return len(data.level)

    def __convert_data_to_entity_list__(self, data):
        LOGGER.debug(f'Creating entity for {data.name}.')
        for i in range(self.__get_entity_count__(data)):
            entity = dict()
            entity['PartitionKey'] = f'{data.id}_{data.level[i+1]}'
            entity['RowKey'] = f'{datetime.datetime.now().strftime("%Y-%m")}'
            entity['SeasonId'] = datetime.datetime.now().strftime('%Y-%m')
            entity['Id'] = data.id
            entity['Name'] = data.name
            entity['Level'] = data.level[i+1]
            yield entity

    def __get_item_data__(self, func, category:str):
        items = self.__get_item_list__(category)
        for item in items:
            LOGGER.debug(f'Scraping {item} data from {category} category.')
            data = func(item)
            return self.__convert_data_to_entity_list__(data)

    def __get_function__(self, category:str):
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

    def __get_data__(self, category:str):
        func = self.__get_function__(category)
        return self.__get_item_data__(func, category)

    def __update_table__(self, category:str) -> None:
        entities = self.__get_data__(category)
        self.__write_data_to_table__(entities=entities)

    def process_table(self) -> None:
        if self.scrape_enabled:
            LOGGER.debug(f'Troop table {self.table} is updating.')
            for category in self.categories:
                LOGGER.debug(f'Updating table with {category} data.')
                self.__update_table__(category)
        else:
            LOGGER.debug(f'Troop table {self.table} is not updated because TroopSettings.ScrapeEnabled is {self.scrape_enabled}.')