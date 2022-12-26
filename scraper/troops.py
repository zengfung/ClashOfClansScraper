import logging
import coc
import datetime

from typing import Any
from typing import List
from typing import Callable
from typing import Dict
from typing import Generator
from typing import Union
from scraper import CONFIG
from scraper.storage import StorageHandler
from scraper.utils import try_get_attr

LOGGER = logging.getLogger(__name__)

class TroopTableHandler(StorageHandler):
    """
    The troop table is updated once a month. This data is not scraped 
    directly from the Clash of Clans API, and is instead obtained from the 
    coc.py library. Therefore, the data may not always be up to date.

    Attributes
    ----------
    coc_client : coc.Client
        The client used to access the Clash of Clans API.
    table : str
        The name of the table to be updated.
    categories : List[str]
        The categories of troops to scrape.
    scrape_enabled : bool
        Determines if data scraping should be performed.
    null_id_scrape_enabled : bool
        Determines if data scraping should be performed for items with a 
        null id.
    
    Methods
    -------
    __is_valid_category__(category: str) -> bool
        Determines if the given category is valid.
    __get_item_list__(category: str) -> List[str]
        Returns the list of items for the given category.
    __get_entity_count__(data: coc.abc.DataContainer) -> int
        Returns the number of entities to insert/upsert to the table for a 
        particular item.
    __try_get_attr__(data: coc.abc.DataContainer, attr: str, index: int = None) -> Union[float,int,str]
        Returns the value of the given attribute for the given data if the 
        attribute exists. Otherwise, returns None.
    __convert_data_to_entity_list__(data: coc.abc.DataContainer) -> Generator[Dict[str,Union[float,int,str]],None,None]
        Converts the given data to a list of entities to insert/upsert to
        the table.
    __get_item_data__(item: str, category: str) -> coc.abc.DataContainer
        Gets the data for the given item and category.
    __get_data__(category: str) -> Generator[Dict[str,Union[float,int,str]],None,None]
        Returns a generator of dictionaries containing the data for each
        entity of all items in the category to be inserted/upserted to the table.
    __update_table__(category: str) -> None
        Updates the table for the given category.
    process_table() -> None
        Updates the troop table.
    """

    configs = CONFIG['TroopSettings']
    table = configs['TableName']
    categories = configs['Categories']
    scrape_enabled = configs['ScrapeEnabled']
    null_id_scrape_enabled = configs['NullIdScrapeEnabled']

    def __init__(self, coc_client:coc.Client, **kwargs) -> None:
        """
        Parameters
        ----------
        coc_client : coc.Client
            The client used to access the Clash of Clans API.
        """

        super().__init__(self.table, **kwargs)
        self.coc_client = coc_client
        self.categories = [category for category in self.categories if self.__is_valid_category__(category)]

    def __is_valid_category__(self, category: str) -> bool:
        """
        Determines if the given category is valid.

        Parameters
        ----------
        category : str
            The category to check.

        Returns
        -------
        bool
            True if the category is valid, False otherwise.
        """

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
                LOGGER.error(f'{category} is not a valid category.')
                return False

    def __get_item_list__(self, category: str) -> List[str]:
        """
        Returns the list of items for the given category.

        Parameters
        ----------
        category : str
            The category to get the item list for.

        Returns
        -------
        List[str]
            The list of items for the given category.
        """

        match category:
            case "hero":
                return coc.HERO_ORDER
            case "pet":
                return coc.HERO_PETS_ORDER
            case "troop":
                return coc.HOME_TROOP_ORDER + coc.SUPER_TROOP_ORDER
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

    def __get_entity_count__(self, data: coc.abc.DataContainer) -> int:
        """
        Returns the number of entities to insert/upsert to the table for a
        particular item.

        Parameters
        ----------
        data : coc.abc.DataContainer
            The data to get the entity count for.

        Returns
        -------
        int
            The number of entities to insert/upsert to the table for a
            particular item.
        """

        attrs = dir(data)
        count = 0
        for attr in attrs:
            a = getattr(data, attr)
            if isinstance(a, list):
                count = max(count, len(a))
        return count

    def __convert_data_to_entity_list__(self, data: coc.abc.DataContainer) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        """
        Converts the given data to a list of entities to insert/upsert to
        the table.

        Parameters
        ----------
        data : coc.abc.DataContainer
            The data to convert to a list of entities.

        Yields
        ------
        Dict[str,Union[float,int,str]]
            A dictionary containing the data for an entity to insert/upsert
            to the table.
        """

        LOGGER.debug(f'Creating entity for {try_get_attr(data, "name")} with ID {try_get_attr(data, "id")}.')
        for i in range(self.__get_entity_count__(data)):
            entity = dict()
            # Mandatory keys
            # TODO: How to deal with hero pet scenario where there is no id?
            entity['PartitionKey'] = f'{try_get_attr(data, "id")}_{try_get_attr(data, "level", i+1, default=i+1)}'
            entity['RowKey'] = f'{datetime.datetime.now().strftime("%Y-%m")}'

            # Identity keys
            entity['SeasonId'] = datetime.datetime.now().strftime('%Y-%m')
            entity['Id'] = try_get_attr(data, "id")
            entity['Name'] = try_get_attr(data, "name")

            # Details
            lab_level = try_get_attr(data, "lab_level", i+1)
            townhall_level = try_get_attr(data, "lab_to_townhall", lab_level) if lab_level is not None else None
            upgrade_time = try_get_attr(data, "upgrade_time", i+1)
            entity['Range'] = try_get_attr(data, "range", i+1)
            entity['Dps'] = try_get_attr(data, "dps", i+1)
            entity['GroundTarget'] = try_get_attr(data, "ground_target")
            entity['Hitpoints'] = try_get_attr(data, "hitpoints", i+1)
            entity['HousingSpace'] = try_get_attr(data, "housing_space")
            entity['LabLevel'] = try_get_attr(data, "lab_level", i+1)
            entity['TownhallLevel'] = townhall_level
            entity['Speed'] = try_get_attr(data, "speed", i+1)
            entity['Level'] = try_get_attr(data, "level", i+1, default=i+1)
            entity['UpgradeCost'] = try_get_attr(data, "upgrade_cost", i+1)
            entity['UpgradeResource'] = try_get_attr(data, "upgrade_resource").name
            entity['UpgradeTime'] = upgrade_time.total_seconds() if upgrade_time is not None else None
            entity['IsHomeVillage'] = try_get_attr(data, "_is_home_village")

            # Spells and troops
            # Note: Cooldown and Duration only applies to super troops, and 
            # is always a list of 1 item.
            cooldown = try_get_attr(data, "cooldown", 1)
            duration = try_get_attr(data, "duration", 1)
            original_troop = try_get_attr(data, "original_troop")
            entity['TrainingCost'] = try_get_attr(data, "training_cost", i+1)
            entity['TrainingTime'] = try_get_attr(data, "training_time", i+1)
            entity['IsElixirSpell'] = try_get_attr(data, "is_elixir_spell")
            entity['IsDarkSpell'] = try_get_attr(data, "is_dark_spell")
            entity['IsElixirTroop'] = try_get_attr(data, "is_elixir_troop")
            entity['IsDarkTroop'] = try_get_attr(data, "is_dark_troop")
            entity['IsSiegeMachine'] = try_get_attr(data, "is_siege_machine")
            entity['IsSuperTroop'] = try_get_attr(data, "is_super_troop")
            entity['Cooldown'] = cooldown.total_seconds() if cooldown is not None else None
            entity['Duration'] = duration.total_seconds() if duration is not None else None
            entity['MinOriginalLevel'] = try_get_attr(data, "min_original_level")
            entity['OriginalTroopId'] = try_get_attr(original_troop, "id") if original_troop is not None else None

            # Heroes and pets
            regeneration_time = try_get_attr(data, "regeneration_time", i+1)
            entity['AbilityTime'] = try_get_attr(data, "ability_time", i+1)
            entity['AbilityTroopCount'] = try_get_attr(data, "ability_troop_count", i+1)
            entity['RequiredTownhallLevel'] = try_get_attr(data, "required_th_level", i+1)
            entity['RegenerationTime'] = regeneration_time.total_seconds() if regeneration_time is not None else None
            
            yield entity

    def __get_item_data__(self, item: str, category: str) -> coc.abc.DataContainer:
        """
        Gets the data for the given item and category.

        Parameters
        ----------
        item : str
            The item to get the data for.
        category : str
            The category of the item.
        
        Returns
        -------
        coc.abc.DataContainer
            The data for the given item and category.
        """

        match category:
            case "hero":
                return self.coc_client.get_hero(item)
            case "pet":
                return self.coc_client.get_pet(item)
            case "troop" | \
                 "elixir_troop" | \
                 "dark_elixir_troop" | \
                 "siege_machine" | \
                 "super_troop" | \
                 "home_troop":
                return self.coc_client.get_troop(item, is_home_village=True)
            case "builder_troop":
                return self.coc_client.get_troop(item, is_home_village=False)
            case "spell" | \
                 "elixir_spell" | \
                 "dark_elixir_spell":
                return self.coc_client.get_spell(item)
            case _:
                LOGGER.error(f'{category} is not a valid category.')
                return None

    def __get_data__(self, category: str) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        """
        Retrieves the data for the given category and converts it to a list
        of entities to insert/upsert to the table.

        Parameters
        ----------
        category : str
            The category of the data to retrieve.

        Yields
        ------
        Dict[str,Union[float,int,str]]
            A dictionary containing the data for an entity to insert/upsert
            to the table.
        """

        items = self.__get_item_list__(category)
        for item in items:
            data = self.__get_item_data__(item, category)

            if data is None:
                LOGGER.warning(f'No data found for {item}.')
                LOGGER.warning(f'{item} data from {category} category is not scrape-able.')
                continue

            if try_get_attr(data, 'id') is None and \
               not self.null_id_scrape_enabled:
                LOGGER.warning(f'No ID found for {item} and null_id_scrape_enabled is set to {self.null_id_scrape_enabled}.')
                LOGGER.warning(f'{item} data from {category} category is not scrape-able.')
                continue

            try:
                LOGGER.debug(f'Scraping {item} data from {category} category.')
                yield from self.__convert_data_to_entity_list__(data)
            except Exception as ex:
                LOGGER.error(f'Unable to update table with {item} data from {category} category.')
                LOGGER.error(str(ex))

    def __update_table__(self, category: str) -> None:
        """
        Updates the table with the data for the given category.

        Parameters
        ----------
        category : str
            The category of the data to retrieve.
        """
        
        # TODO: How to prevent data scraping if data is already present in table?
        entities = self.__get_data__(category)
        self.__write_data_to_table__(entities=entities)

    def process_table(self) -> None:
        """
        Updates the table with the data for all categories.
        """

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