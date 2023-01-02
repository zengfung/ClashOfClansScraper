import logging
import coc
import datetime

from collections.abc import Iterator
from scraper import CONFIG
from scraper.coc_client import CocClientHandler
from scraper.storage import TableStorageHandler
from scraper.utils import try_get_attr
from azure.data.tables import TableEntity

LOGGER = logging.getLogger(__name__)

class TroopTableHandler(CocClientHandler):
    """
    The troop table is updated once a month. This data is not scraped 
    directly from the Clash of Clans API, and is instead obtained from the 
    coc.py library. Therefore, the data may not always be up to date.

    Attributes
    ----------
    categories : list[str]
        The categories of troops to scrape.
    scrape_enabled : bool
        Determines if data scraping should be performed.
    null_id_scrape_enabled : bool
        Determines if data scraping should be performed for items with a 
        null id.
    abandon_scrape_if_entity_exists : bool
        Determines if the scrape should be abandoned if the entity exists in
        the table.
    
    Methods
    -------
    process_table(coc_client_handling: bool = True) -> None
        Updates the troop table.
    """

    configs = CONFIG['TroopSettings']
    table_name = configs['TableName']
    categories = configs['Categories']
    scrape_enabled = configs['ScrapeEnabled']
    abandon_scrape_if_entity_exists = configs['AbandonScrapeIfEntityExists']
    null_id_scrape_enabled = configs['NullIdScrapeEnabled']

    def __init__(
            self, 
            coc_email: str,
            coc_password: str,
            coc_client: coc.Client = None,
            **kwargs) -> None:
        """
        Parameters
        ----------
        **kwargs
            Keyword arguments to pass to the StorageHandler class.
        """

        super().__init__(coc_email=coc_email, coc_password=coc_password, coc_client=coc_client)
        self.table_handler = TableStorageHandler(table_name=self.table_name, **kwargs)
        self.categories = [category for category in self.categories if self.__is_valid_category(category)]

    def __is_valid_category(self, category: str) -> bool:
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

    def __get_item_list(self, category: str) -> list[str]:
        """
        Returns the list of items for the given category.

        Parameters
        ----------
        category : str
            The category to get the item list for.

        Returns
        -------
        list[str]
            The list of items for the given category.
        """

        match category:
            case "hero":
                return coc.HERO_ORDER
            case "pet":
                return coc.HERO_PETS_ORDER
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
            case _:
                LOGGER.error(f'No available list for item type {category}!')
                return None

    def __get_entity_count(self, data: coc.abc.DataContainer) -> int:
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

    def __convert_data_to_entity_list(self, data: coc.abc.DataContainer) -> Iterator[TableEntity]:
        """
        Converts the given data to a list of entities to insert/upsert to
        the table.

        Parameters
        ----------
        data : coc.abc.DataContainer
            The data to convert to a list of entities.

        Yields
        ------
        collections.abc.Iterator[azure.data.tables.TableEntity]
            The data converted to an enumerable of entities.
        """

        LOGGER.debug(f'Creating entity for {try_get_attr(data, "name")} with ID {try_get_attr(data, "id")}.')
        for i in range(self.__get_entity_count(data)):
            entity = TableEntity()
            # Mandatory keys
            # TODO: How to deal with hero pet scenario where there is no unique id?
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

    def __is_item_from_home_village(self, item: str, category: str) -> bool:
        """
        Returns whether the given item is from the home village.

        Parameters
        ----------
        item : str
            The item to check.
        category : str
            The category of the item.

        Returns
        -------
        bool
            Whether the given item is from the home village.
        """

        match category:
            case 'builder_troop':
                return False
            case 'hero':
                return item != 'Battle Machine'
            case _:
                return True

    def __does_item_data_exist(self, item: str, category: str) -> bool:
        """
        Returns whether or not the given item data exists in the table.

        Parameters
        ----------
        item : str
            The item to check for.
        category : str
            The category of the item to check for.

        Returns
        -------
        bool
            True if the item data exists in the table, otherwise False.
        """

        LOGGER.debug(f'Checking if {item} exists in table {self.table_name}.')
        
        row_key = datetime.datetime.now().strftime('%Y-%m')
        is_home_village = 'true' if self.__is_item_from_home_village(item, category) else 'false'

        query_filter = f"RowKey eq '{row_key}' and Name eq '{item}' and IsHomeVillage eq {is_home_village}"
        results = self.table_handler.try_query_entities(query_filter=query_filter, retries_remaining=self.table_handler.retry_entity_extraction_count, select='PartitionKey')
        
        has_results = bool(next(results, False))
        return has_results
    
    def __get_item_data(self, item: str, category: str) -> coc.abc.DataContainer:
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
            case "elixir_troop" | \
                "dark_elixir_troop" | \
                "siege_machine" | \
                "super_troop" | \
                "home_troop":
                return self.coc_client.get_troop(item, is_home_village=True)
            case "builder_troop":
                return self.coc_client.get_troop(item, is_home_village=False)
            case "spell":
                return self.coc_client.get_spell(item)
            case _:
                LOGGER.error(f'{category} is not a valid category.')
                return None

    def __get_data(self, category: str) -> Iterator[TableEntity]:
        """
        Retrieves the data for the given category and converts it to a list
        of entities to insert/upsert to the table.

        Parameters
        ----------
        category : str
            The category of the data to retrieve.

        Yields
        ------
        azure.data.tables.TableEntity
            A table entity to insert/upsert to the table.
        """

        items = self.__get_item_list(category)
        for item in items:
            should_abaondon_scrape = self.abandon_scrape_if_entity_exists and self.__does_item_data_exist(item, category)
            if should_abaondon_scrape:
                LOGGER.debug(f'Abandoning scrape for {item} in category {category} because it already exists.')
                continue

            data = self.__get_item_data(item, category)

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
                yield from self.__convert_data_to_entity_list(data)
            except Exception as ex:
                LOGGER.error(f'Unable to update table with {item} data from {category} category.')
                LOGGER.error(str(ex))

    def __update_table(self, category: str) -> None:
        """
        Updates the table with the data for the given category.

        Parameters
        ----------
        category : str
            The category of the data to retrieve.

        Returns
        -------
        None
        """
        
        entities = self.__get_data(category)
        self.table_handler.write_data_to_table(entities=entities)

    async def process_table(self, coc_client_handling: bool = True) -> None:
        """
        Updates the table with the data for all categories.

        Parameters
        ----------
        coc_client_handling : bool, optional
            (Default: True) Whether or not to handle the coc client session
            automatically.

        Returns
        -------
        None
        """

        if coc_client_handling:
            await self.start_coc_client_session()

        if self.scrape_enabled:
            LOGGER.debug(f'Troop table {self.table_name} is updating.')
            for category in self.categories:
                try:
                    LOGGER.debug(f'Updating table with {category} data.')
                    self.__update_table(category)
                except Exception as ex:
                    LOGGER.error(f'Unable to update table with {category} data.')
                    LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Troop table {self.table_name} is not updated because TroopSettings.ScrapeEnabled is {self.scrape_enabled}.')

        if coc_client_handling:
            await self.close_coc_client_session()