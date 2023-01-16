import logging
import coc
import datetime

from typing import Union
from collections.abc import Iterator
from scraper import CONFIG
from scraper.storage import TableStorageHandler
from scraper.utils import try_get_attr
from azure.data.tables import TableEntity

LOGGER = logging.getLogger(__name__)

class PlayerTroopsTableHandler(object):
    """
    This table contains a player's current troop progress in the game.

    Attributes
    ----------
    table_name : str
        The name of the table in Azure Table Storage.
    scrape_enabled : bool
        Whether the player's troop data should be scraped or not.
    abandon_scrape_if_entity_exists : bool
        Determines if the scrape should be abandoned if the entity exists in
        the table.
    validation_troop_id : str
        The troop id to use for validation if `abandon_scrape_if_entity_exists` is True.

    Methods
    -------
    process_table(data: coc.abc.BasePlayer) -> None
        Processes the player's troop data and adds it to the table.
    """

    configs = CONFIG['PlayerTroopsSettings']
    table_name = configs['TableName']
    scrape_enabled = configs['ScrapeEnabled']
    abandon_scrape_if_entity_exists = configs['AbandonScrapeIfEntityExists']
    validation_troop_id = configs['ValidationTroopId']

    def __init__(self, **kwargs) -> None:
        """
        Parameters
        ----------
        **kwargs
            Keyword arguments to pass to the TableStorageHandler class.
        """

        self.table_handler = TableStorageHandler(table_name=self.table_name, **kwargs)

    def __does_player_troop_data_exist(self, player_tag: str, troop_id: str = validation_troop_id) -> bool:
        """
        Checks if the player's troop data exists in the table.

        Parameters
        ----------
        player_tag : str
            The player tag to check if data exists for.
        troop_id : str, optional
            (Default: self.validation_troop_id) The troop id to check if 
            data exists for.
        
        Returns
        -------
        bool
            True if the player troop data exists in the table, False otherwise.
        """

        LOGGER.debug(f'Checking if troop {troop_id} of {player_tag} exists in table {self.table_name}.')
        
        # To speed up the checking process, we assume that every player that we
        # want to scrape has at least unlocked a single troop (troop_id: 4000000),
        # which is the 1st troop to be unlocked in the game. 
        partition_key = self.__get_partition_key(player_tag=player_tag, troop_id=troop_id)
        row_key = self.__get_row_key()

        entity = self.table_handler.try_get_entity(partition_key=partition_key, row_key=row_key, select='PartitionKey', retries_remaining=self.table_handler.retry_entity_extraction_count)
        return entity is not None

    def __is_super_troop_active(self, troop: coc.abc.DataContainer, data: coc.abc.BasePlayer) -> bool:
        """
        Checks if a super troop is active for the player.
        
        Parameters
        ----------
        troop : coc.abc.DataContainer
            The troop to check.
        data : coc.abc.BasePlayer
            The player data to check.

        Returns
        -------
        bool
            Whether the super troop is active or not.
        """

        return try_get_attr(data, 'is_super_troop', default=False) and troop in data.home_troops

    def __get_partition_key(self, player_tag: str, troop_id: str) -> str:
        """
        Gets the partition key for the player's troop data.

        Parameters
        ----------
        player_tag : str
            The player tag to get the partition key for.
        troop_id : str
            The troop id to get the partition key for.

        Returns
        -------
        str
            The partition key for the player's troop data.
        """

        return f'{player_tag.lstrip("#")}-{troop_id}'

    def __get_row_key(self) -> str:
        """
        Gets the row key for the player's troop data.

        Returns
        -------
        str
            The row key for the player's troop data.
        """

        return datetime.datetime.now().strftime('%Y-%m-%d')

    def __get_troop_list(self, data: coc.abc.BasePlayer) -> list[Union[coc.Hero, coc.Troop, coc.Spell]]:
        """
        Gets the list of troops for the player.

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The player data to get the troops for.

        Returns
        -------
        list[Union[coc.Hero, coc.Troop, coc.Spell]]
            The list of troops for the player.
        """

        inactive_super_troops = list(set(data.super_troops) - set(data.home_troops))
        return data.heroes + data.hero_pets + data.spells + data.home_troops + data.builder_troops + inactive_super_troops
        

    def __convert_data_to_entity_list(self, data: coc.abc.BasePlayer) -> Iterator[TableEntity]:
        """
        Converts the player's troop data to a list of table entities.

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The player data to convert to table entities.
        
        Yields
        ------
        TableEntity
            The list of table entities.
        """

        troop_list = self.__get_troop_list(data=data)
        for troop in troop_list:
            # Skip troop if troop object is None or its Id and Level is None
            if (troop is None or \
                try_get_attr(troop, 'id') is None or \
                try_get_attr(troop, 'level') is None):
                LOGGER.debug(f'Skipping {troop} as it is either (1) None, (2) has None id, or (3) has None level.')
                continue

            entity = TableEntity()
            entity['PartitionKey'] = self.__get_partition_key(player_tag=try_get_attr(data, 'tag'), troop_id=try_get_attr(troop, 'id'))
            entity['RowKey'] = self.__get_row_key()
            entity['TroopId'] = try_get_attr(troop, 'id')
            entity['TroopLevel'] = try_get_attr(troop, 'level')
            entity['TroopVillage'] = try_get_attr(troop, 'village')
            entity['TroopTownhallMaxLevel'] = troop.get_max_level_for_townhall(data.town_hall) if hasattr(troop, 'get_max_level_for_townhall') else None
            entity['TroopIsMaxForTownhall'] = try_get_attr(troop, 'is_max_for_townhall')
            entity['TroopIsActive'] = self.__is_super_troop_active(troop, data) if try_get_attr(data, 'is_super_troop') is not None else None
            
            yield entity

    def process_table(self, data: coc.abc.BasePlayer) -> None:
        """
        Processes the player's troop data and inserts it into the table.

        Parameters
        ----------
        data : coc.abc.BasePlayer
            The player data to process.
        """

        should_abandon_scrape = self.abandon_scrape_if_entity_exists and self.__does_player_troop_data_exist(player_tag=try_get_attr(data, 'tag'))
        if should_abandon_scrape:
            LOGGER.info(f'Abandoning scrape for {try_get_attr(data, "tag")} as data already exists in table {self.table_name}.')
            return None

        entities = self.__convert_data_to_entity_list(data=data)
        self.table_handler.write_data_to_table(entities=entities)