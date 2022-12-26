import logging
import coc
import datetime

from typing import Dict
from typing import Union
from typing import Generator
from scraper import CONFIG
from scraper.storage import StorageHandler

LOGGER = logging.getLogger(__name__)
    
class GoldPassTableHandler(StorageHandler):
    """
    The Gold Pass table is updated once a month.  The table is updated with 
    the current season's data.

    Attributes
    ----------
    coc_client : coc.Client
        The client used to access the Clash of Clans API.
    table : str
        The name of the table.
    scrape_enabled : bool
        Determines if data scraping should be performed.
    
    Methods
    -------
    __convert_timedelta_to_days__(dt: datetime.timedelta) -> float
        Converts the given timedelta to days.
    __convert_data_to_entity_list__(data: coc) -> Generator[Dict[str,Union[float,int,str]],None,None]
        Converts the given data to an enumerable of entities.
    __update_table__() -> None
        Updates the table with the current Gold Pass Season data.
    process_table() -> None
        Public API to update the table with the current Gold Pass Season data.
    """

    configs = CONFIG['GoldPassSettings']
    scrape_enabled = configs['ScrapeEnabled']
    abandon_scrape_if_entity_exists = configs['AbandonScrapeIfEntityExists']

    def __init__(self, **kwargs) -> None:
        """
        Parameters
        ----------
        coc_client : coc.Client
            The client used to access the Clash of Clans API.
        """

        super().__init__(table_name=self.configs['TableName'], **kwargs)

    def __does_entity_exist__(self) -> bool:
        """
        Determines if the entity exists in the table.

        Returns
        -------
        bool
            True if the entity exists in the table, otherwise False.
        """

        LOGGER.debug(f'Checking if entity exists in table {self.table_name}.')
        partition_key = self.__get_partition_key__()
        row_key = self.__get_row_key__()
        entity = self.try_get_entity(partition_key, row_key, select='PartitionKey')
        return entity is not None

    def __convert_timedelta_to_days__(self, dt: datetime.timedelta) -> float:
        """
        Converts the given timedelta to days.

        Parameters
        ----------
        dt : datetime.timedelta
            The timedelta to convert.

        Returns
        -------
        float
            The timedelta converted to days.
        """

        return dt / datetime.timedelta(days=1)

    def __get_partition_key__(self) -> str:
        """
        Gets the partition key for the current month, i.e. the current year.

        Returns
        -------
        str
            The partition key for the current month.
        """

        return datetime.datetime.now().strftime('%Y')

    def __get_row_key__(self) -> str:
        """
        Gets the row key for the current month, i.e. the current month.

        Returns
        -------
        str
            The row key for the current month.
        """

        return datetime.datetime.now().strftime('%m')

    def __convert_data_to_entity_list__(self, data: coc.miscmodels.GoldPassSeason) -> Generator[Dict[str,Union[float,int,str]],None,None]:
        """
        Converts the given data to an enumerable of entities.

        Parameters
        ----------
        data : coc.miscmodels.GoldPassSeason
            The data to convert.

        Returns
        -------
        Generator[Dict[str,Union[float,int,str]],None,None]
            The data converted to an enumerable of entities.
        """

        LOGGER.debug(f'Creating entity for Gold Pass Season.')
        entity = dict()
        entity['PartitionKey'] = self.__get_partition_key__()
        entity['RowKey'] = self.__get_row_key__()
        entity['SeasonId'] = datetime.datetime.now().strftime('%Y-%m')
        entity['StartTime'] = data.start_time.time
        entity['EndTime'] = data.end_time.time
        entity['Duration'] = self.__convert_timedelta_to_days__(data.duration)
        yield entity

    async def __update_table__(self) -> None:
        """
        Scrape and update the table with the current Gold Pass Season data.
        """

        should_abandon_scrape = self.abandon_scrape_if_entity_exists and self.__does_entity_exist__()
        if should_abandon_scrape:
            LOGGER.info(f'Abandoning scrape because entity exists in table {self.table_name} and GoldPassSettings.AbandonScrapeIfEntityExists is {self.abandon_scrape_if_entity_exists}.')
            return None

        LOGGER.debug('Scraping Gold Pass data.')
        data = await self.coc_client.get_current_goldpass_season()
        entities = self.__convert_data_to_entity_list__(data)
        self.write_data_to_table(entities=entities)

    async def process_table(self, coc_client_handling: bool = True) -> None:
        """
        Public API to update the table with the current Gold Pass Season data.
        """

        if coc_client_handling:
            await self.start_coc_client_session()
        
        if self.scrape_enabled:
            try:
                LOGGER.info(f'Gold Pass table {self.table_name} is updating.')
                await self.__update_table__()
            except Exception as ex:
                LOGGER.error(f'Unable to update table with Gold Pass Season data.')
                LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Gold Pass table {self.table_name} is not updated because GoldPassSettings.ScrapeEnabled is {self.scrape_enabled}.')

        if coc_client_handling:
            await self.close_coc_client_session()