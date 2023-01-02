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
    
class GoldPassTableHandler(CocClientHandler):
    """
    The Gold Pass table is updated once a month.  The table is updated with 
    the current season's data.

    Attributes
    ----------
    scrape_enabled : bool
        Determines if data scraping should be performed.
    abandon_scrape_if_entity_exists : bool
        Determines if the scrape should be abandoned if the entity exists in
        the table.
    
    Methods
    -------
    process_table() -> None
        Public API to update the table with the current Gold Pass Season data.
    """

    configs = CONFIG['GoldPassSettings']
    table_name = configs['TableName']
    scrape_enabled = configs['ScrapeEnabled']
    abandon_scrape_if_entity_exists = configs['AbandonScrapeIfEntityExists']

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

    def __does_entity_exist(self) -> bool:
        """
        Determines if the entity exists in the table.

        Returns
        -------
        bool
            True if the entity exists in the table, otherwise False.
        """

        LOGGER.debug(f'Checking if entity exists in table {self.table_name}.')
        partition_key = self.__get_partition_key()
        row_key = self.__get_row_key()
        entity = self.table_handler.try_get_entity(partition_key, row_key, select='PartitionKey', retries_remaining=self.table_handler.retry_entity_extraction_count)
        return entity is not None

    def __convert_timedelta_to_days(self, dt: datetime.timedelta) -> float:
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

    def __get_partition_key(self) -> str:
        """
        Gets the partition key for the current month, i.e. the current year.

        Returns
        -------
        str
            The partition key for the current month.
        """

        return datetime.datetime.now().strftime('%Y')

    def __get_row_key(self) -> str:
        """
        Gets the row key for the current month, i.e. the current month.

        Returns
        -------
        str
            The row key for the current month.
        """

        return datetime.datetime.now().strftime('%m')

    def __convert_data_to_entity_list(self, data: coc.miscmodels.GoldPassSeason) -> Iterator[TableEntity]:
        """
        Converts the given data to an iterable of entities.

        Parameters
        ----------
        data : coc.miscmodels.GoldPassSeason
            The data to convert.

        Yields
        ------
        azure.data.tables.TableEntity
            The data converted to an enumerable of entities.
        """

        LOGGER.debug(f'Creating entity for Gold Pass Season.')
        start_time = try_get_attr(data, 'start_time', default=None)
        end_time = try_get_attr(data, 'end_time', default=None)
        duration = try_get_attr(data, 'duration', default=None)

        entity = TableEntity()
        entity['PartitionKey'] = self.__get_partition_key()
        entity['RowKey'] = self.__get_row_key()
        entity['SeasonId'] = datetime.datetime.now().strftime('%Y%m')
        entity['StartTime'] = start_time.time if start_time is not None else None
        entity['EndTime'] = end_time.time if end_time is not None else None
        entity['Duration'] = self.__convert_timedelta_to_days(duration) if duration is not None else None
        yield entity

    async def __get_data(self, retries_remaining: int = 0) -> coc.miscmodels.GoldPassSeason:
        """
        Gets the current Gold Pass Season data.

        Parameters
        ----------
        retries_remaining : int, optional
            (Default: 0) The number of retries remaining.

        Raises
        ------
        coc.errors.Forbidden
            If the client is not authorized to access the data.

        Returns
        -------
        coc.miscmodels.GoldPassSeason
            The current Gold Pass Season data.
        """

        assert retries_remaining >= 0, 'retries_remaining must be greater than or equal to 0.'
        try:
            LOGGER.debug('Getting Gold Pass data.')
            return await self.coc_client.get_current_goldpass_season()
        except coc.errors.Forbidden as ex:
            LOGGER.error(f'Encountered Forbidden error while getting Gold Pass data.')
            
            if retries_remaining > 0:
                LOGGER.debug(f'Retrying getting Gold Pass data.  Retries remaining: {retries_remaining}.')
                await self.restart_coc_client_session()
                return await self.__get_data(retries_remaining-1)
            else:
                raise ex

    async def __update_table(self) -> None:
        """
        Scrape and update the table with the current Gold Pass Season data.

        Raises
        ------
        coc.errors.Forbidden
            If the client is not authorized to access the data.

        Returns
        -------
        None
        """

        should_abandon_scrape = self.abandon_scrape_if_entity_exists and self.__does_entity_exist()
        if should_abandon_scrape:
            LOGGER.debug(f'Abandoning scrape because entity exists in table {self.table_name} and GoldPassSettings.AbandonScrapeIfEntityExists is {self.abandon_scrape_if_entity_exists}.')
            return None

        LOGGER.debug('Scraping Gold Pass data.')
        data = await self.__get_data()
        entities = self.__convert_data_to_entity_list(data)
        self.table_handler.write_data_to_table(entities=entities)

    async def process_table(self, coc_client_handling: bool = True) -> None:
        """
        Public API to update the table with the current Gold Pass Season data.

        Parameters
        ----------
        coc_client_handling : bool, optional
            (Default: True) Whether or not to handle the coc client session
            automatically.

        Raises
        ------
        coc.errors.Forbidden
            If the client is not authorized to access the data.

        Returns
        -------
        None
        """

        if coc_client_handling:
            await self.start_coc_client_session()
        
        if self.scrape_enabled:
            try:
                LOGGER.debug(f'Gold Pass table {self.table_name} is updating.')
                await self.__update_table()
            except Exception as ex:
                LOGGER.error(f'Unable to update table with Gold Pass Season data.')
                LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Gold Pass table {self.table_name} is not updated because GoldPassSettings.ScrapeEnabled is {self.scrape_enabled}.')

        if coc_client_handling:
            await self.close_coc_client_session()