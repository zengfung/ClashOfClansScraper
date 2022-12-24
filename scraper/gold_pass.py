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
    table = configs['TableName']
    scrape_enabled = configs['ScrapeEnabled']

    def __init__(self, coc_client: coc.Client, **kwargs) -> None:
        """
        Parameters
        ----------
        coc_client : coc.Client
            The client used to access the Clash of Clans API.
        """

        super().__init__(self.table, **kwargs)
        self.coc_client = coc_client

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
        entity['PartitionKey'] = datetime.datetime.now().strftime('%Y')
        entity['RowKey'] = datetime.datetime.now().strftime('%m')
        entity['SeasonId'] = datetime.datetime.now().strftime('%Y-%m')
        entity['StartTime'] = data.start_time.time
        entity['EndTime'] = data.end_time.time
        entity['Duration'] = self.__convert_timedelta_to_days__(data.duration)
        yield entity

    async def __update_table__(self) -> None:
        """
        Scrape and update the table with the current Gold Pass Season data.
        """

        # TODO: How to prevent data scraping if data is already present in table?
        LOGGER.debug('Scraping Gold Pass data.')
        data = await self.coc_client.get_current_goldpass_season()
        entities = self.__convert_data_to_entity_list__(data)
        self.__write_data_to_table__(entities=entities)

    async def process_table(self) -> None:
        """
        Public API to update the table with the current Gold Pass Season data.
        """
        
        if self.scrape_enabled:
            try:
                LOGGER.info(f'Gold Pass table {self.table} is updating.')
                await self.__update_table__()
            except Exception as ex:
                LOGGER.error(f'Unable to update table with Gold Pass Season data.')
                LOGGER.error(str(ex))
        else:
            LOGGER.info(f'Gold Pass table {self.table} is not updated because GoldPassSettings.ScrapeEnabled is {self.scrape_enabled}.')